#!/usr/bin/env bash
# Reproduce the submitted finite models using the pinned upstream TLC jar.
# Supports zero, one, or multiple model-family arguments and executes all
# (model, .cfg) pairs in parallel (LPT-scheduled, bounded by TLA_JOBS and
# per-JVM TLA_MAX_HEAP) while emitting deterministic ordered logs + coverage.
set -euo pipefail
cd "$(dirname "$0")/.."
repo_dir="$PWD"
base_root="tla/execution_models"

group_roots=()
if [[ $# -eq 0 ]]; then
  group_roots=("$base_root")
else
  for fam in "$@"; do
    if [[ ! "$fam" =~ ^[a-z0-9_]+$ ]]; then
      echo "Usage: $0 [model-family ...]" >&2
      exit 2
    fi
    if [[ ! -d "$base_root/$fam" ]]; then
      echo "Unknown model family: $fam" >&2
      exit 2
    fi
    group_roots+=("$base_root/$fam")
  done
fi

jar="$repo_dir/tla/tools/tla2tools.jar"
expected=936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88
if [[ ! -f "$jar" ]]; then
  mkdir -p "$(dirname "$jar")"
  curl --proto '=https' --tlsv1.2 -fL --retry 3 https://github.com/tlaplus/tlaplus/releases/download/v1.7.4/tla2tools.jar -o "$jar"
fi
actual="$(shasum -a 256 "$jar" | cut -d ' ' -f 1)"
[[ "$actual" == "$expected" ]] || { echo 'TLC jar checksum mismatch' >&2; exit 1; }

exec python3 - "$repo_dir" "$jar" "${group_roots[@]}" <<'PY'
from pathlib import Path
import concurrent.futures
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading

repo_dir = Path(sys.argv[1])
jar_path = Path(sys.argv[2])
group_roots = [repo_dir / p for p in sys.argv[3:]]

sys.path.insert(0, str(repo_dir / "scripts"))
from tlc_coverage import CoverageError, check_logs  # noqa: E402

SUMMARY_RE = re.compile(r"([0-9,]+) states generated")


def detect_jobs_and_heap() -> tuple[int, str]:
    cpus = os.cpu_count() or 4
    avail_mb = 8192
    try:
        for line in Path("/proc/meminfo").read_text().splitlines():
            if line.startswith("MemAvailable:"):
                avail_mb = int(line.split()[1]) // 1024
                break
    except OSError:
        pass

    env_jobs = os.environ.get("TLA_JOBS", "").strip()
    if env_jobs.isdigit() and int(env_jobs) >= 1:
        jobs = int(env_jobs)
    else:
        mem_slots = max(1, avail_mb // 2048)
        jobs = max(1, min(cpus, mem_slots, 12))

    heap = os.environ.get("TLA_MAX_HEAP", "").strip() or "2560m"
    return jobs, heap


def discover_model_dirs(roots: list[Path]) -> list[Path]:
    found: set[Path] = set()
    for root in roots:
        for cfg in root.glob("**/02_models/*.cfg"):
            if cfg.is_file():
                found.add(cfg.parent)
    return sorted(found)


def estimate_cfg_weight(src_dir: Path, tla_file: Path, cfg_file: Path) -> tuple[int, int]:
    ev_dir = src_dir.parent / "04_evidence"
    candidates = [ev_dir / f"{cfg_file.stem}.log"]
    if ev_dir.is_dir():
        candidates.extend(ev_dir.glob(f"runs/**/{cfg_file.stem}.log"))
    for log_p in candidates:
        if log_p.is_file():
            try:
                tail = log_p.read_bytes()[-4096:].decode("utf-8", errors="ignore")
                matches = SUMMARY_RE.findall(tail)
                if matches:
                    return (2, int(matches[-1].replace(",", "")))
            except OSError:
                pass
    try:
        sz = tla_file.stat().st_size + cfg_file.stat().st_size
    except OSError:
        sz = 0
    return (1, sz)


def main() -> int:
    jobs, max_heap = detect_jobs_and_heap()
    model_dirs = discover_model_dirs(group_roots)
    if not model_dirs:
        return 0

    with tempfile.TemporaryDirectory() as scratch_s:
        scratch = Path(scratch_s)
        models = []
        tasks = []

        for m_idx, src_dir in enumerate(model_dirs):
            tla_files = sorted(
                p for p in src_dir.glob("*.tla")
                if p.is_file() and "_TTrace_" not in p.name
            )
            if len(tla_files) != 1:
                rel = src_dir.relative_to(repo_dir) if src_dir.is_relative_to(repo_dir) else src_dir
                print(f"Ambiguous model directory: {rel}", file=sys.stderr)
                return 1

            tla_src = tla_files[0]
            cfg_srcs = sorted(p for p in src_dir.glob("*.cfg") if p.is_file())
            group_dir = Path(tempfile.mkdtemp(prefix="model.", dir=scratch))
            frozen_tla = group_dir / tla_src.name
            shutil.copyfile(tla_src, frozen_tla)
            frozen_cfgs = []
            for cfg_src in cfg_srcs:
                dst = group_dir / cfg_src.name
                shutil.copyfile(cfg_src, dst)
                frozen_cfgs.append((cfg_src, dst))

            models.append({
                "idx": m_idx,
                "src_dir": src_dir,
                "group_dir": group_dir,
                "model_name": frozen_tla.stem,
                "frozen_tla": frozen_tla,
                "num_cfgs": len(frozen_cfgs),
                "results": [None] * len(frozen_cfgs),
                "done_count": 0,
                "emitted": False,
            })

            for c_idx, (cfg_src, cfg_dst) in enumerate(frozen_cfgs):
                weight = estimate_cfg_weight(src_dir, tla_src, cfg_src)
                tasks.append((weight, m_idx, c_idx, cfg_dst))

        # Schedule heaviest configurations first (LPT) to minimize tail latency.
        tasks.sort(key=lambda item: (-item[0][0], -item[0][1], item[1], item[2]))

        lock = threading.Lock()
        stop_event = threading.Event()
        active_procs: set[subprocess.Popen] = set()
        first_failure = None  # (m_idx, c_idx, rc, err_msg)
        next_emit = 0

        def terminate_procs(procs):
            for proc in procs:
                try:
                    proc.terminate()
                except OSError:
                    pass

        def flush_ready_locked() -> tuple[int, str | None] | None:
            nonlocal next_emit
            while next_emit < len(models):
                m = models[next_emit]
                if m["done_count"] < m["num_cfgs"]:
                    break
                log_paths = []
                for c_idx, res in enumerate(m["results"]):
                    rc, log_path = res
                    txt = log_path.read_text(errors="replace")
                    sys.stdout.write(txt)
                    if not txt.endswith("\n"):
                        sys.stdout.write("\n")
                    sys.stdout.flush()
                    if rc != 0:
                        return (rc, None)
                    log_paths.append(log_path)
                try:
                    cov = check_logs(log_paths, m["model_name"])
                    cov["model_sha256"] = hashlib.sha256(m["frozen_tla"].read_bytes()).hexdigest()
                except (OSError, CoverageError) as err:
                    return (1, str(err))
                print(json.dumps(cov, indent=2), flush=True)
                shutil.rmtree(m["group_dir"], ignore_errors=True)
                m["emitted"] = True
                next_emit += 1
            return None

        def run_cfg(task):
            _, m_idx, c_idx, cfg_dst = task
            if stop_event.is_set():
                return
            m = models[m_idx]
            group_dir = m["group_dir"]
            run_dir = Path(tempfile.mkdtemp(prefix="run.", dir=group_dir))
            states_dir = run_dir / "states"
            java_tmp = run_dir / "java"
            states_dir.mkdir(parents=True, exist_ok=True)
            java_tmp.mkdir(parents=True, exist_ok=True)
            log_path = run_dir / "tlc.log"

            cmd = [
                "java",
                f"-Djava.io.tmpdir={java_tmp}",
                f"-Xmx{max_heap}",
                "-XX:+UseParallelGC",
                "-cp",
                str(jar_path),
                "tlc2.TLC",
                "-workers",
                "1",
                "-coverage",
                "1",
                "-metadir",
                str(states_dir),
                "-config",
                cfg_dst.name,
                m["frozen_tla"].name,
            ]

            with open(log_path, "wb") as lf:
                proc = subprocess.Popen(
                    cmd,
                    cwd=group_dir,
                    stdout=lf,
                    stderr=subprocess.STDOUT,
                )
                with lock:
                    if stop_event.is_set():
                        proc.terminate()
                    else:
                        active_procs.add(proc)
                try:
                    rc = proc.wait()
                finally:
                    with lock:
                        active_procs.discard(proc)

            shutil.rmtree(states_dir, ignore_errors=True)
            shutil.rmtree(java_tmp, ignore_errors=True)

            nonlocal first_failure
            to_kill = []
            with lock:
                if stop_event.is_set():
                    return
                m["results"][c_idx] = (rc, log_path)
                m["done_count"] += 1
                if rc != 0:
                    stop_event.set()
                    first_failure = (m_idx, c_idx, rc, None)
                    to_kill = list(active_procs)
                else:
                    err_info = flush_ready_locked()
                    if err_info is not None:
                        stop_event.set()
                        first_failure = (next_emit, 0, err_info[0], err_info[1])
                        to_kill = list(active_procs)
            if to_kill:
                terminate_procs(to_kill)

        effective_workers = min(jobs, len(tasks))
        with concurrent.futures.ThreadPoolExecutor(max_workers=effective_workers) as pool:
            futures = [pool.submit(run_cfg, t) for t in tasks]
            for fut in futures:
                fut.result()

        if first_failure is not None:
            fail_m_idx, fail_c_idx, fail_rc, err_msg = first_failure
            if err_msg is not None:
                print(err_msg, file=sys.stderr)
                return fail_rc if fail_rc != 0 else 1
            # Ensure the failing configuration's TLC output was printed to stdout.
            fail_m = models[fail_m_idx]
            if not fail_m["emitted"]:
                for c_idx in range(fail_m["num_cfgs"]):
                    res = fail_m["results"][c_idx]
                    if res is None:
                        continue
                    rc, log_path = res
                    txt = log_path.read_text(errors="replace")
                    sys.stdout.write(txt)
                    if not txt.endswith("\n"):
                        sys.stdout.write("\n")
                    sys.stdout.flush()
                    if rc != 0:
                        break
            return fail_rc if fail_rc != 0 else 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
PY
