#!/usr/bin/env bash
# Reproduce the submitted finite-state TLC models, zero-drift source-anchor
# lockfiles (`02_models/anchors.lock`), and Snowcat-typed Apalache/Z3 symbolic
# inductive specifications (`IndInv`).
# Supports zero, one, or multiple model-family arguments and executes all
# (model, .cfg) pairs and symbolic proofs across a CPU-budgeted LPT scheduler:
#   - `anchors.lock` SHA-256 verification checks every cited Rust source line
#     before launching JVMs and fails closed on any missing or drifted anchor.
#   - Coverage basis configurations run with `-coverage 60` (`-workers 1..4`
#     scaled by state count, C1 fast-JIT only below 25k states, C2 JIT above) to
#     prove 100% nonzero action coverage without 1-minute AST cost-tree dumps.
#   - Multi-million-state scaling configurations whose sibling `.cfg`s already
#     cover all model actions run with multi-worker `OffHeapDiskFPSet` (`-workers K`,
#     zero-touch `@TLAPlusOperator` bytecode overrides, and congruent `VIEW`
#     bisimulation quotients in isolated scratch directories) and automatically
#     re-run against the untouched `.tla`/`.cfg` with `-coverage 60` if any action
#     were ever left uncovered.
#   - When Apalache (`tla/tools/apalache/lib/apalache.jar` or `APALACHE_BIN`) is
#     installed, Snowcat-typed specifications defining `IndInv` run static type
#     checking plus unbounded `base` (`Init => IndInv`), `step` (`IndInv /\ Next => IndInv'`),
#     and safety entailment checks concurrently across the bounded worker pool.
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
if command -v sha256sum >/dev/null 2>&1; then
  actual="$(sha256sum "$jar" | cut -d ' ' -f 1)"
else
  actual="$(shasum -a 256 "$jar" | cut -d ' ' -f 1)"
fi
[[ "$actual" == "$expected" ]] || { echo 'TLC jar checksum mismatch' >&2; exit 1; }

exec python3 - "$repo_dir" "$jar" "${group_roots[@]}" <<'PY'
from pathlib import Path
import concurrent.futures
import hashlib
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import threading

repo_dir = Path(sys.argv[1])
jar_path = Path(sys.argv[2])
group_roots = [repo_dir / p for p in sys.argv[3:]]

sys.path.insert(0, str(repo_dir / "scripts"))
from tlc_coverage import CoverageError, check_logs, final_counts  # noqa: E402
try:
    from tlc_native_overrides import emit_override_classes  # noqa: E402
except ImportError:
    emit_override_classes = None

SUMMARY_RE = re.compile(r"([0-9,]+) states generated, ([0-9,]+) distinct states found, 0 states left on queue\.")
PROGRESS_RE = re.compile(
    r"Progress\(([0-9]+)\).*?: ([0-9,]+) states generated(?: \(([0-9,]+) s/min\))?"
)
PAUSED_LINE_RE = re.compile(
    r"^([A-Za-z0-9_]+):\s*Progress\(([0-9]+)\).*?: ([0-9,]+) states generated(?: \(([0-9,]+) s/min\))?"
)
MAX_ID_RE = re.compile(r"^\s*MaxId\s*=\s*([0-9]+)\b", re.M)
POLICY_RE = re.compile(r'^\s*Policy\s*=\s*"(Spot|UsdM)"', re.M)
MAX_CALLS_RE = re.compile(r"^\s*MaxCalls\s*=\s*([0-9]+)\b", re.M)
VIEW_CFG_RE = re.compile(r"^\s*VIEW\b", re.M)
TERM_LINE_RE = re.compile(r"^====+", re.M)

SCALING_THRESHOLD = 2_000_000
ULTRA_THRESHOLD = 300_000_000
MEGA_THRESHOLD = 50_000_000
FAST_JIT_THRESHOLD = 25_000
MULTI_COV_4W_THRESHOLD = 150_000
MULTI_COV_2W_THRESHOLD = 50_000
OFFHEAP_COV_THRESHOLD = 500_000
EQUIV_SCALAR_RATE_PER_MIN = 6_500_000

DEPTH_SYNC_VIEW_SNIPPET = (
    "DepthSyncObs ==\n"
    '  IF outcome = "View" THEN <<"View", viewNow >= s.book.stamp /\\ viewNow - s.book.stamp <= MaximumAge>>\n'
    '  ELSE IF outcome \\in {"ClockRegression","DeltaInvalid","Overflow","ApplyFailure",\n'
    '                        "SnapshotInvalid","LateReplayFailure","ReplayFailure","Exhausted"} THEN <<"Fail">>\n'
    '  ELSE <<"Ok">>\n'
    "DepthSyncView == <<s, issued, DepthSyncObs>>\n"
)
CALIB_VIEW_SNIPPET = 'CalibView == [p EXCEPT !.lastError = "None"]\n'


def paused_equiv_states(
    depth_str: str,
    gen_str: str,
    rate_str: str | None,
    model_name: str,
    cfg_src: Path | None = None,
) -> int:
    _depth = int(depth_str)
    gen = int(gen_str.replace(",", ""))
    if model_name == "L0_Calibration" and cfg_src is not None:
        try:
            m_calls = MAX_CALLS_RE.search(cfg_src.read_text(errors="ignore"))
            if m_calls:
                max_calls = int(m_calls.group(1))
                if max_calls >= 6:
                    return 400_000_000
                if max_calls == 5:
                    return 80_000_000
                return 25_000_000
        except OSError:
            pass
    if rate_str:
        rate = int(rate_str.replace(",", ""))
        if rate > 0:
            cpu_equiv = int((gen / rate) * EQUIV_SCALAR_RATE_PER_MIN * 2)
            if model_name == "L0_Calibration" and emit_override_classes is not None:
                cpu_equiv //= 13
            elif rate < 350_000:
                cpu_equiv *= 6
            return max(gen * 2, cpu_equiv)
    return gen * 2


def detect_budget() -> tuple[int, int, str | None]:
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
        total_cores = int(env_jobs)
    else:
        mem_cores = max(1, avail_mb // 1024)
        total_cores = max(1, min(cpus, mem_cores, 16))

    explicit_heap = os.environ.get("TLA_MAX_HEAP", "").strip() or None
    return total_cores, avail_mb, explicit_heap


def discover_model_dirs(roots: list[Path]) -> list[Path]:
    found: set[Path] = set()
    for root in roots:
        for cfg in root.glob("**/02_models/*.cfg"):
            if cfg.is_file():
                found.add(cfg.parent)
    return sorted(found)


def verify_anchor_locks(root_dir: Path, model_dirs: list[Path]) -> tuple[bool, str | None]:
    if os.environ.get("TLA_SKIP_ANCHORS", "").strip() == "1":
        return True, None
    require_locks = (root_dir / "Cargo.toml").is_file()
    file_cache: dict[str, list[bytes] | None] = {}
    sha_cache: dict[tuple[str, int], str] = {}
    drifts: list[str] = []
    locks_checked = 0
    lines_checked = 0

    def load_lines(rel_path: str) -> list[bytes] | None:
        if rel_path in file_cache:
            return file_cache[rel_path]
        try:
            raw = (root_dir / rel_path).read_bytes()
        except OSError:
            file_cache[rel_path] = None
            return None
        if not raw:
            parts: list[bytes] = []
        else:
            parts = raw.split(b"\n")
            if raw.endswith(b"\n"):
                parts.pop()
        file_cache[rel_path] = parts
        return parts

    for src_dir in model_dirs:
        lock = src_dir / "anchors.lock"
        rel_lvl = (
            str(src_dir.parent.relative_to(root_dir))
            if src_dir.parent.is_relative_to(root_dir)
            else str(src_dir.parent)
        )
        if not lock.is_file():
            if require_locks:
                drifts.append(f"MISSING anchors.lock [{rel_lvl}]")
            continue
        locks_checked += 1
        for raw_ln in lock.read_text(errors="replace").splitlines():
            if not raw_ln or raw_ln.startswith("#"):
                continue
            cols = raw_ln.split("\t")
            if len(cols) < 3 or not cols[1].isdigit():
                continue
            path_s, line_s, old_sha = cols[0], cols[1], cols[2].strip()
            ln = int(line_s)
            lines_checked += 1
            lines = load_lines(path_s)
            if lines is None:
                drifts.append(f"DRIFT  {path_s}:{ln}   [{rel_lvl}]  (file missing)")
                continue
            if ln < 1 or ln > len(lines):
                drifts.append(f"DRIFT  {path_s}:{ln}   [{rel_lvl}]  (line out of range)")
                continue
            key = (path_s, ln)
            cur_sha = sha_cache.get(key)
            if cur_sha is None:
                cur_sha = hashlib.sha256(lines[ln - 1]).hexdigest()
                sha_cache[key] = cur_sha
            if cur_sha != old_sha:
                drifts.append(f"DRIFT  {path_s}:{ln}   [{rel_lvl}]")

    if drifts:
        msg = "\n".join(drifts[:50])
        if len(drifts) > 50:
            msg += f"\n... and {len(drifts) - 50} additional drifted anchor(s)"
        return False, msg

    if locks_checked > 0:
        print(
            f"anchors.lock check: {lines_checked} anchor line(s) across {locks_checked} lock(s); 0 changed",
            file=sys.stderr,
            flush=True,
        )
    return True, None


INDINV_DEF_RE = re.compile(r"^\s*IndInv\s*==", re.M)
CINIT_DEF_RE = re.compile(r"^\s*ConstInit\s*==", re.M)
SAFETY_CONJ_RE = re.compile(r"^\s*(SafetyConjunction|SafetyInv)\s*==", re.M)


def discover_apalache_jar(root_dir: Path) -> tuple[Path | None, list[str]]:
    if os.environ.get("TLA_SKIP_APALACHE", "").strip() == "1":
        return None, []
    candidates: list[Path] = []
    env_jar = os.environ.get("APALACHE_JAR", "").strip()
    if env_jar:
        candidates.append(Path(env_jar))
    env_bin = os.environ.get("APALACHE_BIN", "").strip()
    if env_bin:
        bp = Path(env_bin).resolve()
        candidates.append(bp.parent.parent / "lib" / "apalache.jar")
    candidates.append(root_dir / "tla" / "tools" / "apalache" / "lib" / "apalache.jar")
    which_bin = shutil.which("apalache-mc")
    if which_bin:
        wp = Path(which_bin).resolve()
        candidates.append(wp.parent.parent / "lib" / "apalache.jar")

    found_jar: Path | None = None
    for cand in candidates:
        if cand.is_file():
            found_jar = cand
            break
    if found_jar is None:
        return None, []

    compat_args: list[str] = []
    try:
        probe = subprocess.run(
            ["java", "--sun-misc-unsafe-memory-access=allow", "-version"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=10,
        )
        if probe.returncode == 0:
            compat_args.append("--sun-misc-unsafe-memory-access=allow")
    except (OSError, subprocess.SubprocessError):
        pass
    return found_jar, compat_args


def parse_apalache_spec(tla_file: Path) -> dict | None:
    try:
        txt = tla_file.read_text(errors="ignore")
    except OSError:
        return None
    if "@type:" not in txt or not INDINV_DEF_RE.search(txt):
        return None
    m_safe = SAFETY_CONJ_RE.search(txt)
    return {
        "has_cinit": bool(CINIT_DEF_RE.search(txt)),
        "safety_op": m_safe.group(1) if m_safe else None,
    }


def is_archive_log(rel_parts: tuple[str, ...]) -> bool:
    return any(
        p.startswith("pre-") or p.startswith("initial") or p == "late-replay-witness"
        for p in rel_parts[:-1]
    )


def match_cfg_index(log_stem: str, model_name: str, cfg_stems: list[str]) -> int | None:
    for idx, c_stem in enumerate(cfg_stems):
        if log_stem == c_stem or log_stem == f"tlc-{c_stem}":
            return idx
    norm = log_stem[6:] if log_stem.startswith("final-") else log_stem
    base_model = re.sub(r"_v\d+$", "", model_name)
    candidates = [model_name] if base_model == model_name else [model_name, base_model]
    if norm in ("primary", "main"):
        for cand in candidates:
            for idx, c_stem in enumerate(cfg_stems):
                if c_stem == cand or c_stem.endswith("_primary"):
                    return idx
    for cand in candidates:
        prefix = cand + "_"
        for idx, c_stem in enumerate(cfg_stems):
            if c_stem.startswith(prefix) and c_stem[len(prefix):] == norm:
                return idx
    return None


def plan_model_configs(
    src_dir: Path,
    tla_file: Path,
    cfg_srcs: list[Path],
) -> tuple[list[tuple[int, int]], set[int]]:
    """Return per-cfg LPT weights `(tier, score)` and the required coverage-mode index set."""
    lvl = src_dir.parent
    model_name = tla_file.stem
    cfg_stems = [c.stem for c in cfg_srcs]
    n_cfgs = len(cfg_srcs)

    # Prefer current evidence logs over `pre-*` / `initial-*` historical drafts.
    # Historical archive drafts may come from earlier `.tla` revisions with shifted
    # line numbers, so `cov_by_cfg` only records non-archive logs.
    best_states: list[int | None] = [None] * n_cfgs
    best_is_archive: list[bool] = [True] * n_cfgs
    is_paused_cfg: list[bool] = [False] * n_cfgs
    cov_by_cfg: dict[int, dict[str, int]] = {}

    for lp in sorted(lvl.glob("**/*.log")):
        if not lp.is_file():
            continue
        rel_parts = lp.relative_to(lvl).parts
        arch = is_archive_log(rel_parts)
        c_idx = match_cfg_index(lp.stem, model_name, cfg_stems)
        if c_idx is None:
            continue
        try:
            raw = lp.read_bytes()
        except OSError:
            continue
        tail = raw[-16384:].decode("utf-8", errors="ignore")
        s_matches = SUMMARY_RE.findall(tail)
        if s_matches:
            gen = int(s_matches[-1][0].replace(",", ""))
            if best_states[c_idx] is None or (best_is_archive[c_idx] and not arch) or (
                best_is_archive[c_idx] == arch and gen > best_states[c_idx]
            ):
                best_states[c_idx] = gen
                best_is_archive[c_idx] = arch
            if not arch and c_idx not in cov_by_cfg:
                try:
                    counts = final_counts(raw.decode("utf-8", errors="ignore"), model_name)
                    cov_by_cfg[c_idx] = counts
                except CoverageError:
                    pass
        else:
            p_matches = PROGRESS_RE.findall(tail)
            if p_matches and best_states[c_idx] is None:
                last_depth, last_gen, last_rate = p_matches[-1]
                best_states[c_idx] = paused_equiv_states(
                    last_depth, last_gen, last_rate or None, model_name, cfg_srcs[c_idx]
                )
                is_paused_cfg[c_idx] = True

    for ptxt in sorted(lvl.glob("**/PAUSED.txt")):
        try:
            for ln in ptxt.read_text(errors="ignore").splitlines():
                m = PAUSED_LINE_RE.match(ln.strip())
                if m:
                    c_idx = match_cfg_index(m.group(1), model_name, cfg_stems)
                    if c_idx is not None:
                        val = paused_equiv_states(
                            m.group(2), m.group(3), m.group(4), model_name, cfg_srcs[c_idx]
                        )
                        base = 0 if best_is_archive[c_idx] else (best_states[c_idx] or 0)
                        best_states[c_idx] = max(base, val)
                        best_is_archive[c_idx] = False
                        is_paused_cfg[c_idx] = True
        except OSError:
            pass

    for pjson in sorted(lvl.glob("**/paused*.json")):
        try:
            data = json.loads(pjson.read_text(errors="ignore"))
            for key, info in (data.get("runs") or {}).items():
                if not isinstance(info, dict):
                    continue
                c_idx = match_cfg_index(key, model_name, cfg_stems)
                if c_idx is None:
                    continue
                pm = PROGRESS_RE.findall(str(info.get("last_progress", "")))
                if pm:
                    last_depth, last_gen, last_rate = pm[-1]
                    val = paused_equiv_states(
                        last_depth, last_gen, last_rate or None, model_name, cfg_srcs[c_idx]
                    )
                    base = 0 if best_is_archive[c_idx] else (best_states[c_idx] or 0)
                    best_states[c_idx] = max(base, val)
                    best_is_archive[c_idx] = False
                    is_paused_cfg[c_idx] = True
        except (OSError, ValueError):
            pass

    weights: list[tuple[int, int]] = []
    for c_idx, cfg_src in enumerate(cfg_srcs):
        st = best_states[c_idx]
        if st is not None:
            if model_name == "L0_DepthSync" and st >= SCALING_THRESHOLD:
                # Normalize post-`DepthSyncView` (`8.53x` bisimulation quotient) state count.
                st = max(SCALING_THRESHOLD, st // 8)
                best_states[c_idx] = st
            tier = 3 if is_paused_cfg[c_idx] else 2
            weights.append((tier, st))
        else:
            try:
                sz = tla_file.stat().st_size + cfg_src.stat().st_size
            except OSError:
                sz = 0
            weights.append((1, sz))

    # Determine which configurations must run with `-coverage 60` vs coverage-free
    # multi-worker `OffHeapDiskFPSet`.
    if n_cfgs <= 1 or not cov_by_cfg:
        return weights, set(range(n_cfgs))

    all_actions: set[str] = set()
    for counts in cov_by_cfg.values():
        all_actions.update(counts.keys())

    # Every `< 2M`-state config runs cheaply with `-workers 1 -coverage 60`.
    cov_indices: set[int] = {
        idx for idx in range(n_cfgs)
        if best_states[idx] is None or best_states[idx] < SCALING_THRESHOLD
    }
    covered: set[str] = set()
    for idx in cov_indices:
        if idx in cov_by_cfg:
            covered.update(k for k, v in cov_by_cfg[idx].items() if v > 0)

    # Greedily add the lightest remaining configuration(s) needed to cover 100% of actions.
    while not all_actions.issubset(covered):
        uncovered = all_actions - covered
        best_pick = None
        best_key = None
        for idx, counts in cov_by_cfg.items():
            if idx in cov_indices:
                continue
            newly = sum(1 for k in uncovered if counts.get(k, 0) > 0)
            if newly == 0:
                continue
            cost = best_states[idx] or SCALING_THRESHOLD
            cand_key = (cost / newly, cost, idx)
            if best_key is None or cand_key < best_key:
                best_key = cand_key
                best_pick = idx
        if best_pick is None:
            # Historical logs did not cover every action across subsets; keep all in coverage mode.
            return weights, set(range(n_cfgs))
        cov_indices.add(best_pick)
        covered.update(k for k, v in cov_by_cfg[best_pick].items() if v > 0)

    if not cov_indices:
        lightest = min(range(n_cfgs), key=lambda i: best_states[i] or 0)
        cov_indices.add(lightest)

    return weights, cov_indices


def main() -> int:
    total_cores, avail_mb, explicit_heap = detect_budget()
    model_dirs = discover_model_dirs(group_roots)
    if not model_dirs:
        return 0

    anchors_ok, anchor_err = verify_anchor_locks(repo_dir, model_dirs)
    if not anchors_ok:
        print(anchor_err, file=sys.stderr)
        return 1

    apalache_jar, apalache_compat_args = discover_apalache_jar(repo_dir)

    with tempfile.TemporaryDirectory() as scratch_s:
        scratch = Path(scratch_s)
        override_base = None
        override_depth = None
        if emit_override_classes is not None:
            override_base = emit_override_classes(
                scratch / "overrides_base", include_depth_sync=False
            )
            override_depth = emit_override_classes(
                scratch / "overrides_depth", include_depth_sync=True
            )
        models = []
        pending_tasks = []

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
                frozen_cfgs.append(dst)

            weights, cov_indices = plan_model_configs(src_dir, tla_src, cfg_srcs)
            sym_spec = parse_apalache_spec(frozen_tla) if apalache_jar is not None else None

            scale_dir: Path | None = None
            if (
                frozen_tla.stem in ("L0_DepthSync", "L0_Calibration")
                and any(idx not in cov_indices for idx in range(len(frozen_cfgs)))
            ):
                scale_dir = group_dir / "scale_view"
                scale_dir.mkdir(parents=True, exist_ok=True)
                orig_tla_txt = frozen_tla.read_text(errors="ignore")
                if frozen_tla.stem == "L0_DepthSync":
                    snippet = DEPTH_SYNC_VIEW_SNIPPET
                    view_name = "DepthSyncView"
                else:
                    snippet = CALIB_VIEW_SNIPPET
                    view_name = "CalibView"
                scale_tla_txt = TERM_LINE_RE.sub(
                    lambda m_term, s=snippet: s + m_term.group(0),
                    orig_tla_txt,
                    count=1,
                )
                (scale_dir / frozen_tla.name).write_text(scale_tla_txt)
                for cfg_dst in frozen_cfgs:
                    cfg_txt = cfg_dst.read_text(errors="ignore")
                    if not VIEW_CFG_RE.search(cfg_txt):
                        cfg_txt = cfg_txt.rstrip() + f"\nVIEW {view_name}\n"
                    (scale_dir / cfg_dst.name).write_text(cfg_txt)

            total_subtasks = len(frozen_cfgs) + (1 if sym_spec is not None else 0)
            models.append({
                "idx": m_idx,
                "src_dir": src_dir,
                "group_dir": group_dir,
                "scale_dir": scale_dir,
                "model_name": frozen_tla.stem,
                "frozen_tla": frozen_tla,
                "frozen_cfgs": frozen_cfgs,
                "cov_indices": cov_indices,
                "num_cfgs": len(frozen_cfgs),
                "total_subtasks": total_subtasks,
                "sym_spec": sym_spec,
                "sym_result": None,  # dict | None
                "results": [None] * len(frozen_cfgs),  # (rc, log_path, has_coverage)
                "done_count": 0,
                "verifying": False,
                "verified_cov": None,  # dict | None
                "emitted": False,
            })

            for c_idx, cfg_dst in enumerate(frozen_cfgs):
                use_cov = c_idx in cov_indices
                tier, score = weights[c_idx]
                est_states = score if tier >= 2 else 0
                pending_tasks.append({
                    "kind": "tlc",
                    "priority": (-tier, -score, m_idx, c_idx),
                    "m_idx": m_idx,
                    "c_idx": c_idx,
                    "cfg_dst": cfg_dst,
                    "use_cov": use_cov,
                    "req_cores": 1,
                    "est_states": est_states,
                })

            if sym_spec is not None:
                pending_tasks.append({
                    "kind": "apalache",
                    "priority": (-2, -250_000, m_idx, len(frozen_cfgs)),
                    "m_idx": m_idx,
                    "c_idx": len(frozen_cfgs),
                    "cfg_dst": frozen_tla,
                    "use_cov": True,
                    "req_cores": max(1, min(2, total_cores)),
                    "est_states": 0,
                })

        has_ultra = any(
            t["kind"] == "tlc" and (not t["use_cov"]) and t["est_states"] >= ULTRA_THRESHOLD
            for t in pending_tasks
        )
        for task in pending_tasks:
            if task["kind"] == "apalache":
                continue
            est_states = task["est_states"]
            if task["use_cov"]:
                if est_states >= MULTI_COV_4W_THRESHOLD:
                    task["req_cores"] = max(1, min(4, total_cores))
                elif est_states >= MULTI_COV_2W_THRESHOLD:
                    task["req_cores"] = max(1, min(2, total_cores))
                else:
                    task["req_cores"] = 1
            elif est_states >= ULTRA_THRESHOLD:
                if total_cores >= 16:
                    task["req_cores"] = min(12, total_cores)
                else:
                    task["req_cores"] = max(1, min(8, total_cores))
            elif est_states >= MEGA_THRESHOLD:
                if has_ultra and total_cores >= 16:
                    task["req_cores"] = min(4, total_cores)
                else:
                    task["req_cores"] = max(1, min(8, total_cores))
            else:
                task["req_cores"] = max(1, min(4, total_cores))

        pending_tasks.sort(key=lambda t: t["priority"])

        cond = threading.Condition()
        stop_event = threading.Event()
        active_procs: set[subprocess.Popen] = set()
        avail_cores = total_cores
        first_failure = None  # (m_idx, c_idx, rc, err_msg)
        next_emit = 0
        interrupted_sig = [0]

        def terminate_procs(procs):
            for proc in procs:
                try:
                    os.killpg(proc.pid, signal.SIGTERM)
                except OSError:
                    try:
                        proc.terminate()
                    except OSError:
                        pass

        def on_signal(signum, _frame):
            interrupted_sig[0] = signum
            stop_event.set()
            with cond:
                to_kill = list(active_procs)
                pending_tasks.clear()
                cond.notify_all()
            terminate_procs(to_kill)

        old_sigint = signal.signal(signal.SIGINT, on_signal)
        old_sigterm = signal.signal(signal.SIGTERM, on_signal)

        def invoke_tlc(
            group_dir: Path,
            tla_name: str,
            cfg_name: str,
            workers: int,
            use_cov: bool,
            est_states: int,
        ) -> tuple[int, Path]:
            run_dir = Path(tempfile.mkdtemp(prefix="run.", dir=group_dir))
            states_dir = run_dir / "states"
            java_tmp = run_dir / "java"
            states_dir.mkdir(parents=True, exist_ok=True)
            java_tmp.mkdir(parents=True, exist_ok=True)
            log_path = run_dir / "tlc.log"

            if explicit_heap is not None:
                heap = explicit_heap
            elif not use_cov and est_states >= MEGA_THRESHOLD:
                heap = "3072m" if avail_mb >= 16384 else "2048m"
            elif not use_cov:
                heap = "2048m"
            elif est_states >= SCALING_THRESHOLD:
                heap = "2560m"
            else:
                heap = "1536m"

            depth_max_id: int | None = None
            depth_policy = "Spot"
            if tla_name == "L0_DepthSync.tla" and override_depth is not None:
                try:
                    cfg_txt = (group_dir / cfg_name).read_text(errors="ignore")
                    m_max = MAX_ID_RE.search(cfg_txt)
                    if m_max:
                        depth_max_id = int(m_max.group(1))
                    m_pol = POLICY_RE.search(cfg_txt)
                    if m_pol:
                        depth_policy = m_pol.group(1)
                except OSError:
                    pass

            cmd = [
                "java",
                f"-Djava.io.tmpdir={java_tmp}",
                f"-Xmx{heap}",
                "-XX:+UseParallelGC",
            ]
            if workers == 1:
                cmd.extend(["-XX:ParallelGCThreads=2", "-XX:ConcGCThreads=1"])
                if est_states < FAST_JIT_THRESHOLD:
                    cmd.extend(["-XX:+TieredCompilation", "-XX:TieredStopAtLevel=1"])
            else:
                gc_threads = min(4, workers)
                cmd.extend([
                    f"-XX:ParallelGCThreads={gc_threads}",
                    "-XX:ConcGCThreads=1",
                    "-XX:MaxInlineLevel=24",
                    "-XX:FreqInlineSize=512",
                ])
                if not use_cov or est_states >= OFFHEAP_COV_THRESHOLD:
                    if explicit_heap is None and not use_cov:
                        young = "1536m" if heap == "3072m" else "1024m"
                        cmd.append(f"-Xmn{young}")
                    offheap = (
                        "3072m"
                        if (not use_cov and est_states >= MEGA_THRESHOLD and avail_mb >= 16384)
                        else "1536m"
                    )
                    cmd.extend([
                        f"-XX:MaxDirectMemorySize={offheap}",
                        "-Dtlc2.tool.fp.FPSet.impl=tlc2.tool.fp.OffHeapDiskFPSet",
                    ])

            if depth_max_id is not None:
                cmd.extend([
                    f"-Dtickwire.tlc.maxId={depth_max_id}",
                    f"-Dtickwire.tlc.policy={depth_policy}",
                ])
                cp_arg = f"{override_depth}{os.pathsep}{jar_path}"
            elif override_base is not None:
                cp_arg = f"{override_base}{os.pathsep}{jar_path}"
            else:
                cp_arg = str(jar_path)

            cmd.extend([
                "-cp",
                cp_arg,
                "tlc2.TLC",
                "-checkpoint",
                "0",
                "-workers",
                str(workers),
            ])
            if use_cov:
                cmd.extend(["-coverage", "60"])
            cmd.extend([
                "-metadir",
                str(states_dir),
                "-config",
                cfg_name,
                tla_name,
            ])

            with open(log_path, "wb") as lf:
                proc = subprocess.Popen(
                    cmd,
                    cwd=group_dir,
                    stdout=lf,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                )
                with cond:
                    if stop_event.is_set():
                        terminate_procs([proc])
                    else:
                        active_procs.add(proc)
                try:
                    rc = proc.wait()
                finally:
                    with cond:
                        active_procs.discard(proc)

            shutil.rmtree(states_dir, ignore_errors=True)
            shutil.rmtree(java_tmp, ignore_errors=True)
            return rc, log_path

        def invoke_apalache(
            group_dir: Path,
            tla_name: str,
            sym_spec: dict,
        ) -> tuple[int, dict | str]:
            sym_dir = Path(tempfile.mkdtemp(prefix="apalache.", dir=group_dir))
            user_home = sym_dir / "home"
            tlaplus_cfg_dir = user_home / ".tlaplus"
            tlaplus_cfg_dir.mkdir(parents=True, exist_ok=True)
            (tlaplus_cfg_dir / "apalache.cfg").write_text("common {\n  enable-stats = false\n}\n")

            heap = explicit_heap if explicit_heap is not None else "2560m"
            phases: list[tuple[str, str, str, int]] = [
                ("base", "Init", "IndInv", 0),
                ("step", "IndInv", "IndInv", 1),
            ]
            if sym_spec["safety_op"] is not None:
                phases.append(("entailment", "IndInv", sym_spec["safety_op"], 0))

            passed_phases: list[str] = []
            try:
                for phase_name, init_op, inv_op, length in phases:
                    if stop_event.is_set():
                        return (1, "Apalache interrupted")
                    phase_tmp = sym_dir / f"tmp_{phase_name}"
                    phase_out = sym_dir / f"out_{phase_name}"
                    phase_tmp.mkdir(parents=True, exist_ok=True)
                    phase_out.mkdir(parents=True, exist_ok=True)
                    phase_log = sym_dir / f"{phase_name}.log"

                    cmd = [
                        "java",
                        *apalache_compat_args,
                        "-XX:+TieredCompilation",
                        "-XX:TieredStopAtLevel=1",
                        "-XX:ParallelGCThreads=2",
                        "-XX:ConcGCThreads=1",
                        f"-Xmx{heap}",
                        "-XX:+UseG1GC",
                        "-XX:G1PeriodicGCInterval=600000",
                        "-XX:+G1PeriodicGCInvokesConcurrent",
                        f"-Duser.home={user_home}",
                        f"-Djava.io.tmpdir={phase_tmp}",
                        "-jar",
                        str(apalache_jar),
                        "check",
                        f"--init={init_op}",
                        "--next=Next",
                        f"--inv={inv_op}",
                        f"--length={length}",
                        "--no-deadlock",
                        f"--out-dir={phase_out}",
                    ]
                    if sym_spec["has_cinit"]:
                        cmd.append("--cinit=ConstInit")
                    cmd.append(tla_name)

                    with open(phase_log, "wb") as lf:
                        proc = subprocess.Popen(
                            cmd,
                            cwd=group_dir,
                            stdout=lf,
                            stderr=subprocess.STDOUT,
                            start_new_session=True,
                        )
                        with cond:
                            if stop_event.is_set():
                                terminate_procs([proc])
                            else:
                                active_procs.add(proc)
                        try:
                            rc = proc.wait()
                        finally:
                            with cond:
                                active_procs.discard(proc)

                    log_txt = phase_log.read_text(errors="replace")
                    shutil.rmtree(phase_tmp, ignore_errors=True)
                    shutil.rmtree(phase_out, ignore_errors=True)
                    if (
                        rc != 0
                        or "All expressions are typed" not in log_txt
                        or "The outcome is: NoError" not in log_txt
                        or "EXITCODE: OK" not in log_txt
                    ):
                        return (
                            rc if rc != 0 else 1,
                            f"Apalache {phase_name} failed for {tla_name} (rc={rc}):\n{log_txt}",
                        )
                    passed_phases.append(phase_name)
            finally:
                shutil.rmtree(sym_dir, ignore_errors=True)

            return (0, {"status": "NoError", "phases": passed_phases})

        def flush_ready_locked() -> tuple[int, str | None] | None:
            nonlocal next_emit
            while next_emit < len(models):
                m = models[next_emit]
                if m["verified_cov"] is None:
                    break
                for res in m["results"]:
                    rc, log_path, _ = res
                    txt = log_path.read_text(errors="replace")
                    sys.stdout.write(txt)
                    if not txt.endswith("\n"):
                        sys.stdout.write("\n")
                    sys.stdout.flush()
                    if rc != 0:
                        return (rc, None)
                cov = m["verified_cov"]
                if isinstance(cov, Exception):
                    return (1, str(cov))
                print(json.dumps(cov, indent=2), flush=True)
                shutil.rmtree(m["group_dir"], ignore_errors=True)
                m["emitted"] = True
                next_emit += 1
            return None

        def verify_completed_model(m_idx: int) -> None:
            nonlocal avail_cores, first_failure
            m = models[m_idx]
            cov_logs: list[Path] = []
            scale_entries: list[tuple[int, Path]] = []
            for c_idx, res in enumerate(m["results"]):
                rc, log_path, has_cov = res
                if rc != 0:
                    return
                if has_cov:
                    cov_logs.append(log_path)
                else:
                    scale_entries.append((c_idx, log_path))

            try:
                cov = check_logs(
                    cov_logs,
                    m["model_name"],
                    scaling_paths=[lp for _, lp in scale_entries],
                )
                cov["model_sha256"] = hashlib.sha256(m["frozen_tla"].read_bytes()).hexdigest()
                if m["sym_result"] is not None:
                    cov["apalache_inductive"] = m["sym_result"]
            except CoverageError as err:
                # Failsafe: if sibling coverage configs left an action uncovered while
                # scaling configs ran without `-coverage`, re-run the scaling configs
                # with `-workers 1 -coverage 60` before declaring an uncovered action.
                if scale_entries and "uncovered across all configurations:" in str(err):
                    for c_idx, _ in scale_entries:
                        if stop_event.is_set():
                            return
                        with cond:
                            while not stop_event.is_set() and avail_cores < 1:
                                cond.wait()
                            if stop_event.is_set():
                                return
                            avail_cores -= 1
                        try:
                            rc2, lp2 = invoke_tlc(
                                m["group_dir"],
                                m["frozen_tla"].name,
                                m["frozen_cfgs"][c_idx].name,
                                1,
                                True,
                                SCALING_THRESHOLD,
                            )
                        finally:
                            with cond:
                                avail_cores += 1
                                cond.notify_all()
                        with cond:
                            m["results"][c_idx] = (rc2, lp2, True)
                        if rc2 != 0:
                            with cond:
                                if first_failure is None:
                                    stop_event.set()
                                    first_failure = (m_idx, c_idx, rc2, None)
                                    to_kill = list(active_procs)
                                else:
                                    to_kill = []
                                cond.notify_all()
                            if to_kill:
                                terminate_procs(to_kill)
                            return
                        cov_logs.append(lp2)
                    try:
                        cov = check_logs(cov_logs, m["model_name"])
                        cov["model_sha256"] = hashlib.sha256(m["frozen_tla"].read_bytes()).hexdigest()
                        if m["sym_result"] is not None:
                            cov["apalache_inductive"] = m["sym_result"]
                    except (OSError, CoverageError) as err2:
                        cov = err2
                else:
                    cov = err
            except OSError as err:
                cov = err

            to_kill = []
            with cond:
                if stop_event.is_set():
                    return
                m["verified_cov"] = cov
                err_info = flush_ready_locked()
                if err_info is not None and first_failure is None:
                    stop_event.set()
                    first_failure = (next_emit, 0, err_info[0], err_info[1])
                    to_kill = list(active_procs)
                    pending_tasks.clear()
                    cond.notify_all()
            if to_kill:
                terminate_procs(to_kill)

        def acquire_next_task() -> dict | None:
            nonlocal avail_cores
            with cond:
                while True:
                    if stop_event.is_set() or not pending_tasks:
                        return None
                    # Strict LPT head reservation: if the highest-priority task needs
                    # `head_req` cores, wait until `avail_cores >= head_req` rather than
                    # starving multi-worker heavy runs behind 1-core tasks.
                    head = pending_tasks[0]
                    if head["req_cores"] <= avail_cores:
                        pending_tasks.pop(0)
                        # If this is the last mega-scale task and the entire machine is
                        # otherwise idle, let it claim all available cores so no tail
                        # cores sit idle.
                        if (
                            head["kind"] == "tlc"
                            and not head["use_cov"]
                            and head["est_states"] >= MEGA_THRESHOLD
                            and avail_cores == total_cores
                            and not any(
                                t["kind"] == "tlc"
                                and not t["use_cov"]
                                and t["est_states"] >= SCALING_THRESHOLD
                                for t in pending_tasks
                            )
                        ):
                            head["req_cores"] = total_cores
                        avail_cores -= head["req_cores"]
                        return head
                    cond.wait()

        def worker_loop() -> None:
            nonlocal avail_cores, first_failure
            while True:
                task = acquire_next_task()
                if task is None:
                    return
                m_idx = task["m_idx"]
                c_idx = task["c_idx"]
                req_cores = task["req_cores"]
                m = models[m_idx]

                if task["kind"] == "apalache":
                    try:
                        sym_rc, sym_payload = invoke_apalache(
                            m["group_dir"],
                            m["frozen_tla"].name,
                            m["sym_spec"],
                        )
                    finally:
                        with cond:
                            avail_cores += req_cores
                            cond.notify_all()

                    need_verify = False
                    to_kill = []
                    with cond:
                        if stop_event.is_set():
                            return
                        m["done_count"] += 1
                        if sym_rc != 0:
                            stop_event.set()
                            if first_failure is None:
                                first_failure = (m_idx, c_idx, sym_rc, str(sym_payload))
                            to_kill = list(active_procs)
                            pending_tasks.clear()
                            cond.notify_all()
                        else:
                            m["sym_result"] = sym_payload
                            if m["done_count"] == m["total_subtasks"] and not m["verifying"]:
                                m["verifying"] = True
                                need_verify = True

                    if to_kill:
                        terminate_procs(to_kill)
                        return
                    if need_verify:
                        verify_completed_model(m_idx)
                    continue

                exec_dir = (
                    m["scale_dir"]
                    if (not task["use_cov"] and m["scale_dir"] is not None)
                    else m["group_dir"]
                )
                try:
                    rc, log_path = invoke_tlc(
                        exec_dir,
                        m["frozen_tla"].name,
                        task["cfg_dst"].name,
                        req_cores,
                        task["use_cov"],
                        task["est_states"],
                    )
                finally:
                    with cond:
                        avail_cores += req_cores
                        cond.notify_all()

                need_verify = False
                to_kill = []
                with cond:
                    if stop_event.is_set():
                        return
                    m["results"][c_idx] = (rc, log_path, task["use_cov"])
                    m["done_count"] += 1
                    if rc != 0:
                        stop_event.set()
                        if first_failure is None:
                            first_failure = (m_idx, c_idx, rc, None)
                        to_kill = list(active_procs)
                        pending_tasks.clear()
                        cond.notify_all()
                    elif m["done_count"] == m["total_subtasks"] and not m["verifying"]:
                        m["verifying"] = True
                        need_verify = True

                if to_kill:
                    terminate_procs(to_kill)
                    return

                if need_verify:
                    verify_completed_model(m_idx)

        try:
            pool_threads = min(total_cores, max(1, len(pending_tasks)))
            with concurrent.futures.ThreadPoolExecutor(max_workers=pool_threads) as pool:
                futs = [pool.submit(worker_loop) for _ in range(pool_threads)]
                for fut in futs:
                    fut.result()
        finally:
            signal.signal(signal.SIGINT, old_sigint)
            signal.signal(signal.SIGTERM, old_sigterm)

        if interrupted_sig[0] != 0:
            return 128 + interrupted_sig[0]

        if first_failure is not None:
            fail_m_idx, _fail_c_idx, fail_rc, err_msg = first_failure
            if err_msg is not None:
                print(err_msg, file=sys.stderr)
                return fail_rc if fail_rc != 0 else 1
            fail_m = models[fail_m_idx]
            if not fail_m["emitted"]:
                for c_idx in range(fail_m["num_cfgs"]):
                    res = fail_m["results"][c_idx]
                    if res is None:
                        continue
                    rc, log_path, _ = res
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
