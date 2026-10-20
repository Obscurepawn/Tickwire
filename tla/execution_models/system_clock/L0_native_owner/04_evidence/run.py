#!/usr/bin/env python3
"""Run every level configuration against one frozen copy; keep raw TLC output."""
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import time

EVIDENCE = Path(__file__).resolve().parent
LEVEL = EVIDENCE.parent
ROOT = LEVEL.parents[3]
MODEL = LEVEL / "02_models/L0_NativeOwner.tla"
RUNNER = Path("/Users/bytedance/.codex/skills/vibe-tla/scripts/run_tlc.sh")
JAVA = Path("/opt/homebrew/opt/openjdk/bin")
JAR = ROOT / "tla/tools/tla2tools.jar"
SOURCES = [
    "crates/tickwire-clock-sys/src/lib.rs",
    "crates/tickwire-clock-sys/tests/native.rs",
    "crates/tickwire-binance-clock/src/lib.rs",
    "crates/tickwire-binance-clock/src/client.rs",
    "crates/tickwire-binance-clock/tests/https.rs",
    "crates/tickwire-binance-clock/tests/system_clock.rs",
    "crates/tickwire-clock/src/lib.rs",
]


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


before = {p: sha(ROOT / p) for p in SOURCES}
results = {
    "model_sha256": sha(MODEL),
    "jar_sha256": sha(JAR),
    "source_sha256": before,
    "configs": [],
}
env = os.environ.copy()
env["PATH"] = str(JAVA) + os.pathsep + env["PATH"]
with tempfile.TemporaryDirectory(prefix="tickwire-system-clock-") as temporary:
    frozen = Path(temporary)
    shutil.copyfile(MODEL, frozen / MODEL.name)
    (frozen / "config.env").write_text(
        f"TLA_TOOLS_JAR={JAR}\nTLC_WORKERS=1\nTLC_TIMEOUT=1800\n"
    )
    configs = sorted(MODEL.parent.glob("*.cfg"))
    for cfg in configs:
        shutil.copyfile(cfg, frozen / cfg.name)
    for cfg in configs:
        java_tmp = frozen / (cfg.stem + "-java")
        java_tmp.mkdir()
        env["JAVA_TOOL_OPTIONS"] = f"-Xmx2g -Djava.io.tmpdir={java_tmp}"
        command = [
            str(RUNNER), str(frozen / MODEL.name), cfg.name, "--det", "--coverage"
        ]
        log = EVIDENCE / (cfg.stem + ".log")
        started = time.monotonic()
        with log.open("w") as out:
            out.write("command: " + json.dumps(command) + "\n")
            out.write("model_sha256: " + results["model_sha256"] + "\n")
            out.flush()
            process = subprocess.run(command, env=env, stdout=out, stderr=subprocess.STDOUT)
        text = log.read_text()
        counts = re.search(
            r"(\d+) states generated, (\d+) distinct states found, 0 states left on queue",
            text,
        )
        depth = re.search(r"The depth of the complete state graph search is (\d+)", text)
        if process.returncode or not counts or not depth:
            raise RuntimeError(f"{cfg.name} did not complete successfully; see {log}")
        entry = {
            "name": cfg.name, "sha256": sha(cfg),
            "log": str(log.relative_to(ROOT)), "log_sha256": sha(log),
            "generated": int(counts[1]), "distinct": int(counts[2]),
            "depth": int(depth[1]),
            "wall_seconds": round(time.monotonic() - started, 3),
            "exit_code": process.returncode,
        }
        results["configs"].append(entry)
        print(json.dumps(entry), flush=True)
after = {p: sha(ROOT / p) for p in SOURCES}
assert before == after, "source changed during the checks"
assert results["model_sha256"] == sha(MODEL), "model changed during the checks"
results["source_unchanged_during_run"] = True
(EVIDENCE / "results.json").write_text(json.dumps(results, indent=2) + "\n")
