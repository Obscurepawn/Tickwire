#!/usr/bin/env python3
"""Build both revisions with the same harness/profile, then alternate run order."""
import argparse
import csv
import hashlib
import io
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys
import tempfile

root = Path(__file__).resolve().parents[3]
parser = argparse.ArgumentParser()
parser.add_argument("--before-rev", default="cd8bde270ac257b296100979621c60c64f965e42")
parser.add_argument("--samples", type=int, default=200_000)
parser.add_argument("--rounds", type=int, default=5)
parser.add_argument("--harness", choices=["batch_apply", "sparse_apply"], default="batch_apply")
parser.add_argument("--output", type=Path, required=True)
args = parser.parse_args()
assert args.samples > 0 and args.rounds > 0
cargo = shutil.which("cargo") or str(Path.home() / ".cargo/bin/cargo")
rustc = shutil.which("rustc") or str(Path.home() / ".cargo/bin/rustc")
source_path = "crates/tickwire-book/src/lib.rs"
before = subprocess.check_output(["git", "show", f"{args.before_rev}:{source_path}"], cwd=root)
after = (root / source_path).read_bytes()
harness = (root / f"crates/tickwire-book/benches/{args.harness}.rs").read_bytes()
manifest = """[package]
name = "tickwire-book"
version = "0.1.0"
edition = "2024"
[[bench]]
name = "batch_apply"
harness = false
[profile.release]
overflow-checks = true
lto = "thin"
codegen-units = 1
"""
args.output.mkdir(parents=True, exist_ok=True)
metadata = {
    "before_revision": args.before_rev,
    "harness": args.harness,
    "source_sha256": {k: hashlib.sha256(v).hexdigest() for k, v in
                      [("before", before), ("after", after), ("harness", harness)]},
    "compiler": subprocess.check_output([rustc, "-Vv"], text=True),
    "platform": platform.platform(),
    "cpu": subprocess.check_output(["sysctl", "-n", "machdep.cpu.brand_string"], text=True).strip()
           if platform.system() == "Darwin" else platform.processor(),
    "profile": {"opt-level": 3, "overflow-checks": True, "lto": "thin", "codegen-units": 1},
    "samples_per_case_per_round": args.samples,
    "rounds": args.rounds,
    "warmup_events_per_case": 10_000,
    "timing": "Instant before apply through view+best_bid+best_ask; generation and sample push outside",
    "limitations": ["synthetic normalized input", "no affinity or CPU isolation",
                    "macOS timer quantization; raw elapsed values, no timer-overhead subtraction",
                    "excludes feed decode, transport, queues, strategy, IPC and persistence"],
    "command": " ".join(sys.argv),
}
with tempfile.TemporaryDirectory(prefix="tickwire-book-compare-") as temporary:
    binaries = {}
    for variant, source in [("before", before), ("after", after)]:
        path = Path(temporary) / variant
        (path / "src").mkdir(parents=True)
        (path / "benches").mkdir()
        (path / "src/lib.rs").write_bytes(source)
        (path / "benches/batch_apply.rs").write_bytes(harness)
        # Unit-test-only modules are not included by cargo bench --no-run.
        (path / "Cargo.toml").write_text(manifest)
        subprocess.run([cargo, "bench", "--bench", "batch_apply", "--no-run"], cwd=path, check=True)
        binaries[variant] = next(p for p in (path / "target/release/deps").glob("batch_apply-*")
                                 if p.is_file() and os.access(p, os.X_OK) and p.suffix != ".d")
    with (args.output / "results.csv").open("w", newline="") as output:
        writer = None
        for run in range(1, args.rounds + 1):
            for variant in (["before", "after"] if run % 2 else ["after", "before"]):
                print(f"round={run} variant={variant}", file=sys.stderr, flush=True)
                result = subprocess.check_output([str(binaries[variant])],
                    env={**os.environ, "TICKWIRE_BENCH_SAMPLES": str(args.samples)}, text=True)
                reader = csv.DictReader(io.StringIO(result))
                if writer is None:
                    writer = csv.DictWriter(output, fieldnames=["round", "variant"] + reader.fieldnames, lineterminator="\n")
                    writer.writeheader()
                for row in reader:
                    writer.writerow({"round": run, "variant": variant, **row})
                output.flush()
(args.output / "environment.json").write_text(json.dumps(metadata, indent=2) + "\n")
