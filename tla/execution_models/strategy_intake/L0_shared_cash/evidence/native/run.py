"""Inject two real strategy queues into the repo's existing loopback process fixture."""
import argparse
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--repeat", type=int, default=1, help="number of independent Bybit and OKX process pairs")
args = parser.parse_args()
if args.repeat < 1:
    parser.error("--repeat must be positive")

here = Path(__file__).resolve().parent
repo = next(p for p in here.parents if (p / "crates/tickwire-spot-runner").is_dir())
crate = repo / "crates/tickwire-spot-runner"
subprocess.run(["cargo", "build", "--locked", "--offline", "-p", "tickwire-spot-runner"], cwd=repo, check=True)
with tempfile.TemporaryDirectory(prefix="tw-isolation-probe-") as scratch:
    work = Path(scratch) / "tickwire-spot-runner"
    work.mkdir()
    (Path(scratch) / "tickwire-md").symlink_to(repo / "crates/tickwire-md", target_is_directory=True)
    for name in ["examples", "tests"]:
        shutil.copytree(crate / name, work / name)
    subprocess.run(["patch", "-s", "-p1", "-i", str(here / "two_queues.patch")], cwd=work, check=True)
    deps = (crate / "Cargo.toml").read_text().split("[dependencies]", maxsplit=1)[1]
    deps = re.sub(r'path = "../(tickwire-[^"]+)"', lambda m: f'path = "{repo}/crates/{m[1]}"', deps)
    (work / "Cargo.toml").write_text(
        '[package]\nname="tickwire-peer-isolation-probe"\nversion="0.0.0"\nedition="2024"\n'
        'autotests=false\nautoexamples=false\n[[test]]\nname="two_queues"\npath="tests/reconcile_process.rs"\n'
        '[dependencies]' + deps)
    shutil.copy(repo / "Cargo.lock", work / "Cargo.lock")
    env = dict(os.environ, CARGO_TARGET_DIR=str(repo / "target"), TICKWIRE_TEST_RUNNER=str(repo / "target/debug/tickwire-spot-runner"))
    for attempt in range(args.repeat):
        print(f"Native process pair {attempt + 1}/{args.repeat}", flush=True)
        subprocess.run(["cargo", "test", "--offline", "--test", "two_queues",
                        "real_reconcile_process", "--", "--nocapture"], cwd=work, env=env, check=True)
