"""Replay the original two-queue input against the current live executable."""
import os
from pathlib import Path
import re
import shutil
import subprocess
import tarfile
import tempfile

here = Path(__file__).resolve().parent
repo = next(p for p in here.parents if (p / "crates/tickwire-spot-runner").is_dir())
crate = repo / "crates/tickwire-spot-runner"
fixture = "crates/tickwire-spot-runner"
original = here.parents[2] / "L0_shared_cash/evidence/native/two_queues.patch"

target = repo / "target"
env = dict(os.environ, CARGO_TARGET_DIR=str(target), TICKWIRE_TEST_RUNNER=str(target / "debug" / crate.name))
subprocess.run(["cargo", "build", "--locked", "--offline", "-p", crate.name], cwd=repo, env=env, check=True)
with tempfile.TemporaryDirectory(prefix="tickwire-management-") as temp:
    root = Path(temp)
    with tempfile.TemporaryFile() as archive:
        subprocess.run(["git", "archive", "9b2ed630721911e01046822bddc9db21b2fa5aee",
                        f"{fixture}/tests", f"{fixture}/examples"], cwd=repo, check=True, stdout=archive)
        archive.seek(0)
        with tarfile.open(fileobj=archive) as files:
            files.extractall(root, filter="data")
    work = root / fixture
    (work.parent / "tickwire-md").symlink_to(crate.parent / "tickwire-md", target_is_directory=True)
    for patch in (original, here / "accepted_cancel.patch"):
        subprocess.run(["patch", "-s", "-p1", "-i", patch], cwd=work, check=True)
    deps = (crate / "Cargo.toml").read_text().split("[dependencies]", 1)[1]
    deps = re.sub(r'path = "../(tickwire-[^"]+)"', lambda m: f'path = "{crate.parent / m[1]}"', deps)
    (work / "Cargo.toml").write_text(
        '[package]\nname="tickwire-management-probe"\nversion="0.0.0"\nedition="2024"\n'
        'autotests=false\nautoexamples=false\n[[test]]\nname="two_queues"\npath="tests/reconcile_process.rs"\n'
        '[dependencies]' + deps)
    shutil.copy(repo / "Cargo.lock", work / "Cargo.lock")
    subprocess.run(["cargo", "test", "--offline", "--test", "two_queues", "real_reconcile_process",
                    "--", "--nocapture"], cwd=work, env=env, check=True)
