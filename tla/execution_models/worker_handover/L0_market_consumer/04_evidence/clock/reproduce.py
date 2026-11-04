"""Run the socket regression in a temporary copy of the pinned Rust workspace."""
from pathlib import Path
import os
import subprocess
import tarfile
import tempfile

evidence = Path(__file__).resolve().parent
repo = Path(subprocess.check_output(["git", "-C", evidence, "rev-parse", "--show-toplevel"], text=True).strip())
revision = "9b2ed630721911e01046822bddc9db21b2fa5aee"

with tempfile.TemporaryDirectory(prefix="tickwire-clock-") as tmp:
    workspace = Path(tmp)
    with tempfile.TemporaryFile() as archive:
        subprocess.run(["git", "-C", repo, "archive", "--format=tar", revision], check=True, stdout=archive)
        archive.seek(0)
        with tarfile.open(fileobj=archive) as files:
            files.extractall(workspace, filter="data")
    host = workspace / "crates/tickwire-strategy-host/src"
    for target, extra in [(host / "market_transition/tests.rs", "clock_case.rs"), (host / "readiness.rs", "readiness_fixture.rs")]:
        with target.open("a") as out:
            out.write("\n" + (evidence / extra).read_text())
    env = os.environ.copy()
    env.setdefault("CARGO_TARGET_DIR", str(repo / "target"))
    subprocess.run(["cargo", "test", "--locked", "--offline", "-p", "tickwire-strategy-host",
                    "--lib", "worker_receipt_created_after_host_sample", "--", "--nocapture"],
                   cwd=workspace, env=env, check=True)
