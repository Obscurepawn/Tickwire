"""Run each exact model/config copy with private TLC state and Java temp paths."""
import argparse
import concurrent.futures
import hashlib
import os
from pathlib import Path
import shutil
import subprocess

parser = argparse.ArgumentParser()
parser.add_argument("--output-dir", type=Path, required=True)
parser.add_argument("--parallel", type=int, default=1)
parser.add_argument("--config", action="append",
                    choices=["primary", "rounding", "boundary", "usdm"])
args = parser.parse_args()
level = Path(__file__).resolve().parents[1]
repo = level.parents[3]
runner = Path.home() / ".codex/skills/vibe-tla/scripts/run_tlc.sh"
model = level / "02_models/L0_Calibration.tla"
out = args.output_dir.resolve()
out.mkdir(parents=True, exist_ok=True)


def run(name):
    destination = out / name
    destination.mkdir()
    temporary = destination / "java-tmp"
    temporary.mkdir()
    config = "L0_Calibration" + ("" if name == "primary" else "_" + name) + ".cfg"
    for source in [model, level / "02_models" / config]:
        shutil.copyfile(source, destination / source.name)
    (destination / "config.env").write_text(
        f"REPO_ROOT={repo}\nTLA_TOOLS_JAR={repo}/tla/tools/tla2tools.jar\n"
        "TLC_WORKERS=1\nTLC_TIMEOUT=0\n")
    environment = os.environ.copy()
    environment["JAVA_TOOL_OPTIONS"] = f"-Djava.io.tmpdir={temporary} -Xmx3g"
    command = ["bash", str(runner), str(destination / model.name),
               str(destination / config), "--det", "--coverage=60"]
    with (out / (name + ".log")).open("w") as log:
        result = subprocess.run(command, env=environment, stdout=log,
                                stderr=subprocess.STDOUT, check=False)
    return name, result.returncode, hashlib.sha256(model.read_bytes()).hexdigest()


with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, args.parallel)) as pool:
    results = list(pool.map(run, args.config or ["primary", "rounding", "boundary", "usdm"]))
for name, status, digest in results:
    print(name, "exit", status, "model_sha256", digest)
raise SystemExit(any(status for _, status, _ in results))
