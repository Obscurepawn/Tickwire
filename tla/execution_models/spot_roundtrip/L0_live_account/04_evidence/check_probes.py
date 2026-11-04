"""Run both deliberately stronger expectations against the unchanged route model."""
from pathlib import Path
import shutil
import subprocess
import tempfile

evidence = Path(__file__).resolve().parent
model = evidence.parent / "02_models" / "L0_SpotRoundtrip.tla"
jar = evidence.parents[3] / "tools" / "tla2tools.jar"
for config, module, expected in (
    ("strong_progress.cfg", model, "Temporal properties were violated"),
    ("SampleBoundary.cfg", evidence / "SampleBoundary.tla", "Action property ByteRequiresCurrent is violated"),
):
    with tempfile.TemporaryDirectory() as directory:
        for source in {model, module, evidence / config}:
            shutil.copy(source, directory)
        result = subprocess.run(
            ["java", "-Xmx512m", "-XX:+UseParallelGC", "-cp", str(jar), "tlc2.TLC",
             "-workers", "1", "-coverage", "1", "-config", config, module.name],
            cwd=directory, capture_output=True, text=True,
        )
    output = result.stdout + result.stderr
    if result.returncode == 0 or "Error: " + expected not in output:
        raise RuntimeError(output)
    print(config, "exit", result.returncode, expected)
    for line in output.splitlines():
        if line.startswith("State ") or " states left on queue." in line:
            print(line)
