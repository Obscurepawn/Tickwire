"""Run the two safety configurations and the isolated peer-containment probe."""
from pathlib import Path
import shutil
import subprocess
import tempfile

out = Path(__file__).resolve().parent
models = out.parent / "02_models"
repo = next(parent for parent in out.parents if (parent / "tla/tools/tla2tools.jar").is_file())
module = "L0_SharedCash"
with tempfile.TemporaryDirectory(dir=out) as tmp:
    work = Path(tmp)
    for path in [*models.glob("*.cfg"), models / f"{module}.tla", out / "PeerCanContinue.cfg"]:
        shutil.copy(path, work)
    for cfg in [module, module + "_restart", "PeerCanContinue"]:
        command = ["java", "-Xmx320m", "-XX:+UseParallelGC", "-cp", str(repo / "tla/tools/tla2tools.jar"),
                   "tlc2.TLC", "-workers", "1", "-fp", "0", "-seed", "1", "-coverage", "1",
                   "-metadir", str(work / ("states-" + cfg)), "-config", cfg + ".cfg", module + ".tla"]
        log_path = out / (cfg + ".log")
        with log_path.open("w") as log:
            result = subprocess.run(command, cwd=work, stdout=log, stderr=subprocess.STDOUT)
        output = log_path.read_text()
        expected = 12 if cfg == "PeerCanContinue" else 0
        if result.returncode != expected or (expected and "Invariant PeerCanContinue is violated" not in output):
            raise SystemExit(f"{cfg}: unexpected exit {result.returncode}. See {log_path}")
        print(cfg, result.returncode)
