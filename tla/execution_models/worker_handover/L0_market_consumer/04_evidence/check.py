"""Rebuild the two safety logs and the expected receipt-clock counterexample."""
from pathlib import Path
import shutil
import subprocess
import tempfile

evidence = Path(__file__).resolve().parent
model = evidence.parent / "02_models/L0_MarketConsumer.tla"
repo = evidence.parents[4]
jar = repo / "tla/tools/tla2tools.jar"
logs = []
for cfg in [evidence / "clock/reject.cfg", *sorted(model.parent.glob("*.cfg"))]:
    negative = cfg.stem == "reject"
    log = cfg.with_suffix(".log") if negative else evidence / f"{cfg.stem}.log"
    with tempfile.TemporaryDirectory() as scratch, log.open("w") as output:
        shutil.copy(model, scratch)
        shutil.copy(cfg, scratch)
        run = subprocess.run(["java", "-Xmx256m", "-XX:+UseParallelGC", "-cp", jar, "tlc2.TLC",
                              "-workers", "1", "-fp", "0", "-coverage", "1", "-metadir", "states",
                              "-config", cfg.name, model.name], cwd=scratch, stdout=output, stderr=subprocess.STDOUT)
    raw = log.read_text()
    if negative:
        assert run.returncode == 12 and "Invariant ValidReceiptHandled is violated" in raw, raw[-2000:]
    else:
        assert run.returncode == 0, raw[-2000:]
        logs.append(log)
    print("\n".join(s for s in raw.splitlines() if "is violated" in s or "states generated," in s or "depth of" in s))
with (evidence / "coverage.json").open("w") as output:
    subprocess.run(["python3", repo / "scripts/tlc_coverage.py", "--model", model.stem,
                    "--model-file", model, *logs], stdout=output, check=True)
