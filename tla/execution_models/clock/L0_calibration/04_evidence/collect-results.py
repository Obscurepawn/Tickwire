"""Verify completed runs, source anchors and exact inputs before writing results."""
import hashlib
import json
from pathlib import Path
import re
import sys

level = Path(__file__).resolve().parents[1]
repo = level.parents[3]
evidence = level / "04_evidence"
sys.path.insert(0, str(repo / "scripts"))
from tlc_coverage import check_logs


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def relative(path):
    return str(path.relative_to(repo))


source_hashes = {
    "crates/tickwire-clock/src/lib.rs":
        "14cb9c43701fec8ea9339a5009b497bc0e629a9bae8aa3d8f22caa8a5cd0ffb2",
    "crates/tickwire-clock/tests/calibration.rs":
        "e2a9b1b266acb78d5a68f8ffb68ff45ee818b8006ba701858f88d9f03788ca40",
}
for path, expected in source_hashes.items():
    assert sha(repo / path) == expected, path
model = level / "02_models/L0_Calibration.tla"
assert sha(model) == "cc453c8ae056e4eccbcecc362d88f94c6bbac549ae6c2540e3e12a330d475ffd"
lock = level / "02_models/anchors.lock"
anchors = 0
for row in lock.read_text().splitlines():
    if not row or row.startswith("#"):
        continue
    path, line, expected = row.split("\t")
    text = (repo / path).read_text().splitlines()[int(line) - 1]
    assert hashlib.sha256(text.encode()).hexdigest() == expected, (path, line)
    anchors += 1
names = ["primary", "rounding", "boundary", "usdm"]
logs = [evidence / ("final-" + name + ".log") for name in names]
coverage = check_logs(logs, "L0_Calibration")
assert coverage["entries"] == 16
runs = []
for name, log in zip(names, logs):
    text = log.read_text()
    counts = re.search(
        r"([\d,]+) states generated, ([\d,]+) distinct states found, 0 states left on queue.", text)
    generated, distinct = (int(n.replace(",", "")) for n in counts.groups())
    depth = int(re.search(r"depth of the complete state graph search is (\d+)", text).group(1))
    elapsed = int(re.search(r">>> elapsed: (\d+)s \(exit 0\)", text).group(1))
    final = text.rsplit("The coverage statistics at ", 1)[1].split("End of statistics.", 1)[0]
    actions = {}
    for key, distinct_count, generated_count in re.findall(
            r"^<(.+ of module L0_Calibration[^>]*)>: (\d+):(\d+)$", final, re.M):
        actions[key] = {"distinct": int(distinct_count), "generated": int(generated_count)}
    assert len(actions) == 16 and all(v["generated"] > 0 for v in actions.values())
    cfg = level / "02_models" / ("L0_Calibration" + ("" if name == "primary" else "_" + name) + ".cfg")
    runs.append({"name": name, "cfg": relative(cfg), "cfg_sha256": sha(cfg),
                 "log": relative(log), "log_sha256": sha(log), "generated": generated,
                 "distinct": distinct, "depth": depth, "elapsed_seconds": elapsed,
                 "coverage": actions})
helpers = {}
for name in ["ClockWords.tla.txt", "ClockWordsCheck.tla.txt", "ClockWordsCheck.cfg.txt",
             "word-vectors.py", "word-vectors.json", "word-final-check.log",
             "word-embedding-check.log", "IntLimitProbe.tla.txt", "IntLimitProbe.cfg.txt",
             "int-limit.log", "run-models.py"]:
    helpers[relative(evidence / name)] = sha(evidence / name)
result = {
    "model": relative(model), "model_sha256": sha(model),
    "jar_sha256": sha(repo / "tla/tools/tla2tools.jar"),
    "source_sha256": source_hashes, "invariants": 15,
    "anchors": {"lines": anchors, "sha256": sha(lock)},
    "runs": runs, "coverage": coverage, "arithmetic_evidence_sha256": helpers,
    "reviews": [
        "market_core: exact digits, independent Python cases and continuous truth witness PASS; read only",
        "ipc_codec: Core operators versus frozen source and constructor boundary PASS; read only",
        "root: final evidence review pending",
    ],
}
(evidence / "results.json").write_text(json.dumps(result, indent=2) + "\n")
for run in runs:
    print(run["name"], run["generated"], run["distinct"], run["depth"], run["elapsed_seconds"])
print(anchors, "anchors; 16 final entries per configuration; all hashes verified")
