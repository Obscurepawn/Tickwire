"""Check wire, risk, configuration and instrument contracts from the repository root."""
import json
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]


def check(name):
    result = subprocess.run(
        [sys.executable, f"scripts/verify-{name}.py"],
        cwd=ROOT, stdout=subprocess.PIPE, text=True, check=True,
    )
    return json.loads(result.stdout)


def main():
    wal = json.loads((ROOT / "specs/contracts/journal-wire-v1.json").read_text())["wal"]
    for section in ("header", "trailer"):
        size = sum(field["bytes"] for field in wal[f"{section}_fields"])
        if size != wal[f"{section}_bytes"]:
            raise ValueError(f"Invalid journal {section} layout")
    checks = {name: check(name) for name in (
        "ipc-contract", "risk-vectors", "config-contract", "instrument-catalog",
    )}
    checks["journal_frame"] = {"header": wal["header_bytes"], "trailer": wal["trailer_bytes"]}
    print(json.dumps(checks, indent=2))


if __name__ == "__main__":
    main()
