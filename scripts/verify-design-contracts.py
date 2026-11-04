"""Check wire, risk, configuration and instrument contracts from the repository root."""
import hashlib
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
    journal = json.loads((ROOT / "specs/contracts/journal-wire-v1.json").read_text())
    wal = journal["wal"]
    event = journal["embedded_venue_event"]
    vectors = ROOT / "crates/tickwire-venue/tests/data"
    original = bytes.fromhex((vectors / "multi-asset-fill.hex").read_text())
    trade = bytes.fromhex((vectors / "trade-only-fill.hex").read_text())
    # These two independently transcribed vectors differ only by envelope version and fill status.
    differences = [(old, new) for old, new in zip(original, trade, strict=True) if old != new]
    versions, statuses = event["versions"], event["fill_order_status"]
    if (differences != [(versions["ordinary"], versions["fill_with_unreported_lifecycle"]),
                        (statuses["PartiallyFilled"], statuses["Unreported"])]
            or statuses["Unknown"] != 8 or event["max_bytes"] != 16_384
            or hashlib.sha256(trade).hexdigest() != (vectors / "trade-only-fill.sha256").read_text().strip()):
        raise ValueError("Embedded venue-event definitions differ from the canonical vectors")
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
