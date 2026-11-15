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
    # Note: `tickwire-venue/tests/contract.rs` and `ipc-wire-v1.json` are self-contained
    # with zero WAL (`journal-wire-v1.json`) framing dependencies.
    checks = {name: check(name) for name in (
        "ipc-contract", "risk-vectors", "config-contract", "instrument-catalog",
    )}
    print(json.dumps(checks, indent=2))


if __name__ == "__main__":
    main()
