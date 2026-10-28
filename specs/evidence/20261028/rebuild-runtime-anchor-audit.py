#!/usr/bin/env python3
"""Read-only Declared-rebuild installation runtime anchor audit. Does not run TLC or rewrite source locks."""
import argparse
import importlib.util
import json
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    parser.add_argument("--base", default="6d9fc5e8b861b615d632a3f03871fee1e280335a")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    path = Path(__file__).with_name("market-expansion-core-anchor-audit.py")
    spec = importlib.util.spec_from_file_location("core_anchor_audit", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    report = module.audit(args.repo.resolve(), args.base)
    report["interpretation"] = [
        "Read-only comparison with the previously pushed CaptureOnly runtime stage. No model, lock, or historical TLC result was changed.",
        "Same-process kind9 installation, retained warm engine and history, v4 recovery, kind10 rollback sealing and schema4 App publication require final formal verification after functional development.",
        "A fixed-line hash match is not a renewed correspondence proof; historical evidence remains limited to its original source snapshot and finite domains.",
        "Native component and actual process tests are recorded separately; this audit makes no TLC or production-qualification claim.",
    ]
    output = json.dumps(report, ensure_ascii=False, indent=2) + "\n"
    if args.output:
        args.output.write_text(output)
    else:
        print(output, end="")


if __name__ == "__main__":
    main()
