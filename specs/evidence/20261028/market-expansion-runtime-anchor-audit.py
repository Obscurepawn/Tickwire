#!/usr/bin/env python3
"""Read-only runtime-stage anchor audit. Does not run TLC or rewrite source locks."""
import argparse
import importlib.util
import json
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    parser.add_argument("--base", default="e20ce050c17998951d5f5657709c9ca63bc3b481")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    path = Path(__file__).with_name("market-expansion-core-anchor-audit.py")
    spec = importlib.util.spec_from_file_location("core_anchor_audit", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    report = module.audit(args.repo.resolve(), args.base)
    report["interpretation"] = [
        "Read-only comparison with the previously pushed expansion-core stage. No model, lock, or historical TLC result was changed.",
        "The new runtime Arm/Commit protocol, consumer re-JOIN, durable control decisions, producer-frontier cursor basis and process acceptance scenario require final formal verification after functional development.",
        "A fixed-line hash match is not a renewed correspondence proof; historical evidence remains limited to its original source snapshot and finite domains.",
        "Native runtime tests and scenario evidence are recorded separately; this audit makes no TLC or production-qualification claim.",
    ]
    output = json.dumps(report, ensure_ascii=False, indent=2) + "\n"
    if args.output:
        args.output.write_text(output)
    else:
        print(output, end="")


if __name__ == "__main__":
    main()
