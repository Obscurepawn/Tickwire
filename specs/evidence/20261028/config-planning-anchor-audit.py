#!/usr/bin/env python3
"""Read-only Configuration planning anchor audit. Does not run TLC or rewrite source locks."""
import argparse
import importlib.util
import json
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    parser.add_argument("--base", default="e1e14caf339768f48f18dbbd763338bcbd6a2fa8")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    path = Path(__file__).with_name("market-expansion-core-anchor-audit.py")
    spec = importlib.util.spec_from_file_location("core_anchor_audit", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    report = module.audit(args.repo.resolve(), args.base)
    report["interpretation"] = [
        "Read-only comparison with the previously pushed rebuild runtime stage. No model, lock, or historical TLC result was changed.",
        "Pure configuration plans, dual-ReadIndex drafts, bounded background work and runtime observation verification await final formal verification after functional development.",
        "A fixed-line hash match is not a renewed correspondence proof; historical evidence remains limited to its original source snapshot and finite domains.",
        "This audit makes no TLC or production-qualification claim.",
    ]
    output = json.dumps(report, ensure_ascii=False, indent=2) + "\n"
    if args.output:
        args.output.write_text(output)
    else:
        print(output, end="")


if __name__ == "__main__":
    main()
