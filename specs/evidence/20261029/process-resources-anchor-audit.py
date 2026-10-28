#!/usr/bin/env python3
"""Read-only Managed process resource anchor audit. Does not run TLC or rewrite source locks."""
import argparse
import importlib.util
import json
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    parser.add_argument("--base", default="2bbd292517569525cc3f1439f876ba59d40636b2")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    path = Path(__file__).parent.parent / "20261028/market-expansion-core-anchor-audit.py"
    spec = importlib.util.spec_from_file_location("core_anchor_audit", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    report = module.audit(args.repo.resolve(), args.base)
    report["interpretation"] = [
        "Read-only comparison with the previously pushed immutable executable stage. No model, lock, or historical TLC result was changed.",
        "Pre-exec cgroup migration, per-thread resource observations and purpose-specific launch routing await final formal verification after functional development.",
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
