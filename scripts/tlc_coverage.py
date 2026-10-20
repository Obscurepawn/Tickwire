#!/usr/bin/env python3
"""Validate completed TLC runs and aggregate final action coverage."""
from pathlib import Path
import argparse
import json
import re
import sys

SUCCESS = "Model checking completed. No error has been found."
START = "The coverage statistics at "
END = "End of statistics."
LONG_END = (
    "End of statistics (please note that for performance reasons large models",
    "are best checked with coverage and cost statistics disabled).",
)
ENTRY = re.compile(r"^<(.+ of module ([A-Za-z_][A-Za-z_0-9]*)(?: \([^>]*\))?)>: ([0-9]+):([0-9]+)$")
PROPERTY = re.compile(r"^<.+ of module ([A-Za-z_][A-Za-z_0-9]*)(?: \([^>]*\))?>$")
SUMMARY = re.compile(
    r"^([0-9,]+) states generated, ([0-9,]+) distinct states found, "
    r"([0-9,]+) states left on queue\.$"
)


class CoverageError(ValueError):
    """A TLC log is incomplete, incompatible, or lacks action coverage."""


def final_counts(text: str, model: str) -> dict[str, int]:
    """Require a single completed run, then read its final full coverage block."""
    lines = text.splitlines()
    successes = [i for i, line in enumerate(lines) if line == SUCCESS]
    if len(successes) != 1 or any(line.startswith("Error:") for line in lines):
        raise CoverageError("expected exactly one successful model checking run")
    starts = [i for i, line in enumerate(lines) if line.startswith(START)]
    if not starts or starts[-1] <= successes[0]:
        raise CoverageError("missing final coverage after successful checking")
    start = starts[-1]
    end = None
    counts = {}
    for index in range(start + 1, len(lines)):
        line = lines[index]
        if line == END:
            end = index
            break
        if line == LONG_END[0]:
            if index + 1 >= len(lines) or lines[index + 1] != LONG_END[1]:
                raise CoverageError("truncated coverage footer")
            end = index + 1
            break
        if line.startswith("<"):
            prop = PROPERTY.fullmatch(line)
            if prop and prop.group(1) == model:
                # Invariants have expression-cost children, not successor counts.
                continue
            match = ENTRY.fullmatch(line)
            if not match:
                raise CoverageError(f"malformed coverage entry: {line}")
            key, module, distinct, generated = match.groups()
            if module != model or key in counts or int(distinct) > int(generated):
                raise CoverageError(f"incompatible or duplicate coverage entry: {line}")
            counts[key] = int(generated)
    if end is None or not counts:
        raise CoverageError("missing complete final coverage block")
    summaries = [(i, SUMMARY.fullmatch(line)) for i, line in enumerate(lines)]
    summaries = [(i, match) for i, match in summaries if match is not None]
    finished = [i for i, line in enumerate(lines) if line.startswith("Finished in ")]
    if len(summaries) != 1 or len(finished) != 1:
        raise CoverageError("missing unique final state summary or finish marker")
    index, summary = summaries[0]
    generated, distinct, queued = (int(value.replace(",", "")) for value in summary.groups())
    if not (end < index < finished[0]) or queued != 0 or distinct == 0 or generated < distinct:
        raise CoverageError("state exploration did not finish with an empty queue")
    return counts


def check_logs(paths: list[Path], model: str) -> dict:
    if not paths:
        raise CoverageError("at least one completed configuration is required")
    totals: dict[str, int] = {}
    for path in paths:
        try:
            counts = final_counts(path.read_text(), model)
        except (OSError, UnicodeError, CoverageError) as error:
            raise CoverageError(f"{path}: {error}") from error
        if totals and totals.keys() != counts.keys():
            raise CoverageError(f"{path}: final action entries differ across configurations")
        for key, value in counts.items():
            totals[key] = totals.get(key, 0) + value
    missing = [key for key, value in totals.items() if value == 0]
    if missing:
        raise CoverageError("uncovered across all configurations: " + ", ".join(missing))
    return {"model": model, "runs": len(paths), "entries": len(totals), "generated": totals}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", required=True, help="Exact TLA module name")
    parser.add_argument("logs", nargs="+", type=Path)
    args = parser.parse_args()
    try:
        result = check_logs(args.logs, args.model)
    except CoverageError as error:
        print(error, file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
