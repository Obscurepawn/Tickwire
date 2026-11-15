#!/usr/bin/env python3
"""Validate completed TLC runs and aggregate final action coverage."""
from pathlib import Path
import argparse
import hashlib
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
CALL_SITE = re.compile(r" \([0-9]+ [0-9]+ [0-9]+ [0-9]+\)$")
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


def verify_completed_run(text: str) -> tuple[int, int]:
    """Validate a completed coverage-free scaling TLC run (0 queued states, no errors)."""
    lines = text.splitlines()
    successes = [i for i, line in enumerate(lines) if line == SUCCESS]
    if len(successes) != 1 or any(line.startswith("Error:") for line in lines):
        raise CoverageError("expected exactly one successful model checking run")
    summaries = [(i, SUMMARY.fullmatch(line)) for i, line in enumerate(lines)]
    summaries = [(i, match) for i, match in summaries if match is not None]
    finished = [i for i, line in enumerate(lines) if line.startswith("Finished in ")]
    if len(summaries) != 1 or len(finished) != 1:
        raise CoverageError("missing unique final state summary or finish marker")
    index, summary = summaries[0]
    generated, distinct, queued = (int(value.replace(",", "")) for value in summary.groups())
    if not (successes[0] < index < finished[0]) or queued != 0 or distinct == 0 or generated < distinct:
        raise CoverageError("state exploration did not finish with an empty queue")
    return generated, distinct


def check_logs(
    paths: list[Path],
    model: str,
    scaling_paths: list[Path] | None = None,
) -> dict:
    if not paths:
        raise CoverageError("at least one completed configuration is required")
    totals: dict[str, int] = {}
    declarations: dict[str, str] = {}
    for path in paths:
        try:
            counts = final_counts(path.read_text(), model)
        except (OSError, UnicodeError, CoverageError) as error:
            raise CoverageError(f"{path}: {error}") from error
        for key, value in counts.items():
            # TLC can report several call sites for one operator declaration.
            # Preserve each site's full key when accumulating successor counts.
            declaration = CALL_SITE.sub("", key)
            name = declaration.split(" line ", 1)[0]
            if name in declarations and declarations[name] != declaration:
                raise CoverageError(f"{path}: declaration changed for action {name}")
            declarations[name] = declaration
            totals[key] = totals.get(key, 0) + value
    extra_scaling = list(scaling_paths or [])
    for spath in extra_scaling:
        try:
            verify_completed_run(spath.read_text())
        except (OSError, UnicodeError, CoverageError) as error:
            raise CoverageError(f"{spath}: {error}") from error
    missing = [key for key, value in totals.items() if value == 0]
    if missing:
        raise CoverageError("uncovered across all configurations: " + ", ".join(missing))
    return {
        "model": model,
        "runs": len(paths) + len(extra_scaling),
        "entries": len(totals),
        "generated": totals,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", required=True, help="Exact TLA module name")
    parser.add_argument("--model-file", type=Path, help="Frozen source used for all configurations")
    parser.add_argument(
        "--scaling-log",
        action="append",
        default=[],
        type=Path,
        help="Completed multi-worker scaling TLC log verified without AST coverage",
    )
    parser.add_argument("logs", nargs="+", type=Path)
    args = parser.parse_args()
    try:
        result = check_logs(args.logs, args.model, scaling_paths=args.scaling_log)
        if args.model_file is not None:
            if args.model_file.stem != args.model:
                raise CoverageError("model filename must match module name")
            result["model_sha256"] = hashlib.sha256(args.model_file.read_bytes()).hexdigest()
    except (OSError, CoverageError) as error:
        print(error, file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
