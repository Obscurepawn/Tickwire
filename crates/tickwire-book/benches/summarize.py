#!/usr/bin/env python3
"""Summarize per-run quantiles without pretending they are pooled quantiles."""
import collections
import csv
from pathlib import Path
import statistics
import sys

path = Path(sys.argv[1])
groups = collections.defaultdict(list)
with path.open() as source:
    for row in csv.DictReader(source):
        groups[(row["workload"], int(row["levels_per_side"]), int(row["updates_total"]),
                row["variant"])].append(row)
fields = ["workload", "levels_per_side", "updates_total", "variant", "rounds"]
for metric in ["p50_ns", "p99_ns", "p999_ns", "max_ns"]:
    fields += [f"{metric}_median", f"{metric}_min", f"{metric}_max"]
with path.with_name("summary.csv").open("w", newline="") as output:
    writer = csv.DictWriter(output, fieldnames=fields, lineterminator="\n")
    writer.writeheader()
    for (workload, levels, updates, variant), runs in groups.items():
        row = dict(zip(fields[:5], [workload, levels, updates, variant, len(runs)]))
        for metric in ["p50_ns", "p99_ns", "p999_ns", "max_ns"]:
            values = [int(run[metric]) for run in runs]
            row[f"{metric}_median"] = int(statistics.median(values))
            row[f"{metric}_min"] = min(values)
            row[f"{metric}_max"] = max(values)
        writer.writerow(row)
