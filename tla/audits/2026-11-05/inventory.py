"""Print the audit inventory using the pinned source revision and the submitted models."""
from collections import defaultdict
from functools import cache
import hashlib
from pathlib import Path
import re
import subprocess

ROOT = Path(__file__).resolve().parents[3]
REV = "9b2ed630721911e01046822bddc9db21b2fa5aee"
MODELS = ROOT / "tla/execution_models"
RUST_SOURCE = re.compile(r"crates/(tickwire-[\w-]+)/src/[\w./-]+\.rs")

def git(*args):
    return subprocess.check_output(["git", "-C", str(ROOT), *args])

@cache
def source(path):
    result = subprocess.run(["git", "-C", str(ROOT), "show", f"{REV}:{path}"],
                            capture_output=True)
    if result.returncode:
        return []
    return [hashlib.sha256(line).hexdigest() for line in result.stdout.splitlines()]

def classify_runs(rows):
    relocated = changed = 0
    for path, records in rows.items():
        lines = source(path)
        runs = []
        for idx, digest in sorted(records):
            if not runs or idx != runs[-1][-1][0] + 1:
                runs.append([])
            runs[-1].append((idx, digest))
        for run in runs:
            start = run[0][0]
            hashes = [digest for _, digest in run]
            if lines[start:start + len(hashes)] == hashes:
                continue
            matches = sum(lines[i:i + len(hashes)] == hashes
                          for i, h in enumerate(lines) if h == hashes[0])
            if matches == 1:
                relocated += 1
            else:
                changed += 1
    return relocated, changed


def model_anchors(folder):
    files = list((folder / "02_models").glob("*.tla"))
    if (folder / "00_input.md").exists():
        files.append(folder / "00_input.md")
    if folder.parent.name == "spot_account_recovery":
        files.append(folder.parent / "README.md")
    return set(RUST_SOURCE.findall("\n".join(f.read_text() for f in files)))

def main():
    old = set(git("ls-tree", "-r", "--name-only", REV, "tla/execution_models").decode().splitlines())
    crates = sorted(git("ls-tree", "-d", "--name-only", REV + ":crates").decode().splitlines())
    historic, added = defaultdict(set), defaultdict(set)
    drift = []
    for folder in sorted(MODELS.glob("*/L*/02_models")):
        models = list(folder.glob("*.tla"))
        if not models:
            continue
        level = folder.parent
        family = level.parent.name
        before = any(str(m.relative_to(ROOT)) in old for m in models)
        cites = model_anchors(level)
        lock = folder / "anchors.lock"
        total = mismatches = 0
        records = defaultdict(list)
        if lock.exists():
            for row in lock.read_text().splitlines():
                if not row or row.startswith("#"):
                    continue
                path, number, expected = row.split("\t")
                if m := RUST_SOURCE.fullmatch(path):
                    cites.add(m.group(1))
                lines = source(path)
                idx = int(number) - 1
                records[path].append((idx, expected))
                total += 1
                mismatches += not (0 <= idx < len(lines) and lines[idx] == expected)
        for crate in cites:
            (historic if before else added)[crate].add(family)
        if before:
            drift.append((level.relative_to(MODELS), total, mismatches, *classify_runs(records)))
    print("# Source-anchor inventory\n")
    print(f"Source revision `{REV[:12]}`. Scope includes all {len(crates)} Cargo workspace crates.")
    print("A family name records a production source reference in a level input, model or line lock. "
          "A reference can supply a caller assumption. It does not establish coverage of every function in that crate. "
          "A dash means the audited artifacts contain no explicit production source path for the crate.")
    print("\n| Crate | Previous families | Families added in this audit |\n| --- | --- | --- |")
    for crate in crates:
        print(f"| `{crate}` | {', '.join(sorted(historic[crate])) or '—'} | {', '.join(sorted(added[crate])) or '—'} |")
    print(f"\n{sum(bool(historic[c]) for c in crates)} crates had previous references. "
          f"{sum(not (historic[c] or added[c]) for c in crates)} have no direct source reference after this audit.")
    print("\n## Previous line locks\n")
    print("For each pre-existing level, the script compares the stored SHA-256 at each recorded file and line against the pinned Rust tree. "
          "A moved line counts as a mismatch. Contiguous recorded ranges are classified separately. A verbatim match at one other position in the same file counts as relocated. A changed, missing or ambiguous range needs a source review. A matching lock checks the cited text only. The historical input assumptions can change elsewhere.")
    print("\n| Level | Recorded lines | Changed lines | Relocated ranges | Ranges needing review |\n| --- | ---: | ---: | ---: | ---: |")
    for level, total, mismatch, moved, review in drift:
        print(f"| `{level}` | {total if total else 'No lock'} | {mismatch if total else '—'} | {moved} | {review} |")
    locked = [(n,m) for _,n,m,_,_ in drift if n]
    print(f"\n{len(locked)} of {len(drift)} previous levels have locks. "
          f"{sum(m > 0 for _,m in locked)} locks contain a mismatch. "
          f"{sum(m for _,m in locked):,} of {sum(n for n,_ in locked):,} recorded line references differ.")
    print("\nReproduce with `python3 tla/audits/2026-11-05/inventory.py` at the audited checkout.")

if __name__ == "__main__":
    main()
