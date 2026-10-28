#!/usr/bin/env python3
"""Read-only historical anchor audit; never runs TLC or changes model/lock files.

Usage: python3 market-expansion-core-anchor-audit.py --repo PATH --output REPORT.json
HEAD is the default comparison baseline; --base accepts another local Git revision.
Line hashes use UTF-8 line text without its newline, matching anchors.lock headers.
"""
import argparse
import collections
import datetime
import hashlib
import json
import pathlib
import re
import subprocess


def sha(data):
    return hashlib.sha256(data).hexdigest()


def compact(numbers):
    spans = []
    for n in sorted(numbers):
        if spans and n == spans[-1][1] + 1:
            spans[-1][1] = n
        else:
            spans.append([n, n])
    return [str(a) if a == b else f"{a}-{b}" for a, b in spans]


def audit(repo, base):
    def git(*args):
        return subprocess.check_output(["git", "-C", str(repo), *args])

    base_commit = git("rev-parse", base).decode().strip()
    tracked = set(git("diff", "--name-only", base_commit).decode().splitlines())
    untracked = set(git("ls-files", "--others", "--exclude-standard").decode().splitlines())
    changed = tracked | untracked
    lock_paths = sorted((repo / "tla").rglob("anchors.lock"))
    anchored_sources = set()
    locks = []
    overlaps = []
    for path in lock_paths:
        lock = str(path.relative_to(repo))
        raw_lock = path.read_bytes()
        groups = collections.defaultdict(list)
        for number, line in enumerate(raw_lock.decode().splitlines(), 1):
            fields = line.split()
            if not fields or fields[0].startswith("#"):
                continue
            if len(fields) != 3 or not fields[1].isdigit() or not re.fullmatch(r"[0-9a-f]{64}", fields[2]):
                raise ValueError(f"Unsupported anchor format: {lock}:{number}")
            source, line_number, expected = fields
            groups[source].append((int(line_number), expected))
            anchored_sources.add(source)
        locks.append({"path": lock, "sha256": sha(raw_lock), "source_count": len(groups),
                      "anchor_count": sum(map(len, groups.values())),
                      "changed_sources": sorted(changed & groups.keys())})
        if not changed.intersection(groups):
            continue
        evidence_path = path.parent.parent / "04_evidence" / "results.json"
        evidence = json.loads(evidence_path.read_text()) if evidence_path.is_file() else {}
        files = []
        for source, entries in sorted(groups.items()):
            prior = subprocess.run(["git", "-C", str(repo), "show", f"{base_commit}:{source}"], capture_output=True)
            before = prior.stdout if prior.returncode == 0 else None
            after = (repo / source).read_bytes() if (repo / source).is_file() else None
            before_lines = before.decode().splitlines() if before is not None else []
            after_lines = after.decode().splitlines() if after is not None else []
            pre_bad, post_bad, newly_bad, incidental = [], [], [], []
            details = []
            for line_number, expected in entries:
                def line_hash(lines):
                    return sha(lines[line_number - 1].encode()) if line_number <= len(lines) else None
                old_hash, new_hash = line_hash(before_lines), line_hash(after_lines)
                old_bad, new_bad = old_hash != expected, new_hash != expected
                if old_bad:
                    pre_bad.append(line_number)
                if new_bad:
                    post_bad.append(line_number)
                if new_bad and not old_bad:
                    newly_bad.append(line_number)
                if old_bad and not new_bad:
                    incidental.append(line_number)
                if old_bad != new_bad:
                    details.append({"line": line_number, "expected_sha256": expected,
                                    "baseline_line_sha256": old_hash, "worktree_line_sha256": new_hash,
                                    "classification": "newly_mismatched" if new_bad else "incidental_match"})
            file_diff = git("diff", "--unified=0", base_commit, "--", source).decode()
            files.append({"path": source, "modified_in_this_diff": source in changed,
                          "anchor_count": len(entries), "locked_line_ranges": compact(n for n, _ in entries),
                          "historical_evidence_sha256": evidence.get("source_sha256", {}).get(source),
                          "baseline_exists": before is not None, "worktree_exists": after is not None,
                          "baseline_sha256": sha(before) if before is not None else None, "worktree_sha256": sha(after) if after is not None else None,
                          "baseline_mismatch_count": len(pre_bad), "worktree_mismatch_count": len(post_bad),
                          "baseline_mismatch_lines": pre_bad, "worktree_mismatch_lines": post_bad,
                          "new_mismatch_lines": newly_bad, "incidental_match_lines": incidental,
                          "changed_match_details": details,
                          "diff_hunks": [line for line in file_diff.splitlines() if line.startswith("@@")],
                          "diff_sha256": sha(file_diff.encode())})
        overlaps.append({"lock": lock, "files": files,
                         "historical_evidence": str(evidence_path.relative_to(repo)) if evidence else None,
                         "historical_configs": list(evidence.get("results", {})) if isinstance(evidence.get("results"), dict) else [r.get("config") for r in (evidence.get("results") or []) if isinstance(r, dict)]})
    tla_changes = git("diff", "--name-only", base_commit, "--", "tla").decode().splitlines()
    return {"schema": 1, "scope": "Read-only source-anchor audit; no TLC run, model changes, or lock rebasing",
            "generated_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "repository_at_run": str(repo), "baseline_commit": base_commit,
            "line_hash_method": "SHA-256 of UTF-8 line text without newline at the historical one-based line number",
            "tracked_changed_count": len(tracked), "untracked_count": len(untracked),
            "locks_scanned": len(locks), "lock_inventory": locks,
            "changed_anchored_sources": sorted(changed & anchored_sources),
            "changed_rust_without_historical_anchor": sorted(p for p in changed - anchored_sources if p.endswith(".rs")),
            "directly_affected_locks": overlaps, "tla_changes_since_baseline": tla_changes,
            "interpretation": [
                "Read-only comparison against the stage baseline; no TLC execution or historical lock rebasing.",
                "Missing historical source paths are recorded with existence=false and null digests; deleted documentation is not restored.",
                "Fixed-line mismatches indicate source correspondence drift, not proof of a behavior regression or renewed verification.",
                "Projection launch binding, current process identity sampling and the Host startup seam are outside historical finite-model evidence.",
                "Historical proofs remain tied to their original sources and boundaries; new formal verification is deferred until functional development is complete."
            ]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=pathlib.Path, default=pathlib.Path.cwd())
    parser.add_argument("--base", default="HEAD")
    parser.add_argument("--output", type=pathlib.Path)
    args = parser.parse_args()
    report = audit(args.repo.resolve(), args.base)
    output = json.dumps(report, ensure_ascii=False, indent=2) + "\n"
    if args.output:
        args.output.write_text(output)
    else:
        print(output, end="")


if __name__ == "__main__":
    main()
