# L0 TLC run log

Re-run from the repository root via `./scripts/check-tla.sh join_owner` (single worker, `-coverage 1`, `2 GiB` heap, pinned `tla/tools/tla2tools.jar` SHA-256 `936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88`).

| Config | Generated | Distinct | Depth | Seconds | `Next` actions | Result |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| [`L0_JoinOwner.cfg`](04_evidence/L0_JoinOwner.log) | 34,226 | 2,181 | 23 | 1.922 | 22 | PASS |
| [`L0_JoinOwner_two.cfg`](04_evidence/L0_JoinOwner_two.log) | 14,022,367 | 321,256 | 35 | 79.547 | 22 | PASS |
| [`L0_JoinOwner_wrap.cfg`](04_evidence/L0_JoinOwner_wrap.log) | 2,689 | 172 | 14 | 1.291 | 16 | PASS |
| [`L0_JoinOwner_wrap_two.cfg`](04_evidence/L0_JoinOwner_wrap_two.log) | 336,733 | 7,652 | 25 | 3.507 | 16 | PASS |

Every `Next` action enabled in each configuration achieved `generated > 0` in the final coverage block, with fresh detach, exact historical ACK retry, and conflicting ACK rejection split into distinct named actions. Machine-readable counts and digests are archived in [results.json](04_evidence/results.json), and line-level source locks in [anchors.lock](02_models/anchors.lock).
