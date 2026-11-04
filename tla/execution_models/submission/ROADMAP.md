# Submission model inventory

| Layer | State domain | Reproducible result |
| --- | --- | --- |
| Commit gate | One serialized reducer, two command digests, finite order slots and cash | [Two configurations](L0_commit_gate/03_run_log.md) |
| Crash and receipt loss | One submitter, local journal, bounded crashes and four transport outcomes | [Three configurations](L1_crash_receipt/03_run_log.md) |
| Economic facts | Historical reducer and mock restart at the October 20 revision | [Four configurations and preserved evidence](L2_economic_facts/03_run_log.md) |

Run all three layers from the repository root.

```sh
./scripts/check-tla.sh submission
```
