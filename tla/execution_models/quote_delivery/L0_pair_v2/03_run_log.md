# Verification runs

Run `./scripts/check-tla.sh quote_delivery` from the repository root. The pinned TLC 2.19 checker used one worker for each config.

| Maximum batches | Generated states | Distinct states | Depth | Remaining queue |
| --- | ---: | ---: | ---: | ---: |
| 2 | 713 | 447 | 10 | 0 |
| 3 | 16,213 | 9,006 | 15 | 0 |

Both runs checked the original four invariants, subscription retention and sticky revocation. TLC reported successors for Init and every Next action. [Aggregate action counts](evidence/coverage.json)

The observer regression failed on the pre-fix logic at stale sequence 4 and passes under the separated observation-acknowledgement and sticky continuity-gate semantics; run `cargo test --locked -p tickwire-market-source` and `cargo test --locked -p tickwire-strategy-sdk` to verify paired-quote completeness, JOIN snapshot cuts, and SHM subscriber polling.

