# Verification runs

Run `./scripts/check-tla.sh quote_delivery` from the repository root. The pinned TLC 2.19 checker used one worker for each config.

| Maximum batches | Generated states | Distinct states | Depth | Remaining queue |
| --- | ---: | ---: | ---: | ---: |
| 2 | 713 | 447 | 10 | 0 |
| 3 | 16,213 | 9,006 | 15 | 0 |

Both runs checked the original four invariants, subscription retention and sticky revocation. TLC reported successors for Init and every Next action. [Aggregate action counts](evidence/coverage.json)

The policy regression failed on the original code at stale sequence 4. Run `cargo test --locked -p tickwire-runtime-policy revoked_quote_generation` to check denial through the outage and new-generation authority. Run `cargo test --locked -p tickwire-spot-runner --test reconcile_process execution_source_only_announces_complete_fresh_quote_pairs` for the authenticated Linux JOIN, stale pair and later complete BBO.
