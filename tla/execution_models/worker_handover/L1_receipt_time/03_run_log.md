# TLC runs

Run `./scripts/check-tla.sh worker_handover` at the repository root with the pinned TLC 2.19 jar. Both configurations enable `ValidReceiptHandled`, the invariant that failed in the [seven-state trace](../L0_market_consumer/04_counterexamples.md).

| Maximum old cursor | Maximum successor lead | Distinct states | Depth | States left |
| ---: | ---: | ---: | ---: | ---: |
| 2 | 0 | 5,996 | 34 | 0 |
| 4 | 2 | 28,888 | 36 | 0 |

Run `cargo test --locked -p tickwire-strategy-host report_dispatch_validates_against_packet_receive_time` for the authenticated socket case. A worker stamp one nanosecond past the host's loop-start time succeeds when received at that stamp. A future stamp and a receipt read at the approval deadline both fail.

[Primary TLC output](04_evidence/L1_MarketConsumer_v2.log) · [Expanded TLC output](04_evidence/L1_MarketConsumer_v2_ahead.log)

As a sensitivity check, setting `pollAt` to the prior loop sample in `SamplePacket` makes `ValidReceiptHandled` fail at depth 9. Both positive runs use the post-read clock sample.
