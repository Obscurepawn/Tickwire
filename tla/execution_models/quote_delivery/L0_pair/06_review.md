# Independent review of BBO subscription loss

Reviewed against the pre-fix `MarketSink` observer modeled in `L0_QuotePair`. The observed failure is permanent stoppage of `ShmSource` after an ordinary two-record unavailable BBO notification, even though the continuity gate correctly closes trading on the invalidated generation.

**Reconstruction.** The publisher can receive an unavailable event after it has already published an exact quote. It then publishes two well-formed records on the same ring: stale feed state followed by stale exact quote. The receiver applies each record and reports each discontinuous view to its `MarketSink`. Its observation carries the applied ring sequence, so the second record has a higher sequence even when its timestamp equals the first record's timestamp. See `crates/tickwire-market-publisher/src/spot.rs:58-75`, `crates/tickwire-market-publisher/src/spot.rs:129-159`, `crates/tickwire-market-source/src/replica.rs:136-183`, `crates/tickwire-market-source/src/replica.rs:265-331`, and `crates/tickwire-market-source/src/shm.rs:283-322`.

**Refutation attempts.** A same-sequence fast path cannot accept the second record because sequence `4 > 3`. Polling the two records in one budget still invokes `MarketSink::market_observed` twice. Returning `false` from `MarketSink::market_observed` on sequence `4` causes `ShmSource::announce_if_actionable` to return `Err(Error::Sink)` and `ShmSource::poll` to set `self.stopped = true`, causing subsequent polls to return `Ok(0)` permanently. See `crates/tickwire-market-source/src/lib.rs:56-75`, `crates/tickwire-market-source/src/shm.rs:200-215`, and `crates/tickwire-market-source/src/shm.rs:330-363`.

**Recovery boundary.** An invalidated generation must remain closed to trading, while forward well-formed records on the authenticated SHM ring continue to advance the observed sequence watermark without tripping `Error::Sink`. See [`L0_pair_v2`](../L0_pair_v2/05_output.md).

**Disposition.** Confirmed availability defect in the pre-fix observer, resolved in `L0_QuotePair_v2`: a valid two-record producer unavailability notice must keep continuity closed for the invalidated generation while retaining the healthy `ShmSource` subscription across both stale records.

