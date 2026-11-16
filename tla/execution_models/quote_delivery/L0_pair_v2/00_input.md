# Subscriber consumption after a quote outage

One authenticated `tickwire-md` publisher, one `MarketSink` subscriber, and one `tickwire-strategy-sdk` reader share the same bounded SHM market history. The subscriber joins at a complete quote. The publisher may send an unavailable quote in the same generation and resume with a fresh pair. The `MarketSink` subscriber must accept every ordered observation without latching `Error::Sink` while keeping the generation's continuity gate closed from the first invalidation onward. The [original model](../L0_pair/05_output.md) and its [five-state counterexample](../L0_pair/04_counterexamples.md) describe the pre-fix observer.

Source boundaries in this revision:

* The publisher emits the stale feed state followed by the stale exact quote at `crates/tickwire-market-publisher/src/spot.rs:100-159` and commits atomic batches via `crates/tickwire-shm/src/mapped.rs:397-445`.
* The reader identifies complete pairs and authenticated JOIN cuts at `crates/tickwire-market-source/src/replica.rs:136-183`, `crates/tickwire-market-source/src/replica.rs:265-331`, and `crates/tickwire-market-source/src/replica.rs:413-499`. Ring registration, polling, and `Error::Sink` `stopped` latching are at `crates/tickwire-market-source/src/shm.rs:200-215` and `crates/tickwire-market-source/src/shm.rs:268-363`.
* Strategy quote readiness and `MarketSink` observation contracts are at `crates/tickwire-strategy-sdk/src/worker.rs:610-716`, `crates/tickwire-strategy-sdk/src/market.rs:227-267`, and `crates/tickwire-market-source/src/lib.rs:56-75`.

