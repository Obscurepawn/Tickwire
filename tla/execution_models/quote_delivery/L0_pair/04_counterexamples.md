# A valid unavailable BBO permanently fails its healthy SHM subscription

The five-state trace starts with a live two-record BBO. The subscriber joins at ring sequence 2. The owner publishes one unavailable BBO as `FeedState` 3 followed by stale quote 4. The pre-fix `MarketSink` observer accepts mark 3 and invalidates the generation, then attempts a second mark at 4 because the feed remains noncontinuous (`crates/tickwire-market-source/src/shm.rs:283-322`). Rejecting a newer same-generation mark after invalidation causes `ShmSource` to interpret that `false` return as `Error::Sink` and latch `stopped = true` (`crates/tickwire-market-source/src/shm.rs:200-215` and `crates/tickwire-market-source/src/shm.rs:330-363`). [TLC trace](evidence/retention.log)

All frames are valid. The control connection stays open and the actor runs sequentially. Ring generation and source connectivity are separate identities: revoking continuity for the invalidated generation is intended safety behavior, whereas latching `Error::Sink` permanently stops future `ShmSource::poll` reads on a healthy ring.

The fix separates idempotent generation unavailability from a true subscription/sink error: `MarketSink::market_observed` (`crates/tickwire-market-source/src/lib.rs:56-75`) accepts ordered forward sequence observations while keeping continuity closed for the invalidated generation, so `ShmSource` stays attached without latching `Error::Sink`. [Independent review](06_review.md)

