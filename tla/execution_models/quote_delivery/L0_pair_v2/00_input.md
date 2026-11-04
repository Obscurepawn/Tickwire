# Account consumption after a quote outage

One authenticated publisher, one execution subscription and one SDK reader share the same bounded market history. The account joins at a complete quote. The publisher may send an unavailable quote in the same generation and resume with a fresh pair. The execution subscription must accept every ordered observation and keep the generation closed to trading from the first invalidation onward. The [original model](../L0_pair/05_output.md) and its [five-state counterexample](../L0_pair/04_counterexamples.md) describe the earlier observer.

Source boundaries in this revision:

* The publisher emits the stale feed state followed by the stale exact quote at `crates/tickwire-market-publisher/src/spot.rs:100-159`.
* The reader identifies complete pairs and authenticated JOIN cuts at `crates/tickwire-market-source/src/replica.rs:127-174`. Ring registration and polling are at `crates/tickwire-market-source/src/shm.rs:268-321`. A false sink result retires the source at `crates/tickwire-app/src/market.rs:206-245`.
* The observer selects the subscription input at `crates/tickwire-runtime-policy/src/lib.rs:1063-1081`. Generation, sequence, timestamp and sticky permission are updated at `crates/tickwire-runtime-policy/src/lib.rs:605-670`. The order gate reads this live bit and the referenced generation at `crates/tickwire-runtime-policy/src/lib.rs:1029-1047`.
