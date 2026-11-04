# L2-001. Failed recovery left the old engine active

The original boundary configuration found a six-state trace. The old engine admitted two orders and released the first after a zero-byte result. A fill on the second consumed 999 of 1,000 units. Restart attempted to reserve both durable orders and replay the fill. The checked sum overflowed the model's arithmetic bound and restart returned an error. The old engine could still accept orders.

The repaired implementation freezes the old engine on entry to recovery. A Rust regression exercised the same branch near `i128::MAX`. The retained TLC and Rust logs record the failure and fix. [Original trace](04_evidence/pre_fix/boundary-red.log) · [Fixed Rust test](04_evidence/v2/rust-recovery-green.log) · [Original review](https://github.com/Obscurepawn/Tickwire/tree/04351dbb59522c43d6b7a1b27366c4a2ee89ed9c/tla/execution_models/submission/L2_economic_facts/04_counterexamples.md)
