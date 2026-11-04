# Local management refusals

An authenticated Bybit or OKX cash group has two file strategies sharing one serialized account. Each member can supply a valid original local and client ID for an order. The account authorizes the logical strategy, current manager and expected owner revision after the runner verifies the order binding. A management refusal belongs to its producing queue. A missing or inconsistent original stops the sender. [Routing contract](../../../../specs/implementation/spot-strategy-routing.md#submit-and-observe)

The pre-fix model and its eight-state counterexample are [archived](../L0_shared_cash/04_counterexamples.md). The current code branches at `crates/tickwire-account/src/admission.rs:369-397` and `crates/tickwire-spot-runner/src/lib.rs:126-149`. The [v2 source map](02_models/L1_SharedCash_v2.tla) covers the surrounding submit, transport and callback actions.
