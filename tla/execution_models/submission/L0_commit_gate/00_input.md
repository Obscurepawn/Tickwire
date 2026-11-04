# Local commit gate

TLC explores reservations for two possible immutable command digests. One account owner has a bounded number of order slots and a finite amount of cash. Changing the gate or freezing the owner can interleave with actions on these orders.

The reducer accepts a receipt for the reserved record after local persistence. It samples the gate again before returning an order for its first send. An ambiguous send keeps the reservation. A verified zero-byte rejection can terminate an unsent order and release its cash. [Model](02_models/L0_CommitGate.tla) · [Execution reducer](../../../../crates/tickwire-execution/src/lib.rs)
