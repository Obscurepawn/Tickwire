# tickwire-handover

This crate captures and verifies the input boundary between a retiring strategy and its successor.

Keep checkpoint generation, input cursor and account frontier together. A late fill still belongs to its original order after execution management moves.

[Crate API](src/lib.rs)

Specifications include [handover inputs](../../specs/implementation/handover-inputs.md).

```sh
cargo test --locked -p tickwire-handover --all-targets
```
