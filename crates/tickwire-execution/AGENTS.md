# tickwire-execution

This deterministic order reducer tracks intent, send progress, cancellation, terminal status and economic fills.

Apply a late fill after rejection of a cancel and a repeated trade ID after restart. Track budget reserved, released and spent independently throughout the sequence.

[Crate API](src/lib.rs) · [lifecycle](tests/lifecycle.rs)

Specifications include [command binding](../../specs/implementation/command-binding.md), [execution core](../../specs/implementation/execution-core.md), [mock abort](../../specs/implementation/mock-abort.md).

```sh
cargo test --locked -p tickwire-execution --all-targets
```
