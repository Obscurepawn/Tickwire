# tickwire-fencing

This crate validates evidence that establishes which owner may act for a risk domain.

Bind each proof to its epoch, scope, current clock and original source. Test expiration and a stale owner attempting to use a proof from an earlier term.

[Crate API](src/lib.rs)

Specifications include [execution core](../../specs/implementation/execution-core.md), [guardian](../../specs/implementation/guardian.md).

```sh
cargo test --locked -p tickwire-fencing --all-targets
```
