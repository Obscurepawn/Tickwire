# tickwire-recovery

Recovery objects bind a command to its immutable dependencies and verified history.

Check digest, object length and all required edges before constructing a usable closure. Cover missing objects and a valid object borrowed from a different order.

[Crate API](src/lib.rs) · [closure](tests/closure.rs) · [handover rebuild](tests/handover_rebuild.rs)

Specifications include [account recovery binding](../../specs/implementation/account-recovery-binding.md), [recovery closure](../../specs/implementation/recovery-closure.md).

```sh
cargo test --locked -p tickwire-recovery --all-targets
```
