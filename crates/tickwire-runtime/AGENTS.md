# tickwire-runtime

The node drives market sessions, private input, account sends and strategy intake on an explicit event loop.

Trace an iteration from incoming account evidence to command selection and first submission. Sample fresh time after blocking or policy work. The [runtime turn](src/node.rs) polls clock, market, private input, account and then strategies. The [strategy interface](src/node/strategy.rs) delivers account history and consumes commands. Keep replay and rollback proof construction in the [handover module](src/node/handover.rs).

[Crate API](src/lib.rs) · [handover closure](tests/handover_closure.rs)

Specifications include [account gateway](../../specs/implementation/account-gateway.md), [instrument catalog](../../specs/implementation/instrument-catalog.md), [reconciliation controller](../../specs/implementation/reconciliation-controller.md), [recovery closure](../../specs/implementation/recovery-closure.md), [strategy handover](../../specs/implementation/strategy-handover.md).

```sh
cargo test --locked -p tickwire-runtime --all-targets
```
