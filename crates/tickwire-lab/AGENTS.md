# tickwire-lab

The lab drives real local processes and a deterministic exchange peer through trading scenarios.

Use a scenario that exercises the component being changed and inspect the captured process receipts. A loopback result describes that transport, profile and input load.

[Crate API](src/lib.rs) · [install cost probe](tests/install_cost_probe.rs)

Specifications include [market expansion](../../specs/implementation/market-expansion.md), [strategy handover](../../specs/implementation/strategy-handover.md).

```sh
cargo test --locked -p tickwire-lab --all-targets
```
