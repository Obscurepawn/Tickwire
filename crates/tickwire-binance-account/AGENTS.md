# tickwire-binance-account

This owner journals Binance private inputs and correlates orders, trades and reconciliation responses with the account.

Trace a fill from its native trade ID to the account record. Preserve source order through recovery, including fills that arrive after a terminal order report.

[Crate API](src/lib.rs) · [account observation](tests/account_observation.rs) · [network ingress](tests/network_ingress.rs) · [persistence](tests/persistence.rs)

Specifications include [binance ws trading](../../specs/implementation/binance-ws-trading.md), [spot amend](../../specs/implementation/spot-amend.md), [usdm account facts](../../specs/implementation/usdm-account-facts.md).

```sh
cargo test --locked -p tickwire-binance-account --all-targets
```
