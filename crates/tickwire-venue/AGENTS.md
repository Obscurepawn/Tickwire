# tickwire-venue

The venue domain model defines immutable commands and economic facts shared by adapters and the account. Its stable catalog IDs are 1 for Binance, 2 for Bybit and 3 for OKX. The same IDs identify the venue on market records, durable routes and account commands.

Preserve stable IDs, exact decimal units and canonical bytes. Keep protocol-specific signing and native error codes in the connector that owns them.

[Crate API](src/lib.rs) · [account input](tests/account_input.rs) · [amendment](tests/amendment.rs) · [commands](tests/commands.rs) · [contract](tests/contract.rs) · [events](tests/events.rs)

Specifications include [usdm account facts](../../specs/implementation/usdm-account-facts.md), [venue](../../specs/implementation/venue.md).

```sh
cargo test --locked -p tickwire-venue --all-targets
```
