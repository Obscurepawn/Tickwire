# tickwire-binance-private

This network owner handles Binance private sessions and delivers ordered events and gap notices.

Trace authentication through subscription and renewal. A disconnect invalidates account freshness for that connection epoch. Reconciliation reestablishes the baseline for new risk.

[Crate API](src/lib.rs) · [clock binding](tests/clock_binding.rs) · [failures](tests/failures.rs) · [protocol server](tests/protocol_server.rs) · [requests](tests/requests.rs) · [spot lifecycle](tests/spot_lifecycle.rs)

Specifications include [binance private session](../../specs/implementation/binance-private-session.md).

```sh
cargo test --locked -p tickwire-binance-private --all-targets
```
