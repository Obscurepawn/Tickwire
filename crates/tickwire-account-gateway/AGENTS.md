# tickwire-account-gateway

The account gateway binds a canonical command to its recovery closure, admission and outbound bytes.

Follow `AccountGateway::submit` from balance reservation through journal sync. Then trace `poll` to the TLS first-write check. Cover exact retries, grant expiry before output and partial sends. The latter case must retain the reservation for reconciliation.

[Crate API](src/lib.rs) · [submission and retries](tests/submission.rs) · [Binance REST](tests/binance_rest.rs) · [ingress](tests/ingress.rs) · [lifecycle](tests/lifecycle.rs) · [persistence failures](tests/persistence_failures.rs)

Specifications include [account gateway](../../specs/implementation/account-gateway.md), [account recovery binding](../../specs/implementation/account-recovery-binding.md).

```sh
cargo test --locked -p tickwire-account-gateway --all-targets
```
