# tickwire-binance-private

`tickwire-binance-private` (`#![forbid(unsafe_code)]`) drives authenticated Binance Spot WebSocket API user-data
subscriptions (`SpotLogon` / `SpotSignature`) and USD-M listen-key private streams (`UsdmCreate` / `UsdmKeepalive` /
`UsdmClose`) over nonblocking `tickwire-transport` sockets (`PrivateClient`). Stateless runtimes poll `PrivateClient`
under explicit I/O budgets to receive ordered `PrivateEvent` frames and connection-epoch gap/disconnect notices without
local WAL files, complementing the multi-market ORS `GuardedValue` stream-token hooks in `tickwire-binance-account`.

## Invariants and Change Guidelines

* **Epoch-scoped session freshness**: Every connect, logon, subscription, or listen-key renewal in
  [src/session.rs](src/session.rs) and [src/client.rs](src/client.rs) is bound to a non-zero `connection_epoch` and
  `AccountScope` (`risk_domain_id`, `venue == BINANCE`, `account`, `credential_generation`); any disconnect, protocol
  error, missed heartbeat, or listen-key expiry invalidates freshness for that epoch and triggers stateless upstream
  reconciliation.
* **First-write clock revalidation for signed control frames**: Before sending signed Spot logon or subscription
  frames in [src/request.rs](src/request.rs) and [src/client.rs](src/client.rs), invoke `Clock::check_signed` against
  the original `SignedTime`, `AccountScope`, and `recv_window_micros`.
* **Redacted `Debug` for credentials and raw payloads**: Preserve manual `Debug` redaction on `Authentication`
  (`Authentication([REDACTED])`) and `PrivateEvent` (`payload: "[REDACTED]"`) in [src/lib.rs](src/lib.rs) so API keys,
  signing keys, and raw user-data JSON never leak into logs or error traces.
* **Strict correlation of control responses**: Match control replies in [src/response.rs](src/response.rs) by exact
  `RequestToken` (`{connection_epoch}-{serial}`) and reject unsolicited or cross-epoch acknowledgements with
  `Error::Protocol`.

## Verification and Specifications

* **Source entrypoints**: [src/lib.rs](src/lib.rs) · [src/client.rs](src/client.rs) · [src/session.rs](src/session.rs)
  · [src/request.rs](src/request.rs) · [src/response.rs](src/response.rs)
* **Integration tests**: [tests/spot_lifecycle.rs](tests/spot_lifecycle.rs) ·
  [tests/usdm_lifecycle.rs](tests/usdm_lifecycle.rs) · [tests/clock_binding.rs](tests/clock_binding.rs) ·
  [tests/requests.rs](tests/requests.rs) · [tests/protocol_server.rs](tests/protocol_server.rs) ·
  [tests/failures.rs](tests/failures.rs)
* **Specifications and models**: [binance-private-session.md](../../specs/implementation/binance-private-session.md) ·
  [binance_private_session](../../tla/execution_models/binance_private_session/ROADMAP.md)

```sh
cargo test --locked -p tickwire-binance-private --all-targets
./scripts/check-tla.sh binance_private_session
```
