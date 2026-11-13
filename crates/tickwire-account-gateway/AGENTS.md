# tickwire-account-gateway

`tickwire-account-gateway` (`#![forbid(unsafe_code)]`) coordinates one `Account` owner, its `AccountJournal`,
recovery-closure verification, and outbound REST or WebSocket transport (`AccountGateway` and
`websocket::WsAccountGateway`). Runtime callers submit canonical commands, ingest authenticated venue events, and drive
`poll` to advance connections; wire signing (`Encoder`), control policy (`Authority`, `ReactivationVerifier`), and
socket framing (`tickwire-transport`) remain injected dependencies with no permissive defaults.

## Invariants and Change Guidelines

* **Durable closure and WAL sync before output**: In [src/lib.rs](src/lib.rs) and [src/binding.rs](src/binding.rs),
  `submit` reserves exposure in `Account`, verifies dependencies via `Authority::prepare`, and syncs `Admit` in
  `AccountJournal` before staging output; persistence failures never mutate live account state or emit network bytes.
* **Fresh gate recheck at the first application byte**: Immediately before passing the first application byte to HTTP
  ([src/lib.rs](src/lib.rs)) or WebSocket ([src/websocket.rs](src/websocket.rs)) transport, re-evaluate
  `Authority::commit_state` and `Authority::current_gate` (`now < lease_until`, matching `writer_epoch`,
  `strategy_generation`, `config_revision`, `owner_isolated`, `action_permitted`, and `market_valid`).
* **Pre-first-byte abort vs. `MayHaveSent` retention**: Only a pre-first-byte gate refusal or transport proof that zero
  application bytes left the process may append `AbortBeforeSend` and release holds; once a single byte is accepted or a
  send stalls mid-stream, retain the reservation for venue reconciliation.
* **Exact-prefix reactivation**: Keep recovery and live reactivation gates in [src/recovery.rs](src/recovery.rs) bound
  to the exact `HistoryFrontier` and `CanonicalBaseline` digest before opening replacement connections.

## Verification and Specifications

* **Source and tests**: [src/lib.rs](src/lib.rs) · [src/binding.rs](src/binding.rs) ·
  [src/recovery.rs](src/recovery.rs) · [src/websocket.rs](src/websocket.rs) ·
  [tests/submission.rs](tests/submission.rs) · [tests/lifecycle.rs](tests/lifecycle.rs) ·
  [tests/binance_rest.rs](tests/binance_rest.rs) · [tests/websocket.rs](tests/websocket.rs) ·
  [tests/ingress.rs](tests/ingress.rs) · [tests/income_closure.rs](tests/income_closure.rs) ·
  [tests/recovery_metadata.rs](tests/recovery_metadata.rs) ·
  [tests/persistence_failures.rs](tests/persistence_failures.rs)
* **Specifications and models**: [account-gateway.md](../../specs/implementation/account-gateway.md) ·
  [account-recovery-binding.md](../../specs/implementation/account-recovery-binding.md) ·
  [account_gateway](../../tla/execution_models/account_gateway/ROADMAP.md) ·
  [spot_roundtrip](../../tla/execution_models/spot_roundtrip/ROADMAP.md) ·
  [binance_roundtrip](../../tla/execution_models/binance_roundtrip/ROADMAP.md)

```sh
cargo test --locked -p tickwire-account-gateway --all-targets
./scripts/check-tla.sh account_gateway
```
