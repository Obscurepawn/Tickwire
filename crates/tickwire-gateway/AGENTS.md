# tickwire-gateway

`tickwire-gateway` (`#![forbid(unsafe_code)]`) binds `BusinessJournal` durability, `Authorization` policy, protocol
wire encoding (`Encoder`, including `binance::BinanceRestEncoder`), and a single `tickwire-transport::Connection` into
`JournaledSender`. Callers admit `CanonicalCommand` values for durable persistence and poll `JournaledSender` to drive
nonblocking TLS output. Multi-asset balance math belongs to `tickwire-account-gateway` / `tickwire-account`, while
socket framing lives in `tickwire-transport`.

## Invariants and Change Guidelines

* **Single in-flight send bound to durable command bytes**: `JournaledSender` in [src/lib.rs](src/lib.rs) verifies
  route/domain/epoch identity, persists `PreparedIntent` in `BusinessJournal` with a `VerifiedClosure`, and requires
  `EncodedRequest::digest` to match `CanonicalCommand::digest` and `Route::authority` before queuing bytes on
  `Connection`.
* **Final `Authorization::current_gate` at first byte**: Pass the gate check into `Connection::drive_with_gate` so
  lease expiry, epoch/generation/revision drift, market invalidity, or loss of owner isolation aborts the send before
  the first application byte enters TLS or the socket.
* **Zero-byte abort vs. `Unknown` recovery posture**: Record `AbortedBeforeSend` only when `SendState::NoBytes` proves
  zero application bytes were accepted; partial sends (`MayHaveSent`), disconnects after first write, or recovered
  intents without `AbortedBeforeSend` transition to `Status::Unknown` and freeze new admissions until reconciled.
* **Pure offline request signing in `binance`**: Keep `BinanceRestEncoder` in [src/binance.rs](src/binance.rs)
  deterministic over `CanonicalCommand` parameters without wall-clock calls or hidden retries.

## Verification and Specifications

* **Source entrypoints**: [src/lib.rs](src/lib.rs) · [src/binance.rs](src/binance.rs)
* **Integration tests**: [tests/lifecycle.rs](tests/lifecycle.rs) · [tests/binance_rest.rs](tests/binance_rest.rs)
* **Specifications and models**: [gateway.md](../../specs/implementation/gateway.md) ·
  [command-binding.md](../../specs/implementation/command-binding.md) ·
  [gateway](../../tla/execution_models/gateway/ROADMAP.md)

```sh
cargo test --locked -p tickwire-gateway --all-targets
./scripts/check-tla.sh gateway
```
