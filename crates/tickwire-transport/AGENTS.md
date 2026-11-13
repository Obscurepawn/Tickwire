# tickwire-transport

`tickwire-transport` (`unsafe_code = "forbid"` via workspace lints) owns explicitly driven, nonblocking TCP (`mio`),
TLS (`rustls` with early data disabled), HTTP/1.1 (`http`), WebSocket (`ws`), and FIX (`fix`) framing under bounded
byte and syscall budgets (`Limits`, `IoBudget`). Gateways and venue session clients call `Connection` and protocol
framers from single-threaded event loops without background threads, async executors, or automatic reconnects. Retry
policy, durability, and venue semantics belong strictly to the caller.

## Invariants and Change Guidelines

* **First-application-byte gate boundary**: In [src/connection.rs](src/connection.rs), invoke the caller's gate
  callback immediately before copying the very first application byte into `rustls::ClientConnection` or the plain
  `TcpStream`; if the gate refuses or the deadline has already passed while `accepted_bytes == 0`, report
  `SendState::NoBytes` and never emit application bytes.
* **Conservative `MayHaveSent` vs. `Flushed` accounting**: Once at least one application byte enters the TLS buffer or
  kernel socket (`accepted_bytes > 0`), transition to `SendState::MayHaveSent` and report `SendState::Flushed` only
  after all application bytes and pending TLS ciphertexts are written to the socket; any post-first-byte failure
  reports `SendState::Failed` while preserving `accepted_bytes`.
* **TLS 0-RTT prohibition and bounded buffers**: Reject `ClientConfig` instances with `enable_early_data == true`
  (`Error::Protocol("early data disabled")`), enforce `HARD_LIMIT` (16 MiB) across `Limits`, and honor `IoBudget`
  (`max_syscalls`, `max_bytes`) on every `drive` step so one peer cannot starve the reactor.
* **Strict protocol framing**: Keep HTTP response/chunk parsing in [src/http.rs](src/http.rs), RFC 6455
  masking/control-frame validation in [src/ws.rs](src/ws.rs), and FIX checksum/body-length framing in
  [src/fix.rs](src/fix.rs) bounded against `max_receive_bytes`.

## Verification and Specifications

* **Source entrypoints**: [src/lib.rs](src/lib.rs) · [src/connection.rs](src/connection.rs) ·
  [src/http.rs](src/http.rs) · [src/ws.rs](src/ws.rs) · [src/fix.rs](src/fix.rs)
* **Integration tests**: [tests/framing.rs](tests/framing.rs) · [tests/network.rs](tests/network.rs)
* **Specifications and models**: [transport.md](../../specs/implementation/transport.md) ·
  [transport](../../tla/execution_models/transport/ROADMAP.md) ·
  [gateway](../../tla/execution_models/gateway/ROADMAP.md)

```sh
cargo test --locked -p tickwire-transport --all-targets
./scripts/check-tla.sh transport
```
