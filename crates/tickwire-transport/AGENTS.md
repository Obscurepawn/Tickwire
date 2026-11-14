# tickwire-transport

`tickwire-transport` (`#![deny(unsafe_code)]`, `#![deny(unsafe_op_in_unsafe_fn)]` with a single audited `sys_tcp`
local-IP socket-bind boundary in [src/connection.rs](src/connection.rs)) owns explicitly driven, nonblocking TCP (`mio`),
TLS (`rustls` with
early data disabled), HTTP/1.1 (`http`), WebSocket (`ws`), and FIX (`fix`) framing under bounded byte and syscall
budgets (`Limits`, `IoBudget`), hot-path EWMA execution route selection and multi-connection pool lifecycle
(`order_router`), together with optional off-hot-path background connection dialing (`dialer`) and
asynchronous HTTP/1.1 REST reconciliation querying (`rest_querier`). Hot-path `REQ`/`RESP` loops drive `Connection`,
`OrderConnectionPool`, and protocol framers on single-threaded reactors, while `Dialer` (`"tw-dialer"`) and
`AsyncRestQuerier` (`"tw-rest-query"`) isolate DNS resolution, TCP/TLS/WS `101` handshakes, and status HTTP queries
onto dedicated worker threads behind allocation-free handoff primitives.

## Invariants and Change Guidelines

* **First-application-byte gate boundary**: In [src/connection.rs](src/connection.rs), invoke the caller's gate
  callback immediately before copying the very first application byte into `rustls::ClientConnection` or the plain
  `TcpStream`; if the gate refuses or the deadline has already passed while `accepted_bytes == 0`, report
  `SendState::NoBytes` and never emit application bytes.
* **Conservative `MayHaveSent` vs. `Flushed` accounting**: Once at least one application byte enters the TLS buffer or
  kernel socket (`accepted_bytes > 0`), transition to `SendState::MayHaveSent` and report `SendState::Flushed` only
  after all application bytes and pending TLS ciphertexts are written to the socket; any post-first-byte failure
  reports `SendState::Failed` while preserving `accepted_bytes`.
* **Local source-IP binding (`connect_from`)**: Reject IPv4/IPv6 family mismatches between `address` and `local_ip`
  with `Error::Bounds` before opening a socket, bind `SocketAddr::new(ip, 0)` on a nonblocking close-on-exec TCP
  socket (`sys_tcp`) wrapped immediately in `OwnedFd` -> `mio::net::TcpStream`, enable `TCP_NODELAY`, and preserve
  `local_ip()` and `peer_addr_target()`.
* **TLS 0-RTT prohibition and bounded buffers**: Reject `ClientConfig` instances with `enable_early_data == true`
  (`Error::Protocol("early data disabled")`), enforce `HARD_LIMIT` (16 MiB) across `Limits`, and honor `IoBudget`
  (`max_syscalls`, `max_bytes`) on every `drive` step so one peer cannot starve the reactor.
* **Single-hand `DialBaton` and `IpPool` discipline**: In [src/dialer.rs](src/dialer.rs), preserve disjoint ownership
  across `DialState::{Idle, Wanted, Dialing, Ready, Stopped}` (`Stopped` is terminal), resolve DNS only on
  `"tw-dialer"`, deregister sockets from `"tw-dialer"`'s `mio::Poll` before `deliver_ready`, and rank `IpPool` spares
  (`in_use == 0`) by `max(avg_rtt_us, last_rtt_us)` (`1/30` integer EWMA) before busy peers.
* **Reply-slot reservation and send-time signing**: In [src/rest_querier.rs](src/rest_querier.rs), keep producer
  `enqueue` zero-allocation, pull tasks only while
  `waiting.len() + usize::from(inflight.is_some()) + reply_queue.len() < capacity`, invoke `RestRequestBuilder` at
  actual send time, and count only `accepted_bytes > 0` attempts toward `query_count`.
* **Strict protocol framing**: Keep HTTP response/chunk parsing in [src/http.rs](src/http.rs), RFC 6455
  masking/control-frame validation in [src/ws.rs](src/ws.rs), and FIX checksum/body-length framing in
  [src/fix.rs](src/fix.rs) bounded against `max_receive_bytes`.
* **Epoch-fenced integer EWMA order routing**: In [src/order_router.rs](src/order_router.rs), forbid `unsafe` and
  floating-point arithmetic, keep `pick`, `acquire_inflight`, `release_inflight`, `note_sample`, and `on_disconnect`
  allocation-free over fixed `MAX_ORDER_CONNECTIONS` (`16`) slots, never multiply `ema_us` by `inflight`, enforce
  `9/8` hysteresis (`last_score * 8 <= best_score * 9`), fence stale samples/releases with monotonic `epoch` tokens,
  and release externally owned `IpPool` peer leases at most once per installed link.
* **Offline in-process loopback TLS 1.3 warmer (`warmup`)**: In [src/warmup.rs](src/warmup.rs) (`#![forbid(unsafe_code)]`),
  `WarmupConfig` (`warmup_interval_ns == 0` disables warming) and `WarmupTracker` gate idle-window cache warming without
  syscalls, while `TlsLoopbackWarmer` pairs an isolated, purely in-memory TLS 1.3 `rustls::ClientConnection` and
  `rustls::ServerConnection` over static embedded DER credentials (`const &[u8]`) and `[u8; 2048]` scratch buffers;
  never write synthetic plaintext into any live `Connection` or `rustls::ClientConnection` (which would advance live
  AEAD record nonces and cause `bad_record_mac` on the next real order).

## Verification and Specifications

* **Source entrypoints**: [src/lib.rs](src/lib.rs) · [src/connection.rs](src/connection.rs) ·
  [src/order_router.rs](src/order_router.rs) · [src/warmup.rs](src/warmup.rs) · [src/dialer.rs](src/dialer.rs) ·
  [src/rest_querier.rs](src/rest_querier.rs) · [src/http.rs](src/http.rs) ·
  [src/ws.rs](src/ws.rs) · [src/fix.rs](src/fix.rs)
* **Integration tests**: [tests/framing.rs](tests/framing.rs) · [tests/network.rs](tests/network.rs) ·
  [tests/dialer_and_querier.rs](tests/dialer_and_querier.rs) ·
  [tests/order_router_pool.rs](tests/order_router_pool.rs) · [tests/warmup.rs](tests/warmup.rs)
* **Specifications and models**: [transport.md](../../specs/implementation/transport.md) ·
  [transport](../../tla/execution_models/transport/ROADMAP.md) ·
  [gateway](../../tla/execution_models/gateway/ROADMAP.md)

```sh
cargo test --locked -p tickwire-transport --all-targets
./scripts/check-tla.sh transport
```
