# Nonblocking Transport, First-Byte Gate, Wire Framing, Dialer, and REST Querier

`tickwire-transport::Connection` provides a bounded, explicitly driven nonblocking TCP and TLS stream built on `mio` `1.2.4` (`TCP_NODELAY` enabled) and `rustls` `0.23.46`. Callers register socket `Interest` with their own reactor, supply monotonic `Instant` timestamps, and drive each poll with an explicit `IoBudget`. Alongside `Connection` and the standalone HTTP/1.1, WebSocket, and text FIX codecs (also reused by the blocking Bybit/OKX public quote reader in [spot-quote-input.md](spot-quote-input.md)), the crate provides two off-hot-path worker subsystems so latency-critical `REQ`/`RESP` loops never block on DNS resolution, connection establishment, or reconciliation HTTP queries:

* **`dialer` (`IpPool`, `DialBaton`, `Dialer`)**: One background `"tw-dialer"` OS thread owns DNS resolution (`ToSocketAddrs`), candidate peer RTT ranking (`IpPool`), source-IP-bound TCP/TLS handshakes (`Connection::connect_from`), and optional WebSocket HTTP `101` upgrades (`ws::ClientHandshake`), handing fully established `DialedLink` instances back to single-threaded session owners via a single-slot atomic `DialBaton`.
* **`rest_querier` (`AsyncRestQuerier`)**: One background `"tw-rest-query"` OS thread drives a persistent keep-alive HTTP/1.1 `Connection` fed by preallocated fixed-capacity task and reply rings, building signed requests at actual send time (`RestRequestBuilder`) and enforcing a reply-slot reservation invariant so completed replies never drop or block.

`Connection` owns a single in-flight outbound `SendSlot` (identified by a connection-local monotonic `u64` send ID), a bounded plaintext receive ring (`VecDeque<u8>`), optional local source IP metadata (`local_ip`), the target peer address (`peer_addr_target`), and TLS session state. Venue error classification, WebSocket heartbeat/close orchestration, FIX logon/sequence state, and durable cross-connection order identity belong to higher-level session and gateway owners.

## Limits, Budgets, Source Binding, and Protocol Codecs

Every `Limits` field (`max_send_bytes`, `max_receive_bytes`, `tls_buffer_bytes`, `max_accept_bytes`) and `PreparedSend` payload must be nonzero and `<= 16 MiB` (`HARD_LIMIT`; defaults: `1 MiB` send/receive, `16 KiB` TLS buffer and per-call accept chunk). `IoBudget` defaults to `8` syscalls and `65,536` network bytes per `drive` call.

| Boundary | Enforcement Rules |
| --- | --- |
| **TCP / TLS (`Connection`)** | `connect_from(address, local_ip, ...)` validates IPv4/IPv6 family agreement when `local_ip` is `Some(ip)` (`Error::Bounds` on mismatch), binds `SocketAddr::new(ip, 0)` via the audited `sys_tcp` nonblocking `SOCK_CLOEXEC` boundary (`OwnedFd` -> `mio::net::TcpStream`) before `connect(address)`, and enables `TCP_NODELAY`. Caps syscalls, socket bytes, and decrypted plaintext bytes per `drive`; stops reading once `rx.len() == max_receive_bytes`; rejects `enable_early_data` (`0-RTT`) TLS configs with `Error::Protocol("early data disabled")`. |
| **HTTP/1.1 (`http`)** | Rejects CRLF header injection, duplicate `Content-Length`, ambiguous `Transfer-Encoding` + `Content-Length`, oversized chunk headers/bodies, and premature truncation. `next_head_response` decodes `HEAD` / `CONNECT` replies without allocating a body buffer for `Content-Length`. |
| **WebSocket (`ws`)** | Validates HTTP `101` `Upgrade` and `Sec-WebSocket-Accept`, client frame masking, server `RSV` flags, minimal length encodings, control-frame rules (`<= 125` bytes, unfragmented), message fragmentation, and UTF-8 text validity—distinguishing complete messages, `NeedInput`, and per-poll turn exhaustion. |
| **Text FIX (`fix`)** | Locates trailer `10=` (`CheckSum`) strictly via `9=` (`BodyLength`), verifies modulo-256 checksum equality, and rejects truncated, malformed, or oversized frames. |
| **Off-Hot-Path Dialer (`dialer`)** | Resolves DNS exclusively on `"tw-dialer"`; bounds peer tables to `max_peers` (default `32`); bounds TCP+TLS connect by `connect_timeout` and full TCP+TLS+WS `101` dial by `dial_timeout`; deregisters sockets from `"tw-dialer"`'s `mio::Poll` before `DialBaton` handoff while leaving any coalesced post-`101` WebSocket frames untouched in `Connection`'s receive buffer. |
| **Async REST Querier (`rest_querier`)** | Preallocates fixed-capacity task and reply rings (`capacity`); producer `enqueue` is nonblocking and zero-allocation; worker pulls new tasks only while `waiting.len() + usize::from(inflight.is_some()) + reply_queue.len() < capacity`. |

## Core Invariants

* **Zero plaintext exposure on `queue`**: `Connection::queue(prepared)` stages one immutable `PreparedSend` in `SendState::Queued` (`accepted_bytes = 0`), rejecting concurrent sends with `Error::Busy`. While TCP connect and TLS handshake packets may already traverse the network via `drive`, zero application bytes are passed to `rustls` or the socket during `queue`.
* **Synchronous gate and post-gate deadline recheck (`try_send`)**: `try_send(now, authorize)` first checks `now.max(Instant::now()) >= deadline`. While `accepted_bytes == 0` and `is_ready()` holds, it invokes `authorize()` synchronously **immediately before** writing plaintext to `rustls::Writer` or the raw socket, then re-checks `now.max(Instant::now()) >= deadline` *after* `authorize()` returns—aborting with `NoBytes(Failure::GateDenied)` or `NoBytes(Failure::Deadline)` if denied or expired during the callback.
* **Exact `NoBytes` vs `MayHaveSent` boundary**: If `write` accepts `0` bytes or returns `WouldBlock` / `Interrupted`, `accepted_bytes` stays `0` and the next poll must pass `authorize()` again. The first positive write (`n > 0`, capped per call by `max_accept_bytes`) transitions state irreversibly to `SendState::MayHaveSent`, and `SendState::NoBytes(why)` is reachable *if and only if* `accepted_bytes == 0`.
* **Flush precedence over same-poll EOF**: `SendState::Flushed` requires `accepted_bytes == total_bytes` and `!tls.wants_write()`. Inside `drive`, `mark_flushed()` executes immediately after `tls.write_tls()` and *before* `tls.read_tls()`—ensuring a completed local flush is recorded and preserved even if peer EOF arrives later in that very same poll (`Flushed` proves local socket drain only, never remote venue acknowledgement).
* **Single-hand `DialBaton` ownership and `IpPool` ranking**: `DialBaton` enforces disjoint ownership across `DialState::{Idle = 0, Wanted = 1, Dialing = 2, Ready = 3, Stopped = 4}`:
  * Only the session owner transitions `Idle -> Wanted` (`want(due_mono_ns)`) and `Ready -> Idle` (`take_ready()`, resetting `failures = 0`).
  * Only `"tw-dialer"` transitions `Wanted -> Dialing` (`claim(now_mono_ns)` when `now_mono_ns >= due_mono_ns`), `Dialing -> Ready` (`deliver_ready(link)`), and `Dialing -> Wanted` (`fail(next_due_mono_ns)`, incrementing `failures` with backoff `min(max_ms, initial_ms * 2^min(failures, 16))` ms). `Stopped` is terminal; concurrent `reset()` or `stop()` during `Dialing` causes a late `deliver_ready` to drop the unneeded socket and release its `IpPool` lease.
  * `IpPool` tracks `{ addr, in_use, avg_rtt_us, last_rtt_us, last_failed }`, updating `avg_rtt_us` on `note_syn(addr, Some(rtt))` with integer EWMA `(old * 29 + rtt) / 30` (first sample `= rtt`) and evicting failed peers (`note_syn(addr, None)`) when `in_use == 0` and at least one other peer remains. `take_ip` prefers spare peers (`in_use == 0`) ordered by `max(avg_rtt_us, last_rtt_us)` with unprobed and `last_failed` entries last, falling back to minimum `in_use`. `replace_with_unused(current)` atomically swaps `current` for the best spare peer (`addr != current && in_use == 0`), leaving `current`'s lease intact when no spare exists.
* **`AsyncRestQuerier` reply-slot reservation and `query_count` accounting**:
  * `"tw-rest-query"` dequeues from `task_queue` only while `waiting.len() + usize::from(inflight.is_some()) + reply_queue.len() < capacity`. Every task admitted into `waiting` or `inflight` holds an implicit reservation in `reply_queue`, guaranteeing that completion can always push its `RestReply` without blocking or dropping. Draining replies wakes `"tw-rest-query"` whenever backpressured tasks remain in `task_queue`.
  * `RestRequestBuilder::build_request` runs at actual send time so request timestamps and HMAC/Ed25519 signatures are freshly computed on every retry; `Ok(None)` or encode failure completes immediately with `RestStatus::NotBuilt` without perturbing the keep-alive socket.
  * `query_count` increments if and only if `accepted_bytes > 0` is written for an attempt. Retries occur on HTTP `5xx`, `SendFail`, `ReadFail`, and `Timeout` with task backoff `min(timeout_ms, interval_ms * 2^(query_count - 1))` (`query_count.saturating_sub(1)` when `query_count == 0`) while `(max_query_count == 0 || query_count < max_query_count)` and `(end_mono_ns == 0 || next_send_ns < end_mono_ns)`. Keep-alive disconnects/errors reconnect with `min(timeout_ms, interval_ms * 2^connect_fails)` backoff. Any task whose `end_mono_ns` passes before a single byte is sent completes with `RestStatus::Expired` and `query_count == 0`.

## Transitions and Failure Semantics

While `accepted_bytes == 0`, `GateDenied`, pre-write `Deadline`, or explicit `cancel()` records `SendState::NoBytes(why)` without tearing down an otherwise healthy connection (though pre-ready `connect_deadline` expiry shuts down the socket). Once `accepted_bytes > 0`, any `cancel()`, `Deadline`, socket I/O error, TLS alert, or peer disconnect transitions the send to `SendState::Failed(why)`, preserves the exact `accepted_bytes` count for upstream uncertainty handling, and immediately shuts down both TLS and the TCP socket (`Shutdown::Both`) so a truncated frame can never corrupt the stream. During `drive`, reaching `max_syscalls`, `max_bytes`, or `max_receive_bytes` pauses socket/TLS reads cleanly until the caller drains `Connection::read` and polls again.

## Verification and References

```sh
cargo test --locked -p tickwire-transport --all-targets
./scripts/check-tla.sh
```

Integration tests exercise real loopback TCP/TLS backpressure, local source-IP binding and IPv4/IPv6 mismatch rejection, tiny `max_accept_bytes` partial writes, certificate hostname mismatches, `0-RTT` rejection, post-callback deadline expiration, zero-byte vs partial-write cancellation, HTTP chunked transfer encoding, fragmented WebSocket streams, `DialBaton` concurrent races, `IpPool` EWMA ranking and spare failover, `Dialer` TLS+WebSocket `101` upgrades with reconnect backoff, and `AsyncRestQuerier` HTTP/1.1 keep-alive reuse, `503 -> 200` retry recovery, `max_query_count` capping, `end_mono_ns` expiration, and full-ring backpressure. In addition, the finite `L0_first_output` TLA+ model exhaustively checks `Queued` / `MayHaveSent` / `Flushed` / `NoBytes` / `Failed` acceptance and deadline transitions (abstracting over TLS cryptography and NIC wire transit).

* [Transport crate types, `Limits`, `IoBudget`, and `SendState`](../../crates/tickwire-transport/src/lib.rs)
* [Nonblocking `Connection`, `connect_from`, and first-byte gate](../../crates/tickwire-transport/src/connection.rs)
* [Off-hot-path `Dialer`, `DialBaton`, and `IpPool`](../../crates/tickwire-transport/src/dialer.rs)
* [Off-hot-path `AsyncRestQuerier` and reply-slot reservation](../../crates/tickwire-transport/src/rest_querier.rs)
* [HTTP/1.1 request encoder and response decoder](../../crates/tickwire-transport/src/http.rs)
* [WebSocket handshake, frame encoder, and streaming decoder](../../crates/tickwire-transport/src/ws.rs)
* [Text FIX frame validator](../../crates/tickwire-transport/src/fix.rs)
* [TCP/TLS backpressure, gate, and cancellation tests](../../crates/tickwire-transport/tests/network.rs)
* [Dialer, baton, IP pool, and async REST querier tests](../../crates/tickwire-transport/tests/dialer_and_querier.rs)
* [HTTP, WebSocket, and FIX framing tests](../../crates/tickwire-transport/tests/framing.rs)
* [Finite TLA+ first-output model results (`L0_first_output`)](../../tla/execution_models/transport/L0_first_output/05_output.md)
* [Public spot top-of-book input specification](spot-quote-input.md)
