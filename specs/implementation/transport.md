# Nonblocking Transport, First-Byte Gate, and Wire Framing

`tickwire-transport::Connection` provides a bounded, explicitly driven nonblocking TCP and TLS stream built on `mio` `1.2.4` (`TCP_NODELAY` enabled) and `rustls` `0.23.46`. Callers resolve DNS addresses up front during cold setup, register socket `Interest` with their own reactor, supply monotonic `Instant` timestamps, and drive each poll with an explicit `IoBudget`. Alongside `Connection`, the crate provides standalone, allocation-bounded HTTP/1.1, WebSocket, and text FIX framing codecs—reused both by nonblocking Binance session owners and by the dedicated blocking Bybit/OKX public quote reader ([spot-quote-input.md](spot-quote-input.md)).

`Connection` owns a single in-flight outbound `SendSlot` (identified by a connection-local monotonic `u64` send ID), a bounded plaintext receive ring (`VecDeque<u8>`), and the TLS session state. Venue error classification, reconnect backoff, WebSocket heartbeat/close orchestration, FIX logon/sequence state, and durable cross-connection order identity belong to higher-level session and gateway owners.

## Limits, Budgets, and Protocol Codecs

Every `Limits` field (`max_send_bytes`, `max_receive_bytes`, `tls_buffer_bytes`, `max_accept_bytes`) and `PreparedSend` payload must be nonzero and `<= 16 MiB` (`HARD_LIMIT`; defaults: `1 MiB` send/receive, `16 KiB` TLS buffer and per-call accept chunk). `IoBudget` defaults to `8` syscalls and `65,536` network bytes per `drive` call.

| Boundary | Enforcement Rules |
| --- | --- |
| **TCP / TLS (`Connection`)** | Caps syscalls, socket bytes, and decrypted plaintext bytes per `drive`; stops reading from socket or `rustls` once `rx.len() == max_receive_bytes`. Verifies X.509 roots and `ServerName`; rejects `enable_early_data` (`0-RTT`) TLS configs with `Error::Protocol("early data disabled")`. |
| **HTTP/1.1 (`http`)** | Rejects CRLF header injection, duplicate `Content-Length`, ambiguous `Transfer-Encoding` + `Content-Length`, oversized chunk headers/bodies, and premature truncation. `next_head_response` decodes `HEAD` / `CONNECT` replies without allocating a body buffer for `Content-Length`. |
| **WebSocket (`ws`)** | Validates HTTP `101` `Upgrade` and `Sec-WebSocket-Accept`, client frame masking, server `RSV` flags, minimal length encodings, control-frame rules (`<= 125` bytes, unfragmented), message fragmentation, and UTF-8 text validity—distinguishing complete messages, `NeedInput`, and per-poll turn exhaustion. |
| **Text FIX (`fix`)** | Locates trailer `10=` (`CheckSum`) strictly via `9=` (`BodyLength`), verifies modulo-256 checksum equality, and rejects truncated, malformed, or oversized frames. |

## Core Invariants

* **Zero plaintext exposure on `queue`**: `Connection::queue(prepared)` stages one immutable `PreparedSend` in `SendState::Queued` (`accepted_bytes = 0`), rejecting concurrent sends with `Error::Busy`. While TCP connect and TLS handshake packets may already traverse the network via `drive`, zero application bytes are passed to `rustls` or the socket during `queue`.
* **Synchronous gate and post-gate deadline recheck (`try_send`)**: `try_send(now, authorize)` first checks `now.max(Instant::now()) >= deadline`. While `accepted_bytes == 0` and `is_ready()` holds, it invokes `authorize()` synchronously **immediately before** writing plaintext to `rustls::Writer` or the raw socket, then re-checks `now.max(Instant::now()) >= deadline` *after* `authorize()` returns—aborting with `NoBytes(Failure::GateDenied)` or `NoBytes(Failure::Deadline)` if denied or expired during the callback.
* **Exact `NoBytes` vs `MayHaveSent` boundary**: If `write` accepts `0` bytes or returns `WouldBlock` / `Interrupted`, `accepted_bytes` stays `0` and the next poll must pass `authorize()` again. The first positive write (`n > 0`, capped per call by `max_accept_bytes`) transitions state irreversibly to `SendState::MayHaveSent`, and `SendState::NoBytes(why)` is reachable *if and only if* `accepted_bytes == 0`.
* **Flush precedence over same-poll EOF**: `SendState::Flushed` requires `accepted_bytes == total_bytes` and `!tls.wants_write()`. Inside `drive`, `mark_flushed()` executes immediately after `tls.write_tls()` and *before* `tls.read_tls()`—ensuring a completed local flush is recorded and preserved even if peer EOF arrives later in that very same poll (`Flushed` proves local socket drain only, never remote venue acknowledgement).

## Transitions and Failure Semantics

While `accepted_bytes == 0`, `GateDenied`, pre-write `Deadline`, or explicit `cancel()` records `SendState::NoBytes(why)` without tearing down an otherwise healthy connection (though pre-ready `connect_deadline` expiry shuts down the socket). Once `accepted_bytes > 0`, any `cancel()`, `Deadline`, socket I/O error, TLS alert, or peer disconnect transitions the send to `SendState::Failed(why)`, preserves the exact `accepted_bytes` count for upstream uncertainty handling, and immediately shuts down both TLS and the TCP socket (`Shutdown::Both`) so a truncated frame can never corrupt the stream. During `drive`, reaching `max_syscalls`, `max_bytes`, or `max_receive_bytes` pauses socket/TLS reads cleanly until the caller drains `Connection::read` and polls again.

## Verification and References

```sh
cargo test --locked -p tickwire-transport
./scripts/check-tla.sh
```

Integration tests exercise real loopback TCP/TLS backpressure, tiny `max_accept_bytes` partial writes, certificate hostname mismatches, `0-RTT` rejection, post-callback deadline expiration, zero-byte vs partial-write cancellation, HTTP chunked transfer encoding, and fragmented WebSocket streams. In addition, the finite `L0_first_output` TLA+ model exhaustively checks `Queued` / `MayHaveSent` / `Flushed` / `NoBytes` / `Failed` acceptance and deadline transitions (abstracting over TLS cryptography and NIC wire transit).

* [Transport crate types, `Limits`, `IoBudget`, and `SendState`](../../crates/tickwire-transport/src/lib.rs)
* [Nonblocking `Connection` and first-byte gate](../../crates/tickwire-transport/src/connection.rs)
* [HTTP/1.1 request encoder and response decoder](../../crates/tickwire-transport/src/http.rs)
* [WebSocket handshake, frame encoder, and streaming decoder](../../crates/tickwire-transport/src/ws.rs)
* [Text FIX frame validator](../../crates/tickwire-transport/src/fix.rs)
* [TCP/TLS backpressure, gate, and cancellation tests](../../crates/tickwire-transport/tests/network.rs)
* [HTTP, WebSocket, and FIX framing tests](../../crates/tickwire-transport/tests/framing.rs)
* [Finite TLA+ first-output model results (`L0_first_output`)](../../tla/execution_models/transport/L0_first_output/05_output.md)
* [Public spot top-of-book input specification](spot-quote-input.md)
