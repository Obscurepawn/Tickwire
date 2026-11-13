# Binance Public Depth Session

`PublicDepthClient` drives one nonblocking TLS WebSocket depth stream alongside one nonblocking HTTPS `/depth` snapshot connection for a single Binance `Market` (`Spot` or `Usdm`), `Environment` (`Production` or `Test`), and catalog instrument. Using caller-supplied `mio` readiness notifications, monotonic clock samples, and `PublicPermit` rate-limit gates, it coordinates handshake, subscription, snapshot bridging via [DepthSync](binance-depth-sync.md), borrowed `BookView` publication, ping/pong servicing, bounded backoff, and proactive stream rotation before Binance's 24-hour connection limit expires.

Built with `#![forbid(unsafe_code)]`, `PublicDepthClient` owns the two `tickwire-transport::Connection` instances (`Lane::Stream` and `Lane::Snapshot`) and the embedded `DepthSync` state machine. Downstream topic publishers consume transient `Notice` callbacks under their own single-writer shared-memory discipline, while Bybit and OKX top-of-book streams use a separate snapshot-only runner ([spot-quote-input.md](spot-quote-input.md)).

## Session Model, Encodings, and Bounds

`PublicDepthClient` supports JSON depth across all four `(Spot | Usdm) x (Production | Test)` combinations (`SubscribeMode::RawUrl` or `SubscribeMode::Explicit`; speeds `Default`, `Ms100`, and `Ms500`), plus `PublicDepthClient::new_sbe` for Spot production binary incremental depth ([binance-sbe.md](binance-sbe.md)), where `SbeAccess` validates a 1–512 byte alphanumeric Ed25519 API key sent only on the WebSocket `Upgrade` and never logged or sent to the REST snapshot host.

Each poll runs within an explicit `PollBudget` (defaulting to `IoBudget::default()`, `32` WebSocket messages, `64` decoder turns, and `64` sink notifications) and enforces configured `Limits` (`request_bytes`, `message_bytes`, `http_bytes`, `http_headers`, `control_frames`, `timestamp_segments`, and `total_buffer_bytes`) and `Timing` (`connect_ns`, `request_ns`, `first_event_ns`, `heartbeat_ns`, `pong_ns`, `rotate_ns`, `backoff_ns`, and `max_backoff_ns`). `PollReport` returns consumed syscalls, network bytes, decoded/applied/ignored counts, `runnable` status, `needs_registration`, and the earliest `next_deadline_ns`—ensuring a quiet socket still wakes on time for a snapshot retry or request deadline even when its next heartbeat lies further in the future.

## Core Invariants

* **Final-byte receive timestamping and causal drain order**: Every decrypted byte segment carries the monotonic timestamp at which it was read from the socket, and a multi-segment WebSocket frame inherits the timestamp of the segment containing its *final* byte. Whenever buffered WebSocket bytes remain after a decoder turn yields (as opposed to `NeedInput`), the client drains those buffered frames *before* advancing the `DepthSync` clock for any snapshot request, HTTP response, failure, or timeout, temporarily suspending fresh socket reads while draining so a high-rate diff stream cannot starve snapshot installation.
* **First-byte quota and deadline gate**: Before the first plaintext byte of any outbound `Connect`, `Upgrade`, `Subscribe`, `Pong`, `Close`, or `Snapshot` action enters TLS, the transport callback requests `PublicPermit::allow(&PublicCharge, now)`, resamples local monotonic time, and verifies both operation and connection deadlines. Zero-byte `WouldBlock` retries repeat these checks idempotently using the same `(owner_id, domain, source, lane, connection_epoch, operation, kind)` identity, whereas partially accepted frames resume from their saved byte offset.
* **Immediate invalidation before close or reconnect**: Observing a peer `Close` frame or disconnect immediately invalidates the book before entering the bounded `Closing` phase (which finishes any already-started outbound frame and echoes the close payload) or advancing to the next `connection_epoch` and `book_generation`.

## Transitions and Failure Semantics

Once the stream enters `Phase::Receiving`, subscription confirmation and the first depth delta may arrive in either order; as soon as both are recorded, the client opens or reuses `Lane::Snapshot` to fetch REST depth and bridge buffered deltas. Each synchronization step emits `Notice::Sync`—lending the raw JSON or SBE delta slice (`raw_delta`), `DepthEncoding`, `SyncOutcome`, and, once `Live`, a borrowed `BookView` valid strictly for the duration of `PublicSink::on_notice`. Limited snapshots prove prices only within their covered boundary. HTTP responses additionally report status and headers via `PublicPermit::observe_http` and `Notice::Http`.

* **Same-socket sequence gap**: Emits `Notice::Gap`, advances `book_generation`, and starts a fresh snapshot attempt on the existing WebSocket subscription without reconnecting.
* **Disconnect, protocol error, or rotation deadline**: Invalidates the book, emits `Notice::Gap` if previously active, enters `Backoff` (up to `max_backoff_ns`), and reconnects under a strictly higher `connection_epoch` and `book_generation`.
* **Fatal terminal conditions**: Returning `false` from `PublicSink::on_notice`, overflowing epoch/generation/operation counters (`Error::Exhausted`), or observing clock regression or a changed `ClockDomain` (`Error::Clock`) transitions the client permanently to `Phase::Stopped`.

## Verification and References

```sh
cargo test --locked -p tickwire-binance-public
```

Local TCP/TLS integration suites verify all four Spot/USD-M product-environment routes, same-socket snapshot rebuilds after injected sequence gaps, disconnect/reconnect epoch transitions, WebSocket `Close` echoing, causal buffered-frame timestamp ordering, and exact `next_deadline_ns` wake scheduling on quiet sockets.

* [Public depth and event session crate root](../../crates/tickwire-binance-public/src/lib.rs)
* [Depth synchronization rules](binance-depth-sync.md)
* [Binance Spot SBE specification](binance-sbe.md)
* [Bybit and OKX public spot BBO specification](spot-quote-input.md)
* [TCP/TLS product and environment tests](../../crates/tickwire-binance-public/tests/network.rs)
* [Poll-budget and retry wake-scheduling tests](../../crates/tickwire-binance-public/tests/scheduling.rs)
* [Same-socket gap rebuild and reconnect tests](../../crates/tickwire-binance-public/tests/rebuild.rs)
