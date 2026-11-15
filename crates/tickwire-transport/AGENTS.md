# tickwire-transport

`tickwire-transport` (`#![forbid(unsafe_code)]`) owns explicitly driven nonblocking TCP (`mio`), TLS (`rustls` with
early data disabled), HTTP/1.1 (`http`), WebSocket (`ws`), and FIX (`fix`) codecs; vtable-free multi-protocol execution
links and `[local_ip, count]` multi-connection pools (`links`); zero-allocation single-pass JSON execution scanning
(`json_scan`); lock-free double-buffered EWMA execution routing (`order_router`); hardware `rdtsc` stage and spinlock
contention tracing (`latency_tracer`); offline TLS 1.3 cache warming (`warmup`); and `housekeeping_cpus`-pinned
background connection dialing (`dialer`) and asynchronous HTTPS reconciliation querying (`rest_querier`). Socket
device/IP binding delegates to [`tickwire-control-sys`](../tickwire-control-sys/AGENTS.md) and hardware cycle-counter
reads delegate to [`tickwire-clock-sys`](../tickwire-clock-sys/AGENTS.md).

## Invariants and Change Guidelines

* **Zero `unsafe` & audited OS delegation**: Keep `#![forbid(unsafe_code)]` at crate root. Route `SO_BINDTODEVICE`
  (`nic` taking precedence over `local_ip`) and nonblocking `bind` + `connect` through
  `tickwire_control_sys::open_bound_nonblocking_tcp_stream`, CPU pinning through
  `tickwire_control_sys::spawn_pinned_thread`, and TSC reads through `tickwire_clock_sys::rdtsc`.
* **First-application-byte gate & partial/short-write self-retirement**: Preserve `Connection::try_send` synchronous
  gate and `MayHaveSent` / `Flushed` invariants in [src/connection.rs](src/connection.rs). On
  `Connection::send_frame_nonblocking` / `LockedConnection::send_frame`, any TCP partial write (`0 < n < len`), EOF, or
  TLS `write_tls` `WouldBlock`/EOF/error while `tls.wants_write()` remains `true` immediately retires and shuts down the
  socket, sets `close_pending`, and triggers `mio::Waker::wake()` (`eventfd`) outside the spinlock.
* **Fine-grained spinlock purity & lock hierarchy**: In [src/links.rs](src/links.rs) and
  [src/latency_tracer.rs](src/latency_tracer.rs), `TimedSpinLock`, `LockedConnection`, and `GuardedValue` spin strictly
  in userspace via `try_lock()`, never overlap with any `Order` or `StpBook` spinlock, and perform **zero logging, zero
  heap allocation, and zero protocol parsing** while held.
* **Zero-allocation single-pass `JsonScan` / `MsgParser`**: In [src/json_scan.rs](src/json_scan.rs), scan venue frames
  in one bounded pass without `Vec`, `String`, `Box`, `serde_json::Value`, or floating-point math, extracting synonymous
  Binance/OKX/Bybit IDs, checked integer ticks/lots, fees, `rateLimits[]`, and error codes/messages as borrowed slices.
* **Vtable-free `LinkOps` & multi-connection pools**: Keep `LinkOps` (`WsLink`, `HttpsLink`, `FixLink`) free of `dyn`
  dispatch; preserve `WsLink` `16`-slot `reply_id` correlation, `loginChallenge` auth, and FIFO `early_frames` replay;
  preserve `FixLink` `Logon`/`Heartbeat`/`TestRequest`/`ResendRequest` and `64`-slot `RefSeqNum` `Reject` hints; and
  allow `UrlConnections` to open multi-socket WS, HTTPS, or FIX pools per `[local_ip, count]` and `product_mask` with
  zero mutual exclusion.
* **Integer EWMA routing & lock-free `PublishedRoutes`**: In [src/order_router.rs](src/order_router.rs), use integer
  math only, honor `SampleMask` (`IN_BAND` default, `ALL` enabling `SampleKind::Tcping`), enforce `9/8` hysteresis
  (`last_score * 8 <= best_score * 9`), and publish `tickwire_venue::OrderRoute` snapshots via double-buffered
  Acquire/Release `PublishedRoutes`.
* **`housekeeping_cpus` worker pinning**: Spawn `"tw-dialer"` ([src/dialer.rs](src/dialer.rs)), `"tw-rest-query"`
  ([src/rest_querier.rs](src/rest_querier.rs)), and `"tw-latency-sink"`
  ([src/latency_tracer.rs](src/latency_tracer.rs)) via `spawn_pinned_thread(..., housekeeping_cpus, ...)` **before**
  creating `mio::Poll`, resolving DNS, performing TCP/TLS handshakes, or allocating worker buffers.
* **Live TLS nonce preservation (`warmup`)**: Never write synthetic plaintext into a live `Connection`; keep
  [src/warmup.rs](src/warmup.rs) isolated to its in-memory loopback TLS 1.3 pair.

## Verification and Specifications

* **Source entrypoints**: [src/lib.rs](src/lib.rs) · [src/connection.rs](src/connection.rs) ·
  [src/json_scan.rs](src/json_scan.rs) · [src/links.rs](src/links.rs) ·
  [src/order_router.rs](src/order_router.rs) · [src/latency_tracer.rs](src/latency_tracer.rs) ·
  [src/warmup.rs](src/warmup.rs) · [src/dialer.rs](src/dialer.rs) ·
  [src/rest_querier.rs](src/rest_querier.rs) · [src/http.rs](src/http.rs) ·
  [src/ws.rs](src/ws.rs) · [src/fix.rs](src/fix.rs)
* **Integration tests**: [tests/framing.rs](tests/framing.rs) · [tests/network.rs](tests/network.rs) ·
  [tests/dialer_and_querier.rs](tests/dialer_and_querier.rs) ·
  [tests/order_router_pool.rs](tests/order_router_pool.rs) · [tests/warmup.rs](tests/warmup.rs) ·
  [tests/housekeeping_affinity.rs](tests/housekeeping_affinity.rs) ·
  [tests/stage1_transport.rs](tests/stage1_transport.rs)
* **Specifications and models**: [transport.md](../../specs/implementation/transport.md) ·
  [transport](../../tla/execution_models/transport/ROADMAP.md) ·
  [gateway](../../tla/execution_models/gateway/ROADMAP.md) ·
  [hotpath_ultralow_latency](../../tla/execution_models/hotpath_ultralow_latency/ROADMAP.md)

```sh
cargo test --locked -p tickwire-transport --all-targets
cargo clippy --locked -p tickwire-transport --all-targets -- -D warnings
./scripts/check-tla.sh transport
```
