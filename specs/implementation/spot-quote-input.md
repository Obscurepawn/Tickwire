# Public Spot Top-of-Book Input

Public spot ingestion acquires unauthenticated level-one replacement snapshots from Bybit (`stream.bybit.com:443/v5/public/spot` on `orderbook.1.SYMBOL`) and OKX (`ws.okx.com:443/ws/v5/public` on `bbo-tbt`) and normalizes them into Tickwire's exact shared-memory best-bid/offer stream. Each network connection subscribes to a single instrument, verifies the upstream TLS hostname—including across an optional HTTP `CONNECT` proxy tunnel—and rejects all market frames until the venue returns a matching subscription acknowledgement for the expected request ID, channel, and symbol.

A dedicated cold housekeeping reader thread (`"md-cold"`, spawned via `tickwire_control_sys::spawn_pinned_thread` onto optional `housekeeping_cpus` before pinning the topic reactor thread onto optional `reactor_cpu`) owns the TLS socket, WebSocket framing, and JSON parsing, while a single-threaded topic owner (`TopicOwner`) owns shared-memory ring publication, JOIN snapshot state, and reader admissions. Neither layer holds trading credentials, maintains incremental depth levels, or authorizes order risk; downstream strategy workers (`tickwire-strategy-sdk`) consume the published shared-memory ring through independent authenticated cursors, enforce quote freshness, and stamp `IntentBody::tick_mono_ns` for microsecond stale-order gating inside the stateless ORS execution process (`tickwire-runtime`, which holds zero SHM quote subscriptions).

## State Model and Bounds

The source runs in two operational modes within `tickwire-md`:

* **Diagnostic inspection (`tickwire-md --spot-quotes FILE`)**: Accepts explicit `QuoteConfig` parameters without requiring a signed catalog artifact and emits newline-delimited JSON `Notice` records (`quote` or `unavailable`). Each `Quote` carries `venue_id`, `symbol`, `instrument_id`, `instrument_version`, `connection_epoch`, `venue_sequence`, optional Bybit `update_id`, `exchange_event_ns`, process-relative `received_mono_ns`, and optional `bid`/`ask` pairs in integral ticks and lots.
* **Authenticated topic publication (`--spot-market`)**: Requires an approved `Production` `Spot` catalog matching the exact numeric venue, cash product, symbol, instrument ID/version, catalog version, and price/quantity increments. Reopening an active topic control file requires strictly greater `ring_epoch` and `feed_generation` values than the retired incarnation.

| Boundary | Limit or Encoding | Purpose |
| --- | --- | --- |
| Symbol format | 1–64 bytes (`A-Z`, `0-9`, `-`) | Prevents ambiguous or malformed venue channel subscriptions. |
| WebSocket payload | 16 KiB (`MAX_MESSAGE`) | Bounds single-frame memory residency and borrowed JSON parsing. |
| Levels per side | 0 or 1 (`Side<T>`) | Rejects multi-level depth frames without allocating a `Vec`. |
| Connection timeouts | 5 s TCP connect, 15 s TLS/setup, 15 s ping, 35 s idle | Bounds undetected silent drops on the blocking reader thread. |
| Handoff queue | `sync_channel(1)` (`Publication`) | Bounds in-memory queuing between socket reader and topic owner. |
| IPC exact BBO | Message type `265` (512 bytes) | Carries two-sided uncrossed `bid`/`ask` ticks, lots, and cross-sequence. |
| IPC feed state | Message type `261` (512 bytes, `0x42424f31` / `BBO1`) | Publishes aggregate liveness (`2` = live, `3` = stale/unavailable). |
| Topic limits | 1 level/side, 1 MiB cache, 1 MiB snapshot, 4 KiB max delta | Fixed shared-memory footprint for single-instrument slot `0`. |

Connection epochs start at `1` per process and increase monotonically across reconnects. Within an active connection, Tickwire applies replacement snapshots in WebSocket delivery order. Because Bybit can resend an unchanged L1 snapshot after roughly three seconds with the same update ID (and resets `u = 1` across service restarts), and OKX can resend an unchanged snapshot after roughly a minute while reserving predecessor-sequence continuity rules for incremental channels, Tickwire treats every valid snapshot as a fresh observation, retains native sequence IDs for attribution (exposing raw Bybit `u` on the diagnostic stream and cross-sequence `seq` in the shared BBO record), and orders downstream consumption strictly by shared-memory ring sequence.

## Core Invariants

* **Two-sided uncrossed publication**: An exact IPC BBO record (`type 265`) always represents a strictly uncrossed, two-sided quote (`0 < bid_ticks < ask_ticks`, positive lots, nonzero exchange timestamp). An empty side or crossed venue snapshot never enters the ring as a live quote; instead, it immediately transitions the topic to unavailable.
* **Asymmetric gate ordering across `S` and `S+1`**: When publishing a fresh quote, the topic owner writes `type 265` at ring sequence `S` and live `FeedState` (`type 261`, `feed_state = 2`) at `S+1` with identical ingress timestamps, ensuring subscribers apply the new price before seeing a live gate. Conversely, when transitioning to unavailable, the owner writes stale `FeedState` (`type 261`, `feed_state = 3`) at `S` *before* republishing the retained `type 265` quote (`feed_state = 3`) at `S+1`, closing the aggregate gate before any reader can observe the stale quote.
* **Explicit depth non-coverage**: Because level-one replacement snapshots provide no outer book state, `MarketView::best_bid()`, `best_ask()`, and `quantity()` remain `Unknown` on `BBO1` topics. Strategies read `MarketView::best_quote()` only while `continuous()` and `QUOTE` feed state report live, measuring staleness via `state_age_ns(QUOTE, now)`.
* **Split-pair safety across SHM consumers and ORS admission**: Strategy workers (`MarketReplica::drain` / `MarketView::callback_ready_at`) and `ShmSource` (`MarketView::announce_execution_state`) withhold callbacks and readiness marks at sequence `S` and trigger only at `S+1` once both records of the pair are applied. If a poll budget stops at `S`, no strategy callback or `tick_mono_ns` intent is emitted until `S+1` is consumed, and the ORS `REQ` hot thread independently refuses any stale outbound `IntentRecord` exceeding `max_order_age_ns` (`23 µs` default, `RefusalReason::RejectStaleOrder`).

## Transitions and Failure Semantics

When a strategy joins the topic, installing the JOIN snapshot (which stores feed state and quote as member rows 1 and 2 and records the ring frontier in `last_instrument_event_seq`) initializes its replica without firing a callback. During live ring consumption, an active strategy receives exactly one `on_market` callback per fresh two-sided pair at `S+1`—including the first valid pair after reconnect—whereas stale pairs, future ingress timestamps, or observations exceeding the strategy's maximum age budget generate zero callbacks.

Any socket disconnect, malformed UTF-8 or JSON payload, off-grid decimal, unacknowledged snapshot, rejected subscription, extra level, or 35-second idle timeout marks the session failed, emits an unavailable transition, and reconnects under bounded backoff on a higher connection epoch. Heartbeat frames (`pong` or Bybit ping responses) keep the connection alive without refreshing quote age. When a missing side, crossed book, or disconnect closes the live gate, the publisher preserves the previous exact quote's identity and ingress timestamp while marking its `feed_state` stale (`3`). A subsequent two-sided uncrossed snapshot restores live SDK callbacks while consumers keep their cursors attached across the outage without requiring a ring restart.

Because the reader hands updates to the topic owner over a capacity-one blocking channel, a stalled topic owner eventually applies backpressure to the reader thread, leaving disconnect detection and kernel socket residence unbounded while stalled; freshness guards remain sound because ingress timestamps reflect user-space read time on the monotonic clock. Any shared-memory ring publication failure sets `needs_bank` and terminates the owner rather than continuing with partial state.

## Performance and Cost

Startup (`Session::new` and `TopicOwner` setup) pays once for catalog digest verification, PEM root loading, DNS resolution, TCP/TLS handshakes, shared-memory bank and snapshot allocation, hardware SIMD capability selection (`Backend::detect()`: 32-byte `AVX2` after `is_x86_feature_detected!("avx2")` or baseline 16-byte `SSE2` on `x86_64`, baseline 16-byte `NEON` on `aarch64`, and universal 64-bit `SWAR` + scalar fallback elsewhere), and pre-reduction of `price_tick` and `quantity_step` into integer `StepSpec` divisors.

Per acknowledged quote (`Session::on_text`), the reader thread streams 64-byte slices directly from the bounded 16 KiB WebSocket frame buffer (`0` frame copies, `0` heap allocations, `0` syscalls, `0` floating-point operations; `Quote.symbol` borrows from `QuoteConfig`), classifies quotes (`"`), RFC 8259 control bytes (`0x00..=0x1F`), structural braces/brackets (`{`, `}`, `[`, `]`), and ASCII digits (`0..=9`) into 64-bit register bitmasks, checks unescaped control bytes inside strings via `prefix_xor`, parses at most one level per side, and converts ASCII decimal strings directly into integer `i64` ticks and lots via checked `u64`/`i128` arithmetic (`parse_positive_units`) with identical divisibility, positivity, overflow, and positive-spread (`0 < bid_ticks < ask_ticks`) semantics to the reference `serde_json` + `Decimal` oracle (`Session::decode_serde_oracle`). Topic publication hands `Publication` across the `sync_channel(1)` hop and writes at most two fixed 512-byte records (`[u8; 512]` on the stack) regardless of subscriber count. TLS decryption, WebSocket control framing, and optional `--spot-quotes` JSON serialization remain on the reader thread and may allocate or block independently of shared-memory publication.

### Audited SIMD JSON Boundary (`crates/tickwire-md/src/spot_quotes/simd_json.rs`)

All SIMD `unsafe` code is isolated inside `simd_json.rs` under `#![deny(unsafe_op_in_unsafe_fn)]`, while sibling modules (`network.rs` and `protocol.rs`) enforce `#[forbid(unsafe_code)]`:

* **Bounds and alignment**: Unaligned read-only vector loads (`_mm256_loadu_si256` requiring `offset + 32 <= chunk.len()`, `_mm_loadu_si128` and `vld1q_u8` requiring `offset + 16 <= chunk.len()`, all with 1-byte alignment over initialized shared `&[u8]` slices) never dereference aligned Rust references; `< 16`-byte tails use safe 8-byte SWAR (`u64::from_le_bytes`) and safe scalar indexing.
* **Capability token and UTF-8 soundness**: `Backend` is an opaque capability token ensuring `Avx2` executes only after `is_x86_feature_detected!("avx2")` succeeds. Any frame containing a backslash escape (`\`), non-ASCII byte (`>= 0x80`), deep unknown-field nesting (`depth >= 32`), or unknown-field floating-point / >19-digit numbers delegates via `FastOutcome::Fallback` to `Session::decode_serde_oracle`, avoiding `from_utf8_unchecked` while guaranteeing exact RFC 8259 equivalence across all inputs.

### Measured Native BBO Decode Latency (`spot_bbo_decode`)

```sh
taskset -c 2 cargo bench --locked -p tickwire-md --bench spot_bbo_decode
```

The benchmark starts `std::time::Instant::now()` on an acknowledged native JSON frame in memory and stops after `Session::on_text` and `Quote::publication` construct `Publication::Quote(SpotQuote)` (including two monotonic clock reads per sample while excluding TLS decryption, cross-thread channel handoff, shared-memory ring writes, and disk I/O). Each release `bench` round (`lto = "thin"`, `codegen-units = 1`, `rustc 1.99.1`) measures 100,000 two-sided frames per venue following 4,096 warm-up frames. Across five consecutive rounds pinned to CPU 2 on an `AMD Ryzen 7 8845H w/ Radeon 780M Graphics` (`x86_64`, Zen 4, 8C/16T, `AVX2` + `SSE2`) running `Linux 6.18.58.1-microsoft-standard-WSL2 #1 SMP PREEMPT_DYNAMIC x86_64` (1-minute load average `1.49–1.60` for the `serde_json` baseline and `0.16–0.78` for the `AVX2` fast path), the five-round median percentiles were:

| Venue | Decoder Path | p50 | p90 | p99 | p99.9 | Per-round p99.9 range |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Bybit (`orderbook.1`) | Reference `serde_json` + `Decimal` oracle | 571 ns | 611 ns | 732 ns | 1,212 ns | 1,072 – 1,573 ns |
| Bybit (`orderbook.1`) | Audited `simd_json` (`AVX2` + `StepSpec`) | 170 ns | 180 ns | 261 ns | 381 ns | 331 – 941 ns |
| OKX (`bbo-tbt`) | Reference `serde_json` + `Decimal` oracle | 672 ns | 722 ns | 962 ns | 1,944 ns | 1,814 – 2,165 ns |
| OKX (`bbo-tbt`) | Audited `simd_json` (`AVX2` + `StepSpec`) | 161 ns | 180 ns | 230 ns | 361 ns | 351 – 842 ns |

For end-to-end single-host context, see the [native quote decoding benchmark](single-host-trading.md#reproduce-native-quote-decoding).

## Verification and References

```sh
cargo test --locked -p tickwire-md --lib --test spot_quotes --test spot_quotes_simd_oracle
cargo test --locked -p tickwire-spot-runner --test reconcile_cli
```

* [Spot quote session and wire decoder](../../crates/tickwire-md/src/spot_quotes.rs)
* [Audited SIMD JSON scanner and integer unit parser](../../crates/tickwire-md/src/spot_quotes/simd_json.rs)
* [Differential SIMD vs. `serde_json` oracle tests](../../crates/tickwire-md/tests/spot_quotes_simd_oracle.rs)
* [Spot BBO decode percentile benchmark](../../crates/tickwire-md/benches/spot_bbo_decode.rs)
* [Authenticated spot market topic runner](../../crates/tickwire-md/src/spot_market.rs)
* [Single-writer spot BBO publication](../../crates/tickwire-market-publisher/src/spot.rs)
* [Shared-memory market source and split-pair cursor](../../crates/tickwire-market-source/src/shm.rs)
* [JOIN snapshot and split-pair admission tests](../../crates/tickwire-spot-runner/tests/reconcile_cli/market.rs)
* [Runtime market policy](runtime-policy.md)
* [Native quote decoding benchmark](single-host-trading.md#reproduce-native-quote-decoding)
* [Bybit v5 public orderbook specification](https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook)
* [OKX v5 order book channel specification](https://www.okx.com/docs-v5/en/#order-book-trading-market-data-ws-order-book-channel)
