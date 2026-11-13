# Binance Public Event Publication and Replica Storage

Alongside order-book depth, `tickwire-md` runs dedicated `PublicEventClient` owners—each subscribing to a single Binance product, catalog instrument, and `EventKind`—that decode complete source frames, normalize prices and quantities against approved catalog increments, and hand typed observations to the single-writer `TopicPublisher`.

| Source Stream | Published Exact Categories | Wire Stream Suffixes |
| --- | --- | --- |
| **Binance Spot JSON** | Individual `Trade` (`264`), `BestBidAsk` (`265`) | `@trade`, `@bookTicker` |
| **Binance Spot Production SBE** | Individual `Trade` (`264`), `BestBidAsk` (`265`) | `@trade`, `@bestBidAsk` |
| **Binance USD-M JSON** | Aggregate `Trade` (`264`), `BestBidAsk` (`265`), `MarkFunding` (`266`) | `@aggTrade`, `@bookTicker`, `@markPrice` / `@markPrice@1s` |

## Wire Schema and Exact Event Validation

Exact market records (`264`, `265`, `266`) occupy the standard 512-byte IPC slot (`128`-byte `RecordHeader` + `384`-byte `MarketExactBody`) and require `EXACT_MARKET_EVENTS` (feature bit `9`, `1 << 9`) end-to-end across ring headers and JOIN snapshots, while legacy record types `257..=263` retain their original encoding. Every exact record binds nonzero `feed_generation`, `instrument_id`, `instrument_version`, `source_id`, and `source_connection_epoch`, enforces a strict `value_validity` bitmask where cleared bits require zeroed payload fields, and rejects off-grid or overflowing decimals:

* **Trade (`264`, category bit `2` / `TRADE`)**: Carries positive `price_ticks` and `quantity_lots`, `buyer_is_maker` / `aggregate` flags, trade ID, and—for USD-M `aggTrade`—`venue_seq_first <= venue_seq_last` raw trade IDs plus optional non-RPI `normal_quantity_lots` (`0 <= nq <= quantity_lots`).
* **Best bid/ask (`265`, category bit `1` / `QUOTE`)**: Carries strictly uncrossed `0 < bid_price_ticks < ask_price_ticks`, non-negative bid/ask lots, and `venue_seq_first == venue_seq_last == update_id`. Because Spot JSON `bookTicker` omits an exchange event timestamp, its timestamp validity bit (`bit 0`) and `source_timestamp_ns` remain `0`; Spot SBE converts microsecond timestamps to nanoseconds via checked multiplication by `1,000`.
* **Mark and funding (`266`, category bit `4` / `MARK_FUNDING`)**: Carries positive `mark_price` and `index_price`, plus optional `estimated_settlement_price`, `moving_average`, `funding_rate`, and `next_funding_utc_ns` (explicitly distinguishing omitted Perpetual/Delivery fields via `value_validity` from numeric zero). `DecimalWire` encodes a signed little-endian `i128` coefficient, an `i8` exponent in `-38..=38`, and 7 reserved zero bytes, normalizing `0` to exponent `0` and stripping trailing decimal zeros.

For SBE streams, the event owner validates the entire binary repeating group (schema `1`, version `0`, template, block length, and group bounds) before delivering its first row, saving its cursor index if the per-poll notification budget runs out mid-group. Spot production SBE authenticates its WebSocket `Upgrade` with an API key while public TLS verifies the upstream hostname; `Connect`, `Upgrade`, `Subscribe`, and `Pong` consume configured `PublicPermit` quotas, and explicit subscriptions require the exact ACK after transmission. If socket EOF arrives in the same read batch as complete frames, the owner invalidates the connection *first* and delivers those trailing observations with `usable = false`.

## Core Invariants

* **Independent freshness across depth and event slots**: Each instrument row tracks four state slots—`Quote (1)`, `Trade (2)`, `MarkFunding (4)`, and `FeedState (8)`—each stamping its own monotonic ingress time. While an independent `BestBidAsk` source populates the `Quote` slot, depth levels and coverage remain owned exclusively by the depth source: an arriving trade or BBO event never refreshes stale depth age, and aggregate `FeedState` reports live only when depth is continuous *and* all required event categories are live.
* **Dense slot uniqueness without external ID truncation**: External `u64` instrument IDs appear on the wire, in triggers, in `TWMR0001` checkpoints, and in strategy input logs, while each topic maps them at startup to contiguous local slots ([instrument-index.md](instrument-index.md)). `BookKey` pairs `(venue, local_slot, source)` where slot `0` is valid; reusing a `BookKey` within a topic fails as an identity error even if external `u64` IDs differ in their upper bits.
* **Replica continuity and checkpoint integrity**: A consumer joins at snapshot `S` (`last_event` recording the ring position) and applies every ring record from `S + 1`; any sequence gap, identity mismatch, or capacity violation permanently blocks the replica. `TWMR0001` replica checkpoints (bounded to `4,096` instruments and `64 MiB`) persist materialized views and the blocked flag, re-verifying SHA-256 digest, catalog identity, sort order, coverage, and depth capacity on restore.
* **Fault-isolated publication**: A rejected book view never mutates another instrument's row. If encoding or ring publication fails after caching a valid view, the published ring prefix does not advance and the fault sink invalidates the bank, closes the live gate, and blocks new JOIN snapshots until a replacement bank is installed.

## Hot-Path Storage and Topic Expansion

`TopicPublisher` pre-allocates four `MarketState` slots and two depth arrays (up to `5,000` cached levels per side) per instrument row, sharing topic-wide scratch buffers for `4,096` delta levels and `4,096` encoded levels. Steady-state depth publication formats up to three 512-byte records on the stack (transitioning from inline level encoding at `<= 12` levels to shared-memory arena payloads at `>= 13` levels) and publishes them in a single batch with zero heap allocations inside a `2 MiB` thread stack. `reserved_cache_bytes()` tracks exact heap capacities across rows, state slots, levels, catalog strings, decoder contexts, dense index, and workspaces against `cache_bytes` (excluding allocator metadata, thread stack, SHM mmaps, and JOIN snapshots). On the consumer side, `MarketReplica` merges sorted deltas inside two shared workspaces (`320,000` bytes total at the `10,000`-level-per-side ceiling), returning `Known(0)` for absent covered ticks and `Unknown` outside coverage.

During live expansion (`prepare_expansion`), the publisher warms only newly appended instruments in a private candidate sharing the same catalog and a higher `instrument_set_version`, verifying peak dual-instance `cache_bytes`, subscriber detachment, and candidate freshness before copying existing rows, advancing ring/feed generations at `old.published + 1`, republishing member states, and freezing the predecessor bank (invoking the fault sink if bank freeze fails).

## Verification and References

```sh
cargo test --locked -p tickwire-md --test market_events --test publisher_hot --test instrument_slots --test sbe_publication
cargo test --locked -p tickwire-market-publisher -p tickwire-ipc
```

Linux integration suites exercise all seven Spot/USD-M JSON and SBE event combinations over local TLS/WebSocket servers and real `memfd` shared memory, testing exact IPC byte vectors, late JOINs, `TWMR0001` checkpoint restores, independent depth/event freshness, and arena/capacity fault paths.

* [Binance JSON and SBE market-event decoder](../../crates/tickwire-binance/src/market.rs)
* [Topic publisher exact-event routing](../../crates/tickwire-market-publisher/src/events.rs)
* [Topic publisher live expansion](../../crates/tickwire-market-publisher/src/expansion.rs)
* [Exact 512-byte IPC record and `DecimalWire` validation](../../crates/tickwire-ipc/src/market_exact.rs)
* [IPC byte-vector test suites](../../crates/tickwire-ipc/tests)
* [End-to-end TLS-to-SHM market event tests](../../crates/tickwire-md/tests/market_events.rs)
* [Zero-allocation, inline/arena boundary, and capacity tests](../../crates/tickwire-md/tests/publisher_hot.rs)
* [Process-local dense instrument index specification](instrument-index.md)
