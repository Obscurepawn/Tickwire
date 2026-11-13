# Binance Public Depth and Private Event Decoding

`tickwire-binance::feed` and `tickwire-binance::events` decode complete HTTP response bodies and WebSocket payloads for Binance Spot and USD-M Futures into strongly typed, fixed-point Rust representations. Callers supply the target `Market`, `InstrumentBinding`, `BookKey`, or `SessionContext` up front so every decoded observation is bound to an explicit catalog and session identity.

Both modules are pure, side-effect-free decoders: parsing a depth snapshot, incremental diff, execution report, or account balance patch never mutates an order book, alters account cash or reservations, grants risk capacity, or establishes stream continuity on its own. Network lifecycle, snapshot bridging, durable accounting, and reconciliation belong to separate single-writer state owners.

## Wire Schemas and Bounds

All decimal fields are parsed directly from ASCII text into exact fixed-point `Decimal` values without floating-point intermediates, rejecting values whose scale exceeds the catalog asset's exponent or grid step.

| Input Family | Decoded Representation & Rejection Rules |
| --- | --- |
| **Spot / USD-M `exchangeInfo`** | Extracts symbol, product, trading status, and exact price/lot grid filters while preserving unrecognized statuses and filter types. Rejects duplicate symbols, duplicate filters, coin-margined contracts masquerading as USD-M, or oversized payloads. |
| **REST `/depth` snapshot** | Binds the symbol-free response to `DepthContext`, retains `lastUpdateId` (plus required `E`/`T` on USD-M), and marks both sides incomplete (`bids_complete = false`, `asks_complete = false`). Rejects off-grid, oversized, crossed, or improperly sorted levels atomically. |
| **WebSocket / SBE `depthUpdate`** | Decodes `U`/`u` (and `pu` on USD-M), maps `0` quantity to level removal, validates combined-stream suffixes (`@depth`, `@depth@100ms`, Spot `@depth@1000ms`, USD-M `@depth@500ms`), and rejects partial-book, RPI, or coin-margined streams. Product-specific continuity and duplicate-price checks belong to `Book`. |
| **`executionReport` / `ORDER_TRADE_UPDATE`** | Retains native order/execution/trade IDs, order price/size, cumulative and last quote amounts, commissions, and realized PnL. Preserves unknown venue statuses verbatim and distinguishes omitted JSON fields (`None`) from explicit numeric zero (`Some(0)`). |
| **`events::bind_order`** | Correlates `OrderUpdate` against the original `CanonicalCommand`, persistent order mapping, account ID, side, and catalog asset versions. Venue-modified order quantity or price remains observed data; unmatched orders or unknown statuses route to correlation or reconciliation. |
| **Account, list, and algo events** | Decodes `outboundAccountPosition`, `balanceUpdate`, `ACCOUNT_UPDATE`, `ACCOUNT_CONFIG_UPDATE`, `listStatus`, and algo updates as sparse patches where omitted `B` or `P` sections leave those fields unspecified. Terminal list/algo notices never imply child fills; unknown event types decode to `PrivateBody::Other`. |

## Core Invariants

* **Exact economic sign and non-synthesis rules**: A positive exchange commission maps to a negative cash flow, whereas a negative commission represents a fee rebate. USD-M realized PnL remains isolated in its settlement asset. If the venue omits a quoted notional amount (`cumulative_quote` or `last_quote`), the decoder leaves it `None` rather than synthesizing an approximate product from price and quantity.
* **External `u64` ID vs local `u32` `BookKey` separation**: IPC records, journals, logs, and economic bindings always carry the full nonzero `u64` catalog `instrument_id`. By contrast, `BookKey` pairs nonzero `venue` and `source` IDs with an owner-assigned `u32` dense slot (where slot `0` is valid via [instrument-index.md](instrument-index.md)); callers must never derive `BookKey.instrument` by truncating a 64-bit external ID.
* **Zero partial side effects on failed workspace decode**: `DepthWorkspace::new(max_total_levels)` reserves combined bid-plus-ask level storage (`1..=5,000` levels; the shared-memory publisher configures `4,096`). `decode_diff_into` and `decode_sbe_diff_into` return a `DepthUpdateRef` borrowing that workspace until the next exclusive call, guaranteeing that any malformed input leaves no observable partial update. (Fixed-array `decode_diff::<N>` and `decode_sbe_diff::<N>` retain per-side `N` bounds up to `MAX_DEPTH_LEVELS = 5,000` and `MAX_MARKET_MESSAGE_BYTES = 16 MiB`.)

## Transitions and Failure Semantics

Every decode or binding call validates caller context (`DepthContext` or `SessionContext`) before inspecting wire fields. Malformed UTF-8, invalid JSON escapes, duplicate known keys, off-grid decimals, crossed or unsorted snapshot levels, coin-margined symbols on USD-M contexts, or partial/RPI stream suffixes reject the entire message atomically without mutating caller state. On private user-data streams, unmatched order updates or unrecognized venue statuses fail `bind_order` cleanly so the caller can quarantine the raw event or trigger REST reconciliation rather than guessing order state.

## Performance and Cost

To eliminate per-delta heap allocations on hot market-data paths, the custom JSON depth scanner processes escaped known and unknown keys, skips arbitrary ignored subtrees up to a maximum container nesting depth of `128`, caps decoded decimal token length at `80` bytes, and rejects invalid UTF-8, bad escape sequences, malformed numbers, or truncated containers. Regression tests verify zero heap allocations and safe execution within a `2 MiB` thread stack across repeated JSON and SBE workspace decodes, while cold `exchangeInfo` metadata and sparse private account events may allocate bounded vectors and strings.

## Verification and References

```sh
cargo test --locked -p tickwire-binance --test feed --test depth_workspace --test market_server
cargo test --locked -p tickwire-binance --test private_events --test private_structures --test private_server
```

Local TLS integration fixtures stream framed public depth updates into `tickwire-book` and feed private execution and account events through `bind_order` and `bind_structure`.

* [Public depth snapshot, diff, and metadata decoders](../../crates/tickwire-binance/src/feed.rs)
* [Private execution, account, and order-binding decoders](../../crates/tickwire-binance/src/events.rs)
* [Zero-allocation `DepthWorkspace` regression tests](../../crates/tickwire-binance/tests/depth_workspace.rs)
* [Process-local dense instrument index specification](instrument-index.md)
