# Binance Spot SBE Market Data

`tickwire-binance-sbe` decodes fixed-layout Simple Binary Encoding (SBE) market-data frames for Binance Spot production without runtime XML parsing, dynamic reflection, or `unsafe` code (`#![forbid(unsafe_code)]`). Compile-time layout constants are generated offline from upstream `stream_1_0.xml` pinned at commit `f4a9c0be68235f758b46f571f7313c63d1306b25` (`schemaId = 1`, `version = 0`), covering four templates: trades (`TradesStreamEvent`), best bid/ask (`BestBidAskStreamEvent`), 20-level partial book snapshots (`DepthSnapshotStreamEvent`), and incremental depth diffs (`DepthDiffStreamEvent`).

This crate owns binary framing validation and fixed-point tick/lot normalization, while `DepthStreamClient::new_sbe` and `PublicEventClient` own transport lifecycle and `TopicPublisher` owns shared-memory publication ([binance-market-events.md](binance-market-events.md)).

## Binary Schema and Normalization Bounds

Before returning a borrowed `Frame` view, `decode_frame` validates the complete WebSocket binary payload in a single pass against caller `Limits`:

* **Header and structural framing**: Verifies `schemaId == 1`, `version == 0` (`Error::Schema`, `Error::Version`), recognized `templateId` (`Error::Template`), exact root `blockLength` and repeating-group entry widths (`Error::BlockLength`), valid enum/null discriminants (`Error::Value`), ASCII/UTF-8 symbol trailers (`Error::Utf8`), group counts within configured `Limits` (`Error::Capacity`), and zero unconsumed trailing bytes (`Error::Bounds`).
* **Exact decimal scaling**: SBE encodes prices and quantities as signed integer mantissas paired with base-10 exponents. `decimal_to_units` converts each mantissa/exponent pair directly into catalog `price_tick` and `quantity_step` integer counts, rejecting non-integral remainders (`Error::NotIntegral`) or `i64` range overflows (`Error::Overflow`).
* **Microsecond-to-nanosecond timestamps**: Binance Spot SBE timestamps carry microsecond resolution; normalization multiplies by `1,000` with checked overflow arithmetic to produce canonical Unix nanoseconds and binds every observation to the expected `Identity` (`instrument_id`, `instrument_version`, `catalog_version`, `catalog_hash`, `BookKey`, and `generation`).

## Core Invariants

* **API-key isolation between WSS and HTTPS**: Configuring `sbe_api_key_file` on a Binance Spot production feed connects the WebSocket client to `stream-sbe.binance.com` with TLS hostname verification and attaches the Ed25519 `X-MBX-APIKEY` header solely on the WebSocket HTTP `Upgrade` request (no private cryptographic signing key is required). Initial and rebuild REST `/api/v3/depth` snapshots still query `api.binance.com` *without* transmitting that API key.
* **Subscription ACK gate before REST snapshot**: Both raw stream URLs and explicit JSON subscription frames are supported on SBE connections. Because binary depth frames may arrive on the wire before the explicit text subscription ACK, `DepthStreamClient::new_sbe` buffers incoming binary diffs and withholds the HTTPS `/api/v3/depth` snapshot request until the exact subscription ACK is confirmed.
* **Depth-20 non-substitution**: Incremental SBE diffs (`U..=u`, zero-quantity removals) feed the standard Spot `DepthSync` reducer under the exact same stale-token discard, generation, replay, capacity, and partial-coverage rules as JSON depth. An SBE depth-20 snapshot proves coverage only `Through(last_price)` on non-empty sides (or `Unknown` on empty sides) and can never substitute for the REST `/api/v3/depth` baseline to initialize an incremental diff book.

## Transitions and Failure Semantics

During live incremental depth synchronization, `Notice::Sync` tags `DepthEncoding::SpotSbeV1` and lends the raw binary frame slice alongside the validated `BookView`, allowing `TopicPublisher` to emit normalized depth deltas and `Quote`/`FeedState` records to shared memory. Independent SBE trade and `bestBidAsk` streams feed dedicated event owners that validate repeating groups upfront, pause cleanly across per-poll notification budgets, and publish exact `264` and `265` IPC records, while consuming strategies read the exact normalized IPC records and arena payloads directly from shared memory at independent cursors. Any unknown schema, version, template, truncated group, off-grid decimal, or microsecond-to-nanosecond overflow rejects the frame immediately and triggers the session owner's standard gap/disconnect handling.

## Verification and References

```sh
cargo test --locked -p tickwire-binance-sbe
cargo test --locked -p tickwire-binance-public --test sbe_config --test sbe_network
cargo test --locked -p tickwire-md --test sbe_publication --test market_events
```

An independent Python generator constructs little-endian binary SBE test vectors directly from literal field widths rather than sharing Rust encoder code. On Linux, end-to-end integration tests drive a local TLS SBE WebSocket server, HTTPS REST snapshot baseline, subsequent binary incremental diffs and trades, real `memfd` shared-memory rings, and consumer replicas—verifying tick/lot conversion, sequence bridging, nanosecond scaling, and zero-allocation workspace decoding.

* [Fixed-schema SBE decoder and normalizer crate](../../crates/tickwire-binance-sbe/src/lib.rs)
* [Pinned upstream `stream_1_0.xml` digest and metadata](../../crates/tickwire-binance-sbe/schema/source.json)
* [Independent Python binary fixture generator](../../crates/tickwire-binance-sbe/tools/fixtures.py)
* [End-to-end TLS SBE to shared-memory integration tests](../../crates/tickwire-md/tests/sbe_publication.rs)
* [Binance exact market-event publication specification](binance-market-events.md)
