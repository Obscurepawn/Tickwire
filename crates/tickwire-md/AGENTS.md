# tickwire-md

`tickwire-md` owns the standalone market-data publisher process (`tickwire-md`), driving public Binance (Spot JSON/SBE and USD-M), Bybit V5, and
OKX V5 WebSocket/REST feeds, normalizing integer ticks and lots against approved catalogs, publishing into shared-memory topic banks, and serving
authenticated local JOIN requests. Crate-wide `#![deny(unsafe_code)]` isolates `unsafe` to two audited boundaries: `JoinOwner::receive_cursor` inside
[src/server.rs](src/server.rs) and the SIMD structural JSON scanner inside [src/spot_quotes/simd_json.rs](src/spot_quotes/simd_json.rs)
(`#![deny(unsafe_op_in_unsafe_fn)]`), while sibling spot modules ([src/spot_quotes/network.rs](src/spot_quotes/network.rs) and
[src/spot_quotes/protocol.rs](src/spot_quotes/protocol.rs)) enforce `#[forbid(unsafe_code)]`; strategy execution and account risk remain in worker and
account processes.

## Invariants and Change Guidelines

* **Hot-path vs cold-path separation**: Prebind `Catalog` entries ([src/catalogs.rs](src/catalogs.rs)), dense feed slots
  ([src/config/feed_index.rs](src/config/feed_index.rs)), socket poll registrations, and decoder scratch
  ([src/engine/cold.rs](src/engine/cold.rs)) during startup or warming. Steady-state quote and depth publication in
  [src/engine.rs](src/engine.rs) and [src/spot_quotes/protocol.rs](src/spot_quotes/protocol.rs) must run allocation-free (checked by
  [tests/publisher_hot.rs](tests/publisher_hot.rs)).
* **Authenticated JOIN admission**: [src/server.rs](src/server.rs) only accepts `HELLO` frames matching a `0o700`-directory `Admission` file
  (`{pid}-{request_id}.json`) verified against kernel `SO_PEERCRED` (`pid`, `uid`, `gid`), expected `request_id`, and `ResourceHeader` hashes
  before invoking `JoinOwner::receive_cursor`.
* **Warming expansion and restart validation**: Topic instrument expansion in [src/engine/expansion.rs](src/engine/expansion.rs) and
  [src/server/expansion.rs](src/server/expansion.rs) warms the successor bank while keeping old mappings alive until readers detach, persisting
  recovery state in [src/recovery/store.rs](src/recovery/store.rs) and validating lineage on restart
  ([src/engine/restart_validation.rs](src/engine/restart_validation.rs)).
* **Spot BBO normalization and audited SIMD JSON boundary**: For Bybit (`orderbook.1`) and OKX (`bbo-tbt`) in [src/spot_quotes.rs](src/spot_quotes.rs),
  [src/spot_quotes/simd_json.rs](src/spot_quotes/simd_json.rs), and [src/spot_market.rs](src/spot_market.rs), enforce exact integer tick/step
  divisibility (`StepSpec` + `parse_positive_units`, zero floating-point arithmetic), positive spread (`0 < bid_ticks < ask_ticks`), and
  connection-epoch fencing, and publish unavailability notices immediately on disconnect. Keep unaligned SIMD loads (`_mm256_loadu_si256` with
  `offset + 32 <= chunk.len()`, `_mm_loadu_si128` / `vld1q_u8` with `offset + 16 <= chunk.len()`, plus 64-bit SWAR + scalar tail) gated behind the
  opaque `Backend` capability token (`is_x86_feature_detected!("avx2")` probed once at session creation), never use `from_utf8_unchecked`, and delegate
  any frame containing `\` or non-ASCII bytes (`>= 0x80`) to `Session::decode_serde_oracle` so every input matches the reference `serde_json` oracle
  ([tests/spot_quotes_simd_oracle.rs](tests/spot_quotes_simd_oracle.rs)).

## Verification and Specifications

* **Source, examples, and benchmarks**: [src/lib.rs](src/lib.rs) · [src/main.rs](src/main.rs) · [src/engine.rs](src/engine.rs) ·
  [src/server.rs](src/server.rs) · [src/spot_quotes.rs](src/spot_quotes.rs) ·
  [src/spot_quotes/simd_json.rs](src/spot_quotes/simd_json.rs) · [src/spot_market.rs](src/spot_market.rs) ·
  [benches/spot_bbo_decode.rs](benches/spot_bbo_decode.rs) · [examples/spot-market.md](examples/spot-market.md) ·
  [Deploy two strategies](../../README.md#deploy-two-strategies)
* **Integration tests**: [tests/publisher_hot.rs](tests/publisher_hot.rs) · [tests/market_events.rs](tests/market_events.rs) ·
  [tests/sbe_publication.rs](tests/sbe_publication.rs) · [tests/instrument_slots.rs](tests/instrument_slots.rs) ·
  [tests/spot_quotes.rs](tests/spot_quotes.rs) · [tests/spot_quotes_simd_oracle.rs](tests/spot_quotes_simd_oracle.rs) ·
  [tests/spot_market.rs](tests/spot_market.rs)
* **Specifications and models**: [spot-quote-input.md](../../specs/implementation/spot-quote-input.md) ·
  [binance-market-events.md](../../specs/implementation/binance-market-events.md) · [binance-sbe.md](../../specs/implementation/binance-sbe.md) ·
  [market-expansion.md](../../specs/implementation/market-expansion.md) · [quote_delivery](../../tla/execution_models/quote_delivery/ROADMAP.md)

```sh
cargo test --locked -p tickwire-md --all-targets
cargo bench --locked -p tickwire-md --bench spot_bbo_decode
python3 scripts/run-two-strategies.py
```
