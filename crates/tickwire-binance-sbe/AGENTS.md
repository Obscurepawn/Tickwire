# tickwire-binance-sbe

`tickwire-binance-sbe` (`#![forbid(unsafe_code)]`) decodes fixed-schema Binance Spot SBE binary market-data frames
(`stream_1_0.xml`: trades, best bid/ask quotes, diff depth, and partial book snapshots) into borrowed zero-copy views
(`decode_frame`) and normalized integer ticks/lots (`normalize_depth`, `decimal_to_units`). `tickwire-binance` and
`tickwire-binance-public` drive it on inbound binary WebSocket frames; socket transport, book storage, and JSON
fallback live outside this crate.

## Invariants and Change Guidelines

* **Header, schema, and group-dimension bounds**: Always validate message header length, `schema_id`, `version`,
  `template_id`, root `block_length`, repeating-group dimensions against `Limits`, and trailing UTF-8 symbol length in
  [src/decode.rs](src/decode.rs) and [src/schema.rs](src/schema.rs) before slicing body bytes—never use `unsafe`
  pointer casts or dynamic reflection.
* **Exact mantissa/exponent scaling without residue**: `decimal_to_units` in [src/decimal.rs](src/decimal.rs) scales
  SBE `(mantissa, exponent)` pairs into integer catalog units with checked arithmetic, returning `Error::NotIntegral`
  if a fractional remainder would be truncated and `Error::Overflow` if the scaled quantity or price exceeds bounds.
* **Instrument identity verification on normalization**: `normalize_depth` in [src/normalize.rs](src/normalize.rs)
  verifies the trailing symbol and exponent scales against `ExpectedInstrument` before emitting `DepthUpdate` levels
  (`Coverage::Diff` vs. `Coverage::Snapshot`).
* **Pinned XML schema and deterministic fixtures**: Regenerate constants in [src/schema.rs](src/schema.rs) and hex
  vectors in `tests/fixtures/` via [tools/generate.py](tools/generate.py) and [tools/fixtures.py](tools/fixtures.py)
  only when updating [schema/stream_1_0.xml](schema/stream_1_0.xml) and [schema/source.json](schema/source.json).

## Verification and Specifications

* **Source and schema**: [src/lib.rs](src/lib.rs) · [src/decode.rs](src/decode.rs) · [src/decimal.rs](src/decimal.rs)
  · [src/normalize.rs](src/normalize.rs) · [src/schema.rs](src/schema.rs) ·
  [schema/stream_1_0.xml](schema/stream_1_0.xml)
* **Integration tests and fixtures**: [tests/codec.rs](tests/codec.rs) · [quote.hex](tests/fixtures/quote.hex) ·
  [trades.hex](tests/fixtures/trades.hex) · [diff.hex](tests/fixtures/diff.hex) ·
  [partial.hex](tests/fixtures/partial.hex)
* **Specifications**: [binance-sbe.md](../../specs/implementation/binance-sbe.md)

```sh
cargo test --locked -p tickwire-binance-sbe --all-targets
```
