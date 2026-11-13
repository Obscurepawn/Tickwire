# tickwire-instrument-catalog

`tickwire-instrument-catalog` (`#![forbid(unsafe_code)]`) owns bounded decoding, economic
validation, canonical serialization, and cryptographic digesting of versioned `Catalog` and
`Artifact` objects shared across market data, strategy workers, and execution. It builds
`tickwire-venue` asset and instrument bindings from raw JSON bytes without owning filesystem
persistence, shared-memory transport, or dense runtime slot tables (`tickwire-instrument-index`).

## Invariants and Change Guidelines

* **Pre-parse resource and digest gates**: In `decode` ([src/format.rs](src/format.rs)), enforce
  non-empty raw length `<= MAX_BYTES` (2 MiB), verify SHA-256 equality against
  `expected_artifact_sha256` (`Error::Digest`), and check JSON nesting `<= MAX_DEPTH` (16) before
  invoking `serde_json`, capping collection sizes at `MAX_ASSETS` (4096) and `MAX_INSTRUMENTS`
  (16,384) during streaming visitor deserialization.
* **Strict schema and economic validation**: Keep `#[serde(deny_unknown_fields)]` on wire types in
  [src/format.rs](src/format.rs) and [src/lib.rs](src/lib.rs). Validate schema version `1`, non-zero
  venue/asset/instrument IDs, exponent bounds (`-38..=38`), unique symbols and IDs, positive
  tick/step scales, and consistent `Market` (`Spot` vs `Usdm`) and `Contract` (`Cash`, `Perpetual`,
  `Delivery`) combinations.
* **Dual-digest identity**: Preserve the distinction between `artifact_hash` (SHA-256 over exact
  input `raw_bytes`) and `catalog_hash` (SHA-256 over `tickwire.instrument-catalog.v1\0`
  concatenated with sorted `canonical_bytes`). Sorted asset and instrument vectors in `Catalog`
  ([src/lib.rs](src/lib.rs)) also back `binary_search_by_key` lookups—never mutate their ordering.
* **Contract vector parity**: Any change to canonical field ordering, decimal formatting, or digest
  domain separation requires updating
  [instrument-catalog-v1.json](../../specs/contracts/instrument-catalog-v1.json) and passing the
  independent Python verifier.

## Verification and Specifications

* **Source and tests**: [src/lib.rs](src/lib.rs) · [src/format.rs](src/format.rs) ·
  [tests/catalog.rs](tests/catalog.rs) · [tests/vectors.rs](tests/vectors.rs)
* **Specifications and contracts**:
  [instrument-catalog.md](../../specs/implementation/instrument-catalog.md) ·
  [instrument-catalog-v1.json](../../specs/contracts/instrument-catalog-v1.json)

```sh
cargo test --locked -p tickwire-instrument-catalog --all-targets
python3 scripts/verify-instrument-catalog.py
```
