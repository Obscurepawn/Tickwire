# Instrument Economics and Identity Catalog

`tickwire-instrument-catalog` binds a venue, market, and environment to the exact fixed-point economic meaning of every traded asset and instrument. Because Tickwire executes exclusively in integer ticks and lots, identical wire price strings produce different internal tick counts under different catalog definitions: in the process integration fixture, `"1.00"` decodes to `100` ticks under a `0.01` increment and `50` ticks under a `0.02` increment, causing a strategy with a `75`-tick threshold to emit an order intent in the first configuration and none in the second.

The catalog crate owns deterministic decoding, canonicalization, and dual SHA-256 identity (raw artifact digest versus canonical runtime digest) in safe Rust (`#![forbid(unsafe_code)]`). It does not activate a deployment on its own or own dynamic account state: configuration approval, market-data topic pinning, account risk limits, fee schedules, and trading permissions are enforced by their respective runtime owners at startup.

## State Model and Bounds

`Catalog::decode(raw, expected_artifact_sha256)` verifies the SHA-256 digest of the raw bytes *before* parsing schema `1` JSON, ensuring untrusted or corrupted files fail before structural deserialization.

| Parameter | Bound or Rule | Rejection Error |
| --- | --- | --- |
| Document size | Raw and canonical JSON each `<= 2 MiB` (`MAX_BYTES`) | `Error::Bounds` |
| JSON nesting | Maximum container depth `16` (`MAX_DEPTH`) | `Error::Bounds` / `Encoding` |
| Asset count | `1..=4,096` (`MAX_ASSETS`), unique IDs and symbols | `Error::Bounds` / `Duplicate` |
| Instrument count | `1..=16,384` (`MAX_INSTRUMENTS`), unique IDs and symbols | `Error::Bounds` / `Duplicate` |
| Numeric IDs & versions | Nonzero decimal `u64` strings with no leading zeros | `Error::Encoding` / `Identity` |
| Venue & environment | Nonzero `u32` venue; `Production` or `Test` environment | `Error::Identity` |
| Asset scale | Exponent in `-38..=38` (standalone fee assets permitted) | `Error::Economics` |
| Price & quantity steps | Canonical positive venue decimals; `base_asset != quote_asset` | `Error::Economics` |

Only three `(Market, Product, Contract)` combinations are valid:

1. `Spot` / `Spot` / `Cash`
2. `Usdm` / `LinearPerpetual` / `Perpetual`
3. `Usdm` / `LinearFuture` / `Delivery` (requires a nonzero decimal `expiry_utc_ns` string)

Unknown or duplicate JSON fields, unresolved asset references, or mismatched market/product triples fail validation immediately.

## Core Invariants

* **Dual-digest separation (`artifact_hash` vs `catalog_hash`)**: `artifact_hash` is the SHA-256 of the exact raw file bytes pinned by deployment manifests. `catalog_hash` (the runtime digest) is the SHA-256 over the ASCII domain prefix `tickwire.instrument-catalog.v1`, a single `0x00` byte, and the canonical JSON representation (assets and instruments sorted by ascending numeric `u64` ID with fixed field order). Whitespace-only reformatting changes `artifact_hash` while preserving `catalog_hash`.
* **Complete economic binding**: Each `Instrument` projects an `InstrumentBinding` carrying its `instrument_id`, `instrument_version`, `catalog_version`, `catalog_hash`, symbol, asset IDs, and tick/lot increments. `Catalog::asset_matches` and `Catalog::instrument_matches` verify full structural and economic equality via binary search over sorted IDs, preventing partial ID-only matches.
* **Immutable in-memory retention**: Once loaded at startup, runtimes retain the verified raw catalog bytes in memory; modifying or replacing a file on disk while the process runs cannot alter an installed tick size or lot step.

## Startup Binding and Recovery Semantics

Every runtime component validates catalog bindings during startup before opening network sockets, allocating shared-memory banks, or enabling strategy callbacks:

| Consumer | Startup and Lifecycle Enforcement |
| --- | --- |
| **Configuration service** | Binds each approved catalog artifact to its deployment venue name, market, and environment; schema validity alone never activates a runtime. |
| **Binance `tickwire-md`** | Optional `Config.catalogs` pins up to 32 approved raw artifacts (`<= 2 MiB` combined) in memory (`Catalogs::load`). Every configured feed is checked before acquiring quota, opening sockets, or creating SHM banks. Live expansions must draw exclusively from those already-pinned in-memory catalogs. |
| **Bybit / OKX `tickwire-md`** | `--spot-market` requires an approved `Production` `Spot` catalog matching the exact numeric venue, `Cash` product, symbol, instrument/catalog versions, and both increments. (`--spot-quotes` takes explicit CLI config for diagnostics and does not assert catalog approval.) |
| **ORS `tickwire-app`** | Validates optional `OrsAppConfig.instrument_catalog` (`CatalogVerificationConfig`: `path`, `expected_sha256`, optional `expected_catalog_version`) via `Catalog::decode` at cold startup before constructing `BinanceBootstrapCatalog` / `UtaVenueSession` bindings; balances, fees, permissions, and risk limits remain with their dedicated owners. |
| **Registered strategy** | Requires matching bootstrap metadata, projected catalog reference, and deployment inputs, verifying both the account binding and live JOIN snapshot headers before invoking user callbacks. |

On cold startup or restart, `tickwire-md` and `tickwire-app` verify their configured static catalog files against pinned SHA-256 digests solely in memory (with zero local recovery checkpoints or WAL files). Missing or substituted catalog bytes, downgrading a catalog-enabled configuration to unconfigured, or changing instrument economics across a live expansion fails closed, and a restarted market-data process warms live upstream feeds and requires fresh consumer JOINs.

## Verification and References

```sh
cargo test --locked -p tickwire-instrument-catalog
cargo test --locked -p tickwire-app --test catalog_process
python3 scripts/verify-instrument-catalog.py
```

* [Catalog types and lookup API](../../crates/tickwire-instrument-catalog/src/lib.rs)
* [Schema 1 parser, canonicalizer, and digest computation](../../crates/tickwire-instrument-catalog/src/format.rs)
* [End-to-end catalog tick-scaling process fixtures](../../crates/tickwire-app/tests/catalog_process)
* [Golden machine contract vector (`instrument-catalog-v1.json`)](../contracts/instrument-catalog-v1.json)
* [Independent Python contract verifier](../../scripts/verify-instrument-catalog.py)
