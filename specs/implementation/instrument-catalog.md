# Instrument economics and identity

Use a catalog to bind a venue, an environment, and the economic meaning of each price and quantity. A tick count can change even when the exchange sends the same price string. In the process fixture, `"1.00"` becomes 100 ticks with an increment of 0.01 and 50 ticks with an increment of 0.02. The registered strategy’s threshold of 75 produces one intent in the first case and none in the second. [Catalog format](../../crates/tickwire-instrument-catalog/src/format.rs) · [Process fixtures](../../crates/tickwire-app/tests/catalog_process)

## Decode and identify

`Catalog::decode(raw, expected_artifact_sha256)` checks the raw digest before parsing schema 1. Raw and canonical documents each fit 2 MiB. Maximum nesting is 16. The catalog contains 1–4096 assets and 1–16384 instruments. IDs, versions and delivery expiry use nonzero decimal `u64` strings without leading zeros. Unknown or duplicate fields, duplicate asset/instrument IDs or symbols, bad references and noncanonical increments fail validation.

The venue is a nonzero numeric `u32`. Bind it explicitly to the approved deployment name. Environment is `Production` or `Test`. The market/product/contract combinations are Spot/Spot/Cash, Usdm/LinearPerpetual/Perpetual, and Usdm/LinearFuture/Delivery. Delivery includes `expiry_utc_ns`. Base and quote assets must differ. Assets carry symbols, versions and exponents from −38 through 38. A separate fee asset may exist without appearing in a pair. Price and quantity increments are canonical positive venue decimals.

The artifact digest identifies exact file bytes. The runtime digest is SHA-256 over the ASCII bytes `tickwire.instrument-catalog.v1`, one NUL byte, then canonical JSON. Canonicalization sorts assets and instruments by numeric ID and uses fixed field order. A formatting-only change can preserve the runtime digest while changing the artifact digest. The catalog supplies each `InstrumentBinding` with its own version and runtime digest. `asset_matches` and `instrument_matches` compare the complete economic binding. [Independent vector](../contracts/instrument-catalog-v1.json) · [Python verifier](../../scripts/verify-instrument-catalog.py)

## Bind it at startup

| Consumer | What it checks |
| --- | --- |
| Configuration service | Binds each approved object to its deployment venue, market and environment. Content validation alone cannot activate a runtime. |
| Binance MD | Optional `Config.catalogs` pins up to 32 approved raw files, 2 MiB total. Check every configured feed before obtaining quota, opening sockets, allocating a bank or writing a recovery record. An expansion must use members of those same pinned catalogs. |
| Bybit/OKX spot MD | `--spot-market` requires an approved production spot catalog for the exact numeric venue. Check the cash product, symbol, instrument/version, catalog version and both increments. The diagnostic `--spot-quotes` command takes explicit configuration and does not prove catalog approval. |
| Account App | An optional `Deployment.instrument_catalog` is compared with all baseline assets and instruments, scope, environment, configured feeds and command route. Fees, balances, permissions and risk limits have separate owners. |
| Registered strategy | A catalog-enabled launch requires matching bootstrap, projected catalog reference and deployment inputs. Verify the current account and JOIN inputs before enabling user callbacks. |

The Binance runtime retains the original bytes in memory. A file replacement cannot change an installed price tick. MD recovery saves the approved raw objects and revalidates them. A missing or replaced object, a downgrade from configured to absent, or a change of economics across an expansion fails. A process restart still needs real source warmup and fresh JOINs.
