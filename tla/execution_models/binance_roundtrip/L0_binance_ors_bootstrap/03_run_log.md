# L0 Binance ORS Stateless Bootstrap & Multi-Market Session Run Log

## Verification Command

```sh
./scripts/check-tla.sh binance_roundtrip
```

## TLC Exhaustive Model-Checking Results

- Module: [`02_models/L0_BinanceOrsBootstrap.tla`](02_models/L0_BinanceOrsBootstrap.tla) (`model_sha256: 405847f2a502fcb69d9cba2ba36b820fbea83c35ee55978373803d2e0892ef58`)
- Run 1 ([`02_models/L0_BinanceOrsBootstrap.cfg`](02_models/L0_BinanceOrsBootstrap.cfg), `BalancesIncludeOpenOrderLocks = TRUE` Spot):
  - Verdict: `No error has been found` (`exit 0`)
  - States: `1,977` states generated, `211` distinct states found, `0` states left on queue (depth `9`, wall time `01s`)
- Run 2 ([`02_models/L0_BinanceOrsBootstrap_derivatives.cfg`](02_models/L0_BinanceOrsBootstrap_derivatives.cfg), `BalancesIncludeOpenOrderLocks = FALSE` Derivatives):
  - Verdict: `No error has been found` (`exit 0`)
  - States: `1,977` states generated, `211` distinct states found, `0` states left on queue (depth `9`, wall time `01s`)
- Combined Action Coverage (`scripts/tlc_coverage.py`): `8/8` entries nonzero (`100%` action coverage across `2` runs).
