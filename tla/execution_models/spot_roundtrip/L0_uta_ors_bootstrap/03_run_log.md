# L0 Bybit V5 & OKX V5 UTA ORS Bootstrap Run Log

## Verification Command

```sh
./scripts/check-tla.sh spot_roundtrip
```

## TLC Exhaustive Model-Checking Results

- Module: [`02_models/L0_UtaOrsBootstrap.tla`](02_models/L0_UtaOrsBootstrap.tla) (`model_sha256: 7b1a5daa0cc45144b0e5aa33d1b4ce69b9c15a034c571361c851304f0a7f5b94`)
- Run 1 ([`02_models/L0_UtaOrsBootstrap.cfg`](02_models/L0_UtaOrsBootstrap.cfg), `WebSocketTrade`, `MaxInflightPerConn = 2`):
  - Verdict: `No error has been found` (`exit 0`)
  - States: `28,676` states generated, `2,773` distinct states found, `0` states left on queue (depth `12`, wall time `02s`)
- Run 2 ([`02_models/L0_UtaOrsBootstrap_https.cfg`](02_models/L0_UtaOrsBootstrap_https.cfg), `PooledHttps`, `MaxInflightPerConn = 1`):
  - Verdict: `No error has been found` (`exit 0`)
  - States: `28,676` states generated, `2,773` distinct states found, `0` states left on queue (depth `12`, wall time `02s`)
- Combined Action Coverage (`scripts/tlc_coverage.py`): `10/10` entries nonzero (`100%` action coverage across `2` runs).
