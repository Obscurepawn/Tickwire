# L0 Per-Account ORS QueryDesk Run Log

## Verification Command

```sh
./scripts/check-tla.sh query_desk
```

## TLC Exhaustive Model-Checking Results

- Module: [`02_models/L0_OrsQueryDesk.tla`](02_models/L0_OrsQueryDesk.tla) (`model_sha256: 2445401e944fc9cc6c977a9fb0246d3ec4f0580083ab861a211915894fc23217`)
- Run 1 ([`02_models/L0_OrsQueryDesk.cfg`](02_models/L0_OrsQueryDesk.cfg), `DeskCap = 1`, `MaxQueryCount = 0`):
  - Verdict: `No error has been found` (`exit 0`)
  - States: `2,404,480` states generated, `672,496` distinct states found, `0` states left on queue (depth `31`, wall time `68s`)
- Run 2 ([`02_models/L0_OrsQueryDesk_bounded.cfg`](02_models/L0_OrsQueryDesk_bounded.cfg), `DeskCap = 1`, `MaxQueryCount = 2`):
  - Verdict: `No error has been found` (`exit 0`)
  - States: `1,791,828` states generated, `479,654` distinct states found, `0` states left on queue (depth `30`, wall time `52s`)
- Combined Action Coverage (`scripts/tlc_coverage.py`): `21/21` entries nonzero (`100%` action coverage across `2` runs).
