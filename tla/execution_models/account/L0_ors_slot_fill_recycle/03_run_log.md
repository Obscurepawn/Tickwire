# L0 ORS OrderSlot, FillRecordPool & Recycling Run Log

## Verification Command

```sh
./scripts/check-tla.sh account
```

## TLC Exhaustive Model-Checking Results

- Module: [`02_models/L0_OrsSlotFillRecycle.tla`](02_models/L0_OrsSlotFillRecycle.tla) (`model_sha256: e359a3697b9279b2c88aba72c9b257c4c48d50a2b8b7b5e9618c40287c85f39d`)
- Run 1 ([`02_models/L0_OrsSlotFillRecycle.cfg`](02_models/L0_OrsSlotFillRecycle.cfg)):
  - Verdict: `No error has been found` (`exit 0`)
  - States: `653,749` states generated, `97,889` distinct states found, `0` states left on queue (depth `36`, wall time `20s`)
- Run 2 ([`02_models/L0_OrsSlotFillRecycle_dryrun_stp.cfg`](02_models/L0_OrsSlotFillRecycle_dryrun_stp.cfg)):
  - Verdict: `No error has been found` (`exit 0`)
  - States: `727` states generated, `220` distinct states found, `0` states left on queue (depth `17`, wall time `01s`)
- Combined Action Coverage (`scripts/tlc_coverage.py`): `14/14` entries nonzero (`100%` action coverage across `2` runs).
