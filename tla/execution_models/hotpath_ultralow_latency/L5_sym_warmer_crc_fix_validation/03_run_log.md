# Run log — `L5_SymWarmerCrcFixValidation`

- **Model**: [02_models/L5_SymWarmerCrcFixValidation.tla](02_models/L5_SymWarmerCrcFixValidation.tla)
- **Apalache / Z3 symbolic verification (`run_apalache.sh`)**:
  - `typecheck`: Snowcat `OK` (`0.932s`).
  - `inductive:base` (`ConstInit /\ Init => IndInv`): `NoError` (`2.190s`).
  - `inductive:step` (`ConstInit /\ IndInv /\ Next => IndInv'`): `NoError` (`2.404s`).
  - `inductive:entailment` (`ConstInit /\ IndInv => SafetyConjunction`): `NoError` (`2.950s`).
  - `bmc --length=6` (`ConstInit`, `SafetyConjunction`): `NoError` (`9.239s`).
- **TLC finite verification (`./scripts/check-tla.sh hotpath_ultralow_latency`)**:
  - [02_models/L5_SymWarmerCrcFixValidation.cfg](02_models/L5_SymWarmerCrcFixValidation.cfg) (`WarmupInterval = 2`): `13,953` generated, `3,130` distinct, depth `14`, `00s`, `0` errors.
  - [02_models/L5_SymWarmerCrcFixValidation_Disabled.cfg](02_models/L5_SymWarmerCrcFixValidation_Disabled.cfg) (`WarmupInterval = 0`): `6,385` generated, `1,324` distinct, depth `13`, `00s`, `0` errors; combined coverage `15/15` entries (`Init` + `14` `Next` actions) nonzero.
