# Run log — `L3_SymRingWarmerInductive`

- **Model**: [02_models/L3_SymRingWarmerInductive.tla](02_models/L3_SymRingWarmerInductive.tla)
- **Apalache / Z3 symbolic verification (`run_apalache.sh`)**:
  - `typecheck`: Snowcat `OK` (`1.79s`).
  - `inductive:base` (`ConstInit /\ Init => IndInv`): `NoError` (`2.151s`).
  - `inductive:step` (`ConstInit /\ IndInv /\ Next => IndInv'`): `NoError` (`2.343s`).
  - `inductive:entailment` (`ConstInit /\ IndInv => SafetyConjunction`): `NoError` (`2.187s`).
  - `bmc --length=6` (`ConstInit`, `SafetyConjunction`): `NoError` (`3.912s`).
- **TLC finite verification (`./scripts/check-tla.sh hotpath_ultralow_latency`)**:
  - [02_models/L3_SymRingWarmerInductive.cfg](02_models/L3_SymRingWarmerInductive.cfg) (`WarmupInterval = 2`): `212,709` generated, `87,552` distinct, depth `25`, `06s`, `0` errors.
  - [02_models/L3_SymRingWarmerInductive_Disabled.cfg](02_models/L3_SymRingWarmerInductive_Disabled.cfg) (`WarmupInterval = 0`): `46,980` generated, `19,304` distinct, depth `21`, `02s`, `0` errors; combined coverage `18/18` entries (`Init` + `17` `Next` actions) nonzero.
