# Run log — `L4_SymSimdCrcBmc`

- **Model**: [02_models/L4_SymSimdCrcBmc.tla](02_models/L4_SymSimdCrcBmc.tla)
- **Apalache / Z3 symbolic verification (`run_apalache.sh`)**:
  - `typecheck`: Snowcat `OK` (`1.885s`).
  - `inductive:base` (`ConstInit /\ Init => IndInv`): `NoError` (`3.345s`).
  - `inductive:step` (`ConstInit /\ IndInv /\ Next => IndInv'`): `NoError` (`7.929s`).
  - `inductive:entailment` (`ConstInit /\ IndInv => SafetyConjunction`): `NoError` (`3.129s`).
  - `bmc --length=6` (`ConstInit`, `SafetyConjunction`): `NoError` (`12.377s`).
- **TLC finite verification (`./scripts/check-tla.sh hotpath_ultralow_latency`)**:
  - [02_models/L4_SymSimdCrcBmc.cfg](02_models/L4_SymSimdCrcBmc.cfg): `816` generated, `816` distinct, depth `12`, `00s`, `0` errors; coverage `7/7` entries (`Init` + `6` `Next` actions) nonzero.
