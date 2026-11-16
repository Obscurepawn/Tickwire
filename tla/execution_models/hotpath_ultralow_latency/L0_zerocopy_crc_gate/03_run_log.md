# Run log — `L0_ZeroCopyAndCrcGate`

- **Engine**: TLC (`./scripts/check-tla.sh hotpath_ultralow_latency`)
- **Model**: [02_models/L0_ZeroCopyAndCrcGate.tla](02_models/L0_ZeroCopyAndCrcGate.tla)
- **Run 1 (`CrcEnabled = FALSE`)**: [02_models/L0_ZeroCopyAndCrcGate.cfg](02_models/L0_ZeroCopyAndCrcGate.cfg)
  - States: `59,751` generated, `19,928` distinct, `0` left on queue, depth `25`, wall time `01s`.
  - Verdict: `0` errors (`TypeOK`, `ZeroNoAliasWhileBorrowed`, `ZeroCopyLayoutAndHeaderSoundness`, `CrcGateModeConsistency`, `ArenaBorrowLifetimeSoundness` all hold; `0` deadlocks).
- **Run 2 (`CrcEnabled = TRUE`)**: [02_models/L0_ZeroCopyAndCrcGate_CrcOn.cfg](02_models/L0_ZeroCopyAndCrcGate_CrcOn.cfg)
  - States: `294,623` generated, `91,262` distinct, `0` left on queue, depth `26`, wall time `06s`.
  - Verdict: `0` errors; combined coverage `10/10` entries (`Init` + all `9` `Next` actions) fired with `> 0` generated states.
