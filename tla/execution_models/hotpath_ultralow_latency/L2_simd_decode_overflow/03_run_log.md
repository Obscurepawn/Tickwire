# Run log — `L2_SimdDecodeAndOverflowGuard`

- **Engine**: TLC (`./scripts/check-tla.sh hotpath_ultralow_latency`)
- **Model**: [02_models/L2_SimdDecodeAndOverflowGuard.tla](02_models/L2_SimdDecodeAndOverflowGuard.tla)
- **Run 1 (`Venue = "Bybit"`, `Profile = "release"`)**: [02_models/L2_SimdDecodeAndOverflowGuard.cfg](02_models/L2_SimdDecodeAndOverflowGuard.cfg)
  - States: `27,369` generated, `998` distinct, `0` left on queue, depth `7`, wall time `03s`.
  - Verdict: `0` errors (`TypeOK`, `ExactOracleEquivalence`, `TickLotAndPositiveSpreadSoundness`, `NoSilentArithmeticWrap`, `ZeroHotPathHeapAllocation` all hold).
- **Run 2 (`Venue = "Okx"`, `Profile = "bench"`)**: [02_models/L2_SimdDecodeAndOverflowGuard_OkxBench.cfg](02_models/L2_SimdDecodeAndOverflowGuard_OkxBench.cfg)
  - States: `27,369` generated, `998` distinct, `0` left on queue, depth `7`, wall time `03s`.
  - Verdict: `0` errors; combined coverage `12/12` entries (`Init` + all `11` `Next` actions) fired with `> 0` generated states.
