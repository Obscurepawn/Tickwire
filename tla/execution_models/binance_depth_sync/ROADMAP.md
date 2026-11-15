# Binance `DepthSync` + `Book` Verification Roadmap

| Level | Status | Result |
| --- | --- | --- |
| [L0_coordinator](L0_coordinator/05_output.md) | `PASS` | Explicit-state Spot & USD-M primary (`73,269` / `73,897` distinct) and `Capacity = 2` scaling (`3,491,866` / `3,492,316` distinct via congruent `DepthSyncView` + `@TLAPlusOperator` bytecode overrides) verify all 7 state invariants and 6 transition properties across `23/23` actions. |
| [L1_sym_coordinator](L1_sym_coordinator/05_output.md) | `PASS` | `Apalache 0.63.0 + Z3` Mode S2 unbounded symbolic induction (`IndInv`, $\forall k \ge 0$, `9.23s`) + Mode S1 64-bit `0 .. 2^64 - 1` `BMC` ($k=5$, `10.217s`) + dual TLC smoke (`36,470` / `36,270` distinct, `23/23` actions) verify all 13 `L0` safety obligations plus `SpotPlusOneSafe` `u64::MAX` overflow freedom across arbitrary `cfg_capacity \in 1..2^64-1`. |
