# Status — Closed (`PASS`)

- `L0_coordinator` (`5cf86194dd707b058fdebdd0b4d5255b7f491445b73c78c09bf53c59218f5c97`): all 4 explicit-state configurations (`L0_DepthSync.cfg`, `L0_UsdM.cfg`, `L0_SpotBuffered.cfg`, `L0_UsdMBuffered.cfg`) completed with `0` errors and `23/23` action coverage (`04_evidence/final-runner.log`, `04_evidence/coverage.json`).
- `L1_sym_coordinator` (`6be638d0543cb8e53b68cf2d77351723f04cefe9d826ef67bbc8067edf0c5d78`): Apalache/Z3 `IndInv` unbounded induction ($\forall k \ge 0$) + 64-bit `BMC` ($k=5$) + Spot/USD-M explicit TLC (`23/23` actions) completed with `0` production bugs.
