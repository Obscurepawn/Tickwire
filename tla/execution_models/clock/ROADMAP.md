# Clock formal verification roadmap

| Level | Status | Summary |
| --- | --- | --- |
| `L0_calibration` | PASS | All four bounded configurations passed exhaustive BFS (`@TLAPlusOperator` native `Word` acceleration with unchanged model SHA-256), `16/16` actions covered |
| [`L1_sym_calibration`](L1_sym_calibration/05_output.md) | PASS | `Apalache 0.63.0 + Z3` unbounded symbolic induction (`IndInv`, `\A k >= 0`) + 64-bit `0 .. 2^64 - 1` BMC + bounded TLC `16/16` action coverage across all 15 `Calibrator` safety invariants |
