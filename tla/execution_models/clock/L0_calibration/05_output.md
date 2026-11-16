# L0 calibration verification output

PASS across all four bounded configurations (`boundary`, `primary`, `rounding`, `usdm`) with zero open source candidates; unbounded 64-bit symbolic induction across all drift rates is additionally proved in [`../L1_sym_calibration/05_output.md`](../L1_sym_calibration/05_output.md).

All 15 safety invariants hold across state typing, fault clearing, token and interval identity, serial non-reuse, side-effect-free stale rejection, monotonic observation, valid read bounds, exact signature quantization, send-gate bounds, continuous server-truth containment, delivery-budget windows, anchor preservation on rejected samples, non-extension of validity on reads, and fault revocation across `begin`, `finish`, `cancel`, `read`, `sign_time`, and `check_send`.
