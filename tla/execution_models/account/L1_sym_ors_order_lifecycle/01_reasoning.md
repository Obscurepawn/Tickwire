# L1 Symbolic 64-Bit / 128-Bit ORS Order Lifecycle Reasoning

## Dual-Engine `QF_LIA` Encoding & Inductive Strengthening

- **Linear `QF_LIA` Arithmetic Over Full `u64` / `i128` Domains**: Instead of multiplying two symbolic variables (`qty * px`), notional deltas are modeled via linear `QF_LIA` bounds (`delta_notional >= delta_qty` and proportional hold-release bounds `delta_hold <= body_hold_rem`), keeping every verification condition inside decidable linear integer arithmetic so Z3 discharges 101 inductive step VCs in under 4 seconds.
- **SANY 32-Bit Literal Safety**: Constants above `2^31 - 1` are constructed algebraically from `Pow30 == 1073741824` (`U64_MAX == (16 * Pow30 * Pow30) - 1`, `U128_MAX == (256 * Pow30 * Pow30 * Pow30 * Pow30) - 1`).
- **Inductive Strengthening (`IndInv`)**:
  1. Enforced total 128-bit wallet cap `(wallet_quote_free + wallet_quote_locked) <= EffU128Max` on maker rebate inflows while holds are active so releasing holds can never overflow `EffU128Max`.
  2. Coupled `(body_status = ST_PENDING_NEW) => (ack_cnt = 0)` so unreachable `PendingNew` states with `ack_cnt = 1` cannot violate `ack_cnt' <= 1` on `Ack`.
- **Dual-Engine Execution (`BoundedMode`)**: `ConstInit` sets `BoundedMode = FALSE` for unbounded Apalache `inductive` and `bmc` proofs, while `.cfg` files set `BoundedMode = TRUE` (`MaxBoundedSteps = 8`) so `./scripts/check-tla.sh account` achieves 100% TLC action coverage across all 12 transitions.
