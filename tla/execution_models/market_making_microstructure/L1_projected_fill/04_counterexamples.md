# Counterexamples and Rust fixes

Adversarial inspection and code-faithful modeling of `L1_ProjectedFill` across `tickwire-account` and `tickwire-strategy-sdk` uncovered a real denominator-mismatch bug when `CumulativeQuote` or `AveragePrice` status frames interleaved with `LimitFallback` status frames or settled-first `FillEvent`s (`ExactIncrementalGapPriceDenominator`). It was reproduced with failing Red tests in both crates, fixed in Rust, and verified to exhaustion under TLC.

## Bug 1: Mixed `LimitFallback` / settled-first fills corrupted `CumulativeQuote` and `AveragePrice` incremental `gap_price` denominators (`ExactIncrementalGapPriceDenominator`)

- **Scenario**:
  1. Status #1 arrives without `cumulative_quote` (`LimitFallback`) at `cum_qty = 2`: `exch_cum_qty` advances from `0` to `2`, while `prev_cum_quote` remains `None` (`0`).
  2. Status #2 arrives with `CumulativeQuote { cum_quote = 306 }` at `cum_qty = 3` (`3` lots filled near `102`).
  3. Separately, when `FillEvent` (`settled_filled = 2 @ 100`) arrives *before* Status #1 (`cum_qty = 3, avg_px = 102`), `prev_avg` was `None` while `exch_cum_qty` was `0`.
- **Pre-fix behavior**: Both `Account::on_cum_qty` and `FillWatermarkTracker::estimate_gap_price` divided `cum_quote - prev_quote (306 - 0 = 306)` by `exch_delta_qty = cum_qty - prev_exch_cum_qty (3 - 2 = 1)`, fabricating a **3x inflated** `gap_price = 306` instead of `306 / 3 = 102`! Similarly, when `AveragePrice` followed a `LimitFallback` status where `prev_avg` stayed `None`, `prev_exch_qty * prev_avg` collapsed to `0` while dividing by `cum_qty - prev_exch_cum_qty`.
- **Fix**: Added dedicated `exch_quote_qty: Fixed` and `exch_avg_qty: Fixed` watermarks alongside `exch_cum_qty` on `Order` (`crates/tickwire-account/src/lib.rs:348-355`, `crates/tickwire-account/src/projected_fill.rs:150-245`) and `OrderWatermarkEntry` (`crates/tickwire-strategy-sdk/src/microstructure.rs:622-650, 910-965, 1205-1277`) so `CumulativeQuote` divides `cum_quote - prev_cum_quote` by `cum_qty - exch_quote_qty` and `AveragePrice` weights `prev_avg` by `exch_avg_qty` and divides by `cum_qty - exch_avg_qty`.
- **Regression tests**: `projected_fill_gap_price_tracks_quote_and_avg_denominators_across_limit_fallback_and_settled_first_fills` in `crates/tickwire-account/tests/stp_modify_projected_fill.rs` and `fill_watermark_tracker_gap_price_tracks_quote_and_avg_denominators_across_limit_fallback_and_settled_first_fills` in `crates/tickwire-strategy-sdk/tests/microstructure_sdk.rs`.
