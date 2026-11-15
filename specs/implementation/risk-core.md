# Risk Arithmetic, Flat Lock-Free RMS, and Settlement Core

`tickwire-risk` provides deterministic fixed-point decimal arithmetic, conservative spot and derivative margin formulas, flat lock-free per-symbol Risk Management System (`RmsEngine` / `SymbolLimits`) pre-trade checks, single-asset reservation reducers, and bounded delivery-settlement tracking. The crate forbids `unsafe` code, performs zero floating-point operations, and has no I/O, OS clock, or external service dependencies. Callers must bind every input to a single account, asset, product mode, contract instance, configuration revision, and observation timestamp; missing or expired inputs must never be replaced with zero to admit risk-increasing orders.

Concentrating rounding direction, scale alignment, bracket continuity, price-cage bounds, rolling rate/quantity windows, and overflow checks inside `tickwire-risk` guarantees that live ORS order admission (`tickwire-account`) and stateless exchange-snapshot bootstrap arrive at identical integer-atom reservations across architectures and compiler versions. Multi-wallet portfolio aggregation, order slot pools, self-trade prevention, and venue reconciliation remain the responsibility of `tickwire-account`.

## State Model and Bounds

`Fixed` represents every monetary and quantity value as signed `i128` integer atoms paired with an explicit decimal scale in `0..=38`. `RmsEngine` indexes preallocated `SymbolLimits` slots directly by compact `symbol_id: usize` with zero hash/tree lookups and zero locks on the `REQ` hot path. `Settlement<N>` tracks delivery settlement evidence for a single `(account, contract, delivery)` tuple using inline fixed-capacity storage for up to `N` real income identifiers.

| Domain | Mathematical and Structural Contract | Failure Behavior |
| --- | --- | --- |
| Decimal parsing & scaling | Parses strings directly into signed `i128` atoms with scale `0..=38`; addition and subtraction require identical scales or explicit conversion; cross-asset transfers are forbidden. | Extra nonzero fractional digits return `InvalidDecimal`; mismatched scales return `ScaleMismatch`; cross-asset operations return `AssetMismatch`. |
| Rounding & multiplication | Cancels common decimal factors (`10^k`) prior to multiplication; applies a single explicit `Rounding` (`Floor` toward −∞, `Ceiling` toward +∞) at the target output scale for multi-factor fee or collateral values; preserves valid negative balances. | Any unrepresentable intermediate (including inside `div_to`) or zero divisor returns `ArithmeticOverflow` or `DivisionByZero`. |
| Flat per-symbol RMS (`RmsEngine`) | Direct `O(1)` slice lookup by `symbol_id: usize`; three-tier `1s / 5s / 10s` rolling window ring counters (`100` buckets of `100 ms` with bias-free monotonic tick progression) for `new_count` and `order_qty`; exact `10^-9` (`ppb`) fixed-point `FatFingerRatio` buy/sell price cage; single-order max quantity & notional; controlled `bypass_rms`; atomic `RESP` fee accumulator; transactional `SIGUSR1` `reload_limits`. | Out-of-range symbols, disabled symbols, missing reference prices, fat-finger violations, single-order breaches, or rolling window breaches reject in `O(1)` without polluting rolling counters; invalid reload configs preserve prior limits and generation untouched. |
| Single-asset reservations | Evaluates candidate balances and venue caps before committing; outflow reserves round up and inflows round down; elapsed timers alone release zero funds. | `InsufficientAsset`, `InsufficientCollateral`, and `VenueAvailableCap` leave state unchanged; arithmetic or structural faults also latch `GateClosed`. |
| Spot locks & fee shortfalls | Requires complete explanation of baseline locks without subtracting baseline open-order locks from free funds twice; when a received-asset fee exceeds gross proceeds, reserves the remaining liability externally. | Unexplained locks or uncovered fee shortfalls fail with `Unreconciled` or `InsufficientAsset`. |
| Futures exposure & modes | Includes unconfirmed orders in worst-case capital; one-way mode evaluates both long and short position endpoints; hedge mode enforces `BUY == open_long + close_short` and `SELL == open_short + close_long` while checking each side's exposure independently. | Mode mismatches or invalid hedge decompositions fail with `InvalidInput` or `InsufficientCollateral`. |
| Margin brackets & collateral | Tiers start at zero notional, join contiguously without gaps, and maintain continuous cumulative maintenance margin; exact tier boundaries select the higher tier; unapproved positive collateral receives zero value while liabilities are still subtracted in full. | Discontinuous tiers or unapproved coefficients fail with `InvalidBrackets` or `BracketSemanticsUnverified`. |
| Position reduction | Subtracts existing reduce-only allocations, rounds down to the instrument quantity step, verifies actual position sufficiency, and validates side and `reduce_only` flags under the active position mode. | Insufficient claimable position or wrong-way flags fail before mutating reservations. |

## Core Invariants

* **Mathematical signed division**: Floor and ceiling operations obey mathematical inequality (`floor(-1 / 2) = -1`, `ceil(-1 / 2) = 0`) rather than CPU integer truncation toward zero, preventing sign-dependent rounding drift on rebates, negative cashflows, and short positions.
* **Exact integer price cages and zero-lock `REQ` RMS**: `FatFingerRatio` parses decimal ratios (`"0.05"`, `"250bps"`, `"50000ppm"`) into exact parts-per-billion (`10^-9`) integers with zero floating-point operations. Buy orders enforce `price_ticks <= floor(ref_ticks * (10^9 + ppb) / 10^9)` and sell orders enforce `price_ticks >= ceil(ref_ticks * (10^9 - ppb) / 10^9)` in checked `i128` arithmetic. Per-symbol limit cells publish configuration snapshots across an even/odd atomic sequence counter so `SIGUSR1` off-thread reloads never contend on a lock on `REQ`, while `RESP` accumulates realized execution fees via `AtomicI64::fetch_add` on a separate cache line.
* **No optimistic release on timers**: Neither local reservation reducers nor `Settlement<N>` ever release principal, position holds, or fee allowances solely because a deadline or delivery timestamp passed. Public cash/spot quote normalization likewise establishes none of these account or settlement predicates.
* **Reference settlement values never credit cash**: In `Settlement<N>`, theoretical reference settlement calculations exist strictly to check reconciliation completeness against real venue income rows and never post a synthetic wallet credit.
* **Latch-on-anomaly settlement safety**: Conflicting income rows under the same ID, exhausted `N` deduplication slots, or integer overflow perform zero partial state updates, reopen reconciliation, and permanently latch `unresolved_evidence`.

## State Transitions and Failure Semantics

### Flat Lock-Free Symbol RMS (`RmsEngine` & `SymbolLimits`)

1. **Three-tier rolling windows (`1s / 5s / 10s`)**: Each symbol owns private `REQ`-thread `RollingWindow` instances for `new_count` and `order_qty` backed by 100 fixed 100 ms ring buckets (`10s` span) maintaining incremental `1s` (10 buckets), `5s` (50 buckets), and `10s` (100 buckets) sums. All checks execute before committing the candidate order to the ring, so rejected intents never consume rolling rate or quantity budget. When `bypass_rms` is requested and permitted by `allow_bypass`, soft rate/quantity/price-cage limits are bypassed while committed flow is still recorded into the rolling windows.
2. **Transactional `SIGUSR1` limit reload**: `RmsConfigTable::parse_str` and `RmsReloadHandle::reload_limits` parse and validate the entire static configuration off-thread before touching any atomic cell. Malformed syntax, out-of-range `symbol_id`s, non-monotonic `1s/5s/10s` caps, or invalid ratios abort the reload with `RmsConfigError`, retaining existing active limits, rolling counters, reference prices, and generation counter unchanged.

### Bounded Delivery Settlement (`Settlement<N>`)

`Settlement<N>` ingests real delivery income events (deduplicated by native income ID), order lifecycle completions, position updates, and theoretical reference figures for one expiring contract instance:

1. **Idempotent ingestion**: Re-applying an identical real income event is a no-op; observing a known income ID with altered amount or metadata returns `DuplicateConflict`, reopens reconciliation, and latches `unresolved_evidence`.
2. **Single guarded release**: Position and fee reserves release once—and only when account facts are complete, all orders on the contract are terminal, actual position equals zero, wallet discrepancy equals zero, and reference discrepancy equals zero.
3. **Post-release reopening**: If a new, previously unseen real income row arrives after an instance was marked reconciled, `Settlement<N>` reopens reconciliation so the discrepancy cannot be ignored; if capacity `N` is full when late income arrives, `unresolved_evidence` latches and only a full-evidence snapshot rebuild can clear the instance.

## Performance and Cost

All `Fixed` arithmetic executes in registers over `i128` integers with zero heap allocation, `RmsEngine` performs `O(1)` direct flat-array indexing with zero heap allocation and zero locks on `REQ`, and `Settlement<N>` bounds memory layout statically via const generics. Callers pay deterministic constant-time arithmetic per order check and `O(B + N)` scans over small, statically bounded margin brackets (`B`) and settlement income entries (`N`).

## Verification and References

```sh
cargo test --locked -p tickwire-risk --all-targets
cargo test --locked --release -p tickwire-risk --all-targets
python3 scripts/verify-risk-vectors.py
```

The golden fixture suite validates 21 scenarios and 131 expected output fields against the machine-readable risk vector contract, while dedicated test suites cover negative floor/ceiling division, received-asset fee shortfalls, malformed `BUY`/`SELL` hedge-mode splits, `1s/5s/10s` rolling window expiry, exact fixed-point `fat_finger_ratio` price cages, `SIGUSR1` reload rollback on parse errors, and late unrecordable income arriving after settlement release.

* Source modules: [risk error codes and exports](../../crates/tickwire-risk/src/lib.rs), [fixed-point math](../../crates/tickwire-risk/src/fixed.rs), [flat lock-free symbol RMS](../../crates/tickwire-risk/src/rms.rs), [spot calculations](../../crates/tickwire-risk/src/spot.rs), [futures margin and brackets](../../crates/tickwire-risk/src/futures.rs), [reservation reducer](../../crates/tickwire-risk/src/reservation.rs), [delivery settlement](../../crates/tickwire-risk/src/settlement.rs)
* Contracts and test suites: [risk golden vectors v1](../contracts/risk-vectors-v1.json), [boundary regression tests](../../crates/tickwire-risk/tests/boundaries.rs), [RMS regression tests](../../crates/tickwire-risk/tests/rms.rs)
* Related specifications: [multi-asset account state machine](account.md), [USDⓈ-M income and account observations](usdm-account-facts.md)
