# Multi-Asset Account State Machine

`Account` (`tickwire-account`) is the deterministic, single-writer financial state machine for one risk domain (`Scope`: `risk_domain_id`, `venue`, `environment`, `account`), initialized from an immutable opening `Snapshot` and instrument `Catalog`. It evaluates pre-trade asset, margin, and exposure reservations at command admission, books authenticated fills, fees, funding flows, and external adjustments, attributes balances and derivative positions to originating strategies, and reproduces identical ledger states during crash replay. `Account` performs no I/O or network calls of its own; `AccountGateway` owns write-ahead journal synchronization and the final first-byte transmission check.

Keeping all multi-asset risk math and order-liability tracking inside an in-memory value type allows callers to clone `Account` into a disposable candidate, test complex transitions atomically, and discard the candidate on any arithmetic, capacity, or policy failure without corrupting live balances.

## State Model and Bounds

Every monetary and quantity value uses `Fixed { atoms, scale }` (`i128` atoms, decimal scale `0..=38`) paired with an explicit `AssetKey { wallet_id, asset_id }` and versioned `AssetRef`. Reported fill quantities, quote amounts, fees, and realized PnL must be exactly representable at the target asset's configured scale. Risk evaluations always round potential outflows up and inflows down.

| Resource / Parameter | Limit or Rule | Enforcement |
| --- | --- | --- |
| Tracked assets & venue caps | `MAX_ASSETS = 64` | Validated in `Account::from_snapshot`; `locked` must equal `nontrade_locked` because genesis snapshots carry zero coverage for pre-existing live orders. |
| Instrument catalog & positions | `MAX_INSTRUMENTS = 64` (up to `128` side positions) | Validated at construction; strategy `0` serves as the explicit unattributed ownership bucket for remainders and external adjustments. |
| Active / retained orders | `MAX_ORDERS = 1,024` | Keyed by `OrderKey { intent: IntentKey { epoch, intent_id }, client_id }`. |
| Deduplicated economic facts | `MAX_FACTS = 16,384` | Keyed by `(OrderKey, kind, native_id)` so out-of-order and duplicate venue reports apply at most once. |
| Strategy & bracket limits | `MAX_STRATEGIES = 1,024`, `MAX_BRACKETS = 128` | Every submitting strategy requires an explicit gross-quantity limit on the target `InstrumentPolicy`. |
| Ledger sequence | `u64` (`..=u64::MAX`) | Checked increment permits reaching `u64::MAX` exactly and rejects any subsequent mutation. |

### Spot Availability and Fee Reserves

Spot admission enforces both local balance accounting (`baseline + cashflow - reserved`) and the venue-reported free-balance cap (`venue_available`), ensuring pre-existing non-trade locks are never double-subtracted or silently spent. A spot buy reserves remaining base quantity valued at the upper buy-price bound; a spot sell reserves remaining base quantity directly. When fees are charged in the received asset, `Account` separates fees already earned on booked fills (whose fee breakdown has not yet been closed) from the external balance top-up required to cover worst-case fees on the unfilled remainder. General, tax, and special fee components (`FeePolicy.rates: [Fixed; 3]`) round upward independently, with a per-order rounding allowance scaled by `maximum_fills` times the number of non-zero fee components. Assets flagged `exclusive_fee_pool` are ring-fenced for third-asset fee settlement and excluded from ordinary order principal, while still reserving receiving-asset fallback coverage.

### USDⓈ-M Stress Capital and Shadow Holds

USDⓈ-M admission evaluates stress capital across tiered maintenance brackets, effective leverage (`min(configured_leverage, venue_leverage)`), stress unrealized losses (`stress_low` / `stress_high`), execution and close-out exit fees, funding upper bounds, `pending_outbound`, `capital_buffer`, and rounding allowances. Positive unrealized PnL is never credited as usable margin, only the wallet's designated settlement asset supplies collateral, isolated wallets are isolated to a single instrument each, and delivery futures do not inherit a perpetual contract's `funding_settlements` count.

To remain conservative between asynchronous venue balance snapshots, each risk-increasing admission records an incremental shadow capital hold `H = max(0, after - before)` and checks shadow availability `A0 - ΣH - D >= 0`:

* Fee credit allocated to new orders in a wallet cannot exceed `H`; actual fee debits consume that order's `shadow_fee_credit` first before increasing uncovered wallet debits `D`.
* Ordinary terminal order statuses, fills, and `FeesReconciled` closures preserve `H` so sequential turnover cannot outrun an unrefreshed capital baseline `A0`; only a proven synchronous pre-send `Aborted` transition with zero contrary venue evidence releases `H`.
* Non-settlement fees lacking valuation (`shadow_unvalued`) and external debits still update actual asset balances immediately while marking shadow capital unreconciled until a verified `Reactivated` record establishes a fresh `A0`.

## Core Invariants

* **Idempotent retries vs. intent conflicts**: Re-submitting an `IntentKey` with byte-identical `CanonicalCommand` payloads returns the original `Admission` idempotently. Reusing an `IntentKey` with different command bytes immediately freezes the account.
* **Administrative availability under freeze or deficit**: `Cancel`, `CancelList`, `CancelAlgo`, and `Query` validate scope, instrument, target order/structure identity, current `ManagerIdentity` (or management grant) and `owner_revision`, and deadline. They remain admissible even when balances are negative, `valid_until` has expired, or `Account` is frozen. Conversely, cancel-replace reserves the combined exposure of both the existing order and the replacement order, and a cancel request alone never releases principal or margin.
* **Atomic position reduction accounting**: Reduce-only derivative orders reserve unclaimed actual position size on the applicable one-way (`Both`) or hedge (`Long`/`Short`) side. Each fill updates actual position, strategy ownership attribution, and remaining `reduce` reservation together; if outstanding reduction holds ever exceed actual position size, `Account` freezes. External position replacements preserve active order reduction holds and attribute the net delta to bucket `0`.
* **Irreversible booking of real economic facts**: Venue fills, signed multi-asset fees (including maker rebates), spot `last_quote` settlements, and USDⓈ-M realized PnL book even if actual spend or loss exceeds prior reservations or drives an asset balance negative. Exact duplicate trade IDs are no-ops on cash and positions while still advancing terminal or unknown flags; reusing a trade ID with altered economics preserves the first booking and freezes the account.
* **Contradicted aborts restore liability**: Any prior acknowledgement, fee observation (including zero), fill, terminal status, or unknown flag forbids a local pre-send abort. If an acknowledgement or execution event arrives *after* a local abort (`reopen_aborted`), `Account` restores the order's principal and derivative reduction liabilities without allocating a duplicate ticket and freezes new risk.

## State Transitions and Failure Semantics

### Order Liability and Partial Evidence

An order's open liability remains active until terminal venue evidence and complete fill accounting agree. If a terminal order observation reports cumulative filled quantity ahead of individual trade events, principal and reduction reservations stay held for `max(effective, observed_cumulative) - filled`. If a subsequent trade pushes `filled` beyond the order's effective or terminal cumulative quantity, `Account` books the real cash and position movement and immediately calls `freeze()`.

### Freeze, Uncertainty Flags, and Deterministic Replay

Calling `Account::freeze()` flips a one-way gate that blocks risk-increasing admissions without altering balances, reservations, positions, or `ledger_sequence`, leaving the state machine free to continue ingesting authentic fills, fees, and cancellations. Clearing `frozen` requires a verified, journaled `Reactivated` record carrying fresh remote reconciliation evidence and an updated `valid_until` deadline.

Per-order uncertainty tracks three orthogonal flags:

1. `observed_unknown`: set when an ingress observation explicitly reports unknown order status.
2. `recovery_unknown`: injected during journal replay for orders lacking terminal certainty across a restart.
3. `observation_unreconciled`: latched when terminal cumulative quantity is missing, inconsistent, or contradicted after an uncreated-leg closure.

Either `observed_unknown` or `recovery_unknown` marks `OrderView` publicly unknown, and neither `frozen` nor `recovery_unknown` ever causes a replayed duplicate trade to double-book cash or positions.

During crash recovery, `Account` starts from the immutable `Snapshot` and `Catalog` and replays ordered `RecoveryRecord` entries (`Admit`, `Aborted`, `IngressEvent`, `IngressAccountInput`, `FeesReconciled`, external cash/position/transfer records, `StrategyHandover`, conditional/income closures, and `Reactivated`). Every `Admit` record embeds the full `CanonicalCommand`, original `evaluated_at` timestamp, and expected `AdmissionImage`; replay recomputes admission deterministically and fails if any dependency is missing, any input conflicts, `AdmissionImage` diverges, or sequence arithmetic overflows. A successful replay always returns `Account` in the frozen state until new live reconciliation completes.

## Performance and Cost

`Account` stores assets, catalog policies, positions, orders, structures, amendments, admissions, and deduplicated facts in bounded `BTreeMap` collections. Admission clones `Account` so failed candidates drop cleanly without rollback code; accordingly, per-submission CPU and heap-clone cost scales with the number of retained orders and facts within `MAX_ORDERS` and `MAX_FACTS`. No system calls, locks, or clock reads occur inside `tickwire-account`.

## Verification and References

```sh
cargo test --locked -p tickwire-account --all-targets
```

* Core implementation: [account state machine](../../crates/tickwire-account/src/lib.rs), [public types and bounds](../../crates/tickwire-account/src/types.rs), [admission checks](../../crates/tickwire-account/src/admission.rs), [risk and fee calculations](../../crates/tickwire-account/src/calculation.rs)
* Event and recovery reducers: [order event application](../../crates/tickwire-account/src/events.rs), [external balance and position updates](../../crates/tickwire-account/src/external.rs), [replay and reactivation](../../crates/tickwire-account/src/recovery.rs)
* Regression suites and formal models: [USDⓈ-M futures tests](../../crates/tickwire-account/tests/futures.rs), [edge-case regressions](../../crates/tickwire-account/tests/edge_cases.rs), [recovery replay tests](../../crates/tickwire-account/tests/recovery.rs), [TLA+ account model plan](../../tla/execution_models/account/PLAN.md)
* Adjacent specifications: [account gateway](account-gateway.md), [risk arithmetic](risk-core.md), [conditional orders](conditional-orders.md), [spot keep-priority amend](spot-amend.md), [USDⓈ-M account facts](usdm-account-facts.md)
