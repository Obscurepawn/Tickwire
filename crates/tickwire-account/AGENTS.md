# tickwire-account

`tickwire-account` (`#![forbid(unsafe_code)]`) owns both the zero-allocation Unified ORS `OrderManager` execution engine
(`OrderManager<CHECK_STP>`, `FillRecordPool`, `OrsStpBook`, `DopState`, `OrsQueryDesk`, `UtaWalletLedger`) and the
single-domain deterministic `Account` state machine over `tickwire-risk`, `tickwire-venue`, and `tickwire-shm-sys` with
zero local WAL, zero disk I/O, and zero floating-point arithmetic.

## Invariants and Change Guidelines

* **Zero-allocation ORS slot pool, `dry_run` scratch slots, and immediate retirement**:
  - Keep `OrderManager<CHECK_STP>` ([src/order_manager.rs](src/order_manager.rs)) backed by startup-preallocated
    (`MlockedSlice`) power-of-two per-strategy order slot rings (`slot_idx = base + (seqnum & (slots_per_strat - 1))`)
    plus one dedicated `dry_run` scratch slot per strategy (`0..=254`).
  - Refuse slot collisions on `Claimed` or `Live` occupants immediately with `RefusalReason::ALLOCATION_ERROR` without
    resizing or evicting active orders; inspect `dry_run` **only** at slot selection and at transport-send skip.
  - Transition terminal orders (full fill, cancel to zero, or definitive reject) immediately to `SlotState::Retired` and
    unlink from `OrsStpBook` in the **same call**—never retain slots waiting on `fee_reconciled`.
* **Fine-grained spinlock hierarchy & lock-free send-path STP**:
  - Synchronize ORS critical paths via [src/spinlock.rs](src/spinlock.rs) (`TtasSpinLock`, `CachePadded`) with zero
    coarse process-wide mutexes, zero logging, zero heap allocation, and zero protocol parsing under any spinlock.
  - Enforce `LockedOrder -> LockedSymbolBook` lock ordering at compile time via `OrderLockProof` in
    [src/ors_stp.rs](src/ors_stp.rs). Publish each symbol's opposite-side best price in an `AtomicI64` word so send-path
    `self_cross` is 100% lock-free, while maintaining dual old+new price STP entries during strategy (`3.8`) and venue
    (`3.7`) modifications.
  - Multi-order admin scans (`set_strat_enabled`, filtered `find_orders`, `on_socket_drop`) lock and release one order at
    a time in ascending `(strat_id, slot_offset)` order.
* **`FillRecordPool` deduplication & post-trade fee `amendment` (`TRADE_LITE` / rebates)**:
  - Route ORS executions through the `2^fill_map_exponent` bucket `FillRecordPool` ([src/fill_record.rs](src/fill_record.rs))
    keyed by `(ors_order_id, exec_ref, side, symbol_id)` with lazy `fill_memory_s` overwrite reuse.
  - Deduplicate identical fills arriving on both order socket replies and private streams (`Duplicate`), and emit
    `OrsReport::Fill { amendment: true, .. }` when `TRADE_LITE` (`fee == None`) is followed by a full trade/fee patch or
    post-trade commission/rebate revision—updating wallet fee delta **only** in [src/uta_ledger.rs](src/uta_ledger.rs)
    without double-applying fill quantities, holds, or positions (even after the order slot has already retired!).
* **Microstructure, `DopState`, `OrsQueryDesk`, and stateless Exchange REST bootstrap**:
  - Maintain `One ack` (`PendingNew -> Open` once; second ack on `Open` is a no-op unless venue-modified price/qty moved),
    optional `FrontAck`, `pending_cxl` cancel reason attribution (`NO_REASON` vs `MKT_CANCELED` / `MKT_IOC_CANCELED` /
    `MKT_REJECT_POST_ONLY` / `MKT_REDUCE_ONLY_CANCELED` / `MKT_CANCELED_SELFTRADE` / `ADMIN_ACTION`),
    `cancel-before-ack` (`allow_cancel_before_ack`), and `ProjectedFill` (`on_cum_qty`) watermarks so terminal cancels
    release `cxl_qty = effective_qty - projected_fill_qty` without double-deduction.
  - Enforce dual-sequence `login`, `account_ref` routing (`0` -> first authorized account; unauthorized ->
    `REJECT_ROUTING_ID_MISMATCH`), and granular `(account_ref, strat_id)` `set_strat_enabled` in
    [src/dop_state.rs](src/dop_state.rs).
  - Drive per-account `StatusAsk` reconciliation, `Place`+`Cancel` `cancel_after` coalescing, `Cancel` exponential backoff
    (`1s..=64s`, max `8` attempts), fills-before-cancel snapshot settlement, non-`Live` full-table cleanup, and optional
    `max_query_count` dead-slot release (`INTERNAL_CANCEL`) in [src/ors_query_desk.rs](src/ors_query_desk.rs).
  - Recover statelessly on cold boot exclusively from Exchange REST snapshots (`Account + OpenOrders`) +
    `SelfDescribingClientId` (`tw1...`) via `OrderManager::bootstrap_from_exchange` and `Account::from_live_exchange`
    ([src/live_bootstrap.rs](src/live_bootstrap.rs)).
* **In-place `Account` state machine (zero whole-account cloning)**:
  - Mutate `Account` ([src/admission.rs](src/admission.rs), [src/events.rs](src/events.rs),
    [src/modifications.rs](src/modifications.rs), [src/structures.rs](src/structures.rs),
    [src/account_inputs.rs](src/account_inputs.rs), [src/external.rs](src/external.rs),
    [src/recovery.rs](src/recovery.rs)) in place or over lightweight single-domain local rollbacks—never clone `Account`
    or `catalog` (`Arc<BTreeMap<u64, InstrumentPolicy>>`), retire terminal zero-remainder orders immediately without
    waiting on `fee_reconciled`, and absorb `TRADE_LITE` / fee-patch updates without freezing.

## Verification and Specifications

* **Source and tests**: [src/lib.rs](src/lib.rs) · [src/order_manager.rs](src/order_manager.rs) ·
  [src/fill_record.rs](src/fill_record.rs) · [src/ors_stp.rs](src/ors_stp.rs) ·
  [src/dop_state.rs](src/dop_state.rs) · [src/ors_query_desk.rs](src/ors_query_desk.rs) ·
  [src/uta_ledger.rs](src/uta_ledger.rs) · [src/spinlock.rs](src/spinlock.rs) ·
  [src/pool.rs](src/pool.rs) · [src/query_desk.rs](src/query_desk.rs) ·
  [src/stp_book.rs](src/stp_book.rs) · [src/projected_fill.rs](src/projected_fill.rs) ·
  [src/modifications.rs](src/modifications.rs) · [src/admission.rs](src/admission.rs) ·
  [src/events.rs](src/events.rs) · [src/live_bootstrap.rs](src/live_bootstrap.rs) ·
  [tests/ors_order_manager.rs](tests/ors_order_manager.rs) ·
  [tests/stp_modify_projected_fill.rs](tests/stp_modify_projected_fill.rs) ·
  [tests/query_desk.rs](tests/query_desk.rs) · [tests/admission.rs](tests/admission.rs) ·
  [tests/windowed_eviction.rs](tests/windowed_eviction.rs) · [tests/live_bootstrap.rs](tests/live_bootstrap.rs) ·
  [tests/amend.rs](tests/amend.rs) · [tests/conditional.rs](tests/conditional.rs) ·
  [tests/account_inputs.rs](tests/account_inputs.rs) · [tests/facts.rs](tests/facts.rs) ·
  [tests/futures.rs](tests/futures.rs) · [tests/handover.rs](tests/handover.rs) ·
  [tests/nonincreasing.rs](tests/nonincreasing.rs) · [tests/recovery.rs](tests/recovery.rs) ·
  [tests/edge_cases.rs](tests/edge_cases.rs)
* **Specifications and models**: [account.md](../../specs/implementation/account.md) ·
  [conditional-orders.md](../../specs/implementation/conditional-orders.md) ·
  [spot-amend.md](../../specs/implementation/spot-amend.md) ·
  [usdm-account-facts.md](../../specs/implementation/usdm-account-facts.md) ·
  [spot-strategy-routing.md](../../specs/implementation/spot-strategy-routing.md) ·
  [account](../../tla/execution_models/account/ROADMAP.md) ·
  [cash_evidence](../../tla/execution_models/cash_evidence/ROADMAP.md) ·
  [strategy_intake](../../tla/execution_models/strategy_intake/ROADMAP.md)

```sh
cargo test --locked -p tickwire-account --all-targets
./scripts/check-tla.sh account
```
