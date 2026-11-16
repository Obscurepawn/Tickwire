---- MODULE L1_SymOrsQueryDesk ----
(*
 * Dual-Engine Symbolic 64-Bit Inductive & Bounded Model (Apalache Z3 `QF_LIA` + TLC)
 * for Per-Account `OrsQueryDesk` Monotonic Merge, Exponential `Cancel` Backoff,
 * Catch-Up Fills Before Remainder Cancel, `-2013` / `NotFound` Disambiguation,
 * Non-`Live` Full-Table Eviction, and `max_query_count` Contract:
 *   - `OrsQueryDesk` (`schedule_status_ask`, `mark_cancel_after`,
 *     `schedule_cancel_backoff`, `poll_due`, `complete_order`, `cancel_backoff_delay_ns`):
 *       crates/tickwire-account/src/ors_query_desk.rs:13-47
 *       crates/tickwire-account/src/ors_query_desk.rs:119-134
 *       crates/tickwire-account/src/ors_query_desk.rs:165-386
 *   - `OrderManager` (`request_cancel` coalescing, `on_socket_drop`,
 *     `on_query_inconclusive`, `reconcile_query_snapshot`):
 *       crates/tickwire-account/src/order_manager.rs:1508-1536
 *       crates/tickwire-account/src/order_manager.rs:2449-2656
 *   - `BinanceAccountSession` (`schedule_delayed_place_query`,
 *     `schedule_delayed_cancel_query`, `-2013` uncreated `NACK`, `DispatchCancel`):
 *       crates/tickwire-binance-account/src/session.rs:2219-2280
 *       crates/tickwire-binance-account/src/session.rs:2495-2520
 *       crates/tickwire-binance-account/src/session.rs:3010-3205
 *   - `UtaAccountSession` & REST `StatusAsk` decoding (`poll_due_queries`,
 *     `apply_status_ask_reply`, `decode_status_ask_snapshot`):
 *       crates/tickwire-spot-execution/src/rest_bootstrap.rs:896-1215
 *       crates/tickwire-spot-execution/src/session.rs:2141-2287
 *       crates/tickwire-spot-execution/src/session.rs:3526-3543
 *)
EXTENDS Integers

CONSTANTS
  (* @type: Bool; *)
  BoundedMode,
  (* @type: Bool; *)
  OptInMaxCap

VARIABLES
  (* --- Symbolic 64-bit account & desk configuration --- *)
  (* var <-> code: initial `UtaWalletLedger` free lots (`crates/tickwire-account/src/uta_ledger.rs:21-26`) *)
  (* @type: Int; *)
  cfg_init_budget_lots,
  (* var <-> code: `OrderManager::max_query_count` (`crates/tickwire-account/src/order_manager.rs:2495-2498`) *)
  (* @type: Int; *)
  cfg_max_query_limit,

  (* --- Monotonic nanosecond clock (`u64`) --- *)
  (* var <-> code: `now_mono_ns` (`crates/tickwire-account/src/ors_query_desk.rs:46-47`, `crates/tickwire-account/src/ors_query_desk.rs:314-320`) *)
  (* @type: Int; *)
  now_mono_ns,

  (* --- `UtaWalletLedger` free & open-order hold lots --- *)
  (* var <-> code: `AssetBucket::free_atoms` (`crates/tickwire-account/src/uta_ledger.rs:21-26`, `crates/tickwire-account/src/uta_ledger.rs:206-275`) *)
  (* @type: Int; *)
  wallet_free_lots,
  (* var <-> code: `AssetBucket::locked_atoms` / `OrsOrder::hold_amount_atoms` (`crates/tickwire-account/src/order_manager.rs:520-523`) *)
  (* @type: Int; *)
  wallet_locked_lots,

  (* --- Primary symbolic `OrderSlot` + `OrsOrder` state --- *)
  (* var <-> code: `OrderManager::is_slot_live` (`crates/tickwire-account/src/order_manager.rs:1143-1153`) *)
  (* @type: Bool; *)
  slot_live,
  (* var <-> code: `OrsOrder::status` (`crates/tickwire-account/src/order_manager.rs:455-464`) *)
  (* @type: Int; *)
  order_status,
  (* var <-> code: `OrsOrder::orig_qty_lots` (`crates/tickwire-account/src/order_manager.rs:514-518`) *)
  (* @type: Int; *)
  orig_lots,
  (* var <-> code: `OrsOrder::filled_qty_lots` (`crates/tickwire-account/src/order_manager.rs:514-518`) *)
  (* @type: Int; *)
  filled_lots,
  (* var <-> code: `OrsCanceledReport::canceled_qty_lots` (`crates/tickwire-account/src/order_manager.rs:2500-2514`, `crates/tickwire-account/src/order_manager.rs:2610-2648`) *)
  (* @type: Int; *)
  last_canceled_lots,
  (* var <-> code: `OrsOrderQuerySnapshot::cum_filled_lots` applied before cancel (`crates/tickwire-account/src/order_manager.rs:2538-2564`) *)
  (* @type: Int; *)
  snap_cum_applied,
  (* var <-> code: `OrsOrder::pending_cxl` (`crates/tickwire-account/src/order_manager.rs:509-510`) *)
  (* @type: Int; *)
  pending_cxl,
  (* var <-> code: `OrsOrder::query_fail_count` (`crates/tickwire-account/src/order_manager.rs:513-514`, `crates/tickwire-account/src/order_manager.rs:2493-2498`) *)
  (* @type: Int; *)
  query_fail_count,
  (* var <-> code: terminal classification (`crates/tickwire-account/src/order_manager.rs:2488-2656`) *)
  (* @type: Int; *)
  terminal_kind,
  (* var <-> code: `was_open_or_filled` guard in `NotFound` reconciliation (`crates/tickwire-account/src/order_manager.rs:2624-2628`) *)
  (* @type: Bool; *)
  was_open_before_term,
  (* var <-> code: opt-in `INTERNAL_CANCEL` retirement marker (`crates/tickwire-account/src/order_manager.rs:2499-2518`) *)
  (* @type: Bool; *)
  retired_via_cap,
  (* var <-> code: `OrsQueryReconcileAction::DispatchCancel` (`crates/tickwire-account/src/order_manager.rs:2582-2595`) *)
  (* @type: Bool; *)
  dispatched_coalesced_cxl,

  (* --- Preallocated `OrsQueryDesk` entry + full-table peer occupant --- *)
  (* var <-> code: `OrsQueryEntry::occupied` (`crates/tickwire-account/src/ors_query_desk.rs:30-31`) *)
  (* @type: Bool; *)
  desk_self_occupied,
  (* var <-> code: `OrsQueryEntry::origin` (`crates/tickwire-account/src/ors_query_desk.rs:21-26`, `crates/tickwire-account/src/ors_query_desk.rs:38-39`) *)
  (* @type: Int; *)
  desk_origin,
  (* var <-> code: `OrsQueryEntry::cancel_after` (`crates/tickwire-account/src/ors_query_desk.rs:39-40`, `crates/tickwire-account/src/ors_query_desk.rs:186-188`) *)
  (* @type: Bool; *)
  desk_cancel_after,
  (* var <-> code: `OrsQueryEntry::query_attempts` (`crates/tickwire-account/src/ors_query_desk.rs:41-42`, `crates/tickwire-account/src/ors_query_desk.rs:361-368`) *)
  (* @type: Int; *)
  desk_query_attempts,
  (* var <-> code: `OrsQueryEntry::cancel_attempts` (`crates/tickwire-account/src/ors_query_desk.rs:43-44`, `crates/tickwire-account/src/ors_query_desk.rs:269-275`) *)
  (* @type: Int; *)
  desk_cancel_attempts,
  (* var <-> code: `OrsQueryEntry::cancel_exhausted` (`crates/tickwire-account/src/ors_query_desk.rs:45-46`, `crates/tickwire-account/src/ors_query_desk.rs:269-271`) *)
  (* @type: Bool; *)
  desk_cancel_exhausted,
  (* var <-> code: `OrsQueryEntry::next_due_mono_ns` (`crates/tickwire-account/src/ors_query_desk.rs:46-47`, `crates/tickwire-account/src/ors_query_desk.rs:189-191`) *)
  (* @type: Int; *)
  desk_next_due_ns,
  (* var <-> code: `cancel_backoff_delay_ns` (`crates/tickwire-account/src/ors_query_desk.rs:131-134`) *)
  (* @type: Int; *)
  last_backoff_delay_ns,
  (* var <-> code: sibling desk occupant in capacity-bounded `AccountDeskInner` (`crates/tickwire-account/src/ors_query_desk.rs:88-91`, `crates/tickwire-account/src/ors_query_desk.rs:197-206`) *)
  (* @type: Bool; *)
  peer_desk_occupied,
  (* var <-> code: `is_order_live(entry.slot_idx, entry.ors_order_id)` on sibling occupant (`crates/tickwire-account/src/ors_query_desk.rs:199-203`) *)
  (* @type: Bool; *)
  peer_live,
  (* var <-> code: witness that no `Live` entry is ever evicted (`crates/tickwire-account/src/ors_query_desk.rs:198-206`) *)
  (* @type: Bool; *)
  evicted_live_order,
  (* var <-> code: `RefusalReason::TooManyPendingActions` (`crates/tickwire-account/src/ors_query_desk.rs:204-206`) *)
  (* @type: Bool; *)
  refused_too_many_pending,

  (* --- Monotonic `schedule_status_ask` merge witness --- *)
  (* var <-> code: `schedule_status_ask` existing-entry merge (`crates/tickwire-account/src/ors_query_desk.rs:184-193`) *)
  (* @type: Bool; *)
  merge_witness_active,
  (* @type: Int; *)
  prev_due_on_merge,
  (* @type: Bool; *)
  prev_cancel_after_on_merge,
  (* @type: Int; *)
  prev_query_att_on_merge,
  (* @type: Int; *)
  prev_cancel_att_on_merge,

  (* --- Bounded-mode step counter for TLC smoke execution --- *)
  (* @type: Int; *)
  step_cnt

vars == <<
  cfg_init_budget_lots, cfg_max_query_limit, now_mono_ns,
  wallet_free_lots, wallet_locked_lots, slot_live, order_status,
  orig_lots, filled_lots, last_canceled_lots, snap_cum_applied,
  pending_cxl, query_fail_count, terminal_kind, was_open_before_term,
  retired_via_cap, dispatched_coalesced_cxl, desk_self_occupied,
  desk_origin, desk_cancel_after, desk_query_attempts, desk_cancel_attempts,
  desk_cancel_exhausted, desk_next_due_ns, last_backoff_delay_ns,
  peer_desk_occupied, peer_live, evicted_live_order, refused_too_many_pending,
  merge_witness_active, prev_due_on_merge, prev_cancel_after_on_merge,
  prev_query_att_on_merge, prev_cancel_att_on_merge, step_cnt
>>

(* --- Exact 64-bit constants constructed without raw literals > 2147483647 --- *)
Pow30 == 1073741824
U64_MAX == (16 * Pow30 * Pow30) - 1
NsPerSec == 1000000000

EffU64Max == IF BoundedMode THEN 80 ELSE U64_MAX
EffMaxLots == IF BoundedMode THEN 2 ELSE (4 * Pow30)
BaseBackoffNs == IF BoundedMode THEN 1 ELSE NsPerSec
StatusQueryDelayNs == IF BoundedMode THEN 2 ELSE (6 * NsPerSec)
MaxCancelBackoffAttempts == 8
MaxBoundedSteps == 6

ST_FREE == 0
ST_PENDING_NEW == 1
ST_OPEN == 2
ST_TERMINAL == 3

ORG_PLACE == 0
ORG_CANCEL == 1
ORG_SOCKET_DROP == 2

TERM_NONE == 0
TERM_FILLED == 1
TERM_CANCELED == 2
TERM_REJECTED_NACK == 3
TERM_INTERNAL_CANCEL == 4

(* @type: (Int, Int) => Int; *)
SatAddU64(a, b) ==
  IF a + b > EffU64Max THEN EffU64Max ELSE a + b

(* Linear `QF_LIA` encoding of `cancel_backoff_delay_ns(base_backoff_ns, attempt)`:
   `base * (1 << min(attempt, 6))` -> `1, 2, 4, 8, 16, 32, 64` x `BaseBackoffNs`
   (`crates/tickwire-account/src/ors_query_desk.rs:131-134`). *)
(* @type: (Int) => Int; *)
CancelBackoffDelayNs(att) ==
  IF att <= 0 THEN BaseBackoffNs
  ELSE IF att = 1 THEN 2 * BaseBackoffNs
  ELSE IF att = 2 THEN 4 * BaseBackoffNs
  ELSE IF att = 3 THEN 8 * BaseBackoffNs
  ELSE IF att = 4 THEN 16 * BaseBackoffNs
  ELSE IF att = 5 THEN 32 * BaseBackoffNs
  ELSE 64 * BaseBackoffNs

ValidBackoffDelaySet == {
  0,
  BaseBackoffNs,
  2 * BaseBackoffNs,
  4 * BaseBackoffNs,
  8 * BaseBackoffNs,
  16 * BaseBackoffNs,
  32 * BaseBackoffNs,
  64 * BaseBackoffNs
}

(* Apalache symbolic constant initializer *)
ConstInit ==
  /\ BoundedMode = FALSE
  /\ OptInMaxCap \in BOOLEAN

StepGuard ==
  (~BoundedMode) \/ (step_cnt < MaxBoundedSteps)

NextStepCnt ==
  IF BoundedMode THEN step_cnt + 1 ELSE 0

ClearSelfDesk ==
  /\ desk_self_occupied' = FALSE
  /\ desk_origin' = ORG_PLACE
  /\ desk_cancel_after' = FALSE
  /\ desk_query_attempts' = 0
  /\ desk_cancel_attempts' = 0
  /\ desk_cancel_exhausted' = FALSE
  /\ desk_next_due_ns' = 0
  /\ last_backoff_delay_ns' = 0
  /\ merge_witness_active' = FALSE
  /\ prev_due_on_merge' = 0
  /\ prev_cancel_after_on_merge' = FALSE
  /\ prev_query_att_on_merge' = 0
  /\ prev_cancel_att_on_merge' = 0

Init ==
  /\ IF BoundedMode
     THEN /\ cfg_init_budget_lots = 2
          /\ cfg_max_query_limit = (IF OptInMaxCap THEN 1 ELSE 0)
     ELSE /\ cfg_init_budget_lots \in 1..EffMaxLots
          /\ IF OptInMaxCap
             THEN cfg_max_query_limit \in 1..EffU64Max
             ELSE cfg_max_query_limit = 0
  /\ now_mono_ns = 0
  /\ wallet_free_lots = cfg_init_budget_lots
  /\ wallet_locked_lots = 0
  /\ slot_live = FALSE
  /\ order_status = ST_FREE
  /\ orig_lots = 0
  /\ filled_lots = 0
  /\ last_canceled_lots = 0
  /\ snap_cum_applied = 0
  /\ pending_cxl = 0
  /\ query_fail_count = 0
  /\ terminal_kind = TERM_NONE
  /\ was_open_before_term = FALSE
  /\ retired_via_cap = FALSE
  /\ dispatched_coalesced_cxl = FALSE
  /\ desk_self_occupied = FALSE
  /\ desk_origin = ORG_PLACE
  /\ desk_cancel_after = FALSE
  /\ desk_query_attempts = 0
  /\ desk_cancel_attempts = 0
  /\ desk_cancel_exhausted = FALSE
  /\ desk_next_due_ns = 0
  /\ last_backoff_delay_ns = 0
  /\ peer_desk_occupied = FALSE
  /\ peer_live = FALSE
  /\ evicted_live_order = FALSE
  /\ refused_too_many_pending = FALSE
  /\ merge_witness_active = FALSE
  /\ prev_due_on_merge = 0
  /\ prev_cancel_after_on_merge = FALSE
  /\ prev_query_att_on_merge = 0
  /\ prev_cancel_att_on_merge = 0
  /\ step_cnt = 0

(* 1. Admit symbolic order and reserve hold in `UtaWalletLedger`. *)
SubmitOrder ==
  /\ StepGuard
  /\ order_status = ST_FREE
  /\ \E qty \in (IF BoundedMode THEN {2} ELSE 1..EffMaxLots) :
       /\ qty <= cfg_init_budget_lots
       /\ orig_lots' = qty
       /\ wallet_free_lots' = cfg_init_budget_lots - qty
       /\ wallet_locked_lots' = qty
       /\ slot_live' = TRUE
       /\ order_status' = ST_PENDING_NEW
       /\ step_cnt' = NextStepCnt
       /\ UNCHANGED <<
            cfg_init_budget_lots, cfg_max_query_limit, now_mono_ns,
            filled_lots, last_canceled_lots, snap_cum_applied, pending_cxl,
            query_fail_count, terminal_kind, was_open_before_term,
            retired_via_cap, dispatched_coalesced_cxl, desk_self_occupied,
            desk_origin, desk_cancel_after, desk_query_attempts,
            desk_cancel_attempts, desk_cancel_exhausted, desk_next_due_ns,
            last_backoff_delay_ns, peer_desk_occupied, peer_live,
            evicted_live_order, refused_too_many_pending, merge_witness_active,
            prev_due_on_merge, prev_cancel_after_on_merge,
            prev_query_att_on_merge, prev_cancel_att_on_merge
          >>

(* 2. Sibling order occupies or retires in the capacity-1 desk slot. *)
PeerOccupyOrRetireInDesk ==
  /\ StepGuard
  /\ ~desk_self_occupied
  /\ \E p_live \in BOOLEAN :
       /\ ~(peer_desk_occupied /\ peer_live = p_live)
       /\ peer_desk_occupied' = TRUE
       /\ peer_live' = p_live
       /\ step_cnt' = NextStepCnt
       /\ UNCHANGED <<
            cfg_init_budget_lots, cfg_max_query_limit, now_mono_ns,
            wallet_free_lots, wallet_locked_lots, slot_live, order_status,
            orig_lots, filled_lots, last_canceled_lots, snap_cum_applied,
            pending_cxl, query_fail_count, terminal_kind, was_open_before_term,
            retired_via_cap, dispatched_coalesced_cxl, desk_self_occupied,
            desk_origin, desk_cancel_after, desk_query_attempts,
            desk_cancel_attempts, desk_cancel_exhausted, desk_next_due_ns,
            last_backoff_delay_ns, evicted_live_order, refused_too_many_pending,
            merge_witness_active, prev_due_on_merge, prev_cancel_after_on_merge,
            prev_query_att_on_merge, prev_cancel_att_on_merge
          >>

(* 3. `OrsQueryDesk::schedule_status_ask` on free slot OR after evicting a non-`Live` peer entry
      (`crates/tickwire-account/src/ors_query_desk.rs:195-225`). *)
ScheduleStatusAskOrEvictNonLive ==
  /\ StepGuard
  /\ slot_live
  /\ ~desk_self_occupied
  /\ (~peer_desk_occupied \/ ~peer_live)
  /\ \E org \in {ORG_PLACE, ORG_CANCEL, ORG_SOCKET_DROP} :
       /\ (org = ORG_CANCEL => order_status = ST_OPEN)
       /\ LET due == IF org = ORG_SOCKET_DROP
                     THEN now_mono_ns
                     ELSE SatAddU64(now_mono_ns, StatusQueryDelayNs)
          IN
          /\ evicted_live_order' = (evicted_live_order \/ (peer_desk_occupied /\ peer_live))
          /\ peer_desk_occupied' = FALSE
          /\ peer_live' = FALSE
          /\ desk_self_occupied' = TRUE
          /\ desk_origin' = org
          /\ desk_cancel_after' = (org = ORG_CANCEL)
          /\ desk_query_attempts' = 0
          /\ desk_cancel_attempts' = 0
          /\ desk_cancel_exhausted' = FALSE
          /\ desk_next_due_ns' = due
          /\ last_backoff_delay_ns' = 0
          /\ pending_cxl' = (IF org = ORG_CANCEL THEN SatAddU64(pending_cxl, 1) ELSE pending_cxl)
          /\ merge_witness_active' = FALSE
          /\ prev_due_on_merge' = 0
          /\ prev_cancel_after_on_merge' = FALSE
          /\ prev_query_att_on_merge' = 0
          /\ prev_cancel_att_on_merge' = 0
          /\ step_cnt' = NextStepCnt
          /\ UNCHANGED <<
               cfg_init_budget_lots, cfg_max_query_limit, now_mono_ns,
               wallet_free_lots, wallet_locked_lots, slot_live, order_status,
               orig_lots, filled_lots, last_canceled_lots, snap_cum_applied,
               query_fail_count, terminal_kind, was_open_before_term,
               retired_via_cap, dispatched_coalesced_cxl,
               refused_too_many_pending
             >>

(* 4. Full-table refusal (`TooManyPendingActions`) when occupied peer entry is `Live`
      (`crates/tickwire-account/src/ors_query_desk.rs:204-206`). *)
ScheduleRefuseFullLivePeer ==
  /\ StepGuard
  /\ slot_live
  /\ ~desk_self_occupied
  /\ peer_desk_occupied
  /\ peer_live
  /\ refused_too_many_pending' = TRUE
  /\ step_cnt' = NextStepCnt
  /\ UNCHANGED <<
       cfg_init_budget_lots, cfg_max_query_limit, now_mono_ns,
       wallet_free_lots, wallet_locked_lots, slot_live, order_status,
       orig_lots, filled_lots, last_canceled_lots, snap_cum_applied,
       pending_cxl, query_fail_count, terminal_kind, was_open_before_term,
       retired_via_cap, dispatched_coalesced_cxl, desk_self_occupied,
       desk_origin, desk_cancel_after, desk_query_attempts,
       desk_cancel_attempts, desk_cancel_exhausted, desk_next_due_ns,
       last_backoff_delay_ns, peer_desk_occupied, peer_live,
       evicted_live_order, merge_witness_active, prev_due_on_merge,
       prev_cancel_after_on_merge, prev_query_att_on_merge,
       prev_cancel_att_on_merge
     >>

(* 5. Monotonic merge in `schedule_status_ask` / `mark_cancel_after`:
      moves `next_due_mono_ns` earlier ONLY (`due_mono_ns < entry.next_due_mono_ns`),
      latches `cancel_after`, and never resets retry counters (`crates/tickwire-account/src/ors_query_desk.rs:184-193`). *)
CoalesceOntoPendingStatusAsk ==
  /\ StepGuard
  /\ slot_live
  /\ desk_self_occupied
  /\ \E org \in {ORG_CANCEL, ORG_SOCKET_DROP} :
     \E cand_due \in (IF BoundedMode THEN 0..3 ELSE 0..EffU64Max) :
       /\ prev_due_on_merge' = desk_next_due_ns
       /\ prev_cancel_after_on_merge' = desk_cancel_after
       /\ prev_query_att_on_merge' = desk_query_attempts
       /\ prev_cancel_att_on_merge' = desk_cancel_attempts
       /\ merge_witness_active' = TRUE
       /\ desk_cancel_after' = (desk_cancel_after \/ (org = ORG_CANCEL))
       /\ desk_next_due_ns' = (IF cand_due < desk_next_due_ns THEN cand_due ELSE desk_next_due_ns)
       /\ pending_cxl' = (IF org = ORG_CANCEL THEN SatAddU64(pending_cxl, 1) ELSE pending_cxl)
       /\ step_cnt' = NextStepCnt
       /\ UNCHANGED <<
            cfg_init_budget_lots, cfg_max_query_limit, now_mono_ns,
            wallet_free_lots, wallet_locked_lots, slot_live, order_status,
            orig_lots, filled_lots, last_canceled_lots, snap_cum_applied,
            query_fail_count, terminal_kind, was_open_before_term,
            retired_via_cap, dispatched_coalesced_cxl, desk_self_occupied,
            desk_origin, desk_query_attempts, desk_cancel_attempts,
            desk_cancel_exhausted, last_backoff_delay_ns, peer_desk_occupied,
            peer_live, evicted_live_order, refused_too_many_pending
          >>

(* 6. Exponential `Cancel` backoff (`1s..=64s`, capped at `8` attempts) via
      `schedule_cancel_backoff` / `poll_due(RetryCancel)` (`crates/tickwire-account/src/ors_query_desk.rs:265-277`,
      `crates/tickwire-account/src/ors_query_desk.rs:344-360`). *)
ScheduleOrPollCancelBackoff ==
  /\ StepGuard
  /\ order_status = ST_OPEN
  /\ (desk_self_occupied \/ ~peer_desk_occupied)
  /\ ~desk_cancel_exhausted
  /\ LET cur_att == IF desk_self_occupied THEN desk_cancel_attempts ELSE 0
         cur_q_att == IF desk_self_occupied THEN desk_query_attempts ELSE 0
     IN
     /\ IF cur_att >= MaxCancelBackoffAttempts
        THEN /\ desk_self_occupied' = TRUE
             /\ desk_origin' = ORG_CANCEL
             /\ desk_cancel_after' = TRUE
             /\ desk_query_attempts' = cur_q_att
             /\ desk_cancel_attempts' = MaxCancelBackoffAttempts
             /\ desk_cancel_exhausted' = TRUE
             /\ desk_next_due_ns' = desk_next_due_ns
             /\ last_backoff_delay_ns' = last_backoff_delay_ns
        ELSE LET dly == CancelBackoffDelayNs(cur_att) IN
             /\ desk_self_occupied' = TRUE
             /\ desk_origin' = ORG_CANCEL
             /\ desk_cancel_after' = TRUE
             /\ desk_query_attempts' = cur_q_att
             /\ desk_cancel_attempts' = cur_att + 1
             /\ desk_cancel_exhausted' = FALSE
             /\ desk_next_due_ns' = SatAddU64(now_mono_ns, dly)
             /\ last_backoff_delay_ns' = dly
     /\ pending_cxl' = (IF pending_cxl = 0 THEN 1 ELSE pending_cxl)
     /\ merge_witness_active' = FALSE
     /\ prev_due_on_merge' = 0
     /\ prev_cancel_after_on_merge' = FALSE
     /\ prev_query_att_on_merge' = 0
     /\ prev_cancel_att_on_merge' = 0
     /\ step_cnt' = NextStepCnt
     /\ UNCHANGED <<
          cfg_init_budget_lots, cfg_max_query_limit, now_mono_ns,
          wallet_free_lots, wallet_locked_lots, slot_live, order_status,
          orig_lots, filled_lots, last_canceled_lots, snap_cum_applied,
          query_fail_count, terminal_kind, was_open_before_term,
          retired_via_cap, dispatched_coalesced_cxl, peer_desk_occupied,
          peer_live, evicted_live_order, refused_too_many_pending
        >>

(* 7. `poll_due` dispatches due `StatusAsk` (`crates/tickwire-account/src/ors_query_desk.rs:360-371`). *)
PollDueDispatchStatusAsk ==
  /\ StepGuard
  /\ slot_live
  /\ desk_self_occupied
  /\ ~desk_cancel_exhausted
  /\ now_mono_ns >= desk_next_due_ns
  /\ ~(desk_origin = ORG_CANCEL /\ desk_cancel_attempts > 0)
  /\ desk_query_attempts' = SatAddU64(desk_query_attempts, 1)
  /\ desk_next_due_ns' = SatAddU64(now_mono_ns, BaseBackoffNs)
  /\ merge_witness_active' = FALSE
  /\ prev_due_on_merge' = 0
  /\ prev_cancel_after_on_merge' = FALSE
  /\ prev_query_att_on_merge' = 0
  /\ prev_cancel_att_on_merge' = 0
  /\ step_cnt' = NextStepCnt
  /\ UNCHANGED <<
       cfg_init_budget_lots, cfg_max_query_limit, now_mono_ns,
       wallet_free_lots, wallet_locked_lots, slot_live, order_status,
       orig_lots, filled_lots, last_canceled_lots, snap_cum_applied,
       pending_cxl, query_fail_count, terminal_kind, was_open_before_term,
       retired_via_cap, dispatched_coalesced_cxl, desk_self_occupied,
       desk_origin, desk_cancel_after, desk_cancel_attempts,
       desk_cancel_exhausted, last_backoff_delay_ns, peer_desk_occupied,
       peer_live, evicted_live_order, refused_too_many_pending
     >>

(* 8. Hot-path `Ack` + optional partial fill while order remains `Open`. *)
HotAckOrPartialFill ==
  /\ StepGuard
  /\ slot_live
  /\ \E add_fill \in (IF BoundedMode THEN 0..1 ELSE 0..EffMaxLots) :
       /\ filled_lots + add_fill < orig_lots
       /\ order_status' = ST_OPEN
       /\ filled_lots' = filled_lots + add_fill
       /\ wallet_locked_lots' = wallet_locked_lots - add_fill
       /\ step_cnt' = NextStepCnt
       /\ UNCHANGED <<
            cfg_init_budget_lots, cfg_max_query_limit, now_mono_ns,
            wallet_free_lots, slot_live, orig_lots, last_canceled_lots,
            snap_cum_applied, pending_cxl, query_fail_count, terminal_kind,
            was_open_before_term, retired_via_cap, dispatched_coalesced_cxl,
            desk_self_occupied, desk_origin, desk_cancel_after,
            desk_query_attempts, desk_cancel_attempts, desk_cancel_exhausted,
            desk_next_due_ns, last_backoff_delay_ns, peer_desk_occupied,
            peer_live, evicted_live_order, refused_too_many_pending,
            merge_witness_active, prev_due_on_merge, prev_cancel_after_on_merge,
            prev_query_att_on_merge, prev_cancel_att_on_merge
          >>

(* 9. `reconcile_query_snapshot` (`Open` / `PartiallyFilled`) emits `DispatchCancel` if `cancel_after`
      or `pending_cxl > 0` (`crates/tickwire-account/src/order_manager.rs:2571-2598`). *)
ReconcileOpenAndCoalescedCancel ==
  /\ StepGuard
  /\ slot_live
  /\ desk_self_occupied
  /\ order_status' = ST_OPEN
  /\ dispatched_coalesced_cxl' = (dispatched_coalesced_cxl \/ desk_cancel_after \/ (pending_cxl > 0))
  /\ ClearSelfDesk
  /\ step_cnt' = NextStepCnt
  /\ UNCHANGED <<
       cfg_init_budget_lots, cfg_max_query_limit, now_mono_ns,
       wallet_free_lots, wallet_locked_lots, slot_live, orig_lots,
       filled_lots, last_canceled_lots, snap_cum_applied, pending_cxl,
       query_fail_count, terminal_kind, was_open_before_term, retired_via_cap,
       peer_desk_occupied, peer_live, evicted_live_order,
       refused_too_many_pending
     >>

(* 10. `reconcile_query_snapshot` (`Canceled` / `Expired` / `Filled`) applies catch-up fills
       via `on_fill` strictly BEFORE releasing remaining `canceled_qty_lots`
       (`crates/tickwire-account/src/order_manager.rs:2531-2564`, `crates/tickwire-account/src/order_manager.rs:2604-2622`). *)
ReconcileSnapshotTerminalWithCatchup ==
  /\ StepGuard
  /\ slot_live
  /\ desk_self_occupied
  /\ \E snap_cum \in (IF BoundedMode THEN 0..2 ELSE 0..EffMaxLots) :
     \E is_filled_status \in BOOLEAN :
       /\ filled_lots <= snap_cum
       /\ snap_cum <= orig_lots
       /\ (is_filled_status => snap_cum = orig_lots)
       /\ LET rem_cxl == orig_lots - snap_cum IN
          /\ filled_lots' = snap_cum
          /\ snap_cum_applied' = snap_cum
          /\ last_canceled_lots' = rem_cxl
          /\ wallet_locked_lots' = 0
          /\ wallet_free_lots' = wallet_free_lots + rem_cxl
          /\ slot_live' = FALSE
          /\ order_status' = ST_TERMINAL
          /\ was_open_before_term' = (order_status = ST_OPEN \/ snap_cum > 0)
          /\ terminal_kind' = (IF is_filled_status THEN TERM_FILLED ELSE TERM_CANCELED)
          /\ ClearSelfDesk
          /\ step_cnt' = NextStepCnt
          /\ UNCHANGED <<
               cfg_init_budget_lots, cfg_max_query_limit, now_mono_ns,
               orig_lots, pending_cxl, query_fail_count, retired_via_cap,
               dispatched_coalesced_cxl, peer_desk_occupied, peer_live,
               evicted_live_order, refused_too_many_pending
             >>

(* 11. `reconcile_query_snapshot(NotFound)` (`-2013` / Bybit `110001` / OKX `51603`):
       `PendingNew` with `filled == 0` -> `Rejected(NACK)`; `Open` or `filled > 0` -> `Canceled`
       (`crates/tickwire-account/src/order_manager.rs:2623-2653`, `crates/tickwire-binance-account/src/session.rs:3010-3035`). *)
ReconcileNotFound ==
  /\ StepGuard
  /\ slot_live
  /\ desk_self_occupied
  /\ LET was_open_or_filled == (order_status = ST_OPEN \/ filled_lots > 0)
         rem == orig_lots - filled_lots
     IN
     /\ wallet_locked_lots' = 0
     /\ wallet_free_lots' = wallet_free_lots + rem
     /\ last_canceled_lots' = rem
     /\ snap_cum_applied' = filled_lots
     /\ slot_live' = FALSE
     /\ order_status' = ST_TERMINAL
     /\ was_open_before_term' = was_open_or_filled
     /\ terminal_kind' = (IF was_open_or_filled THEN TERM_CANCELED ELSE TERM_REJECTED_NACK)
     /\ ClearSelfDesk
     /\ step_cnt' = NextStepCnt
     /\ UNCHANGED <<
          cfg_init_budget_lots, cfg_max_query_limit, now_mono_ns, orig_lots,
          filled_lots, pending_cxl, query_fail_count, retired_via_cap,
          dispatched_coalesced_cxl, peer_desk_occupied, peer_live,
          evicted_live_order, refused_too_many_pending
        >>

(* 12. `OrderManager::on_query_inconclusive`: unbounded retry when `~OptInMaxCap`,
       opt-in `INTERNAL_CANCEL` retirement when `OptInMaxCap` and `query_fail_count >= limit`
       (`crates/tickwire-account/src/order_manager.rs:2488-2518`). *)
ReconcileInconclusive ==
  /\ StepGuard
  /\ slot_live
  /\ desk_self_occupied
  /\ LET next_fail == SatAddU64(query_fail_count, 1)
         rem == orig_lots - filled_lots
     IN
     IF OptInMaxCap /\ next_fail >= cfg_max_query_limit
     THEN /\ query_fail_count' = next_fail
          /\ wallet_locked_lots' = 0
          /\ wallet_free_lots' = wallet_free_lots + rem
          /\ last_canceled_lots' = rem
          /\ snap_cum_applied' = filled_lots
          /\ slot_live' = FALSE
          /\ order_status' = ST_TERMINAL
          /\ was_open_before_term' = (order_status = ST_OPEN \/ filled_lots > 0)
          /\ terminal_kind' = TERM_INTERNAL_CANCEL
          /\ retired_via_cap' = TRUE
          /\ ClearSelfDesk
          /\ step_cnt' = NextStepCnt
          /\ UNCHANGED <<
               cfg_init_budget_lots, cfg_max_query_limit, now_mono_ns,
               orig_lots, filled_lots, pending_cxl, dispatched_coalesced_cxl,
               peer_desk_occupied, peer_live, evicted_live_order,
               refused_too_many_pending
             >>
     ELSE /\ query_fail_count' = next_fail
          /\ step_cnt' = NextStepCnt
          /\ UNCHANGED <<
               cfg_init_budget_lots, cfg_max_query_limit, now_mono_ns,
               wallet_free_lots, wallet_locked_lots, slot_live, order_status,
               orig_lots, filled_lots, last_canceled_lots, snap_cum_applied,
               pending_cxl, terminal_kind, was_open_before_term,
               retired_via_cap, dispatched_coalesced_cxl, desk_self_occupied,
               desk_origin, desk_cancel_after, desk_query_attempts,
               desk_cancel_attempts, desk_cancel_exhausted, desk_next_due_ns,
               last_backoff_delay_ns, peer_desk_occupied, peer_live,
               evicted_live_order, refused_too_many_pending,
               merge_witness_active, prev_due_on_merge,
               prev_cancel_after_on_merge, prev_query_att_on_merge,
               prev_cancel_att_on_merge
             >>

(* 13. Monotonic clock progression (`now_mono_ns`). *)
AdvanceClock ==
  /\ StepGuard
  /\ now_mono_ns < EffU64Max
  /\ \E dt \in (IF BoundedMode THEN {1, 2} ELSE 1..EffU64Max) :
       /\ now_mono_ns' = SatAddU64(now_mono_ns, dt)
       /\ step_cnt' = NextStepCnt
       /\ UNCHANGED <<
            cfg_init_budget_lots, cfg_max_query_limit, wallet_free_lots,
            wallet_locked_lots, slot_live, order_status, orig_lots,
            filled_lots, last_canceled_lots, snap_cum_applied, pending_cxl,
            query_fail_count, terminal_kind, was_open_before_term,
            retired_via_cap, dispatched_coalesced_cxl, desk_self_occupied,
            desk_origin, desk_cancel_after, desk_query_attempts,
            desk_cancel_attempts, desk_cancel_exhausted, desk_next_due_ns,
            last_backoff_delay_ns, peer_desk_occupied, peer_live,
            evicted_live_order, refused_too_many_pending, merge_witness_active,
            prev_due_on_merge, prev_cancel_after_on_merge,
            prev_query_att_on_merge, prev_cancel_att_on_merge
          >>

(* 14. Bounded-mode horizon stutter so finite TLC runs never false-deadlock at `MaxBoundedSteps`. *)
BoundedHorizonStutter ==
  /\ BoundedMode
  /\ step_cnt = MaxBoundedSteps
  /\ UNCHANGED vars

Next ==
  \/ SubmitOrder
  \/ PeerOccupyOrRetireInDesk
  \/ ScheduleStatusAskOrEvictNonLive
  \/ ScheduleRefuseFullLivePeer
  \/ CoalesceOntoPendingStatusAsk
  \/ ScheduleOrPollCancelBackoff
  \/ PollDueDispatchStatusAsk
  \/ HotAckOrPartialFill
  \/ ReconcileOpenAndCoalescedCancel
  \/ ReconcileSnapshotTerminalWithCatchup
  \/ ReconcileNotFound
  \/ ReconcileInconclusive
  \/ AdvanceClock
  \/ BoundedHorizonStutter

(* ---- Safety Invariants ---- *)

TypeOK ==
  /\ cfg_init_budget_lots \in 1..EffMaxLots
  /\ cfg_max_query_limit \in 0..EffU64Max
  /\ (IF OptInMaxCap
      THEN 1 <= cfg_max_query_limit
      ELSE cfg_max_query_limit = 0)
  /\ now_mono_ns \in 0..EffU64Max
  /\ wallet_free_lots \in 0..EffMaxLots
  /\ wallet_free_lots <= cfg_init_budget_lots
  /\ wallet_locked_lots \in 0..EffMaxLots
  /\ wallet_locked_lots <= cfg_init_budget_lots
  /\ slot_live \in BOOLEAN
  /\ order_status \in {ST_FREE, ST_PENDING_NEW, ST_OPEN, ST_TERMINAL}
  /\ orig_lots \in 0..EffMaxLots
  /\ orig_lots <= cfg_init_budget_lots
  /\ filled_lots \in 0..EffMaxLots
  /\ filled_lots <= orig_lots
  /\ last_canceled_lots \in 0..EffMaxLots
  /\ last_canceled_lots <= orig_lots
  /\ snap_cum_applied \in 0..EffMaxLots
  /\ snap_cum_applied <= orig_lots
  /\ pending_cxl \in 0..EffU64Max
  /\ query_fail_count \in 0..EffU64Max
  /\ terminal_kind \in {TERM_NONE, TERM_FILLED, TERM_CANCELED, TERM_REJECTED_NACK, TERM_INTERNAL_CANCEL}
  /\ was_open_before_term \in BOOLEAN
  /\ retired_via_cap \in BOOLEAN
  /\ dispatched_coalesced_cxl \in BOOLEAN
  /\ desk_self_occupied \in BOOLEAN
  /\ desk_origin \in {ORG_PLACE, ORG_CANCEL, ORG_SOCKET_DROP}
  /\ desk_cancel_after \in BOOLEAN
  /\ desk_query_attempts \in 0..EffU64Max
  /\ desk_cancel_attempts \in 0..MaxCancelBackoffAttempts
  /\ desk_cancel_exhausted \in BOOLEAN
  /\ desk_next_due_ns \in 0..EffU64Max
  /\ last_backoff_delay_ns \in ValidBackoffDelaySet
  /\ peer_desk_occupied \in BOOLEAN
  /\ peer_live \in BOOLEAN
  /\ evicted_live_order \in BOOLEAN
  /\ refused_too_many_pending \in BOOLEAN
  /\ merge_witness_active \in BOOLEAN
  /\ prev_due_on_merge \in 0..EffU64Max
  /\ prev_cancel_after_on_merge \in BOOLEAN
  /\ prev_query_att_on_merge \in 0..EffU64Max
  /\ prev_cancel_att_on_merge \in 0..MaxCancelBackoffAttempts
  /\ step_cnt \in 0..MaxBoundedSteps

(* Merging a `Cancel` or `SocketDrop` `StatusAsk` never postpones `next_due_mono_ns`,
   latches `cancel_after`, and never resets `query_attempts` or `cancel_attempts`. *)
MonotonicMergeNoPostponement ==
  merge_witness_active =>
    /\ desk_self_occupied
    /\ desk_next_due_ns <= prev_due_on_merge
    /\ (prev_cancel_after_on_merge => desk_cancel_after)
    /\ desk_query_attempts >= prev_query_att_on_merge
    /\ desk_cancel_attempts >= prev_cancel_att_on_merge

(* Exponential cancel backoff is bounded at 8 attempts (`1, 2, 4, 8, 16, 32, 64, 64s`) without `u64` overflow. *)
CancelBackoffBounded ==
  /\ 0 <= desk_cancel_attempts /\ desk_cancel_attempts <= MaxCancelBackoffAttempts
  /\ (desk_cancel_exhausted => desk_cancel_attempts = MaxCancelBackoffAttempts)
  /\ ((desk_self_occupied /\ desk_origin = ORG_CANCEL) => desk_cancel_after)
  /\ last_backoff_delay_ns \in ValidBackoffDelaySet
  /\ 0 <= desk_next_due_ns /\ desk_next_due_ns <= EffU64Max

(* Snapshot catch-up fills are credited strictly before remainder cancel so `filled + canceled == orig_lots`. *)
SnapshotFillsBeforeRemainderCancel ==
  (order_status = ST_TERMINAL) =>
    /\ filled_lots >= snap_cum_applied
    /\ filled_lots + last_canceled_lots = orig_lots
    /\ ~desk_self_occupied

(* `-2013` / `NotFound` emits `Rejected(NACK)` iff uncreated (`PendingNew` with `0` fills);
   previously `Open` or partially filled orders close as `Canceled` preserving fills. *)
UncreatedNotFoundVsOpenNotFoundSound ==
  /\ (terminal_kind = TERM_REJECTED_NACK) =>
       (~was_open_before_term /\ filled_lots = 0 /\ last_canceled_lots = orig_lots)
  /\ ((order_status = ST_TERMINAL /\ was_open_before_term) =>
       terminal_kind /= TERM_REJECTED_NACK)

(* Full-table cleanup never evicts a `Live` entry and enforces capacity-1 mutual exclusion. *)
ActiveLiveProtectedOnEviction ==
  /\ ~evicted_live_order
  /\ ~(desk_self_occupied /\ peer_desk_occupied)

(* Default `~OptInMaxCap` (`None` / `Some(0)`) never emits `INTERNAL_CANCEL` regardless of `query_fail_count`;
   opt-in cap emits `INTERNAL_CANCEL` iff `query_fail_count >= cfg_max_query_limit`. *)
MaxQueryCountContract ==
  /\ (~OptInMaxCap => (terminal_kind /= TERM_INTERNAL_CANCEL /\ ~retired_via_cap))
  /\ ((terminal_kind = TERM_INTERNAL_CANCEL) <=>
       (OptInMaxCap /\ retired_via_cap /\ query_fail_count >= cfg_max_query_limit))

(* Exact 64-bit `UtaWalletLedger` hold and wallet conservation. *)
SolvencyPreserved ==
  /\ wallet_free_lots + wallet_locked_lots + filled_lots = cfg_init_budget_lots
  /\ (slot_live => wallet_locked_lots = orig_lots - filled_lots)
  /\ (~slot_live => wallet_locked_lots = 0)

SafetyConjunction ==
  /\ TypeOK
  /\ MonotonicMergeNoPostponement
  /\ CancelBackoffBounded
  /\ SnapshotFillsBeforeRemainderCancel
  /\ UncreatedNotFoundVsOpenNotFoundSound
  /\ ActiveLiveProtectedOnEviction
  /\ MaxQueryCountContract
  /\ SolvencyPreserved

(* Inductive invariant strengthening `SafetyConjunction` for 1-step Z3 `QF_LIA` induction *)
IndInv ==
  /\ SafetyConjunction
  /\ ((~BoundedMode) => (step_cnt = 0))
  /\ (slot_live <=> (order_status \in {ST_PENDING_NEW, ST_OPEN}))
  /\ ((order_status = ST_FREE) =>
       /\ orig_lots = 0
       /\ filled_lots = 0
       /\ last_canceled_lots = 0
       /\ snap_cum_applied = 0
       /\ pending_cxl = 0
       /\ query_fail_count = 0
       /\ terminal_kind = TERM_NONE
       /\ ~was_open_before_term
       /\ ~retired_via_cap
       /\ ~dispatched_coalesced_cxl
       /\ ~desk_self_occupied)
  /\ ((order_status = ST_PENDING_NEW) =>
       /\ orig_lots >= 1
       /\ filled_lots = 0
       /\ last_canceled_lots = 0
       /\ snap_cum_applied = 0
       /\ terminal_kind = TERM_NONE
       /\ ~was_open_before_term
       /\ ~retired_via_cap
       /\ desk_cancel_attempts = 0
       /\ ~desk_cancel_exhausted)
  /\ ((order_status = ST_OPEN) =>
       /\ orig_lots >= 1
       /\ filled_lots < orig_lots
       /\ last_canceled_lots = 0
       /\ snap_cum_applied = 0
       /\ terminal_kind = TERM_NONE
       /\ ~retired_via_cap)
  /\ ((order_status = ST_TERMINAL) =>
       /\ orig_lots >= 1
       /\ terminal_kind \in {TERM_FILLED, TERM_CANCELED, TERM_REJECTED_NACK, TERM_INTERNAL_CANCEL})
  /\ ((terminal_kind = TERM_FILLED) => (filled_lots = orig_lots /\ last_canceled_lots = 0))
  /\ (retired_via_cap => (order_status = ST_TERMINAL /\ terminal_kind = TERM_INTERNAL_CANCEL))
  /\ ((OptInMaxCap /\ slot_live) => (query_fail_count < cfg_max_query_limit))
  /\ ((~desk_self_occupied) =>
       /\ desk_origin = ORG_PLACE
       /\ ~desk_cancel_after
       /\ desk_query_attempts = 0
       /\ desk_cancel_attempts = 0
       /\ ~desk_cancel_exhausted
       /\ desk_next_due_ns = 0
       /\ last_backoff_delay_ns = 0
       /\ ~merge_witness_active)
  /\ ((~merge_witness_active) =>
       /\ prev_due_on_merge = 0
       /\ ~prev_cancel_after_on_merge
       /\ prev_query_att_on_merge = 0
       /\ prev_cancel_att_on_merge = 0)
  /\ (peer_live => peer_desk_occupied)
====
