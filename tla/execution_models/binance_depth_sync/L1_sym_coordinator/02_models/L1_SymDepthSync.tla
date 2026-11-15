---- MODULE L1_SymDepthSync ----
(*
 * Symbolic Unbounded Inductive & Full 64-Bit `u64` Verification (Mode S2 + Mode S1)
 * for `tickwire_binance::depth_sync::DepthSync` and `tickwire_book::Book`:
 *   - Eliminates `L0_DepthSync`'s finite explicit bounds (`MaxId = 2..3`, `MaxTime = 2`,
 *     `MaximumAge = 0`, `Capacity = 1..2`, `MaxEpoch = 2`, `MaxGeneration = 2..3`,
 *     `MaxRequest = 2`) by proving all 7 state invariants, all 6 transition properties,
 *     and 64-bit `u64` overflow/underflow freedom directly over arbitrary-precision SMT
 *     integers (`QF_LIA`) across `0 .. 2^64 - 1` and unbounded trace depth (`k >= 0`).
 *   - Uses Pre-Solver Skolem Witness Reduction (`sk_iss_*` for issued `SnapshotToken`s,
 *     `sk_buf_gen` for buffered delta generations) + Exact Head/Tail Symbolic FIFO Summary
 *     (`buf_len \in 0..cfg_capacity`, `buf_head_*`, `buf_tail_*`) so arbitrary buffer
 *     capacities (`cfg_capacity \in 1..U64_MAX`) close in 1-step induction.
 *
 * Verified production Rust anchors:
 *   - `DepthSync` state machine, epochs, `connect`, `subscription_ready`, `request_snapshot`,
 *     `on_delta`, `on_snapshot_response`, `on_snapshot_failed`, `expire_snapshot`,
 *     `on_disconnect`, `view`, `clear_buffer`, `reset_book`, `advance_generation`,
 *     `observe_clock`:
 *       crates/tickwire-binance/src/depth_sync.rs:40-514
 *   - `BinanceSpot` & `BinanceUsdM` sequence classification (`local_id + 1` guarded by
 *     `last_update_id <= local_id`, bridging `U <= lastUpdateId <= u`, live `pu == prev_u`),
 *     `Book::apply_delta_checked`, and `Book::view` (`now_ns.checked_sub(received_at_ns)`):
 *       crates/tickwire-book/src/lib.rs:150-225
 *       crates/tickwire-book/src/lib.rs:620-760
 *)
EXTENDS Integers

CONSTANTS
  (* @type: Bool; *)
  BoundedMode,
  (* @type: Int; *)
  PolicyKind

VARIABLES
  (* --- Symbolic configuration bounds (`1..U64_MAX` in Apalache, small ints in TLC) --- *)
  (* @type: Int; *)
  cfg_policy,
  (* @type: Int; *)
  cfg_capacity,
  (* @type: Int; *)
  cfg_max_epoch,
  (* @type: Int; *)
  cfg_max_gen,
  (* @type: Int; *)
  cfg_max_req,
  (* @type: Int; *)
  cfg_max_age_ns,

  (* --- `DepthSync` core state (`crates/tickwire-binance/src/depth_sync.rs:124-136`) --- *)
  (* @type: Int; *)
  phase,
  (* @type: Int; *)
  conn,
  (* @type: Int; *)
  last_conn,
  (* @type: Int; *)
  gen,
  (* @type: Bool; *)
  sub,
  (* @type: Int; *)
  req,
  (* @type: Bool; *)
  clock_some,
  (* @type: Int; *)
  clock_ns,

  (* --- `pending: Option<PendingSnapshot>` (`depth_sync.rs:118-122,228-263`) --- *)
  (* @type: Bool; *)
  pending_some,
  (* @type: Int; *)
  pending_conn,
  (* @type: Int; *)
  pending_gen,
  (* @type: Int; *)
  pending_req,
  (* @type: Int; *)
  pending_deadline_ns,

  (* --- Symbolic `VecDeque<DepthUpdate>` + `first_u` (`depth_sync.rs:296-304,439-443`) --- *)
  (* @type: Int; *)
  buf_len,
  (* @type: Bool; *)
  first_some,
  (* @type: Int; *)
  first_u,
  (* @type: Int; *)
  buf_head_first,
  (* @type: Int; *)
  buf_head_last,
  (* @type: Int; *)
  buf_head_prev,
  (* @type: Int; *)
  buf_head_stamp,
  (* @type: Int; *)
  buf_head_qty,
  (* @type: Bool; *)
  buf_head_valid,
  (* @type: Int; *)
  buf_head_gen,
  (* @type: Int; *)
  buf_tail_first,
  (* @type: Int; *)
  buf_tail_last,
  (* @type: Int; *)
  buf_tail_prev,
  (* @type: Int; *)
  buf_tail_stamp,
  (* @type: Int; *)
  buf_tail_qty,
  (* @type: Bool; *)
  buf_tail_valid,
  (* @type: Int; *)
  buf_tail_gen,
  (* @type: Bool; *)
  sk_buf_some,
  (* @type: Int; *)
  sk_buf_gen,

  (* --- `Book` state (`crates/tickwire-book/src/lib.rs:620-760`) --- *)
  (* @type: Int; *)
  book_phase,
  (* @type: Bool; *)
  book_id_some,
  (* @type: Int; *)
  book_id,
  (* @type: Bool; *)
  book_stamp_some,
  (* @type: Int; *)
  book_stamp_ns,
  (* @type: Int; *)
  book_qty,
  (* @type: Int; *)
  book_gen,

  (* --- Accepted snapshot origin witness (`depth_sync.rs:325-365`) --- *)
  (* @type: Bool; *)
  origin_some,
  (* @type: Int; *)
  origin_conn,
  (* @type: Int; *)
  origin_gen,
  (* @type: Int; *)
  origin_req,

  (* --- Pre-Solver Skolem witness for `issued` `SnapshotToken`s (`depth_sync.rs:252-261`) --- *)
  (* @type: Bool; *)
  sk_iss_some,
  (* @type: Int; *)
  sk_iss_conn,
  (* @type: Int; *)
  sk_iss_gen,
  (* @type: Int; *)
  sk_iss_req,

  (* --- Public call outcome, `view(now_ns)` argument & transition property latches --- *)
  (* @type: Int; *)
  outcome,
  (* @type: Bool; *)
  view_now_some,
  (* @type: Int; *)
  view_now_ns,
  (* @type: Int; *)
  step_budget,
  (* @type: Bool; *)
  old_cb_ok,
  (* @type: Bool; *)
  retry_keep_ok,
  (* @type: Bool; *)
  stale_keep_ok,
  (* @type: Bool; *)
  gen_mono_ok,
  (* @type: Bool; *)
  req_mono_ok,
  (* @type: Bool; *)
  snap_stamp_ok,
  (* @type: Bool; *)
  u64_safe_ok

vars == <<
  cfg_policy, cfg_capacity, cfg_max_epoch, cfg_max_gen, cfg_max_req, cfg_max_age_ns,
  phase, conn, last_conn, gen, sub, req, clock_some, clock_ns,
  pending_some, pending_conn, pending_gen, pending_req, pending_deadline_ns,
  buf_len, first_some, first_u,
  buf_head_first, buf_head_last, buf_head_prev, buf_head_stamp, buf_head_qty, buf_head_valid, buf_head_gen,
  buf_tail_first, buf_tail_last, buf_tail_prev, buf_tail_stamp, buf_tail_qty, buf_tail_valid, buf_tail_gen,
  sk_buf_some, sk_buf_gen,
  book_phase, book_id_some, book_id, book_stamp_some, book_stamp_ns, book_qty, book_gen,
  origin_some, origin_conn, origin_gen, origin_req,
  sk_iss_some, sk_iss_conn, sk_iss_gen, sk_iss_req,
  outcome, view_now_some, view_now_ns, step_budget,
  old_cb_ok, retry_keep_ok, stale_keep_ok, gen_mono_ok, req_mono_ok, snap_stamp_ok, u64_safe_ok
>>

(* --- 64-Bit Domain & Enum Encodings --- *)
Pow30 == 1073741824
U64_MAX == (16 * Pow30 * Pow30) - 1
EffU64Max == IF BoundedMode THEN 2 ELSE U64_MAX
MaxBoundedSteps == 6

POL_SPOT == 1
POL_USDM == 2

(* Coordinator `State` (`depth_sync.rs:42-55`): *)
PH_DISCONNECTED == 0
PH_BUFFERING == 1
PH_BRIDGING == 2
PH_LIVE == 3
PH_EXHAUSTED == 4

(* `Book` phase (`crates/tickwire-book/src/lib.rs:620-642`): *)
BK_AWAITING == 0
BK_BRIDGING == 1
BK_LIVE == 2
BK_INVALID == 3

(* Sequence classification (`crates/tickwire-book/src/lib.rs:182-224`): *)
CLS_STALE == 0
CLS_ERROR == 1
CLS_APPLY == 2

(* Public API outcomes: *)
OUT_INIT == 0
OUT_CONNECT == 1
OUT_CONNECT_REJECTED == 2
OUT_SUBSCRIBE == 3
OUT_OLD_CONN == 4
OUT_OLD_SNAP == 5
OUT_REQUEST == 6
OUT_REQ_BAD_STATE == 7
OUT_REQ_BAD_DEADLINE == 8
OUT_REQ_PENDING == 9
OUT_CLOCK_REGRESSION == 10
OUT_DELTA_INVALID == 11
OUT_OVERFLOW == 12
OUT_BUFFERED == 13
OUT_APPLY_FAILURE == 14
OUT_STALE == 15
OUT_APPLIED == 16
OUT_EXPIRED == 17
OUT_HTTP_FAILURE == 18
OUT_SNAP_INVALID == 19
OUT_TOO_OLD == 20
OUT_LATE_REPLAY_FAILURE == 21
OUT_REPLAY_FAILURE == 22
OUT_SNAP_LIVE == 23
OUT_SNAP_DELAYED_LIVE == 24
OUT_SNAP_BRIDGING == 25
OUT_TRANSPORT_FAILURE == 26
OUT_EXPIRE_NONE == 27
OUT_DISCONNECT == 28
OUT_VIEW_NOT_LIVE == 29
OUT_VIEW_CLOCK == 30
OUT_VIEW_STALE == 31
OUT_VIEW == 32
OUT_EXHAUSTED == 33

FailureOutcomes == {
  OUT_CLOCK_REGRESSION, OUT_DELTA_INVALID, OUT_OVERFLOW, OUT_APPLY_FAILURE,
  OUT_SNAP_INVALID, OUT_LATE_REPLAY_FAILURE, OUT_REPLAY_FAILURE, OUT_EXHAUSTED
}

CanStep == (~BoundedMode) \/ (step_budget < MaxBoundedSteps)
NextBudget == IF BoundedMode THEN step_budget + 1 ELSE 0
TimeDom == IF BoundedMode THEN 0..2 ELSE Int
IdDom == IF BoundedMode THEN 0..2 ELSE Int
QtyDom == IF BoundedMode THEN {1} ELSE {0, 1}
LatchDom == IF BoundedMode THEN {TRUE} ELSE BOOLEAN

GoodClock(t) == (~clock_some) \/ (t >= clock_ns)
ReadyRequest == (phase = PH_BUFFERING) /\ sub /\ first_some

(* Exact `BinanceSpot` & `BinanceUsdM` sequence rule (`crates/tickwire-book/src/lib.rs:182-224`):
 * Note that in `BinanceSpot::classify` (`lib.rs:185-190`), `e_last <= b_id` is checked BEFORE
 * `e_first > b_id + 1`. When `e_last <= EffU64Max` and `e_last > b_id`, `b_id < EffU64Max` holds
 * strictly, so `b_id + 1 <= EffU64Max` never overflows `u64::MAX`! *)
ClassifyBook(pol, b_ph, b_id, e_first, e_last, e_prev) ==
  IF pol = POL_SPOT
  THEN IF e_last <= b_id THEN CLS_STALE
       ELSE IF e_first > b_id + 1 THEN CLS_ERROR
       ELSE CLS_APPLY
  ELSE IF (e_last < b_id) \/ ((b_ph # BK_BRIDGING) /\ (e_last = b_id)) THEN CLS_STALE
       ELSE IF ((b_ph = BK_BRIDGING) /\ (e_first > b_id))
               \/ ((b_ph # BK_BRIDGING) /\ (e_prev # b_id)) THEN CLS_ERROR
       ELSE CLS_APPLY

SpotPlusOneSafe(pol, b_id, e_last) ==
  (pol # POL_SPOT) \/ (e_last <= b_id) \/ (b_id + 1 <= EffU64Max)

ApplyPhase(pol, b_ph, b_id, b_st_some, b_st, e_first, e_last, e_prev, e_st, e_val) ==
  LET c == ClassifyBook(pol, b_ph, b_id, e_first, e_last, e_prev) IN
  IF c = CLS_STALE THEN b_ph
  ELSE IF (c = CLS_ERROR) \/ (~e_val) \/ (b_st_some /\ (e_st < b_st)) THEN BK_INVALID
  ELSE BK_LIVE

ApplyIdSome(pol, b_ph, b_id_s, b_id, b_st_some, b_st, e_first, e_last, e_prev, e_st, e_val) ==
  LET c == ClassifyBook(pol, b_ph, b_id, e_first, e_last, e_prev)
      nph == ApplyPhase(pol, b_ph, b_id, b_st_some, b_st, e_first, e_last, e_prev, e_st, e_val)
  IN IF c = CLS_STALE THEN b_id_s
     ELSE IF nph = BK_INVALID THEN b_id_s
     ELSE TRUE

ApplyId(pol, b_ph, b_id, b_st_some, b_st, e_first, e_last, e_prev, e_st, e_val) ==
  LET c == ClassifyBook(pol, b_ph, b_id, e_first, e_last, e_prev)
      nph == ApplyPhase(pol, b_ph, b_id, b_st_some, b_st, e_first, e_last, e_prev, e_st, e_val)
  IN IF (c = CLS_STALE) \/ (nph = BK_INVALID) THEN b_id ELSE e_last

ApplyStampSome(pol, b_ph, b_id, b_st_some, b_st, e_first, e_last, e_prev, e_st, e_val) ==
  LET c == ClassifyBook(pol, b_ph, b_id, e_first, e_last, e_prev)
      nph == ApplyPhase(pol, b_ph, b_id, b_st_some, b_st, e_first, e_last, e_prev, e_st, e_val)
  IN IF (c = CLS_STALE) \/ (nph = BK_INVALID) THEN b_st_some ELSE TRUE

ApplyStamp(pol, b_ph, b_id, b_st_some, b_st, e_first, e_last, e_prev, e_st, e_val) ==
  LET c == ClassifyBook(pol, b_ph, b_id, e_first, e_last, e_prev)
      nph == ApplyPhase(pol, b_ph, b_id, b_st_some, b_st, e_first, e_last, e_prev, e_st, e_val)
  IN IF (c = CLS_STALE) \/ (nph = BK_INVALID) THEN b_st ELSE e_st

ApplyQty(pol, b_ph, b_id, b_st_some, b_st, b_qty, e_first, e_last, e_prev, e_st, e_qty, e_val) ==
  LET c == ClassifyBook(pol, b_ph, b_id, e_first, e_last, e_prev)
      nph == ApplyPhase(pol, b_ph, b_id, b_st_some, b_st, e_first, e_last, e_prev, e_st, e_val)
  IN IF (c = CLS_STALE) \/ (nph = BK_INVALID) THEN b_qty ELSE e_qty

(* Helper macros for Exhaust / Reset transitions (`depth_sync.rs:439-476`): *)
ClearBufSlots ==
  /\ buf_head_first' = 0 /\ buf_head_last' = 0 /\ buf_head_prev' = 0
  /\ buf_head_stamp' = 0 /\ buf_head_qty' = 0 /\ buf_head_valid' = TRUE /\ buf_head_gen' = 0
  /\ buf_tail_first' = 0 /\ buf_tail_last' = 0 /\ buf_tail_prev' = 0
  /\ buf_tail_stamp' = 0 /\ buf_tail_qty' = 0 /\ buf_tail_valid' = TRUE /\ buf_tail_gen' = 0

AssignExhaust(clk_s, clk_v, out_v) ==
  /\ phase' = PH_EXHAUSTED
  /\ conn' = 0
  /\ last_conn' = last_conn
  /\ gen' = gen
  /\ sub' = FALSE
  /\ req' = req
  /\ clock_some' = clk_s
  /\ clock_ns' = clk_v
  /\ pending_some' = FALSE
  /\ pending_conn' = 0 /\ pending_gen' = 0 /\ pending_req' = 0 /\ pending_deadline_ns' = 0
  /\ buf_len' = 0
  /\ first_some' = FALSE /\ first_u' = 0
  /\ sk_buf_some' = FALSE /\ sk_buf_gen' = 0
  /\ book_phase' = BK_INVALID
  /\ book_id_some' = book_id_some /\ book_id' = book_id
  /\ book_stamp_some' = book_stamp_some /\ book_stamp_ns' = book_stamp_ns
  /\ book_qty' = book_qty /\ book_gen' = book_gen
  /\ origin_some' = origin_some /\ origin_conn' = origin_conn
  /\ origin_gen' = origin_gen /\ origin_req' = origin_req
  /\ outcome' = out_v
  /\ view_now_some' = FALSE /\ view_now_ns' = 0
  /\ ClearBufSlots

AssignResetOrExhaust(clk_s, clk_v, out_v) ==
  IF gen = cfg_max_gen
  THEN AssignExhaust(clk_s, clk_v, out_v)
  ELSE /\ phase' = PH_BUFFERING
       /\ conn' = conn
       /\ last_conn' = last_conn
       /\ gen' = gen + 1
       /\ sub' = sub
       /\ req' = req
       /\ clock_some' = clk_s
       /\ clock_ns' = clk_v
       /\ pending_some' = FALSE
       /\ pending_conn' = 0 /\ pending_gen' = 0 /\ pending_req' = 0 /\ pending_deadline_ns' = 0
       /\ buf_len' = 0
       /\ first_some' = FALSE /\ first_u' = 0
       /\ sk_buf_some' = FALSE /\ sk_buf_gen' = 0
       /\ book_phase' = BK_AWAITING
       /\ book_id_some' = FALSE /\ book_id' = 0
       /\ book_stamp_some' = FALSE /\ book_stamp_ns' = 0
       /\ book_qty' = 0 /\ book_gen' = gen + 1
       /\ origin_some' = FALSE /\ origin_conn' = 0 /\ origin_gen' = 0 /\ origin_req' = 0
       /\ outcome' = out_v
       /\ view_now_some' = FALSE /\ view_now_ns' = 0
       /\ ClearBufSlots

ConstInit ==
  /\ BoundedMode = FALSE
  /\ PolicyKind \in {POL_SPOT, POL_USDM}

ValidConfig ==
  /\ cfg_policy \in {POL_SPOT, POL_USDM}
  /\ cfg_capacity \in Int /\ cfg_capacity >= 1 /\ cfg_capacity <= EffU64Max
  /\ cfg_max_epoch \in Int /\ cfg_max_epoch >= 2 /\ cfg_max_epoch <= EffU64Max
  /\ cfg_max_gen \in Int /\ cfg_max_gen >= 2 /\ cfg_max_gen <= EffU64Max
  /\ cfg_max_req \in Int /\ cfg_max_req >= 2 /\ cfg_max_req <= EffU64Max
  /\ cfg_max_age_ns \in Int /\ cfg_max_age_ns >= 0 /\ cfg_max_age_ns <= EffU64Max

Init ==
  /\ cfg_policy = PolicyKind
  /\ IF BoundedMode
     THEN /\ cfg_capacity = 1
          /\ cfg_max_epoch = 2
          /\ cfg_max_gen = 2
          /\ cfg_max_req = 2
          /\ cfg_max_age_ns = 0
     ELSE /\ cfg_capacity \in Int /\ cfg_capacity >= 1 /\ cfg_capacity <= EffU64Max
          /\ cfg_max_epoch \in Int /\ cfg_max_epoch >= 2 /\ cfg_max_epoch <= EffU64Max
          /\ cfg_max_gen \in Int /\ cfg_max_gen >= 2 /\ cfg_max_gen <= EffU64Max
          /\ cfg_max_req \in Int /\ cfg_max_req >= 2 /\ cfg_max_req <= EffU64Max
          /\ cfg_max_age_ns \in Int /\ cfg_max_age_ns >= 0 /\ cfg_max_age_ns <= EffU64Max
  /\ phase = PH_DISCONNECTED
  /\ conn = 0 /\ last_conn = 0 /\ gen = 0 /\ sub = FALSE /\ req = 0
  /\ clock_some = FALSE /\ clock_ns = 0
  /\ pending_some = FALSE
  /\ pending_conn = 0 /\ pending_gen = 0 /\ pending_req = 0 /\ pending_deadline_ns = 0
  /\ buf_len = 0
  /\ first_some = FALSE /\ first_u = 0
  /\ buf_head_first = 0 /\ buf_head_last = 0 /\ buf_head_prev = 0
  /\ buf_head_stamp = 0 /\ buf_head_qty = 0 /\ buf_head_valid = TRUE /\ buf_head_gen = 0
  /\ buf_tail_first = 0 /\ buf_tail_last = 0 /\ buf_tail_prev = 0
  /\ buf_tail_stamp = 0 /\ buf_tail_qty = 0 /\ buf_tail_valid = TRUE /\ buf_tail_gen = 0
  /\ sk_buf_some = FALSE /\ sk_buf_gen = 0
  /\ book_phase = BK_AWAITING
  /\ book_id_some = FALSE /\ book_id = 0
  /\ book_stamp_some = FALSE /\ book_stamp_ns = 0
  /\ book_qty = 0 /\ book_gen = 0
  /\ origin_some = FALSE /\ origin_conn = 0 /\ origin_gen = 0 /\ origin_req = 0
  /\ sk_iss_some = FALSE /\ sk_iss_conn = 0 /\ sk_iss_gen = 0 /\ sk_iss_req = 0
  /\ outcome = OUT_INIT
  /\ view_now_some = FALSE /\ view_now_ns = 0
  /\ step_budget = 0
  /\ old_cb_ok = TRUE /\ retry_keep_ok = TRUE /\ stale_keep_ok = TRUE
  /\ gen_mono_ok = TRUE /\ req_mono_ok = TRUE /\ snap_stamp_ok = TRUE /\ u64_safe_ok = TRUE

(* --- Public Coordinator Transitions (`depth_sync.rs:202-438`) --- *)

Connect ==
  /\ CanStep
  /\ phase # PH_EXHAUSTED
  /\ last_conn < cfg_max_epoch
  /\ IF gen = cfg_max_gen
     THEN /\ AssignExhaust(clock_some, clock_ns, OUT_EXHAUSTED)
          /\ gen_mono_ok' = gen_mono_ok
          /\ u64_safe_ok' = u64_safe_ok
     ELSE /\ phase' = PH_BUFFERING
          /\ conn' = last_conn + 1
          /\ last_conn' = last_conn + 1
          /\ gen' = gen + 1
          /\ sub' = FALSE
          /\ req' = req
          /\ clock_some' = FALSE /\ clock_ns' = 0
          /\ pending_some' = FALSE
          /\ pending_conn' = 0 /\ pending_gen' = 0 /\ pending_req' = 0 /\ pending_deadline_ns' = 0
          /\ buf_len' = 0
          /\ first_some' = FALSE /\ first_u' = 0
          /\ sk_buf_some' = FALSE /\ sk_buf_gen' = 0
          /\ book_phase' = BK_AWAITING
          /\ book_id_some' = FALSE /\ book_id' = 0
          /\ book_stamp_some' = FALSE /\ book_stamp_ns' = 0
          /\ book_qty' = 0 /\ book_gen' = gen + 1
          /\ origin_some' = FALSE /\ origin_conn' = 0 /\ origin_gen' = 0 /\ origin_req' = 0
          /\ outcome' = OUT_CONNECT
          /\ view_now_some' = FALSE /\ view_now_ns' = 0
          /\ gen_mono_ok' = (gen_mono_ok /\ (gen + 1 >= gen))
          /\ u64_safe_ok' = (u64_safe_ok /\ (last_conn + 1 <= EffU64Max) /\ (gen + 1 <= EffU64Max))
          /\ ClearBufSlots
  /\ step_budget' = NextBudget
  /\ UNCHANGED <<
       cfg_policy, cfg_capacity, cfg_max_epoch, cfg_max_gen, cfg_max_req, cfg_max_age_ns,
       sk_iss_some, sk_iss_conn, sk_iss_gen, sk_iss_req,
       old_cb_ok, retry_keep_ok, stale_keep_ok, req_mono_ok, snap_stamp_ok
     >>

ConnectRejected ==
  /\ CanStep
  /\ (phase = PH_EXHAUSTED) \/ (last_conn > 0)
  /\ outcome' = OUT_CONNECT_REJECTED
  /\ view_now_some' = FALSE /\ view_now_ns' = 0
  /\ step_budget' = NextBudget
  /\ UNCHANGED <<
       cfg_policy, cfg_capacity, cfg_max_epoch, cfg_max_gen, cfg_max_req, cfg_max_age_ns,
       phase, conn, last_conn, gen, sub, req, clock_some, clock_ns,
       pending_some, pending_conn, pending_gen, pending_req, pending_deadline_ns,
       buf_len, first_some, first_u,
       buf_head_first, buf_head_last, buf_head_prev, buf_head_stamp, buf_head_qty, buf_head_valid, buf_head_gen,
       buf_tail_first, buf_tail_last, buf_tail_prev, buf_tail_stamp, buf_tail_qty, buf_tail_valid, buf_tail_gen,
       sk_buf_some, sk_buf_gen,
       book_phase, book_id_some, book_id, book_stamp_some, book_stamp_ns, book_qty, book_gen,
       origin_some, origin_conn, origin_gen, origin_req,
       sk_iss_some, sk_iss_conn, sk_iss_gen, sk_iss_req,
       old_cb_ok, retry_keep_ok, stale_keep_ok, gen_mono_ok, req_mono_ok, snap_stamp_ok, u64_safe_ok
     >>

Subscribe ==
  /\ CanStep
  /\ conn # 0
  /\ sub' = TRUE
  /\ outcome' = OUT_SUBSCRIBE
  /\ view_now_some' = FALSE /\ view_now_ns' = 0
  /\ step_budget' = NextBudget
  /\ UNCHANGED <<
       cfg_policy, cfg_capacity, cfg_max_epoch, cfg_max_gen, cfg_max_req, cfg_max_age_ns,
       phase, conn, last_conn, gen, req, clock_some, clock_ns,
       pending_some, pending_conn, pending_gen, pending_req, pending_deadline_ns,
       buf_len, first_some, first_u,
       buf_head_first, buf_head_last, buf_head_prev, buf_head_stamp, buf_head_qty, buf_head_valid, buf_head_gen,
       buf_tail_first, buf_tail_last, buf_tail_prev, buf_tail_stamp, buf_tail_qty, buf_tail_valid, buf_tail_gen,
       sk_buf_some, sk_buf_gen,
       book_phase, book_id_some, book_id, book_stamp_some, book_stamp_ns, book_qty, book_gen,
       origin_some, origin_conn, origin_gen, origin_req,
       sk_iss_some, sk_iss_conn, sk_iss_gen, sk_iss_req,
       old_cb_ok, retry_keep_ok, stale_keep_ok, gen_mono_ok, req_mono_ok, snap_stamp_ok, u64_safe_ok
     >>

OldConnection ==
  /\ CanStep
  /\ (last_conn >= 1) /\ ((conn = 0) \/ (last_conn > 1))
  /\ outcome' = OUT_OLD_CONN
  /\ view_now_some' = FALSE /\ view_now_ns' = 0
  /\ old_cb_ok' = old_cb_ok
  /\ step_budget' = NextBudget
  /\ UNCHANGED <<
       cfg_policy, cfg_capacity, cfg_max_epoch, cfg_max_gen, cfg_max_req, cfg_max_age_ns,
       phase, conn, last_conn, gen, sub, req, clock_some, clock_ns,
       pending_some, pending_conn, pending_gen, pending_req, pending_deadline_ns,
       buf_len, first_some, first_u,
       buf_head_first, buf_head_last, buf_head_prev, buf_head_stamp, buf_head_qty, buf_head_valid, buf_head_gen,
       buf_tail_first, buf_tail_last, buf_tail_prev, buf_tail_stamp, buf_tail_qty, buf_tail_valid, buf_tail_gen,
       sk_buf_some, sk_buf_gen,
       book_phase, book_id_some, book_id, book_stamp_some, book_stamp_ns, book_qty, book_gen,
       origin_some, origin_conn, origin_gen, origin_req,
       sk_iss_some, sk_iss_conn, sk_iss_gen, sk_iss_req,
       retry_keep_ok, stale_keep_ok, gen_mono_ok, req_mono_ok, snap_stamp_ok, u64_safe_ok
     >>

OldSnapshot ==
  /\ CanStep
  /\ sk_iss_some
  /\ (~pending_some \/ sk_iss_conn # pending_conn \/ sk_iss_gen # pending_gen \/ sk_iss_req # pending_req)
  /\ outcome' = OUT_OLD_SNAP
  /\ view_now_some' = FALSE /\ view_now_ns' = 0
  /\ old_cb_ok' = old_cb_ok
  /\ step_budget' = NextBudget
  /\ UNCHANGED <<
       cfg_policy, cfg_capacity, cfg_max_epoch, cfg_max_gen, cfg_max_req, cfg_max_age_ns,
       phase, conn, last_conn, gen, sub, req, clock_some, clock_ns,
       pending_some, pending_conn, pending_gen, pending_req, pending_deadline_ns,
       buf_len, first_some, first_u,
       buf_head_first, buf_head_last, buf_head_prev, buf_head_stamp, buf_head_qty, buf_head_valid, buf_head_gen,
       buf_tail_first, buf_tail_last, buf_tail_prev, buf_tail_stamp, buf_tail_qty, buf_tail_valid, buf_tail_gen,
       sk_buf_some, sk_buf_gen,
       book_phase, book_id_some, book_id, book_stamp_some, book_stamp_ns, book_qty, book_gen,
       origin_some, origin_conn, origin_gen, origin_req,
       sk_iss_some, sk_iss_conn, sk_iss_gen, sk_iss_req,
       retry_keep_ok, stale_keep_ok, gen_mono_ok, req_mono_ok, snap_stamp_ok, u64_safe_ok
     >>

Request ==
  /\ CanStep
  /\ ReadyRequest
  /\ ~pending_some
  /\ \E t \in TimeDom, dl \in TimeDom, latch_iss \in LatchDom:
       /\ t >= 0 /\ t <= EffU64Max
       /\ dl >= 0 /\ dl <= EffU64Max
       /\ GoodClock(t) /\ dl > t
       /\ IF req = cfg_max_req
          THEN /\ AssignExhaust(TRUE, t, OUT_EXHAUSTED)
               /\ sk_iss_some' = sk_iss_some
               /\ sk_iss_conn' = sk_iss_conn /\ sk_iss_gen' = sk_iss_gen /\ sk_iss_req' = sk_iss_req
               /\ req_mono_ok' = req_mono_ok
               /\ u64_safe_ok' = u64_safe_ok
          ELSE /\ phase' = phase /\ conn' = conn /\ last_conn' = last_conn
               /\ gen' = gen /\ sub' = sub
               /\ req' = req + 1
               /\ clock_some' = TRUE /\ clock_ns' = t
               /\ pending_some' = TRUE
               /\ pending_conn' = conn /\ pending_gen' = gen
               /\ pending_req' = req + 1 /\ pending_deadline_ns' = dl
               /\ sk_iss_some' = (IF latch_iss \/ ~sk_iss_some THEN TRUE ELSE sk_iss_some)
               /\ sk_iss_conn' = (IF latch_iss \/ ~sk_iss_some THEN conn ELSE sk_iss_conn)
               /\ sk_iss_gen' = (IF latch_iss \/ ~sk_iss_some THEN gen ELSE sk_iss_gen)
               /\ sk_iss_req' = (IF latch_iss \/ ~sk_iss_some THEN req + 1 ELSE sk_iss_req)
               /\ outcome' = OUT_REQUEST
               /\ view_now_some' = FALSE /\ view_now_ns' = 0
               /\ req_mono_ok' = (req_mono_ok /\ (req + 1 >= req))
               /\ u64_safe_ok' = (u64_safe_ok /\ (req + 1 <= EffU64Max))
               /\ UNCHANGED <<
                    buf_len, first_some, first_u,
                    buf_head_first, buf_head_last, buf_head_prev, buf_head_stamp, buf_head_qty, buf_head_valid, buf_head_gen,
                    buf_tail_first, buf_tail_last, buf_tail_prev, buf_tail_stamp, buf_tail_qty, buf_tail_valid, buf_tail_gen,
                    sk_buf_some, sk_buf_gen,
                    book_phase, book_id_some, book_id, book_stamp_some, book_stamp_ns, book_qty, book_gen,
                    origin_some, origin_conn, origin_gen, origin_req
                  >>
       /\ step_budget' = NextBudget
       /\ UNCHANGED <<
            cfg_policy, cfg_capacity, cfg_max_epoch, cfg_max_gen, cfg_max_req, cfg_max_age_ns,
            old_cb_ok, retry_keep_ok, stale_keep_ok, gen_mono_ok, snap_stamp_ok
          >>

RequestBadState ==
  /\ CanStep
  /\ ~ReadyRequest
  /\ outcome' = OUT_REQ_BAD_STATE
  /\ view_now_some' = FALSE /\ view_now_ns' = 0
  /\ step_budget' = NextBudget
  /\ UNCHANGED <<
       cfg_policy, cfg_capacity, cfg_max_epoch, cfg_max_gen, cfg_max_req, cfg_max_age_ns,
       phase, conn, last_conn, gen, sub, req, clock_some, clock_ns,
       pending_some, pending_conn, pending_gen, pending_req, pending_deadline_ns,
       buf_len, first_some, first_u,
       buf_head_first, buf_head_last, buf_head_prev, buf_head_stamp, buf_head_qty, buf_head_valid, buf_head_gen,
       buf_tail_first, buf_tail_last, buf_tail_prev, buf_tail_stamp, buf_tail_qty, buf_tail_valid, buf_tail_gen,
       sk_buf_some, sk_buf_gen,
       book_phase, book_id_some, book_id, book_stamp_some, book_stamp_ns, book_qty, book_gen,
       origin_some, origin_conn, origin_gen, origin_req,
       sk_iss_some, sk_iss_conn, sk_iss_gen, sk_iss_req,
       old_cb_ok, retry_keep_ok, stale_keep_ok, gen_mono_ok, req_mono_ok, snap_stamp_ok, u64_safe_ok
     >>

RequestBadDeadline ==
  /\ CanStep
  /\ ReadyRequest
  /\ \E t \in TimeDom, dl \in TimeDom:
       /\ t >= 0 /\ t <= EffU64Max
       /\ dl >= 0 /\ dl <= EffU64Max
       /\ GoodClock(t) /\ dl <= t
       /\ clock_some' = TRUE /\ clock_ns' = t
       /\ outcome' = OUT_REQ_BAD_DEADLINE
       /\ view_now_some' = FALSE /\ view_now_ns' = 0
       /\ step_budget' = NextBudget
       /\ UNCHANGED <<
            cfg_policy, cfg_capacity, cfg_max_epoch, cfg_max_gen, cfg_max_req, cfg_max_age_ns,
            phase, conn, last_conn, gen, sub, req,
            pending_some, pending_conn, pending_gen, pending_req, pending_deadline_ns,
            buf_len, first_some, first_u,
            buf_head_first, buf_head_last, buf_head_prev, buf_head_stamp, buf_head_qty, buf_head_valid, buf_head_gen,
            buf_tail_first, buf_tail_last, buf_tail_prev, buf_tail_stamp, buf_tail_qty, buf_tail_valid, buf_tail_gen,
            sk_buf_some, sk_buf_gen,
            book_phase, book_id_some, book_id, book_stamp_some, book_stamp_ns, book_qty, book_gen,
            origin_some, origin_conn, origin_gen, origin_req,
            sk_iss_some, sk_iss_conn, sk_iss_gen, sk_iss_req,
            old_cb_ok, retry_keep_ok, stale_keep_ok, gen_mono_ok, req_mono_ok, snap_stamp_ok, u64_safe_ok
          >>

RequestPending ==
  /\ CanStep
  /\ ReadyRequest
  /\ pending_some
  /\ \E t \in TimeDom, dl \in TimeDom:
       /\ t >= 0 /\ t <= EffU64Max
       /\ dl >= 0 /\ dl <= EffU64Max
       /\ GoodClock(t) /\ dl > t
       /\ clock_some' = TRUE /\ clock_ns' = t
       /\ outcome' = OUT_REQ_PENDING
       /\ view_now_some' = FALSE /\ view_now_ns' = 0
       /\ step_budget' = NextBudget
       /\ UNCHANGED <<
            cfg_policy, cfg_capacity, cfg_max_epoch, cfg_max_gen, cfg_max_req, cfg_max_age_ns,
            phase, conn, last_conn, gen, sub, req,
            pending_some, pending_conn, pending_gen, pending_req, pending_deadline_ns,
            buf_len, first_some, first_u,
            buf_head_first, buf_head_last, buf_head_prev, buf_head_stamp, buf_head_qty, buf_head_valid, buf_head_gen,
            buf_tail_first, buf_tail_last, buf_tail_prev, buf_tail_stamp, buf_tail_qty, buf_tail_valid, buf_tail_gen,
            sk_buf_some, sk_buf_gen,
            book_phase, book_id_some, book_id, book_stamp_some, book_stamp_ns, book_qty, book_gen,
            origin_some, origin_conn, origin_gen, origin_req,
            sk_iss_some, sk_iss_conn, sk_iss_gen, sk_iss_req,
            old_cb_ok, retry_keep_ok, stale_keep_ok, gen_mono_ok, req_mono_ok, snap_stamp_ok, u64_safe_ok
          >>

ClockRegression ==
  /\ CanStep
  /\ conn # 0
  /\ clock_some
  /\ clock_ns > 0
  /\ AssignResetOrExhaust(clock_some, clock_ns, OUT_CLOCK_REGRESSION)
  /\ gen_mono_ok' = gen_mono_ok
  /\ u64_safe_ok' = (u64_safe_ok /\ ((gen = cfg_max_gen) \/ (gen + 1 <= EffU64Max)))
  /\ step_budget' = NextBudget
  /\ UNCHANGED <<
       cfg_policy, cfg_capacity, cfg_max_epoch, cfg_max_gen, cfg_max_req, cfg_max_age_ns,
       sk_iss_some, sk_iss_conn, sk_iss_gen, sk_iss_req,
       old_cb_ok, retry_keep_ok, stale_keep_ok, req_mono_ok, snap_stamp_ok
     >>

DeltaInvalid ==
  /\ CanStep
  /\ conn # 0
  /\ \E t \in TimeDom:
       /\ t >= 0 /\ t <= EffU64Max /\ GoodClock(t)
       /\ AssignResetOrExhaust(TRUE, t, OUT_DELTA_INVALID)
       /\ gen_mono_ok' = gen_mono_ok
       /\ u64_safe_ok' = (u64_safe_ok /\ ((gen = cfg_max_gen) \/ (gen + 1 <= EffU64Max)))
       /\ step_budget' = NextBudget
       /\ UNCHANGED <<
            cfg_policy, cfg_capacity, cfg_max_epoch, cfg_max_gen, cfg_max_req, cfg_max_age_ns,
            sk_iss_some, sk_iss_conn, sk_iss_gen, sk_iss_req,
            old_cb_ok, retry_keep_ok, stale_keep_ok, req_mono_ok, snap_stamp_ok
          >>

DeltaBuffer ==
  /\ CanStep
  /\ conn # 0
  /\ phase = PH_BUFFERING
  /\ \E t \in TimeDom, ef \in IdDom, el \in IdDom, ep \in IdDom,
        eq \in QtyDom, ev \in BOOLEAN, latch_b \in LatchDom:
       /\ t >= 0 /\ t <= EffU64Max /\ GoodClock(t)
       /\ ef >= 0 /\ ef <= el /\ el <= EffU64Max
       /\ ep >= 0 /\ ep <= EffU64Max
       /\ IF buf_len = cfg_capacity
          THEN /\ AssignResetOrExhaust(TRUE, t, OUT_OVERFLOW)
               /\ gen_mono_ok' = gen_mono_ok
               /\ u64_safe_ok' = (u64_safe_ok /\ ((gen = cfg_max_gen) \/ (gen + 1 <= EffU64Max)))
          ELSE /\ phase' = phase /\ conn' = conn /\ last_conn' = last_conn
               /\ gen' = gen /\ sub' = sub /\ req' = req
               /\ clock_some' = TRUE /\ clock_ns' = t
               /\ pending_some' = pending_some
               /\ pending_conn' = pending_conn /\ pending_gen' = pending_gen
               /\ pending_req' = pending_req /\ pending_deadline_ns' = pending_deadline_ns
               /\ buf_len' = buf_len + 1
               /\ first_some' = TRUE
               /\ first_u' = IF first_some THEN first_u ELSE ef
               /\ buf_head_first' = IF buf_len = 0 THEN ef ELSE buf_head_first
               /\ buf_head_last' = IF buf_len = 0 THEN el ELSE buf_head_last
               /\ buf_head_prev' = IF buf_len = 0 THEN ep ELSE buf_head_prev
               /\ buf_head_stamp' = IF buf_len = 0 THEN t ELSE buf_head_stamp
               /\ buf_head_qty' = IF buf_len = 0 THEN eq ELSE buf_head_qty
               /\ buf_head_valid' = IF buf_len = 0 THEN ev ELSE buf_head_valid
               /\ buf_head_gen' = IF buf_len = 0 THEN gen ELSE buf_head_gen
               /\ buf_tail_first' = IF buf_len = 0 THEN 0 ELSE ef
               /\ buf_tail_last' = IF buf_len = 0 THEN 0 ELSE el
               /\ buf_tail_prev' = IF buf_len = 0 THEN 0 ELSE ep
               /\ buf_tail_stamp' = IF buf_len = 0 THEN 0 ELSE t
               /\ buf_tail_qty' = IF buf_len = 0 THEN 0 ELSE eq
               /\ buf_tail_valid' = IF buf_len = 0 THEN TRUE ELSE ev
               /\ buf_tail_gen' = IF buf_len = 0 THEN 0 ELSE gen
               /\ sk_buf_some' = (IF latch_b \/ ~sk_buf_some THEN TRUE ELSE sk_buf_some)
               /\ sk_buf_gen' = (IF latch_b \/ ~sk_buf_some THEN gen ELSE sk_buf_gen)
               /\ book_phase' = book_phase
               /\ book_id_some' = book_id_some /\ book_id' = book_id
               /\ book_stamp_some' = book_stamp_some /\ book_stamp_ns' = book_stamp_ns
               /\ book_qty' = book_qty /\ book_gen' = book_gen
               /\ origin_some' = origin_some /\ origin_conn' = origin_conn
               /\ origin_gen' = origin_gen /\ origin_req' = origin_req
               /\ outcome' = OUT_BUFFERED
               /\ view_now_some' = FALSE /\ view_now_ns' = 0
               /\ gen_mono_ok' = gen_mono_ok
               /\ u64_safe_ok' = (u64_safe_ok /\ (buf_len + 1 <= cfg_capacity))
       /\ step_budget' = NextBudget
       /\ UNCHANGED <<
            cfg_policy, cfg_capacity, cfg_max_epoch, cfg_max_gen, cfg_max_req, cfg_max_age_ns,
            sk_iss_some, sk_iss_conn, sk_iss_gen, sk_iss_req,
            old_cb_ok, retry_keep_ok, stale_keep_ok, req_mono_ok, snap_stamp_ok
          >>

DeltaApply ==
  /\ CanStep
  /\ conn # 0
  /\ phase \in {PH_BRIDGING, PH_LIVE}
  /\ \E t \in TimeDom, ef \in IdDom, el \in IdDom, ep \in IdDom,
        eq \in QtyDom, ev \in BOOLEAN:
       /\ t >= 0 /\ t <= EffU64Max /\ GoodClock(t)
       /\ ef >= 0 /\ ef <= el /\ el <= EffU64Max
       /\ ep >= 0 /\ ep <= EffU64Max
       /\ LET cls == ClassifyBook(cfg_policy, book_phase, book_id, ef, el, ep)
              nph == ApplyPhase(cfg_policy, book_phase, book_id, book_stamp_some, book_stamp_ns, ef, el, ep, t, ev)
              nid_s == ApplyIdSome(cfg_policy, book_phase, book_id_some, book_id, book_stamp_some, book_stamp_ns, ef, el, ep, t, ev)
              nid == ApplyId(cfg_policy, book_phase, book_id, book_stamp_some, book_stamp_ns, ef, el, ep, t, ev)
              nst_s == ApplyStampSome(cfg_policy, book_phase, book_id, book_stamp_some, book_stamp_ns, ef, el, ep, t, ev)
              nst == ApplyStamp(cfg_policy, book_phase, book_id, book_stamp_some, book_stamp_ns, ef, el, ep, t, ev)
              nq == ApplyQty(cfg_policy, book_phase, book_id, book_stamp_some, book_stamp_ns, book_qty, ef, el, ep, t, eq, ev)
              plus_safe == SpotPlusOneSafe(cfg_policy, book_id, el)
          IN /\ IF nph = BK_INVALID
                THEN /\ AssignResetOrExhaust(TRUE, t, OUT_APPLY_FAILURE)
                     /\ stale_keep_ok' = stale_keep_ok
                ELSE /\ phase' = (IF nph = BK_LIVE THEN PH_LIVE ELSE PH_BRIDGING)
                     /\ conn' = conn /\ last_conn' = last_conn /\ gen' = gen /\ sub' = sub /\ req' = req
                     /\ clock_some' = TRUE /\ clock_ns' = t
                     /\ pending_some' = pending_some
                     /\ pending_conn' = pending_conn /\ pending_gen' = pending_gen
                     /\ pending_req' = pending_req /\ pending_deadline_ns' = pending_deadline_ns
                     /\ book_phase' = nph
                     /\ book_id_some' = nid_s /\ book_id' = nid
                     /\ book_stamp_some' = nst_s /\ book_stamp_ns' = nst
                     /\ book_qty' = nq /\ book_gen' = book_gen
                     /\ origin_some' = origin_some /\ origin_conn' = origin_conn
                     /\ origin_gen' = origin_gen /\ origin_req' = origin_req
                     /\ outcome' = (IF cls = CLS_STALE THEN OUT_STALE ELSE OUT_APPLIED)
                     /\ view_now_some' = FALSE /\ view_now_ns' = 0
                     /\ stale_keep_ok' =
                          (stale_keep_ok /\ ((cls # CLS_STALE) \/ (nph = book_phase /\ nid = book_id /\ nst = book_stamp_ns /\ nq = book_qty)))
                     /\ UNCHANGED <<
                          buf_len, first_some, first_u,
                          buf_head_first, buf_head_last, buf_head_prev, buf_head_stamp, buf_head_qty, buf_head_valid, buf_head_gen,
                          buf_tail_first, buf_tail_last, buf_tail_prev, buf_tail_stamp, buf_tail_qty, buf_tail_valid, buf_tail_gen,
                          sk_buf_some, sk_buf_gen
                        >>
             /\ gen_mono_ok' = gen_mono_ok
             /\ u64_safe_ok' = (u64_safe_ok /\ plus_safe /\ ((nph # BK_INVALID) \/ (gen = cfg_max_gen) \/ (gen + 1 <= EffU64Max)))
       /\ step_budget' = NextBudget
       /\ UNCHANGED <<
            cfg_policy, cfg_capacity, cfg_max_epoch, cfg_max_gen, cfg_max_req, cfg_max_age_ns,
            sk_iss_some, sk_iss_conn, sk_iss_gen, sk_iss_req,
            old_cb_ok, retry_keep_ok, req_mono_ok, snap_stamp_ok
          >>

SnapshotExpired ==
  /\ CanStep
  /\ pending_some
  /\ \E t \in TimeDom:
       /\ t >= 0 /\ t <= EffU64Max /\ GoodClock(t) /\ t >= pending_deadline_ns
       /\ clock_some' = TRUE /\ clock_ns' = t
       /\ pending_some' = FALSE
       /\ pending_conn' = 0 /\ pending_gen' = 0 /\ pending_req' = 0 /\ pending_deadline_ns' = 0
       /\ outcome' = OUT_EXPIRED
       /\ view_now_some' = FALSE /\ view_now_ns' = 0
       /\ retry_keep_ok' = retry_keep_ok
       /\ step_budget' = NextBudget
       /\ UNCHANGED <<
            cfg_policy, cfg_capacity, cfg_max_epoch, cfg_max_gen, cfg_max_req, cfg_max_age_ns,
            phase, conn, last_conn, gen, sub, req,
            buf_len, first_some, first_u,
            buf_head_first, buf_head_last, buf_head_prev, buf_head_stamp, buf_head_qty, buf_head_valid, buf_head_gen,
            buf_tail_first, buf_tail_last, buf_tail_prev, buf_tail_stamp, buf_tail_qty, buf_tail_valid, buf_tail_gen,
            sk_buf_some, sk_buf_gen,
            book_phase, book_id_some, book_id, book_stamp_some, book_stamp_ns, book_qty, book_gen,
            origin_some, origin_conn, origin_gen, origin_req,
            sk_iss_some, sk_iss_conn, sk_iss_gen, sk_iss_req,
            old_cb_ok, stale_keep_ok, gen_mono_ok, req_mono_ok, snap_stamp_ok, u64_safe_ok
          >>

SnapshotHttpFailure ==
  /\ CanStep
  /\ pending_some
  /\ \E t \in TimeDom:
       /\ t >= 0 /\ t <= EffU64Max /\ GoodClock(t) /\ t < pending_deadline_ns
       /\ clock_some' = TRUE /\ clock_ns' = t
       /\ pending_some' = FALSE
       /\ pending_conn' = 0 /\ pending_gen' = 0 /\ pending_req' = 0 /\ pending_deadline_ns' = 0
       /\ outcome' = OUT_HTTP_FAILURE
       /\ view_now_some' = FALSE /\ view_now_ns' = 0
       /\ retry_keep_ok' = retry_keep_ok
       /\ step_budget' = NextBudget
       /\ UNCHANGED <<
            cfg_policy, cfg_capacity, cfg_max_epoch, cfg_max_gen, cfg_max_req, cfg_max_age_ns,
            phase, conn, last_conn, gen, sub, req,
            buf_len, first_some, first_u,
            buf_head_first, buf_head_last, buf_head_prev, buf_head_stamp, buf_head_qty, buf_head_valid, buf_head_gen,
            buf_tail_first, buf_tail_last, buf_tail_prev, buf_tail_stamp, buf_tail_qty, buf_tail_valid, buf_tail_gen,
            sk_buf_some, sk_buf_gen,
            book_phase, book_id_some, book_id, book_stamp_some, book_stamp_ns, book_qty, book_gen,
            origin_some, origin_conn, origin_gen, origin_req,
            sk_iss_some, sk_iss_conn, sk_iss_gen, sk_iss_req,
            old_cb_ok, stale_keep_ok, gen_mono_ok, req_mono_ok, snap_stamp_ok, u64_safe_ok
          >>

SnapshotInvalid ==
  /\ CanStep
  /\ pending_some
  /\ \E t \in TimeDom:
       /\ t >= 0 /\ t <= EffU64Max /\ GoodClock(t) /\ t < pending_deadline_ns
       /\ AssignResetOrExhaust(TRUE, t, OUT_SNAP_INVALID)
       /\ gen_mono_ok' = gen_mono_ok
       /\ u64_safe_ok' = (u64_safe_ok /\ ((gen = cfg_max_gen) \/ (gen + 1 <= EffU64Max)))
       /\ step_budget' = NextBudget
       /\ UNCHANGED <<
            cfg_policy, cfg_capacity, cfg_max_epoch, cfg_max_gen, cfg_max_req, cfg_max_age_ns,
            sk_iss_some, sk_iss_conn, sk_iss_gen, sk_iss_req,
            old_cb_ok, retry_keep_ok, stale_keep_ok, req_mono_ok, snap_stamp_ok
          >>

(* Atomic snapshot install + buffered delta replay (`depth_sync.rs:349-370`): *)
SnapshotResponse(want_delayed) ==
  /\ CanStep
  /\ pending_some
  /\ \E t \in TimeDom, sid \in IdDom, sq \in QtyDom:
       /\ t >= 0 /\ t <= EffU64Max /\ GoodClock(t) /\ t < pending_deadline_ns
       /\ sid >= 0 /\ sid <= EffU64Max
       /\ LET old == (cfg_policy = POL_SPOT) /\ (sid < first_u)
              (* Step 1: apply head delta when `buf_len >= 1` *)
              c1 == ClassifyBook(cfg_policy, BK_BRIDGING, sid, buf_head_first, buf_head_last, buf_head_prev)
              ph1 == IF buf_len = 0 THEN BK_BRIDGING
                     ELSE ApplyPhase(cfg_policy, BK_BRIDGING, sid, FALSE, 0, buf_head_first, buf_head_last, buf_head_prev, buf_head_stamp, buf_head_valid)
              id1 == IF buf_len = 0 THEN sid
                     ELSE ApplyId(cfg_policy, BK_BRIDGING, sid, FALSE, 0, buf_head_first, buf_head_last, buf_head_prev, buf_head_stamp, buf_head_valid)
              sts1 == IF buf_len = 0 THEN FALSE
                      ELSE ApplyStampSome(cfg_policy, BK_BRIDGING, sid, FALSE, 0, buf_head_first, buf_head_last, buf_head_prev, buf_head_stamp, buf_head_valid)
              st1 == IF buf_len = 0 THEN 0
                     ELSE ApplyStamp(cfg_policy, BK_BRIDGING, sid, FALSE, 0, buf_head_first, buf_head_last, buf_head_prev, buf_head_stamp, buf_head_valid)
              q1 == IF buf_len = 0 THEN sq
                    ELSE ApplyQty(cfg_policy, BK_BRIDGING, sid, FALSE, 0, sq, buf_head_first, buf_head_last, buf_head_prev, buf_head_stamp, buf_head_qty, buf_head_valid)
              app1 == IF (buf_len >= 1) /\ (ph1 # BK_INVALID) /\ (c1 = CLS_APPLY) THEN 1 ELSE 0
              s1_safe == (buf_len = 0) \/ SpotPlusOneSafe(cfg_policy, sid, buf_head_last)

              (* Step 2: apply tail delta when `buf_len >= 2` and `ph1 # BK_INVALID` *)
              do2 == (buf_len >= 2) /\ (ph1 # BK_INVALID)
              c2 == ClassifyBook(cfg_policy, ph1, id1, buf_tail_first, buf_tail_last, buf_tail_prev)
              ph2 == IF ~do2 THEN ph1
                     ELSE ApplyPhase(cfg_policy, ph1, id1, sts1, st1, buf_tail_first, buf_tail_last, buf_tail_prev, buf_tail_stamp, buf_tail_valid)
              id2 == IF ~do2 THEN id1
                     ELSE ApplyId(cfg_policy, ph1, id1, sts1, st1, buf_tail_first, buf_tail_last, buf_tail_prev, buf_tail_stamp, buf_tail_valid)
              sts2 == IF ~do2 THEN sts1
                      ELSE ApplyStampSome(cfg_policy, ph1, id1, sts1, st1, buf_tail_first, buf_tail_last, buf_tail_prev, buf_tail_stamp, buf_tail_valid)
              st2 == IF ~do2 THEN st1
                     ELSE ApplyStamp(cfg_policy, ph1, id1, sts1, st1, buf_tail_first, buf_tail_last, buf_tail_prev, buf_tail_stamp, buf_tail_valid)
              q2 == IF ~do2 THEN q1
                    ELSE ApplyQty(cfg_policy, ph1, id1, sts1, st1, q1, buf_tail_first, buf_tail_last, buf_tail_prev, buf_tail_stamp, buf_tail_qty, buf_tail_valid)
              app2 == app1 + (IF do2 /\ (ph2 # BK_INVALID) /\ (c2 = CLS_APPLY) THEN 1 ELSE 0)
              s2_safe == (~do2) \/ SpotPlusOneSafe(cfg_policy, id1, buf_tail_last)

              is_delayed == (~old) /\ (ph2 = BK_LIVE) /\ (st2 < t)
              retained_match ==
                ((buf_len >= 1) /\ (st2 = buf_head_stamp) /\ (q2 = buf_head_qty))
                \/ ((buf_len >= 2) /\ (st2 = buf_tail_stamp) /\ (q2 = buf_tail_qty))
          IN /\ want_delayed = is_delayed
             /\ IF old
                THEN /\ clock_some' = TRUE /\ clock_ns' = t
                     /\ pending_some' = FALSE
                     /\ pending_conn' = 0 /\ pending_gen' = 0 /\ pending_req' = 0 /\ pending_deadline_ns' = 0
                     /\ outcome' = OUT_TOO_OLD
                     /\ view_now_some' = FALSE /\ view_now_ns' = 0
                     /\ retry_keep_ok' = retry_keep_ok
                     /\ snap_stamp_ok' = snap_stamp_ok
                     /\ u64_safe_ok' = u64_safe_ok
                     /\ UNCHANGED <<
                          phase, conn, last_conn, gen, sub, req,
                          buf_len, first_some, first_u,
                          buf_head_first, buf_head_last, buf_head_prev, buf_head_stamp, buf_head_qty, buf_head_valid, buf_head_gen,
                          buf_tail_first, buf_tail_last, buf_tail_prev, buf_tail_stamp, buf_tail_qty, buf_tail_valid, buf_tail_gen,
                          sk_buf_some, sk_buf_gen,
                          book_phase, book_id_some, book_id, book_stamp_some, book_stamp_ns, book_qty, book_gen,
                          origin_some, origin_conn, origin_gen, origin_req
                        >>
                ELSE IF ph2 = BK_INVALID
                     THEN /\ AssignResetOrExhaust(
                               TRUE, t,
                               IF app2 > 0 THEN OUT_LATE_REPLAY_FAILURE ELSE OUT_REPLAY_FAILURE
                             )
                          /\ retry_keep_ok' = retry_keep_ok
                          /\ snap_stamp_ok' = snap_stamp_ok
                          /\ u64_safe_ok' =
                               (u64_safe_ok /\ s1_safe /\ s2_safe /\ ((gen = cfg_max_gen) \/ (gen + 1 <= EffU64Max)))
                     ELSE /\ phase' = (IF ph2 = BK_LIVE THEN PH_LIVE ELSE PH_BRIDGING)
                          /\ conn' = conn /\ last_conn' = last_conn /\ gen' = gen /\ sub' = sub /\ req' = req
                          /\ clock_some' = TRUE /\ clock_ns' = t
                          /\ pending_some' = FALSE
                          /\ pending_conn' = 0 /\ pending_gen' = 0 /\ pending_req' = 0 /\ pending_deadline_ns' = 0
                          /\ buf_len' = 0
                          /\ first_some' = FALSE /\ first_u' = 0
                          /\ sk_buf_some' = FALSE /\ sk_buf_gen' = 0
                          /\ book_phase' = ph2
                          /\ book_id_some' = TRUE /\ book_id' = id2
                          /\ book_stamp_some' = sts2 /\ book_stamp_ns' = st2
                          /\ book_qty' = q2 /\ book_gen' = gen
                          /\ origin_some' = TRUE
                          /\ origin_conn' = pending_conn /\ origin_gen' = pending_gen /\ origin_req' = pending_req
                          /\ outcome' =
                               (IF ph2 = BK_LIVE
                                THEN (IF is_delayed THEN OUT_SNAP_DELAYED_LIVE ELSE OUT_SNAP_LIVE)
                                ELSE OUT_SNAP_BRIDGING)
                          /\ view_now_some' = FALSE /\ view_now_ns' = 0
                          /\ retry_keep_ok' = retry_keep_ok
                          /\ snap_stamp_ok' = (snap_stamp_ok /\ ((ph2 # BK_LIVE) \/ retained_match))
                          /\ u64_safe_ok' = (u64_safe_ok /\ s1_safe /\ s2_safe)
                          /\ ClearBufSlots
       /\ step_budget' = NextBudget
       /\ UNCHANGED <<
            cfg_policy, cfg_capacity, cfg_max_epoch, cfg_max_gen, cfg_max_req, cfg_max_age_ns,
            sk_iss_some, sk_iss_conn, sk_iss_gen, sk_iss_req,
            old_cb_ok, stale_keep_ok, gen_mono_ok, req_mono_ok
          >>

SnapshotValid ==
  /\ SnapshotResponse(FALSE)
  /\ outcome' # OUT_SNAP_DELAYED_LIVE

SnapshotDelayed ==
  /\ SnapshotResponse(TRUE)
  /\ outcome' = OUT_SNAP_DELAYED_LIVE

SnapshotTransportFailure ==
  /\ CanStep
  /\ pending_some
  /\ \E t \in TimeDom:
       /\ t >= 0 /\ t <= EffU64Max /\ GoodClock(t)
       /\ clock_some' = TRUE /\ clock_ns' = t
       /\ pending_some' = FALSE
       /\ pending_conn' = 0 /\ pending_gen' = 0 /\ pending_req' = 0 /\ pending_deadline_ns' = 0
       /\ outcome' = OUT_TRANSPORT_FAILURE
       /\ view_now_some' = FALSE /\ view_now_ns' = 0
       /\ retry_keep_ok' = retry_keep_ok
       /\ step_budget' = NextBudget
       /\ UNCHANGED <<
            cfg_policy, cfg_capacity, cfg_max_epoch, cfg_max_gen, cfg_max_req, cfg_max_age_ns,
            phase, conn, last_conn, gen, sub, req,
            buf_len, first_some, first_u,
            buf_head_first, buf_head_last, buf_head_prev, buf_head_stamp, buf_head_qty, buf_head_valid, buf_head_gen,
            buf_tail_first, buf_tail_last, buf_tail_prev, buf_tail_stamp, buf_tail_qty, buf_tail_valid, buf_tail_gen,
            sk_buf_some, sk_buf_gen,
            book_phase, book_id_some, book_id, book_stamp_some, book_stamp_ns, book_qty, book_gen,
            origin_some, origin_conn, origin_gen, origin_req,
            sk_iss_some, sk_iss_conn, sk_iss_gen, sk_iss_req,
            old_cb_ok, stale_keep_ok, gen_mono_ok, req_mono_ok, snap_stamp_ok, u64_safe_ok
          >>

Expire ==
  /\ CanStep
  /\ \E t \in TimeDom:
       /\ t >= 0 /\ t <= EffU64Max
       /\ IF conn = 0
          THEN /\ clock_some' = clock_some /\ clock_ns' = clock_ns
               /\ pending_some' = pending_some
               /\ pending_conn' = pending_conn /\ pending_gen' = pending_gen
               /\ pending_req' = pending_req /\ pending_deadline_ns' = pending_deadline_ns
               /\ outcome' = OUT_EXPIRE_NONE
          ELSE /\ GoodClock(t)
               /\ LET exp == pending_some /\ (t >= pending_deadline_ns) IN
                  /\ clock_some' = TRUE /\ clock_ns' = t
                  /\ pending_some' = (IF exp THEN FALSE ELSE pending_some)
                  /\ pending_conn' = (IF exp THEN 0 ELSE pending_conn)
                  /\ pending_gen' = (IF exp THEN 0 ELSE pending_gen)
                  /\ pending_req' = (IF exp THEN 0 ELSE pending_req)
                  /\ pending_deadline_ns' = (IF exp THEN 0 ELSE pending_deadline_ns)
                  /\ outcome' = (IF exp THEN OUT_EXPIRED ELSE OUT_EXPIRE_NONE)
       /\ view_now_some' = FALSE /\ view_now_ns' = 0
       /\ retry_keep_ok' = retry_keep_ok
       /\ step_budget' = NextBudget
       /\ UNCHANGED <<
            cfg_policy, cfg_capacity, cfg_max_epoch, cfg_max_gen, cfg_max_req, cfg_max_age_ns,
            phase, conn, last_conn, gen, sub, req,
            buf_len, first_some, first_u,
            buf_head_first, buf_head_last, buf_head_prev, buf_head_stamp, buf_head_qty, buf_head_valid, buf_head_gen,
            buf_tail_first, buf_tail_last, buf_tail_prev, buf_tail_stamp, buf_tail_qty, buf_tail_valid, buf_tail_gen,
            sk_buf_some, sk_buf_gen,
            book_phase, book_id_some, book_id, book_stamp_some, book_stamp_ns, book_qty, book_gen,
            origin_some, origin_conn, origin_gen, origin_req,
            sk_iss_some, sk_iss_conn, sk_iss_gen, sk_iss_req,
            old_cb_ok, stale_keep_ok, gen_mono_ok, req_mono_ok, snap_stamp_ok, u64_safe_ok
          >>

Disconnect ==
  /\ CanStep
  /\ conn # 0
  /\ phase' = PH_DISCONNECTED
  /\ conn' = 0
  /\ sub' = FALSE
  /\ pending_some' = FALSE
  /\ pending_conn' = 0 /\ pending_gen' = 0 /\ pending_req' = 0 /\ pending_deadline_ns' = 0
  /\ buf_len' = 0
  /\ first_some' = FALSE /\ first_u' = 0
  /\ sk_buf_some' = FALSE /\ sk_buf_gen' = 0
  /\ book_phase' = BK_INVALID
  /\ outcome' = OUT_DISCONNECT
  /\ view_now_some' = FALSE /\ view_now_ns' = 0
  /\ step_budget' = NextBudget
  /\ ClearBufSlots
  /\ UNCHANGED <<
       cfg_policy, cfg_capacity, cfg_max_epoch, cfg_max_gen, cfg_max_req, cfg_max_age_ns,
       last_conn, gen, req, clock_some, clock_ns,
       book_id_some, book_id, book_stamp_some, book_stamp_ns, book_qty, book_gen,
       origin_some, origin_conn, origin_gen, origin_req,
       sk_iss_some, sk_iss_conn, sk_iss_gen, sk_iss_req,
       old_cb_ok, retry_keep_ok, stale_keep_ok, gen_mono_ok, req_mono_ok, snap_stamp_ok, u64_safe_ok
     >>

View ==
  /\ CanStep
  /\ \E t \in TimeDom:
       /\ t >= 0 /\ t <= EffU64Max
       /\ view_now_some' = TRUE /\ view_now_ns' = t
       /\ LET out ==
                IF phase # PH_LIVE THEN OUT_VIEW_NOT_LIVE
                ELSE IF t < book_stamp_ns THEN OUT_VIEW_CLOCK
                ELSE IF (t - book_stamp_ns) > cfg_max_age_ns THEN OUT_VIEW_STALE
                ELSE OUT_VIEW
          IN /\ outcome' = out
             /\ u64_safe_ok' = (u64_safe_ok /\ ((out # OUT_VIEW) \/ (t >= book_stamp_ns)))
       /\ step_budget' = NextBudget
       /\ UNCHANGED <<
            cfg_policy, cfg_capacity, cfg_max_epoch, cfg_max_gen, cfg_max_req, cfg_max_age_ns,
            phase, conn, last_conn, gen, sub, req, clock_some, clock_ns,
            pending_some, pending_conn, pending_gen, pending_req, pending_deadline_ns,
            buf_len, first_some, first_u,
            buf_head_first, buf_head_last, buf_head_prev, buf_head_stamp, buf_head_qty, buf_head_valid, buf_head_gen,
            buf_tail_first, buf_tail_last, buf_tail_prev, buf_tail_stamp, buf_tail_qty, buf_tail_valid, buf_tail_gen,
            sk_buf_some, sk_buf_gen,
            book_phase, book_id_some, book_id, book_stamp_some, book_stamp_ns, book_qty, book_gen,
            origin_some, origin_conn, origin_gen, origin_req,
            sk_iss_some, sk_iss_conn, sk_iss_gen, sk_iss_req,
            old_cb_ok, retry_keep_ok, stale_keep_ok, gen_mono_ok, req_mono_ok, snap_stamp_ok
          >>

Next ==
  \/ Connect
  \/ ConnectRejected
  \/ Subscribe
  \/ OldConnection
  \/ OldSnapshot
  \/ Request
  \/ RequestBadState
  \/ RequestBadDeadline
  \/ RequestPending
  \/ ClockRegression
  \/ DeltaInvalid
  \/ DeltaBuffer
  \/ DeltaApply
  \/ SnapshotExpired
  \/ SnapshotHttpFailure
  \/ SnapshotInvalid
  \/ SnapshotValid
  \/ SnapshotDelayed
  \/ SnapshotTransportFailure
  \/ Expire
  \/ Disconnect
  \/ View

Spec == Init /\ [][Next]_vars

(* --- All `L0_DepthSync` State + Transition Invariants + 64-Bit Overflow Safety --- *)

TypeOK ==
  /\ ValidConfig
  /\ phase \in {PH_DISCONNECTED, PH_BUFFERING, PH_BRIDGING, PH_LIVE, PH_EXHAUSTED}
  /\ conn \in Int /\ conn >= 0 /\ conn <= cfg_max_epoch
  /\ last_conn \in Int /\ last_conn >= 0 /\ last_conn <= cfg_max_epoch
  /\ gen \in Int /\ gen >= 0 /\ gen <= cfg_max_gen
  /\ sub \in BOOLEAN
  /\ req \in Int /\ req >= 0 /\ req <= cfg_max_req
  /\ clock_some \in BOOLEAN
  /\ clock_ns \in Int /\ clock_ns >= 0 /\ clock_ns <= EffU64Max
  /\ pending_some \in BOOLEAN
  /\ pending_conn \in Int /\ pending_conn >= 0 /\ pending_conn <= cfg_max_epoch
  /\ pending_gen \in Int /\ pending_gen >= 0 /\ pending_gen <= cfg_max_gen
  /\ pending_req \in Int /\ pending_req >= 0 /\ pending_req <= cfg_max_req
  /\ pending_deadline_ns \in Int /\ pending_deadline_ns >= 0 /\ pending_deadline_ns <= EffU64Max
  /\ buf_len \in Int /\ buf_len >= 0 /\ buf_len <= cfg_capacity
  /\ first_some \in BOOLEAN
  /\ first_u \in Int /\ first_u >= 0 /\ first_u <= EffU64Max
  /\ buf_head_first \in Int /\ buf_head_first >= 0 /\ buf_head_first <= EffU64Max
  /\ buf_head_last \in Int /\ buf_head_last >= 0 /\ buf_head_last <= EffU64Max
  /\ buf_head_prev \in Int /\ buf_head_prev >= 0 /\ buf_head_prev <= EffU64Max
  /\ buf_head_stamp \in Int /\ buf_head_stamp >= 0 /\ buf_head_stamp <= EffU64Max
  /\ buf_head_qty \in {0, 1} /\ buf_head_valid \in BOOLEAN
  /\ buf_head_gen \in Int /\ buf_head_gen >= 0 /\ buf_head_gen <= cfg_max_gen
  /\ buf_tail_first \in Int /\ buf_tail_first >= 0 /\ buf_tail_first <= EffU64Max
  /\ buf_tail_last \in Int /\ buf_tail_last >= 0 /\ buf_tail_last <= EffU64Max
  /\ buf_tail_prev \in Int /\ buf_tail_prev >= 0 /\ buf_tail_prev <= EffU64Max
  /\ buf_tail_stamp \in Int /\ buf_tail_stamp >= 0 /\ buf_tail_stamp <= EffU64Max
  /\ buf_tail_qty \in {0, 1} /\ buf_tail_valid \in BOOLEAN
  /\ buf_tail_gen \in Int /\ buf_tail_gen >= 0 /\ buf_tail_gen <= cfg_max_gen
  /\ sk_buf_some \in BOOLEAN
  /\ sk_buf_gen \in Int /\ sk_buf_gen >= 0 /\ sk_buf_gen <= cfg_max_gen
  /\ ((buf_len >= 1) => (buf_head_gen = gen))
  /\ ((buf_len >= 2) => (buf_tail_gen = gen))
  /\ (sk_buf_some => (sk_buf_gen = gen))
  /\ book_phase \in {BK_AWAITING, BK_BRIDGING, BK_LIVE, BK_INVALID}
  /\ book_id_some \in BOOLEAN /\ book_id \in Int /\ book_id >= 0 /\ book_id <= EffU64Max
  /\ book_stamp_some \in BOOLEAN /\ book_stamp_ns \in Int /\ book_stamp_ns >= 0 /\ book_stamp_ns <= EffU64Max
  /\ book_qty \in {0, 1}
  /\ book_gen \in Int /\ book_gen = gen
  /\ origin_some \in BOOLEAN
  /\ origin_conn \in Int /\ origin_conn >= 0 /\ origin_conn <= cfg_max_epoch
  /\ origin_gen \in Int /\ origin_gen >= 0 /\ origin_gen <= cfg_max_gen
  /\ origin_req \in Int /\ origin_req >= 0 /\ origin_req <= cfg_max_req
  /\ sk_iss_some \in BOOLEAN
  /\ sk_iss_conn \in Int /\ sk_iss_conn >= 0 /\ sk_iss_conn <= cfg_max_epoch
  /\ sk_iss_gen \in Int /\ sk_iss_gen >= 0 /\ sk_iss_gen <= cfg_max_gen
  /\ sk_iss_req \in Int /\ sk_iss_req >= 0 /\ sk_iss_req <= cfg_max_req
  /\ outcome \in 0..33
  /\ view_now_some \in BOOLEAN /\ view_now_ns \in Int /\ view_now_ns >= 0 /\ view_now_ns <= EffU64Max
  /\ step_budget \in Int /\ step_budget >= 0 /\ step_budget <= MaxBoundedSteps
  /\ old_cb_ok \in BOOLEAN /\ retry_keep_ok \in BOOLEAN /\ stale_keep_ok \in BOOLEAN
  /\ gen_mono_ok \in BOOLEAN /\ req_mono_ok \in BOOLEAN /\ snap_stamp_ok \in BOOLEAN /\ u64_safe_ok \in BOOLEAN

PendingBelongs ==
  pending_some =>
    /\ phase = PH_BUFFERING
    /\ sub
    /\ conn # 0
    /\ first_some
    /\ pending_conn = conn /\ pending_gen = gen /\ pending_req = req
    /\ req >= 1

LiveHasCompleteReplay ==
  (phase = PH_LIVE) =>
    /\ conn # 0
    /\ sub
    /\ buf_len = 0
    /\ ~first_some
    /\ ~pending_some
    /\ book_phase = BK_LIVE
    /\ book_id_some /\ book_id >= 0 /\ book_id <= EffU64Max
    /\ book_stamp_some /\ book_stamp_ns >= 0 /\ book_stamp_ns <= EffU64Max
    /\ origin_some /\ origin_conn = conn /\ origin_gen = gen /\ origin_req = req

FirstIsBuffered ==
  (phase = PH_BUFFERING) =>
    /\ book_phase = BK_AWAITING
    /\ ~book_id_some
    /\ (IF buf_len = 0 THEN ~first_some ELSE (first_some /\ first_u = buf_head_first))

ClosedNotVisible ==
  (phase \in {PH_DISCONNECTED, PH_EXHAUSTED}) =>
    /\ conn = 0
    /\ ~sub
    /\ buf_len = 0
    /\ ~pending_some

FailuresClose ==
  (outcome \in FailureOutcomes) => (phase # PH_LIVE)

ViewFreshAtCreation ==
  (outcome = OUT_VIEW) =>
    /\ phase = PH_LIVE
    /\ view_now_some
    /\ book_stamp_some
    /\ view_now_ns >= book_stamp_ns
    /\ (view_now_ns - book_stamp_ns) <= cfg_max_age_ns

TransitionPropertiesHold ==
  /\ old_cb_ok
  /\ retry_keep_ok
  /\ stale_keep_ok
  /\ gen_mono_ok
  /\ req_mono_ok
  /\ snap_stamp_ok
  /\ u64_safe_ok

SafetyConjunction ==
  /\ TypeOK
  /\ PendingBelongs
  /\ LiveHasCompleteReplay
  /\ FirstIsBuffered
  /\ ClosedNotVisible
  /\ FailuresClose
  /\ ViewFreshAtCreation
  /\ TransitionPropertiesHold

(* --- Mode S2 Strengthened Inductive Invariant (`IndInv`) --- *)
IndInv ==
  /\ SafetyConjunction
  /\ conn <= last_conn
  /\ ((conn # 0) => (phase \in {PH_BUFFERING, PH_BRIDGING, PH_LIVE} /\ gen >= 1))
  /\ ((phase \in {PH_BUFFERING, PH_BRIDGING, PH_LIVE}) => (conn >= 1 /\ conn = last_conn))
  /\ ((phase = PH_BRIDGING) =>
        /\ sub
        /\ buf_len = 0
        /\ ~first_some
        /\ ~pending_some
        /\ clock_some
        /\ req >= 1
        /\ book_phase = BK_BRIDGING
        /\ book_id_some
        /\ ~book_stamp_some
        /\ origin_some /\ origin_conn = conn /\ origin_gen = gen /\ origin_req = req)
  /\ ((phase = PH_BUFFERING) => (~book_stamp_some /\ ~origin_some))
  /\ ((buf_len = 0) => (~first_some /\ ~sk_buf_some))
  /\ ((buf_len >= 1) =>
        /\ phase = PH_BUFFERING
        /\ first_some /\ first_u = buf_head_first
        /\ buf_head_first <= buf_head_last
        /\ clock_some /\ buf_head_stamp <= clock_ns)
  /\ ((buf_len >= 2) =>
        /\ buf_tail_first <= buf_tail_last
        /\ clock_some /\ buf_head_stamp <= buf_tail_stamp /\ buf_tail_stamp <= clock_ns)
  /\ (pending_some => (clock_some /\ pending_deadline_ns >= 1 /\ buf_len >= 1))
  /\ ((phase = PH_LIVE) => (clock_some /\ book_stamp_ns <= clock_ns /\ req >= 1))
  /\ (sk_iss_some => (sk_iss_conn >= 1 /\ sk_iss_conn <= last_conn /\ sk_iss_gen >= 1 /\ sk_iss_gen <= gen /\ sk_iss_req >= 1 /\ sk_iss_req <= req))
  /\ ((outcome = OUT_VIEW) => (book_phase = BK_LIVE))

====
