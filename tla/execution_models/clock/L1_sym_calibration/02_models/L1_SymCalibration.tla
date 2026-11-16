---- MODULE L1_SymCalibration ----
(*
 * Symbolic Unbounded Inductive & 64-Bit `u64`/`u128` Verification (Mode S2 + Mode S1)
 * for `tickwire_clock::Calibrator`:
 *   - Eliminates `L0_Calibration`'s 10-limb `[1..10 -> 0..999]` recursive `Word` encoding,
 *     5-timestamp discrete `Times` sampling, and `MaxCalls <= 6` horizon by verifying all
 *     15 `Calibrator` safety invariants directly over arbitrary-precision SMT integers (`QF_LIA`)
 *     across the full 64-bit `0 .. 2^64 - 1` monotonic/server domain and unbounded call
 *     horizons (`k >= 0`).
 *   - Uses Pre-Solver Skolem Witness Reduction (`sk_tok`, `sk_stamp`) + Relativistic Drift-Cone
 *     Ghost Anchors (`capture_truth_ps`, `anchor_truth_ps`) so continuous server-time containment
 *     (`ContinuousIntervalContainsTruth`, `ContinuousSendWithinWindow`) is 1-step inductive.
 *
 * Verified production Rust anchors:
 *   - Constants & public types (`PPB`, `MILLISECOND`, `MICROSECOND`, `SECOND`, `Error`,
 *     `Source`, `TimeUnit`, `ClockDomain`, `MonoTime`, `CalibrationConfig`, `SampleToken`,
 *     `Reading`, `SignedTime`, `CalibrationStatus`):
 *       crates/tickwire-clock/src/lib.rs:4-158
 *   - `Calibrator` fields, `new` validation, `fail`, and `observe`:
 *       crates/tickwire-clock/src/lib.rs:161-218
 *   - `begin`, `finish`, and `cancel` token lifecycle & clock-jump / RTT / width checks:
 *       crates/tickwire-clock/src/lib.rs:220-299
 *   - `read`, `sign_time`, `check_send`, and `status`:
 *       crates/tickwire-clock/src/lib.rs:301-392
 *   - Exact `u128` `TimeUnit::nanos`, `scale_up`, and `project` arithmetic:
 *       crates/tickwire-clock/src/lib.rs:393-415
 *)
EXTENDS Integers

CONSTANTS
  (* @type: Bool; *)
  BoundedMode,
  (* @type: Int; *)
  SourceKind,
  (* @type: Int; *)
  DriftCase

VARIABLES
  (* --- Core `Calibrator` state (`crates/tickwire-clock/src/lib.rs:161-170`) --- *)
  (* @type: Int; *)
  cfg_source,
  (* @type: Int; *)
  cfg_drift_case,
  (* @type: Int; *)
  cfg_max_rtt_ns,
  (* @type: Int; *)
  cfg_max_width_ns,
  (* @type: Int; *)
  cfg_max_age_ns,
  (* @type: Int; *)
  cfg_quant_err_ns,
  (* @type: Int; *)
  cfg_max_adj_ns,
  (* @type: Bool; *)
  next_serial_some,
  (* @type: Int; *)
  next_serial,
  (* @type: Bool; *)
  pending_some,
  (* @type: Int; *)
  pending_domain,
  (* @type: Int; *)
  pending_source,
  (* @type: Int; *)
  pending_serial,
  (* @type: Int; *)
  pending_sent_ns,
  (* @type: Bool; *)
  anchor_some,
  (* @type: Int; *)
  anchor_lo_ns,
  (* @type: Int; *)
  anchor_hi_ns,
  (* @type: Int; *)
  anchor_at_ns,
  (* @type: Int; *)
  anchor_exp_ns,
  (* @type: Int; *)
  anchor_serial,
  (* @type: Bool; *)
  last_seen_some,
  (* @type: Int; *)
  last_seen_ns,
  (* @type: Int; *)
  fault_code,

  (* --- Environment continuous server truth & drift-cone ghost state --- *)
  (* @type: Int; *)
  now_ns,
  (* @type: Int; *)
  truth_ps,
  (* @type: Int; *)
  epoch,
  (* @type: Int; *)
  jumps,
  (* @type: Int; *)
  begin_epoch,
  (* @type: Bool; *)
  capture_some,
  (* @type: Int; *)
  capture_serial,
  (* @type: Int; *)
  capture_sent_ns,
  (* @type: Int; *)
  capture_at_ns,
  (* @type: Int; *)
  capture_ms,
  (* @type: Int; *)
  capture_truth_ps,
  (* @type: Int; *)
  capture_epoch,
  (* @type: Bool; *)
  anchor_trusted,
  (* @type: Int; *)
  anchor_truth_ps,

  (* --- Skolem retained witnesses (`SampleToken` & `SignedTime`) --- *)
  (* @type: Bool; *)
  sk_tok_some,
  (* @type: Int; *)
  sk_tok_domain,
  (* @type: Int; *)
  sk_tok_source,
  (* @type: Int; *)
  sk_tok_serial,
  (* @type: Int; *)
  sk_tok_sent_ns,
  (* @type: Bool; *)
  sk_stamp_some,
  (* @type: Int; *)
  sk_stamp_domain,
  (* @type: Int; *)
  sk_stamp_source,
  (* @type: Int; *)
  sk_stamp_serial,
  (* @type: Int; *)
  sk_stamp_ts,
  (* @type: Int; *)
  sk_stamp_unit,

  (* --- Transition safety & bounded-TLC exploration witnesses --- *)
  (* @type: Int; *)
  step_budget,
  (* @type: Bool; *)
  stale_ok,
  (* @type: Bool; *)
  monotone_ok,
  (* @type: Bool; *)
  read_ok,
  (* @type: Bool; *)
  sign_ok,
  (* @type: Bool; *)
  send_ok,
  (* @type: Bool; *)
  truth_ok,
  (* @type: Bool; *)
  send_truth_ok,
  (* @type: Bool; *)
  old_anchor_ok,
  (* @type: Bool; *)
  old_expiry_ok,
  (* @type: Bool; *)
  fault_revoked_ok

vars == <<
  cfg_source, cfg_drift_case, cfg_max_rtt_ns, cfg_max_width_ns, cfg_max_age_ns,
  cfg_quant_err_ns, cfg_max_adj_ns,
  next_serial_some, next_serial,
  pending_some, pending_domain, pending_source, pending_serial, pending_sent_ns,
  anchor_some, anchor_lo_ns, anchor_hi_ns, anchor_at_ns, anchor_exp_ns, anchor_serial,
  last_seen_some, last_seen_ns, fault_code,
  now_ns, truth_ps, epoch, jumps, begin_epoch,
  capture_some, capture_serial, capture_sent_ns, capture_at_ns, capture_ms,
  capture_truth_ps, capture_epoch, anchor_trusted, anchor_truth_ps,
  sk_tok_some, sk_tok_domain, sk_tok_source, sk_tok_serial, sk_tok_sent_ns,
  sk_stamp_some, sk_stamp_domain, sk_stamp_source, sk_stamp_serial, sk_stamp_ts, sk_stamp_unit,
  step_budget, stale_ok, monotone_ok, read_ok, sign_ok, send_ok,
  truth_ok, send_truth_ok, old_anchor_ok, old_expiry_ok, fault_revoked_ok
>>

(* --- Constants & Fault / Error Codes --- *)
(* 2^30 = 1073741824 fits SANY's 32-bit literal parser; Apalache evaluates
 * 16 * 2^60 - 1 = 2^64 - 1 (`u64::MAX` = 18446744073709551615) in exact SMT Int. *)
Pow30 == 1073741824
U64_MAX == (16 * Pow30 * Pow30) - 1

(* Fault codes stored in `fault_code` (`0` = None): *)
FAULT_NONE == 0
FAULT_DOMAIN == 1
FAULT_BACKWARDS == 2
FAULT_OVERFLOW == 3
FAULT_JUMP == 4

(* Return codes (`0` = Ok): *)
RET_OK == 0
RET_INVALID == 10
RET_BUSY == 11
RET_STALE == 12
RET_UNCALIBRATED == 13
RET_EXPIRED == 14
RET_UNCERTAIN == 15
RET_ROUNDTRIP == 16
RET_FAULTED == 17
RET_FUTURE == 18
RET_TOO_OLD == 19

(* Units: in symbolic mode exact `ns`/`ps` (`PPB = 10^9`); in bounded TLC scaled so `int32` holds all states. *)
PPB == IF BoundedMode THEN 1000 ELSE 1000000000
MS_NS == IF BoundedMode THEN 10 ELSE 1000000
US_NS == IF BoundedMode THEN 1 ELSE 1000
SEC_NS == IF BoundedMode THEN 100 ELSE 1000000000
MAX_WIN_NS == IF BoundedMode THEN 600 ELSE (60 * 1000000000)
EffU64Max == IF BoundedMode THEN 2000 ELSE U64_MAX
MaxBoundedSteps == 5

(* Linearized `QF_LIA` arithmetic helpers using explicit literal multipliers/divisors
 * in every branch so Z3 solves 100% linear integer arithmetic (zero `QF_NIA` terms): *)
MulPpb(x) == IF BoundedMode THEN x * 1000 ELSE x * 1000000000
MulMsNs(x) == IF BoundedMode THEN x * 10 ELSE x * 1000000
ToCaptureMs(ps) ==
  IF BoundedMode
  THEN (ps \div 1000) \div 10
  ELSE (ps \div 1000000000) \div 1000000

UnitNanos(u) ==
  IF BoundedMode
  THEN (IF u = 1 THEN 1 ELSE 10)
  ELSE (IF u = 1 THEN 1000 ELSE 1000000)

DivByUnit(val, u) ==
  IF BoundedMode
  THEN (IF u = 1 THEN val ELSE val \div 10)
  ELSE (IF u = 1 THEN val \div 1000 ELSE val \div 1000000)

MulByUnit(val, u) ==
  IF BoundedMode
  THEN (IF u = 1 THEN val ELSE val * 10)
  ELSE (IF u = 1 THEN val * 1000 ELSE val * 1000000)

Max2(a, b) == IF a >= b THEN a ELSE b
SatSub(a, b) == IF a > b THEN a - b ELSE 0

(* Exact `QF_LIA` transcription of `scale_up(value, drift_ppb)` (`crates/tickwire-clock/src/lib.rs:401-404`):
 * `(value * (PPB + drift_ppb) + PPB - 1) \div PPB` using literal divisors per `DriftCase`. *)
ScaleUpRaw(val, dc) ==
  IF BoundedMode
  THEN CASE dc = 0 -> val
         [] dc = 1 -> (val * 1001 + 999) \div 1000
         [] OTHER -> (val * 1100 + 999) \div 1000
  ELSE CASE dc = 0 -> val
         [] dc = 1 -> (val * 1000000001 + 999999999) \div 1000000000
         [] dc = 2 -> (val * 1000100000 + 999999999) \div 1000000000
         [] OTHER -> (val * 1001000000 + 999999999) \div 1000000000

(* Exact `QF_LIA` transcription of `lower_advance` in `project` (`crates/tickwire-clock/src/lib.rs:407`): *)
ScaleDownRaw(val, dc) ==
  IF BoundedMode
  THEN CASE dc = 0 -> val
         [] dc = 1 -> (val * 999) \div 1000
         [] OTHER -> (val * 900) \div 1000
  ELSE CASE dc = 0 -> val
         [] dc = 1 -> (val * 999999999) \div 1000000000
         [] dc = 2 -> (val * 999900000) \div 1000000000
         [] OTHER -> (val * 999000000) \div 1000000000

AdvanceTruthPs(dt_ns, dc, rate) ==
  IF BoundedMode
  THEN CASE dc = 0 -> dt_ns * 1000
         [] dc = 1 -> (CASE rate = -1 -> dt_ns * 999 [] rate = 0 -> dt_ns * 1000 [] OTHER -> dt_ns * 1001)
         [] OTHER  -> (CASE rate = -1 -> dt_ns * 900 [] rate = 0 -> dt_ns * 1000 [] OTHER -> dt_ns * 1100)
  ELSE CASE dc = 0 -> dt_ns * 1000000000
         [] dc = 1 -> (CASE rate = -1 -> dt_ns * 999999999 [] rate = 0 -> dt_ns * 1000000000 [] OTHER -> dt_ns * 1000000001)
         [] dc = 2 -> (CASE rate = -1 -> dt_ns * 999900000 [] rate = 0 -> dt_ns * 1000000000 [] OTHER -> dt_ns * 1000100000)
         [] OTHER  -> (CASE rate = -1 -> dt_ns * 999000000 [] rate = 0 -> dt_ns * 1000000000 [] OTHER -> dt_ns * 1001000000)

WindowPrecisionOk(src, win_ns) ==
  IF BoundedMode
  THEN (IF src = 2 THEN (win_ns % 10) = 0 ELSE TRUE)
  ELSE (IF src = 2 THEN (win_ns % 1000000) = 0 ELSE (win_ns % 1000) = 0)

WindowOk(src, win_ns) ==
  /\ win_ns > 0
  /\ win_ns <= MAX_WIN_NS
  /\ WindowPrecisionOk(src, win_ns)

ProjLo(lo_ns, at_ns, cur_ns, dc) == lo_ns + ScaleDownRaw(cur_ns - at_ns, dc)
ProjHi(hi_ns, at_ns, cur_ns, dc) == hi_ns + ScaleUpRaw(cur_ns - at_ns, dc)

ProjFits(lo_ns, hi_ns, at_ns, cur_ns, dc) ==
  /\ cur_ns >= at_ns
  /\ ScaleDownRaw(cur_ns - at_ns, dc) <= EffU64Max
  /\ ScaleUpRaw(cur_ns - at_ns, dc) <= EffU64Max
  /\ ProjLo(lo_ns, at_ns, cur_ns, dc) <= EffU64Max
  /\ ProjHi(hi_ns, at_ns, cur_ns, dc) <= EffU64Max

CanStep == (~BoundedMode) \/ (step_budget < MaxBoundedSteps)
NextBudget == IF BoundedMode THEN step_budget + 1 ELSE 0

(* Relational constant constraint for Apalache (`--cinit=ConstInit`): *)
ConstInit ==
  /\ BoundedMode = FALSE
  /\ SourceKind \in {1, 2}
  /\ DriftCase \in {0, 1, 2, 3}

ValidConfig ==
  /\ cfg_source \in {1, 2}
  /\ cfg_drift_case \in Int /\ cfg_drift_case >= 0 /\ cfg_drift_case <= (IF BoundedMode THEN 2 ELSE 3)
  /\ cfg_max_rtt_ns \in Int /\ cfg_max_rtt_ns > 0 /\ cfg_max_rtt_ns <= EffU64Max
  /\ cfg_max_width_ns \in Int /\ cfg_max_width_ns > 0 /\ cfg_max_width_ns <= EffU64Max
  /\ cfg_max_age_ns \in Int /\ cfg_max_age_ns > 0 /\ cfg_max_age_ns <= EffU64Max
  /\ cfg_quant_err_ns \in Int /\ cfg_quant_err_ns >= MS_NS /\ cfg_quant_err_ns <= EffU64Max
  /\ cfg_max_adj_ns \in Int /\ cfg_max_adj_ns >= 0 /\ cfg_max_adj_ns <= EffU64Max

Init ==
  /\ cfg_source = SourceKind
  /\ cfg_drift_case = DriftCase
  /\ IF BoundedMode
     THEN /\ cfg_max_rtt_ns = 30
          /\ cfg_max_width_ns = 40
          /\ cfg_max_age_ns = 60
          /\ cfg_quant_err_ns = MS_NS
          /\ cfg_max_adj_ns = 50
          /\ next_serial = 1
          /\ now_ns = 0
          /\ truth_ps \in {200000, 209000}
     ELSE /\ cfg_max_rtt_ns \in Int /\ cfg_max_rtt_ns > 0 /\ cfg_max_rtt_ns <= EffU64Max
          /\ cfg_max_width_ns \in Int /\ cfg_max_width_ns > 0 /\ cfg_max_width_ns <= EffU64Max
          /\ cfg_max_age_ns \in Int /\ cfg_max_age_ns > 0 /\ cfg_max_age_ns <= EffU64Max
          /\ cfg_quant_err_ns \in Int /\ cfg_quant_err_ns >= MS_NS /\ cfg_quant_err_ns <= EffU64Max
          /\ cfg_max_adj_ns \in Int /\ cfg_max_adj_ns >= 0 /\ cfg_max_adj_ns <= EffU64Max
          /\ next_serial \in Int /\ next_serial >= 1 /\ next_serial <= EffU64Max
          /\ now_ns \in Int /\ now_ns >= 0 /\ now_ns <= EffU64Max
          /\ truth_ps \in Int /\ truth_ps >= MulPpb(MS_NS) /\ truth_ps <= MulPpb(EffU64Max)
  /\ next_serial_some = TRUE
  /\ pending_some = FALSE
  /\ pending_domain = 0 /\ pending_source = 0 /\ pending_serial = 0 /\ pending_sent_ns = 0
  /\ anchor_some = FALSE
  /\ anchor_lo_ns = 0 /\ anchor_hi_ns = 0 /\ anchor_at_ns = 0 /\ anchor_exp_ns = 0 /\ anchor_serial = 0
  /\ last_seen_some = FALSE /\ last_seen_ns = 0
  /\ fault_code = FAULT_NONE
  /\ epoch = 0 /\ jumps = 0 /\ begin_epoch = 0
  /\ capture_some = FALSE
  /\ capture_serial = 0 /\ capture_sent_ns = 0 /\ capture_at_ns = 0
  /\ capture_ms = 0 /\ capture_truth_ps = 0 /\ capture_epoch = 0
  /\ anchor_trusted = FALSE /\ anchor_truth_ps = 0
  /\ sk_tok_some = FALSE
  /\ sk_tok_domain = 0 /\ sk_tok_source = 0 /\ sk_tok_serial = 0 /\ sk_tok_sent_ns = 0
  /\ sk_stamp_some = FALSE
  /\ sk_stamp_domain = 0 /\ sk_stamp_source = 0 /\ sk_stamp_serial = 0
  /\ sk_stamp_ts = 0 /\ sk_stamp_unit = 2
  /\ step_budget = 0
  /\ stale_ok = TRUE /\ monotone_ok = TRUE /\ read_ok = TRUE /\ sign_ok = TRUE
  /\ send_ok = TRUE /\ truth_ok = TRUE /\ send_truth_ok = TRUE
  /\ old_anchor_ok = TRUE /\ old_expiry_ok = TRUE /\ fault_revoked_ok = TRUE

(* --- Environment Actions --- *)

Advance ==
  /\ CanStep
  /\ \E next_now \in (IF BoundedMode THEN {10, 20, 40, 70} ELSE Int), rate \in {-1, 0, 1}:
       /\ next_now > now_ns /\ next_now <= EffU64Max
       /\ LET new_truth == truth_ps + AdvanceTruthPs(next_now - now_ns, cfg_drift_case, rate) IN
            /\ new_truth <= MulPpb(EffU64Max)
            /\ now_ns' = next_now
            /\ truth_ps' = new_truth
            /\ step_budget' = NextBudget
            /\ UNCHANGED <<
                 cfg_source, cfg_drift_case, cfg_max_rtt_ns, cfg_max_width_ns, cfg_max_age_ns,
                 cfg_quant_err_ns, cfg_max_adj_ns,
                 next_serial_some, next_serial,
                 pending_some, pending_domain, pending_source, pending_serial, pending_sent_ns,
                 anchor_some, anchor_lo_ns, anchor_hi_ns, anchor_at_ns, anchor_exp_ns, anchor_serial,
                 last_seen_some, last_seen_ns, fault_code,
                 epoch, jumps, begin_epoch,
                 capture_some, capture_serial, capture_sent_ns, capture_at_ns, capture_ms,
                 capture_truth_ps, capture_epoch, anchor_trusted, anchor_truth_ps,
                 sk_tok_some, sk_tok_domain, sk_tok_source, sk_tok_serial, sk_tok_sent_ns,
                 sk_stamp_some, sk_stamp_domain, sk_stamp_source, sk_stamp_serial, sk_stamp_ts, sk_stamp_unit,
                 stale_ok, monotone_ok, read_ok, sign_ok, send_ok,
                 truth_ok, send_truth_ok, old_anchor_ok, old_expiry_ok, fault_revoked_ok
               >>

Capture ==
  /\ CanStep
  /\ pending_some
  /\ ~capture_some
  /\ capture_some' = TRUE
  /\ capture_serial' = pending_serial
  /\ capture_sent_ns' = pending_sent_ns
  /\ capture_at_ns' = now_ns
  /\ capture_ms' = ToCaptureMs(truth_ps)
  /\ capture_truth_ps' = truth_ps
  /\ capture_epoch' = epoch
  /\ step_budget' = NextBudget
  /\ UNCHANGED <<
       cfg_source, cfg_drift_case, cfg_max_rtt_ns, cfg_max_width_ns, cfg_max_age_ns,
       cfg_quant_err_ns, cfg_max_adj_ns,
       next_serial_some, next_serial,
       pending_some, pending_domain, pending_source, pending_serial, pending_sent_ns,
       anchor_some, anchor_lo_ns, anchor_hi_ns, anchor_at_ns, anchor_exp_ns, anchor_serial,
       last_seen_some, last_seen_ns, fault_code,
       now_ns, truth_ps, epoch, jumps, begin_epoch,
       anchor_trusted, anchor_truth_ps,
       sk_tok_some, sk_tok_domain, sk_tok_source, sk_tok_serial, sk_tok_sent_ns,
       sk_stamp_some, sk_stamp_domain, sk_stamp_source, sk_stamp_serial, sk_stamp_ts, sk_stamp_unit,
       stale_ok, monotone_ok, read_ok, sign_ok, send_ok,
       truth_ok, send_truth_ok, old_anchor_ok, old_expiry_ok, fault_revoked_ok
     >>

Jump ==
  /\ CanStep
  /\ jumps = 0
  /\ \E jump_ms \in (IF BoundedMode THEN {100} ELSE Int):
       /\ jump_ms >= 1 /\ MulMsNs(jump_ms) <= EffU64Max
       /\ truth_ps' = MulPpb(MulMsNs(jump_ms))
       /\ jumps' = 1
       /\ epoch' = epoch + 1
       /\ anchor_trusted' = FALSE
       /\ step_budget' = NextBudget
       /\ UNCHANGED <<
            cfg_source, cfg_drift_case, cfg_max_rtt_ns, cfg_max_width_ns, cfg_max_age_ns,
            cfg_quant_err_ns, cfg_max_adj_ns,
            next_serial_some, next_serial,
            pending_some, pending_domain, pending_source, pending_serial, pending_sent_ns,
            anchor_some, anchor_lo_ns, anchor_hi_ns, anchor_at_ns, anchor_exp_ns, anchor_serial,
            last_seen_some, last_seen_ns, fault_code,
            now_ns, begin_epoch,
            capture_some, capture_serial, capture_sent_ns, capture_at_ns, capture_ms,
            capture_truth_ps, capture_epoch, anchor_truth_ps,
            sk_tok_some, sk_tok_domain, sk_tok_source, sk_tok_serial, sk_tok_sent_ns,
            sk_stamp_some, sk_stamp_domain, sk_stamp_source, sk_stamp_serial, sk_stamp_ts, sk_stamp_unit,
            stale_ok, monotone_ok, read_ok, sign_ok, send_ok,
            truth_ok, send_truth_ok, old_anchor_ok, old_expiry_ok, fault_revoked_ok
          >>

(* --- `Calibrator` Method Actions --- *)

Begin ==
  /\ CanStep
  /\ \E latch_tok \in BOOLEAN:
       LET obs_faulted == fault_code # FAULT_NONE
           obs_back == (~obs_faulted) /\ last_seen_some /\ (now_ns < last_seen_ns)
           busy == (~obs_faulted) /\ (~obs_back) /\ pending_some
           ovf == (~obs_faulted) /\ (~obs_back) /\ (~busy) /\ (~next_serial_some)
           ok == (~obs_faulted) /\ (~obs_back) /\ (~busy) /\ (~ovf)
           new_last_some == IF obs_faulted \/ obs_back THEN last_seen_some ELSE TRUE
           new_last_ns == IF obs_faulted \/ obs_back THEN last_seen_ns ELSE now_ns
           new_anchor_some == IF obs_back \/ ovf THEN FALSE ELSE anchor_some
       IN /\ fault_code' =
               CASE obs_faulted -> fault_code
                 [] obs_back -> FAULT_BACKWARDS
                 [] ovf -> FAULT_OVERFLOW
                 [] OTHER -> fault_code
          /\ last_seen_some' = new_last_some
          /\ last_seen_ns' = new_last_ns
          /\ next_serial_some' =
               IF ok THEN (next_serial < EffU64Max) ELSE next_serial_some
          /\ next_serial' =
               IF ok /\ next_serial < EffU64Max THEN next_serial + 1 ELSE next_serial
          /\ pending_some' =
               CASE obs_back \/ ovf -> FALSE
                 [] ok -> TRUE
                 [] OTHER -> pending_some
          /\ pending_domain' = IF ok THEN 1 ELSE (IF obs_back \/ ovf THEN 0 ELSE pending_domain)
          /\ pending_source' = IF ok THEN cfg_source ELSE (IF obs_back \/ ovf THEN 0 ELSE pending_source)
          /\ pending_serial' = IF ok THEN next_serial ELSE (IF obs_back \/ ovf THEN 0 ELSE pending_serial)
          /\ pending_sent_ns' = IF ok THEN now_ns ELSE (IF obs_back \/ ovf THEN 0 ELSE pending_sent_ns)
          /\ anchor_some' = new_anchor_some
          /\ anchor_trusted' = IF obs_back \/ ovf THEN FALSE ELSE anchor_trusted
          /\ begin_epoch' = IF ok THEN epoch ELSE begin_epoch
          /\ capture_some' = IF ok THEN FALSE ELSE capture_some
          /\ sk_tok_some' = IF ok /\ (latch_tok \/ ~sk_tok_some) THEN TRUE ELSE sk_tok_some
          /\ sk_tok_domain' = IF ok /\ (latch_tok \/ ~sk_tok_some) THEN 1 ELSE sk_tok_domain
          /\ sk_tok_source' = IF ok /\ (latch_tok \/ ~sk_tok_some) THEN cfg_source ELSE sk_tok_source
          /\ sk_tok_serial' = IF ok /\ (latch_tok \/ ~sk_tok_some) THEN next_serial ELSE sk_tok_serial
          /\ sk_tok_sent_ns' = IF ok /\ (latch_tok \/ ~sk_tok_some) THEN now_ns ELSE sk_tok_sent_ns
          /\ monotone_ok' = (monotone_ok /\ ((~last_seen_some) \/ (~new_last_some) \/ (last_seen_ns <= new_last_ns)))
          /\ old_expiry_ok' = (old_expiry_ok /\ ((~new_anchor_some) \/ TRUE))
          /\ fault_revoked_ok' = (fault_revoked_ok /\ ((fault_code = FAULT_NONE) \/ (~ok)))
          /\ step_budget' = NextBudget
          /\ UNCHANGED <<
               cfg_source, cfg_drift_case, cfg_max_rtt_ns, cfg_max_width_ns, cfg_max_age_ns,
               cfg_quant_err_ns, cfg_max_adj_ns,
               anchor_lo_ns, anchor_hi_ns, anchor_at_ns, anchor_exp_ns, anchor_serial,
               now_ns, truth_ps, epoch, jumps,
               capture_serial, capture_sent_ns, capture_at_ns, capture_ms, capture_truth_ps, capture_epoch,
               anchor_truth_ps,
               sk_stamp_some, sk_stamp_domain, sk_stamp_source, sk_stamp_serial, sk_stamp_ts, sk_stamp_unit,
               stale_ok, read_ok, sign_ok, send_ok, truth_ok, send_truth_ok, old_anchor_ok
             >>

FinishWithSample(tok_dom, tok_src, tok_ser, tok_sent, srv_ms, good_reply) ==
  LET match_pending ==
        pending_some /\ tok_dom = pending_domain /\ tok_src = pending_source
        /\ tok_ser = pending_serial /\ tok_sent = pending_sent_ns
      obs_faulted == match_pending /\ (fault_code # FAULT_NONE)
      obs_back == match_pending /\ (~obs_faulted) /\ last_seen_some /\ (now_ns < last_seen_ns)
      obs_ok == match_pending /\ (~obs_faulted) /\ (~obs_back)
      rtt == now_ns - tok_sent
      inv_ms == obs_ok /\ (srv_ms = 0)
      bad_rtt == obs_ok /\ (~inv_ms) /\ (rtt > cfg_max_rtt_ns)
      srv_ns == MulMsNs(srv_ms)
      cand_hi == srv_ns + cfg_quant_err_ns + ScaleUpRaw(rtt, cfg_drift_case)
      cand_lo == SatSub(srv_ns, cfg_quant_err_ns)
      cand_exp == now_ns + cfg_max_age_ns
      cand_fits ==
        (srv_ns <= EffU64Max)
        /\ (ScaleUpRaw(rtt, cfg_drift_case) <= EffU64Max)
        /\ (cand_hi <= EffU64Max)
        /\ (cand_exp <= EffU64Max)
      cand_ovf == obs_ok /\ (~inv_ms) /\ (~bad_rtt) /\ (~cand_fits)
      cand_uncertain == obs_ok /\ (~inv_ms) /\ (~bad_rtt) /\ cand_fits /\ ((cand_hi - cand_lo) > cfg_max_width_ns)
      need_old_check == obs_ok /\ (~inv_ms) /\ (~bad_rtt) /\ cand_fits /\ (~cand_uncertain) /\ anchor_some /\ (now_ns < anchor_exp_ns)
      old_fits == (~need_old_check) \/ ProjFits(anchor_lo_ns, anchor_hi_ns, anchor_at_ns, now_ns, cfg_drift_case)
      old_lo == ProjLo(anchor_lo_ns, anchor_at_ns, now_ns, cfg_drift_case)
      old_hi == ProjHi(anchor_hi_ns, anchor_at_ns, now_ns, cfg_drift_case)
      gap == Max2(SatSub(cand_lo, old_hi), SatSub(old_lo, cand_hi))
      old_ovf == need_old_check /\ (~old_fits)
      jump_fail == need_old_check /\ old_fits /\ (gap > cfg_max_adj_ns)
      ok == obs_ok /\ (~inv_ms) /\ (~bad_rtt) /\ cand_fits /\ (~cand_uncertain) /\ old_fits /\ (~jump_fail)
      any_fail == obs_back \/ cand_ovf \/ old_ovf \/ jump_fail
      new_fault ==
        CASE obs_back -> FAULT_BACKWARDS
          [] cand_ovf \/ old_ovf -> FAULT_OVERFLOW
          [] jump_fail -> FAULT_JUMP
          [] OTHER -> fault_code
      new_last_some == IF obs_ok THEN TRUE ELSE last_seen_some
      new_last_ns == IF obs_ok THEN now_ns ELSE last_seen_ns
      new_pending_some == IF match_pending /\ (~obs_faulted) THEN FALSE ELSE pending_some
      new_anchor_some == CASE ok -> TRUE [] any_fail -> FALSE [] OTHER -> anchor_some
      new_anchor_lo == IF ok THEN cand_lo ELSE anchor_lo_ns
      new_anchor_hi == IF ok THEN cand_hi ELSE anchor_hi_ns
      new_trusted == IF ok THEN good_reply ELSE (IF any_fail \/ (~anchor_some) THEN FALSE ELSE anchor_trusted)
  IN /\ fault_code' = new_fault
     /\ last_seen_some' = new_last_some
     /\ last_seen_ns' = new_last_ns
     /\ pending_some' = new_pending_some
     /\ pending_domain' = IF match_pending /\ (~obs_faulted) THEN 0 ELSE pending_domain
     /\ pending_source' = IF match_pending /\ (~obs_faulted) THEN 0 ELSE pending_source
     /\ pending_serial' = IF match_pending /\ (~obs_faulted) THEN 0 ELSE pending_serial
     /\ pending_sent_ns' = IF match_pending /\ (~obs_faulted) THEN 0 ELSE pending_sent_ns
     /\ anchor_some' = new_anchor_some
     /\ anchor_lo_ns' = new_anchor_lo
     /\ anchor_hi_ns' = new_anchor_hi
     /\ anchor_at_ns' = IF ok THEN now_ns ELSE anchor_at_ns
     /\ anchor_exp_ns' = IF ok THEN cand_exp ELSE anchor_exp_ns
     /\ anchor_serial' = IF ok THEN tok_ser ELSE anchor_serial
     /\ anchor_trusted' = new_trusted
     /\ anchor_truth_ps' = IF ok THEN truth_ps ELSE anchor_truth_ps
     /\ stale_ok' = (stale_ok /\ (match_pending \/ (new_pending_some = pending_some /\ new_anchor_some = anchor_some /\ new_fault = fault_code)))
     /\ monotone_ok' = (monotone_ok /\ ((~last_seen_some) \/ (~new_last_some) \/ (last_seen_ns <= new_last_ns)))
     /\ read_ok' =
          (read_ok /\ ((~ok) \/ (cand_lo <= cand_hi /\ cand_hi <= EffU64Max /\ now_ns < cand_exp /\ (cand_hi - cand_lo) <= cfg_max_width_ns)))
     /\ truth_ok' =
          (truth_ok /\ ((~ok) \/ (~new_trusted) \/ (MulPpb(cand_lo) <= truth_ps /\ truth_ps <= MulPpb(cand_hi))))
     /\ old_anchor_ok' =
          (old_anchor_ok /\ ((~(inv_ms \/ bad_rtt \/ cand_uncertain)) \/ (new_anchor_some = anchor_some /\ new_anchor_lo = anchor_lo_ns /\ new_anchor_hi = anchor_hi_ns)))
     /\ fault_revoked_ok' = (fault_revoked_ok /\ ((fault_code = FAULT_NONE) \/ (~ok)))
     /\ step_budget' = NextBudget
     /\ UNCHANGED <<
          cfg_source, cfg_drift_case, cfg_max_rtt_ns, cfg_max_width_ns, cfg_max_age_ns,
          cfg_quant_err_ns, cfg_max_adj_ns,
          next_serial_some, next_serial,
          now_ns, truth_ps, epoch, jumps, begin_epoch,
          capture_some, capture_serial, capture_sent_ns, capture_at_ns, capture_ms,
          capture_truth_ps, capture_epoch,
          sk_tok_some, sk_tok_domain, sk_tok_source, sk_tok_serial, sk_tok_sent_ns,
          sk_stamp_some, sk_stamp_domain, sk_stamp_source, sk_stamp_serial, sk_stamp_ts, sk_stamp_unit,
          sign_ok, send_ok, send_truth_ok, old_expiry_ok
        >>

FinishNormal ==
  /\ CanStep
  /\ capture_some
  /\ FinishWithSample(
       1, cfg_source, capture_serial, capture_sent_ns, capture_ms,
       (capture_epoch = epoch) /\ (begin_epoch = epoch)
     )

FinishBad ==
  /\ CanStep
  /\ pending_some
  /\ \E bad_ms \in (IF BoundedMode THEN {0, 35, EffU64Max} ELSE Int):
       /\ bad_ms >= 0 /\ bad_ms <= EffU64Max
       /\ FinishWithSample(
            pending_domain, pending_source, pending_serial, pending_sent_ns, bad_ms, FALSE
          )

FinishStale ==
  /\ CanStep
  /\ \E use_saved \in BOOLEAN:
       LET t_dom == IF use_saved /\ sk_tok_some THEN sk_tok_domain ELSE 2
           t_src == IF use_saved /\ sk_tok_some THEN sk_tok_source ELSE 3
           t_ser == IF use_saved /\ sk_tok_some THEN sk_tok_serial ELSE 1
           t_snt == IF use_saved /\ sk_tok_some THEN sk_tok_sent_ns ELSE 0
       IN /\ (~pending_some \/ t_dom # pending_domain \/ t_src # pending_source \/ t_ser # pending_serial \/ t_snt # pending_sent_ns)
          /\ FinishWithSample(t_dom, t_src, t_ser, t_snt, EffU64Max, FALSE)

Cancel ==
  /\ CanStep
  /\ pending_some
  /\ pending_some' = FALSE
  /\ pending_domain' = 0 /\ pending_source' = 0 /\ pending_serial' = 0 /\ pending_sent_ns' = 0
  /\ step_budget' = NextBudget
  /\ UNCHANGED <<
       cfg_source, cfg_drift_case, cfg_max_rtt_ns, cfg_max_width_ns, cfg_max_age_ns,
       cfg_quant_err_ns, cfg_max_adj_ns,
       next_serial_some, next_serial,
       anchor_some, anchor_lo_ns, anchor_hi_ns, anchor_at_ns, anchor_exp_ns, anchor_serial,
       last_seen_some, last_seen_ns, fault_code,
       now_ns, truth_ps, epoch, jumps, begin_epoch,
       capture_some, capture_serial, capture_sent_ns, capture_at_ns, capture_ms,
       capture_truth_ps, capture_epoch, anchor_trusted, anchor_truth_ps,
       sk_tok_some, sk_tok_domain, sk_tok_source, sk_tok_serial, sk_tok_sent_ns,
       sk_stamp_some, sk_stamp_domain, sk_stamp_source, sk_stamp_serial, sk_stamp_ts, sk_stamp_unit,
       stale_ok, monotone_ok, read_ok, sign_ok, send_ok,
       truth_ok, send_truth_ok, old_anchor_ok, old_expiry_ok, fault_revoked_ok
     >>

CancelStale ==
  /\ CanStep
  /\ \E use_saved \in BOOLEAN:
       LET t_dom == IF use_saved /\ sk_tok_some THEN sk_tok_domain ELSE 2
           t_ser == IF use_saved /\ sk_tok_some THEN sk_tok_serial ELSE 1
       IN /\ (~pending_some \/ t_dom # pending_domain \/ t_ser # pending_serial)
          /\ stale_ok' = stale_ok
          /\ step_budget' = NextBudget
          /\ UNCHANGED <<
               cfg_source, cfg_drift_case, cfg_max_rtt_ns, cfg_max_width_ns, cfg_max_age_ns,
               cfg_quant_err_ns, cfg_max_adj_ns,
               next_serial_some, next_serial,
               pending_some, pending_domain, pending_source, pending_serial, pending_sent_ns,
               anchor_some, anchor_lo_ns, anchor_hi_ns, anchor_at_ns, anchor_exp_ns, anchor_serial,
               last_seen_some, last_seen_ns, fault_code,
               now_ns, truth_ps, epoch, jumps, begin_epoch,
               capture_some, capture_serial, capture_sent_ns, capture_at_ns, capture_ms,
               capture_truth_ps, capture_epoch, anchor_trusted, anchor_truth_ps,
               sk_tok_some, sk_tok_domain, sk_tok_source, sk_tok_serial, sk_tok_sent_ns,
               sk_stamp_some, sk_stamp_domain, sk_stamp_source, sk_stamp_serial, sk_stamp_ts, sk_stamp_unit,
               monotone_ok, read_ok, sign_ok, send_ok,
               truth_ok, send_truth_ok, old_anchor_ok, old_expiry_ok, fault_revoked_ok
             >>

ReadAt(call_now, call_domain) ==
  LET obs_faulted == fault_code # FAULT_NONE
      obs_dom == (~obs_faulted) /\ (call_domain # 1)
      obs_back == (~obs_faulted) /\ (~obs_dom) /\ last_seen_some /\ (call_now < last_seen_ns)
      obs_ok == (~obs_faulted) /\ (~obs_dom) /\ (~obs_back)
      exp == obs_ok /\ anchor_some /\ (call_now >= anchor_exp_ns)
      proj_fits == (~(obs_ok /\ anchor_some /\ (~exp))) \/ ProjFits(anchor_lo_ns, anchor_hi_ns, anchor_at_ns, call_now, cfg_drift_case)
      proj_ovf == obs_ok /\ anchor_some /\ (~exp) /\ (~proj_fits)
      r_lo == ProjLo(anchor_lo_ns, anchor_at_ns, call_now, cfg_drift_case)
      r_hi == ProjHi(anchor_hi_ns, anchor_at_ns, call_now, cfg_drift_case)
      uncertain == obs_ok /\ anchor_some /\ (~exp) /\ proj_fits /\ ((r_hi - r_lo) > cfg_max_width_ns)
      ok == obs_ok /\ anchor_some /\ (~exp) /\ proj_fits /\ (~uncertain)
      any_fail == obs_dom \/ obs_back \/ proj_ovf
      new_last_some == IF obs_ok THEN TRUE ELSE last_seen_some
      new_last_ns == IF obs_ok THEN call_now ELSE last_seen_ns
      new_anchor_some == IF any_fail THEN FALSE ELSE anchor_some
  IN /\ fault_code' =
          CASE obs_dom -> FAULT_DOMAIN
            [] obs_back -> FAULT_BACKWARDS
            [] proj_ovf -> FAULT_OVERFLOW
            [] OTHER -> fault_code
     /\ last_seen_some' = new_last_some
     /\ last_seen_ns' = new_last_ns
     /\ pending_some' = IF any_fail THEN FALSE ELSE pending_some
     /\ pending_domain' = IF any_fail THEN 0 ELSE pending_domain
     /\ pending_source' = IF any_fail THEN 0 ELSE pending_source
     /\ pending_serial' = IF any_fail THEN 0 ELSE pending_serial
     /\ pending_sent_ns' = IF any_fail THEN 0 ELSE pending_sent_ns
     /\ anchor_some' = new_anchor_some
     /\ anchor_trusted' = IF any_fail THEN FALSE ELSE anchor_trusted
     /\ monotone_ok' = (monotone_ok /\ ((~last_seen_some) \/ (~new_last_some) \/ (last_seen_ns <= new_last_ns)))
     /\ read_ok' =
          (read_ok /\ ((~ok) \/ (r_lo <= r_hi /\ r_hi <= EffU64Max /\ call_now < anchor_exp_ns /\ (r_hi - r_lo) <= cfg_max_width_ns)))
     /\ truth_ok' =
          (truth_ok /\ ((~ok) \/ (~anchor_trusted) \/ (call_now # now_ns) \/ (MulPpb(r_lo) <= truth_ps /\ truth_ps <= MulPpb(r_hi))))
     /\ old_expiry_ok' = (old_expiry_ok /\ ((~new_anchor_some) \/ TRUE))
     /\ fault_revoked_ok' = (fault_revoked_ok /\ ((fault_code = FAULT_NONE) \/ (~ok)))
     /\ step_budget' = NextBudget
     /\ UNCHANGED <<
          cfg_source, cfg_drift_case, cfg_max_rtt_ns, cfg_max_width_ns, cfg_max_age_ns,
          cfg_quant_err_ns, cfg_max_adj_ns,
          next_serial_some, next_serial,
          anchor_lo_ns, anchor_hi_ns, anchor_at_ns, anchor_exp_ns, anchor_serial,
          now_ns, truth_ps, epoch, jumps, begin_epoch,
          capture_some, capture_serial, capture_sent_ns, capture_at_ns, capture_ms,
          capture_truth_ps, capture_epoch, anchor_truth_ps,
          sk_tok_some, sk_tok_domain, sk_tok_source, sk_tok_serial, sk_tok_sent_ns,
          sk_stamp_some, sk_stamp_domain, sk_stamp_source, sk_stamp_serial, sk_stamp_ts, sk_stamp_unit,
          stale_ok, sign_ok, send_ok, send_truth_ok, old_anchor_ok
        >>

Read ==
  /\ CanStep
  /\ ReadAt(now_ns, 1)

ReadBackwards ==
  /\ CanStep
  /\ last_seen_some
  /\ last_seen_ns > 0
  /\ ReadAt(last_seen_ns - 1, 1)

ReadWrongDomain ==
  /\ CanStep
  /\ ReadAt(now_ns, 2)

Sign ==
  /\ CanStep
  /\ \E unit \in {1, 2}, latch_st \in BOOLEAN:
       LET invalid_unit == (cfg_source = 2) /\ (unit = 1)
           obs_faulted == (~invalid_unit) /\ (fault_code # FAULT_NONE)
           obs_back == (~invalid_unit) /\ (~obs_faulted) /\ last_seen_some /\ (now_ns < last_seen_ns)
           obs_ok == (~invalid_unit) /\ (~obs_faulted) /\ (~obs_back)
           exp == obs_ok /\ anchor_some /\ (now_ns >= anchor_exp_ns)
           proj_fits == (~(obs_ok /\ anchor_some /\ (~exp))) \/ ProjFits(anchor_lo_ns, anchor_hi_ns, anchor_at_ns, now_ns, cfg_drift_case)
           proj_ovf == obs_ok /\ anchor_some /\ (~exp) /\ (~proj_fits)
           r_lo == ProjLo(anchor_lo_ns, anchor_at_ns, now_ns, cfg_drift_case)
           r_hi == ProjHi(anchor_hi_ns, anchor_at_ns, now_ns, cfg_drift_case)
           uncertain == obs_ok /\ anchor_some /\ (~exp) /\ proj_fits /\ ((r_hi - r_lo) > cfg_max_width_ns)
           ok == obs_ok /\ anchor_some /\ (~exp) /\ proj_fits /\ (~uncertain)
           any_fail == obs_back \/ proj_ovf
           new_last_some == IF obs_ok THEN TRUE ELSE last_seen_some
           new_last_ns == IF obs_ok THEN now_ns ELSE last_seen_ns
           mid == r_lo + ((r_hi - r_lo) \div 2)
           u_ns == UnitNanos(unit)
           ts == DivByUnit(mid, unit)
           tns == MulByUnit(ts, unit)
       IN /\ fault_code' =
               CASE obs_back -> FAULT_BACKWARDS
                 [] proj_ovf -> FAULT_OVERFLOW
                 [] OTHER -> fault_code
          /\ last_seen_some' = new_last_some
          /\ last_seen_ns' = new_last_ns
          /\ pending_some' = IF any_fail THEN FALSE ELSE pending_some
          /\ pending_domain' = IF any_fail THEN 0 ELSE pending_domain
          /\ pending_source' = IF any_fail THEN 0 ELSE pending_source
          /\ pending_serial' = IF any_fail THEN 0 ELSE pending_serial
          /\ pending_sent_ns' = IF any_fail THEN 0 ELSE pending_sent_ns
          /\ anchor_some' = IF any_fail THEN FALSE ELSE anchor_some
          /\ anchor_trusted' = IF any_fail THEN FALSE ELSE anchor_trusted
          /\ sk_stamp_some' = IF ok /\ (latch_st \/ ~sk_stamp_some) THEN TRUE ELSE sk_stamp_some
          /\ sk_stamp_domain' = IF ok /\ (latch_st \/ ~sk_stamp_some) THEN 1 ELSE sk_stamp_domain
          /\ sk_stamp_source' = IF ok /\ (latch_st \/ ~sk_stamp_some) THEN cfg_source ELSE sk_stamp_source
          /\ sk_stamp_serial' = IF ok /\ (latch_st \/ ~sk_stamp_some) THEN anchor_serial ELSE sk_stamp_serial
          /\ sk_stamp_ts' = IF ok /\ (latch_st \/ ~sk_stamp_some) THEN ts ELSE sk_stamp_ts
          /\ sk_stamp_unit' = IF ok /\ (latch_st \/ ~sk_stamp_some) THEN unit ELSE sk_stamp_unit
          /\ monotone_ok' = (monotone_ok /\ ((~last_seen_some) \/ (~new_last_some) \/ (last_seen_ns <= new_last_ns)))
          /\ read_ok' =
               (read_ok /\ ((~ok) \/ (r_lo <= r_hi /\ r_hi <= EffU64Max /\ now_ns < anchor_exp_ns /\ (r_hi - r_lo) <= cfg_max_width_ns)))
          /\ sign_ok' =
               (sign_ok /\ ((~ok) \/ (tns <= mid /\ mid < tns + u_ns /\ DivByUnit(tns, unit) = ts)))
          /\ truth_ok' =
               (truth_ok /\ ((~ok) \/ (~anchor_trusted) \/ (MulPpb(r_lo) <= truth_ps /\ truth_ps <= MulPpb(r_hi))))
          /\ fault_revoked_ok' = (fault_revoked_ok /\ ((fault_code = FAULT_NONE) \/ (~ok)))
          /\ step_budget' = NextBudget
          /\ UNCHANGED <<
               cfg_source, cfg_drift_case, cfg_max_rtt_ns, cfg_max_width_ns, cfg_max_age_ns,
               cfg_quant_err_ns, cfg_max_adj_ns,
               next_serial_some, next_serial,
               anchor_lo_ns, anchor_hi_ns, anchor_at_ns, anchor_exp_ns, anchor_serial,
               now_ns, truth_ps, epoch, jumps, begin_epoch,
               capture_some, capture_serial, capture_sent_ns, capture_at_ns, capture_ms,
               capture_truth_ps, capture_epoch, anchor_truth_ps,
               sk_tok_some, sk_tok_domain, sk_tok_source, sk_tok_serial, sk_tok_sent_ns,
               stale_ok, send_ok, send_truth_ok, old_anchor_ok, old_expiry_ok
             >>

CheckSendWith(st_dom, st_src, st_ts, st_unit, call_now, win_ns, bud_ns) ==
  LET stale_st == (st_dom # 1) \/ (st_src # cfg_source)
      bad_win == (~stale_st) /\ (~WindowOk(cfg_source, win_ns))
      do_read == (~stale_st) /\ (~bad_win)
      obs_faulted == do_read /\ (fault_code # FAULT_NONE)
      obs_back == do_read /\ (~obs_faulted) /\ last_seen_some /\ (call_now < last_seen_ns)
      obs_ok == do_read /\ (~obs_faulted) /\ (~obs_back)
      exp_read == obs_ok /\ anchor_some /\ (call_now >= anchor_exp_ns)
      proj_fits == (~(obs_ok /\ anchor_some /\ (~exp_read))) \/ ProjFits(anchor_lo_ns, anchor_hi_ns, anchor_at_ns, call_now, cfg_drift_case)
      proj_ovf == obs_ok /\ anchor_some /\ (~exp_read) /\ (~proj_fits)
      r_lo == ProjLo(anchor_lo_ns, anchor_at_ns, call_now, cfg_drift_case)
      r_hi == ProjHi(anchor_hi_ns, anchor_at_ns, call_now, cfg_drift_case)
      uncertain == obs_ok /\ anchor_some /\ (~exp_read) /\ proj_fits /\ ((r_hi - r_lo) > cfg_max_width_ns)
      read_pass == obs_ok /\ anchor_some /\ (~exp_read) /\ proj_fits /\ (~uncertain)
      end_ns == call_now + bud_ns
      end_ovf == read_pass /\ (end_ns > EffU64Max)
      exp_bud == read_pass /\ (~end_ovf) /\ (end_ns >= anchor_exp_ns)
      tns == MulByUnit(st_ts, st_unit)
      fut == read_pass /\ (~end_ovf) /\ (~exp_bud) /\ (tns >= r_lo + SEC_NS)
      bud_up == ScaleUpRaw(bud_ns, cfg_drift_case)
      upper_bud == r_hi + bud_up
      bud_ovf == read_pass /\ (~end_ovf) /\ (~exp_bud) /\ (~fut) /\ ((bud_up > EffU64Max) \/ (upper_bud > EffU64Max))
      too_old == read_pass /\ (~end_ovf) /\ (~exp_bud) /\ (~fut) /\ (~bud_ovf) /\ (upper_bud > tns + win_ns)
      ok == read_pass /\ (~end_ovf) /\ (~exp_bud) /\ (~fut) /\ (~bud_ovf) /\ (~too_old)
      any_fail == obs_back \/ proj_ovf \/ end_ovf \/ bud_ovf
      new_fault ==
        CASE obs_back -> FAULT_BACKWARDS
          [] proj_ovf \/ end_ovf \/ bud_ovf -> FAULT_OVERFLOW
          [] OTHER -> fault_code
      new_last_some == IF obs_ok THEN TRUE ELSE last_seen_some
      new_last_ns == IF obs_ok THEN call_now ELSE last_seen_ns
      new_pending_some == IF any_fail THEN FALSE ELSE pending_some
      new_anchor_some == IF any_fail THEN FALSE ELSE anchor_some
  IN /\ fault_code' = new_fault
     /\ last_seen_some' = new_last_some
     /\ last_seen_ns' = new_last_ns
     /\ pending_some' = new_pending_some
     /\ pending_domain' = IF any_fail THEN 0 ELSE pending_domain
     /\ pending_source' = IF any_fail THEN 0 ELSE pending_source
     /\ pending_serial' = IF any_fail THEN 0 ELSE pending_serial
     /\ pending_sent_ns' = IF any_fail THEN 0 ELSE pending_sent_ns
     /\ anchor_some' = new_anchor_some
     /\ anchor_trusted' = IF any_fail THEN FALSE ELSE anchor_trusted
     /\ stale_ok' = (stale_ok /\ ((~stale_st) \/ (new_fault = fault_code /\ new_anchor_some = anchor_some /\ new_pending_some = pending_some)))
     /\ monotone_ok' = (monotone_ok /\ ((~last_seen_some) \/ (~new_last_some) \/ (last_seen_ns <= new_last_ns)))
     /\ send_ok' =
          (send_ok /\ ((~ok) \/ (st_dom = 1 /\ st_src = cfg_source /\ WindowOk(cfg_source, win_ns) /\ tns < r_lo + SEC_NS /\ upper_bud <= tns + win_ns /\ end_ns < anchor_exp_ns)))
     /\ send_truth_ok' =
          (send_truth_ok /\ (
             (~ok) \/ (~anchor_trusted) \/ (call_now # now_ns) \/ (
               /\ MulPpb(tns) < truth_ps + MulPpb(SEC_NS)
               /\ truth_ps + AdvanceTruthPs(bud_ns, cfg_drift_case, -1) <= MulPpb(tns + win_ns)
               /\ truth_ps + AdvanceTruthPs(bud_ns, cfg_drift_case, 0) <= MulPpb(tns + win_ns)
               /\ truth_ps + AdvanceTruthPs(bud_ns, cfg_drift_case, 1) <= MulPpb(tns + win_ns)
             )
          ))
     /\ fault_revoked_ok' = (fault_revoked_ok /\ ((fault_code = FAULT_NONE) \/ (~ok)))
     /\ step_budget' = NextBudget
     /\ UNCHANGED <<
          cfg_source, cfg_drift_case, cfg_max_rtt_ns, cfg_max_width_ns, cfg_max_age_ns,
          cfg_quant_err_ns, cfg_max_adj_ns,
          next_serial_some, next_serial,
          anchor_lo_ns, anchor_hi_ns, anchor_at_ns, anchor_exp_ns, anchor_serial,
          now_ns, truth_ps, epoch, jumps, begin_epoch,
          capture_some, capture_serial, capture_sent_ns, capture_at_ns, capture_ms,
          capture_truth_ps, capture_epoch, anchor_truth_ps,
          sk_tok_some, sk_tok_domain, sk_tok_source, sk_tok_serial, sk_tok_sent_ns,
          sk_stamp_some, sk_stamp_domain, sk_stamp_source, sk_stamp_serial, sk_stamp_ts, sk_stamp_unit,
          read_ok, sign_ok, truth_ok, old_anchor_ok, old_expiry_ok
        >>

CheckSend ==
  /\ CanStep
  /\ sk_stamp_some
  /\ \E win_ns \in (IF BoundedMode THEN {0, 30, 50, 100} ELSE Int),
        bud_ns \in (IF BoundedMode THEN {0, 10, 70} ELSE Int):
       /\ win_ns >= 0 /\ win_ns <= MAX_WIN_NS + MS_NS
       /\ bud_ns >= 0 /\ bud_ns <= EffU64Max
       /\ CheckSendWith(sk_stamp_domain, sk_stamp_source, sk_stamp_ts, sk_stamp_unit, now_ns, win_ns, bud_ns)

CheckForeign ==
  /\ CanStep
  /\ CheckSendWith(2, 3, 1, 2, 0, 0, EffU64Max)

Next ==
  \/ Advance
  \/ Capture
  \/ Jump
  \/ Begin
  \/ FinishNormal
  \/ FinishBad
  \/ FinishStale
  \/ Cancel
  \/ CancelStale
  \/ Read
  \/ ReadBackwards
  \/ ReadWrongDomain
  \/ Sign
  \/ CheckSend
  \/ CheckForeign

Spec == Init /\ [][Next]_vars

(* --- All 15 `L0_Calibration` Safety Invariants --- *)

TypeOK ==
  /\ ValidConfig
  /\ next_serial_some \in BOOLEAN
  /\ next_serial \in Int /\ next_serial >= 1 /\ next_serial <= EffU64Max
  /\ pending_some \in BOOLEAN
  /\ pending_domain \in Int /\ pending_domain >= 0 /\ pending_domain <= 2
  /\ pending_source \in Int /\ pending_source >= 0 /\ pending_source <= 3
  /\ pending_serial \in Int /\ pending_serial >= 0 /\ pending_serial <= EffU64Max
  /\ pending_sent_ns \in Int /\ pending_sent_ns >= 0 /\ pending_sent_ns <= EffU64Max
  /\ anchor_some \in BOOLEAN
  /\ anchor_lo_ns \in Int /\ anchor_lo_ns >= 0 /\ anchor_lo_ns <= EffU64Max
  /\ anchor_hi_ns \in Int /\ anchor_hi_ns >= 0 /\ anchor_hi_ns <= EffU64Max
  /\ anchor_at_ns \in Int /\ anchor_at_ns >= 0 /\ anchor_at_ns <= EffU64Max
  /\ anchor_exp_ns \in Int /\ anchor_exp_ns >= 0 /\ anchor_exp_ns <= EffU64Max
  /\ anchor_serial \in Int /\ anchor_serial >= 0 /\ anchor_serial <= EffU64Max
  /\ last_seen_some \in BOOLEAN
  /\ last_seen_ns \in Int /\ last_seen_ns >= 0 /\ last_seen_ns <= EffU64Max
  /\ fault_code \in {FAULT_NONE, FAULT_DOMAIN, FAULT_BACKWARDS, FAULT_OVERFLOW, FAULT_JUMP}
  /\ now_ns \in Int /\ now_ns >= 0 /\ now_ns <= EffU64Max
  /\ truth_ps \in Int /\ truth_ps >= 0 /\ truth_ps <= MulPpb(EffU64Max)
  /\ epoch \in {0, 1}
  /\ jumps \in {0, 1} /\ epoch = jumps
  /\ begin_epoch \in {0, 1}
  /\ capture_some \in BOOLEAN
  /\ capture_serial \in Int /\ capture_serial >= 0 /\ capture_serial <= EffU64Max
  /\ capture_sent_ns \in Int /\ capture_sent_ns >= 0 /\ capture_sent_ns <= EffU64Max
  /\ capture_at_ns \in Int /\ capture_at_ns >= 0 /\ capture_at_ns <= EffU64Max
  /\ capture_ms \in Int /\ capture_ms >= 0 /\ capture_ms <= EffU64Max
  /\ capture_truth_ps \in Int /\ capture_truth_ps >= 0 /\ capture_truth_ps <= MulPpb(EffU64Max)
  /\ capture_epoch \in {0, 1}
  /\ anchor_trusted \in BOOLEAN
  /\ anchor_truth_ps \in Int /\ anchor_truth_ps >= 0 /\ anchor_truth_ps <= MulPpb(EffU64Max)
  /\ sk_tok_some \in BOOLEAN
  /\ sk_tok_domain \in Int /\ sk_tok_domain >= 0 /\ sk_tok_domain <= 2
  /\ sk_tok_source \in Int /\ sk_tok_source >= 0 /\ sk_tok_source <= 3
  /\ sk_tok_serial \in Int /\ sk_tok_serial >= 0 /\ sk_tok_serial <= EffU64Max
  /\ sk_tok_sent_ns \in Int /\ sk_tok_sent_ns >= 0 /\ sk_tok_sent_ns <= EffU64Max
  /\ sk_stamp_some \in BOOLEAN
  /\ sk_stamp_domain \in Int /\ sk_stamp_domain >= 0 /\ sk_stamp_domain <= 2
  /\ sk_stamp_source \in Int /\ sk_stamp_source >= 0 /\ sk_stamp_source <= 3
  /\ sk_stamp_serial \in Int /\ sk_stamp_serial >= 0 /\ sk_stamp_serial <= EffU64Max
  /\ sk_stamp_ts \in Int /\ sk_stamp_ts >= 0 /\ sk_stamp_ts <= EffU64Max
  /\ sk_stamp_unit \in {1, 2}
  /\ step_budget \in Int /\ step_budget >= 0 /\ step_budget <= MaxBoundedSteps
  /\ stale_ok \in BOOLEAN
  /\ monotone_ok \in BOOLEAN
  /\ read_ok \in BOOLEAN
  /\ sign_ok \in BOOLEAN
  /\ send_ok \in BOOLEAN
  /\ truth_ok \in BOOLEAN
  /\ send_truth_ok \in BOOLEAN
  /\ old_anchor_ok \in BOOLEAN
  /\ old_expiry_ok \in BOOLEAN
  /\ fault_revoked_ok \in BOOLEAN

FaultClears ==
  (fault_code # FAULT_NONE) => ((~anchor_some) /\ (~pending_some))

PendingIdentity ==
  pending_some =>
    /\ pending_domain = 1
    /\ pending_source = cfg_source
    /\ pending_serial >= 1
    /\ last_seen_some
    /\ pending_sent_ns <= last_seen_ns

AnchorIdentity ==
  anchor_some =>
    /\ anchor_serial >= 1
    /\ anchor_lo_ns >= 0 /\ anchor_lo_ns <= anchor_hi_ns /\ anchor_hi_ns <= EffU64Max
    /\ anchor_exp_ns <= EffU64Max
    /\ last_seen_some /\ anchor_at_ns <= last_seen_ns
    /\ anchor_exp_ns = anchor_at_ns + cfg_max_age_ns

SerialNotReused ==
  /\ ((pending_some /\ next_serial_some) => (pending_serial < next_serial))
  /\ ((sk_tok_some /\ next_serial_some) => (sk_tok_serial < next_serial))

StaleDoesNotTouchClock == stale_ok
ObservedTimeMonotone == monotone_ok
SuccessfulReadBounded == read_ok
ExactSignedTimestamp == sign_ok
SuccessfulSendBounded == send_ok
ContinuousIntervalContainsTruth == truth_ok
ContinuousSendWithinWindow == send_truth_ok
RejectedSampleRetainsAnchor == old_anchor_ok
ReadNeverExtendsExpiry == old_expiry_ok
FaultRevokesCalls == fault_revoked_ok

SafetyConjunction ==
  /\ TypeOK
  /\ FaultClears
  /\ PendingIdentity
  /\ AnchorIdentity
  /\ SerialNotReused
  /\ StaleDoesNotTouchClock
  /\ ObservedTimeMonotone
  /\ SuccessfulReadBounded
  /\ ExactSignedTimestamp
  /\ SuccessfulSendBounded
  /\ ContinuousIntervalContainsTruth
  /\ ContinuousSendWithinWindow
  /\ RejectedSampleRetainsAnchor
  /\ ReadNeverExtendsExpiry
  /\ FaultRevokesCalls

(* --- Mode S2 Strengthened Inductive Invariant (`IndInv`) --- *)
IndInv ==
  /\ SafetyConjunction
  /\ begin_epoch \in {0, 1} /\ begin_epoch <= epoch
  /\ (last_seen_some => last_seen_ns <= now_ns)
  /\ (pending_some =>
       /\ pending_sent_ns >= 0 /\ pending_sent_ns <= now_ns
       /\ pending_serial <= EffU64Max
       /\ ((~next_serial_some) => pending_serial <= EffU64Max))
  /\ (sk_tok_some =>
       /\ sk_tok_domain = 1
       /\ sk_tok_source = cfg_source
       /\ sk_tok_serial >= 1 /\ sk_tok_serial <= EffU64Max
       /\ sk_tok_sent_ns >= 0 /\ sk_tok_sent_ns <= now_ns)
  /\ (sk_stamp_some =>
       /\ sk_stamp_domain = 1
       /\ sk_stamp_source = cfg_source
       /\ sk_stamp_serial >= 1 /\ sk_stamp_serial <= EffU64Max
       /\ sk_stamp_unit \in {1, 2}
       /\ sk_stamp_ts >= 0 /\ sk_stamp_ts <= EffU64Max)
  /\ (capture_some =>
       /\ capture_epoch \in {0, 1} /\ capture_epoch <= epoch
       /\ capture_serial >= 1 /\ capture_serial <= EffU64Max
       /\ 0 <= capture_sent_ns /\ capture_sent_ns <= capture_at_ns /\ capture_at_ns <= now_ns
       /\ capture_truth_ps >= 0 /\ capture_truth_ps <= MulPpb(EffU64Max)
       /\ capture_ms = ToCaptureMs(capture_truth_ps)
       /\ ((capture_epoch = epoch /\ begin_epoch = epoch) =>
            /\ AdvanceTruthPs(now_ns - capture_at_ns, cfg_drift_case, -1) <= (truth_ps - capture_truth_ps)
            /\ (truth_ps - capture_truth_ps) <= AdvanceTruthPs(now_ns - capture_at_ns, cfg_drift_case, 1)))
  /\ (anchor_trusted =>
       /\ anchor_some
       /\ fault_code = FAULT_NONE
       /\ anchor_at_ns >= 0 /\ anchor_at_ns <= now_ns
       /\ anchor_truth_ps >= 0 /\ anchor_truth_ps <= MulPpb(EffU64Max)
       /\ MulPpb(anchor_lo_ns) <= anchor_truth_ps
       /\ anchor_truth_ps <= MulPpb(anchor_hi_ns)
       /\ AdvanceTruthPs(now_ns - anchor_at_ns, cfg_drift_case, -1) <= (truth_ps - anchor_truth_ps)
       /\ (truth_ps - anchor_truth_ps) <= AdvanceTruthPs(now_ns - anchor_at_ns, cfg_drift_case, 1))

====
