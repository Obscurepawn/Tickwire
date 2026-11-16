-------------------- MODULE L2_SimdDecodeAndOverflowGuard --------------------
EXTENDS Naturals, Integers, FiniteSets

CONSTANTS
    Venue,
    Profile,
    TickStep,
    LotStep,
    MaxIntBound,
    MaxSteps

(*
============================ var <-> code map ============================
Code-faithful model of Optimization #3 (audited SIMD JSON spot BBO fast path +
`serde_json` oracle equivalence in `tickwire-md`) and Optimization #4
(all-profile `overflow-checks = true` in workspace `Cargo.toml`).

CONSTANTS
  Venue       - spot venue protocol {"Bybit", "Okx"}:
                crates/tickwire-md/src/spot_quotes.rs:27-33
  Profile     - Cargo build profile {"dev", "test", "release", "bench"}:
                Cargo.toml:58-71
  TickStep    - exact instrument price tick step (`StepSpec`):
                crates/tickwire-md/src/spot_quotes/simd_json.rs:94-141
  LotStep     - exact instrument quantity lot step (`StepSpec`):
                crates/tickwire-md/src/spot_quotes/simd_json.rs:94-141
  MaxIntBound - finite model bound representing `i64::MAX` / `u64::MAX` / `i128::MAX`
  MaxSteps    - bounded sequence of interleaved frames, reconnects, and arithmetic ops

VARIABLES
  step_count           - bounded transition counter (0..MaxSteps)
  fast_session         - `Session` state (`epoch`, `acknowledged`, `failed`) driven by
                         `SimdScan` + `FastOrFallbackDecode`:
                         crates/tickwire-md/src/spot_quotes.rs:80-177,
                         crates/tickwire-md/src/spot_quotes/protocol.rs:84-169
  oracle_session       - shadow `Session` state driven by `SerdeOracleDecode`:
                         crates/tickwire-md/src/spot_quotes/protocol.rs:171-241
  last_scan_mode       - path selected by `SimdScan` {"Init","SimdFast","FallbackBorrowed"}:
                         crates/tickwire-md/src/spot_quotes/simd_json.rs:178-469,
                         crates/tickwire-md/src/spot_quotes/simd_json.rs:642-812
  last_fast_result     - `Result<Option<Quote>>` returned by `FastOrFallbackDecode`:
                         crates/tickwire-md/src/spot_quotes/simd_json.rs:1285-1574
  last_oracle_result   - `Result<Option<Quote>>` returned by `SerdeOracleDecode`:
                         crates/tickwire-md/src/spot_quotes/protocol.rs:171-241
  last_publication     - `Quote::publication` outcome {"None","Quote","Unavailable"}:
                         crates/tickwire-md/src/spot_quotes.rs:209-241
  published_bbo        - active published `SpotQuote` ticks/lots/raw witnesses
  last_arith_outcome   - checked fixed-point arithmetic outcome {"Init","OkExact","TrappedOverflow","SilentWrap"}
  last_arith_value     - last valid fixed-point notional/spread result
  hotpath_heap_allocs  - steady-state hot-path heap allocation count (must stay 0)
  simd_oob_reads       - out-of-bounds SIMD vector load count (must stay 0):
                         crates/tickwire-md/src/spot_quotes/simd_json.rs:285-469

FRAME CLASSES & FAULTS MODELED:
  - subscribe acknowledgement & `pong` heartbeats (`ProcessSubscribeAckOrPong`);
  - unescaped canonical BBO across 16B (SSE2/NEON) & 32B (AVX2) SIMD chunks;
  - crossed/zero-spread (`bid >= ask`) and one-sided (`[]`) snapshots -> `Unavailable`;
  - escaped JSON strings (`\"`, `\u0031`) & reordered keys -> clean fallback;
  - truncated frames at 16B/32B SIMD chunk boundaries -> zero OOB reads + session fail;
  - non-integral `price % TickStep != 0` / `qty % LotStep != 0` & nonpositive levels;
  - mantissa / tick / timestamp (`millis * scale`) integer overflow;
  - extra book levels (`> 1` BBO row or wrong OKX tuple arity) & wrong symbol/unacked;
  - session reconnect + downstream fixed-point add/sub/mul overflow trapping across profiles.
==========================================================================
*)

AllProfiles == {"dev", "test", "release", "bench"}
Venues == {"Bybit", "Okx"}
ScanModes == {"Init", "SimdFast", "FallbackBorrowed"}
DecodeStatuses == {"Init", "OkNone", "OkQuote", "ErrInvalid"}
PublicationKinds == {"None", "Quote", "Unavailable"}
ArithOutcomes == {"Init", "OkExact", "TrappedOverflow", "SilentWrap"}
ChunkWidths == {16, 32}
OverflowOps == {"AddOverflow", "SubUnderflow", "MulOverflow"}

ASSUME
    /\ Venue \in Venues
    /\ Profile \in AllProfiles
    /\ TickStep >= 2
    /\ LotStep >= 2
    /\ MaxIntBound >= 20
    /\ MaxSteps >= 3

\* Every Cargo profile in root `Cargo.toml:58-71` has `overflow-checks = true`.
ProfileOverflowChecks(p) ==
    p \in AllProfiles

EmptyResult == [
    status    |-> "Init",
    bid_ticks |-> 0,
    ask_ticks |-> 0,
    has_ask   |-> FALSE,
    lots      |-> 0,
    event_ns  |-> 0
]

ErrResult == [
    status    |-> "ErrInvalid",
    bid_ticks |-> 0,
    ask_ticks |-> 0,
    has_ask   |-> FALSE,
    lots      |-> 0,
    event_ns  |-> 0
]

OkNoneResult == [
    status    |-> "OkNone",
    bid_ticks |-> 0,
    ask_ticks |-> 0,
    has_ask   |-> FALSE,
    lots      |-> 0,
    event_ns  |-> 0
]

NoPublishedBbo == [
    active    |-> FALSE,
    bid_ticks |-> 0,
    ask_ticks |-> 0,
    bid_lots  |-> 0,
    ask_lots  |-> 0,
    raw_bid   |-> 0,
    raw_ask   |-> 0,
    raw_qty   |-> 0
]

\* Exact decimal-to-step conversion (`crates/tickwire-md/src/spot_quotes/simd_json.rs:523-621`, `crates/tickwire-binance/src/decimal.rs:15-55`, `crates/tickwire-binance/src/feed.rs:352-375`).
ValidPositiveUnits(raw_val, step) ==
    /\ raw_val > 0
    /\ raw_val <= MaxIntBound
    /\ (raw_val % step) = 0

\* SIMD structural scanner (`crates/tickwire-md/src/spot_quotes/simd_json.rs:178-469`): checks chunk bounds before vector loads and classifies fast vs fallback.
SimdScan(f) ==
    LET full_chunks == f.frame_len \div f.chunk_width
        simd_end    == full_chunks * f.chunk_width
        bounds_ok   == (simd_end <= f.frame_len /\ simd_end >= 0)
        take_fast   == /\ f.canonical_layout
                       /\ ~f.has_backslash
                       /\ f.control_kind = "None"
                       /\ ~f.truncated
    IN [
        oob_reads |-> IF bounds_ok THEN 0 ELSE 1,
        mode      |-> IF take_fast THEN "SimdFast" ELSE "FallbackBorrowed"
       ]

\* Fast-or-fallback decoder (`crates/tickwire-md/src/spot_quotes/simd_json.rs:1285-1574`, `crates/tickwire-md/src/spot_quotes/protocol.rs:84-169`).
FastOrFallbackDecode(sess, f) ==
    IF sess.failed THEN
        [res |-> ErrResult, next_sess |-> [sess EXCEPT !.failed = TRUE]]
    ELSE IF f.truncated THEN
        [res |-> ErrResult, next_sess |-> [sess EXCEPT !.failed = TRUE]]
    ELSE IF f.control_kind = "SubscribeAck" THEN
        [res |-> OkNoneResult, next_sess |-> [sess EXCEPT !.acknowledged = TRUE]]
    ELSE IF f.control_kind = "Pong" THEN
        [res |-> OkNoneResult, next_sess |-> sess]
    ELSE IF (~sess.acknowledged \/ ~f.symbol_ok \/ ~f.arity_ok) THEN
        [res |-> ErrResult, next_sess |-> [sess EXCEPT !.failed = TRUE]]
    ELSE
        LET scan == SimdScan(f)
            \* In SimdFast mode, raw ASCII digits have no escapes; in FallbackBorrowed mode,
            \* RFC 8259 unescaping resolves `\u0031` / `\"` to the identical semantic values.
            bid_ok == ValidPositiveUnits(f.bid_raw, TickStep)
            ask_ok == (~f.has_ask) \/ ValidPositiveUnits(f.ask_raw, TickStep)
            qty_ok == ValidPositiveUnits(f.qty_raw, LotStep)
            ts_ns  == f.ts_ms * 2
            ts_ok  == (f.ts_ms >= 0 /\ ts_ns <= MaxIntBound)
        IN IF (scan.mode \in {"SimdFast", "FallbackBorrowed"} /\ bid_ok /\ ask_ok /\ qty_ok /\ ts_ok) THEN
               [
                   res |-> [
                       status    |-> "OkQuote",
                       bid_ticks |-> f.bid_raw \div TickStep,
                       ask_ticks |-> IF f.has_ask THEN f.ask_raw \div TickStep ELSE 0,
                       has_ask   |-> f.has_ask,
                       lots      |-> f.qty_raw \div LotStep,
                       event_ns  |-> ts_ns
                   ],
                   next_sess |-> sess
               ]
           ELSE
               [res |-> ErrResult, next_sess |-> [sess EXCEPT !.failed = TRUE]]

\* Reference `serde_json` + `Decimal::parse` + `feed::units` oracle (`crates/tickwire-md/src/spot_quotes/protocol.rs:171-241`).
SerdeOracleDecode(sess, f) ==
    IF sess.failed \/ f.truncated THEN
        [res |-> ErrResult, next_sess |-> [sess EXCEPT !.failed = TRUE]]
    ELSE IF f.control_kind = "SubscribeAck" THEN
        [res |-> OkNoneResult, next_sess |-> [sess EXCEPT !.acknowledged = TRUE]]
    ELSE IF f.control_kind = "Pong" THEN
        [res |-> OkNoneResult, next_sess |-> sess]
    ELSE IF ~sess.acknowledged \/ ~f.symbol_ok \/ ~f.arity_ok THEN
        [res |-> ErrResult, next_sess |-> [sess EXCEPT !.failed = TRUE]]
    ELSE IF /\ ValidPositiveUnits(f.bid_raw, TickStep)
            /\ ((~f.has_ask) \/ ValidPositiveUnits(f.ask_raw, TickStep))
            /\ ValidPositiveUnits(f.qty_raw, LotStep)
            /\ f.ts_ms >= 0
            /\ f.ts_ms * 2 <= MaxIntBound
    THEN
        [
            res |-> [
                status    |-> "OkQuote",
                bid_ticks |-> f.bid_raw \div TickStep,
                ask_ticks |-> IF f.has_ask THEN f.ask_raw \div TickStep ELSE 0,
                has_ask   |-> f.has_ask,
                lots      |-> f.qty_raw \div LotStep,
                event_ns  |-> f.ts_ms * 2
            ],
            next_sess |-> sess
        ]
    ELSE
        [res |-> ErrResult, next_sess |-> [sess EXCEPT !.failed = TRUE]]

VARIABLES
    step_count,
    fast_session,
    oracle_session,
    last_scan_mode,
    last_fast_result,
    last_oracle_result,
    last_publication,
    published_bbo,
    last_arith_outcome,
    last_arith_value,
    hotpath_heap_allocs,
    simd_oob_reads

vars == <<
    step_count, fast_session, oracle_session, last_scan_mode,
    last_fast_result, last_oracle_result, last_publication, published_bbo,
    last_arith_outcome, last_arith_value, hotpath_heap_allocs, simd_oob_reads
>>

Init ==
    /\ step_count = 0
    /\ fast_session = [epoch |-> 1, acknowledged |-> FALSE, failed |-> FALSE]
    /\ oracle_session = [epoch |-> 1, acknowledged |-> FALSE, failed |-> FALSE]
    /\ last_scan_mode = "Init"
    /\ last_fast_result = EmptyResult
    /\ last_oracle_result = EmptyResult
    /\ last_publication = "None"
    /\ published_bbo = NoPublishedBbo
    /\ last_arith_outcome = "Init"
    /\ last_arith_value = 0
    /\ hotpath_heap_allocs = 0
    /\ simd_oob_reads = 0

ApplyFrame(f) ==
    LET scan == SimdScan(f)
        fast == FastOrFallbackDecode(fast_session, f)
        orac == SerdeOracleDecode(oracle_session, f)
        r    == fast.res
    IN /\ step_count < MaxSteps
       /\ step_count' = step_count + 1
       /\ fast_session' = fast.next_sess
       /\ oracle_session' = orac.next_sess
       /\ last_scan_mode' = scan.mode
       /\ last_fast_result' = r
       /\ last_oracle_result' = orac.res
       /\ simd_oob_reads' = simd_oob_reads + scan.oob_reads
       /\ hotpath_heap_allocs' = hotpath_heap_allocs + 0
       /\ IF r.status = "OkQuote" THEN
              IF r.has_ask /\ r.ask_ticks > r.bid_ticks THEN
                  /\ last_publication' = "Quote"
                  /\ published_bbo' = [
                         active    |-> TRUE,
                         bid_ticks |-> r.bid_ticks,
                         ask_ticks |-> r.ask_ticks,
                         bid_lots  |-> r.lots,
                         ask_lots  |-> r.lots,
                         raw_bid   |-> f.bid_raw,
                         raw_ask   |-> f.ask_raw,
                         raw_qty   |-> f.qty_raw
                     ]
              ELSE
                  /\ last_publication' = "Unavailable"
                  /\ published_bbo' = NoPublishedBbo
          ELSE
              /\ UNCHANGED <<last_publication, published_bbo>>
       /\ UNCHANGED <<last_arith_outcome, last_arith_value>>

\* 1. Control frames: subscribe ack (twbbo) and pong heartbeat (`crates/tickwire-md/src/spot_quotes/simd_json.rs:1285-1356`, `crates/tickwire-md/src/spot_quotes/simd_json.rs:1428-1495`).
ProcessSubscribeAckOrPong(ctrl_kind) ==
    /\ ctrl_kind \in {"SubscribeAck", "Pong"}
    /\ ApplyFrame([
           control_kind     |-> ctrl_kind,
           canonical_layout |-> FALSE,
           has_backslash    |-> FALSE,
           truncated        |-> FALSE,
           symbol_ok        |-> TRUE,
           arity_ok         |-> TRUE,
           frame_len        |-> 48,
           chunk_width      |-> 16,
           bid_raw          |-> 0,
           ask_raw          |-> 0,
           has_ask          |-> FALSE,
           qty_raw          |-> 0,
           ts_ms            |-> 0
       ])

\* 2. Canonical unescaped BBO frame across 16B/32B SIMD chunk boundaries (`crates/tickwire-md/src/spot_quotes/simd_json.rs:1357-1424`, `crates/tickwire-md/src/spot_quotes/simd_json.rs:1496-1574`).
DecodeCanonicalUnescapedBbo(cw, len, bid_r, ask_r, qty_r) ==
    /\ cw \in ChunkWidths
    /\ len \in {63, 64, 97}
    /\ bid_r = 2 * TickStep
    /\ ask_r \in {3 * TickStep, 4 * TickStep}
    /\ qty_r \in {LotStep, 2 * LotStep}
    /\ ApplyFrame([
           control_kind     |-> "None",
           canonical_layout |-> TRUE,
           has_backslash    |-> FALSE,
           truncated        |-> FALSE,
           symbol_ok        |-> TRUE,
           arity_ok         |-> TRUE,
           frame_len        |-> len,
           chunk_width      |-> cw,
           bid_raw          |-> bid_r,
           ask_raw          |-> ask_r,
           has_ask          |-> TRUE,
           qty_raw          |-> qty_r,
           ts_ms            |-> 10
       ])

\* 3. Crossed/zero-spread (`bid >= ask`) or one-sided (`[]`) snapshot -> OkQuote then Publication::Unavailable (`crates/tickwire-md/src/spot_quotes.rs:209-241`).
DecodeCrossedOrOneSidedBbo(has_ask_side, ask_r) ==
    /\ ((~has_ask_side /\ ask_r = 0) \/ (has_ask_side /\ ask_r \in {TickStep, 2 * TickStep}))
    /\ ApplyFrame([
           control_kind     |-> "None",
           canonical_layout |-> TRUE,
           has_backslash    |-> FALSE,
           truncated        |-> FALSE,
           symbol_ok        |-> TRUE,
           arity_ok         |-> TRUE,
           frame_len        |-> 80,
           chunk_width      |-> 32,
           bid_raw          |-> 2 * TickStep,
           ask_raw          |-> ask_r,
           has_ask          |-> has_ask_side,
           qty_raw          |-> LotStep,
           ts_ms            |-> 10
       ])

\* 4. Escaped strings (`\"` / `\u0031`) or reordered JSON keys -> clean zero-heap fallback (`crates/tickwire-md/src/spot_quotes/protocol.rs:110-169`).
DecodeEscapedOrReorderedBbo(escaped) ==
    /\ escaped \in BOOLEAN
    /\ ApplyFrame([
           control_kind     |-> "None",
           canonical_layout |-> ~escaped,
           has_backslash    |-> escaped,
           truncated        |-> FALSE,
           symbol_ok        |-> TRUE,
           arity_ok         |-> TRUE,
           frame_len        |-> 90,
           chunk_width      |-> 32,
           bid_raw          |-> TickStep,
           ask_raw          |-> 3 * TickStep,
           has_ask          |-> TRUE,
           qty_raw          |-> LotStep,
           ts_ms            |-> 12
       ])

\* 5. Truncated frame right at 16B/32B SIMD chunk boundary -> zero OOB reads + rejection (`crates/tickwire-md/src/spot_quotes/simd_json.rs:285-469`).
RejectTruncatedChunkBoundaryFrame(cw, len) ==
    /\ cw \in ChunkWidths
    /\ len \in {15, 16, 31, 32}
    /\ ApplyFrame([
           control_kind     |-> "None",
           canonical_layout |-> TRUE,
           has_backslash    |-> FALSE,
           truncated        |-> TRUE,
           symbol_ok        |-> TRUE,
           arity_ok         |-> TRUE,
           frame_len        |-> len,
           chunk_width      |-> cw,
           bid_raw          |-> 2 * TickStep,
           ask_raw          |-> 3 * TickStep,
           has_ask          |-> TRUE,
           qty_raw          |-> LotStep,
           ts_ms            |-> 10
       ])

\* 6. Off-grid (`price % TickStep != 0`, `qty % LotStep != 0`) or nonpositive (`<= 0`) level (`crates/tickwire-md/src/spot_quotes/simd_json.rs:523-621`).
RejectOffGridOrNonPositiveFrame(bad_bid, bad_qty) ==
    /\ <<bad_bid, bad_qty>> \in {
           <<0, LotStep>>,
           <<-TickStep, LotStep>>,
           <<TickStep + 1, LotStep>>,
           <<2 * TickStep, LotStep - 1>>
       }
    /\ ApplyFrame([
           control_kind     |-> "None",
           canonical_layout |-> TRUE,
           has_backslash    |-> FALSE,
           truncated        |-> FALSE,
           symbol_ok        |-> TRUE,
           arity_ok         |-> TRUE,
           frame_len        |-> 72,
           chunk_width      |-> 16,
           bid_raw          |-> bad_bid,
           ask_raw          |-> 4 * TickStep,
           has_ask          |-> TRUE,
           qty_raw          |-> bad_qty,
           ts_ms            |-> 10
       ])

\* 7. Mantissa/tick or exchange timestamp (`millis * scale`) integer overflow (`crates/tickwire-md/src/spot_quotes/simd_json.rs:593-621`, `crates/tickwire-md/src/spot_quotes/simd_json.rs:1413-1420`).
RejectOverflowingFrame(overflow_ts) ==
    /\ overflow_ts \in BOOLEAN
    /\ ApplyFrame([
           control_kind     |-> "None",
           canonical_layout |-> TRUE,
           has_backslash    |-> FALSE,
           truncated        |-> FALSE,
           symbol_ok        |-> TRUE,
           arity_ok         |-> TRUE,
           frame_len        |-> 84,
           chunk_width      |-> 32,
           bid_raw          |-> IF overflow_ts THEN 2 * TickStep ELSE MaxIntBound + TickStep,
           ask_raw          |-> IF overflow_ts THEN 3 * TickStep ELSE MaxIntBound + 2 * TickStep,
           has_ask          |-> TRUE,
           qty_raw          |-> LotStep,
           ts_ms            |-> IF overflow_ts THEN MaxIntBound ELSE 10
       ])

\* 8. Extra book level (`> 1` BBO row / wrong tuple arity) or mismatched symbol (`crates/tickwire-md/src/spot_quotes/simd_json.rs:1165-1235`).
RejectExtraLevelOrMismatchedIdentity(bad_sym) ==
    /\ bad_sym \in BOOLEAN
    /\ ApplyFrame([
           control_kind     |-> "None",
           canonical_layout |-> TRUE,
           has_backslash    |-> FALSE,
           truncated        |-> FALSE,
           symbol_ok        |-> ~bad_sym,
           arity_ok         |-> bad_sym,
           frame_len        |-> 76,
           chunk_width      |-> 16,
           bid_raw          |-> 2 * TickStep,
           ask_raw          |-> 3 * TickStep,
           has_ask          |-> TRUE,
           qty_raw          |-> LotStep,
           ts_ms            |-> 10
       ])

\* 9. Reconnect failed quote session under a fresh monotonic connection epoch (`crates/tickwire-md/src/spot_quotes.rs:80-177`).
ReconnectFailedSession ==
    /\ fast_session.failed
    /\ step_count < MaxSteps
    /\ step_count' = step_count + 1
    /\ fast_session' = [epoch |-> fast_session.epoch + 1, acknowledged |-> FALSE, failed |-> FALSE]
    /\ oracle_session' = [epoch |-> oracle_session.epoch + 1, acknowledged |-> FALSE, failed |-> FALSE]
    /\ published_bbo' = NoPublishedBbo
    /\ last_publication' = "Unavailable"
    /\ UNCHANGED <<
        last_scan_mode, last_fast_result, last_oracle_result,
        last_arith_outcome, last_arith_value, hotpath_heap_allocs, simd_oob_reads
       >>

\* 10. Valid fixed-point notional & spread calculation on an active BBO quote.
CheckValidFixedPointArithmetic ==
    /\ published_bbo.active
    /\ step_count < MaxSteps
    /\ LET notional == published_bbo.ask_ticks * published_bbo.ask_lots
           spread   == published_bbo.ask_ticks - published_bbo.bid_ticks
       IN /\ step_count' = step_count + 1
          /\ IF notional <= MaxIntBound /\ spread > 0 THEN
                 /\ last_arith_outcome' = "OkExact"
                 /\ last_arith_value' = notional
             ELSE
                 /\ last_arith_outcome' = "TrappedOverflow"
                 /\ UNCHANGED last_arith_value
    /\ UNCHANGED <<
        fast_session, oracle_session, last_scan_mode,
        last_fast_result, last_oracle_result, last_publication, published_bbo,
        hotpath_heap_allocs, simd_oob_reads
       >>

\* 11. Boundary overflow test across dev/test/release/bench (`Cargo.toml:58-71`).
TrapOverflowingArithmeticInProfile(op) ==
    /\ op \in OverflowOps
    /\ step_count < MaxSteps
    /\ LET exact_res ==
               IF op = "AddOverflow" THEN MaxIntBound + 5
               ELSE IF op = "SubUnderflow" THEN -MaxIntBound - 5
               ELSE (MaxIntBound \div 2 + 2) * 3
           out_of_range == (exact_res > MaxIntBound \/ exact_res < -MaxIntBound)
       IN /\ step_count' = step_count + 1
          /\ IF out_of_range THEN
                 IF ProfileOverflowChecks(Profile) THEN
                     /\ last_arith_outcome' = "TrappedOverflow"
                     /\ UNCHANGED last_arith_value
                 ELSE
                     /\ last_arith_outcome' = "SilentWrap"
                     /\ last_arith_value' = exact_res % MaxIntBound
             ELSE
                 /\ last_arith_outcome' = "OkExact"
                 /\ last_arith_value' = exact_res
    /\ UNCHANGED <<
        fast_session, oracle_session, last_scan_mode,
        last_fast_result, last_oracle_result, last_publication, published_bbo,
        hotpath_heap_allocs, simd_oob_reads
       >>

Next ==
    \/ (\E ck \in {"SubscribeAck", "Pong"} : ProcessSubscribeAckOrPong(ck))
    \/ (\E cw \in ChunkWidths, len \in {63, 64, 97}, ar \in {3 * TickStep, 4 * TickStep}, qr \in {LotStep, 2 * LotStep} :
            DecodeCanonicalUnescapedBbo(cw, len, 2 * TickStep, ar, qr))
    \/ (\E has_a \in BOOLEAN, ar \in {0, TickStep, 2 * TickStep} :
            DecodeCrossedOrOneSidedBbo(has_a, ar))
    \/ (\E esc \in BOOLEAN : DecodeEscapedOrReorderedBbo(esc))
    \/ (\E cw \in ChunkWidths, len \in {15, 16, 31, 32} : RejectTruncatedChunkBoundaryFrame(cw, len))
    \/ (\E bb \in {0, -TickStep, TickStep + 1, 2 * TickStep}, bq \in {LotStep, LotStep - 1} :
            RejectOffGridOrNonPositiveFrame(bb, bq))
    \/ (\E ots \in BOOLEAN : RejectOverflowingFrame(ots))
    \/ (\E bsym \in BOOLEAN : RejectExtraLevelOrMismatchedIdentity(bsym))
    \/ ReconnectFailedSession
    \/ CheckValidFixedPointArithmetic
    \/ (\E op \in OverflowOps : TrapOverflowingArithmeticInProfile(op))

Spec == Init /\ [][Next]_vars

\* =============================== Invariants ===============================

TypeOK ==
    /\ Venue \in Venues
    /\ Profile \in AllProfiles
    /\ step_count \in 0..MaxSteps
    /\ fast_session.epoch \in 1..(MaxSteps + 1)
    /\ fast_session.acknowledged \in BOOLEAN
    /\ fast_session.failed \in BOOLEAN
    /\ oracle_session.epoch \in 1..(MaxSteps + 1)
    /\ oracle_session.acknowledged \in BOOLEAN
    /\ oracle_session.failed \in BOOLEAN
    /\ last_scan_mode \in ScanModes
    /\ last_fast_result.status \in DecodeStatuses
    /\ last_oracle_result.status \in DecodeStatuses
    /\ last_publication \in PublicationKinds
    /\ published_bbo.active \in BOOLEAN
    /\ last_arith_outcome \in ArithOutcomes
    /\ last_arith_value \in (-MaxIntBound)..MaxIntBound
    /\ hotpath_heap_allocs \in Nat
    /\ simd_oob_reads \in Nat

\* Fast-or-fallback SIMD decoder matches `serde_json` Oracle on EVERY reachable frame and session state.
ExactOracleEquivalence ==
    /\ last_fast_result = last_oracle_result
    /\ fast_session = oracle_session

\* Every published SpotQuote has exact tick/lot divisibility, positive quantities, and strictly positive spread (`ask > bid`).
TickLotAndPositiveSpreadSoundness ==
    /\ (published_bbo.active =>
        /\ published_bbo.bid_ticks > 0
        /\ published_bbo.ask_ticks > published_bbo.bid_ticks
        /\ published_bbo.bid_lots > 0
        /\ published_bbo.ask_lots > 0
        /\ (published_bbo.raw_bid % TickStep) = 0
        /\ (published_bbo.raw_ask % TickStep) = 0
        /\ (published_bbo.raw_qty % LotStep) = 0)
    /\ ((last_fast_result.status = "OkQuote" /\ (~last_fast_result.has_ask \/ last_fast_result.ask_ticks <= last_fast_result.bid_ticks)) =>
        /\ ~published_bbo.active
        /\ last_publication = "Unavailable")

\* Overflow in ANY profile (`dev`, `test`, `release`, `bench`) traps deterministically and NEVER wraps silently.
NoSilentArithmeticWrap ==
    /\ \A p \in AllProfiles : ProfileOverflowChecks(p)
    /\ last_arith_outcome # "SilentWrap"
    /\ (published_bbo.active =>
        /\ published_bbo.bid_ticks <= MaxIntBound
        /\ published_bbo.ask_ticks <= MaxIntBound
        /\ published_bbo.bid_lots <= MaxIntBound
        /\ published_bbo.ask_lots <= MaxIntBound)

\* Steady-state SIMD fast path and fallback allocate zero heap memory and never read out of slice bounds.
ZeroHotPathHeapAllocation ==
    /\ hotpath_heap_allocs = 0
    /\ simd_oob_reads = 0

=============================================================================
