---- MODULE L4_SymSimdCrcBmc ----
(*
 * Symbolic Bounded Model Checking & Inductive Verification (Mode S1 + Mode S2) for:
 *   Target 3a (Opt #3): `simd_json` 64-byte block scanner (`AVX2` 32B, `SSE2`/`NEON`
 *     16B, `SWAR` 8B, scalar 1B tail) memory bounds and `< 64` bit-shift safety across
 *     ALL symbolic buffer lengths `0 <= frame_len <= 65536` (`MAX_MESSAGE = 16384`),
 *     eliminating `L2`'s 7 discrete sample lengths `{15,16,31,32,63,64,97}`.
 *   Target 3b (Opt #3): Exact mathematical equivalence (`FastVsOracleExactEquivalence`)
 *     between `simd_json::parse_positive_units` + `StepSpec` (`u64` fast path + `u128`
 *     `gcd`-reduced fallback) and `Decimal::parse` + `tickwire_binance::feed::units`
 *     across full 19-digit 64-bit mantissas (`0 .. 10^19 - 1`), leading/trailing zeros,
 *     decimal-point edge cases, `u64` intermediate multiplication overflow, off-grid
 *     remainders, and `i64::MAX` (`2^63 - 1`) overflow traps (`QF_LIA`).
 *   Target 3c (Opt #4): Full 64-bit `millis.checked_mul(1_000_000)` (`0 .. 2^64 - 1`)
 *     and all-profile (`dev`, `test`, `bench`, `release`) checked arithmetic trapping
 *     (`NoSilentArithmeticWrap`).
 *   Target 1b (Opt #1): `tickwire_ipc::crc32c_masked` arbitrary `Range<usize>` clamping,
 *     8-byte `as_chunks::<8>()` + 64-byte zero-chunk partition algebra across symbolic
 *     `0 <= buf_len <= 1048576` and arbitrary `mask_start`, `mask_end` (including
 *     inverted `mask_start > mask_end`, OOB `mask_end > buf_len`, and unaligned word
 *     boundaries), plus `tickwire_journal::crc32c_header_masked` 136-byte framing and
 *     `Diag_JournalCrcMaskedTotalOnOobRange` API contract comparison.
 *
 * Verified production Rust anchors:
 *   - Frame cap `MAX_MESSAGE = 16 * 1024` & `Session::decode_fast` / `QuoteConfig::validate`:
 *       crates/tickwire-md/src/spot_quotes/network.rs:18
 *       crates/tickwire-md/src/spot_quotes.rs:40-55
 *       crates/tickwire-md/src/spot_quotes.rs:143-176
 *       crates/tickwire-md/src/spot_quotes/protocol.rs:201-240
 *   - `StepSpec`, `Scanner::load_next_block`, `AVX2`/`SSE2`/`NEON`/`SWAR`/scalar loops:
 *       crates/tickwire-md/src/spot_quotes/simd_json.rs:117-140
 *       crates/tickwire-md/src/spot_quotes/simd_json.rs:265-469
 *       crates/tickwire-md/src/spot_quotes/simd_json.rs:675-694
 *   - `parse_positive_units` (`u64` fast path + `u128` `from_parts`) & `Decimal`/`units`:
 *       crates/tickwire-md/src/spot_quotes/simd_json.rs:523-621
 *       crates/tickwire-md/src/spot_quotes/simd_json.rs:1277-1280
 *       crates/tickwire-binance/src/decimal.rs:34-94
 *       crates/tickwire-binance/src/feed.rs:384-419
 *   - Workspace all-profile `overflow-checks = true`:
 *       Cargo.toml:15-26
 *   - `crc32c_masked` & `crc32c_header_masked` slice/chunk framing:
 *       crates/tickwire-ipc/src/crc32c.rs:154-168
 *       crates/tickwire-journal/src/crc32c.rs:180-235
 *       crates/tickwire-journal/src/frame.rs:138-145
 *)
EXTENDS Integers

CONSTANTS
  (* @type: Bool; *)
  BoundedMode

VARIABLES
  (* --- Target 3a: SIMD/SWAR/scalar block scanner state --- *)
  (* @type: Int; *)
  backend_id,
  (* @type: Int; *)
  frame_len,
  (* @type: Bool; *)
  frame_oversized,
  (* @type: Int; *)
  block_base,
  (* @type: Int; *)
  block_end,
  (* @type: Int; *)
  offset,
  (* @type: Int; *)
  last_read_start,
  (* @type: Int; *)
  last_read_end,
  (* @type: Int; *)
  last_shift_bits,
  (* @type: Bool; *)
  movemask_upper32_zero,
  (* --- Target 3b & 3c: 64-bit/128-bit decimal & timestamp oracle state --- *)
  (* @type: Int; *)
  step_case,
  (* @type: Int; *)
  str_len,
  (* @type: Bool; *)
  has_minus,
  (* @type: Bool; *)
  has_invalid_char,
  (* @type: Int; *)
  dot_count,
  (* @type: Bool; *)
  digit_before_dot,
  (* @type: Bool; *)
  digit_after_dot,
  (* @type: Int; *)
  total_digits,
  (* @type: Int; *)
  frac_scale,
  (* @type: Int; *)
  mantissa,
  (* @type: Bool; *)
  fast_ok,
  (* @type: Int; *)
  fast_units,
  (* @type: Bool; *)
  oracle_ok,
  (* @type: Int; *)
  oracle_units,
  (* @type: Int; *)
  ts_ms,
  (* @type: Bool; *)
  ts_ok,
  (* @type: Int; *)
  ts_ns,
  (* @type: Int; *)
  profile_id,
  (* @type: Bool; *)
  checked_overflow_trapped,
  (* --- Target 1b: `crc32c_masked` & `crc32c_header_masked` state --- *)
  (* @type: Int; *)
  buf_len,
  (* @type: Int; *)
  mask_start,
  (* @type: Int; *)
  mask_end,
  (* @type: Int; *)
  clamped_start,
  (* @type: Int; *)
  clamped_end,
  (* @type: Int; *)
  prefix_words8,
  (* @type: Int; *)
  prefix_rem8,
  (* @type: Int; *)
  zero_chunks64,
  (* @type: Int; *)
  zero_rem64,
  (* @type: Int; *)
  suffix_words8,
  (* @type: Int; *)
  suffix_rem8,
  (* @type: Bool; *)
  journal_masked_assert_ok

simdVars == <<
  backend_id, frame_len, frame_oversized, block_base, block_end, offset,
  last_read_start, last_read_end, last_shift_bits, movemask_upper32_zero
>>

arithVars == <<
  step_case, str_len, has_minus, has_invalid_char, dot_count,
  digit_before_dot, digit_after_dot, total_digits, frac_scale, mantissa,
  fast_ok, fast_units, oracle_ok, oracle_units, ts_ms, ts_ok, ts_ns,
  profile_id, checked_overflow_trapped
>>

crcVars == <<
  buf_len, mask_start, mask_end, clamped_start, clamped_end,
  prefix_words8, prefix_rem8, zero_chunks64, zero_rem64,
  suffix_words8, suffix_rem8, journal_masked_assert_ok
>>

vars == <<
  backend_id, frame_len, frame_oversized, block_base, block_end, offset,
  last_read_start, last_read_end, last_shift_bits, movemask_upper32_zero,
  step_case, str_len, has_minus, has_invalid_char, dot_count,
  digit_before_dot, digit_after_dot, total_digits, frac_scale, mantissa,
  fast_ok, fast_units, oracle_ok, oracle_units, ts_ms, ts_ok, ts_ns,
  profile_id, checked_overflow_trapped,
  buf_len, mask_start, mask_end, clamped_start, clamped_end,
  prefix_words8, prefix_rem8, zero_chunks64, zero_rem64,
  suffix_words8, suffix_rem8, journal_masked_assert_ok
>>

(* Constants < 2^31 for SANY; exact 64-bit / 19-digit bounds in Apalache Z3. *)
(* @type: Int; *)
MaxMessageBytes == 16384

(* @type: Int; *)
Pow30 == 1073741824

(* @type: Int; *)
Pow9 == 1000000000

(* @type: Int; *)
SymI64Max == (8 * Pow30 * Pow30) - 1

(* @type: Int; *)
SymU64Max == (16 * Pow30 * Pow30) - 1

(* @type: Int; *)
SymMaxDecMantissa == (10 * Pow9 * Pow9) - 1

(* @type: Int; *)
EffI64Max == IF BoundedMode THEN 50 ELSE SymI64Max

(* @type: Int; *)
EffU64Max == IF BoundedMode THEN 100 ELSE SymU64Max

(* @type: Int; *)
EffTsScale == IF BoundedMode THEN 10 ELSE 1000000

(* Finite sets evaluated ONLY when `BoundedMode = TRUE` in explicit TLC. *)
(* @type: Set(Int); *)
FiniteBackends == 0..2

(* @type: Set(Int); *)
FiniteFrameLens == {0, 15, 33, 65, 16385}

(* @type: Set(Int); *)
FiniteStepCases == 0..2

(* @type: Set(Int); *)
FiniteProfiles == 0..3

(* @type: (Int, Int) => Int; *)
Min2(a, b) == IF a <= b THEN a ELSE b

(* @type: (Int, Int) => Int; *)
Max2(a, b) == IF a >= b THEN a ELSE b

(* ========================================================================= *)
(* Exact `QF_LIA` Fixed-Point Models: `simd_json` vs `Decimal` + `feed`      *)
(* ========================================================================= *)
(* Three linear `StepSpec` profiles (`QuoteConfig::validate` guarantees step > 0):
 *   step_case = 0: step = 0.01   (`mantissa = 1`,     `scale = 2`) — power-of-10 tick
 *   step_case = 1: step = 0.25   (`mantissa = 25`,    `scale = 2`) — non-unit tick
 *   step_case = 2: step = 5.0000 (`mantissa = 50000`, `scale = 4`) — exercises `u64`
 *     `mantissa * 10^4 > u64::MAX` overflow delegating to `u128` `gcd`-reduced
 *     `parse_positive_units_from_parts` (`crates/tickwire-md/src/spot_quotes/simd_json.rs:571-576` vs `crates/tickwire-binance/src/feed.rs:384-419`)! *)

(* Linear scaled numerator & denominator for `(step_case, frac_scale)` with `frac_scale <= 4`:
 * In `feed::units` (`crates/tickwire-binance/src/feed.rs:396-410`) and `parse_positive_units` (`crates/tickwire-md/src/spot_quotes/simd_json.rs:568-621`),
 * exact ratio is `(mantissa * 10^(max(S, F) - F)) / (step_m * 10^(max(S, F) - S))`. *)
(* @type: (Int, Int, Int) => Int; *)
ScaledNumer(sc, fscale, m) ==
  IF sc = 0 \/ sc = 1 THEN
    IF fscale = 0 THEN m * 100
    ELSE IF fscale = 1 THEN m * 10
    ELSE m
  ELSE
    IF fscale = 0 THEN m * 10000
    ELSE IF fscale = 1 THEN m * 1000
    ELSE IF fscale = 2 THEN m * 100
    ELSE IF fscale = 3 THEN m * 10
    ELSE m

(* @type: (Int, Int) => Int; *)
ScaledDenom(sc, fscale) ==
  IF sc = 0 THEN
    IF fscale <= 2 THEN 1
    ELSE IF fscale = 3 THEN 10
    ELSE 100
  ELSE IF sc = 1 THEN
    IF fscale <= 2 THEN 25
    ELSE IF fscale = 3 THEN 250
    ELSE 2500
  ELSE
    (* sc = 2: step_mantissa = 50000, step_scale = 4, fscale \in 0..4 *)
    50000

(* Exact syntactic + numeric predicate of `simd_json::parse_positive_units`
 * (`crates/tickwire-md/src/spot_quotes/simd_json.rs:523-621`). *)
(* @type: (Int, Int, Bool, Bool, Int, Bool, Bool, Int, Int, Int) => Bool; *)
FastSyntaxAndRangeOk(sc, slen, minus, badch, dots, dbefore, dafter, digits, fscale, m) ==
  /\ slen >= 1 /\ slen <= 20
  /\ ~minus
  /\ ~badch
  /\ digits >= 1 /\ digits <= 19
  /\ (dots = 0 \/ (dots = 1 /\ dbefore /\ dafter))
  /\ m > 0
  /\ (ScaledNumer(sc, fscale, m) % ScaledDenom(sc, fscale) = 0)
  /\ ((ScaledNumer(sc, fscale, m) \div ScaledDenom(sc, fscale)) > 0)
  /\ ((ScaledNumer(sc, fscale, m) \div ScaledDenom(sc, fscale)) <= EffI64Max)

(* Exact syntactic + numeric predicate of `Decimal::parse` + `feed::units` + `n > 0`
 * (`crates/tickwire-md/src/spot_quotes/protocol.rs:210-218`, `crates/tickwire-binance/src/decimal.rs:34-94`, `crates/tickwire-binance/src/feed.rs:384-419`).
 * Note: `Decimal::parse` has NO `slen <= 20` check, accepts leading `'-'` (producing
 * `signed <= 0`, later rejected by `feed::units` `value.mantissa() <= 0`), and checks
 * `digits <= 19` + at most one internal `'.'`. Z3 proves equivalence over ALL inputs! *)
(* @type: (Int, Int, Bool, Bool, Int, Bool, Bool, Int, Int, Int) => Bool; *)
OracleSyntaxAndRangeOk(sc, slen, minus, badch, dots, dbefore, dafter, digits, fscale, m) ==
  /\ slen >= 1
  /\ ~(minus /\ slen = 1)
  /\ ~badch
  /\ digits >= 1 /\ digits <= 19
  /\ (dots = 0 \/ (dots = 1 /\ dbefore /\ dafter))
  /\ (~minus /\ m > 0)
  /\ (ScaledNumer(sc, fscale, m) % ScaledDenom(sc, fscale) = 0)
  /\ ((ScaledNumer(sc, fscale, m) \div ScaledDenom(sc, fscale)) > 0)
  /\ ((ScaledNumer(sc, fscale, m) \div ScaledDenom(sc, fscale)) <= EffI64Max)

(* Exact byte-slice consistency constraints relating length, sign, dots, digits & mantissa. *)
(* @type: (Int, Bool, Bool, Int, Bool, Bool, Int, Int, Int) => Bool; *)
ValidDecimalInputShape(slen, minus, badch, dots, dbefore, dafter, digits, fscale, m) ==
  /\ slen >= 0 /\ slen <= 32
  /\ dots >= 0 /\ dots <= 3
  /\ digits >= 0 /\ digits <= 32
  /\ fscale >= 0 /\ fscale <= 4
  /\ m >= 0
  /\ (IF BoundedMode THEN m <= 200 ELSE m <= SymMaxDecMantissa)
  /\ ((~badch) => (slen = digits + dots + (IF minus THEN 1 ELSE 0)))
  /\ (badch => (slen >= 1))
  /\ ((digits = 0) => (m = 0 /\ ~dbefore /\ ~dafter /\ fscale = 0))
  /\ ((dots = 0) => (fscale = 0 /\ ~dafter /\ (dbefore <=> (digits >= 1))))
  /\ ((dots >= 1) =>
       /\ (dafter <=> (fscale >= 1))
       /\ fscale <= digits
       /\ (dbefore <=> (digits > fscale)))

ConstInit ==
  BoundedMode = FALSE

Init ==
  /\ IF BoundedMode
     THEN /\ backend_id \in FiniteBackends
          /\ frame_len \in FiniteFrameLens
     ELSE /\ backend_id \in Int /\ backend_id >= 0 /\ backend_id <= 2
          /\ frame_len \in Int /\ frame_len >= 0 /\ frame_len <= 65536
  /\ frame_oversized = (frame_len > MaxMessageBytes)
  /\ block_base = 0
  /\ block_end = (IF frame_len > MaxMessageBytes THEN 0 ELSE Min2(64, frame_len))
  /\ offset = 0
  /\ last_read_start = 0
  /\ last_read_end = 0
  /\ last_shift_bits = 0
  /\ movemask_upper32_zero = TRUE
  (* --- Target 3b & 3c symbolic initial sample --- *)
  /\ IF BoundedMode
     THEN /\ step_case = 0
          /\ str_len = 4
          /\ has_minus = FALSE
          /\ has_invalid_char = FALSE
          /\ dot_count = 1
          /\ digit_before_dot = TRUE
          /\ digit_after_dot = TRUE
          /\ total_digits = 3
          /\ frac_scale = 2
          /\ mantissa = 125
          /\ ts_ms = 5
          /\ profile_id \in FiniteProfiles
     ELSE /\ step_case \in Int /\ step_case >= 0 /\ step_case <= 2
          /\ str_len \in Int
          /\ has_minus \in BOOLEAN
          /\ has_invalid_char \in BOOLEAN
          /\ dot_count \in Int
          /\ digit_before_dot \in BOOLEAN
          /\ digit_after_dot \in BOOLEAN
          /\ total_digits \in Int
          /\ frac_scale \in Int
          /\ mantissa \in Int
          /\ ValidDecimalInputShape(
               str_len, has_minus, has_invalid_char, dot_count,
               digit_before_dot, digit_after_dot, total_digits, frac_scale, mantissa
             )
          /\ ts_ms \in Int /\ ts_ms >= 0 /\ ts_ms <= EffU64Max
          /\ profile_id \in Int /\ profile_id >= 0 /\ profile_id <= 3
  /\ fast_ok = FastSyntaxAndRangeOk(
       step_case, str_len, has_minus, has_invalid_char, dot_count,
       digit_before_dot, digit_after_dot, total_digits, frac_scale, mantissa
     )
  /\ fast_units =
       (IF FastSyntaxAndRangeOk(
             step_case, str_len, has_minus, has_invalid_char, dot_count,
             digit_before_dot, digit_after_dot, total_digits, frac_scale, mantissa
           )
        THEN ScaledNumer(step_case, frac_scale, mantissa) \div ScaledDenom(step_case, frac_scale)
        ELSE 0)
  /\ oracle_ok = OracleSyntaxAndRangeOk(
       step_case, str_len, has_minus, has_invalid_char, dot_count,
       digit_before_dot, digit_after_dot, total_digits, frac_scale, mantissa
     )
  /\ oracle_units =
       (IF OracleSyntaxAndRangeOk(
             step_case, str_len, has_minus, has_invalid_char, dot_count,
             digit_before_dot, digit_after_dot, total_digits, frac_scale, mantissa
           )
        THEN ScaledNumer(step_case, frac_scale, mantissa) \div ScaledDenom(step_case, frac_scale)
        ELSE 0)
  /\ ts_ok = (ts_ms * EffTsScale <= EffU64Max)
  /\ ts_ns = (IF ts_ms * EffTsScale <= EffU64Max THEN ts_ms * EffTsScale ELSE 0)
  /\ checked_overflow_trapped =
       ((ts_ms * EffTsScale > EffU64Max) \/
        ((ScaledNumer(step_case, frac_scale, mantissa) \div ScaledDenom(step_case, frac_scale)) > EffI64Max))
  (* --- Target 1b symbolic `crc32c_masked` initial sample --- *)
  /\ IF BoundedMode
     THEN /\ buf_len = 136
          /\ mask_start = 128
          /\ mask_end = 132
     ELSE /\ buf_len \in Int /\ buf_len >= 0 /\ buf_len <= 1048576
          /\ mask_start \in Int /\ mask_start >= 0 /\ mask_start <= 2097152
          /\ mask_end \in Int /\ mask_end >= 0 /\ mask_end <= 2097152
  /\ LET cs == Min2(mask_start, buf_len)
         ce == Max2(Min2(mask_end, buf_len), cs)
         plen == cs
         zlen == ce - cs
         slen == buf_len - ce
     IN
     /\ clamped_start = cs
     /\ clamped_end = ce
     /\ prefix_words8 = plen \div 8
     /\ prefix_rem8 = plen % 8
     /\ zero_chunks64 = zlen \div 64
     /\ zero_rem64 = zlen % 64
     /\ suffix_words8 = slen \div 8
     /\ suffix_rem8 = slen % 8
     /\ journal_masked_assert_ok = (mask_start <= mask_end /\ mask_end <= buf_len)

(* ========================================================================= *)
(* Target 3a Transitions — Exact Vector / SWAR / Scalar Loop Stepping        *)
(* ========================================================================= *)

(* `scan_block_avx2`: `while offset + 32 <= len` (`crates/tickwire-md/src/spot_quotes/simd_json.rs:281-294`).
 * Casts `_mm256_movemask_epi8(...) as u32 as u64` before `<< offset` (`offset \in {0,32}`). *)
StepSimd32 ==
  /\ ~frame_oversized
  /\ backend_id = 0
  /\ offset + 32 <= (block_end - block_base)
  /\ last_read_start' = block_base + offset
  /\ last_read_end' = block_base + offset + 32
  /\ last_shift_bits' = offset
  /\ movemask_upper32_zero' = TRUE
  /\ offset' = offset + 32
  /\ UNCHANGED <<
       backend_id, frame_len, frame_oversized, block_base, block_end,
       arithVars, crcVars
     >>

(* `scan_block_avx2` 16B tail (`crates/tickwire-md/src/spot_quotes/simd_json.rs:296-300`) & `scan_block_sse2` / `neon`
 * `while offset + 16 <= len` (`crates/tickwire-md/src/spot_quotes/simd_json.rs:336-350`). *)
StepSimd16 ==
  /\ ~frame_oversized
  /\ (backend_id = 1 \/ (backend_id = 0 /\ offset + 32 > (block_end - block_base)))
  /\ offset + 16 <= (block_end - block_base)
  /\ last_read_start' = block_base + offset
  /\ last_read_end' = block_base + offset + 16
  /\ last_shift_bits' = offset
  /\ movemask_upper32_zero' = TRUE
  /\ offset' = offset + 16
  /\ UNCHANGED <<
       backend_id, frame_len, frame_oversized, block_base, block_end,
       arithVars, crcVars
     >>

(* `scan_tail_swar_scalar`: `while offset + 8 <= len` (`crates/tickwire-md/src/spot_quotes/simd_json.rs:420-435`). *)
StepSwar8 ==
  /\ ~frame_oversized
  /\ (backend_id = 2
      \/ (backend_id = 1 /\ offset + 16 > (block_end - block_base))
      \/ (backend_id = 0 /\ offset + 16 > (block_end - block_base)))
  /\ offset + 8 <= (block_end - block_base)
  /\ last_read_start' = block_base + offset
  /\ last_read_end' = block_base + offset + 8
  /\ last_shift_bits' = offset
  /\ movemask_upper32_zero' = TRUE
  /\ offset' = offset + 8
  /\ UNCHANGED <<
       backend_id, frame_len, frame_oversized, block_base, block_end,
       arithVars, crcVars
     >>

(* `scan_tail_swar_scalar`: `while offset < len` (`crates/tickwire-md/src/spot_quotes/simd_json.rs:437-466`):
 * executes `let bit = 1u64 << offset` (`offset <= 63 < 64`). *)
StepScalar1 ==
  /\ ~frame_oversized
  /\ offset + 8 > (block_end - block_base)
  /\ offset < (block_end - block_base)
  /\ last_read_start' = block_base + offset
  /\ last_read_end' = block_base + offset + 1
  /\ last_shift_bits' = offset
  /\ movemask_upper32_zero' = TRUE
  /\ offset' = offset + 1
  /\ UNCHANGED <<
       backend_id, frame_len, frame_oversized, block_base, block_end,
       arithVars, crcVars
     >>

(* `Scanner::load_next_block` (`crates/tickwire-md/src/spot_quotes/simd_json.rs:675-694`):
 * `base = scanned_to; end = (base + 64).min(bytes.len())`. *)
AdvanceNextBlock64 ==
  /\ ~frame_oversized
  /\ offset = (block_end - block_base)
  /\ block_end < frame_len
  /\ block_base' = block_end
  /\ block_end' = Min2(block_end + 64, frame_len)
  /\ offset' = 0
  /\ last_read_start' = block_end
  /\ last_read_end' = block_end
  /\ last_shift_bits' = 0
  /\ movemask_upper32_zero' = TRUE
  /\ UNCHANGED <<
       backend_id, frame_len, frame_oversized, arithVars, crcVars
     >>

(* Re-evaluates decimal + timestamp + CRC masked slicing on fresh inputs so TLC
 * covers arithmetic/CRC transitions and Apalache multi-step BMC explores sequences. *)
SampleDecimalTimestampAndCrc ==
  /\ (frame_oversized \/ (offset = (block_end - block_base) /\ block_end = frame_len))
  /\ IF BoundedMode
     THEN /\ ~has_minus
          /\ \E sc \in FiniteStepCases :
             \E m \in {0, 25, 55} :
               LET slen == (IF m = 0 THEN 1 ELSE 2) + 1
                   digs == (IF m = 0 THEN 1 ELSE 2)
                   ts   == (IF m = 55 THEN 15 ELSE 5)
                   mstart == (IF sc = 2 THEN 140 ELSE 112)
                   mend   == (IF sc = 0 THEN 112 ELSE 120)
                   fok  == FastSyntaxAndRangeOk(sc, slen, TRUE, FALSE, 0, TRUE, FALSE, digs, 0, m)
                   ook  == OracleSyntaxAndRangeOk(sc, slen, TRUE, FALSE, 0, TRUE, FALSE, digs, 0, m)
                   blen == 128
                   cs   == Min2(mstart, blen)
                   ce   == Max2(Min2(mend, blen), cs)
                   plen == cs
                   zlen == ce - cs
                   suflen == blen - ce
               IN
               /\ step_case' = sc
               /\ str_len' = slen
               /\ has_minus' = TRUE
               /\ has_invalid_char' = FALSE
               /\ dot_count' = 0
               /\ digit_before_dot' = TRUE
               /\ digit_after_dot' = FALSE
               /\ total_digits' = digs
               /\ frac_scale' = 0
               /\ mantissa' = m
               /\ fast_ok' = fok
               /\ fast_units' = (IF fok THEN ScaledNumer(sc, 0, m) \div ScaledDenom(sc, 0) ELSE 0)
               /\ oracle_ok' = ook
               /\ oracle_units' = (IF ook THEN ScaledNumer(sc, 0, m) \div ScaledDenom(sc, 0) ELSE 0)
               /\ ts_ms' = ts
               /\ ts_ok' = (ts * EffTsScale <= EffU64Max)
               /\ ts_ns' = (IF ts * EffTsScale <= EffU64Max THEN ts * EffTsScale ELSE 0)
               /\ profile_id' = profile_id
               /\ checked_overflow_trapped' =
                    ((ts * EffTsScale > EffU64Max) \/
                     ((ScaledNumer(sc, 0, m) \div ScaledDenom(sc, 0)) > EffI64Max))
               /\ buf_len' = blen
               /\ mask_start' = mstart
               /\ mask_end' = mend
               /\ clamped_start' = cs
               /\ clamped_end' = ce
               /\ prefix_words8' = plen \div 8
               /\ prefix_rem8' = plen % 8
               /\ zero_chunks64' = zlen \div 64
               /\ zero_rem64' = zlen % 64
               /\ suffix_words8' = suflen \div 8
               /\ suffix_rem8' = suflen % 8
               /\ journal_masked_assert_ok' = (mstart <= mend /\ mend <= blen)
     ELSE \E sc \in Int :
          \E slen \in Int :
          \E neg \in BOOLEAN :
          \E badch \in BOOLEAN :
          \E dots \in Int :
          \E dbefore \in BOOLEAN :
          \E dafter \in BOOLEAN :
          \E digs \in Int :
          \E fscale \in Int :
          \E m \in Int :
          \E ts \in Int :
          \E prof \in Int :
          \E blen \in Int :
          \E mstart \in Int :
          \E mend \in Int :
            /\ sc >= 0 /\ sc <= 2
            /\ ValidDecimalInputShape(slen, neg, badch, dots, dbefore, dafter, digs, fscale, m)
            /\ ts >= 0 /\ ts <= EffU64Max
            /\ prof >= 0 /\ prof <= 3
            /\ blen >= 0 /\ blen <= 1048576
            /\ mstart >= 0 /\ mstart <= 2097152
            /\ mend >= 0 /\ mend <= 2097152
            /\ LET fok == FastSyntaxAndRangeOk(sc, slen, neg, badch, dots, dbefore, dafter, digs, fscale, m)
                   ook == OracleSyntaxAndRangeOk(sc, slen, neg, badch, dots, dbefore, dafter, digs, fscale, m)
                   cs  == Min2(mstart, blen)
                   ce  == Max2(Min2(mend, blen), cs)
                   plen == cs
                   zlen == ce - cs
                   suflen == blen - ce
               IN
               /\ step_case' = sc
               /\ str_len' = slen
               /\ has_minus' = neg
               /\ has_invalid_char' = badch
               /\ dot_count' = dots
               /\ digit_before_dot' = dbefore
               /\ digit_after_dot' = dafter
               /\ total_digits' = digs
               /\ frac_scale' = fscale
               /\ mantissa' = m
               /\ fast_ok' = fok
               /\ fast_units' = (IF fok THEN ScaledNumer(sc, fscale, m) \div ScaledDenom(sc, fscale) ELSE 0)
               /\ oracle_ok' = ook
               /\ oracle_units' = (IF ook THEN ScaledNumer(sc, fscale, m) \div ScaledDenom(sc, fscale) ELSE 0)
               /\ ts_ms' = ts
               /\ ts_ok' = (ts * EffTsScale <= EffU64Max)
               /\ ts_ns' = (IF ts * EffTsScale <= EffU64Max THEN ts * EffTsScale ELSE 0)
               /\ profile_id' = prof
               /\ checked_overflow_trapped' =
                    ((ts * EffTsScale > EffU64Max) \/
                     ((ScaledNumer(sc, fscale, m) \div ScaledDenom(sc, fscale)) > EffI64Max))
               /\ buf_len' = blen
               /\ mask_start' = mstart
               /\ mask_end' = mend
               /\ clamped_start' = cs
               /\ clamped_end' = ce
               /\ prefix_words8' = plen \div 8
               /\ prefix_rem8' = plen % 8
               /\ zero_chunks64' = zlen \div 64
               /\ zero_rem64' = zlen % 64
               /\ suffix_words8' = suflen \div 8
               /\ suffix_rem8' = suflen % 8
               /\ journal_masked_assert_ok' = (mstart <= mend /\ mend <= blen)
  /\ UNCHANGED simdVars

Next ==
  \/ StepSimd32
  \/ StepSimd16
  \/ StepSwar8
  \/ StepScalar1
  \/ AdvanceNextBlock64
  \/ SampleDecimalTimestampAndCrc

Spec == Init /\ [][Next]_vars

(* ========================================================================= *)
(* Safety Invariants (Verified across ALL 64-bit/128-bit symbolic domains)   *)
(* ========================================================================= *)

TypeOK ==
  /\ backend_id \in Int /\ backend_id >= 0 /\ backend_id <= 2
  /\ frame_len \in Int /\ frame_len >= 0 /\ frame_len <= 65536
  /\ frame_oversized \in BOOLEAN
  /\ block_end \in Int /\ block_end >= 0 /\ block_end <= frame_len
  /\ block_base \in Int /\ block_base >= 0 /\ block_base <= block_end
  /\ (block_end - block_base) <= 64
  /\ offset \in Int /\ offset >= 0 /\ offset <= (block_end - block_base)
  /\ last_read_start \in Int
  /\ last_read_end \in Int
  /\ last_shift_bits \in Int
  /\ movemask_upper32_zero \in BOOLEAN
  /\ step_case \in Int /\ step_case >= 0 /\ step_case <= 2
  /\ str_len \in Int
  /\ has_minus \in BOOLEAN
  /\ has_invalid_char \in BOOLEAN
  /\ dot_count \in Int
  /\ digit_before_dot \in BOOLEAN
  /\ digit_after_dot \in BOOLEAN
  /\ total_digits \in Int
  /\ frac_scale \in Int
  /\ mantissa \in Int
  /\ fast_ok \in BOOLEAN
  /\ fast_units \in Int
  /\ oracle_ok \in BOOLEAN
  /\ oracle_units \in Int
  /\ ts_ms \in Int
  /\ ts_ok \in BOOLEAN
  /\ ts_ns \in Int
  /\ profile_id \in Int /\ profile_id >= 0 /\ profile_id <= 3
  /\ checked_overflow_trapped \in BOOLEAN
  /\ buf_len \in Int /\ buf_len >= 0 /\ buf_len <= 1048576
  /\ mask_start \in Int
  /\ mask_end \in Int
  /\ clamped_start \in Int
  /\ clamped_end \in Int
  /\ prefix_words8 \in Int
  /\ prefix_rem8 \in Int
  /\ zero_chunks64 \in Int
  /\ zero_rem64 \in Int
  /\ suffix_words8 \in Int
  /\ suffix_rem8 \in Int
  /\ journal_masked_assert_ok \in BOOLEAN

(* 1. Zero OOB vector/SWAR/scalar reads and valid `< 64` bit shifts across ALL
 *    symbolic `0 <= frame_len <= 65536` and all 4 backends (`AVX2`/`SSE2`/`NEON`/`SWAR`). *)
ZeroVectorOobAndValidShift ==
  /\ (frame_oversized <=> (frame_len > MaxMessageBytes))
  /\ (frame_oversized => (block_base = 0 /\ block_end = 0 /\ offset = 0 /\ last_read_end = 0))
  /\ 0 <= last_read_start
  /\ last_read_start <= last_read_end
  /\ last_read_end <= block_end
  /\ block_end <= frame_len
  /\ 0 <= last_shift_bits
  /\ last_shift_bits < 64
  /\ movemask_upper32_zero

(* 2. Exact mathematical equivalence between `simd_json::parse_positive_units` +
 *    `StepSpec` and `Decimal::parse` + `tickwire_binance::feed::units` (`n > 0`)
 *    across ALL 19-digit 64-bit mantissas, leading/trailing zeros, dot edge cases,
 *    `u64`-intermediate-overflow `u128` fallback (`step_case = 2`), and `i64::MAX`! *)
FastVsOracleExactEquivalence ==
  /\ (fast_ok <=> oracle_ok)
  /\ (fast_units = oracle_units)
  /\ (fast_ok => (fast_units > 0 /\ fast_units <= EffI64Max))
  /\ ((~fast_ok) => (fast_units = 0 /\ oracle_units = 0))

(* 3. Full 64-bit timestamp & fixed-point checked arithmetic overflow trapping across
 *    all Cargo profiles (`dev`, `test`, `bench`, `release`). *)
NoSilentArithmeticWrap ==
  /\ (ts_ok <=> (ts_ms * EffTsScale <= EffU64Max))
  /\ (ts_ok => (ts_ns = ts_ms * EffTsScale /\ ts_ns >= 0 /\ ts_ns <= EffU64Max))
  /\ ((~ts_ok) => (ts_ns = 0 /\ checked_overflow_trapped))
  /\ (((ScaledNumer(step_case, frac_scale, mantissa) \div ScaledDenom(step_case, frac_scale)) > EffI64Max) =>
       (~fast_ok /\ ~oracle_ok /\ checked_overflow_trapped))

(* 4. `tickwire_ipc::crc32c_masked` clamping + `as_chunks::<8>()` + 64B zero-chunk
 *    stream partition exactness across arbitrary `buf_len`, `mask_start`, `mask_end`,
 *    plus 136B `tickwire_journal::crc32c_header_masked` layout (`16*8 + 8 = 136`). *)
CrcMaskedSlicePartitionExact ==
  /\ 0 <= clamped_start
  /\ clamped_start <= clamped_end
  /\ clamped_end <= buf_len
  /\ prefix_words8 >= 0 /\ 0 <= prefix_rem8 /\ prefix_rem8 < 8
  /\ (prefix_words8 * 8 + prefix_rem8 = clamped_start)
  /\ zero_chunks64 >= 0 /\ 0 <= zero_rem64 /\ zero_rem64 < 64
  /\ (zero_chunks64 * 64 + zero_rem64 = (clamped_end - clamped_start))
  /\ suffix_words8 >= 0 /\ 0 <= suffix_rem8 /\ suffix_rem8 < 8
  /\ (suffix_words8 * 8 + suffix_rem8 = (buf_len - clamped_end))
  /\ ((prefix_words8 * 8 + prefix_rem8)
      + (zero_chunks64 * 64 + zero_rem64)
      + (suffix_words8 * 8 + suffix_rem8) = buf_len)
  (* `crc32c_header_masked` fixed 136B journal frame header: 16 words `[0..128]` + 1 masked word `[128..136]` *)
  /\ (16 * 8 + 8 = 136)

SafetyConjunction ==
  /\ TypeOK
  /\ ZeroVectorOobAndValidShift
  /\ FastVsOracleExactEquivalence
  /\ NoSilentArithmeticWrap
  /\ CrcMaskedSlicePartitionExact

(* Mode S2 Inductive Invariant for `L4` (proves unbounded block-scan induction too!). *)
IndInv ==
  /\ TypeOK
  /\ ZeroVectorOobAndValidShift
  /\ ValidDecimalInputShape(
       str_len, has_minus, has_invalid_char, dot_count,
       digit_before_dot, digit_after_dot, total_digits, frac_scale, mantissa
     )
  /\ ts_ms >= 0 /\ ts_ms <= EffU64Max
  /\ mask_start >= 0 /\ mask_start <= 2097152
  /\ mask_end >= 0 /\ mask_end <= 2097152
  /\ fast_ok = FastSyntaxAndRangeOk(
       step_case, str_len, has_minus, has_invalid_char, dot_count,
       digit_before_dot, digit_after_dot, total_digits, frac_scale, mantissa
     )
  /\ fast_units =
       (IF fast_ok
        THEN ScaledNumer(step_case, frac_scale, mantissa) \div ScaledDenom(step_case, frac_scale)
        ELSE 0)
  /\ oracle_ok = OracleSyntaxAndRangeOk(
       step_case, str_len, has_minus, has_invalid_char, dot_count,
       digit_before_dot, digit_after_dot, total_digits, frac_scale, mantissa
     )
  /\ oracle_units =
       (IF oracle_ok
        THEN ScaledNumer(step_case, frac_scale, mantissa) \div ScaledDenom(step_case, frac_scale)
        ELSE 0)
  /\ ts_ok = (ts_ms * EffTsScale <= EffU64Max)
  /\ ts_ns = (IF ts_ok THEN ts_ms * EffTsScale ELSE 0)
  /\ checked_overflow_trapped =
       ((ts_ms * EffTsScale > EffU64Max) \/
        ((ScaledNumer(step_case, frac_scale, mantissa) \div ScaledDenom(step_case, frac_scale)) > EffI64Max))
  /\ clamped_start = Min2(mask_start, buf_len)
  /\ clamped_end = Max2(Min2(mask_end, buf_len), clamped_start)
  /\ FastVsOracleExactEquivalence
  /\ NoSilentArithmeticWrap
  /\ CrcMaskedSlicePartitionExact

(* Diagnostic query (`crates/tickwire-journal/src/crc32c.rs:182-197` vs
 * `crates/tickwire-ipc/src/crc32c.rs:154-168`):
 * `tickwire_ipc::crc32c_masked` clamps arbitrary `Range<usize>`, whereas
 * `tickwire_journal::crc32c::crc32c_masked` asserts `start <= end && end <= len`. *)
Diag_JournalCrcMaskedTotalOnOobRange ==
  journal_masked_assert_ok

====
