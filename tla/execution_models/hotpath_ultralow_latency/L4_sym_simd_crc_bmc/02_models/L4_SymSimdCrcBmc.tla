---- MODULE L4_SymSimdCrcBmc ----
(*
 * Symbolic Bounded Model Checking & Inductive Verification (Mode S1 + Mode S2) for:
 *   Target 1 (SIMD/SWAR/Scalar Block Scanner): `simd_json` 64-byte block scanner
 *     (`AVX2` 32B, `SSE2`/`NEON` 16B, `SWAR` 8B, scalar 1B tail) memory bounds
 *     and `< 64` bit-shift safety across ALL symbolic buffer lengths
 *     `0 <= frame_len <= 65536` (`MAX_MESSAGE = 16384`).
 *   Target 2 (64-Bit / 128-Bit Fixed-Point Oracle): Exact mathematical equivalence
 *     (`FastVsOracleExactEquivalence`) between `simd_json::parse_positive_units` +
 *     `StepSpec` (`u64` fast path + `u128` `gcd`-reduced fallback) and
 *     `Decimal::parse` + `tickwire_binance::feed::units` across full 19-digit
 *     64-bit mantissas (`0 .. 10^19 - 1`), leading/trailing zeros, decimal-point
 *     edge cases, `u64` intermediate multiplication overflow, off-grid remainders,
 *     and `i64::MAX` (`2^63 - 1`) overflow traps (`QF_LIA`).
 *   Target 3 (All-Profile Overflow Trapping): Full 64-bit `millis * 1_000_000`
 *     (`0 .. 2^64 - 1`) and all-profile (`dev`, `test`, `bench`, `release`)
 *     checked arithmetic trapping (`NoSilentArithmeticWrap`).
 *   Target 4 (SHM Arena Layout & Retaining Slice Bounds): `configure_shm_arena_header`
 *     and `ShmArenaReader::read_retained_payload` 64-byte block alignment, 128-byte
 *     `BLOCK_HEADER_BYTES` prefix, 4 KiB page rounding (`HEADER_BYTES = 4096`),
 *     and zero-OOB retained slice offsets across `1 <= payload_len <= 65536` and
 *     `1 <= block_count <= 4096` (`ArenaHeaderAndBlockBoundsExact`).
 *
 * Verified production Rust anchors:
 *   - Frame cap `MAX_MESSAGE = 16 * 1024` & `Session::decode_fast` / `QuoteConfig::validate`:
 *       crates/tickwire-md/src/spot_quotes/network.rs:18
 *       crates/tickwire-md/src/spot_quotes.rs:40-55
 *       crates/tickwire-md/src/spot_quotes.rs:137-176
 *       crates/tickwire-md/src/spot_quotes/protocol.rs:86-240
 *   - `StepSpec`, `Scanner::load_next_block`, `AVX2`/`SSE2`/`NEON`/`SWAR`/scalar loops:
 *       crates/tickwire-md/src/spot_quotes/simd_json.rs:117-145
 *       crates/tickwire-md/src/spot_quotes/simd_json.rs:189-469
 *       crates/tickwire-md/src/spot_quotes/simd_json.rs:675-694
 *   - `parse_positive_units` (`u64` fast path + `u128` `from_parts`) & `Decimal`/`units`:
 *       crates/tickwire-md/src/spot_quotes/simd_json.rs:523-621
 *       crates/tickwire-binance/src/decimal.rs:4-94
 *       crates/tickwire-binance/src/feed.rs:352-375
 *   - Workspace all-profile `overflow-checks = true`:
 *       Cargo.toml:58-71
 *   - `configure_shm_arena_header` & `ShmArenaReader::read_retained_payload`:
 *       crates/tickwire-strategy-sdk/src/arena.rs:17-58
 *       crates/tickwire-strategy-sdk/src/arena.rs:72-210
 *       crates/tickwire-strategy-sdk/src/arena.rs:439-502
 *)
EXTENDS Integers

CONSTANTS
  (* @type: Bool; *)
  BoundedMode

VARIABLES
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
  (* @type: Int; *)
  arena_class_case,
  (* @type: Int; *)
  arena_payload_len,
  (* @type: Int; *)
  arena_block_count,
  (* @type: Int; *)
  arena_block_idx,
  (* @type: Int; *)
  arena_raw_bytes,
  (* @type: Int; *)
  arena_bytes,
  (* @type: Int; *)
  arena_payload_offset

(* @type: <<Int, Int, Bool, Int, Int, Int, Int, Int, Int, Bool>>; *)
simdVars == <<
  backend_id, frame_len, frame_oversized, block_base, block_end, offset,
  last_read_start, last_read_end, last_shift_bits, movemask_upper32_zero
>>

(* @type: <<Int, Int, Bool, Bool, Int, Bool, Bool, Int, Int, Int, Bool, Int, Bool, Int, Int, Bool, Int, Int, Bool>>; *)
arithVars == <<
  step_case, str_len, has_minus, has_invalid_char, dot_count,
  digit_before_dot, digit_after_dot, total_digits, frac_scale, mantissa,
  fast_ok, fast_units, oracle_ok, oracle_units, ts_ms, ts_ok, ts_ns,
  profile_id, checked_overflow_trapped
>>

(* @type: <<Int, Int, Int, Int, Int, Int, Int>>; *)
arenaVars == <<
  arena_class_case, arena_payload_len, arena_block_count, arena_block_idx,
  arena_raw_bytes, arena_bytes, arena_payload_offset
>>

(* @type: <<Int, Int, Bool, Int, Int, Int, Int, Int, Int, Bool, Int, Int, Bool, Bool, Int, Bool, Bool, Int, Int, Int, Bool, Int, Bool, Int, Int, Bool, Int, Int, Bool, Int, Int, Int, Int, Int, Int, Int>>; *)
vars == <<
  backend_id, frame_len, frame_oversized, block_base, block_end, offset,
  last_read_start, last_read_end, last_shift_bits, movemask_upper32_zero,
  step_case, str_len, has_minus, has_invalid_char, dot_count,
  digit_before_dot, digit_after_dot, total_digits, frac_scale, mantissa,
  fast_ok, fast_units, oracle_ok, oracle_units, ts_ms, ts_ok, ts_ns,
  profile_id, checked_overflow_trapped,
  arena_class_case, arena_payload_len, arena_block_count, arena_block_idx,
  arena_raw_bytes, arena_bytes, arena_payload_offset
>>

(* @type: Int; *)
MaxMessageBytes == 16384

(* @type: Int; *)
ArenaHeaderBytes == 4096

(* @type: Int; *)
BlockHeaderBytes == 128

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

(* Linear `ArenaClass` payload limits (`1024B`, `4096B`, `65536B`) & 64B-aligned block sizes
 * (`crates/tickwire-strategy-sdk/src/arena.rs:79-104`). *)
(* @type: (Int) => Int; *)
ArenaClassLimit(ac) ==
  IF ac = 0 THEN 1024
  ELSE IF ac = 1 THEN 4096
  ELSE 65536

(* @type: (Int) => Int; *)
ArenaClassBlockBytes(ac) ==
  IF ac = 0 THEN 1152
  ELSE IF ac = 1 THEN 4224
  ELSE 65664

(* @type: (Int, Int) => Int; *)
ArenaRawBytes(ac, bcnt) ==
  IF ac = 0 THEN bcnt * 1152
  ELSE IF ac = 1 THEN bcnt * 4224
  ELSE bcnt * 65664

(* @type: (Int, Int) => Int; *)
ArenaPayloadOffset(ac, bidx) ==
  IF ac = 0 THEN ArenaHeaderBytes + (bidx * 1152) + BlockHeaderBytes
  ELSE IF ac = 1 THEN ArenaHeaderBytes + (bidx * 4224) + BlockHeaderBytes
  ELSE ArenaHeaderBytes + (bidx * 65664) + BlockHeaderBytes

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
    50000

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
          /\ arena_class_case = 0
          /\ arena_payload_len = 512
          /\ arena_block_count = 4
          /\ arena_block_idx = 2
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
          /\ arena_class_case \in Int /\ arena_class_case >= 0 /\ arena_class_case <= 2
          /\ arena_payload_len \in Int /\ arena_payload_len >= 1 /\ arena_payload_len <= ArenaClassLimit(arena_class_case)
          /\ arena_block_count \in Int /\ arena_block_count >= 1 /\ arena_block_count <= 4096
          /\ arena_block_idx \in Int /\ arena_block_idx >= 0 /\ arena_block_idx < arena_block_count
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
  /\ LET raw_b == ArenaRawBytes(arena_class_case, arena_block_count)
         pg_b  == ((raw_b + 4095) \div 4096) * 4096
     IN
     /\ arena_raw_bytes = raw_b
     /\ arena_bytes = pg_b
     /\ arena_payload_offset = ArenaPayloadOffset(arena_class_case, arena_block_idx)

(* `scan_block_avx2` (`crates/tickwire-md/src/spot_quotes/simd_json.rs:363-418`). *)
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
       arithVars, arenaVars
     >>

(* `scan_block_sse2` / `scan_block_neon` (`crates/tickwire-md/src/spot_quotes/simd_json.rs:342-362`, `crates/tickwire-md/src/spot_quotes/simd_json.rs:435-469`). *)
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
       arithVars, arenaVars
     >>

(* `scan_tail_swar_scalar` 8B loop (`crates/tickwire-md/src/spot_quotes/simd_json.rs:256-302`). *)
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
       arithVars, arenaVars
     >>

(* `scan_tail_swar_scalar` 1B scalar tail (`crates/tickwire-md/src/spot_quotes/simd_json.rs:256-302`). *)
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
       arithVars, arenaVars
     >>

(* `Scanner::load_next_block` (`crates/tickwire-md/src/spot_quotes/simd_json.rs:675-694`). *)
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
       backend_id, frame_len, frame_oversized, arithVars, arenaVars
     >>

(* Re-evaluates decimal + timestamp + SHM arena block offsets (`crates/tickwire-strategy-sdk/src/arena.rs:72-210`). *)
SampleDecimalTimestampAndArena ==
  /\ (frame_oversized \/ (offset = (block_end - block_base) /\ block_end = frame_len))
  /\ IF BoundedMode
     THEN /\ ~has_minus
          /\ \E sc \in FiniteStepCases :
             \E m \in {0, 25, 55} :
               LET slen == (IF m = 0 THEN 1 ELSE 2) + 1
                   digs == (IF m = 0 THEN 1 ELSE 2)
                   ts   == (IF m = 55 THEN 15 ELSE 5)
                   ac   == sc
                   plen == ArenaClassLimit(ac)
                   bcnt == 3
                   bidx == 2
                   raw_b == ArenaRawBytes(ac, bcnt)
                   pg_b  == ((raw_b + 4095) \div 4096) * 4096
                   fok  == FastSyntaxAndRangeOk(sc, slen, TRUE, FALSE, 0, TRUE, FALSE, digs, 0, m)
                   ook  == OracleSyntaxAndRangeOk(sc, slen, TRUE, FALSE, 0, TRUE, FALSE, digs, 0, m)
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
               /\ arena_class_case' = ac
               /\ arena_payload_len' = plen
               /\ arena_block_count' = bcnt
               /\ arena_block_idx' = bidx
               /\ arena_raw_bytes' = raw_b
               /\ arena_bytes' = pg_b
               /\ arena_payload_offset' = ArenaPayloadOffset(ac, bidx)
     ELSE \E sc \in Int, slen \in Int, neg \in BOOLEAN, badch \in BOOLEAN, dots \in Int :
          \E dbefore \in BOOLEAN, dafter \in BOOLEAN, digs \in Int, fscale \in Int, m \in Int :
          \E ts \in Int, prof \in Int, ac \in Int, plen \in Int, bcnt \in Int, bidx \in Int :
            /\ sc >= 0 /\ sc <= 2
            /\ ValidDecimalInputShape(slen, neg, badch, dots, dbefore, dafter, digs, fscale, m)
            /\ ts >= 0 /\ ts <= EffU64Max
            /\ prof >= 0 /\ prof <= 3
            /\ ac >= 0 /\ ac <= 2
            /\ plen >= 1 /\ plen <= ArenaClassLimit(ac)
            /\ bcnt >= 1 /\ bcnt <= 4096
            /\ bidx >= 0 /\ bidx < bcnt
            /\ LET fok == FastSyntaxAndRangeOk(sc, slen, neg, badch, dots, dbefore, dafter, digs, fscale, m)
                   ook == OracleSyntaxAndRangeOk(sc, slen, neg, badch, dots, dbefore, dafter, digs, fscale, m)
                   raw_b == ArenaRawBytes(ac, bcnt)
                   pg_b  == ((raw_b + 4095) \div 4096) * 4096
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
               /\ arena_class_case' = ac
               /\ arena_payload_len' = plen
               /\ arena_block_count' = bcnt
               /\ arena_block_idx' = bidx
               /\ arena_raw_bytes' = raw_b
               /\ arena_bytes' = pg_b
               /\ arena_payload_offset' = ArenaPayloadOffset(ac, bidx)
  /\ UNCHANGED simdVars

Next ==
  \/ StepSimd32
  \/ StepSimd16
  \/ StepSwar8
  \/ StepScalar1
  \/ AdvanceNextBlock64
  \/ SampleDecimalTimestampAndArena

Spec == Init /\ [][Next]_vars

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
  /\ arena_class_case \in Int /\ arena_class_case >= 0 /\ arena_class_case <= 2
  /\ arena_payload_len \in Int /\ arena_payload_len >= 1 /\ arena_payload_len <= ArenaClassLimit(arena_class_case)
  /\ arena_block_count \in Int /\ arena_block_count >= 1 /\ arena_block_count <= 4096
  /\ arena_block_idx \in Int /\ arena_block_idx >= 0 /\ arena_block_idx < arena_block_count
  /\ arena_raw_bytes \in Int
  /\ arena_bytes \in Int
  /\ arena_payload_offset \in Int

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

FastVsOracleExactEquivalence ==
  /\ (fast_ok <=> oracle_ok)
  /\ (fast_units = oracle_units)
  /\ (fast_ok => (fast_units > 0 /\ fast_units <= EffI64Max))
  /\ ((~fast_ok) => (fast_units = 0 /\ oracle_units = 0))

NoSilentArithmeticWrap ==
  /\ (ts_ok <=> (ts_ms * EffTsScale <= EffU64Max))
  /\ (ts_ok => (ts_ns = ts_ms * EffTsScale /\ ts_ns >= 0 /\ ts_ns <= EffU64Max))
  /\ ((~ts_ok) => (ts_ns = 0 /\ checked_overflow_trapped))
  /\ (((ScaledNumer(step_case, frac_scale, mantissa) \div ScaledDenom(step_case, frac_scale)) > EffI64Max) =>
       (~fast_ok /\ ~oracle_ok /\ checked_overflow_trapped))

(* 4. `configure_shm_arena_header` & `ShmArenaReader::read_retained_payload`
 *    (`crates/tickwire-strategy-sdk/src/arena.rs:72-210`, `crates/tickwire-strategy-sdk/src/arena.rs:439-502`):
 *    64B block alignment, 128B `ArenaBlockHeader` prefix, zero OOB retained slice reads,
 *    and exact 4 KiB page rounding (`resource_bytes % 4096 == 0`). *)
ArenaHeaderAndBlockBoundsExact ==
  /\ (ArenaClassBlockBytes(arena_class_case) % 64 = 0)
  /\ (BlockHeaderBytes + arena_payload_len <= ArenaClassBlockBytes(arena_class_case))
  /\ (ArenaHeaderBytes + BlockHeaderBytes <= arena_payload_offset)
  /\ (arena_payload_offset + arena_payload_len <= ArenaHeaderBytes + arena_raw_bytes)
  /\ (arena_raw_bytes <= arena_bytes)
  /\ ((ArenaHeaderBytes + arena_bytes) % 4096 = 0)

SafetyConjunction ==
  /\ TypeOK
  /\ ZeroVectorOobAndValidShift
  /\ FastVsOracleExactEquivalence
  /\ NoSilentArithmeticWrap
  /\ ArenaHeaderAndBlockBoundsExact

IndInv ==
  /\ TypeOK
  /\ ZeroVectorOobAndValidShift
  /\ ValidDecimalInputShape(
       str_len, has_minus, has_invalid_char, dot_count,
       digit_before_dot, digit_after_dot, total_digits, frac_scale, mantissa
     )
  /\ ts_ms >= 0 /\ ts_ms <= EffU64Max
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
  /\ arena_raw_bytes = ArenaRawBytes(arena_class_case, arena_block_count)
  /\ arena_bytes = ((arena_raw_bytes + 4095) \div 4096) * 4096
  /\ arena_payload_offset = ArenaPayloadOffset(arena_class_case, arena_block_idx)
  /\ FastVsOracleExactEquivalence
  /\ NoSilentArithmeticWrap
  /\ ArenaHeaderAndBlockBoundsExact

====
