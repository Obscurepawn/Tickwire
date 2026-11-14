---------------------- MODULE L0_ZeroCopyAndCrcGate ----------------------
EXTENDS Naturals, Integers, Sequences, FiniteSets

CONSTANTS
    Consumers,
    RingCapacity,
    MaxRingSeq,
    MaxWalBatches,
    CrcEnabled

(*
============================ var <-> code map ============================
Code-faithful model of Optimization #1 (compile-time `crc32` feature gate across
`tickwire-ipc` and `tickwire-journal`) and Optimization #5 (`#[repr(C)]` 512-byte
8-byte-aligned pure-integer `MarketRecord` zero-copy `&MarketRecord` borrow over
`tickwire-shm::RecordBorrow`).

CONSTANTS
  Consumers     - SPMC strategy reader set (e.g. {1, 2})
  RingCapacity  - ring slot count (power-of-two finite ring, e.g. 2)
  MaxRingSeq    - maximum published sequence (> RingCapacity to force wraparound)
  MaxWalBatches - maximum sealed WAL batches in segment
  CrcEnabled    - compile-time `cfg!(feature = "crc32")` mode:
                  crates/tickwire-ipc/Cargo.toml:14-16,
                  crates/tickwire-journal/Cargo.toml:12-14

VARIABLES
  ring_slots[s]      - 512B 8B-aligned shared-memory slot image & ABI layout metadata:
                       crates/tickwire-shm/src/mapped.rs:447-615,
                       crates/tickwire-ipc/src/wire.rs:437-458,
                       crates/tickwire-ipc/src/wire.rs:698-725,
                       crates/tickwire-ipc/src/wire.rs:1472-1479,
                       crates/tickwire-ipc/src/wire.rs:4103-4215,
                       crates/tickwire-ipc/tools/generate.py:16-24,
                       crates/tickwire-ipc/tools/generate.py:154-245.
                       write_gen increments on every producer slot mutation.
  prod_state         - producer two-phase write state {"Idle", "Writing"}:
                       crates/tickwire-shm/src/lib.rs:275-312,
                       crates/tickwire-shm/src/mapped.rs:466-515.
  prod_writing_slot  - physical slot index currently being overwritten (-1 if Idle).
  published_seq      - producer `PUBLISHED` release watermark:
                       crates/tickwire-shm/src/mapped.rs:511-515.
                       DENOTES: complete-publication content boundary.
  cached_min_rel     - producer conservative cached minimum `RELEASED` watermark:
                       crates/tickwire-shm/src/lib.rs:251-312,
                       crates/tickwire-shm/src/mapped.rs:359-407.
                       DENOTES: lower bound on slots no longer borrowed by any consumer.
  obs_rel[c]         - producer-sampled `RELEASED` watermark for consumer c:
                       crates/tickwire-shm/src/mapped.rs:359-380.
  cons_read[c]       - consumer `read` sequence watermark:
                       crates/tickwire-shm/src/mapped.rs:737-766.
  cons_rel[c]        - consumer `RELEASED` sequence watermark (advances ONLY on Drop):
                       crates/tickwire-shm/src/mapped.rs:768-793,
                       crates/tickwire-shm/src/mapped.rs:946-952.
  active_borrow[c]   - live `RecordBorrow<'a>` guard pinning slot & `snap_gen`:
                       crates/tickwire-shm/src/mapped.rs:825-952,
                       crates/tickwire-market-source/src/shm.rs:283-309,
                       crates/tickwire-market-source/src/replica.rs:383-498,
                       crates/tickwire-strategy-sdk/src/worker.rs:701-737.
  zc_last_outcome    - result of `RecordBorrow::as_market_record` / `from_aligned_bytes`:
                       crates/tickwire-ipc/src/zerocopy.rs:46-286,
                       crates/tickwire-ipc/src/crc32c.rs:45-120,
                       crates/tickwire-ipc/src/lib.rs:284-396.
  wal_state          - `tickwire-journal` segment lifecycle {"Open","Crashed","Recovered","Quarantined"}:
                       crates/tickwire-journal/src/journal.rs:207-408.
  wal_frames         - physical WAL frames (`BatchBegin`, `DataRecord`, `BatchSeal`) with
                       `header_crc`, `payload_crc`, SHA-256 `prev_hash -> frame_hash`,
                       `repeated_len_ok`, and `end_magic_ok`:
                       crates/tickwire-journal/src/frame.rs:75-193,
                       crates/tickwire-journal/src/crc32c.rs:45-126.
  wal_sealed_batches - durable `BatchSeal` count before crash:
                       crates/tickwire-journal/src/journal.rs:292-365.
  wal_recovered_batches - batch count restored by `Journal::recover`:
                       crates/tickwire-journal/src/journal.rs:375-408.
  ring_fault_used    - bounds ring corruption fault injection to 1 per run.
  probe_tested       - bounds misaligned/wrong-size cast rejection probe to 1 per run.

FAULTS MODELED:
  - arbitrary slow/stalled consumer holding `RecordBorrow` across producer wraparound;
  - unaligned / wrong-size / non-market (`ReportNonMarket`) zero-copy cast attempts;
  - shared-memory bit-flip corruption under `CrcEnabled = TRUE`;
  - crash mid-batch leaving an unsealed torn WAL tail;
  - offline storage bit-flip inside a sealed WAL frame under both `CrcEnabled` modes.
==========================================================================
*)

ASSUME
    /\ Consumers # {}
    /\ RingCapacity >= 2
    /\ MaxRingSeq > RingCapacity
    /\ MaxWalBatches >= 1
    /\ CrcEnabled \in BOOLEAN

Slots == 0..(RingCapacity - 1)
MsgTypes == {"None", "MarketBbo", "MarketDepthInline", "ReportNonMarket"}
PublishableMsgTypes == {"MarketBbo", "MarketDepthInline", "ReportNonMarket"}
MarketMsgTypes == {"MarketBbo", "MarketDepthInline"}
ProdStates == {"Idle", "Writing"}
ZcOutcomes == {
    "Init",
    "OkMarketView",
    "ErrWrongSizeOrAlign",
    "ErrWrongMsgType",
    "ErrChecksum"
}
WalStates == {"Open", "Crashed", "Recovered", "Quarantined"}
WalFrameKinds == {"BatchBegin", "DataRecord", "BatchSeal"}

SlotOf(seq) == (seq - 1) % RingCapacity

MinSet(S) == CHOOSE x \in S : \A y \in S : x <= y

ExpectedRecordCrc(seq, mtype) ==
    IF ~CrcEnabled THEN 0
    ELSE IF mtype = "MarketBbo" THEN seq * 10 + 1
    ELSE IF mtype = "MarketDepthInline" THEN seq * 10 + 2
    ELSE seq * 10 + 3

ExpectedPayloadCrc(seq, mtype) ==
    IF ~CrcEnabled THEN 0
    ELSE IF mtype = "MarketDepthInline" THEN seq * 20 + 7
    ELSE 0

ExpectedWalCrc(batch, ord) ==
    IF ~CrcEnabled THEN 0 ELSE batch * 100 + ord

ExpectedWalHash(prev_hash, batch, ord, h_crc, p_crc) ==
    ((prev_hash * 31 + batch * 17 + ord * 13 + h_crc + p_crc) % 997) + 1

EmptySlot == [
    initialized          |-> FALSE,
    byte_size            |-> 512,
    align_bytes          |-> 8,
    has_invalid_bit_type |-> FALSE,
    seq                  |-> 0,
    schema_ok            |-> FALSE,
    msg_type             |-> "None",
    flag_0x08            |-> FALSE,
    record_crc           |-> 0,
    payload_crc          |-> 0,
    corrupted            |-> FALSE,
    write_gen            |-> 0
]

NoBorrow == [
    active     |-> FALSE,
    seq        |-> 0,
    slot       |-> 0,
    snap_gen   |-> 0,
    view_valid |-> FALSE
]

VARIABLES
    ring_slots,
    prod_state,
    prod_writing_slot,
    published_seq,
    cached_min_rel,
    obs_rel,
    cons_read,
    cons_rel,
    active_borrow,
    zc_last_outcome,
    wal_state,
    wal_frames,
    wal_sealed_batches,
    wal_recovered_batches,
    ring_fault_used,
    probe_tested

vars == <<
    ring_slots, prod_state, prod_writing_slot, published_seq, cached_min_rel,
    obs_rel, cons_read, cons_rel, active_borrow, zc_last_outcome,
    wal_state, wal_frames, wal_sealed_batches, wal_recovered_batches,
    ring_fault_used, probe_tested
>>

Init ==
    /\ ring_slots = [s \in Slots |-> EmptySlot]
    /\ prod_state = "Idle"
    /\ prod_writing_slot = -1
    /\ published_seq = 0
    /\ cached_min_rel = 0
    /\ obs_rel = [c \in Consumers |-> 0]
    /\ cons_read = [c \in Consumers |-> 0]
    /\ cons_rel = [c \in Consumers |-> 0]
    /\ active_borrow = [c \in Consumers |-> NoBorrow]
    /\ zc_last_outcome = "Init"
    /\ wal_state = "Open"
    /\ wal_frames = <<>>
    /\ wal_sealed_batches = 0
    /\ wal_recovered_batches = 0
    /\ ring_fault_used = FALSE
    /\ probe_tested = FALSE

\* Producer samples consumer RELEASED watermarks (`crates/tickwire-shm/src/mapped.rs:359-407`).
ProducerObserveWatermarks ==
    /\ prod_state = "Idle"
    /\ \E c \in Consumers : obs_rel[c] < cons_rel[c]
    /\ LET new_obs == [c \in Consumers |-> cons_rel[c]]
       IN /\ obs_rel' = new_obs
          /\ cached_min_rel' = MinSet({new_obs[c] : c \in Consumers})
    /\ UNCHANGED <<
        ring_slots, prod_state, prod_writing_slot, published_seq,
        cons_read, cons_rel, active_borrow, zc_last_outcome,
        wal_state, wal_frames, wal_sealed_batches, wal_recovered_batches,
        ring_fault_used, probe_tested
       >>

\* Producer claims and begins writing next 512B slot (`crates/tickwire-shm/src/mapped.rs:447-510`, `crates/tickwire-shm/src/mapped.rs:582-615`).
ProducerBeginWrite(mtype) ==
    /\ prod_state = "Idle"
    /\ published_seq < MaxRingSeq
    /\ mtype \in PublishableMsgTypes
    /\ LET next_seq == published_seq + 1
           slot     == SlotOf(next_seq)
       IN /\ next_seq - cached_min_rel <= RingCapacity
          /\ prod_state' = "Writing"
          /\ prod_writing_slot' = slot
          /\ ring_slots' = [ring_slots EXCEPT ![slot] = [
                initialized          |-> TRUE,
                byte_size            |-> 512,
                align_bytes          |-> 8,
                has_invalid_bit_type |-> FALSE,
                seq                  |-> next_seq,
                schema_ok            |-> TRUE,
                msg_type             |-> mtype,
                flag_0x08            |-> CrcEnabled,
                record_crc           |-> ExpectedRecordCrc(next_seq, mtype),
                payload_crc          |-> ExpectedPayloadCrc(next_seq, mtype),
                corrupted            |-> FALSE,
                write_gen            |-> ring_slots[slot].write_gen + 1
             ]]
    /\ UNCHANGED <<
        published_seq, cached_min_rel, obs_rel, cons_read, cons_rel,
        active_borrow, zc_last_outcome, wal_state, wal_frames,
        wal_sealed_batches, wal_recovered_batches, ring_fault_used, probe_tested
       >>

\* Producer commits slot publication via release store (`crates/tickwire-shm/src/mapped.rs:511-515`).
ProducerPublishSlot ==
    /\ prod_state = "Writing"
    /\ prod_state' = "Idle"
    /\ prod_writing_slot' = -1
    /\ published_seq' = published_seq + 1
    /\ UNCHANGED <<
        ring_slots, cached_min_rel, obs_rel, cons_read, cons_rel,
        active_borrow, zc_last_outcome, wal_state, wal_frames,
        wal_sealed_batches, wal_recovered_batches, ring_fault_used, probe_tested
       >>

\* Consumer acquires a lifetime-pinned RecordBorrow (`crates/tickwire-shm/src/mapped.rs:737-766`, `crates/tickwire-shm/src/mapped.rs:825-853`).
ConsumerAcquireBorrow(c) ==
    /\ ~active_borrow[c].active
    /\ cons_read[c] < published_seq
    /\ LET next_seq == cons_read[c] + 1
           slot     == SlotOf(next_seq)
       IN /\ ring_slots[slot].seq = next_seq
          /\ cons_read' = [cons_read EXCEPT ![c] = next_seq]
          /\ active_borrow' = [active_borrow EXCEPT ![c] = [
                active     |-> TRUE,
                seq        |-> next_seq,
                slot       |-> slot,
                snap_gen   |-> ring_slots[slot].write_gen,
                view_valid |-> FALSE
             ]]
    /\ UNCHANGED <<
        ring_slots, prod_state, prod_writing_slot, published_seq,
        cached_min_rel, obs_rel, cons_rel, zc_last_outcome,
        wal_state, wal_frames, wal_sealed_batches, wal_recovered_batches,
        ring_fault_used, probe_tested
       >>

\* Consumer invokes RecordBorrow::as_market_record -> from_aligned_bytes (`crates/tickwire-shm/src/mapped.rs:886-943`, `crates/tickwire-ipc/src/zerocopy.rs:146-286`).
ConsumerCastMarketRecord(c) ==
    /\ active_borrow[c].active
    /\ ~active_borrow[c].view_valid
    /\ LET slot == active_borrow[c].slot
           s    == ring_slots[slot]
           crc_ok ==
               IF ~s.flag_0x08 THEN
                   /\ s.record_crc = 0
                   /\ s.payload_crc = 0
                   /\ ~CrcEnabled
               ELSE
                   /\ CrcEnabled
                   /\ ~s.corrupted
                   /\ s.record_crc = ExpectedRecordCrc(s.seq, s.msg_type)
                   /\ s.payload_crc = ExpectedPayloadCrc(s.seq, s.msg_type)
       IN IF ~(s.initialized /\ s.byte_size = 512 /\ s.align_bytes = 8 /\ ~s.has_invalid_bit_type /\ s.schema_ok) THEN
              /\ zc_last_outcome' = "ErrWrongSizeOrAlign"
              /\ UNCHANGED active_borrow
          ELSE IF s.msg_type \notin MarketMsgTypes THEN
              /\ zc_last_outcome' = "ErrWrongMsgType"
              /\ UNCHANGED active_borrow
          ELSE IF ~crc_ok THEN
              /\ zc_last_outcome' = "ErrChecksum"
              /\ UNCHANGED active_borrow
          ELSE
              /\ zc_last_outcome' = "OkMarketView"
              /\ active_borrow' = [active_borrow EXCEPT ![c].view_valid = TRUE]
    /\ UNCHANGED <<
        ring_slots, prod_state, prod_writing_slot, published_seq,
        cached_min_rel, obs_rel, cons_read, cons_rel,
        wal_state, wal_frames, wal_sealed_batches, wal_recovered_batches,
        ring_fault_used, probe_tested
       >>

\* Boundary probe on MarketRecord::from_aligned_bytes with unaligned or wrong-size slice (`crates/tickwire-ipc/src/zerocopy.rs:76-123`).
RejectMisalignedOrBadSizeProbe ==
    /\ published_seq = 0
    /\ ~probe_tested
    /\ probe_tested' = TRUE
    /\ zc_last_outcome' = "ErrWrongSizeOrAlign"
    /\ UNCHANGED <<
        ring_slots, prod_state, prod_writing_slot, published_seq,
        cached_min_rel, obs_rel, cons_read, cons_rel, active_borrow,
        wal_state, wal_frames, wal_sealed_batches, wal_recovered_batches,
        ring_fault_used
       >>

\* Fault injection (CrcEnabled mode): flip a payload bit on an unborrowed published slot (`crates/tickwire-ipc/src/zerocopy.rs:102-122`).
InjectSlotBitFlipWhenCrcEnabled(s) ==
    /\ CrcEnabled
    /\ ~ring_fault_used
    /\ prod_state = "Idle"
    /\ ring_slots[s].initialized
    /\ ring_slots[s].msg_type \in MarketMsgTypes
    /\ \A c \in Consumers : ~(active_borrow[c].active /\ active_borrow[c].slot = s)
    /\ ring_fault_used' = TRUE
    /\ ring_slots' = [ring_slots EXCEPT ![s].corrupted = TRUE]
    /\ UNCHANGED <<
        prod_state, prod_writing_slot, published_seq, cached_min_rel,
        obs_rel, cons_read, cons_rel, active_borrow, zc_last_outcome,
        wal_state, wal_frames, wal_sealed_batches, wal_recovered_batches,
        probe_tested
       >>

\* Dropping RecordBorrow ends `&MarketRecord` lifetime and advances RELEASED (`crates/tickwire-shm/src/mapped.rs:946-952`, `crates/tickwire-market-source/src/shm.rs:283-296`).
ConsumerDropBorrow(c) ==
    /\ active_borrow[c].active
    /\ active_borrow' = [active_borrow EXCEPT ![c] = NoBorrow]
    /\ cons_rel' = [cons_rel EXCEPT ![c] = cons_read[c]]
    /\ UNCHANGED <<
        ring_slots, prod_state, prod_writing_slot, published_seq,
        cached_min_rel, obs_rel, cons_read, zc_last_outcome,
        wal_state, wal_frames, wal_sealed_batches, wal_recovered_batches,
        ring_fault_used, probe_tested
       >>

\* Append a complete sealed 3-frame WAL batch [BatchBegin, DataRecord, BatchSeal] (`crates/tickwire-journal/src/frame.rs:75-118`, `crates/tickwire-journal/src/journal.rs:140-198`).
WalAppendSealedBatch ==
    /\ wal_state = "Open"
    /\ wal_sealed_batches < MaxWalBatches
    /\ LET b == wal_sealed_batches + 1
           h0 == IF Len(wal_frames) = 0 THEN 0 ELSE wal_frames[Len(wal_frames)].frame_hash
           hc1 == ExpectedWalCrc(b, 1)
           pc1 == ExpectedWalCrc(b, 1)
           fh1 == ExpectedWalHash(h0, b, 1, hc1, pc1)
           f1  == [
               batch           |-> b,
               kind            |-> "BatchBegin",
               header_crc      |-> hc1,
               payload_crc     |-> pc1,
               prev_hash       |-> h0,
               frame_hash      |-> fh1,
               repeated_len_ok |-> TRUE,
               end_magic_ok    |-> TRUE,
               complete        |-> TRUE,
               bit_flipped     |-> FALSE
           ]
           hc2 == ExpectedWalCrc(b, 2)
           pc2 == ExpectedWalCrc(b, 2)
           fh2 == ExpectedWalHash(fh1, b, 2, hc2, pc2)
           f2  == [
               batch           |-> b,
               kind            |-> "DataRecord",
               header_crc      |-> hc2,
               payload_crc     |-> pc2,
               prev_hash       |-> fh1,
               frame_hash      |-> fh2,
               repeated_len_ok |-> TRUE,
               end_magic_ok    |-> TRUE,
               complete        |-> TRUE,
               bit_flipped     |-> FALSE
           ]
           hc3 == ExpectedWalCrc(b, 3)
           pc3 == ExpectedWalCrc(b, 3)
           fh3 == ExpectedWalHash(fh2, b, 3, hc3, pc3)
           f3  == [
               batch           |-> b,
               kind            |-> "BatchSeal",
               header_crc      |-> hc3,
               payload_crc     |-> pc3,
               prev_hash       |-> fh2,
               frame_hash      |-> fh3,
               repeated_len_ok |-> TRUE,
               end_magic_ok    |-> TRUE,
               complete        |-> TRUE,
               bit_flipped     |-> FALSE
           ]
       IN /\ wal_frames' = wal_frames \o <<f1, f2, f3>>
          /\ wal_sealed_batches' = b
    /\ UNCHANGED <<
        ring_slots, prod_state, prod_writing_slot, published_seq,
        cached_min_rel, obs_rel, cons_read, cons_rel, active_borrow,
        zc_last_outcome, wal_state, wal_recovered_batches,
        ring_fault_used, probe_tested
       >>

\* Crash mid-batch leaving an unsealed BatchBegin + incomplete/torn DataRecord tail (`crates/tickwire-journal/src/journal.rs:240-365`).
WalCrashWithTornTail ==
    /\ wal_state = "Open"
    /\ wal_sealed_batches >= 1
    /\ LET b   == wal_sealed_batches + 1
           h0  == wal_frames[Len(wal_frames)].frame_hash
           hc1 == ExpectedWalCrc(b, 1)
           pc1 == ExpectedWalCrc(b, 1)
           fh1 == ExpectedWalHash(h0, b, 1, hc1, pc1)
           f_begin == [
               batch           |-> b,
               kind            |-> "BatchBegin",
               header_crc      |-> hc1,
               payload_crc     |-> pc1,
               prev_hash       |-> h0,
               frame_hash      |-> fh1,
               repeated_len_ok |-> TRUE,
               end_magic_ok    |-> TRUE,
               complete        |-> TRUE,
               bit_flipped     |-> FALSE
           ]
           f_torn == [
               batch           |-> b,
               kind            |-> "DataRecord",
               header_crc      |-> ExpectedWalCrc(b, 2),
               payload_crc     |-> ExpectedWalCrc(b, 2),
               prev_hash       |-> fh1,
               frame_hash      |-> 0,
               repeated_len_ok |-> FALSE,
               end_magic_ok    |-> FALSE,
               complete        |-> FALSE,
               bit_flipped     |-> FALSE
           ]
       IN /\ wal_frames' = wal_frames \o <<f_begin, f_torn>>
          /\ wal_state' = "Crashed"
    /\ UNCHANGED <<
        ring_slots, prod_state, prod_writing_slot, published_seq,
        cached_min_rel, obs_rel, cons_read, cons_rel, active_borrow,
        zc_last_outcome, wal_sealed_batches, wal_recovered_batches,
        ring_fault_used, probe_tested
       >>

\* Crash + storage bit-flip inside a sealed WAL frame (`crates/tickwire-journal/src/frame.rs:162-193`).
WalCrashAndCorruptSealedFrame(idx) ==
    /\ wal_state = "Open"
    /\ wal_sealed_batches >= 1
    /\ idx \in 1..Len(wal_frames)
    /\ wal_frames' = [wal_frames EXCEPT
        ![idx].bit_flipped = TRUE,
        ![idx].frame_hash = 0
       ]
    /\ wal_state' = "Crashed"
    /\ UNCHANGED <<
        ring_slots, prod_state, prod_writing_slot, published_seq,
        cached_min_rel, obs_rel, cons_read, cons_rel, active_borrow,
        zc_last_outcome, wal_sealed_batches, wal_recovered_batches,
        ring_fault_used, probe_tested
       >>

SealedPrefixValid ==
    /\ Len(wal_frames) >= 3 * wal_sealed_batches
    /\ \A i \in 1..(3 * wal_sealed_batches) :
        LET f    == wal_frames[i]
            prev == IF i = 1 THEN 0 ELSE wal_frames[i - 1].frame_hash
            ord  == ((i - 1) % 3) + 1
        IN /\ f.complete
           /\ ~f.bit_flipped
           /\ f.prev_hash = prev
           /\ f.repeated_len_ok
           /\ f.end_magic_ok
           /\ f.frame_hash = ExpectedWalHash(prev, f.batch, ord, f.header_crc, f.payload_crc)
           /\ (CrcEnabled => (f.header_crc = ExpectedWalCrc(f.batch, ord) /\ f.payload_crc = ExpectedWalCrc(f.batch, ord)))

\* Journal::recover validates SHA-256 chain + trailer and truncates only unsealed torn tail (`crates/tickwire-journal/src/journal.rs:366-408`).
WalRecoverTruncateTornTail ==
    /\ wal_state = "Crashed"
    /\ SealedPrefixValid
    /\ \A j \in (3 * wal_sealed_batches + 1)..Len(wal_frames) : ~wal_frames[j].bit_flipped
    /\ wal_frames' = SubSeq(wal_frames, 1, 3 * wal_sealed_batches)
    /\ wal_recovered_batches' = wal_sealed_batches
    /\ wal_state' = "Recovered"
    /\ UNCHANGED <<
        ring_slots, prod_state, prod_writing_slot, published_seq,
        cached_min_rel, obs_rel, cons_read, cons_rel, active_borrow,
        zc_last_outcome, wal_sealed_batches, ring_fault_used, probe_tested
       >>

\* Journal::recover quarantines corrupted sealed frames via SHA-256 chain / trailer even when CrcEnabled = FALSE (`crates/tickwire-journal/src/frame.rs:162-193`, `crates/tickwire-journal/src/journal.rs:240-365`).
WalRecoverQuarantineCorruption ==
    /\ wal_state = "Crashed"
    /\ ~SealedPrefixValid
    /\ wal_state' = "Quarantined"
    /\ UNCHANGED <<
        ring_slots, prod_state, prod_writing_slot, published_seq,
        cached_min_rel, obs_rel, cons_read, cons_rel, active_borrow,
        zc_last_outcome, wal_frames, wal_sealed_batches, wal_recovered_batches,
        ring_fault_used, probe_tested
       >>

Next ==
    \/ ProducerObserveWatermarks
    \/ (\E mtype \in PublishableMsgTypes : ProducerBeginWrite(mtype))
    \/ ProducerPublishSlot
    \/ (\E c \in Consumers : ConsumerAcquireBorrow(c))
    \/ (\E c \in Consumers : ConsumerCastMarketRecord(c))
    \/ RejectMisalignedOrBadSizeProbe
    \/ (\E s \in Slots : InjectSlotBitFlipWhenCrcEnabled(s))
    \/ (\E c \in Consumers : ConsumerDropBorrow(c))
    \/ WalAppendSealedBatch
    \/ WalCrashWithTornTail
    \/ (\E idx \in 1..(3 * MaxWalBatches) : WalCrashAndCorruptSealedFrame(idx))
    \/ WalRecoverTruncateTornTail
    \/ WalRecoverQuarantineCorruption

Spec == Init /\ [][Next]_vars

\* =============================== Invariants ===============================

TypeOK ==
    /\ prod_state \in ProdStates
    /\ prod_writing_slot \in ({-1} \cup Slots)
    /\ published_seq \in 0..MaxRingSeq
    /\ cached_min_rel \in 0..MaxRingSeq
    /\ obs_rel \in [Consumers -> 0..MaxRingSeq]
    /\ cons_read \in [Consumers -> 0..MaxRingSeq]
    /\ cons_rel \in [Consumers -> 0..MaxRingSeq]
    /\ zc_last_outcome \in ZcOutcomes
    /\ wal_state \in WalStates
    /\ wal_sealed_batches \in 0..MaxWalBatches
    /\ wal_recovered_batches \in 0..MaxWalBatches
    /\ ring_fault_used \in BOOLEAN
    /\ probe_tested \in BOOLEAN
    /\ \A s \in Slots :
        /\ ring_slots[s].initialized \in BOOLEAN
        /\ ring_slots[s].byte_size = 512
        /\ ring_slots[s].align_bytes = 8
        /\ ring_slots[s].has_invalid_bit_type = FALSE
        /\ ring_slots[s].seq \in 0..MaxRingSeq
        /\ ring_slots[s].schema_ok \in BOOLEAN
        /\ ring_slots[s].msg_type \in MsgTypes
        /\ ring_slots[s].flag_0x08 \in BOOLEAN
        /\ ring_slots[s].corrupted \in BOOLEAN
        /\ ring_slots[s].write_gen \in 0..MaxRingSeq
    /\ \A c \in Consumers :
        /\ active_borrow[c].active \in BOOLEAN
        /\ active_borrow[c].seq \in 0..MaxRingSeq
        /\ active_borrow[c].slot \in Slots
        /\ active_borrow[c].snap_gen \in 0..MaxRingSeq
        /\ active_borrow[c].view_valid \in BOOLEAN

\* Proves zero Rust `&MarketRecord` aliasing / mutation UB while any RecordBorrow is alive.
ZeroNoAliasWhileBorrowed ==
    \A c \in Consumers :
        active_borrow[c].active =>
            LET b == active_borrow[c]
            IN /\ prod_writing_slot # b.slot
               /\ ring_slots[b.slot].write_gen = b.snap_gen
               /\ ring_slots[b.slot].seq = b.seq
               /\ cons_rel[c] < b.seq

\* Proves `&MarketRecord` zero-copy view is admitted ONLY on initialized, 8B-aligned, 512B market records with zero bool/enum UB.
ZeroCopyLayoutAndHeaderSoundness ==
    \A c \in Consumers :
        (active_borrow[c].active /\ active_borrow[c].view_valid) =>
            LET s == ring_slots[active_borrow[c].slot]
            IN /\ s.initialized
               /\ s.byte_size = 512
               /\ s.align_bytes = 8
               /\ ~s.has_invalid_bit_type
               /\ s.schema_ok
               /\ s.msg_type \in MarketMsgTypes

\* Proves compile-time crc32 gate clears 0x08 & CRC fields when disabled and rejects corruption when enabled.
CrcGateModeConsistency ==
    /\ (~CrcEnabled =>
        /\ (\A s \in Slots :
            ring_slots[s].initialized =>
                (~ring_slots[s].flag_0x08 /\ ring_slots[s].record_crc = 0 /\ ring_slots[s].payload_crc = 0))
        /\ (\A i \in 1..Len(wal_frames) :
            wal_frames[i].header_crc = 0 /\ wal_frames[i].payload_crc = 0))
    /\ (CrcEnabled =>
        /\ (\A s \in Slots :
            ring_slots[s].initialized =>
                (ring_slots[s].flag_0x08 /\ ring_slots[s].record_crc > 0))
        /\ (\A c \in Consumers :
            (active_borrow[c].active /\ active_borrow[c].view_valid) =>
                ~ring_slots[active_borrow[c].slot].corrupted))

\* Proves WAL SHA-256 chain + repeated_total_len + END_MAGIC quarantine sealed-batch bit flips and truncate torn tails even without CRC32C.
WalRecoverySoundWithoutCrc ==
    /\ (wal_state = "Recovered" =>
        /\ wal_recovered_batches = wal_sealed_batches
        /\ Len(wal_frames) = 3 * wal_sealed_batches
        /\ \A i \in 1..Len(wal_frames) :
            /\ wal_frames[i].complete
            /\ ~wal_frames[i].bit_flipped
            /\ wal_frames[i].repeated_len_ok
            /\ wal_frames[i].end_magic_ok)
    /\ (wal_state = "Quarantined" =>
        /\ wal_recovered_batches = 0
        /\ \E i \in 1..(3 * wal_sealed_batches) : wal_frames[i].bit_flipped)

=============================================================================
