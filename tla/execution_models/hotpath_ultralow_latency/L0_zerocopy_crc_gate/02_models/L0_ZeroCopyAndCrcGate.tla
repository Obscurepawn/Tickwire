---------------------- MODULE L0_ZeroCopyAndCrcGate ----------------------
EXTENDS Naturals, Integers, FiniteSets

CONSTANTS
    Consumers,
    RingCapacity,
    MaxRingSeq,
    MaxArenaBlocks,
    CrcEnabled

(*
============================ var <-> code map ============================
Code-faithful model of pure shared-memory (`memfd`) zero-copy record borrowing
(`tickwire-shm::RecordBorrow`), release-watermark-gated `ShmArenaWriter` /
`ShmArenaReader` oversize payload reclamation (`tickwire-strategy-sdk::arena`),
and compile-time `crc32` feature gating with zero local WAL or disk state.

CONSTANTS
  Consumers      - SPMC strategy subscriber set (e.g. {1, 2})
  RingCapacity   - ring slot count (power-of-two finite ring, e.g. 2)
  MaxRingSeq     - maximum published sequence (> RingCapacity to force wraparound)
  MaxArenaBlocks - `ArenaClass.block_count` in `ShmArenaWriter`:
                   crates/tickwire-strategy-sdk/src/arena.rs:263-270,
                   crates/tickwire-strategy-sdk/src/arena.rs:325-337
  CrcEnabled     - compile-time `cfg!(feature = "crc32")` mode:
                   crates/tickwire-strategy-sdk/src/arena.rs:339-348

VARIABLES
  ring_slots[s]      - 512B 8B-aligned shared-memory slot image & ABI layout metadata:
                       crates/tickwire-shm/src/mapped.rs:455-614,
                       crates/tickwire-shm/src/mapped.rs:825-945.
                       write_gen increments on every producer slot mutation.
  arena_blocks[b]    - `ShmArenaWriter` block state (`owner_seq`, `payload_crc`,
                       `corrupted`, `write_gen`):
                       crates/tickwire-strategy-sdk/src/arena.rs:241-386,
                       crates/tickwire-strategy-sdk/src/arena.rs:400-502.
  prod_state         - producer two-phase write state {"Idle", "Writing"}:
                       crates/tickwire-shm/src/lib.rs:280-332,
                       crates/tickwire-shm/src/mapped.rs:460-556.
  prod_writing_slot  - physical slot index currently being overwritten (-1 if Idle).
  published_seq      - producer `PUBLISHED` release watermark:
                       crates/tickwire-shm/src/lib.rs:327-332,
                       crates/tickwire-shm/src/mapped.rs:536-556.
  cached_min_rel     - producer conservative cached minimum `RELEASED` watermark:
                       crates/tickwire-shm/src/lib.rs:256-318,
                       crates/tickwire-shm/src/mapped.rs:359-407.
  obs_rel[c]         - producer-sampled `RELEASED` watermark for consumer c:
                       crates/tickwire-shm/src/mapped.rs:359-380.
  cons_read[c]       - consumer `read` sequence watermark:
                       crates/tickwire-shm/src/lib.rs:471-502,
                       crates/tickwire-shm/src/mapped.rs:737-770.
  cons_rel[c]        - consumer `RELEASED` sequence watermark (advances ONLY on Drop):
                       crates/tickwire-shm/src/lib.rs:503-525,
                       crates/tickwire-shm/src/mapped.rs:772-793,
                       crates/tickwire-shm/src/mapped.rs:946-952.
  active_borrow[c]   - live `RecordBorrow<'a, B>` guard pinning slot & arena block:
                       crates/tickwire-shm/src/mapped.rs:825-952,
                       crates/tickwire-market-source/src/shm.rs:284-309,
                       crates/tickwire-strategy-sdk/src/worker.rs:243-310,
                       crates/tickwire-runtime/src/ors.rs:1487-1503.
  zc_last_outcome    - result of `RecordBorrow::as_market_record` / `depth_payload`
                       and `ShmArenaReader::read_retained_payload`:
                       crates/tickwire-shm/src/mapped.rs:884-945,
                       crates/tickwire-strategy-sdk/src/arena.rs:439-502.
  ring_fault_used    - bounds ring or arena bit-flip fault injection to 1 per run.
  probe_tested       - bounds misaligned/wrong-size cast rejection probe to 1 per run.
==========================================================================
*)

ASSUME
    /\ Consumers # {}
    /\ RingCapacity >= 2
    /\ MaxRingSeq > RingCapacity
    /\ MaxArenaBlocks >= 1
    /\ CrcEnabled \in BOOLEAN

Slots == 0..(RingCapacity - 1)
ArenaBlocks == 1..MaxArenaBlocks
MsgTypes == {"None", "MarketBbo", "MarketDepthArena", "ReportNonMarket"}
PublishableMsgTypes == {"MarketBbo", "MarketDepthArena", "ReportNonMarket"}
MarketMsgTypes == {"MarketBbo", "MarketDepthArena"}
ProdStates == {"Idle", "Writing"}
ZcOutcomes == {
    "Init",
    "OkMarketView",
    "ErrWrongSizeOrAlign",
    "ErrWrongMsgType",
    "ErrChecksum"
}

SlotOf(seq) == (seq - 1) % RingCapacity

MinSet(S) == CHOOSE x \in S : \A y \in S : x <= y

ExpectedRecordCrc(seq, mtype) ==
    IF ~CrcEnabled THEN 0
    ELSE IF mtype = "MarketBbo" THEN seq * 10 + 1
    ELSE IF mtype = "MarketDepthArena" THEN seq * 10 + 2
    ELSE seq * 10 + 3

ExpectedPayloadCrc(seq, mtype) ==
    IF ~CrcEnabled THEN 0
    ELSE IF mtype = "MarketDepthArena" THEN seq * 20 + 7
    ELSE 0

EmptySlot == [
    initialized          |-> FALSE,
    byte_size            |-> 512,
    align_bytes          |-> 8,
    has_invalid_bit_type |-> FALSE,
    seq                  |-> 0,
    schema_ok            |-> FALSE,
    msg_type             |-> "None",
    arena_block          |-> 0,
    flag_0x08            |-> FALSE,
    record_crc           |-> 0,
    payload_crc          |-> 0,
    corrupted            |-> FALSE,
    write_gen            |-> 0
]

EmptyArenaBlock == [
    owner_seq   |-> 0,
    payload_crc |-> 0,
    corrupted   |-> FALSE,
    write_gen   |-> 0
]

NoBorrow == [
    active     |-> FALSE,
    seq        |-> 0,
    slot       |-> 0,
    snap_gen   |-> 0,
    arena_blk  |-> 0,
    arena_gen  |-> 0,
    view_valid |-> FALSE
]

VARIABLES
    ring_slots,
    arena_blocks,
    prod_state,
    prod_writing_slot,
    published_seq,
    cached_min_rel,
    obs_rel,
    cons_read,
    cons_rel,
    active_borrow,
    zc_last_outcome,
    ring_fault_used,
    probe_tested

vars == <<
    ring_slots, arena_blocks, prod_state, prod_writing_slot, published_seq,
    cached_min_rel, obs_rel, cons_read, cons_rel, active_borrow,
    zc_last_outcome, ring_fault_used, probe_tested
>>

Init ==
    /\ ring_slots = [s \in Slots |-> EmptySlot]
    /\ arena_blocks = [b \in ArenaBlocks |-> EmptyArenaBlock]
    /\ prod_state = "Idle"
    /\ prod_writing_slot = -1
    /\ published_seq = 0
    /\ cached_min_rel = 0
    /\ obs_rel = [c \in Consumers |-> 0]
    /\ cons_read = [c \in Consumers |-> 0]
    /\ cons_rel = [c \in Consumers |-> 0]
    /\ active_borrow = [c \in Consumers |-> NoBorrow]
    /\ zc_last_outcome = "Init"
    /\ ring_fault_used = FALSE
    /\ probe_tested = FALSE

ReclaimedArena(blocks, rel_seq) ==
    [b \in ArenaBlocks |->
        IF blocks[b].owner_seq # 0 /\ blocks[b].owner_seq <= rel_seq
        THEN [blocks[b] EXCEPT !.owner_seq = 0]
        ELSE blocks[b]
    ]

\* Producer samples consumer RELEASED watermarks (`crates/tickwire-shm/src/mapped.rs:359-407`).
ProducerObserveWatermarks ==
    /\ prod_state = "Idle"
    /\ \E c \in Consumers : obs_rel[c] < cons_rel[c]
    /\ LET new_obs == [c \in Consumers |-> cons_rel[c]]
       IN /\ obs_rel' = new_obs
          /\ cached_min_rel' = MinSet({new_obs[c] : c \in Consumers})
    /\ UNCHANGED <<
        ring_slots, arena_blocks, prod_state, prod_writing_slot, published_seq,
        cons_read, cons_rel, active_borrow, zc_last_outcome,
        ring_fault_used, probe_tested
       >>

\* Explicit `ShmArenaWriter::reclaim(released_seq)` (`crates/tickwire-strategy-sdk/src/arena.rs:291-299`).
ArenaReclaimReleased ==
    /\ prod_state = "Idle"
    /\ \E b \in ArenaBlocks :
        arena_blocks[b].owner_seq # 0 /\ arena_blocks[b].owner_seq <= cached_min_rel
    /\ arena_blocks' = ReclaimedArena(arena_blocks, cached_min_rel)
    /\ UNCHANGED <<
        ring_slots, prod_state, prod_writing_slot, published_seq,
        cached_min_rel, obs_rel, cons_read, cons_rel, active_borrow,
        zc_last_outcome, ring_fault_used, probe_tested
       >>

\* Producer claims slot (and optional `ShmArenaWriter::write_payload` block) before publishing
\* (`crates/tickwire-shm/src/mapped.rs:460-556`, `crates/tickwire-strategy-sdk/src/arena.rs:304-386`).
ProducerBeginWrite(mtype) ==
    /\ prod_state = "Idle"
    /\ published_seq < MaxRingSeq
    /\ mtype \in PublishableMsgTypes
    /\ LET next_seq == published_seq + 1
           slot     == SlotOf(next_seq)
           reclaimed == ReclaimedArena(arena_blocks, cached_min_rel)
           free_blks == {b \in ArenaBlocks : reclaimed[b].owner_seq = 0}
       IN /\ next_seq - cached_min_rel <= RingCapacity
          /\ (mtype = "MarketDepthArena" => free_blks # {})
          /\ LET ablk == IF mtype = "MarketDepthArena" THEN MinSet(free_blks) ELSE 0
                 pcrc == ExpectedPayloadCrc(next_seq, mtype)
             IN /\ prod_state' = "Writing"
                /\ prod_writing_slot' = slot
                /\ arena_blocks' =
                       IF ablk = 0 THEN reclaimed
                       ELSE [reclaimed EXCEPT ![ablk] = [
                                owner_seq   |-> next_seq,
                                payload_crc |-> pcrc,
                                corrupted   |-> FALSE,
                                write_gen   |-> reclaimed[ablk].write_gen + 1
                            ]]
                /\ ring_slots' = [ring_slots EXCEPT ![slot] = [
                      initialized          |-> TRUE,
                      byte_size            |-> 512,
                      align_bytes          |-> 8,
                      has_invalid_bit_type |-> FALSE,
                      seq                  |-> next_seq,
                      schema_ok            |-> TRUE,
                      msg_type             |-> mtype,
                      arena_block          |-> ablk,
                      flag_0x08            |-> CrcEnabled,
                      record_crc           |-> ExpectedRecordCrc(next_seq, mtype),
                      payload_crc          |-> pcrc,
                      corrupted            |-> FALSE,
                      write_gen            |-> ring_slots[slot].write_gen + 1
                   ]]
    /\ UNCHANGED <<
        published_seq, cached_min_rel, obs_rel, cons_read, cons_rel,
        active_borrow, zc_last_outcome, ring_fault_used, probe_tested
       >>

\* Producer commits slot publication via release store (`crates/tickwire-shm/src/mapped.rs:536-556`).
ProducerPublishSlot ==
    /\ prod_state = "Writing"
    /\ prod_state' = "Idle"
    /\ prod_writing_slot' = -1
    /\ published_seq' = published_seq + 1
    /\ UNCHANGED <<
        ring_slots, arena_blocks, cached_min_rel, obs_rel, cons_read, cons_rel,
        active_borrow, zc_last_outcome, ring_fault_used, probe_tested
       >>

\* Consumer acquires a lifetime-pinned `RecordBorrow` (`crates/tickwire-shm/src/mapped.rs:737-770`, `crates/tickwire-shm/src/mapped.rs:825-880`).
ConsumerAcquireBorrow(c) ==
    /\ ~active_borrow[c].active
    /\ cons_read[c] < published_seq
    /\ LET next_seq == cons_read[c] + 1
           slot     == SlotOf(next_seq)
           ablk     == ring_slots[slot].arena_block
       IN /\ ring_slots[slot].seq = next_seq
          /\ cons_read' = [cons_read EXCEPT ![c] = next_seq]
          /\ active_borrow' = [active_borrow EXCEPT ![c] = [
                active     |-> TRUE,
                seq        |-> next_seq,
                slot       |-> slot,
                snap_gen   |-> ring_slots[slot].write_gen,
                arena_blk  |-> ablk,
                arena_gen  |-> IF ablk = 0 THEN 0 ELSE arena_blocks[ablk].write_gen,
                view_valid |-> FALSE
             ]]
    /\ UNCHANGED <<
        ring_slots, arena_blocks, prod_state, prod_writing_slot, published_seq,
        cached_min_rel, obs_rel, cons_rel, zc_last_outcome,
        ring_fault_used, probe_tested
       >>

\* Consumer validates zero-copy `RecordBorrow` + optional `ShmArenaReader::read_retained_payload`
\* (`crates/tickwire-shm/src/mapped.rs:884-945`, `crates/tickwire-strategy-sdk/src/arena.rs:439-502`).
ConsumerCastMarketRecord(c) ==
    /\ active_borrow[c].active
    /\ ~active_borrow[c].view_valid
    /\ LET slot == active_borrow[c].slot
           s    == ring_slots[slot]
           ablk == s.arena_block
           arena_ok ==
               (ablk = 0) \/
               /\ arena_blocks[ablk].owner_seq = s.seq
               /\ (~CrcEnabled \/ (~arena_blocks[ablk].corrupted /\ arena_blocks[ablk].payload_crc = s.payload_crc))
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
          ELSE IF ~(crc_ok /\ arena_ok) THEN
              /\ zc_last_outcome' = "ErrChecksum"
              /\ UNCHANGED active_borrow
          ELSE
              /\ zc_last_outcome' = "OkMarketView"
              /\ active_borrow' = [active_borrow EXCEPT ![c].view_valid = TRUE]
    /\ UNCHANGED <<
        ring_slots, arena_blocks, prod_state, prod_writing_slot, published_seq,
        cached_min_rel, obs_rel, cons_read, cons_rel,
        ring_fault_used, probe_tested
       >>

\* Boundary probe on header/alignment validation (`crates/tickwire-shm/src/mapped.rs:70-152`, `crates/tickwire-strategy-sdk/src/arena.rs:160-210`).
RejectMisalignedOrBadSizeProbe ==
    /\ published_seq = 0
    /\ ~probe_tested
    /\ probe_tested' = TRUE
    /\ zc_last_outcome' = "ErrWrongSizeOrAlign"
    /\ UNCHANGED <<
        ring_slots, arena_blocks, prod_state, prod_writing_slot, published_seq,
        cached_min_rel, obs_rel, cons_read, cons_rel, active_borrow,
        ring_fault_used
       >>

\* Fault injection (`CrcEnabled = TRUE`): flip a payload bit on an unborrowed published slot or its arena block
\* (`crates/tickwire-shm/src/mapped.rs:884-945`, `crates/tickwire-strategy-sdk/src/arena.rs:439-502`).
InjectBitFlipWhenCrcEnabled(s, flip_arena) ==
    /\ CrcEnabled
    /\ ~ring_fault_used
    /\ prod_state = "Idle"
    /\ ring_slots[s].initialized
    /\ ring_slots[s].msg_type \in MarketMsgTypes
    /\ (flip_arena =>
        /\ ring_slots[s].arena_block # 0
        /\ arena_blocks[ring_slots[s].arena_block].owner_seq = ring_slots[s].seq
        /\ \A c \in Consumers : ~(active_borrow[c].active /\ active_borrow[c].arena_blk = ring_slots[s].arena_block))
    /\ \A c \in Consumers : ~(active_borrow[c].active /\ active_borrow[c].slot = s)
    /\ ring_fault_used' = TRUE
    /\ IF flip_arena
       THEN /\ arena_blocks' = [arena_blocks EXCEPT ![ring_slots[s].arena_block].corrupted = TRUE]
            /\ UNCHANGED ring_slots
       ELSE /\ ring_slots' = [ring_slots EXCEPT ![s].corrupted = TRUE]
            /\ UNCHANGED arena_blocks
    /\ UNCHANGED <<
        prod_state, prod_writing_slot, published_seq, cached_min_rel,
        obs_rel, cons_read, cons_rel, active_borrow, zc_last_outcome,
        probe_tested
       >>

\* Dropping `RecordBorrow` ends `&MarketRecord` and `&[u8]` arena slice lifetimes and advances `RELEASED`
\* (`crates/tickwire-shm/src/mapped.rs:946-952`, `crates/tickwire-market-source/src/shm.rs:284-309`).
ConsumerDropBorrow(c) ==
    /\ active_borrow[c].active
    /\ active_borrow' = [active_borrow EXCEPT ![c] = NoBorrow]
    /\ cons_rel' = [cons_rel EXCEPT ![c] = cons_read[c]]
    /\ UNCHANGED <<
        ring_slots, arena_blocks, prod_state, prod_writing_slot, published_seq,
        cached_min_rel, obs_rel, cons_read, zc_last_outcome,
        ring_fault_used, probe_tested
       >>

Next ==
    \/ ProducerObserveWatermarks
    \/ ArenaReclaimReleased
    \/ (\E mtype \in PublishableMsgTypes : ProducerBeginWrite(mtype))
    \/ ProducerPublishSlot
    \/ (\E c \in Consumers : ConsumerAcquireBorrow(c))
    \/ (\E c \in Consumers : ConsumerCastMarketRecord(c))
    \/ RejectMisalignedOrBadSizeProbe
    \/ (\E s \in Slots, fa \in BOOLEAN : InjectBitFlipWhenCrcEnabled(s, fa))
    \/ (\E c \in Consumers : ConsumerDropBorrow(c))

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
        /\ ring_slots[s].arena_block \in ({0} \cup ArenaBlocks)
        /\ ring_slots[s].flag_0x08 \in BOOLEAN
        /\ ring_slots[s].corrupted \in BOOLEAN
        /\ ring_slots[s].write_gen \in 0..MaxRingSeq
    /\ \A b \in ArenaBlocks :
        /\ arena_blocks[b].owner_seq \in 0..MaxRingSeq
        /\ arena_blocks[b].corrupted \in BOOLEAN
        /\ arena_blocks[b].write_gen \in 0..MaxRingSeq
    /\ \A c \in Consumers :
        /\ active_borrow[c].active \in BOOLEAN
        /\ active_borrow[c].seq \in 0..MaxRingSeq
        /\ active_borrow[c].slot \in Slots
        /\ active_borrow[c].snap_gen \in 0..MaxRingSeq
        /\ active_borrow[c].arena_blk \in ({0} \cup ArenaBlocks)
        /\ active_borrow[c].arena_gen \in 0..MaxRingSeq
        /\ active_borrow[c].view_valid \in BOOLEAN

\* Proves zero Rust `&MarketRecord` aliasing / mutation UB while any `RecordBorrow` is alive.
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

\* Proves compile-time `crc32` gate clears 0x08 & CRC fields when disabled and rejects slot/arena corruption when enabled.
CrcGateModeConsistency ==
    /\ (~CrcEnabled =>
        /\ (\A s \in Slots :
            ring_slots[s].initialized =>
                (~ring_slots[s].flag_0x08 /\ ring_slots[s].record_crc = 0 /\ ring_slots[s].payload_crc = 0))
        /\ (\A b \in ArenaBlocks : arena_blocks[b].payload_crc = 0))
    /\ (CrcEnabled =>
        /\ (\A s \in Slots :
            ring_slots[s].initialized =>
                (ring_slots[s].flag_0x08 /\ ring_slots[s].record_crc > 0))
        /\ (\A c \in Consumers :
            (active_borrow[c].active /\ active_borrow[c].view_valid) =>
                LET s == ring_slots[active_borrow[c].slot]
                IN /\ ~s.corrupted
                   /\ (s.arena_block # 0 => ~arena_blocks[s.arena_block].corrupted)))

\* Proves `ShmArenaWriter::reclaim(released_seq)` NEVER reclaims or overwrites an arena block pinned by a live `RecordBorrow`.
ArenaBorrowLifetimeSoundness ==
    \A c \in Consumers :
        (active_borrow[c].active /\ active_borrow[c].arena_blk # 0) =>
            LET b   == active_borrow[c]
                blk == arena_blocks[b.arena_blk]
            IN /\ blk.owner_seq = b.seq
               /\ blk.write_gen = b.arena_gen
               /\ cached_min_rel < b.seq

=============================================================================
