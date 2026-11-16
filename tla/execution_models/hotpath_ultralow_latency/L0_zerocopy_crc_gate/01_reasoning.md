# Modeling reasoning — `L0_ZeroCopyAndCrcGate`

## Scope and state owners
1. **SPMC `broadcast_ring` & `RecordBorrow` lifetime pinning (`crates/tickwire-shm/src/lib.rs:256-332`, `crates/tickwire-shm/src/mapped.rs:455-614`, `crates/tickwire-shm/src/mapped.rs:737-952`)**:
   - Single producer owns `published_seq`, `cached_min_rel`, two-step physical slot mutation (`prod_state = "Writing"` on `prod_writing_slot` followed by `ProducerPublishSlot`), and conservative watermark observation (`obs_rel[c]`).
   - Independent consumers `c \in Consumers` (`crates/tickwire-market-source/src/shm.rs:284-309`, `crates/tickwire-strategy-sdk/src/worker.rs:243-310`, `crates/tickwire-runtime/src/ors.rs:1487-1503`) own `cons_read[c]`, active `RecordBorrow` guard (`active_borrow[c]`), and contiguous `RELEASED` cursor (`cons_rel[c]`), which advances only upon `ConsumerDropBorrow(c)` (`Drop for RecordBorrow`).
   - With `RingCapacity = 2` and `MaxRingSeq = 3`, sequence `3` wraps around onto slot `(3 - 1) % 2 = 0` (previously occupied by sequence `1`), forcing full producer wraparound contention against lagging consumers holding live zero-copy borrows.
2. **Release-watermark-reclaimed SHM retained arena (`crates/tickwire-strategy-sdk/src/arena.rs:160-210`, `crates/tickwire-strategy-sdk/src/arena.rs:241-386`, `crates/tickwire-strategy-sdk/src/arena.rs:400-502`)**:
   - Models `ShmArenaWriter::reclaim` and `write_payload` over finite `ArenaBlocks = 2`, proving no arena block attached to a slot with `active_borrow[c] > 0` is ever overwritten (`ArenaBorrowLifetimeSoundness`) and every `ShmArenaReader::read_retained_payload` verifies block-span and optional CRC integrity before exposing a slice.
3. **`#[repr(C)]` layout & compile-time `crc32` gate (`crates/tickwire-shm/src/mapped.rs:825-952`)**:
   - Models 512-byte/1024-byte, 8-byte-aligned slots with pure primitive integer/byte fields (`has_invalid_bit_type = FALSE`), message-type discrimination (`MarketBbo`, `MarketDepthInline`, vs. non-market `ReportNonMarket`), misaligned/wrong-size buffer cast probes (`ProbeInvalidZeroCopyCast`), and compile-time `CrcEnabled \in BOOLEAN` controlling `RecordHeader` bit `0x08` (`CHECKSUM_PRESENT`), `record_crc32c`, `payload_crc32c`, and bit-flip rejection (`InjectBitFlipWhenCrcEnabled`).

## Configurations and invariants
- **`L0_ZeroCopyAndCrcGate.cfg`**: `Consumers = {1, 2}`, `RingCapacity = 2`, `ArenaBlocks = 2`, `MaxRingSeq = 3`, `CrcEnabled = FALSE` (default ultra-low-latency build).
- **`L0_ZeroCopyAndCrcGate_CrcOn.cfg`**: `Consumers = {1, 2}`, `RingCapacity = 2`, `ArenaBlocks = 2`, `MaxRingSeq = 3`, `CrcEnabled = TRUE` (integrity-diagnostic build with slot and arena bit-flip injection).
- **Safety invariants**: `TypeOK`, `ZeroNoAliasWhileBorrowed`, `ZeroCopyLayoutAndHeaderSoundness`, `CrcGateModeConsistency`, `ArenaBorrowLifetimeSoundness`.
