# Modeling reasoning — `L0_ZeroCopyAndCrcGate`

## Scope and state owners
1. **SPMC `broadcast_ring` & `RecordBorrow` lifetime pinning (`crates/tickwire-shm/src/mapped.rs:447-615`, `crates/tickwire-shm/src/mapped.rs:737-952`)**:
   - Single producer owns `published_seq`, `cached_min_rel`, two-step physical slot mutation (`prod_state = "Writing"` on `prod_writing_slot` followed by `ProducerPublishSlot`), and conservative watermark observation (`obs_rel[c]`).
   - Independent consumers `c \in Consumers` (`crates/tickwire-market-source/src/shm.rs:283-309`, `crates/tickwire-strategy-sdk/src/worker.rs:701-737`) own `cons_read[c]`, active `RecordBorrow` guard (`active_borrow[c]`), and contiguous `RELEASED` cursor (`cons_rel[c]`), which advances only upon `ConsumerDropBorrow(c)` (`Drop for RecordBorrow`).
   - With `RingCapacity = 2` and `MaxRingSeq = 3`, sequence `3` wraps around onto slot `(3 - 1) % 2 = 0` (previously occupied by sequence `1`), forcing full producer wraparound contention against lagging consumers holding live `&MarketRecord` borrows.
2. **`#[repr(C)]` `MarketRecord` layout & compile-time `crc32` gate (`crates/tickwire-ipc/src/zerocopy.rs:46-286`, `crates/tickwire-ipc/src/wire.rs:4103-4215`, `crates/tickwire-ipc/tools/generate.py:154-245`)**:
   - Models 512-byte (`128`-byte `RecordHeader` + `384`-byte `MarketBody`), 8-byte-aligned slots with pure primitive integer/byte fields (`has_invalid_bit_type = FALSE`, zero `bool`/`enum`), message-type discrimination (`MarketBbo`, `MarketDepthInline`, vs. 512-byte non-market `ReportNonMarket`), misaligned/wrong-size buffer cast probes (`Error::Unaligned` / `Error::Length`), and compile-time `CrcEnabled \in BOOLEAN` controlling `RecordHeader` bit `0x08` (`CHECKSUM_PRESENT`), `record_crc32c`, and `payload_crc32c`.
3. **Physical WAL batch integrity & recovery (`crates/tickwire-journal/src/frame.rs:75-193`, `crates/tickwire-journal/src/journal.rs:240-398`)**:
   - Models 3-frame WAL batches (`BatchBegin`, `DataRecord`, `BatchSeal`) with `header_crc32c` and `payload_crc32c` gated by `CrcEnabled`, alongside unconditional SHA-256 `prev_hash -> frame_hash` chaining over `(header[0..136], payload, repeated_total_len, END_MAGIC)`.
   - Injects both crash-induced unsealed torn tails and offline storage bit-flips inside sealed batches, verifying that `Journal::recover` deterministically truncates torn tails while quarantining any sealed-batch bit flip even when `CrcEnabled = FALSE` (where `BatchSeal` `payload_len == 0` + 8-byte `total` padding bounds prevent bit-flipped `BatchSeal` headers from masquerading as torn tails).

## Configurations and invariants
- **`L0_ZeroCopyAndCrcGate.cfg`**: `Consumers = {1, 2}`, `RingCapacity = 2`, `MaxRingSeq = 3`, `MaxWalBatches = 2`, `CrcEnabled = FALSE` (default ultra-low-latency build).
- **`L0_ZeroCopyAndCrcGate_CrcOn.cfg`**: `Consumers = {1, 2}`, `RingCapacity = 2`, `MaxRingSeq = 3`, `MaxWalBatches = 2`, `CrcEnabled = TRUE` (`--features crc32` strict checksum build).
- **Invariants**: `TypeOK`, `ZeroNoAliasWhileBorrowed`, `ZeroCopyLayoutAndHeaderSoundness`, `CrcGateModeConsistency`, `WalRecoverySoundWithoutCrc`.
