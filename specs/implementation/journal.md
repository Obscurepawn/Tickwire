# Physical journal

`tickwire-journal` appends and recovers WAL v1 data batches. The physical receipt states what bytes reached a local synchronization point. The business layer separately validates command identity, recovery dependencies and execution authority. [Journal](../../crates/tickwire-journal/src/journal.rs) · [Frame codec](../../crates/tickwire-journal/src/frame.rs)

## Write one batch

1. Create a journal with one domain and stream. A frame has a 136-byte header, a 40-byte trailer, 8-byte alignment and a maximum total length of 1 MiB. Version 1 recognizes 12 frame types, requires zero flags and validates zero padding. Decode returns the consumed frame length.
2. A data batch has an empty Begin, 1–32 DataRecords and an empty Seal. It fits in 64 KiB and cannot span segments. The default segment size is 256 MiB. Check epoch, count, lengths and arithmetic before touching the store.
3. Write the complete batch and call `sync_data` once. Only a successful sync returns `PhysicalSyncReceipt`. An I/O failure poisons the writer. A missing receipt does not establish that the batch failed to reach stable storage.

`FileStore` holds an exclusive advisory lock. It creates Unix directories with mode 0700 and explicitly unlocks the writer even if another process briefly inherited a copy of its FD. The caller supplies a trusted parent and correct permissions on existing paths.

## Roll and recover

`MANIFEST.json` binds a digest, version, configuration, generation and active segment. To publish the next segment, synchronize its file and directory, sync a temporary manifest, rename it and sync the directory again. Segment numbers start at one and remain contiguous. Recovery accepts at most one extra `active_segment + 1`, provided the physical chain still validates. Reject an empty interior segment, missing named segment or larger gap.

Scan the entire chain and validate any independently supplied `PhysicalFrontier` **before** modifying files. A frontier binds domain, stream, segment, end offset, cumulative LSN, last sequence, epoch, batch number and Seal hash. A frontier derived from the same scan cannot detect an entirely removed suffix.

If the final batch lacks a complete frame or Seal, truncate that incomplete tail only after the trusted prefix is satisfied, then resynchronize. Keep every complete, valid sealed batch, even if its writer never received a receipt. A complete frame with invalid CRC, padding, trailer or SHA is corruption. Preserve the files for investigation.

## Verify

```sh
cargo test --locked -p tickwire-journal
cargo clippy --locked -p tickwire-journal --all-targets -- -D warnings
```

The process fixture kills a writer halfway through a batch, before sync, after sync but before its receipt, and after receipt. A historical Linux run discarded 372 bytes of an incomplete batch. The other three cuts recovered complete batches. Bytes visible in a surviving kernel page cache after SIGKILL do not prove durability across device power loss. The single-segment model covers sync, lost receipts, poison, crash, incomplete tails and independent frontiers. [Crash tests](../../crates/tickwire-journal/tests/process.rs) · [Manifest tests](../../crates/tickwire-journal/tests/manifest.rs) · [Model scope](../../tla/execution_models/physical_journal/L0_batch_sync_recovery/05_output.md)
