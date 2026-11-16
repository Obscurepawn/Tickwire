# tickwire-snapshot

`tickwire-snapshot` (`#![forbid(unsafe_code)]`) owns cold-path construction (`build_topic` and
`build_topic_with_coverage` in [src/builder.rs](src/builder.rs)) and authenticated-context validation (`validate` and
`ValidatedTopic` in [src/lib.rs](src/lib.rs)) of immutable normalized topic snapshots used when a
strategy joins a live market feed. Shared-memory `memfd` sealing belongs in `tickwire-shm-sys`, and
control-channel descriptor offers and cursor activation belong in `tickwire-join`.

## Invariants and Change Guidelines

* **Authenticated `JoinContext` binding**: `validate` in [src/lib.rs](src/lib.rs) never trusts
  self-attestation from snapshot bytes alone. Every header, `TopicSnapshotEntry`, and member
  `SnapshotHeader` must match the externally supplied `ResourceHeader`, `JoinBody` (`snapshot_id`,
  `snapshot_seq`, `snapshot_hash`, `feed_generation`, `topic_id`), `catalog_hash`, and sorted
  `InstrumentSpec` list.
* **Page-aligned, non-overlapping layout bounds**: Enforce `MAX_TOPIC_SNAPSHOT_BYTES` (64 MiB),
  checked offset arithmetic (`range`), 4096-byte page alignment between member regions in
  [src/builder.rs](src/builder.rs), zero-filled inter-section padding, and exact SHA-256
  verification of state, depth, and kind-7 `DepthCoverage` sections (`snapshot_kind` `6` or `7`).
* **Required state liveness, expiry windows, and `crc32` feature gate**: Verify
  `created_mono_ns <= now_mono_ns < expires_mono_ns` (`Error::Deadline`) during both build and
  reader validation. `ValidatedTopic::check_readiness` proves snapshot baseline validity and
  `required_states` (`QUOTE`, `TRADE`, `MARK_FUNDING`) presence, while live cursor catch-up and
  execution gating remain with the caller. Optional `--features crc32` (`default = []`) controls
  whether `builder::normalize_record` retains checksum flag `0x08` and computes `record_crc32c`
  versus clearing `0x08` and writing `record_crc32c = 0`.

## Verification and Specifications

* **Source and tests**: [src/lib.rs](src/lib.rs) · [src/builder.rs](src/builder.rs) ·
  [tests/builder.rs](tests/builder.rs) · [tests/validation.rs](tests/validation.rs) ·
  [tests/immutable_linux.rs](tests/immutable_linux.rs)
* **Specifications**: [topic-snapshot.md](../../specs/implementation/topic-snapshot.md) ·
  [topic-snapshot-builder.md](../../specs/implementation/topic-snapshot-builder.md) ·
  [immutable-resource.md](../../specs/implementation/immutable-resource.md)

```sh
cargo test --locked -p tickwire-snapshot --all-targets
./scripts/check-linux.sh
```
