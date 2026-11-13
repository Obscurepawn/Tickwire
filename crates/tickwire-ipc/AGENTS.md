# tickwire-ipc

`tickwire-ipc` (`#![forbid(unsafe_code)]`) defines the fixed-layout little-endian binary wire ABI (`Wire` trait,
`Record`, control envelopes, checkpoints, snapshots, and `ResourceHeader`) over owned or borrowed byte slices. Live
shared-memory mapping, atomic publication barriers, and descriptor passing belong in `tickwire-shm-sys`, `tickwire-shm`,
and `tickwire-control-sys`.

## Invariants and Change Guidelines

* **Codegen and contract parity**: [src/wire.rs](src/wire.rs), [tests/layouts.rs](tests/layouts.rs), and
  [tests/vectors.rs](tests/vectors.rs) are generated from [ipc-wire-v1.json](../../specs/contracts/ipc-wire-v1.json) and
  [ipc-vectors-v1.json](../../specs/contracts/ipc-vectors-v1.json) via [tools/generate.py](tools/generate.py) (recording
  `WIRE_CONTRACT_SHA256`). Never hand-edit field offsets in [src/wire.rs](src/wire.rs); update the JSON contract,
  regenerate, and run the independent Python verifier.
* **Strict reserved-byte, sequence, and ABI checks**: Every `Wire::decode` implementation must enforce exact
  `Self::SIZE` (`Error::Bounds`), zero-filled padding (`zero(...)` -> `Error::ReservedNonzero`), `schema_major == 1` /
  `schema_minor == 0`, and sequence numbers `< (1 << 63)` (`Error::SequenceExhausted`).
* **Checksum and semantic validators**: Keep Castagnoli `crc32c` (`0x82f63b78`) masked over embedded checksum fields
  (`crc_masked`) in [src/lib.rs](src/lib.rs), and maintain semantic invariants for `Record` variants, arena blocks
  (`validate_arena_block`), config/release records, checkpoints, and snapshots across [src/config.rs](src/config.rs),
  [src/checkpoint.rs](src/checkpoint.rs), [src/market_exact.rs](src/market_exact.rs), and
  [src/snapshot.rs](src/snapshot.rs).
* **Stack-allocated decode path**: Preserve fixed-size array layouts (`#[allow(clippy::large_enum_variant)]` on
  `Record`) so `decode_record` performs zero heap allocations on hot-path ring payloads.

## Verification and Specifications

* **Source and tools**: [src/lib.rs](src/lib.rs) · [src/wire.rs](src/wire.rs) · [src/config.rs](src/config.rs) ·
  [src/checkpoint.rs](src/checkpoint.rs) · [src/market_exact.rs](src/market_exact.rs) ·
  [src/snapshot.rs](src/snapshot.rs) · [tools/generate.py](tools/generate.py) ·
  [tools/exact_event_fixtures.py](tools/exact_event_fixtures.py)
* **Integration tests**: [tests/layouts.rs](tests/layouts.rs) · [tests/layout_extensions.rs](tests/layout_extensions.rs)
  · [tests/market_exact.rs](tests/market_exact.rs) · [tests/release_layouts.rs](tests/release_layouts.rs) ·
  [tests/replay_control.rs](tests/replay_control.rs) · [tests/semantics.rs](tests/semantics.rs) ·
  [tests/vectors.rs](tests/vectors.rs)
* **Specifications and contracts**: [ipc-codec.md](../../specs/implementation/ipc-codec.md) ·
  [ipc-wire-v1.json](../../specs/contracts/ipc-wire-v1.json) ·
  [ipc-vectors-v1.json](../../specs/contracts/ipc-vectors-v1.json)

```sh
cargo test --locked -p tickwire-ipc --all-targets
python3 scripts/verify-ipc-contract.py
```
