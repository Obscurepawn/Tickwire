# tickwire-ipc

`tickwire-ipc` defines the fixed-layout little-endian binary wire ABI (`Wire` trait, `Record`, `#[repr(C)]` zero-copy
market, inline intent, and inline ORS report views, DOP logon control frames, checkpoints, snapshots, and
`ResourceHeader`) over owned or borrowed byte slices. Crate-level `[lints.rust] unsafe_code = "deny"` and
`#[forbid(unsafe_code)]` on every general codec module keep all non-audited code safe, isolating `#[allow(unsafe_code)]`
+ `#![deny(unsafe_op_in_unsafe_fn)]` strictly to [src/crc32c.rs](src/crc32c.rs) and [src/zerocopy.rs](src/zerocopy.rs).
Live shared-memory mapping, atomic publication barriers, and descriptor passing belong in `tickwire-shm-sys`,
`tickwire-shm`, and `tickwire-control-sys`.

## Invariants and Change Guidelines

* **Codegen, `#[repr(C)]` layouts, and contract parity**: [src/wire.rs](src/wire.rs), [tests/layouts.rs](tests/layouts.rs),
  and [tests/vectors.rs](tests/vectors.rs) are generated from [ipc-wire-v1.json](../../specs/contracts/ipc-wire-v1.json)
  and [ipc-vectors-v1.json](../../specs/contracts/ipc-vectors-v1.json) via [tools/generate.py](tools/generate.py)
  (recording `WIRE_CONTRACT_SHA256`). Zero-copy SHM ring and DOP logon structures (`RecordHeader`, `DepthLevel`,
  `MarketBody`, `MarketRecord`, `DecimalWire`, `MarketExactBody`, `MarketExactRecord`, `TriggerRef`, `IntentBody`,
  `IntentRecord`, `ReportBody`, `ReportRecord`, `DopLoginBody`, `DopLogoutBody`, `DopEchoBody`, `MktSession`,
  `OrsSessionsBody`, and `DopHeartBeatBody`) carry `#[repr(C)]`, explicit `_pad_*` reserved fields, and compile-time
  `target_endian = "little"`, `size_of`, `align_of`, and per-field `core::mem::offset_of!` assertions with zero `bool`
  or `enum` fields. Never hand-edit [src/wire.rs](src/wire.rs); update the generator or JSON contract, regenerate, and
  run `scripts/verify-ipc-contract.py`.
* **Pure in-memory inline ORS IPC & optional SHM Arena payloads**: `IntentRecord` (`1,024` bytes, messages `513..=525`)
  and `ReportRecord` (`512` bytes, messages `769..=781`) carry unified ORS commands (`strat_id`, `account_ref`,
  `msg_seqnum`, `order_seqnum`, `tick_mono_ns`, `intent_flags`, `time_in_force` including `RPI`, NewOrder / Cancel /
  Modify / CancelAll / RFQ fields) and execution reports (`Ack`, `FrontAck`, `Nack`, `CancelNack`, `Fill` with
  `amendment` fee/rebate updates, `Canceled`, `Modified`, `ModifyRejected`, `ExchRateLimit`, `OrsShutDown`,
  `OrsRestricted`, `RfqQuoteAck`, `RfqQuoteNack`, compact `TerminalReason` `0..=30`, `orig_seqnum`) directly inside the
  SHM ring slot with zero `FileBlobStore` or disk I/O. Oversize payloads bind an in-memory SHM Arena block
  (`payload_kind == 2`, `arena_offset == header.payload_offset >= 128`, `arena_len == header.payload_length`) validated
  by `validate_intent_arena_payload` and `validate_report_arena_payload`.
* **DOP logon and heartbeat control frames (`96..=100`)**: `ControlFrame::decode` validates `DopLogin` (`96`),
  `DopLogout` (`97`), `DopEcho` (`98`, enforcing `ready_to_trade == 1` only when `connected`, `enabled`, and
  `market_connected` are all `1`), `OrsSessions` (`99`, up to `32` `MktSession` routing rows), and `DopHeartBeat`
  (`100`); `ControlFrame::decode_negotiated` gates opcodes `96..=100` on `DOP_PROTOCOL_FEATURE_MASK` (`1 << 10`).
* **Audited zero-copy views (`ZeroCopyRecord`, `ZeroCopyIntentRecord`)**: [src/zerocopy.rs](src/zerocopy.rs) seals
  `ZeroCopyRecord` (`MarketRecord`, `MarketExactRecord`, `ReportRecord` — 512 bytes) and `ZeroCopyIntentRecord`
  (`IntentRecord` — 1,024 bytes) plus `MarketStateRef`. Callers must supply an 8-byte-aligned, immutable slot slice
  (`Error::Unaligned` otherwise); all header ABI, sequence, field-whitelist, reserved-padding, and semantic checks run
  before returning `&Self`.
* **Hardware CRC32C and optional `crc32` feature (`default = []`)**: [src/crc32c.rs](src/crc32c.rs) implements Castagnoli
  `crc32c`, `crc32c_masked`, `crc32c_update`, and `crc32c_slicing8` (`const TABLES: [[u32; 256]; 8]`) using `core::arch`
  hardware intrinsics (`x86_64` SSE4.2 `_mm_crc32_u64`/`_mm_crc32_u8`, `aarch64` `__crc32cd`/`__crc32cb`) with runtime
  detection and zero bit-by-bit loops. Pure `crc32c*` functions remain unconditionally available, while wire checksum
  verification in [src/lib.rs](src/lib.rs) is gated behind `--features crc32` (default OFF skips recomputation while
  still enforcing zero CRC fields when flag `0x08` is clear).
* **Strict reserved-byte, sequence, and stack-allocated decode checks**: Every `Wire::decode` implementation enforces
  exact `Self::SIZE` (`Error::Bounds`), zero-filled `_pad_*` / reserved slices (`Error::ReservedNonzero`),
  `schema_major == 1` / `schema_minor == 0`, sequence numbers `< (1 << 63)` (`Error::SequenceExhausted`), and zero heap
  allocations on hot-path ring payloads.

## Verification and Specifications

* **Source, benches, and tools**: [src/lib.rs](src/lib.rs) · [src/crc32c.rs](src/crc32c.rs) ·
  [src/zerocopy.rs](src/zerocopy.rs) · [src/wire.rs](src/wire.rs) · [src/config.rs](src/config.rs) ·
  [src/checkpoint.rs](src/checkpoint.rs) · [src/market_exact.rs](src/market_exact.rs) ·
  [src/snapshot.rs](src/snapshot.rs) · [benches/crc32c_and_zerocopy.rs](benches/crc32c_and_zerocopy.rs) ·
  [tools/generate.py](tools/generate.py) · [tools/exact_event_fixtures.py](tools/exact_event_fixtures.py)
* **Integration tests**: [tests/layouts.rs](tests/layouts.rs) · [tests/layout_extensions.rs](tests/layout_extensions.rs)
  · [tests/market_exact.rs](tests/market_exact.rs) · [tests/release_layouts.rs](tests/release_layouts.rs) ·
  [tests/replay_control.rs](tests/replay_control.rs) · [tests/semantics.rs](tests/semantics.rs) ·
  [tests/vectors.rs](tests/vectors.rs)
* **Specifications and contracts**: [ipc-codec.md](../../specs/implementation/ipc-codec.md) ·
  [ipc-wire-v1.json](../../specs/contracts/ipc-wire-v1.json) ·
  [ipc-vectors-v1.json](../../specs/contracts/ipc-vectors-v1.json)

```sh
cargo test --locked -p tickwire-ipc --all-targets
cargo test --locked -p tickwire-ipc --features crc32 --all-targets
python3 scripts/verify-ipc-contract.py
```
