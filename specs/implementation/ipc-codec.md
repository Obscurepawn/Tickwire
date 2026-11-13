# IPC Wire Codec and Semantic Record Validation

`tickwire-ipc` defines the fixed-layout ABI 1.0 binary representations and deterministic validation rules for control-plane frames, shared-memory ring records, arena depth blocks, immutable resource envelopes, and normalized market snapshots. Generated little-endian encoders and decoders operate exclusively on stable, caller-owned byte slices without runtime JSON dependencies, heap allocation on hot record decode (`Record` uses stack-allocated variants), or cryptographic signature verification.

The crate owns byte-level framing, field whitelists, Castagnoli CRC32C integrity checks, and structural payload invariants. It intentionally does *not* access live shared-memory mappings: reading a producer slot or shared cursor page directly through a normal Rust slice while another process mutates it is undefined behavior, so callers must first establish slot stability or copy bytes using the acquire/release shared-memory protocol in `tickwire-shm-sys` and `tickwire-shm`. File-descriptor permissions, `memfd` seals, `SCM_RIGHTS` transfer, `SO_PEERCRED` peer identity, arena reuse watermarks, and trading risk grants remain enforced by their respective subsystem owners.

## State Model and Bounds

All wire structures enforce ABI `1.0`, exact byte lengths, zeroed reserved padding, zeroed inactive union branches, and sequence/generation numbers strictly below `2^63` (`Error::SequenceExhausted` at `>= 2^63`). Integrity digests use Castagnoli CRC32C (reflected polynomial `0x82f63b78`, initial value and final XOR `0xffffffff`) for frames and ring records, and SHA-256 for immutable resource descriptors and snapshot bundles.

| Wire Construct | Size / Layout | Structural and Semantic Validation |
| --- | --- | --- |
| `Wire` fixed types | Generated `T::SIZE` | Exact slice length; encoder zeros all padding; decoder rejects nonzero reserved bytes (`ReservedNonzero`) and unused union fields outside the message type's field whitelist. |
| `RecordHeader` & ring records | 128-byte header; 512 bytes (`257..=266`, `769..=781`) or 1,024 bytes (`513..=520`) | Nonzero `sequence`, `topic_or_scope_id`, and `producer_generation`; `flags` restricted to bits `0..=3` (`0x0f`); absent timestamp fields (bits 0–2) and disabled CRCs (bit 3 unset) must be zero; when bit 3 (`0x08`) is set, `record_crc32c` covers the record with bytes `112..120` masked to zero; intent/report/ownership types (`>= 513`) require `feed_generation == 0`. |
| Depth updates (`259`) | Up to 4,096 total levels (`16` bytes/level) | `bid_count + ask_count == level_count <= 4,096`; quantities `>= 0`; bids strictly descending, asks strictly ascending. `<= 12` levels (`payload_kind = 1`) sit inline at bytes `320..512` with zeroed tail and optional `payload_crc32c`. `> 12` levels (`payload_kind = 2`) zero inline bytes `320..512`, bind `allocation_seq == sequence`, `payload_offset >= 128`, and select the smallest fitting `ArenaClass` (`1..=4`) whose 128-byte `ArenaBlockHeader` matches `resource_id`, `ring_epoch`, `allocation_seq`, `class_id`, `payload_length`, zeroed block tail, and `payload_crc32c`. |
| Exact market events (`264..=266`) | 512 bytes; feature bit 9 (`0x200`) | Covers trades (`264`), exact BBO (`265`), and mark/funding (`266`). Type `265` requires matching first/last venue sequence, positive bid price, strictly higher ask price, nonnegative sizes, and valid source ID/epoch; Bybit and OKX publishers require strictly positive bid and ask sizes and mark missing or crossed books unavailable without altering the wire schema. |
| `IntentRecord` (`513..=520`) & `ReportRecord` | 1,024 bytes / 512 bytes | Intents allow `0..=8` triggers (each 64 bytes with nonzero `topic_id`, `sequence < 2^63`, and `feed_generation`), `reduce_only <= 1`, and zeroed unused trigger slots. Reports validate length-prefixed `venue_order_id` and `venue_trade_id` with zeroed trailing padding. |
| `ControlFrame` | 128-byte `ControlEnvelope` (`magic = "TWCTL001"`) + body (`<= 65,536` bytes total) | `header_crc32c` (masking bytes `116..120`) and `body_crc32c` both verified; `status <= 44`; error flag (`flags == 18`) iff `status != 0`; `fd_count <= 16` and nonzero only on `RESOURCE_OFFER` (opcode `3`), where `OfferPrefix` carries `1..=16` 128-byte `ResourceDescriptor` entries with unique in-range `fd_index`, `resource_kind` in `1..=9`, `access` in `1..=2`, and `seal_profile` in `1..=2`. |
| Snapshot bundle | Up to 64 MiB; page-aligned (4,096-byte) sections | Full instrument-registry coverage, contiguous page-aligned member objects, verified SHA-256 digests, common snapshot cut sequence, matching feed/catalog bindings, normalized state rows/levels, and mandatory `FeedState` per member. |

## Core Invariants

* **Capability negotiation precedes body interpretation**: `ControlFrame::decode_negotiated` checks negotiated session feature bits before parsing extension opcodes—specifically requiring feature bit 5 (`0x20`) for configuration-control opcodes `80..=84`—so unnegotiated peers cannot reach extension decoders.
* **Lease non-extension**: Configuration and execution grant validators check roles, host boot ID, configuration revision/hash, and durable receipt bindings against caller-supplied identities, and guarantee that a valid execution lease can never extend an already expired risk grant.
* **Typed reply correlation on control transitions**: `QUIESCE` (opcode `20`) success replies (`flags == 10`) must carry `evidence_kind == 1` and a zero `exit_evidence_id`. `SWITCH` (opcode `19`) replies require zero `snapshot_id` and `snapshot_seq`, pairing `flags == 10` with `switch_kind == 2` and `flags == 18` with `status == 32` and `switch_kind == 3`.

## Operations and Failure Semantics

* **Replay control protocol (opcodes `41` / `42`)**: Replay request and reply frames carry fixed 256-byte `ReplayControlBody` payloads (`schema == 1`, `replay_kind` in `1..=10`, `object_bytes` in `1..=67,108,864`, and nonzero `instance_id`, `strategy_generation`, `logical_strategy_id`, `challenge`, `valid_until_mono_ns`, `object_digest`, `expected_cut_digest`, `operation_id`, and `operation_revision`). Kinds `1..=5` preserve legacy success-only reply rules; kinds `6..=7` arm and commit capture windows; kinds `8..=10` (`CaptureSeal`, `RebuildInstall`, `RebuildRollback`) additionally permit explicit refusal replies (`flags == 18`) with `status` in `1..=3` (`Unsupported`, `Busy`, `Invalid`), whereas acceptance uses `flags == 10`.
  * `CaptureSeal` (`8`) binds the capture ID, revision `1`, account prefix, contract digest, object digest, and challenge; both worker and host independently verify the live report session, PID, monotonic clock, shared cursor, and durable `WindowSeal`.
  * `RebuildInstall` (`9`) binds a schema-4 operation and revision, warm-input row count, and sealed window digest, which the SDK verifies alongside its retained input chain before installation.
  * `RebuildRollback` (`10`) sets expected sequence to zero, binds the original installation digest, and requires both a separate probe result and a complete recovery object set.
* **Checkpoint sealing and continuous observation**: Checkpoint seal requests (`96` bytes) and replies (`256` bytes) correlate via `validate_checkpoint_reply(request, reply, previous_ready_sequence)`, requiring `schema == 1`, nonzero 16-byte `challenge`, matching `instance_id`, `logical_strategy_id`, `strategy_generation`, `seal_mode`, and echoed `report_sequence`, plus `ready.ready_sequence > previous_ready_sequence`. In exact modes (`seal_mode` `0` or `1`), `ready.exec_report_sequence` must equal `request.report_sequence`. In `CHECKPOINT_OBSERVE_CURRENT` (`seal_mode = 2`), `request.report_sequence` acts as a floor (`ready.exec_report_sequence >= request.report_sequence`) while the reply echoes the requested floor and the embedded `READY` record reports the actual observed watermark. Authenticated callers must still verify session liveness, deadline, and retained account-prefix digests.

## Verification and References

```sh
python3 crates/tickwire-ipc/tools/generate.py wire
python3 crates/tickwire-ipc/tools/generate.py tests
cargo test --locked -p tickwire-ipc
cargo clippy --locked -p tickwire-ipc --all-targets -- -D warnings
```

The code generator embeds the SHA-256 digest of `ipc-wire-v1.json` directly into the generated Rust source so contract drift fails review and verification. Automated suites run 46 generated field-offset/size layout tests, 94 independent golden-vector tests (`ipc-vectors-v1.json`) that catch symmetric encoder/decoder bugs, and dedicated semantic suites covering CRC bit flips, truncation, nonzero reserved bytes, arena class mismatches, replay refusals, and exact-value market records.

* Crate implementation: [lib.rs](../../crates/tickwire-ipc/src/lib.rs), [market_exact.rs](../../crates/tickwire-ipc/src/market_exact.rs), [snapshot.rs](../../crates/tickwire-ipc/src/snapshot.rs), [checkpoint.rs](../../crates/tickwire-ipc/src/checkpoint.rs)
* Machine contracts and test suites: [ipc-wire-v1.json](../contracts/ipc-wire-v1.json), [ipc-vectors-v1.json](../contracts/ipc-vectors-v1.json), [crates/tickwire-ipc/tests](../../crates/tickwire-ipc/tests)
* Related specifications: [Runtime observation](runtime-observation.md), [Shared-memory ring](shm-ring.md), [Control sessions](control-session.md), [Control transport](control-transport.md)
