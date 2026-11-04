# IPC wire codec

The ABI 1.0 codec reads stable byte slices for control frames, ring records, resource headers and snapshots. Generated Rust types implement exact little-endian field offsets. Decode into a typed value, then apply the semantic check for its context. An ordinary page read is unsafe while another process writes the producer or cursor page. Use the SHM access protocol for those pages. [Wire contract](../contracts/ipc-wire-v1.json) · [Codec](../../crates/tickwire-ipc/src/lib.rs)

## Record and resource checks

| Input | Required validation |
| --- | --- |
| Fixed layout | `Wire` checks exact length and zero reserved bytes. The encoder writes zero padding. Message type selects one length, field whitelist and set of unused union fields. |
| Ring record | `decode_record` checks header, flags, permitted fields, kind, value-validity masks, CRC when enabled and sequence below `2^63`. Absent time fields and disabled CRC must be zero. |
| Depth payload | Prices are strictly descending for bids and ascending for asks. Quantities are nonnegative, and the payload length matches the counts. Up to 12 levels fit inline. Larger payloads require a matching resource, ring, sequence and the smallest fitting arena class. Bounds and unused tail bytes are checked. |
| Exact event | Trades, BBO and mark/funding use types 264–266 and feature bit 9. A BBO (265) has matching first/last venue sequence, positive bid, strictly higher ask, nonnegative sizes, and a source ID and epoch. Bybit and OKX publishers require two positive sizes and mark missing/crossed snapshots unavailable. No change to the wire format is required for a second venue. |
| Control frame | `ControlFrame::decode` checks the fixed header, complete packet length, opcode/flags/status, FD count and both CRCs. RESOURCE_OFFER has 1–16 descriptors of 128 bytes with unique, in-range FD indexes. QUIESCE/SWITCH replies must match their typed body. |
| Config and grant | Negotiate features before interpreting the body. Verify roles, boot, revision/hash and receipt evidence against caller-supplied identities. A valid execution lease cannot extend an expired grant. |
| Snapshot bundle | Check exact registry coverage, contiguous page-aligned objects, member digest and common snapshot sequence, feed and catalog. Validate normalized state records and levels and require FeedState. |

CRC32C uses the Castagnoli polynomial. SHA-256 binds immutable objects. FD access, seals, SCM_RIGHTS, Unix peer identity, acquire/release ordering, arena reuse and risk authorization are enforced by their respective owners. The codec has no runtime JSON dependency and performs no signature verification. [Exact records](../../crates/tickwire-ipc/src/market_exact.rs) · [Snapshot validation](../../crates/tickwire-ipc/src/snapshot.rs)

## Replay and observation control

Replay request/reply opcodes 41/42 use 256-byte bodies. Kinds 1–5 retain their original success semantics. Kinds 6/7 arm and commit. Kinds 8–10 also support explicit refusal, echoing the request with flags 18 and status 1–3 (Unsupported, Busy, Invalid). Success uses flags 10.

* `CaptureSeal` (8) binds capture ID, revision 1, account prefix, contract digest, object digest and challenge. The worker and host verify the actual report session, PID, clock, cursor and durable WindowSeal.
* `RebuildInstall` (9) binds a schema-4 operation and revision, warm-input row count and sealed window digest. The SDK verifies the installation and its retained input chain.
* `RebuildRollback` (10) sets expected sequence to zero, binds the original installation digest, and requires a separate probe result and complete recovery object list.

Checkpoint requests/replies use 96/256 bytes. With `seal_mode=2` (ObserveCurrent), the requested account watermark is a lower bound. The reply echoes it and the internal READY record carries the observed position. Modes 0/1 require equality. `validate_checkpoint_reply` checks the value relationships, authenticated callers must also check the actual session, deadline and retained prefix. [Observation contract](runtime-observation.md)

## Regenerate and verify

```sh
python3 crates/tickwire-ipc/tools/generate.py wire
python3 crates/tickwire-ipc/tools/generate.py tests
cargo test --locked -p tickwire-ipc
cargo clippy --locked -p tickwire-ipc --all-targets -- -D warnings
```

The generator embeds the machine contract digest in Rust output. The repository has 46 generated layout tests and 94 independent golden-vector tests. Separate semantic tests exercise CRC corruption, truncation, unused fields, layout extensions, replay control and exact-value records. Byte fixtures check compatibility even when the encoder and decoder would make the same mistake. [Golden vectors](../contracts/ipc-vectors-v1.json) · [Test sources](../../crates/tickwire-ipc/tests)
