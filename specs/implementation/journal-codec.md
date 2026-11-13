# Scalar Business Journal Codec and Logical Hash Chain

`tickwire-journal-codec` defines the deterministic CBOR wire representation (`SCHEMA_VERSION = 2`) and cryptographic hash chain (`LogicalChain`) for scalar execution events: `PreparedIntent`, `Fill`, `Fee`, `Terminal`, and `AbortedBeforeSend`. Every canonical record fits in at most `MAX_ENCODED_EVENT_BYTES = 236` bytes (superseding the retired schema-1 format, whose 201-byte sample vectors are retained strictly as version-rejection fixtures).

Each record decouples the physical append envelope (`journal_seq` and current `writer_exec_epoch`) from the originating order's immutable `OrderIdentity` (`intent_id`, `intent_seq`, and `intent_exec_epoch`). A successor writer taking over after failover can therefore record a `Fill`, `Fee`, `Terminal`, or `AbortedBeforeSend` against an earlier writer's order without altering that event's economic identity. The codec owns canonical byte serialization and hash-chain continuity; `tickwire-journal` owns physical framing and disk synchronization, while `tickwire-business-journal` and the execution reducer enforce order-state legality, duplicate fill/fee suppression, and live send authority.

## State Model and Bounds

Wire encoding uses definite-length CBOR maps with strictly ascending integer keys, fixed enum tags, and big-endian shortest-form integers, remaining completely independent of Rust struct layout. Negative integers compute their unsigned magnitude via bitwise complement (`!value`) so `i128::MIN` round-trips without negation overflow; values requiring 9 to 16 magnitude bytes use CBOR bignum tags `2` (positive) and `3` (negative) with no leading zero bytes and strict signed-`i128` range checks.

| Construct | Canonical Structure / Bound |
| --- | --- |
| Top-level `Record` (`<= 236` bytes) | 7-entry CBOR map (`keys 0..=6`): `schema_version = 2`, `risk_domain_id` (`[u8; 16]`), `journal_epoch`, `journal_seq`, `writer_exec_epoch`, event `kind` (`1..=5`), and nested event map |
| Event `kind` tags (`1..=5`) | `1` `PreparedIntent` (15 keys, incl. `unit_budget: i128`, `fee_budget: i128`, `recovery_object`, `commit_profile`, `command_digest`), `2` `Fill` (9 keys), `3` `Fee` (8 keys), `4` `Terminal` (8 keys: `1` Filled, `2` Canceled, `3` Rejected, `4` Expired), `5` `AbortedBeforeSend` (6 keys) |
| `business_digest` (`SHA-256`) | Hashes `["tickwire.business-event", 2, risk_domain_id, kind, payload]`; invariant under changes to `journal_seq` or `writer_exec_epoch` |
| `logical_payload_hash` (`SHA-256`) | Hashes `["tickwire.logical-payload", 2, writer_exec_epoch, business_digest]`; binds the active writer epoch to the economic event |
| `LogicalChain` (`SHA-256`) | Genesis hashes `["tickwire.logical-genesis", 2, domain, journal_epoch]`; each step hashes `prev_hash \|\| CBOR(["tickwire.logical-record", 2, domain, journal_epoch, journal_seq, kind, logical_payload_hash])` |

## Core Invariants

* **Zero-trust, allocation-safe canonical decoding**: Decoding rejects inputs exceeding `MAX_ENCODED_EVENT_BYTES` (`236` bytes) up front, never pre-allocates buffers from untrusted CBOR length prefixes, requires exact ascending map keys (`0..n`) with no unknown, duplicate, or reordered fields, forbids indefinite lengths and non-minimal integer encodings, and rejects trailing bytes. Any byte sequence accepted by `decode` reproduces the exact input bytes when passed back through `encode`.
* **Position-invariant economic digest**: `business_digest` commits to `risk_domain_id`, the originating `OrderIdentity` (including `intent_exec_epoch`), event `kind`, and every economic or authorization field—such as `quantity`, `unit_budget`, `fee_budget`, `strategy_generation`, `config_revision`, `deadline`, `recovery_object`, `commit_profile`, and `command_digest` on `PreparedIntent`. Mutating any economic or policy parameter alters `business_digest`, whereas recording or re-verifying the same economic fact at a different `journal_seq` or under a newer `writer_exec_epoch` leaves `business_digest` unchanged.
* **Layered logical hash chain**: `LogicalChain` binds stream genesis, continuous `journal_seq` (`1, 2, ...` with `u64::MAX` exhaustion checks), non-decreasing `writer_exec_epoch`, event `kind`, and `logical_payload_hash` while intentionally excluding physical WAL segment numbers, byte offsets, batch boundaries, and LSNs. Physical layout compaction or segment sizing changes therefore never invalidate logical event hashes, while any dropped, reordered, or spliced business record breaks the chain immediately.

## Operations and Failure Semantics

* **Non-mutating chain rejection**: `LogicalChain::append` (and `verify_step`) validates `risk_domain_id == domain`, `journal_epoch == epoch`, `journal_seq == last_seq + 1` (without overflow), `writer_exec_epoch >= last_writer_epoch` (and `> 0`), and any caller-supplied expected hash *before* updating the chain cursor. Any mismatch returns an error and leaves the prior `LogicalChain` state intact.
* **Separation of chain validity from execution legality**: An economically duplicate `Fill` or `Fee` appended at `last_seq + 1` is a valid `LogicalChain` transition; deduplicating repeated venue fills or fees belongs to the deterministic execution reducer. Likewise, a valid `LogicalChain` proves neither that physical WAL bytes reached stable media nor that the process currently holds a valid lease to transmit orders.

## Verification and References

```sh
cargo test --locked -p tickwire-journal-codec
cargo clippy --locked -p tickwire-journal-codec --all-targets -- -D warnings
```

Golden vector suites verify exact CBOR encodings, `business_digest`, `logical_payload_hash`, `LogicalChain` genesis/step transitions, and `i128::MIN`/`i128::MAX` bignum boundaries, while the preserved schema-1 fixture verifies version rejection. Property and mutation tests exercise truncation, trailing bytes, key reordering/duplication, non-shortest integers, overlong payloads, and exhaustive single-byte flips (verifying round-trip byte identity on every valid mutation).

* Record definitions, CBOR encoder/decoder, and logical chain: [lib.rs](../../crates/tickwire-journal-codec/src/lib.rs), [cbor.rs](../../crates/tickwire-journal-codec/src/cbor.rs), [chain.rs](../../crates/tickwire-journal-codec/src/chain.rs)
* Golden vectors and codec tests: [golden-v2.json](../../crates/tickwire-journal-codec/tests/golden-v2.json), [golden.json](../../crates/tickwire-journal-codec/tests/golden.json), [codec.rs](../../crates/tickwire-journal-codec/tests/codec.rs)
* Related specifications: [Command binding](command-binding.md), [Physical write-ahead log](journal.md), [Scalar execution journal](business-journal.md)
