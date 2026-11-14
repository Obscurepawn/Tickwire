# Deterministic Market-Topic Snapshot Construction

`tickwire-snapshot::builder` (`build_topic` and `build_topic_with_coverage`) assembles a complete, page-aligned market-topic baseline image (`BuiltTopic`) at a single topic sequence cut `S`. After the topic owner reserves a pending `JOIN` gate at `S` under its single-writer boundary and collects borrowed `InstrumentSnapshot` state from each registered instrument, the builder validates cross-member consistency, sizes the entire bundle with checked arithmetic prior to allocation, normalizes state rows and depth ladders, computes hierarchical SHA-256 digests, and runs the receiver's full `tickwire-snapshot::validate` pass before returning any deliverable bytes.

The builder is pure cold-path construction code (`#![forbid(unsafe_code)]`): it owns deterministic binary layout, row canonicalization, and self-verification, while leaving live capture atomicity at cut `S`, pending-gate retention on the ring, Linux `memfd` `0x0f` sealing, and `SCM_RIGHTS` delivery to `JoinOwner` and `ImmutableMapping`. A failed build returns an `Error` without mutating the caller's ring, cursor, or `JOIN` registration.

## State Model and Bounds

`BuildRequest` binds the bank/snapshot `ResourceHeader` template, `JoinBody`, `catalog_hash`, `instrument_set_version`, cut `sequence` (`S`), creation/expiration/current monotonic timestamps, `logical_strategy_id`, registered `InstrumentSpec` slice, `InstrumentSnapshot` slice, and caller `byte_limit`.

| Parameter or Layout Element | Enforced Bound / Geometry |
| --- | --- |
| Aggregate image size | `0 < total_bytes <= byte_limit <= MAX_TOPIC_SNAPSHOT_BYTES` (`64 MiB`, `67,108,864` bytes); allocated once via `Vec::try_reserve_exact(total)` |
| Registered instruments | `1..=4,096` entries, strictly ascending `instrument_id`, `instrument_version` in `1..2^63`, exact 1:1 match with `request.members` (`Error::ResyncRequired` on count, ID, or version mismatch) |
| Cut sequence `S` | `0 <= S <= 2^63 - 2` (`S == 0` represents an initial unavailable baseline; `S < 2^63 - 1` guarantees catch-up `J = S + 1 < 2^63` cannot exhaust sequence space); `ResourceHeader.first_seq` of the underlying bank is preserved unchanged even when `first_seq != J` |
| Outer manifest (`0..member_start`) | `0..4096`: `ResourceHeader`; `4096..4352`: top-level `SnapshotHeader`; `4352..(4352 + 128 * N)`: `TopicSnapshotEntry` table, zero-padded to 4,096-byte `member_start` |
| Kind-6 vs. kind-7 member layout | Each member starts on a 4,096-byte boundary and is zero-padded to a 4,096-byte multiple. Kind `6` (`build_topic`, feature bit `6` unset): 256-byte `SnapshotHeader` + two 64-byte `SnapshotSection` descriptors (`384` bytes) + `512 * state_rows` + `16 * (bids + asks)`. Kind `7` (`build_topic_with_coverage`, feature bit `6` / `0x40` set): three 64-byte section descriptors (`448` bytes) + state rows + depth levels + 32-byte `DepthCoverage` (`480` fixed overhead bytes before page rounding). Legacy canonical single-instrument fixture size is `12 KiB` (`12,288` bytes). |

## Core Invariants

* **Checked pre-sizing before single allocation**: Before reserving heap memory, `build` computes `member_start` and sums every page-rounded `member_bytes` with checked arithmetic against both `request.byte_limit` and `64 MiB`, preventing unbounded transient allocations on oversized depth ladders.
* **Cross-member cut, identity, and timestamp coherence**: Every `InstrumentSnapshot` must have a nonzero `snapshot_id` distinct from `resource.resource_id` and all sibling members, `at_sequence == S`, `last_event_sequence <= S`, matching `feed_generation`, `catalog_version`, and `catalog_hash`, and `created_mono_ns <= now_mono_ns < expires_mono_ns`. Each member supplies `1..=4` distinct state categories (`257`/`265` Quote, `258`/`264` Trade, `260`/`266` MarkFunding, and mandatory `261` `FeedState` in `1..=5`), with exact-event types `>= 264` allowed only when `EXACT_MARKET_EVENTS` (feature bit `9`) is enabled. Every input state record must satisfy `record.sequence <= member.last_event_sequence`, `payload_crc32c == 0`, matching instrument/feed/topic/producer identities, and monotonic `ingress_mono_ns` / `publish_mono_ns <= member.created_mono_ns`.
* **Canonical row re-indexing, optional `crc32` gating, and honest book representation**: Output state rows are sorted deterministically by ascending `message_type`, assigned snapshot-local sequence numbers `1..=states.len()`, stripped of external payload references, normalized according to the compile-time `crc32` feature (default OFF clears flag bit `3` / `0x08` and writes `record_crc32c = 0`; `--features crc32` preserves flag `0x08` and recomputes hardware-accelerated Castagnoli CRC32C), and verified via `decode_record` while preserving original observation timestamps. Depth levels require strictly positive `price_ticks` and `qty_lots`, strictly descending bids, strictly ascending asks, and—whenever `FeedState == 2` (`Live`)—an uncrossed top of book (`best_bid < best_ask`). The builder never synthesizes placeholder levels: a Bybit or OKX BBO-only member encodes its type-`265` BBO state row alongside empty `bids`/`asks` slices and `coverage_flags == 3` (`bid_kind`/`ask_kind` non-complete in `DepthCoverage`).
* **Binding completion without silent override**: If `request.join` already populates nonzero `snapshot_id`, `snapshot_seq`, `join_seq`, `topic_instrument_set_hash`, or `topic_instrument_set_version`, the builder requires exact agreement with the derived values; zero template fields are filled with `(resource_id, S, S + 1, set_hash, instrument_set_version)`, and `snapshot_hash` is bound to the final whole-file SHA-256.

## Operations and Failure Semantics

* **Hierarchical digest sealing and mandatory receiver round-trip**: For each member, the builder writes zero-padded sections, hashes each `SnapshotSection` payload, hashes the complete page-aligned member slice into its `TopicSnapshotEntry`, hashes the top-level `SnapshotHeader` and `ResourceHeader` (`descriptor_hash` with bytes `272..304` zeroed), and computes the whole-file SHA-256 (`BuiltTopic::file_hash()`). Before returning `Ok(BuiltTopic)`, `build` invokes `tickwire_snapshot::validate(&bytes, &JoinContext { ... })` on the finished image.
* **Structural validity vs. readiness**: `build_topic` intentionally runs structural `validate` rather than `check_readiness`: a topic whose venue feed is still bootstrapping (`FeedState != 2` or missing a required category) produces a valid `BuiltTopic` that can be sealed and delivered during `JOIN`, while `check_readiness` continues to hold subscriber activation until live ring records complete the required categories.

## Verification and References

```sh
cargo test --locked -p tickwire-snapshot --test builder --test validation
cargo test --locked -p tickwire-md --test spot_quotes
```

Builder tests cover maximum 4,096-member bundles, cumulative `byte_limit` exhaustion before allocation, mismatched `JoinBody`/catalog/sequence bindings, crossed live books, kind-7 `DepthCoverage` outlier boundaries, byte-exact compatibility with independent golden fixtures, and end-to-end spot BBO round trips across snapshot baseline and subsequent ring deltas.

* Builder implementation: [builder.rs](../../crates/tickwire-snapshot/src/builder.rs)
* Test suites: [builder.rs](../../crates/tickwire-snapshot/tests/builder.rs), [validation.rs](../../crates/tickwire-snapshot/tests/validation.rs), [spot_quotes.rs](../../crates/tickwire-md/tests/spot_quotes.rs)
* Related specifications: [Validate a market-topic baseline](topic-snapshot.md), [Immutable Linux resources](immutable-resource.md), [JOIN protocol](join.md)
