# Market-Topic Snapshot Validation and Readiness Gate

`tickwire-snapshot::validate` verifies an immutable market-topic baseline image (`bytes`) against an externally authenticated `JoinContext`—comprising the approved `ResourceHeader`, catalog SHA-256, strictly sorted `InstrumentSpec` registry, `JoinBody`, and current `CLOCK_MONOTONIC` nanosecond timestamp—before returning a zero-copy `ValidatedTopic<'a>` view. Separating structural validation from `check_readiness(now_mono_ns)` allows a newly connected subscriber or publisher to parse and inspect a structurally sound bootstrap baseline while still refusing to activate live trading consumption until every registered instrument and required market-state category is live and fresh.

This crate is `#![forbid(unsafe_code)]`, performs cold-path validation (allocating bounded member and snapshot-ID sets), and neither inspects OS file descriptors nor mutates rings or cursors. Callers must first verify `O_RDONLY` file access, `0x0f` `memfd` seals, and outer envelope digests via `ImmutableResource` (`tickwire-shm-sys`), and later verify catch-up cursor watermarks (`tickwire-join`) and independent trading risk grants before enabling execution.

## State Model and Bounds

A topic snapshot image embeds an outer 4,096-byte `ResourceHeader` (`event_class == 7`), a 256-byte top-level `SnapshotHeader` (`offset 4096`), `entry_count` 128-byte `TopicSnapshotEntry` descriptors (`offset 4352`), and contiguous 4,096-byte-aligned per-instrument member snapshots starting at the next page boundary.

| Structure or Parameter | Enforced Bound / Representation |
| --- | --- |
| Bundle size & membership | `bytes.len() <= 64 MiB` (`67,108,864` bytes); `1..=4,096` strictly ascending `(instrument_id, instrument_version)` specs (`instrument_version` in `1..2^63`) |
| Cut & join sequence | Common topic cut `S = join.snapshot_seq`; first ring sequence for catch-up `J = S + 1 < 2^63` |
| Required state bitmask (`required_states`) | Subset of `0x07`: `QUOTE = 1` (types `257` or `265`), `TRADE = 2` (types `258` or `264`), `MARK_FUNDING = 4` (types `260` or `266`); type `261` (`FeedState`) is mandatory in every member |
| Member `snapshot_kind` | Kind `6` (legacy normalized member: 256-byte header + two 64-byte section descriptors for state rows and depth levels) or kind `7` (adds a third 64-byte section descriptor at `+384` for `DepthCoverage` exact side boundaries) |
| Monotonic time windows | `resource.created_mono_ns <= topic.created_mono_ns`; `created_mono_ns <= now_mono_ns < expires_mono_ns` on the topic and every member; `now_mono_ns < join.readiness_deadline_mono_ns`; every present `ingress_mono_ns` / `publish_mono_ns` in a state row `<= member.header.created_mono_ns` (exchange UTC `source_timestamp_ns` lives in a separate clock domain) |

## Core Invariants

* **Cross-envelope identity pinning**: `validate` requires exact equality between the decoded outer `ResourceHeader` and `context.expected_resource`, then cross-checks `resource_id == topic.snapshot_id`, `channel_id`, `channel_epoch`, `ring_epoch`, `feed_generation`, `topic_id`, `catalog_version`, `initial_membership_version == join.membership_version`, `qos`, `topic.catalog_hash == context.catalog_hash`, `exec_generation == 0` (at both topic and member levels), matching `logical_strategy_id` (when nonzero), and `consumer_instance` equal to either `[0; 16]` or `join.subscriber_instance`. All mandatory UUIDs and generation counters must be nonzero and `< 2^63`, `required_features & 0x1c == 0x1c`, and `join_flags` must fall in `1..=3` with bit `0` set.
* **Global `snapshot_id` uniqueness and full registry coverage**: Even while individual categories are still bootstrapping from the venue, the snapshot must contain one valid normalized member for *every* registered instrument in strictly ascending order, verified with contiguous page-aligned sections, zeroed padding, and per-section/per-member SHA-256 digests (`validate_topic_bundle`). A bounded `HashSet` verifies that the top-level `snapshot_id` and every member's nonzero `entry.snapshot_id` are mutually distinct across the entire bundle.
* **Normalized state rows and venue-specific quote coverage**: Every 512-byte `MarketState` row in a member must match `resource.topic_id` and `resource.producer_generation`, and its mandatory type-`261` `FeedState` row must equal `entry.feed_state`. A BBO-only Bybit or OKX spot member satisfies `QUOTE` (`1`) using an exact type-`265` BBO row paired with an empty depth ladder and explicit unknown depth-side coverage (`coverage_flags = 3`).

## Operations and Failure Semantics

* **Zero-copy member inspection (`ValidatedTopic::member`)**: `member(index)` bounds-checks `index < entry_count` and every section offset/length with checked `u64` arithmetic before returning `Member<'a>`, which borrows `state_records: &'a [u8]` and `depth_levels: &'a [u8]` directly from the sealed image alongside decoded headers and optional `DepthCoverage`.
* **Two-stage readiness (`check_readiness`)**: During `validate`, `ValidatedTopic` computes `deadline = min(topic.expires_mono_ns, join.readiness_deadline_mono_ns, min_i(member_i.expires_mono_ns))` and sets `live_required_state = true` if and only if every member has `entry.feed_state == 2` (`Live`), every bit in `instrument.required_states` was observed among that member's state rows, and every row contributing to a required category also carries `record.feed_state() == 2`. Calling `check_readiness(now_mono_ns)` enforces `validated_at <= now_mono_ns < deadline` (`Error::Deadline` on clock regression or expiry) and `live_required_state == true` (`Error::NotReady` otherwise).

## Verification and References

```sh
cargo test --locked -p tickwire-snapshot -p tickwire-join
cargo test --locked -p tickwire-md --test spot_quotes
```

Test fixtures exercise byte-by-byte truncation boundaries, multi-instrument cuts, duplicate `snapshot_id` collisions, digest-valid identity and monotonic-window mismatches, bootstrapping/unavailable state categories, kernel-sealed `ImmutableResource` mappings on Linux, and Bybit/OKX spot BBO snapshots transitioning cleanly into live and stale ring updates.

* Receiver validation implementation: [lib.rs](../../crates/tickwire-snapshot/src/lib.rs)
* Test suites: [validation.rs](../../crates/tickwire-snapshot/tests/validation.rs), [immutable_linux.rs](../../crates/tickwire-snapshot/tests/immutable_linux.rs), [spot_quotes.rs](../../crates/tickwire-md/tests/spot_quotes.rs)
* Related specifications: [Build a market-topic baseline](topic-snapshot-builder.md), [Immutable Linux resources](immutable-resource.md), [JOIN protocol](join.md), [Shared-memory broadcast ring](shm-ring.md)
