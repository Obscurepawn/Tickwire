# Validate a market-topic baseline

`tickwire-snapshot::validate(bytes, &JoinContext)` validates an entire immutable topic image against an authenticated registry and JOIN. It returns a `ValidatedTopic` that borrows those exact bytes. It neither modifies the image nor advances a ring or cursor. The caller first checks the actual FD, access and seals, then supplies the independently approved resource header, catalog hash, instrument set and JOIN. [Receiver](../../crates/tickwire-snapshot/src/lib.rs) · [Builder](topic-snapshot-builder.md)

## Check the cut and every member

* Accept at most 64 MiB and 1–4096 registered members. Validate the 4096-byte resource header, all object hashes, contiguous page-aligned sections, padding, full strictly sorted instrument set and common cut `S`. The next ring record starts at `J = S + 1`.
* Match the entire resource header with the expected header. Check channel, topic, feed/ring generations, catalog, membership and QoS across the resource, manifest and JOIN body. A nonzero resource consumer identity must match the subscriber.
* Validate every member as a normalized market snapshot. Its snapshot ID must be nonzero, unique and different from the top-level ID. Verify each state record’s topic and producer generation. The member’s mandatory FeedState must agree with its manifest entry.
* Enforce `resource.created <= topic.created` and `created <= now < expires` for the topic and each member. The JOIN deadline must still be in the future. Every valid monotonic ingress or publication time must be no later than that member’s creation time. Exchange UTC timestamps are in a different clock domain.
* Check each instrument against its required categories of Quote (1), Trade (2) and MarkFunding (4). The full member list is required even while an individual category is bootstrapping. A BBO-only Bybit/OKX member can satisfy Quote with type 265 and an empty depth ladder. Its unknown coverage remains explicit.

## Structure, readiness and use

`member(index)` bounds-checks the index and byte ranges. It returns read-only state and depth slices plus copied header values. A structurally valid image can still have missing required state or a non-live member. `check_readiness(now)` requires every listed member and every required category to be live, a clock at least as recent as validation, and time strictly before the earliest bound deadline. The caller separately checks the latest observation age and authenticated cursor before activating the reader.

Validation is cold-path work. It may allocate bounded member and snapshot-ID collections. The producer’s builder and authenticated owner pass the immutable baseline to the real reader, which resumes at `S+1`. [JOIN protocol](join.md) · [Shared ring](shm-ring.md)

```sh
cargo test --locked -p tickwire-snapshot -p tickwire-join
cargo test --locked -p tickwire-md --test spot_quotes
```

The fixtures exercise every truncation point, multiple instruments on one cut, duplicate IDs, digest-valid identity/time mismatches, unavailable categories, real sealed images and a BBO snapshot followed by live and stale records. [Receiver tests](../../crates/tickwire-snapshot/tests/validation.rs) · [Immutable mappings](../../crates/tickwire-snapshot/tests/immutable_linux.rs) · [Spot BBO round trip](../../crates/tickwire-md/tests/spot_quotes.rs)
