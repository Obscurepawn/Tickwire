# Build a market-topic baseline

The topic owner reserves a pending JOIN at cut `S`, captures every member under its single-writer boundary and supplies borrowed inputs to the builder. `build_topic` returns a complete owned `BuiltTopic` containing its bytes, resource header, snapshot header and finalized JOIN. `build_topic_with_coverage` additionally preserves exact depth boundaries. [Builder](../../crates/tickwire-snapshot/src/builder.rs) · [JOIN owner](join.md)

## Bind the inputs

* Require 1–4096 registered instruments, strictly ascending, with exactly one matching ID/version member per entry. Snapshot IDs are nonzero and unique throughout the object. All members agree on cut, feed generation, catalog version and hash. Their last event cannot be later than `S`. The highest allowed cut is `2^63 − 2`, so `J = S + 1` fits below sequence exhaustion. Cut zero can describe an initial unavailable baseline.
* Validate identity and time across resource, catalog, membership, subscription, bank and cursor. An existing nonzero snapshot or set binding must match the derived one. Preserve the supplied bank’s `ResourceHeader.first_seq`. A bank’s first record and a particular subscriber’s `J` can differ.
* Each member supplies one to four distinct state categories including FeedState. Preserve Quote, Trade and MarkFunding when present. Input record sequences may not exceed the member's last event. Sort output state rows by message type, assign snapshot-local row numbers, recompute enabled CRCs and preserve original observation times.
* For depth, require positive prices and quantities, strictly decreasing bids, increasing asks, and an uncrossed live book. Reject contradictory order or coverage. Never fill in missing levels. A Bybit/OKX quote-only member may have a type-265 BBO with empty depth arrays and unknown side coverage. Missing required categories or an unavailable member can yield a structurally valid baseline that still fails readiness.

## Lay out and seal

Size the entire manifest and all page-aligned members using checked arithmetic **before** allocating the output. The aggregate object must fit both the caller’s `byte_limit` and 64 MiB. A legacy kind-6 normalized member contains a 256-byte header, two 64-byte section descriptors and their state/levels payloads. A kind-7 member has three descriptors and adds exact coverage under the required feature bit. Offsets are absolute file positions. Write zero padding, hash sections and each whole child, then bind the manifest and resource digests. The known legacy fixture occupies 12 KiB.

Before returning, run the same structural receiver validation used by the subscriber. Keep readiness separate. Failure returns no deliverable bytes and cannot mutate the caller’s ring, gate, cursor or JOIN. The owner controls the actual capture at one cut, memfd sealing and descriptor delivery. The subsequent READ_READY and JOIN_READY exchanges establish reader activation. [Receiver](topic-snapshot.md) · [Immutable resource](immutable-resource.md)

```sh
cargo test --locked -p tickwire-snapshot --test builder --test validation
cargo test --locked -p tickwire-md --test spot_quotes
```

Tests cover 4096 members, cumulative budget exhaustion, mismatched bindings, exact depth coverage and compatibility with the independent fixture. The spot round trip checks that baseline and subsequent live records retain the same BBO and coverage semantics.
