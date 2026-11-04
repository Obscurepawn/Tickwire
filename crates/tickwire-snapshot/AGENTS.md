# tickwire-snapshot

This crate builds and verifies immutable market snapshots for readers joining a live topic.

Tie the content digest to the topic, source generation and ring frontier. Test bounded decoding and a publisher advancing while the reader installs the snapshot.

[Crate API](src/lib.rs) · [builder](tests/builder.rs) · [immutable linux](tests/immutable_linux.rs) · [validation](tests/validation.rs)

Specifications include [topic snapshot builder](../../specs/implementation/topic-snapshot-builder.md), [topic snapshot](../../specs/implementation/topic-snapshot.md).

```sh
cargo test --locked -p tickwire-snapshot --all-targets
```
