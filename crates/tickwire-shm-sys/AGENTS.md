# tickwire-shm-sys

This crate exposes safe ownership of the Linux maps used by the ring and the descriptor transport.

Review lengths, alignment, permissions, seals, atomic ordering and mapping lifetimes at each unsafe operation. Exercise the mapping from another process.

[Crate API](src/lib.rs) · [immutable](tests/immutable.rs) · [mapping](tests/mapping.rs) · [process](tests/process.rs)

Specifications include [immutable resource](../../specs/implementation/immutable-resource.md), [shm mapping](../../specs/implementation/shm-mapping.md).

```sh
cargo test --locked -p tickwire-shm-sys --all-targets
```
