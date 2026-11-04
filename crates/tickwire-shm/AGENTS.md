# tickwire-shm

The shared-memory ring owns publication order and reader cursors for each bank.

A reader can consume only a committed batch. Cover wraparound, lag and a writer switching generations. Measure contention with several independent readers.

[Crate API](src/lib.rs) · [join pending](tests/join_pending.rs) · [mapped](tests/mapped.rs) · [protocol](tests/protocol.rs)

Specifications include [shm mapping](../../specs/implementation/shm-mapping.md), [shm ring](../../specs/implementation/shm-ring.md).

```sh
cargo test --locked -p tickwire-shm --all-targets
```
