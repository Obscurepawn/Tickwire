# tickwire-journal

This crate owns physical WAL framing, sequence checks and recovery of a durable prefix.

Insert a crash or torn write around each append boundary. Distinguish complete records, an incomplete tail and corrupted data inside the retained prefix.

[Crate API](src/lib.rs) · [frame](tests/frame.rs) · [journal](tests/journal.rs) · [manifest](tests/manifest.rs) · [process](tests/process.rs) · [recovery validator](tests/recovery_validator.rs)

Specifications include [journal](../../specs/implementation/journal.md).

```sh
cargo test --locked -p tickwire-journal --all-targets
```
