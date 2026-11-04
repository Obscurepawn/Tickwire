# tickwire-config-fs

The Linux file store persists configuration using descriptor-relative operations.

Check the path walk, temporary file, synchronization and replacement as one update. Cover crashes on each side of publication and links that could redirect the write.

[Crate API](src/lib.rs) · [files](tests/files.rs) · [journal](tests/journal.rs) · [journal process](tests/journal_process.rs)

Specifications include [config file store](../../specs/implementation/config-file-store.md), [config journal store](../../specs/implementation/config-journal-store.md).

```sh
cargo test --locked -p tickwire-config-fs --all-targets
```
