# tickwire-config-journal

The configuration journal records the order in which a local process observed and applied revisions.

Verify identity, content hash and sequence when reopening the log. A durable receipt can then be matched with the process's effective-file observation.

[Crate API](src/lib.rs) · [journal](tests/journal.rs)

Specifications include [config journal store](../../specs/implementation/config-journal-store.md), [config local journal](../../specs/implementation/config-local-journal.md).

```sh
cargo test --locked -p tickwire-config-journal --all-targets
```
