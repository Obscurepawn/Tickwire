# tickwire-journal-codec

The bounded journal codec defines canonical bytes for business records.

Preserve tags, integer ranges and field order. Verify the size boundary, truncated records and round trips against the retained vectors before changing a record.

[Crate API](src/lib.rs) · [codec](tests/codec.rs)

Specifications include [command binding](../../specs/implementation/command-binding.md), [journal codec](../../specs/implementation/journal-codec.md).

```sh
cargo test --locked -p tickwire-journal-codec --all-targets
```
