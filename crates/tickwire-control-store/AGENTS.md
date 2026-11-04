# tickwire-control-store

The control store journals local worker and market control requests. An exact retry returns the stored completion.

Trace request admission and completion through the journal sync. Test a duplicate key after restart with both identical and conflicting payloads.

[Crate API](src/lib.rs) · [durable](tests/durable.rs)

Specifications include [control session](../../specs/implementation/control-session.md).

```sh
cargo test --locked -p tickwire-control-store --all-targets
```
