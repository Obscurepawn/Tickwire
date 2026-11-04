# tickwire-control-store

The control store persists consensus state and snapshots across restarts.

Trace a storage update from the Raft batch to its sync boundary. Validate the retained index range and snapshot consistency when reopening the store.

[Crate API](src/lib.rs) · [durable](tests/durable.rs)

Specifications include [control session](../../specs/implementation/control-session.md).

```sh
cargo test --locked -p tickwire-control-store --all-targets
```
