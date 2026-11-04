# tickwire-supervisor

The supervisor owns child lifetimes and their authenticated control connections.

Keep PID, instance and launch observations tied to the child actually started. Exercise startup timeout, failed authentication, shutdown and resource cleanup.

[Crate API](src/lib.rs) · [authenticated image](tests/authenticated_image.rs) · [current process identity](tests/current_process_identity.rs) · [observation](tests/observation.rs) · [resources](tests/resources.rs)

Specifications include [control session](../../specs/implementation/control-session.md), [supervisor process](../../specs/implementation/supervisor-process.md).

```sh
cargo test --locked -p tickwire-supervisor --all-targets
```
