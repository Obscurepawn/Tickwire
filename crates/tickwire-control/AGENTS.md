# tickwire-control

Authenticated control sessions relate a peer, a connection and a scoped operation.

When changing a message, follow the challenge, epoch and request identity through acceptance and disconnect. Preserve replay protection and the owner's ordered session history.

[Crate API](src/lib.rs) · [session](tests/session.rs) · [transport linux](tests/transport_linux.rs)

Specifications include [control session](../../specs/implementation/control-session.md).

```sh
cargo test --locked -p tickwire-control --all-targets
```
