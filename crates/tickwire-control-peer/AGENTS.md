# tickwire-control-peer

This crate transports Raft peer traffic between authenticated control nodes.

Check the remote node identity before admitting a message to the Raft group. Exercise disconnect, duplicate traffic and peer reconfiguration with the connection state machine.

[Crate API](src/lib.rs) · [api owner](tests/api_owner.rs) · [config runner](tests/config_runner.rs) · [config startup](tests/config_startup.rs) · [frames](tests/frames.rs) · [network](tests/network.rs)

Specifications include [config server](../../specs/implementation/config-server.md), [control raft service](../../specs/implementation/control-raft-service.md).

```sh
cargo test --locked -p tickwire-control-peer --all-targets
```
