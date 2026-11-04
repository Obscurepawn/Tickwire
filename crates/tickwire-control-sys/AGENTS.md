# tickwire-control-sys

This Linux boundary transfers authenticated control messages and file descriptors over Unix sockets.

Check peer credentials and the exact number and ownership of received descriptors. Cover malformed control messages, truncated ancillary data and cleanup after rejection.

[Crate API](src/lib.rs) · [executable](tests/executable.rs) · [platform](tests/platform.rs) · [process resources](tests/process_resources.rs) · [transport](tests/transport.rs)

Specifications include [control transport](../../specs/implementation/control-transport.md), [supervisor process](../../specs/implementation/supervisor-process.md).

```sh
cargo test --locked -p tickwire-control-sys --all-targets
```
