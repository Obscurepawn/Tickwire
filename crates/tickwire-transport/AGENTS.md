# tickwire-transport

This crate drives nonblocking TCP, TLS, HTTP and WebSocket I/O with bounded budgets.

A poll may finish only part of a send. Preserve the connection epoch and accepted-byte count, and invoke the final gate at the first application write.

[Crate API](src/lib.rs) · [framing](tests/framing.rs) · [network](tests/network.rs)

Specifications include [transport](../../specs/implementation/transport.md).

```sh
cargo test --locked -p tickwire-transport --all-targets
```
