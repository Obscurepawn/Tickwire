# tickwire-ipc

The IPC crate defines the fixed binary layout for market, account and strategy records.

Check alignment, offsets, endianness, reserved bytes and checksum coverage with the published wire vectors. An ABI change has readers in multiple processes.

[Crate API](src/lib.rs) · [layout extensions](tests/layout_extensions.rs) · [layouts](tests/layouts.rs) · [market exact](tests/market_exact.rs) · [release layouts](tests/release_layouts.rs) · [replay control](tests/replay_control.rs)

Specifications include [ipc codec](../../specs/implementation/ipc-codec.md).

```sh
cargo test --locked -p tickwire-ipc --all-targets
```
