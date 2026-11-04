# tickwire-business-journal

This journal binds intent records, wire identity and commit evidence in a recoverable order.

Check the crash point on each side of local synchronization. A recovered intent can have reached the venue, even when the local process never recorded a response.

[Crate API](src/lib.rs) · [runtime](tests/runtime.rs)

Specifications include [business journal](../../specs/implementation/business-journal.md), [command binding](../../specs/implementation/command-binding.md), [mock abort](../../specs/implementation/mock-abort.md).

```sh
cargo test --locked -p tickwire-business-journal --all-targets
```
