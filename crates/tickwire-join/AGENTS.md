# tickwire-join

The JOIN protocol gives a consumer a consistent snapshot and ring cursor before live consumption.

Cover a writer advancing the ring between snapshot acquisition and activation. The reader must start from an authenticated baseline in the right bank and generation.

[Crate API](src/lib.rs) · [flow](tests/flow.rs) · [owner](tests/owner.rs) · [process linux](tests/process_linux.rs)

Specifications include [join](../../specs/implementation/join.md).

```sh
cargo test --locked -p tickwire-join --all-targets
```
