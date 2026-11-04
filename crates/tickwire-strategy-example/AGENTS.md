# tickwire-strategy-example

The counting example ships two registered worker binaries with different checkpoint schemas.

Use the old executable to replay its own callbacks before migrating the state. The history must preserve whether an economic action has already been consumed.

[Crate API](src/lib.rs) · [source replay](tests/source_replay.rs)

Specifications include [strategy rebuild](../../specs/implementation/strategy-rebuild.md), [strategy replay](../../specs/implementation/strategy-replay.md).

```sh
cargo test --locked -p tickwire-strategy-example --all-targets
```
