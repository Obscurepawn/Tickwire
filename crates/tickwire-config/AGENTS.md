# tickwire-config

Configuration values identify a revision and immutable content for each applied setting.

Test that canonical encoding distinguishes incompatible identities and reproduces the same hash for a semantic round trip. Deployment and policy owners consume the qualified result.

[Crate API](src/lib.rs) · [artifacts](tests/artifacts.rs) · [candidate](tests/candidate.rs) · [capacity](tests/capacity.rs) · [changes](tests/changes.rs) · [delivery](tests/delivery.rs)

Specifications include [config candidate](../../specs/implementation/config-candidate.md), [config locator](../../specs/implementation/config-locator.md), [strategy parameter consumption](../../specs/implementation/strategy-parameter-consumption.md).

```sh
cargo test --locked -p tickwire-config --all-targets
```
