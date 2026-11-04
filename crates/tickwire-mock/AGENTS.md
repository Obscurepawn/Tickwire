# tickwire-mock

Test doubles provide deterministic venue and storage behavior for order round trips.

Inject a fault at the same boundary the real transport exposes. Report actual submit counts, accepted bytes, late fills and retained budget in the test.

[Crate API](src/lib.rs) · [services](tests/services.rs)

Specifications include [execution core](../../specs/implementation/execution-core.md), [mock abort](../../specs/implementation/mock-abort.md).

```sh
cargo test --locked -p tickwire-mock --all-targets
```
