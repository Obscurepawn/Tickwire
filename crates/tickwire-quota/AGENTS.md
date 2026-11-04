# tickwire-quota

One local owner consumes budget from the supervisor's quota allocations.

Preserve the allocation epoch and time window while several request kinds compete. Exercise boundary times, exhausted capacity and a denied reservation.

[Crate API](src/lib.rs) · [quota](tests/quota.rs)

Specifications include [quota](../../specs/implementation/quota.md).

```sh
cargo test --locked -p tickwire-quota --all-targets
```
