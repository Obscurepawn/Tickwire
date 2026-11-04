# tickwire-lifecycle

The release coordinator advances a group of members using signed facts and durable decisions.

Tie every transition to its operation, member set and verified predecessor. Cover restart between decision persistence and member acknowledgement.

[Crate API](src/lib.rs)

Specifications include [lifecycle coordinator](../../specs/implementation/lifecycle-coordinator.md).

```sh
cargo test --locked -p tickwire-lifecycle --all-targets
```
