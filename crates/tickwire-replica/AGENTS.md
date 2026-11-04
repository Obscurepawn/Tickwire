# tickwire-replica

Replication transfers a verified ordered prefix to another owner.

Retain the original logical record and writer identity. Cover duplicate batches, prefix conflicts and the exact receipt required by a synchronous profile.

[Crate API](src/lib.rs)

Specifications include [replica](../../specs/implementation/replica.md).

```sh
cargo test --locked -p tickwire-replica --all-targets
```
