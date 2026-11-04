# tickwire-control-raft

This module drives the Raft state machine for low-frequency configuration and execution authority.

Keep proposal, commitment, application and read-index completion distinct. Cover leader changes and snapshot installation before accepting reads from the new revision.

[Crate API](src/lib.rs) · [cluster](tests/cluster.rs) · [membership](tests/membership.rs) · [snapshot ready](tests/snapshot_ready.rs)

Specifications include [control raft service](../../specs/implementation/control-raft-service.md), [control raft](../../specs/implementation/control-raft.md).

```sh
cargo test --locked -p tickwire-control-raft --all-targets
```
