# Shared cash and the requesting strategy

Source revision `9b2ed630721911e01046822bddc9db21b2fa5aee`. The existing account and gateway models track an admitted command and its resources. This level adds two actual file-queue owners, their local result callbacks, the spot REST session and the account runner that routes a reply.

The production deployment constructs each member from a distinct directory in `crates/tickwire-spot-runner/src/live/config.rs:91-106`. The group requires distinct logical IDs and remembers each configured slot in `crates/tickwire-app/src/strategy.rs:433-466`. The runner polls transport before asking the group for another submission in `crates/tickwire-spot-runner/src/live.rs:321-345`. [Routing contract](../../../../specs/implementation/spot-strategy-routing.md) · [Worker isolation requirement](../../../../specs/implementation/single-host-trading.md#acceptance)
