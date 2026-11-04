# tickwire-app

This crate loads deployment files and assembles account runtimes, strategy hosts, control clients and process observation.

Start at the command in main.rs and follow the owner it constructs. Inspect and run must agree on member identity and risk domain. For a public-market deployment with two independent workers, run the [single-host example](../../README.md#deploy-two-strategies). Keep the failing-member and shadow tests when changing scheduling.

[Crate API](src/lib.rs) · [catalog process](tests/catalog_process.rs) · [failover files](tests/failover_files.rs) · [inputs process](tests/inputs_process.rs) · [parameters process](tests/parameters_process.rs) · [projection launch](tests/projection_launch.rs)

## Deployment entry points

`inspect` and `run` use the same group preflight. Validate each slot, instance and risk domain before starting workers. A shared bank needs one cursor per member, and a shared server needs distinct JOIN requests under the same supervisor epoch. Reserve each member's command, report and recovery directories, including paths reached through symlinks. The runtime owns clock calibration, authenticated account input and reconciliation. The host owns the child, its ingress lanes and the mapping from a submission result back to its member. [Group preflight](src/launch/group.rs) · [Strategy scheduling](src/strategy.rs) · [Runtime](../tickwire-runtime/src/node.rs)

Three events establish a market transition. A control decision is persisted first. The target process then observes the approved identity and replacement, and the effective receipt is synchronized before output is released. After a durable write, resample the clock and recheck the binding and deadline. An old receipt may be valid for audit while a restarted process still waits for fresh authority. [Market transitions](src/control/market.rs) · [Observation inbox](src/observation/inbox.rs) · [Startup preflight](src/release_preflight.rs)

Specifications include [binance market events](../../specs/implementation/binance-market-events.md), [instrument catalog](../../specs/implementation/instrument-catalog.md), [market expansion](../../specs/implementation/market-expansion.md), [reconciliation controller](../../specs/implementation/reconciliation-controller.md), [runtime observation](../../specs/implementation/runtime-observation.md), [strategy input consumption](../../specs/implementation/strategy-input-consumption.md), [strategy parameter consumption](../../specs/implementation/strategy-parameter-consumption.md), [supervisor process](../../specs/implementation/supervisor-process.md).

```sh
cargo test --locked -p tickwire-app --all-targets
```
