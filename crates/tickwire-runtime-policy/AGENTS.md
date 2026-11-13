# tickwire-runtime-policy

`tickwire-runtime-policy` (`#![forbid(unsafe_code)]`) owns the in-memory control-plane policy reducer (`ControlPlane`)
that combines supervisor `OwnerLease` fencing, `StrategyGrant` and `ManagementGrant` permissions, market-feed
freshness (`FeedRequirement`, `FeedUpdate`), and verified private-state reconciliation (`ReconciliationRequest`) into
`tickwire-account-gateway::Gate` decisions. Single-threaded trading runtimes feed authenticated control updates, feed
marks, and private connection state into `ControlPlane` and attach it as a `GateProvider`; OS process isolation,
network I/O, and cryptographic proof generation belong to upstream callers.

## Invariants and Change Guidelines

* **Conjunction of lease, mode, reconciliation, and grant at every gate**: In [src/lib.rs](src/lib.rs), `Gate`
  evaluation samples `PolicyClock` synchronously and requires a live `OwnerLease` (`now < valid_until_ns`, matching
  `executor_id`, `owner_generation`, `writer_epoch`, and `ClockDomain`) before permitting any command; `Mode::Stopped`
  blocks all actions, `Mode::ManageOnly` permits only risk-reducing cancels and queries, and `Mode::Trading`
  additionally requires fresh `required_feeds` and reconciled private state for risk increases.
* **Surviving `ManagementGrant` for revocations**: Keep `ManagementGrant` authorization independent of revoked or
  expired `StrategyGrant` slots so the executor can still cancel or query outstanding orders for named strategies
  without opening new exposure.
* **Exact-prefix reconciliation binding**: `ReconciliationVerifier::verify` must attest the exact
  `(scope, product, writer_epoch, input_stream, connection_epoch, gap_generation, through_sequence, baseline_digest, history_digest, query_coverage_digest)`
  tuple; a raw socket reconnect never restores reconciliation on its own.
* **Monotonic epoch and generation ratchets**: Enforce non-decreasing `lease_epoch`, `authorization_epoch`,
  `strategy_generation`, and `FeedUpdate` sequence/timestamp marks across [src/lib.rs](src/lib.rs) and
  [src/bootstrap.rs](src/bootstrap.rs), rejecting stale or conflicting control inputs.

## Verification and Specifications

* **Source and unit tests**: [src/lib.rs](src/lib.rs) · [src/bootstrap.rs](src/bootstrap.rs) ·
  [src/tests.rs](src/tests.rs)
* **Specifications and models**: [runtime-policy.md](../../specs/implementation/runtime-policy.md) ·
  [binance_roundtrip](../../tla/execution_models/binance_roundtrip/ROADMAP.md) ·
  [spot_roundtrip](../../tla/execution_models/spot_roundtrip/ROADMAP.md) ·
  [quote_delivery](../../tla/execution_models/quote_delivery/ROADMAP.md)

```sh
cargo test --locked -p tickwire-runtime-policy --all-targets
```
