# tickwire-binance-quota

`tickwire-binance-quota` (`#![forbid(unsafe_code)]`) maps typed Binance Spot and USD-M operations—trading commands,
WebSocket session maintenance, reconciliation REST reads, clock calibration queries, and public stream/snapshot
actions—onto weighted multi-window costs enforced by a single local `tickwire-quota::ExecLimiter` (`Endpoint`), and
provides `MultiIpCommandQuota` over `tickwire_quota::rate_limiter::VenueRateLimiter` for multi-source-IP,
action-class-isolated hot-path execution. Session and execution adapters obtain permits without performing network I/O
or reading wall clocks directly.

## Invariants and Change Guidelines

* **Exhaustive `(Source, Action)` rule tables**: `validate_policy` in [src/lib.rs](src/lib.rs) and default policies in
  [src/bootstrap.rs](src/bootstrap.rs) require non-zero `generation`, `starts_ns < ends_ns`, unique `(source, action)`
  pairs within `MAX_RULES` (`256`), strictly increasing window durations per rule, and non-zero `Cost` units; any new
  endpoint or `CommandAction` must declare its exact `Class`, `api_weight`, `websocket_messages`, and order-count
  cost.
* **Multi-IP and action-class isolation**: `classify_command_action`, `classify_read_action`, and `MultiIpCommandQuota`
  in [src/lib.rs](src/lib.rs) strictly isolate pure cancellations (`Cancel`, `CancelList`, `CancelAlgo`) and queries
  (`Query`, `ReadAction`) from account `ORDER` caps, charge `SpotList { legs, .. }` its exact leg count with
  transactional rollback on IP exhaustion, distinguish Spot `Amend` (`counts_as_order = false`, weight `4`) from USD-M
  `Modify` (`counts_as_order = true`), isolate HTTP `429` (`note_ip_rate_limited`) to the affected `IpKey` while
  leaving sibling source IPs active, and freeze the entire account on HTTP `418` (`note_account_banned`).
* **Idempotent attempt identity across zero-byte retries**: `Endpoint` hashes full binding context (`owner_id`,
  `ClockDomain`, epoch, serial/intent, and action kind) up to `MAX_IDENTITIES` (`4096`), and
  `MultiIpCommandQuota::consume_command_for_ip` deduplicates by `(intent_exec_epoch, intent_id, transport, local_ip)`
  so repeated zero-byte `WouldBlock` polls on the same link never double-debit windows while conflicting digests
  immediately freeze the owner.
* **Live policy rollover without losing history**: `PreparedPolicy` in [src/lib.rs](src/lib.rs) validates a higher
  `generation` for the same `producer` and `ClockDomain` and retains all prior `IdentityEntry` records on commit so
  in-flight retries remain idempotent across policy updates.
* **Shared budget protection**: Ensure clock calibration (`TimeConnect`/`TimeQuery`), private session renewals,
  reconciliation reads (`ReadQuery`), and order commands share consistent window accounting and honor `418`/`429`
  feedback.

## Verification and Specifications

* **Source and unit tests**: [src/lib.rs](src/lib.rs) · [src/bootstrap.rs](src/bootstrap.rs) ·
  [src/read_tests.rs](src/read_tests.rs)
* **Integration tests**: [tests/permits.rs](tests/permits.rs) ·
  [tests/multi_ip_isolation.rs](tests/multi_ip_isolation.rs)
* **Specifications and models**: [binance-quota.md](../../specs/implementation/binance-quota.md) ·
  [quota.md](../../specs/implementation/quota.md) ·
  [request_budget](../../tla/execution_models/request_budget/ROADMAP.md)

```sh
cargo test --locked -p tickwire-binance-quota --all-targets
./scripts/check-tla.sh request_budget
```
