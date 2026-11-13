# tickwire-binance-quota

`tickwire-binance-quota` (`#![forbid(unsafe_code)]`) maps typed Binance Spot and USD-M operations—trading commands,
WebSocket session maintenance, reconciliation REST reads, clock calibration queries, and public stream/snapshot
actions—onto weighted multi-window costs enforced by a single local `tickwire-quota::ExecLimiter` (`Endpoint`).
Session and execution adapters obtain `TimePermit`, `PublicPermit`, and command/read permits from `Endpoint` without
performing network I/O or reading wall clocks directly.

## Invariants and Change Guidelines

* **Exhaustive `(Source, Action)` rule tables**: `validate_policy` in [src/lib.rs](src/lib.rs) and default policies in
  [src/bootstrap.rs](src/bootstrap.rs) require non-zero `generation`, `starts_ns < ends_ns`, unique `(source, action)`
  pairs within `MAX_RULES` (`256`), strictly increasing window durations per rule, and non-zero `Cost` units; any new
  endpoint or `CommandAction` must declare its exact `Class`, `api_weight`, `websocket_messages`, and order-count
  cost.
* **Idempotent attempt identity across zero-byte retries**: `Endpoint` hashes full binding context (`owner_id`,
  `ClockDomain`, epoch, serial/intent, and action kind) up to `MAX_IDENTITIES` (`4096`) so repeated pre-first-byte
  checks for the same attempt reuse the existing `AttemptId` instead of double-charging the window.
* **Live policy rollover without losing history**: `PreparedPolicy` in [src/lib.rs](src/lib.rs) validates a higher
  `generation` for the same `producer` and `ClockDomain` and retains all prior `IdentityEntry` records on commit so
  in-flight retries remain idempotent across policy updates.
* **Shared budget protection**: Ensure clock calibration (`TimeConnect`/`TimeQuery`), private session renewals,
  reconciliation reads (`ReadQuery`), and order commands share the same `ExecLimiter` windows and honor `418`/`429`
  freeze feedback.

## Verification and Specifications

* **Source and unit tests**: [src/lib.rs](src/lib.rs) · [src/bootstrap.rs](src/bootstrap.rs) ·
  [src/read_tests.rs](src/read_tests.rs)
* **Integration tests**: [tests/permits.rs](tests/permits.rs)
* **Specifications and models**: [binance-quota.md](../../specs/implementation/binance-quota.md) ·
  [quota.md](../../specs/implementation/quota.md) ·
  [request_budget](../../tla/execution_models/request_budget/ROADMAP.md)

```sh
cargo test --locked -p tickwire-binance-quota --all-targets
./scripts/check-tla.sh request_budget
```
