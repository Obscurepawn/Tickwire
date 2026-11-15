# tickwire-binance-quota

`tickwire-binance-quota` (`#![forbid(unsafe_code)]`) maps typed Binance Spot, USD-M, COIN-M, and Options
operations—trading commands, WebSocket session maintenance, reconciliation REST reads, clock calibration
queries, and public stream/snapshot actions—onto weighted multi-window costs enforced by a single local
`tickwire-quota::ExecLimiter` (`Endpoint`), provides `MultiIpCommandQuota` over
`tickwire_quota::rate_limiter::VenueRateLimiter`, and exposes `SharedBinanceOrsQuota` over the process-wide
cross-account lock-free `tickwire_quota::SharedRateLimiter`. The crate depends directly on `tickwire-venue`,
`tickwire-binance`, and `tickwire-binance-private` with zero dependency on legacy `tickwire-binance-account`
state, zero network I/O, and zero floating-point arithmetic.

## Invariants and Change Guidelines

* **Exhaustive `(Source, Action)` rule tables**: `validate_policy` in [src/lib.rs](src/lib.rs) and default policies in
  [src/bootstrap.rs](src/bootstrap.rs) require non-zero `generation`, `starts_ns < ends_ns`, unique `(source, action)`
  pairs within `MAX_RULES` (`256`), strictly increasing window durations per rule, and non-zero `Cost` units; any new
  endpoint or `CommandAction` must declare its exact `Class`, `api_weight`, `websocket_messages`, and order-count
  cost.
* **Multi-IP, cross-account ORS sharing, and action-class isolation**: `classify_command_action`,
  `classify_market_command_action`, `classify_read_action`, `MultiIpCommandQuota`, and `SharedBinanceOrsQuota`
  in [src/lib.rs](src/lib.rs) strictly isolate pure cancellations (`Cancel`, `CancelList`, `CancelAlgo`) and queries
  (`Query`, `ReadAction`) from account `ORDER` caps across Spot (`100/10s`, `200,000/86,400s`, `6,000/60s` IP),
  USD-M (`300/10s`, `1,200/60s`, `2,400/60s` IP), COIN-M (`1,200/60s`, `2,400/60s` IP), and Options
  (`100/10s`, `1,200/60s`, `2,400/60s` IP). `SpotList { legs, .. }` charges its exact leg count with atomic
  rollback on IP exhaustion, Spot `Amend` (`counts_as_order = false`, weight `4`) is distinguished from derivatives
  `Modify` (`counts_as_order = true`), `pick_route_and_spend_command_or_nack` enforces single-pick NACK without
  retrying another connection, HTTP `429` cools down only `local_ip`, and HTTP `418` freezes the account.
* **Idempotent attempt identity across zero-byte retries**: `Endpoint` hashes full binding context (`owner_id`,
  `ClockDomain`, epoch, serial/intent, and action kind) up to `MAX_IDENTITIES` (`4096`), and
  `MultiIpCommandQuota::consume_command_for_ip` deduplicates by `(intent_exec_epoch, intent_id, transport, local_ip)`
  so repeated zero-byte `WouldBlock` polls on the same link never double-debit windows while conflicting digests
  immediately freeze the owner.
* **Live policy rollover & rising-edge `rateLimits[]` reconciliation**: `PreparedPolicy` in [src/lib.rs](src/lib.rs)
  validates a higher `generation` for the same `producer` and `ClockDomain` and retains all prior `IdentityEntry`
  records on commit. `SharedBinanceOrsQuota::reconcile_venue_usage` emits rising-edge `ExchRateLimitEvent`
  notifications at `exch_warn_pct` (`80%`) and `100%` ceiling, re-arming only after usage drops below threshold.

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
