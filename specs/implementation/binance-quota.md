# Binance Endpoint Request Quotas

`tickwire-binance-quota` maps typed Binance network actions—HTTPS clock calibration, private user-data session control, public market-data streams/snapshots, read-only account reconciliation queries, WebSocket order-entry session control, guardian connects, and signed trading commands across Spot, USD-M, COIN-M, and European Options—onto a single shared local `tickwire_quota::ExecLimiter` (`Endpoint`), provides `MultiIpCommandQuota` over `tickwire_quota::rate_limiter::VenueRateLimiter`, and exposes `SharedBinanceOrsQuota` over the process-wide lock-free `tickwire_quota::shared::SharedRateLimiter` ([quota.md](quota.md)). Built in safe Rust (`#![forbid(unsafe_code)]`) with zero dependency on legacy `tickwire-binance-account` state, the crate binds directly to `tickwire-venue`, `tickwire-binance`, and `tickwire-binance-private` types, enforces policy lifetimes, validates caller-reported wire weights against explicit policy rules, and constructs collision-free domain-separated attempt keys.

`Endpoint`, `MultiIpCommandQuota`, and `SharedBinanceOrsQuota` own local request classification, single-pick route-IP admission, and attempt-identity deduplication, while grant sizing, lease renewal schedules, and strategy fan-out of rising-edge `ExchRateLimit` notifications belong to the control plane and ORS execution owner.

## Policy Model and Bounds

Each `Policy` binds a nonzero monotonic `generation`, an active `[starts_ns, ends_ns)` monotonic validity window (`starts_ns < ends_ns`), and `1..=256` (`MAX_RULES`) unique `(Source, Action)` rules. Each `Rule` specifies `Class` (`Normal` vs `Recovery`), expected `api_weight`, expected `websocket_messages`, and `1..=16` (`quota::MAX_COSTS`) strictly sorted, nonzero `Cost { window, units }` entries. `prepare_policy` and `commit_policy` atomically install a newer policy generation (`policy.generation > current`, `ends_ns > now`) while retaining all previously recorded attempt identities.

| Resource or Family | Bound / Key Tag | Bound Identity Fields |
| --- | --- | --- |
| `identities` / `MultiIpCommandQuota` attempts | `<= 4,096` (`MAX_IDENTITIES`) | Maps attempt key to 32-byte `digest` (`Error::Capacity` / `MultiIpQuotaError::Capacity` at `4,097th` entry). |
| `TimePermit` (`tag 1`) | `TimeConnect`, `TimeQuery` | `owner_id`, `source`, `connection_epoch`, `action`, `api_weight`. |
| `ControlPermit` (`tag 2`) | Spot/USD-M private stream & listen-key ops | `owner_id`, `AccountScope` (incl. `credential_generation`), `connection_epoch`, `attempt`, `digest`, `bytes`, optional `subscription_id`. |
| `PublicPermit` (`tag 3`) | `Stream` / `Snapshot` lanes (`Connect`, `Upgrade`, `Subscribe`, `Pong`, `Close`, `Snapshot`) | `owner_id`, `source`, `connection_epoch`, `operation`, `action`, `api_weight`, fixed WS message count (`1` for `Subscribe`/`Pong`/`Close`). |
| `Command` (`tag 4`) | REST / WS order, cancel, list, algo, peg, SOR | `owner_id`, `source`, `risk_domain_id`, `account`, `intent_exec_epoch`, `intent_id`, `transport`, `command.digest()`. |
| `ReadPermit` (`tag 5`) | `Connect`, `TradePage`, `AccountSnapshot`, `OpenOrders`, `ConditionalOrders`, `OrderQuery`, `Income`, `StructureQuery`, `AmendmentHistory` | `owner_id`, `AccountScope`, query family, `request_epoch`, `ReadOperation` phase, and exact `reconciliation::*Query` parameters. |
| `Guardian` / `CommandSession` (`tags 6, 7`) | Guardian connect & WS trading control (`Connect`, `Upgrade`, `Ping`, `Pong`, `Close`) | `owner_id`, `source`, `ws::SessionIdentity` (`input_stream`, `execution_instance`, `writer_epoch`, `connection_epoch`, `operation`, stream/baseline/credential digests). |
| `MultiIpCommandQuota` / `SharedBinanceOrsQuota` | `1..=16` `IpKey`s (`BinanceSpot`, `BinanceUsdm`, `BinanceCoinm`, `BinanceOptions`) | `(intent_exec_epoch, intent_id, transport, local_ip)` + `command.digest()`, sharing `local_ip` weight across all `AccountSession`s in `SharedRateLimiter`. |

## Core Invariants

* **Strict rule and weight agreement**: Every `Endpoint::consume_*` call requires `starts_ns <= now < ends_ns`, an exact `(Source, Action)` match in `Policy.rules`, and exact equality between caller-reported `(api_weight, websocket_messages)` and the rule's configured values (`Error::Policy` or `Error::Identity` otherwise). Trading commands, read-reconciliation queries, and command-session control actions carry `0` wire-reported weight and rely strictly on explicit rule `costs`.
* **Action-class isolation across `ORDER`, `CANCEL`, and `QUERY` budgets**: `classify_command_action`, `classify_market_command_action`, and `classify_read_action` partition Binance Spot, USD-M, COIN-M, and Options sends into `ActionBudgetClass`:
  * Order-counting actions (`New`, `CancelReplace`, `AdvancedCancelReplace`, `PeggedNew`, `Sor`, `Algo`, derivatives `Modify`, and `SpotList { legs, .. }` charging `legs` order units) debit account `ORDER` windows (`100/10s` + `200,000/86,400s` on Spot; `300/10s` + `1,200/60s` on USD-M; `1,200/60s` on COIN-M; `100/10s` + `1,200/60s` on Options) transactionally alongside the chosen `local_ip`'s `60s` weight window (`6,000/60s` on Spot; `2,400/60s` on USD-M, COIN-M, and Options, with `0` IP weight on derivatives WebSocket order placement).
  * Spot `Amend` (`/api/v3/order/amend/keepPriority`) charges IP weight `4` with `counts_as_order = false`, whereas derivatives `Modify` charges `counts_as_order = true`.
  * Pure cancel actions (`Cancel`, `CancelList`, `CancelAlgo`) and pure query/reconciliation actions (`Query`, `ReadAction`) **never** touch account `ORDER` windows, guaranteeing cancel and status availability when order-placement caps are saturated.
* **Single-pick route admission and cross-account shared IP budgets**: In `SharedBinanceOrsQuota`, `pick_route_and_spend_command_or_nack` selects a single outbound connection (`OrderRouter::pick`) and calls `spend_command_for_ip` to charge that `account_ref`'s market `ORDER` buckets plus the shared `RateRule::ip` bucket for `route.local_ip`. Exhaustion immediately returns a rate-limit NACK without retrying another connection.
* **Idempotent zero-byte retries vs freeze-on-conflict**: `Endpoint` hashes `"tickwire.binance-quota.charge.v1\0" || policy.generation || key || action || weights || content || class || costs` into a 32-byte `digest`. `MultiIpCommandQuota::consume_command_for_ip` and `consume_intent_for_ip` key retries by `(intent_exec_epoch, intent_id, transport, local_ip)`: repeating a zero-byte `WouldBlock` send on the same link returns `Consumed::Duplicate` without double-debiting any window, failing over to a sibling `local_ip` charges that sibling's IP budget while enforcing identical `digest` across `(intent_exec_epoch, intent_id)`, and any conflicting `digest` or `ClockDomain` mismatch immediately freezes the owner.

## Transitions and Failure Semantics

During normal operation, the supervisor inspects `Endpoint::consumer()` to issue new `Grant` capabilities (`install`) and commit `LeaseRenewal` updates (`renew`), upgrades endpoint rules via `prepare_policy` / `commit_policy` without clearing recorded `identities`, provisions a per-account `MultiIpCommandQuota` via `bootstrap::MultiIpBootstrapConfig`, or attaches multiple `AccountSession`s to one process-wide `SharedRateLimiter` via `SharedBinanceOrsQuota`. When a permit adapter returns `false`, `Endpoint::last_error()` retains the exact `Policy`, `Identity`, `Capacity`, or `Quota` rejection reason while denying the outbound write before its first byte.

In single-IP `Endpoint` mode, HTTP `429` or `418` freezes the shared `ExecLimiter`. In `MultiIpCommandQuota` and `SharedBinanceOrsQuota`, `observe_http_status(local_ip, 429, retry_until_ns)` isolates the cooldown strictly to `local_ip` so sibling `local_ip` routes continue admitting orders, cancels, and queries, whereas `418` freezes the account across all IPs. Venue `rateLimits[]` frames reconcile through `reconcile_venue_usage`, emitting rising-edge `ExchRateLimitEvent` notifications when crossing `exch_warn_pct` (`80%`) or `100%` ceiling and re-arming only after reported usage drops below threshold.

## Verification and References

```sh
cargo test --locked -p tickwire-binance-quota --all-targets
```

Unit and integration tests verify policy validation and hot policy upgrades, exact retry idempotency across all identity families, multi-IP `429` cooldown isolation vs `418` account ban freeze, `SpotList` multi-leg transactional rollback, `ORDER` vs `CANCEL`/`QUERY` budget isolation across Spot/USD-M/COIN-M/Options, cross-account shared `local_ip` contention, single-pick NACK behavior, and rising-edge `rateLimits[]` reconciliation.

* [Binance quota endpoint, `MultiIpCommandQuota`, `SharedBinanceOrsQuota`, and permit adapters](../../crates/tickwire-binance-quota/src/lib.rs)
* [Declarative single-IP and multi-IP bootstrap configurations](../../crates/tickwire-binance-quota/src/bootstrap.rs)
* [Permit deduplication, policy upgrade, and freeze tests](../../crates/tickwire-binance-quota/tests/permits.rs)
* [Multi-source-IP isolation, `SpotList` rollback, cross-account ORS sharing, and `429`/`418` tests](../../crates/tickwire-binance-quota/tests/multi_ip_isolation.rs)
* [Core grant, `ExecLimiter`, `RateLimiter`, and `SharedRateLimiter` specification](quota.md)
