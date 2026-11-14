# Binance Endpoint Request Quotas

`tickwire-binance-quota` maps typed Binance network actions—HTTPS clock calibration, private user-data session control, public market-data streams/snapshots, read-only account reconciliation queries, WebSocket order-entry session control, guardian connects, and signed trading commands—onto a single shared local `tickwire_quota::ExecLimiter` (`Endpoint`), and provides `MultiIpCommandQuota` over `tickwire_quota::rate_limiter::VenueRateLimiter` ([quota.md](quota.md)) for multi-source-IP, class-isolated command and query execution. Built in safe Rust (`#![forbid(unsafe_code)]`), `Endpoint` enforces policy lifetimes, validates caller-reported wire weights against explicit policy rules, constructs collision-free domain-separated attempt keys, and exposes read-only `consumer()` metadata so the supervisor can issue grants and renew leases without duplicating state.

`Endpoint` and `MultiIpCommandQuota` own local request classification and attempt-identity deduplication, while durable grant sizing, lease renewal schedules, and distribution of authoritative venue feedback belong to the control plane and execution owner.

## Policy Model and Bounds

Each `Policy` binds a nonzero monotonic `generation`, an active `[starts_ns, ends_ns)` monotonic validity window (`starts_ns < ends_ns`), and `1..=256` (`MAX_RULES`) unique `(Source, Action)` rules. Each `Rule` specifies `Class` (`Normal` vs `Recovery`), expected `api_weight`, expected `websocket_messages`, and `1..=16` (`quota::MAX_COSTS`) strictly sorted, nonzero `Cost { window, units }` entries. `prepare_policy` and `commit_policy` atomically install a newer policy generation (`policy.generation > current`, `ends_ns > now`) while retaining all previously recorded attempt identities.

| Resource or Family | Bound / Key Tag | Bound Identity Fields |
| --- | --- | --- |
| `identities` / `MultiIpCommandQuota` attempts | `<= 4,096` (`MAX_IDENTITIES`) | Maps attempt key to 32-byte `digest` (`Error::Capacity` / `MultiIpQuotaError::Capacity` at `4,097th` entry). |
| `TimePermit` (`tag 1`) | `TimeConnect`, `TimeQuery` | `owner_id`, `source`, `connection_epoch`, `action`, `api_weight`. |
| `ControlPermit` (`tag 2`) | Spot/USD-M private stream & listen-key ops | `owner_id`, `AccountScope` (incl. `credential_generation`), `connection_epoch`, `attempt`, `digest`, `bytes`, optional `subscription_id`. |
| `PublicPermit` (`tag 3`) | `Stream` / `Snapshot` lanes (`Connect`, `Upgrade`, `Subscribe`, `Pong`, `Close`, `Snapshot`) | `owner_id`, `source`, `connection_epoch`, `operation`, `action`, `api_weight`, fixed WS message count (`1` for `Subscribe`/`Pong`/`Close`). |
| `Command` (`tag 4`) | REST / WS order, cancel, list, algo, peg, SOR | `owner_id`, `source`, `risk_domain_id`, `account`, `intent_exec_epoch`, `intent_id`, `transport`, `command.digest()`. |
| `ReadPermit` (`tag 5`) | `Connect`, `TradePage`, `AccountSnapshot`, `OpenOrders`, `ConditionalOrders`, `OrderQuery`, `Income`, `StructureQuery`, `AmendmentHistory` | `owner_id`, `AccountScope`, query family, `request_epoch`, operation phase, and exact query parameters. |
| `Guardian` / `CommandSession` (`tags 6, 7`) | Guardian connect & WS trading control (`Connect`, `Upgrade`, `Ping`, `Pong`, `Close`) | `owner_id`, `source`, `SessionIdentity` (`input_stream`, `execution_instance`, `writer_epoch`, `connection_epoch`, `operation`, stream/baseline/credential digests). |
| `MultiIpCommandQuota` | `1..=16` `IpKey`s (`BinanceSpot` / `BinanceUsdm`) | `(intent_exec_epoch, intent_id, transport, local_ip)` + `command.digest()`. |

## Core Invariants

* **Strict rule and weight agreement**: Every `Endpoint::consume_*` call requires `starts_ns <= now < ends_ns`, an exact `(Source, Action)` match in `Policy.rules`, and exact equality between caller-reported `(api_weight, websocket_messages)` and the rule's configured values (`Error::Policy` or `Error::Identity` otherwise). Trading commands, read-reconciliation queries, and command-session control actions carry `0` wire-reported weight and rely strictly on explicit rule `costs`.
* **Action-class isolation across `ORDER`, `CANCEL`, and `QUERY` budgets**: `classify_command_action` and `classify_read_action` partition Binance Spot and USD-M sends into `ActionBudgetClass`:
  * Order-counting actions (`New`, `CancelReplace`, `AdvancedCancelReplace`, `PeggedNew`, `Sor`, USD-M `Algo`, USD-M `Modify`, and `SpotList { legs, .. }` charging `legs` order units) debit account `ORDER` windows (`100/10s` + `200,000/86,400s` on Spot; `300/10s` + `1,200/60s` on USD-M) transactionally alongside the chosen `local_ip`'s `60s` weight window (`6,000/60s` on Spot; `2,400/60s` on USD-M, with `0` IP weight on USD-M WebSocket order/cancel/modify).
  * Spot `Amend` (`/api/v3/order/amend/keepPriority`) charges IP weight `4` with `counts_as_order = false`, whereas USD-M `Modify` (`PUT /fapi/v1/order`) charges `counts_as_order = true`.
  * Pure cancel actions (`Cancel`, `CancelList`, `CancelAlgo`) and pure query/reconciliation actions (`Query`, `ReadAction`) **never** touch account `ORDER` windows, guaranteeing cancel and status availability when order-placement caps are saturated.
* **Idempotent zero-byte retries vs freeze-on-conflict**: `Endpoint` hashes `"tickwire.binance-quota.charge.v1\0" || policy.generation || key || action || weights || content || class || costs` into a 32-byte `digest`. `MultiIpCommandQuota::consume_command_for_ip` and `consume_intent_for_ip` key retries by `(intent_exec_epoch, intent_id, transport, local_ip)`: repeating a zero-byte `WouldBlock` send on the same link returns `Consumed::Duplicate` without double-debiting any window, failing over to a sibling `local_ip` charges that sibling's IP budget while enforcing identical `digest` across `(intent_exec_epoch, intent_id)`, and any conflicting `digest` or `ClockDomain` mismatch immediately freezes the owner.
* **Irreversible command consumption inside the first-byte gate**: `consume_command` and `consume_command_for_ip` execute inside `GateAuthority`'s fresh first-accept check (`now < command.deadline`, never cached across turns). Once debited, quota units remain consumed even if the subsequent socket write accepts zero bytes, encounters a venue rejection, or ends in an unknown outcome; if a multi-window `allow` rejects (such as an exhausted per-IP weight window on a multi-leg `SpotList`), `RateLimiter` rolls back all earlier window debits in that call so zero account `ORDER` units leak.

## Transitions and Failure Semantics

During normal operation, the supervisor inspects `Endpoint::consumer()` to issue new `Grant` capabilities (`install`) and commit `LeaseRenewal` updates (`renew`), upgrades endpoint rules via `prepare_policy` / `commit_policy` without clearing recorded `identities`, or provisions a multi-egress `MultiIpCommandQuota` via `bootstrap::MultiIpBootstrapConfig`. When a permit adapter returns `false`, `Endpoint::last_error()` retains the exact `Policy`, `Identity`, `Capacity`, or `Quota` rejection reason while denying the outbound write before its first byte.

In single-IP `Endpoint` mode, HTTP `429` or `418` freezes the shared `ExecLimiter`. In `MultiIpCommandQuota`, `observe_http_status(local_ip, 429, retry_until_ns)` (`note_ip_rate_limited`) isolates the cooldown strictly to `local_ip` so sibling `local_ip` routes continue admitting orders, cancels, and queries (`can_afford_ip` / `consume_for_ip`), whereas `418` (`note_account_banned`) freezes the entire account across all IPs, and `reconcile_rate_limit_header` (`reconcile_ip_weight_header`, `reconcile_order_count_header`) conservatively raises local window usage and signals `warn` at `exch_warn_pct` (default `80%`).

## Verification and References

```sh
cargo test --locked -p tickwire-binance-quota --all-targets
```

Unit and integration tests verify policy validation and hot policy upgrades, exact retry idempotency across all identity families, multi-IP `429` cooldown isolation vs `418` account ban freeze, `SpotList` multi-leg transactional rollback, `ORDER` vs `CANCEL`/`QUERY` budget isolation, and exchange rate-limit header reconciliation.

* [Binance quota endpoint, `MultiIpCommandQuota`, and permit adapters](../../crates/tickwire-binance-quota/src/lib.rs)
* [Declarative single-IP and multi-IP bootstrap configurations](../../crates/tickwire-binance-quota/src/bootstrap.rs)
* [Permit deduplication, policy upgrade, and freeze tests](../../crates/tickwire-binance-quota/tests/permits.rs)
* [Multi-source-IP isolation, `SpotList` rollback, and `429`/`418` tests](../../crates/tickwire-binance-quota/tests/multi_ip_isolation.rs)
* [Core durable grant, `ExecLimiter`, and `RateLimiter` specification](quota.md)
