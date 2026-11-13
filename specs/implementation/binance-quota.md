# Binance Endpoint Request Quotas

`tickwire-binance-quota` maps typed Binance network actions—HTTPS clock calibration, private user-data session control, public market-data streams/snapshots, read-only account reconciliation queries, WebSocket order-entry session control, guardian connects, and signed trading commands—onto a single shared local `tickwire_quota::ExecLimiter` ([quota.md](quota.md)). Built in safe Rust (`#![forbid(unsafe_code)]`), `Endpoint` enforces policy lifetimes, validates caller-reported wire weights against explicit policy rules, constructs collision-free domain-separated attempt keys, and exposes read-only `consumer()` metadata so the supervisor can issue grants and renew leases without duplicating state.

`Endpoint` owns local request classification and attempt-identity deduplication, while venue rate-limit definitions, grant sizing, lease renewal schedules, and distribution of authoritative venue feedback belong to the control plane.

## Policy Model and Bounds

Each `Policy` binds a nonzero monotonic `generation`, an active `[starts_ns, ends_ns)` monotonic validity window (`starts_ns < ends_ns`), and `1..=256` (`MAX_RULES`) unique `(Source, Action)` rules. Each `Rule` specifies `Class` (`Normal` vs `Recovery`), expected `api_weight`, expected `websocket_messages`, and `1..=16` (`quota::MAX_COSTS`) strictly sorted, nonzero `Cost { window, units }` entries. `prepare_policy` and `commit_policy` atomically install a newer policy generation (`policy.generation > current`, `ends_ns > now`) while retaining all previously recorded attempt identities.

| Resource or Family | Bound / Key Tag | Bound Identity Fields |
| --- | --- | --- |
| `identities` table | `<= 4,096` (`MAX_IDENTITIES`) | Maps `(key -> digest, AttemptId { producer, sequence })`; rejects `4,097th` distinct attempt with `Error::Capacity`. |
| `TimePermit` (`tag 1`) | `TimeConnect`, `TimeQuery` | `owner_id`, `source`, `connection_epoch`, `action`, `api_weight`. |
| `ControlPermit` (`tag 2`) | Spot/USD-M private stream & listen-key ops | `owner_id`, `AccountScope` (incl. `credential_generation`), `connection_epoch`, `attempt`, `digest`, `bytes`, optional `subscription_id`. |
| `PublicPermit` (`tag 3`) | `Stream` / `Snapshot` lanes (`Connect`, `Upgrade`, `Subscribe`, `Pong`, `Close`, `Snapshot`) | `owner_id`, `source`, `connection_epoch`, `operation`, `action`, `api_weight`, fixed WS message count (`1` for `Subscribe`/`Pong`/`Close`). |
| `Command` (`tag 4`) | REST / WS order, cancel, list, algo, peg, SOR | `owner_id`, `source`, `risk_domain_id`, `account`, `intent_exec_epoch`, `intent_id`, `transport`, `command.digest()`. |
| `ReadPermit` (`tag 5`) | `Connect`, `TradePage`, `AccountSnapshot`, `OpenOrders`, `ConditionalOrders`, `OrderQuery`, `Income`, `StructureQuery`, `AmendmentHistory` | `owner_id`, `AccountScope`, query family, `request_epoch`, operation phase, and exact query parameters. |
| `Guardian` / `CommandSession` (`tags 6, 7`) | Guardian connect & WS trading control (`Connect`, `Upgrade`, `Ping`, `Pong`, `Close`) | `owner_id`, `source`, `SessionIdentity` (`input_stream`, `execution_instance`, `writer_epoch`, `connection_epoch`, `operation`, stream/baseline/credential digests). |

## Core Invariants

* **Strict rule and weight agreement**: Every `consume_*` call requires `starts_ns <= now < ends_ns`, an exact `(Source, Action)` match in `Policy.rules`, and exact equality between caller-reported `(api_weight, websocket_messages)` and the rule's configured values (`Error::Policy` or `Error::Identity` otherwise). Trading commands, read-reconciliation queries, and command-session control actions carry `0` wire-reported weight and rely strictly on explicit rule `costs`.
* **Idempotent zero-byte retries vs freeze-on-conflict**: `Endpoint` hashes `"tickwire.binance-quota.charge.v1\0" || policy.generation || key || action || weights || content || class || costs` into a 32-byte `digest`. Retrying an existing `key` (such as after a zero-byte `WouldBlock` transport poll) reuses its original `AttemptId` and re-verifies the original grants and authorization deadline in `ExecLimiter` if and only if `digest` matches; any `key` reuse with a differing `digest`—or any `ClockDomain` mismatch—immediately calls `limiter.freeze()` and fails with `Conflict` / `Identity`.
* **Irreversible command consumption inside the first-byte gate**: `consume_command` executes inside `GateAuthority`'s fresh first-accept check (`now < command.deadline`, never cached across turns) and deduplicates by `(risk_domain_id, account, intent_exec_epoch, intent_id, transport)` plus `command.digest()`. Once debited, quota units remain consumed even if the subsequent socket write accepts zero bytes, encounters a venue rejection, or ends in an unknown outcome.

## Transitions and Failure Semantics

During normal operation, the supervisor inspects `Endpoint::consumer()` to issue new `Grant` capabilities (`install`) and commit `LeaseRenewal` updates (`renew`), or upgrades endpoint rules via `prepare_policy` / `commit_policy` without clearing recorded `identities`. When a permit adapter (`TimePermit`, `ControlPermit`, `PublicPermit`, or `ReadPermit`) returns `false`, `Endpoint::last_error()` retains the exact `Policy`, `Identity`, `Capacity`, or `Quota` rejection reason for diagnostics while denying the outbound write before its first byte. Observing HTTP or WebSocket status `429` (`RateLimited`) or `418` (`Banned`) on public `HttpFeedback` or trading session feedback—or encountering any `ClockDomain` mismatch or conflicting retry digest—immediately freezes the shared `ExecLimiter`, halting all subsequent charges until supervisor intervention; `Endpoint::stop(self)` consumes the owner and yields its final `StoppedReceipt`.

## Verification and References

```sh
cargo test --locked -p tickwire-binance-quota
```

Unit and permit integration tests verify policy validation and hot policy upgrades, exact retry idempotency across all seven identity families, immediate consumer freeze on conflicting attempt digests or `429`/`418` responses, 4,096-entry capacity bounds, and read/command scope isolation.

* [Binance quota endpoint, policy rules, and permit adapters](../../crates/tickwire-binance-quota/src/lib.rs)
* [Permit deduplication, policy upgrade, and freeze tests](../../crates/tickwire-binance-quota/tests/permits.rs)
* [Core durable grant and `ExecLimiter` specification](quota.md)
