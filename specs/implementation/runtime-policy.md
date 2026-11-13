# Order Submission Policy

`ControlPlane` (`tickwire-runtime-policy`) enforces supervisor leases, strategy action permissions, market-stream freshness, and private-stream reconciliation inside a single event-loop thread. Shared handles within that thread coordinate over `Rc<RefCell<State>>` (forbidding `unsafe` code and returning `Error::Busy` on reentrant borrows, while `detached_copy()` provides an isolated snapshot for side-effect-free preflight checks). Right before the transport accepts the first plaintext byte of an admitted order, `PolicyGate` samples monotonic time—backed in production by `tickwire_clock_sys::SystemClock`—and evaluates `ControlPlane` to produce a one-shot `Gate`.

Separating dynamic runtime readiness (`ControlPlane`) from financial ledger math (`Account`) and wire framing (`Encoder`) keeps authorization fail-closed: permissions default to zero, leases and generations advance monotonically, and any clock anomaly, market gap, or private-stream disconnect immediately revokes risk-increasing output without corrupting account balances. Request signing and venue rate-limit quotas remain owned by the exchange adapter at send time, while durable admission and reservations remain owned by `AccountGateway`.

## State Model and Bounds

Configuration fixes `Scope`, `Product`, `executor_id`, initial `owner_generation` and `writer_epoch`, `ClockDomain`, `Versions` (`config_revision`, `risk_policy_version`, `order_config_version`, `catalog_version`, `catalog_hash`), and pre-validated memory bounds:

| Resource | Hard Upper Bound | Retention and Ordering Rule |
| --- | --- | --- |
| Registered strategies | `MAX_STRATEGIES = 1,024` | Keyed by nonzero `logical_strategy_id`; tracks highest `strategy_generation`, `authorization_epoch`, `last_instance`, and revocation latch. |
| Tracked market streams | `MAX_FEEDS = 256` | Keyed by `FeedKey { topic_id, instrument_id }`; tracks `feed_generation`, `ring_epoch`, `instrument_version`, and `live` flag. |
| Publication marks per stream | `MAX_FEED_MARKS = 1,024` | Bounded `VecDeque<(sequence, published_at_ns)>`; evicting an old mark automatically invalidates any command trigger referencing that sequence. |
| Required streams per strategy | `MAX_REQUIRED_FEEDS = 8` | Strictly ascending `FeedKey` slice inside `StrategyGrant`; mandatory (`!is_empty()`) whenever `submit` or `amend` is enabled. |

Supervisor grants comprise three distinct capabilities:

* **`OwnerLease`**: Binds `executor_id`, `owner_generation`, `writer_epoch`, monotonic `lease_epoch`, `ClockDomain`, `valid_until_ns`, and a nonzero 32-byte `isolation_record`.
* **`StrategyGrant`**: Binds `logical_strategy_id`, 16-byte `instance_id`, `strategy_generation`, `authorization_epoch`, `config_revision`, `valid_until_ns`, `Actions { submit, cancel, query, amend }`, and `required_feeds` (`max_age_ns`, `max_sequence_lag`, `instrument_version`).
* **`ManagementGrant`**: Authorizes `cancel` and/or `query` across a sorted list of logical strategy IDs, surviving revocation of an individual worker's `StrategyGrant` so orphaned orders can still be canceled or inspected.

## Core Invariants

* **One-way clock fault closure**: A failed clock sample, backward monotonic reading (`now < last_mono`), or mismatched `ClockDomain` permanently latches `faulted = true` (as does `ControlPlane::halt()`, which also sets `Mode::Stopped`), closing the current owner against all further grants.
* **Monotonic epochs and instance-scoped worker stops**: `lease_epoch`, `control_revision`, `authorization_epoch`, and `strategy_generation` never move backward. Once a strategy generation is revoked (`revoke_strategy`, `stop_strategy`, or `stop_strategy_instance`), re-enabling that logical strategy requires both a strictly higher `authorization_epoch` and a newer `(strategy_generation, instance_id)`. Calling `stop_strategy_instance` checks exact `(instance_id, strategy_generation)` equality so a delayed exit notification from an old worker process cannot revoke a newly spawned replacement worker.
* **Atomic writer replacement resets trading permissions**: `replace_writer` atomically installs a higher `writer_epoch` and `OwnerLease` while forcing `Mode::Stopped` and revoking all active `StrategyGrant`s, `ManagementGrant`, and private-stream reconciliation until the new owner explicitly reconfigures and reconciles them.
* **Generation-latched market feed invalidation**: When `NodeObserver` consumes an unavailable (`!live`) market record or receives `invalidate_feed`, `feed.live` becomes `false` for the current `(feed_generation, ring_epoch)`. The observer cursor continues advancing through the trailing stale quote and any later complete quotes on that generation, but **only** an observation arriving on a strictly newer `feed_generation` or `ring_epoch` clears old marks and reopens `feed.live = true`. Any backward sequence, regressed timestamp, regressed generation/ring epoch, or mismatched `instrument_version` marks `feed.live = false` and rejects the update.

## State Transitions and Failure Semantics

### Evaluating a First-Byte `Gate`

When `PolicyGate::current` evaluates `(command, admission)` at monotonic `now`:

1. **Identity and lease checks**: `evaluate` verifies clock monotonicity, `Mode != Mode::Stopped`, exact `Scope`, `Product`, `writer_epoch == intent_exec_epoch`, `config_revision`, `created_mono_ns <= now < deadline`, `admission` digest/intent match, and an unexpired `OwnerLease`.
2. **Administrative and reducing-amendment path**: `Cancel`, `CancelList`, `CancelAlgo`, and `Query` may be authorized by either a matching `StrategyGrant` or `ManagementGrant`. `AmendKeepPriority` requires an active `StrategyGrant` with `Actions.amend == true` (`ManagementGrant` cannot authorize it), and skips market/private trading checks *only* when its closed `Admission` satisfies `!admission.increases_risk()` (`Modify` and `CancelReplace` remain subject to full trading qualification).
3. **New-exposure trading path**: All risk-increasing commands require `Mode::Trading`, exact `Versions` equality (`risk_policy_version`, `order_config_version`, `catalog_version`, `catalog_hash`), an active `StrategyGrant` whose `required_feeds` match the command's trigger references one-for-one within `max_sequence_lag` and `published_at + max_age_ns`, and a connected, reconciled private source (`ReconciliationRequest` / `ActivationProof` verified via `ReconciliationVerifier`) matching the current `connection_epoch`, `GapToken` `gap_generation`, continuous physical input-WAL `through_sequence`, `account_epoch`, and `ledger_sequence <= admission.ledger_sequence()`. If `Account` freezes or the input stream experiences a gap, disconnect, or sequence regression, private reconciliation is immediately revoked and `revocation` increments so an in-flight `complete_reconciliation` call cannot install stale proof.

The resulting `Gate.lease_until` is clamped to the earliest expiration across `OwnerLease`, the authorizing strategy/management grant, private reconciliation validity, and every required market trigger's `published_at + max_age_ns`.

## Verification and References

```sh
cargo test --locked -p tickwire-runtime-policy --all-targets
```

* Implementation: [ControlPlane, PolicyGate, and NodeObserver](../../crates/tickwire-runtime-policy/src/lib.rs), [policy bootstrap helpers](../../crates/tickwire-runtime-policy/src/bootstrap.rs), [Linux monotonic system clock](../../crates/tickwire-clock-sys/src/lib.rs)
* Related specifications: [account gateway](account-gateway.md), [spot keep-priority quantity reduction](spot-amend.md), [account reconciliation and terminal fee maintenance](reconciliation-controller.md)
