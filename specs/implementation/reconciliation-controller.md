# Account Reconciliation and Terminal Fee Maintenance

`Controller` (`tickwire-app/src/reconciliation.rs`) drives authenticated read-only REST queries, trade-history pagination, optional USDⓈ-M income scans, and three-snapshot parity proofs for Binance Spot and USDⓈ-M accounts. It serves two distinct roles under one state machine: restoring trading eligibility when an account is frozen or unreconciled, and—on an already-healthy account—periodically closing outstanding terminal-order fee allowances (`FeesReconciled`) and renewing time-bounded trading qualification (`Reactivated`) before expiry.

An HTTP `200` response from a venue endpoint never closes fee reserves, clears unknown order flags, or enables risk-increasing admissions on its own. Every reconciliation or renewal round must persist raw query responses to the input WAL, verify contiguous pagination coverage, match remote snapshots against the exact `Account` ledger sequence and continuous private-stream frontier, and synchronize the resulting proof record in `AccountJournal`.

## State Model, Triggers, and Configuration Bounds

`Config` requires nonzero `reader_id` (matching `ReadBinding.owner_id`), `first_request_epoch`, `first_execution_connection_epoch`, `retry_ns`, `valid_for_ns`, and `fee_maintenance_interval_ns` (defaulting to `1_000_000_000` ns / `1 s`; `0` is rejected at construction). `OrderWork::should_start` initiates a reconciliation round under three independent conditions:

| Trigger Path | Preconditions | Throttling and Sender Rules |
| --- | --- | --- |
| Unhealthy / frozen recovery | `!healthy` (account frozen or private frontier unreconciled) | Runs immediately regardless of `fee_maintenance_interval_ns` or sender idle checks; failed rounds back off by `retry_ns`. |
| Qualification renewal | `now >= valid_until.saturating_sub(valid_for_ns / 2)` | Fires at the halfway mark of the persisted `Account::valid_until()` window—even on an empty account with zero orders and no income scan—once the outbound REST sender is idle; never deferred by the fee timer. |
| Terminal fee maintenance | Healthy account with ≥ 1 non-aborted terminal order holding open fees, idle REST sender, and `now >= maintenance_at` | Waits for `fee_maintenance_interval_ns` after the previous completed round; failures back off by `retry_ns`. |

## Core Invariants

* **Fresh candidate work on every round**: Each round reloads target orders, conditional structures, and amendments directly from the current `Account` state; a new round never reuses a cached empty order set from an earlier round, even when `connection_epoch` and `gap_generation` remain unchanged.
* **Authoritative persisted expiry deadline**: Renewal timing derives strictly from `Account::valid_until()`, established by the genesis baseline or the latest durable `Reactivated` record. Starting a round, waiting on a busy REST sender, failing a query, or scheduling `retry_ns` backoff never extends `valid_until`; once `now >= valid_until`, `ControlPlane` denies new risk-increasing orders immediately.
* **Proven trade coverage before `FeesReconciled`**: Closing an order's remaining fee budget requires querying the order, paginating its trade history contiguously from initial cursor `0`, and confirming that booked fills match reported cumulative quantity.
* **Ledger-advancing closures invalidate earlier snapshots**: Whenever an intermediate step—such as USDⓈ-M `AccountInputsReconciled` (account tag `14`)—advances `Account::ledger_sequence()`, any remote snapshots bound to the earlier ledger sequence are discarded, and all three snapshots (account, open orders, conditional orders) must be re-collected against the new `ledger_sequence` before `Reactivated` can commit.

## State Transitions and Failure Semantics

### Standard Round Execution

During an active round, private WebSocket frames continue to append to the durable input WAL while `Controller` shares rate-limit quotas, pagination limits, and `IoBudget` across its read-only clients (`ReadOnlyOrderReader`, `ReadOnlyAmendmentReader`, `ReadOnlyStructureReader`, `ReadOnlyReconciler`, and `AccountSnapshotReader`):

1. **Begin and query**: After `BeginReconciliation`, the controller queries each loaded order, structure, and amendment, paginates trades from cursor `0`, and commits `FeesReconciled` for qualifying terminal orders.
2. **Optional USDⓈ-M income scan (`income_account_scan`)**: For wallets with an existing approved `IncomeBaseline` (without one, only `income_baseline_import` can establish initial coverage; choosing a `lookback_ms` window never invents a completeness frontier), `Controller` queries `[calibrated_now - lookback_ms, calibrated_now]` within configured `interval_ns`, per-page `limit`, and `maximum_queries`. In `IncomeUse::Account` mode, raw HTTP bytes and per-row effects persist to the input WAL before order/snapshot checks; if eligible `Booked` funding rows reconcile via tag `14`, prior snapshots are dropped and re-collected at the updated ledger sequence. Unknown observations or pending debits—including unexplained transfers whose net balance cancels to zero—keep `Account` frozen and schedule `retry_ns` backoff.
3. **Three-snapshot parity and `Reactivated`**: Once account, open-order, and conditional-order snapshots match the current `Account` state and continuous input prefix, `AccountGateway::reactivate` synchronizes `Reactivated` and updates `ControlPlane` via `ActivationProof`. Success resets per-round cursors and schedules `maintenance_at = now.saturating_add(fee_maintenance_interval_ns)`.
4. **Failure or frontier change**: Any HTTP/transport error (such as a `503` snapshot response) or any mid-round change in private `(connection_epoch, gap_generation)` discards partial per-round state, leaving `Account` frozen and preserving its original `valid_until` deadline across crash replay.

## Verification and References

```sh
cargo test --locked -p tickwire-app -p tickwire-binance-account
```

Loopback TLS integration tests exercise the production `Controller` against a local exchange peer across single-charge funding-fee reconciliation, unexplained transfers held pending, consecutive renewals on an empty account, and `503` snapshot faults followed by reopening both WALs and their recovery closure stores.

* Controller and proof implementations: [reconciliation controller and renewal scheduler](../../crates/tickwire-app/src/reconciliation.rs), [app income scan integration](../../crates/tickwire-app/src/reconciliation/income.rs), [ActivationProof](../../crates/tickwire-binance-account/src/reconciliation/activation.rs), [trade pagination reconciler](../../crates/tickwire-binance-account/src/reconciliation/trades.rs), [remote snapshot verifier](../../crates/tickwire-binance-account/src/reconciliation/snapshot.rs), [account record codec](../../crates/tickwire-account-codec/src/codec.rs)
* Network test suite: [income and renewal loopback TLS tests](../../crates/tickwire-app/src/reconciliation/income_network_tests.rs)
* Related specifications: [multi-asset account state machine](account.md), [account gateway](account-gateway.md), [order submission policy](runtime-policy.md), [USDⓈ-M income and account observations](usdm-account-facts.md), [conditional orders](conditional-orders.md)
