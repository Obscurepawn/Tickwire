# USDⓈ-M Income and Account Observations

Derivative wallets experience cash and collateral changes outside individual order executions—most notably periodic `FUNDING_FEE` settlements and asynchronous `ACCOUNT_UPDATE` WebSocket snapshots alongside `REALIZED_PNL` and `COMMISSION` income rows. `CanonicalAccountInput` (`tickwire-venue`) carries these normalized wallet and income observations into `Account` (`tickwire-account`) independently of order-level `CanonicalEvent` records, preventing unexplained wallet movements, pre-cutoff income rows, or duplicate balance-change deltas from silently corrupting trading margin.

## State Model, Record Tags, and Bounds

| Artifact / Mechanism | Limit or Record Tag | Contract |
| --- | --- | --- |
| `CanonicalAccountInput` | ≤ 60 KiB; ≤ 64 assets, ≤ 128 positions | Encodes normalized USDⓈ-M income rows and sparse `ACCOUNT_UPDATE` snapshots (`ExternalTrade` is encodable on the wire but rejected by the `Account` reducer). |
| `IngressAccountInput` | Account record **tag `12`** | Applies one validated `CanonicalAccountInput` tied to `(input_stream, input_sequence)`. |
| `IncomeBaseline` | Account record **tag `13`**; ≤ 4,096 unique rows | Establishes genesis income covered set for a derivative wallet before orders or post-baseline income rows are applied. |
| `AccountInputsReconciled` | Account record **tag `14`** | Advances eligible post-baseline `Booked` funding rows to `Reconciled` in one atomic `ledger_sequence` step without enlarging the baseline covered set. |
| `IncomeObservationsAssociated` | Account record **tag `15`** | One-use cryptographic association clearing all currently unexplained funding `ACCOUNT_UPDATE` observations on a single Cross wallet while preserving cash, caps, holds, and order state. |

## Core Invariants

* **Unique `(incomeType, tranId)` identity and strict baseline cutoff**: Every income row is keyed per account by `(incomeType, tranId)` and binds exact asset version, wallet, symbol, trade ID, signed amount, timestamp, and `info` string. Reusing `(incomeType, tranId)` with altered fields returns `Error::Conflict` and freezes `Account`. Replaying a row inside the approved `IncomeBaseline` covered set leaves cash unchanged, whereas any previously unseen row dated *before* the baseline cutoff remains unresolved and blocks `apply_reactivation`.
* **Conservative funding and asymmetric unmatched holds**: Post-baseline `FUNDING_FEE` books its signed cash movement once—debits reduce capital immediately, while unattributed credits increase wallet cash paired with an equal hold on available capital (`credit_holds`) so unattributed inflows cannot back new risk until tag `14` reconciles them. `REALIZED_PNL` and `COMMISSION` match local fill components on `(symbol, native_trade_id, asset, exact_amount)` so fill-first and income-first arrivals each change cash once; unmatched debits reserve funds immediately (`pending_account_debit`) while unmatched credits remain unavailable.
* **`ACCOUNT_UPDATE` caps availability without double-booking**: `ACCOUNT_UPDATE` retains sparse balance and position entries alongside `reason`. A lower exchange-reported wallet balance bounds available capital immediately via `caps`, balance-change deltas alone never create a second cashflow, exact duplicates add no debit, and any unexplained observation keeps `Account` frozen until reconciled or associated.

## State Transitions and Failure Semantics

1. **Income reconciliation (`Account::reconcile_account_inputs`, Tag `14`)**: Verifies `scope`, `account_epoch`, `ledger_sequence`, `baseline_digest`, `history_digest`, state digest, and non-zero query/snapshot digests, transitioning eligible post-baseline `Booked` funding rows to `Reconciled` while preserving cash, credit holds, capital caps, and open-order liabilities. Pending debits, unresolved observations, or mismatched digests reject the closure and freeze `Account`.
2. **Funding observation association (`Account::associate_income_observations`, Tag `15`)**: Verifies strictly sorted `FUNDING_FEE` transaction IDs and matching `ACCOUNT_UPDATE` digests against `state_digest`, clears only the approved unexplained observations, consumes the association ID once, and increments `ledger_sequence` by `1` while leaving balances, caps, holds, ownership, and orders untouched.

## Verification and References

```sh
cargo test --locked -p tickwire-account --test account_inputs
cargo test --locked -p tickwire-account-codec --test account_inputs
```

* Canonical types and reducers: [account.rs](../../crates/tickwire-venue/src/account.rs), [account_inputs.rs](../../crates/tickwire-account/src/account_inputs.rs), [codec.rs](../../crates/tickwire-account-codec/src/codec.rs)
* Test suites and related specs: [account_inputs.rs](../../crates/tickwire-account/tests/account_inputs.rs), [Stateless account and OrderManager](account.md), [Account baseline and record codec](account-codec.md), [Risk arithmetic and flat RMS](risk-core.md)
