# USDⓈ-M Income and Account Observations

Derivative wallets experience cash and collateral changes outside individual order executions—most notably periodic `FUNDING_FEE` settlements and asynchronous `ACCOUNT_UPDATE` WebSocket snapshots alongside `REALIZED_PNL` and `COMMISSION` income rows. `CanonicalAccountInput` (`tickwire-venue`) carries these normalized wallet and income observations into `Account` independently of order-level `CanonicalEvent` records. For every observation, the Binance input owner synchronizes the authenticated raw wire payload and canonical normalization decision to the input WAL *first*, validates the resulting transition on a candidate `Account`, synchronizes the corresponding `AccountJournal` record, and only then publishes the updated in-memory `Account`.

Separating raw input durability from candidate ledger validation ensures financial evidence is never lost on validation errors while preventing unexplained wallet movements, pre-cutoff income rows, or duplicate balance-change deltas from silently corrupting trading margin.

## State Model, Record Tags, and Bounds

| Artifact / Mechanism | Limit or Record Tag | Contract |
| --- | --- | --- |
| `CanonicalAccountInput` | ≤ 60 KiB; ≤ 64 assets, ≤ 128 positions | Encodes normalized USDⓈ-M income rows and sparse `ACCOUNT_UPDATE` snapshots (`ExternalTrade` is encodable on the wire but rejected by the `Account` reducer). |
| `IncomeReader` & `IncomeScan` | ≤ 16 rows/page; ≤ 128 queries/scan | Shares the TLS connection, calibrated signature clock, and first-byte quota gate; splits saturated inclusive `[start, end]` millisecond windows and marks the scan incomplete if a single millisecond saturates. |
| `IngressAccountInput` | Account record **tag `12`** | Appends one validated `CanonicalAccountInput` tied to `(input_stream, input_sequence)`. |
| `IncomeBaseline` | Account record **tag `13`**; proposal ≤ 54 KiB, ≤ 128 rows, ≤ 128 refs | Ed25519-signed genesis income covered set verified by `IncomeBaselineAuthority` across 3 matching remote snapshots (account, open orders, conditional orders) spanning zero account/order changes. |
| `AccountInputsReconciled` | Account record **tag `14`** | Advances eligible post-baseline `Booked` funding rows to `Reconciled` in one atomic `ledger_sequence` step without enlarging the baseline covered set. |
| `IncomeObservationsAssociated` | Account record **tag `15`** (`6` approval dependencies) | One-use cryptographic association clearing all currently unexplained funding `ACCOUNT_UPDATE` observations on a single Cross wallet; rejected by generic `record_fact`. |

## Core Invariants

* **Unique `(incomeType, tranId)` identity and strict baseline cutoff**: Every income row is keyed per account by `(incomeType, tranId)` and binds exact asset version, wallet, symbol, trade ID, signed amount, timestamp, and `info` string. Reusing `(incomeType, tranId)` with altered fields is a conflict. Replaying a row inside the approved `IncomeBaseline` covered set leaves cash unchanged, whereas any previously unseen row dated *before* the baseline cutoff remains unresolved and blocks reactivation (matching wallet totals, short pages, or Binance asset update timestamps can never manufacture baseline coverage).
* **Conservative funding and asymmetric unmatched holds**: Post-baseline `FUNDING_FEE` books its signed cash movement once—debits reduce capital immediately, while unattributed credits increase wallet cash paired with an equal hold on available capital so unattributed inflows cannot back new risk, keeping `Account` frozen until tag `14` reconciles them. `REALIZED_PNL` and `COMMISSION` match local fill components on `(symbol, native_trade_id, asset, exact_amount)` so fill-first and income-first arrivals each change cash once; unmatched debits reserve funds immediately while unmatched credits remain unavailable.
* **`ACCOUNT_UPDATE` caps availability without double-booking**: `ACCOUNT_UPDATE` retains sparse `B`/`P` entries, `E`/`T` timestamps, balance-change (`bc`) deltas, and raw position figures. A lower exchange-reported wallet balance bounds available capital immediately, `bc` alone never creates a second cashflow, exact duplicates add no debit, and any unexplained observation keeps `Account` frozen.
* **Two-WAL auditability on baseline import**: `RestOwner::import_income_baseline` binds the risk administrator's Ed25519 signature to the request, baseline, stream IDs, writer epoch, `ledger_sequence`, clock, and validity, referencing the input prefix prior to approval while `through_sequence` covers the synced approval. Until tag `13` reaches the account WAL, offline audits surface the pending import in `input_facts_not_in_account` (reopen tests verify `1` unmatched effect after input-WAL sync and `0` after account-WAL sync, with recovered `Account` remaining frozen).

## State Transitions and Failure Semantics

### Automated Baseline Import and Periodic Income Scans

When configured, `reconciliation.income_baseline_import` executes bounded income queries, collects the three remote snapshots, and polls without duplicate network queries for the administrator's signed approval, then collects fresh snapshots at the resulting `ledger_sequence`. During live operation on a wallet that already has an approved `IncomeBaseline`, `income_account_scan` queries a configured lookback window in `Account` mode (persisting the raw HTTP response, normalized rows, and account effects in a single input-WAL batch).

Committing tag `14` (`AccountInputsReconciled`) binds `scope`, `account_epoch`, `ledger_sequence`, `baseline_digest`, `history_digest`, input cut, and verified query/snapshot digests, transitioning eligible post-baseline `Booked` funding rows to `Reconciled` while preserving cash, credit holds, capital caps, and open-order liabilities. Missing objects, `Pending` debits, unknown observations, conflicts, pre-cutoff keys, or mismatched snapshots fail validation; subsequent `Reactivated` eligibility always requires a fresh set of all three snapshots at the new `ledger_sequence`.

### Explicit Cross-Wallet Funding Association (Tag `15`)

When asynchronous `ACCOUNT_UPDATE` observations reflect Cross-wallet funding movements that require explicit operator attestation, `associate_income_observations` validates a leaf-signed manifest chained through a signed leaf certificate and active policy digest/revision back to the deployment root key:

1. **Strict prefix isolation**: Replay derives the opening wallet from `CanonicalBaseline` and the ordered account prefix. Between the anchor and cut, *only* the selected funding fees and observations, exact duplicates, an existing tag `14`, and verified reactivations may appear—even an unrelated transaction that nets to zero invalidates the selection. Every row must satisfy per-asset `bc`, wallet, and cross-wallet balance equations, originate on the same private `connection_epoch`, carry empty position changes, have complete `Account`-mode query coverage, and match fresh snapshots at the current ledger/input cut.
2. **Input sync before candidate check before account sync**: `tickwire-app` synchronizes the policy and approval alongside acceptance time and writer epoch in the input WAL, validates tag `15` on a candidate `Account`, and synchronizes tag `15` to `AccountJournal`. Applying tag `15` clears only the approved observations, consumes the association ID once, and increments `ledger_sequence` by `1` while leaving cash, caps, holds, reservations, ownership, orders, and baseline untouched.
3. **Replay stability and Unix file intake**: Historical replay after policy expiration or leaf-key rotation verifies against the original authority bound at acceptance time, and a new writer epoch may finish an input-only record solely from the identical account prefix. The optional association scheduler reads bounded regular files using `O_NOFOLLOW | O_NONBLOCK` on Unix (waiting while files are absent), then collects fresh snapshots, applies tag `14` if `Booked` rows remain, and acquires a final snapshot triple before `Reactivated`.

Unknown income types keep `Account` frozen. Liquidation, auto-deleveraging (ADL), contract delivery settlement, external transfers, per-strategy funding attribution, root-key rotation, and an external approval-signing service remain outside this implementation.

## Verification and References

```sh
cargo test --locked -p tickwire-venue -p tickwire-account -p tickwire-binance-account
cargo test --locked -p tickwire-app
```

* Canonical types and reducers: [CanonicalAccountInput](../../crates/tickwire-venue/src/account.rs), [AccountGateway tag 12–15 methods](../../crates/tickwire-account-gateway/src/lib.rs), [account record codec](../../crates/tickwire-account-codec/src/codec.rs)
* Binance income readers and schedulers: [income reader and millisecond-splitting scan](../../crates/tickwire-binance-account/src/reconciliation/income.rs), [Ed25519 baseline import authority](../../crates/tickwire-binance-account/src/reconciliation/income_import.rs), [app association scheduler](../../crates/tickwire-app/src/reconciliation/association.rs), [app income reconciliation](../../crates/tickwire-app/src/reconciliation/income.rs)
* Historical evidence and related specs: [historical offline income replay validation](https://github.com/Obscurepawn/Tickwire/tree/c6ce8c6d13ba87b3ef93f8c3e1d04500068a198a/docs/validation/offline-income-replay), [multi-asset account state machine](account.md), [account reconciliation and terminal fee maintenance](reconciliation-controller.md)
