# Ordered Multi-Asset Account Journal, History Frontier, and Replay

`tickwire-account-journal` binds a single multi-asset `AccountScope` (`risk_domain_id`, `venue`, `account`, `environment`), physical WAL configuration, initial `writer_epoch`, and immutable `CanonicalBaseline` into an ordered durable log (`AccountJournal<S: Backend>`). Every canonical `RecoveryRecord` occupies one physical `DataRecord` starting at sequence `1` (`record.sequence() == physical_seq`), while the reducer's `ledger_sequence` advances independently on state-mutating account transitions.

`AccountJournal` owns atomic bootstrap/baseline publication, live retry deduplication, cryptographic `HistoryFrontier` chaining, and verified crash replay through `RecoveryVerifier`. An `AppendReceipt` proves only that an account record belongs to a locally synchronized WAL prefix; `tickwire-recovery` and the account gateway remain responsible for recovery-closure synchronization and live order-sending leases.

## State Model and Bounds

`FileBackend` wraps `tickwire-journal::store::FileStore` inside a private `0700` canonicalized directory whose single-writer `LOCK` protects every file in the root. Account creation publishes `ACCOUNT-BASELINE.cbor` (`0600`, `create_new` + `sync_all` + directory `sync_all`), then `ACCOUNT-BOOTSTRAP.json`, and finally initializes the physical `Journal` (`MANIFEST.json` and `segment-00000000000000000001.wal`), refusing to overwrite any existing or partially initialized directory.

| Artifact or State | Layout / Enforced Bound |
| --- | --- |
| `ACCOUNT-BASELINE.cbor` | Canonical `Snapshot` + `Catalog` (`<= 2 MiB`), SHA-256 pinned in `ACCOUNT-BOOTSTRAP.json` and every `CanonicalRecord` |
| `ACCOUNT-BOOTSTRAP.json` | JSON envelope (`<= 64 KiB`, `version = 1`) binding physical `Config`, `AccountScope`, `initial_writer_epoch`, `baseline_digest`, and `sha256` |
| Record capacity & alignment | `1..=MAX_RECORDS` (`MAX_FACTS + MAX_ORDERS`); 1 `CanonicalRecord` (`<= 60 KiB`) per physical `DataRecord` |
| `HistoryFrontier` (`128` bytes) | Magic `"TWAF0001"` + `risk_domain_id` + `journal_epoch` + `writer_epoch` + `sequence` + `baseline_digest` + `chain_hash`; content-addressed by `SHA-256(bytes)` |
| History step hash (`chain_hash`) | `SHA-256("tickwire.account-history-record.v1" \|\| prev_chain \|\| writer_epoch_le \|\| sequence_le \|\| record.digest())` |

## Core Invariants

* **Order- and writer-sensitive `HistoryFrontier`**: Because `HistoryFrontier::advance` hashes `writer_epoch`, `sequence`, and the full `CanonicalRecord` digest at every step, any permutation of records or change in `writer_epoch` yields a distinct 128-byte frontier and `SHA-256` digest—even when final asset balances and open positions match. High-assurance records (`Reactivated`, `AccountInputsReconciled`, `IncomeObservationsAssociated`, `UncreatedAlgoClosed`) must match `self.history.digest()` at the exact point of append.
* **Asymmetric live retry idempotency versus strict WAL uniqueness**: On a live active `AccountJournal`, exact retries of an admission (`IntentKey`), venue/account ingress record (`(input_stream, input_sequence)`), income observation association (`request_id`), or history-pinned closure (`AccountInputsReconciled` / `UncreatedAlgoClosed` keyed by `history_digest`) return the cached `AppendReceipt` without appending to the WAL or advancing `HistoryFrontier`; submitting different content under any of those existing keys immediately sets `poisoned = true` and returns `Error::Conflict`. During physical `recover`, however, *any* duplicate persisted key in the WAL—even a byte-identical copy—is rejected immediately, because a healthy writer never writes a retry twice.
* **Non-consecutive ingress indexing with physical-order replay**: Ingress sequences recorded in `AccountJournal` need not be contiguous (`1, 2, ...`) because the upstream connector input log also carries stream registrations, gap markers, raw query payloads, and quarantine frames. In-memory `(input_stream, input_sequence)` indexes (`ingress_event`, `ingress_events`, `ingress_account_inputs`) support deterministic cross-checks without invoking the reducer, while state replay always executes strictly in physical `AccountJournal` sequence order.
* **All-or-nothing sync and writer poisoning**: `append` validates baseline digest, scope, catalog membership, `writer_epoch`, and `MAX_RECORDS` before touching the store, then calls `physical.append_batch(writer_epoch, &[encoded.bytes()])`. In-memory records, lookup maps, and `HistoryFrontier` advance only after physical `sync_data` succeeds; any storage error sets `poisoned = true` because the batch may or may not have reached disk, requiring a full reopen and recovery before trusting the tail.

## Operations and Failure Semantics

* **Evidence-verified, frozen crash replay and `recovery_writer_floor`**: `AccountJournal::recover` validates `ACCOUNT-BOOTSTRAP.json` against trusted caller parameters, decodes `ACCOUNT-BASELINE.cbor`, and validates the physical WAL plus any independently supplied `PhysicalFrontier` *before* truncating an unsealed final batch. It then steps `HistoryFrontier` from genesis, invoking `RecoveryVerifier` (`verify_admission`, `verify_reactivation`, `verify_handover`, `verify_income_baseline`, `verify_income_association`, `verify_account_inputs_closure`, and `verify_uncreated_algo`) against the exact preceding `HistoryFrontier` and replaying `Account::recover` (which recomputes every `Admit` at its original `evaluated_at` and compares the full admission image). No partial state is exposed on failure. A recovered journal starts with a frozen `Account`, `recovered == true`, and `recovery_writer_floor = Some(last_writer_epoch)`, rejecting new `Admit` appends (`Error::Recovered`) until `append_reactivation` synchronizes a `Reactivated` record from a strictly newer `writer_epoch > floor`.
* **Historical Spot fee-closure compatibility**: Legacy Spot journals may contain a `FeesReconciled` record written after two terminal venue observations reported disagreeing filled quantities. When the target order already records that terminal disagreement and the proposed fee-reconciled quantity equals booked fills while remaining strictly below the retained terminal total, replay preserves the record and its `HistoryFrontier` hash as a rejected local decision—keeping the fee reservation intact and leaving `ledger_sequence` unchanged—while leaving the recovered account frozen pending fresh economic evidence. Unknown orders or unexplained quantities still fail replay.
* **Exclusive multi-directory locking for offline replay and export**: All instances of an account and risk domain share one canonical journal directory; opening a second `FileBackend` (even via symlink) fails immediately on `LOCK`. Offline inspection and export tools acquire exclusive locks across the account, input, and recovery-object directories *before* copying any file, hold all three locks through verification, and unlock explicitly on every exit path without acquiring any order-sending lease.

## Verification and References

```sh
cargo test --locked -p tickwire-account-journal
cargo clippy --locked -p tickwire-account-journal --all-targets -- -D warnings
```

Automated suites verify bootstrap/baseline creation failure modes, admission and ingress retry deduplication, conflict poisoning, duplicate-key WAL rejection, `RecoveryVerifier` history-frontier binding, `recovery_writer_floor` takeover gating, legacy Spot fee-closure replay, and multi-process directory locking.

* Journal state machine, file backend, and history frontier: [lib.rs](../../crates/tickwire-account-journal/src/lib.rs), [backend.rs](../../crates/tickwire-account-journal/src/backend.rs), [history.rs](../../crates/tickwire-account-journal/src/history.rs)
* Test suites: [ingress.rs](../../crates/tickwire-account-journal/tests/ingress.rs), [persistence.rs](../../crates/tickwire-account-journal/tests/persistence.rs)
* Formal verification: [Account journal TLA+ model](../../tla/execution_models/account_journal/PLAN.md)
* Related specifications: [Account baseline and record codec](account-codec.md), [Physical write-ahead log](journal.md), [Recovery closures](recovery-closure.md), [Account gateway](account-gateway.md)
