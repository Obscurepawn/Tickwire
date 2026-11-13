# tickwire-account-journal

`tickwire-account-journal` (`#![forbid(unsafe_code)]`) owns ordered persistence and replay of `CanonicalBaseline` and
`CanonicalRecord` history on top of `tickwire-journal`. `AccountGateway` and recovery tools drive `AccountJournal` to
publish immutable bootstrap/baseline files, append sequenced account records, and reconstruct both `Account` state and
`HistoryFrontier` digests on reopen. Its `AppendReceipt` proves local durability only—trading authority, lease
validity, and exchange reconciliation live above this layer.

## Invariants and Change Guidelines

* **Bootstrap and baseline immutability under one lock**: `FileBackend` in [src/backend.rs](src/backend.rs) relies on
  `FileStore` 's single-writer directory lock and writes `ACCOUNT-BOOTSTRAP.json` and `ACCOUNT-BASELINE.cbor` with
  `create_new(true)`, `sync_all()` on the file, and `sync_all()` on the parent directory; never overwrite or silently
  truncate published identity files (`src/identity.rs` in [src/identity.rs](src/identity.rs)).
* **Hash-chained `HistoryFrontier` and monotonic sequences**: Maintain contiguous 1-based record sequences and rolling
  frontier digests in [src/history.rs](src/history.rs) across both append and recovery; poison the journal handle
  immediately if an I/O or physical WAL failure leaves in-memory state uncertain relative to disk.
* **Writer epoch progression and idempotent deduplication**: Enforce non-decreasing `writer_epoch` floors across
  restarts while preserving original `(epoch, intent_id)` admission identities in `admission_origins`, and deduplicate
  repeated admissions, closures, associations, and ingress events by exact canonical digest (`Error::Conflict` on
  mismatch).
* **Verifier-gated replay**: During reopen in [src/lib.rs](src/lib.rs), pass every admission, handover, income
  association, uncreated-algo closure, and reactivation record through `RecoveryVerifier` against the exact preceding
  `HistoryFrontier` before advancing the reconstructed `Account`.

## Verification and Specifications

* **Source entrypoints**: [src/lib.rs](src/lib.rs) · [src/backend.rs](src/backend.rs) ·
  [src/history.rs](src/history.rs) · [src/identity.rs](src/identity.rs)
* **Integration tests and fixtures**: [tests/persistence.rs](tests/persistence.rs) ·
  [tests/ingress.rs](tests/ingress.rs) · [tests/income_closure.rs](tests/income_closure.rs) ·
  [history-vectors.json](tests/data/history-vectors.json)
* **Specifications and models**: [account-journal.md](../../specs/implementation/account-journal.md) ·
  [account_journal](../../tla/execution_models/account_journal/ROADMAP.md) ·
  [cash_evidence](../../tla/execution_models/cash_evidence/ROADMAP.md)

```sh
cargo test --locked -p tickwire-account-journal --all-targets
./scripts/check-tla.sh account_journal
```
