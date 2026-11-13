# Binding Recovery Dependencies to Account Admission

Every order admitted through `AccountGateway` must be reproducible byte-for-byte after a host crash—not merely as an isolated strategy intent, but against the exact opening `CanonicalBaseline` and preceding `AccountJournal` history that justified its financial reservation. `tickwire-recovery` and `tickwire-account-gateway` enforce this provenance by binding three account-level immutable objects into the strategy's `RecoveryManifest` before any `Admit` record is appended to the write-ahead log.

Without binding the account baseline and preceding journal prefix into the recovery closure, a manifest could prove what the worker intended to trade while leaving ambiguous which balance state, fee schedule, or prior order reservations were active when admission succeeded. Requiring a verified, `fsync`-backed capability over both worker and account dependencies turns replay verification into a deterministic local hash check.

## Dependency Model and Bounds

A `RecoveryManifest` binds an admission to up to `MAX_OBJECTS` (`4,096`) content-addressed `DependencyObject` descriptors and `MAX_EDGES` (`16,384`) directed acyclic dependency edges, capped at `MAX_MANIFEST_BYTES` (`1 MiB`), `MAX_OBJECT_BYTES` (`64 MiB` per object), and `MAX_CLOSURE_BYTES` (`256 MiB` aggregate across the closure).

| Dependency Kind | Constant / Role | Content and Structural Requirement |
| --- | --- | --- |
| Kinds `1..=12` | Core worker & execution closure | Retain their standard byte layouts and DAG rules: kind `1` stores the exact `CanonicalRecord` (`Admit`) bytes whose SHA-256 equals `ClosureBinding.business_digest`; kind `4` matches `action_grant_ref`; kind `7` matches `operation_ref`; kind `12` covers contiguous worker input segments across `InputBoundary`. |
| Kind `13` | `COMMAND_DEPENDENCY_KIND` | Complete canonical CBOR bytes of the admitted `CanonicalCommand`. |
| Kind `14` | `ACCOUNT_BASELINE_DEPENDENCY_KIND` | Complete canonical CBOR bytes of `CanonicalBaseline` (opening `Snapshot` and instrument `Catalog`). |
| Kind `15` | `ACCOUNT_HISTORY_DEPENDENCY_KIND` | Fixed 128-byte `HistoryFrontier` summary (`TWAF0001` magic, 16-byte `risk_domain_id`, 16-byte physical `stream` epoch, 32-byte `baseline` digest, `venue`, `account`, `environment`, `record_count`, and 32-byte rolling SHA-256 chain hash). The account WAL stores the underlying records themselves. |

Object descriptors inside `RecoveryManifest.objects` must appear in strictly ascending `(kind, content_hash)` order. Whenever either account dependency kind (`14` or `15`) is present, `validate_shape` requires **exactly one** descriptor each of kinds `13`, `14`, and `15`.

## Core Invariants

* **Exact digest equivalence on `Admit`**: `ClosureBinding.business_digest` equals the SHA-256 digest of the complete kind-`1` `CanonicalRecord` (`RecoveryRecord::Admit`), which embeds the `CanonicalCommand`, original `evaluated_at` timestamp, and expected `AdmissionImage`.
* **Single-pass verification and durability on live publish**: For a new submission, `Persistence::publish_with_objects` validates the manifest shape, checks each supplied slice's byte length, SHA-256 `content_hash`, and kind-`12` input-stream continuity, and synchronizes each dependency object once via `BlobStore::put_sync` before persisting `StoreKey::Manifest(root)` and `StoreKey::Binding(hash)` (`tickwire.manifest-binding.v1`).
* **Complete triple required by `AccountGateway`**: `VerifiedClosure` privately records `command_digest`, `account_baseline_digest`, and `account_history_digest`. `AccountGateway` compares those three digests against `Some(command.digest())`, `Some(baseline.digest())`, and `Some(preceding.digest())`, alongside checking `preceding.record_count() + 1 == record.sequence()`. Any legacy manifest omitting kinds `14` and `15` leaves the account digests `None` and is rejected with `Error::Identity`.
* **Idempotent repair across interrupted directory syncs**: On restart, `Persistence::verify` reads the binding pointer, manifest, and every referenced object from disk, verifies all lengths and SHA-256 hashes, and re-invokes `put_sync` on each object, manifest, and binding before returning `VerifiedClosure`—ensuring durability even if a prior run crashed after atomic file rename but before parent-directory synchronization.

## State Transitions and Failure Semantics

During `AccountGateway::submit`, closure preparation runs *after* candidate `Account::admit` succeeds in memory and *before* `AccountJournal::append` writes to the WAL:

1. If `Authority::prepare` or `binding::closure` fails (missing object, hash mismatch, legacy manifest without kinds `14`/`15`, or disk I/O error), `AccountGateway` halts its sender and returns an error *before* publishing the candidate `Account` or appending `Admit`.
2. Once `VerifiedClosure` is verified and `AccountJournal::append` is invoked, the admission is bound to the exact `HistoryFrontier` (`record_count` and writer-sensitive rolling chain hash over `tickwire.account-history-empty.v1` and `tickwire.account-history-record.v1`).
3. During `AccountJournal::recover`, `RecoveryVerifier::verify_admission` reconstructs the `AdmissionRequest` at each historical `Admit` position from the live `CanonicalBaseline` and the running `HistoryFrontier` accumulated over all preceding WAL records, calls `Persistence::verify`, and then replays `Account` using the recorded `evaluated_at`, `CanonicalCommand`, and `AdmissionImage`. Any tampered baseline, truncated prefix, or mismatched `AdmissionImage` aborts recovery before an `AccountGateway` instance can be constructed.

## Verification and References

```sh
cargo test --locked -p tickwire-recovery --test closure
cargo test --locked -p tickwire-account-gateway --test binance_rest
```

* Recovery closure and frontier implementations: [recovery manifest and Persistence](../../crates/tickwire-recovery/src/lib.rs), [account gateway closure binding](../../crates/tickwire-account-gateway/src/binding.rs), [128-byte HistoryFrontier](../../crates/tickwire-account-journal/src/history.rs)
* Regression suites: [recovery closure tests](../../crates/tickwire-recovery/tests/closure.rs), [file-backed account gateway tests](../../crates/tickwire-account-gateway/tests/binance_rest.rs)
* Related specifications: [account gateway](account-gateway.md), [binding a persisted intent to its command](command-binding.md), [recovery dependency persistence](recovery-closure.md)
