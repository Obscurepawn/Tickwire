# Binding a Persisted Intent to Its Canonical Command

Scalar execution engines (`tickwire-execution`) and compact business-journal records (`tickwire-journal-codec`) represent orders as fixed-size records so hot-path reservation reducers remain `Copy` and allocation-free, whereas venue adapters require variable-length order parameters (native symbols, limit/trigger prices, time-in-force, conditional legs, and trailing terms) encoded in `CanonicalCommand` (`tickwire-venue`). Tickwire bridges these two representations without ambiguity by embedding a mandatory 32-byte SHA-256 `command_digest` inside `OrderRequest` and `PreparedIntent`, while storing the full `CanonicalCommand` bytes as a verified, `fsync`-backed recovery dependency at kind `13`.

Without cryptographic binding at both the WAL schema layer and the recovery-closure layer, a crash-recovery path or buggy caller could pair an approved scalar budget reservation with altered order parameters under the same intent ID. Requiring exact digest agreement across `OrderRequest`, CBOR schema `2`, and `VerifiedClosure` guarantees byte-level immutability from reservation through recovery.

## Encoding and Dependency Model

| Layer | Type / Field | Size and Versioning Contract |
| --- | --- | --- |
| Scalar execution (`tickwire-execution`) | `OrderRequest.command_digest: [u8; 32]` | Mandatory nonzero SHA-256 digest; `Order<T>` and `OrderRequest` remain fixed-size `Copy` structs while participating in request equality, `DurableRecord`, and commit receipts. |
| Business codec (`tickwire-journal-codec`) | `PreparedIntent` map key `14` (`SCHEMA_VERSION = 2`) | Deterministic CBOR record bounded to `MAX_ENCODED_EVENT_BYTES = 236` bytes. Domain-separated SHA-256 tags cover `tickwire.business-event`, `tickwire.logical-payload`, chain genesis, and chain nodes. |
| Recovery store (`tickwire-recovery`) | `COMMAND_DEPENDENCY_KIND = 13` | Optional single `DependencyObject` in `RecoveryManifest` storing the exact `CanonicalCommand` CBOR bytes, verified by byte length, SHA-256 `content_hash`, and `BlobStore::put_sync`. |
| Closure capability (`tickwire-recovery`) | `VerifiedClosure::command_digest() -> Option<Hash>` | Privately populated only when dependency kind `13` is present, hash-verified, and synchronized in storage. |

The business journal's bootstrap file pins `SCHEMA_VERSION = 2`. Current readers reject legacy schema-`1` records (`DecodeError::Schema`) because schema `1` lacked map key `14` and could not bind full venue order parameters, and `BusinessJournal` rejects legacy `CommitProfile::LegacySyncReplica` records with `Error::UnsupportedProfile`.

## Core Invariants

* **Zero digests fail before state entry**: `Engine::checked_budget` rejects `command_digest == [0; 32]` with `Error::InvalidRequest` before `reserve` or `recover_intent` can allocate an order slot or lock capital.
* **Business digest alone cannot authorize commit**: A `VerifiedClosure` whose `ClosureBinding.business_digest` matches `PreparedIntent`—but whose manifest omits dependency kind `13` (`command_digest() == None`) or carries a mismatched hash—is rejected by `BusinessJournal::commit_intent` (`Error::Invalid("closure does not include command")`).
* **Three-point digest verification**: Initial `commit_intent`, idempotent retry returns on an already-committed `intent_id`, and post-crash recovery (`Runtime::recover` and `JournaledSender::recover`) each independently verify that `VerifiedClosure::command_digest()` equals `Some(record.request.command_digest)`.
* **Same-intent mutation poisons the writer**: Submitting a `DurableRecord` under an existing `intent_id` or `sequence` with a different `command_digest` (or any other changed field) poisons `BusinessJournal` and returns `Error::Conflict`.

## State Transitions and Failure Semantics

### Responsibility Boundaries

Command binding proves byte identity and durable storage equivalence between `PreparedIntent` and `CanonicalCommand`. Adjacent layers retain their distinct responsibilities:

* `tickwire-venue` validates internal command syntax, decimal scales, tick/step alignment, and order-type rules when constructing or decoding `CanonicalCommand`.
* `JournaledSender` and `AccountGateway` verify route authority, environment, multi-asset margin/cash sufficiency, live `Gate` authorization, and wire encoding before the first plaintext byte enters TLS.
* Calling the scalar `Exchange::submit` port in `tickwire-execution` models handing an order to an adapter interface; it does not by itself prove whether a network socket accepted plaintext bytes.

### Formal Model Scope (TLA+ L1)

The bounded TLA+ `L1_command_binding` model checks digest equivalence, missing or duplicate kind-`13` objects, same-intent parameter conflicts, WAL write/sync faults, and crash recovery. Its `Send` action observes invocation of the `Exchange::submit` interface rather than wire-level protocol decoding or the TLS transport's first-byte gate.

## Verification and References

```sh
cargo test --locked -p tickwire-journal-codec -p tickwire-recovery -p tickwire-business-journal
```

* Core implementations: [scalar OrderRequest](../../crates/tickwire-execution/src/lib.rs), [business CBOR codec schema 2](../../crates/tickwire-journal-codec/src/lib.rs), [recovery manifest kind 13](../../crates/tickwire-recovery/src/lib.rs), [BusinessJournal commit and recovery](../../crates/tickwire-business-journal/src/lib.rs)
* Golden vectors, tests, and TLA+ model: [schema-2 golden vectors](../../crates/tickwire-journal-codec/tests/golden-v2.json), [business journal runtime tests](../../crates/tickwire-business-journal/tests/runtime.rs), [recovery closure tests](../../crates/tickwire-recovery/tests/closure.rs), [TLA+ L1 command-binding model and output](../../tla/execution_models/business_journal/L1_command_binding/05_output.md)
* Related specifications: [canonical trading commands and venue events](venue.md), [durable command sending](gateway.md), [binding recovery dependencies to account admission](account-recovery-binding.md)
