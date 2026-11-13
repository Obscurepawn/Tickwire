# Account Gateway

`AccountGateway<S, A, E>` (`tickwire-account-gateway`) is the single-owner execution boundary that binds one `Account` state machine, one append-only `AccountJournal<S>`, one policy `Authority`, one wire `Encoder`, and one TLS `Connection` (supporting both HTTP/REST and WebSocket order framing) to a fixed `Route`. Both Spot and USDⓈ-M runtimes instantiate this gateway so that financial reservations, recovery-closure persistence, journal durability, and first-byte socket authorization obey one uniform protocol.

Separating pure risk calculation (`Account`) from journaled I/O (`AccountGateway`) solves the split-brain window between local disk durability and network transmission: a command never enters the outbound socket queue until its recovery closure and `Admit` record are synchronized on disk, and it never crosses into TLS plaintext until the gateway re-verifies ownership, lease validity, deadline, and market readiness at the exact moment of the first write.

## State Model and Bounds

At creation (`AccountGateway::create`), `Config.writer_epoch` must equal `journal.initial_writer_epoch`, and the `Config`, `CanonicalBaseline`, and physical journal bootstrap must all agree on `Scope` (`risk_domain_id`, `venue`, `account`, `environment`) and `Product` (`Spot`, `LinearPerpetual`, or `LinearFuture`).

| Field / Structure | Bound or Representation | Purpose |
| --- | --- | --- |
| `entries` | `BTreeMap<IntentKey, Entry>`, `1..=MAX_ORDERS` (`1,024`) | Retains every admitted command, its `CommittedAdmission` receipt, evaluation timestamp, risk-increase classification, `Status`, and byte-level `OutputProgress`. |
| `pending` | `Option<(IntentKey, u64)>` (single slot) | Enforces at most one in-flight outbound command on the connection at a time, pairing `IntentKey` with the transport send token. |
| `Status` | `AwaitingCommit`, `Queued`, `PartiallyAccepted`, `Flushed`, `Unknown`, `Aborted` | Tracks outbound transport lifecycle independently of venue-level execution reports. |
| `Gate` | Sampled synchronously via `Authority::current_gate` | Captures monotonic `now`, `lease_until`, `writer_epoch`, `strategy_generation`, `config_revision`, `owner_isolated`, `action_permitted`, and `market_valid`. |

## Core Invariants

* **Intent lookup precedes health and capacity checks**: `submit` inspects `entries` by `IntentKey` before checking `halted`, `pending`, `max_intents`, account freeze state, or deadlines. A byte-identical retry always returns its recorded `Status`; different canonical command bytes under an existing `IntentKey` immediately `halt()` the sender and return `Error::Conflict`.
* **Gateway exclusivity over `Admit` and `Aborted`**: Only `AccountGateway` emits `RecoveryRecord::Admit` and `RecoveryRecord::Aborted`. Every other transition—venue order/fill ingress, account inputs, fee closures, external adjustments, and reactivations—validates on a candidate `Account` and synchronizes to `AccountJournal` before mutating `self.account`.
* **Ambiguous storage retains reservations**: If `Authority::prepare` fails before `journal.append`, admission aborts cleanly without mutating `self.account`. Once `journal.append` is invoked for `Admit`, any storage or sync error installs the candidate `Account` (retaining its financial reservation) and halts the gateway until crash recovery establishes whether the record reached disk.
* **Strict zero-byte proof required for local abort**: Releasing an admitted reservation via `Aborted` requires positive transport proof that zero application bytes were accepted (`SendState::NoBytes` with `accepted_bytes == 0` for the active send token, a `Connection::queue` failure, or `cancel_pending` / `terminate_unsent_strategy` while `accepted_bytes == 0`) *and* candidate validation confirming no contradictory venue event has arrived.
* **Local flush is not venue delivery**: Reaching `Status::Flushed` only indicates that plaintext bytes entered the local TLS session and socket buffers; reservations remain held until authenticated venue order and fill evidence arrives.

## State Transitions and Failure Semantics

![Four-stage order submission pipeline showing idempotent intent lookup and fixed-point hold, synchronous recovery closure and WAL commit, grant re-verification immediately before the first TLS plaintext byte, and the deterministic split between proven zero-byte aborts (releasing exposure) versus partial sends or restarts (freezing the account until venue reconciliation).](../../assets/order-lifecycle.svg)

### Submission and First-Byte Authorization

1. **Deduplicate and encode**: After checking `entries` for `IntentKey` retries or conflicts, `submit` verifies `!halted`, `pending.is_none()`, and `entries.len() < max_intents`, validates command routing and `writer_epoch`, and invokes `Encoder::encode(&command)` in memory without touching the socket. The resulting `EncodedRequest` must match `command.digest()`, `route.authority`, and `max_send_bytes`.
2. **Candidate admission and closure binding**: `submit` clones `self.account` and calls `candidate.admit(&command, evaluated_at)`. If `admit` fails and freezes the candidate, `self.account.freeze()` is latched and the error returns without journaling. Otherwise, the gateway builds `CanonicalRecord` (`Admit` carrying `command`, `evaluated_at`, and `AdmissionImage`), calls `Authority::prepare` over `(baseline, record, history_frontier)`, and checks the returned `VerifiedClosure` digests.
3. **Journal sync and transport queueing**: After `journal.append` succeeds and its `AppendReceipt` matches sequence, record digest, and baseline digest, `self.account` advances to `candidate`. `Authority::commit_state` determines whether the entry starts in `Status::Queued` (`CommitState::Ready`) or `Status::AwaitingCommit` (`CommitState::Pending`), and `Connection::queue` stages the `PreparedSend`. If `queue` fails (guaranteeing zero bytes sent), the gateway synchronizes `Aborted` and halts.
4. **First-byte gate recheck in `poll`**: While `accepted_bytes == 0`, `poll` checks `Authority::commit_state` and, inside `Connection::try_send`, invokes `Authority::current_gate` plus `account.check_admission(admission, gate.now)`. The callback requires `gate.now >= evaluated_at`, `gate.now < lease_until`, `gate.now < deadline`, matching `writer_epoch`, `intent_exec_epoch`, `strategy_generation`, and `config_revision`, `owner_isolated && action_permitted`, and—whenever `admission.increases_risk()`—`gate.market_valid`. If the callback denies permission before any byte is accepted, the transport yields `SendState::NoBytes`, allowing `AccountGateway` to journal `Aborted` and release the reservation cleanly.
5. **Partial send failure**: Once `accepted_bytes > 0`, any transport error or peer close marks the intent `Status::Unknown`, calls `self.account.freeze()`, drops the connection, and halts the gateway.

### Ingress Events, Economic Closures, and Terminal Fee Release

`record_ingress_event` and `record_account_input` deduplicate by `(input_stream, input_sequence)`: an exact match returns `Applied::Duplicate`, whereas differing content at an existing sequence halts the gateway with `Error::Conflict`. Validated transitions synchronize to `AccountJournal` before updating `self.account`. If `candidate.apply_event` or `apply_account_input` rejects an already-persisted upstream observation, the upstream input WAL retains the raw evidence while `AccountGateway` freezes `self.account`; so long as the gateway sender itself is not halted, authorized `Cancel` and `Query` requests can still be sent.

Local fee release (`FeesReconciled`) requires a terminal order whose reported cumulative quantity equals its booked fills. Fee preflight inspects the single target order and `ledger_sequence` without cloning `Account`; candidate validation then precedes appending `FeesReconciled`, and any fee budget is released only after the journal sync completes. A conflicting cumulative quantity freezes `self.account` and leaves `AccountJournal` at its preceding sequence while keeping prior order and fill records intact.

### Crash Recovery and Reactivation

`AccountGateway::recover` and `recover_with_reactivation` verify the journal bootstrap, `CanonicalBaseline`, physical segment chain, monotonic writer epochs, and every historical `Admit` closure and reconciliation proof before replaying `Account`. The recovered gateway always starts with `halted = true`, `account.frozen() == true`, `pending = None`, and `connection = None`. Repeating a previously admitted `IntentKey` returns its reconstructed `Status` without resending.

Resuming new order flow after recovery requires verified remote reconciliation, a durably synchronized `RecoveryRecord::Reactivated` via `reactivate`, and a fresh `ConnectionConfig`. By contrast, a live healthy gateway renewing its qualification under the same owner and `writer_epoch` can synchronize `Reactivated` while keeping its existing TLS connection open.

## Verification and References

```sh
cargo test --locked -p tickwire-account-gateway --all-targets
```

* Gateway implementation and bindings: [AccountGateway](../../crates/tickwire-account-gateway/src/lib.rs), [route and gate binding](../../crates/tickwire-account-gateway/src/binding.rs), [WebSocket framing](../../crates/tickwire-account-gateway/src/websocket.rs)
* Journal and codec layers: [AccountJournal](../../crates/tickwire-account-journal/src/lib.rs), [account record codec](../../crates/tickwire-account-codec/src/lib.rs)
* Integration test suites: [submission and first-byte tests](../../crates/tickwire-account-gateway/tests/submission.rs), [file-backed Binance REST recovery tests](../../crates/tickwire-account-gateway/tests/binance_rest.rs)
* Related specifications: [multi-asset account state machine](account.md), [binding recovery dependencies to account admission](account-recovery-binding.md), [order submission policy](runtime-policy.md)
