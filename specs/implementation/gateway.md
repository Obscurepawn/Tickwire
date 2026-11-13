# Durable Command Sending

`JournaledSender<S, A, E>` (`tickwire-gateway`) is the single-owner outbound gateway for the scalar business-journal pipeline, uniting `BusinessJournal<S>`, a trusted `Authorization` service, a protocol `Encoder`, and a non-blocking TLS `Connection`. Callers submit a scalar `DurableRecord` alongside its full `CanonicalCommand`; `JournaledSender` verifies that both name the exact same identity, persists the recovery closure and business intent before queueing bytes, and re-verifies live authorization immediately prior to the first plaintext write into TLS.

`JournaledSender` isolates transport durability and first-byte fencing from multi-asset portfolio accounting. While `Authorization` supplies strategy permissions and budget checks at commit and send time, scalar `OrderRequest` records alone do not track multi-asset balances, private fills, or fee settlements—those responsibilities belong to the upstream account runtime (`AccountGateway` for multi-asset Binance Spot/USDⓈ-M, and the dedicated spot execution adapter for Bybit and OKX).

## State Model and Bounds

| Component / Type | Bound or Variants | Contract |
| --- | --- | --- |
| `Config` | `writer_epoch > 0`, `max_intents` in `1..=1,000,000` | Binds `BusinessConfig`, `writer_epoch`, and `Route` (`venue`, `account`, `environment`, `product`, TLS `authority`). |
| `EncodedRequest` | `1 B ..= 16 MiB` (`16,777,216` bytes), nonzero `command_digest` | Constructed purely in memory by `Encoder::encode`, then bounded against connection `max_send_bytes` inside `PreparedSend`. |
| `pending` | `Option<(u64, u64)>` (single in-flight slot) | Serializes network transmission to one active `(intent_id, send_id)` pair on the connection at a time. |
| `Status` | `Queued`, `Sending`, `AwaitingResponse`, `Unknown`, `Aborted` | Tracks per-intent sender state across `entries: BTreeMap<u64, Entry>`. |

## Core Invariants

* **Idempotent exact retry, fail-stop on conflict**: `submit` looks up `intent_id` in `entries` before checking `frozen`, `pending`, or `max_intents`. An identical `(DurableRecord, CanonicalCommand)` pair returns its recorded `Status`; any difference under the same `intent_id` triggers `freeze_unknown()` and returns `Error::Conflict`.
* **Complete field equivalence between scalar record and command**: `validate_binding` enforces exact agreement between `DurableRecord` and `CanonicalCommand` across `risk_domain_id`, `venue`, `account`, `environment`, `product`, `intent_id`, `intent_exec_epoch` (equal to `writer_epoch` on fresh submissions), `strategy_generation`, `config_revision`, `deadline`, `recovery_object`, and `command_digest`, plus non-overflowing positive budget parameters.
* **Encoding and WAL synchronization precede socket queueing**: `Encoder::encode` prepares the entire wire payload and verifies `digest` and `authority` before touching `Authorization` or `BusinessJournal`; an encoding error leaves both journal and socket untouched. Only after `Authorization::authorize` returns a `VerifiedClosure` and `BusinessJournal::commit_intent` synchronizes to disk may `Connection::queue` stage the message.
* **Synchronous gate recheck before first plaintext byte**: In `poll`, while zero application bytes have been accepted by TLS, `Connection::try_send` queries `Authorization::current_gate(&command, record)` on every attempt and checks `enabled`, `market_valid`, `account_valid`, `recovered`, `owner_isolated`, `now < lease_until`, `now < deadline`, and matching `writer_epoch`, `strategy_generation`, and `config_revision`, followed by the transport's own deadline check. Once any plaintext byte is accepted, the transport sends the remaining bytes of the original immutable buffer without rewriting offsets or headers.
* **Durable abort precedes `Status::Aborted`**: A zero-byte transport outcome (`SendState::NoBytes` with `accepted_bytes == 0`, or a `Connection::queue` rejection) appends and synchronizes `codec::Event::AbortedBeforeSend` to `BusinessJournal` *before* setting `Status::Aborted`.

## State Transitions and Failure Semantics

### Live Send and Ambiguous Transport Faults

During `poll(now, budget)`, `Connection::drive` advances the TLS handshake and flushes buffered ciphertext within `IoBudget`:

1. **Zero-byte denial or cancellation**: If `current_gate` denies permission, the send deadline expires before the first byte, or `cancel()` is invoked prior to byte acceptance, `Connection` reports `SendState::NoBytes` with `accepted_bytes == 0`, allowing `JournaledSender` to journal `AbortedBeforeSend`, mark `Status::Aborted`, and clear `pending`.
2. **Local completion (`AwaitingResponse`)**: Once all bytes of `PreparedSend` flush into the connection (`SendState::Flushed`), the entry transitions from `Sending` to `AwaitingResponse` and `pending` clears, while the underlying risk reservation remains held pending an authenticated venue report.
3. **Partial send, peer close, or journal sync failure**: Any error from `commit_intent`, `append_fact(AbortedBeforeSend)`, `Connection::drive`, `SendState::Failed`, or a remote `peer_closed` event calls `freeze_unknown()`, latching `frozen = true` and transitioning every non-`Aborted` entry in `entries` to `Status::Unknown`.

### Crash Recovery

`JournaledSender::recover` replays `BusinessJournal::recover` under the new `writer_epoch`, invoking `Authorization::recover` on every `PreparedIntent` to reload `CanonicalCommand` and verify its `VerifiedClosure` (`binding` and `command_digest`). An intent recovers as `Status::Aborted` if and only if a durable `AbortedBeforeSend` record follows it with zero contradicting economic facts (`Fill`, `Fee`, or `Terminal`); conflicting abort/fact sequences fail recovery with `Error::Conflict`, and all other historical intents recover as `Status::Unknown`. The recovered `JournaledSender` holds `connection = None` and `frozen = true`: a newer `writer_epoch` can inspect historical statuses from earlier epochs without rewriting their original `intent_exec_epoch` or retransmitting them.

## Verification and References

```sh
cargo test --locked -p tickwire-gateway --all-targets
```

Integration suites test both lifecycle fault injection and real TLS signing against an independent loopback Binance REST peer for Spot and USDⓈ-M requests.

* Implementation: [JournaledSender](../../crates/tickwire-gateway/src/lib.rs), [Binance REST encoders](../../crates/tickwire-gateway/src/binance.rs), [non-blocking TLS transport](../../crates/tickwire-transport/src/lib.rs)
* Test suites: [sender lifecycle and recovery tests](../../crates/tickwire-gateway/tests/lifecycle.rs), [signed Binance REST TLS tests](../../crates/tickwire-gateway/tests/binance_rest.rs)
* Related specifications: [binding a persisted intent to its command](command-binding.md), [account gateway](account-gateway.md), [Bybit and OKX spot execution](spot-execution.md)
