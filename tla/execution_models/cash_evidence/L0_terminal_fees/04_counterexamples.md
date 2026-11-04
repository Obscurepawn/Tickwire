# Rejected fee decision makes account replay fail

For a previously readable account journal, every newly synchronized fee decision must remain applicable on replay. The lower reported quantity is the fault input. The program changes durable state in response.

| TLC state | Event | Stored quantities or evidence |
| ---: | --- | --- |
| 1–2 | Initialize, private terminal | Private reports cumulative 1. No individual fills are booked. |
| 3–4 | Read both REST scans | Both report terminal cumulative 0, zero fills and matching wallet. |
| 5 | Record REST terminal | The ledger retains the maximum observed cumulative of 1. Booked quantity stays 0. |
| 6–7 | Accept wallet, sync fee decision | The journal contains a fee closure for 0. |
| 8–9 | Crash, reopen | The reducer compares the closure with terminal quantity 1 and rejects the journal. |

The live feeder compares REST cumulative against individually booked fills. The wallet checker accepts balances consistent with zero fills; exchange and local locks both permit a canceled order. The gateway appends the fee record before checking the exact stored terminal cumulative. The private-history proof would reject the inconsistency later in the same scan. Its position after the append prevents it from protecting the durable prefix. [Capture and append path](../../../../crates/tickwire-spot-runner/src/reconcile/ingest.rs#L270) · [Gateway append before apply](../../../../crates/tickwire-account-gateway/src/lib.rs#L1042) · [Reducer precondition](../../../../crates/tickwire-account/src/admission.rs#L484)

The Linux process witness uses the unmodified account executable, signed loopback REST responses and authenticated private streams. After strategy 8 receives an ACK, the peer reports a private terminal cumulative of 0.001 BTC. REST replies that the order was canceled with zero fills and unchanged wallets. Both venue profiles reject fee application. A clean shutdown and restart with a newer approved writer epoch yields `Journal(Account(Unreconciled))`. The crash in the TLC trace is unnecessary for the native repro. No new order was authorized by this divergence. The evidence establishes the behavior for supplied authenticated disagreement; how often a venue could generate it remains unknown. [Raw TLC trace](04_evidence/prefix_failure.log) · [Actual process output](04_evidence/native.log) · [Reproducer](04_evidence/reproduce.sh)

Preserve the prior WAL prefix by applying the proposed fee closure to an account candidate, then synchronize it and publish the candidate. A regression should deliver these two inconsistent reports, reopen the same account, and verify a subsequent matching inventory can be evaluated. Existing affected journals would require a separate repair procedure.
