# The owner's next cancellation reaches the venue

**Retired, fixed.** The account returns a distinct management refusal for an identified order whose owner, manager or revision does not match. The runner delivers the result through the source member's callback and continues polling. It propagates origin, scope, persistence and other integrity errors as before. Classification adds a match on the error path and uses the existing order lookup. The successful order path performs no extra queue operation, lookup or allocation. [Account check](../../../../crates/tickwire-account/src/admission.rs#L369-L397) · [Runner callback](../../../../crates/tickwire-spot-runner/src/lib.rs#L126-L149)

All three [v2 configurations](03_run_log.md) preserve the two queues' cash, send-count, authority and receipt properties. The original peer-containment invariant holds. The additional step property checks that each classified refusal leaves the process running and preserves the account and transport state. The 11-state reachability trace exercises a funded, acknowledged buy on each side and a successful owner cancellation after the foreign request.

## Linux regression

Run `python3 tla/execution_models/strategy_intake/L1_management_refusal/evidence/native/run.py` from the repository root. It builds the current production executable, supplies the historical two-queue trigger on loopback and validates each venue's signed cancel and ACK. The revised fixture gives buy 801 and cancel 802 distinct immutable recovery-object IDs. The historical fixture reused the buy’s ID on the previously unreachable cancel, which would itself create a recovery binding conflict. Both current processes delivered the typed refusal for 901, sent exactly one signed cancel for 802, returned its ACK to member 8 and shut down normally. [Native outputs](evidence/native/result.log) · [Fixture delta](evidence/native/accepted_cancel.patch)

## Fidelity and coverage

The model checks 12 actions spanning intake, origin and authority failure, admission, wire, ACK, abort and restart. It bounds the environment to two strategies, four command keys and one outstanding short REST request. The Rust process fixture exercises real signed HTTP but uses a loopback exchange and a selected sequence of arrivals.
