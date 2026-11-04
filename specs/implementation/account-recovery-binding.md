# Binding recovery dependencies to account admission

A recovery manifest binds a command to the exact account history used for its admission. Kind 14 stores the baseline and catalog, kind 15 the preceding account-journal prefix, and kind 13 the full command. Each account binding requires exactly one of each.

The digest of the full kind 1 admission record equals `ClosureBinding.business_digest`. Reproduction uses the recorded evaluation time, command and `AdmissionImage`. For a new order, `Persistence::publish_with_objects` checks the supplied bytes and synchronizes each dependency once before publishing the manifest and its binding. On restart, `Persistence::verify` reads the dependencies from storage and checks their lengths, digests and synchronization before returning a `VerifiedClosure`. An older manifest leaves the two account digests empty and the account gateway rejects it for this operation.

The gateway compares its actual baseline and journal prefix to the manifest. The prefix object stores the range, stream, baseline, record count and history hash. The WAL supplies the records themselves. Kinds 1–12 retain their prior byte encoding and coverage requirements. Exercise the [closure tests](../../crates/tickwire-recovery/tests/closure.rs) and [file-backed gateway tests](../../crates/tickwire-account-gateway/tests/binance_rest.rs).
