# Fee-closure verification

**Retired (fixed).** The gateway refuses the original nine-state counterexample before the fee decision enters the journal. Source observations remain durable. The [four model checks](03_run_log.md) also cover a crash after a valid write and two explicit historical input sequences.

## Fidelity and coverage

The model covers twelve transitions from private capture through scanning, fee publication, qualification and restart. TLC exercised all twelve. The [source map](02_models/L1_TerminalFees_v2.tla) gives the account, scan and journal boundary for each. One spot order, bounded integer quantities, stable authenticated input and the two historical prefix shapes define the domain. The Rust regressions also check the stored history digest, unchanged ledger sequence and replay of later matching fills. [Durable file test](../../../../crates/tickwire-account-journal/tests/persistence.rs) · [Reducer tests](../../../../crates/tickwire-account/tests/recovery.rs) · [Gateway refusal](../../../../crates/tickwire-account-gateway/tests/lifecycle.rs)
