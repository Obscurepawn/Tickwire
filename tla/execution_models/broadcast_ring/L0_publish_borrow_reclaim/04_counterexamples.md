# L0 counterexample log

All five configurations passed with zero TLC safety counterexamples and zero open candidates.

Prior to TLC modeling, source review caught a forged pure-state `Quiesced` hazard (reproduced and fixed in `crates/tickwire-shm/tests/mapped.rs` `pure_state_quiescence_cannot_detach_a_real_live_borrow`), after which mapped `detach` checks live registered cursor `lifecycle == 5`, exact watermarks, and `quiesced_seq`; L0 models and verifies the fixed protocol (`RejectForged`).
