# L0 output

PASS across serialized `RequestStore` calls within an authenticated session: verifies at-most-once `Execute` ticket issuance per key, registration-before-delivery, matching-digest retry resolution prior to `expected_state_version` checks, non-eviction of registered entries, immutability of terminal responses, and total/pending capacity gates. Session lease expiration and renewal compose in `L1_session`.

## Fidelity & coverage

All state variables and transitions cite exact Rust source lines in `crates/tickwire-control/src/requests.rs` (locked in `anchors.lock`, with artifact digests in `04_evidence/results.json`), and every `Next` action achieved `generated > 0` across the three configurations.
