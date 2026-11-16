# L1 output

PASS: composes `RequestStore` idempotency with `CLOCK_MONOTONIC` session lease deadlines and renewals. Verifies at-most-once `Execute` ticket issuance per key, registration-before-delivery, matching-digest retry precedence over version checks, non-eviction of registered entries, immutability of terminal responses, capacity/pending gates, permanent session closure on clock regression or deadline expiry with zero subsequent ticket issuance, strict `nonce` + increasing-`revision` renewal guards, and post-close completion recording for previously issued tickets.

## Fidelity & coverage

All state variables and transitions cite exact Rust source lines in `crates/tickwire-control` (locked in `anchors.lock`, with artifact digests in `04_evidence/results.json`), and every `Next` action achieved `generated > 0` across all three configurations.
