# L0 review and follow-up notes

Run `anchors.sh check` whenever `Producer` claim/membership logic, `Reader` contiguous completion tracking, or mapped cursor detachment changes, and re-verify semantic equivalence beyond line-hash stability.

During parallel Rust review, `RecordBorrow` was tightened to store `NonNull<[u8]>` rather than a long-lived `&'a [u8]` field, constructing short-lived slices only inside `bytes(&self)` so borrows cannot outlive `Drop` advancing `released`; this aliasing refinement leaves the L0 state machine unchanged.

Subsequent JOIN integration added `register_pending_cursor`, `activate_join`, and `detach_registered` (modeled in `join_owner/L0_pending_activation`) while preserving the pending reclamation gate verified here; updated line-anchor digests are archived in [join-followup](04_evidence/join-followup/current-sha256.json).
