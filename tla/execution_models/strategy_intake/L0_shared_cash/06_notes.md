# Management refusal and peer isolation

**Confirmed.** A member with a known order locator can send a syntactically valid cancel under its own identity for another member's order. The account properly denies management before admission, but the runner propagates this error into the shared process exit. The independent review distinguishes this local authorization error from a conflicting account WAL record. [Reproduction](04_counterexamples.md) · [Adversarial review](06_review.md)

A fix can give unauthorized management a distinct pre-admission error and stop or refuse the offending member while keeping the healthy account and peer alive. Preserve the existing process-wide stop for mismatched immutable aliases, corrupted account state and ambiguous sends. A regression should continue until the healthy member's queued cancellation reaches the native gateway and gets its own reply.

**Retired, fixed.** The [v2 level](../L1_management_refusal/05_output.md) separates a known target's management denial from an origin conflict. The former eight-state probe exhausted 698 states. A new 11-state trace reaches the waiting peer's cancel ACK.
