# Output (`L1_SymOrsSessionRouting`)

PASS across both explicit-state TLC (`15,936` distinct states, 100% action coverage, `3s`) and symbolic Apalache `0.63.0` + Z3 (`typecheck` `1.86s`, unbounded `IndInv` induction `4s` parallel wall, 64-bit `QF_LIA` `bmc --length=8` `28.14s`).

## Proved Properties (`SafetyInv` & `IndInv`)

- **`AuthorizedAndHoldInv`**: Across all `account_ref \in 0..255` and `SymStrat \in 0..254`, `0` resolves strictly to `SymAcctA`, unauthorized sibling `SymAcctB` (`REJECT_ROUTING_ID_MISMATCH`) and unregistered IDs (`INVALID_ACCOUNT_ID`) maintain `hold_acct_b = 0`, and `hold_acct_a` exactly equals the count of live order slots on `SymAcctA`.
- **`MonotonicDopSeqInv`**: Across unbounded worker disconnects and reconnects with arbitrary 64-bit `login_seq \in 0..2^63`, `next_ord_seq` never regresses below `floor_ord_seq` or below any active order's `ord_seq + 1`, active slots never collide on `ord_seq`, and `last_recv_seq` resets to `0` on logout while rejecting `0` and gaps without cursor mutation.
- **`StaleOrderGateInv`**: Over the full 64-bit `0 .. 2^64 - 1` domain for `now_ns`, `create_ts_ns`, and `MaxOrderAgeNs` (including `DEFAULT_MAX_ORDER_AGE_NS = 23_000`), every admitted order satisfies `SaturatingSubU64(now_ns, create_ts_ns) <= MaxOrderAgeNs` whenever `MaxOrderAgeNs > 0`, and `RejectStaleOrder` never freezes `enabled_acct_a` or mutates `hold_acct_a`.
- **`DisabledSweepInv`**: Granular `(acct_arg, strat_arg)` disablement (`acct_arg = 0` wildcard, `strat_arg = 255` wildcard) and ascending single-slot `sweep_cursor` transitions (`1 -> 2 -> 0`) with `cancel_on_ack` guarantee zero resting `Open` orders on disabled `(SymAcctA, SymStrat)` pairs for all execution depths `k >= 0`.
