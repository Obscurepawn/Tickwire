# L1 Symbolic 64-Bit ORS QueryDesk Reasoning

## Dual-Engine `QF_LIA` Encoding & Inductive Proof Design

- **Exact 64-Bit Nanosecond Deadlines & Linear Backoff Table**: Constructs `NsPerSec == 1000000000`, `StatusQueryDelayNs == 6 * NsPerSec`, and `U64_MAX == (16 * Pow30 * Pow30) - 1` without raw literals above `2^31 - 1`. Encodes `cancel_backoff_delay_ns(base, attempt)` (`base * (1 << min(attempt, 6))`) as a piecewise linear multiplier (`1, 2, 4, 8, 16, 32, 64 * BaseBackoffNs`) paired with `SatAddU64`, keeping Z3 strictly in decidable `QF_LIA`.
- **Symbolic `OptInMaxCap \in BOOLEAN` Unified Induction**: In Apalache mode (`ConstInit == BoundedMode = FALSE /\ OptInMaxCap \in BOOLEAN`), Z3 simultaneously proves the inductive invariant over **both** unbounded `max_query_count == None / Some(0)` (`~OptInMaxCap`) and arbitrary `cfg_max_query_limit \in 1..2^64-1` (`OptInMaxCap`).
- **Inductive Coupling (`IndInv`)**: Couples `(OptInMaxCap /\ slot_live) => (query_fail_count < cfg_max_query_limit)` so `IndInv` excludes unreachable live states already past the configured cap, and clears `merge_witness_active` when `poll_due` or `schedule_cancel_backoff` advances `desk_next_due_ns` to the next retry interval so the merge witness isolates `schedule_status_ask` coalescing.
