# L1 Symbolic 64-Bit ORS QueryDesk Verdict

- **Status**: **PASS** (Apalache Z3 `QF_LIA` unbounded inductive proof + depth-5 BMC + TLC `BoundedMode` 100% action coverage)
- **Model**: [`02_models/L1_SymOrsQueryDesk.tla`](02_models/L1_SymOrsQueryDesk.tla)
- **Reasoning**: [`01_reasoning.md`](01_reasoning.md)
- **Run Log**: [`03_run_log.md`](03_run_log.md)

## Verified Properties

- `SafetyConjunction` and `IndInv` hold inductively over unbounded 64-bit nanosecond timestamps (`StatusQueryDelayNs = 6s`, `BaseBackoffNs = 1s`) and symbolic 64-bit lot sizes.
- `OrsQueryDesk::schedule_status_ask` monotonic merge never postpones deadlines or resets retry counters, 8-attempt exponential `Cancel` backoff (`1s..=64s`) never overflows `u64`, catch-up fills settle strictly before remainder cancels, `-2013` / `NotFound` disambiguates uncreated vs open/filled orders, `Live` entries are protected during full-table cleanup, and `max_query_count` (`None` / `Some(0)` vs `Some(limit)`) holds across all reachable states.
