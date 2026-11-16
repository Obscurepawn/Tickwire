# Per-Account ORS QueryDesk Verification Status

All per-order `OrsQueryDesk` scheduling, monotonic `cancel_after` coalescing, exponential `Cancel` backoff, snapshot catch-up fill ordering, `-2013` / `NotFound` disambiguation, non-`Live` full-table eviction, and `max_query_count` invariants are verified in [`L0_ors_query_desk`](L0_ors_query_desk/05_output.md) and [`L1_sym_ors_query_desk`](L1_sym_ors_query_desk/05_output.md) against `tickwire-account`, `tickwire-binance-account`, and `tickwire-spot-execution`.
