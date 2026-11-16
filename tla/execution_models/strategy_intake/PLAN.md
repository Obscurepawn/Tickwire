# Unified ORS Strategy Intake and Session Routing

In Tickwire's stateless 3-process shared-memory topology, each strategy worker exchanges inline `IntentRecord` (`1024B`) and `ReportRecord` (`512B`) slots over sealed `memfd` rings with a single venue ORS process (`1 ORS = 1 Venue`) managing `1..N` `AccountSession`s (`account_ref = 1..255`, with `0` resolving to the strategy's first authorized account).

This verification ladder proves multi-account routing isolation, `DopState` monotonic `max(login_seq, next_order_seqnum, 1)` recovery across worker restarts, `last_recv_seqnum` transport gap rejection, the microsecond `max_order_age_ns` (`DEFAULT_MAX_ORDER_AGE_NS = 23_000`) per-order `RejectStaleOrder` gate, and granular `(account_ref, strat_id)` disablement paired with ascending one-order-at-a-time auto-cancel sweeps (`ADMIN_ACTION` / `cancel_on_ack`):

- [L0 ORS session routing](L0_ors_session_routing/05_output.md) exhaustively checks two `AccountSession`s, two strategy workers with asymmetric account authorizations, multi-slot ascending admin sweeps, and worker restart cycles under TLC.
- [L1 symbolic ORS session routing](L1_sym_ors_session_routing/05_output.md) lifts `account_ref = 0..255`, `strat_id = 0..254` (`ALL_ORS_STRATS = 255`), sequence floors, and 64-bit monotonic nanosecond timestamps (`0 .. 2^64 - 1`) to Snowcat-typed dual-engine verification (finite TLC + Apalache `QF_LIA` `inductive` and `bmc`).
