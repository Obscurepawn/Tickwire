# L0 output

PASS across all four bounded configurations (`2,181`, `321,256`, `172`, and `7,652` distinct states at `Capacity = 64`; see [01_reasoning.md](01_reasoning.md) and [03_run_log.md](03_run_log.md)).

Verified invariants: `Pending` and `Expired` entries retain baseline `S` slot protection; activation requires dual `READ_READY`, live cursor registration, and catch-up to current `published`; concurrent joins do not overwrite existing entries' birth membership versions; live borrows and unread records preserve exact slot sequence contents across wraparound; failed deliveries and timeouts never detach; detachment requires live cursor `Quiesced` state with exact watermarks; and idempotent historical `JOIN_READY` / `Quiesce` retries never corrupt publisher state.

## Fidelity & coverage

The union across the four configurations covers all 23 modeled transition families (`22` in primary/two-reader configs and `16` in full-ring wrap configs, all with `generated > 0`), spanning 14 of 17 stateful `JoinOwner` / `JoinConsumer` entry points (with stateless frame codecs validated by Linux `SCM_RIGHTS` integration tests).
