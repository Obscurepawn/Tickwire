# L0 output

All five configurations passed at physical `Capacity = 64` (up to `930,364` distinct states at depth `33` in `two_readers`, plus three full-ring wraparound prefix configurations covering physical slot reuse at sequences `65`/`66` across 1 and 2 readers). Zero open candidates remain.

Verified safety invariants: published batches always have complete slot payloads (`CompleteBeforePublication`); active borrows and unread records retain exact slot sequence contents (`NoOverwriteBorrow`, `UnreadProtected`); `cached_min` never exceeds true member reclamation frontiers (`ConservativeCache`); pending JOIN baselines block wraparound reuse (`MemberGate`); `released` and `applied` advance as independent contiguous prefixes; `Freeze` never cuts across an open claim (`FreezeBoundary`); and `detach` requires the registered shared cursor to be `Quiesced` with matching watermarks and zero live borrows (`DetachedSafe`). See [01_reasoning.md](01_reasoning.md) and [03_run_log.md](03_run_log.md).

## Fidelity & coverage

- Fidelity: 17 transitions modeled with exact `file:line` source citations and zero unresolved branches.
- Code coverage: 14 of 14 protocol mutation methods in `crates/tickwire-shm/src/lib.rs` (`join`, `join_ready`, `observe_release`, `claim`, `publish`, `abort`, `prepare_freeze`, `freeze`, `detach`, `borrow_next`, `release`, `apply`, `start_quiesce`, `quiesced`) plus mapped `register_cursor`, `publish`, and `detach` evidence boundaries.
- Model coverage: all enabled actions fired across all five configurations (`17` transitions in `primary`/`two_readers`, `16` in `pending_wrap`, `15` in `wrap`/`wrap_two`).
