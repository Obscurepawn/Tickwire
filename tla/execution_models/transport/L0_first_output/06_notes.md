# Follow-up Notes

- Dual-thread `REQ` / `RESP` spinlock non-overlap (`LockedOrder` vs `LockedConnection`, leaf-only `OrsStpBook`), `send_frame_nonblocking` TLS/TCP short-write self-retire (`SendOutcome::ShortWriteRetired -> OrsQueryDesk`), `DialBaton` deferred reset/close handoff, and `64008` / `graceful_drain_connection` zero-dropped-reply switchover are modeled and proved symbolically in [L1_sym_dual_thread_switchover](../L1_sym_dual_thread_switchover/05_output.md).
- Deadline semantics in `L0_FirstOutput` reflect the exact monotonic clock checks inside `try_send` and `drive`; physical NIC wire serialization latency after kernel `write` acceptance remains outside the scope of user-space state machines.
