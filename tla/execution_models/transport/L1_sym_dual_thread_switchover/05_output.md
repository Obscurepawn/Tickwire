# Output (`L1_SymDualThreadSwitchover`)

PASS across both explicit-state TLC (`2,056` distinct states, native deadlock checking enabled with `0` deadlocks, 100% coverage across all 25 `Next` actions in `1s`) and symbolic Apalache `0.63.0` + Z3 (`typecheck` `1.41s`, unbounded 64-bit `IndInv` induction `4s` parallel wall, `bmc --length=8` `5.35s`).

## Proved Properties (`SafetyInv` & `IndInv`)

- **`NoOverlappingOrderAndConnLocks` & `StpBookLeafHierarchyInv`**: Neither `REQ` nor `RESP` ever holds `LockedOrder` and `LockedConnection` simultaneously; `OrsStpBook` is acquired strictly as a leaf while holding `LockedOrder` (`OrderLockProof`), while send-path `self_cross` reads atomic `bbo_atomic_seq` lock-free.
- **`SpinlockDeadlockFreedomInv` & native TLC deadlock-freedom**: Every lock-holding state releases its spinlocks without waiting on peer spinlocks, and unsent `PendingNew` frames roll back cleanly (`NoBytes`) if `PublishedRoutes` becomes unready between `LockedOrder` release and `LockedConnection` acquisition.
- **`NoInvalidDialPromotedInv`**: `DialBaton` (`Idle -> Wanted -> Dialing -> Ready`) paired with `reset` and `take_ready` token/close validation ensures close-requested or expired-token dials never promote to `standby_established` or active service.
- **`SwitchoverAndShortWriteSafetyInv`**: `send_frame_nonblocking` TLS/TCP short writes immediately retire the socket (`active_close_pending = TRUE`, `active_published_ready = FALSE`) and route the uncertain order to `OrsQueryDesk` (`query_desk_pending = TRUE`); `64008` maintenance drain unpublishes `active_published_ready`, dials standby in parallel, and drains all `active_inflight` replies (`dropped_replies = 0`) before switchover.
