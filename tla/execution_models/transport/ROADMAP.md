# Transport Verification Levels

| Level | Scope and Result |
| --- | --- |
| [L0 first output](L0_first_output/05_output.md) | Synchronous first-output gate, partial TLS acceptance vs. socket flush, deadlines, and `NoBytes` / `MayHaveSent` / `Failed` / `Flushed` evidence. PASS across 4 TLC configurations (`688`..`5,944` distinct states, 100% action coverage). |
| [L1 symbolic dual-thread switchover](L1_sym_dual_thread_switchover/05_output.md) | Dual-thread `REQ`/`RESP` non-overlapping `LockedOrder` vs `LockedConnection` hierarchy, leaf `OrsStpBook` + atomic BBO `self_cross`, `DialBaton` deferred reset/token check, `ShortWriteRetired -> OrsQueryDesk`, and `64008` maintenance drain switchover. PASS under TLC (`2,056` distinct states, `CHECK_DEADLOCK TRUE`, 100% coverage, `1s`) and Apalache `typecheck` + `inductive` (`IndInv`, `4s`) + `bmc` (`k = 8`, `5.35s`). |
