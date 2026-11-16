# Transport Follow-up

**Verified.** Both [L0 first output](L0_first_output/05_output.md) and [L1 symbolic dual-thread switchover](L1_sym_dual_thread_switchover/05_output.md) pass exhaustive finite TLC (`./scripts/check-tla.sh transport`), Snowcat typechecking, unbounded 64-bit `QF_LIA` induction (`IndInv`), and `k = 8` bounded model checking, with all Rust source anchors locked in `02_models/anchors.lock`.

**Watch.** Re-run `./scripts/check-tla.sh transport`, `anchors.sh check tla execution_models/transport/`, and Apalache `inductive` + `bmc` whenever `Connection::send_frame_nonblocking`, `DialBaton`, `LockedConnection` / `LockedOrder` / `OrsStpBook` locking, or `graceful_drain_connection` changes.
