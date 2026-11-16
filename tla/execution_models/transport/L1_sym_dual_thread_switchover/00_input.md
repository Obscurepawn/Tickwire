# Input: Dual-Thread Spinlock Hierarchy, `DialBaton`, Short-Write Retire & `64008` Switchover (`L1_SymDualThreadSwitchover`)

Target scope: Snowcat-typed dual-engine (`BoundedMode`) verification of ORS `REQ` and `RESP` hot-thread spinlock discipline, `"tw-dialer"` `DialBaton` lock-free handoff, TLS/TCP short-write connection self-retirement (`SendOutcome::ShortWriteRetired -> OrsQueryDesk`), and venue maintenance `notice` (`64008` / `graceful_drain_connection`) smooth drain and standby switchover.

## Production Rust Anchors

- `DialBaton` (`Idle -> Wanted -> Dialing -> Ready`), `take_ready`, `fail`, `reset`, and `GuardedValue` stream/auth token read on dial:
  - `crates/tickwire-transport/src/dialer.rs:177-183`
  - `crates/tickwire-transport/src/dialer.rs:264-420`
  - `crates/tickwire-transport/src/dialer.rs:1224-1234`
  - `crates/tickwire-transport/src/links.rs:882-945`
  - `crates/tickwire-transport/src/links.rs:1488-1560`
  - `crates/tickwire-transport/src/order_router.rs:945-1026`
- Non-overlapping `LockedOrder` vs `LockedConnection` spinlocks, leaf-only `OrsStpBook` (`OrderLockProof`), and lock-free atomic BBO `self_cross`:
  - `crates/tickwire-account/src/spinlock.rs:1-157`
  - `crates/tickwire-account/src/ors_stp.rs:174-224`
  - `crates/tickwire-runtime/src/session.rs:672-785`
  - `crates/tickwire-spot-execution/src/session.rs:1103-1295`
  - `crates/tickwire-transport/src/links.rs:1108-1139`
  - `crates/tickwire-transport/src/links.rs:1173-1197`
- `send_frame_nonblocking` TLS/TCP short-write self-retire (`ShortWriteRetired`) and `OrsQueryDesk` reconciliation:
  - `crates/tickwire-transport/src/connection.rs:168-175`
  - `crates/tickwire-transport/src/connection.rs:238-302`
  - `crates/tickwire-runtime/src/session.rs:764-785`
  - `crates/tickwire-spot-execution/src/session.rs:1268-1278`
- Server maintenance `notice` (`64008` / `graceful_drain_connection`) route unpublish, standby pre-dial, in-flight reply drain, and switchover:
  - `crates/tickwire-transport/src/order_router.rs:629-643`
  - `crates/tickwire-spot-execution/src/json_parser.rs:1174-1177`
  - `crates/tickwire-spot-execution/src/session.rs:1943-1986`
  - `crates/tickwire-spot-execution/src/session.rs:2562-2566`
  - `crates/tickwire-spot-execution/src/session.rs:3354-3384`
  - `crates/tickwire-runtime/src/ors.rs:523-591`
  - `crates/tickwire-runtime/src/ors.rs:2410-2450`
  - `crates/tickwire-runtime/src/ors.rs:2591-2593`
