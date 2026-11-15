# Stateless Strategy Supervision and Runtime Observation (`tickwire-strategy-host`)

## Scope and Ownership

[`tickwire-strategy-host`](../../crates/tickwire-strategy-host/src/lib.rs) supervises sealed strategy worker processes (`tickwire-supervisor`) and collects live OS / cgroup v2 resource observations in Tickwire's pure stateless 3-process shared-memory topology:

- **External `tickwire-md` SHM Join Only:** Strategy workers **always** join an external `tickwire-md` market-data server (`tickwire_market_source::ConnectConfig`). Local embedded-publisher fallbacks and cutover state machines are absent. Before spawning a worker, `tickwire-strategy-host` registers the worker's one-shot admission ticket via `tickwire_md::server::register_admission` so `tickwire-md` can verify kernel `SO_PEERCRED` + `/proc/<pid>/stat` process identity.
- **Direct ORS Inline SHM Rings & DOP Wiring:** For each supervised strategy worker (`strat_id: 0..=254`), `tickwire-strategy-host` provisions a `0o700` `PrivateDirectory`, binds authenticated `intent.sock` and `report.sock` Unix endpoints, attaches the ORS-side `IntentReader` (`1,024B` `IntentRecord`) and `ReportWriter` (`512B` `ReportRecord` backed by sealed `memfd`), and wires `Bootstrap.dop_socket` / `DopConfig` so the worker completes `DopLogin` (`DopEcho` + `OrsSessions`) against the ORS `ControlServer`.
- **Zero WAL, Checkpoint, or Recovery-Closure Disk Reads:** Observation and readiness inspection ([`observation.rs`](../../crates/tickwire-strategy-host/src/observation.rs), [`readiness.rs`](../../crates/tickwire-strategy-host/src/readiness.rs)) inspect only live in-memory supervisor state, SHM ring sequence cursors, `OrsRuntimePolicy` (`active` / `enabled` / `Mode`), and live kernel `/proc/<pid>` + cgroup v2 telemetry via [`tickwire-control-sys`](../../crates/tickwire-control-sys/AGENTS.md) and [`tickwire-supervisor`](../../crates/tickwire-supervisor/AGENTS.md).

## Supervised Worker Lifecycle (`remote.rs`)

1. **Sealed ELF & Bootstrap Preparation:**
   - The host copies or seals the worker executable into an anonymous `F_SEAL_SEAL | F_SEAL_SHRINK | F_SEAL_GROW | F_SEAL_WRITE` `memfd` (`tickwire-supervisor`), writes `bootstrap.json` (`tickwire_strategy_sdk::Bootstrap`) inside the worker's `0o700` runtime directory, and registers `tickwire-md` admission (`AdmissionEntry`).
   - `Bootstrap` carries `logical_strategy_id`, `strat_id`, `account_ref`, optional `worker_cpu` and `housekeeping_cpus`, `market: ConnectConfig`, `dop_socket` + `DopConfig`, `intent` + `report` endpoints, and pinned `InstrumentCatalog` digest metadata.
2. **Authenticated Lane Attachment:**
   - Over `intent.sock`, the host verifies child `PeerCredentials` + `ProcessIdentity` (`pid`, `uid`, `starttime_ticks`), completes `ClientHandshake` / `ServerHello`, accepts the worker's sealed `IntentRecord` `memfd` (`offer_id = [7; 16]`), and returns the ORS `ReaderCursor` `memfd` (`offer_id = [8; 16]`) to form an `IntentReader`.
   - Over `report.sock`, the host creates the ORS `ReportRecord` `Publisher<1>` (`512B` slots), offers its read-only bank `memfd` (`offer_id = [14; 16]`) to the worker, and receives the worker's `ReaderCursor` `memfd` (`offer_id = [15; 16]`) to form a `ReportWriter`.
3. **Crash Detection & Clean Re-Spawn:**
   - `Registry::reap` detects worker exit immediately without polling files. On worker exit or operator stop, the host marks `strat_id` inactive (`OrsRuntimePolicy::set_active(strat_id, false)`) and notifies the ORS to cancel open orders for `strat_id`. Respawning increments `generation` and issues a fresh `instance_id` and `tickwire-md` admission ticket.

## Double-Sampled Live Resource Observation (`observation.rs`, `readiness.rs`)

Cold-path monitoring (`housekeeping_cpus` only; never called on hot `REQ`/`RESP` threads) samples worker health without `libc` calls in `tickwire-strategy-host`:

- **Process Identity Guard (`DoubleSampledObservation`):** Samples `ProcessIdentity` (`pid`, `starttime_ticks`) before and after reading `/proc/<pid>/stat`, `/proc/<pid>/status`, `/proc/<pid>/schedstat`, and optional cgroup v2 `memory.current` / `cpu.stat` counters (`tickwire_supervisor::ChildHandle::resources`). If the PID was recycled or exited mid-read (`before != after`), the sample fails closed with `ObservationError::IdentityChanged` rather than attributing stale metrics.
- **Stateless Readiness (`WorkerReadiness`):** Combines live process liveness (`Running`), attached SHM `IntentReader` / `ReportWriter` lane health, and `OrsRuntimePolicy` (`Mode::Trading`, `active[strat_id]`, `enabled[account_ref][strat_id]`) with monotonic age bounds (`FreshObservation`).

## Safety and Hygiene Invariants

1. `tickwire-strategy-host` uses `#![deny(unsafe_code)]` + `#![deny(unsafe_op_in_unsafe_fn)]` strictly for audited `tickwire_shm::Bank` / `IntentReader` / `ReportWriter` attachment invariants, with **zero direct `libc` dependency**—all OS process, signal, and CPU-affinity primitives delegate to `tickwire-control-sys` and `tickwire-supervisor`.
2. Zero floating-point arithmetic (`f32`/`f64`) and zero dependencies on retired journal, recovery, release, control-store, or venue-specific crates.
3. Verified by `cargo test --locked -p tickwire-strategy-host --all-targets`.
