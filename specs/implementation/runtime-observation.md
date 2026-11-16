# Stateless Strategy Supervision and Runtime Observation (`tickwire-strategy-host`)

## Scope and Ownership

[`tickwire-strategy-host`](../../crates/tickwire-strategy-host/src/lib.rs) supervises sealed strategy worker processes (`tickwire-supervisor`) and collects live OS / cgroup v2 resource observations in Tickwire's pure stateless 3-process shared-memory topology:

- **External `tickwire-md` SHM Join Only:** Strategy workers **always** join an external `tickwire-md` market-data server (`tickwire_market_source::ConnectConfig`). Local embedded-publisher fallbacks and cutover state machines are absent. Before spawning a worker, `tickwire-strategy-host` registers the worker's one-shot admission ticket via `tickwire_md::server::register_admission` so `tickwire-md` can verify kernel `SO_PEERCRED` + `/proc/<pid>/stat` process identity.
- **Direct ORS Inline SHM Rings & DOP Wiring:** For each supervised strategy worker (`strat_id: 0..=254`), `tickwire-strategy-host` provisions a `0o700` `PrivateDirectory`, binds authenticated `intent.sock` and `report.sock` Unix `SOCK_SEQPACKET` endpoints, attaches the ORS-side `IntentReader` (`1,024B` `IntentRecord`) and `ReportWriter` (`512B` `ReportRecord` backed by sealed `memfd`), wires `Bootstrap.dop_socket` + `Bootstrap.dop` (`DopConfig`) so the worker completes `DopLogin` (`DopEcho` + `OrsSessions`) against the ORS `ControlServer`, and exposes audited cross-thread SPSC ring factories (`prepare_cross_thread_intent_lane`, `prepare_cross_thread_report_lane`, `CrossThreadIntentStarter`, `CrossThreadReportStarter`) so `"tw-ors-req"` and `"tw-ors-resp"` map their SPSC rings directly on their pinned hot threads without `unsafe impl Send`.
- **Zero WAL, Checkpoint, or Recovery-Closure Disk Reads:** Observation and readiness inspection ([`observation.rs`](../../crates/tickwire-strategy-host/src/observation.rs), [`readiness.rs`](../../crates/tickwire-strategy-host/src/readiness.rs), [`resources.rs`](../../crates/tickwire-strategy-host/src/resources.rs)) inspect only live in-memory supervisor state, SHM ring sequence cursors, `OrsRuntimePolicy` (`mode`, `is_connected`, `is_active`, `is_strat_enabled`), and live kernel `/proc/<pid>` + cgroup v2 telemetry via [`tickwire-control-sys`](../../crates/tickwire-control-sys/AGENTS.md) and [`tickwire-supervisor`](../../crates/tickwire-supervisor/AGENTS.md).

## Supervised Worker Lifecycle (`remote.rs`)

1. **Sealed ELF & Bootstrap Preparation (`RemoteLaunch` -> `RemoteHostedStrategy::launch`):**
   - The host copies and seals the worker executable into an anonymous `F_SEAL_SEAL | F_SEAL_SHRINK | F_SEAL_GROW | F_SEAL_WRITE` `memfd` (`tickwire-supervisor::Registry`), writes `worker.json` (`tickwire_strategy_sdk::Bootstrap`, `0o600`) inside the worker's `0o700` runtime directory, spawns the child under optional `ProcessResources` cgroup v2 limits, and registers `tickwire-md` admission (`tickwire_md::server::Admission`).
   - `Bootstrap` carries `logical_strategy_id`, `strat_id`, `instance_id`, `generation`, `account_ref`, optional `worker_cpu` and `housekeeping_cpus`, `market: ConnectConfig`, optional `dop_socket` + `dop: Option<DopConfig>`, `intent` + `report` endpoints, and optional `instrument_catalog` or `catalog_file` + `catalog_digest`.
2. **Authenticated Lane Attachment:**
   - Over `intent.sock`, `Registry::accept` verifies child `SO_PEERCRED` + `ProcessIdentity` (`pid`, `uid`, `gid`) and sealed `/proc/<pid>/exe` continuity across `HELLO` / `WELCOME`, accepts the worker's sealed `IntentRecord` `memfd` (`offer_id = [7; 16]`), attaches `IntentReader`, and returns the ORS `Subscriber` cursor `memfd` (`offer_id = [8; 16]`).
   - Over `report.sock`, the host verifies the same owned `ProcessIdentity`, completes `Session::accept`, offers its `ReportRecord` `Publisher<1>` (`512B` slots) read-only bank `memfd` (`offer_id = [14; 16]`) to the worker, and receives and registers the worker's `Subscriber` cursor `memfd` (`offer_id = [15; 16]`) to form a `ReportWriter`.
3. **Crash Detection & Clean Re-Spawn:**
   - `RemoteHostedStrategy::maintain` polls `Registry::observe_exits` without polling files. On worker exit or `RemoteHostedStrategy::terminate`, the host marks `strat_id` inactive (`OrsRuntimePolicy::set_active(strat_id, false)`) and caches `ExitObservation`. Respawning increments `generation` and issues a fresh `instance_id` and `tickwire-md` `Admission` ticket.

## Double-Sampled Live Resource Observation (`observation.rs`, `readiness.rs`)

Cold-path monitoring (`housekeeping_cpus` only; never called on hot `REQ`/`RESP` threads) samples worker health without raw OS-ABI calls in `tickwire-strategy-host`:

- **Process Identity Guard (`DoubleSampledObservation`):** Calls `Registry::observe` (`LiveChildObservation`) before and after optional cgroup v2 `Registry::observe_resources` (`LiveResourceObservation`), comparing `identity()`, `process()`, `start_ticks()`, `host_boot()`, `image_hash()`, and `image_bytes()`. If the PID exited, replaced its image, or was recycled mid-read (`before != after`), the sample fails closed with `ObservationError::IdentityChanged` (or `ObservationError::StaleSample` if elapsed sampling time exceeds `ObservationRequest::max_sample_age_ns`, default `50 ms`).
- **Stateless Readiness (`ReadyProof` & `WorkerReadiness`):** Combines live `process_alive` status, `lanes_attached`, `intent_last_msg_seqnum`, `report_next_seq`, and `OrsRuntimePolicy` (`mode`, `dop_logged_in`, `active`, `account_enabled` on `resolved_account_ref`) inside `WorkerReadiness` (`is_ready_to_trade()`, `can_manage_orders()`), paired with monotonic age bounds in `FreshObservation::collect`.

## Safety and Hygiene Invariants

1. `tickwire-strategy-host` uses `#![deny(unsafe_code)]` + `#![deny(unsafe_op_in_unsafe_fn)]` strictly for audited `IntentReader::attach`, `ReportReceiver::attach`, and `Publisher::register_cursor` attachment invariants, with **zero direct `libc` dependency**—all OS process, signal, and CPU-affinity primitives delegate to `tickwire-control-sys` and `tickwire-supervisor`.
2. Zero floating-point arithmetic (`f32`/`f64`) and zero dependencies on retired journal, recovery, release, control-store, or venue-specific crates.
3. Verified by `cargo test --locked -p tickwire-strategy-host --all-targets` ([`tests/stateless_host.rs`](../../crates/tickwire-strategy-host/tests/stateless_host.rs)).
