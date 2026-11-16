# `tickwire-strategy-host` Engineering Guide (`AGENTS.md`)

Governing specification: [`specs/implementation/runtime-observation.md`](../../specs/implementation/runtime-observation.md).

## Single-Writer & Process Topology Ownership

`tickwire-strategy-host` supervises sealed strategy worker processes (`tickwire-supervisor`) and samples cold-path OS / cgroup v2 telemetry in Tickwire's pure stateless 3-process shared-memory topology:

- **External `tickwire-md` SHM Join Only:** Supervised strategy workers always attach to an external `tickwire-md` market-data publisher (`tickwire_market_source::ConnectConfig`). Embedded local-publisher fallbacks and cutover state machines are absent. After spawning the sealed ELF via `Registry`, `RemoteHostedStrategy` registers the one-shot `tickwire_md::server::Admission` ticket bound to the actual kernel `ProcessIdentity` (`pid`, `uid`, `gid`).
- **Stateless Inline SHM Lanes & DOP Wiring (`remote.rs`):** Provisions a `0o700` `PrivateDirectory`, binds authenticated `intent.sock` and `report.sock` Unix `SOCK_SEQPACKET` listeners, verifies kernel `SO_PEERCRED` + sealed `/proc/<pid>/exe` identity, and exchanges `F_SEAL_FUTURE_WRITE` `memfd` descriptors to form `IntentReader` (`1,024B` `IntentRecord`) and `ReportWriter` (`512B` `ReportRecord`) with zero disk-backed queue or blob store. Also provides audited cross-thread SPSC ring attachment factories (`prepare_cross_thread_intent_lane`, `prepare_cross_thread_report_lane`) so `"tw-ors-req"` and `"tw-ors-resp"` map their SPSC rings directly on their pinned hot threads without `unsafe impl Send`.
- **Cold-Path Double-Sampled Observation ([`src/observation.rs`](src/observation.rs), [`src/readiness.rs`](src/readiness.rs), [`src/resources.rs`](src/resources.rs)):** Samples `LiveChildObservation` (`pid`, `start_ticks`, `host_boot`, `image_hash`, `image_bytes`) before and after optional cgroup v2 `LiveResourceObservation` (`DoubleSampledObservation` / `FreshObservation`), failing closed with `ObservationError::IdentityChanged` if the child exited or was recycled mid-read, and combines process liveness with `OrsRuntimePolicy` (`mode`, `is_connected`, `is_active`, `is_strat_enabled`) inside `WorkerReadiness`.

## Safety & Hygiene Invariants

1. `#![deny(unsafe_code)]` and `#![deny(unsafe_op_in_unsafe_fn)]`: `unsafe` blocks exist strictly at documented `IntentReader::attach`, `ReportReceiver::attach`, and `Publisher::register_cursor` SHM attachment boundaries in [`src/remote.rs`](src/remote.rs) (tested in [`tests/stateless_host.rs`](tests/stateless_host.rs)).
2. Zero direct raw C-ABI dependency—all OS credential, `/proc`, cgroup v2, signal, and CPU-affinity operations delegate to `tickwire-control-sys` and `tickwire-supervisor`.
3. Zero integer-violating float arithmetic, zero WAL/journal/checkpoint disk state, and zero venue-specific dependencies.
