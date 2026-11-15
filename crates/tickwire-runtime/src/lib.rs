//! Venue-agnostic Unified Order Routing System (`ORS`) runtime framework (`1 ORS process = 1 Venue`).
//!
//! `tickwire-runtime` implements the generic execution process coordinate plane without naming any
//! concrete exchange (`#![forbid(unsafe_code)]`, zero public market-data sockets, zero SHM quote
//! subscriptions, and zero local WAL/journal/lease persistence):
//!
//! - [`session`]: [`VenueSession`] trait and [`AccountSessionContext`] (`submit_new`,
//!   `submit_cancel`, `submit_modify`, optional RFQ quote hooks defaulting to
//!   [`tickwire_venue::RefusalReason::RfqQuoteNotSupported`], `on_message` / `on_disconnected` with
//!   `graceful_drain_connection` server-notice smooth switchover, and cold-path REST /
//!   `bootstrap_snapshots` hooks).
//! - [`ors`]: [`OrderRoutingSystem`], [`AccountSession`], [`OrsCpuConfig`], [`OrsRuntimeConfig`],
//!   busy-spin [`ReqWorker`] (`req_cpu`, enforcing the default `23 µs` `max_order_age_ns`
//!   stale-order gate with [`tickwire_venue::RefusalReason::RejectStaleOrder`]), and `mio::Poll`
//!   [`RespWorker`] (`resp_cpu`, `epoll_wait_ms = 0`).
//! - [`admin`]: Lock-free [`AdminCommands`] (`control -> REQ` and `control -> RESP`) and
//!   `housekeeping_cpus`-pinned Unix `SOCK_SEQPACKET` DOP [`ControlServer`] (`DopLogin`,
//!   `DopLogout`, idle `DopHeartBeat`, and operator `set_strat_enabled` auto-cancel).
//! - [`files`]: Cold-startup and off-thread `SIGUSR1` static RMS configuration file loader
//!   ([`load_rms_config_file`], [`build_rms_engine_from_file`], [`reload_rms_from_file`]).
//! - [`node`]: Multi-threaded [`OrsProcess`] isolating `REQ` and `RESP` hot CPUs from
//!   `housekeeping_cpus` cold threads (`ControlServer`, `Dialer`, `AsyncRestQuerier`,
//!   `LatencyTracer`, and `main` `SignalWaiter`) and executing deterministic `SIGUSR1`, `SIGUSR2`,
//!   and `SIGINT`/`SIGTERM` graceful shutdown.

#![forbid(unsafe_code)]

pub mod admin;
pub mod files;
pub mod node;
pub mod ors;
pub mod session;

pub use admin::{
    AdminCommands, AdminQueue, ControlServer, ControlServerConfig, DEFAULT_ADMIN_QUEUE_CAPACITY,
    DEFAULT_DOP_HB_INTERVAL_MS, DopAccountSessionInfo, ReqAdminCommand, RespAdminCommand,
};
pub use files::{
    RmsFileError, build_rms_engine_from_file, load_rms_config_file, reload_rms_from_file,
};
pub use node::{DEFAULT_SHUTDOWN_DRAIN_TIMEOUT, OrsProcess, OrsProcessConfig, OrsSignalOutcome};
pub use ors::{
    AccountSession, DEFAULT_MAX_ORDER_AGE_NS, DEFAULT_REQ_REPORT_QUEUE_CAPACITY, IntentLaneFactory,
    OrderRoutingSystem, OrsCpuConfig, OrsRuntimeConfig, ReportLaneFactory, ReqWorker, RespWorker,
};
pub use session::{
    AccountSessionContext, EncodedOrderFrame, LoginReply, OwnedOrderQuerySnapshot,
    QueryStatusRequest, RfqQuoteCancelRequest, RfqQuoteRequest, SessionEndpoint,
    SessionMessageEffects, SessionRequestKind, SessionRestEndpoint, SessionUrlPoolSpec,
    SessionVenueEvent, StatusReplyOutcome, SubmitCancelOutcome, SubmitModifyOutcome,
    SubmitNewOutcome, UrlRole, VenueSession,
};
