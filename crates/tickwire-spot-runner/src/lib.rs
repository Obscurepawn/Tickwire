//! Stateless Unified Trading Account (`Bybit V5` and `OKX V5`) ORS runtime adapter and process
//! runner built directly on [`tickwire_runtime`] and [`tickwire_spot_execution`].
//!
//! # Architecture & Invariants
//!
//! - **Pure Stateless Execution**: Holds zero local write-ahead logs, journals, disk blob stores,
//!   or dynamic configuration patch files. Cold boot and restart recover open-order holds,
//!   derivative/option positions, `OrsStpBook` entries, strategy attribution, and per-strategy
//!   `DopState::next_order_seqnum` watermarks exclusively from authenticated Exchange REST
//!   snapshots (`Account + Positions + OpenOrders`) paired with 32-character `[0-9a-z]`
//!   `SelfDescribingClientId` (`tw1...`) tokens.
//! - **Generic `tickwire-runtime` Integration**: [`session::UtaVenueSession`] implements
//!   [`tickwire_runtime::VenueSession`] across all 6 Unified Trading Account products (`Spot`,
//!   `LinearPerpetual`, `InversePerpetual`, `LinearFuture`, `InverseFuture`, and `Option`),
//!   supporting `TimeInForce::Rpi`, shared `[local_ip, count]` pools (`product_mask = 0`) over
//!   both multiplexed WebSocket trade (`WsLink`) and pooled HTTP/1.1 keep-alive (`HttpsLink`),
//!   single-pick `SharedRateLimiter` NACK admission, optional Options/Block `RFQ` quoting,
//!   `execution.fast -> execution` fee `amendment`, and server-maintenance `notice` graceful
//!   connection switchover.
//! - **Strict Hot/Cold Isolation & Safety**: `#![forbid(unsafe_code)]`, zero raw OS-ABI crate
//!   calls, zero floating-point arithmetic, zero `sleep` calls on hot paths, and non-overlapping
//!   `Order` and `Connection` spinlocks with zero allocation, logging, or parsing inside any
//!   critical section.

#![forbid(unsafe_code)]

pub mod config;
pub mod paper_peer;
pub mod runner;
pub mod session;

pub use config::{
    PreparedSpotRunner, SpotRunnerConfig, UtaAssetConfig, UtaCpuSpec, UtaEndpointConfig,
    UtaEnvironmentKind, UtaProductKind, UtaSymbolConfig, UtaTransportKind, UtaVenueKind,
    collect_pem_certs,
};
pub use paper_peer::{PaperRunSummary, run_paper_demo};
pub use runner::{SpotInspectSummary, inspect_config, run_until_signal, spawn_uta_ors_process};
pub use session::{
    BybitVenueSession, OkxVenueSession, UtaEndpointSpec, UtaVenueSession, UtaVenueSessionConfig,
};
pub use tickwire_spot_execution::{Environment, SpotVenue, UtaTransportMode};
