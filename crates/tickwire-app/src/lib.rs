//! Unified stateless single-venue Order Routing System (`ORS`) execution process (`tickwire-app`,
//! `1 ORS process = 1 Venue`).
//!
//! # Architecture & Invariants
//!
//! - **Pure Stateless 3-Process Topology**: Holds zero public market-data sockets, zero SHM quote
//!   subscriptions, zero local write-ahead logs, zero journals, zero disk blob files, and zero
//!   dynamic configuration patches. Cold boot and restart recover balances, derivative/option
//!   positions, open-order holds, `OrsStpBook` entries, strategy attribution, and per-strategy
//!   `DopState::next_order_seqnum` watermarks exclusively from authenticated Exchange REST
//!   snapshots (`Account + OpenOrders`) paired with 32-character `[0-9a-z]`
//!   `SelfDescribingClientId` (`tw1...`) tokens.
//! - **Unified Venue Selector (`1 ORS process = 1 Venue`)**: [`venue::UnifiedVenueSession`] wires
//!   [`binance_session::BinanceVenueSession`] (Spot, USD-M, Coin-M, and European Options) and
//!   [`tickwire_spot_runner::UtaVenueSession`] (`Bybit V5` and `OKX V5` 6-product Unified Trading
//!   Accounts) directly onto [`tickwire_runtime::OrderRoutingSystem`] and
//!   [`tickwire_runtime::OrsProcess`].
//! - **Disjoint Hot vs. Housekeeping Cores & Lock Discipline**: Isolates dedicated `REQ`
//!   (`req_cpu`, enforcing the default `23,000 ns` microsecond stale-order gate with
//!   `RefusalReason::RejectStaleOrder`) and `RESP` (`resp_cpu`, `epoll_wait_ms = 0`) threads from
//!   `housekeeping_cpus` (`ControlServer`, `Dialer`, `AsyncRestQuerier`, `LatencyTracer`, and
//!   `SignalWaiter`), never overlapping `Order` and `Connection` spinlocks and performing zero
//!   allocations, logging, or parsing inside any spinlock.

#![deny(unsafe_code)]
#![deny(unsafe_op_in_unsafe_fn)]

pub mod binance_session;
pub mod config;
pub mod external_lanes;
pub mod runtime;
pub mod venue;

pub use binance_session::{BinanceMarketEndpoint, BinanceVenueSession, BinanceVenueSessionConfig};
pub use config::{
    BinanceAssetConfig, BinanceMarketConfigBlock, BinanceMarketKind, BinanceSymbolConfig,
    CatalogVerificationConfig, OrsAppConfig, OrsVenueKind, PreparedOrsApp,
};
pub use external_lanes::{
    ExternalLaneKeepAlive, ExternalStrategyLaneConfig, prepare_external_strategy_lanes,
};
pub use runtime::{
    OrsAppHandle, OrsInspectSummary, OrsPositionSummary, OrsShutdownSummary,
    OrsWalletBalanceSummary, inspect_ors_config, run_unified_ors_until_signal,
    spawn_unified_ors_process,
};
pub use tickwire_spot_runner::{
    UtaAssetConfig, UtaCpuSpec, UtaEndpointConfig, UtaEnvironmentKind, UtaSymbolConfig,
    UtaTransportKind,
};
pub use venue::UnifiedVenueSession;
