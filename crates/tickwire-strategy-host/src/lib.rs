//! Stateless parent-owned strategy worker supervision (`RemoteHostedStrategy`), audited cross-thread
//! SHM SPSC lane factories, explicit cgroup v2 resource plans (`ProcessResources`), and cold-path
//! double-sampled kernel telemetry (`DoubleSampledObservation`, `FreshObservation`).
//!
//! Workers always join an external `tickwire-md` market-data publisher over sealed `memfd` rings
//! and exchange inline `1,024B` `IntentRecord` and `512B` `ReportRecord` slots with the ORS
//! execution process—with zero WAL, journal, disk-backed queue, or raw OS-ABI crate dependencies.
#![deny(unsafe_code)]
#![deny(unsafe_op_in_unsafe_fn)]

use std::fmt;

pub mod observation;
pub mod readiness;
pub mod remote;
pub mod resources;

pub use observation::{
    DoubleSampledObservation, FreshObservation, ObservationError, ObservationRequest,
};
pub use readiness::{ReadyProof, WorkerReadiness};
pub use remote::{
    AdmissionRegisteredHook, CrossThreadIntentReaderFactory, CrossThreadIntentStarter,
    CrossThreadReportStarter, CrossThreadReportWriterFactory, RemoteHostedStrategy, RemoteLaunch,
    StrategyLanes, prepare_cross_thread_intent_lane, prepare_cross_thread_report_lane,
};
pub use resources::{GroupResources, ProcessResources};

/// Errors produced by `tickwire-strategy-host`.
#[derive(Debug)]
pub enum Error {
    Invalid,
    Timeout,
    Disconnected,
    LanesDetached,
    Io(std::io::Error),
    Json(serde_json::Error),
    Clock(tickwire_clock_sys::Error),
    Control(tickwire_control::Error),
    Transport(tickwire_control_sys::Error),
    Wire(tickwire_ipc::Error),
    MarketSource(tickwire_market_source::Error),
    Shm(tickwire_shm::Error),
    MappedShm(tickwire_shm::MappedError),
    Sdk(tickwire_strategy_sdk::Error),
    Supervisor(tickwire_supervisor::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<serde_json::Error> for Error {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(value)
    }
}

impl From<tickwire_clock_sys::Error> for Error {
    fn from(value: tickwire_clock_sys::Error) -> Self {
        Self::Clock(value)
    }
}

impl From<tickwire_control::Error> for Error {
    fn from(value: tickwire_control::Error) -> Self {
        Self::Control(value)
    }
}

impl From<tickwire_control_sys::Error> for Error {
    fn from(value: tickwire_control_sys::Error) -> Self {
        Self::Transport(value)
    }
}

impl From<tickwire_ipc::Error> for Error {
    fn from(value: tickwire_ipc::Error) -> Self {
        Self::Wire(value)
    }
}

impl From<tickwire_market_source::Error> for Error {
    fn from(value: tickwire_market_source::Error) -> Self {
        Self::MarketSource(value)
    }
}

impl From<tickwire_shm::Error> for Error {
    fn from(value: tickwire_shm::Error) -> Self {
        Self::Shm(value)
    }
}

impl From<tickwire_shm::MappedError> for Error {
    fn from(value: tickwire_shm::MappedError) -> Self {
        Self::MappedShm(value)
    }
}

impl From<tickwire_strategy_sdk::Error> for Error {
    fn from(value: tickwire_strategy_sdk::Error) -> Self {
        Self::Sdk(value)
    }
}

impl From<tickwire_supervisor::Error> for Error {
    fn from(value: tickwire_supervisor::Error) -> Self {
        Self::Supervisor(value)
    }
}
