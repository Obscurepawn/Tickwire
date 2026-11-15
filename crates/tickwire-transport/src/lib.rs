//! Bounded, explicitly driven `#![forbid(unsafe_code)]` nonblocking network transport,
//! vtable-free multi-protocol execution `LinkOps` (`WsLink`, `HttpsLink`, `FixLink`),
//! zero-allocation single-pass `JsonScan` / `MsgParser`, lock-free `PublishedRoutes` EWMA routing,
//! hardware `rdtsc` `LatencyTracer` & `TimedSpinLock`, and `housekeeping_cpus`-pinned `Dialer` and
//! `AsyncRestQuerier` workers.
#![forbid(unsafe_code)]

pub use mio;
pub use rustls;
use std::{
    io,
    sync::{Arc, OnceLock},
    time::Instant,
};

#[derive(Debug)]
pub enum Error {
    Bounds,
    Busy,
    Protocol(&'static str),
    Io(io::Error),
    Tls(rustls::Error),
    Closed,
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}
impl From<rustls::Error> for Error {
    fn from(e: rustls::Error) -> Self {
        Self::Tls(e)
    }
}
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub max_send_bytes: usize,
    pub max_receive_bytes: usize,
    pub tls_buffer_bytes: usize,
    pub max_accept_bytes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            max_send_bytes: 1 << 20,
            max_receive_bytes: 1 << 20,
            tls_buffer_bytes: 16384,
            max_accept_bytes: 16384,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct IoBudget {
    pub max_syscalls: usize,
    pub max_bytes: usize,
}
impl Default for IoBudget {
    fn default() -> Self {
        Self {
            max_syscalls: 8,
            max_bytes: 65536,
        }
    }
}

pub enum Security {
    Plain,
    Tls {
        server_name: rustls::pki_types::ServerName<'static>,
        config: Arc<rustls::ClientConfig>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Failure {
    Deadline,
    GateDenied,
    Cancelled,
    Disconnected,
    Io,
    Tls,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SendState {
    Queued,
    MayHaveSent,
    Flushed,
    NoBytes(Failure),
    Failed(Failure),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SendReport {
    pub id: u64,
    pub state: SendState,
    pub accepted_bytes: usize,
    pub total_bytes: usize,
}

pub struct PreparedSend {
    bytes: Vec<u8>,
    deadline: Instant,
}
impl PreparedSend {
    pub fn new(bytes: &[u8], deadline: Instant, max_bytes: usize) -> Result<Self> {
        if bytes.is_empty() || bytes.len() > max_bytes || max_bytes > 16 * 1024 * 1024 {
            return Err(Error::Bounds);
        }
        Ok(Self {
            bytes: bytes.to_vec(),
            deadline,
        })
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct DriveReport {
    pub ready: bool,
    pub received: usize,
    pub network_read: usize,
    pub network_written: usize,
    pub syscalls: usize,
    pub peer_closed: bool,
}

pub type ClockFn = Arc<dyn Fn() -> u64 + Send + Sync>;

pub fn mono_now_ns() -> u64 {
    static EPOCH: OnceLock<Instant> = OnceLock::new();
    let base = *EPOCH.get_or_init(Instant::now);
    1_000_000_000u64.saturating_add(u64::try_from(base.elapsed().as_nanos()).unwrap_or(u64::MAX))
}

mod connection;
pub use connection::Connection;
pub mod dialer;
pub mod fix;
pub mod http;
pub mod json_scan;
pub use json_scan::{JsonScan, MsgParser, ScannedExecFrame, ScannedRateLimit};
pub mod latency_tracer;
pub use latency_tracer::{
    FindingKind, LatencyFinding, LatencyTracer, LatencyTracerConfig, LockDomain, LockId, MarkSite,
    ThreadTag, ThreadTracer, TimedSpinLock,
};
pub mod links;
pub use links::{
    FixInboundAction, FixLink, FixSessionConfig, FixSessionState, GuardedValue, HttpsLink,
    LinkKind, LinkOps, LockedConnection, ReplyIdSlot, RequestRef, SendOutcome, UrlBindPair,
    UrlConnections, WsLink, WsLoginChallenge, WsLoginState,
};
pub mod order_router;
pub use order_router::{PublishedRoutes, SampleMask};
pub mod rest_querier;
pub mod warmup;
pub use warmup::{TlsLoopbackWarmer, WarmupConfig, WarmupTracker};
pub mod ws;
