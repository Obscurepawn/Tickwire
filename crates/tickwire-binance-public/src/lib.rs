//! Explicitly driven public Binance JSON depth: WSS subscription plus HTTPS snapshot.
#![forbid(unsafe_code)]
use std::{net::SocketAddr, sync::Arc};
pub use tickwire_binance::{
    depth_sync::{Config as DepthConfig, DepthPolicy, Outcome as SyncOutcome, State as DepthState},
    feed::TimeUnit,
};
pub use tickwire_binance_clock::{
    ClockDomain, LocalTime, MonoTime, MonotonicClock, Source, SystemClock,
};
pub use tickwire_book::{BinanceSpot, BinanceUsdM, BookView};
pub use tickwire_transport::{IoBudget, Limits as IoLimits, mio, rustls};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Invalid,
    Bounds,
    State,
    Denied,
    Exhausted,
    Clock,
    Transport,
    Protocol,
    Timeout,
    Closed,
    Sink,
    View(tickwire_book::ViewError),
    Sync(tickwire_binance::depth_sync::SyncError),
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SubscribeMode {
    RawUrl,
    Explicit,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DepthSpeed {
    Default,
    Ms100,
    Ms500,
}
#[derive(Clone, Copy, Debug)]
pub struct Timing {
    pub connect_ns: u64,
    pub request_ns: u64,
    pub first_event_ns: u64,
    pub heartbeat_ns: u64,
    pub pong_ns: u64,
    pub rotate_ns: u64,
    pub backoff_ns: u64,
    pub max_backoff_ns: u64,
}
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub io: IoLimits,
    pub request_bytes: usize,
    pub message_bytes: usize,
    pub http_bytes: usize,
    pub http_headers: usize,
    pub control_frames: usize,
    pub timestamp_segments: usize,
    pub total_buffer_bytes: usize,
}
#[derive(Clone, Debug)]
pub struct Config {
    pub owner_id: [u8; 16],
    pub source: Source,
    pub domain: ClockDomain,
    pub first_connection_epoch: u64,
    pub first_operation: u64,
    pub depth: DepthConfig,
    pub subscribe: SubscribeMode,
    pub speed: DepthSpeed,
    pub timing: Timing,
    pub limits: Limits,
}
pub struct NetworkConfig {
    pub stream_address: SocketAddr,
    pub snapshot_address: SocketAddr,
    pub tls: Arc<rustls::ClientConfig>,
}
/// Explicit access to the official Spot SBE endpoint. The server validates key type;
/// this client never logs the key or sends it to the REST snapshot endpoint.
pub struct SbeAccess {
    api_key: String,
}
impl SbeAccess {
    pub fn ed25519_api_key(api_key: String) -> Result<Self> {
        if api_key.is_empty()
            || api_key.len() > 512
            || !api_key.bytes().all(|b| b.is_ascii_alphanumeric())
        {
            return Err(Error::Invalid);
        }
        Ok(Self { api_key })
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Lane {
    Stream,
    Snapshot,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChargeKind {
    Connect,
    Upgrade,
    Subscribe,
    Pong,
    Close,
    Snapshot,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PublicCharge {
    pub owner_id: [u8; 16],
    pub domain: ClockDomain,
    pub source: Source,
    pub lane: Lane,
    pub connection_epoch: u64,
    pub operation: u64,
    pub kind: ChargeKind,
    pub api_weight: u32,
}
#[derive(Debug)]
pub struct HttpFeedback {
    pub charge: PublicCharge,
    pub status: u16,
    pub headers: Vec<(String, Vec<u8>)>,
}
pub trait PublicPermit {
    /// Recheck every zero-accept attempt; charging is idempotent for the full identity.
    fn allow(&mut self, charge: &PublicCharge, now: MonoTime) -> bool;
    fn observe_http(&mut self, feedback: &HttpFeedback);
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    Idle,
    Connecting,
    Upgrading,
    Subscribing,
    Receiving,
    Closing,
    Backoff,
    Stopped,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Status {
    pub phase: Phase,
    pub connection_epoch: u64,
    pub book_generation: u64,
    pub depth: DepthState,
    pub last_update_id: Option<u64>,
    pub snapshot_pending: bool,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GapNotice {
    pub connection_epoch: u64,
    pub book_generation: u64,
    pub reason: Error,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DepthEncoding {
    Json,
    SpotSbeV1,
}
pub enum Notice<'a> {
    Sync {
        connection_epoch: u64,
        book_generation: u64,
        received_mono_ns: Option<u64>,
        /// Exact JSON event or SBE binary frame; borrowed only during this callback.
        raw_delta: Option<&'a [u8]>,
        encoding: DepthEncoding,
        outcome: SyncOutcome,
        view: Option<BookView<'a>>,
    },
    Gap(GapNotice),
    Http(&'a HttpFeedback),
}
pub trait PublicSink {
    /// False permanently stops this source. Any view is borrowed for this callback only.
    fn on_notice(&mut self, notice: Notice<'_>) -> bool;
}
#[derive(Clone, Copy, Debug)]
pub struct PollBudget {
    pub io: IoBudget,
    pub max_ws_messages: usize,
    pub max_decoder_turns: usize,
    pub max_notifications: usize,
}
impl Default for PollBudget {
    fn default() -> Self {
        Self {
            io: IoBudget::default(),
            max_ws_messages: 32,
            max_decoder_turns: 64,
            max_notifications: 64,
        }
    }
}
#[derive(Clone, Copy, Debug, Default)]
pub struct PollReport {
    pub io_budget_exhausted: bool,
    pub syscalls: usize,
    pub network_bytes: usize,
    pub decoded: usize,
    pub applied: usize,
    pub ignored: usize,
    pub runnable: bool,
    pub needs_registration: bool,
    pub next_deadline_ns: Option<u64>,
}
#[derive(Clone, Copy, Debug)]
pub struct IoTokens {
    pub stream: mio::Token,
    pub snapshot: mio::Token,
}
mod client;
mod protocol;
pub use client::PublicDepthClient;
