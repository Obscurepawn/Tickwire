use crate::*;
use std::{net::SocketAddr, sync::Arc};
#[derive(Clone, Copy, Debug)]
pub struct Timing {
    pub connect_ns: u64,
    pub request_ns: u64,
    pub renew_ns: u64,
    pub rotate_ns: u64,
    pub heartbeat_ns: u64,
    pub backoff_ns: u64,
    pub max_backoff_ns: u64,
    pub recv_window_micros: u64,
}
impl Default for Timing {
    fn default() -> Self {
        Self {
            connect_ns: 10_000_000_000,
            request_ns: 5_000_000_000,
            renew_ns: 1_800_000_000_000,
            rotate_ns: 82_800_000_000_000,
            heartbeat_ns: 60_000_000_000,
            backoff_ns: 100_000_000,
            max_backoff_ns: 30_000_000_000,
            recv_window_micros: 5_000_000,
        }
    }
}
#[derive(Clone, Copy, Debug)]
pub struct SessionLimits {
    pub request_bytes: usize,
    pub message_bytes: usize,
    pub early_messages: usize,
    pub early_bytes: usize,
    pub control_frames: usize,
    pub io: tickwire_transport::Limits,
}
impl Default for SessionLimits {
    fn default() -> Self {
        Self {
            request_bytes: 16_384,
            message_bytes: 1 << 20,
            early_messages: 32,
            early_bytes: 1 << 20,
            control_frames: 8,
            io: tickwire_transport::Limits::default(),
        }
    }
}
#[derive(Clone, Debug)]
pub struct SessionConfig {
    pub scope: AccountScope,
    pub timing: Timing,
    pub limits: SessionLimits,
}
impl SessionConfig {
    pub fn new(scope: AccountScope) -> Self {
        let mut timing = Timing::default();
        if scope.market == Market::Usdm {
            timing.heartbeat_ns = 600_000_000_000;
        }
        Self {
            scope,
            timing,
            limits: SessionLimits::default(),
        }
    }
}
pub struct NetworkConfig {
    pub stream_address: SocketAddr,
    pub management_address: Option<SocketAddr>,
    pub tls: Arc<rustls::ClientConfig>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    Disconnected,
    Backoff,
    Connecting,
    Authenticating,
    Subscribing,
    CreatingKey,
    Receiving,
    Closing,
    Stopped,
    Exhausted,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GapToken {
    pub(crate) scope: AccountScope,
    pub(crate) epoch: u64,
    pub(crate) generation: u64,
}
impl GapToken {
    pub fn scope(&self) -> &AccountScope {
        &self.scope
    }
    pub fn connection_epoch(&self) -> u64 {
        self.epoch
    }
    pub fn generation(&self) -> u64 {
        self.generation
    }
}
#[derive(Clone, Debug)]
pub struct GapNotice {
    pub token: GapToken,
    pub reason: Error,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SessionStatus {
    pub phase: Phase,
    pub connection_epoch: u64,
    pub subscription_id: Option<u64>,
    pub lease_until_ns: Option<u64>,
    pub renew_at_ns: Option<u64>,
    pub request: Option<RequestToken>,
    pub remote_close_unknown: bool,
    pub registration_changed: bool,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChargeKind {
    StreamConnect,
    ManagementConnect,
    Upgrade,
    Api(Operation),
    Pong,
    Close,
}
#[derive(Clone, Debug)]
pub struct Charge {
    pub scope: AccountScope,
    pub connection_epoch: u64,
    pub attempt: u64,
    pub kind: ChargeKind,
    pub bytes: usize,
    pub digest: [u8; 32],
    pub api_weight: u32,
    pub websocket_messages: u32,
}
pub trait ControlPermit {
    fn allow(&mut self, charge: &Charge, now: &ClockSample) -> bool;
}
impl<F: FnMut(&Charge, &ClockSample) -> bool> ControlPermit for F {
    fn allow(&mut self, c: &Charge, n: &ClockSample) -> bool {
        self(c, n)
    }
}
#[derive(Clone, Copy, Debug)]
pub struct PollBudget {
    pub io: tickwire_transport::IoBudget,
    pub max_messages: usize,
    pub max_control_requests: usize,
}
impl Default for PollBudget {
    fn default() -> Self {
        Self {
            io: tickwire_transport::IoBudget::default(),
            max_messages: 32,
            max_control_requests: 2,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ControlState {
    NoBytes,
    MayHaveApplied,
    Confirmed,
    Rejected,
}
#[derive(Clone, Copy, Debug)]
pub struct ControlOutcome {
    pub token: RequestToken,
    pub state: ControlState,
    pub status: Option<u16>,
    pub code: Option<i64>,
}
// Callbacks use a stack value; boxing would add an allocation per private event.
#[allow(clippy::large_enum_variant)]
pub enum Delivery<'a> {
    State(SessionStatus),
    Gap(GapNotice),
    Event(PrivateEvent<'a>),
    Control(ControlOutcome),
}
#[derive(Clone, Copy, Debug)]
pub enum StopMode {
    Disconnect,
    CloseRemoteStream,
}
#[derive(Clone, Copy, Debug)]
pub struct RegistrationTokens {
    pub stream: mio::Token,
    pub management: mio::Token,
}
#[derive(Clone, Copy, Debug, Default)]
pub struct PollReport {
    /// A failed drive may have consumed unreported IO; no further IO this poll.
    pub io_budget_exhausted: bool,
    pub syscalls: usize,
    pub network_bytes: usize,
    pub messages: usize,
    pub control_requests: usize,
    pub yielded: bool,
    pub registration_changed: bool,
}
