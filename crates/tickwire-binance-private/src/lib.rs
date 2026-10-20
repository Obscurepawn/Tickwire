//! Authenticated, bounded private stream sessions over actual nonblocking sockets.
#![forbid(unsafe_code)]
use std::time::Instant;
pub use tickwire_binance::{
    auth::SigningKey,
    events::{SessionContext, SessionIdentity},
    request::{Market, Method, Timestamp},
};
pub use tickwire_clock::{ClockDomain, MonoTime, SignedTime};
pub use tickwire_transport::{mio, rustls};
pub use tickwire_venue::Environment;

mod client;
pub mod request;
mod response;
mod session;
pub use client::PrivateClient;
pub use session::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Bounds,
    Identity,
    Unsupported,
    Clock,
    ClockDomain,
    Stale,
    Busy,
    Protocol,
    Transport,
    Authentication,
    Capacity,
    Closed,
    Exhausted,
    Sink,
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AccountScope {
    pub risk_domain_id: [u8; 16],
    pub venue: u32,
    pub environment: Environment,
    pub account: u64,
    pub market: Market,
    pub credential_generation: u64,
}
impl AccountScope {
    pub fn validate(&self) -> Result<()> {
        if self.risk_domain_id == [0; 16]
            || self.venue != 1
            || self.account == 0
            || self.credential_generation == 0
        {
            return Err(Error::Identity);
        }
        Ok(())
    }
}
pub enum Authentication<'a> {
    SpotLogon {
        api_key: &'a str,
        signing_key: &'a SigningKey,
    },
    SpotSignature {
        api_key: &'a str,
        signing_key: &'a SigningKey,
    },
    UsdmApiKey {
        api_key: &'a str,
    },
}
impl std::fmt::Debug for Authentication<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Authentication([REDACTED])")
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Operation {
    SpotLogon,
    SpotSubscribe,
    SpotSubscribeSignature,
    SpotStatus,
    SpotSubscriptions,
    SpotUnsubscribe { subscription_id: u64 },
    SpotLogout,
    UsdmCreate,
    UsdmKeepalive,
    UsdmClose,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RequestToken {
    connection_epoch: u64,
    serial: u64,
    operation: Operation,
}
impl RequestToken {
    pub fn new(connection_epoch: u64, serial: u64, operation: Operation) -> Result<Self> {
        if connection_epoch == 0 || serial == 0 {
            return Err(Error::Identity);
        }
        Ok(Self {
            connection_epoch,
            serial,
            operation,
        })
    }
    pub fn connection_epoch(&self) -> u64 {
        self.connection_epoch
    }
    pub fn serial(&self) -> u64 {
        self.serial
    }
    pub fn operation(&self) -> Operation {
        self.operation
    }
    pub fn id(&self) -> String {
        format!("{}-{}", self.connection_epoch, self.serial)
    }
}
#[derive(Clone, Copy, Debug)]
pub struct ClockSample {
    pub instant: Instant,
    pub monotonic: MonoTime,
    pub signing_time: Option<SignedTime>,
}
impl ClockSample {
    pub fn monotonic_ns(&self) -> u64 {
        self.monotonic.nanos()
    }
    pub fn signing_timestamp(&self) -> Option<Timestamp> {
        self.signing_time.map(|t| match t.unit() {
            tickwire_clock::TimeUnit::Milliseconds => Timestamp::Milliseconds(t.timestamp()),
            tickwire_clock::TimeUnit::Microseconds => Timestamp::Microseconds(t.timestamp()),
        })
    }
}
pub trait Clock {
    fn sample(&mut self) -> Result<ClockSample>;
    /// Check the original signed stamp against the current calibrated interval and
    /// the adapter's remaining delivery budget. There is no default authorization.
    fn check_signed(
        &mut self,
        scope: &AccountScope,
        stamp: &SignedTime,
        now: &ClockSample,
        recv_window_micros: u64,
    ) -> Result<()>;
}

/// Borrowed event; raw bytes are available explicitly but never formatted by Debug.
pub struct PrivateEvent<'a>(pub(crate) tickwire_binance::events::PrivateEvent<'a>);
impl std::fmt::Debug for PrivateEvent<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrivateEvent")
            .field("session", self.session())
            .field("received_mono_ns", &self.received_mono_ns())
            .field("payload", &"[REDACTED]")
            .finish()
    }
}
impl<'a> PrivateEvent<'a> {
    pub fn session(&self) -> &SessionContext {
        self.0.session()
    }
    pub fn body(&self) -> &tickwire_binance::events::PrivateBody<'a> {
        self.0.body()
    }
    pub fn raw(&self) -> &'a serde_json::value::RawValue {
        self.0.raw()
    }
    pub fn event_time_ns(&self) -> u64 {
        self.0.event_time_ns()
    }
    pub fn transaction_time_ns(&self) -> Option<u64> {
        self.0.transaction_time_ns()
    }
    pub fn received_mono_ns(&self) -> u64 {
        self.0.received_mono_ns()
    }
}

impl PrivateEvent<'_> {
    /// Bind this authenticated session event to a durable local creating command.
    pub fn bind_order(
        &self,
        command: &tickwire_venue::CanonicalCommand,
        identity: &tickwire_venue::OrderIdentity,
        assets: &[tickwire_venue::AssetRef],
    ) -> tickwire_binance::Result<tickwire_venue::CanonicalEvent> {
        tickwire_binance::events::bind_order(&self.0, command, identity, assets)
    }
}
