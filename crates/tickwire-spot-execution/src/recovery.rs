//! Authenticated, replayable recovery evidence for the exclusive cash spot account profile.
mod collect;
mod rows;
use crate::{
    Credentials, SpotVenue, authority,
    network::{self, Result},
};
use serde_json::Value;
use std::{
    cell::Cell,
    sync::Arc,
    time::{Duration, Instant},
};
use tickwire_transport::{http::Response, rustls};
use tickwire_venue::{Decimal, Environment, OrderStatus, OrderType, Side, TimeInForce};
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteBalance {
    pub symbol: String,
    pub total: Decimal,
    pub available: Decimal,
    pub locked: Decimal,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteOrder {
    pub symbol: String,
    pub native_id: String,
    pub client_id: String,
    pub side: Side,
    pub status: OrderStatus,
    pub order_type: OrderType,
    pub time_in_force: Option<TimeInForce>,
    pub qty: Decimal,
    pub cumulative: Decimal,
    pub price: Option<Decimal>,
    pub created_ms: u64,
    pub updated_ms: u64,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteFill {
    pub symbol: String,
    pub client_id: String,
    pub native_id: String,
    pub trade_id: String,
    pub side: Side,
    pub qty: Decimal,
    pub price: Decimal,
    pub fee: Decimal,
    pub fee_currency: String,
    pub time_ms: u64,
    pub wire_row: Vec<u8>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderLookup {
    pub symbol: String,
    pub client_id: String,
}
#[derive(Debug)]
pub struct RestEvidence {
    pub venue: SpotVenue,
    pub environment: Environment,
    pub uid: String,
    pub start_ms: u64,
    pub end_ms: u64,
    pub balances: Vec<RemoteBalance>,
    pub open_orders: Vec<RemoteOrder>,
    pub history: Vec<RemoteOrder>,
    pub target_orders: Vec<RemoteOrder>,
    pub fills: Vec<RemoteFill>,
    pub fingerprint: [u8; 32],
    pub evidence_bytes: Vec<u8>,
}
pub struct RestReader<'a, 'k> {
    credentials: &'a Credentials<'k>,
    environment: Environment,
    tls: Arc<rustls::ClientConfig>,
    clock: Cell<(u64, Instant)>,
}
impl<'a, 'k> RestReader<'a, 'k> {
    pub fn new(
        credentials: &'a Credentials<'k>,
        environment: Environment,
        tls: Arc<rustls::ClientConfig>,
    ) -> Result<Self> {
        if !credentials.valid() {
            return Err("invalid private REST credentials".into());
        }
        let clock = Cell::new(network::venue_time(
            credentials.venue,
            authority(credentials.venue, environment),
            tls.clone(),
        )?);
        Ok(Self {
            credentials,
            environment,
            tls,
            clock,
        })
    }
    pub fn now_ms(&self) -> Result<u64> {
        if self.clock.get().1.elapsed() > Duration::from_secs(30) {
            self.clock.set(network::venue_time(
                self.credentials.venue,
                authority(self.credentials.venue, self.environment),
                self.tls.clone(),
            )?);
        }
        let (ms, at) = self.clock.get();
        ms.checked_add(at.elapsed().as_millis().try_into()?)
            .ok_or_else(|| "venue clock overflow".into())
    }
    fn read(&self, path: &str) -> Result<Response> {
        let wire = crate::sign(
            self.credentials,
            self.environment,
            self.now_ms()?,
            &crate::Request {
                method: "GET",
                path: path.to_owned(),
                body: vec![],
            },
        )
        .map_err(|_| "private read signing failed")?;
        network::get(
            authority(self.credentials.venue, self.environment),
            self.tls.clone(),
            &wire,
        )
    }
    pub fn verify_uid(&self, expected: &str) -> Result<()> {
        let q = crate::observe::Query::Identity;
        let page = q.evidence(
            self.credentials.venue,
            self.read(&q.path(self.credentials.venue)?)?,
        )?;
        if page["success"] != true || page["rows"][0]["uid"] != expected {
            return Err("authenticated account UID mismatch".into());
        }
        Ok(())
    }
    pub fn acquire(&self, expected_uid: &str, start_ms: u64, end_ms: u64) -> Result<RestEvidence> {
        self.acquire_for(expected_uid, start_ms, end_ms, &[])
    }
    pub fn acquire_for(
        &self,
        expected_uid: &str,
        start_ms: u64,
        end_ms: u64,
        targets: &[OrderLookup],
    ) -> Result<RestEvidence> {
        self.acquire_while(expected_uid, start_ms, end_ms, targets, || true)
    }

    /// Stop before the next signed query when the owner cancels or the scan expires.
    pub fn acquire_while(
        &self,
        expected_uid: &str,
        start_ms: u64,
        end_ms: u64,
        targets: &[OrderLookup],
        running: impl Fn() -> bool,
    ) -> Result<RestEvidence> {
        collect::acquire_for(
            self.credentials.venue,
            self.environment,
            expected_uid,
            (start_ms, end_ms),
            self.now_ms()?,
            targets,
            |path| {
                if !running() {
                    return Err("account scan expired or stopped".into());
                }
                self.read(path)
            },
        )
    }
}
pub use collect::{acquire, acquire_for, replay_evidence};

pub fn decode_order_row(venue: SpotVenue, row: &Value) -> Result<RemoteOrder> {
    rows::order(venue, row)
}
