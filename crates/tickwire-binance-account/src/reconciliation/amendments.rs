//! Bounded read-only amendment pages for an already-persisted ordinary Spot order.
use super::*;
use crate::rest::{RestDisposition, RestOutcome};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AmendmentQuery {
    pub intent: IntentKey,
    pub order_id: [u8; 16],
    pub from_execution_id: u64,
    pub limit: u16,
    pub private_connection_epoch: u64,
    pub gap_generation: u64,
}
impl AmendmentQuery {
    pub(crate) fn validate(self) -> Result<()> {
        if self.intent.epoch == 0
            || self.intent.intent_id == 0
            || self.order_id == [0; 16]
            || self.from_execution_id > i64::MAX as u64
            || self.limit == 0
            || self.limit > 16
            || self.private_connection_epoch == 0
            || self.gap_generation == 0
        {
            return Err(Error::Identity);
        }
        Ok(())
    }
}
pub trait AmendmentPermit {
    fn allow(
        &mut self,
        scope: &AccountScope,
        query: &AmendmentQuery,
        request_epoch: u64,
        operation: ReadOperation,
        now: &ClockSample,
    ) -> bool;
}
impl<F: FnMut(&AccountScope, &AmendmentQuery, u64, ReadOperation, &ClockSample) -> bool>
    AmendmentPermit for F
{
    fn allow(
        &mut self,
        s: &AccountScope,
        q: &AmendmentQuery,
        e: u64,
        o: ReadOperation,
        n: &ClockSample,
    ) -> bool {
        self(s, q, e, o, n)
    }
}
#[derive(Clone, Debug)]
pub struct AmendmentPage {
    pub query: AmendmentQuery,
    pub connection_epoch: u64,
    pub input: InputKey,
    pub through_sequence: u64,
    pub accepted: bool,
    pub returned: usize,
    pub next_execution_id: u64,
    pub page_exhausted: bool,
}
struct PendingAmendment {
    query: AmendmentQuery,
    epoch: u64,
    wire: wire::WireRead,
}
pub struct ReadOnlyAmendmentReader {
    config: ReconcileConfig,
    next_epoch: u64,
    pending: Option<PendingAmendment>,
    last: Option<ClockSample>,
    faulted: bool,
}
impl ReadOnlyAmendmentReader {
    pub fn new(config: ReconcileConfig) -> Result<Self> {
        let validated = ReadOnlyReconciler::new(config.clone())?;
        Ok(Self {
            next_epoch: validated.next_epoch,
            config,
            pending: None,
            last: None,
            faulted: false,
        })
    }
    pub fn cancel(&mut self) {
        self.pending = None;
    }
    pub fn busy(&self) -> bool {
        self.pending.is_some()
    }
    pub fn request_epoch(&self) -> Option<u64> {
        self.pending.as_ref().map(|p| p.epoch)
    }
    pub fn start<S: Store, B: Backend, A: Authority, E: Encoder>(
        &mut self,
        owner: &mut RestOwner<S, B, A, E>,
        query: AmendmentQuery,
        credentials: &Credentials<'_>,
        clock: &mut impl Clock,
        permit: &mut impl AmendmentPermit,
    ) -> Result<()> {
        owner.gateway.freeze_account();
        if self.faulted {
            return Err(Error::Stopped);
        }
        if self.pending.is_some() {
            return Err(Error::Busy);
        }
        owner.ingress.ready(&owner.gateway)?;
        query.validate()?;
        let scope = &owner.ingress.config.baseline.snapshot().scope;
        if self.config.scope.market != Market::Spot
            || self.config.scope.risk_domain_id != scope.risk_domain_id
            || self.config.scope.venue != scope.venue
            || self.config.scope.account != scope.account
            || self.config.scope.environment != scope.environment
            || self.config.scope.market != owner.ingress.market()?
            || credentials.market != self.config.scope.market
            || credentials.account != scope.account
            || credentials.environment != scope.environment
        {
            return Err(Error::Identity);
        }
        if self.config.max_response_bytes > owner.ingress.config.max_raw_bytes {
            return Err(Error::Capacity);
        }
        let reg = owner
            .ingress
            .registry
            .get(&query.intent)
            .ok_or(Error::Identity)?;
        let identity = reg
            .orders
            .iter()
            .find(|id| id.order_id == query.order_id)
            .ok_or(Error::Identity)?;
        if owner.gateway.command(query.intent) != Some(&reg.command) {
            return Err(Error::Identity);
        }
        let started = clock.sample().map_err(Error::Clock)?;
        wire::ordered(&mut self.last, started)?;
        let stamp = started
            .signing_time
            .ok_or(Error::Clock(tickwire_binance_private::Error::Clock))?;
        source(&self.config.scope, &stamp, &started)?;
        clock
            .check_signed(
                &self.config.scope,
                &stamp,
                &started,
                self.config.recv_window_micros,
            )
            .map_err(Error::Clock)?;
        let bytes = encode(
            &self.config,
            &reg.command,
            identity,
            query,
            credentials,
            stamp,
        )?;
        let epoch = self.next_epoch;
        self.next_epoch = self.next_epoch.checked_add(1).ok_or(Error::Capacity)?;
        if !permit.allow(
            &self.config.scope,
            &query,
            epoch,
            ReadOperation::Connect,
            &started,
        ) {
            return Err(Error::Stopped);
        }
        let now = clock.sample().map_err(Error::Clock)?;
        wire::ordered(&mut self.last, now)?;
        if now.monotonic_ns()
            >= started
                .monotonic_ns()
                .checked_add(self.config.request_timeout_ns)
                .ok_or(Error::Capacity)?
            || now.instant
                >= started
                    .instant
                    .checked_add(Duration::from_nanos(self.config.request_timeout_ns))
                    .ok_or(Error::Capacity)?
        {
            return Err(Error::Stopped);
        }
        let wire = wire::WireRead::new(&self.config, stamp, started, &bytes)?;
        self.pending = Some(PendingAmendment { query, epoch, wire });
        Ok(())
    }
    pub fn poll<S: Store, B: Backend, A: Authority, E: Encoder>(
        &mut self,
        owner: &mut RestOwner<S, B, A, E>,
        clock: &mut impl Clock,
        permit: &mut impl AmendmentPermit,
        budget: IoBudget,
    ) -> Result<Option<AmendmentPage>> {
        let Some(p) = self.pending.as_mut() else {
            return Ok(None);
        };
        let query = p.query;
        let epoch = p.epoch;
        let scope = &self.config.scope;
        let result = p
            .wire
            .poll(&self.config, clock, &mut self.last, budget, |now| {
                permit.allow(scope, &query, epoch, ReadOperation::TradePage, now)
            });
        let response = match result {
            Ok(Some(r)) => r,
            Ok(None) => return Ok(None),
            Err(error) => {
                if matches!(
                    error,
                    Error::Clock(tickwire_binance_private::Error::ClockDomain)
                ) {
                    self.faulted = true;
                }
                return Err(error);
            }
        };
        self.pending = None;
        let RestOutcome {
            input,
            disposition,
            applied_events,
            ..
        } = owner.ingress.persist_amendment_query(
            &mut owner.gateway,
            query,
            epoch,
            response.received,
            response.status,
            response.raw,
            response.body.as_deref(),
        )?;
        let next_execution_id = owner
            .ingress
            .stored_rest()
            .last()
            .and_then(|r| r.events.last())
            .and_then(|e| match &e.event().body {
                v::VenueEventBody::Amendment(a) => a.execution_id.checked_add(1),
                _ => None,
            })
            .unwrap_or(query.from_execution_id);
        Ok(Some(AmendmentPage {
            query,
            connection_epoch: epoch,
            input: input.ok_or(Error::Identity)?,
            through_sequence: owner.applied_position(),
            accepted: disposition == RestDisposition::Observed,
            returned: applied_events,
            next_execution_id,
            page_exhausted: applied_events < query.limit as usize,
        }))
    }
    pub fn register(
        &mut self,
        registry: &tickwire_transport::mio::Registry,
        token: tickwire_transport::mio::Token,
    ) -> Result<()> {
        self.pending
            .as_mut()
            .ok_or(Error::Busy)?
            .wire
            .connection
            .register(registry, token)
            .map_err(Error::Transport)
    }
    pub fn reregister(
        &mut self,
        registry: &tickwire_transport::mio::Registry,
        token: tickwire_transport::mio::Token,
    ) -> Result<()> {
        self.pending
            .as_mut()
            .ok_or(Error::Busy)?
            .wire
            .connection
            .reregister(registry, token)
            .map_err(Error::Transport)
    }
}
fn identity_parameter(identity: &v::OrderIdentity) -> Result<(&'static str, Value<'_>)> {
    if let Some(native) = &identity.venue_order_id {
        let order = native
            .as_str()
            .parse::<u64>()
            .map_err(|_| Error::Identity)?;
        if order > i64::MAX as u64 {
            return Err(Error::Identity);
        }
        Ok(("orderId", Value::Integer(order)))
    } else {
        Err(Error::Identity)
    }
}
fn encode(
    config: &ReconcileConfig,
    command: &CanonicalCommand,
    identity: &v::OrderIdentity,
    query: AmendmentQuery,
    credentials: &Credentials<'_>,
    stamp: SignedTime,
) -> Result<Vec<u8>> {
    if credentials.api_key.is_empty()
        || credentials.api_key.len() > 256
        || !credentials
            .api_key
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_".contains(&b))
    {
        return Err(Error::Identity);
    }
    if config.scope.market == Market::Usdm && stamp.unit() != tickwire_clock::TimeUnit::Milliseconds
    {
        return Err(Error::Identity);
    }
    let window = if config.recv_window_micros.is_multiple_of(1000) {
        (config.recv_window_micros / 1000).to_string()
    } else {
        format!(
            "{}.{:03}",
            config.recv_window_micros / 1000,
            config.recv_window_micros % 1000
        )
        .trim_end_matches('0')
        .to_owned()
    };
    let mut params = Params::<6>::new();
    params
        .push(
            "symbol",
            Value::String(command.command().context.instrument.symbol.as_str()),
        )
        .map_err(|_| Error::Encoding)?;
    let (key, value) = identity_parameter(identity)?;
    params.push(key, value).map_err(|_| Error::Encoding)?;
    params
        .push("fromExecutionId", Value::Integer(query.from_execution_id))
        .map_err(|_| Error::Encoding)?;
    params
        .push("limit", Value::Integer(query.limit as u64))
        .map_err(|_| Error::Encoding)?;
    params
        .push("timestamp", Value::Integer(stamp.timestamp()))
        .map_err(|_| Error::Encoding)?;
    params
        .push("recvWindow", Value::Number(&window))
        .map_err(|_| Error::Encoding)?;
    let mut preimage = [0u8; 8192];
    let n = params.rest(&mut preimage).map_err(|_| Error::Encoding)?;
    let endpoint = if config.scope.market == Market::Spot {
        Endpoint::SpotRest
    } else {
        Endpoint::UsdmRest
    };
    let mut signature = [0u8; MAX_SIGNATURE];
    let size = credentials
        .signing_key
        .sign(endpoint, &preimage[..n], &mut signature)
        .map_err(|_| Error::Encoding)?;
    let mut signed = String::from_utf8(preimage[..n].to_vec()).map_err(|_| Error::Encoding)?;
    signed.push_str("&signature=");
    for b in &signature[..size] {
        if b.is_ascii_alphanumeric() || b"-._~".contains(b) {
            signed.push(*b as char);
        } else {
            signed.push_str(&format!("%{b:02X}"));
        }
    }
    let path = if config.scope.market == Market::Spot {
        "/api/v3/order/amendments"
    } else {
        "/fapi/v1/order"
    };
    http::encode_request(
        "GET",
        &format!("{path}?{signed}"),
        &[
            ("Host", authority(&config.scope)),
            ("X-MBX-APIKEY", credentials.api_key),
        ],
        &[],
        config.limits.max_send_bytes,
    )
    .map_err(Error::Transport)
}

impl<S: Store, B: Backend, A: Authority, E: Encoder> RestOwner<S, B, A, E> {
    pub fn reconciliation_amendments(
        &self,
        private_connection_epoch: u64,
        gap_generation: u64,
    ) -> Vec<AmendmentQuery> {
        let mut result = Vec::new();
        for mutation in self.account().amendments() {
            let Some(reg) = self.ingress.registry.get(&mutation.order.intent) else {
                continue;
            };
            let Some(id) = reg
                .orders
                .iter()
                .find(|id| id.client_id.as_ref() == Some(&mutation.order.client_id))
            else {
                continue;
            };
            if !result.iter().any(|q: &AmendmentQuery| {
                q.intent == mutation.order.intent && q.order_id == id.order_id
            }) {
                result.push(AmendmentQuery {
                    intent: mutation.order.intent,
                    order_id: id.order_id,
                    from_execution_id: 0,
                    limit: 16,
                    private_connection_epoch,
                    gap_generation,
                });
            }
        }
        result
    }
}
