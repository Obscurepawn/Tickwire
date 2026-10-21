//! Exact parent queries retain the original conditional command and never synthesize a child result.
use super::*;
use crate::rest::{RestDisposition, RestOutcome};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StructureQuery {
    pub intent: IntentKey,
    pub structure_id: [u8; 16],
    pub private_connection_epoch: u64,
    pub gap_generation: u64,
}
impl StructureQuery {
    pub(crate) fn validate(self) -> Result<()> {
        if self.intent.epoch == 0
            || self.intent.intent_id == 0
            || self.structure_id == [0; 16]
            || self.private_connection_epoch == 0
            || self.gap_generation == 0
        {
            return Err(Error::Identity);
        }
        Ok(())
    }
}
pub trait StructurePermit {
    fn allow(
        &mut self,
        scope: &AccountScope,
        query: &StructureQuery,
        request_epoch: u64,
        operation: ReadOperation,
        now: &ClockSample,
    ) -> bool;
}
impl<F: FnMut(&AccountScope, &StructureQuery, u64, ReadOperation, &ClockSample) -> bool>
    StructurePermit for F
{
    fn allow(
        &mut self,
        s: &AccountScope,
        q: &StructureQuery,
        e: u64,
        o: ReadOperation,
        n: &ClockSample,
    ) -> bool {
        self(s, q, e, o, n)
    }
}
#[derive(Clone, Debug)]
pub struct StructureQueryOutcome {
    pub query: StructureQuery,
    pub connection_epoch: u64,
    pub input: InputKey,
    pub through_sequence: u64,
    pub accepted: bool,
}
struct PendingStructure {
    query: StructureQuery,
    epoch: u64,
    wire: wire::WireRead,
}
pub struct ReadOnlyStructureReader {
    config: ReconcileConfig,
    next_epoch: u64,
    pending: Option<PendingStructure>,
    last: Option<ClockSample>,
    faulted: bool,
}
impl ReadOnlyStructureReader {
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
        query: StructureQuery,
        credentials: &Credentials<'_>,
        clock: &mut impl Clock,
        permit: &mut impl StructurePermit,
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
        if self.config.scope.risk_domain_id != scope.risk_domain_id
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
        let identity = reg.orders.first().ok_or(Error::Identity)?;
        let parent = match &reg.command.command().body {
            v::CommandBody::SpotList(list) => list.list_id,
            v::CommandBody::Algo(algo) => algo.parent_order_id,
            _ => return Err(Error::Identity),
        };
        if parent != query.structure_id {
            return Err(Error::Identity);
        }
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
        let bytes = encode(&self.config, &reg.command, identity, credentials, stamp)?;
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
        self.pending = Some(PendingStructure { query, epoch, wire });
        Ok(())
    }
    pub fn poll<S: Store, B: Backend, A: Authority, E: Encoder>(
        &mut self,
        owner: &mut RestOwner<S, B, A, E>,
        clock: &mut impl Clock,
        permit: &mut impl StructurePermit,
        budget: IoBudget,
    ) -> Result<Option<StructureQueryOutcome>> {
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
        } = owner.ingress.persist_structure_query(
            &mut owner.gateway,
            query,
            epoch,
            self.last.ok_or(Error::Identity)?.monotonic.domain(),
            response.received,
            response.status,
            response.raw,
            response.body.as_deref(),
        )?;
        Ok(Some(StructureQueryOutcome {
            query,
            connection_epoch: epoch,
            input: input.ok_or(Error::Identity)?,
            through_sequence: owner.applied_position(),
            accepted: matches!(
                disposition,
                RestDisposition::Observed | RestDisposition::NeedsReconciliation
            ) && applied_events == 1,
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
fn identity_parameter<'a>(
    command: &'a CanonicalCommand,
    identity: &'a v::OrderIdentity,
) -> Result<(&'static str, Value<'a>)> {
    let (native, client, native_key, client_key) = match &command.command().body {
        v::CommandBody::SpotList(list) if identity.list_id == Some(list.list_id) => (
            &identity.venue_list_id,
            &list.client_id,
            "orderListId",
            "origClientOrderId",
        ),
        v::CommandBody::Algo(algo) if identity.parent_order_id == Some(algo.parent_order_id) => (
            &identity.venue_algo_id,
            &algo.order.client_id,
            "algoId",
            "clientAlgoId",
        ),
        _ => return Err(Error::Identity),
    };
    if let Some(native) = native {
        let id = native
            .as_str()
            .parse::<u64>()
            .map_err(|_| Error::Identity)?;
        if id > i64::MAX as u64 {
            return Err(Error::Identity);
        }
        Ok((native_key, Value::Integer(id)))
    } else {
        Ok((client_key, Value::String(client.as_str())))
    }
}
fn encode(
    config: &ReconcileConfig,
    command: &CanonicalCommand,
    identity: &v::OrderIdentity,
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
    let mut params = Params::<3>::new();
    let (key, value) = identity_parameter(command, identity)?;
    params.push(key, value).map_err(|_| Error::Encoding)?;
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
        "/api/v3/orderList"
    } else {
        "/fapi/v1/algoOrder"
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
    pub fn reconciliation_structures(
        &self,
        private_connection_epoch: u64,
        gap_generation: u64,
    ) -> Vec<StructureQuery> {
        self.account()
            .structures()
            .filter(|parent| {
                !parent.legs.iter().all(|key| {
                    self.account()
                        .order(key)
                        .is_some_and(|o| o.aborted || o.terminal && o.fee_reconciled && !o.unknown)
                })
            })
            .map(|parent| StructureQuery {
                intent: parent.intent,
                structure_id: parent.structure_id,
                private_connection_epoch,
                gap_generation,
            })
            .collect()
    }
}
