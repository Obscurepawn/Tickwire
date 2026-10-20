//! Actual authenticated balance/open-order reads, including empty-account bootstrap.
use super::*;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SnapshotKind {
    Account,
    OpenOrders,
    ConditionalOrders,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SnapshotQuery {
    pub kind: SnapshotKind,
    pub private_connection_epoch: u64,
    pub gap_generation: u64,
}
impl SnapshotQuery {
    pub(crate) fn validate(self) -> Result<()> {
        if self.private_connection_epoch == 0 || self.gap_generation == 0 {
            Err(Error::Identity)
        } else {
            Ok(())
        }
    }
}
pub trait SnapshotPermit {
    fn allow(
        &mut self,
        scope: &AccountScope,
        query: SnapshotQuery,
        request_epoch: u64,
        operation: ReadOperation,
        now: &ClockSample,
    ) -> bool;
}
impl<F: FnMut(&AccountScope, SnapshotQuery, u64, ReadOperation, &ClockSample) -> bool>
    SnapshotPermit for F
{
    fn allow(
        &mut self,
        s: &AccountScope,
        q: SnapshotQuery,
        e: u64,
        o: ReadOperation,
        n: &ClockSample,
    ) -> bool {
        self(s, q, e, o, n)
    }
}
#[derive(Clone)]
pub(crate) struct SnapshotRecord {
    pub query: SnapshotQuery,
    pub epoch: u64,
    pub received: u64,
    pub clock_boot_id: [u8; 16],
    pub clock_generation: u64,
    pub ledger: u64,
    pub status: u16,
    pub complete: bool,
    pub raw: Vec<u8>,
}
/// Constructed only after the exact authenticated HTTP response is synchronized.
/// This is one remote observation; it does not independently attest account closure.
pub struct SnapshotEvidence {
    input: InputKey,
    query: SnapshotQuery,
    epoch: u64,
    received: u64,
    domain: tickwire_clock::ClockDomain,
    ledger: u64,
    baseline: v::Hash,
    account_stream: [u8; 16],
    raw_hash: v::Hash,
    body: Vec<u8>,
}
impl SnapshotEvidence {
    pub fn domain(&self) -> tickwire_clock::ClockDomain {
        self.domain
    }
    pub fn input(&self) -> InputKey {
        self.input
    }
    pub fn query(&self) -> SnapshotQuery {
        self.query
    }
    pub fn connection_epoch(&self) -> u64 {
        self.epoch
    }
    pub fn received_mono_ns(&self) -> u64 {
        self.received
    }
    pub fn ledger_sequence(&self) -> u64 {
        self.ledger
    }
    pub fn baseline_digest(&self) -> v::Hash {
        self.baseline
    }
    pub fn account_stream(&self) -> [u8; 16] {
        self.account_stream
    }
    pub fn raw_hash(&self) -> v::Hash {
        self.raw_hash
    }
    pub fn body(&self) -> &[u8] {
        &self.body
    }
}
pub struct SnapshotOutcome {
    pub input: InputKey,
    pub through_sequence: u64,
    pub evidence: Option<SnapshotEvidence>,
}
struct SnapshotPending {
    query: SnapshotQuery,
    epoch: u64,
    wire: wire::WireRead,
}
pub struct AccountSnapshotReader {
    config: ReconcileConfig,
    next_epoch: u64,
    last: Option<ClockSample>,
    pending: Option<SnapshotPending>,
    faulted: bool,
}
impl AccountSnapshotReader {
    pub fn new(config: ReconcileConfig) -> Result<Self> {
        let validated = ReadOnlyReconciler::new(config.clone())?;
        Ok(Self {
            next_epoch: validated.next_epoch,
            config,
            last: None,
            pending: None,
            faulted: false,
        })
    }
    pub fn cancel(&mut self) {
        self.pending = None;
    }
    pub fn request_epoch(&self) -> Option<u64> {
        self.pending.as_ref().map(|p| p.epoch)
    }
    pub fn busy(&self) -> bool {
        self.pending.is_some()
    }
    pub fn start<S: Store, B: Backend, A: Authority, E: Encoder>(
        &mut self,
        owner: &mut RestOwner<S, B, A, E>,
        query: SnapshotQuery,
        credentials: &Credentials<'_>,
        clock: &mut impl Clock,
        permit: &mut impl SnapshotPermit,
    ) -> Result<()> {
        if self.faulted {
            return Err(Error::Stopped);
        }
        if self.pending.is_some() {
            return Err(Error::Busy);
        }
        owner.gateway.freeze_account();
        owner.ingress.ready(&owner.gateway)?;
        query.validate()?;
        let scope = &owner.ingress.config.baseline.snapshot().scope;
        if self.config.scope.risk_domain_id != scope.risk_domain_id
            || self.config.scope.venue != scope.venue
            || self.config.scope.environment != scope.environment
            || self.config.scope.account != scope.account
            || self.config.scope.market != owner.ingress.market()?
            || credentials.account != scope.account
            || credentials.environment != scope.environment
            || credentials.market != self.config.scope.market
            || self.config.max_response_bytes > owner.ingress.config.max_raw_bytes
        {
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
        let bytes = encode(&self.config, query, credentials, stamp)?;
        let epoch = self.next_epoch;
        self.next_epoch = self.next_epoch.checked_add(1).ok_or(Error::Capacity)?;
        if !permit.allow(
            &self.config.scope,
            query,
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
        self.pending = Some(SnapshotPending { query, epoch, wire });
        Ok(())
    }
    pub fn poll<S: Store, B: Backend, A: Authority, E: Encoder>(
        &mut self,
        owner: &mut RestOwner<S, B, A, E>,
        clock: &mut impl Clock,
        permit: &mut impl SnapshotPermit,
        budget: IoBudget,
    ) -> Result<Option<SnapshotOutcome>> {
        let Some(pending) = self.pending.as_mut() else {
            return Ok(None);
        };
        let query = pending.query;
        let scope = &self.config.scope;
        let response = match pending
            .wire
            .poll(&self.config, clock, &mut self.last, budget, |now| {
                permit.allow(scope, query, pending.epoch, ReadOperation::TradePage, now)
            }) {
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
        let pending = self.pending.take().unwrap();
        owner.gateway.freeze_account();
        owner.ingress.ready(&owner.gateway)?;
        let record = SnapshotRecord {
            query,
            epoch: pending.epoch,
            received: response.received,
            clock_boot_id: self
                .last
                .ok_or(Error::Identity)?
                .monotonic
                .domain()
                .boot_id(),
            clock_generation: self
                .last
                .ok_or(Error::Identity)?
                .monotonic
                .domain()
                .generation(),
            ledger: owner.account().ledger_sequence(),
            status: response.status,
            complete: response.status == 200 && response.body.is_some(),
            raw: response.raw,
        };
        let key = owner.ingress.append(Record::Snapshot(record))?;
        owner.ingress.cursor = owner.ingress.records.len();
        let evidence = owner.ingress.snapshot_evidence(key.sequence)?;
        Ok(Some(SnapshotOutcome {
            input: key,
            through_sequence: owner.applied_position(),
            evidence,
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
impl<S: Store> AccountIngress<S> {
    pub fn snapshot_evidence(&self, sequence: u64) -> Result<Option<SnapshotEvidence>> {
        let index = usize::try_from(sequence)
            .ok()
            .and_then(|n| n.checked_sub(1))
            .ok_or(Error::Identity)?;
        let Record::Snapshot(record) = self.records.get(index).ok_or(Error::Identity)? else {
            return Err(Error::Identity);
        };
        if !record.complete {
            return Ok(None);
        }
        let mut decoder =
            ResponseDecoder::new(self.config.max_raw_bytes, 256).map_err(Error::Transport)?;
        decoder.push(&record.raw).map_err(Error::Transport)?;
        decoder.finish();
        let response = decoder
            .next_response()
            .map_err(Error::Transport)?
            .ok_or(Error::Encoding)?;
        if response.status != 200 || !decoder.take_buffered().is_empty() {
            return Err(Error::Encoding);
        }
        Ok(Some(SnapshotEvidence {
            input: InputKey {
                stream: self.input_stream(),
                sequence,
            },
            query: record.query,
            epoch: record.epoch,
            received: record.received,
            domain: tickwire_clock::ClockDomain::new(record.clock_boot_id, record.clock_generation)
                .map_err(|_| Error::Identity)?,
            ledger: record.ledger,
            baseline: self.config.baseline.digest(),
            account_stream: self.config.account_stream,
            raw_hash: ring::digest::digest(&ring::digest::SHA256, &record.raw)
                .as_ref()
                .try_into()
                .unwrap(),
            body: response.body,
        }))
    }
}
fn encode(
    config: &ReconcileConfig,
    query: SnapshotQuery,
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
    params
        .push("timestamp", Value::Integer(stamp.timestamp()))
        .map_err(|_| Error::Encoding)?;
    params
        .push("recvWindow", Value::Number(&window))
        .map_err(|_| Error::Encoding)?;
    let mut preimage = [0u8; 1024];
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
    let endpoint = match (config.scope.market, query.kind) {
        (Market::Spot, SnapshotKind::Account) => "/api/v3/account",
        (Market::Spot, SnapshotKind::OpenOrders) => "/api/v3/openOrders",
        (Market::Usdm, SnapshotKind::Account) => "/fapi/v3/account",
        (Market::Usdm, SnapshotKind::OpenOrders) => "/fapi/v1/openOrders",
        (Market::Spot, SnapshotKind::ConditionalOrders) => "/api/v3/openOrderList",
        (Market::Usdm, SnapshotKind::ConditionalOrders) => "/fapi/v1/openAlgoOrders",
    };
    http::encode_request(
        "GET",
        &format!("{endpoint}?{signed}"),
        &[
            ("Host", authority(&config.scope)),
            ("X-MBX-APIKEY", credentials.api_key),
        ],
        &[],
        config.limits.max_send_bytes,
    )
    .map_err(Error::Transport)
}
