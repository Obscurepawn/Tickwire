//! Authenticated bounded income ranges. A short page is query evidence, not account activation.
use super::*;

pub const MAX_INCOME_ROWS: usize = 16;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IncomeUse {
    BaselineCoverage,
    Account,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IncomeQuery {
    pub start_time_ms: u64,
    pub end_time_ms: u64,
    pub limit: u16,
    pub private_connection_epoch: u64,
    pub gap_generation: u64,
    pub purpose: IncomeUse,
}
impl IncomeQuery {
    pub fn validate(self) -> Result<()> {
        if self.start_time_ms > self.end_time_ms
            || self.end_time_ms > i64::MAX as u64
            || self.limit == 0
            || usize::from(self.limit) > MAX_INCOME_ROWS
            || self.private_connection_epoch == 0
            || self.gap_generation == 0
        {
            return Err(Error::Identity);
        }
        Ok(())
    }
    /// Split inclusive intervals without overlap. A saturated millisecond is incomplete.
    pub fn split(self) -> Result<(Self, Self)> {
        self.validate()?;
        if self.start_time_ms == self.end_time_ms {
            return Err(Error::Capacity);
        }
        let middle = self.start_time_ms + (self.end_time_ms - self.start_time_ms) / 2;
        Ok((
            Self {
                end_time_ms: middle,
                ..self
            },
            Self {
                start_time_ms: middle + 1,
                ..self
            },
        ))
    }
}
#[derive(Clone)]
pub(crate) struct IncomeRecord {
    pub query: IncomeQuery,
    pub epoch: u64,
    pub received: u64,
    pub clock_boot_id: [u8; 16],
    pub clock_generation: u64,
    pub status: u16,
    pub accepted: bool,
    pub raw: Vec<u8>,
    pub rows: Vec<v::CanonicalAccountInput>,
}
/// A synchronized authenticated query result. Construction is private; incomplete
/// or rejected responses never yield evidence. Coverage needs all interval leaves.
pub struct IncomeEvidence {
    input: InputKey,
    query: IncomeQuery,
    raw_hash: v::Hash,
    rows: Vec<v::CanonicalAccountInput>,
    received: u64,
    domain: tickwire_clock::ClockDomain,
    baseline: v::Hash,
}
impl IncomeEvidence {
    pub fn input(&self) -> InputKey {
        self.input
    }
    pub fn query(&self) -> IncomeQuery {
        self.query
    }
    pub fn raw_hash(&self) -> v::Hash {
        self.raw_hash
    }
    pub fn rows(&self) -> &[v::CanonicalAccountInput] {
        &self.rows
    }
    pub fn saturated(&self) -> bool {
        self.rows.len() == self.query.limit as usize
    }
    pub fn received_mono_ns(&self) -> u64 {
        self.received
    }
    pub fn domain(&self) -> tickwire_clock::ClockDomain {
        self.domain
    }
    pub fn baseline_digest(&self) -> v::Hash {
        self.baseline
    }
}
pub struct IncomeOutcome {
    pub input: InputKey,
    pub through_sequence: u64,
    pub evidence: Option<IncomeEvidence>,
}
/// Bounded depth-first interval scan. The completion result only covers the
/// requested retained range; it grants no baseline or activation authority.
pub struct IncomeScan {
    pending: Vec<IncomeQuery>,
    leaves: Vec<u64>,
    maximum_queries: usize,
    queries: usize,
    stream: Option<[u8; 16]>,
    baseline: Option<v::Hash>,
    domain: Option<tickwire_clock::ClockDomain>,
    last_sequence: u64,
    last_received: u64,
    failed: bool,
}
impl IncomeScan {
    pub fn new(query: IncomeQuery, maximum_queries: usize) -> Result<Self> {
        query.validate()?;
        if maximum_queries == 0 || maximum_queries > 128 {
            return Err(Error::Capacity);
        }
        Ok(Self {
            pending: vec![query],
            leaves: Vec::new(),
            maximum_queries,
            queries: 0,
            stream: None,
            baseline: None,
            domain: None,
            last_sequence: 0,
            last_received: 0,
            failed: false,
        })
    }
    pub fn next_query(&self) -> Result<Option<IncomeQuery>> {
        if self.failed {
            return Err(Error::Stopped);
        }
        Ok(self.pending.last().copied())
    }
    pub fn complete(&self) -> bool {
        !self.failed && self.pending.is_empty()
    }
    pub fn leaf_sequences(&self) -> &[u64] {
        &self.leaves
    }
    pub fn accept(&mut self, e: &IncomeEvidence) -> Result<()> {
        let result = (|| {
            if self.failed
                || self.pending.last().copied() != Some(e.query)
                || e.input.sequence <= self.last_sequence
                || e.received < self.last_received
                || self.stream.is_some_and(|v| v != e.input.stream)
                || self.baseline.is_some_and(|v| v != e.baseline)
                || self.domain.is_some_and(|v| v != e.domain)
            {
                return Err(Error::Identity);
            }
            let split = if e.saturated() {
                Some(e.query.split()?)
            } else {
                None
            };
            if self.queries >= self.maximum_queries
                || split.is_some()
                    && self.queries + 1 + self.pending.len() + 1 > self.maximum_queries
            {
                return Err(Error::Capacity);
            }
            self.stream = Some(e.input.stream);
            self.baseline = Some(e.baseline);
            self.domain = Some(e.domain);
            self.last_sequence = e.input.sequence;
            self.last_received = e.received;
            self.queries += 1;
            self.pending.pop();
            if let Some((left, right)) = split {
                self.pending.push(right);
                self.pending.push(left);
            } else {
                self.leaves.push(e.input.sequence);
            }
            Ok(())
        })();
        if result.is_err() {
            self.failed = true;
        }
        result
    }
}
pub trait IncomePermit {
    fn allow(
        &mut self,
        scope: &AccountScope,
        query: &IncomeQuery,
        epoch: u64,
        operation: ReadOperation,
        now: &ClockSample,
    ) -> bool;
}
impl<F: FnMut(&AccountScope, &IncomeQuery, u64, ReadOperation, &ClockSample) -> bool> IncomePermit
    for F
{
    fn allow(
        &mut self,
        s: &AccountScope,
        q: &IncomeQuery,
        e: u64,
        o: ReadOperation,
        n: &ClockSample,
    ) -> bool {
        self(s, q, e, o, n)
    }
}
struct PendingIncome {
    query: IncomeQuery,
    epoch: u64,
    wire: wire::WireRead,
}
pub struct IncomeReader {
    config: ReconcileConfig,
    next_epoch: u64,
    pending: Option<PendingIncome>,
    last: Option<ClockSample>,
    faulted: bool,
}
impl IncomeReader {
    pub fn new(config: ReconcileConfig) -> Result<Self> {
        if config.scope.market != Market::Usdm {
            return Err(Error::Unsupported);
        }
        let validated = ReadOnlyReconciler::new(config.clone())?;
        Ok(Self {
            next_epoch: validated.next_epoch,
            config,
            pending: None,
            last: None,
            faulted: false,
        })
    }
    pub fn busy(&self) -> bool {
        self.pending.is_some()
    }
    pub fn cancel(&mut self) {
        self.pending = None;
    }
    pub fn request_epoch(&self) -> Option<u64> {
        self.pending.as_ref().map(|p| p.epoch)
    }
    pub fn start<S: Store, B: Backend, A: Authority, E: Encoder>(
        &mut self,
        owner: &mut RestOwner<S, B, A, E>,
        query: IncomeQuery,
        credentials: &Credentials<'_>,
        clock: &mut impl Clock,
        permit: &mut impl IncomePermit,
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
            || credentials.market != Market::Usdm
            || credentials.account != scope.account
            || credentials.environment != scope.environment
        {
            return Err(Error::Identity);
        }
        if self.config.max_response_bytes > owner.ingress.config.max_raw_bytes {
            return Err(Error::Capacity);
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
        self.pending = Some(PendingIncome { query, epoch, wire });
        Ok(())
    }
    pub fn poll<S: Store, B: Backend, A: Authority, E: Encoder>(
        &mut self,
        owner: &mut RestOwner<S, B, A, E>,
        clock: &mut impl Clock,
        permit: &mut impl IncomePermit,
        budget: IoBudget,
    ) -> Result<Option<IncomeOutcome>> {
        let Some(p) = self.pending.as_mut() else {
            return Ok(None);
        };
        let query = p.query;
        let epoch = p.epoch;
        let scope = &self.config.scope;
        let response = match p
            .wire
            .poll(&self.config, clock, &mut self.last, budget, |now| {
                permit.allow(scope, &query, epoch, ReadOperation::TradePage, now)
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
        self.pending = None;
        owner.gateway.freeze_account();
        owner.ingress.ready(&owner.gateway)?;
        let domain = self.last.ok_or(Error::Identity)?.monotonic.domain();
        let mut record = IncomeRecord {
            query,
            epoch,
            received: response.received,
            clock_boot_id: domain.boot_id(),
            clock_generation: domain.generation(),
            status: response.status,
            accepted: false,
            raw: response.raw,
            rows: Vec::new(),
        };
        if response.status == 200
            && let Some(body) = response.body
            && let Ok(rows) = bind(&owner.ingress.config.baseline, query, &body)
        {
            let mut candidate = owner.gateway.account().clone();
            if query.purpose == IncomeUse::BaselineCoverage
                || rows
                    .iter()
                    .all(|r| candidate.apply_account_input(r).is_ok())
            {
                record.accepted = true;
                record.rows = rows;
            }
        }
        let input = owner.ingress.commit_income(&mut owner.gateway, record)?;
        Ok(Some(IncomeOutcome {
            input,
            through_sequence: owner.applied_position(),
            evidence: owner.ingress.income_evidence(input.sequence)?,
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
fn encode(
    config: &ReconcileConfig,
    query: IncomeQuery,
    credentials: &Credentials<'_>,
    stamp: SignedTime,
) -> Result<Vec<u8>> {
    query.validate()?;
    if credentials.api_key.is_empty()
        || credentials.api_key.len() > 256
        || !credentials
            .api_key
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_".contains(&b))
        || stamp.unit() != tickwire_clock::TimeUnit::Milliseconds
    {
        return Err(Error::Identity);
    }
    let window = (config.recv_window_micros / 1000).to_string();
    let mut params = Params::<5>::new();
    for (key, value) in [
        ("startTime", query.start_time_ms),
        ("endTime", query.end_time_ms),
        ("limit", u64::from(query.limit)),
        ("timestamp", stamp.timestamp()),
    ] {
        params
            .push(key, Value::Integer(value))
            .map_err(|_| Error::Encoding)?;
    }
    params
        .push("recvWindow", Value::Number(&window))
        .map_err(|_| Error::Encoding)?;
    let mut preimage = [0u8; 8192];
    let n = params.rest(&mut preimage).map_err(|_| Error::Encoding)?;
    let mut signature = [0u8; MAX_SIGNATURE];
    let size = credentials
        .signing_key
        .sign(Endpoint::UsdmRest, &preimage[..n], &mut signature)
        .map_err(|_| Error::Encoding)?;
    let mut signed = String::from_utf8(preimage[..n].to_vec()).map_err(|_| Error::Encoding)?;
    signed.push_str("&signature=");
    for b in &signature[..size] {
        if b.is_ascii_alphanumeric() || b"-._~".contains(b) {
            signed.push(*b as char)
        } else {
            signed.push_str(&format!("%{b:02X}"))
        }
    }
    http::encode_request(
        "GET",
        &format!("/fapi/v1/income?{signed}"),
        &[
            ("Host", authority(&config.scope)),
            ("X-MBX-APIKEY", credentials.api_key),
        ],
        &[],
        config.limits.max_send_bytes,
    )
    .map_err(Error::Transport)
}
impl<S: Store> AccountIngress<S> {
    pub(crate) fn validate_income(&self, record: &IncomeRecord) -> Result<()> {
        record.query.validate()?;
        if record.epoch == 0
            || record.clock_boot_id == [0; 16]
            || record.clock_generation == 0
            || record.raw.len() > self.config.max_raw_bytes
            || record.rows.len() > record.query.limit as usize
            || (!record.accepted && !record.rows.is_empty())
        {
            return Err(Error::Identity);
        }
        if record.accepted {
            let mut decoder =
                ResponseDecoder::new(self.config.max_raw_bytes, 256).map_err(Error::Transport)?;
            decoder.push(&record.raw).map_err(Error::Transport)?;
            decoder.finish();
            let response = decoder
                .next_response()
                .map_err(Error::Transport)?
                .ok_or(Error::Encoding)?;
            if record.status != 200
                || response.status != record.status
                || !decoder.take_buffered().is_empty()
                || bind(&self.config.baseline, record.query, &response.body)? != record.rows
            {
                return Err(Error::Identity);
            }
        }
        Ok(())
    }
    fn commit_income<B: Backend, A: Authority, E: Encoder>(
        &mut self,
        gateway: &mut tickwire_account_gateway::AccountGateway<B, A, E>,
        record: IncomeRecord,
    ) -> Result<InputKey> {
        self.validate_income(&record)?;
        let sequence = self.position().checked_add(1).ok_or(Error::Capacity)?;
        let mut records = vec![Record::Income(record.clone())];
        if record.query.purpose == IncomeUse::Account {
            for index in 0..record.rows.len() {
                records.push(Record::IncomeEffect {
                    response: sequence,
                    index: index as u8,
                });
            }
        }
        let encoded = records
            .iter()
            .map(codec::encode)
            .collect::<Result<Vec<_>>>()?;
        let retained = encoded.iter().try_fold(self.retained, |n, b| {
            n.checked_add(b.len()).ok_or(Error::Capacity)
        })?;
        if self.records.len() + records.len() > self.config.max_records
            || retained > self.config.max_retained_bytes
        {
            self.stopped = true;
            return Err(Error::Capacity);
        }
        let slices = encoded.iter().map(Vec::as_slice).collect::<Vec<_>>();
        if let Err(e) = self.journal.append_batch(self.config.writer_epoch, &slices) {
            self.stopped = true;
            gateway.freeze_account();
            return Err(e.into());
        }
        self.records.extend(records);
        self.retained = retained;
        if record.query.purpose == IncomeUse::Account {
            for (index, row) in record.rows.into_iter().enumerate() {
                if let Err(e) = gateway.record_account_input(
                    self.input_stream(),
                    sequence + index as u64 + 1,
                    row,
                ) {
                    self.stopped = true;
                    return Err(Error::Gateway(e));
                }
            }
        }
        self.cursor = self.records.len();
        Ok(InputKey {
            stream: self.input_stream(),
            sequence,
        })
    }
    pub fn income_evidence(&self, sequence: u64) -> Result<Option<IncomeEvidence>> {
        let index = usize::try_from(sequence)
            .ok()
            .and_then(|n| n.checked_sub(1))
            .ok_or(Error::Identity)?;
        let Record::Income(r) = self.records.get(index).ok_or(Error::Identity)? else {
            return Err(Error::Identity);
        };
        self.validate_income(r)?;
        if !r.accepted {
            return Ok(None);
        }
        Ok(Some(IncomeEvidence {
            input: InputKey {
                stream: self.input_stream(),
                sequence,
            },
            query: r.query,
            raw_hash: ring::digest::digest(&ring::digest::SHA256, &r.raw)
                .as_ref()
                .try_into()
                .unwrap(),
            rows: r.rows.clone(),
            received: r.received,
            domain: tickwire_clock::ClockDomain::new(r.clock_boot_id, r.clock_generation)
                .map_err(|_| Error::Identity)?,
            baseline: self.config.baseline.digest(),
        }))
    }
}

#[derive(serde::Deserialize)]
struct Income<'a> {
    #[serde(borrow)]
    symbol: &'a str,
    #[serde(borrow, rename = "incomeType")]
    kind: &'a str,
    income: tickwire_binance::decimal::Decimal,
    #[serde(borrow)]
    asset: &'a str,
    #[serde(borrow)]
    info: Option<&'a str>,
    time: u64,
    #[serde(rename = "tranId")]
    id: i64,
    #[serde(borrow, rename = "tradeId")]
    trade: Option<&'a str>,
}
struct Rows(usize);
impl<'de> serde::de::DeserializeSeed<'de> for Rows {
    type Value = Vec<&'de serde_json::value::RawValue>;
    fn deserialize<D: serde::Deserializer<'de>>(
        self,
        d: D,
    ) -> std::result::Result<Self::Value, D::Error> {
        struct Visitor(usize);
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = Vec<&'de serde_json::value::RawValue>;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("bounded income rows")
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut a: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                let mut out = Vec::new();
                while let Some(row) = a.next_element()? {
                    if out.len() == self.0 {
                        return Err(serde::de::Error::custom("income row limit"));
                    }
                    out.push(row);
                }
                Ok(out)
            }
        }
        d.deserialize_seq(Visitor(self.0))
    }
}
pub(crate) fn bind(
    baseline: &CanonicalBaseline,
    query: IncomeQuery,
    raw: &[u8],
) -> Result<Vec<v::CanonicalAccountInput>> {
    use serde::de::DeserializeSeed;
    query.validate()?;
    if raw.len() > 32 * 1024 {
        return Err(Error::Capacity);
    }
    let scope = &baseline.snapshot().scope;
    let identity = crate::account_facts::identity(
        baseline,
        &tickwire_binance::events::SessionIdentity {
            risk_domain_id: scope.risk_domain_id,
            venue: scope.venue,
            environment: scope.environment,
            account: scope.account,
            connection_epoch: query.private_connection_epoch,
        },
    )?;
    let mut parser = serde_json::Deserializer::from_slice(raw);
    let rows = Rows(query.limit as usize)
        .deserialize(&mut parser)
        .map_err(|_| Error::Encoding)?;
    parser.end().map_err(|_| Error::Encoding)?;
    let mut keys = std::collections::BTreeSet::new();
    let mut out = Vec::with_capacity(rows.len());
    for raw in rows {
        let row: Income<'_> = serde_json::from_str(raw.get()).map_err(|_| Error::Encoding)?;
        if row.time < query.start_time_ms
            || row.time > query.end_time_ms
            || !keys.insert((row.kind, row.id))
        {
            return Err(Error::Identity);
        }
        let trade = row
            .trade
            .filter(|s| !s.is_empty())
            .map(|s| {
                let id = s.parse::<u64>().map_err(|_| Error::Identity)?;
                if id > i64::MAX as u64 || id.to_string() != s {
                    return Err(Error::Identity);
                }
                v::VenueTradeId::new(s).map_err(|_| Error::Encoding)
            })
            .transpose()?;
        let input = v::CanonicalAccountInput::new(v::AccountInput {
            identity: identity.clone(),
            body: v::AccountInputBody::Income(v::IncomeRecord {
                income_type: v::BoundedText::new(row.kind).map_err(|_| Error::Encoding)?,
                transaction_id: v::BoundedText::new(row.id.to_string())
                    .map_err(|_| Error::Encoding)?,
                delta: v::AssetDelta {
                    asset: crate::account_facts::asset(baseline, identity.wallet_id, row.asset)?,
                    amount: crate::account_facts::decimal(row.income)?,
                },
                symbol: if row.symbol.is_empty() {
                    None
                } else {
                    Some(v::Symbol::new(row.symbol).map_err(|_| Error::Encoding)?)
                },
                trade_id: trade,
                venue_time_ms: row.time,
                info: row
                    .info
                    .filter(|s| !s.is_empty())
                    .map(v::BoundedMessage::new)
                    .transpose()
                    .map_err(|_| Error::Encoding)?,
            }),
        })
        .map_err(|_| Error::Encoding)?;
        out.push(input);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn fixture() -> (CanonicalBaseline, IncomeQuery) {
        let (s, c, _) = crate::unit_fixture::futures_fixture();
        (
            CanonicalBaseline::new(s, c).unwrap(),
            IncomeQuery {
                start_time_ms: 1,
                end_time_ms: 100,
                limit: 2,
                private_connection_epoch: 7,
                gap_generation: 3,
                purpose: IncomeUse::BaselineCoverage,
            },
        )
    }
    #[test]
    fn scan_covers_every_interval_leaf_and_rejects_saturated_millisecond() {
        let (b, mut query) = fixture();
        query.limit = 1;
        query.start_time_ms = 99;
        query.end_time_ms = 100;
        let row=bind(&b,query,br#"[{"symbol":"","incomeType":"FUNDING_FEE","income":"-2","asset":"USDT","time":99,"tranId":42,"tradeId":""}]"#).unwrap();
        let domain = tickwire_clock::ClockDomain::new([7; 16], 1).unwrap();
        let make = |query, seq, rows| IncomeEvidence {
            input: InputKey {
                stream: [8; 16],
                sequence: seq,
            },
            query,
            raw_hash: [1; 32],
            rows,
            received: seq,
            domain,
            baseline: b.digest(),
        };
        let mut scan = IncomeScan::new(query, 3).unwrap();
        scan.accept(&make(query, 1, row.clone())).unwrap();
        let left = scan.next_query().unwrap().unwrap();
        assert_eq!((left.start_time_ms, left.end_time_ms), (99, 99));
        assert!(scan.accept(&make(left, 2, row)).is_err());
        assert!(!scan.complete());
        assert!(scan.next_query().is_err());
        let mut scan = IncomeScan::new(query, 3).unwrap();
        scan.accept(&make(query, 1, Vec::new())).unwrap();
        assert!(scan.complete());
        assert_eq!(scan.leaf_sequences(), &[1]);
    }
    #[test]
    fn income_preserves_exchange_key_signed_amount_and_component_reference() {
        let (b, q) = fixture();
        let rows=bind(&b,q,br#"[{"symbol":"BTCUSDT","incomeType":"COMMISSION","income":"-0.2","asset":"USDT","info":"","time":99,"tranId":-42,"tradeId":"17"}]"#).unwrap();
        let v::AccountInputBody::Income(i) = &rows[0].input().body else {
            panic!("income")
        };
        assert_eq!(i.transaction_id.as_str(), "-42");
        assert_eq!(i.delta.amount, "-0.2".parse().unwrap());
        assert_eq!(i.trade_id.as_ref().unwrap().as_str(), "17");
        assert_eq!(rows[0].input().identity.wallet_id, 1);
    }
    #[test]
    fn income_bounds_duplicate_keys_and_saturation_are_explicit() {
        let (b, mut q) = fixture();
        let row = r#"{"symbol":"","incomeType":"FUNDING_FEE","income":"-2","asset":"USDT","info":"","time":99,"tranId":42,"tradeId":""}"#;
        assert!(bind(&b, q, format!("[{row},{row}]").as_bytes()).is_err());
        q.limit = 1;
        assert!(bind(&b, q, format!("[{row},{row}]").as_bytes()).is_err());
        q.end_time_ms = 98;
        assert!(bind(&b, q, format!("[{row}]").as_bytes()).is_err());
        let (left, right) = q.split().unwrap();
        assert_eq!(left.end_time_ms + 1, right.start_time_ms);
        q.start_time_ms = q.end_time_ms;
        assert!(q.split().is_err());
    }
}
