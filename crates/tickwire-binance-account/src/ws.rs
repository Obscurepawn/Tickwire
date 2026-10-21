//! Durable identities for the single-owner WebSocket trading session.
use crate::*;
pub(crate) mod lifecycle;
pub(crate) mod owner;
pub(crate) mod response;
pub use owner::{ControlAction, SessionState, WebSocketSettings, WsConfig};
pub use response::{ResponseDecision, StoredWsResponse, WsResponseOutcome};
use std::{cell::Cell, rc::Rc};

struct OwnerState {
    alive: Cell<bool>,
    epoch: Cell<u64>,
}
pub(crate) struct WsOwner(Rc<OwnerState>);
impl WsOwner {
    pub(crate) fn new() -> Self {
        Self(Rc::new(OwnerState {
            alive: Cell::new(true),
            epoch: Cell::new(0),
        }))
    }
}
impl Drop for WsOwner {
    fn drop(&mut self) {
        self.0.alive.set(false);
    }
}

pub const MAX_REQUEST_ID_BYTES: usize = 64;
pub const MAX_REQUEST_BINDINGS: usize = 4096;

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct SessionRecord {
    pub identity: SessionIdentity,
    pub sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionIdentity {
    pub input_stream: [u8; 16],
    pub account_stream: [u8; 16],
    pub baseline_digest: v::Hash,
    pub scope: tickwire_account::Scope,
    pub product: v::Product,
    pub execution_instance: [u8; 16],
    pub writer_epoch: u64,
    pub connection_epoch: u64,
    pub credential_ref_digest: v::Hash,
}
/// The current input owner minted this only after synchronizing its reservation.
pub struct DurableWsSession {
    identity: SessionIdentity,
    input: InputKey,
    owner: Rc<OwnerState>,
}
impl DurableWsSession {
    pub fn is_current(&self) -> bool {
        self.owner.alive.get() && self.owner.epoch.get() == self.identity.connection_epoch
    }

    pub fn identity(&self) -> &SessionIdentity {
        &self.identity
    }
    pub fn input(&self) -> InputKey {
        self.input
    }
}
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct WsRequestId(String);
impl WsRequestId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
/// In-memory plan, not a durable binding or permission to send.
#[derive(Clone)]
pub struct WsRequestPlan {
    session: SessionIdentity,
    input: InputKey,
    id: WsRequestId,
    command: CanonicalCommand,
    owner: Rc<OwnerState>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignedWire {
    pub source: tickwire_clock::Source,
    pub domain: tickwire_clock::ClockDomain,
    pub timestamp: u64,
    pub unit: tickwire_clock::TimeUnit,
    pub window_ns: u64,
    pub request_hash: v::Hash,
    pub frame_hash: v::Hash,
    pub request_bytes: u32,
    pub frame_bytes: u32,
}
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct BindingRecord {
    session: SessionRecord,
    sequence: u64,
    command: CanonicalCommand,
    account_sequence: u64,
    record_digest: v::Hash,
    physical: tickwire_journal::PhysicalFrontier,
    history: [u8; 128],
    wire: SignedWire,
}
/// Read-only recovery/replication identity. This is not a live sending capability.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BindingEvidence {
    pub input: InputKey,
    pub input_record_hash: v::Hash,
    pub account_sequence: u64,
    pub command_digest: v::Hash,
}
/// Live-only capability: decoding or replaying a binding never recreates this type.
pub struct DurableWsBinding {
    record: BindingRecord,
    owner: Rc<OwnerState>,
}
impl DurableWsBinding {
    pub fn is_current(&self) -> bool {
        self.owner.alive.get() && self.owner.epoch.get() == self.session().connection_epoch
    }
    pub fn matches_plan(&self, plan: &WsRequestPlan) -> bool {
        self.is_current()
            && plan.is_current()
            && Rc::ptr_eq(&self.owner, &plan.owner)
            && self.input() == plan.input
            && self.session() == &plan.session
            && self.record.command == plan.command
    }
    pub fn record_digest(&self) -> v::Hash {
        self.record.record_digest
    }
    pub fn physical_frontier(&self) -> &tickwire_journal::PhysicalFrontier {
        &self.record.physical
    }

    pub fn input(&self) -> InputKey {
        InputKey {
            stream: self.record.session.identity.input_stream,
            sequence: self.record.sequence,
        }
    }
    pub fn account_sequence(&self) -> u64 {
        self.record.account_sequence
    }
    pub fn command_digest(&self) -> v::Hash {
        self.record.command.digest()
    }
    pub fn frame_hash(&self) -> v::Hash {
        self.record.wire.frame_hash
    }
    pub fn frame_bytes(&self) -> usize {
        self.record.wire.frame_bytes as usize
    }
    pub fn session(&self) -> &SessionIdentity {
        &self.record.session.identity
    }
    pub fn request_id(&self) -> WsRequestId {
        request_id(self.session(), self.record.sequence)
    }
    pub fn signed_wire(&self) -> &SignedWire {
        &self.record.wire
    }
}
impl<S: Store> AccountIngress<S> {
    pub fn persist_ws_binding<B, A, E>(
        &mut self,
        plan: WsRequestPlan,
        gateway: &tickwire_account_gateway::AccountGateway<B, A, E>,
        wire: SignedWire,
    ) -> Result<DurableWsBinding>
    where
        B: tickwire_account_journal::Backend,
        A: tickwire_account_gateway::Authority,
        E: tickwire_account_gateway::Encoder,
    {
        self.ready(gateway)?;
        let key = intent(&plan.command);
        let session = self.latest_ws_session().ok_or(Error::Identity)?.clone();
        let committed = gateway.committed(key).ok_or(Error::Identity)?;
        let origin = gateway.admission_origin(key).ok_or(Error::Identity)?;
        if !plan.is_current()
            || !Rc::ptr_eq(&plan.owner, &self.ws_owner.0)
            || plan.session != session.identity
            || plan.input.stream != self.input_stream()
            || plan.session.writer_epoch != self.config.writer_epoch
            || gateway.command(key) != Some(&plan.command)
            || committed.admission().command_digest() != plan.command.digest()
            || committed.receipt() != origin.receipt()
            || gateway
                .output_progress(key)
                .is_none_or(|p| p.accepted_bytes != 0)
            || !matches!(
                gateway.status(key),
                Some(
                    tickwire_account_gateway::Status::AwaitingCommit
                        | tickwire_account_gateway::Status::Queued
                )
            )
        {
            return Err(Error::Identity);
        }
        let receipt = committed.receipt();
        let record = BindingRecord {
            session,
            sequence: plan.input.sequence,
            command: plan.command,
            account_sequence: receipt.sequence(),
            record_digest: receipt.record_digest(),
            physical: receipt.physical_frontier().clone(),
            history: origin
                .history()
                .bytes()
                .try_into()
                .map_err(|_| Error::Identity)?,
            wire,
        };
        let previous_matches = self
            .ws_bindings()
            .find(|r| intent(&r.command) == key)
            .map(|previous| previous == &record);
        if let Some(matches) = previous_matches {
            if !matches {
                self.stopped = true;
                return Err(Error::Conflict);
            }
            return Ok(DurableWsBinding {
                record,
                owner: Rc::clone(&self.ws_owner.0),
            });
        }
        if self.ws_bindings().count() >= MAX_REQUEST_BINDINGS {
            return Err(Error::Capacity);
        }
        self.validate_ws_binding(
            &record,
            self.position().checked_add(1).ok_or(Error::Capacity)?,
            self.config.writer_epoch,
        )?;
        self.verify_ws_binding(gateway, &record)?;
        self.append(Record::WsBinding(Box::new(record.clone())))?;
        self.cursor = self.records.len();
        Ok(DurableWsBinding {
            record,
            owner: Rc::clone(&self.ws_owner.0),
        })
    }
    pub fn ws_binding_evidence(&self, key: IntentKey) -> Result<Option<BindingEvidence>> {
        self.ws_bindings()
            .find(|b| intent(&b.command) == key)
            .map(|b| {
                let bytes = codec::encode(&Record::WsBinding(Box::new(b.clone())))?;
                Ok(BindingEvidence {
                    input: InputKey {
                        stream: self.input_stream(),
                        sequence: b.sequence,
                    },
                    input_record_hash: ring::digest::digest(&ring::digest::SHA256, &bytes)
                        .as_ref()
                        .try_into()
                        .unwrap(),
                    account_sequence: b.account_sequence,
                    command_digest: b.command.digest(),
                })
            })
            .transpose()
    }
    pub fn validate_ws_origins<B: tickwire_account_journal::Backend>(
        &self,
        journal: &tickwire_account_journal::AccountJournal<B>,
    ) -> Result<()> {
        for b in self.ws_bindings() {
            let origin = journal
                .admission_origin(intent(&b.command))
                .ok_or(Error::Identity)?;
            let receipt = origin.receipt();
            if receipt.baseline_digest()!=b.session.identity.baseline_digest || receipt.sequence()!=b.account_sequence
                || receipt.record_digest()!=b.record_digest || receipt.physical_frontier()!=&b.physical || origin.history().bytes()!=b.history
                || !journal.records().iter().any(|r|matches!(r.record(),tickwire_account::RecoveryRecord::Admit{command,..} if command.as_ref()==&b.command)) {return Err(Error::Identity);}
        }
        Ok(())
    }
    fn ws_bindings(&self) -> impl Iterator<Item = &BindingRecord> {
        self.records.iter().filter_map(|r| {
            if let Record::WsBinding(b) = r {
                Some(b.as_ref())
            } else {
                None
            }
        })
    }
    pub(crate) fn validate_ws_binding(
        &self,
        r: &BindingRecord,
        sequence: u64,
        writer: u64,
    ) -> Result<()> {
        valid_binding(r)?;
        if self.latest_ws_session() != Some(&r.session)
            || r.sequence != sequence
            || r.session.identity.writer_epoch != writer
            || self
                .ws_bindings()
                .any(|old| intent(&old.command) == intent(&r.command))
        {
            return Err(Error::Identity);
        }
        if self.ws_bindings().count() >= MAX_REQUEST_BINDINGS {
            return Err(Error::Capacity);
        }
        Ok(())
    }
    pub(crate) fn verify_ws_binding<
        B: tickwire_account_journal::Backend,
        A: tickwire_account_gateway::Authority,
        E: tickwire_account_gateway::Encoder,
    >(
        &self,
        gateway: &tickwire_account_gateway::AccountGateway<B, A, E>,
        r: &BindingRecord,
    ) -> Result<()> {
        let key = intent(&r.command);
        let origin = gateway.admission_origin(key).ok_or(Error::Identity)?;
        let receipt = origin.receipt();
        if gateway.command(key) != Some(&r.command)
            || receipt.baseline_digest() != r.session.identity.baseline_digest
            || receipt.sequence() != r.account_sequence
            || receipt.record_digest() != r.record_digest
            || receipt.physical_frontier() != &r.physical
            || origin.history().bytes() != r.history
        {
            return Err(Error::Identity);
        }
        Ok(())
    }
}
fn request_id(session: &SessionIdentity, sequence: u64) -> WsRequestId {
    use base64::Engine;
    let mut bytes = Vec::with_capacity(40);
    bytes.extend(session.input_stream);
    bytes.extend(session.writer_epoch.to_be_bytes());
    bytes.extend(session.connection_epoch.to_be_bytes());
    bytes.extend(sequence.to_be_bytes());
    WsRequestId(format!(
        "w1:{}",
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
    ))
}

fn valid_binding(r: &BindingRecord) -> Result<()> {
    valid_session(&r.session)?;
    let s = &r.session.identity;
    let c = &r.command.command().context.request;
    let w = &r.wire;
    let expected_source = match (s.product, s.scope.environment) {
        (v::Product::Spot, v::Environment::Production) => tickwire_clock::Source::SpotProduction,
        (v::Product::Spot, v::Environment::Test) => tickwire_clock::Source::SpotTest,
        (_, v::Environment::Production) => tickwire_clock::Source::UsdmProduction,
        (_, v::Environment::Test) => tickwire_clock::Source::UsdmTest,
    };
    if r.sequence <= r.session.sequence
        || r.account_sequence == 0
        || r.record_digest == [0; 32]
        || c.risk_domain_id != s.scope.risk_domain_id
        || c.venue != s.scope.venue
        || c.account != s.scope.account
        || c.environment != s.scope.environment
        || c.product != s.product
        || c.intent_exec_epoch != s.writer_epoch
        || r.physical.stream_epoch != s.account_stream
        || r.physical.domain != s.scope.risk_domain_id
        || r.physical.exec_epoch != s.writer_epoch
        || r.physical.last_seq != r.account_sequence
        || r.physical.seal_hash == [0; 32]
        || w.source != expected_source
        || w.window_ns == 0
        || w.window_ns > 60_000_000_000
        || (s.product != v::Product::Spot && w.unit != tickwire_clock::TimeUnit::Milliseconds)
        || w.request_hash == [0; 32]
        || w.frame_hash == [0; 32]
        || w.request_bytes == 0
        || w.request_bytes > 16_384
        || w.frame_bytes != w.request_bytes + if w.request_bytes <= 125 { 6 } else { 8 }
    {
        return Err(Error::Identity);
    }
    Ok(())
}
impl WsRequestPlan {
    pub fn is_current(&self) -> bool {
        self.owner.alive.get() && self.owner.epoch.get() == self.session.connection_epoch
    }

    pub fn request_id(&self) -> &WsRequestId {
        &self.id
    }
    pub fn input(&self) -> InputKey {
        self.input
    }
    pub fn command(&self) -> &CanonicalCommand {
        &self.command
    }
    pub fn session(&self) -> &SessionIdentity {
        &self.session
    }
}
impl<S: Store> AccountIngress<S> {
    /// Revokes all live WS plans/bindings before dropping or replacing a connection.
    pub fn invalidate_ws_session(&mut self) {
        self.ws_owner.0.epoch.set(0);
    }
    pub fn reserve_ws_session(
        &mut self,
        execution_instance: [u8; 16],
        credential_ref_digest: v::Hash,
    ) -> Result<DurableWsSession> {
        if self.stopped {
            return Err(Error::Stopped);
        }
        if self.cursor != self.records.len() {
            return Err(Error::ReplayPending);
        }
        let connection_epoch = self.latest_ws_session().map_or(Ok(1), |r| {
            r.identity
                .connection_epoch
                .checked_add(1)
                .ok_or(Error::Capacity)
        })?;
        let record = SessionRecord {
            identity: SessionIdentity {
                input_stream: self.config.physical.stream_epoch,
                account_stream: self.config.account_stream,
                baseline_digest: self.config.baseline.digest(),
                scope: self.config.baseline.snapshot().scope.clone(),
                product: product(&self.config)?,
                execution_instance,
                writer_epoch: self.config.writer_epoch,
                connection_epoch,
                credential_ref_digest,
            },
            sequence: self.position().checked_add(1).ok_or(Error::Capacity)?,
        };
        self.validate_ws_session(&record, record.sequence, self.config.writer_epoch)?;
        let input = self.append(Record::WsSession(record.clone()))?;
        self.ws_owner.0.epoch.set(connection_epoch);
        self.cursor = self.records.len();
        Ok(DurableWsSession {
            identity: record.identity,
            input,
            owner: Rc::clone(&self.ws_owner.0),
        })
    }
    pub fn plan_ws_request(
        &self,
        session: &DurableWsSession,
        command: CanonicalCommand,
    ) -> Result<WsRequestPlan> {
        if self.stopped {
            return Err(Error::Stopped);
        }
        if self.cursor != self.records.len() {
            return Err(Error::ReplayPending);
        }
        let actual = self.latest_ws_session().ok_or(Error::Identity)?;
        let c = command.command();
        let r = &c.context.request;
        if !session.owner.alive.get()
            || session.owner.epoch.get() != session.identity.connection_epoch
            || !Rc::ptr_eq(&session.owner, &self.ws_owner.0)
            || session.identity != actual.identity
            || session.input.sequence != actual.sequence
            || session.input.stream != self.input_stream()
            || actual.identity.writer_epoch != self.config.writer_epoch
            || r.intent_exec_epoch != self.config.writer_epoch
            || r.risk_domain_id != actual.identity.scope.risk_domain_id
            || r.venue != actual.identity.scope.venue
            || r.account != actual.identity.scope.account
            || r.environment != actual.identity.scope.environment
            || r.product != actual.identity.product
            || !self
                .config
                .baseline
                .catalog()
                .entries
                .iter()
                .any(|e| e.product == r.product && e.instrument == c.context.instrument)
        {
            return Err(Error::Identity);
        }
        let input = InputKey {
            stream: self.input_stream(),
            sequence: self.position().checked_add(1).ok_or(Error::Capacity)?,
        };
        let id = request_id(&actual.identity, input.sequence);
        Ok(WsRequestPlan {
            session: actual.identity.clone(),
            input,
            id,
            command,
            owner: Rc::clone(&self.ws_owner.0),
        })
    }

    fn latest_ws_session(&self) -> Option<&SessionRecord> {
        self.records.iter().rev().find_map(|r| match r {
            Record::WsSession(session) => Some(session),
            _ => None,
        })
    }

    pub(crate) fn validate_ws_session(
        &self,
        record: &SessionRecord,
        sequence: u64,
        writer: u64,
    ) -> Result<()> {
        let i = &record.identity;
        valid_session(record)?;
        if i.input_stream != self.input_stream()
            || i.account_stream != self.config.account_stream
            || i.baseline_digest != self.config.baseline.digest()
            || i.scope != self.config.baseline.snapshot().scope
            || i.product != product(&self.config)?
            || i.writer_epoch != writer
            || record.sequence != sequence
        {
            return Err(Error::Identity);
        }
        if let Some(previous) = self.latest_ws_session() {
            if i.connection_epoch
                != previous
                    .identity
                    .connection_epoch
                    .checked_add(1)
                    .ok_or(Error::Capacity)?
                || i.writer_epoch < previous.identity.writer_epoch
                || (i.writer_epoch == previous.identity.writer_epoch
                    && i.execution_instance != previous.identity.execution_instance)
            {
                return Err(Error::Identity);
            }
        } else if i.connection_epoch != 1 {
            return Err(Error::Identity);
        }
        Ok(())
    }
}

fn valid_session(r: &SessionRecord) -> Result<()> {
    let i = &r.identity;
    if i.input_stream == [0; 16]
        || i.account_stream == [0; 16]
        || i.baseline_digest == [0; 32]
        || i.scope.risk_domain_id == [0; 16]
        || i.scope.venue != 1
        || i.scope.account == 0
        || i.execution_instance == [0; 16]
        || i.writer_epoch == 0
        || i.connection_epoch == 0
        || i.credential_ref_digest == [0; 32]
        || r.sequence == 0
        || !matches!(
            i.product,
            v::Product::Spot | v::Product::LinearPerpetual | v::Product::LinearFuture
        )
    {
        return Err(Error::Identity);
    }
    Ok(())
}

pub(crate) fn encode_session(r: &SessionRecord) -> Result<Vec<u8>> {
    valid_session(r)?;
    let i = &r.identity;
    let mut b = b"TWWS0001".to_vec();
    b.extend(i.input_stream);
    b.extend(i.account_stream);
    b.extend(i.baseline_digest);
    b.extend(i.scope.risk_domain_id);
    b.extend(i.scope.venue.to_le_bytes());
    b.extend(i.scope.account.to_le_bytes());
    b.push(match i.scope.environment {
        v::Environment::Production => 1,
        v::Environment::Test => 2,
    });
    b.push(match i.product {
        v::Product::Spot => 1,
        v::Product::LinearPerpetual => 2,
        v::Product::LinearFuture => 3,
        _ => return Err(Error::Unsupported),
    });
    b.extend(i.execution_instance);
    b.extend(i.writer_epoch.to_le_bytes());
    b.extend(i.connection_epoch.to_le_bytes());
    b.extend(i.credential_ref_digest);
    b.extend(r.sequence.to_le_bytes());
    Ok(b)
}

pub(crate) fn decode_session(bytes: &[u8]) -> Result<SessionRecord> {
    let mut r = Reader::new(bytes);
    if r.take(8)? != b"TWWS0001" {
        return Err(Error::Encoding);
    }
    let input_stream = r.fixed()?;
    let account_stream = r.fixed()?;
    let baseline_digest = r.fixed()?;
    let risk_domain_id = r.fixed()?;
    let venue = u32::from_le_bytes(r.fixed()?);
    let account = r.u64()?;
    let environment = match r.byte()? {
        1 => v::Environment::Production,
        2 => v::Environment::Test,
        _ => return Err(Error::Encoding),
    };
    let product = match r.byte()? {
        1 => v::Product::Spot,
        2 => v::Product::LinearPerpetual,
        3 => v::Product::LinearFuture,
        _ => return Err(Error::Encoding),
    };
    let execution_instance = r.fixed()?;
    let writer_epoch = r.u64()?;
    let connection_epoch = r.u64()?;
    let credential_ref_digest = r.fixed()?;
    let sequence = r.u64()?;
    let out = SessionRecord {
        identity: SessionIdentity {
            input_stream,
            account_stream,
            baseline_digest,
            scope: tickwire_account::Scope {
                risk_domain_id,
                venue,
                environment,
                account,
            },
            product,
            execution_instance,
            writer_epoch,
            connection_epoch,
            credential_ref_digest,
        },
        sequence,
    };
    if encode_session(&out)? != bytes {
        return Err(Error::Encoding);
    }
    Ok(out)
}
pub(crate) fn encode_binding(r: &BindingRecord) -> Result<Vec<u8>> {
    valid_binding(r)?;
    let mut out = b"TWWB0001".to_vec();
    for bytes in [&encode_session(&r.session)?[..], r.command.bytes()] {
        out.extend((bytes.len() as u32).to_le_bytes());
        out.extend(bytes);
    }
    out.extend(r.sequence.to_le_bytes());
    out.extend(r.account_sequence.to_le_bytes());
    out.extend(r.record_digest);
    let p = &r.physical;
    out.extend(p.stream_epoch);
    out.extend(p.domain);
    for n in [
        p.segment_id,
        p.end_offset,
        p.end_lsn,
        p.last_seq,
        p.exec_epoch,
        p.batch_number,
    ] {
        out.extend(n.to_le_bytes());
    }
    out.extend(p.seal_hash);
    out.extend(r.history);
    let w = &r.wire;
    out.push(match w.source {
        tickwire_clock::Source::SpotProduction => 1,
        tickwire_clock::Source::SpotTest => 2,
        tickwire_clock::Source::UsdmProduction => 3,
        tickwire_clock::Source::UsdmTest => 4,
    });
    out.extend(w.domain.boot_id());
    out.extend(w.domain.generation().to_le_bytes());
    out.extend(w.timestamp.to_le_bytes());
    out.push(match w.unit {
        tickwire_clock::TimeUnit::Milliseconds => 1,
        tickwire_clock::TimeUnit::Microseconds => 2,
    });
    out.extend(w.window_ns.to_le_bytes());
    out.extend(w.request_hash);
    out.extend(w.frame_hash);
    out.extend(w.request_bytes.to_le_bytes());
    out.extend(w.frame_bytes.to_le_bytes());
    Ok(out)
}
pub(crate) fn decode_binding(bytes: &[u8]) -> Result<BindingRecord> {
    if bytes.len() > MAX_RECORD_BYTES {
        return Err(Error::Capacity);
    }
    let mut r = Reader::new(bytes);
    if r.take(8)? != b"TWWB0001" {
        return Err(Error::Encoding);
    }
    let session = decode_session(r.blob()?)?;
    let command = CanonicalCommand::decode(r.blob()?).map_err(|_| Error::Encoding)?;
    let sequence = r.u64()?;
    let account_sequence = r.u64()?;
    let record_digest = r.fixed()?;
    let physical = tickwire_journal::PhysicalFrontier {
        stream_epoch: r.fixed()?,
        domain: r.fixed()?,
        segment_id: r.u64()?,
        end_offset: r.u64()?,
        end_lsn: r.u64()?,
        last_seq: r.u64()?,
        exec_epoch: r.u64()?,
        batch_number: r.u64()?,
        seal_hash: r.fixed()?,
    };
    let history = r.fixed()?;
    let source = match r.byte()? {
        1 => tickwire_clock::Source::SpotProduction,
        2 => tickwire_clock::Source::SpotTest,
        3 => tickwire_clock::Source::UsdmProduction,
        4 => tickwire_clock::Source::UsdmTest,
        _ => return Err(Error::Encoding),
    };
    let domain =
        tickwire_clock::ClockDomain::new(r.fixed()?, r.u64()?).map_err(|_| Error::Encoding)?;
    let timestamp = r.u64()?;
    let unit = match r.byte()? {
        1 => tickwire_clock::TimeUnit::Milliseconds,
        2 => tickwire_clock::TimeUnit::Microseconds,
        _ => return Err(Error::Encoding),
    };
    let wire = SignedWire {
        source,
        domain,
        timestamp,
        unit,
        window_ns: r.u64()?,
        request_hash: r.fixed()?,
        frame_hash: r.fixed()?,
        request_bytes: u32::from_le_bytes(r.fixed()?),
        frame_bytes: u32::from_le_bytes(r.fixed()?),
    };
    let out = BindingRecord {
        session,
        sequence,
        command,
        account_sequence,
        record_digest,
        physical,
        history,
        wire,
    };
    if encode_binding(&out)? != bytes {
        return Err(Error::Encoding);
    }
    Ok(out)
}
struct Reader<'a> {
    bytes: &'a [u8],
    position: usize,
}
impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self.position.checked_add(n).ok_or(Error::Encoding)?;
        let bytes = self.bytes.get(self.position..end).ok_or(Error::Encoding)?;
        self.position = end;
        Ok(bytes)
    }
    fn blob(&mut self) -> Result<&'a [u8]> {
        let n = u32::from_le_bytes(self.fixed()?) as usize;
        self.take(n)
    }
    fn fixed<const N: usize>(&mut self) -> Result<[u8; N]> {
        Ok(self.take(N)?.try_into().unwrap())
    }
    fn byte(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.fixed()?))
    }
}

#[cfg(test)]
mod tests;
