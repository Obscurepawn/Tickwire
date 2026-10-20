//! Durable command admission and a single-owner, guarded network send.
#![forbid(unsafe_code)]

pub mod binance;

use std::{collections::BTreeMap, net::SocketAddr, sync::Arc, time::Instant};
use tickwire_business_journal::{
    AuthorizationError, AuthorizationRequest, Backend, BusinessConfig, BusinessJournal,
    closure_binding, intent_record,
};
use tickwire_execution::{self as ex, DurableRecord, Gate};
use tickwire_journal::PhysicalFrontier;
use tickwire_journal_codec as codec;
use tickwire_recovery::VerifiedClosure;
use tickwire_transport::{
    Connection, IoBudget, Limits, PreparedSend, Security, SendReport, SendState, rustls,
};
use tickwire_venue::{CanonicalCommand, Environment, Product};

#[derive(Debug)]
pub enum Error {
    Journal(tickwire_business_journal::Error),
    Authorization(AuthorizationError),
    Transport(tickwire_transport::Error),
    Identity,
    Invalid,
    Busy,
    Conflict,
    Frozen,
    Capacity,
    Encoding,
}
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Route {
    pub venue: u32,
    pub account: u64,
    pub environment: Environment,
    pub product: Product,
    pub authority: String,
}
pub struct Config {
    pub business: BusinessConfig,
    pub writer_epoch: u64,
    pub route: Route,
    pub max_intents: usize,
}
pub struct ConnectionConfig {
    pub address: SocketAddr,
    pub tls: Arc<rustls::ClientConfig>,
    pub limits: Limits,
    pub connect_deadline: Instant,
}
/// Trusted protocol encoder: bytes must encode this exact canonical command.
pub trait Encoder {
    fn encode(&mut self, command: &CanonicalCommand) -> Result<EncodedRequest>;
}
pub struct EncodedRequest {
    digest: [u8; 32],
    authority: String,
    bytes: Vec<u8>,
}
impl EncodedRequest {
    pub fn new(digest: [u8; 32], authority: String, bytes: Vec<u8>) -> Result<Self> {
        if digest == [0; 32] || bytes.is_empty() || bytes.len() > 16 * 1024 * 1024 {
            return Err(Error::Invalid);
        }
        Ok(Self {
            digest,
            authority,
            bytes,
        })
    }
}
/// Trusted strategy, account and risk authority. There is no permissive default.
pub trait Authorization {
    fn authorize(
        &mut self,
        command: &CanonicalCommand,
        request: AuthorizationRequest<'_>,
    ) -> std::result::Result<VerifiedClosure, AuthorizationError>;
    /// Recheck mode/catalog/permissions and risk approval before returning a fresh gate.
    fn current_gate(
        &mut self,
        command: &CanonicalCommand,
        record: DurableRecord,
    ) -> std::result::Result<Gate, AuthorizationError>;
    /// Load original canonical bytes and verify an existing synchronized closure.
    fn recover(
        &mut self,
        request: AuthorizationRequest<'_>,
    ) -> std::result::Result<(CanonicalCommand, VerifiedClosure), AuthorizationError>;
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    Queued,
    Sending,
    AwaitingResponse,
    Unknown,
    Aborted,
}
impl From<tickwire_business_journal::Error> for Error {
    fn from(v: tickwire_business_journal::Error) -> Self {
        Self::Journal(v)
    }
}
impl From<tickwire_transport::Error> for Error {
    fn from(v: tickwire_transport::Error) -> Self {
        Self::Transport(v)
    }
}
struct Entry {
    command: CanonicalCommand,
    record: DurableRecord,
    identity: codec::OrderIdentity,
    status: Status,
    has_facts: bool,
}
pub struct JournaledSender<S: Backend, A: Authorization, E: Encoder> {
    journal: BusinessJournal<S>,
    config: Config,
    authorization: A,
    encoder: E,
    connection: Option<Connection>,
    entries: BTreeMap<u64, Entry>,
    pending: Option<(u64, u64)>,
    max_send_bytes: usize,
    frozen: bool,
}
impl<S: Backend, A: Authorization, E: Encoder> JournaledSender<S, A, E> {
    pub fn create(
        backend: S,
        config: Config,
        authorization: A,
        encoder: E,
        connection: ConnectionConfig,
    ) -> Result<Self> {
        validate_config(&config)?;
        let server_name = config
            .route
            .authority
            .clone()
            .try_into()
            .map_err(|_| Error::Identity)?;
        let max_send_bytes = connection.limits.max_send_bytes;
        let connection = Connection::connect(
            connection.address,
            Security::Tls {
                server_name,
                config: connection.tls,
            },
            connection.limits,
            connection.connect_deadline,
        )?;
        let journal =
            BusinessJournal::create(backend, config.business.clone(), config.writer_epoch)?;
        Ok(Self {
            journal,
            config,
            authorization,
            encoder,
            connection: Some(connection),
            entries: BTreeMap::new(),
            pending: None,
            max_send_bytes,
            frozen: false,
        })
    }
    pub fn recover(
        backend: S,
        config: Config,
        mut authorization: A,
        encoder: E,
        required: Option<&PhysicalFrontier>,
    ) -> Result<Self> {
        validate_config(&config)?;
        let journal = BusinessJournal::recover(
            backend,
            config.business.clone(),
            config.writer_epoch,
            required,
        )?;
        let mut entries = BTreeMap::<u64, Entry>::new();
        for event in journal.events() {
            let identity = event.event.order();
            if let codec::Event::PreparedIntent(p) = event.event {
                if entries.len() == config.max_intents {
                    return Err(Error::Capacity);
                }
                let record = restore_record(p);
                let binding = closure_binding(event)?;
                let payload = codec::business_payload(event);
                let (command, closure) = authorization
                    .recover(AuthorizationRequest {
                        binding: &binding,
                        record: &record,
                        intent_payload: &payload,
                    })
                    .map_err(Error::Authorization)?;
                validate_binding(&config, record, &command, false)?;
                if closure.binding() != &binding
                    || closure.command_digest() != Some(command.digest())
                {
                    return Err(Error::Identity);
                }
                if entries
                    .insert(
                        identity.intent_id,
                        Entry {
                            command,
                            record,
                            identity,
                            status: Status::Unknown,
                            has_facts: false,
                        },
                    )
                    .is_some()
                {
                    return Err(Error::Conflict);
                }
            } else {
                let entry = entries
                    .get_mut(&identity.intent_id)
                    .ok_or(Error::Identity)?;
                if identity != entry.identity {
                    return Err(Error::Identity);
                }
                if matches!(event.event, codec::Event::AbortedBeforeSend { .. }) {
                    if entry.has_facts {
                        return Err(Error::Conflict);
                    }
                    entry.status = Status::Aborted;
                } else {
                    if entry.status == Status::Aborted {
                        return Err(Error::Conflict);
                    }
                    entry.has_facts = true;
                }
            }
        }
        Ok(Self {
            journal,
            config,
            authorization,
            encoder,
            connection: None,
            entries,
            pending: None,
            max_send_bytes: 0,
            frozen: true,
        })
    }
    pub fn submit(
        &mut self,
        record: DurableRecord,
        command: CanonicalCommand,
        deadline: Instant,
    ) -> Result<Status> {
        let id = record.request.key.intent;
        if let Some(old) = self.entries.get(&id) {
            if old.record == record && old.command.bytes() == command.bytes() {
                return Ok(old.status);
            }
            self.freeze_unknown();
            return Err(Error::Conflict);
        }
        if self.frozen {
            return Err(Error::Frozen);
        }
        if self.pending.is_some() {
            return Err(Error::Busy);
        }
        if self.entries.len() == self.config.max_intents {
            return Err(Error::Capacity);
        }
        validate_binding(&self.config, record, &command, true)?;
        let wire = self.encoder.encode(&command)?;
        if wire.digest != command.digest() || wire.authority != self.config.route.authority {
            return Err(Error::Identity);
        }
        let send = PreparedSend::new(&wire.bytes, deadline, self.max_send_bytes)?;
        let seq = self
            .journal
            .position()
            .checked_add(1)
            .ok_or(Error::Capacity)?;
        let event = intent_record(&self.config.business, self.config.writer_epoch, seq, record);
        let binding = closure_binding(&event)?;
        let payload = codec::business_payload(&event);
        let closure = self
            .authorization
            .authorize(
                &command,
                AuthorizationRequest {
                    binding: &binding,
                    record: &record,
                    intent_payload: &payload,
                },
            )
            .map_err(Error::Authorization)?;
        // Only a synchronized command closure and WAL can reach the network queue.
        if let Err(error) = self.journal.commit_intent(record, &closure) {
            self.freeze_unknown();
            return Err(error.into());
        }
        self.entries.insert(
            id,
            Entry {
                command,
                record,
                identity: event.event.order(),
                status: Status::Queued,
                has_facts: false,
            },
        );
        match self.connection.as_mut().ok_or(Error::Frozen)?.queue(send) {
            Ok(send_id) => {
                self.pending = Some((id, send_id));
                Ok(Status::Queued)
            }
            Err(error) => {
                // queue has accepted no application bytes.
                self.freeze_unknown();
                self.abort(id)?;
                Err(error.into())
            }
        }
    }
    pub fn poll(&mut self, now: Instant, budget: IoBudget) -> Result<()> {
        if self.frozen {
            return Err(Error::Frozen);
        }
        let driven = self
            .connection
            .as_mut()
            .ok_or(Error::Frozen)?
            .drive(now, budget);
        if let Some((id, _)) = self.pending {
            self.settle()?;
            if self.pending.is_some() && driven.is_ok() {
                let entry = &self.entries[&id];
                let epoch = self.config.writer_epoch;
                let authorization = &mut self.authorization;
                let result = self.connection.as_mut().unwrap().try_send(now, || {
                    authorization
                        .current_gate(&entry.command, entry.record)
                        .is_ok_and(|gate| permits(gate, entry.record, epoch))
                });
                match result {
                    Ok(report) => self.apply_report(report)?,
                    Err(error) => {
                        self.freeze_unknown();
                        return Err(error.into());
                    }
                }
            }
        }
        match driven {
            Ok(report) if !report.peer_closed => Ok(()),
            other => {
                self.freeze_unknown();
                match other {
                    Err(error) => Err(error.into()),
                    Ok(_) => Ok(()),
                }
            }
        }
    }
    pub fn cancel(&mut self) -> Result<()> {
        if self.frozen {
            return Err(Error::Frozen);
        }
        if self.pending.is_none() {
            return Err(Error::Invalid);
        }
        let report = self.connection.as_mut().ok_or(Error::Frozen)?.cancel()?;
        self.apply_report(report)
    }
    pub fn status(&self, intent: u64) -> Option<Status> {
        self.entries.get(&intent).map(|e| e.status)
    }
    pub fn frozen(&self) -> bool {
        self.frozen
    }
    pub fn journal_position(&self) -> u64 {
        self.journal.position()
    }
    pub fn read(&mut self, out: &mut [u8]) -> usize {
        self.connection.as_mut().map_or(0, |c| c.read(out))
    }
    pub fn register(
        &mut self,
        registry: &tickwire_transport::mio::Registry,
        token: tickwire_transport::mio::Token,
    ) -> Result<()> {
        Ok(self
            .connection
            .as_mut()
            .ok_or(Error::Frozen)?
            .register(registry, token)?)
    }
    pub fn reregister(
        &mut self,
        registry: &tickwire_transport::mio::Registry,
        token: tickwire_transport::mio::Token,
    ) -> Result<()> {
        Ok(self
            .connection
            .as_mut()
            .ok_or(Error::Frozen)?
            .reregister(registry, token)?)
    }
    fn settle(&mut self) -> Result<()> {
        let report = self
            .connection
            .as_ref()
            .and_then(|c| c.send_report())
            .ok_or(Error::Invalid)?;
        self.apply_report(report)
    }
    fn apply_report(&mut self, report: SendReport) -> Result<()> {
        let (id, send_id) = self.pending.ok_or(Error::Invalid)?;
        if report.id != send_id {
            self.freeze_unknown();
            return Err(Error::Identity);
        }
        match report.state {
            SendState::Queued => {}
            SendState::MayHaveSent => self.entries.get_mut(&id).unwrap().status = Status::Sending,
            SendState::Flushed => {
                self.entries.get_mut(&id).unwrap().status = Status::AwaitingResponse;
                self.connection.as_mut().unwrap().take_finished();
                self.pending = None;
            }
            SendState::NoBytes(_) => {
                if report.accepted_bytes != 0 {
                    self.freeze_unknown();
                    return Err(Error::Invalid);
                }
                self.abort(id)?;
                self.connection.as_mut().unwrap().take_finished();
                self.pending = None;
            }
            SendState::Failed(_) => self.freeze_unknown(),
        }
        Ok(())
    }
    fn abort(&mut self, id: u64) -> Result<()> {
        let identity = self.entries[&id].identity;
        if let Err(error) = self
            .journal
            .append_fact(codec::Event::AbortedBeforeSend { order: identity })
        {
            self.freeze_unknown();
            return Err(error.into());
        }
        self.entries.get_mut(&id).unwrap().status = Status::Aborted;
        Ok(())
    }
    fn freeze_unknown(&mut self) {
        self.frozen = true;
        for entry in self.entries.values_mut() {
            if entry.status != Status::Aborted {
                entry.status = Status::Unknown;
            }
        }
    }
}
fn product(p: Product) -> Result<ex::Product> {
    match p {
        Product::Spot => Ok(ex::Product::Spot),
        Product::LinearPerpetual | Product::LinearFuture => Ok(ex::Product::UsdM),
        _ => Err(Error::Identity),
    }
}
fn validate_config(c: &Config) -> Result<()> {
    if c.writer_epoch == 0
        || c.max_intents == 0
        || c.max_intents > 1_000_000
        || c.route.venue != c.business.domain.venue
        || c.route.account != c.business.domain.account
        || product(c.route.product)? != c.business.domain.product
    {
        return Err(Error::Identity);
    }
    Ok(())
}
fn validate_binding(
    config: &Config,
    record: DurableRecord,
    command: &CanonicalCommand,
    fresh: bool,
) -> Result<()> {
    let c = &command.command().context.request;
    let r = record.request;
    let route = &config.route;
    if c.risk_domain_id != config.business.physical.domain
        || c.venue != route.venue
        || c.account != route.account
        || c.environment != route.environment
        || c.product != route.product
        || r.key.domain() != config.business.domain
        || product(c.product)? != r.key.product
        || c.intent_id != r.key.intent
        || c.intent_exec_epoch != r.epoch
        || c.strategy_generation != r.strategy_generation
        || c.config_revision != r.config_revision
        || c.deadline != r.deadline
        || c.recovery_object != r.recovery_object
        || command.digest() != r.command_digest
        || record.sequence == 0
        || r.epoch > config.writer_epoch
        || (fresh && r.epoch != config.writer_epoch)
    {
        return Err(Error::Identity);
    }
    if r.quantity == 0
        || r.unit_budget <= 0
        || r.fee_budget < 0
        || r.commit_profile != ex::CommitProfile::LocalDurable
        || i128::from(r.quantity)
            .checked_mul(r.unit_budget)
            .and_then(|b| b.checked_add(r.fee_budget))
            .is_none()
    {
        return Err(Error::Invalid);
    }
    Ok(())
}
fn permits(g: Gate, record: DurableRecord, writer_epoch: u64) -> bool {
    let r = record.request;
    g.enabled
        && g.market_valid
        && g.account_valid
        && g.recovered
        && g.owner_isolated
        && g.now < g.lease_until
        && g.now < r.deadline
        && g.epoch == writer_epoch
        && g.epoch == r.epoch
        && g.strategy_generation == r.strategy_generation
        && g.config_revision == r.config_revision
}
fn restore_record(p: codec::PreparedIntent) -> DurableRecord {
    DurableRecord {
        sequence: p.order.intent_seq,
        request: ex::OrderRequest {
            key: ex::OrderKey {
                venue: p.order.venue,
                account: p.order.account,
                product: match p.order.product {
                    codec::Product::Spot => ex::Product::Spot,
                    codec::Product::UsdM => ex::Product::UsdM,
                },
                intent: p.order.intent_id,
            },
            command_digest: p.command_digest,
            quantity: p.quantity,
            unit_budget: p.unit_budget,
            fee_budget: p.fee_budget,
            epoch: p.order.intent_exec_epoch,
            strategy_generation: p.strategy_generation,
            config_revision: p.config_revision,
            deadline: p.deadline,
            recovery_object: p.recovery_object,
            commit_profile: match p.commit_profile {
                codec::CommitProfile::LocalDurable => ex::CommitProfile::LocalDurable,
                codec::CommitProfile::SyncReplica => ex::CommitProfile::SyncReplica,
            },
        },
    }
}

impl EncodedRequest {
    pub fn command_digest(&self) -> [u8; 32] {
        self.digest
    }
    pub fn authority(&self) -> &str {
        &self.authority
    }
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}
