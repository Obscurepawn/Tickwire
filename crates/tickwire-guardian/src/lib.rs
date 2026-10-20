//! Independent, audited Binance query/cancellation owner.
#![forbid(unsafe_code)]
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};
use tickwire_binance::{
    request::{self, Credentials, Market, RequestOptions, Timestamp, Transport},
    response,
};
use tickwire_binance_clock::{BinanceTimeClient, MonotonicClock};
use tickwire_binance_quota::{CommandBinding, Endpoint, TimeBinding};
use tickwire_clock::{ClockDomain, MonoTime, SignedTime, Source, TimeUnit};
use tickwire_fencing::{Action, ExecutionIdentity, Scope, SignedDocument, Verifier};
use tickwire_journal::{PhysicalFrontier, store::Store};
use tickwire_transport::{
    Connection, IoBudget, Limits, PreparedSend, Security, SendState, http, mio, rustls,
};
use tickwire_venue::{CanonicalCommand, CommandBody, Environment, Product};
pub type Hash = [u8; 32];
pub type Result<T> = core::result::Result<T, Error>;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Invalid,
    Identity,
    Denied,
    Busy,
    Capacity,
    AlreadyUsed,
    Audit,
    Clock,
    Transport,
    Encoding,
    Poisoned,
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
#[derive(Clone, Serialize, Deserialize)]
#[serde(
    tag = "mode",
    content = "document",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum Authorization {
    Emergency(SignedDocument),
    Fenced(SignedDocument),
}
impl Authorization {
    fn digest(&self) -> Hash {
        match self {
            Self::Emergency(d) | Self::Fenced(d) => d.digest(),
        }
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub guardian_id: [u8; 16],
    pub scope: Scope,
    pub old_identity: ExecutionIdentity,
    pub audit: tickwire_journal::Config,
    pub writer_epoch: u64,
    pub allowed_instruments: Vec<u64>,
    pub max_requests: usize,
    pub recv_window_micros: u64,
    pub delivery_budget_ns: u64,
    pub request_timeout_ns: u64,
    pub max_response_bytes: usize,
    pub audit_degraded: bool,
}
impl Config {
    fn validate(&self) -> Result<()> {
        self.scope.validate().map_err(|_| Error::Identity)?;
        if self.guardian_id == [0; 16]
            || self.audit.domain != self.scope.risk_domain
            || self.writer_epoch == 0
            || self.max_requests == 0
            || self.max_requests > 4096
            || self.allowed_instruments.is_empty()
            || self.allowed_instruments.len() > 4096
            || self.allowed_instruments.contains(&0)
            || self.allowed_instruments.windows(2).any(|w| w[0] >= w[1])
            || self.recv_window_micros == 0
            || self.recv_window_micros > 60_000_000
            || self.delivery_budget_ns == 0
            || self.request_timeout_ns == 0
            || self.request_timeout_ns > 60_000_000_000
            || self.max_response_bytes == 0
            || self.max_response_bytes > 1024 * 1024
            || !self.audit_degraded
        {
            return Err(Error::Invalid);
        }
        Ok(())
    }
    fn binding(&self) -> Result<Vec<u8>> {
        let mut copy = self.clone();
        copy.writer_epoch = 1;
        serde_json::to_vec(&copy).map_err(|_| Error::Encoding)
    }
    fn command(&self, command: &CanonicalCommand) -> Result<Action> {
        let c = command.command();
        let r = &c.context.request;
        if r.venue != self.scope.venue
            || r.account != self.scope.account
            || r.risk_domain_id != self.scope.risk_domain
            || product_code(r.product) != Some(self.scope.product)
            || environment_code(r.environment) != self.scope.environment
            || !self
                .allowed_instruments
                .contains(&c.context.instrument.instrument_id)
        {
            return Err(Error::Identity);
        }
        action(&c.body)
    }
}
pub struct Network {
    pub address: SocketAddr,
    pub tls: Arc<rustls::ClientConfig>,
    pub limits: Limits,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResultKind {
    Unknown,
    NoBytes,
    Response,
    Rejected,
    NotFound,
    RateLimited,
    Authentication,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Outcome {
    pub kind: ResultKind,
    pub status: u16,
    pub accepted_bytes: usize,
    pub request_bytes: usize,
    pub raw_response: Vec<u8>,
    pub raw_digest: Hash,
    pub received_ns: u64,
    pub audit_degraded: bool,
}
pub struct PollResult {
    pub command_digest: Hash,
    pub outcome: Outcome,
}
struct Entry {
    command: CanonicalCommand,
    authorization: Authorization,
    outcome: Option<Outcome>,
}
struct Active {
    digest: Hash,
    connection: Connection,
    decoder: http::ResponseDecoder,
    raw: Vec<u8>,
    stamp: SignedTime,
    deadline: Instant,
    deadline_ns: u64,
    ended: bool,
    invalid: bool,
    clock_failed: bool,
}
pub struct Guardian<S: Store, C: MonotonicClock> {
    config: Config,
    verifier: Verifier,
    clock: BinanceTimeClient<C>,
    quota: Endpoint,
    network: Network,
    audit: audit::Audit<S>,
    entries: BTreeMap<Hash, Entry>,
    active: Option<Active>,
    next_epoch: u64,
    last: Option<tickwire_binance_clock::LocalTime>,
    poisoned: bool,
    quota_started: bool,
}
mod audit;
pub mod cli;
impl<S: Store, C: MonotonicClock> Guardian<S, C> {
    pub fn create(
        store: S,
        config: Config,
        verifier: Verifier,
        clock: BinanceTimeClient<C>,
        quota: Endpoint,
        network: Network,
    ) -> Result<Self> {
        config.validate()?;
        let audit = audit::Audit::create(store, &config)?;
        Self::from_parts(config, verifier, clock, quota, network, audit)
    }
    pub fn recover(
        store: S,
        config: Config,
        verifier: Verifier,
        clock: BinanceTimeClient<C>,
        quota: Endpoint,
        network: Network,
        required: Option<&PhysicalFrontier>,
    ) -> Result<Self> {
        config.validate()?;
        let audit = audit::Audit::recover(store, &config, required)?;
        Self::from_parts(config, verifier, clock, quota, network, audit)
    }
    fn from_parts(
        config: Config,
        verifier: Verifier,
        clock: BinanceTimeClient<C>,
        quota: Endpoint,
        network: Network,
        audit: audit::Audit<S>,
    ) -> Result<Self> {
        if clock.source() != source(&config.scope)?
            || clock.domain() != quota.consumer().clock_domain()
            || network.tls.enable_early_data
        {
            return Err(Error::Identity);
        }
        let mut entries: BTreeMap<Hash, Entry> = BTreeMap::new();
        let mut identities = BTreeMap::new();
        let mut next_epoch = 1u64;
        for event in audit.events().iter().skip(1) {
            match event {
                audit::Event::Start {
                    command,
                    authorization,
                    connection_epoch,
                    started_ns,
                    clock_boot_id,
                    clock_generation,
                    wire_digest,
                } => {
                    let command = CanonicalCommand::decode(command).map_err(|_| Error::Audit)?;
                    config.command(&command)?;
                    let r = &command.command().context.request;
                    let historical = MonoTime::new(
                        ClockDomain::new(*clock_boot_id, *clock_generation)
                            .map_err(|_| Error::Identity)?,
                        *started_ns,
                    );
                    let used = entries
                        .values()
                        .filter(|e| e.authorization.digest() == authorization.digest())
                        .count();
                    permission(
                        &config,
                        &verifier,
                        &command,
                        authorization,
                        historical,
                        used,
                    )?;
                    if r.deadline <= *started_ns || *wire_digest == [0; 32] {
                        return Err(Error::Audit);
                    }
                    if entries.len() >= config.max_requests
                        || *connection_epoch < next_epoch
                        || identities
                            .insert((r.intent_exec_epoch, r.intent_id), command.digest())
                            .is_some()
                    {
                        return Err(Error::Audit);
                    }
                    next_epoch = connection_epoch.checked_add(1).ok_or(Error::Capacity)?;
                    let entry = Entry {
                        command: command.clone(),
                        authorization: authorization.clone(),
                        outcome: None,
                    };
                    if entries.insert(command.digest(), entry).is_some() {
                        return Err(Error::Audit);
                    }
                }
                audit::Event::Complete {
                    command_digest,
                    outcome,
                } => {
                    let e = entries.get_mut(command_digest).ok_or(Error::Audit)?;
                    if e.outcome.is_some()
                        || outcome.accepted_bytes > outcome.request_bytes
                        || outcome.raw_response.len() > config.max_response_bytes
                        || hash(&outcome.raw_response) != outcome.raw_digest
                        || (outcome.kind == ResultKind::NoBytes && outcome.accepted_bytes != 0)
                    {
                        return Err(Error::Audit);
                    }
                    e.outcome = Some(outcome.clone());
                }
                audit::Event::QuotaRun { .. } => {}
                audit::Event::Config { .. } => return Err(Error::Audit),
            }
        }
        Ok(Self {
            config,
            verifier,
            clock,
            quota,
            network,
            audit,
            entries,
            active: None,
            next_epoch,
            last: None,
            poisoned: false,
            quota_started: false,
        })
    }
    /// Persist this external allocation's consumer identity before any network use.
    /// Reopening never resets a previously started allocation.
    pub fn start_quota_run(
        &mut self,
        producer: [u8; 16],
        consumer: [u8; 16],
        generation: u64,
    ) -> Result<()> {
        if self.quota_started || self.poisoned {
            return Err(Error::AlreadyUsed);
        }
        let actual = self.quota.consumer().identity();
        if actual.instance != consumer
            || actual.generation != generation
            || self.quota.producer_id() != producer
        {
            return Err(Error::Identity);
        }
        self.audit.start_quota_run(producer, consumer, generation)?;
        self.quota_started = true;
        Ok(())
    }
    pub fn start_calibration(&mut self) -> Result<u64> {
        self.ready()?;
        let binding = self.time_binding();
        self.clock
            .start(&mut self.quota.time(binding))
            .map_err(|_| Error::Clock)
    }
    pub fn poll_calibration(
        &mut self,
        budget: IoBudget,
    ) -> Result<tickwire_binance_clock::PollReport> {
        self.ready()?;
        let binding = self.time_binding();
        self.clock
            .poll(&mut self.quota.time(binding), budget)
            .map_err(|_| Error::Clock)
    }
    pub fn calibration_in_flight(&self) -> bool {
        self.clock.in_flight().is_some()
    }
    pub fn register_clock(&mut self, registry: &mio::Registry, token: mio::Token) -> Result<()> {
        self.clock
            .register(registry, token)
            .map_err(|_| Error::Transport)
    }
    pub fn register(&mut self, registry: &mio::Registry, token: mio::Token) -> Result<()> {
        self.active
            .as_mut()
            .ok_or(Error::Busy)?
            .connection
            .register(registry, token)
            .map_err(|_| Error::Transport)
    }
    pub fn busy(&self) -> bool {
        self.active.is_some()
    }
    pub fn audit_position(&self) -> u64 {
        self.audit.position()
    }
    pub fn outcome(&self, digest: Hash) -> Option<&Outcome> {
        self.entries.get(&digest).and_then(|e| e.outcome.as_ref())
    }
    pub fn contains(&self, digest: Hash) -> bool {
        self.entries.contains_key(&digest)
    }
    pub fn unresolved(&self) -> impl Iterator<Item = Hash> + '_ {
        self.entries
            .iter()
            .filter(|(_, e)| {
                e.outcome
                    .as_ref()
                    .is_none_or(|o| o.kind == ResultKind::Unknown)
            })
            .map(|(h, _)| *h)
    }
    pub fn local_time(&mut self) -> Result<tickwire_binance_clock::LocalTime> {
        self.sample().map(|r| r.local)
    }
    fn ready(&self) -> Result<()> {
        if self.poisoned {
            Err(Error::Poisoned)
        } else if !self.quota_started {
            Err(Error::Denied)
        } else {
            Ok(())
        }
    }
    fn time_binding(&self) -> TimeBinding {
        TimeBinding {
            owner_id: self.config.guardian_id,
            source: self.clock.source(),
            domain: self.clock.domain(),
        }
    }
    fn command_binding(&self) -> Result<CommandBinding> {
        Ok(CommandBinding {
            owner_id: self.config.guardian_id,
            risk_domain_id: self.config.scope.risk_domain,
            account: self.config.scope.account,
            product: product(self.config.scope.product)?,
            environment: environment(self.config.scope.environment)?,
            domain: self.clock.domain(),
        })
    }
    fn sample(&mut self) -> Result<tickwire_binance_clock::ClockReading> {
        let reading = self.clock.sample().map_err(|_| Error::Clock)?;
        ordered(&mut self.last, reading.local)?;
        Ok(reading)
    }
    fn used(&self, authorization: &Authorization) -> usize {
        self.entries
            .values()
            .filter(|e| e.authorization.digest() == authorization.digest())
            .count()
    }
    fn permission(
        &self,
        command: &CanonicalCommand,
        authorization: &Authorization,
        now: MonoTime,
        used: usize,
    ) -> Result<()> {
        permission(
            &self.config,
            &self.verifier,
            command,
            authorization,
            now,
            used,
        )
    }
    pub fn submit(
        &mut self,
        command: CanonicalCommand,
        authorization: Authorization,
        credentials: &Credentials<'_>,
    ) -> Result<Hash> {
        self.ready()?;
        let digest = command.digest();
        // Caller can retrieve the durable result; it cannot reenroll old requests.
        if let Some(old) = self.entries.get(&digest) {
            if old.command == command {
                return Ok(digest);
            }
            return Err(Error::Identity);
        }
        if self.active.is_some() {
            return Err(Error::Busy);
        }
        if self.entries.len() >= self.config.max_requests {
            return Err(Error::Capacity);
        }
        let r = &command.command().context.request;
        if self.entries.values().any(|e| {
            let p = &e.command.command().context.request;
            (p.intent_exec_epoch, p.intent_id) == (r.intent_exec_epoch, r.intent_id)
        }) {
            return Err(Error::Identity);
        }
        self.config.command(&command)?;
        let before = self.sample()?;
        let stamp = before.signed.ok_or(Error::Clock)?;
        self.permission(
            &command,
            &authorization,
            before.local.monotonic,
            self.used(&authorization),
        )?;
        if before.local.monotonic.nanos() >= r.deadline
            || credentials.account != self.config.scope.account
            || credentials.environment != environment(self.config.scope.environment)?
            || credentials.market != market(&self.config.scope)
        {
            return Err(Error::Identity);
        }
        let prepared = request::prepare(
            &command,
            credentials,
            RequestOptions {
                transport: Transport::Rest,
                timestamp: timestamp(stamp),
                recv_window_micros: self.config.recv_window_micros,
                request_id: "guardian",
            },
            request::MAX_REQUEST_BYTES,
        )
        .map_err(|_| Error::Encoding)?;
        let bytes = http::encode_request(
            prepared.method().as_str(),
            prepared.path(),
            &[
                ("Host", prepared.authority()),
                (
                    "X-MBX-APIKEY",
                    prepared.api_key_header().ok_or(Error::Encoding)?,
                ),
                ("Connection", "close"),
            ],
            prepared.payload(),
            self.network.limits.max_send_bytes,
        )
        .map_err(|_| Error::Encoding)?;
        let epoch = self.next_epoch;
        self.next_epoch = epoch.checked_add(1).ok_or(Error::Capacity)?;
        let now = before.local.monotonic.nanos();
        let deadline_ns = now
            .checked_add(self.config.request_timeout_ns)
            .ok_or(Error::Capacity)?
            .min(r.deadline);
        let deadline = before
            .local
            .instant
            .checked_add(Duration::from_nanos(
                deadline_ns.checked_sub(now).ok_or(Error::Clock)?,
            ))
            .ok_or(Error::Capacity)?;
        let event = audit::Event::Start {
            command: command.bytes().to_vec(),
            authorization: authorization.clone(),
            wire_digest: hash(&bytes),
            connection_epoch: epoch,
            started_ns: now,
            clock_boot_id: self.clock.domain().boot_id(),
            clock_generation: self.clock.domain().generation(),
        };
        if let Err(error) = self.audit.append(event) {
            self.poisoned = true;
            return Err(error);
        }
        self.entries.insert(
            digest,
            Entry {
                command,
                authorization,
                outcome: None,
            },
        );
        let start = (|| {
            let binding = self.command_binding()?;
            let current = self.sample()?.local;
            self.quota
                .consume_guardian_connect(&binding, epoch, current.monotonic)
                .map_err(|_| Error::Denied)?;
            if current.instant >= deadline || current.monotonic.nanos() >= deadline_ns {
                return Err(Error::Denied);
            }
            let mut connection = Connection::connect(
                self.network.address,
                Security::Tls {
                    server_name: authority(&self.config.scope)
                        .to_owned()
                        .try_into()
                        .map_err(|_| Error::Identity)?,
                    config: self.network.tls.clone(),
                },
                self.network.limits,
                deadline,
            )
            .map_err(|_| Error::Transport)?;
            connection
                .queue(
                    PreparedSend::new(&bytes, deadline, self.network.limits.max_send_bytes)
                        .map_err(|_| Error::Encoding)?,
                )
                .map_err(|_| Error::Transport)?;
            Ok(Active {
                digest,
                connection,
                decoder: http::ResponseDecoder::new(self.config.max_response_bytes, 64)
                    .map_err(|_| Error::Capacity)?,
                raw: Vec::new(),
                stamp,
                deadline,
                deadline_ns,
                ended: false,
                invalid: false,
                clock_failed: false,
            })
        })();
        match start {
            Ok(active) => self.active = Some(active),
            Err(_) => {
                let outcome = Outcome {
                    kind: ResultKind::NoBytes,
                    status: 0,
                    accepted_bytes: 0,
                    request_bytes: bytes.len(),
                    raw_response: vec![],
                    raw_digest: hash(&[]),
                    received_ns: now,
                    audit_degraded: self.config.audit_degraded,
                };
                self.complete(digest, outcome)?;
            }
        }
        Ok(digest)
    }
    fn complete(&mut self, digest: Hash, outcome: Outcome) -> Result<()> {
        if let Err(error) = self.audit.append(audit::Event::Complete {
            command_digest: digest,
            outcome: outcome.clone(),
        }) {
            self.poisoned = true;
            return Err(error);
        }
        self.entries
            .get_mut(&digest)
            .ok_or(Error::Identity)?
            .outcome = Some(outcome);
        Ok(())
    }
    pub fn poll(&mut self, budget: IoBudget) -> Result<Option<PollResult>> {
        self.ready()?;
        if budget.max_bytes == 0 || budget.max_syscalls == 0 {
            return Err(Error::Capacity);
        }
        let Some(mut active) = self.active.take() else {
            return Ok(None);
        };
        let result = self.drive(&mut active, budget);
        match result {
            Ok(Some(outcome)) => {
                self.complete(active.digest, outcome.clone())?;
                self.poisoned |= active.clock_failed;
                Ok(Some(PollResult {
                    command_digest: active.digest,
                    outcome,
                }))
            }
            Ok(None) => {
                self.active = Some(active);
                Ok(None)
            }
            Err(error) => {
                // Preserve bounded bytes already read and the acceptance boundary.
                let report = active.connection.send_report();
                let accepted = report.map_or(0, |r| r.accepted_bytes);
                let outcome = Outcome {
                    kind: if accepted == 0 {
                        ResultKind::NoBytes
                    } else {
                        ResultKind::Unknown
                    },
                    status: 0,
                    accepted_bytes: accepted,
                    request_bytes: report.map_or(0, |r| r.total_bytes),
                    raw_digest: hash(&active.raw),
                    raw_response: active.raw,
                    received_ns: self.last.map_or(0, |s| s.monotonic.nanos()),
                    audit_degraded: self.config.audit_degraded,
                };
                self.complete(active.digest, outcome)?;
                if error == Error::Clock {
                    self.poisoned = true;
                }
                Err(error)
            }
        }
    }
    fn drive(&mut self, a: &mut Active, budget: IoBudget) -> Result<Option<Outcome>> {
        let now = match self.sample() {
            Ok(reading) => reading.local,
            Err(_) => {
                // Clock failure disables new IO, but previously received bytes must
                // still be drained and recorded before the owner halts.
                a.clock_failed = true;
                a.ended = true;
                self.last.ok_or(Error::Clock)?
            }
        };
        if !a.ended {
            match a.connection.drive(now.instant, budget) {
                Ok(report) => a.ended = report.peer_closed,
                Err(_) => a.ended = true,
            }
        }
        if !a.ended {
            let entry = self.entries.get(&a.digest).ok_or(Error::Identity)?;
            let binding = self.command_binding()?;
            let used = self
                .used(&entry.authorization)
                .checked_sub(1)
                .ok_or(Error::Identity)?;
            let config = &self.config;
            let verifier = &self.verifier;
            let quota = &mut self.quota;
            let clock = &mut self.clock;
            let last = &mut self.last;
            let mut fault: Option<Error> = None;
            a.connection
                .try_send(now.instant, || {
                    let allowed: Result<bool> = (|| {
                        let initial = clock.sample().map_err(|_| Error::Clock)?.local;
                        ordered(last, initial)?;
                        permission(
                            config,
                            verifier,
                            &entry.command,
                            &entry.authorization,
                            initial.monotonic,
                            used,
                        )?;
                        quota
                            .consume_command(
                                &binding,
                                &entry.command,
                                Transport::Rest,
                                initial.monotonic,
                            )
                            .map_err(|_| Error::Denied)?;
                        // This sample is after every permission/quota check. No
                        // external callback can advance time after it and before write.
                        let validity = authorization_validity(
                            verifier,
                            config,
                            &entry.authorization,
                            initial.monotonic,
                        )?;
                        let final_sample = clock
                            .check_send_sample(
                                &a.stamp,
                                config
                                    .recv_window_micros
                                    .checked_mul(1000)
                                    .ok_or(Error::Capacity)?,
                                config.delivery_budget_ns,
                            )
                            .map_err(|_| Error::Clock)?;
                        ordered(last, final_sample)?;
                        let final_ns = final_sample.monotonic.nanos();
                        Ok(final_sample.monotonic.domain() == a.stamp.domain()
                            && final_sample.instant < a.deadline
                            && final_ns < a.deadline_ns
                            && final_ns < entry.command.command().context.request.deadline
                            && final_ns < validity)
                    })();
                    match allowed {
                        Ok(allow) => allow,
                        Err(error) => {
                            fault = Some(error);
                            false
                        }
                    }
                })
                .ok();
            if let Some(error) = fault
                && error == Error::Clock
            {
                a.clock_failed = true;
                a.ended = true;
            }
        }
        let mut chunk = [0u8; 4096];
        let mut drained = 0usize;
        while drained < budget.max_bytes {
            let limit = chunk.len().min(budget.max_bytes - drained);
            let n = a.connection.read(&mut chunk[..limit]);
            if n == 0 {
                break;
            }
            drained += n;
            if a.raw
                .len()
                .checked_add(n)
                .is_none_or(|n| n > self.config.max_response_bytes)
            {
                a.invalid = true;
                a.ended = true;
                break;
            }
            a.raw.extend_from_slice(&chunk[..n]);
            if a.decoder.push(&chunk[..n]).is_err() {
                a.invalid = true;
                a.ended = true;
                break;
            }
        }
        // Complete buffered replies are consumed before observing final EOF.
        if drained == budget.max_bytes && !a.invalid {
            return Ok(None);
        }
        if a.ended {
            a.decoder.finish();
        }
        let response = a.decoder.next_response();
        let send = a.connection.send_report().ok_or(Error::Transport)?;
        let terminal = matches!(send.state, SendState::NoBytes(_) | SendState::Failed(_));
        if response.as_ref().is_ok_and(Option::is_none)
            && !a.ended
            && !terminal
            && now.monotonic.nanos() < a.deadline_ns
            && now.instant < a.deadline
        {
            return Ok(None);
        }
        let mut status = 0;
        let mut kind = if send.accepted_bytes == 0 {
            ResultKind::NoBytes
        } else {
            ResultKind::Unknown
        };
        if let Ok(Some(reply)) = response {
            status = reply.status;
            if send.accepted_bytes == send.total_bytes
                && send.total_bytes > 0
                && !a.invalid
                && a.decoder.take_buffered().is_empty()
                && let Ok(decoded) =
                    response::decode_rest(market(&self.config.scope), status, &reply.body)
            {
                kind = match decoded.failure {
                    None => ResultKind::Response,
                    Some(response::FailureKind::Rejected) => ResultKind::Rejected,
                    Some(response::FailureKind::NotFound) => ResultKind::NotFound,
                    Some(response::FailureKind::RateLimited | response::FailureKind::Banned) => {
                        ResultKind::RateLimited
                    }
                    Some(response::FailureKind::Authentication) => ResultKind::Authentication,
                    _ => ResultKind::Unknown,
                };
            }
        }
        Ok(Some(Outcome {
            kind,
            status,
            accepted_bytes: send.accepted_bytes,
            request_bytes: send.total_bytes,
            raw_digest: hash(&a.raw),
            raw_response: std::mem::take(&mut a.raw),
            received_ns: self.last.unwrap_or(now).monotonic.nanos(),
            audit_degraded: self.config.audit_degraded,
        }))
    }
}
fn authorization_validity(
    verifier: &Verifier,
    config: &Config,
    authorization: &Authorization,
    now: MonoTime,
) -> Result<u64> {
    match authorization {
        Authorization::Emergency(d) => verifier
            .verify_emergency(d, &config.scope, config.guardian_id, now)
            .map(|c| c.claims().validity.expires_ns)
            .map_err(|_| Error::Denied),
        Authorization::Fenced(d) => verifier
            .verify_fence(d, &config.scope, &config.old_identity, now)
            .map(|c| c.validity().expires_ns)
            .map_err(|_| Error::Denied),
    }
}
fn permission(
    config: &Config,
    verifier: &Verifier,
    command: &CanonicalCommand,
    authorization: &Authorization,
    now: MonoTime,
    used: usize,
) -> Result<()> {
    let action = config.command(command)?;
    match authorization {
        Authorization::Emergency(d) => verifier
            .verify_emergency(d, &config.scope, config.guardian_id, now)
            .map_err(|_| Error::Denied)?
            .allows(
                command.command().context.instrument.instrument_id,
                action,
                command.digest(),
                used,
                now,
            )
            .map_err(|_| Error::Denied),
        Authorization::Fenced(d) => verifier
            .verify_fence(d, &config.scope, &config.old_identity, now)
            .map(|_| ())
            .map_err(|_| Error::Denied),
    }
}
fn action(body: &CommandBody) -> Result<Action> {
    match body {
        CommandBody::Query(_) => Ok(Action::Query),
        CommandBody::Cancel(_) => Ok(Action::Cancel),
        CommandBody::CancelList(_) => Ok(Action::CancelList),
        CommandBody::CancelAlgo(_) => Ok(Action::CancelAlgo),
        _ => Err(Error::Denied),
    }
}
fn hash(bytes: &[u8]) -> Hash {
    Sha256::digest(bytes).into()
}
fn ordered(
    last: &mut Option<tickwire_binance_clock::LocalTime>,
    now: tickwire_binance_clock::LocalTime,
) -> Result<()> {
    if last.is_some_and(|old| {
        old.monotonic.domain() != now.monotonic.domain()
            || old.monotonic.nanos() > now.monotonic.nanos()
            || old.instant > now.instant
    }) {
        return Err(Error::Clock);
    }
    *last = Some(now);
    Ok(())
}
fn product_code(p: Product) -> Option<u8> {
    match p {
        Product::Spot => Some(1),
        Product::LinearPerpetual => Some(2),
        Product::LinearFuture => Some(3),
        _ => None,
    }
}
fn environment_code(e: Environment) -> u8 {
    match e {
        Environment::Production => 1,
        Environment::Test => 2,
    }
}
fn product(code: u8) -> Result<Product> {
    match code {
        1 => Ok(Product::Spot),
        2 => Ok(Product::LinearPerpetual),
        3 => Ok(Product::LinearFuture),
        _ => Err(Error::Identity),
    }
}
fn environment(code: u8) -> Result<Environment> {
    match code {
        1 => Ok(Environment::Production),
        2 => Ok(Environment::Test),
        _ => Err(Error::Identity),
    }
}
fn market(scope: &Scope) -> Market {
    if scope.product == 1 {
        Market::Spot
    } else {
        Market::Usdm
    }
}
fn source(scope: &Scope) -> Result<Source> {
    match (scope.product, scope.environment) {
        (1, 1) => Ok(Source::SpotProduction),
        (1, 2) => Ok(Source::SpotTest),
        (2 | 3, 1) => Ok(Source::UsdmProduction),
        (2 | 3, 2) => Ok(Source::UsdmTest),
        _ => Err(Error::Identity),
    }
}
fn authority(scope: &Scope) -> &'static str {
    match (scope.product, scope.environment) {
        (1, 1) => "api.binance.com",
        (1, _) => "testnet.binance.vision",
        (_, 1) => "fapi.binance.com",
        _ => "demo-fapi.binance.com",
    }
}
fn timestamp(s: SignedTime) -> Timestamp {
    match s.unit() {
        TimeUnit::Milliseconds => Timestamp::Milliseconds(s.timestamp()),
        TimeUnit::Microseconds => Timestamp::Microseconds(s.timestamp()),
    }
}

#[cfg(test)]
#[path = "../../tickwire-binance/tests/common/mod.rs"]
mod fixture;
#[cfg(test)]
mod tests;
