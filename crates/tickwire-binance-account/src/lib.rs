//! Durable private-event input and order identity registration.
#![forbid(unsafe_code)]
use std::collections::BTreeMap;
use tickwire_account::{Applied, IntentKey};
use tickwire_account_codec::CanonicalBaseline;
use tickwire_journal::{Journal, PhysicalFrontier, store::Store};
use tickwire_venue::{self as v, CanonicalCommand, CanonicalEvent};
mod codec;
mod registry;
mod runtime;
pub mod rest;
pub mod reconciliation;
use codec::Record;
use registry::Registration;
pub const MAX_RECORD_BYTES: usize = 60 * 1024;

#[derive(Clone)]
pub struct Config {
    pub physical: tickwire_journal::Config,
    pub writer_epoch: u64,
    pub baseline: CanonicalBaseline,
    pub account_stream: [u8; 16],
    pub max_records: usize,
    pub max_registrations: usize,
    pub max_raw_bytes: usize,
    pub max_retained_bytes: usize,
}
#[derive(Debug)]
pub enum Error {
    Journal(tickwire_journal::Error),
    Gateway(tickwire_account_gateway::Error),
    Encoding,
    Identity,
    Conflict,
    Capacity,
    Unsupported,
    Stopped,
    ReplayPending,
    Busy,
    Transport(tickwire_transport::Error),
    Clock(tickwire_binance_private::Error),
}
pub type Result<T> = std::result::Result<T, Error>;
impl From<tickwire_journal::Error> for Error {
    fn from(error: tickwire_journal::Error) -> Self {
        Self::Journal(error)
    }
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InputKey {
    pub stream: [u8; 16],
    pub sequence: u64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QuarantineReason {
    UnknownOrder,
    AmbiguousOrder,
    UnadmittedOrder,
    BindingConflict,
    AccountRejected,
    AccountReconciliation,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    Applied {
        input: InputKey,
        applied: Applied,
    },
    Quarantined {
        input: InputKey,
        reason: QuarantineReason,
    },
}
#[derive(Clone, Copy, Debug)]
pub enum StoredDecision<'a> {
    Bound(&'a CanonicalEvent),
    Quarantine(QuarantineReason),
}
#[derive(Clone, Copy)]
pub struct StoredEvent<'a> {
    pub input: InputKey,
    pub connection_epoch: u64,
    pub subscription_id: Option<u64>,
    pub time_unit: tickwire_binance::feed::TimeUnit,
    pub received_mono_ns: u64,
    /// Exact event JSON received from the authenticated private session.
    pub raw: &'a [u8],
    pub decision: StoredDecision<'a>,
}
#[derive(Clone)]
struct SourceMeta {
    epoch: u64,
    subscription: Option<u64>,
    micros: bool,
    received: u64,
}
#[derive(Clone)]
enum Decision {
    Bound(Box<CanonicalEvent>),
    Quarantine(QuarantineReason),
}
pub struct AccountIngress<S: Store> {
    journal: Journal<S>,
    config: Config,
    records: Vec<Record>,
    registry: BTreeMap<IntentKey, Registration>,
    retained: usize,
    cursor: usize,
    stopped: bool,
}
impl<S: Store> AccountIngress<S> {
    pub fn create(store: S, config: Config) -> Result<Self> {
        validate_config(&config)?;
        let mut journal = Journal::create(store, config.physical.clone())?;
        let record = Record::Config(codec::config(&config));
        let bytes = codec::encode(&record)?;
        journal.append_batch(config.writer_epoch, &[&bytes])?;
        Ok(Self {
            journal,
            config,
            retained: bytes.len(),
            records: vec![record],
            registry: BTreeMap::new(),
            cursor: 1,
            stopped: false,
        })
    }
    pub fn recover(store: S, config: Config, required: Option<&PhysicalFrontier>) -> Result<Self> {
        validate_config(&config)?;
        if required.is_some_and(|f| {
            f.domain != config.physical.domain || f.stream_epoch != config.physical.stream_epoch
        }) {
            return Err(Error::Identity);
        }
        let (journal, recovered) = Journal::recover(store, required)?;
        if journal.config() != &config.physical
            || recovered.records.is_empty()
            || recovered.records.len() > config.max_records
        {
            return Err(Error::Identity);
        }
        let mut input = Self {
            journal,
            config,
            records: Vec::new(),
            registry: BTreeMap::new(),
            retained: 0,
            cursor: 1,
            stopped: false,
        };
        let expected = codec::config(&input.config);
        let mut writer = 0;
        let mut effects: Option<(u64,usize,usize)> = None;
        for physical in recovered.records {
            if physical.seq != input.records.len() as u64 + 1
                || physical.exec_epoch < writer
                || physical.exec_epoch == 0
                || physical.exec_epoch > input.config.writer_epoch
            {
                return Err(Error::Identity);
            }
            writer = physical.exec_epoch;
            let record = codec::decode(&physical.payload)?;
            if effects.is_some() && !matches!(record,Record::RestEffect{..}) {return Err(Error::Identity);}
            match &record {
                Record::Config(bytes) if input.records.is_empty() && *bytes == expected => {}
                Record::Config(_) => return Err(Error::Identity),
                _ if input.records.is_empty() => return Err(Error::Identity),
                Record::Register(command, ids) => {
                    if command.command().context.request.intent_exec_epoch != physical.exec_epoch {
                        return Err(Error::Identity);
                    }
                    let key = intent(command);
                    if input.registry.contains_key(&key) {
                        return Err(Error::Conflict);
                    }
                    let reg =
                        registry::prepare(&input.config, &input.registry, command, ids, false)?;
                    input.registry.insert(key, reg);
                }
                Record::Event(meta, raw, decision) => {
                    if meta.epoch == 0 || raw.len() > input.config.max_raw_bytes {
                        return Err(Error::Identity);
                    }
                    if let Decision::Bound(event) = decision {
                        input.check_event(meta, event)?;
                        registry::learn(&mut input.registry, event)?;
                    }
                }
                Record::Snapshot(record) => {if record.raw.len()>input.config.max_raw_bytes {return Err(Error::Capacity);}}
                Record::Rest(record) => {
                    input.validate_rest(record)?;
                    if record.command.command().context.request.intent_exec_epoch>physical.exec_epoch {return Err(Error::Identity);}
                    if !record.events.is_empty() {effects=Some((physical.seq,0,record.events.len()));}
                }
                Record::RestEffect{response,index} => {
                    let Some((expected,next,count))=effects else{return Err(Error::Identity);};
                    if *response!=expected || *index as usize!=next || physical.seq!=expected+next as u64+1 {return Err(Error::Identity);}
                    let Some(Record::Rest(header))=input.records.get(expected as usize-1) else{return Err(Error::Identity);};
                    registry::learn(&mut input.registry,&header.events[next])?;
                    effects=if next+1==count{None}else{Some((expected,next+1,count))};
                }
                // Epoch zero is the authenticated owner's initial disconnected
                // gap, before its first network connection has been assigned.
                Record::Gap { generation, .. } if *generation == 0 => {
                    return Err(Error::Identity);
                }
                Record::Gap { .. } | Record::BeginReconciliation { .. } => {}
            }
            input.retained = input
                .retained
                .checked_add(physical.payload.len())
                .ok_or(Error::Capacity)?;
            if input.retained > input.config.max_retained_bytes {
                return Err(Error::Capacity);
            }
            input.records.push(record);
        }
        if effects.is_some() {return Err(Error::Identity);}
        Ok(input)
    }
    pub fn register_command(&mut self, command: CanonicalCommand, ids: &[[u8; 16]]) -> Result<()> {
        if self.stopped {
            return Err(Error::Stopped);
        }
        if self.cursor != self.records.len() {
            return Err(Error::ReplayPending);
        }
        let key = intent(&command);
        if let Some(old) = self.registry.get(&key) {
            if old.command.bytes() == command.bytes() && old.ids == ids {
                return Ok(());
            }
            self.stopped = true;
            return Err(Error::Conflict);
        }
        let reg = match registry::prepare(&self.config, &self.registry, &command, ids, true) {
            Ok(reg) => reg,
            Err(Error::Conflict) => {
                self.stopped = true;
                return Err(Error::Conflict);
            }
            Err(error) => return Err(error),
        };
        self.append(Record::Register(command, ids.to_vec()))?;
        self.registry.insert(key, reg);
        self.cursor = self.records.len();
        Ok(())
    }
    pub fn registered(&self, key: IntentKey) -> Option<&CanonicalCommand> {
        self.registry.get(&key).map(|r| &r.command)
    }
    pub fn position(&self) -> u64 {
        self.records.len() as u64
    }
    pub fn applied_position(&self) -> u64 { self.cursor as u64 }
    pub fn input_stream(&self) -> [u8;16] { self.config.physical.stream_epoch }
    pub fn stopped(&self) -> bool {
        self.stopped
    }
    pub fn quarantined(&self) -> usize {
        self.records
            .iter()
            .filter(|r| {
                matches!(
                    r,
                    Record::Event(_, _, Decision::Quarantine(_)) | Record::Gap { .. }
                )
            })
            .count()
    }
    /// Durable observations in input-WAL order, including quarantined facts.
    /// No mutable account state or authentication capability is exposed.
    pub fn stored_events(&self) -> impl Iterator<Item = StoredEvent<'_>> {
        self.records
            .iter()
            .enumerate()
            .filter_map(|(index, record)| {
                let Record::Event(meta, raw, decision) = record else {
                    return None;
                };
                Some(StoredEvent {
                    input: InputKey {
                        stream: self.config.physical.stream_epoch,
                        sequence: index as u64 + 1,
                    },
                    connection_epoch: meta.epoch,
                    subscription_id: meta.subscription,
                    time_unit: if meta.micros {
                        tickwire_binance::feed::TimeUnit::Microseconds
                    } else {
                        tickwire_binance::feed::TimeUnit::Milliseconds
                    },
                    received_mono_ns: meta.received,
                    raw,
                    decision: match decision {
                        Decision::Bound(event) => StoredDecision::Bound(event),
                        Decision::Quarantine(reason) => StoredDecision::Quarantine(*reason),
                    },
                })
            })
    }
    fn append(&mut self, record: Record) -> Result<InputKey> {
        if self.stopped {
            return Err(Error::Stopped);
        }
        let bytes = codec::encode(&record)?;
        let retained = self
            .retained
            .checked_add(bytes.len())
            .ok_or(Error::Capacity)?;
        if self.records.len() >= self.config.max_records
            || retained > self.config.max_retained_bytes
        {
            self.stopped = true;
            return Err(Error::Capacity);
        }
        if let Err(error) = self
            .journal
            .append_batch(self.config.writer_epoch, &[&bytes])
        {
            self.stopped = true;
            return Err(error.into());
        }
        self.records.push(record);
        self.retained = retained;
        Ok(InputKey {
            stream: self.config.physical.stream_epoch,
            sequence: self.position(),
        })
    }
    fn check_event(&self, meta: &SourceMeta, event: &CanonicalEvent) -> Result<()> {
        let id = &event.event().identity;
        let scope = &self.config.baseline.snapshot().scope;
        if id.risk_domain_id != scope.risk_domain_id
            || id.venue != scope.venue
            || id.environment != scope.environment
            || id.account != scope.account
            || id.product != product(&self.config)?
            || id.source_connection_epoch != meta.epoch
            || id.received_mono_ns != meta.received
        {
            return Err(Error::Identity);
        }
        Ok(())
    }
}
fn intent(command: &CanonicalCommand) -> IntentKey {
    let request = &command.command().context.request;
    IntentKey {
        epoch: request.intent_exec_epoch,
        intent_id: request.intent_id,
    }
}
fn product(config: &Config) -> Result<v::Product> {
    let p = config
        .baseline
        .catalog()
        .entries
        .first()
        .ok_or(Error::Identity)?
        .product;
    if config
        .baseline
        .catalog()
        .entries
        .iter()
        .any(|e| e.product != p)
    {
        return Err(Error::Identity);
    }
    Ok(p)
}
fn validate_config(config: &Config) -> Result<()> {
    if config.physical.domain != config.baseline.snapshot().scope.risk_domain_id
        || config.account_stream == [0; 16]
        || config.account_stream == config.physical.stream_epoch
        || config.writer_epoch == 0
        || config.writer_epoch >= 1 << 63
        || config.max_records < 2
        || config.max_records > 65_536
        || config.max_registrations == 0
        || config.max_registrations > tickwire_account::MAX_ORDERS
        || config.max_raw_bytes == 0
        || config.max_raw_bytes > 32 * 1024
        || config.max_retained_bytes < MAX_RECORD_BYTES
        || config.max_retained_bytes > 256 * 1024 * 1024
    {
        return Err(Error::Capacity);
    }
    if !matches!(
        product(config)?,
        v::Product::Spot | v::Product::LinearFuture | v::Product::LinearPerpetual
    ) {
        return Err(Error::Unsupported);
    }
    Ok(())
}

#[cfg(test)]
#[path = "../../tickwire-account/tests/common/mod.rs"]
mod unit_fixture;
