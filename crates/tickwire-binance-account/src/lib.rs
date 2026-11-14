//! Durable private-event input and order identity registration.
#![forbid(unsafe_code)]
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet};
use tickwire_account::{Applied, IntentKey};
use tickwire_account_codec::CanonicalBaseline;
use tickwire_journal::{Journal, PhysicalFrontier, store::Store};
use tickwire_venue::{self as v, CanonicalCommand, CanonicalEvent};
pub mod account_facts;
mod amendment;
mod codec;
pub mod query_desk_driver;
pub mod reconciliation;
mod registry;
pub mod rest;
mod runtime;
mod structure;
pub mod ws;
use codec::Record;
pub use query_desk_driver::{
    PerOrderQueryConfig, PerOrderQueryDriver, PerOrderQueryPermit, PerOrderQueryStep,
    QueryOperation,
};
use registry::{OrderRegistry, Registration};
pub const MAX_RECORD_BYTES: usize = 60 * 1024;

#[derive(Clone)]
pub struct Config {
    pub income_baseline_authority: Option<reconciliation::income_import::IncomeBaselineAuthority>,
    pub income_association_authority:
        Option<reconciliation::income_association::AssociationAuthority>,
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
    AccountBound(&'a v::CanonicalAccountInput),
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
    AccountBound(Box<v::CanonicalAccountInput>),
    Quarantine(QuarantineReason),
}
pub struct AccountIngress<S: Store> {
    journal: Journal<S>,
    config: Config,
    baseline_assets: Vec<v::AssetRef>,
    records: Vec<Record>,
    record_sizes: Vec<usize>,
    record_mono_ns: Vec<u64>,
    evicted_prefix: usize,
    registry: OrderRegistry,
    registered_mono_ns: BTreeMap<IntentKey, u64>,
    evicted_intents: BTreeSet<IntentKey>,
    verified_gateway_intents: RefCell<BTreeSet<IntentKey>>,
    min_gateway_record_count: Cell<u64>,
    min_gateway_ledger_seq: Cell<u64>,
    last_paired_state: Cell<Option<([u8; 32], u64, u64)>>,
    latest_ws_session: Option<ws::SessionRecord>,
    latest_gap_marker: Option<(u64, u64)>,
    last_observed_mono_ns: u64,
    retention_window_ns: Option<u64>,
    retention_window_overridden: bool,
    retained: usize,
    cursor: usize,
    stopped: bool,
    closure_store: Option<Box<dyn reconciliation::income_closure::ClosureStore>>,
    ws_owner: ws::WsOwner,
}
impl<S: Store> AccountIngress<S> {
    /// Consume the input owner and retain its exclusive storage lock without append access.
    pub fn close_for_recovery(self) -> Result<tickwire_journal::ClosedJournal<S>> {
        self.journal.close_for_recovery().map_err(Into::into)
    }
    pub fn export_durable(
        &mut self,
        first_sequence: u64,
        maximum_records: usize,
        maximum_bytes: usize,
    ) -> Result<tickwire_journal::DurableExport> {
        self.journal
            .export_durable(first_sequence, maximum_records, maximum_bytes)
            .map_err(Into::into)
    }
    pub fn with_retention_window_ns(mut self, window_ns: Option<u64>) -> Self {
        self.set_retention_window_ns(window_ns);
        self
    }
    pub fn set_retention_window_ns(&mut self, window_ns: Option<u64>) {
        self.retention_window_ns = window_ns;
        self.retention_window_overridden = true;
    }
    pub fn retention_window_ns(&self) -> Option<u64> {
        self.retention_window_ns
    }
    #[inline]
    pub(crate) fn seq_at(&self, local_idx: usize) -> u64 {
        (self.evicted_prefix + local_idx) as u64 + 1
    }
    #[inline]
    pub(crate) fn local_index(&self, seq: u64) -> Option<usize> {
        usize::try_from(seq)
            .ok()?
            .checked_sub(1)?
            .checked_sub(self.evicted_prefix)
            .filter(|&idx| idx < self.records.len())
    }
    #[inline]
    pub(crate) fn record_at_seq(&self, seq: u64) -> Option<&Record> {
        self.records.get(self.local_index(seq)?)
    }
    #[inline]
    pub(crate) fn local_end(&self, through: u64) -> Result<usize> {
        let n = usize::try_from(through).map_err(|_| Error::Identity)?;
        if n == 0 {
            return Err(Error::Identity);
        }
        let end = n.checked_sub(self.evicted_prefix).ok_or(Error::Identity)?;
        if end == 0 || end > self.records.len() {
            return Err(Error::Identity);
        }
        Ok(end)
    }
    pub(crate) fn observe_mono_ns(&mut self, mono_ns: u64) {
        if mono_ns == 0 {
            return;
        }
        if self.last_observed_mono_ns == 0 {
            for slot in &mut self.record_mono_ns {
                if *slot == 0 {
                    *slot = mono_ns;
                }
            }
            for slot in self.registered_mono_ns.values_mut() {
                if *slot == 0 {
                    *slot = mono_ns;
                }
            }
        }
        self.last_observed_mono_ns = self.last_observed_mono_ns.max(mono_ns);
    }
    pub(crate) fn record_timestamp(&self, record: &Record) -> u64 {
        let ts = match record {
            Record::Register(cmd, _) => cmd.command().context.strategy.created_mono_ns,
            Record::Event(meta, _, _) => meta.received,
            Record::Snapshot(s) => s.received,
            Record::Rest(r) => r.received,
            Record::Income(r) => r.received,
            Record::IncomeImport(r) => r.accepted,
            Record::IncomeAssociation(r) => r.accepted,
            Record::WsResponse(r) => r.received,
            Record::WsSessionEvent(e) => e.received,
            Record::WsBinding(b) => b.command.command().context.strategy.created_mono_ns,
            Record::Config(_)
            | Record::WsSession(_)
            | Record::WsEffect { .. }
            | Record::RestEffect { .. }
            | Record::IncomeEffect { .. }
            | Record::AssociationPolicy(_)
            | Record::Gap { .. }
            | Record::BeginReconciliation { .. } => 0,
        };
        ts.max(self.last_observed_mono_ns)
    }
    pub(crate) fn push_encoded_record(&mut self, record: Record, encoded_len: usize, mono_ns: u64) {
        self.observe_mono_ns(mono_ns);
        let stamp = if mono_ns > 0 {
            mono_ns.max(self.last_observed_mono_ns)
        } else {
            self.record_timestamp(&record)
        };
        self.observe_mono_ns(stamp);
        match &record {
            Record::WsSession(session) => {
                self.latest_ws_session = Some(session.clone());
            }
            Record::BeginReconciliation { epoch, generation }
            | Record::Gap {
                epoch, generation, ..
            } => {
                self.latest_gap_marker = Some(
                    self.latest_gap_marker
                        .map_or((*epoch, *generation), |old| old.max((*epoch, *generation))),
                );
            }
            _ => {}
        }
        self.records.push(record);
        self.record_sizes.push(encoded_len);
        self.record_mono_ns.push(stamp);
        self.last_paired_state.set(None);
    }
}
fn extract_baseline_assets(config: &Config) -> Vec<v::AssetRef> {
    let mut assets = Vec::new();
    for asset in &config.baseline.snapshot().assets {
        if !assets.contains(&asset.asset) {
            assets.push(asset.asset.clone());
        }
    }
    assets
}
impl<S: Store> AccountIngress<S> {
    pub fn create(store: S, config: Config) -> Result<Self> {
        Self::create_from_live_exchange(store, config, &[])
    }
    pub fn create_from_live_exchange(
        store: S,
        config: Config,
        imported: &[reconciliation::live_bootstrap::ImportedOrderRegistration],
    ) -> Result<Self> {
        validate_config(&config)?;
        let baseline_assets = extract_baseline_assets(&config);
        let registry = reconciliation::live_bootstrap::build_imported_registry(&config, imported)?;
        let mut registered_mono_ns = BTreeMap::new();
        for (key, reg) in &registry {
            registered_mono_ns.insert(*key, reg.command.command().context.strategy.created_mono_ns);
        }
        let mut journal = Journal::create(store, config.physical.clone())?;
        let record = Record::Config(codec::config(&config));
        let bytes = codec::encode(&record)?;
        journal.append_batch(config.writer_epoch, &[&bytes])?;
        Ok(Self {
            journal,
            config,
            baseline_assets,
            retained: bytes.len(),
            records: vec![record],
            record_sizes: vec![bytes.len()],
            record_mono_ns: vec![0],
            evicted_prefix: 0,
            registry,
            registered_mono_ns,
            evicted_intents: BTreeSet::new(),
            verified_gateway_intents: RefCell::new(BTreeSet::new()),
            min_gateway_record_count: Cell::new(0),
            min_gateway_ledger_seq: Cell::new(0),
            last_paired_state: Cell::new(None),
            latest_ws_session: None,
            latest_gap_marker: None,
            last_observed_mono_ns: 0,
            retention_window_ns: Some(tickwire_account::DEFAULT_RETENTION_WINDOW_NS),
            retention_window_overridden: false,
            cursor: 1,
            stopped: false,
            closure_store: None,
            ws_owner: ws::WsOwner::new(),
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
        let baseline_assets = extract_baseline_assets(&config);
        let mut input = Self {
            journal,
            config,
            baseline_assets,
            records: Vec::new(),
            record_sizes: Vec::new(),
            record_mono_ns: Vec::new(),
            evicted_prefix: 0,
            registry: OrderRegistry::new(),
            registered_mono_ns: BTreeMap::new(),
            evicted_intents: BTreeSet::new(),
            verified_gateway_intents: RefCell::new(BTreeSet::new()),
            min_gateway_record_count: Cell::new(0),
            min_gateway_ledger_seq: Cell::new(0),
            last_paired_state: Cell::new(None),
            latest_ws_session: None,
            latest_gap_marker: None,
            last_observed_mono_ns: 0,
            retention_window_ns: Some(tickwire_account::DEFAULT_RETENTION_WINDOW_NS),
            retention_window_overridden: false,
            retained: 0,
            cursor: 1,
            stopped: false,
            closure_store: None,
            ws_owner: ws::WsOwner::new(),
        };
        let expected = codec::config(&input.config);
        let mut writer = 0;
        let mut effects: Option<(u64, usize, usize, u8)> = None;
        for physical in recovered.records {
            if physical.seq != input.position() + 1
                || physical.exec_epoch < writer
                || physical.exec_epoch == 0
                || physical.exec_epoch > input.config.writer_epoch
            {
                return Err(Error::Identity);
            }
            writer = physical.exec_epoch;
            let record = codec::decode(&physical.payload)?;
            if effects.is_some()
                && !matches!(
                    record,
                    Record::RestEffect { .. }
                        | Record::IncomeEffect { .. }
                        | Record::WsEffect { .. }
                )
            {
                return Err(Error::Identity);
            }
            match &record {
                Record::Config(bytes) if input.records.is_empty() && *bytes == expected => {}
                Record::Config(_) => return Err(Error::Identity),
                _ if input.records.is_empty() => return Err(Error::Identity),
                Record::WsSessionEvent(r) => input.validate_ws_event(r, physical.exec_epoch)?,
                Record::WsResponse(r) => {
                    input.validate_ws_response(r, physical.exec_epoch)?;
                    if !r.events.is_empty() {
                        effects = Some((physical.seq, 0, r.events.len(), 2));
                    }
                }
                Record::WsEffect { response, index } => {
                    let Some((expected, next, count, 2)) = effects else {
                        return Err(Error::Identity);
                    };
                    if *response != expected
                        || *index as usize != next
                        || physical.seq != expected + next as u64 + 1
                    {
                        return Err(Error::Identity);
                    }
                    let Some(local_idx) = input.local_index(expected) else {
                        return Err(Error::Identity);
                    };
                    let Record::WsResponse(header) = &input.records[local_idx] else {
                        return Err(Error::Identity);
                    };
                    registry::learn(&mut input.registry, &header.events[next])?;
                    effects = if next + 1 == count {
                        None
                    } else {
                        Some((expected, next + 1, count, 2))
                    };
                }
                Record::WsBinding(binding) => {
                    input.validate_ws_binding(binding, physical.seq, physical.exec_epoch)?;
                }
                Record::WsSession(session) => {
                    input.validate_ws_session(session, physical.seq, physical.exec_epoch)?;
                }
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
                    input.registered_mono_ns.insert(
                        key,
                        command
                            .command()
                            .context
                            .strategy
                            .created_mono_ns
                            .max(input.last_observed_mono_ns),
                    );
                    input.registry.insert(key, reg);
                }
                Record::Event(meta, raw, decision) => {
                    if meta.epoch == 0 || raw.len() > input.config.max_raw_bytes {
                        return Err(Error::Identity);
                    }
                    if let Decision::Bound(event) = decision {
                        input.check_event(meta, event)?;
                        if matches!(event.event().body, v::VenueEventBody::Structure(_)) {
                            input.check_structure_raw(meta, raw, event)?;
                        }
                        if matches!(event.event().body, v::VenueEventBody::Amendment(_)) {
                            input.check_amendment_raw(meta, raw, event)?;
                        }
                        registry::learn(&mut input.registry, event)?;
                    } else if let Decision::AccountBound(event) = decision {
                        input.check_account_event(meta, raw, event)?;
                    }
                }
                Record::Snapshot(record) => {
                    if record.raw.len() > input.config.max_raw_bytes {
                        return Err(Error::Capacity);
                    }
                }
                Record::Rest(record) => {
                    input.validate_rest(record)?;
                    if record.command.command().context.request.intent_exec_epoch
                        > physical.exec_epoch
                    {
                        return Err(Error::Identity);
                    }
                    if !record.events.is_empty() {
                        effects = Some((physical.seq, 0, record.events.len(), 0));
                    }
                }
                Record::IncomeAssociation(record) => {
                    input.validate_association_record(record, physical.seq, physical.exec_epoch)?;
                }
                Record::AssociationPolicy(policy) => {
                    input.validate_association_policy(policy, physical.exec_epoch)?;
                }
                Record::IncomeImport(record) => {
                    input.validate_import(record, physical.seq, physical.exec_epoch)?;
                }
                Record::Income(record) => {
                    input.validate_income(record)?;
                    if record.query.purpose == reconciliation::income::IncomeUse::Account
                        && !record.rows.is_empty()
                    {
                        effects = Some((physical.seq, 0, record.rows.len(), 1));
                    }
                }
                Record::IncomeEffect { response, index } => {
                    let Some((expected, next, count, 1)) = effects else {
                        return Err(Error::Identity);
                    };
                    if *response != expected
                        || *index as usize != next
                        || physical.seq != expected + next as u64 + 1
                    {
                        return Err(Error::Identity);
                    }
                    effects = if next + 1 == count {
                        None
                    } else {
                        Some((expected, next + 1, count, 1))
                    };
                }
                Record::RestEffect { response, index } => {
                    let Some((expected, next, count, 0)) = effects else {
                        return Err(Error::Identity);
                    };
                    if *response != expected
                        || *index as usize != next
                        || physical.seq != expected + next as u64 + 1
                    {
                        return Err(Error::Identity);
                    }
                    let Some(local_idx) = input.local_index(expected) else {
                        return Err(Error::Identity);
                    };
                    let Record::Rest(header) = &input.records[local_idx] else {
                        return Err(Error::Identity);
                    };
                    registry::learn(&mut input.registry, &header.events[next])?;
                    effects = if next + 1 == count {
                        None
                    } else {
                        Some((expected, next + 1, count, 0))
                    };
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
            input.push_encoded_record(record, physical.payload.len(), 0);
        }
        if effects.is_some() {
            return Err(Error::Identity);
        }
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
        let created_mono = command
            .command()
            .context
            .strategy
            .created_mono_ns
            .max(self.last_observed_mono_ns);
        self.append(Record::Register(command, ids.to_vec()))?;
        self.registered_mono_ns.insert(key, created_mono);
        self.registry.insert(key, reg);
        self.cursor = self.records.len();
        Ok(())
    }
    pub fn registered(&self, key: IntentKey) -> Option<&CanonicalCommand> {
        self.registry.get(&key).map(|r| &r.command)
    }
    pub fn position(&self) -> u64 {
        (self.evicted_prefix + self.records.len()) as u64
    }
    pub fn applied_position(&self) -> u64 {
        (self.evicted_prefix + self.cursor) as u64
    }
    pub fn input_stream(&self) -> [u8; 16] {
        self.config.physical.stream_epoch
    }
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
                        sequence: self.seq_at(index),
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
                        Decision::AccountBound(event) => StoredDecision::AccountBound(event),
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
        self.push_encoded_record(record, bytes.len(), 0);
        self.retained = retained;
        Ok(InputKey {
            stream: self.config.physical.stream_epoch,
            sequence: self.position(),
        })
    }
    fn check_account_event(
        &self,
        meta: &SourceMeta,
        raw: &[u8],
        event: &v::CanonicalAccountInput,
    ) -> Result<()> {
        let actual = account_facts::decode_observation(
            &self.config.baseline,
            meta,
            raw,
            self.config.max_raw_bytes,
        )?;
        if &actual != event {
            return Err(Error::Identity);
        }
        Ok(())
    }
    pub fn account_inputs(&self) -> impl Iterator<Item = (InputKey, &v::CanonicalAccountInput)> {
        self.records.iter().enumerate().filter_map(|(index, _)| {
            self.account_effect(index).map(|e| {
                (
                    InputKey {
                        stream: self.input_stream(),
                        sequence: self.seq_at(index),
                    },
                    e,
                )
            })
        })
    }
    pub(crate) fn account_effect(&self, index: usize) -> Option<&v::CanonicalAccountInput> {
        match self.records.get(index)? {
            Record::Event(_, _, Decision::AccountBound(event)) => Some(event),
            Record::IncomeEffect { response, index } => match self.record_at_seq(*response)? {
                Record::Income(record) => record.rows.get(*index as usize),
                _ => None,
            },
            _ => None,
        }
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
    if let Some(authority) = &config.income_association_authority {
        authority.validate()?;
    }
    if let Some(authority) = &config.income_baseline_authority {
        authority.validate()?;
    }
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
#[path = "../../tickwire-account-gateway/tests/common/mod.rs"]
mod gateway_fixture;
#[cfg(test)]
use gateway_fixture::fixture as unit_fixture;
