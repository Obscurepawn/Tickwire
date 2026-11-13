//! Ordered account persistence; receipts are durability evidence, not trading authority.
#![forbid(unsafe_code)]
use std::{
    collections::{BTreeMap, BTreeSet},
    io,
};
use tickwire_account::{Account, IntentKey, OrderKey, RecoveryRecord, Scope};
use tickwire_account_codec::{CanonicalBaseline, CanonicalRecord, Hash};
use tickwire_journal::{Journal, PhysicalFrontier, store::Store};
mod backend;
mod history;
mod identity;
pub use backend::{AccountBackend, FileBackend, InMemoryBackend, MmapAsyncBackend};
pub use history::HistoryFrontier;
/// Exact immutable bootstrap bytes for dependency publication before journal creation.
pub fn bootstrap_bytes(config: &Config) -> Result<Vec<u8>> {
    identity::bootstrap(config)
}

pub const MAX_RECORDS: usize = tickwire_account::MAX_FACTS + tickwire_account::MAX_ORDERS;
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub physical: tickwire_journal::Config,
    pub baseline_digest: Hash,
    pub initial_writer_epoch: u64,
    pub scope: Scope,
}
#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Physical(tickwire_journal::Error),
    Codec(tickwire_account_codec::Error),
    Account(tickwire_account::Error),
    Verification(VerificationError),
    Identity,
    Sequence,
    Epoch,
    AlreadyExists,
    Conflict,
    Capacity,
    Poisoned,
    Recovered,
}
pub type Result<T> = std::result::Result<T, Error>;
impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}
impl From<tickwire_journal::Error> for Error {
    fn from(e: tickwire_journal::Error) -> Self {
        Self::Physical(e)
    }
}
impl From<tickwire_account_codec::Error> for Error {
    fn from(e: tickwire_account_codec::Error) -> Self {
        Self::Codec(e)
    }
}
impl From<tickwire_account::Error> for Error {
    fn from(e: tickwire_account::Error) -> Self {
        Self::Account(e)
    }
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerificationError {
    Missing,
    Invalid,
    Unavailable,
}
pub trait RecoveryVerifier {
    fn verify_uncreated_algo(
        &mut self,
        _baseline: &CanonicalBaseline,
        _record: &CanonicalRecord,
        _preceding: &HistoryFrontier,
        _prior: &[CanonicalRecord],
        _writer_epoch: u64,
    ) -> std::result::Result<(), VerificationError> {
        Err(VerificationError::Invalid)
    }
    fn verify_income_association(
        &mut self,
        _baseline: &CanonicalBaseline,
        _record: &CanonicalRecord,
        _preceding: &HistoryFrontier,
        _prior: &[CanonicalRecord],
        _writer_epoch: u64,
    ) -> std::result::Result<(), VerificationError> {
        Err(VerificationError::Invalid)
    }
    fn verify_account_inputs_closure(
        &mut self,
        _baseline: &CanonicalBaseline,
        _record: &CanonicalRecord,
        _preceding: &HistoryFrontier,
        _prior: &[CanonicalRecord],
        _writer_epoch: u64,
    ) -> std::result::Result<(), VerificationError> {
        Err(VerificationError::Invalid)
    }

    fn verify_income_baseline(
        &mut self,
        _baseline: &CanonicalBaseline,
        _record: &CanonicalRecord,
        _preceding: &HistoryFrontier,
        _prior: &[CanonicalRecord],
    ) -> std::result::Result<(), VerificationError> {
        Err(VerificationError::Invalid)
    }
    fn verify_handover(
        &mut self,
        _baseline: &CanonicalBaseline,
        _record: &CanonicalRecord,
        _prior: &[CanonicalRecord],
    ) -> std::result::Result<(), VerificationError> {
        Err(VerificationError::Invalid)
    }
    fn verify_reactivation(
        &mut self,
        _baseline: &CanonicalBaseline,
        _record: &CanonicalRecord,
        _preceding: &HistoryFrontier,
        _prior: &[CanonicalRecord],
    ) -> std::result::Result<(), VerificationError> {
        Err(VerificationError::Invalid)
    }

    fn verify_admission(
        &mut self,
        baseline: &CanonicalBaseline,
        record: &CanonicalRecord,
        preceding: &HistoryFrontier,
    ) -> std::result::Result<(), VerificationError>;
}
/// Implementations synchronize immutable account files and honor Store guarantees.
pub trait Backend: Store {
    fn read_bootstrap(&mut self) -> io::Result<Option<Vec<u8>>>;
    fn publish_bootstrap(&mut self, bytes: &[u8]) -> io::Result<()>;
    fn read_baseline(&mut self) -> io::Result<Option<Vec<u8>>>;
    fn publish_baseline(&mut self, bytes: &[u8]) -> io::Result<()>;
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendReceipt {
    baseline: Hash,
    sequence: u64,
    digest: Hash,
    physical: PhysicalFrontier,
}
impl AppendReceipt {
    pub fn baseline_digest(&self) -> Hash {
        self.baseline
    }
    pub fn sequence(&self) -> u64 {
        self.sequence
    }
    pub fn record_digest(&self) -> Hash {
        self.digest
    }
    pub fn physical_frontier(&self) -> &PhysicalFrontier {
        &self.physical
    }
}
/// Original admission record and its post-record history, reconstructed from actual WAL bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmissionOrigin {
    receipt: AppendReceipt,
    history: HistoryFrontier,
}
impl AdmissionOrigin {
    pub fn receipt(&self) -> &AppendReceipt {
        &self.receipt
    }
    pub fn history(&self) -> &HistoryFrontier {
        &self.history
    }
}
enum AdmissionSlot {
    InRecords(usize, AppendReceipt),
    Retained(Box<CanonicalRecord>, AppendReceipt),
}

pub struct AccountJournal<S: Backend> {
    physical: Journal<S>,
    config: Config,
    baseline: CanonicalBaseline,
    writer_epoch: u64,
    evicted_prefix: usize,
    records: Vec<CanonicalRecord>,
    admissions: BTreeMap<IntentKey, AdmissionSlot>,
    admission_origins: BTreeMap<IntentKey, AdmissionOrigin>,
    closures: BTreeMap<Hash, (usize, AppendReceipt)>,
    associations: BTreeMap<[u8; 16], (usize, AppendReceipt)>,
    ingress: BTreeMap<([u8; 16], u64), (usize, AppendReceipt)>,
    retention_window_ns: Option<u64>,
    last_mono_ns: u64,
    intent_open_legs: BTreeMap<IntentKey, BTreeSet<OrderKey>>,
    retired_intents: BTreeMap<IntentKey, u64>,
    poisoned: bool,
    recovered: bool,
    recovery_writer_floor: Option<u64>,
    history: HistoryFrontier,
}
pub struct Recovered<S: Backend> {
    pub journal: AccountJournal<S>,
    pub account: Account,
    pub discarded_tail_bytes: u64,
}
impl<S: Backend> AccountJournal<S> {
    pub fn export_durable(
        &mut self,
        first_sequence: u64,
        maximum_records: usize,
        maximum_bytes: usize,
    ) -> Result<tickwire_journal::DurableExport> {
        self.physical
            .export_durable(first_sequence, maximum_records, maximum_bytes)
            .map_err(Into::into)
    }
    pub fn create(mut backend: S, config: Config, baseline: CanonicalBaseline) -> Result<Self> {
        identity::config(&config, &baseline)?;
        if backend.read_bootstrap()?.is_some()
            || backend.read_baseline()?.is_some()
            || backend.read_manifest()?.is_some()
            || !backend.segment_ids()?.is_empty()
        {
            return Err(Error::AlreadyExists);
        }
        let bootstrap = identity::bootstrap(&config)?;
        // Dependency first; a partial create remains unavailable and is never overwritten.
        backend.publish_baseline(baseline.bytes())?;
        backend.publish_bootstrap(&bootstrap)?;
        let physical = Journal::create(backend, config.physical.clone())?;
        let history = HistoryFrontier::empty(&config)?;
        Ok(Self {
            physical,
            writer_epoch: config.initial_writer_epoch,
            config,
            baseline,
            evicted_prefix: 0,
            records: Vec::new(),
            admissions: BTreeMap::new(),
            admission_origins: BTreeMap::new(),
            ingress: BTreeMap::new(),
            closures: BTreeMap::new(),
            associations: BTreeMap::new(),
            retention_window_ns: None,
            last_mono_ns: 0,
            intent_open_legs: BTreeMap::new(),
            retired_intents: BTreeMap::new(),
            poisoned: false,
            recovered: false,
            recovery_writer_floor: None,
            history,
        })
    }
    pub fn recover(
        mut backend: S,
        expected: Config,
        new_writer_epoch: u64,
        required: Option<&PhysicalFrontier>,
        verifier: &mut impl RecoveryVerifier,
    ) -> Result<Recovered<S>> {
        identity::epoch(new_writer_epoch)?;
        if new_writer_epoch < expected.initial_writer_epoch {
            return Err(Error::Epoch);
        }
        let bootstrap = backend.read_bootstrap()?.ok_or(Error::Identity)?;
        if bootstrap != identity::bootstrap(&expected)? {
            return Err(Error::Identity);
        }
        let raw = backend.read_baseline()?.ok_or(Error::Identity)?;
        let baseline = CanonicalBaseline::decode(&raw)?;
        identity::config(&expected, &baseline)?;
        if required.is_some_and(|f| {
            f.domain != expected.physical.domain || f.stream_epoch != expected.physical.stream_epoch
        }) {
            return Err(Error::Identity);
        }
        let (physical, recovery) = Journal::recover(backend, required)?;
        if physical.config() != &expected.physical {
            return Err(Error::Identity);
        }
        if recovery.records.len() > MAX_RECORDS {
            return Err(Error::Capacity);
        }
        let mut records = Vec::with_capacity(recovery.records.len());
        let mut admissions = BTreeMap::new();
        let mut admission_origins = BTreeMap::new();
        let mut ingress = BTreeMap::new();
        let mut closures = BTreeMap::new();
        let mut associations = BTreeMap::new();
        let mut previous_epoch = expected.initial_writer_epoch;
        for raw in &recovery.records {
            identity::epoch(raw.exec_epoch)?;
            if raw.exec_epoch < previous_epoch || raw.exec_epoch > new_writer_epoch {
                return Err(Error::Epoch);
            }
            previous_epoch = raw.exec_epoch;
            let record = CanonicalRecord::decode(&raw.payload)?;
            if record.baseline() != expected.baseline_digest {
                return Err(Error::Identity);
            }
            if record.sequence() != raw.seq || record.sequence() != records.len() as u64 + 1 {
                return Err(Error::Sequence);
            }
            identity::record(&expected, &baseline, raw.exec_epoch, record.record())?;
            if let Some(key) = admission_key(record.record()) {
                // A valid live retry has no second physical record.
                if admissions.contains_key(&key) {
                    return Err(Error::Conflict);
                }
                // Covers this record after resynchronizing the recovered prefix;
                // it is not the record's original per-append frontier.
                let receipt = receipt(&record, recovery.frontier.as_ref().ok_or(Error::Sequence)?);
                admissions.insert(key, AdmissionSlot::InRecords(records.len(), receipt));
            }
            if let Some(key) = ingress_key(record.record()) {
                if ingress.contains_key(&key) {
                    return Err(Error::Conflict);
                }
                let receipt = receipt(&record, recovery.frontier.as_ref().ok_or(Error::Sequence)?);
                ingress.insert(key, (records.len(), receipt));
            }
            if let RecoveryRecord::IncomeObservationsAssociated(b) = record.record() {
                let receipt = receipt(&record, recovery.frontier.as_ref().ok_or(Error::Sequence)?);
                if associations
                    .insert(b.request_id, (records.len(), receipt))
                    .is_some()
                {
                    return Err(Error::Conflict);
                }
            }
            if let Some(key) = closure_history(record.record()) {
                let receipt = receipt(&record, recovery.frontier.as_ref().ok_or(Error::Sequence)?);
                if closures.insert(key, (records.len(), receipt)).is_some() {
                    return Err(Error::Conflict);
                }
            }
            records.push(record);
        }
        // Nothing is returned until all structural checks, historical closures and
        // deterministic reducer replay have succeeded.
        let mut history = HistoryFrontier::empty(&expected)?;
        for (index, (record, physical_record)) in records.iter().zip(&recovery.records).enumerate()
        {
            if let RecoveryRecord::IncomeObservationsAssociated(b) = record.record() {
                if b.history_digest != history.digest() {
                    return Err(Error::Identity);
                }
                verifier
                    .verify_income_association(
                        &baseline,
                        record,
                        &history,
                        &records[..index],
                        physical_record.exec_epoch,
                    )
                    .map_err(Error::Verification)?;
            }
            if admission_key(record.record()).is_some() {
                verifier
                    .verify_admission(&baseline, record, &history)
                    .map_err(Error::Verification)?;
            }
            if let RecoveryRecord::UncreatedAlgoClosed(b) = record.record() {
                if b.history_digest != history.digest() {
                    return Err(Error::Identity);
                }
                verifier
                    .verify_uncreated_algo(
                        &baseline,
                        record,
                        &history,
                        &records[..index],
                        physical_record.exec_epoch,
                    )
                    .map_err(Error::Verification)?;
            }
            if let RecoveryRecord::AccountInputsReconciled(b) = record.record() {
                if b.history_digest != history.digest() {
                    return Err(Error::Identity);
                }
                verifier
                    .verify_account_inputs_closure(
                        &baseline,
                        record,
                        &history,
                        &records[..index],
                        physical_record.exec_epoch,
                    )
                    .map_err(Error::Verification)?;
            }
            if let RecoveryRecord::Reactivated(binding) = record.record() {
                if binding.history_digest != history.digest() {
                    return Err(Error::Identity);
                }
                verifier
                    .verify_reactivation(&baseline, record, &history, &records[..index])
                    .map_err(Error::Verification)?;
            }
            if matches!(record.record(), RecoveryRecord::StrategyHandover(_)) {
                verifier
                    .verify_handover(&baseline, record, &records[..index])
                    .map_err(Error::Verification)?;
            }
            if matches!(record.record(), RecoveryRecord::IncomeBaseline(_)) {
                verifier
                    .verify_income_baseline(&baseline, record, &history, &records[..index])
                    .map_err(Error::Verification)?;
            }
            history = history.advance(physical_record.exec_epoch, record);
            if let Some(key) = admission_key(record.record()) {
                let original = receipt(
                    record,
                    recovery
                        .record_frontier(record.sequence())
                        .ok_or(Error::Sequence)?,
                );
                admission_origins.insert(
                    key,
                    AdmissionOrigin {
                        receipt: original,
                        history: history.clone(),
                    },
                );
            }
        }
        let inputs: Vec<_> = records.iter().map(|r| r.record().clone()).collect();
        let account = Account::recover(
            baseline.snapshot().clone(),
            baseline.catalog().clone(),
            &inputs,
        )?;
        let recovery_writer_floor = recovery
            .records
            .last()
            .map_or(expected.initial_writer_epoch, |r| r.exec_epoch);
        let mut journal = Self {
            physical,
            config: expected,
            baseline,
            writer_epoch: new_writer_epoch,
            evicted_prefix: 0,
            records,
            admissions,
            admission_origins,
            ingress,
            closures,
            associations,
            retention_window_ns: None,
            last_mono_ns: 0,
            intent_open_legs: BTreeMap::new(),
            retired_intents: BTreeMap::new(),
            poisoned: false,
            recovered: true,
            recovery_writer_floor: Some(recovery_writer_floor),
            history,
        };
        let replayed: Vec<_> = journal.records.iter().map(|r| r.record().clone()).collect();
        for rec in &replayed {
            journal.observe_record_retirement(rec);
        }
        Ok(Recovered {
            journal,
            account,
            discarded_tail_bytes: recovery.discarded_tail_bytes,
        })
    }

    pub fn with_retention_window_ns(mut self, window_ns: Option<u64>) -> Self {
        self.retention_window_ns = window_ns;
        self
    }

    pub fn set_retention_window_ns(&mut self, window_ns: Option<u64>) {
        self.retention_window_ns = window_ns;
    }

    pub fn retention_window_ns(&self) -> Option<u64> {
        self.retention_window_ns
    }

    pub fn evicted_prefix(&self) -> usize {
        self.evicted_prefix
    }

    fn record_at_logical(&self, logical_idx: usize) -> Option<&CanonicalRecord> {
        logical_idx
            .checked_sub(self.evicted_prefix)
            .and_then(|local| self.records.get(local))
    }

    fn admission_entry(&self, key: &IntentKey) -> Option<(&CanonicalRecord, &AppendReceipt)> {
        match self.admissions.get(key)? {
            AdmissionSlot::InRecords(idx, r) => Some((self.record_at_logical(*idx)?, r)),
            AdmissionSlot::Retained(rec, r) => Some((rec.as_ref(), r)),
        }
    }

    fn observe_record_retirement(&mut self, record: &RecoveryRecord) {
        if let Some(t) = record_mono_ns(record)
            && t > 0
        {
            self.last_mono_ns = self.last_mono_ns.max(t);
        }
        let stamp = self.last_mono_ns;
        match record {
            RecoveryRecord::Admit {
                command, expected, ..
            } => {
                if expected.orders.is_empty() {
                    if !matches!(
                        command.command().body,
                        tickwire_venue::CommandBody::AmendKeepPriority { .. }
                    ) {
                        self.retired_intents.insert(expected.intent, stamp);
                    }
                } else {
                    self.intent_open_legs
                        .insert(expected.intent, expected.orders.iter().cloned().collect());
                }
            }
            RecoveryRecord::VenueEvent(ev) | RecoveryRecord::IngressEvent { event: ev, .. } => {
                match &ev.event().body {
                    tickwire_venue::VenueEventBody::Amendment(a) => {
                        self.retired_intents.insert(
                            IntentKey {
                                epoch: a.request_intent_exec_epoch,
                                intent_id: a.request_intent_id,
                            },
                            stamp,
                        );
                    }
                    tickwire_venue::VenueEventBody::AmendmentRejected(a) => {
                        self.retired_intents.insert(
                            IntentKey {
                                epoch: a.request_intent_exec_epoch,
                                intent_id: a.request_intent_id,
                            },
                            stamp,
                        );
                    }
                    _ => {}
                }
            }
            RecoveryRecord::Aborted { intent, .. } => {
                self.intent_open_legs.remove(intent);
                self.retired_intents.insert(*intent, stamp);
            }
            RecoveryRecord::FeesReconciled { order, .. } => {
                let done = if let Some(legs) = self.intent_open_legs.get_mut(&order.intent) {
                    legs.remove(order);
                    legs.is_empty()
                } else {
                    false
                };
                if done {
                    self.intent_open_legs.remove(&order.intent);
                    self.retired_intents.insert(order.intent, stamp);
                }
            }
            RecoveryRecord::UncreatedAlgoClosed(b) => {
                self.intent_open_legs.remove(&b.intent);
                self.retired_intents.insert(b.intent, stamp);
            }
            _ => {}
        }
    }

    /// Compacts the in-memory `records` prefix down to at most `max_retained` suffix entries,
    /// preserving logical sequence continuity (`evicted_prefix`) and `HistoryFrontier`.
    /// Admissions listed in `keep_admissions` retain their exact `CanonicalRecord` and
    /// `AdmissionOrigin` for retry deduplication and venue provenance checks.
    pub fn compact_before(
        &mut self,
        keep_admissions: &BTreeSet<IntentKey>,
        max_retained: usize,
    ) -> usize {
        let mut stale_retained = Vec::new();
        for (key, slot) in &self.admissions {
            if matches!(slot, AdmissionSlot::Retained(..)) && !keep_admissions.contains(key) {
                stale_retained.push(*key);
            }
        }
        for key in stale_retained {
            self.admissions.remove(&key);
            self.admission_origins.remove(&key);
            self.retired_intents.remove(&key);
            self.intent_open_legs.remove(&key);
        }
        if self.records.len() <= max_retained {
            return 0;
        }
        let trim_len = self.records.len() - max_retained;
        for local_idx in 0..trim_len {
            let logical_idx = self.evicted_prefix + local_idx;
            let rec = &self.records[local_idx];
            if let Some(key) = admission_key(rec.record()) {
                if keep_admissions.contains(&key) {
                    if let Some(AdmissionSlot::InRecords(idx, receipt)) = self.admissions.get(&key)
                        && *idx == logical_idx
                    {
                        let cloned_receipt = receipt.clone();
                        self.admissions.insert(
                            key,
                            AdmissionSlot::Retained(Box::new(rec.clone()), cloned_receipt),
                        );
                    }
                } else {
                    self.admissions.remove(&key);
                    self.admission_origins.remove(&key);
                    self.retired_intents.remove(&key);
                    self.intent_open_legs.remove(&key);
                }
            }
            if let Some(ikey) = ingress_key(rec.record())
                && self
                    .ingress
                    .get(&ikey)
                    .is_some_and(|(idx, _)| *idx == logical_idx)
            {
                self.ingress.remove(&ikey);
            }
            if let Some(ckey) = closure_history(rec.record())
                && self
                    .closures
                    .get(&ckey)
                    .is_some_and(|(idx, _)| *idx == logical_idx)
            {
                self.closures.remove(&ckey);
            }
            if let RecoveryRecord::IncomeObservationsAssociated(b) = rec.record()
                && self
                    .associations
                    .get(&b.request_id)
                    .is_some_and(|(idx, _)| *idx == logical_idx)
            {
                self.associations.remove(&b.request_id);
            }
        }
        self.records.drain(..trim_len);
        self.evicted_prefix += trim_len;
        trim_len
    }

    pub fn append(&mut self, record: RecoveryRecord) -> Result<AppendReceipt> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        if let RecoveryRecord::IncomeObservationsAssociated(b) = &record {
            if let Some((index, receipt)) = self.associations.get(&b.request_id) {
                if self
                    .record_at_logical(*index)
                    .is_some_and(|r| r.record() == &record)
                {
                    return Ok(receipt.clone());
                }
                self.poisoned = true;
                return Err(Error::Conflict);
            }
            if b.history_digest != self.history.digest() {
                return Err(Error::Identity);
            }
        }
        if let Some(key) = closure_history(&record) {
            if let Some((index, receipt)) = self.closures.get(&key) {
                if self
                    .record_at_logical(*index)
                    .is_some_and(|r| r.record() == &record)
                {
                    return Ok(receipt.clone());
                }
                self.poisoned = true;
                return Err(Error::Conflict);
            }
            if key != self.history.digest() {
                return Err(Error::Identity);
            }
        }
        if let RecoveryRecord::Reactivated(binding) = &record {
            if binding.history_digest != self.history.digest() {
                return Err(Error::Identity);
            }
            if self
                .recovery_writer_floor
                .is_some_and(|floor| binding.writer_epoch <= floor)
            {
                return Err(Error::Epoch);
            }
        }
        let key = admission_key(&record);
        if key.is_some() && self.recovered {
            return Err(Error::Recovered);
        }
        if let Some(key) = key
            && let Some((prior_rec, receipt)) = self.admission_entry(&key)
        {
            if prior_rec.record() == &record {
                return Ok(receipt.clone());
            }
            self.poisoned = true;
            return Err(Error::Conflict);
        }
        let input = ingress_key(&record);
        if let Some(key) = input
            && let Some((index, receipt)) = self.ingress.get(&key)
        {
            if self
                .record_at_logical(*index)
                .is_some_and(|r| r.record() == &record)
            {
                return Ok(receipt.clone());
            }
            self.poisoned = true;
            return Err(Error::Conflict);
        }
        if self.records.len() >= MAX_RECORDS {
            if let Some(window_ns) = self.retention_window_ns {
                identity::record(&self.config, &self.baseline, self.writer_epoch, &record)?;
                let effective_now = self.last_mono_ns.max(record_mono_ns(&record).unwrap_or(0));
                if effective_now > 0 {
                    let keep_admissions: BTreeSet<IntentKey> = self
                        .admissions
                        .keys()
                        .copied()
                        .filter(|k| {
                            !self
                                .retired_intents
                                .get(k)
                                .is_some_and(|t| effective_now >= t.saturating_add(window_ns))
                        })
                        .collect();
                    self.compact_before(&keep_admissions, MAX_RECORDS / 2);
                }
            }
            if self.records.len() >= MAX_RECORDS {
                return Err(Error::Capacity);
            }
        }
        identity::record(&self.config, &self.baseline, self.writer_epoch, &record)?;
        let encoded =
            CanonicalRecord::new(self.config.baseline_digest, self.position() + 1, record)?;
        let next_history = self.history.advance(self.writer_epoch, &encoded);
        let sync = match self
            .physical
            .append_batch(self.writer_epoch, &[encoded.bytes()])
        {
            Ok(sync) => sync,
            Err(error) => {
                // The filesystem may retain some or all of the attempted batch.
                self.poisoned = true;
                return Err(error.into());
            }
        };
        let receipt = receipt(&encoded, sync.frontier());
        let logical_idx = self.evicted_prefix + self.records.len();
        if let Some(key) = key {
            self.admission_origins.insert(
                key,
                AdmissionOrigin {
                    receipt: receipt.clone(),
                    history: next_history.clone(),
                },
            );
            self.admissions
                .insert(key, AdmissionSlot::InRecords(logical_idx, receipt.clone()));
        }
        if let Some(key) = input {
            self.ingress.insert(key, (logical_idx, receipt.clone()));
        }
        if matches!(encoded.record(), RecoveryRecord::Reactivated(_)) {
            self.recovered = false;
            self.recovery_writer_floor = None;
        }
        if let Some(key) = closure_history(encoded.record()) {
            self.closures.insert(key, (logical_idx, receipt.clone()));
        }
        if let RecoveryRecord::IncomeObservationsAssociated(b) = encoded.record() {
            self.associations
                .insert(b.request_id, (logical_idx, receipt.clone()));
        }
        self.observe_record_retirement(encoded.record());
        self.records.push(encoded);
        self.history = next_history;
        Ok(receipt)
    }
    pub fn append_reactivation(
        &mut self,
        binding: tickwire_account::Reactivation,
    ) -> Result<AppendReceipt> {
        if binding.writer_epoch < self.writer_epoch
            || self
                .recovery_writer_floor
                .is_some_and(|floor| binding.writer_epoch <= floor)
        {
            return Err(Error::Epoch);
        }
        identity::epoch(binding.writer_epoch)?;
        let record = RecoveryRecord::Reactivated(Box::new(binding.clone()));
        identity::record(&self.config, &self.baseline, binding.writer_epoch, &record)?;
        CanonicalRecord::new(self.config.baseline_digest, self.position() + 1, record)?;
        if binding.history_digest != self.history.digest() {
            return Err(Error::Identity);
        }
        let old_writer = self.writer_epoch;
        self.writer_epoch = binding.writer_epoch;
        let result = self.append(RecoveryRecord::Reactivated(Box::new(binding)));
        if result.is_err() && !self.poisoned {
            self.writer_epoch = old_writer;
        }
        result
    }
    pub fn position(&self) -> u64 {
        (self.evicted_prefix + self.records.len()) as u64
    }
    pub fn writer_epoch(&self) -> u64 {
        self.writer_epoch
    }
    pub fn poisoned(&self) -> bool {
        self.poisoned
    }
    /// Last durable writer at recovery, until a newer writer's reactivation is synchronized.
    pub fn recovery_writer_floor(&self) -> Option<u64> {
        self.recovery_writer_floor
    }
    pub fn recovered(&self) -> bool {
        self.recovered
    }
    pub fn records(&self) -> &[CanonicalRecord] {
        &self.records
    }
    /// Exact durable input identity; lookup never invokes the account reducer.
    pub fn ingress_event(
        &self,
        stream: [u8; 16],
        sequence: u64,
    ) -> Option<&tickwire_venue::CanonicalEvent> {
        let (index, _) = self.ingress.get(&(stream, sequence))?;
        match self.record_at_logical(*index)?.record() {
            RecoveryRecord::IngressEvent { event, .. } => Some(event),
            _ => None,
        }
    }
    pub fn ingress_events(
        &self,
    ) -> impl Iterator<Item = ([u8; 16], u64, &tickwire_venue::CanonicalEvent)> {
        self.ingress
            .iter()
            .filter_map(|((stream, sequence), (index, _))| {
                let RecoveryRecord::IngressEvent { event, .. } =
                    self.record_at_logical(*index)?.record()
                else {
                    return None;
                };
                Some((*stream, *sequence, event.as_ref()))
            })
    }
    pub fn ingress_record(&self, stream: [u8; 16], sequence: u64) -> Option<&RecoveryRecord> {
        let (index, _) = self.ingress.get(&(stream, sequence))?;
        Some(self.record_at_logical(*index)?.record())
    }
    pub fn ingress_account_inputs(
        &self,
    ) -> impl Iterator<Item = ([u8; 16], u64, &tickwire_venue::CanonicalAccountInput)> {
        self.ingress
            .iter()
            .filter_map(|((stream, sequence), (index, _))| {
                match self.record_at_logical(*index)?.record() {
                    RecoveryRecord::IngressAccountInput { input, .. } => {
                        Some((*stream, *sequence, input.as_ref()))
                    }
                    _ => None,
                }
            })
    }
    pub fn baseline(&self) -> &CanonicalBaseline {
        &self.baseline
    }
    pub fn config(&self) -> &Config {
        &self.config
    }
    pub fn admission_origin(&self, key: IntentKey) -> Option<&AdmissionOrigin> {
        self.admission_origins.get(&key)
    }
    pub fn history_frontier(&self) -> &HistoryFrontier {
        &self.history
    }
}
fn ingress_key(record: &RecoveryRecord) -> Option<([u8; 16], u64)> {
    match record {
        RecoveryRecord::IngressEvent {
            input_stream,
            input_sequence,
            ..
        }
        | RecoveryRecord::IngressAccountInput {
            input_stream,
            input_sequence,
            ..
        } => Some((*input_stream, *input_sequence)),
        _ => None,
    }
}
fn admission_key(record: &RecoveryRecord) -> Option<IntentKey> {
    match record {
        RecoveryRecord::Admit { expected, .. } => Some(expected.intent),
        _ => None,
    }
}
fn receipt(record: &CanonicalRecord, frontier: &PhysicalFrontier) -> AppendReceipt {
    AppendReceipt {
        baseline: record.baseline(),
        sequence: record.sequence(),
        digest: record.digest(),
        physical: frontier.clone(),
    }
}

fn closure_history(record: &RecoveryRecord) -> Option<Hash> {
    match record {
        RecoveryRecord::AccountInputsReconciled(b) => Some(b.history_digest),
        RecoveryRecord::UncreatedAlgoClosed(b) => Some(b.history_digest),
        _ => None,
    }
}

fn record_mono_ns(record: &RecoveryRecord) -> Option<u64> {
    match record {
        RecoveryRecord::Admit { evaluated_at, .. } => Some(*evaluated_at),
        RecoveryRecord::VenueEvent(ev) | RecoveryRecord::IngressEvent { event: ev, .. } => {
            Some(ev.event().identity.received_mono_ns)
        }
        RecoveryRecord::Reactivated(b) => Some(b.evaluated_at),
        _ => None,
    }
}
