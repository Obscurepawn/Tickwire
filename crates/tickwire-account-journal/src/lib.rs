//! Ordered account persistence; receipts are durability evidence, not trading authority.
#![forbid(unsafe_code)]
use std::{collections::BTreeMap, io};
use tickwire_account::{Account, IntentKey, RecoveryRecord, Scope};
use tickwire_account_codec::{CanonicalBaseline, CanonicalRecord, Hash};
use tickwire_journal::{Journal, PhysicalFrontier, store::Store};
mod backend;
mod history;
mod identity;
pub use backend::FileBackend;
pub use history::HistoryFrontier;

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
    fn verify_reactivation(&mut self,_baseline:&CanonicalBaseline,_record:&CanonicalRecord,_preceding:&HistoryFrontier,_prior:&[CanonicalRecord])->std::result::Result<(),VerificationError>{Err(VerificationError::Invalid)}

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
pub struct AccountJournal<S: Backend> {
    physical: Journal<S>,
    config: Config,
    baseline: CanonicalBaseline,
    writer_epoch: u64,
    records: Vec<CanonicalRecord>,
    admissions: BTreeMap<IntentKey, (usize, AppendReceipt)>,
    ingress: BTreeMap<([u8; 16], u64), (usize, AppendReceipt)>,
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
            records: Vec::new(),
            admissions: BTreeMap::new(),
            ingress: BTreeMap::new(),
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
        let mut ingress = BTreeMap::new();
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
                admissions.insert(key, (records.len(), receipt));
            }
            if let Some(key) = ingress_key(record.record()) {
                if ingress.contains_key(&key) {
                    return Err(Error::Conflict);
                }
                let receipt = receipt(&record, recovery.frontier.as_ref().ok_or(Error::Sequence)?);
                ingress.insert(key, (records.len(), receipt));
            }
            records.push(record);
        }
        // Nothing is returned until all structural checks, historical closures and
        // deterministic reducer replay have succeeded.
        let mut history = HistoryFrontier::empty(&expected)?;
        for (index,(record, physical_record)) in records.iter().zip(&recovery.records).enumerate() {
            if admission_key(record.record()).is_some() {
                verifier
                    .verify_admission(&baseline, record, &history)
                    .map_err(Error::Verification)?;
            }
            if let RecoveryRecord::Reactivated(binding)=record.record() {
                if binding.history_digest!=history.digest(){return Err(Error::Identity);}
                verifier.verify_reactivation(&baseline,record,&history,&records[..index]).map_err(Error::Verification)?;
            }
            history = history.advance(physical_record.exec_epoch, record);
        }
        let inputs: Vec<_> = records.iter().map(|r| r.record().clone()).collect();
        let account = Account::recover(
            baseline.snapshot().clone(),
            baseline.catalog().clone(),
            &inputs,
        )?;
        let recovery_writer_floor=recovery.records.last().map_or(expected.initial_writer_epoch,|r|r.exec_epoch);
        Ok(Recovered {
            journal: Self {
                physical,
                config: expected,
                baseline,
                writer_epoch: new_writer_epoch,
                records,
                admissions,
                ingress,
                poisoned: false,
                recovered: true,
                recovery_writer_floor: Some(recovery_writer_floor),
                history,
            },
            account,
            discarded_tail_bytes: recovery.discarded_tail_bytes,
        })
    }
    pub fn append(&mut self, record: RecoveryRecord) -> Result<AppendReceipt> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        if let RecoveryRecord::Reactivated(binding)=&record {
            if binding.history_digest!=self.history.digest(){return Err(Error::Identity);}
            if self.recovery_writer_floor.is_some_and(|floor|binding.writer_epoch<=floor){return Err(Error::Epoch);}
        }
        let key = admission_key(&record);
        if key.is_some() && self.recovered {
            return Err(Error::Recovered);
        }
        if let Some(key) = key
            && let Some((index, receipt)) = self.admissions.get(&key)
        {
            if self.records[*index].record() == &record {
                return Ok(receipt.clone());
            }
            self.poisoned = true;
            return Err(Error::Conflict);
        }
        let input = ingress_key(&record);
        if let Some(key) = input
            && let Some((index, receipt)) = self.ingress.get(&key)
        {
            if self.records[*index].record() == &record {
                return Ok(receipt.clone());
            }
            self.poisoned = true;
            return Err(Error::Conflict);
        }
        if self.records.len() >= MAX_RECORDS {
            return Err(Error::Capacity);
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
        if let Some(key) = key {
            self.admissions
                .insert(key, (self.records.len(), receipt.clone()));
        }
        if let Some(key) = input {
            self.ingress
                .insert(key, (self.records.len(), receipt.clone()));
        }
        if matches!(encoded.record(),RecoveryRecord::Reactivated(_)){self.recovered=false;self.recovery_writer_floor=None;}
        self.records.push(encoded);
        self.history = next_history;
        Ok(receipt)
    }
    pub fn append_reactivation(&mut self,binding:tickwire_account::Reactivation)->Result<AppendReceipt>{
        if binding.writer_epoch<self.writer_epoch || self.recovery_writer_floor.is_some_and(|floor|binding.writer_epoch<=floor) {return Err(Error::Epoch);}
        identity::epoch(binding.writer_epoch)?;
        let record=RecoveryRecord::Reactivated(Box::new(binding.clone()));
        identity::record(&self.config,&self.baseline,binding.writer_epoch,&record)?;
        CanonicalRecord::new(self.config.baseline_digest,self.position()+1,record)?;
        if binding.history_digest!=self.history.digest(){return Err(Error::Identity);}
        let old_writer=self.writer_epoch;
        self.writer_epoch=binding.writer_epoch;
        let result=self.append(RecoveryRecord::Reactivated(Box::new(binding)));
        if result.is_err()&&!self.poisoned{self.writer_epoch=old_writer;}
        result
    }
    pub fn position(&self) -> u64 {
        self.records.len() as u64
    }
    pub fn writer_epoch(&self) -> u64 {
        self.writer_epoch
    }
    pub fn poisoned(&self) -> bool {
        self.poisoned
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
        match self.records[*index].record() {
            RecoveryRecord::IngressEvent { event, .. } => Some(event),
            _ => unreachable!("ingress index contains only ingress records"),
        }
    }
    pub fn ingress_events(
        &self,
    ) -> impl Iterator<Item = ([u8; 16], u64, &tickwire_venue::CanonicalEvent)> {
        self.ingress.iter().map(|((stream, sequence), (index, _))| {
            let RecoveryRecord::IngressEvent { event, .. } = self.records[*index].record() else {
                unreachable!("ingress index")
            };
            (*stream, *sequence, event.as_ref())
        })
    }
    pub fn baseline(&self) -> &CanonicalBaseline {
        &self.baseline
    }
    pub fn config(&self) -> &Config {
        &self.config
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
