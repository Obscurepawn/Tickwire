//! Deterministic external services. Heap allocation is allowed in test doubles, not hot reducers.
use std::collections::BTreeMap;
use tickwire_execution::{ports::*, *};

#[derive(Debug, Clone)]
pub struct MockSql {
    row: ConfigRow,
    pub unavailable: bool,
}
impl MockSql {
    pub fn new(payload: &[u8]) -> Self {
        Self {
            row: ConfigRow {
                revision: 1,
                payload: payload.to_vec(),
            },
            unavailable: false,
        }
    }
}
impl ConfigStore for MockSql {
    fn load(&self) -> Result<ConfigRow, ServiceError> {
        if self.unavailable {
            Err(ServiceError::Unavailable)
        } else {
            Ok(self.row.clone())
        }
    }
    fn compare_exchange(
        &mut self,
        expected: u64,
        payload: &[u8],
    ) -> Result<ConfigRow, ServiceError> {
        if self.unavailable {
            return Err(ServiceError::Unavailable);
        }
        if expected != self.row.revision {
            return Err(ServiceError::Conflict);
        }
        let revision = expected.checked_add(1).ok_or(ServiceError::Capacity)?;
        self.row = ConfigRow {
            revision,
            payload: payload.to_vec(),
        };
        Ok(self.row.clone())
    }
}
#[derive(Debug, Clone, Default)]
pub struct MockKv {
    objects: BTreeMap<u64, Vec<u8>>,
    pub fail_writes: bool,
}
impl ObjectStore for MockKv {
    fn put_immutable(&mut self, key: u64, payload: &[u8]) -> Result<(), ServiceError> {
        if self.fail_writes {
            return Err(ServiceError::Unavailable);
        }
        if let Some(prior) = self.objects.get(&key) {
            return if prior == payload {
                Ok(())
            } else {
                Err(ServiceError::Conflict)
            };
        }
        self.objects.insert(key, payload.to_vec());
        Ok(())
    }
    fn get(&self, key: u64) -> Result<&[u8], ServiceError> {
        self.objects
            .get(&key)
            .map(Vec::as_slice)
            .ok_or(ServiceError::MissingDependency)
    }
}
pub use tickwire_execution::ports::ExecutionFact as Fact;
#[derive(Debug, Clone)]
pub struct MockJournal {
    pub objects: MockKv,
    records: Vec<DurableRecord>,
    facts: Vec<Fact>,
    pub fail_next_commit: bool,
    pub lose_next_receipt: bool,
    pub replica_ready: bool,
    pub fail_next_fact: bool,
}
impl Default for MockJournal {
    fn default() -> Self {
        Self {
            objects: MockKv::default(),
            records: Vec::new(),
            facts: Vec::new(),
            fail_next_commit: false,
            lose_next_receipt: false,
            replica_ready: true,
            fail_next_fact: false,
        }
    }
}
/// Exact test dependency bytes; production will use the content-addressed RecoveryManifest ABI.
fn dependency_bytes(r: OrderRequest) -> Vec<u8> {
    format!("{r:?}").into_bytes()
}
impl Journal for MockJournal {
    fn append_fact(&mut self, f: Fact) -> Result<(), ServiceError> {
        if std::mem::take(&mut self.fail_next_fact) {
            return Err(ServiceError::Unavailable);
        }
        self.facts.push(f);
        Ok(())
    }

    fn facts(&self) -> &[Fact] {
        &self.facts
    }

    fn commit(&mut self, record: DurableRecord) -> Result<CommitReceipt, ServiceError> {
        if std::mem::take(&mut self.fail_next_commit) {
            return Err(ServiceError::Unavailable);
        }
        if self.objects.get(record.request.recovery_object)? != dependency_bytes(record.request) {
            return Err(ServiceError::Conflict);
        }
        if let Some(prior) = self.records.iter().find(|r| {
            r.request.key == record.request.key
                || (r.request.key.domain() == record.request.key.domain()
                    && r.sequence == record.sequence)
        }) {
            if *prior != record {
                return Err(ServiceError::Conflict);
            }
        } else {
            self.records.push(record);
        }
        if std::mem::take(&mut self.lose_next_receipt) {
            return Err(ServiceError::Unavailable);
        }
        Ok(CommitReceipt {
            record,
            local_durable: true,
            replica_durable: self.replica_ready,
        })
    }
    fn committed(&self) -> &[DurableRecord] {
        &self.records
    }
}
#[derive(Debug, Clone)]
pub struct MockExchange {
    pub next_outcome: SubmitOutcome,
    pub accepted: Vec<OrderRequest>,
    pub submit_calls: usize,
}
impl Default for MockExchange {
    fn default() -> Self {
        Self {
            next_outcome: SubmitOutcome::Acknowledged,
            accepted: Vec::new(),
            submit_calls: 0,
        }
    }
}
impl Exchange for MockExchange {
    fn submit(&mut self, request: OrderRequest) -> SubmitOutcome {
        self.submit_calls += 1;
        let outcome = std::mem::replace(&mut self.next_outcome, SubmitOutcome::Acknowledged);
        if outcome != SubmitOutcome::NoBytesAccepted {
            self.accepted.push(request);
        }
        outcome
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeError {
    Domain(Error),
    Service(ServiceError),
}
impl From<Error> for RuntimeError {
    fn from(e: Error) -> Self {
        Self::Domain(e)
    }
}
impl From<ServiceError> for RuntimeError {
    fn from(e: ServiceError) -> Self {
        Self::Service(e)
    }
}
/// A reference orchestration using the same ports as future live drivers.
/// It deliberately has no wall-clock sleeps or hidden retries.
pub struct MockRuntime<const N: usize, const T: usize> {
    pub engine: Engine<N, T>,
    pub journal: MockJournal,
    pub config: MockSql,
    pub exchange: MockExchange,
    domain: Domain,
    capital: i128,
    gate: Gate,
}
impl<const N: usize, const T: usize> MockRuntime<N, T> {
    pub fn new(domain: Domain, capital: i128, gate: Gate) -> Result<Self, Error> {
        Ok(Self {
            engine: Engine::new(domain, capital, gate)?,
            journal: MockJournal::default(),
            config: MockSql::new(b"reference"),
            exchange: MockExchange::default(),
            domain,
            capital,
            gate,
        })
    }
    pub fn submit(&mut self, r: OrderRequest, profile: CommitProfile) -> Result<(), RuntimeError> {
        if self.config.load()?.revision != r.config_revision {
            return Err(ServiceError::Conflict.into());
        }
        self.engine.reserve(r)?;
        self.journal
            .objects
            .put_immutable(r.recovery_object, &dependency_bytes(r))?;
        self.resume_submit(r.key.intent, profile)
    }
    pub fn resume_submit(&mut self, id: u64, profile: CommitProfile) -> Result<(), RuntimeError> {
        let record = self.engine.durable_record(id)?;
        if self.config.load()?.revision != record.request.config_revision {
            return Err(ServiceError::Conflict.into());
        }
        let receipt = self.journal.commit(record)?;
        self.engine.accept_commit(receipt, profile)?;
        let request = self.engine.begin_send(id)?;
        match self.exchange.submit(request) {
            SubmitOutcome::PendingResponse => {}
            SubmitOutcome::Acknowledged => self.engine.ack(id)?,
            SubmitOutcome::MayHaveSent => self.engine.uncertain(id)?,
            SubmitOutcome::NoBytesAccepted => self.engine.transport_rejected_without_bytes(id)?,
        }
        Ok(())
    }
    fn record(&mut self, f: Fact) -> Result<(), RuntimeError> {
        if let Err(error) = self.journal.append_fact(f) {
            self.engine.freeze();
            return Err(error.into());
        }
        // Preserve the received fact even when it cannot be applied. Recovery then
        // reports the unresolved fact instead of silently dropping financial evidence.
        f.apply(&mut self.engine)?;
        Ok(())
    }
    pub fn record_fill(&mut self, id: u64, fill: Fill) -> Result<(), RuntimeError> {
        self.record(Fact::Fill { intent: id, fill })
    }
    pub fn record_fee(&mut self, intent: u64, id: u64, amount: i128) -> Result<(), RuntimeError> {
        self.record(Fact::Fee { intent, id, amount })
    }
    pub fn record_terminal(
        &mut self,
        intent: u64,
        status: Terminal,
        cumulative: u64,
    ) -> Result<(), RuntimeError> {
        self.record(Fact::Terminal {
            intent,
            status,
            cumulative,
        })
    }
    /// Journal and exchange survive this simulated exec-process crash. No permission is restored.
    pub fn restart(&mut self) -> Result<(), RuntimeError> {
        let mut engine = Engine::new(self.domain, self.capital, self.gate)?;
        engine.freeze();
        for record in self.journal.committed() {
            engine.recover_intent(*record)?;
        }
        for fact in self.journal.facts() {
            fact.apply(&mut engine)?;
        }
        self.engine = engine;
        Ok(())
    }
}
