use crate::{Config, Error, Event, FileFact, Identity, Phase, Prepared, Receipt, Result, Version};
use std::collections::BTreeMap;

struct Step {
    bytes: Vec<u8>,
    event: Event,
    receipt: Receipt,
}
struct Operation {
    prepared: Prepared,
    steps: BTreeMap<Phase, Step>,
}
pub struct OperationView<'a> {
    operation: &'a Operation,
}
impl OperationView<'_> {
    pub fn prepared(&self) -> &Prepared {
        &self.operation.prepared
    }
    pub fn event(&self, phase: Phase) -> Option<&Event> {
        self.operation.steps.get(&phase).map(|s| &s.event)
    }
    pub fn receipt(&self, phase: Phase) -> Option<&Receipt> {
        self.operation.steps.get(&phase).map(|s| &s.receipt)
    }
}
pub(crate) struct Ledger {
    operations: BTreeMap<Identity, Operation>,
    active: Option<Identity>,
    effective: Option<Version>,
    bytes: usize,
    max_operations: usize,
    max_bytes: usize,
}
fn positive(value: u64) -> bool {
    (1..=i64::MAX as u64).contains(&value)
}
fn version(v: &Version) -> bool {
    positive(v.revision)
        && positive(v.policy_revision)
        && [v.global_hash, v.projection_hash, v.object_hash]
            .iter()
            .all(|h| *h != [0; 32])
}
fn file(f: &FileFact) -> bool {
    f.render_hash != [0; 32] && (1..=8 * 1024 * 1024).contains(&f.bytes) && f.inode > 0
}
pub(crate) fn identity(event: &Event) -> (Identity, Phase) {
    match event {
        Event::Prepared(p) => (p.operation, Phase::Prepared),
        Event::Desired { operation, .. } => (*operation, Phase::Desired),
        Event::RuntimeObserved { operation, .. } => (*operation, Phase::RuntimeObserved),
        Event::Effective { operation, .. } => (*operation, Phase::Effective),
    }
}
impl Ledger {
    pub(crate) fn new(config: &Config, header_bytes: usize) -> Result<Self> {
        if header_bytes > config.max_recorded_bytes {
            return Err(Error::Capacity);
        }
        Ok(Self {
            operations: BTreeMap::new(),
            active: None,
            effective: None,
            bytes: header_bytes,
            max_operations: config.max_operations,
            max_bytes: config.max_recorded_bytes,
        })
    }
    pub(crate) fn check(&self, event: &Event, bytes: &[u8]) -> Result<Option<Receipt>> {
        let (id, phase) = identity(event);
        if let Some(step) = self.operations.get(&id).and_then(|op| op.steps.get(&phase)) {
            return if step.bytes == bytes {
                Ok(Some(step.receipt.clone()))
            } else {
                Err(Error::Conflict)
            };
        }
        if self
            .bytes
            .checked_add(bytes.len())
            .is_none_or(|n| n > self.max_bytes)
        {
            return Err(Error::Capacity);
        }
        if let Event::Prepared(p) = event {
            if id == [0; 16]
                || !version(&p.next)
                || p.base.as_ref().is_some_and(|v| !version(v))
                || p.control_commit_ref == [0; 32]
                || p.target_set_hash == [0; 32]
                || p.expected_instance == [0; 16]
                || !positive(p.exec_generation)
                || !(1..=4 * 1024 * 1024).contains(&p.projection_object_bytes)
            {
                return Err(Error::Invalid("prepared identity or version"));
            }
            if self.active.is_some()
                || self.operations.contains_key(&id)
                || p.base != self.effective
                || p.base
                    .as_ref()
                    .is_some_and(|v| p.next.revision <= v.revision)
            {
                return Err(Error::Conflict);
            }
            if self.operations.len() >= self.max_operations {
                return Err(Error::Capacity);
            }
            return Ok(None);
        }
        let op = self.operations.get(&id).ok_or(Error::Conflict)?;
        if self.active != Some(id) {
            return Err(Error::Conflict);
        }
        let (expected, link, valid) = match event {
            Event::Desired {
                prepared_ref,
                file: f,
                ..
            } => (Phase::Prepared, *prepared_ref, file(f)),
            Event::RuntimeObserved {
                desired_ref,
                fact: f,
                ..
            } => {
                let p = &op.prepared;
                (
                    Phase::Desired,
                    *desired_ref,
                    f.instance == p.expected_instance
                        && f.boot != [0; 16]
                        && f.host_boot != [0; 16]
                        && (p.expected_boot == [0; 16] || f.boot == p.expected_boot)
                        && (p.expected_host_boot == [0; 16] || f.host_boot == p.expected_host_boot)
                        && f.exec_generation == p.exec_generation
                        && positive(f.applied_event_seq)
                        && f.witness_ref != [0; 32],
                )
            }
            Event::Effective {
                runtime_ref,
                file: f,
                ..
            } => (Phase::RuntimeObserved, *runtime_ref, file(f)),
            Event::Prepared(_) => unreachable!(),
        };
        if !valid
            || op.steps.last_key_value().map(|(p, _)| *p) != Some(expected)
            || op
                .steps
                .get(&expected)
                .is_none_or(|step| step.receipt.record_hash != link)
        {
            return Err(Error::Conflict);
        }
        Ok(None)
    }
    /// Called only with the event already checked and a real synchronized receipt.
    pub(crate) fn apply(&mut self, event: &Event, bytes: Vec<u8>, receipt: Receipt) {
        let (id, phase) = identity(event);
        if let Event::Prepared(p) = event {
            self.operations.insert(
                id,
                Operation {
                    prepared: (**p).clone(),
                    steps: BTreeMap::new(),
                },
            );
            self.active = Some(id);
        }
        let op = self.operations.get_mut(&id).expect("checked operation");
        self.bytes += bytes.len();
        op.steps.insert(
            phase,
            Step {
                bytes,
                event: event.clone(),
                receipt,
            },
        );
        if phase == Phase::Effective {
            self.effective = Some(op.prepared.next.clone());
            self.active = None;
        }
    }
    pub(crate) fn phase(&self, id: Identity) -> Option<Phase> {
        self.operations
            .get(&id)?
            .steps
            .last_key_value()
            .map(|(p, _)| *p)
    }
    pub(crate) fn effective(&self) -> Option<&Version> {
        self.effective.as_ref()
    }
    pub(crate) fn operation(&self, id: Identity) -> Option<OperationView<'_>> {
        self.operations
            .get(&id)
            .map(|operation| OperationView { operation })
    }
    pub(crate) fn operations(&self) -> impl Iterator<Item = OperationView<'_>> {
        self.operations
            .values()
            .map(|operation| OperationView { operation })
    }
    pub(crate) fn active_operation(&self) -> Option<Identity> {
        self.active
    }
}
