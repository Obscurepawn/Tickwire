//! Explicitly approved market transition files, synchronized by ControlInbox.
use super::Binding;
use crate::{Result, invalid, market::Sources, strategy::AppStrategy};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, path::PathBuf};
use tickwire_clock::MonoTime;
use tickwire_market_source::transition::{Approval, Commit, MAX_CONTROL_BYTES, Phase, Receipt};
mod files;
pub(super) mod history;
use history::History;

fn fresh(
    boundary: &mut impl FnMut(Option<(&Decision, MonoTime)>) -> Result<MonoTime>,
    previous: MonoTime,
) -> Result<MonoTime> {
    checked_time(previous, boundary(None)?)
}
fn checked_time(previous: MonoTime, current: MonoTime) -> Result<MonoTime> {
    if current.domain() != previous.domain() || current.nanos() < previous.nanos() {
        return Err(invalid("market durable boundary clock regressed"));
    }
    Ok(current)
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub consumer_directory: PathBuf,
    pub effective_directory: PathBuf,
    pub maximum_operations: usize,
    pub maximum_per_poll: usize,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Consumer {
    pub pid: u32,
    pub instance: [u8; 16],
    pub generation: u64,
}
impl Consumer {
    fn stem(self) -> String {
        let hex: String = self.instance.iter().map(|v| format!("{v:02x}")).collect();
        format!("{}-{hex}", self.pid)
    }
    fn matches(self, config: &tickwire_market_source::ConnectConfig) -> bool {
        self.pid != 0
            && self.instance == config.endpoint.local.instance
            && self.generation == config.endpoint.local.generation
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum DecisionAction {
    Arm {
        approval: Box<Approval>,
    },
    Commit {
        commit: Box<Commit>,
        previous_applied: u64,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Decision {
    pub binding: Binding,
    pub consumer: Consumer,
    pub action: DecisionAction,
}
impl Decision {
    fn id(&self) -> [u8; 16] {
        match &self.action {
            DecisionAction::Arm { approval } => approval.operation_id,
            DecisionAction::Commit { commit, .. } => commit.operation_id,
        }
    }
}
struct Operation {
    approval: Approval,
    commit: Option<Commit>,
    receipt: Option<Receipt>,
    finished: bool,
}
pub(super) struct Controller {
    config: Config,
    sources: Option<Sources>,
    history: History,
    active: BTreeMap<Consumer, Operation>,
    next: usize,
    arm_stamps: BTreeMap<Consumer, files::Stamp>,
}
impl Controller {
    pub fn new(config: Config, sources: Option<Sources>, history: History) -> Result<Self> {
        if config.maximum_operations == 0
            || config.maximum_operations > 128
            || config.maximum_per_poll == 0
            || config.maximum_per_poll > 32
            || history.len() > config.maximum_operations
            || config.consumer_directory == config.effective_directory
        {
            return Err(invalid("market control capacity or directory"));
        }
        files::directory(&config.consumer_directory)?;
        files::directory(&config.effective_directory)?;
        Ok(Self {
            config,
            sources,
            history,
            active: BTreeMap::new(),
            next: 0,
            arm_stamps: BTreeMap::new(),
        })
    }
    /// Some((decision, admitted_at)) preserves its verified admission time in the WAL;
    /// None only refreshes the
    /// boundary. Both return a fresh clock sample after verifying the live Binding.
    pub fn poll(
        &mut self,
        binding: &Binding,
        strategy: &mut dyn AppStrategy,
        now: MonoTime,
        mut persist: impl FnMut(Option<(&Decision, MonoTime)>) -> Result<MonoTime>,
    ) -> Result<usize> {
        files::directory(&self.config.consumer_directory)?;
        files::directory(&self.config.effective_directory)?;
        let mut consumers = self
            .sources
            .as_ref()
            .map_or_else(Vec::new, Sources::consumers);
        strategy.market_consumers(&mut consumers);
        consumers.sort();
        consumers.dedup();
        if consumers.len() > 64 {
            return Err(invalid("market control consumer bound"));
        }
        // Losing an actual consumer cannot manufacture a completed operation.
        if self
            .active
            .iter()
            .any(|(c, o)| !o.finished && !consumers.contains(c))
        {
            return Err(invalid("market consumer disappeared"));
        }
        if consumers.is_empty() {
            return Ok(0);
        }
        let count = consumers.len().min(self.config.maximum_per_poll);
        for offset in 0..count {
            let consumer = consumers[(self.next + offset) % consumers.len()];
            self.poll_one(consumer, binding, strategy, now, &mut persist)?;
        }
        self.next = (self.next + count) % consumers.len();
        Ok(count)
    }
    fn local(&self, c: Consumer) -> bool {
        self.sources
            .as_ref()
            .is_some_and(|s| s.consumers().contains(&c))
    }
    fn poll_one(
        &mut self,
        c: Consumer,
        binding: &Binding,
        strategy: &mut dyn AppStrategy,
        now: MonoTime,
        persist: &mut impl FnMut(Option<(&Decision, MonoTime)>) -> Result<MonoTime>,
    ) -> Result<()> {
        let local = self.local(c);
        let stem = c.stem();
        if let Some(mut op) = self.active.remove(&c) {
            let result = self.advance(c, &mut op, strategy, local, now, persist);
            self.active.insert(c, op);
            result?;
        }
        let arm_path = self
            .config
            .consumer_directory
            .join(format!("{stem}.arm.json"));
        let stamp = files::stamp(&arm_path, MAX_CONTROL_BYTES)?;
        let changed = stamp
            .as_ref()
            .is_some_and(|s| self.arm_stamps.get(&c) != Some(s));
        if changed && let Some(bytes) = files::read(&arm_path, MAX_CONTROL_BYTES)? {
            let approval = Approval::decode(&bytes)?;
            let decision = Decision {
                binding: binding.clone(),
                consumer: c,
                action: DecisionAction::Arm {
                    approval: Box::new(approval.clone()),
                },
            };
            if !self.history.contains(&decision)? {
                if self.active.get(&c).is_some_and(|o| !o.finished) {
                    return Err(invalid("market operation already pending"));
                }
                let accepted = fresh(persist, now)?;
                self.history
                    .check(&decision, accepted, self.config.maximum_operations)?;
                if local {
                    self.sources
                        .as_ref()
                        .ok_or_else(|| invalid("missing market source"))?
                        .validate_arm(&approval, accepted)?;
                }
                let synchronized = checked_time(accepted, persist(Some((&decision, accepted)))?)?;
                self.history
                    .record(decision, accepted, self.config.maximum_operations)?;
                approval.validate(&approval.previous, synchronized)?;
                let result = if local {
                    self.sources.as_ref().unwrap().arm(&approval, synchronized)
                } else {
                    strategy.arm_market(c, &approval)
                };
                let rejected = result.is_err();
                if let Err(error) = result {
                    eprintln!(
                        "{}",
                        serde_json::json!({"event":"market_arm_rejected","pid":c.pid,"error":error.to_string()})
                    );
                }
                self.active.insert(
                    c,
                    Operation {
                        approval,
                        commit: None,
                        receipt: None,
                        finished: rejected,
                    },
                );
            }
        }
        if let Some(stamp) = stamp {
            self.arm_stamps.insert(c, stamp);
        } else {
            self.arm_stamps.remove(&c);
        }
        let Some(mut op) = self.active.remove(&c) else {
            return Ok(());
        };
        let result = (|| {
            self.advance(c, &mut op, strategy, local, now, persist)?;
            if op.finished
                || op.commit.is_some()
                || op
                    .receipt
                    .as_ref()
                    .is_none_or(|r| r.phase != Phase::Quiesced)
            {
                return Ok(());
            }
            let path = self
                .config
                .consumer_directory
                .join(format!("{stem}.commit.json"));
            let Some(bytes) = files::read(&path, MAX_CONTROL_BYTES)? else {
                return Ok(());
            };
            let commit = Commit::decode(&bytes)?;
            // A previous operation's immutable commit may remain until MD publishes the next one.
            if commit.operation_id != op.approval.operation_id {
                return Ok(());
            }
            let previous_applied = op
                .receipt
                .as_ref()
                .and_then(|r| r.previous_applied)
                .ok_or_else(|| invalid("missing actual quiesce watermark"))?;
            let decision = Decision {
                binding: binding.clone(),
                consumer: c,
                action: DecisionAction::Commit {
                    commit: Box::new(commit.clone()),
                    previous_applied,
                },
            };
            let accepted = fresh(persist, now)?;
            self.history
                .check(&decision, accepted, self.config.maximum_operations)?;
            let synchronized = checked_time(accepted, persist(Some((&decision, accepted)))?)?;
            self.history
                .record(decision, accepted, self.config.maximum_operations)?;
            commit.validate(&op.approval, previous_applied, synchronized)?;
            if local {
                self.sources
                    .as_ref()
                    .unwrap()
                    .commit(&op.approval, &commit, synchronized)?;
            } else {
                strategy.commit_market(c, &commit)?;
            }
            op.commit = Some(commit);
            Ok(())
        })();
        self.active.insert(c, op);
        result
    }
    #[allow(clippy::too_many_arguments)]
    fn advance(
        &self,
        c: Consumer,
        op: &mut Operation,
        strategy: &dyn AppStrategy,
        local: bool,
        now: MonoTime,
        boundary: &mut impl FnMut(Option<(&Decision, MonoTime)>) -> Result<MonoTime>,
    ) -> Result<()> {
        if op.finished {
            return Ok(());
        }
        let mut now = fresh(boundary, now)?;
        if !local && strategy.market_rejection(c).is_some() {
            if op.receipt.as_ref().is_some_and(|r| r.phase != Phase::Armed) {
                return Err(invalid("market rejection after quiescence"));
            }
            op.finished = true;
            return Ok(());
        }
        let receipt = if local {
            self.sources.as_ref().unwrap().receipt(&op.approval)
        } else {
            strategy
                .market_transition(c)
                .filter(|r| r.operation_id == op.approval.operation_id)
                .cloned()
        };
        if now.nanos() >= op.approval.deadline_mono_ns {
            if receipt
                .as_ref()
                .or(op.receipt.as_ref())
                .is_some_and(|r| r.phase != Phase::Armed)
            {
                return Err(invalid("market transition expired after quiescence"));
            }
            op.finished = true;
            return Ok(());
        }
        let Some(receipt) = receipt else {
            return Ok(());
        };
        op.approval
            .validate_receipt(&receipt, c.pid, receipt.phase, now)?;
        if op.receipt.as_ref() == Some(&receipt) {
            return Ok(());
        }
        if let Some(previous) = &op.receipt {
            if !matches!(
                (previous.phase, receipt.phase),
                (Phase::Armed, Phase::Quiesced) | (Phase::Quiesced, Phase::Activated)
            ) || previous.observed_mono_ns > receipt.observed_mono_ns
                || (previous.previous_applied.is_some()
                    && previous.previous_applied != receipt.previous_applied)
            {
                return Err(invalid("market receipt phase or watermark regression"));
            }
        } else if receipt.phase != Phase::Armed {
            return Err(invalid("market receipt before armed"));
        }
        if receipt.phase == Phase::Activated {
            let commit = op
                .commit
                .as_ref()
                .ok_or_else(|| invalid("activation without durable commit"))?;
            let bank = commit.validate(
                &op.approval,
                receipt
                    .previous_applied
                    .ok_or_else(|| invalid("activation watermark"))?,
                now,
            )?;
            if receipt.next_descriptor_hash != Some(bank.descriptor_hash) {
                return Err(invalid("activation descriptor mismatch"));
            }
            let effective = serde_json::to_vec(
                &serde_json::json!({"schema":1,"approval_hash":op.approval.digest()?,"commit":commit,"receipt":receipt}),
            )?;
            files::replace(
                &self
                    .config
                    .effective_directory
                    .join(format!("{}.json", c.stem())),
                &effective,
            )?;
            now = fresh(boundary, now)?;
            op.approval
                .validate_receipt(&receipt, c.pid, receipt.phase, now)?;
        }
        let bytes = serde_json::to_vec(&receipt)?;
        now = fresh(boundary, now)?;
        op.approval
            .validate_receipt(&receipt, c.pid, receipt.phase, now)?;
        files::replace(
            &self
                .config
                .consumer_directory
                .join(format!("{}.receipt.json", c.stem())),
            &bytes,
        )?;
        now = fresh(boundary, now)?;
        op.approval
            .validate_receipt(&receipt, c.pid, receipt.phase, now)?;
        if receipt.phase == Phase::Activated {
            if local {
                self.sources.as_ref().unwrap().release(
                    &op.approval,
                    op.commit.as_ref().unwrap(),
                    now,
                )?;
            }
            op.finished = true;
        }
        op.receipt = Some(receipt);
        Ok(())
    }
}

#[cfg(test)]
mod tests;

pub(super) const MAX_DECISION_BYTES: usize = MAX_CONTROL_BYTES + 2048;
pub(super) fn open_objects(journal: &std::path::Path) -> Result<tickwire_recovery::FileBlobStore> {
    let path = journal.join("market-objects");
    let store = tickwire_recovery::FileBlobStore::open(&path)?;
    files::directory(&path)?;
    Ok(store)
}
pub(super) fn read_decision(
    store: &tickwire_recovery::FileBlobStore,
    object: tickwire_release::Object,
) -> Result<Decision> {
    use tickwire_recovery::BlobStore;
    object.validate(MAX_DECISION_BYTES as u64)?;
    let bytes = store.get(
        tickwire_recovery::StoreKey::Object(object.digest),
        MAX_DECISION_BYTES,
    )?;
    if tickwire_release::Object::of(&bytes) != object {
        return Err(invalid("market decision object identity"));
    }
    Ok(serde_json::from_slice(&bytes)?)
}
