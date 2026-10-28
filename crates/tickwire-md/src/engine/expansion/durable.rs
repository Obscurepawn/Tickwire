use super::*;
use crate::recovery::{Consumed, Phase as RecoveryPhase, Snapshot, Store};
use tickwire_market_source::protocol::wire_bytes;

pub(super) struct Durable {
    pub(super) store: Option<Box<Store>>,
    pub(super) historical: BTreeMap<[u8; 16], [u8; 32]>,
    pub(super) ready: bool,
    pub(super) closed: bool,
    pub(super) report_phase: Option<RecoveryPhase>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum After {
    Startup,
    Reserve,
    Revoke,
    Applied,
    Complete,
    Report,
    Bank,
}

pub(super) fn open(
    control: &config::ExpansionControl,
    current: &Config,
    banks: &[ResourceHeader],
) -> Result<(Option<Durable>, config::FeedIndex)> {
    let Some(approved) = &control.recovery else {
        return Ok((None, config::FeedIndex::new(&current.feeds)?));
    };
    let mut store = Box::new(Store::open(&approved.directory)?);
    let historical;
    let index;
    if let Some(previous) = store.current() {
        if approved.restart_from != store.current_hash() {
            return Err(invalid("MD restart requires the exact current checkpoint"));
        }
        super::super::restart_validation::validate(previous, current, banks)?;
        if store.seen_instance(current.identity.instance)
            || banks.iter().any(|bank| {
                store.seen_resource(bank.resource_id) || store.seen_channel(bank.channel_id)
            })
            || current.feeds.iter().any(|feed| {
                store.seen_owner(feed.owner_id)
                    || feed
                        .events
                        .iter()
                        .any(|event| store.seen_owner(event.owner_id))
            })
        {
            return Err(invalid("MD restart reuses historical identities"));
        }
        index = config::FeedIndex::restore(&current.feeds, &previous.instrument_ids)?;
        historical = previous
            .operations
            .iter()
            .map(|op| (op.operation_id, op.approval_digest))
            .collect();
    } else {
        if approved.restart_from.is_some() {
            return Err(invalid("MD restart checkpoint is missing"));
        }
        historical = BTreeMap::new();
        index = config::FeedIndex::new(&current.feeds)?;
    }
    let mut reserved_owners = store
        .current()
        .map(|s| s.reserved_owners.iter().copied().collect::<BTreeSet<_>>())
        .unwrap_or_default();
    reserved_owners.extend(source_owners(current));
    let next = Snapshot {
        schema: 1,
        sequence: store.current().map_or(Ok(1), |s| {
            s.sequence
                .checked_add(1)
                .ok_or_else(|| invalid("MD recovery sequence"))
        })?,
        previous: store.current_hash().unwrap_or([0; 32]),
        phase: RecoveryPhase::Starting,
        revision: control.initial_revision,
        config_hash: crate::expansion::config_hash(current)?,
        config: current.clone(),
        instrument_ids: index.instrument_ids().to_vec(),
        banks: banks
            .iter()
            .map(wire_bytes)
            .collect::<std::result::Result<_, _>>()?,
        operations: historical
            .iter()
            .map(|(&operation_id, &approval_digest)| Consumed {
                operation_id,
                approval_digest,
            })
            .collect(),
        reserved_owners: reserved_owners.into_iter().collect(),
    };
    // Startup is a cold operation. Consume this approved head before allocating
    // resources or opening network sessions for the new incarnation.
    store.write(next)?;
    Ok((
        Some(Durable {
            store: Some(store),
            historical,
            ready: false,
            closed: false,
            report_phase: None,
        }),
        index,
    ))
}

impl Control {
    pub(super) fn recovery_ready(&self) -> bool {
        self.durable.as_ref().is_none_or(|d| d.ready)
    }
    pub(super) fn operation_room(&self, operation: [u8; 16]) -> bool {
        let mut ids = self.history.keys().copied().collect::<BTreeSet<_>>();
        if let Some(durable) = &self.durable {
            ids.extend(durable.historical.keys().copied());
        }
        ids.contains(&operation) || ids.len() < 128
    }
    pub(super) fn snapshot(&self, runtime: &mut Runtime, phase: RecoveryPhase) -> Result<Snapshot> {
        let store = self
            .durable
            .as_ref()
            .and_then(|d| d.store.as_ref())
            .ok_or_else(|| invalid("MD recovery owner busy"))?;
        let old = store
            .current()
            .ok_or_else(|| invalid("MD recovery head missing"))?;
        let mut operations = old
            .operations
            .iter()
            .map(|op| (op.operation_id, op.approval_digest))
            .collect::<BTreeMap<_, _>>();
        for (id, record) in &self.history {
            // Conflicting retries do not replace the already consumed digest.
            operations.entry(*id).or_insert(record.approval_digest);
        }
        let mut reserved_owners = old.reserved_owners.iter().copied().collect::<BTreeSet<_>>();
        reserved_owners.extend(source_owners(&runtime.config));
        if let Some(flight) = &self.flight {
            reserved_owners.extend(source_owners(&flight.plan.target_config));
        }
        let mut banks = Vec::with_capacity(runtime.topics.len());
        for (i, topic) in runtime.topics.iter_mut().enumerate() {
            let actual = topic.split().0.header();
            let reserved = ResourceHeader::decode(&old.banks[i])?;
            // A durable reservation remains consumed even if allocation failed.
            banks.push(if reserved.ring_epoch > actual.ring_epoch {
                old.banks[i].clone()
            } else {
                wire_bytes(actual)?
            });
        }
        Ok(Snapshot {
            schema: 1,
            sequence: old
                .sequence
                .checked_add(1)
                .ok_or_else(|| invalid("MD recovery sequence"))?,
            previous: store
                .current_hash()
                .ok_or_else(|| invalid("MD recovery head missing"))?,
            phase,
            revision: runtime.revision,
            config_hash: crate::expansion::config_hash(&runtime.config)?,
            config: runtime.config.clone(),
            instrument_ids: runtime.index.instrument_ids().to_vec(),
            banks,
            operations: operations
                .into_iter()
                .map(|(operation_id, approval_digest)| Consumed {
                    operation_id,
                    approval_digest,
                })
                .collect(),
            reserved_owners: reserved_owners.into_iter().collect(),
        })
    }
    pub(super) fn persist(
        &mut self,
        after: After,
        phase: RecoveryPhase,
        runtime: &mut Runtime,
    ) -> Result<()> {
        let snapshot = self.snapshot(runtime, phase)?;
        self.persist_snapshot(after, snapshot)
    }
    fn persist_snapshot(&mut self, after: After, snapshot: Snapshot) -> Result<()> {
        let store = self
            .durable
            .as_mut()
            .and_then(|d| d.store.take())
            .ok_or_else(|| invalid("MD recovery owner busy"))?;
        self.submit(
            Action::Persist(after),
            cold::Task::Persist {
                store,
                snapshot: Box::new(snapshot),
            },
        )
    }
    pub(super) fn startup_ready(&mut self, runtime: &mut Runtime) -> Result<bool> {
        if self.recovery_ready() {
            return Ok(true);
        }
        if !self.worker.busy()
            && runtime
                .topics
                .iter()
                .all(|t| t.state().live() && !t.needs_bank())
        {
            let effective = Effective {
                schema: 1,
                revision: runtime.revision,
                config_hash: crate::expansion::config_hash(&runtime.config)?,
                config: runtime.config.clone(),
            };
            self.submit(
                Action::StartupEffective,
                cold::Task::Write(vec![(
                    self.config.effective_path.clone(),
                    encode(&effective, crate::expansion::MAX_PLAN_BYTES)?,
                )]),
            )?;
        }
        Ok(false)
    }

    /// Reserve fault-replacement identity before its first allocation. This
    /// shares the single cold owner with expansion; a busy owner postpones it.
    pub(in crate::engine) fn reserve_bank(
        &mut self,
        topic: usize,
        bank: &ResourceHeader,
    ) -> Result<bool> {
        let Some(durable) = &self.durable else {
            return Ok(true);
        };
        if durable.closed || self.worker.busy() || self.flight.is_some() || self.reporting.is_some()
        {
            return Ok(false);
        }
        let store = durable
            .store
            .as_ref()
            .ok_or_else(|| invalid("MD recovery owner missing"))?;
        let old = store
            .current()
            .ok_or_else(|| invalid("MD recovery head missing"))?;
        if !matches!(old.phase, RecoveryPhase::Starting | RecoveryPhase::Ready) {
            return Ok(false);
        }
        let bytes = wire_bytes(bank)?;
        if old.banks.get(topic) == Some(&bytes) {
            return Ok(true);
        }
        let previous = ResourceHeader::decode(
            old.banks
                .get(topic)
                .ok_or_else(|| invalid("MD recovery topic"))?,
        )?;
        if bank.ring_epoch <= previous.ring_epoch
            || bank.feed_generation <= previous.feed_generation
            || store.seen_resource(bank.resource_id)
        {
            return Err(invalid("MD fault replacement reuses a durable reservation"));
        }
        let mut next = old.clone();
        next.sequence = next
            .sequence
            .checked_add(1)
            .ok_or_else(|| invalid("MD recovery sequence"))?;
        next.previous = store
            .current_hash()
            .ok_or_else(|| invalid("MD recovery head missing"))?;
        next.banks[topic] = bytes;
        self.persist_snapshot(After::Bank, next)?;
        Ok(false)
    }
}
fn source_owners(config: &Config) -> impl Iterator<Item = [u8; 16]> + '_ {
    config.feeds.iter().flat_map(|feed| {
        std::iter::once(feed.owner_id).chain(feed.events.iter().map(|event| event.owner_id))
    })
}
