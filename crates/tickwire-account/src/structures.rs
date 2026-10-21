use super::*;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Kind {
    List,
    Algo,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Structure {
    intent: IntentKey,
    instrument: u64,
    list: bool,
    kind: v::StructureKind,
    command_digest: v::Hash,
    client: v::ClientId,
    native: Option<v::VenueOrderId>,
    legs: Vec<OrderKey>,
    children: BTreeMap<v::ClientId, v::StructureChild>,
    confirmed: bool,
    triggered: bool,
    terminal: bool,
    unknown: bool,
    closure: Option<Box<UncreatedAlgoClosed>>,
}

#[derive(Debug, Clone)]
pub(super) struct Management {
    structure: [u8; 16],
    original_epoch: u64,
    instrument: u64,
    list: bool,
    client: Option<v::ClientId>,
    native: Option<v::VenueOrderId>,
    revision: u64,
    strategy: u64,
    manager: ManagerIdentity,
}

impl Account {
    pub fn structure(&self, id: &[u8; 16]) -> Option<StructureView> {
        let s = self.structures.get(id)?;
        Some(StructureView {
            structure_id: *id,
            intent: s.intent,
            kind: s.kind,
            command_digest: s.command_digest,
            venue_parent_id: s.native.clone(),
            confirmed: s.confirmed,
            triggered: s.triggered,
            terminal: s.terminal,
            unknown: s.unknown || s.legs.iter().any(|k| self.orders[k].recovery_unknown),
            legs: s.legs.clone(),
            children: s.children.values().cloned().collect(),
        })
    }
    pub fn structures(&self) -> impl Iterator<Item = StructureView> + '_ {
        self.structures.keys().filter_map(|id| self.structure(id))
    }
    /// Version changes only after a durable parent observation has changed state.
    pub fn conditional_state_version(&self) -> u16 {
        if self.structures.values().any(|s| s.closure.is_some()) {
            3
        } else if self.structure_observed {
            2
        } else {
            1
        }
    }

    pub(super) fn structure_state_digest(&self, strategy: Option<u64>) -> v::Hash {
        use sha2::{Digest, Sha256};
        fn text(h: &mut Sha256, v: &str) {
            h.update((v.len() as u64).to_le_bytes());
            h.update(v.as_bytes());
        }
        fn optional(h: &mut Sha256, v: Option<&str>) {
            h.update([u8::from(v.is_some())]);
            if let Some(v) = v {
                text(h, v)
            }
        }
        let selected: Vec<_> = self
            .structures
            .iter()
            .filter(|(_, s)| strategy.is_none_or(|id| self.orders[&s.legs[0]].strategy == id))
            .collect();
        let mut h = Sha256::new();
        if self.conditional_state_version() == 3 {
            h.update(b"tickwire.conditional-state.v2");
        } else {
            h.update(b"tickwire.conditional-state.v1");
        }
        h.update((selected.len() as u64).to_le_bytes());
        for (id, s) in selected {
            h.update(id);
            h.update(s.intent.epoch.to_le_bytes());
            h.update(s.intent.intent_id.to_le_bytes());
            h.update(s.instrument.to_le_bytes());
            h.update([match s.kind {
                v::StructureKind::Oco => 1,
                v::StructureKind::Oto => 2,
                v::StructureKind::Otoco => 3,
                v::StructureKind::Algo => 4,
            }]);
            h.update(s.command_digest);
            text(&mut h, s.client.as_str());
            optional(&mut h, s.native.as_ref().map(|v| v.as_str()));
            h.update([
                u8::from(s.confirmed),
                u8::from(s.triggered),
                u8::from(s.terminal),
                u8::from(s.unknown),
            ]);
            h.update((s.legs.len() as u64).to_le_bytes());
            for key in &s.legs {
                if self.conditional_state_version() == 3 {
                    h.update([u8::from(self.orders[key].uncreated)]);
                }
                h.update(key.intent.epoch.to_le_bytes());
                h.update(key.intent.intent_id.to_le_bytes());
                text(&mut h, key.client_id.as_str());
            }
            if self.conditional_state_version() == 3 {
                h.update([u8::from(s.closure.is_some())]);
                if let Some(c) = &s.closure {
                    h.update(c.coverage_digest);
                    h.update(c.history_digest);
                    h.update(c.state_digest);
                    h.update(c.ledger_sequence.to_le_bytes());
                }
            }
            h.update((s.children.len() as u64).to_le_bytes());
            for (client, child) in &s.children {
                text(&mut h, client.as_str());
                h.update(child.order.order_id);
                optional(
                    &mut h,
                    child.order.venue_order_id.as_ref().map(|v| v.as_str()),
                );
                optional(
                    &mut h,
                    child.reported_client_id.as_ref().map(|v| v.as_str()),
                );
            }
        }
        h.finalize().into()
    }
    pub(super) fn register_structure(
        &mut self,
        command: &v::Command,
        orders: &[OrderKey],
    ) -> Result<()> {
        let (id, client, kind) = match &command.body {
            v::CommandBody::SpotList(l) => (
                l.list_id,
                &l.client_id,
                match l.kind {
                    v::ListKind::Oco => v::StructureKind::Oco,
                    v::ListKind::Oto => v::StructureKind::Oto,
                    v::ListKind::Otoco => v::StructureKind::Otoco,
                },
            ),
            v::CommandBody::Algo(a) => (
                a.parent_order_id,
                &a.order.client_id,
                v::StructureKind::Algo,
            ),
            _ => return Ok(()),
        };
        let list = kind != v::StructureKind::Algo;
        let r = &command.context.request;
        let instrument = command.context.instrument.instrument_id;
        if self.structures.contains_key(&id)
            || self
                .structures
                .values()
                .any(|s| s.instrument == instrument && s.list == list && &s.client == client)
        {
            return Err(Error::Conflict);
        }
        self.structures.insert(
            id,
            Structure {
                intent: IntentKey {
                    epoch: r.intent_exec_epoch,
                    intent_id: r.intent_id,
                },
                instrument,
                list,
                kind,
                command_digest: self.orders[&orders[0]].digest,
                client: client.clone(),
                native: None,
                legs: orders.to_vec(),
                children: BTreeMap::new(),
                confirmed: false,
                triggered: false,
                terminal: false,
                unknown: false,
                closure: None,
            },
        );
        Ok(())
    }

    pub(super) fn structure_management(&self, command: &v::Command) -> Result<Management> {
        let (structure, original_epoch, client, native, revision, kind) = match &command.body {
            v::CommandBody::CancelList(t) => (
                t.list_id,
                t.original_intent_exec_epoch,
                &t.client_id,
                &t.venue_list_id,
                t.expected_owner_revision,
                Kind::List,
            ),
            v::CommandBody::CancelAlgo(t) => (
                t.parent_order_id,
                t.original_intent_exec_epoch,
                &t.client_id,
                &t.venue_algo_id,
                t.expected_owner_revision,
                Kind::Algo,
            ),
            _ => return Err(Error::Identity),
        };
        let check = Management {
            structure,
            original_epoch,
            client: client.clone(),
            native: native.clone(),
            revision,
            list: kind == Kind::List,
            instrument: command.context.instrument.instrument_id,
            strategy: command.context.strategy.logical_strategy_id,
            manager: ManagerIdentity {
                instance: command.context.strategy.instance_id,
                generation: command.context.request.strategy_generation,
            },
        };
        self.check_structure_management(&check)?;
        Ok(check)
    }

    pub(super) fn check_structure_management(&self, check: &Management) -> Result<()> {
        let s = self
            .structures
            .get(&check.structure)
            .ok_or(Error::UnknownOrder)?;
        if s.instrument != check.instrument
            || s.intent.epoch != check.original_epoch
            || s.list != check.list
            || check.revision == 0
            || check.client.as_ref().is_some_and(|id| id != &s.client)
            || check
                .native
                .as_ref()
                .is_some_and(|id| s.native.as_ref() != Some(id))
            || (check.client.is_none() && check.native.is_none())
            || s.legs.is_empty()
        {
            return Err(Error::Identity);
        }
        for key in &s.legs {
            let order = self.orders.get(key).ok_or(Error::UnknownOrder)?;
            if order.strategy != check.strategy
                || order.manager != check.manager
                || order.owner_revision != check.revision
            {
                return Err(Error::Identity);
            }
        }
        Ok(())
    }

    pub(super) fn observe_structure(
        &mut self,
        observed: &v::StructureObservation,
        instrument: u64,
    ) -> Result<Applied> {
        let p = &observed.parent;
        let old = self
            .structures
            .get(&p.structure_id)
            .ok_or(Error::UnknownOrder)?
            .clone();
        if old.instrument != instrument
            || old.kind != p.kind
            || old.client != p.client_id
            || old.intent
                != (IntentKey {
                    epoch: p.original_intent_exec_epoch,
                    intent_id: p.original_intent_id,
                })
            || old
                .native
                .as_ref()
                .zip(p.venue_parent_id.as_ref())
                .is_some_and(|(a, b)| a != b)
        {
            return Err(Error::Identity);
        }
        if let Some(native) = &p.venue_parent_id
            && self.structures.iter().any(|(id, s)| {
                *id != p.structure_id
                    && s.instrument == instrument
                    && s.list == old.list
                    && s.native.as_ref() == Some(native)
            })
        {
            return Err(Error::Conflict);
        }
        let prior_orders: Vec<_> = old.legs.iter().map(|k| self.orders[k].clone()).collect();
        let mut next = old.clone();
        if p.venue_parent_id.is_some() {
            next.native = p.venue_parent_id.clone();
        }
        next.confirmed = true;
        next.triggered |= observed.status == v::StructureStatus::Triggered
            || !observed.children.as_slice().is_empty();
        next.terminal |= matches!(
            observed.status,
            v::StructureStatus::Terminal | v::StructureStatus::Rejected
        );
        next.unknown |= observed.status == v::StructureStatus::Unknown;
        for child in observed.children.as_slice() {
            let id = &child.order;
            let key = OrderKey {
                intent: old.intent,
                client_id: id.client_id.clone().ok_or(Error::Identity)?,
            };
            if !old.legs.contains(&key) {
                return Err(Error::UnknownOrder);
            }
            if let Some(prior) = next.children.get(&key.client_id)
                && (prior.order != child.order
                    || prior
                        .reported_client_id
                        .as_ref()
                        .zip(child.reported_client_id.as_ref())
                        .is_some_and(|(a, b)| a != b))
            {
                return Err(Error::Conflict);
            }
            if self.orders.iter().any(|(other, o)| {
                *other != key
                    && ((o.instrument == instrument
                        && o.native.is_some()
                        && o.native == id.venue_order_id)
                        || o.internal_id == Some(id.order_id))
            }) {
                return Err(Error::Conflict);
            }
            self.bind_native(&key, id)?;
            let mut merged = child.clone();
            if merged.reported_client_id.is_none() {
                merged.reported_client_id = next
                    .children
                    .get(&key.client_id)
                    .and_then(|p| p.reported_client_id.clone());
            }
            next.children.insert(key.client_id, merged);
        }
        for key in &old.legs {
            let o = self.orders.get_mut(key).unwrap();
            o.confirmed = true;
            if (!o.uncreated
                || !observed.children.as_slice().is_empty()
                || matches!(
                    observed.status,
                    v::StructureStatus::Live
                        | v::StructureStatus::Triggered
                        | v::StructureStatus::Unknown
                        | v::StructureStatus::Pending
                ))
                && o.reopen_aborted()?
            {
                self.frozen = true;
            }
            if next.unknown {
                o.observed_unknown = true;
                self.frozen = true;
            }
        }
        let changed = next != old
            || !self.structure_observed
            || prior_orders.iter().any(|o| self.orders[&o.key] != *o);
        if !changed {
            return Ok(Applied::Duplicate);
        }
        self.structures.insert(p.structure_id, next);
        self.structure_observed = true;
        self.next_sequence()?;
        if self.evaluate().is_err() {
            self.frozen = true;
        }
        self.check_reduce_consistency()?;
        Ok(Applied::New)
    }
}

impl Account {
    /// Deterministic reducer only: the owner must verify and persist the complete
    /// parent query/snapshot evidence before publishing this result.
    pub fn uncreated_algo_closure(&self, id: &[u8; 16]) -> Option<&UncreatedAlgoClosed> {
        self.structures.get(id)?.closure.as_deref()
    }
    pub fn close_uncreated_algo(&mut self, b: &UncreatedAlgoClosed) -> Result<Applied> {
        if let Some(old) = self.uncreated_algo_closure(&b.structure_id) {
            return if old == b {
                Ok(Applied::Duplicate)
            } else {
                self.freeze_error(Error::Conflict)
            };
        }
        if b.scope != self.scope
            || b.account_epoch != self.epoch
            || b.ledger_sequence != self.sequence
            || b.input_stream == [0; 16]
            || b.through_sequence == 0
            || b.coverage_digest == [0; 32]
            || self.account_inputs_state_digest(b.baseline_digest, b.history_digest)?
                != b.state_digest
        {
            return self.freeze_error(Error::Identity);
        }
        let s = self
            .structures
            .get(&b.structure_id)
            .ok_or(Error::UnknownOrder)?;
        if s.kind != v::StructureKind::Algo
            || s.intent != b.intent
            || s.command_digest != b.command_digest
            || s.native.as_ref() != Some(&b.venue_parent_id)
            || !s.confirmed
            || !s.terminal
            || s.triggered
            || s.unknown
            || !s.children.is_empty()
            || s.legs.len() != 1
        {
            return self.freeze_error(Error::Unreconciled);
        }
        let key = s.legs[0].clone();
        let order = &self.orders[&key];
        if order.aborted
            || order.uncreated
            || order.economic
            || order.terminal
            || order.filled.atoms() != 0
            || !order.paid.is_empty()
            || order.native.is_some()
            || order.observed_unknown
            || order.observation_unreconciled
        {
            return self.freeze_error(Error::Unreconciled);
        }
        let mut candidate = self.clone();
        let o = candidate.orders.get_mut(&key).unwrap();
        o.uncreated = true;
        o.fees_closed = true;
        o.reduce = o.reduce.zero_like();
        candidate
            .structures
            .get_mut(&b.structure_id)
            .unwrap()
            .closure = Some(Box::new(b.clone()));
        candidate.structure_observed = true;
        candidate.next_sequence()?;
        candidate.frozen = true;
        candidate.evaluate()?;
        *self = candidate;
        Ok(Applied::New)
    }
}
