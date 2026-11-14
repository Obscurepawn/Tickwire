use super::*;

#[derive(Debug, Clone)]
pub(super) struct Amendment {
    pub order: OrderKey,
    pub command_digest: v::Hash,
    pub manager: ManagerIdentity,
    pub owner_revision: u64,
    pub before: Fixed,
    pub after: Fixed,
    pub aborted: bool,
    pub confirmed: bool,
    pub rejected: Option<v::BoundedText<32>>,
    pub executions: BTreeSet<(u8, u64)>,
    pub retired_mono_ns: Option<u64>,
}
impl PartialEq for Amendment {
    fn eq(&self, other: &Self) -> bool {
        self.order == other.order
            && self.command_digest == other.command_digest
            && self.manager == other.manager
            && self.owner_revision == other.owner_revision
            && self.before == other.before
            && self.after == other.after
            && self.aborted == other.aborted
            && self.confirmed == other.confirmed
            && self.rejected == other.rejected
            && self.executions == other.executions
    }
}
impl Eq for Amendment {}

impl Account {
    pub(super) fn reject_amendment(
        &mut self,
        a: &v::AmendmentRejection,
        instrument: u64,
    ) -> Result<Applied> {
        let key = IntentKey {
            epoch: a.request_intent_exec_epoch,
            intent_id: a.request_intent_id,
        };
        let pending = self.amendments.get(&key).ok_or(Error::UnknownOrder)?;
        let o = &self.orders[&pending.order];
        if pending.command_digest != a.request_digest
            || o.instrument != instrument
            || pending.order.intent.epoch != a.order.original_intent_exec_epoch
            || pending.order.intent.intent_id != a.order.original_intent_id
            || a.order.client_id.as_ref() != Some(&pending.order.client_id)
            || a.order.venue_order_id != o.native
            || Some(a.order.order_id) != o.internal_id
        {
            return Err(Error::Identity);
        }
        if pending.confirmed || pending.aborted {
            return Err(Error::Conflict);
        }
        if let Some(old) = &pending.rejected {
            return if old == &a.venue_code {
                if self.orders[&pending.order].in_doubt == Some(DoubtReason::AmendAmbiguous) {
                    let order_key = pending.order.clone();
                    self.orders.get_mut(&order_key).unwrap().in_doubt = None;
                    let stamp = self.last_mono_ns;
                    self.refresh_order_retirement(&order_key, stamp);
                }
                Ok(Applied::Duplicate)
            } else {
                Err(Error::Conflict)
            };
        }
        let order_key = pending.order.clone();
        self.next_sequence()?;
        let stamp = self.last_mono_ns;
        if self.orders[&order_key].in_doubt == Some(DoubtReason::AmendAmbiguous) {
            self.orders.get_mut(&order_key).unwrap().in_doubt = None;
        }
        let entry = self.amendments.get_mut(&key).unwrap();
        entry.rejected = Some(a.venue_code.clone());
        entry.retired_mono_ns = Some(stamp);
        if let Some(adm) = self.admissions.get_mut(&key) {
            adm.retired_mono_ns = Some(stamp);
        }
        self.refresh_order_retirement(&order_key, stamp);
        Ok(Applied::New)
    }
    pub(super) fn observe_amendment(
        &mut self,
        a: &v::AmendmentObservation,
        instrument: u64,
    ) -> Result<Applied> {
        let request = IntentKey {
            epoch: a.request_intent_exec_epoch,
            intent_id: a.request_intent_id,
        };
        let old = self
            .amendments
            .get(&request)
            .ok_or(Error::UnknownOrder)?
            .clone();
        let order = self.orders.get(&old.order).ok_or(Error::UnknownOrder)?;
        let before = exact_decimal(a.previous_quantity, order.original.scale())?;
        let after = exact_decimal(a.new_quantity, order.original.scale())?;
        if order.instrument != instrument
            || a.request_digest != old.command_digest
            || before != old.before
            || after != old.after
            || a.order.original_intent_id != old.order.intent.intent_id
            || a.order.original_intent_exec_epoch != old.order.intent.epoch
            || a.order.client_id.as_ref() != Some(&old.order.client_id)
            || a.order.venue_order_id != order.native
            || Some(a.order.order_id) != order.internal_id
        {
            return Err(Error::Identity);
        }
        let source = match a.execution_source {
            v::AmendmentSource::RestResponse => 1,
            v::AmendmentSource::RestHistory => 2,
            v::AmendmentSource::PrivateStream => 3,
        };
        let execution = (source, a.execution_id);
        if self.amendments.iter().any(|(key, other)| {
            *key != request && other.order == old.order && other.executions.contains(&execution)
        }) {
            return Err(Error::Conflict);
        }
        if !old.executions.contains(&execution)
            && self
                .amendments
                .values()
                .map(|x| x.executions.len())
                .sum::<usize>()
                >= MAX_FACTS
        {
            return Err(Error::Capacity);
        }
        let prior_order = order.clone();
        if !old.confirmed {
            let reopened = self.reopen_order(&old.order)?;
            let o = self.orders.get_mut(&old.order).unwrap();
            if old.aborted
                || old.rejected.is_some()
                || o.effective != before
                || reopened
                || o.filled.atoms() > after.atoms()
            {
                o.observation_unreconciled = true;
                self.frozen = true;
            }
            // A later, already observed decrease cannot be undone by a delayed result.
            o.effective = o.effective.min(after)?;
            o.confirmed = true;
        }
        let now = self.amendments.get_mut(&request).unwrap();
        now.confirmed = true;
        now.executions.insert(execution);
        if let Some(observation) = &a.observation {
            self.observe(&old.order, observation)?;
        }
        if self.orders[&old.order].in_doubt == Some(DoubtReason::AmendAmbiguous) {
            self.orders.get_mut(&old.order).unwrap().in_doubt = None;
        }
        if self.amendments[&request] == old && self.orders[&old.order] == prior_order {
            if self.orders[&old.order].in_doubt != prior_order.in_doubt {
                let stamp = self.last_mono_ns;
                self.refresh_order_retirement(&old.order, stamp);
            }
            return Ok(Applied::Duplicate);
        }
        let stamp = self.last_mono_ns;
        if let Some(entry) = self.amendments.get_mut(&request)
            && entry.retired_mono_ns.is_none()
        {
            entry.retired_mono_ns = Some(stamp);
        }
        if let Some(adm) = self.admissions.get_mut(&request)
            && adm.retired_mono_ns.is_none()
        {
            adm.retired_mono_ns = Some(stamp);
        }
        self.sync_order_in_stp_book(&old.order)?;
        self.refresh_order_retirement(&old.order, stamp);
        self.next_sequence()?;
        if self.evaluate().is_err() {
            self.frozen = true;
        }
        Ok(Applied::New)
    }
    pub(super) fn prepare_amendment(&self, command: &v::CanonicalCommand) -> Result<Amendment> {
        let c = command.command();
        let v::CommandBody::AmendKeepPriority {
            target,
            new_quantity,
            new_client_id,
        } = &c.body
        else {
            return Err(Error::Unsupported);
        };
        if c.context.request.product != v::Product::Spot {
            return Err(Error::Unsupported);
        }
        let manager = ManagerIdentity {
            instance: c.context.strategy.instance_id,
            generation: c.context.request.strategy_generation,
        };
        let o = target
            .client_id
            .as_ref()
            .and_then(|cid| {
                self.orders_by_client_id
                    .get(&(c.context.instrument.instrument_id, cid.clone()))
            })
            .and_then(|keys| {
                keys.iter().filter_map(|k| self.orders.get(k)).find(|o| {
                    o.instrument == c.context.instrument.instrument_id
                        && o.key.intent.epoch == target.original_intent_exec_epoch
                        && o.internal_id == Some(target.order_id)
                        && o.native.is_some()
                        && o.native == target.venue_order_id
                        && target.client_id.as_ref() == Some(&o.key.client_id)
                })
            })
            .ok_or(Error::UnknownOrder)?;
        if o.strategy != c.context.strategy.logical_strategy_id
            || o.manager != manager
            || o.owner_revision != target.expected_owner_revision
            || new_client_id.as_ref() != Some(&o.key.client_id)
        {
            return Err(Error::Identity);
        }
        if !o.amendable
            || o.spec.mode != v::ProductMode::Spot
            || o.spec.order_type != v::OrderType::Limit
            || o.spec.time_in_force != Some(v::TimeInForce::Gtc)
            || o.spec.iceberg_quantity.is_some()
            || o.spec.trigger.is_some()
            || o.spec.trailing.is_some()
            || self.structures().any(|s| s.legs.contains(&o.key))
        {
            return Err(Error::Unsupported);
        }
        if o.terminal
            || o.aborted
            || o.uncreated
            || !o.confirmed
            || o.observation_unreconciled
            || o.in_doubt.is_some()
        {
            return Err(Error::Unreconciled);
        }
        if o.pending_modify.is_some()
            || self
                .amendments
                .values()
                .any(|a| a.order == o.key && !a.aborted && !a.confirmed && a.rejected.is_none())
        {
            return Err(Error::TooManyPendingActions);
        }
        let after = exact_decimal(*new_quantity, o.original.scale())?;
        if after.atoms() <= o.filled.atoms() || after.atoms() >= o.effective.atoms() {
            return Err(Error::Limit);
        }
        Ok(Amendment {
            order: o.key.clone(),
            command_digest: command.digest(),
            manager,
            owner_revision: o.owner_revision,
            before: o.effective,
            after,
            aborted: false,
            confirmed: false,
            rejected: None,
            executions: BTreeSet::new(),
            retired_mono_ns: None,
        })
    }
    pub(super) fn check_amendment(&self, a: &Amendment) -> Result<()> {
        let o = self.orders.get(&a.order).ok_or(Error::UnknownOrder)?;
        if o.manager != a.manager || o.owner_revision != a.owner_revision {
            return Err(Error::Identity);
        }
        if a.aborted
            || a.confirmed
            || a.rejected.is_some()
            || !o.confirmed
            || o.observation_unreconciled
            || o.in_doubt.is_some()
            || o.terminal
            || o.aborted
            || o.uncreated
            || o.effective != a.before
            || o.filled.atoms() >= a.after.atoms()
        {
            return Err(Error::Unreconciled);
        }
        Ok(())
    }
    pub(super) fn abort_amendment(&mut self, key: IntentKey) -> Result<Applied> {
        let a = self.amendments.get(&key).ok_or(Error::UnknownOrder)?;
        if a.confirmed || a.rejected.is_some() {
            return self.freeze_error(Error::Unreconciled);
        }
        if a.aborted {
            return Ok(Applied::Duplicate);
        }
        let order_key = a.order.clone();
        self.next_sequence()?;
        let stamp = self.last_mono_ns;
        let entry = self.amendments.get_mut(&key).unwrap();
        entry.aborted = true;
        entry.retired_mono_ns = Some(stamp);
        if let Some(adm) = self.admissions.get_mut(&key) {
            adm.retired_mono_ns = Some(stamp);
        }
        self.refresh_order_retirement(&order_key, stamp);
        Ok(Applied::New)
    }
    pub(super) fn known_order_quantity(&self, key: &OrderKey, quantity: Fixed) -> bool {
        let o = &self.orders[key];
        quantity == o.original
            || quantity == o.effective
            || o.pending_modify
                .as_ref()
                .is_some_and(|pm| pm.requested_quantity == quantity)
            || o.known_quantities.contains(&quantity)
            || self.amendments.values().any(|a| {
                a.order == *key && !a.aborted && a.rejected.is_none() && a.after == quantity
            })
            || self.modifications.values().any(|m| {
                m.order == *key
                    && !m.aborted
                    && m.rejected.is_none()
                    && (m.requested_quantity == quantity || m.applied_quantity == Some(quantity))
            })
    }
    pub fn amendments(&self) -> impl Iterator<Item = AmendmentView> + '_ {
        self.amendments.iter().map(|(intent, a)| AmendmentView {
            intent: *intent,
            command_digest: a.command_digest,
            order: a.order.clone(),
            manager: a.manager,
            owner_revision: a.owner_revision,
            previous_quantity: a.before,
            new_quantity: a.after,
            aborted: a.aborted,
            confirmed: a.confirmed,
            rejected: a.rejected.clone(),
            executions: a
                .executions
                .iter()
                .map(|(source, id)| {
                    (
                        match source {
                            1 => v::AmendmentSource::RestResponse,
                            2 => v::AmendmentSource::RestHistory,
                            _ => v::AmendmentSource::PrivateStream,
                        },
                        *id,
                    )
                })
                .collect(),
        })
    }
    pub(super) fn amendment_state_digest(&self, strategy: Option<u64>) -> v::Hash {
        use sha2::{Digest, Sha256};
        fn text(h: &mut Sha256, value: &str) {
            h.update((value.len() as u64).to_le_bytes());
            h.update(value.as_bytes());
        }
        fn fixed(h: &mut Sha256, value: Fixed) {
            h.update(value.atoms().to_le_bytes());
            h.update([value.scale()]);
        }
        let selected: Vec<_> = self
            .amendments
            .iter()
            .filter(|(_, a)| strategy.is_none_or(|s| self.orders[&a.order].strategy == s))
            .collect();
        let mut h = Sha256::new();
        h.update(b"tickwire.amendment-state.v1");
        h.update((selected.len() as u64).to_le_bytes());
        for (intent, a) in selected {
            for value in [
                intent.epoch,
                intent.intent_id,
                a.order.intent.epoch,
                a.order.intent.intent_id,
            ] {
                h.update(value.to_le_bytes());
            }
            text(&mut h, a.order.client_id.as_str());
            h.update(a.command_digest);
            h.update(a.manager.instance);
            h.update(a.manager.generation.to_le_bytes());
            h.update(a.owner_revision.to_le_bytes());
            for value in [
                a.before,
                a.after,
                self.orders[&a.order].effective,
                self.orders[&a.order].observed_cumulative,
            ] {
                fixed(&mut h, value);
            }
            h.update([u8::from(a.aborted), u8::from(a.confirmed)]);
            h.update([u8::from(a.rejected.is_some())]);
            if let Some(code) = &a.rejected {
                text(&mut h, code.as_str());
            }
            h.update((a.executions.len() as u64).to_le_bytes());
            for (source, id) in &a.executions {
                h.update([*source]);
                h.update(id.to_le_bytes());
            }
        }
        h.finalize().into()
    }
}
