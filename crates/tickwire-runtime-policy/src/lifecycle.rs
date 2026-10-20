use super::*;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HandoverBinding {
    pub operation_id: [u8; 16],
    pub operation_revision: u64,
    pub members_digest: Hash,
    pub logical_strategy_id: u64,
    pub instance_id: [u8; 16],
    pub strategy_generation: u64,
    pub valid_until_ns: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HandoverGate {
    pub binding: HandoverBinding,
    pub risk_gate_revision: u64,
    pub handover_sequence: u64,
    pub handover_digest: Hash,
}
#[derive(Clone)]
pub(super) struct Slot {
    pub binding: HandoverBinding,
    pub risk_gate_revision: u64,
    pub handover_sequence: u64,
    pub handover_digest: Hash,
    pub private_revocation: u64,
    pub released: bool,
    allowed: bool,
}
impl Slot {
    fn image(&self) -> HandoverGate {
        HandoverGate {
            binding: self.binding.clone(),
            risk_gate_revision: self.risk_gate_revision,
            handover_sequence: self.handover_sequence,
            handover_digest: self.handover_digest,
        }
    }
}
pub(super) fn close_one(s: &mut State, strategy: u64) -> Result<()> {
    if let Some(h) = s.handovers.get_mut(&strategy) {
        h.released = false;
        h.allowed = false;
        h.risk_gate_revision = h.risk_gate_revision.checked_add(1).ok_or_else(|| {
            s.faulted = true;
            Error::Capacity
        })?;
    }
    Ok(())
}
pub(super) fn close_all(s: &mut State) -> Result<()> {
    for h in s.handovers.values_mut() {
        h.released = false;
        h.allowed = false;
        h.risk_gate_revision = h.risk_gate_revision.checked_add(1).ok_or_else(|| {
            s.faulted = true;
            Error::Capacity
        })?;
    }
    Ok(())
}
impl ControlPlane {
    pub fn strategy_grant(&self, strategy: u64) -> Result<Option<StrategyGrant>> {
        let s = self.0.try_borrow().map_err(|_| Error::Busy)?;
        Ok(s.strategies.get(&strategy).and_then(|s| s.current.clone()))
    }
    /// Closes both old strategy authority and the new risk gate synchronously.
    /// The lifecycle owner persists the operation before acknowledging success.
    pub fn begin_handover(&self, binding: HandoverBinding) -> Result<HandoverGate> {
        if binding.operation_id == [0; 16]
            || binding.operation_revision == 0
            || binding.members_digest == [0; 32]
            || binding.logical_strategy_id == 0
            || binding.instance_id == [0; 16]
            || binding.strategy_generation == 0
            || binding.valid_until_ns == 0
        {
            return Err(Error::Identity);
        }
        let mut s = self.0.try_borrow_mut().map_err(|_| Error::Busy)?;
        let id = binding.logical_strategy_id;
        if let Some(old) = s.handovers.get(&id) {
            if old.binding == binding && old.allowed {
                return Ok(old.image());
            }
            if old.binding.operation_id == binding.operation_id
                && binding.operation_revision <= old.binding.operation_revision
            {
                return Err(Error::Stale);
            }
            if binding.strategy_generation <= old.binding.strategy_generation {
                return Err(Error::Stale);
            }
        } else if s.handovers.len() >= s.config.max_strategies {
            return Err(Error::Capacity);
        }
        if let Some(old) = s.strategies.get(&id)
            && (binding.strategy_generation <= old.highest_generation
                || binding.instance_id == old.last_instance)
        {
            return Err(Error::Stale);
        }
        let revision = s
            .handovers
            .get(&id)
            .map_or(Some(1), |h| h.risk_gate_revision.checked_add(1))
            .ok_or(Error::Capacity)?;
        let slot = Slot {
            binding,
            risk_gate_revision: revision,
            handover_sequence: 0,
            handover_digest: [0; 32],
            private_revocation: s.private.revocation,
            released: false,
            allowed: true,
        };
        if let Some(old) = s.strategies.get_mut(&id) {
            old.current = None;
            old.generation_revoked = true;
        }
        let image = slot.image();
        s.handovers.insert(id, slot);
        Ok(image)
    }
    pub fn handover_gate(&self, strategy: u64) -> Result<Option<HandoverGate>> {
        let s = self.0.try_borrow().map_err(|_| Error::Busy)?;
        Ok(s.handovers.get(&strategy).map(Slot::image))
    }
    pub fn handover_is_current(&self, expected: &HandoverGate, now: MonoTime) -> Result<bool> {
        let s = self.0.try_borrow().map_err(|_| Error::Busy)?;
        Ok(!s.faulted
            && now.domain() == s.config.domain
            && s.handovers
                .get(&expected.binding.logical_strategy_id)
                .is_some_and(|h| {
                    h.allowed
                        && h.image() == *expected
                        && h.private_revocation == s.private.revocation
                        && now.nanos() < h.binding.valid_until_ns
                }))
    }
    pub fn handover_is_released(&self, expected: &HandoverGate, now: MonoTime) -> Result<bool> {
        if !self.handover_is_current(expected, now)? {
            return Ok(false);
        }
        let s = self.0.try_borrow().map_err(|_| Error::Busy)?;
        Ok(s.handovers
            .get(&expected.binding.logical_strategy_id)
            .is_some_and(|h| h.released))
    }
    /// Called only after the account owner synchronized StrategyHandover.
    pub fn handover_committed(
        &self,
        expected: &HandoverGate,
        sequence: u64,
        digest: Hash,
    ) -> Result<HandoverGate> {
        if sequence == 0 || digest == [0; 32] {
            return Err(Error::Identity);
        }
        let mut s = self.0.try_borrow_mut().map_err(|_| Error::Busy)?;
        let h = s
            .handovers
            .get_mut(&expected.binding.logical_strategy_id)
            .ok_or(Error::Identity)?;
        if !h.allowed || h.released {
            return Err(Error::Denied);
        }
        if h.handover_sequence == sequence && h.handover_digest == digest {
            return Ok(h.image());
        }
        if h.image() != *expected || h.handover_sequence != 0 {
            return Err(Error::Stale);
        }
        h.handover_sequence = sequence;
        h.handover_digest = digest;
        Ok(h.image())
    }
    /// A verified group release compares the complete local gate, then installs
    /// its new grant. A close/revoke or newer operation makes old releases stale.
    pub fn release_handover(
        &self,
        expected: &HandoverGate,
        grant: StrategyGrant,
        now: MonoTime,
    ) -> Result<()> {
        {
            let s = self.0.try_borrow().map_err(|_| Error::Busy)?;
            let h = s
                .handovers
                .get(&expected.binding.logical_strategy_id)
                .ok_or(Error::Identity)?;
            if s.faulted
                || now.domain() != s.config.domain
                || !h.allowed
                || h.image() != *expected
                || h.handover_sequence == 0
                || h.handover_digest == [0; 32]
                || now.nanos() >= h.binding.valid_until_ns
                || h.private_revocation != s.private.revocation
                || grant.logical_strategy_id != h.binding.logical_strategy_id
                || grant.instance_id != h.binding.instance_id
                || grant.strategy_generation != h.binding.strategy_generation
                || grant.valid_until_ns > h.binding.valid_until_ns
                || now.nanos() >= grant.valid_until_ns
            {
                return Err(Error::Stale);
            }
        }
        self.install_strategy_grant(grant)?;
        self.0
            .try_borrow_mut()
            .map_err(|_| Error::Busy)?
            .handovers
            .get_mut(&expected.binding.logical_strategy_id)
            .ok_or(Error::Identity)?
            .released = true;
        Ok(())
    }
}
