use super::*;
use sha2::{Digest, Sha256};

impl Account {
    pub fn strategy_orders_digest(&self, strategy: u64) -> v::Hash {
        let mut hash = Sha256::new();
        if !self.amendments.is_empty() {
            hash.update(b"tickwire.strategy-orders.v4");
        } else if self.conditional_state_version() == 3 {
            hash.update(b"tickwire.strategy-orders.v3");
        } else if self.structure_observed {
            hash.update(b"tickwire.strategy-orders.v2");
        } else {
            hash.update(b"tickwire.strategy-orders.v1");
        }
        hash.update(strategy.to_le_bytes());
        for order in self.orders.values().filter(|o| o.strategy == strategy) {
            hash.update(order.key.intent.epoch.to_le_bytes());
            hash.update(order.key.intent.intent_id.to_le_bytes());
            let client = order.key.client_id.as_str().as_bytes();
            hash.update((client.len() as u64).to_le_bytes());
            hash.update(client);
            hash.update(order.digest);
            hash.update(order.origin.instance);
            hash.update(order.origin.generation.to_le_bytes());
            hash.update(order.manager.instance);
            hash.update(order.manager.generation.to_le_bytes());
            hash.update(order.owner_revision.to_le_bytes());
        }
        if self.structure_observed {
            hash.update(self.structure_state_digest(Some(strategy)));
        }
        if !self.amendments.is_empty() {
            hash.update(self.amendment_state_digest(Some(strategy)));
        }
        hash.finalize().into()
    }
    pub fn handover(&self, operation: [u8; 16], revision: u64) -> Option<&StrategyHandover> {
        self.handovers.get(&(operation, revision))
    }
    pub fn apply_strategy_handover(&mut self, h: &StrategyHandover) -> Result<Applied> {
        let key = (h.operation_id, h.operation_revision);
        if let Some(old) = self.handovers.get(&key) {
            return if old == h {
                Ok(Applied::Duplicate)
            } else {
                Err(Error::Conflict)
            };
        }
        if h.scope != self.scope
            || h.account_epoch != self.epoch
            || h.writer_epoch == 0
            || h.operation_id == [0; 16]
            || h.operation_revision == 0
            || h.logical_strategy_id == 0
            || h.from.instance == [0; 16]
            || h.to.instance == [0; 16]
            || h.from.instance == h.to.instance
            || h.from.generation == 0
            || h.to.generation <= h.from.generation
            || h.checkpoint_digest == [0; 32]
            || h.input_seal_digest == [0; 32]
            || h.evaluated_at >= h.valid_until
        {
            return Err(Error::Identity);
        }
        if h.expected_ledger_sequence != self.sequence
            || h.expected_orders_digest != self.strategy_orders_digest(h.logical_strategy_id)
        {
            return Err(Error::Stale);
        }
        if self.handovers.len() >= MAX_STRATEGIES {
            return Err(Error::Capacity);
        }
        if !self.catalog.values().any(|p| {
            p.strategy_limits
                .iter()
                .any(|v| v.0 == h.logical_strategy_id)
        }) || self.orders.values().any(|o| {
            o.strategy == h.logical_strategy_id
                && (o.manager != h.from || o.owner_revision == u64::MAX)
        }) {
            return Err(Error::Identity);
        }
        // Check every fallible operation before changing ownership. No cash,
        // reservation, fill, native order identity, or attribution changes.
        let sequence = self.sequence.checked_add(1).ok_or(Error::Capacity)?;
        for order in self
            .orders
            .values_mut()
            .filter(|o| o.strategy == h.logical_strategy_id)
        {
            order.manager = h.to;
            order.owner_revision += 1;
        }
        self.handovers.insert(key, h.clone());
        self.sequence = sequence;
        Ok(Applied::New)
    }
}
