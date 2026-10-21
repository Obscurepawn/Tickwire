use super::*;
use sha2::{Digest, Sha256};

pub const MAX_CLOSURE_INCOMES: usize = 128;
const MAX_STATE_BYTES: usize = 16 * 1024 * 1024;

impl Account {
    /// The complete set this automatic transition can close. The owner must still
    /// verify exact authenticated income rows, input prefix and fresh snapshots.
    pub fn account_inputs_to_reconcile(&self) -> Result<Vec<v::CanonicalAccountInput>> {
        let state = &self.account_inputs;
        if state.rejected.is_some()
            || self.rejected_event.is_some()
            || state.observations.values().any(|v| *v)
            || state
                .incomes
                .values()
                .any(|e| e.status == IncomeStatus::Pending)
        {
            return Err(Error::Unreconciled);
        }
        let mut rows = Vec::new();
        for entry in state
            .incomes
            .values()
            .filter(|e| e.status == IncomeStatus::Booked)
        {
            let v::AccountInputBody::Income(row) = &entry.input.input().body else {
                return Err(Error::Identity);
            };
            if row.income_type.as_str() != "FUNDING_FEE"
                || !state
                    .baseline
                    .get(&entry.input.input().identity.wallet_id)
                    .is_some_and(|b| row.venue_time_ms > b.through_time_ms)
            {
                return Err(Error::Unreconciled);
            }
            if rows.len() == MAX_CLOSURE_INCOMES {
                return Err(Error::Capacity);
            }
            rows.push(entry.input.clone());
        }
        if rows.is_empty() {
            return Err(Error::Unreconciled);
        }
        Ok(rows)
    }
    pub fn account_inputs_closure(&self, history: v::Hash) -> Option<&AccountInputsReconciled> {
        self.account_inputs.closures.get(&history)
    }
    /// Deterministic state commitment; the caller supplies the actual pinned WAL
    /// history/baseline. Recovery-only unknown/frozen markers remain outside it.
    pub fn account_inputs_state_digest(
        &self,
        baseline: v::Hash,
        history: v::Hash,
    ) -> Result<v::Hash> {
        if baseline == [0; 32] || history == [0; 32] {
            return Err(Error::Identity);
        }
        let state = &self.account_inputs;
        if state.size() > MAX_FACTS
            || state.aliases.len() > state.components.len()
            || state.baseline.len() > MAX_ASSETS
            || state.caps.len() > MAX_ASSETS
            || state.credit_holds.len() > MAX_ASSETS
            || self.orders.len() > MAX_ORDERS
        {
            return Err(Error::Capacity);
        }
        let mut w = StateWriter::new();
        if !self.amendments.is_empty() {
            w.put(b"tickwire.account-input-state.v4");
        } else if self.conditional_state_version() == 3 {
            w.put(b"tickwire.account-input-state.v3");
        } else if self.structure_observed {
            w.put(b"tickwire.account-input-state.v2");
        } else {
            w.put(b"tickwire.account-input-state.v1");
        }
        w.put(
            &(if self.amendments.is_empty() {
                self.conditional_state_version()
            } else {
                4_u16
            })
            .to_le_bytes(),
        );
        w.put(&self.scope.risk_domain_id);
        w.put(&self.scope.venue.to_le_bytes());
        w.byte(match self.scope.environment {
            v::Environment::Production => 1,
            v::Environment::Test => 2,
        });
        w.uint(self.scope.account);
        w.uint(self.epoch);
        w.uint(self.sequence);
        w.put(&baseline);
        w.put(&history);
        w.section(1, state.baseline.len());
        for (wallet, b) in &state.baseline {
            if b.scope != self.scope
                || b.account_epoch != self.epoch
                || b.covered.len() > MAX_BASELINE_INCOMES
            {
                return Err(Error::Identity);
            }
            w.uint(*wallet);
            w.uint(b.ledger_sequence);
            w.uint(b.through_time_ms);
            w.put(&b.coverage_digest);
            w.put(&b.baseline_digest);
            w.put(&b.input_stream);
            w.uint(b.through_sequence);
            let mut covered = BTreeMap::new();
            for input in &b.covered {
                let v::AccountInputBody::Income(row) = &input.input().body else {
                    return Err(Error::Identity);
                };
                if covered.insert(income_key(row), input).is_some() {
                    return Err(Error::Conflict);
                }
            }
            w.count(covered.len());
            for (key, input) in covered {
                w.income_key(&key);
                w.reference(input.bytes());
            }
        }
        w.section(2, state.incomes.len());
        for (key, e) in &state.incomes {
            w.income_key(key);
            w.byte(match e.status {
                IncomeStatus::Baseline => 1,
                IncomeStatus::Booked => 2,
                IncomeStatus::Pending => 3,
                IncomeStatus::Trade => 4,
                IncomeStatus::Reconciled => 5,
            });
            w.reference(e.input.bytes());
        }
        w.section(3, state.components.len());
        for (key, c) in &state.components {
            w.component_key(key);
            w.order_key(&c.order);
            w.text(c.native.as_str());
            w.uint(c.wallet);
            w.asset(&c.delta.asset);
            w.decimal(c.delta.amount);
        }
        w.section(4, state.aliases.len());
        for (key, income) in &state.aliases {
            if !state.components.contains_key(key)
                || !state
                    .incomes
                    .get(income)
                    .is_some_and(|e| e.status == IncomeStatus::Trade)
            {
                return Err(Error::Identity);
            }
            w.component_key(key);
            w.income_key(income);
        }
        w.section(5, state.observations.len());
        for (hash, unresolved) in &state.observations {
            w.put(hash);
            w.byte(u8::from(*unresolved));
        }
        for (tag, values) in [(6, &state.caps), (7, &state.credit_holds)] {
            w.section(tag, values.len());
            for (key, amount) in values {
                w.uint(key.wallet_id);
                w.uint(key.asset_id);
                w.fixed(*amount);
            }
        }
        w.byte(8);
        w.byte(u8::from(state.rejected.is_some()));
        if let Some(input) = &state.rejected {
            w.reference(input.bytes());
        }
        w.section(9, self.orders.len());
        for (key, o) in &self.orders {
            w.order_key(key);
            for b in [
                o.observed_unknown,
                o.observation_unreconciled,
                o.confirmed,
                o.economic,
                o.terminal,
                o.fees_closed,
                o.aborted,
            ] {
                w.byte(u8::from(b));
            }
            for n in [o.original, o.filled, o.reduce] {
                w.fixed(n);
            }
            w.byte(u8::from(o.terminal_cumulative.is_some()));
            if let Some(n) = o.terminal_cumulative {
                w.fixed(n);
            }
        }
        w.byte(10);
        w.byte(u8::from(self.rejected_event.is_some()));
        if let Some(event) = &self.rejected_event {
            w.reference(event.bytes());
        }
        if self.structure_observed {
            w.byte(11);
            w.put(&self.structure_state_digest(None));
        }
        if !self.amendments.is_empty() {
            w.byte(12);
            w.put(&self.amendment_state_digest(None));
        }
        w.finish()
    }
    /// Apply an already-verified durable closure. This method establishes no
    /// remote-query provenance and never activates the account.
    pub fn reconcile_account_inputs(
        &mut self,
        binding: &AccountInputsReconciled,
    ) -> Result<Applied> {
        if let Some(old) = self.account_inputs.closures.get(&binding.history_digest) {
            return if old == binding {
                Ok(Applied::Duplicate)
            } else {
                self.freeze_error(Error::Conflict)
            };
        }
        let result = (|| {
            if binding.scope != self.scope
                || binding.account_epoch != self.epoch
                || binding.ledger_sequence != self.sequence
                || binding.input_stream == [0; 16]
                || binding.through_sequence == 0
                || binding.coverage_digest == [0; 32]
                || self
                    .account_inputs_state_digest(binding.baseline_digest, binding.history_digest)?
                    != binding.state_digest
            {
                return Err(Error::Identity);
            }
            self.account_inputs_to_reconcile()?;
            if self.account_inputs.closures.len() == MAX_FACTS {
                return Err(Error::Capacity);
            }
            let mut candidate = self.clone();
            for e in candidate.account_inputs.incomes.values_mut() {
                if e.status == IncomeStatus::Booked {
                    e.status = IncomeStatus::Reconciled;
                }
            }
            candidate
                .account_inputs
                .closures
                .insert(binding.history_digest, binding.clone());
            candidate.next_sequence()?;
            candidate.frozen = true;
            candidate.evaluate()?;
            *self = candidate;
            Ok(Applied::New)
        })();
        if result.is_err() {
            self.frozen = true;
        }
        result
    }
}

pub(super) struct StateWriter {
    hash: Sha256,
    bytes: usize,
    overflow: bool,
}
impl StateWriter {
    pub(super) fn new() -> Self {
        Self {
            hash: Sha256::new(),
            bytes: 0,
            overflow: false,
        }
    }
    pub(super) fn put(&mut self, bytes: &[u8]) {
        if self.overflow || bytes.len() > MAX_STATE_BYTES.saturating_sub(self.bytes) {
            self.overflow = true;
        } else {
            self.bytes += bytes.len();
            self.hash.update(bytes);
        }
    }
    pub(super) fn byte(&mut self, n: u8) {
        self.put(&[n]);
    }
    pub(super) fn uint(&mut self, n: u64) {
        self.put(&n.to_le_bytes());
    }
    pub(super) fn count(&mut self, n: usize) {
        match u32::try_from(n) {
            Ok(n) => self.put(&n.to_le_bytes()),
            Err(_) => self.overflow = true,
        }
    }
    fn section(&mut self, tag: u8, n: usize) {
        self.byte(tag);
        self.count(n);
    }
    fn text(&mut self, s: &str) {
        match u16::try_from(s.len()) {
            Ok(n) => {
                self.put(&n.to_le_bytes());
                self.put(s.as_bytes());
            }
            Err(_) => self.overflow = true,
        }
    }
    fn reference(&mut self, bytes: &[u8]) {
        self.count(bytes.len());
        self.put(&Sha256::digest(bytes));
    }
    fn fixed(&mut self, n: Fixed) {
        self.put(&n.atoms().to_le_bytes());
        self.byte(n.scale());
    }
    fn decimal(&mut self, n: v::Decimal) {
        self.put(&n.coefficient().to_le_bytes());
        self.byte(n.exponent() as u8);
    }
    pub(super) fn income_key(&mut self, key: &IncomeKey) {
        self.text(&key.0);
        self.text(&key.1);
    }
    fn component_key(&mut self, key: &ComponentKey) {
        self.text(&key.0);
        self.text(&key.1);
        self.byte(key.2);
        self.uint(key.3);
    }
    fn order_key(&mut self, key: &OrderKey) {
        self.uint(key.intent.epoch);
        self.uint(key.intent.intent_id);
        self.text(key.client_id.as_str());
    }
    fn asset(&mut self, a: &v::AssetRef) {
        self.uint(a.id);
        self.uint(a.version);
        self.text(a.symbol.as_str());
        self.byte(a.exponent as u8);
    }
    pub(super) fn finish(self) -> Result<v::Hash> {
        if self.overflow {
            Err(Error::Capacity)
        } else {
            Ok(self.hash.finalize().into())
        }
    }
}
