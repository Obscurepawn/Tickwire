use super::*;
impl Account {
    pub fn admission(&self, intent: IntentKey) -> Option<&Admission> {
        self.admissions.get(&intent)
    }
    /// Replays one immutable baseline/catalog epoch. The returned account is always frozen.
    /// The caller verifies durable closure, record order and input provenance before calling.
    pub fn recover(
        snapshot: Snapshot,
        catalog: Catalog,
        records: &[RecoveryRecord],
    ) -> Result<Self> {
        if records.len() > MAX_FACTS + MAX_ORDERS {
            return Err(Error::Capacity);
        }
        let mut account = Self::from_snapshot(snapshot, catalog)?;
        for record in records {
            match record {
                RecoveryRecord::IncomeBaseline(b) => {
                    account.apply_income_baseline(b)?;
                }
                RecoveryRecord::IngressAccountInput { input, .. } => {
                    account.apply_account_input(input)?;
                }
                RecoveryRecord::StrategyHandover(h) => {
                    account.apply_strategy_handover(h)?;
                }
                RecoveryRecord::Reactivated(binding) => account.apply_reactivation(binding)?,
                RecoveryRecord::Admit {
                    command,
                    evaluated_at,
                    expected,
                } => {
                    let actual = account.admit(command, *evaluated_at)?;
                    if actual.image() != *expected {
                        return Err(Error::Conflict);
                    }
                }
                RecoveryRecord::VenueEvent(event) | RecoveryRecord::IngressEvent { event, .. } => {
                    account.apply_event(event)?;
                }
                RecoveryRecord::Aborted {
                    intent,
                    command_digest,
                } => {
                    let admission = account
                        .admission(*intent)
                        .ok_or(Error::UnknownOrder)?
                        .clone();
                    if admission.command_digest() != *command_digest {
                        return Err(Error::Conflict);
                    }
                    account.apply_abort(&admission)?;
                }
                RecoveryRecord::FeesReconciled { order, cumulative } => {
                    account.reconcile_fees(order, *cumulative)?;
                }
                RecoveryRecord::UnattributedCashFlow { id, delta } => {
                    account.apply_unattributed_cashflow(id, *delta)?;
                }
                RecoveryRecord::ExternalPosition {
                    id,
                    instrument,
                    side,
                    quantity,
                    entry_bound,
                } => {
                    account.apply_external_position(
                        id,
                        *instrument,
                        *side,
                        *quantity,
                        *entry_bound,
                    )?;
                }
                RecoveryRecord::TransferAsset {
                    key,
                    from,
                    to,
                    amount,
                } => {
                    account.transfer_asset_attribution(*key, *from, *to, *amount)?;
                }
                RecoveryRecord::TransferPosition {
                    instrument,
                    side,
                    from,
                    to,
                    quantity,
                } => {
                    account.transfer_position_attribution(
                        *instrument,
                        *side,
                        *from,
                        *to,
                        *quantity,
                    )?;
                }
            }
        }
        account.frozen = true;
        for order in account.orders.values_mut() {
            if !order.aborted
                && (!order.terminal
                    || order.terminal_cumulative != Some(order.filled)
                    || !order.fees_closed)
            {
                order.recovery_unknown = true;
            }
        }
        Ok(account)
    }
}

impl Account {
    /// Apply an already-verified durable reconciliation decision; this does not
    /// itself establish provenance of remote snapshots or historical closure.
    pub fn apply_reactivation(&mut self, binding: &Reactivation) -> Result<()> {
        // Existing snapshot-only proofs do not close income history or unexplained
        // account observations. The versioned income coverage path must do so.
        if self.unresolved_account_inputs() != 0 {
            return self.freeze_error(Error::Unreconciled);
        }
        if binding.scope != self.scope
            || binding.account_epoch != self.epoch
            || binding.ledger_sequence != self.sequence
            || binding.evaluated_at >= binding.valid_until
            || binding.baseline_digest == [0; 32]
            || binding.history_digest == [0; 32]
            || binding.query_coverage_digest == [0; 32]
            || binding.input_stream == [0; 16]
            || binding.through_sequence == 0
            || binding.private_connection_epoch == 0
            || binding.gap_generation == 0
            || binding.clock_boot_id == [0; 16]
            || binding.clock_generation == 0
            || binding.writer_epoch == 0
            || binding.execution_connection_epoch == 0
            || self.catalog.values().any(|p| p.product != binding.product)
        {
            return Err(Error::Identity);
        }
        let mut candidate = self.clone();
        if binding.venue_available.len() != candidate.venue_available.len()
            || binding.venue_available.windows(2).any(|w| w[0].0 >= w[1].0)
        {
            return Err(Error::Identity);
        }
        for (wallet, available) in &binding.venue_available {
            if candidate
                .venue_available
                .get(wallet)
                .is_none_or(|old| old.scale() != available.scale())
                || available.atoms() < 0
            {
                return Err(Error::Identity);
            }
            candidate.venue_available.insert(*wallet, *available);
        }
        // The authenticated balance already includes all orders and cashflows
        // checked by the reconciliation proof. New requests start new shadow holds.
        candidate.shadow_holds.clear();
        candidate.uncovered_debits.clear();
        candidate.shadow_unvalued.clear();
        for order in candidate.orders.values_mut() {
            order.shadow_fee_credit = order.shadow_fee_credit.zero_like();
        }

        for order in candidate.orders.values_mut() {
            order.observed_unknown = false;
            order.recovery_unknown = false;
            order.observation_unreconciled = false;
        }
        candidate.frozen = false;
        candidate.rejected_event = None;
        candidate.valid_until = binding.valid_until;
        candidate.next_sequence()?;
        candidate.evaluate()?;
        *self = candidate;
        Ok(())
    }
}
