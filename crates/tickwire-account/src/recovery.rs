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
                RecoveryRecord::UncreatedAlgoClosed(b) => {
                    account.close_uncreated_algo(b)?;
                }
                RecoveryRecord::IncomeObservationsAssociated(b) => {
                    account.associate_income_observations(b)?;
                }
                RecoveryRecord::AccountInputsReconciled(b) => {
                    account.reconcile_account_inputs(b)?;
                }
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
                    if let Err(error) = account.reconcile_fees(order, *cumulative) {
                        // Older writers synced a proposed fee closure before checking a
                        // regressing terminal observation. It released no funds and
                        // advanced no ledger sequence. Keep the rejected row in history.
                        if error != Error::Unreconciled
                            || !account.orders.get(order).is_some_and(|o| {
                                o.spec.mode == v::ProductMode::Spot
                                    && o.terminal
                                    && o.observation_unreconciled
                                    && !o.fee_reconciled
                                    && o.filled == *cumulative
                                    && o.terminal_cumulative
                                        .is_some_and(|q| q.atoms() > cumulative.atoms())
                            })
                        {
                            return Err(error);
                        }
                    }
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
                && !order.uncreated
                && (!order.terminal
                    || order.terminal_cumulative != Some(order.filled)
                    || !order.fee_reconciled)
            {
                order.recovery_unknown = true;
            }
        }
        let keys: Vec<_> = account.orders.keys().cloned().collect();
        let stamp = account.last_mono_ns;
        for k in keys {
            account.refresh_order_retirement(&k, stamp);
        }
        Ok(account)
    }
}

impl Account {
    /// Apply an already-verified reconciliation decision; this does not
    /// itself establish provenance of remote snapshots or historical closure.
    pub fn apply_reactivation(&mut self, binding: &Reactivation) -> Result<()> {
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
        if binding.venue_available.len() != self.venue_available.len()
            || binding.venue_available.windows(2).any(|w| w[0].0 >= w[1].0)
        {
            return Err(Error::Identity);
        }
        for (wallet, available) in &binding.venue_available {
            if self
                .venue_available
                .get(wallet)
                .is_none_or(|old| old.scale() != available.scale())
                || available.atoms() < 0
            {
                return Err(Error::Identity);
            }
        }
        self.sequence
            .checked_add(1)
            .ok_or(RiskError::ArithmeticOverflow)?;

        let prior_venue_available = self.venue_available.clone();
        for (wallet, available) in &binding.venue_available {
            self.venue_available.insert(*wallet, *available);
        }
        let prior_shadow_holds = std::mem::take(&mut self.shadow_holds);
        let prior_uncovered_debits = std::mem::take(&mut self.uncovered_debits);
        let prior_shadow_unvalued = std::mem::take(&mut self.shadow_unvalued);
        let prior_last_mono_ns = self.last_mono_ns;
        let prior_orders: Vec<_> = self
            .orders
            .iter()
            .map(|(k, o)| {
                (
                    k.clone(),
                    o.shadow_fee_credit,
                    o.observed_unknown,
                    o.recovery_unknown,
                    o.observation_unreconciled,
                    o.in_doubt,
                    o.retired_mono_ns,
                )
            })
            .collect();

        for order in self.orders.values_mut() {
            order.shadow_fee_credit = order.shadow_fee_credit.zero_like();
            order.observed_unknown = false;
            order.recovery_unknown = false;
            order.observation_unreconciled = false;
            order.in_doubt = None;
        }
        self.last_mono_ns = self.last_mono_ns.max(binding.evaluated_at);
        let keys: Vec<_> = self.orders.keys().cloned().collect();
        let stamp = self.last_mono_ns;
        for k in &keys {
            self.refresh_order_retirement(k, stamp);
        }

        if let Err(e) = self.evaluate() {
            self.venue_available = prior_venue_available;
            self.shadow_holds = prior_shadow_holds;
            self.uncovered_debits = prior_uncovered_debits;
            self.shadow_unvalued = prior_shadow_unvalued;
            self.last_mono_ns = prior_last_mono_ns;
            for (k, credit, obs_unk, rec_unk, obs_unrec, doubt, retired) in prior_orders {
                if let Some(o) = self.orders.get_mut(&k) {
                    o.shadow_fee_credit = credit;
                    o.observed_unknown = obs_unk;
                    o.recovery_unknown = rec_unk;
                    o.observation_unreconciled = obs_unrec;
                    o.in_doubt = doubt;
                    o.retired_mono_ns = retired;
                }
            }
            for k in &keys {
                self.refresh_order_retirement(k, prior_last_mono_ns);
            }
            return Err(e);
        }

        self.frozen = false;
        self.rejected_event = None;
        self.valid_until = binding.valid_until;
        self.next_sequence()?;
        self.last_reactivation_sequence = self.sequence;
        Ok(())
    }
}
