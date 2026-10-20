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
                RecoveryRecord::VenueEvent(event) => {
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
