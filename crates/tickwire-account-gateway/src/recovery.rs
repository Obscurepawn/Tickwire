use crate::*;
use tickwire_account_journal::{RecoveryVerifier, VerificationError};

pub(crate) struct Verifier<'a, A: Authority> {
    pub authority: &'a mut A,
    pub reactivation: &'a mut dyn ReactivationVerifier,
}
impl<A: Authority> RecoveryVerifier for Verifier<'_, A> {
    fn verify_reactivation(&mut self,baseline:&CanonicalBaseline,record:&CanonicalRecord,preceding:&HistoryFrontier,prior:&[CanonicalRecord])->std::result::Result<(),VerificationError>{
        let RecoveryRecord::Reactivated(binding)=record.record() else{return Err(VerificationError::Invalid);};
        let inputs:Vec<_>=prior.iter().map(|r|r.record().clone()).collect();
        let account=Account::recover(baseline.snapshot().clone(),baseline.catalog().clone(),&inputs).map_err(|_|VerificationError::Invalid)?;
        self.reactivation.verify(baseline,&account,preceding,binding).map_err(|_|VerificationError::Invalid)
    }

    fn verify_admission(
        &mut self,
        baseline: &CanonicalBaseline,
        record: &CanonicalRecord,
        preceding: &HistoryFrontier,
    ) -> std::result::Result<(), VerificationError> {
        let request = binding::request(baseline, record, preceding)
            .map_err(|_| VerificationError::Invalid)?;
        let capability = self
            .authority
            .verify(request)
            .map_err(|error| match error {
                AuthorityError::Unavailable => VerificationError::Unavailable,
                AuthorityError::Recovery(tickwire_recovery::Error::Missing) => {
                    VerificationError::Missing
                }
                _ => VerificationError::Invalid,
            })?;
        binding::closure(baseline, record, preceding, &capability)
            .map_err(|_| VerificationError::Invalid)
    }
}
pub(crate) fn entries(
    config: &Config,
    records: &[CanonicalRecord],
) -> Result<BTreeMap<IntentKey, Entry>> {
    let mut entries = BTreeMap::new();
    for record in records {
        match record.record() {
            RecoveryRecord::Admit {
                command,
                evaluated_at,
                expected,
            } => {
                binding::command(config, command, false)?;
                if entries.len() >= config.max_intents {
                    return Err(Error::Capacity);
                }
                if entries
                    .insert(
                        binding::intent(command),
                        Entry {
                            command: command.as_ref().clone(),
                            committed: None,
                            evaluated_at: *evaluated_at,
                            increases: expected.increases,
                            status: Status::Unknown,
                            output: None,
                        },
                    )
                    .is_some()
                {
                    return Err(Error::Conflict);
                }
            }
            RecoveryRecord::Aborted {
                intent,
                command_digest,
            } => {
                let entry = entries.get_mut(intent).ok_or(Error::Identity)?;
                if entry.command.digest() != *command_digest {
                    return Err(Error::Identity);
                }
                entry.status = Status::Aborted;
            }
            _ => {}
        }
    }
    Ok(entries)
}
pub(crate) fn apply_fact(account: &mut Account, record: &RecoveryRecord) -> Result<Applied> {
    match record {
        RecoveryRecord::VenueEvent(event) | RecoveryRecord::IngressEvent { event, .. } => {
            account.apply_event(event)
        }
        RecoveryRecord::FeesReconciled { order, cumulative } => {
            account.reconcile_fees(order, *cumulative)
        }
        RecoveryRecord::UnattributedCashFlow { id, delta } => {
            account.apply_unattributed_cashflow(id, *delta)
        }
        RecoveryRecord::ExternalPosition {
            id,
            instrument,
            side,
            quantity,
            entry_bound,
        } => account.apply_external_position(id, *instrument, *side, *quantity, *entry_bound),
        RecoveryRecord::TransferAsset {
            key,
            from,
            to,
            amount,
        } => account
            .transfer_asset_attribution(*key, *from, *to, *amount)
            .map(|()| Applied::New),
        RecoveryRecord::TransferPosition {
            instrument,
            side,
            from,
            to,
            quantity,
        } => account
            .transfer_position_attribution(*instrument, *side, *from, *to, *quantity)
            .map(|()| Applied::New),
        RecoveryRecord::Reactivated(_) | RecoveryRecord::Admit { .. } | RecoveryRecord::Aborted { .. } => {
            return Err(Error::Invalid);
        }
    }
    .map_err(Error::Account)
}
