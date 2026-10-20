//! Shared verification of the account callbacks retained in a strategy handover.
//! This checks recovery data, not strategy authorization or callback execution.
#![forbid(unsafe_code)]
use tickwire_account::Account;
use tickwire_account_codec::{CanonicalBaseline, CanonicalRecord, InputSeal};
use tickwire_recovery::{EncodedInputs, HandoverSeal, MAX_OBJECT_BYTES, digest};

pub const MAX_ACCOUNT_RECORDS: usize = tickwire_account::MAX_FACTS + tickwire_account::MAX_ORDERS;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    Bounds,
    Checkpoint,
    Inputs,
    Callback,
    Baseline,
    Sequence,
    AccountSeal,
    Replay,
    State,
    History,
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}

/// Complete account input data reconstructed from the actual TWINP object.
/// Construction is private; callers still bind it to their source identity/WAL.
pub struct VerifiedHandoverInputs {
    baseline: CanonicalBaseline,
    records: Vec<CanonicalRecord>,
}
impl VerifiedHandoverInputs {
    pub fn baseline(&self) -> &CanonicalBaseline {
        &self.baseline
    }
    pub fn records(&self) -> &[CanonicalRecord] {
        &self.records
    }
    /// Compare the verified callbacks with the independently read actual WAL.
    pub fn matches_history(
        &self,
        baseline: &CanonicalBaseline,
        prior: &[CanonicalRecord],
    ) -> Result<(), Error> {
        if self.baseline.bytes() != baseline.bytes()
            || self.records.len() != prior.len()
            || self
                .records
                .iter()
                .zip(prior)
                .any(|(a, b)| a.bytes() != b.bytes())
        {
            return Err(Error::History);
        }
        Ok(())
    }
}

/// Validate the shared handover format, its checkpoint and all account callbacks.
/// Other input kinds remain in their original order and are checked by their own
/// replay handlers. Every kind-2 row must be a valid TWAI0001 account callback.
pub fn verify(
    seal: &HandoverSeal,
    checkpoint: &[u8],
    input_bytes: &[u8],
    account_input_bytes: &[u8],
) -> Result<VerifiedHandoverInputs, Error> {
    seal.validate().map_err(|_| Error::Bounds)?;
    if seal.report_position > MAX_ACCOUNT_RECORDS as u64
        || input_bytes.len() > MAX_OBJECT_BYTES
        || input_bytes.len() as u64 != seal.input_bytes
        || account_input_bytes.len() as u64 != seal.account_input_bytes
        || digest(input_bytes) != seal.input_digest
        || digest(account_input_bytes) != seal.account_input_digest
    {
        return Err(Error::Inputs);
    }
    seal.verify_checkpoint(checkpoint)
        .map_err(|_| Error::Checkpoint)?;
    // Decode rechecks the bounded bytes instead of trusting public boundary fields.
    let inputs = EncodedInputs::decode(input_bytes.to_vec()).map_err(|_| Error::Inputs)?;
    if inputs.boundary.first_sequence != 1 || inputs.boundary.previous_hash != [0; 32] {
        return Err(Error::Inputs);
    }
    let mut baseline = None;
    let mut account_seal = None;
    let mut records = Vec::new();
    let count = u32::from_be_bytes(inputs.bytes[32..36].try_into().unwrap());
    let mut offset = 68;
    // EncodedInputs has validated every outer row length and exact exhaustion.
    for _ in 0..count {
        let kind = inputs.bytes[offset + 8];
        let length =
            u32::from_be_bytes(inputs.bytes[offset + 9..offset + 13].try_into().unwrap()) as usize;
        let body = &inputs.bytes[offset + 13..offset + 13 + length];
        offset += 13 + length;
        if kind != 2 {
            continue;
        }
        if body.len() < 29
            || &body[..8] != b"TWAI0001"
            || u32::from_le_bytes(body[25..29].try_into().unwrap()) as usize != body.len() - 29
        {
            return Err(Error::Callback);
        }
        let position = u64::from_le_bytes(body[17..25].try_into().unwrap());
        match body[8] {
            0 if position == 0 && baseline.is_none() && records.is_empty() => {
                let b = CanonicalBaseline::decode(&body[29..]).map_err(|_| Error::Baseline)?;
                account_seal = Some(InputSeal::new(b.digest()).map_err(|_| Error::AccountSeal)?);
                baseline = Some(b);
            }
            1 if baseline.is_some() => {
                if records.len() >= MAX_ACCOUNT_RECORDS {
                    return Err(Error::Bounds);
                }
                let record = CanonicalRecord::decode(&body[29..]).map_err(|_| Error::Callback)?;
                if position != records.len() as u64 + 1 || position != record.sequence() {
                    return Err(Error::Sequence);
                }
                account_seal
                    .as_mut()
                    .ok_or(Error::Baseline)?
                    .apply(&record)
                    .map_err(|_| Error::AccountSeal)?;
                records.push(record);
            }
            _ => return Err(Error::Callback),
        }
    }
    let baseline = baseline.ok_or(Error::Baseline)?;
    let account_seal = account_seal.ok_or(Error::Baseline)?;
    if records.len() as u64 != seal.report_position
        || account_seal.digest() != seal.account_input_digest
        || account_seal.object_bytes() != account_input_bytes
    {
        return Err(Error::AccountSeal);
    }
    let replay = records
        .iter()
        .map(|r| r.record().clone())
        .collect::<Vec<_>>();
    let account = Account::recover(
        baseline.snapshot().clone(),
        baseline.catalog().clone(),
        &replay,
    )
    .map_err(|_| Error::Replay)?;
    if account.ledger_sequence() != seal.ledger_sequence
        || account.strategy_orders_digest(seal.logical_strategy) != seal.orders_digest
    {
        return Err(Error::State);
    }
    Ok(VerifiedHandoverInputs { baseline, records })
}

#[cfg(test)]
mod tests;
