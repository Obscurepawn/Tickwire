//! Read-only inspection of SDK-owned views at one saved checkpoint.
use super::*;
use tickwire_account_codec::CanonicalBaseline;

#[derive(Debug)]
pub struct AccountInput {
    baseline: CanonicalBaseline,
    cursor: Cursor,
    position: u64,
    input_digest: Hash,
}
impl AccountInput {
    pub fn baseline(&self) -> &CanonicalBaseline {
        &self.baseline
    }
    pub fn cursor(&self) -> &Cursor {
        &self.cursor
    }
    pub fn position(&self) -> u64 {
        self.position
    }
    pub fn input_digest(&self) -> Hash {
        self.input_digest
    }
}
#[derive(Debug)]
pub struct MarketInput {
    bank: ResourceHeader,
    cursor: Cursor,
    instruments: Vec<(u64, u64)>,
}
impl MarketInput {
    pub fn bank(&self) -> &ResourceHeader {
        &self.bank
    }
    pub fn cursor(&self) -> &Cursor {
        &self.cursor
    }
    pub fn instruments(&self) -> &[(u64, u64)] {
        &self.instruments
    }
}
#[derive(Debug)]
pub struct InputViews {
    accounts: Vec<AccountInput>,
    markets: Vec<MarketInput>,
}
impl InputViews {
    pub fn accounts(&self) -> &[AccountInput] {
        &self.accounts
    }
    pub fn markets(&self) -> &[MarketInput] {
        &self.markets
    }
    pub fn into_parts(self) -> (Vec<AccountInput>, Vec<MarketInput>) {
        (self.accounts, self.markets)
    }
}
/// This inspects persisted SDK state; it neither restores nor calls a strategy.
pub fn inspect(checkpoint: &Checkpoint) -> Result<InputViews> {
    checkpoint.bytes()?;
    let bytes = &checkpoint.business;
    if bytes.len() > MAX_BYTES {
        return Err(Error::Capacity);
    }
    let saved: Saved = serde_json::from_slice(bytes).map_err(|_| Error::Identity)?;
    if saved.schema != 1
        || canonical(&saved)? != *bytes
        || saved.maximum_age_ns == 0
        || saved.markets.is_empty()
        || saved.markets.len() > MAX_STREAMS
        || saved.accounts.len() > MAX_STREAMS
        || saved.markets.len() != checkpoint.runtime.markets.len()
        || saved.accounts.len() != checkpoint.runtime.accounts.len()
    {
        return Err(Error::Identity);
    }
    let mut markets = Vec::with_capacity(saved.markets.len());
    for (saved, cursor) in saved.markets.into_iter().zip(&checkpoint.runtime.markets) {
        let bank = ResourceHeader::decode(&saved.resource).map_err(|_| Error::Identity)?;
        if bank.descriptor_hash != [0; 32] {
            let mut descriptor = bank.clone();
            descriptor.descriptor_hash = [0; 32];
            if Object::of(&crate::bootstrap::wire_bytes(&descriptor).map_err(|_| Error::Identity)?)
                .digest
                != bank.descriptor_hash
            {
                return Err(Error::Identity);
            }
        }
        let replica = crate::MarketReplica::restore_checkpoint(&saved.view, saved.maximum_levels)
            .map_err(|_| Error::Identity)?;
        let view = market(bank, replica, saved.maximum_levels)?;
        if market_stream(&view.resource)? != cursor.stream
            || view.replica.sequence() != cursor.sequence
        {
            return Err(Error::Identity);
        }
        let instruments = view
            .replica
            .instruments()
            .map(|id| {
                Ok((
                    id,
                    view.replica
                        .view(id)
                        .ok_or(Error::Identity)?
                        .trigger()
                        .instrument_version,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        markets.push(MarketInput {
            bank: view.resource,
            cursor: cursor.clone(),
            instruments,
        });
    }
    let mut accounts = Vec::with_capacity(saved.accounts.len());
    for (saved, cursor) in saved.accounts.into_iter().zip(&checkpoint.runtime.accounts) {
        let view = account::AccountReplica::restore(&saved.view)?;
        if saved.stream != cursor.stream
            || saved.stream != account::AccountReplica::stream(view.baseline())
            || view.position() != cursor.sequence
        {
            return Err(Error::Identity);
        }
        accounts.push(AccountInput {
            baseline: view.baseline().clone(),
            cursor: cursor.clone(),
            position: view.position(),
            input_digest: view.seal().digest(),
        });
    }
    Ok(InputViews { accounts, markets })
}

#[cfg(test)]
mod tests;
