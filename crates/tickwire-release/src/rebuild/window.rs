//! Content of a captured input window. There is deliberately no user state or
//! READY in this document; the authenticated owner must bind the actual source.
use super::*;
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Identity {
    pub capture: [u8; 16],
    pub logical_strategy: u64,
    pub risk_domain: [u8; 16],
    pub instance: [u8; 16],
    pub generation: u64,
    pub binary: Hash,
    pub clock: ClockDomain,
}
impl Identity {
    pub fn validate(&self) -> Result<()> {
        if [self.capture, self.risk_domain, self.instance].contains(&[0; 16])
            || self.logical_strategy == 0
            || self.generation == 0
            || self.binary == [0; 32]
            || self.clock.boot_id == [0; 16]
            || self.clock.generation == 0
        {
            return Err(Error::Identity);
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Seal {
    pub schema: u16,
    pub identity: Identity,
    pub contract: Object,
    pub seed: Object,
    pub inputs: Object,
    pub start: Frontier,
    pub end: Frontier,
    pub input_chain: Hash,
    pub account_input_digest: Hash,
}
impl Seal {
    pub fn bytes(&self) -> Result<Vec<u8>> {
        self.identity.validate()?;
        if self.schema != 1
            || self.start.rows != 0
            || self.end.rows == 0
            || self.end.rows > replay::MAX_ROWS as u64
            || self.end.now_ns < self.start.now_ns
            || self.input_chain == [0; 32]
            || self.account_input_digest == [0; 32]
            || self.start.markets.is_empty()
            || self.start.accounts.len() != 1
        {
            return Err(Error::Identity);
        }
        for object in [self.contract, self.seed, self.inputs] {
            object.validate(replay::MAX_BYTES as u64)?;
        }
        for (start, end) in [
            (&self.start.markets, &self.end.markets),
            (&self.start.accounts, &self.end.accounts),
            (&self.start.external, &self.end.external),
        ] {
            if start.len() > replay::MAX_STREAMS || end.len() > replay::MAX_STREAMS {
                return Err(Error::Bounds);
            }
            for cursors in [start, end] {
                if cursors.windows(2).any(|w| w[0].stream >= w[1].stream)
                    || cursors.iter().any(|c| {
                        c.stream.identity == [0; 32]
                            || c.stream.generation == 0
                            || c.content_digest == [0; 32]
                    })
                {
                    return Err(Error::Identity);
                }
            }
            for before in start {
                let after = end
                    .iter()
                    .find(|c| c.stream == before.stream)
                    .ok_or(Error::Identity)?;
                if after.sequence < before.sequence
                    || (after.sequence == before.sequence && after != before)
                {
                    return Err(Error::Identity);
                }
            }
        }
        if self.start.markets.len() != self.end.markets.len()
            || self.start.accounts.len() != self.end.accounts.len()
        {
            return Err(Error::Identity);
        }
        replay::canonical(self)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let value: Self = super::decode(bytes)?;
        value.bytes()?;
        Ok(value)
    }
}
/// Each source row is counted once in the capture order. This is independent of
/// a user factory's output or a replay business-state checksum.
pub fn next_chain(previous: Hash, sequence: u64, row: &TimedInput) -> Result<Hash> {
    if sequence == 0 {
        return Err(Error::Identity);
    }
    let mut bytes = b"tickwire.rebuild.window-input.v1".to_vec();
    bytes.extend(previous);
    bytes.extend(sequence.to_le_bytes());
    bytes.extend(replay::canonical(row)?);
    Ok(tickwire_recovery::digest(&bytes))
}
