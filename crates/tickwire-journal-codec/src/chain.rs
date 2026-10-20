use crate::{
    ChainError, Hash, LogicalChain, Record, SCHEMA_VERSION, cbor::Writer, logical_payload_hash,
};
use sha2::{Digest, Sha256};

impl LogicalChain {
    /// Creates only an original-stream genesis. No epoch transition or trusted
    /// intermediate-frontier constructor is provided by this codec.
    pub fn new(domain: [u8; 16], journal_epoch: [u8; 16]) -> Self {
        let mut w = Writer::new();
        w.array(4);
        w.text("tickwire.logical-genesis");
        w.uint(SCHEMA_VERSION);
        w.bytes(&domain);
        w.bytes(&journal_epoch);
        Self {
            domain,
            journal_epoch,
            last_seq: 0,
            writer_exec_epoch: None,
            hash: Sha256::digest(w.finish()).into(),
        }
    }
    /// Advances one log position. Duplicate economic facts may occupy distinct
    /// positions; deduplication and replay application belong to the reducer.
    pub fn append(&mut self, record: &Record) -> Result<Hash, ChainError> {
        if record.risk_domain_id != self.domain {
            return Err(ChainError::WrongDomain);
        }
        if record.journal_epoch != self.journal_epoch {
            return Err(ChainError::WrongJournalEpoch);
        }
        let next = self
            .last_seq
            .checked_add(1)
            .ok_or(ChainError::SequenceExhausted)?;
        if record.journal_seq != next {
            return Err(ChainError::Sequence);
        }
        if self
            .writer_exec_epoch
            .is_some_and(|epoch| record.writer_exec_epoch < epoch)
        {
            return Err(ChainError::ExecutionEpochRollback);
        }
        let mut w = Writer::new();
        w.array(7);
        w.text("tickwire.logical-record");
        w.uint(SCHEMA_VERSION);
        w.bytes(&self.domain);
        w.bytes(&self.journal_epoch);
        w.uint(record.journal_seq);
        w.uint(record.event.kind());
        w.bytes(&logical_payload_hash(record));
        let mut hasher = Sha256::new();
        hasher.update(self.hash);
        hasher.update(w.finish());
        let hash = hasher.finalize().into();
        // All fallible validation precedes the state transition.
        self.last_seq = next;
        self.writer_exec_epoch = Some(record.writer_exec_epoch);
        self.hash = hash;
        Ok(hash)
    }
    /// Compares the calculated prefix with an externally supplied hash before
    /// publishing state. Equality proves content agreement only.
    pub fn verify_and_append(
        &mut self,
        record: &Record,
        expected: Hash,
    ) -> Result<Hash, ChainError> {
        let mut candidate = *self;
        let hash = candidate.append(record)?;
        if hash != expected {
            return Err(ChainError::HashMismatch);
        }
        *self = candidate;
        Ok(hash)
    }
    pub fn last_seq(&self) -> u64 {
        self.last_seq
    }
    pub fn hash(&self) -> Hash {
        self.hash
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Event, OrderIdentity, Product};

    #[test]
    fn exhausted_sequence_does_not_wrap_or_mutate() {
        // This directly sets a representable end-of-prefix boundary; iterating
        // all u64 positions is neither necessary nor claimed by this test.
        let mut chain = LogicalChain::new([1; 16], [2; 16]);
        chain.last_seq = u64::MAX;
        let before = chain;
        let record = Record {
            risk_domain_id: [1; 16],
            journal_epoch: [2; 16],
            journal_seq: 0,
            writer_exec_epoch: 0,
            event: Event::AbortedBeforeSend {
                order: OrderIdentity {
                    venue: 1,
                    account: 1,
                    product: Product::Spot,
                    intent_id: 1,
                    intent_seq: 1,
                    intent_exec_epoch: 7,
                },
            },
        };
        assert_eq!(chain.append(&record), Err(ChainError::SequenceExhausted));
        assert_eq!(chain, before);
    }
}
