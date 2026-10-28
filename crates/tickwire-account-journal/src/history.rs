use crate::{Config, Hash, Result, Scope, identity};
use sha2::{Digest, Sha256};
use tickwire_account_codec::CanonicalRecord;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryFrontier {
    scope: Scope,
    stream: [u8; 16],
    baseline: Hash,
    count: u64,
    chain: Hash,
    bytes: [u8; 128],
    digest: Hash,
}

impl HistoryFrontier {
    pub(crate) fn empty(config: &Config) -> Result<Self> {
        let mut hash = Sha256::new();
        hash.update(b"tickwire.account-history-empty.v1");
        hash.update(identity::bootstrap(config)?);
        let mut out = Self {
            scope: config.scope.clone(),
            stream: config.physical.stream_epoch,
            baseline: config.baseline_digest,
            count: 0,
            chain: hash.finalize().into(),
            bytes: [0; 128],
            digest: [0; 32],
        };
        out.encode();
        Ok(out)
    }
    pub(crate) fn advance(&self, writer: u64, record: &CanonicalRecord) -> Self {
        let mut hash = Sha256::new();
        hash.update(b"tickwire.account-history-record.v1");
        hash.update(self.chain);
        hash.update(writer.to_le_bytes());
        hash.update(record.sequence().to_le_bytes());
        hash.update(record.digest());
        let mut out = self.clone();
        out.count = record.sequence();
        out.chain = hash.finalize().into();
        out.encode();
        out
    }
    fn encode(&mut self) {
        self.bytes[..8].copy_from_slice(b"TWAF0001");
        self.bytes[8..24].copy_from_slice(&self.scope.risk_domain_id);
        self.bytes[24..40].copy_from_slice(&self.stream);
        self.bytes[40..72].copy_from_slice(&self.baseline);
        self.bytes[72..76].copy_from_slice(&self.scope.venue.to_le_bytes());
        self.bytes[76..84].copy_from_slice(&self.scope.account.to_le_bytes());
        self.bytes[84] = match self.scope.environment {
            tickwire_venue::Environment::Production => 1,
            tickwire_venue::Environment::Test => 2,
        };
        self.bytes[88..96].copy_from_slice(&self.count.to_le_bytes());
        self.bytes[96..].copy_from_slice(&self.chain);
        self.digest = Sha256::digest(self.bytes).into();
    }
    pub fn scope(&self) -> &Scope {
        &self.scope
    }
    pub fn stream(&self) -> [u8; 16] {
        self.stream
    }
    pub fn baseline_digest(&self) -> Hash {
        self.baseline
    }
    pub fn record_count(&self) -> u64 {
        self.count
    }
    pub fn chain_hash(&self) -> Hash {
        self.chain
    }
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub fn digest(&self) -> Hash {
        self.digest
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn hex(s: &str) -> Vec<u8> {
        s.as_bytes()
            .as_chunks::<2>()
            .0
            .iter()
            .map(|b| u8::from_str_radix(std::str::from_utf8(b).unwrap(), 16).unwrap())
            .collect()
    }
    #[test]
    fn independent_python_samples_bind_bootstrap_and_writer_epoch() {
        let data: serde_json::Value =
            serde_json::from_str(include_str!("../tests/data/history-vectors.json")).unwrap();
        let config = Config {
            physical: tickwire_journal::Config::new([2; 16], [1; 16]),
            baseline_digest: [7; 32],
            initial_writer_epoch: 4,
            scope: Scope {
                risk_domain_id: [1; 16],
                venue: 1,
                account: 2,
                environment: tickwire_venue::Environment::Test,
            },
        };
        assert_eq!(
            identity::bootstrap(&config).unwrap(),
            data["bootstrap"].as_str().unwrap().as_bytes()
        );
        let empty = HistoryFrontier::empty(&config).unwrap();
        let record = CanonicalRecord::decode(&hex(data["record"].as_str().unwrap())).unwrap();
        assert_eq!(
            record.digest().as_slice(),
            hex(data["record_digest"].as_str().unwrap())
        );
        for (frontier, key) in [
            (empty.clone(), "empty"),
            (empty.advance(4, &record), "writer_4"),
            (empty.advance(5, &record), "writer_5"),
        ] {
            assert_eq!(frontier.bytes(), hex(data[key]["bytes"].as_str().unwrap()));
            assert_eq!(
                frontier.digest().as_slice(),
                hex(data[key]["digest"].as_str().unwrap())
            );
        }
    }
}
