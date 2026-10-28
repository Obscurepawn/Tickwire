//! Shared immutable strategy-handover metadata. Object bytes remain in BlobStore.
use crate::{Error, Hash, MAX_OBJECT_BYTES, Result};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HandoverSeal {
    pub version: u16,
    pub owner: [u8; 16],
    pub instance: [u8; 16],
    pub generation: u64,
    pub logical_strategy: u64,
    pub report_position: u64,
    pub ledger_sequence: u64,
    pub orders_digest: Hash,
    pub checkpoint_digest: Hash,
    pub checkpoint_bytes: u64,
    pub account_input_digest: Hash,
    pub account_input_bytes: u64,
    pub input_digest: Hash,
    pub input_bytes: u64,
    pub dependencies: Vec<(u16, Hash)>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preflight_receipt_digest: Option<Hash>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replay_installation_digest: Option<Hash>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replay_restore_digest: Option<Hash>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replay_rollback_digest: Option<Hash>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rebuild_installation_digest: Option<Hash>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rebuild_rollback_digest: Option<Hash>,
}
impl HandoverSeal {
    pub const MAX_BYTES: usize = 16384;
    pub fn validate(&self) -> Result<()> {
        if !matches!(
            (
                self.version,
                self.preflight_receipt_digest,
                self.replay_restore_digest,
                self.replay_installation_digest,
                self.replay_rollback_digest,
                self.rebuild_installation_digest,
                self.rebuild_rollback_digest
            ),
            (1, None, None, None, None, None, None)
                | (2, Some(_), None, None, None, None, None)
                | (3, None, Some(_), None, None, None, None)
                | (3, None, Some(_), Some(_), None, None, None)
                | (3, None, Some(_), Some(_), Some(_), None, None)
                | (4, None, None, None, None, Some(_), None)
                | (4, None, None, None, None, Some(_), Some(_))
        ) || self.preflight_receipt_digest == Some([0; 32])
            || self.replay_installation_digest == Some([0; 32])
            || self.replay_restore_digest == Some([0; 32])
            || self.replay_rollback_digest == Some([0; 32])
            || self.rebuild_installation_digest == Some([0; 32])
            || self.rebuild_rollback_digest == Some([0; 32])
            || self.owner == [0; 16]
            || self.owner != self.instance
            || self.generation == 0
            || self.logical_strategy == 0
            || self.orders_digest == [0; 32]
            || self.checkpoint_bytes <= 96
            || self.dependencies.len() != 10
        {
            return Err(Error::Invalid("handover identity"));
        }
        for (hash, length) in [
            (self.checkpoint_digest, self.checkpoint_bytes),
            (self.account_input_digest, self.account_input_bytes),
            (self.input_digest, self.input_bytes),
        ] {
            if hash == [0; 32] || length == 0 || length > MAX_OBJECT_BYTES as u64 {
                return Err(Error::Capacity);
            }
        }
        for (index, (kind, hash)) in self.dependencies.iter().enumerate() {
            if *kind != index as u16 + 2 || *hash == [0; 32] {
                return Err(Error::Invalid("handover dependencies"));
            }
        }
        Ok(())
    }
    pub fn bytes(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let bytes = serde_json::to_vec(self).map_err(|_| Error::Invalid("handover encoding"))?;
        if bytes.len() > Self::MAX_BYTES {
            return Err(Error::Capacity);
        }
        Ok(bytes)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.is_empty() || bytes.len() > Self::MAX_BYTES {
            return Err(Error::Capacity);
        }
        let out: Self =
            serde_json::from_slice(bytes).map_err(|_| Error::Invalid("handover encoding"))?;
        if out.bytes()? != bytes {
            return Err(Error::Invalid("noncanonical handover encoding"));
        }
        Ok(out)
    }
    /// Direct content roots, not a claim that their bytes have been verified.
    pub fn roots(&self) -> Vec<Hash> {
        let mut roots = vec![
            self.checkpoint_digest,
            self.account_input_digest,
            self.input_digest,
        ];
        roots.extend(self.dependencies.iter().map(|(_, hash)| *hash));
        roots.extend(self.preflight_receipt_digest);
        roots.extend(self.replay_installation_digest);
        roots.extend(self.replay_restore_digest);
        roots.extend(self.replay_rollback_digest);
        roots.extend(self.rebuild_installation_digest);
        roots.extend(self.rebuild_rollback_digest);
        roots
    }
    pub fn verify_checkpoint(&self, bytes: &[u8]) -> Result<()> {
        self.validate()?;
        if bytes.len() <= 96
            || bytes.len() as u64 != self.checkpoint_bytes
            || crate::digest(bytes) != self.checkpoint_digest
            || &bytes[..8] != b"TWHS0001"
            || bytes[8..24] != self.instance
            || bytes[24..32] != self.generation.to_le_bytes()
            || bytes[32..40] != self.logical_strategy.to_le_bytes()
            || bytes[40..48] != self.report_position.to_le_bytes()
            || bytes[48..56] != self.ledger_sequence.to_le_bytes()
            || bytes[56..88] != self.orders_digest
            || u64::from_le_bytes(bytes[88..96].try_into().unwrap()) != bytes.len() as u64 - 96
        {
            return Err(Error::Invalid("handover checkpoint"));
        }
        Ok(())
    }
}
