//! Historical strategy transition bindings retained by account journal replay.
use crate::{Error, Hash, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
pub const MAX_MEMBERS: usize = 64;
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Member {
    pub risk_domain: [u8; 16],
    pub exec_instance: [u8; 16],
    pub exec_epoch: u64,
    pub old_instance: [u8; 16],
    pub old_generation: u64,
    pub new_instance: [u8; 16],
    pub new_generation: u64,
    pub clock_boot_id: [u8; 16],
    pub clock_generation: u64,
    pub valid_until_ns: u64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Operation {
    pub schema: u32,
    pub operation_id: [u8; 16],
    pub operation_revision: u64,
    pub logical_strategy_id: u64,
    pub members: Vec<Member>,
    pub artifacts_digest: Hash,
    pub checkpoint_digest: Hash,
    pub rollback_digest: Hash,
    pub clock_boot_id: [u8; 16],
    pub clock_generation: u64,
    pub valid_until_ns: u64,
}
impl Operation {
    pub fn validate(&self) -> Result<()> {
        if !matches!(self.schema, 1..=4)
            || self.operation_id == [0; 16]
            || self.operation_revision == 0
            || self.logical_strategy_id == 0
            || self.members.is_empty()
            || self.members.len() > MAX_MEMBERS
            || self
                .members
                .windows(2)
                .any(|m| m[0].risk_domain >= m[1].risk_domain)
            || [
                self.artifacts_digest,
                self.checkpoint_digest,
                self.rollback_digest,
            ]
            .contains(&[0; 32])
            || self.clock_boot_id == [0; 16]
            || self.clock_generation == 0
            || self.valid_until_ns == 0
        {
            return Err(Error::Invalid);
        }
        for m in &self.members {
            if [
                m.risk_domain,
                m.exec_instance,
                m.old_instance,
                m.new_instance,
            ]
            .contains(&[0; 16])
                || m.exec_epoch == 0
                || m.old_generation == 0
                || m.new_generation <= m.old_generation
                || m.new_instance == m.old_instance
                || m.clock_boot_id == [0; 16]
                || m.clock_generation == 0
                || m.valid_until_ns == 0
            {
                return Err(Error::Identity);
            }
        }
        Ok(())
    }
    pub fn members_digest(&self) -> Result<Hash> {
        self.validate()?;
        digest(b"tickwire.lifecycle.members.v1", &self.members)
    }
}
fn canonical<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let b = serde_json::to_vec(value).map_err(|_| Error::Invalid)?;
    if b.len() > crate::MAX_DOCUMENT {
        return Err(Error::Bounds);
    }
    Ok(b)
}
fn digest<T: Serialize>(domain: &[u8], value: &T) -> Result<Hash> {
    let mut h = Sha256::new();
    h.update(domain);
    h.update(canonical(value)?);
    Ok(h.finalize().into())
}
