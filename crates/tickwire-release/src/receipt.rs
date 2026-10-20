//! Canonical completed-callback data. Decoding is not authentication: Host also
//! checks this against the Registry-owned process and its authenticated message.
use crate::*;
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Process {
    pub instance: [u8; 16],
    pub generation: u64,
    pub pid: u32,
    pub uid: u32,
    pub gid: u32,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields, rename_all = "snake_case")]
pub enum Kind {
    Restore,
    RollbackProbe { parent_receipt: Hash },
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Receipt {
    pub schema: u16,
    pub kind: Kind,
    pub roots: [Hash; 3],
    pub member: Member,
    pub logical_strategy: u64,
    pub process: Process,
    pub binary_digest: Hash,
    pub effective_bootstrap_digest: Hash,
    pub source_proof: Hash,
    pub source_checkpoint: Hash,
    pub source_schema: u64,
    pub method: Method,
    pub output_schema: u64,
    pub output: Object,
    pub source_report_position: u64,
}
impl Receipt {
    pub fn bytes(&self) -> Result<Vec<u8>> {
        self.shape()?;
        encode(self)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let r: Self = decode(bytes)?;
        r.shape()?;
        Ok(r)
    }
    pub fn digest(&self) -> Result<Hash> {
        Ok(tickwire_recovery::digest(&self.bytes()?))
    }
    fn shape(&self) -> Result<()> {
        if self.schema != 1
            || self.roots.contains(&[0; 32])
            || self.logical_strategy == 0
            || self.process.instance == [0; 16]
            || self.process.generation == 0
            || self.process.pid == 0
            || self.binary_digest == [0; 32]
            || self.effective_bootstrap_digest == [0; 32]
            || self.source_proof == [0; 32]
            || self.source_checkpoint == [0; 32]
            || self.source_schema == 0
            || self.output_schema == 0
        {
            return Err(Error::Identity);
        }
        self.output
            .validate(tickwire_recovery::MAX_OBJECT_BYTES as u64)?;
        self.method.check_source(self.source_schema)?;
        if self.method.target_schema() != self.output_schema {
            return Err(Error::Unsupported);
        }
        if matches!(self.kind,Kind::RollbackProbe{parent_receipt} if parent_receipt==[0;32]) {
            return Err(Error::Identity);
        }
        Ok(())
    }
    pub fn verify(&self, release: &MemberRelease<'_>, parent: Option<&Receipt>) -> Result<()> {
        self.shape()?;
        if self.roots != release.roots()
            || self.member != release.target.member
            || self.logical_strategy != release.logical_strategy()
        {
            return Err(Error::Identity);
        }
        let files = match self.kind {
            Kind::Restore => {
                if parent.is_some()
                    || self.process.instance != self.member.new_instance
                    || self.process.generation != self.member.new_generation
                    || self.source_proof != release.migration.source_proof.digest
                    || self.source_checkpoint != release.migration.source_checkpoint.digest
                    || self.source_schema != release.migration.source_schema
                    || self.method != release.migration.method
                    || self.output_schema != release.target.capabilities.state_schema
                {
                    return Err(Error::Identity);
                }
                &release.target.files
            }
            Kind::RollbackProbe { parent_receipt } => {
                let parent = parent.ok_or(Error::Missing)?;
                if parent.kind != Kind::Restore {
                    return Err(Error::Identity);
                }
                parent.verify(release, None)?;
                if parent.digest()? != parent_receipt
                    || self.source_proof != parent_receipt
                    || self.source_checkpoint != parent.output.digest
                    || self.source_schema != parent.output_schema
                    || self.source_report_position != parent.source_report_position
                    || self.method != release.rollback.method
                    || self.output_schema != release.rollback.capabilities.state_schema
                    || self.process.instance == self.member.new_instance
                    || self.process.instance == self.member.old_instance
                {
                    return Err(Error::Identity);
                }
                &release.rollback.files
            }
        };
        if files
            .iter()
            .find(|f| f.role == Role::Binary)
            .map(|f| f.object.digest)
            != Some(self.binary_digest)
        {
            return Err(Error::Identity);
        }
        Ok(())
    }
}

/// Host persists this only after authenticating both child receipts and observing
/// a successful probe exit. Decoding alone provides no live-process authority.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Completion {
    pub schema: u16,
    pub operation: Operation,
    pub roots: [Hash; 3],
    pub member: Member,
    pub logical_strategy: u64,
    pub restored: Object,
    pub rollback: Object,
}
impl Completion {
    pub fn bytes(&self) -> Result<Vec<u8>> {
        self.shape()?;
        encode(self)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let v: Self = decode(bytes)?;
        v.shape()?;
        Ok(v)
    }
    fn shape(&self) -> Result<()> {
        self.operation.validate().map_err(|_| Error::Identity)?;
        if self.schema != 1
            || self.roots.contains(&[0; 32])
            || self.logical_strategy == 0
            || self.restored.digest == self.rollback.digest
            || self.roots
                != [
                    self.operation.artifacts_digest,
                    self.operation.checkpoint_digest,
                    self.operation.rollback_digest,
                ]
            || self.logical_strategy != self.operation.logical_strategy_id
            || !self.operation.members.contains(&self.member)
        {
            return Err(Error::Identity);
        }
        self.restored.validate(MAX_DOCUMENT as u64)?;
        self.rollback.validate(MAX_DOCUMENT as u64)?;
        Ok(())
    }
    pub fn object_roots(&self) -> Vec<Hash> {
        let mut v = self.roots.to_vec();
        v.extend([self.restored.digest, self.rollback.digest]);
        v
    }
    pub fn verify(
        &self,
        release: &MemberRelease<'_>,
        restored: &[u8],
        rollback: &[u8],
    ) -> Result<(Receipt, Receipt)> {
        self.shape()?;
        if self.operation != *release.operation()
            || self.roots != release.roots()
            || self.member != release.target.member
            || self.logical_strategy != release.logical_strategy()
        {
            return Err(Error::Identity);
        }
        self.restored.verify(restored, MAX_DOCUMENT as u64)?;
        self.rollback.verify(rollback, MAX_DOCUMENT as u64)?;
        let a = Receipt::decode(restored)?;
        let b = Receipt::decode(rollback)?;
        a.verify(release, None)?;
        b.verify(release, Some(&a))?;
        if a.process.pid == b.process.pid {
            return Err(Error::Identity);
        }
        Ok((a, b))
    }
}
