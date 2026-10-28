//! Durable target selection. The producer must execute migration/configuration
//! callbacks and authenticate the control request before writing this record.
use super::*;
mod verify;
pub use verify::{Verified, verify};
pub(crate) use verify::{business_state, verify_at};
/// Complete content request carried by authenticated control 41/kind 3. The
/// worker still loads its own qualified deployment files before any callback.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub schema: u16,
    pub operation: crate::Operation,
    pub member: crate::Member,
    pub source_completion: Object,
    pub source_objects: Vec<Object>,
    pub target: Source,
    pub config: VersionedBytes,
}
impl Request {
    pub fn bytes(&self) -> Result<Vec<u8>> {
        self.validate()?;
        canonical(self)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let value: Self = decode(bytes)?;
        value.validate()?;
        Ok(value)
    }
    fn validate(&self) -> Result<()> {
        self.operation.validate().map_err(|_| Error::Identity)?;
        if self.schema != 1
            || self.operation.schema != 3
            || !self.operation.members.contains(&self.member)
            || self.target.logical_strategy != self.operation.logical_strategy_id
            || self.target.instance != self.member.new_instance
            || self.target.generation != self.member.new_generation
            || self.target.binary_digest == [0; 32]
            || self.target.state_schema == 0
            || self.target.input_stream == [0; 16]
            || self.target.clock.boot_id == [0; 16]
            || self.target.clock.generation == 0
            || self.target.clock.boot_id != self.member.clock_boot_id
            || self.target.clock.generation != self.member.clock_generation
            || self.source_objects.is_empty()
            || self.source_objects.len() > 512
            || !self.source_objects.contains(&self.source_completion)
        {
            return Err(Error::Identity);
        }
        self.source_completion.validate(MAX_BYTES as u64)?;
        version(&self.config)?;
        let mut bytes = 0u64;
        let mut previous = None;
        for object in &self.source_objects {
            object.validate(tickwire_recovery::MAX_OBJECT_BYTES as u64)?;
            if previous.is_some_and(|p| p >= object.digest) {
                return Err(Error::Identity);
            }
            previous = Some(object.digest);
            bytes = bytes
                .checked_add(object.bytes)
                .filter(|n| *n <= tickwire_recovery::MAX_CLOSURE_BYTES)
                .ok_or(Error::Bounds)?;
        }
        Ok(())
    }
    /// Caller supplies only bounded object reads. No declaration of execution
    /// in this request substitutes for the checked source completion graph.
    pub fn source_proof(
        &self,
        mut get: impl FnMut(Object) -> Result<Vec<u8>>,
    ) -> Result<source::VerifiedSourceReplay> {
        self.validate()?;
        let mut objects = Vec::with_capacity(self.source_objects.len());
        for object in &self.source_objects {
            let bytes = get(*object)?;
            object.verify(&bytes, tickwire_recovery::MAX_OBJECT_BYTES as u64)?;
            objects.push(bytes);
        }
        source::VerifiedSourceReplay::verify(
            &self.operation,
            &self.member,
            self.source_completion,
            objects,
        )
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Installation {
    pub schema: u16,
    pub request: Object,
    pub source_completion: Object,
    pub previous_target: Object,
    pub previous_commit: Object,
    pub target: Source,
    pub migrated: Object,
    pub config_apply: Object,
    pub checkpoint: Object,
    pub commit: Object,
}
/// The selected target has consumed the exact retained C-after inputs with
/// publication disabled, and synchronized its resulting checkpoint and tail.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Continued {
    pub schema: u16,
    pub installation: Object,
    pub inputs: Object,
    pub checkpoint: Object,
    pub commit: Object,
    pub completed_ns: u64,
}
impl Continued {
    pub fn bytes(&self) -> Result<Vec<u8>> {
        if self.schema != 1 {
            return Err(Error::Identity);
        }
        for o in [self.installation, self.inputs, self.checkpoint, self.commit] {
            o.validate(MAX_BYTES as u64)?;
        }
        canonical(self)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let v: Self = decode(bytes)?;
        v.bytes()?;
        Ok(v)
    }
}
impl Installation {
    pub fn bytes(&self) -> Result<Vec<u8>> {
        self.validate()?;
        canonical(self)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let value: Self = decode(bytes)?;
        value.validate()?;
        Ok(value)
    }
    fn validate(&self) -> Result<()> {
        if self.schema != 1
            || self.target.instance == [0; 16]
            || self.target.generation == 0
            || self.target.input_stream == [0; 16]
        {
            return Err(Error::Identity);
        }
        for o in [
            self.request,
            self.source_completion,
            self.previous_target,
            self.previous_commit,
            self.migrated,
            self.config_apply,
            self.checkpoint,
            self.commit,
        ] {
            o.validate(MAX_BYTES as u64)?;
        }
        Ok(())
    }
}
