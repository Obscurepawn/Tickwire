//! Rollback of the actual C migration/configuration output. The Host separately
//! binds the receipt to its registered subprocess and successful exit.
use super::*;
use crate::{
    Method,
    files::{Location, ObjectFile},
    receipt::Process,
};
use std::{collections::BTreeMap, path::PathBuf};
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Job {
    pub schema: u16,
    pub installation: Object,
    pub objects: Vec<ObjectFile>,
    pub rollback_files: Vec<Location>,
    pub output_directory: PathBuf,
}
impl Job {
    pub fn bytes(&self) -> Result<Vec<u8>> {
        if self.schema != 1
            || self.objects.is_empty()
            || self.objects.len() > 512
            || self.rollback_files.len() > crate::MAX_FILES
            || !self.output_directory.is_absolute()
        {
            return Err(Error::Bounds);
        }
        self.installation.validate(MAX_BYTES as u64)?;
        let mut total = 0u64;
        let mut last = None;
        for f in &self.objects {
            f.object.validate(MAX_BYTES as u64)?;
            if !f.path.is_absolute() || last.is_some_and(|p| p >= f.object.digest) {
                return Err(Error::Identity);
            }
            last = Some(f.object.digest);
            total = total
                .checked_add(f.object.bytes)
                .filter(|n| *n <= tickwire_recovery::MAX_CLOSURE_BYTES)
                .ok_or(Error::Bounds)?;
        }
        if !self.objects.iter().any(|f| f.object == self.installation) {
            return Err(Error::Missing);
        }
        canonical(self)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let v: Self = decode(bytes)?;
        v.bytes()?;
        Ok(v)
    }
    pub fn load(&self) -> Result<install::Verified> {
        self.bytes()?;
        let mut map = BTreeMap::new();
        for f in &self.objects {
            map.insert(f.object.digest, f.read()?);
        }
        install::verify(self.installation.digest, |h| {
            map.get(&h).cloned().ok_or(Error::Missing)
        })
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Receipt {
    pub schema: u16,
    pub job: Object,
    pub installation: Object,
    pub operation: crate::Operation,
    pub member: crate::Member,
    pub process: Process,
    pub binary_digest: Hash,
    pub bootstrap_digest: Hash,
    pub input_checkpoint: Object,
    pub input_state: Object,
    pub source_schema: u64,
    pub method: Method,
    pub output_schema: u64,
    pub output: Object,
}
impl Receipt {
    pub fn bytes(&self) -> Result<Vec<u8>> {
        if self.schema != 1
            || self.process.pid == 0
            || self.process.instance == [0; 16]
            || self.process.generation == 0
            || self.binary_digest == [0; 32]
            || self.bootstrap_digest == [0; 32]
        {
            return Err(Error::Identity);
        }
        for o in [
            self.job,
            self.installation,
            self.input_checkpoint,
            self.input_state,
            self.output,
        ] {
            o.validate(MAX_BYTES as u64)?;
        }
        canonical(self)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let r: Self = decode(bytes)?;
        r.bytes()?;
        Ok(r)
    }
    pub fn verify(&self, installed: &install::Verified) -> Result<()> {
        self.bytes()?;
        let r = &installed.request;
        let get = |hash| {
            installed
                .objects
                .iter()
                .find(|b| Object::of(b).digest == hash)
                .cloned()
                .ok_or(Error::Missing)
        };
        let bundle = crate::ReleaseBundle::decode(
            &get(r.operation.artifacts_digest)?,
            &get(r.operation.checkpoint_digest)?,
            &get(r.operation.rollback_digest)?,
        )?;
        let release = bundle.verify_operation(&r.operation, &r.member)?;
        let expected = |role| {
            release
                .rollback
                .files
                .iter()
                .find(|f| f.role == role)
                .map(|f| f.object.digest)
        };
        if self.operation != r.operation
            || self.member != r.member
            || self.installation != Object::of(&installed.continued.bytes()?)
            || [r.member.new_instance, r.member.old_instance].contains(&self.process.instance)
            || Some(self.binary_digest) != expected(crate::Role::Binary)
            || Some(self.bootstrap_digest) != expected(crate::Role::Bootstrap)
            || self.input_checkpoint != installed.installation.checkpoint
            || self.input_state != Object::of(&installed.installed_business()?)
            || self.source_schema != installed.initial_checkpoint()?.source.state_schema
            || self.method != release.rollback.method
            || self.output_schema != release.rollback.capabilities.state_schema
        {
            return Err(Error::Identity);
        }
        self.method.check_source(self.source_schema)?;
        if self.method.target_schema() != self.output_schema {
            return Err(Error::Unsupported);
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Completion {
    pub schema: u16,
    pub installation: Object,
    pub rollback: Object,
}
impl Completion {
    pub fn bytes(&self) -> Result<Vec<u8>> {
        if self.schema != 1 {
            return Err(Error::Identity);
        }
        self.installation.validate(MAX_BYTES as u64)?;
        self.rollback.validate(MAX_BYTES as u64)?;
        canonical(self)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let v: Self = decode(bytes)?;
        v.bytes()?;
        Ok(v)
    }
}
pub struct Verified {
    pub root: Object,
    pub installation: install::Verified,
    pub receipt: Receipt,
    pub objects: Vec<Vec<u8>>,
}
pub fn verify(root: Hash, mut read: impl FnMut(Hash) -> Result<Vec<u8>>) -> Result<Verified> {
    verify_at(root, &mut read, 0)
}
pub(crate) fn verify_at(
    root: Hash,
    read: &mut dyn FnMut(Hash) -> Result<Vec<u8>>,
    depth: usize,
) -> Result<Verified> {
    if depth >= 16 {
        return Err(Error::Bounds);
    }
    let mut map = BTreeMap::new();
    let mut total = 0usize;
    let mut get = |hash| -> Result<Vec<u8>> {
        if let Some(b) = map.get(&hash) {
            return Ok(Vec::clone(b));
        }
        if map.len() >= 512 {
            return Err(Error::Bounds);
        }
        let bytes = read(hash)?;
        if Object::of(&bytes).digest != hash || bytes.is_empty() || bytes.len() > MAX_BYTES {
            return Err(Error::Identity);
        }
        total = total
            .checked_add(bytes.len())
            .filter(|n| *n <= tickwire_recovery::MAX_CLOSURE_BYTES as usize)
            .ok_or(Error::Bounds)?;
        map.insert(hash, bytes.clone());
        Ok(bytes)
    };
    let bytes = get(root)?;
    let completion = Completion::decode(&bytes)?;
    completion
        .installation
        .verify(&get(completion.installation.digest)?, MAX_BYTES as u64)?;
    let installed = install::verify_at(completion.installation.digest, &mut get, depth + 1)?;
    let bytes = get(completion.rollback.digest)?;
    completion.rollback.verify(&bytes, MAX_BYTES as u64)?;
    let receipt = Receipt::decode(&bytes)?;
    receipt.verify(&installed)?;
    let job_bytes = get(receipt.job.digest)?;
    receipt.job.verify(&job_bytes, MAX_BYTES as u64)?;
    let job = Job::decode(&job_bytes)?;
    if job.installation != completion.installation {
        return Err(Error::Identity);
    }
    for f in &job.objects {
        f.object.verify(&get(f.object.digest)?, MAX_BYTES as u64)?;
    }
    receipt
        .output
        .verify(&get(receipt.output.digest)?, MAX_BYTES as u64)?;
    receipt
        .input_state
        .verify(&get(receipt.input_state.digest)?, MAX_BYTES as u64)?;
    let root_object = Object::of(&get(root)?);
    Ok(Verified {
        root: root_object,
        installation: installed,
        receipt,
        objects: map.into_values().collect(),
    })
}
