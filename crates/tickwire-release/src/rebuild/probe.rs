//! Stored historical probe facts. Content verification neither executes a factory
//! nor replaces the original Host-owned successful child exit observation.
use super::rollback as policy;
use super::*;
use crate::files::{Location, ObjectFile};
use crate::replay::{
    Checkpoint, Cut, MAX_BYTES, canonical, encode_tail, verify_anchor, verify_tail,
};
use std::{collections::BTreeMap, path::PathBuf};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Job {
    pub schema: u16,
    pub attempt: [u8; 16],
    pub operation: Operation,
    pub member: Member,
    pub plans: ObjectFile,
    pub manifest: ObjectFile,
    pub contract: ObjectFile,
    pub inputs: ObjectFile,
    pub rollback: ObjectFile,
    pub objects: Vec<ObjectFile>,
    pub files: Vec<Location>,
    pub output_directory: PathBuf,
}
impl Job {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        super::decode(bytes)
    }
    pub fn bytes(&self) -> Result<Vec<u8>> {
        canonical(self)
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Receipt {
    pub schema: u16,
    pub job: Object,
    pub attempt: [u8; 16],
    pub rollback: Object,
    pub member: Member,
    pub probe: policy::Probe,
    pub pid: u32,
    pub exact: bool,
    pub anchor: Object,
    pub transcript: Object,
    pub checkpoint: Object,
    pub cut: Cut,
}
impl Receipt {
    pub fn bytes(&self) -> Result<Vec<u8>> {
        canonical(self)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > MAX_BYTES {
            return Err(Error::Bounds);
        }
        let v: Self = serde_json::from_slice(bytes).map_err(|_| Error::Encoding)?;
        if v.schema != 1 || v.attempt == [0; 16] || v.pid == 0 || v.bytes()? != bytes {
            return Err(Error::Identity);
        }
        Ok(v)
    }
}

/// Canonical map encoding preserves the Host's original JSON, including nested objects.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Completion {
    pub generation: u64,
    pub gid: u32,
    pub instance: [u8; 16],
    pub job: Object,
    pub pid: u32,
    pub receipt: Object,
    pub schema: u16,
    pub success: bool,
    pub uid: u32,
}
impl Completion {
    pub fn bytes(&self) -> Result<Vec<u8>> {
        if self.schema != 1
            || !self.success
            || self.pid == 0
            || self.generation == 0
            || self.instance == [0; 16]
        {
            return Err(Error::Identity);
        }
        self.job.validate(MAX_BYTES as u64)?;
        self.receipt.validate(MAX_BYTES as u64)?;
        canonical(&serde_json::to_value(self).map_err(|_| Error::Encoding)?)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.is_empty() || bytes.len() > MAX_BYTES {
            return Err(Error::Bounds);
        }
        let value: Self = serde_json::from_slice(bytes).map_err(|_| Error::Encoding)?;
        if value.bytes()? != bytes {
            return Err(Error::Encoding);
        }
        Ok(value)
    }
}
pub struct Verified {
    root: Object,
    completion: Completion,
    receipt: Receipt,
    job: Job,
    checkpoint: Checkpoint,
    objects: BTreeMap<Hash, Vec<u8>>,
}
impl Verified {
    pub fn root(&self) -> Object {
        self.root
    }
    pub fn completion(&self) -> &Completion {
        &self.completion
    }
    pub fn receipt(&self) -> &Receipt {
        &self.receipt
    }
    pub fn job(&self) -> &Job {
        &self.job
    }
    pub fn checkpoint(&self) -> &Checkpoint {
        &self.checkpoint
    }
    pub fn objects(&self) -> impl Iterator<Item = &[u8]> {
        self.objects.values().map(Vec::as_slice)
    }
    pub fn verify(
        root: Object,
        operation: &Operation,
        member: &Member,
        forward: policy::Forward<'_>,
        mut get: impl FnMut(Hash) -> Result<Vec<u8>>,
    ) -> Result<Self> {
        Self::verify_at(root, operation, member, forward, &mut get, 0)
    }
    pub fn verify_at(
        root: Object,
        operation: &Operation,
        member: &Member,
        forward: policy::Forward<'_>,
        get: &mut dyn FnMut(Hash) -> Result<Vec<u8>>,
        depth: usize,
    ) -> Result<Self> {
        if depth >= 16 {
            return Err(Error::Bounds);
        }
        let mut objects = Objects {
            get,
            values: BTreeMap::new(),
            total: 0,
        };
        let completion = Completion::decode(&objects.object(root, MAX_BYTES)?)?;
        let job = Job::decode(&objects.object(completion.job, MAX_BYTES)?)?;
        let receipt = Receipt::decode(&objects.object(completion.receipt, MAX_BYTES)?)?;
        if job.schema != 1
            || job.attempt == [0; 16]
            || job.operation != *operation
            || job.member != *member
            || !job.output_directory.is_absolute()
            || job.objects.len() > 507
            || job.files.len() > crate::MAX_FILES
            || job.files.iter().any(|f| !f.path.is_absolute())
            || receipt.job != completion.job
            || receipt.pid != completion.pid
            || receipt.attempt != job.attempt
            || receipt.member != *member
            || receipt.probe.instance != completion.instance
            || receipt.probe.generation != completion.generation
        {
            return Err(Error::Identity);
        }
        let fixed = [
            (&job.plans, forward.plans.bytes()?),
            (&job.manifest, crate::encode(forward.manifest)?),
            (&job.contract, forward.contract.bytes()?),
            (&job.inputs, forward.inputs.bytes()?),
        ];
        for (file, bytes) in fixed {
            if !file.path.is_absolute() || objects.object(file.object, MAX_BYTES)? != bytes {
                return Err(Error::Identity);
            }
        }
        if !job.rollback.path.is_absolute() {
            return Err(Error::Identity);
        }
        let rollback = policy::Plans::decode(&objects.object(job.rollback.object, MAX_BYTES)?)?;
        for file in &job.objects {
            if !file.path.is_absolute() {
                return Err(Error::Identity);
            }
            objects.object(file.object, tickwire_recovery::MAX_OBJECT_BYTES)?;
        }
        let input = rollback.verify_at(
            operation,
            member,
            forward,
            &mut |h| objects.dependency(h),
            depth,
        )?;
        if receipt.rollback != input.root()
            || job.rollback.object != input.root()
            || receipt.probe != input.plan().probe
            || receipt.exact != input.exact().is_some()
            || job.files.len() != input.plan().files.len()
            || job
                .files
                .iter()
                .zip(&input.plan().files)
                .any(|(path, artifact)| path.role != artifact.role)
        {
            return Err(Error::Identity);
        }
        let anchor = objects.object(receipt.anchor, MAX_BYTES)?;
        let transcript = objects.object(receipt.transcript, MAX_BYTES)?;
        let checkpoint = Checkpoint::decode(&objects.object(receipt.checkpoint, MAX_BYTES)?)?;
        if checkpoint.cut != receipt.cut {
            return Err(Error::Identity);
        }
        validate_result(&input, &checkpoint, &anchor, &transcript)?;
        Ok(Self {
            root,
            completion,
            receipt,
            job,
            checkpoint,
            objects: objects.values,
        })
    }
}

struct Objects<'a> {
    get: &'a mut dyn FnMut(Hash) -> Result<Vec<u8>>,
    values: BTreeMap<Hash, Vec<u8>>,
    total: usize,
}
impl Objects<'_> {
    fn object(&mut self, object: Object, maximum: usize) -> Result<Vec<u8>> {
        object.validate(maximum as u64)?;
        let bytes = self.dependency(object.digest)?;
        object.verify(&bytes, maximum as u64)?;
        Ok(bytes)
    }
    fn dependency(&mut self, hash: Hash) -> Result<Vec<u8>> {
        if let Some(bytes) = self.values.get(&hash) {
            return Ok(bytes.clone());
        }
        if hash == [0; 32] || self.values.len() >= 512 {
            return Err(Error::Bounds);
        }
        let bytes = (self.get)(hash)?;
        if bytes.is_empty()
            || bytes.len() > tickwire_recovery::MAX_OBJECT_BYTES
            || Object::of(&bytes).digest != hash
        {
            return Err(Error::Identity);
        }
        self.total = self
            .total
            .checked_add(bytes.len())
            .filter(|n| *n <= tickwire_recovery::MAX_CLOSURE_BYTES as usize)
            .ok_or(Error::Bounds)?;
        self.values.insert(hash, bytes.clone());
        Ok(bytes)
    }
}

pub fn validate_result(
    input: &policy::Verified,
    checkpoint: &Checkpoint,
    anchor_bytes: &[u8],
    transcript: &[u8],
) -> Result<()> {
    if let Some(exact) = input.exact() {
        if checkpoint != exact.checkpoint()
            || anchor_bytes != exact.anchor().checkpoint().bytes()?
            || transcript != encode_tail(exact.anchor(), exact.tail().rows())?
        {
            return Err(Error::Identity);
        }
    } else {
        let declared = input.declared().ok_or(Error::Identity)?;
        let source = Source {
            logical_strategy: input.logical_strategy(),
            instance: input.plan().probe.instance,
            generation: input.plan().probe.generation,
            binary_digest: declared.contract().binary,
            state_schema: declared.contract().state_schema,
            input_stream: input.plan().probe.input_stream,
            clock: declared.inputs().clock.clone(),
        };
        if checkpoint.source != source
            || checkpoint.runtime.effects.count != 0
            || checkpoint.runtime.config != declared.inputs().config
            || checkpoint.runtime.models != declared.inputs().models
            || checkpoint.cut.sequence != declared.end().rows
            || checkpoint.runtime.now_ns != declared.end().now_ns
            || checkpoint.runtime.markets != declared.end().markets
            || checkpoint.runtime.accounts != declared.end().accounts
            || checkpoint.runtime.external != declared.end().external
        {
            return Err(Error::Identity);
        }
    }
    let anchor = Checkpoint::decode(anchor_bytes)?;
    let models = anchor
        .runtime
        .models
        .iter()
        .map(|m| (m.id, Object::of(&m.value.bytes)))
        .collect::<Vec<_>>();
    let anchor = verify_anchor(
        anchor_bytes,
        &checkpoint.source,
        Object::of(&anchor.runtime.config.bytes),
        &models,
    )?;
    let tail = verify_tail(&anchor, transcript, &checkpoint.cut)?;
    if let Some(declared) = input.declared() {
        let start = anchor.checkpoint();
        if start.cut.sequence != 0
            || start.cut.chain != [0; 32]
            || start.runtime.now_ns != declared.inputs().start_ns
            || start.runtime.config != declared.inputs().config
            || start.runtime.models != declared.inputs().models
            || start.runtime.rng != declared.contract().rng
            || !start.runtime.timers.is_empty()
            || start.runtime.effects.count != 0
            || start.runtime.effects.digest != [0; 32]
            || checkpoint.runtime.effects.digest != [0; 32]
            || tail.rows().len() != declared.inputs().warm.len()
            || tail
                .rows()
                .iter()
                .zip(&declared.inputs().warm)
                .any(|(actual, expected)| {
                    actual.now_ns != expected.now_ns
                        || actual.input != expected.input
                        || !actual.effects.is_empty()
                })
        {
            return Err(Error::Identity);
        }
    }
    Ok(())
}
