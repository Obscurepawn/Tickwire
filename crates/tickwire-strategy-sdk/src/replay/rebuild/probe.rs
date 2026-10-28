//! Isolated old-version computation. Host authenticates its owned child and exit;
//! reading this receipt alone never establishes that a process ran.
use super::*;
use tickwire_release::{
    files::{LoadedFiles, ObjectFile},
    rebuild::rollback as policy,
};

use tickwire_release::rebuild::probe::validate_result;
pub use tickwire_release::rebuild::probe::{Job, Receipt};
/// Read-only qualification. Neither file content nor this value authenticates a child.
pub struct Qualified {
    job: Object,
    input: policy::Verified,
    files: LoadedFiles,
    objects: BTreeMap<Hash, Vec<u8>>,
    total: u64,
}
impl Qualified {
    pub fn input(&self) -> &policy::Verified {
        &self.input
    }
    pub fn files(&self) -> &LoadedFiles {
        &self.files
    }
}
pub fn qualify(job: &Job) -> crate::Result<Qualified> {
    if job.schema != 1
        || job.attempt == [0; 16]
        || !job.output_directory.is_absolute()
        || job.objects.len() > 507
        || job.files.len() > tickwire_release::MAX_FILES
    {
        return Err(crate::Error::Identity);
    }
    job.operation
        .validate()
        .map_err(|_| crate::Error::Identity)?;
    if !job.operation.members.contains(&job.member) {
        return Err(crate::Error::Identity);
    }
    // Declared lengths bound all object reads before the first allocation. A
    // duplicate digest has one byte charge and must retain the exact length.
    let list = [
        &job.plans,
        &job.manifest,
        &job.contract,
        &job.inputs,
        &job.rollback,
    ]
    .into_iter()
    .map(|file| (file, MAX_BYTES))
    .chain(
        job.objects
            .iter()
            .map(|file| (file, tickwire_recovery::MAX_OBJECT_BYTES)),
    )
    .collect::<Vec<_>>();
    let mut lengths = BTreeMap::new();
    let mut total = 0u64;
    for (file, maximum) in &list {
        file.object.validate(*maximum as u64)?;
        if !file.path.is_absolute() {
            return Err(crate::Error::Identity);
        }
        if let Some(old) = lengths.insert(file.object.digest, file.object.bytes) {
            if old != file.object.bytes {
                return Err(crate::Error::Identity);
            }
        } else {
            total = total
                .checked_add(file.object.bytes)
                .filter(|n| *n <= tickwire_recovery::MAX_CLOSURE_BYTES)
                .ok_or(crate::Error::Bounds)?;
        }
    }
    let mut objects = BTreeMap::new();
    for (file, _) in list {
        if let std::collections::btree_map::Entry::Vacant(e) = objects.entry(file.object.digest) {
            e.insert(file.read()?);
        }
    }
    let read = |file: &ObjectFile| -> crate::Result<&[u8]> {
        objects
            .get(&file.object.digest)
            .map(Vec::as_slice)
            .ok_or(crate::Error::Identity)
    };
    let plans = Plans::decode(read(&job.plans)?)?;
    let contract = Contract::decode(read(&job.contract)?)?;
    let inputs = Inputs::decode(read(&job.inputs)?)?;
    let manifest: ReleaseManifest = serde_json::from_slice(read(&job.manifest)?)?;
    if canonical(&manifest)? != read(&job.manifest)? {
        return Err(crate::Error::Identity);
    }
    let rollback = policy::Plans::decode(read(&job.rollback)?)?;
    let input = rollback.verify(
        &job.operation,
        &job.member,
        policy::Forward {
            plans: &plans,
            contract: &contract,
            inputs: &inputs,
            manifest: &manifest,
        },
        |hash| {
            objects
                .get(&hash)
                .cloned()
                .ok_or(tickwire_release::Error::Missing)
        },
    )?;
    let files = tickwire_release::files::load(&input.plan().files, &job.files)?;
    files.recheck()?;
    Ok(Qualified {
        job: Object::of(&job.bytes()?),
        input,
        files,
        objects,
        total,
    })
}
pub fn run_with(
    job: &Job,
    compute: impl FnOnce(&policy::Verified, &LoadedFiles) -> crate::Result<rollback::Computation>,
) -> crate::Result<Receipt> {
    use std::os::unix::fs::{DirBuilderExt, MetadataExt};
    let Qualified {
        input,
        files,
        mut objects,
        mut total,
        ..
    } = qualify(job)?;
    if files
        .object(&tickwire_release::Role::Binary)
        .map(|o| o.digest)
        != Some(live::executable_digest()?)
    {
        return Err(crate::Error::Identity);
    }
    files.recheck()?;
    let parent = job
        .output_directory
        .parent()
        .ok_or(crate::Error::Identity)?;
    let metadata = std::fs::symlink_metadata(parent)?;
    if !metadata.is_dir() || metadata.mode() & 0o022 != 0 {
        return Err(crate::Error::Identity);
    }
    // An existing attempt is never silently re-executed, including after a
    // callback or sync failure. Host recovers its saved result or uses a new
    // explicitly approved probe identity/attempt under a new operation.
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(&job.output_directory)?;
    std::fs::File::open(parent)?.sync_all()?;
    let job_bytes = job.bytes()?;
    crate::recovery::immutable(&job.output_directory.join("ATTEMPT.json"), &job_bytes)?;
    let value = compute(&input, &files)?;
    files.recheck()?;
    let checkpoint = &value.checkpoint;
    if value.rollback != input.root() || value.exact != input.exact().is_some() {
        return Err(crate::Error::Identity);
    }
    validate_result(&input, checkpoint, &value.anchor, &value.transcript)?;
    let output = checkpoint.bytes()?;
    let receipt = Receipt {
        schema: 1,
        job: Object::of(&job_bytes),
        attempt: job.attempt,
        rollback: input.root(),
        member: job.member.clone(),
        probe: input.plan().probe.clone(),
        pid: std::process::id(),
        exact: value.exact,
        anchor: Object::of(&value.anchor),
        transcript: Object::of(&value.transcript),
        checkpoint: Object::of(&output),
        cut: checkpoint.cut.clone(),
    };
    let bytes = receipt.bytes()?;
    for value in [
        &job_bytes,
        &value.anchor,
        &value.transcript,
        &output,
        &bytes,
    ] {
        let hash = Object::of(value).digest;
        if let std::collections::btree_map::Entry::Vacant(e) = objects.entry(hash) {
            total = total
                .checked_add(value.len() as u64)
                .filter(|n| *n <= tickwire_recovery::MAX_CLOSURE_BYTES)
                .ok_or(crate::Error::Bounds)?;
            e.insert(value.clone());
        }
    }
    if objects.len() > 512 {
        return Err(crate::Error::Bounds);
    }
    // The public completion marker is last. Every referenced byte has returned
    // from file+directory sync before this receipt becomes externally readable.
    for value in objects.values() {
        crate::recovery::object(&job.output_directory, value)?;
    }
    crate::recovery::immutable(&job.output_directory.join("REBUILD_PROBE.json"), &bytes)?;
    Ok(receipt)
}

/// Retains verified content only. Host must separately observe its owned child's
/// successful exit before recording this as an executed probe.
pub struct ReadBack {
    receipt: Receipt,
    checkpoint: Checkpoint,
    objects: BTreeMap<Hash, Vec<u8>>,
}
impl ReadBack {
    pub fn receipt(&self) -> &Receipt {
        &self.receipt
    }
    pub fn checkpoint(&self) -> &Checkpoint {
        &self.checkpoint
    }
    pub fn objects(&self) -> impl Iterator<Item = &[u8]> {
        self.objects.values().map(Vec::as_slice)
    }
}
pub fn read_result(job: &Job, qualified: Qualified, pid: u32) -> crate::Result<ReadBack> {
    let Qualified {
        input,
        files,
        mut objects,
        mut total,
        job: expected_job,
    } = qualified;
    if expected_job != Object::of(&job.bytes()?) || pid == 0 {
        return Err(crate::Error::Identity);
    }
    files.recheck()?;
    let bytes = crate::recovery::read(&job.output_directory.join("REBUILD_PROBE.json"), 64 * 1024)?;
    let receipt = Receipt::decode(&bytes)?;
    if receipt.job != expected_job
        || receipt.attempt != job.attempt
        || receipt.member != job.member
        || receipt.rollback != input.root()
        || receipt.probe != input.plan().probe
        || receipt.pid != pid
        || receipt.exact != input.exact().is_some()
    {
        return Err(crate::Error::Identity);
    }
    let mut load = |o: Object| -> crate::Result<Vec<u8>> {
        o.validate(MAX_BYTES as u64)?;
        if let Some(bytes) = objects.get(&o.digest) {
            o.verify(bytes, MAX_BYTES as u64)?;
            return Ok(bytes.clone());
        }
        if objects.len() >= 512 {
            return Err(crate::Error::Bounds);
        }
        total = total
            .checked_add(o.bytes)
            .filter(|n| *n <= tickwire_recovery::MAX_CLOSURE_BYTES)
            .ok_or(crate::Error::Bounds)?;
        let name = o
            .digest
            .iter()
            .map(|n| format!("{n:02x}"))
            .collect::<String>();
        let b = tickwire_release::files::read_object(
            &job.output_directory.join(format!("{name}.bin")),
            o,
        )?;
        objects.insert(o.digest, b.clone());
        Ok(b)
    };
    let anchor = load(receipt.anchor)?;
    let transcript = load(receipt.transcript)?;
    let checkpoint = Checkpoint::decode(&load(receipt.checkpoint)?)?;
    if checkpoint.cut != receipt.cut {
        return Err(crate::Error::Identity);
    }
    validate_result(&input, &checkpoint, &anchor, &transcript)?;
    for b in [job.bytes()?, bytes] {
        if let std::collections::btree_map::Entry::Vacant(e) = objects.entry(Object::of(&b).digest)
        {
            total = total
                .checked_add(b.len() as u64)
                .filter(|n| *n <= tickwire_recovery::MAX_CLOSURE_BYTES)
                .ok_or(crate::Error::Bounds)?;
            e.insert(b);
        }
    }
    if objects.len() > 512 {
        return Err(crate::Error::Bounds);
    }
    Ok(ReadBack {
        receipt,
        checkpoint,
        objects,
    })
}
