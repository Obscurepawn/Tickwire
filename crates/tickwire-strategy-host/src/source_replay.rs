//! Old registered executable, isolated from live lanes and all intent output.
use super::*;
use tickwire_release::{
    Binding, Member, Object, Operation,
    replay::{
        CapturedTail, Checkpoint, ControlInput,
        source::{Completion, SourceReceipt, VerifiedSourceReplay},
    },
};
use tickwire_strategy_sdk::{preflight::ObjectFile, replay::process::SourceJob};

fn object(directory: &Path, bytes: &[u8]) -> Result<ObjectFile> {
    preflight::persist_object(directory, bytes)?;
    Ok(preflight::object_file(directory, Object::of(bytes)))
}
fn find(objects: &[Vec<u8>], digest: [u8; 32]) -> Result<&[u8]> {
    objects
        .iter()
        .find(|b| tickwire_recovery::digest(b) == digest)
        .map(Vec::as_slice)
        .ok_or(Error::Invalid)
}
pub(super) fn cut(objects: &[Vec<u8>], proof: [u8; 32]) -> Result<Checkpoint> {
    let seal = tickwire_recovery::HandoverSeal::decode(find(objects, proof)?)?;
    let retained = tickwire_release::replay::retained::decode(find(objects, seal.input_digest)?)?;
    let c = retained.current();
    let state = tickwire_strategy_sdk::replay::application::business_bytes(&c.business)
        .map_err(tickwire_strategy_sdk::Error::Replay)?;
    if find(objects, seal.checkpoint_digest)?.get(96..) != Some(state.as_slice())
        || c.source.instance != seal.instance
        || c.source.generation != seal.generation
        || c.source.logical_strategy != seal.logical_strategy
    {
        return Err(Error::Invalid);
    }
    Ok(c.clone())
}
// Both authenticated cuts and their object sets remain explicit at this boundary.
#[allow(clippy::too_many_arguments)]
pub(super) fn run(
    spec: &LaunchSpec,
    directory: &Path,
    operation: &Operation,
    member: &Member,
    fixed: tickwire_runtime::node::HandoverReadiness,
    fixed_objects: Vec<Vec<u8>>,
    current: tickwire_runtime::node::HandoverReadiness,
    current_objects: Vec<Vec<u8>>,
) -> Result<VerifiedSourceReplay> {
    operation.validate().map_err(|_| Error::Invalid)?;
    if !operation.members.contains(member)
        || spec.identity.instance != member.old_instance
        || spec.identity.generation != member.old_generation
        || spec.identity.logical_strategy != operation.logical_strategy_id
        || fixed.instance_id != member.old_instance
        || fixed.strategy_generation != member.old_generation
        || current.instance_id != member.old_instance
        || current.strategy_generation != member.old_generation
    {
        return Err(Error::Invalid);
    }
    let a = cut(&fixed_objects, fixed.input_seal_digest)?;
    let c = cut(&current_objects, current.input_seal_digest)?;
    if a.source != c.source
        || a.source.binary_digest != spec.identity.build_hash
        || a.source.clock.boot_id != spec.identity.host_boot
    {
        return Err(Error::Invalid);
    }
    let seal = tickwire_recovery::HandoverSeal::decode(find(
        &current_objects,
        current.input_seal_digest,
    )?)?;
    let retained =
        tickwire_release::replay::retained::decode(find(&current_objects, seal.input_digest)?)?;
    let tail = retained.tail_from(&a)?;
    let binding = Binding {
        operation_id: operation.operation_id,
        revision: operation.operation_revision,
        logical_strategy: operation.logical_strategy_id,
        members_digest: operation.members_digest().map_err(|_| Error::Invalid)?,
    };
    let workspace = PrivateDirectory::create_in(directory, "source-replay")?;
    let anchor = object(workspace.path(), &a.bytes()?)?;
    let tail = object(workspace.path(), &tail)?;
    let checkpoint = object(workspace.path(), &c.bytes()?)?;
    let request = ControlInput::Capture {
        source: a.source.clone(),
        anchor: anchor.object,
        expected: a.cut.clone(),
        pause: true,
    };
    let request = object(workspace.path(), &request.bytes()?)?;
    let capture = CapturedTail {
        schema: 1,
        request: request.object,
        source: a.source.clone(),
        anchor: anchor.object,
        tail: tail.object,
        checkpoint: checkpoint.object,
        cut: c.cut.clone(),
        paused: true,
    };
    let capture = object(workspace.path(), &capture.bytes()?)?;
    let job = SourceJob {
        schema: 1,
        binding,
        source: a.source.clone(),
        request,
        capture,
        anchor,
        tail,
        checkpoint,
        output_directory: workspace.path().join("output"),
    };
    let job_bytes = job.bytes()?;
    let job_file = object(workspace.path(), &job_bytes)?;
    // This identity is only for this separately owned replay subprocess. The
    // live old process identity appears in Source; no old FD/grant is inherited.
    let mut identity = spec.identity.clone();
    let mut unique = workspace.path().as_os_str().as_encoded_bytes().to_vec();
    unique.extend(&job_bytes);
    identity.instance = tickwire_recovery::digest(&unique)[..16]
        .try_into()
        .map_err(|_| Error::Invalid)?;
    identity.generation = 1;
    let mut registry = Registry::new(1)?;
    let process = registry.spawn(LaunchSpec {
        identity: identity.clone(),
        executable: spec.executable.clone(),
        arguments: vec![
            "--source-replay".into(),
            job_file.path.to_str().ok_or(Error::Invalid)?.into(),
        ],
        working_directory: workspace.path().to_owned(),
        environment: Default::default(),
        uid: spec.uid,
        gid: spec.gid,
        inherit_output: true,
    })?;
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(exit) = registry.observe_exits()?.into_iter().next() {
            if exit.instance() != identity.instance
                || exit.pid() != process.pid
                || !exit.status().success()
            {
                return Err(Error::Invalid);
            }
            break;
        }
        if Instant::now() >= deadline {
            return Err(Error::NotReady);
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    let receipt_path = job.output_directory.join("SOURCE_REPLAY.json");
    let metadata = std::fs::symlink_metadata(&receipt_path)?;
    if !metadata.is_file() || metadata.len() > 64 * 1024 {
        return Err(Error::Invalid);
    }
    let bytes = std::fs::read(&receipt_path)?;
    if bytes.len() as u64 != metadata.len() {
        return Err(Error::Invalid);
    }
    let receipt = SourceReceipt::decode(&bytes)?;
    if receipt.pid != process.pid
        || receipt.job != job_file.object
        || receipt.binding != job.binding
        || receipt.source != a.source
        || receipt.capture != job.capture.object
        || receipt.output != job.checkpoint.object
        || receipt.cut != c.cut
    {
        return Err(Error::Invalid);
    }
    let output = preflight::object_file(&job.output_directory, receipt.output).read()?;
    if output != c.bytes()? {
        return Err(Error::Invalid);
    }
    let completion = Completion {
        schema: 1,
        operation: operation.clone(),
        binding: job.binding.clone(),
        member: member.clone(),
        fixed_proof: Object::of(find(&fixed_objects, fixed.input_seal_digest)?),
        current_proof: Object::of(find(&current_objects, current.input_seal_digest)?),
        anchor: job.anchor.object,
        tail: job.tail.object,
        checkpoint: job.checkpoint.object,
        receipt: Object::of(&bytes),
    };
    let mut objects = fixed_objects;
    objects.extend(current_objects);
    objects.extend([
        job.anchor.read()?,
        job.tail.read()?,
        job.checkpoint.read()?,
        job.request.read()?,
        job.capture.read()?,
        job_bytes,
        bytes,
        output,
        completion.bytes()?,
    ]);
    let proof =
        VerifiedSourceReplay::verify(operation, member, Object::of(&completion.bytes()?), objects)?;
    for bytes in proof.objects() {
        preflight::persist_object(directory, bytes)?;
    }
    Ok(proof)
}
