//! Isolated old-binary replay entry. No control socket, market FD, or intent writer
//! is opened here; Host must still match the real child PID and successful exit.
use super::*;
pub use tickwire_release::replay::source::{SourceJob, SourceReceipt};
/// The supplied factory is compiled into this executable. A launcher selects the
/// old executable from the fixed release artifact, then checks this actual PID.
pub fn run_source<F: SourceReplayFactory>(
    job: &SourceJob,
    factory: &F,
) -> crate::Result<SourceReceipt> {
    if job.schema != 1
        || job.binding.operation_id == [0; 16]
        || job.binding.revision == 0
        || job.binding.members_digest == [0; 32]
        || job.binding.logical_strategy != job.source.logical_strategy
        || !job.output_directory.is_absolute()
        || live::executable_digest()? != job.source.binary_digest
    {
        return Err(crate::Error::Identity);
    }
    let request_bytes = job.request.read()?;
    let request = ControlInput::decode(&request_bytes)?;
    let capture = CapturedTail::decode(&job.capture.read()?)?;
    let ControlInput::Capture {
        source,
        anchor,
        expected,
        pause,
    } = request
    else {
        return Err(crate::Error::Identity);
    };
    if !pause
        || !capture.paused
        || source != job.source
        || capture.source != source
        || capture.request != job.request.object
        || capture.anchor != anchor
        || capture.anchor != job.anchor.object
        || capture.tail != job.tail.object
        || capture.checkpoint != job.checkpoint.object
    {
        return Err(crate::Error::Identity);
    }
    let a = Checkpoint::decode(&job.anchor.read()?)?;
    if a.cut != expected {
        return Err(crate::Error::Identity);
    }
    let models = a
        .runtime
        .models
        .iter()
        .map(|m| (m.id, Object::of(&m.value.bytes)))
        .collect::<Vec<_>>();
    let anchor = verify_anchor(
        &a.bytes()?,
        &source,
        Object::of(&a.runtime.config.bytes),
        &models,
    )?;
    let expected = Checkpoint::decode(&job.checkpoint.read()?)?;
    if expected.source != source || expected.cut != capture.cut {
        return Err(crate::Error::Identity);
    }
    let tail = verify_tail(&anchor, &job.tail.read()?, &capture.cut)?;
    let state = replay_source(factory, &anchor, &tail)?;
    if state.checkpoint() != &expected {
        return Err(crate::Error::Identity);
    }
    // No output file is created until actual callbacks and final equality pass.
    use std::os::unix::fs::{DirBuilderExt, MetadataExt};
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(&job.output_directory)
        .or_else(|e| {
            if e.kind() == std::io::ErrorKind::AlreadyExists {
                Ok(())
            } else {
                Err(e)
            }
        })?;
    let meta = std::fs::symlink_metadata(&job.output_directory)?;
    if !meta.is_dir() || meta.mode() & 0o027 != 0 {
        return Err(crate::Error::Identity);
    }
    std::fs::File::open(
        job.output_directory
            .parent()
            .ok_or(crate::Error::Identity)?,
    )?
    .sync_all()?;
    let bytes = state.checkpoint().bytes()?;
    let (_, digest) = crate::recovery::object(&job.output_directory, &bytes)?;
    let output = Object {
        digest,
        bytes: bytes.len() as u64,
    };
    let receipt = SourceReceipt {
        schema: 1,
        job: Object::of(&job.bytes()?),
        binding: job.binding.clone(),
        pid: std::process::id(),
        source: job.source.clone(),
        capture: job.capture.object,
        output,
        cut: capture.cut,
    };
    let bytes = receipt.bytes()?;
    crate::recovery::object(&job.output_directory, &bytes)?;
    let path = job.output_directory.join("SOURCE_REPLAY.json");
    // Same result is retryable only with identical bytes; never replace a previous process proof.
    crate::recovery::immutable(&path, &bytes)?;
    Ok(receipt)
}
pub fn run_native(job: &SourceJob) -> crate::Result<SourceReceipt> {
    run_source(
        job,
        &application::ApplicationFactory {
            binary_digest: job.source.binary_digest,
            business: native::Factory {
                schema: job.source.state_schema,
            },
        },
    )
}
