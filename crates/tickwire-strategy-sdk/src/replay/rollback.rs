//! An isolated, registered rollback executable consumes the installed C output.
//! No live market, account, intent, grant, or inherited strategy FD is attached.
use super::*;
use crate::{
    bootstrap::Bootstrap,
    restore::{self, RestoreFactory},
};
use std::os::unix::fs::{DirBuilderExt, MetadataExt};
use tickwire_release::{
    ReleaseBundle, Role, files,
    replay::rollback::{Job, Receipt},
};

fn qualify(
    job: &Job,
) -> crate::Result<(
    tickwire_release::replay::install::Verified,
    files::LoadedFiles,
    Bootstrap,
    ReleaseBundle,
)> {
    let installed = job.load()?;
    let operation = &installed.request.operation;
    let get = |hash| {
        installed
            .objects
            .iter()
            .find(|b| Object::of(b).digest == hash)
            .map(Vec::as_slice)
            .ok_or(crate::Error::Identity)
    };
    let bundle = ReleaseBundle::decode(
        get(operation.artifacts_digest)?,
        get(operation.checkpoint_digest)?,
        get(operation.rollback_digest)?,
    )?;
    let release = bundle.verify_operation(operation, &installed.request.member)?;
    let files = files::load(&release.rollback.files, &job.rollback_files)?;
    let bootstrap: Bootstrap = serde_json::from_slice(
        files
            .bytes(&Role::Bootstrap)
            .ok_or(crate::Error::Identity)?,
    )?;
    let id = bootstrap.report.local.identity();
    if id != bootstrap.market.local.identity()
        || id.logical_strategy != operation.logical_strategy_id
        || [
            installed.request.member.new_instance,
            installed.request.member.old_instance,
        ]
        .contains(&id.instance)
        || id.build_hash
            != files
                .object(&Role::Binary)
                .ok_or(crate::Error::Identity)?
                .digest
    {
        return Err(crate::Error::Identity);
    }
    live::verify_executable(files.object(&Role::Binary).ok_or(crate::Error::Identity)?)?;
    crate::preflight::verify_recovery_inputs(bootstrap.recovery_template.as_deref(), &files)?;
    Ok((installed, files, bootstrap, bundle))
}
#[allow(unsafe_code)]
fn execute<F: RestoreFactory>(
    job: &Job,
    factory: &F,
    installed: tickwire_release::replay::install::Verified,
    files: files::LoadedFiles,
    bootstrap: Bootstrap,
    bundle: ReleaseBundle,
) -> crate::Result<Receipt> {
    let release =
        bundle.verify_operation(&installed.request.operation, &installed.request.member)?;
    let target = crate::preflight::target(&bootstrap, &files)?;
    let input = restore::VerifiedRestoreInput::for_installed_probe(&release, target, &installed)?;
    files.recheck()?;
    let restored = restore::execute(factory, &input)?;
    files.recheck()?;
    let receipt = Receipt {
        schema: 1,
        job: Object::of(&job.bytes()?),
        installation: job.installation,
        operation: installed.request.operation.clone(),
        member: installed.request.member.clone(),
        process: tickwire_release::receipt::Process {
            instance: bootstrap.market.local.instance,
            generation: bootstrap.market.local.generation,
            pid: std::process::id(),
            uid: unsafe { libc::geteuid() },
            gid: unsafe { libc::getegid() },
        },
        binary_digest: files
            .object(&Role::Binary)
            .ok_or(crate::Error::Identity)?
            .digest,
        bootstrap_digest: files
            .object(&Role::Bootstrap)
            .ok_or(crate::Error::Identity)?
            .digest,
        input_checkpoint: installed.installation.checkpoint,
        input_state: Object::of(input.state()),
        source_schema: installed.request.target.state_schema,
        method: release.rollback.method.clone(),
        output_schema: release.rollback.capabilities.state_schema,
        output: Object::of(restored.checkpoint()),
    };
    receipt.verify(&installed)?;
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
    if !meta.is_dir() || meta.mode() & 0o077 != 0 || meta.uid() != receipt.process.uid {
        return Err(crate::Error::Identity);
    }
    std::fs::File::open(
        job.output_directory
            .parent()
            .ok_or(crate::Error::Identity)?,
    )?
    .sync_all()?;
    for bytes in [
        input.state(),
        restored.checkpoint(),
        &job.bytes()?,
        &receipt.bytes()?,
    ] {
        crate::recovery::object(&job.output_directory, bytes)?;
    }
    crate::recovery::immutable(
        &job.output_directory.join("ROLLBACK_REPLAY.json"),
        &receipt.bytes()?,
    )?;
    Ok(receipt)
}
/// A custom strategy executable supplies its registered implementation here.
pub fn run<F: RestoreFactory>(job: &Job, factory: &F) -> crate::Result<Receipt> {
    let (installed, files, bootstrap, bundle) = qualify(job)?;
    execute(job, factory, installed, files, bootstrap, bundle)
}
pub fn run_with_factory<F: RestoreFactory>(
    job: &Job,
    load: impl FnOnce(&Bootstrap, &files::LoadedFiles) -> crate::Result<F>,
) -> crate::Result<Receipt> {
    let (installed, files, bootstrap, bundle) = qualify(job)?;
    let factory = load(&bootstrap, &files)?;
    execute(job, &factory, installed, files, bootstrap, bundle)
}
pub fn run_native(job: &Job) -> crate::Result<Receipt> {
    let (installed, files, bootstrap, bundle) = qualify(job)?;
    if bootstrap.factory.is_some() {
        return Err(crate::Error::NotReady);
    }
    if bootstrap.demo_enabled {
        execute(
            job,
            &restore::FixedDemoFactory,
            installed,
            files,
            bootstrap,
            bundle,
        )
    } else {
        execute(
            job,
            &restore::ObserveFactory,
            installed,
            files,
            bootstrap,
            bundle,
        )
    }
}
