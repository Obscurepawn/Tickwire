//! Release files are derived from the old child's authenticated, durable cut.
//! Callback completion is produced only by the launched worker and rollback probe.
use crate::{Result, Scenario};
use serde_json::Value;
use std::{
    fs,
    path::{Path, PathBuf},
};
use tickwire_lifecycle::Operation;
use tickwire_recovery::{EncodedInputs, HandoverSeal, digest};
use tickwire_release::{
    Artifact, Binding, InputPrefix, Method, Migration, MigrationSet, Object, ProbeInput,
    ReleaseBundle, ReleaseManifest, Role, Rollback, RollbackPlan, Target, files::Location,
};
use tickwire_strategy_sdk::{
    bootstrap::Bootstrap,
    preflight::{BundleFiles, Job, ObjectFile, SourceFiles},
};

const PROBE_INSTANCE: [u8; 16] = [7; 16];
const PROBE_GENERATION: u64 = 7;
fn hex(b: &[u8]) -> String {
    b.iter().map(|v| format!("{v:02x}")).collect()
}
fn read(path: &Path, max: usize) -> Result<Vec<u8>> {
    if fs::metadata(path)?.len() > max as u64 {
        return Err("preflight input capacity".into());
    }
    Ok(fs::read(path)?)
}
fn object(path: PathBuf, max: usize) -> Result<ObjectFile> {
    let bytes = read(&path, max)?;
    Ok(ObjectFile {
        path,
        object: Object::of(&bytes),
    })
}
fn saved(path: &Path, bytes: &[u8]) -> Result<ObjectFile> {
    crate::generate::write(path, bytes)?;
    Ok(ObjectFile {
        path: path.to_path_buf(),
        object: Object::of(bytes),
    })
}
fn replace_canonical(path: &Path, bytes: &[u8]) -> Result<()> {
    let tmp = path.with_extension("preflight-pending");
    crate::generate::write(&tmp, bytes)?;
    fs::rename(tmp, path)?;
    fs::File::open(path.parent().ok_or("preflight parent")?)?.sync_all()?;
    Ok(())
}
fn source_file(directory: &Path, hash: [u8; 32], expected: Option<u64>) -> Result<ObjectFile> {
    let value = object(
        directory.join(format!("{}.bin", hex(&hash))),
        tickwire_recovery::MAX_OBJECT_BYTES,
    )?;
    if value.object.digest != hash || expected.is_some_and(|n| n != value.object.bytes) {
        return Err("source recovery object binding".into());
    }
    Ok(value)
}
fn actual_source(s: &Scenario, ready: &Value) -> Result<(SourceFiles, HandoverSeal)> {
    let bootstrap: Bootstrap =
        serde_json::from_slice(&read(&s.worker_bootstrap, 2 * 1024 * 1024)?)?;
    let template: tickwire_strategy_sdk::recovery::Template = serde_json::from_slice(&read(
        bootstrap
            .recovery_template
            .as_deref()
            .ok_or("source recovery template")?,
        2 * 1024 * 1024,
    )?)?;
    let directory = &template.output_directory;
    let hash = serde_json::from_value(ready["input_seal_digest"].clone())?;
    let proof = source_file(directory, hash, None)?;
    let seal = HandoverSeal::decode(&proof.read()?)?;
    if seal.instance != [2; 16]
        || seal.generation != 5
        || seal.logical_strategy != 8
        || ready["instance"] != serde_json::json!(seal.instance)
        || ready["generation"] != seal.generation
        || ready["checkpoint_digest"] != serde_json::json!(seal.checkpoint_digest)
        || ready["account_position"] != seal.report_position
        || ready["ledger_sequence"] != seal.ledger_sequence
        || ready["orders_digest"] != serde_json::json!(seal.orders_digest)
        || ready["account_input_digest"] != serde_json::json!(seal.account_input_digest)
        || seal.preflight_receipt_digest.is_some()
    {
        return Err("source CONSUMED differs from sealed callback history".into());
    }
    let source = SourceFiles {
        proof,
        checkpoint: source_file(
            directory,
            seal.checkpoint_digest,
            Some(seal.checkpoint_bytes),
        )?,
        inputs: source_file(directory, seal.input_digest, Some(seal.input_bytes))?,
        account_inputs: source_file(
            directory,
            seal.account_input_digest,
            Some(seal.account_input_bytes),
        )?,
        dependencies: seal
            .dependencies
            .iter()
            .map(|(_, hash)| source_file(directory, *hash, None))
            .collect::<Result<_>>()?,
        prior_preflight: vec![],
    };
    source.objects()?;
    let checkpoint = source.checkpoint.read()?;
    crate::flavor::source_state(s.kind, &checkpoint)?;
    Ok((source, seal))
}
#[cfg(test)]
fn fixed_source_state(checkpoint: &[u8]) -> Result<()> {
    if checkpoint.get(96..104) != Some(b"TWFD0001") || checkpoint.get(104) != Some(&1) {
        return Err("laboratory migration requires actual FixedDemo v1 state".into());
    }
    Ok(())
}

pub(crate) fn artifacts(
    host: &tickwire_app::hosted::HostConfig,
    host_path: PathBuf,
    bootstrap_path: PathBuf,
    command_path: PathBuf,
    recovery_template: Option<&Path>,
) -> Result<(Vec<Artifact>, Vec<Location>)> {
    let mut refs = vec![];
    let mut files = vec![];
    for (role, path) in [
        (Role::Binary, host.executable.clone()),
        (Role::HostConfig, host_path),
        (Role::Bootstrap, bootstrap_path),
        (Role::StrategyConfig, command_path),
    ] {
        let value = object(
            path.clone(),
            if role == Role::Binary {
                tickwire_release::MAX_BINARY as usize
            } else {
                tickwire_recovery::MAX_OBJECT_BYTES
            },
        )?;
        refs.push(Artifact {
            role: role.clone(),
            object: value.object,
        });
        files.push(Location { role, path });
    }
    if let Some(path) = recovery_template {
        let bytes = read(path, tickwire_recovery::MAX_OBJECT_BYTES)?;
        let template: tickwire_strategy_sdk::recovery::Template = serde_json::from_slice(&bytes)?;
        refs.push(Artifact {
            role: Role::RecoveryTemplate,
            object: Object::of(&bytes),
        });
        files.push(Location {
            role: Role::RecoveryTemplate,
            path: path.to_path_buf(),
        });
        for dependency in &template.dependencies {
            let value = object(dependency.path.clone(), tickwire_recovery::MAX_OBJECT_BYTES)?;
            if value.object.digest != dependency.digest {
                return Err("target recovery dependency hash".into());
            }
            if let Some((_, existing)) = refs
                .iter()
                .zip(&files)
                .find(|(a, _)| a.object == value.object)
            {
                if existing.path != dependency.path {
                    return Err("one recovery object uses different committed paths".into());
                }
                continue;
            }
            let role = Role::Dependency(u16::from(dependency.kind));
            refs.push(Artifact {
                role: role.clone(),
                object: value.object,
            });
            files.push(Location {
                role,
                path: dependency.path.clone(),
            });
        }
    }
    tickwire_release::files::load(&refs, &files)?.recheck()?;
    Ok((refs, files))
}
fn documents(
    op: &mut Operation,
    source: &SourceFiles,
    inputs: &EncodedInputs,
    target: Vec<Artifact>,
    rollback: Vec<Artifact>,
    capabilities: tickwire_release::Capabilities,
    schemas: (u64, u64, u64),
) -> Result<[Vec<u8>; 3]> {
    let member = op.members.first().ok_or("preflight member")?.clone();
    let binding = Binding {
        operation_id: op.operation_id,
        revision: op.operation_revision,
        logical_strategy: op.logical_strategy_id,
        members_digest: op.members_digest()?,
    };
    let (source_schema, target_schema, method_id) = schemas;
    let version =
        |files: &Vec<Artifact>| -> Result<[u8; 32]> { Ok(digest(&serde_json::to_vec(files)?)) };
    let manifest = ReleaseManifest {
        schema: 1,
        binding: binding.clone(),
        targets: vec![Target {
            member: member.clone(),
            version: version(&target)?,
            files: target,
            capabilities: capabilities.clone(),
        }],
    };
    let b = &inputs.boundary;
    let migration = MigrationSet {
        schema: 1,
        binding: binding.clone(),
        members: vec![Migration {
            member: member.clone(),
            source_checkpoint: source.checkpoint.object,
            source_schema,
            source_proof: source.proof.object,
            inputs: Some(InputPrefix {
                object: source.inputs.object,
                stream: b.stream,
                first_sequence: b.first_sequence,
                previous_hash: b.previous_hash,
                last_sequence: b.last_sequence,
                last_hash: b.last_hash,
            }),
            method: Method::Migrate {
                source_schema,
                target_schema,
                method_id,
            },
        }],
    };
    let rollback = RollbackPlan {
        schema: 1,
        binding,
        members: vec![Rollback {
            member: member.clone(),
            version: version(&rollback)?,
            files: rollback,
            capabilities,
            method: Method::Restore {
                schema: target_schema,
            },
            probe_input: ProbeInput::TargetRestoreOutput,
        }],
    };
    let bytes = [
        tickwire_release::encode(&manifest)?,
        tickwire_release::encode(&migration)?,
        tickwire_release::encode(&rollback)?,
    ];
    let bundle = ReleaseBundle::decode(&bytes[0], &bytes[1], &bytes[2])?;
    [
        op.artifacts_digest,
        op.checkpoint_digest,
        op.rollback_digest,
    ] = bundle.roots();
    bundle.verify_operation(op, &member)?;
    Ok(bytes)
}

struct CommittedTarget {
    bootstrap: Bootstrap,
    host: tickwire_app::hosted::HostConfig,
    refs: Vec<Artifact>,
    files: Vec<Location>,
}
fn commit_target(s: &Scenario) -> Result<CommittedTarget> {
    let r = &s.root;
    let target_bootstrap_path = r.join("next/bootstrap.json");
    let target_host_path = r.join("next/host.json");
    let bootstrap: Bootstrap =
        serde_json::from_slice(&read(&target_bootstrap_path, 2 * 1024 * 1024)?)?;
    if bootstrap.preflight_job.as_deref() != Some(r.join("next/preflight.json").as_path())
        || !bootstrap.handover_enabled
    {
        return Err("target preflight bootstrap".into());
    }
    let host: tickwire_app::hosted::HostConfig =
        serde_json::from_slice(&read(&target_host_path, 2 * 1024 * 1024)?)?;
    replace_canonical(&target_bootstrap_path, &serde_json::to_vec(&bootstrap)?)?;
    replace_canonical(&target_host_path, &serde_json::to_vec(&host)?)?;
    let template_path = bootstrap
        .recovery_template
        .as_deref()
        .ok_or("target recovery template")?;
    let mut template: tickwire_strategy_sdk::recovery::Template =
        serde_json::from_slice(&read(template_path, 2 * 1024 * 1024)?)?;
    for dependency in &mut template.dependencies {
        let path = match dependency.kind {
            8 => Some(host.executable.clone()),
            9 => Some(target_host_path.clone()),
            11 => Some(crate::flavor::config_path(s.kind, &r.join("next"))),
            _ => None,
        };
        if let Some(path) = path {
            dependency.digest = digest(&read(&path, tickwire_recovery::MAX_OBJECT_BYTES)?);
            dependency.path = path;
        }
    }
    replace_canonical(template_path, &serde_json::to_vec(&template)?)?;
    let (target, target_files) = artifacts(
        &host,
        target_host_path.clone(),
        target_bootstrap_path.clone(),
        crate::flavor::config_path(s.kind, &r.join("next")),
        Some(template_path),
    )?;

    Ok(CommittedTarget {
        bootstrap,
        host,
        refs: target,
        files: target_files,
    })
}

pub(crate) fn prepare(s: &Scenario, ready: &Value) -> Result<Operation> {
    let r = &s.root;
    let (source, _) = actual_source(s, ready)?;
    let app: tickwire_app::config::Deployment =
        serde_json::from_slice(&read(&s.app_config, 2 * 1024 * 1024)?)?;
    let member = crate::handover::member(&app, s.valid_until_ns);
    let mut operation = Operation {
        schema: 3,
        operation_id: [140; 16],
        operation_revision: 1,
        logical_strategy_id: 8,
        members: vec![member.clone()],
        artifacts_digest: [1; 32],
        checkpoint_digest: [2; 32],
        rollback_digest: [3; 32],
        clock_boot_id: s.boot,
        clock_generation: 1,
        valid_until_ns: s.valid_until_ns,
    };
    let committed = commit_target(s)?;
    let bootstrap = committed.bootstrap;
    let host = committed.host;
    let target = committed.refs;
    let target_files = committed.files;

    let mut rollback = bootstrap.clone();
    for endpoint in [
        &mut rollback.market,
        &mut rollback.intent,
        &mut rollback.report,
    ] {
        endpoint.local.instance = PROBE_INSTANCE;
        endpoint.local.generation = PROBE_GENERATION;
        endpoint.control_epoch = 3;
        endpoint.nonce = [145; 32];
    }
    if let Some(external) = &mut rollback.external_market {
        external.endpoint.local = rollback.market.local.clone();
    }
    rollback.command_directory = r.join("rollback/commands");
    rollback.report_directory = r.join("rollback/reports");
    rollback.demo_command_file = Some(r.join("rollback/command.bin"));
    rollback.recovery_template = Some(r.join("rollback/recovery-template.json"));
    let mut command =
        tickwire_venue::CanonicalCommand::decode(&read(&r.join("next/command.bin"), 65536)?)
            .map_err(|e| format!("preflight command: {e:?}"))?
            .command()
            .clone();
    command.context.strategy.instance_id = PROBE_INSTANCE;
    command.context.request.strategy_generation = PROBE_GENERATION;
    command.context.request.intent_id = 5;
    let command = tickwire_venue::CanonicalCommand::new(command)
        .map_err(|e| format!("probe command: {e:?}"))?;
    saved(&r.join("rollback/command.bin"), command.bytes())?;
    crate::flavor::configure(
        s.kind,
        &mut rollback,
        &r.join("rollback"),
        &command,
        6_000_100,
    )?;
    saved(
        &r.join("rollback/bootstrap.json"),
        &serde_json::to_vec(&rollback)?,
    )?;
    let mut rollback_host: tickwire_app::hosted::HostConfig =
        serde_json::from_slice(&serde_json::to_vec(&host)?)?;
    rollback_host.bootstrap = r.join("rollback/bootstrap.json");
    rollback_host.directory_parent = r.join("rollback/runtime");
    rollback_host.control_directory = r.join("rollback/control");
    rollback_host.recovery_directory = r.join("rollback/recovery");
    saved(
        &r.join("rollback/host.json"),
        &serde_json::to_vec(&rollback_host)?,
    )?;
    let mut probe_template: tickwire_strategy_sdk::recovery::Template =
        serde_json::from_slice(&read(
            bootstrap
                .recovery_template
                .as_deref()
                .ok_or("target recovery template")?,
            2 * 1024 * 1024,
        )?)?;
    probe_template.owner = PROBE_INSTANCE;
    probe_template.input_stream = [146; 16];
    probe_template.pin_id = [147; 16];
    probe_template.output_directory = r.join("rollback/recovery");
    probe_template.order_ids = vec![[9; 16]];
    for dependency in &mut probe_template.dependencies {
        dependency.owner = PROBE_INSTANCE;
        let path = match dependency.kind {
            8 => Some(rollback_host.executable.clone()),
            9 => Some(r.join("rollback/host.json")),
            11 => Some(crate::flavor::config_path(s.kind, &r.join("rollback"))),
            _ => None,
        };
        if let Some(path) = path {
            dependency.digest = digest(&read(&path, tickwire_recovery::MAX_OBJECT_BYTES)?);
            dependency.path = path;
        }
    }
    let probe_template_path = r.join("rollback/recovery-template.json");
    saved(&probe_template_path, &serde_json::to_vec(&probe_template)?)?;
    let (rollback, rollback_files) = artifacts(
        &rollback_host,
        r.join("rollback/host.json"),
        r.join("rollback/bootstrap.json"),
        crate::flavor::config_path(s.kind, &r.join("rollback")),
        Some(&probe_template_path),
    )?;
    let input = EncodedInputs::decode(source.inputs.read()?)?;
    let docs = documents(
        &mut operation,
        &source,
        &input,
        target,
        rollback,
        crate::flavor::capabilities(s.kind, &bootstrap)?,
        crate::flavor::schemas(s.kind),
    )?;
    let bundle = BundleFiles {
        manifest: saved(&r.join("handover/release-manifest.json"), &docs[0])?,
        migration: saved(&r.join("handover/migration-set.json"), &docs[1])?,
        rollback: saved(&r.join("handover/rollback-plan.json"), &docs[2])?,
    };
    let job = Job {
        schema: 1,
        operation: operation.clone(),
        member,
        bundle,
        target_files,
        rollback_files,
        source,
        output_directory: r.join("next/preflight-output"),
        effective_bootstrap: Object::of(&serde_json::to_vec(&bootstrap)?),
    };
    // This is a job, never a completion receipt. Host replaces only the effective
    // bootstrap commitment after deriving its actual socket and process fields.
    saved(&r.join("next/preflight.json"), &serde_json::to_vec(&job)?)?;
    saved(
        &r.join("handover/source-consumed.json"),
        &serde_json::to_vec(ready)?,
    )?;
    Ok(operation)
}

fn ready_objects(s: &Scenario, ready: &Value) -> Result<(PathBuf, HandoverSeal, Vec<u8>, Vec<u8>)> {
    let directory = s.root.join("next/recovery");
    let hash = serde_json::from_value(ready["input_seal_digest"].clone())?;
    let seal = HandoverSeal::decode(&source_file(&directory, hash, None)?.read()?)?;
    if seal.version != 3
        || seal.instance != crate::handover::NEW_INSTANCE
        || seal.generation != 6
        || seal.logical_strategy != 8
        || ready["instance"] != serde_json::json!(seal.instance)
        || ready["generation"] != seal.generation
        || ready["account_position"] != seal.report_position
        || ready["checkpoint_digest"] != serde_json::json!(seal.checkpoint_digest)
        || ready["orders_digest"] != serde_json::json!(seal.orders_digest)
        || ready["ledger_sequence"] != seal.ledger_sequence
    {
        return Err("schema3 READY seal binding".into());
    }
    let checkpoint = source_file(
        &directory,
        seal.checkpoint_digest,
        Some(seal.checkpoint_bytes),
    )?
    .read()?;
    let inputs = source_file(&directory, seal.input_digest, Some(seal.input_bytes))?.read()?;
    tickwire_release::replay::startup::verify_seal(&seal, &checkpoint, &inputs, |h| {
        fs::read(directory.join(format!("{}.bin", hex(&h))))
            .map_err(|_| tickwire_release::Error::Missing)
    })?;
    Ok((directory, seal, checkpoint, inputs))
}
#[cfg(test)]
fn action_state(bytes: &[u8], consumed: bool) -> Result<()> {
    // V2 stores consumed, unlike the V1 enabled-and-pending bit at S.
    if bytes.len() != 41 || &bytes[..8] != b"TWFD0002" || bytes[8] != u8::from(consumed) {
        return Err("actual restored action state differs from expected cut".into());
    }
    Ok(())
}
/// Startup proves only the target's restoration of unconsumed S.
pub(crate) fn verify_restored(s: &Scenario, ready: &Value, operation: &Operation) -> Result<Value> {
    let (directory, seal, _, _) = ready_objects(s, ready)?;
    if seal.replay_installation_digest.is_some() || seal.replay_rollback_digest.is_some() {
        return Err("startup unexpectedly claims the later C stages".into());
    }
    let root = seal.replay_restore_digest.ok_or("startup completion")?;
    let verified = tickwire_release::replay::startup::verify(root, |h| {
        fs::read(directory.join(format!("{}.bin", hex(&h))))
            .map_err(|_| tickwire_release::Error::Missing)
    })?;
    let a = &verified.restored;
    let (source_schema, target_schema, method_id) = crate::flavor::schemas(s.kind);
    if operation.schema != 3
        || verified.completion.operation != *operation
        || verified.completion.member != operation.members[0]
        || ready["worker_pid"] != a.process.pid
        || a.process.instance != crate::handover::NEW_INSTANCE
        || a.process.generation != 6
        || a.output_schema != target_schema
        || a.method
            != (Method::Migrate {
                source_schema,
                target_schema,
                method_id,
            })
    {
        return Err("actual startup callback identity or method".into());
    }
    let output = verified
        .objects
        .iter()
        .find(|v| Object::of(v) == a.output)
        .ok_or("startup output")?;
    crate::flavor::target_state(s.kind, output, false)?;
    Ok(
        serde_json::json!({"operation_schema":3,"restore_digest":root,"source_report_position":a.source_report_position,"target_pid":a.process.pid,"action_pending_at_s":true}),
    )
}
/// Final READY binds actual source replay, installation at C and the later probe.
pub(crate) fn verify_completed(
    s: &Scenario,
    ready: &Value,
    operation: &Operation,
    prepared: &tickwire_lifecycle::ReplayPrepared,
    phase: crate::flavor::ProgressPhase,
) -> Result<Value> {
    let (directory, seal, checkpoint, inputs) = ready_objects(s, ready)?;
    let retained = tickwire_release::replay::retained::decode(&inputs)?;
    let final_state = checkpoint.get(96..).ok_or("final checkpoint state")?;
    crate::flavor::target_state(s.kind, final_state, true)?;
    let (_, target_schema, _) = crate::flavor::schemas(s.kind);
    let root = seal
        .replay_rollback_digest
        .ok_or("final READY has no C rollback completion")?;
    let verified = tickwire_release::replay::rollback::verify(root, |h| {
        fs::read(directory.join(format!("{}.bin", hex(&h))))
            .map_err(|_| tickwire_release::Error::Missing)
    })?;
    let installed = &verified.installation;
    let source = installed.request.source_proof(|o| {
        installed
            .objects
            .iter()
            .find(|b| Object::of(b) == o)
            .cloned()
            .ok_or(tickwire_release::Error::Missing)
    })?;
    let fixed = source.fixed();
    let current = source.current();
    let cut = |c: &tickwire_release::replay::Checkpoint| -> Result<tickwire_lifecycle::ReplayCut> {
        let o = Object::of(&c.bytes()?);
        Ok(tickwire_lifecycle::ReplayCut {
            checkpoint: tickwire_lifecycle::ReplayObject {
                digest: o.digest,
                bytes: o.bytes,
            },
            sequence: c.cut.sequence,
            chain: c.cut.chain,
        })
    };
    let b = &verified.receipt;
    if operation.schema != 3
        || installed.request.operation != *operation
        || installed.request.member != operation.members[0]
        || prepared.operation != *operation
        || prepared.fixed != cut(fixed)?
        || prepared.current != cut(current)?
        || fixed.cut.sequence >= current.cut.sequence
        || prepared.source_completion.digest != source.root().digest
        || prepared.target_install.digest != Object::of(&installed.continued.bytes()?).digest
        || ready["worker_pid"]
            .as_u64()
            .is_none_or(|p| p == u64::from(b.process.pid))
        || b.process.instance != PROBE_INSTANCE
        || b.process.generation != PROBE_GENERATION
        || b.output_schema != target_schema
        || b.method
            != (Method::Restore {
                schema: target_schema,
            })
    {
        return Err("actual C replay/installation/probe binding".into());
    }
    let expansion_prefix = if s.kind == crate::ScenarioKind::SpotMarketExpansion {
        Some(crate::expansion::proof::verify_completed(s, &source)?)
    } else {
        None
    };
    let old_state = |c: &tickwire_release::replay::Checkpoint| {
        tickwire_strategy_sdk::replay::application::business_bytes(&c.business)
    };
    let a = old_state(fixed)?;
    let c = old_state(current)?;
    let installed_state = installed.installed_business()?;
    let custom_counters = if crate::flavor::custom(s.kind) {
        let old = crate::flavor::CountingState::decode(&a, tickwire_strategy_example::V1, false)?;
        let at_c = crate::flavor::CountingState::decode(&c, tickwire_strategy_example::V1, true)?;
        let at_install =
            crate::flavor::CountingState::decode(&installed_state, target_schema, true)?;
        let final_state = crate::flavor::CountingState::decode(final_state, target_schema, true)?;
        if old.action != at_c.action
            || at_c.action != at_install.action
            || at_install.action != final_state.action
            || at_c.observations <= old.observations
            || at_c.reports <= old.reports
            || at_install.observations != at_c.observations
            || at_install.reports != at_c.reports
        {
            return Err("counting strategy lost S/C/D counters or action identity".into());
        }
        crate::flavor::counting_progress(&at_install, &final_state, phase)?;
        Some(
            serde_json::json!({"source_s":old,"source_c":at_c,"target_at_c":at_install,"target_d":final_state}),
        )
    } else {
        if a.get(..8) != Some(b"TWFD0001")
            || a.get(8) != Some(&1)
            || c.get(..8) != Some(b"TWFD0001")
            || c.get(8) != Some(&0)
        {
            return Err("old binary did not replay the actual pending-to-consumed action".into());
        }
        None
    };
    crate::flavor::target_state(s.kind, &installed_state, true)?;
    let output = verified
        .objects
        .iter()
        .find(|v| Object::of(v) == b.output)
        .ok_or("rollback output")?;
    crate::flavor::target_state(s.kind, output, true)?;
    if crate::flavor::custom(s.kind) && output != &installed_state {
        return Err("custom rollback did not preserve complete C state".into());
    }
    let startup = tickwire_release::replay::startup::verify(
        seal.replay_restore_digest.ok_or("restore root")?,
        |h| {
            fs::read(directory.join(format!("{}.bin", hex(&h))))
                .map_err(|_| tickwire_release::Error::Missing)
        },
    )?;
    if ready["worker_pid"] != startup.restored.process.pid {
        return Err("final target PID changed".into());
    }
    Ok(
        serde_json::json!({"operation_schema":3,"restore_digest":seal.replay_restore_digest,"installation_digest":seal.replay_installation_digest,"rollback_digest":root,"source_report_position":startup.restored.source_report_position,"source_s_sequence":fixed.cut.sequence,"source_c_sequence":current.cut.sequence,"source_replay_pid":source.receipt().pid,"target_install_sequence":installed.initial_checkpoint()?.cut.sequence,"target_continued_sequence":installed.checkpoint.cut.sequence,"target_final_sequence":retained.current().cut.sequence,"target_pid":startup.restored.process.pid,"rollback_pid":b.process.pid,"target_schema":target_schema,"rollback_schema":b.output_schema,"action_pending_at_s":true,"action_remains_consumed":true,"custom_counters":custom_counters,"expansion_prefix":expansion_prefix}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use tickwire_strategy_sdk::restore::{FixedDemoFactory, RestoreFactory};
    #[test]
    fn source_s_must_precede_the_consumed_economic_action() {
        let mut checkpoint = vec![0; 96];
        checkpoint.extend(b"TWFD0001");
        checkpoint.push(1);
        fixed_source_state(&checkpoint).unwrap();
        checkpoint[104] = 0;
        assert!(fixed_source_state(&checkpoint).is_err());
    }
    #[test]
    fn v2_startup_and_final_action_states_have_opposite_completion_bits() {
        let mut state = b"TWFD0002".to_vec();
        state.extend([0; 33]);
        action_state(&state, false).unwrap();
        assert!(action_state(&state, true).is_err());
        state[8] = 1;
        action_state(&state, true).unwrap();
        assert!(action_state(&state, false).is_err());
    }
    #[test]
    fn release_documents_bind_exact_artifacts_and_input_prefix() {
        let member = tickwire_lifecycle::Member {
            risk_domain: [1; 16],
            exec_instance: [4; 16],
            exec_epoch: 1,
            old_instance: [2; 16],
            old_generation: 5,
            new_instance: [6; 16],
            new_generation: 6,
            clock_boot_id: [9; 16],
            clock_generation: 1,
            valid_until_ns: 1000,
        };
        let mut operation = Operation {
            schema: 3,
            operation_id: [140; 16],
            operation_revision: 1,
            logical_strategy_id: 8,
            members: vec![member.clone()],
            artifacts_digest: [1; 32],
            checkpoint_digest: [2; 32],
            rollback_digest: [3; 32],
            clock_boot_id: [9; 16],
            clock_generation: 1,
            valid_until_ns: 1000,
        };
        let make = |name: &str| ObjectFile {
            path: PathBuf::from(format!("/fixture/{name}")),
            object: Object::of(name.as_bytes()),
        };
        let input = tickwire_recovery::encode_inputs(
            [20; 16],
            1,
            [0; 32],
            &[tickwire_recovery::InputRecord {
                kind: 1,
                payload: b"literal input",
            }],
        )
        .unwrap();
        let source = SourceFiles {
            proof: make("proof"),
            checkpoint: make("checkpoint"),
            inputs: ObjectFile {
                path: "/fixture/input".into(),
                object: Object::of(&input.bytes),
            },
            account_inputs: make("account"),
            dependencies: vec![],
            prior_preflight: vec![],
        };
        let refs = |base: u8| {
            [
                Role::Binary,
                Role::HostConfig,
                Role::Bootstrap,
                Role::StrategyConfig,
            ]
            .into_iter()
            .enumerate()
            .map(|(i, role)| Artifact {
                role,
                object: Object::of(&[base + i as u8]),
            })
            .collect::<Vec<_>>()
        };
        let target = refs(30);
        let rollback = refs(40);
        let bytes = documents(
            &mut operation,
            &source,
            &input,
            target.clone(),
            rollback.clone(),
            FixedDemoFactory.capabilities(),
            crate::flavor::schemas(crate::ScenarioKind::SpotHandover),
        )
        .unwrap();
        let bundle = ReleaseBundle::decode(&bytes[0], &bytes[1], &bytes[2]).unwrap();
        let release = bundle.verify_operation(&operation, &member).unwrap();
        assert_eq!(release.target.files, target);
        assert_eq!(release.rollback.files, rollback);
        assert_eq!(
            release.migration.source_checkpoint,
            source.checkpoint.object
        );
        assert_eq!(release.migration.source_proof, source.proof.object);
        release
            .migration
            .inputs
            .as_ref()
            .unwrap()
            .verify(input.bytes.clone())
            .unwrap();
        let mut wrong_input = input.bytes.clone();
        *wrong_input.last_mut().unwrap() ^= 1;
        assert!(
            release
                .migration
                .inputs
                .as_ref()
                .unwrap()
                .verify(wrong_input)
                .is_err()
        );
        let mut changed = operation.clone();
        changed.artifacts_digest[0] ^= 1;
        assert!(bundle.verify_operation(&changed, &member).is_err());
        let mut noncanonical = bytes[0].clone();
        noncanonical.push(b'\n');
        assert!(ReleaseBundle::decode(&noncanonical, &bytes[1], &bytes[2]).is_err());
        assert_eq!(release.target.capabilities, FixedDemoFactory.capabilities());
        assert_eq!(
            release.rollback.probe_input,
            ProbeInput::TargetRestoreOutput
        );
        assert_ne!(release.migration.method, release.rollback.method);
    }
    #[test]
    fn generated_target_commits_recovery_template_and_referenced_paths() {
        struct Dir(PathBuf);
        impl Drop for Dir {
            fn drop(&mut self) {
                let _ = fs::remove_dir_all(&self.0);
            }
        }
        let root = PathBuf::from(format!(
            "/tmp/twp-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir(&root).unwrap();
        let root = Dir(root);
        let bins = root.0.join("bin");
        fs::create_dir(&bins).unwrap();
        for name in [
            "tickwire-app",
            "tickwire-md",
            "tickwire-strategy-worker",
            "tickwire-replica-receiver",
            "tickwire-lab",
            "tickwire-lifecycle",
        ] {
            fs::write(bins.join(name), name.as_bytes()).unwrap();
        }
        let scenario = crate::generate::generate_handover(&root.0.join("run"), &bins).unwrap();
        let committed = commit_target(&scenario).unwrap();
        let path = committed.bootstrap.recovery_template.as_ref().unwrap();
        let template: tickwire_strategy_sdk::recovery::Template =
            serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
        assert_eq!(committed.refs[4].role, Role::RecoveryTemplate);
        assert_eq!(committed.files[4].path, *path);
        assert_eq!(
            committed.refs[4].object,
            Object::of(&fs::read(path).unwrap())
        );
        for dependency in &template.dependencies {
            assert!(committed.refs.iter().zip(&committed.files).any(|(a,f)|a.object.digest==dependency.digest && f.path==dependency.path));
        }
        assert_eq!(committed.refs.len(), 12); // four core, template, seven distinct dependencies
        assert!(!scenario.root.join("next/preflight.json").exists());
        let dependency = template.dependencies.iter().find(|d| d.kind == 2).unwrap();
        fs::write(&dependency.path, b"changed").unwrap();
        assert!(
            artifacts(
                &committed.host,
                scenario.root.join("next/host.json"),
                scenario.root.join("next/bootstrap.json"),
                scenario.root.join("next/command.bin"),
                Some(path)
            )
            .is_err()
        );
    }
}
