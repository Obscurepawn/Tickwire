use super::*;
use tickwire_release::*;
pub(crate) fn fixture_with_action(
    spent: bool,
) -> (
    ReleaseBundle,
    Operation,
    Member,
    VerifiedSourceReplay,
    crate::restore::Target,
    Source,
    VersionedBytes,
) {
    let (mut op, member, mut completion, mut objects) =
        super::super::source_tests::fixture_with_action(spent);
    let caps = crate::restore::FixedDemoFactory.capabilities();
    let files = [
        Role::Binary,
        Role::HostConfig,
        Role::Bootstrap,
        Role::StrategyConfig,
    ]
    .into_iter()
    .enumerate()
    .map(|(i, role)| Artifact {
        role,
        object: Object::of(&[i as u8 + 1]),
    })
    .collect::<Vec<_>>();
    let fixed_seal = tickwire_recovery::HandoverSeal::decode(
        objects
            .iter()
            .find(|b| Object::of(b) == completion.fixed_proof)
            .unwrap(),
    )
    .unwrap();
    let fixed_inputs = objects
        .iter()
        .find(|b| tickwire_recovery::digest(b) == fixed_seal.input_digest)
        .unwrap();
    let prefix = tickwire_recovery::EncodedInputs::decode(fixed_inputs.clone())
        .unwrap()
        .boundary;
    let manifest = ReleaseManifest {
        schema: 1,
        binding: completion.binding.clone(),
        targets: vec![tickwire_release::Target {
            member: member.clone(),
            version: [42; 32],
            files: files.clone(),
            capabilities: caps.clone(),
        }],
    };
    let migration = MigrationSet {
        schema: 1,
        binding: completion.binding.clone(),
        members: vec![Migration {
            member: member.clone(),
            source_checkpoint: Object {
                digest: fixed_seal.checkpoint_digest,
                bytes: fixed_seal.checkpoint_bytes,
            },
            source_schema: crate::restore::DEMO_V1_SCHEMA,
            source_proof: completion.fixed_proof,
            inputs: Some(InputPrefix {
                object: Object::of(fixed_inputs),
                stream: prefix.stream,
                first_sequence: prefix.first_sequence,
                previous_hash: prefix.previous_hash,
                last_sequence: prefix.last_sequence,
                last_hash: prefix.last_hash,
            }),
            method: Method::Migrate {
                source_schema: crate::restore::DEMO_V1_SCHEMA,
                target_schema: crate::restore::DEMO_SCHEMA,
                method_id: crate::restore::DEMO_MIGRATION,
            },
        }],
    };
    let rollback = RollbackPlan {
        schema: 1,
        binding: completion.binding.clone(),
        members: vec![Rollback {
            member: member.clone(),
            version: [43; 32],
            files: files.clone(),
            capabilities: caps,
            method: Method::Restore {
                schema: crate::restore::DEMO_SCHEMA,
            },
            probe_input: ProbeInput::TargetRestoreOutput,
        }],
    };
    let bundle = ReleaseBundle::decode(
        &encode(&manifest).unwrap(),
        &encode(&migration).unwrap(),
        &encode(&rollback).unwrap(),
    )
    .unwrap();
    [
        op.artifacts_digest,
        op.checkpoint_digest,
        op.rollback_digest,
    ] = bundle.roots();
    op.schema = 3;
    completion.operation = op.clone();
    objects.push(completion.bytes().unwrap());
    let proof = VerifiedSourceReplay::verify(
        &op,
        &member,
        Object::of(&completion.bytes().unwrap()),
        objects,
    )
    .unwrap();
    proof.bind_release(&bundle, &op).unwrap();
    let old = native::Parameters::decode(&proof.current().runtime.config.bytes).unwrap();
    let mut command = CanonicalCommand::decode(&old.command.unwrap())
        .unwrap()
        .command()
        .clone();
    command.context.strategy.instance_id = member.new_instance;
    command.context.request.strategy_generation = member.new_generation;
    command.context.request.config_revision = 3;
    let target = crate::restore::Target {
        instance: member.new_instance,
        generation: member.new_generation,
        logical_strategy: op.logical_strategy_id,
        command: Some(CanonicalCommand::new(command).unwrap()),
        minimum_bid_ticks: 7,
        enabled: true,
    };
    let config = VersionedBytes {
        revision: 3,
        bytes: native::Parameters {
            schema: 1,
            command: Some(target.command.as_ref().unwrap().bytes().to_vec()),
            minimum_bid_ticks: 7,
            enabled: true,
        }
        .bytes()
        .unwrap(),
    };
    let source = Source {
        logical_strategy: op.logical_strategy_id,
        instance: member.new_instance,
        generation: member.new_generation,
        binary_digest: files[0].object.digest,
        state_schema: crate::restore::DEMO_SCHEMA,
        input_stream: member.new_instance,
        clock: proof.current().source.clock.clone(),
    };
    (bundle, op, member, proof, target, source, config)
}
#[test]
fn target_migrates_actual_c_then_applies_new_config_and_keeps_source_views() {
    let (bundle, op, member, proof, target, source, config) = fixture_with_action(false);
    let release = bundle.verify_operation(&op, &member).unwrap();
    let candidate = prepare(
        &crate::restore::FixedDemoFactory,
        &release,
        target,
        &proof,
        source.clone(),
        config.clone(),
    )
    .unwrap();
    assert_eq!(candidate.checkpoint().source, source);
    assert_eq!(candidate.checkpoint().runtime.config, config);
    assert_eq!(
        candidate.checkpoint().runtime.markets,
        proof.current().runtime.markets
    );
    assert_eq!(
        candidate.checkpoint().runtime.accounts,
        proof.current().runtime.accounts
    );
    assert_eq!(
        candidate.checkpoint().runtime.effects,
        proof.current().runtime.effects
    );
    assert_eq!(candidate.checkpoint().cut.sequence, 0);
    let (app, _) = candidate.into_parts();
    let stream = &proof.current().runtime.markets[0].stream;
    assert!(app.market_view(stream, 1001).unwrap().best_bid().is_some());
    let state = native::Factory {
        schema: crate::restore::DEMO_SCHEMA,
    };
    use application::BusinessFactory;
    assert!(
        state
            .restore(
                &app.business_state().unwrap(),
                &RuntimeState {
                    config,
                    ..proof.current().runtime.clone()
                }
            )
            .is_ok()
    );
}

#[test]
fn migrated_spent_action_never_revives_after_target_config_or_new_market_callback() {
    let (bundle, op, member, proof, target, source, config) = fixture_with_action(true);
    assert_eq!(proof.fixed().runtime.effects.count, 0);
    assert_eq!(proof.current().runtime.effects.count, 1);
    let release = bundle.verify_operation(&op, &member).unwrap();
    let candidate = prepare(
        &crate::restore::FixedDemoFactory,
        &release,
        target,
        &proof,
        source,
        config,
    )
    .unwrap();
    let (app, checkpoint) = candidate.into_parts();
    assert_eq!(app.business_state().unwrap()[8], 1);
    let anchor = verify_anchor(
        &checkpoint.bytes().unwrap(),
        &checkpoint.source,
        Object::of(&checkpoint.runtime.config.bytes),
        &[],
    )
    .unwrap();
    let mut engine = Engine::new(&anchor, app).unwrap();
    let result = engine
        .apply_live(
            Input::MarketCallback {
                stream: checkpoint.runtime.markets[0].stream.clone(),
                instrument: 1001,
                invoked: true,
                maximum_age_ns: 1_000_000_000,
            },
            checkpoint.runtime.now_ns + 1,
        )
        .unwrap();
    assert!(result.commands.is_empty());
    assert_eq!(engine.checkpoint().unwrap().runtime.effects.count, 1);
}
#[test]
fn target_rejects_wrong_factory_configuration_and_reused_source_stream() {
    let (bundle, op, member, proof, target, source, mut config) = fixture_with_action(false);
    let release = bundle.verify_operation(&op, &member).unwrap();
    config.revision += 1;
    assert!(
        prepare(
            &crate::restore::FixedDemoFactory,
            &release,
            target.clone(),
            &proof,
            source.clone(),
            config
        )
        .is_err()
    );
    let config = crate::restore::FixedDemoFactory
        .configuration(&target)
        .unwrap();
    let mut source = source;
    source.input_stream = proof.current().source.input_stream;
    assert!(
        prepare(
            &crate::restore::FixedDemoFactory,
            &release,
            target,
            &proof,
            source,
            config
        )
        .is_err()
    );
}
#[test]
fn target_selection_is_synced_with_its_source_objects_before_the_active_cut_changes() {
    let (bundle, op, member, proof, target, source, config) = fixture_with_action(true);
    let release = bundle.verify_operation(&op, &member).unwrap();
    let initial = prepare(
        &crate::restore::FixedDemoFactory,
        &release,
        target.clone(),
        &proof,
        source.clone(),
        config.clone(),
    )
    .unwrap();
    let (app, checkpoint) = initial.into_parts();
    let models = checkpoint
        .runtime
        .models
        .iter()
        .map(|m| (m.id, Object::of(&m.value.bytes)))
        .collect::<Vec<_>>();
    let anchor = verify_anchor(
        &checkpoint.bytes().unwrap(),
        &checkpoint.source,
        Object::of(&checkpoint.runtime.config.bytes),
        &models,
    )
    .unwrap();
    let root = std::env::temp_dir().join(format!(
        "tickwire-replay-install-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let store = tickwire_recovery::FileBlobStore::open(&root).unwrap();
    let mut owner = durable::DurableEngine::new(anchor, app, store).unwrap();
    let mut next = source;
    next.input_stream = [29; 16];
    let candidate = prepare(
        &crate::restore::FixedDemoFactory,
        &release,
        target,
        &proof,
        next.clone(),
        config,
    )
    .unwrap();
    let result = candidate
        .persist(&mut owner, b"authenticated install request", &proof)
        .unwrap();
    let bytes = owner.read_object(result).unwrap();
    let installed = tickwire_release::replay::install::Installation::decode(&bytes).unwrap();
    assert_eq!(installed.target, next);
    assert_eq!(owner.durable_checkpoint().source, next);
    assert_eq!(
        installed.previous_target,
        Object::of(&checkpoint.bytes().unwrap())
    );
    assert_eq!(
        owner.read_object(installed.checkpoint).unwrap(),
        owner.durable_checkpoint().bytes().unwrap()
    );
    for bytes in proof.objects() {
        assert_eq!(owner.read_object(Object::of(bytes)).unwrap(), bytes);
    }
    assert_eq!(owner.strategy().business_state().unwrap()[8], 1);
    drop(owner);
    std::fs::remove_dir_all(root).unwrap();
}
struct FailSelection {
    file: tickwire_recovery::FileBlobStore,
    fail: std::rc::Rc<std::cell::Cell<bool>>,
    key: tickwire_recovery::StoreKey,
}
impl tickwire_recovery::BlobStore for FailSelection {
    fn get(
        &self,
        key: tickwire_recovery::StoreKey,
        max: usize,
    ) -> tickwire_recovery::Result<Vec<u8>> {
        self.file.get(key, max)
    }
    fn put_sync(
        &mut self,
        key: tickwire_recovery::StoreKey,
        bytes: &[u8],
    ) -> tickwire_recovery::Result<()> {
        self.file.put_sync(key, bytes)?;
        if self.fail.get() && key == self.key {
            self.fail.set(false);
            return Err(tickwire_recovery::Error::Io(std::io::Error::other(
                "injected lost synchronization return",
            )));
        }
        Ok(())
    }
}
#[test]
fn uncertain_selection_return_stops_outputs_and_retains_the_actual_durable_phase() {
    for installed in [false, true] {
        let (bundle, op, member, proof, target, source, config) = fixture_with_action(true);
        let release = bundle.verify_operation(&op, &member).unwrap();
        let initial = prepare(
            &crate::restore::FixedDemoFactory,
            &release,
            target.clone(),
            &proof,
            source.clone(),
            config.clone(),
        )
        .unwrap();
        let (app, checkpoint) = initial.into_parts();
        let anchor = verify_anchor(
            &checkpoint.bytes().unwrap(),
            &checkpoint.source,
            Object::of(&checkpoint.runtime.config.bytes),
            &[],
        )
        .unwrap();
        let root = std::env::temp_dir().join(format!(
            "tickwire-replay-lost-install-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let fail = std::rc::Rc::new(std::cell::Cell::new(false));
        let file = tickwire_recovery::FileBlobStore::open(&root).unwrap();
        let store = FailSelection {
            file,
            fail: fail.clone(),
            key: selection_key(&source, installed).unwrap(),
        };
        let mut owner = durable::DurableEngine::new(anchor, app, store).unwrap();
        let mut next = source;
        next.input_stream = [29; 16];
        let candidate = prepare(
            &crate::restore::FixedDemoFactory,
            &release,
            target,
            &proof,
            next,
            config,
        )
        .unwrap();
        fail.set(true);
        assert_eq!(
            candidate.persist(&mut owner, b"authenticated install request", &proof),
            Err(super::Error::Storage)
        );
        assert!(owner.stopped());
        assert_eq!(owner.durable_checkpoint(), &checkpoint);
        assert!(owner.flush().is_err());
        assert!(matches!(
            (installed, owner.selection().unwrap()),
            (true, Some(Selection::Installed(_))) | (false, Some(Selection::Prepared(_)))
        ));
        drop(owner);
        std::fs::remove_dir_all(root).unwrap();
    }
}
struct BadConfig {
    inner: crate::restore::RestoredDemo,
    changed: bool,
}
impl crate::Strategy for BadConfig {
    fn checkpoint(&self) -> crate::Result<Vec<u8>> {
        if self.changed {
            Ok(vec![0])
        } else {
            self.inner.checkpoint()
        }
    }
    fn on_market(
        &mut self,
        v: crate::MarketView<'_>,
        n: u64,
    ) -> crate::Result<Option<CanonicalCommand>> {
        self.inner.on_market(v, n)
    }
    fn apply_config(
        &mut self,
        _bytes: &[u8],
        _context: &mut ReplayContext<'_>,
    ) -> crate::Result<()> {
        self.changed = true;
        Ok(())
    }
}
struct BadFactory;
impl crate::restore::RestoreFactory for BadFactory {
    type Running = BadConfig;
    fn capabilities(&self) -> tickwire_release::Capabilities {
        crate::restore::FixedDemoFactory.capabilities()
    }
    fn configuration(&self, t: &crate::restore::Target) -> crate::restore::Result<VersionedBytes> {
        crate::restore::FixedDemoFactory.configuration(t)
    }
    fn migrate(
        &self,
        i: &crate::restore::VerifiedRestoreInput,
    ) -> crate::restore::Result<BadConfig> {
        Ok(BadConfig {
            inner: crate::restore::FixedDemoFactory.migrate(i)?,
            changed: false,
        })
    }
    fn validate_checkpoint(&self, b: &[u8]) -> crate::restore::Result<()> {
        crate::restore::FixedDemoFactory.validate_checkpoint(b)
    }
}
#[test]
fn configuration_callback_must_leave_a_restorable_target_checkpoint() {
    let (bundle, op, member, proof, target, source, config) = fixture_with_action(false);
    let release = bundle.verify_operation(&op, &member).unwrap();
    assert!(prepare(&BadFactory, &release, target, &proof, source, config).is_err());
}
#[test]
fn install_request_binds_full_operation_member_and_preallocates_only_bounded_objects() {
    let (_, mut op, member, proof, _, source, config) = fixture_with_action(false);
    op.schema = 3;
    let mut objects = proof.objects().map(Object::of).collect::<Vec<_>>();
    objects.sort_by_key(|o| o.digest);
    objects.dedup();
    let request = tickwire_release::replay::install::Request {
        schema: 1,
        operation: op,
        member,
        source_completion: proof.root(),
        source_objects: objects,
        target: source,
        config,
    };
    assert!(request.bytes().is_ok());
    for n in 0..7 {
        let mut bad = request.clone();
        match n {
            0 => bad.member.exec_epoch += 1,
            1 => bad.target.instance = [88; 16],
            2 => bad.operation.schema = 2,
            3 => bad.source_objects.push(bad.source_objects[0]),
            4 => bad.source_objects[0].bytes = u64::MAX,
            5 => bad.source_completion = Object::of(b"unlisted root"),
            _ => bad.config.revision = 0,
        }
        assert!(
            bad.bytes().is_err(),
            "mutation {n} must fail before object reads"
        );
    }
}

#[test]
fn startup_restore_is_a_distinct_stage_and_cannot_decode_as_a_finished_preflight() {
    let (bundle, op, member, proof, target, _, _) = fixture_with_action(false);
    let release = bundle.verify_operation(&op, &member).unwrap();
    let mut objects = proof.objects().map(Vec::from).collect::<Vec<_>>();
    objects.extend(bundle.object_bytes().unwrap());
    let get = |h| {
        objects
            .iter()
            .find(|b| Object::of(b).digest == h)
            .unwrap()
            .as_slice()
    };
    let seal = tickwire_recovery::HandoverSeal::decode(get(release.migration.source_proof.digest))
        .unwrap();
    let input = crate::restore::VerifiedRestoreInput::verify(
        &release,
        target,
        get(release.migration.source_proof.digest),
        get(seal.checkpoint_digest),
        get(seal.input_digest),
        get(seal.account_input_digest),
    )
    .unwrap();
    let restored = crate::restore::execute(&crate::restore::FixedDemoFactory, &input).unwrap();
    let output = restored.checkpoint().to_vec();
    let receipt = tickwire_release::receipt::Receipt {
        schema: 1,
        kind: tickwire_release::receipt::Kind::Restore,
        roots: bundle.roots(),
        member: member.clone(),
        logical_strategy: op.logical_strategy_id,
        process: tickwire_release::receipt::Process {
            instance: member.new_instance,
            generation: member.new_generation,
            pid: 123,
            uid: 1,
            gid: 1,
        },
        binary_digest: release
            .target
            .files
            .iter()
            .find(|f| f.role == Role::Binary)
            .unwrap()
            .object
            .digest,
        effective_bootstrap_digest: [91; 32],
        source_proof: release.migration.source_proof.digest,
        source_checkpoint: release.migration.source_checkpoint.digest,
        source_schema: release.migration.source_schema,
        method: release.migration.method.clone(),
        output_schema: release.target.capabilities.state_schema,
        output: Object::of(&output),
        source_report_position: input.source_report_position(),
    };
    receipt.verify(&release, None).unwrap();
    let receipt_bytes = receipt.bytes().unwrap();
    let stage = tickwire_release::replay::startup::Completion {
        schema: 1,
        operation: op,
        member,
        restored: Object::of(&receipt_bytes),
    };
    let bytes = stage.bytes().unwrap();
    let root = Object::of(&bytes);
    objects.extend([output, receipt_bytes, bytes.clone()]);
    let verified = tickwire_release::replay::startup::verify(root.digest, |h| {
        objects
            .iter()
            .find(|b| Object::of(b).digest == h)
            .cloned()
            .ok_or(tickwire_release::Error::Missing)
    })
    .unwrap();
    assert_eq!(verified.completion, stage);
    assert!(tickwire_release::receipt::Completion::decode(&bytes).is_err());
    assert!(tickwire_release::replay::rollback::Completion::decode(&bytes).is_err());
}
