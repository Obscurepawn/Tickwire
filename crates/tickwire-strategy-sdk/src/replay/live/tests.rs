use super::*;
use crate::bootstrap::{Endpoint, IdentityConfig};
fn bootstrap(path: &std::path::Path) -> Bootstrap {
    let local = IdentityConfig {
        instance: [2; 16],
        generation: 5,
        role: 3,
        logical_strategy: 8,
        host_boot: [5; 16],
        namespace: [6; 16],
        build_hash: [7; 32],
        qualification_hash: [8; 32],
        manifest_hash: [9; 32],
        arch: 1,
        trust_profile: 1,
    };
    let ep = Endpoint {
        path: path.join("control"),
        local: local.clone(),
        peer: local,
        peer_pid: 1,
        peer_uid: 0,
        peer_gid: 0,
        required_features: 0,
        supported_features: 0,
        control_epoch: 1,
        lease_duration_ns: 100,
        max_frame_bytes: 4096,
        nonce: [1; 32],
    };
    Bootstrap {
        version: 1,
        market: ep.clone(),
        external_market: None,
        market_admission_ready: None,
        intent: ep.clone(),
        report: ep,
        report_header: vec![],
        report_cursor_id: [1; 16],
        report_subscription_generation: 1,
        report_directory: path.join("reports"),
        join_file: path.join("join"),
        intent_header: vec![],
        intent_cursor_id: [2; 16],
        intent_subscription_generation: 1,
        risk_domain: [1; 16],
        command_directory: path.join("commands"),
        clock_generation: 1,
        startup_timeout_ms: 100,
        maximum_market_age_ns: 1_000_000_000,
        maximum_levels: 4,
        poll_records: 8,
        idle_sleep_us: 1,
        recovery_template: Some(path.join("recovery.json")),
        handover_enabled: true,
        preflight_job: None,
        factory: None,
        demo_enabled: true,
        demo_command_file: None,
        demo_minimum_bid_ticks: 1,
    }
}
fn demo() -> crate::FixedDemo {
    let mut c = crate::test_command::command();
    c.context.request.deadline = 10_000_000_000;
    c.context.instrument.instrument_id = 1001;
    c.context.instrument.instrument_version = 5;
    let mut refs = c.context.triggers.as_slice().to_vec();
    refs[0].instrument_id = 1001;
    refs[0].instrument_version = 5;
    c.context.triggers = tickwire_venue::BoundedList::new(refs).unwrap();
    crate::FixedDemo::new(CanonicalCommand::new(c).unwrap(), 1, true)
}
struct Temp(std::path::PathBuf);
impl Drop for Temp {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn temporary() -> Temp {
    let p = std::env::temp_dir().join(format!(
        "tickwire-live-replay-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&p).unwrap();
    Temp(p)
}
#[test]
fn production_owner_has_synced_actual_record_and_checkpoint_before_returning_the_order() {
    let root = temporary();
    let config = bootstrap(&root.0);
    std::fs::create_dir(&config.command_directory).unwrap();
    let (resource, replica, raw) = application::tests::market_fixture();
    let mut owner = Live::new(&config, demo(), resource, replica, 1_500_000_000).unwrap();
    let commands = owner.market(&raw, &[], 1_600_000_001).unwrap();
    assert_eq!(commands.len(), 1);
    let head = Head::decode(&std::fs::read(owner.directory.join("HEAD.json")).unwrap()).unwrap();
    let Owner::Recorded(e) = &owner.owner else {
        panic!()
    };
    assert_eq!(head.cut.sequence, 2);
    assert_eq!(head.commit, e.commit());
    let commit = InputCommit::decode(&e.read_object(head.commit).unwrap()).unwrap();
    let checkpoint = Checkpoint::decode(&e.read_object(commit.checkpoint).unwrap()).unwrap();
    assert_eq!(checkpoint.cut, head.cut);
    let rows = verify_tail(
        &verify_anchor(
            &e.anchor().bytes().unwrap(),
            &head.source,
            Object::of(&e.anchor().runtime.config.bytes),
            &[],
        )
        .unwrap(),
        &e.read_object(commit.tail).unwrap(),
        &head.cut,
    )
    .unwrap();
    assert_eq!(
        rows.rows().last().unwrap().effects[0].command,
        commands[0].bytes()
    );
}
#[test]
fn head_publish_failure_stops_the_owner_even_after_the_input_files_are_synced() {
    let root = temporary();
    let config = bootstrap(&root.0);
    std::fs::create_dir(&config.command_directory).unwrap();
    let (resource, replica, raw) = application::tests::market_fixture();
    let mut owner = Live::new(&config, demo(), resource, replica, 1_500_000_000).unwrap();
    std::fs::write(
        owner.directory.join("HEAD.pending"),
        b"interrupted publication",
    )
    .unwrap();
    assert!(owner.market(&raw, &[], 1_600_000_001).is_err());
    let Owner::Recorded(e) = &owner.owner else {
        panic!()
    };
    assert!(e.stopped());
    assert_eq!(e.last_durable_cut().sequence, 2);
    let head = Head::decode(&std::fs::read(owner.directory.join("HEAD.json")).unwrap()).unwrap();
    assert_eq!(head.cut.sequence, 0);
    assert!(owner.market(&raw, &[], 1_600_000_002).is_err());
}
#[test]
fn transition_head_failure_never_publishes_the_new_live_stream() {
    let root = temporary();
    let config = bootstrap(&root.0);
    std::fs::create_dir(&config.command_directory).unwrap();
    let (resource, replica, _) = application::tests::market_fixture();
    let mut live = Live::new(&config, demo(), resource, replica, 1_500_000_000).unwrap();
    assert!(live.supports_market_transition());
    let old = live.market.clone();
    let Owner::Recorded(e) = &mut live.owner else {
        panic!()
    };
    let (input, _, replica) =
        application::transition_tests::fixture(e.durable_checkpoint().runtime.markets[0].clone());
    std::fs::write(
        live.directory.join("HEAD.pending"),
        b"interrupted publication",
    )
    .unwrap();
    assert!(
        persist_transition(
            e,
            &live.directory,
            input,
            &replica.checkpoint().unwrap(),
            1_600_000_003
        )
        .is_err()
    );
    assert!(e.stopped());
    assert_eq!(e.last_durable_cut().sequence, 1);
    assert_eq!(live.market, old);
    let head = Head::decode(&std::fs::read(live.directory.join("HEAD.json")).unwrap()).unwrap();
    assert_eq!(head.cut.sequence, 0);
    assert!(!live.supports_market_transition());
}

#[test]
fn transition_capacity_and_actual_view_mismatch_are_rejected_before_input_apply() {
    for oversized in [false, true] {
        let root = temporary();
        let config = bootstrap(&root.0);
        std::fs::create_dir(&config.command_directory).unwrap();
        let (resource, replica, _) = application::tests::market_fixture();
        let mut live = Live::new(&config, demo(), resource, replica, 1_500_000_000).unwrap();
        let Owner::Recorded(e) = &mut live.owner else {
            panic!()
        };
        let (mut input, _, replica) = application::transition_tests::fixture(
            e.durable_checkpoint().runtime.markets[0].clone(),
        );
        let expected = if oversized {
            let Input::MarketTransition { baseline, .. } = &mut input else {
                panic!()
            };
            baseline.resize(MAX_BYTES + 1, 0);
            replica.checkpoint().unwrap()
        } else {
            vec![1]
        };
        assert!(persist_transition(e, &live.directory, input, &expected, 1_600_000_003).is_err());
        assert!(e.stopped());
        assert_eq!(e.memory_cut().sequence, 0);
        assert_eq!(e.last_durable_cut().sequence, 0);
    }
}
#[test]
fn actual_live_install_retries_return_one_durable_completion_without_repeating_callbacks() {
    install_and_rollback_with_dependency(vec![91; 32], false);
}
#[test]
fn install_and_rollback_preserve_large_opaque_source_dependencies() {
    install_and_rollback_with_dependency(vec![91; MAX_BYTES + 1], false);
}
#[test]
fn schema3_installation_and_rollback_keep_a_post_c_market_transition() {
    install_and_rollback_with_dependency(vec![91; 32], true);
}
fn install_and_rollback_with_dependency(dependency: Vec<u8>, transition: bool) {
    let (bundle, op, member, proof, target, source, config) =
        super::super::install::tests::fixture_with_action(false);
    let dependency_ref = Object::of(&dependency);
    let mut objects = proof.objects().map(Vec::from).collect::<Vec<_>>();
    objects.push(dependency);
    let proof = tickwire_release::replay::source::VerifiedSourceReplay::verify(
        &op,
        &member,
        proof.root(),
        objects,
    )
    .unwrap();
    let release = bundle.verify_operation(&op, &member).unwrap();
    let candidate = install::prepare(
        &crate::restore::FixedDemoFactory,
        &release,
        target.clone(),
        &proof,
        source.clone(),
        config.clone(),
    )
    .unwrap();
    let (app, cp) = candidate.into_parts();
    let anchor = verify_anchor(
        &cp.bytes().unwrap(),
        &cp.source,
        Object::of(&cp.runtime.config.bytes),
        &[],
    )
    .unwrap();
    let root = temporary();
    let store = FileBlobStore::open(&root.0).unwrap();
    let mut e = DurableEngine::new(anchor, app, store).unwrap();
    for bytes in bundle.object_bytes().unwrap() {
        e.store_object(&bytes).unwrap();
    }
    sync_head(&mut e, &root.0).unwrap();
    let mut live = Live {
        owner: Owner::Recorded(Box::new(e)),
        market: cp.runtime.markets[0].stream.clone(),
        account: Some(cp.runtime.accounts[0].stream.clone()),
        maximum_age: 1_000_000_000,
        directory: root.0.clone(),
    };
    let (_, _, raw) = application::tests::market_fixture();
    // These are old Shadow outputs, deliberately not delivered to an intent ring.
    assert_eq!(live.market(&raw, &[], 1_600_000_001).unwrap().len(), 1);
    if transition {
        let Owner::Recorded(e) = &mut live.owner else {
            panic!()
        };
        let (input, _, replica) = application::transition_tests::fixture(
            e.durable_checkpoint().runtime.markets[0].clone(),
        );
        live.market = persist_transition(
            e,
            &root.0,
            input,
            &replica.checkpoint().unwrap(),
            1_650_000_000,
        )
        .unwrap();
    }
    let before = live.committed().unwrap().0.clone();
    let mut objects = proof.objects().map(Object::of).collect::<Vec<_>>();
    objects.sort_by_key(|o| o.digest);
    objects.dedup();
    let mut next = source;
    next.input_stream = [29; 16];
    let input = ControlInput::Install {
        source: before.source.clone(),
        expected: before.cut.clone(),
        request: Box::new(tickwire_release::replay::install::Request {
            schema: 1,
            operation: op,
            member,
            source_completion: proof.root(),
            source_objects: objects,
            target: next,
            config,
        }),
    };
    let result = live
        .install(&input, &release, target.clone(), &proof, 4, || {
            Ok(1_700_000_000)
        })
        .unwrap();
    let after = live.committed().unwrap().0.clone();
    if transition {
        assert_eq!(after.runtime.markets[0].sequence, 1002);
        assert_ne!(
            after.runtime.markets[0].stream,
            proof.current().runtime.markets[0].stream
        );
        let Owner::Recorded(e) = &live.owner else {
            panic!()
        };
        assert!(e.strategy().market_view(&live.market, u64::MAX).is_some());
    }
    assert_eq!(after.runtime.effects.count, 1);
    assert_eq!(live.checkpoint().unwrap()[8], 1);
    assert_eq!(live.control_checkpoint(&input).unwrap(), before);
    assert_eq!(
        live.install(&input, &release, target, &proof, 4, || panic!(
            "retry must not sample or invoke a callback"
        ))
        .unwrap(),
        result
    );
    assert_eq!(live.committed().unwrap().0, &after);
    let head = Head::decode(&std::fs::read(root.0.join("HEAD.json")).unwrap()).unwrap();
    assert_eq!(head.cut, after.cut);
    let verified = live.installation_objects(result).unwrap();
    assert!(
        verified
            .objects
            .iter()
            .any(|b| Object::of(b) == dependency_ref)
    );
    let Owner::Recorded(e) = &mut live.owner else {
        panic!()
    };
    let completed =
        tickwire_release::replay::install::Continued::decode(&e.read_object(result).unwrap())
            .unwrap();
    assert_eq!(completed.checkpoint, Object::of(&after.bytes().unwrap()));
    assert_eq!(verified.checkpoint, after);
    let mut rollback_command = crate::test_command::command();
    rollback_command.context.strategy.instance_id = [87; 16];
    rollback_command.context.request.strategy_generation = 7;
    let rollback_target = crate::restore::Target {
        instance: [87; 16],
        generation: 7,
        logical_strategy: 8,
        command: Some(CanonicalCommand::new(rollback_command).unwrap()),
        minimum_bid_ticks: 1,
        enabled: true,
    };
    let rollback = crate::restore::VerifiedRestoreInput::for_installed_probe(
        &release,
        rollback_target,
        &verified,
    )
    .unwrap();
    // C is unspent; post-C Shadow continuation consumes the opportunity. The
    // probe must test the migrated C output, not S or the later continued state.
    assert_eq!(rollback.state()[8], 0);
    assert_eq!(
        application::business_bytes(&verified.checkpoint.business).unwrap()[8],
        1
    );
    let restored = crate::restore::execute(&crate::restore::FixedDemoFactory, &rollback).unwrap();
    assert_eq!(restored.checkpoint()[8], 0);
    let files = verified
        .objects
        .iter()
        .map(|bytes| {
            crate::recovery::object(&root.0, bytes).unwrap();
            let object = Object::of(bytes);
            tickwire_release::files::ObjectFile {
                path: root.0.join(format!(
                    "{}.bin",
                    object
                        .digest
                        .iter()
                        .map(|b| format!("{b:02x}"))
                        .collect::<String>()
                )),
                object,
            }
        })
        .collect::<Vec<_>>();
    let job = tickwire_release::replay::rollback::Job {
        schema: 1,
        installation: result,
        objects: files,
        rollback_files: vec![],
        output_directory: root.0.join("rollback-output"),
    };
    let job_bytes = job.bytes().unwrap();
    let reopened = tickwire_release::replay::rollback::Job::decode(&job_bytes)
        .unwrap()
        .load()
        .unwrap();
    assert_eq!(reopened.checkpoint, after);
    let role = |role| {
        release
            .rollback
            .files
            .iter()
            .find(|f| f.role == role)
            .unwrap()
            .object
            .digest
    };
    let receipt = tickwire_release::replay::rollback::Receipt {
        schema: 1,
        job: Object::of(&job_bytes),
        installation: result,
        operation: verified.request.operation.clone(),
        member: verified.request.member.clone(),
        process: tickwire_release::receipt::Process {
            instance: [87; 16],
            generation: 7,
            pid: 123,
            uid: 1,
            gid: 1,
        },
        binary_digest: role(tickwire_release::Role::Binary),
        bootstrap_digest: role(tickwire_release::Role::Bootstrap),
        input_checkpoint: verified.installation.checkpoint,
        input_state: Object::of(&verified.installed_business().unwrap()),
        source_schema: verified.request.target.state_schema,
        method: release.rollback.method.clone(),
        output_schema: release.rollback.capabilities.state_schema,
        output: Object::of(restored.checkpoint()),
    };
    let receipt_bytes = receipt.bytes().unwrap();
    let completion = tickwire_release::replay::rollback::Completion {
        schema: 1,
        installation: result,
        rollback: Object::of(&receipt_bytes),
    }
    .bytes()
    .unwrap();
    let mut rollback_objects = verified.objects.clone();
    rollback_objects.extend([
        job_bytes,
        receipt_bytes,
        completion.clone(),
        restored.checkpoint().to_vec(),
    ]);
    let rollback_objects = rollback_objects
        .into_iter()
        .map(|bytes| (Object::of(&bytes).digest, bytes))
        .collect::<std::collections::BTreeMap<_, _>>();
    let read = |h| {
        rollback_objects
            .get(&h)
            .cloned()
            .ok_or(tickwire_release::Error::Missing)
    };
    let rollback_verified =
        tickwire_release::replay::rollback::verify(Object::of(&completion).digest, read).unwrap();
    assert!(
        rollback_verified
            .objects
            .iter()
            .any(|b| Object::of(b) == dependency_ref)
    );
    assert!(
        tickwire_release::replay::rollback::verify(Object::of(&completion).digest, |h| {
            let mut bytes = read(h)?;
            if h == dependency_ref.digest {
                bytes[0] ^= 1;
            }
            Ok(bytes)
        })
        .is_err()
    );
    // The installation cut may be sealed as v3 only with its exact new anchor
    // and real callback tail; old Shadow bytes cannot become the new receipt.
    let (snapshot, catalog, _) = crate::account_fixture::fixture();
    let baseline = tickwire_account_codec::CanonicalBaseline::new(snapshot, catalog).unwrap();
    let (seal_object, seal_objects) =
        super::super::source_tests::closure(e.anchor(), e.recorded(), &after, &baseline);
    let find = |hash| {
        seal_objects
            .iter()
            .find(|b| Object::of(b).digest == hash)
            .unwrap()
            .as_slice()
    };
    let mut seal = tickwire_recovery::HandoverSeal::decode(find(seal_object.digest)).unwrap();
    seal.version = 3;
    seal.preflight_receipt_digest = None;
    seal.replay_restore_digest = Some([42; 32]);
    seal.replay_installation_digest = Some(result.digest);
    // The separate preflight graph is checked by Handover/Authority. This tests
    // the installation-to-retained-cut binding performed at that boundary.
    assert!(
        verified
            .bind_seal(&seal, find(seal.checkpoint_digest), find(seal.input_digest))
            .is_ok()
    );
    let mut wrong = seal.clone();
    wrong.replay_installation_digest = Some([43; 32]);
    assert!(
        verified
            .bind_seal(
                &wrong,
                find(seal.checkpoint_digest),
                find(seal.input_digest)
            )
            .is_err()
    );
    let mut wrong = seal.clone();
    wrong.instance = [44; 16];
    assert!(
        verified
            .bind_seal(
                &wrong,
                find(seal.checkpoint_digest),
                find(seal.input_digest)
            )
            .is_err()
    );
    let mut published = 0;
    e.flush()
        .unwrap()
        .publish(|_| {
            published += 1;
            Ok(())
        })
        .unwrap();
    assert_eq!(published, 0);
}
