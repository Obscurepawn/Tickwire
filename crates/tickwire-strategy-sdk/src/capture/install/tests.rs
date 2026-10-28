use super::*;
use tickwire_release::{Member, Operation};
pub(super) fn fixture() -> (CaptureBootstrap, Request, Seal) {
    let mut capture = super::super::tests::fixture();
    capture.identity.generation = 2;
    capture.market.endpoint.local.generation = 2;
    capture.report.local.generation = 2;
    let local = &capture.identity;
    let mut intent = capture.report.clone();
    intent.path = "/tmp/install-intent.sock".into();
    let mut bank = ResourceHeader::decode(&capture.report_header).unwrap();
    bank.resource_id = [93; 16];
    bank.resource_kind = 2;
    bank.record_bytes = 1024;
    bank.producer_instance = local.instance;
    bank.producer_generation = local.generation;
    bank.consumer_instance = intent.peer.instance;
    let bootstrap = Bootstrap {
        version: 1,
        market: capture.market.endpoint.clone(),
        external_market: Some(capture.market.clone()),
        market_admission_ready: Some(capture.market_admission_ready.clone()),
        intent,
        report: capture.report.clone(),
        report_header: capture.report_header.clone(),
        report_cursor_id: capture.report_cursor_id,
        report_subscription_generation: capture.report_subscription_generation,
        report_directory: capture.report_directory.clone(),
        join_file: "/tmp/install-join.json".into(),
        intent_header: crate::bootstrap::wire_bytes(&bank).unwrap(),
        intent_cursor_id: [94; 16],
        intent_subscription_generation: 1,
        risk_domain: local.risk_domain,
        command_directory: "/tmp/install-command".into(),
        clock_generation: local.clock.generation,
        startup_timeout_ms: 1000,
        maximum_market_age_ns: capture.contract.maximum_age_ns,
        maximum_levels: capture.market.maximum_levels,
        poll_records: 64,
        idle_sleep_us: 100,
        recovery_template: Some("/tmp/install-recovery.json".into()),
        handover_enabled: true,
        preflight_job: None,
        factory: Some(crate::registered::FactoryBinding {
            id: capture.contract.factory.clone(),
            configuration_file: "/tmp/install-config.json".into(),
            configuration: capture.contract.config,
            revision: capture.config.revision,
        }),
        demo_enabled: false,
        demo_command_file: None,
        demo_minimum_bid_ticks: 0,
    };
    let member = Member {
        risk_domain: local.risk_domain,
        exec_instance: [61; 16],
        exec_epoch: 1,
        old_instance: [62; 16],
        old_generation: 1,
        new_instance: local.instance,
        new_generation: local.generation,
        clock_boot_id: local.clock.boot_id,
        clock_generation: local.clock.generation,
        valid_until_ns: capture.deadline_mono_ns,
    };
    let plans = Object::of(b"plans");
    let manifest = Object::of(b"manifest");
    let cursor = tickwire_release::replay::Cursor {
        stream: capture.contract.markets[0].stream.clone(),
        sequence: 1,
        content_digest: [1; 32],
    };
    let start = tickwire_release::rebuild::Frontier {
        now_ns: 1,
        rows: 0,
        markets: vec![cursor.clone()],
        accounts: vec![cursor.clone()],
        external: vec![],
    };
    let mut end = start.clone();
    end.now_ns = 2;
    end.rows = 2;
    end.markets[0].sequence = 2;
    end.markets[0].content_digest = [2; 32];
    let seal = Seal {
        schema: 1,
        identity: local.clone(),
        contract: Object::of(&capture.contract.bytes().unwrap()),
        seed: Object::of(b"seed"),
        inputs: Object::of(b"inputs"),
        start,
        end,
        input_chain: [3; 32],
        account_input_digest: [4; 32],
    };
    let request = content::Request {
        schema: 1,
        operation: Operation {
            schema: 4,
            operation_id: [63; 16],
            operation_revision: 1,
            logical_strategy_id: local.logical_strategy,
            members: vec![member.clone()],
            artifacts_digest: manifest.digest,
            checkpoint_digest: plans.digest,
            rollback_digest: [64; 32],
            clock_boot_id: local.clock.boot_id,
            clock_generation: local.clock.generation,
            valid_until_ns: capture.deadline_mono_ns,
        },
        member,
        plans,
        manifest,
        capture_completion: Object::of(b"capture"),
        capture_receipt: Object::of(b"receipt"),
        capture_seal: Object::of(&seal.bytes().unwrap()),
        target_bootstrap: Object::of(&tickwire_release::replay::canonical(&bootstrap).unwrap()),
        deadline_mono_ns: capture.deadline_mono_ns,
    };
    let mut objects = vec![
        request.plans,
        request.manifest,
        request.capture_completion,
        request.capture_seal,
        request.capture_receipt,
        request.target_bootstrap,
    ];
    objects.sort_by_key(|o| o.digest);
    (
        capture,
        Request {
            request,
            bootstrap,
            objects,
            files: vec![
                tickwire_release::files::Location {
                    role: tickwire_release::Role::Binary,
                    path: "/tmp/worker".into(),
                },
                tickwire_release::files::Location {
                    role: tickwire_release::Role::StrategyConfig,
                    path: "/tmp/install-config.json".into(),
                },
                tickwire_release::files::Location {
                    role: tickwire_release::Role::RecoveryTemplate,
                    path: "/tmp/install-recovery.json".into(),
                },
            ],
        },
        seal,
    )
}
#[test]
fn installation_wire_challenge_binds_exact_seal_operation_and_bootstrap() {
    let (capture, request, seal) = fixture();
    let now = MonoTime::new(capture.market.domain().unwrap(), 100);
    let body = wire_request(&capture, &request, &seal, capture.deadline_mono_ns).unwrap();
    assert_eq!(body.replay_kind, 9);
    assert_eq!(body.expected_sequence, 2);
    let envelope = ControlEnvelope {
        opcode: 41,
        flags: 1,
        request_id: body.challenge,
        ..Default::default()
    };
    verify_request(
        &envelope,
        &body,
        &capture,
        &request,
        &seal,
        now,
        capture.deadline_mono_ns,
    )
    .unwrap();
    for n in 0..5 {
        let mut bad = body.clone();
        match n {
            0 => bad.expected_sequence = 0,
            1 => bad.operation_revision += 1,
            2 => bad.expected_cut_digest = [0; 32],
            3 => bad.challenge = [0; 16],
            _ => bad.object_digest = [0; 32],
        }
        assert!(
            verify_request(
                &envelope,
                &bad,
                &capture,
                &request,
                &seal,
                now,
                capture.deadline_mono_ns
            )
            .is_err()
        );
    }
    let mut changed = request.clone();
    changed.bootstrap.command_directory = "/tmp/replaced".into();
    assert!(changed.bytes().is_err());
}
#[test]
fn installation_object_list_cannot_drop_duplicate_or_oversize_dependencies() {
    let (_, request, _) = fixture();
    Request::decode(&request.bytes().unwrap()).unwrap();
    for n in 0..3 {
        let mut bad = request.clone();
        match n {
            0 => {
                bad.objects.remove(0);
            }
            1 => bad.objects.insert(0, bad.objects[0]),
            _ => bad.objects[0].bytes = tickwire_recovery::MAX_OBJECT_BYTES as u64 + 1,
        }
        assert!(bad.bytes().is_err());
    }
    let mut bytes = request.bytes().unwrap();
    bytes.push(b' ');
    assert!(Request::decode(&bytes).is_err());
}

#[test]
fn installation_files_bind_the_paths_the_target_will_actually_open() {
    let (_, request, _) = fixture();
    assert!(request.bytes().is_ok());
    for role in [
        tickwire_release::Role::StrategyConfig,
        tickwire_release::Role::RecoveryTemplate,
    ] {
        let mut replaced = request.clone();
        replaced
            .files
            .iter_mut()
            .find(|f| f.role == role)
            .unwrap()
            .path = "/tmp/substituted.json".into();
        assert!(
            replaced.bytes().is_err(),
            "replacement path accepted for {role:?}"
        );
        let mut missing = request.clone();
        missing.files.retain(|f| f.role != role);
        assert!(
            missing.bytes().is_err(),
            "unlisted target input accepted for {role:?}"
        );
    }
}

#[test]
fn installation_opaque_dependencies_keep_the_recovery_object_budget() {
    let (_, mut request, _) = fixture();
    let opaque = Object {
        digest: [201; 32],
        bytes: 17 * 1024 * 1024,
    };
    request.objects.push(opaque);
    request.objects.sort_by_key(|o| o.digest);
    assert!(request.bytes().is_ok());
    // Widening the list must not widen a typed root.
    let old = request.request.plans;
    request.request.plans.bytes = 17 * 1024 * 1024;
    *request.objects.iter_mut().find(|o| **o == old).unwrap() = request.request.plans;
    assert!(request.bytes().is_err());
}
