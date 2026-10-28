use super::*;
use crate::Strategy;
use tickwire_release::{
    Binding, Member, Operation,
    replay::source::{Completion, SourceReceipt, VerifiedSourceReplay},
};

fn inputs(
    a: &Checkpoint,
    rows: &[MergedInput],
    c: &Checkpoint,
    baseline: &tickwire_account_codec::CanonicalBaseline,
) -> Vec<u8> {
    let mut callback = b"TWAI0001".to_vec();
    callback.push(0);
    callback.extend(11u64.to_le_bytes());
    callback.extend(0u64.to_le_bytes());
    callback.extend((baseline.bytes().len() as u32).to_le_bytes());
    callback.extend(baseline.bytes());
    let mut owned = vec![
        (2, callback),
        (5, [b"TWM2A001".as_slice(), &a.bytes().unwrap()].concat()),
    ];
    owned.extend(
        rows.iter()
            .map(|r| (5, [b"TWM2R001".as_slice(), &canonical(r).unwrap()].concat())),
    );
    owned.push((5, [b"TWM2C001".as_slice(), &c.bytes().unwrap()].concat()));
    tickwire_recovery::encode_inputs(
        [7; 16],
        1,
        [0; 32],
        &owned
            .iter()
            .map(|(kind, payload)| tickwire_recovery::InputRecord {
                kind: *kind,
                payload,
            })
            .collect::<Vec<_>>(),
    )
    .unwrap()
    .bytes
}
pub(super) fn closure(
    a: &Checkpoint,
    rows: &[MergedInput],
    c: &Checkpoint,
    baseline: &tickwire_account_codec::CanonicalBaseline,
) -> (Object, Vec<Vec<u8>>) {
    let account = tickwire_account::Account::from_snapshot(
        baseline.snapshot().clone(),
        baseline.catalog().clone(),
    )
    .unwrap();
    let business = application::business_bytes(&c.business).unwrap();
    let mut checkpoint = b"TWHS0001".to_vec();
    checkpoint.extend(c.source.instance);
    for v in [
        c.source.generation,
        c.source.logical_strategy,
        0,
        account.ledger_sequence(),
    ] {
        checkpoint.extend(v.to_le_bytes());
    }
    checkpoint.extend(account.strategy_orders_digest(c.source.logical_strategy));
    checkpoint.extend((business.len() as u64).to_le_bytes());
    checkpoint.extend(business);
    let input = inputs(a, rows, c, baseline);
    let account_input = tickwire_account_codec::InputSeal::new(baseline.digest())
        .unwrap()
        .object_bytes()
        .to_vec();
    let seal = tickwire_recovery::HandoverSeal {
        version: 1,
        owner: c.source.instance,
        instance: c.source.instance,
        generation: c.source.generation,
        logical_strategy: c.source.logical_strategy,
        report_position: 0,
        ledger_sequence: account.ledger_sequence(),
        orders_digest: account.strategy_orders_digest(c.source.logical_strategy),
        checkpoint_digest: tickwire_recovery::digest(&checkpoint),
        checkpoint_bytes: checkpoint.len() as u64,
        account_input_digest: tickwire_recovery::digest(&account_input),
        account_input_bytes: account_input.len() as u64,
        input_digest: tickwire_recovery::digest(&input),
        input_bytes: input.len() as u64,
        dependencies: (2..=11).map(|k| (k, baseline.digest())).collect(),
        preflight_receipt_digest: None,
        replay_installation_digest: None,
        replay_restore_digest: None,
        replay_rollback_digest: None,
        rebuild_installation_digest: None,
        rebuild_rollback_digest: None,
    };
    let bytes = seal.bytes().unwrap();
    (
        Object::of(&bytes),
        vec![
            bytes,
            checkpoint,
            input,
            account_input,
            baseline.bytes().to_vec(),
        ],
    )
}
pub(super) fn fixture() -> (Operation, Member, Completion, Vec<Vec<u8>>) {
    fixture_with_action(false)
}
pub(super) fn fixture_with_action(spent: bool) -> (Operation, Member, Completion, Vec<Vec<u8>>) {
    let mut command = crate::test_command::command();
    command.context.request.deadline = 2_000_000_000;
    command.context.instrument.instrument_id = 1001;
    command.context.instrument.instrument_version = 5;
    command.context.instrument.catalog_version = 5;
    command.context.instrument.catalog_hash = [0x22; 32];
    let command = CanonicalCommand::new(command).unwrap();
    let strategy = crate::FixedDemo::new(command.clone(), 65000, true);
    let registration = strategy.replay_registration().unwrap().unwrap();
    let (resource, replica, market_record) = application::tests::market_fixture();
    let app = application::Application::new(strategy, 1_000_000_000, vec![(resource, replica, 4)])
        .unwrap();
    let markets = app.market_cursors().unwrap();
    let mut c = tests::anchor_with(tests::Stateful::initial())
        .checkpoint()
        .clone();
    c.source.state_schema = registration.schema;
    c = Checkpoint::new(
        c.source,
        0,
        [0; 32],
        app.checkpoint().unwrap(),
        RuntimeState {
            now_ns: 1_500_000_000,
            config: registration.config,
            models: vec![],
            markets,
            accounts: vec![],
            external: vec![],
            timers: vec![],
            rng: Rng::Disabled,
            effects: EffectCut {
                count: 0,
                digest: [0; 32],
            },
        },
    )
    .unwrap();
    let a = verify_anchor(
        &c.bytes().unwrap(),
        &c.source,
        Object::of(&c.runtime.config.bytes),
        &[],
    )
    .unwrap();
    let mut engine = Engine::new(&a, app).unwrap();
    let (snapshot, catalog, _) = crate::account_fixture::fixture();
    let baseline = tickwire_account_codec::CanonicalBaseline::new(snapshot, catalog).unwrap();
    engine
        .apply_live(
            Input::AccountCallback {
                stream: account::AccountReplica::stream(&baseline),
                position: 0,
                canonical: baseline.bytes().to_vec(),
            },
            1_500_000_001,
        )
        .unwrap();
    let s = engine.checkpoint().unwrap();
    let (fixed_proof, mut objects) = closure(&c, engine.recorded(), &s, &baseline);
    let mut command = command.command().clone();
    command.context.request.config_revision += 1;
    let revision = command.context.request.config_revision;
    let parameters = native::Parameters {
        schema: 1,
        command: Some(CanonicalCommand::new(command).unwrap().bytes().to_vec()),
        minimum_bid_ticks: 1,
        enabled: true,
    };
    engine
        .apply_live(
            Input::ConfigApply {
                previous: Object::of(&s.runtime.config.bytes),
                next: VersionedBytes {
                    revision,
                    bytes: parameters.bytes().unwrap(),
                },
            },
            1_500_000_002,
        )
        .unwrap();
    if spent {
        let stream = s.runtime.markets[0].stream.clone();
        engine
            .apply_live(
                Input::MarketApplied {
                    stream: stream.clone(),
                    sequence: 1001,
                    received_ns: 1_600_000_000,
                    record: market_record,
                    payload: vec![],
                },
                1_600_000_001,
            )
            .unwrap();
        let effect = engine
            .apply_live(
                Input::MarketCallback {
                    stream,
                    instrument: 1001,
                    invoked: true,
                    maximum_age_ns: 1_000_000_000,
                },
                1_600_000_002,
            )
            .unwrap();
        assert_eq!(effect.commands.len(), 1);
    }
    let end = engine.checkpoint().unwrap();
    let (current_proof, more) = closure(&c, engine.recorded(), &end, &baseline);
    objects.extend(more);
    let member = Member {
        risk_domain: baseline.snapshot().scope.risk_domain_id,
        exec_instance: [6; 16],
        exec_epoch: 1,
        old_instance: s.source.instance,
        old_generation: s.source.generation,
        new_instance: [9; 16],
        new_generation: 6,
        clock_boot_id: s.source.clock.boot_id,
        clock_generation: 1,
        valid_until_ns: 100,
    };
    let op = Operation {
        schema: 2,
        operation_id: [8; 16],
        operation_revision: 1,
        logical_strategy_id: 8,
        members: vec![member.clone()],
        artifacts_digest: [1; 32],
        checkpoint_digest: [2; 32],
        rollback_digest: [3; 32],
        clock_boot_id: s.source.clock.boot_id,
        clock_generation: 1,
        valid_until_ns: 100,
    };
    let binding = Binding {
        operation_id: op.operation_id,
        revision: 1,
        logical_strategy: 8,
        members_digest: op.members_digest().unwrap(),
    };
    let sa = verify_anchor(
        &s.bytes().unwrap(),
        &s.source,
        Object::of(&s.runtime.config.bytes),
        &[],
    )
    .unwrap();
    let tail = encode_tail(&sa, &engine.recorded()[1..]).unwrap();
    let request = ControlInput::Capture {
        source: s.source.clone(),
        anchor: Object::of(&s.bytes().unwrap()),
        expected: s.cut.clone(),
        pause: true,
    }
    .bytes()
    .unwrap();
    let capture = CapturedTail {
        schema: 1,
        request: Object::of(&request),
        source: s.source.clone(),
        anchor: Object::of(&s.bytes().unwrap()),
        tail: Object::of(&tail),
        checkpoint: Object::of(&end.bytes().unwrap()),
        cut: end.cut.clone(),
        paused: true,
    }
    .bytes()
    .unwrap();
    let obj = |o: Object| crate::preflight::ObjectFile {
        path: std::path::PathBuf::from("/qualified-test/object"),
        object: o,
    };
    let job = process::SourceJob {
        schema: 1,
        binding: binding.clone(),
        source: s.source.clone(),
        request: obj(Object::of(&request)),
        capture: obj(Object::of(&capture)),
        anchor: obj(Object::of(&s.bytes().unwrap())),
        tail: obj(Object::of(&tail)),
        checkpoint: obj(Object::of(&end.bytes().unwrap())),
        output_directory: "/qualified-test/output".into(),
    }
    .bytes()
    .unwrap();
    let receipt = SourceReceipt {
        schema: 1,
        job: Object::of(&job),
        binding: binding.clone(),
        pid: 42,
        source: s.source.clone(),
        capture: Object::of(&capture),
        output: Object::of(&end.bytes().unwrap()),
        cut: end.cut.clone(),
    };
    let completion = Completion {
        schema: 1,
        operation: op.clone(),
        binding,
        member: member.clone(),
        fixed_proof,
        current_proof,
        anchor: Object::of(&s.bytes().unwrap()),
        tail: Object::of(&tail),
        checkpoint: receipt.output,
        receipt: Object::of(&receipt.bytes().unwrap()),
    };
    objects.extend([
        s.bytes().unwrap(),
        end.bytes().unwrap(),
        tail,
        request,
        capture,
        job,
        receipt.bytes().unwrap(),
        completion.bytes().unwrap(),
    ]);
    (op, member, completion, objects)
}
#[test]
fn source_evidence_binds_the_actual_job_and_cannot_be_retagged() {
    let (op, member, mut completion, mut objects) = fixture();
    let receipt_bytes = objects
        .iter()
        .find(|b| Object::of(b) == completion.receipt)
        .unwrap();
    let mut receipt = SourceReceipt::decode(receipt_bytes).unwrap();
    let verified = VerifiedSourceReplay::verify(
        &op,
        &member,
        Object::of(&completion.bytes().unwrap()),
        objects.clone(),
    )
    .unwrap();
    assert!(verified.current().cut.sequence > verified.fixed().cut.sequence);
    // This is a content-verifier unit test. PID 42 is not process evidence.
    let fake_job = b"{\"unrelated\":true}".to_vec();
    receipt.job = Object::of(&fake_job);
    completion.receipt = Object::of(&receipt.bytes().unwrap());
    objects.extend([
        fake_job,
        receipt.bytes().unwrap(),
        completion.bytes().unwrap(),
    ]);
    assert!(
        VerifiedSourceReplay::verify(
            &op,
            &member,
            Object::of(&completion.bytes().unwrap()),
            objects
        )
        .is_err()
    );
}
