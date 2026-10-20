use super::*;
fn target() -> Target {
    let mut c = crate::test_command::command();
    c.context.strategy.instance_id = [6; 16];
    c.context.request.strategy_generation = 7;
    Target {
        instance: [6; 16],
        generation: 7,
        logical_strategy: 8,
        command: Some(CanonicalCommand::new(c).unwrap()),
        minimum_bid_ticks: 1,
        enabled: true,
    }
}
fn input(state: Vec<u8>, schema: u64, method: Method) -> VerifiedRestoreInput {
    VerifiedRestoreInput {
        target: target(),
        method,
        expected: FixedDemoFactory.capabilities(),
        state,
        source_schema: schema,
        source_checkpoint: [5; 32],
        source_instance: [2; 16],
        source_generation: 5,
        source_report_position: 0,
        callbacks: None,
    }
}
#[test]
fn spent_legacy_action_is_not_rearmed_and_authority_is_from_target() {
    let c = CanonicalCommand::new(crate::test_command::command()).unwrap();
    let mut old = FixedDemo::new(c, 100, true);
    old.command = None;
    let i = input(
        old.checkpoint().unwrap(),
        DEMO_V1_SCHEMA,
        Method::Migrate {
            source_schema: DEMO_V1_SCHEMA,
            target_schema: DEMO_SCHEMA,
            method_id: DEMO_MIGRATION,
        },
    );
    let restored = execute(&FixedDemoFactory, &i).unwrap();
    assert_eq!(restored.checkpoint()[8], 1);
    let s = restored.into_strategy();
    assert!(s.inner.command.is_none());
    assert_eq!(
        s.inner.original.command().context.strategy.instance_id,
        [6; 16]
    );
}
#[test]
fn spent_v2_action_survives_restore_with_changed_threshold() {
    let mut b = b"TWFD0002".to_vec();
    b.push(1);
    b.extend(action_key(target().command.as_ref().unwrap()));
    let i = input(
        b,
        DEMO_SCHEMA,
        Method::Restore {
            schema: DEMO_SCHEMA,
        },
    );
    assert_eq!(execute(&FixedDemoFactory, &i).unwrap().checkpoint()[8], 1);
}
#[test]
fn default_factory_does_not_claim_restore_completion() {
    struct Empty;
    impl RestoreFactory for Empty {
        type Running = ObserveOnly;
        fn capabilities(&self) -> Capabilities {
            ObserveFactory.capabilities()
        }
    }
    let mut i = input(
        b"TWOBS001".to_vec(),
        OBSERVE_SCHEMA,
        Method::Restore {
            schema: OBSERVE_SCHEMA,
        },
    );
    i.expected = ObserveFactory.capabilities();
    i.target.command = None;
    i.target.enabled = false;
    assert!(matches!(execute(&Empty, &i), Err(Error::Unsupported)));
    assert_eq!(
        execute(&ObserveFactory, &i).unwrap().checkpoint(),
        b"TWOBS001"
    );
}

use crate::account_fixture as fixture;

fn sealed_source() -> (tickwire_recovery::HandoverSeal, Vec<u8>, Vec<u8>, Vec<u8>) {
    use tickwire_account_codec::{CanonicalBaseline, InputSeal};
    let (snapshot, catalog, _) = fixture::fixture();
    let baseline = CanonicalBaseline::new(snapshot, catalog).unwrap();
    let account = tickwire_account::Account::recover(
        baseline.snapshot().clone(),
        baseline.catalog().clone(),
        &[],
    )
    .unwrap();
    let account_input = InputSeal::new(baseline.digest()).unwrap();
    let mut callback = b"TWAI0001".to_vec();
    callback.push(0);
    callback.extend(10u64.to_le_bytes());
    callback.extend(0u64.to_le_bytes());
    callback.extend((baseline.bytes().len() as u32).to_le_bytes());
    callback.extend(baseline.bytes());
    let inputs = tickwire_recovery::encode_inputs(
        [9; 16],
        1,
        [0; 32],
        &[tickwire_recovery::InputRecord {
            kind: 2,
            payload: &callback,
        }],
    )
    .unwrap();
    let state = b"TWOBS001";
    let mut checkpoint = b"TWHS0001".to_vec();
    checkpoint.extend([2; 16]);
    for n in [5u64, 8, 0, account.ledger_sequence()] {
        checkpoint.extend(n.to_le_bytes());
    }
    checkpoint.extend(account.strategy_orders_digest(8));
    checkpoint.extend((state.len() as u64).to_le_bytes());
    checkpoint.extend(state);
    let seal = tickwire_recovery::HandoverSeal {
        preflight_receipt_digest: None,
        version: 1,
        owner: [2; 16],
        instance: [2; 16],
        generation: 5,
        logical_strategy: 8,
        report_position: 0,
        ledger_sequence: account.ledger_sequence(),
        orders_digest: account.strategy_orders_digest(8),
        checkpoint_digest: tickwire_recovery::digest(&checkpoint),
        checkpoint_bytes: checkpoint.len() as u64,
        account_input_digest: account_input.digest(),
        account_input_bytes: account_input.object_bytes().len() as u64,
        input_digest: tickwire_recovery::digest(&inputs.bytes),
        input_bytes: inputs.bytes.len() as u64,
        dependencies: (2..=11).map(|n| (n, [n as u8; 32])).collect(),
    };
    (
        seal,
        checkpoint,
        inputs.bytes,
        account_input.object_bytes().to_vec(),
    )
}
fn release_for(
    seal: &tickwire_recovery::HandoverSeal,
    inputs: &[u8],
) -> (tickwire_release::ReleaseBundle, tickwire_release::Operation) {
    use tickwire_release::*;
    let member = Member {
        risk_domain: [1; 16],
        exec_instance: [3; 16],
        exec_epoch: 4,
        old_instance: [2; 16],
        old_generation: 5,
        new_instance: [6; 16],
        new_generation: 7,
        clock_boot_id: [8; 16],
        clock_generation: 9,
        valid_until_ns: 1000,
    };
    let mut op = Operation {
        schema: 1,
        operation_id: [10; 16],
        operation_revision: 1,
        logical_strategy_id: 8,
        members: vec![member.clone()],
        artifacts_digest: [11; 32],
        checkpoint_digest: [12; 32],
        rollback_digest: [13; 32],
        clock_boot_id: [8; 16],
        clock_generation: 9,
        valid_until_ns: 1000,
    };
    let binding = Binding {
        operation_id: op.operation_id,
        revision: op.operation_revision,
        logical_strategy: 8,
        members_digest: op.members_digest().unwrap(),
    };
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
        object: Object {
            digest: [i as u8 + 1; 32],
            bytes: 10,
        },
    })
    .collect::<Vec<_>>();
    let caps = ObserveFactory.capabilities();
    let method = Method::Restore {
        schema: OBSERVE_SCHEMA,
    };
    let m = ReleaseManifest {
        schema: 1,
        binding: binding.clone(),
        targets: vec![tickwire_release::Target {
            member: member.clone(),
            version: [20; 32],
            files: files.clone(),
            capabilities: caps.clone(),
        }],
    };
    let b = tickwire_recovery::EncodedInputs::decode(inputs.to_vec())
        .unwrap()
        .boundary;
    let migration = MigrationSet {
        schema: 1,
        binding: binding.clone(),
        members: vec![Migration {
            member: member.clone(),
            source_checkpoint: Object {
                digest: seal.checkpoint_digest,
                bytes: seal.checkpoint_bytes,
            },
            source_schema: OBSERVE_SCHEMA,
            source_proof: Object::of(&seal.bytes().unwrap()),
            inputs: Some(InputPrefix {
                object: Object::of(inputs),
                stream: b.stream,
                first_sequence: b.first_sequence,
                previous_hash: b.previous_hash,
                last_sequence: b.last_sequence,
                last_hash: b.last_hash,
            }),
            method: method.clone(),
        }],
    };
    let rollback = RollbackPlan {
        schema: 1,
        binding,
        members: vec![Rollback {
            member,
            version: [21; 32],
            files,
            capabilities: caps,
            method,
            probe_input: ProbeInput::TargetRestoreOutput,
        }],
    };
    let bundle = ReleaseBundle::decode(
        &encode(&m).unwrap(),
        &encode(&migration).unwrap(),
        &encode(&rollback).unwrap(),
    )
    .unwrap();
    [
        op.artifacts_digest,
        op.checkpoint_digest,
        op.rollback_digest,
    ] = bundle.roots();
    (bundle, op)
}
#[test]
fn actual_source_closure_is_checked_before_running_restore() {
    let (mut seal, checkpoint, inputs, account_input) = sealed_source();
    let (bundle, op) = release_for(&seal, &inputs);
    let member = bundle.verify_operation(&op, &op.members[0]).unwrap();
    let mut t = target();
    t.command = None;
    t.enabled = false;
    let verified = VerifiedRestoreInput::verify(
        &member,
        t.clone(),
        &seal.bytes().unwrap(),
        &checkpoint,
        &inputs,
        &account_input,
    )
    .unwrap();
    assert_eq!(
        execute(&ObserveFactory, &verified).unwrap().checkpoint(),
        b"TWOBS001"
    );
    assert_eq!(verified.source_identity(), ([2; 16], 5));
    t.logical_strategy = 9;
    assert!(matches!(
        VerifiedRestoreInput::verify(
            &member,
            t,
            &seal.bytes().unwrap(),
            &checkpoint,
            &inputs,
            &account_input
        ),
        Err(Error::Identity)
    ));
    // Rehashing a syntactically valid stream without the baseline callback cannot
    // replace the actual callback closure, even if all release roots are changed.
    let empty = tickwire_recovery::encode_inputs([9; 16], 1, [0; 32], &[])
        .unwrap()
        .bytes;
    seal.input_digest = tickwire_recovery::digest(&empty);
    seal.input_bytes = empty.len() as u64;
    let (bundle, op) = release_for(&seal, &empty);
    let member = bundle.verify_operation(&op, &op.members[0]).unwrap();
    let mut t = target();
    t.command = None;
    t.enabled = false;
    assert!(matches!(
        VerifiedRestoreInput::verify(
            &member,
            t,
            &seal.bytes().unwrap(),
            &checkpoint,
            &empty,
            &account_input
        ),
        Err(Error::Input)
    ));
}
#[test]
fn old_authority_changed_action_and_unknown_schema_are_rejected() {
    let old = FixedDemo::new(
        CanonicalCommand::new(crate::test_command::command()).unwrap(),
        1,
        true,
    );
    let mut i = input(
        old.checkpoint().unwrap(),
        DEMO_V1_SCHEMA,
        Method::Migrate {
            source_schema: DEMO_V1_SCHEMA,
            target_schema: DEMO_SCHEMA,
            method_id: DEMO_MIGRATION,
        },
    );
    let s = execute(&FixedDemoFactory, &i).unwrap().into_strategy();
    assert!(s.inner.command.is_some());
    i.target.command = Some(CanonicalCommand::new(crate::test_command::command()).unwrap());
    assert!(matches!(
        execute(&FixedDemoFactory, &i),
        Err(Error::Identity)
    ));
    i.target = target();
    let mut c = i.target.command.as_ref().unwrap().command().clone();
    c.context.strategy.economic_intent_id = [99; 16];
    i.target.command = Some(CanonicalCommand::new(c).unwrap());
    assert!(matches!(
        execute(&FixedDemoFactory, &i),
        Err(Error::Identity)
    ));
    i.target = target();
    i.state[8] = 2;
    assert!(matches!(execute(&FixedDemoFactory, &i), Err(Error::State)));
}
#[test]
fn callback_errors_and_invalid_outputs_cannot_finish_restore() {
    use std::cell::Cell;
    struct Factory {
        called: Cell<u32>,
        bad_output: bool,
    }
    struct Bad;
    impl Strategy for Bad {
        fn checkpoint(&self) -> crate::Result<Vec<u8>> {
            Ok(b"bad".to_vec())
        }
        fn on_market(
            &mut self,
            _: crate::MarketView<'_>,
            _: u64,
        ) -> crate::Result<Option<CanonicalCommand>> {
            Ok(None)
        }
    }
    impl RestoreFactory for Factory {
        type Running = Bad;
        fn capabilities(&self) -> Capabilities {
            ObserveFactory.capabilities()
        }
        fn restore(&self, _: &VerifiedRestoreInput) -> Result<Bad> {
            self.called.set(self.called.get() + 1);
            if self.bad_output {
                Ok(Bad)
            } else {
                Err(Error::Callback)
            }
        }
        fn validate_checkpoint(&self, b: &[u8]) -> Result<()> {
            ObserveFactory.validate_checkpoint(b)
        }
    }
    let mut i = input(
        b"TWOBS001".to_vec(),
        OBSERVE_SCHEMA,
        Method::Restore {
            schema: OBSERVE_SCHEMA,
        },
    );
    i.expected = ObserveFactory.capabilities();
    i.target.command = None;
    i.target.enabled = false;
    for bad_output in [false, true] {
        let f = Factory {
            called: Cell::new(0),
            bad_output,
        };
        assert!(execute(&f, &i).is_err());
        assert_eq!(f.called.get(), 1);
    }
}

#[test]
fn completion_requires_actual_target_output_probe_and_complete_source_callbacks() {
    use tickwire_release::{
        Object,
        receipt::{Completion, Kind, Process, Receipt},
    };
    let (mut seal, checkpoint, inputs, account) = sealed_source();
    let dep = b"retained dependency".to_vec();
    seal.dependencies = (2..=11)
        .map(|n| (n, tickwire_recovery::digest(&dep)))
        .collect();
    let (bundle, mut op) = release_for(&seal, &inputs);
    op.schema = 2;
    let release = bundle.verify_operation(&op, &op.members[0]).unwrap();
    let mut t = target();
    t.command = None;
    t.enabled = false;
    let verified = VerifiedRestoreInput::verify(
        &release,
        t,
        &seal.bytes().unwrap(),
        &checkpoint,
        &inputs,
        &account,
    )
    .unwrap();
    let restored = execute(&ObserveFactory, &verified).unwrap();
    let output = restored.checkpoint().to_vec();
    let a = Receipt {
        schema: 1,
        kind: Kind::Restore,
        roots: bundle.roots(),
        member: op.members[0].clone(),
        logical_strategy: 8,
        process: Process {
            instance: [6; 16],
            generation: 7,
            pid: 100,
            uid: 501,
            gid: 20,
        },
        binary_digest: release.target.files[0].object.digest,
        effective_bootstrap_digest: [31; 32],
        source_proof: release.migration.source_proof.digest,
        source_checkpoint: seal.checkpoint_digest,
        source_schema: OBSERVE_SCHEMA,
        method: release.migration.method.clone(),
        output_schema: OBSERVE_SCHEMA,
        output: Object::of(&output),
        source_report_position: 0,
    };
    let mut probe_target = target();
    probe_target.instance = [44; 16];
    probe_target.generation = 1;
    probe_target.command = None;
    probe_target.enabled = false;
    let probe = verified
        .for_probe(&release, probe_target, &a, &output)
        .unwrap();
    let probe_output = execute(&ObserveFactory, &probe)
        .unwrap()
        .checkpoint()
        .to_vec();
    let mut b = a.clone();
    b.kind = Kind::RollbackProbe {
        parent_receipt: a.digest().unwrap(),
    };
    b.process.instance = [44; 16];
    b.process.generation = 1;
    b.process.pid = 101;
    b.binary_digest = release.rollback.files[0].object.digest;
    b.source_proof = a.digest().unwrap();
    b.source_checkpoint = a.output.digest;
    b.output = Object::of(&probe_output);
    let a_raw = a.bytes().unwrap();
    let b_raw = b.bytes().unwrap();
    let c = Completion {
        schema: 1,
        operation: op,
        roots: bundle.roots(),
        member: release.target.member.clone(),
        logical_strategy: 8,
        restored: Object::of(&a_raw),
        rollback: Object::of(&b_raw),
    };
    let c_raw = c.bytes().unwrap();
    let root = tickwire_recovery::digest(&c_raw);
    let mut objects = std::collections::BTreeMap::new();
    for raw in bundle.object_bytes().unwrap().into_iter().chain([
        c_raw,
        a_raw,
        b_raw,
        output,
        probe_output,
        seal.bytes().unwrap(),
        checkpoint,
        inputs,
        account,
        dep,
    ]) {
        objects.insert(tickwire_recovery::digest(&raw), raw);
    }
    let check = |objects: &std::collections::BTreeMap<_, Vec<u8>>| {
        tickwire_release::closure::verify(root, |h| {
            objects
                .get(&h)
                .cloned()
                .ok_or(tickwire_release::Error::Missing)
        })
    };
    let done = check(&objects).unwrap();
    assert_eq!(done.restored, a);
    assert_eq!(done.rollback, b);
    let mut missing = objects.clone();
    missing.remove(&seal.account_input_digest);
    assert!(check(&missing).is_err());
    let mut corrupt = objects.clone();
    corrupt.get_mut(&seal.input_digest).unwrap().push(0);
    assert!(check(&corrupt).is_err());
    let mut wrong = c.clone();
    wrong.operation.operation_revision += 1;
    assert!(
        wrong
            .verify(&release, &a.bytes().unwrap(), &b.bytes().unwrap())
            .is_err()
    );
}
