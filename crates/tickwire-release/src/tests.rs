use super::*;
mod startup;
fn fixture() -> (Operation, ReleaseManifest, MigrationSet, RollbackPlan) {
    let member = Member {
        risk_domain: [1; 16],
        exec_instance: [2; 16],
        exec_epoch: 3,
        old_instance: [4; 16],
        old_generation: 5,
        new_instance: [6; 16],
        new_generation: 7,
        clock_boot_id: [8; 16],
        clock_generation: 9,
        valid_until_ns: 100,
    };
    let op = Operation {
        schema: 1,
        operation_id: [10; 16],
        operation_revision: 11,
        logical_strategy_id: 12,
        members: vec![member.clone()],
        artifacts_digest: [13; 32],
        checkpoint_digest: [14; 32],
        rollback_digest: [15; 32],
        clock_boot_id: [8; 16],
        clock_generation: 9,
        valid_until_ns: 100,
    };
    let b = Binding {
        operation_id: op.operation_id,
        revision: op.operation_revision,
        logical_strategy: op.logical_strategy_id,
        members_digest: op.members_digest().unwrap(),
    };
    let object = Object::of(b"state");
    let files = vec![
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
            bytes: 5,
        },
    })
    .collect::<Vec<_>>();
    let method = Method::Restore { schema: 1 };
    let caps = Capabilities {
        state_schema: 1,
        input_schema: 1,
        output_schema: 1,
        products: vec![Product::Spot],
        position_modes: vec![PositionMode::Spot],
        orders: vec![OrderKind::Limit],
        methods: vec![method.clone()],
    };
    (
        op,
        ReleaseManifest {
            schema: 1,
            binding: b.clone(),
            targets: vec![Target {
                member: member.clone(),
                version: [20; 32],
                files: files.clone(),
                capabilities: caps.clone(),
            }],
        },
        MigrationSet {
            schema: 1,
            binding: b.clone(),
            members: vec![Migration {
                member: member.clone(),
                source_checkpoint: object,
                source_schema: 1,
                source_proof: object,
                inputs: Some(InputPrefix {
                    object: Object {
                        digest: [23; 32],
                        bytes: 100,
                    },
                    stream: [1; 16],
                    first_sequence: 1,
                    previous_hash: [0; 32],
                    last_sequence: 1,
                    last_hash: [21; 32],
                }),
                method: method.clone(),
            }],
        },
        RollbackPlan {
            schema: 1,
            binding: b,
            members: vec![Rollback {
                member,
                version: [22; 32],
                files,
                capabilities: caps,
                method,
                probe_input: ProbeInput::TargetRestoreOutput,
            }],
        },
    )
}
fn bundle(
    op: &mut Operation,
    m: &ReleaseManifest,
    s: &MigrationSet,
    r: &RollbackPlan,
) -> ReleaseBundle {
    let b = ReleaseBundle::decode(
        &encode(m).unwrap(),
        &encode(s).unwrap(),
        &encode(r).unwrap(),
    )
    .unwrap();
    [
        op.artifacts_digest,
        op.checkpoint_digest,
        op.rollback_digest,
    ] = b.roots();
    b
}
#[test]
fn exact_operation_and_every_member_field_are_bound() {
    let (mut op, m, s, r) = fixture();
    let b = bundle(&mut op, &m, &s, &r);
    assert!(b.verify_operation(&op, &op.members[0]).is_ok());
    let mut wrong = op.clone();
    wrong.operation_revision += 1;
    assert!(matches!(
        b.verify_operation(&wrong, &wrong.members[0]),
        Err(Error::Identity)
    ));
    let mut member = op.members[0].clone();
    member.exec_epoch += 1;
    assert!(matches!(
        b.verify_operation(&op, &member),
        Err(Error::Identity)
    ));
}
#[test]
fn formatted_json_is_not_a_canonical_release() {
    let (_, m, s, r) = fixture();
    let mut b = encode(&m).unwrap();
    b.push(b' ');
    assert!(matches!(
        ReleaseBundle::decode(&b, &encode(&s).unwrap(), &encode(&r).unwrap()),
        Err(Error::Encoding)
    ));
}
#[test]
fn capability_or_missing_exact_input_cannot_be_declared_away() {
    let (mut op, m, mut s, r) = fixture();
    let b = bundle(&mut op, &m, &s, &r);
    let p = b.verify_operation(&op, &op.members[0]).unwrap();
    let required = Requirements {
        products: vec![Product::Spot],
        position_modes: vec![PositionMode::Spot],
        orders: vec![OrderKind::Market],
    };
    assert_eq!(
        p.check_capabilities(&m.targets[0].capabilities, &required),
        Err(Error::Unsupported)
    );
    s.members[0].inputs = None;
    let b = bundle(&mut op, &m, &s, &r);
    assert!(matches!(
        b.verify_operation(&op, &op.members[0]),
        Err(Error::Missing)
    ));
}

#[test]
fn every_field_of_the_member_and_every_root_is_checked() {
    let (mut op, m, s, r) = fixture();
    let b = bundle(&mut op, &m, &s, &r);
    for field in 0..10 {
        let mut wrong = op.members[0].clone();
        match field {
            0 => wrong.risk_domain[0] ^= 1,
            1 => wrong.exec_instance[0] ^= 1,
            2 => wrong.exec_epoch += 1,
            3 => wrong.old_instance[0] ^= 1,
            4 => wrong.old_generation += 1,
            5 => wrong.new_instance[0] ^= 1,
            6 => wrong.new_generation += 1,
            7 => wrong.clock_boot_id[0] ^= 1,
            8 => wrong.clock_generation += 1,
            _ => wrong.valid_until_ns += 1,
        }
        assert!(matches!(
            b.verify_operation(&op, &wrong),
            Err(Error::Identity)
        ));
        let mut changed = m.clone();
        changed.targets[0].member = wrong;
        let altered = bundle(&mut op, &changed, &s, &r);
        assert!(matches!(
            altered.verify_operation(&op, &op.members[0]),
            Err(Error::Identity)
        ));
    }
    let b = bundle(&mut op, &m, &s, &r);
    for field in 0..3 {
        let mut wrong = op.clone();
        match field {
            0 => wrong.artifacts_digest[0] ^= 1,
            1 => wrong.checkpoint_digest[0] ^= 1,
            _ => wrong.rollback_digest[0] ^= 1,
        }
        assert!(matches!(
            b.verify_operation(&wrong, &wrong.members[0]),
            Err(Error::Identity)
        ));
    }
}

#[test]
fn missing_files_duplicates_oversize_and_bad_method_fail_preflight() {
    for case in 0..7 {
        let (mut op, mut m, mut s, mut r) = fixture();
        match case {
            0 => {
                m.targets[0].files.remove(2);
            }
            1 => m.targets[0].files[1].object = m.targets[0].files[0].object,
            2 => m.targets[0].files[0].object.bytes = MAX_BINARY + 1,
            3 => s.members[0].source_schema = 2,
            4 => {
                r.members[0].method = Method::Migrate {
                    source_schema: 9,
                    target_schema: 1,
                    method_id: 1,
                }
            }
            5 => {
                m.targets[0].capabilities.methods.clear();
            }
            _ => {
                r.members.clear();
            }
        }
        let b = bundle(&mut op, &m, &s, &r);
        assert!(
            b.verify_operation(&op, &op.members[0]).is_err(),
            "case {case}"
        );
    }
}

#[test]
fn rebuild_requires_a_precise_approved_contract_and_compiled_method() {
    let (mut op, mut m, mut s, r) = fixture();
    let method = Method::Rebuild {
        target_schema: 1,
        contract: [31; 32],
    };
    s.members[0].method = method.clone();
    s.members[0].inputs = None;
    m.targets[0].capabilities.methods.push(method);
    let b = bundle(&mut op, &m, &s, &r);
    let verified = b.verify_operation(&op, &op.members[0]).unwrap();
    let required = Requirements {
        products: vec![Product::Spot],
        position_modes: vec![PositionMode::Spot],
        orders: vec![],
    };
    assert_eq!(
        verified.check_capabilities(&m.targets[0].capabilities, &required),
        Ok(())
    );
    let mut compiled = m.targets[0].capabilities.clone();
    compiled.methods[1] = Method::Rebuild {
        target_schema: 1,
        contract: [32; 32],
    };
    assert_eq!(
        verified.check_capabilities(&compiled, &required),
        Err(Error::Unsupported)
    );
}

#[test]
fn input_prefix_and_object_checks_read_actual_bytes() {
    let encoded = tickwire_recovery::encode_inputs(
        [1; 16],
        1,
        [0; 32],
        &[tickwire_recovery::InputRecord {
            kind: 4,
            payload: b"actual",
        }],
    )
    .unwrap();
    let b = encoded.boundary;
    let mut p = InputPrefix {
        object: Object::of(&encoded.bytes),
        stream: b.stream,
        first_sequence: b.first_sequence,
        previous_hash: b.previous_hash,
        last_sequence: b.last_sequence,
        last_hash: b.last_hash,
    };
    assert!(p.verify(encoded.bytes.clone()).is_ok());
    let mut wrong = encoded.bytes.clone();
    *wrong.last_mut().unwrap() ^= 1;
    assert!(p.verify(wrong).is_err());
    p.last_hash[0] ^= 1;
    assert!(p.verify(encoded.bytes).is_err());
    assert_eq!(Object::of(b"x").verify(b"y", 64), Err(Error::Identity));
}

#[test]
fn restore_and_probe_receipts_bind_actual_parent_output_and_fresh_process() {
    use crate::receipt::{Kind, Process, Receipt};
    let (mut op, m, s, r) = fixture();
    let b = bundle(&mut op, &m, &s, &r);
    let p = b.verify_operation(&op, &op.members[0]).unwrap();
    let restored = Receipt {
        schema: 1,
        kind: Kind::Restore,
        roots: b.roots(),
        member: op.members[0].clone(),
        logical_strategy: op.logical_strategy_id,
        process: Process {
            instance: op.members[0].new_instance,
            generation: op.members[0].new_generation,
            pid: 100,
            uid: 501,
            gid: 20,
        },
        binary_digest: m.targets[0].files[0].object.digest,
        effective_bootstrap_digest: [31; 32],
        source_proof: s.members[0].source_proof.digest,
        source_checkpoint: s.members[0].source_checkpoint.digest,
        source_schema: 1,
        method: Method::Restore { schema: 1 },
        output_schema: 1,
        output: Object::of(b"actual restored bytes"),
        source_report_position: 7,
    };
    assert!(restored.verify(&p, None).is_ok());
    assert_eq!(
        Receipt::decode(&restored.bytes().unwrap()).unwrap(),
        restored
    );
    let mut probe = restored.clone();
    probe.kind = Kind::RollbackProbe {
        parent_receipt: restored.digest().unwrap(),
    };
    probe.process.instance = [33; 16];
    probe.process.pid = 101;
    probe.source_proof = restored.digest().unwrap();
    probe.source_checkpoint = restored.output.digest;
    probe.output = Object::of(b"probe callback output");
    assert!(probe.verify(&p, Some(&restored)).is_ok());
    let mut bad = probe.clone();
    bad.source_checkpoint = s.members[0].source_checkpoint.digest;
    assert!(bad.verify(&p, Some(&restored)).is_err());
    let mut bad = probe.clone();
    bad.process.instance = op.members[0].old_instance;
    assert!(bad.verify(&p, Some(&restored)).is_err());
    let mut changed = restored.clone();
    changed.output = Object::of(b"other output");
    assert!(probe.verify(&p, Some(&changed)).is_err());
    assert!(probe.verify(&p, None).is_err());
    let mut bad = restored.clone();
    bad.roots[1] = [99; 32];
    assert!(bad.verify(&p, None).is_err());
}
