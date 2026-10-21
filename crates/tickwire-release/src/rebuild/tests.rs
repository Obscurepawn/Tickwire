use super::*;
pub(super) fn fixture() -> (Operation, ReleaseManifest, Contract, Inputs, Plans) {
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
    let mut op = Operation {
        schema: 3,
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
    let binding = Binding {
        operation_id: op.operation_id,
        revision: op.operation_revision,
        logical_strategy: op.logical_strategy_id,
        members_digest: op.members_digest().unwrap(),
    };
    let contract = Contract {
        schema: 1,
        factory: "example".into(),
        binary: [21; 32],
        state_schema: 3,
        config: Object::of(b"config"),
        models: vec![],
        ordering: Ordering::ObservedRebuildWindow,
        markets: vec![MarketRequirement {
            stream: StreamKey {
                identity: [22; 32],
                generation: 1,
            },
            instrument: 1,
            category: 3,
            minimum_observations: 1,
            known_bid_prices: vec![10],
            known_ask_prices: vec![11],
        }],
        actions: vec![],
        minimum_span_ns: 0,
        maximum_span_ns: 100,
        maximum_age_ns: 100,
        maximum_rows: 10,
        maximum_bytes: replay::MAX_BYTES as u64,
        rng: Rng::Disabled,
    };
    let mut files = vec![
        crate::Artifact {
            role: Role::Binary,
            object: Object {
                digest: contract.binary,
                bytes: 100,
            },
        },
        crate::Artifact {
            role: Role::HostConfig,
            object: Object::of(b"host"),
        },
        crate::Artifact {
            role: Role::Bootstrap,
            object: Object::of(b"bootstrap"),
        },
        crate::Artifact {
            role: Role::StrategyConfig,
            object: contract.config,
        },
    ];
    files.sort_by(|a, b| a.role.cmp(&b.role));
    let manifest = ReleaseManifest {
        schema: 1,
        binding: binding.clone(),
        targets: vec![crate::Target {
            member: member.clone(),
            version: [23; 32],
            files,
            capabilities: crate::Capabilities {
                state_schema: 3,
                input_schema: 2,
                output_schema: 1,
                products: vec![crate::Product::Spot],
                position_modes: vec![crate::PositionMode::Spot],
                orders: vec![crate::OrderKind::Limit],
                methods: vec![Method::Rebuild {
                    target_schema: 3,
                    contract: Object::of(&contract.bytes().unwrap()).digest,
                }],
            },
        }],
    };
    let inputs = Inputs {
        schema: 1,
        clock: ClockDomain {
            boot_id: [8; 16],
            generation: 9,
        },
        start_ns: 1,
        config: VersionedBytes {
            revision: 1,
            bytes: b"config".to_vec(),
        },
        models: vec![],
        markets: vec![MarketSeed {
            resource: vec![1],
            checkpoint: vec![2],
            maximum_levels: 10,
        }],
        accounts: vec![AccountPrefix {
            baseline: vec![3],
            records: vec![],
        }],
        warm: vec![],
    };
    let plan = Plan {
        schema: 1,
        binding,
        member: member.clone(),
        target: Source {
            logical_strategy: 12,
            instance: [6; 16],
            generation: 7,
            binary_digest: contract.binary,
            state_schema: 3,
            input_stream: [24; 16],
            clock: inputs.clock.clone(),
        },
        contract: Object::of(&contract.bytes().unwrap()),
        inputs: Object::of(&inputs.bytes().unwrap()),
        end: Frontier {
            now_ns: 1,
            rows: 0,
            markets: vec![],
            accounts: vec![],
            external: vec![],
        },
    };
    let plan = Plans {
        schema: 1,
        binding: plan.binding.clone(),
        members: vec![plan],
    };
    op.artifacts_digest = Object::of(&crate::encode(&manifest).unwrap()).digest;
    op.checkpoint_digest = Object::of(&plan.bytes().unwrap()).digest;
    (op, manifest, contract, inputs, plan)
}
#[test]
fn explicit_rebuild_plan_binds_the_actual_contract_and_full_member() {
    let (op, manifest, contract, inputs, plan) = fixture();
    assert!(
        plan.verify(&op, &op.members[0], &contract, &inputs, &manifest)
            .is_ok()
    );
    let mut wrong = op.members[0].clone();
    wrong.exec_epoch += 1;
    assert!(
        plan.verify(&op, &wrong, &contract, &inputs, &manifest)
            .is_err()
    );
    let mut wrong = contract.clone();
    wrong.factory = "other".into();
    assert!(
        plan.verify(&op, &op.members[0], &wrong, &inputs, &manifest)
            .is_err()
    );
    let mut wrong = inputs.clone();
    wrong.config.bytes.push(9);
    assert!(
        plan.verify(&op, &op.members[0], &contract, &wrong, &manifest)
            .is_err()
    );
    let mut wrong = manifest.clone();
    wrong.targets[0].capabilities.methods.clear();
    assert!(
        plan.verify(&op, &op.members[0], &contract, &inputs, &wrong)
            .is_err()
    );
}
#[test]
fn old_process_order_is_not_a_declared_rebuild_contract() {
    let (mut op, mut manifest, mut contract, inputs, mut plan) = fixture();
    contract.ordering = Ordering::RequiresOldProcessOrder;
    plan.members[0].contract = Object::of(&contract.bytes().unwrap());
    manifest.targets[0].capabilities.methods = vec![Method::Rebuild {
        target_schema: 3,
        contract: plan.members[0].contract.digest,
    }];
    op.artifacts_digest = Object::of(&crate::encode(&manifest).unwrap()).digest;
    op.checkpoint_digest = Object::of(&plan.bytes().unwrap()).digest;
    assert_eq!(
        plan.verify(&op, &op.members[0], &contract, &inputs, &manifest),
        Err(Error::Unsupported)
    );
}
