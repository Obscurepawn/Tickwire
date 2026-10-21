use super::*;
use tickwire_release::rebuild::rollback;
use tickwire_release::{
    Artifact, Binding, Capabilities, Method, OrderKind, PositionMode, Product, Role, Target,
};

#[test]
fn exact_old_rollback_requires_the_real_complete_retained_cut() {
    let (mut op, mut member, completion, raw) = crate::replay::source_tests::fixture();
    let mut objects = raw
        .into_iter()
        .map(|b| (Object::of(&b).digest, b))
        .collect::<std::collections::BTreeMap<_, _>>();
    let seal = tickwire_recovery::HandoverSeal::decode(&objects[&completion.current_proof.digest])
        .unwrap();
    let retained =
        tickwire_release::replay::retained::decode(&objects[&seal.input_digest]).unwrap();
    let anchor = retained.anchor().clone();
    let current = retained.current().clone();
    let tail = retained.tail_from(anchor.checkpoint()).unwrap();
    let saved: serde_json::Value = serde_json::from_slice(&current.business).unwrap();
    let baseline = objects[&seal.dependencies[0].1].clone();
    let mut plan_inputs = Inputs {
        schema: 1,
        clock: current.source.clock.clone(),
        start_ns: current.runtime.now_ns,
        config: VersionedBytes {
            revision: 1,
            bytes: b"target parameters".to_vec(),
        },
        models: vec![],
        markets: saved["markets"]
            .as_array()
            .unwrap()
            .iter()
            .map(|m| MarketSeed {
                resource: serde_json::from_value(m["resource"].clone()).unwrap(),
                checkpoint: serde_json::from_value(m["view"].clone()).unwrap(),
                maximum_levels: m["maximum_levels"].as_u64().unwrap() as usize,
            })
            .collect(),
        accounts: vec![AccountPrefix {
            baseline,
            records: vec![],
        }],
        warm: vec![],
    };
    op.schema = 3;
    member.valid_until_ns = 10_000_000_000;
    op.members = vec![member.clone()];
    op.valid_until_ns = 10_000_000_000;
    let binding = Binding {
        operation_id: op.operation_id,
        revision: op.operation_revision,
        logical_strategy: op.logical_strategy_id,
        members_digest: op.members_digest().unwrap(),
    };
    let contract = Contract {
        schema: 1,
        factory: "target_rebuild".into(),
        binary: [81; 32],
        state_schema: 4,
        config: Object::of(&plan_inputs.config.bytes),
        models: vec![],
        ordering: Ordering::ObservedRebuildWindow,
        markets: vec![MarketRequirement {
            stream: current.runtime.markets[0].stream.clone(),
            instrument: 1001,
            category: 1,
            minimum_observations: 1,
            known_bid_prices: vec![],
            known_ask_prices: vec![],
        }],
        actions: vec![],
        minimum_span_ns: 0,
        maximum_span_ns: 100,
        maximum_age_ns: 1_000_000_000,
        maximum_rows: 10,
        maximum_bytes: MAX_BYTES as u64,
        rng: Rng::Disabled,
    };
    let files = vec![
        Artifact {
            role: Role::Binary,
            object: Object {
                digest: contract.binary,
                bytes: 100,
            },
        },
        Artifact {
            role: Role::HostConfig,
            object: Object::of(b"host"),
        },
        Artifact {
            role: Role::Bootstrap,
            object: Object::of(b"bootstrap"),
        },
        Artifact {
            role: Role::StrategyConfig,
            object: contract.config,
        },
    ];
    let caps = Capabilities {
        state_schema: 4,
        input_schema: 2,
        output_schema: 1,
        products: vec![Product::Spot],
        position_modes: vec![PositionMode::Spot],
        orders: vec![OrderKind::Limit],
        methods: vec![Method::Rebuild {
            target_schema: 4,
            contract: Object::of(&contract.bytes().unwrap()).digest,
        }],
    };
    let manifest = ReleaseManifest {
        schema: 1,
        binding: binding.clone(),
        targets: vec![Target {
            member: member.clone(),
            version: [82; 32],
            files: files.clone(),
            capabilities: caps.clone(),
        }],
    };
    let end = Frontier {
        now_ns: current.runtime.now_ns,
        rows: 0,
        markets: current.runtime.markets.clone(),
        accounts: current.runtime.accounts.clone(),
        external: current.runtime.external.clone(),
    };
    let mut target = current.source.clone();
    target.instance = member.new_instance;
    target.generation = member.new_generation;
    target.input_stream = [83; 16];
    target.binary_digest = contract.binary;
    target.state_schema = contract.state_schema;
    let forward = Plans {
        schema: 1,
        binding: binding.clone(),
        members: vec![Plan {
            schema: 1,
            binding: binding.clone(),
            member: member.clone(),
            target,
            contract: Object::of(&contract.bytes().unwrap()),
            inputs: Object::of(&plan_inputs.bytes().unwrap()),
            end,
        }],
    };
    op.artifacts_digest =
        Object::of(&tickwire_release::replay::canonical(&manifest).unwrap()).digest;
    op.checkpoint_digest = Object::of(&forward.bytes().unwrap()).digest;
    let mut old_files = files;
    old_files[0].object.digest = current.source.binary_digest;
    old_files[3].object = Object::of(&current.runtime.config.bytes);
    let mut old_caps = caps;
    old_caps.state_schema = current.source.state_schema;
    old_caps.methods = vec![Method::Restore {
        schema: current.source.state_schema,
    }];
    let rollback = rollback::Plans {
        schema: 1,
        binding,
        members: vec![rollback::MemberPlan {
            member: member.clone(),
            version: [84; 32],
            factory: "fixed_demo_v1".into(),
            files: old_files,
            capabilities: old_caps,
            probe: rollback::Probe {
                instance: [85; 16],
                generation: 1,
                input_stream: [86; 16],
            },
            recovery: rollback::Recovery::ExactOldRestore {
                source: current.source.clone(),
                anchor: Object::of(&anchor.checkpoint().bytes().unwrap()),
                tail: Object::of(&tail),
                checkpoint: Object::of(&current.bytes().unwrap()),
                source_proof: completion.current_proof,
            },
        }],
    };
    op.rollback_digest = Object::of(&rollback.bytes().unwrap()).digest;
    for b in [
        anchor.checkpoint().bytes().unwrap(),
        tail,
        current.bytes().unwrap(),
    ] {
        objects.insert(Object::of(&b).digest, b);
    }
    let verify = |inputs: &Inputs, map: &std::collections::BTreeMap<_, _>| {
        rollback.verify(
            &op,
            &member,
            rollback::Forward {
                plans: &forward,
                contract: &contract,
                inputs,
                manifest: &manifest,
            },
            |h| map.get(&h).cloned().ok_or(tickwire_release::Error::Missing),
        )
    };
    let verified = verify(&plan_inputs, &objects);
    assert!(
        verified.is_ok(),
        "complete actual old cut must be accepted: {:?}",
        verified.err()
    );
    let verified = verify(&plan_inputs, &objects).unwrap();
    struct Factory {
        source: Source,
        restores: std::cell::Cell<usize>,
        fail_at: Option<usize>,
    }
    impl SourceReplayFactory for Factory {
        type Running = application::Application<native::Native>;
        fn binary_digest(&self) -> Hash {
            self.source.binary_digest
        }
        fn state_schema(&self) -> u64 {
            self.source.state_schema
        }
        fn restore_source(&self, anchor: &VerifiedAnchor) -> Result<Self::Running> {
            self.restores.set(self.restores.get() + 1);
            if self.fail_at == Some(self.restores.get()) {
                return Err(Error::Callback);
            }
            application::ApplicationFactory {
                binary_digest: self.source.binary_digest,
                business: native::Factory {
                    schema: self.source.state_schema,
                },
            }
            .restore_source(anchor)
        }
    }
    impl super::rollback::ExactFactory for Factory {
        fn factory_id(&self) -> &str {
            "fixed_demo_v1"
        }
        fn consumed_actions(&self, bytes: &[u8]) -> Result<Vec<[u8; 16]>> {
            // This fixture retains its pending action and has no actual Admit.
            if bytes.get(..8) != Some(b"TWFD0001") || bytes.get(8) != Some(&1) {
                return Err(Error::StateMismatch);
            }
            Ok(vec![])
        }
    }
    let factory = Factory {
        source: current.source.clone(),
        restores: Default::default(),
        fail_at: None,
    };
    let computed = super::rollback::prepare_exact(&factory, &verified);
    assert!(
        computed.is_ok(),
        "old factory must actually replay then restore C: {:?}",
        computed.as_ref().err()
    );
    let computed = computed.unwrap();
    assert_eq!(computed.checkpoint(), &current);
    assert_eq!(
        factory.restores.get(),
        2,
        "each computation restores S and then C"
    );
    let failed_restore = Factory {
        source: current.source.clone(),
        restores: Default::default(),
        fail_at: Some(2),
    };
    assert!(matches!(
        super::rollback::prepare_exact(&failed_restore, &verified),
        Err(Error::Callback)
    ));
    assert_eq!(failed_restore.restores.get(), 2);
    let mut wrong_binary = Factory {
        source: current.source.clone(),
        restores: Default::default(),
        fail_at: None,
    };
    wrong_binary.source.binary_digest[0] ^= 1;
    assert!(matches!(
        super::rollback::prepare_exact(&wrong_binary, &verified),
        Err(Error::Identity)
    ));
    assert_eq!(wrong_binary.restores.get(), 0);
    let mut different_view = plan_inputs.clone();
    different_view.markets[0].maximum_levels += 1;
    assert!(matches!(
        application::verify_rebuild_views(&current.business, &different_view),
        Err(Error::Identity)
    ));
    let mut missing = objects.clone();
    missing.remove(&seal.account_input_digest);
    assert!(verify(&plan_inputs, &missing).is_err());
    plan_inputs.accounts[0].records.push(vec![0]);
    assert!(verify(&plan_inputs, &objects).is_err());
}
