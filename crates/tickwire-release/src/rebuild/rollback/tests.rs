use super::*;

type Fixture = (
    Operation,
    ReleaseManifest,
    Contract,
    Inputs,
    super::super::Plans,
    Plans,
    BTreeMap<Hash, Vec<u8>>,
);
fn fixture() -> Fixture {
    let (mut op, manifest, contract, inputs, forward) = crate::rebuild::tests::fixture();
    let member = op.members[0].clone();
    let mut old = contract.clone();
    old.factory = "old_rebuild".into();
    old.binary = [31; 32];
    old.state_schema = 1;
    old.config = Object::of(b"old parameters");
    let mut old_inputs = inputs.clone();
    old_inputs.config.bytes = b"old parameters".to_vec();
    let mut files = manifest.targets[0].files.clone();
    files[0].object = Object {
        digest: old.binary,
        bytes: 100,
    };
    files[3].object = old.config;
    let mut capabilities = manifest.targets[0].capabilities.clone();
    capabilities.state_schema = old.state_schema;
    capabilities.methods = vec![Method::Rebuild {
        target_schema: old.state_schema,
        contract: Object::of(&old.bytes().unwrap()).digest,
    }];
    let plans = Plans {
        schema: 1,
        binding: forward.binding.clone(),
        members: vec![MemberPlan {
            member: member.clone(),
            version: [32; 32],
            factory: old.factory.clone(),
            files,
            capabilities,
            probe: Probe {
                instance: [33; 16],
                generation: 1,
                input_stream: [34; 16],
            },
            recovery: Recovery::ApprovedOldRebuild {
                contract: Object::of(&old.bytes().unwrap()),
                inputs: Object::of(&old_inputs.bytes().unwrap()),
                end: forward.members[0].end.clone(),
            },
        }],
    };
    op.rollback_digest = Object::of(&plans.bytes().unwrap()).digest;
    let objects = [old.bytes().unwrap(), old_inputs.bytes().unwrap()]
        .into_iter()
        .map(|b| (Object::of(&b).digest, b))
        .collect::<BTreeMap<_, _>>();
    (op, manifest, contract, inputs, forward, plans, objects)
}
fn verify(f: &Fixture) -> Result<Verified> {
    let (op, manifest, contract, inputs, forward, plans, objects) = f;
    plans.verify(
        op,
        &op.members[0],
        Forward {
            plans: forward,
            contract,
            inputs,
            manifest,
        },
        |h| objects.get(&h).cloned().ok_or(Error::Missing),
    )
}
#[test]
fn independently_approved_old_rebuild_uses_fixed_code_config_and_window() {
    let f = fixture();
    let verified = verify(&f);
    assert!(
        verified.is_ok(),
        "approved fixed old rebuild: {:?}",
        verified.err()
    );
}
#[test]
fn approved_rollback_rejects_rebound_code_config_member_cut_or_probe() {
    for mutation in 0..10 {
        let mut f = fixture();
        let p = &mut f.5.members[0];
        match mutation {
            0 => p.factory = "other_factory".into(),
            1 => p.files[0].object.digest[0] ^= 1,
            2 => p.files[3].object.digest[0] ^= 1,
            3 => p.capabilities.state_schema += 1,
            4 => p.member.exec_epoch += 1,
            5 => p.probe.instance = p.member.old_instance,
            6 => p.probe.instance = p.member.new_instance,
            7 => p.probe.input_stream = f.4.members[0].target.input_stream,
            8 => {
                let Recovery::ApprovedOldRebuild { end, .. } = &mut p.recovery else {
                    unreachable!()
                };
                end.now_ns += 1;
            }
            9 => {
                p.capabilities.methods = vec![Method::Restore {
                    schema: p.capabilities.state_schema,
                }]
            }
            _ => unreachable!(),
        }
        if let Ok(bytes) = f.5.bytes() {
            f.0.rollback_digest = Object::of(&bytes).digest;
            assert!(
                verify(&f).is_err(),
                "mutation {mutation} cannot create old recovery proof"
            );
        }
    }
}
#[test]
fn fixed_old_window_cannot_replace_account_prefix_or_preheating_input() {
    for mutation in 0..2 {
        let mut f = fixture();
        let Recovery::ApprovedOldRebuild { inputs, .. } = &mut f.5.members[0].recovery else {
            unreachable!()
        };
        let mut old = Inputs::decode(&f.6[&inputs.digest]).unwrap();
        match mutation {
            0 => old.accounts[0].baseline.push(0),
            1 => old.start_ns += 1,
            _ => unreachable!(),
        }
        let bytes = old.bytes().unwrap();
        *inputs = Object::of(&bytes);
        f.6.insert(inputs.digest, bytes);
        f.0.rollback_digest = Object::of(&f.5.bytes().unwrap()).digest;
        assert!(verify(&f).is_err());
    }
    let mut f = fixture();
    f.6.clear();
    assert!(matches!(verify(&f), Err(Error::Missing)));
}
#[test]
fn opaque_reader_keeps_recovery_budget_but_typed_lookup_rejects_large_cached_object() {
    let bytes = vec![39; 17 * 1024 * 1024];
    let object = Object::of(&bytes);
    let mut objects = Objects {
        read: |hash| {
            if hash == object.digest {
                Ok(bytes.clone())
            } else {
                Err(Error::Missing)
            }
        },
        values: BTreeMap::new(),
        bytes: 0,
    };
    assert_eq!(objects.by_hash(object.digest).unwrap(), bytes);
    assert!(matches!(objects.get(object), Err(Error::Bounds)));
}
#[test]
fn nested_old_restore_limit_is_checked_before_loading_any_object() {
    let (op, manifest, contract, inputs, forward, plans, _) = fixture();
    assert!(matches!(
        plans.verify_at(
            &op,
            &op.members[0],
            Forward {
                plans: &forward,
                contract: &contract,
                inputs: &inputs,
                manifest: &manifest
            },
            &mut |_| panic!("depth limit must precede reads"),
            16
        ),
        Err(Error::Bounds)
    ));
}
