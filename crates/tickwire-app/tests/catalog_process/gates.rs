use super::{Fixture, approved, fixture, market, worker};

pub fn lifecycle(f: &Fixture, owner: &mut dyn tickwire_app::strategy::AppStrategy) {
    use tickwire_ipc::Wire;
    use tickwire_market_source::{
        protocol::{Instrument, wire_bytes},
        transition::{Approval, Commit},
    };
    let before = f.bootstrap.external_market.as_ref().unwrap().clone();
    let mut after = before.clone();
    let mut bank = tickwire_ipc::ResourceHeader::decode(&after.bank_header).unwrap();
    bank.resource_id = [77; 16];
    bank.predecessor_ring_epoch = bank.ring_epoch;
    bank.ring_epoch += 1;
    bank.feed_generation += 1;
    bank.created_mono_ns = 0;
    bank.first_seq = 1;
    bank.cutover_seq = 0;
    bank.descriptor_hash = [0; 32];
    after.bank_header = wire_bytes(&bank).unwrap();
    after.endpoint.control_epoch += 1;
    after.endpoint.nonce = [78; 32];
    after.cursor_id = [79; 16];
    after.request_id = [80; 16];
    after.subscription_generation += 1;
    after.instrument_set_version += 1;
    after.instruments.insert(
        0,
        Instrument {
            id: 1,
            version: 1,
            required_states: 1,
        },
    );
    let now = f.market.now();
    let approval = Approval {
        schema: 1,
        operation_id: [81; 16],
        approval_digest: [82; 32],
        revision: 2,
        previous: before,
        next_template: after,
        deadline_mono_ns: now.nanos() + 10_000_000_000,
    };
    approval.validate(&approval.previous, now).unwrap();
    let previous = f.checkpoint().unwrap().runtime.markets[0].sequence;
    let mut config = approval.next_template.clone();
    bank.created_mono_ns = now.nanos();
    bank.cutover_seq = previous;
    bank.first_seq = previous + 1;
    bank.descriptor_hash = tickwire_recovery::digest(&wire_bytes(&bank).unwrap());
    config.bank_header = wire_bytes(&bank).unwrap();
    let commit = Commit {
        schema: 1,
        operation_id: approval.operation_id,
        approval_digest: approval.approval_digest,
        revision: 2,
        config,
    };
    commit.validate(&approval, previous, now).unwrap();
    let mut consumers = vec![];
    owner.market_consumers(&mut consumers);
    assert_eq!(consumers.len(), 1);
    assert!(
        owner
            .commit_market(consumers[0], &commit)
            .unwrap_err()
            .to_string()
            .contains("input scope")
    );
    assert!(
        owner
            .arm_market(consumers[0], &approval)
            .unwrap_err()
            .to_string()
            .contains("input scope")
    );
    assert!(owner.market_transition(consumers[0]).is_none());
    assert!(!owner.failed());
}

#[test]
#[ignore = "requires fixed release TICKWIRE_CATALOG_WORKER; rehashed global catalog refusal before child"]
fn rehashed_catalog_and_projection_cannot_override_actual_old_economics() {
    let market = market::Market::with_tick("0.01");
    let mut fixture = Fixture::new_with_market(&worker(), 1, 75, market);
    let mut body: serde_json::Value =
        serde_json::from_slice(&fixture.market.artifact.bytes).unwrap();
    body["instruments"][0]["price_tick"] = "0.02".into();
    let bytes = serde_json::to_vec(&body).unwrap();
    let changed = tickwire_instrument_catalog::Artifact {
        digest: tickwire_recovery::digest(&bytes),
        bytes,
    };
    assert_ne!(
        changed.catalog().unwrap().catalog_hash(),
        fixture.market.instrument.catalog_hash
    );
    fixture.bootstrap.instrument_catalog = Some(changed.clone());
    fixture::write(
        &fixture.root.join("bootstrap.json"),
        &serde_json::to_vec(&fixture.bootstrap).unwrap(),
    );
    approved::bind(&mut fixture, true, approved::Extra::None);
    // Even matching declared hashes cannot replace the actual old tick. Update
    // both policy and baseline identity so refusal reaches the economic fields.
    let new_catalog = changed.catalog().unwrap();
    let mut baseline_catalog = fixture.baseline.catalog().clone();
    baseline_catalog.entries[0].instrument.catalog_hash = new_catalog.catalog_hash();
    baseline_catalog.entries[0].instrument.catalog_version = new_catalog.catalog_version();
    fixture.baseline = tickwire_account_codec::CanonicalBaseline::new(
        fixture.baseline.snapshot().clone(),
        baseline_catalog,
    )
    .unwrap();
    fixture.host["projection_inputs"]["account"]["baseline"] =
        serde_json::to_value(tickwire_release::Object::of(fixture.baseline.bytes())).unwrap();
    let mut deployment = approved::deployment(&fixture);
    deployment.policy.versions.catalog_hash = new_catalog.catalog_hash();
    deployment.policy.versions.catalog_version = new_catalog.catalog_version();
    let result = deployment
        .assemble(fixture.baseline.clone())
        .and_then(|runtime| {
            tickwire_app::hosted::build(
                &serde_json::from_value(fixture.host.clone())?,
                &runtime,
                deployment.policy.build().unwrap(),
            )
        });
    let error = match result {
        Ok(_) => panic!("new approved bytes overrode actual baseline/feed economics"),
        Err(error) => error.to_string(),
    };
    assert_eq!(error, "instrument catalog runtime economics");
    fixture.assert_no_worker();
    fixture.market.save_evidence(&fixture.root);
    fixture::write(&fixture.root.join("baseline.raw"), fixture.baseline.bytes());
    println!(
        "{}",
        serde_json::json!({"case":"catalog_rehashed_economics_refused","error":error,"artifact":tickwire_release::Object::of(&changed.bytes),"actual_catalog_hash":fixture.market.instrument.catalog_hash,"changed_catalog_hash":changed.catalog().unwrap().catalog_hash(),"worker_spawned":false,"global_reference_rebound":true,"policy_and_baseline_hash_rebound":true,"root":fixture.root})
    );
}
