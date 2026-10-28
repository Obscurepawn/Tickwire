use super::{Fixture, approved, fixture::write, inputs, worker};
use tickwire_app::strategy::{AppStrategy, MultiStrategy};
use tickwire_ipc::{ResourceHeader, Wire};
use tickwire_market_source::{
    protocol::{Instrument, wire_bytes},
    transition::{Approval, Commit},
};
use tickwire_runtime::node::{Strategy, StrategySwitch};

fn approval(f: &Fixture) -> Approval {
    let before = f.bootstrap.external_market.as_ref().unwrap().clone();
    let mut after = before.clone();
    let mut bank = ResourceHeader::decode(&before.bank_header).unwrap();
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
    after.instruments.push(Instrument {
        id: u64::MAX,
        version: 1,
        required_states: 1,
    });
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
    approval
        .validate(&approval.previous, now)
        .expect("valid expansion, not an unrelated malformed request");
    approval
}
fn commit(f: &Fixture, approval: &Approval) -> Commit {
    let previous = f.checkpoint().unwrap().runtime.markets[0].sequence;
    let now = f.market.now();
    let mut config = approval.next_template.clone();
    let mut bank = ResourceHeader::decode(&config.bank_header).unwrap();
    bank.created_mono_ns = now.nanos();
    bank.cutover_seq = previous;
    bank.first_seq = previous + 1;
    bank.descriptor_hash = tickwire_recovery::digest(&wire_bytes(&bank).unwrap());
    config.bank_header = wire_bytes(&bank).unwrap();
    let commit = Commit {
        schema: 1,
        operation_id: approval.operation_id,
        approval_digest: approval.approval_digest,
        revision: approval.revision,
        config,
    };
    commit
        .validate(approval, previous, now)
        .expect("valid final cut, independently refused by scope gate");
    commit
}
#[test]
#[ignore = "requires fixed release TICKWIRE_INPUTS_WORKER; real owner mutation gates"]
fn scope_proof_rejects_arm_and_direct_commit_before_host_dispatch() {
    let mut f = Fixture::new(&worker(), 1, 100);
    approved::bind(&mut f, false, approved::Extra::None);
    let mut owner = approved::build(&f).unwrap();
    f.seed(&mut *owner);
    approved::prove(&f, &mut *owner);
    let challenge = owner
        .projection_inputs(f.target())
        .unwrap()
        .observation()
        .challenge();
    let mut consumers = vec![];
    owner.market_consumers(&mut consumers);
    assert_eq!(consumers.len(), 1);
    let consumer = consumers[0];
    let a = approval(&f);
    let c = commit(&f, &a);
    // Direct Commit must not depend on a previous rejected Arm to be safe.
    assert!(
        owner
            .commit_market(consumer, &c)
            .unwrap_err()
            .to_string()
            .contains("input scope")
    );
    assert!(
        owner
            .arm_market(consumer, &a)
            .unwrap_err()
            .to_string()
            .contains("input scope")
    );
    assert!(owner.market_transition(consumer).is_none());
    assert!(owner.market_rejection(consumer).is_none());
    assert_eq!(
        owner
            .projection_inputs(f.target())
            .unwrap()
            .observation()
            .challenge(),
        challenge
    );
    let seq = f.market.publish(100);
    f.wait_consumed(&mut *owner, seq);
    assert!(!owner.failed());
    assert_eq!(
        f.next_ready(&mut *owner).unwrap().command,
        f.assert_command(100)
    );
    println!(
        "{}",
        serde_json::json!({"case":"input_mutations_rejected","pid":consumer.pid,"operation_id":a.operation_id,"valid_arm":true,"valid_commit":true,"arm_dispatched":false,"commit_dispatched":false,"continued_market_sequence":seq})
    );
}

fn unscoped_shadow(f: &mut Fixture) {
    let mut local = f.bootstrap.market.local.clone();
    local.instance = [22; 16];
    local.generation = 2;
    for endpoint in [
        &mut f.bootstrap.market,
        &mut f.bootstrap.intent,
        &mut f.bootstrap.report,
    ] {
        endpoint.local = local.clone();
    }
    f.bootstrap.external_market.as_mut().unwrap().endpoint.local = local.clone();
    for raw in [
        &mut f.bootstrap.intent_header,
        &mut f.bootstrap.report_header,
    ] {
        let mut bank = ResourceHeader::decode(raw).unwrap();
        if bank.resource_kind == 2 {
            bank.producer_instance = local.instance;
            bank.producer_generation = local.generation;
        } else {
            bank.consumer_instance = local.instance;
        }
        bank.descriptor_hash = [0; 32];
        *raw = wire_bytes(&bank).unwrap();
    }
    #[derive(serde::Serialize, serde::Deserialize)]
    struct Parameters {
        schema: u16,
        enabled: bool,
        minimum_observations: u64,
        minimum_bid_ticks: i64,
        command: Vec<u8>,
    }
    let mut parameters: Parameters = serde_json::from_slice(&f.params).unwrap();
    let command = inputs::command(&f.bootstrap, &f.market.instrument, f.market.now().nanos());
    parameters.command = command.bytes().to_vec();
    f.params = serde_json::to_vec(&parameters).unwrap();
    f.bootstrap.factory.as_mut().unwrap().configuration = tickwire_release::Object::of(&f.params);
    write(&f.root.join("parameters.json"), &f.params);
    write(
        &f.root.join("bootstrap.json"),
        &serde_json::to_vec(&f.bootstrap).unwrap(),
    );
    approved::bind(f, false, approved::Extra::None);
    f.host["projection_inputs"] = serde_json::Value::Null;
    f.host["shadow"] = true.into();
}
#[test]
#[ignore = "requires fixed release TICKWIRE_INPUTS_WORKER; actual two-worker Multi routing"]
fn multi_cannot_activate_unscoped_shadow_past_scoped_active() {
    let mut active = Fixture::new(&worker(), 1, 100);
    approved::bind(&mut active, false, approved::Extra::None);
    let mut old = approved::build(&active).unwrap();
    active.seed(&mut *old);
    let mut shadow = Fixture::new(&worker(), 1, 100);
    unscoped_shadow(&mut shadow);
    let mut next = approved::build(&shadow).unwrap();
    shadow.seed(&mut *next);
    let old_pid = old
        .observation_process(active.target())
        .unwrap()
        .process()
        .pid;
    let new_pid = next
        .observation_process(shadow.target())
        .unwrap()
        .process()
        .pid;
    assert_ne!(old_pid, new_pid);
    let mut multi = MultiStrategy::new_modes(
        vec![(1, false, old), (1, true, next)],
        approved::policy(&active),
    )
    .unwrap();
    approved::prove(&active, &mut multi);
    assert!(multi.projection_inputs(shadow.target()).is_none());
    let switch = StrategySwitch {
        logical_strategy_id: 1,
        old_instance: active.target().instance,
        old_generation: 1,
        new_instance: shadow.target().instance,
        new_generation: 2,
    };
    assert!(!multi.handover_quiesce(&switch));
    assert!(!multi.handover_activate(&switch));
    // Existing Multi semantics may shadow/quiesce both members on a failed
    // attempt; this asserts fail-closed delivery, not a side-effect-free call.
    assert_eq!(
        multi
            .observation_process(active.target())
            .unwrap()
            .process()
            .pid,
        old_pid
    );
    assert_eq!(
        multi
            .observation_process(shadow.target())
            .unwrap()
            .process()
            .pid,
        new_pid
    );
    let seq = shadow.market.publish(100);
    shadow.wait_durable(|c| c.runtime.markets[0].sequence >= seq);
    for _ in 0..3 {
        assert!(active.next_ready(&mut multi).is_none());
    }
    assert!(!multi.failed());
    assert!(multi.projection_inputs(active.target()).is_some());
    println!(
        "{}",
        serde_json::json!({"case":"input_multi_handover_closed","old_pid":old_pid,"new_pid":new_pid,"old_instance":switch.old_instance,"new_instance":switch.new_instance,"quiesced":false,"activated":false,"delivered":false,"members_may_be_shadowed":true})
    );
}
