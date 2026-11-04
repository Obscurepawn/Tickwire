use super::{Fixture, approved, worker};
use tickwire_ipc::{ResourceHeader, Wire};
use tickwire_market_source::{
    protocol::{Instrument, wire_bytes},
    transition::{Approval, Commit},
};

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
