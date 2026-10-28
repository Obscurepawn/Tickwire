use super::*;
use crate::protocol::{Endpoint, Instrument, wire_bytes};
use crate::test_fixture as fixture;
use tickwire_control::ProcessIdentity;
use tickwire_ipc::Wire;
fn approval() -> Approval {
    let mut policy = fixture::policy(
        fixture::identity(4, 3),
        fixture::identity(3, 2),
        ProcessIdentity {
            pid: 42,
            uid: 1,
            gid: 1,
        },
        fixture::NOW,
    );
    policy.supported_features |= 64;
    let mut bank = fixture::header();
    bank.required_features |= 64;
    bank.descriptor_hash = [0; 32];
    bank.descriptor_hash = Sha256::digest(wire_bytes(&bank).unwrap()).into();
    let previous = ConnectConfig {
        endpoint: Endpoint::new("/tmp/qualified-md.sock".into(), &policy),
        bank_header: wire_bytes(&bank).unwrap(),
        catalog_hash: fixture::registry().catalog_hash,
        instrument_set_version: 1,
        instruments: vec![Instrument {
            id: 1001,
            version: 5,
            required_states: 1,
        }],
        cursor_id: [104; 16],
        subscription_generation: 1,
        request_id: [25; 16],
        maximum_levels: 32,
        startup_timeout_ms: 500,
        clock_generation: 1,
    };
    let mut next_template = previous.clone();
    bank.resource_id = [93; 16];
    bank.predecessor_ring_epoch = bank.ring_epoch;
    bank.ring_epoch += 1;
    bank.feed_generation += 1;
    bank.created_mono_ns = 0;
    bank.cutover_seq = 0;
    bank.first_seq = 1;
    bank.descriptor_hash = [0; 32];
    next_template.bank_header = wire_bytes(&bank).unwrap();
    next_template.cursor_id = [105; 16];
    next_template.request_id = [26; 16];
    next_template.subscription_generation += 1;
    next_template.instrument_set_version += 1;
    next_template.endpoint.control_epoch += 1;
    next_template.endpoint.nonce = [72; 32];
    next_template.instruments.push(Instrument {
        id: u64::MAX,
        version: 5,
        required_states: 1,
    });
    Approval {
        schema: 1,
        operation_id: [11; 16],
        approval_digest: [12; 32],
        revision: 2,
        previous,
        next_template,
        deadline_mono_ns: fixture::NOW + 1_000_000,
    }
}
fn now(a: &Approval) -> MonoTime {
    MonoTime::new(a.previous.domain().unwrap(), fixture::NOW)
}
fn commit(a: &Approval) -> Commit {
    let mut config = a.next_template.clone();
    let mut bank = ResourceHeader::decode(&config.bank_header).unwrap();
    bank.created_mono_ns = fixture::NOW;
    bank.cutover_seq = 6;
    bank.first_seq = 7;
    bank.descriptor_hash = Sha256::digest(wire_bytes(&bank).unwrap()).into();
    config.bank_header = wire_bytes(&bank).unwrap();
    Commit {
        schema: 1,
        operation_id: a.operation_id,
        approval_digest: a.approval_digest,
        revision: a.revision,
        config,
    }
}
#[test]
fn exact_approval_final_cut_and_receipt_bindings() {
    let a = approval();
    let n = now(&a);
    a.validate(&a.previous, n).unwrap();
    let b = Approval::decode(&serde_json::to_vec(&a).unwrap()).unwrap();
    assert_eq!(a.digest().unwrap(), b.digest().unwrap());
    assert_eq!(commit(&a).validate(&a, 6, n).unwrap().first_seq, 7);
    let armed = Receipt::armed(&a, 42, n).unwrap();
    a.validate_receipt(&armed, 42, Phase::Armed, n).unwrap();
    let q = Receipt::quiesced(&a, 42, 6, n).unwrap();
    a.validate_receipt(&q, 42, Phase::Quiesced, n).unwrap();
    assert!(a.validate_receipt(&q, 43, Phase::Quiesced, n).is_err());
    assert!(a.validate_receipt(&q, 42, Phase::Armed, n).is_err());
}
#[test]
fn approval_rejects_changed_identity_membership_clock_or_old_resource() {
    for case in 0..12 {
        let mut a = approval();
        let current = a.previous.clone();
        let n = now(&a);
        match case {
            0 => a.operation_id = [0; 16],
            1 => a.approval_digest = [0; 32],
            2 => a.previous.cursor_id = [9; 16],
            3 => a.next_template.endpoint.peer_pid += 1,
            4 => a.next_template.endpoint.local.generation += 1,
            5 => a.next_template.instruments[0].version += 1,
            6 => {
                a.next_template.instruments.pop();
            }
            7 => a.next_template.cursor_id = a.previous.cursor_id,
            8 => a.next_template.request_id = a.previous.request_id,
            9 => a.deadline_mono_ns = fixture::NOW,
            10 => a.next_template.maximum_levels += 1,
            11 => a.next_template.clock_generation += 1,
            _ => unreachable!(),
        }
        assert!(a.validate(&current, n).is_err(), "case {case}");
    }
}
#[test]
fn commit_rejects_unapproved_changes_bad_hash_or_cut_before_actual_cursor() {
    let a = approval();
    let n = now(&a);
    for case in 0..7 {
        let mut c = commit(&a);
        let mut b = ResourceHeader::decode(&c.config.bank_header).unwrap();
        match case {
            0 => c.operation_id = [9; 16],
            1 => c.approval_digest = [9; 32],
            2 => c.config.request_id = [9; 16],
            3 => b.topic_id += 1,
            4 => b.first_seq = 8,
            5 => b.descriptor_hash = [9; 32],
            6 => c.revision += 1,
            _ => unreachable!(),
        }
        c.config.bank_header = wire_bytes(&b).unwrap();
        assert!(c.validate(&a, 6, n).is_err(), "case {case}");
    }
    assert!(commit(&a).validate(&a, 7, n).is_err());
}

#[test]
fn original_startup_header_may_omit_derived_digest_but_not_lie_about_it() {
    let mut a = approval();
    let n = now(&a);
    let mut bank = ResourceHeader::decode(&a.previous.bank_header).unwrap();
    bank.descriptor_hash = [0; 32];
    a.previous.bank_header = wire_bytes(&bank).unwrap();
    a.validate(&a.previous, n).unwrap();
    bank.descriptor_hash = [42; 32];
    a.previous.bank_header = wire_bytes(&bank).unwrap();
    assert!(a.validate(&a.previous, n).is_err());
}

#[test]
fn unsupported_members_and_forged_receipt_fields_are_rejected_before_quiesce() {
    let a = approval();
    let n = now(&a);
    let mut bad = a.clone();
    bad.next_template.instruments[1].required_states = 8;
    assert!(bad.validate(&a.previous, n).is_err());
    for case in 0..8 {
        let mut r = Receipt::quiesced(&a, 42, 6, n).unwrap();
        match case {
            0 => r.operation_id = [99; 16],
            1 => r.approval_digest = [99; 32],
            2 => r.approval_hash = [99; 32],
            3 => r.revision += 1,
            4 => r.observed_mono_ns += 1,
            5 => r.next_descriptor_hash = Some([1; 32]),
            6 => r.previous_applied = None,
            7 => r.previous_applied = Some(1 << 63),
            _ => unreachable!(),
        }
        assert!(
            a.validate_receipt(&r, 42, Phase::Quiesced, n).is_err(),
            "case {case}"
        );
    }
}
