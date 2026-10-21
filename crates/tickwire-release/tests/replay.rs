use tickwire_release::{Object, replay::*};

fn source() -> Source {
    Source {
        logical_strategy: 8,
        instance: [1; 16],
        generation: 2,
        binary_digest: [3; 32],
        state_schema: 7,
        input_stream: [4; 16],
        clock: ClockDomain {
            boot_id: [5; 16],
            generation: 1,
        },
    }
}
fn anchor() -> VerifiedAnchor {
    let c = Checkpoint::new(
        source(),
        0,
        [0; 32],
        vec![8],
        RuntimeState {
            now_ns: 10,
            config: VersionedBytes {
                revision: 1,
                bytes: b"old config".to_vec(),
            },
            models: vec![],
            markets: vec![],
            accounts: vec![],
            external: vec![],
            timers: vec![Timer {
                id: 1,
                generation: 1,
                deadline_ns: Some(12),
            }],
            rng: Rng::Xoshiro256StarStarV1 {
                state: [1, 2, 3, 4],
                draws: 0,
            },
            effects: EffectCut {
                count: 0,
                digest: [0; 32],
            },
        },
    )
    .unwrap();
    verify_anchor(
        &c.bytes().unwrap(),
        &source(),
        Object::of(b"old config"),
        &[],
    )
    .unwrap()
}
fn rows() -> Vec<MergedInput> {
    vec![
        MergedInput {
            sequence: 1,
            now_ns: 12,
            input: Input::TimerFired {
                id: 1,
                generation: 1,
                deadline_ns: 12,
            },
            effects: vec![],
        },
        MergedInput {
            sequence: 2,
            now_ns: 12,
            input: Input::External {
                stream: StreamKey {
                    identity: [9; 32],
                    generation: 1,
                },
                sequence: 1,
                schema: 9,
                bytes: vec![4],
            },
            effects: vec![],
        },
    ]
}
fn end(a: &VerifiedAnchor, rows: &[MergedInput]) -> Cut {
    let mut c = a.checkpoint().cut.clone();
    for row in rows {
        c.chain = row_chain(c.chain, row).unwrap();
        c.sequence = row.sequence;
    }
    c
}
#[test]
fn exact_equal_time_order_is_retained() {
    let a = anchor();
    let r = rows();
    let tail = verify_tail(&a, &encode_tail(&a, &r).unwrap(), &end(&a, &r)).unwrap();
    assert_eq!(tail.rows(), r);
}
#[test]
fn equal_time_reordering_cannot_match_the_authenticated_cut() {
    let a = anchor();
    let r = rows();
    let mut changed = r.clone();
    changed.swap(0, 1);
    changed[0].sequence = 1;
    changed[1].sequence = 2;
    assert!(verify_tail(&a, &encode_tail(&a, &changed).unwrap(), &end(&a, &r)).is_err());
}
#[test]
fn missing_middle_cannot_be_relabelled_as_a_complete_tail() {
    let a = anchor();
    let r = rows();
    assert!(verify_tail(&a, &encode_tail(&a, &r[1..]).unwrap(), &end(&a, &r)).is_err());
}
#[test]
fn wrong_previous_chain_and_stream_are_rejected() {
    let a = anchor();
    let r = rows();
    let bytes = encode_tail(&a, &r).unwrap();
    for at in [8, 32] {
        let mut b = bytes.clone();
        b[at] ^= 1;
        assert!(verify_tail(&a, &b, &end(&a, &r)).is_err(), "offset {at}");
    }
}
#[test]
fn truncation_extra_bytes_unknown_kind_and_old_format_are_rejected() {
    let a = anchor();
    let r = rows();
    let bytes = encode_tail(&a, &r).unwrap();
    for len in [0, 7, 67, bytes.len() - 1] {
        assert!(verify_tail(&a, &bytes[..len], &end(&a, &r)).is_err());
    }
    let mut old = bytes.clone();
    old[..8].copy_from_slice(b"TWINP001");
    assert!(verify_tail(&a, &old, &end(&a, &r)).is_err());
    let mut extra = bytes.clone();
    extra.push(0);
    assert!(verify_tail(&a, &extra, &end(&a, &r)).is_err());
    let mut unknown = bytes;
    let at = unknown
        .windows(11)
        .position(|w| w == b"timer_fired")
        .unwrap();
    unknown[at] = b'X';
    assert!(verify_tail(&a, &unknown, &end(&a, &r)).is_err());
}
#[test]
fn local_time_cannot_regress_between_actual_callbacks() {
    let a = anchor();
    let mut r = rows();
    r[1].now_ns = 11;
    assert!(verify_tail(&a, &encode_tail(&a, &r).unwrap(), &end(&a, &r)).is_err());
}
#[test]
fn anchor_binds_actual_old_binary_config_timer_and_rng() {
    let a = anchor();
    let bytes = a.checkpoint().bytes().unwrap();
    let mut wrong = source();
    wrong.binary_digest = [10; 32];
    assert!(verify_anchor(&bytes, &wrong, Object::of(b"old config"), &[]).is_err());
    assert!(verify_anchor(&bytes, &source(), Object::of(b"new config"), &[]).is_err());
    for key in ["timers", "rng"] {
        let mut json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        json["runtime"].as_object_mut().unwrap().remove(key);
        assert!(Checkpoint::decode(&serde_json::to_vec(&json).unwrap()).is_err());
    }
    let mut changed = a.checkpoint().clone();
    changed.runtime.timers.clear();
    assert!(changed.bytes().is_err());
    let mut changed = a.checkpoint().clone();
    changed.runtime.rng = Rng::Disabled;
    assert!(changed.bytes().is_err());
}
#[test]
fn count_bounds_are_checked_before_rows_are_allocated() {
    let a = anchor();
    let mut bytes = encode_tail(&a, &[]).unwrap();
    bytes[64..68].copy_from_slice(&u32::MAX.to_be_bytes());
    assert!(verify_tail(&a, &bytes, &a.checkpoint().cut).is_err());
}
