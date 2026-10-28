use super::*;
use crate::plan_archive::content::tests::fixture;
const CLUSTER: Identity = [19; 16];
fn config() -> crate::Config {
    crate::Config {
        cluster: CLUSTER,
        objects: crate::ObjectLimits {
            max_objects: 4096,
            max_inflight: 64,
            max_payload_bytes: 512 * 1024 * 1024,
        },
    }
}

#[test]
fn validated_archive_records_once_and_exact_retry_keeps_the_first_position() {
    let f = fixture();
    let mut state = State::default();
    let index = f.head.raft_index() + 5;
    let term = f.head.raft_term() + 1;
    let first = state
        .apply(f.record.clone(), &f.objects, &f.head, index, term)
        .unwrap();
    assert_eq!(
        first,
        Change::PreparedStored {
            id: f.record.id,
            index,
            term
        }
    );
    let stored = state.get(PreparedQuery::Id(f.record.id)).unwrap().unwrap();
    assert_eq!(stored.command(), &f.record);
    assert_eq!(
        state
            .get(PreparedQuery::Idempotency {
                subject: &f.record.subject,
                key: &f.record.idempotency_key
            })
            .unwrap(),
        Some(stored.clone())
    );
    assert_eq!(
        state
            .apply(f.record.clone(), &f.objects, &f.head, index + 7, term + 2)
            .unwrap(),
        first
    );
    assert_eq!(
        state.get(PreparedQuery::Id(f.record.id)).unwrap(),
        Some(stored)
    );
    assert_eq!(state.records.len(), 1);
    assert_eq!(state.keys.len(), 1);
}

#[test]
fn duplicate_key_or_id_cannot_replace_original_command_or_consume_another_slot() {
    let f = fixture();
    let mut state = State::default();
    let index = f.head.raft_index() + 5;
    let term = f.head.raft_term();
    state
        .apply(f.record.clone(), &f.objects, &f.head, index, term)
        .unwrap();
    let before = state.snapshot(CLUSTER).unwrap();
    for case in 0..7 {
        let mut bad = f.record.clone();
        match case {
            0 => bad.request_digest[0] ^= 1,
            1 => bad.leaf_sha256[0] ^= 1,
            2 => bad.admitted_unix_ms += 1,
            3 => bad.root.hash[0] ^= 1,
            4 => bad.idempotency_key = "different-key".into(),
            5 => bad.subject = "spiffe://tickwire.test/operators/other".into(),
            _ => {
                bad.id[0] ^= 1;
                bad.root.owner = bad.id;
            }
        }
        assert_eq!(
            state.apply(bad, &f.objects, &f.head, index + 1, term),
            Err(Error::Conflict),
            "case {case}"
        );
        assert_eq!(state.snapshot(CLUSTER).unwrap(), before);
    }
}

#[test]
fn rejected_closure_leaves_both_indexes_empty_and_does_not_poison_a_later_valid_record() {
    let f = fixture();
    let mut state = State::default();
    let index = f.head.raft_index() + 5;
    let term = f.head.raft_term();
    for case in 0..4 {
        let mut bad = f.record.clone();
        match case {
            0 => bad.root.key[0] ^= 1,
            1 => bad.root.hash[0] ^= 1,
            2 => bad.request_digest[0] ^= 1,
            _ => bad.if_match = "\"cfg-99-incorrect\"".into(),
        }
        assert!(
            state.apply(bad, &f.objects, &f.head, index, term).is_err(),
            "case {case}"
        );
        assert!(state.get(PreparedQuery::Id(f.record.id)).unwrap().is_none());
        assert!(
            state
                .get(PreparedQuery::Idempotency {
                    subject: &f.record.subject,
                    key: &f.record.idempotency_key
                })
                .unwrap()
                .is_none()
        );
        assert!(state.is_empty());
    }
    state
        .apply(f.record.clone(), &f.objects, &f.head, index, term)
        .unwrap();
    assert_eq!(state.records.len(), 1);
}

#[test]
fn record_block_restoration_checks_history_position_and_complete_archive() {
    let f = fixture();
    let mut state = State::default();
    let index = f.head.raft_index() + 5;
    let term = f.head.raft_term() + 1;
    state
        .apply(f.record.clone(), &f.objects, &f.head, index, term)
        .unwrap();
    let bytes = state.snapshot(CLUSTER).unwrap();
    let restored = State::restore(&bytes, CLUSTER, &f.objects, &f.head, index + 1, term).unwrap();
    assert_eq!(restored.snapshot(CLUSTER).unwrap(), bytes);
    assert_eq!(
        restored.get(PreparedQuery::Id(f.record.id)).unwrap(),
        state.get(PreparedQuery::Id(f.record.id)).unwrap()
    );
    for case in 0..8 {
        let mut bad = bytes.clone();
        match case {
            0 => bad.truncate(bad.len() - 1),
            1 => bad.push(0),
            2 => bad[8..12].copy_from_slice(&0u32.to_le_bytes()),
            3 => bad[12..20].copy_from_slice(&f.head.raft_index().to_le_bytes()),
            4 => bad[20..28].copy_from_slice(&(term + 1).to_le_bytes()),
            5 => bad[28..32].copy_from_slice(&u32::MAX.to_le_bytes()),
            6 => bad[32 + 24] = 255,
            _ => {
                bad[8..12].copy_from_slice(&2u32.to_le_bytes());
                bad.extend_from_slice(&bytes[12..]);
            }
        }
        assert!(
            State::restore(&bad, CLUSTER, &f.objects, &f.head, index + 1, term).is_err(),
            "case {case}"
        );
    }
    assert!(State::restore(&bytes, CLUSTER, &f.objects, &f.head, index - 1, term).is_err());
    let empty = objects::State::new(config());
    assert!(State::restore(&bytes, CLUSTER, &empty, &f.head, index + 1, term).is_err());
    let mut foreign = CLUSTER;
    foreign[0] ^= 1;
    assert!(State::restore(&bytes, foreign, &f.objects, &f.head, index + 1, term).is_err());
}
