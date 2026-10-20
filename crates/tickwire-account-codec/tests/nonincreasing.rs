mod common;
use common::*;
use tickwire_account::*;
use tickwire_account_codec::{CanonicalBaseline, CanonicalRecord, Error};
use tickwire_venue as v;

fn action(command: &v::Command, query: bool) -> v::Command {
    let mut result = command.clone();
    result.context.request.intent_id += if query { 2 } else { 1 };
    result.context.request.deadline = 3000;
    let target = v::OrderTarget {
        order_id: [9; 16],
        expected_owner_revision: 1,
        original_intent_exec_epoch: 4,
        client_id: Some(venue::text("test-1")),
        venue_order_id: None,
    };
    result.body = if query {
        v::CommandBody::Query(target)
    } else {
        v::CommandBody::Cancel(target)
    };
    result
}

#[test]
fn an_owned_cancel_or_query_with_old_risk_version_roundtrips_and_replays() {
    for query in [false, true] {
        let (snapshot, catalog, command) = fixture();
        let baseline = CanonicalBaseline::new(snapshot.clone(), catalog.clone()).unwrap();
        let mut account = Account::from_snapshot(snapshot.clone(), catalog.clone()).unwrap();
        let initial = canonical(command.clone());
        let first = account.admit(&initial, 60).unwrap();
        let first_record = RecoveryRecord::Admit {
            command: Box::new(initial),
            evaluated_at: 60,
            expected: first.image(),
        };
        let mut next = action(&command, query);
        next.context.strategy.risk_policy_version -= 1;
        let next = canonical(next);
        let admitted = account.admit(&next, 1000).unwrap();
        let record = RecoveryRecord::Admit {
            command: Box::new(next),
            evaluated_at: 1000,
            expected: admitted.image(),
        };
        let encoded = CanonicalRecord::new(baseline.digest(), 2, record.clone())
            .expect("valid cancel/query keeps the command's historical risk version");
        let decoded = CanonicalRecord::decode(encoded.bytes()).unwrap();
        assert_eq!(decoded.record(), &record);
        let replayed =
            Account::recover(snapshot, catalog, &[first_record, decoded.record().clone()]).unwrap();
        assert!(replayed.frozen());
        assert_eq!(
            replayed.asset(key(2)).unwrap(),
            account.asset(key(2)).unwrap()
        );
    }
}

#[test]
fn an_admission_image_cannot_disguise_a_new_order_as_nonincreasing() {
    let (mut account, command) = setup();
    let command = canonical(command);
    let mut expected = account.admit(&command, 60).unwrap().image();
    expected.increases = false;
    let result = CanonicalRecord::new(
        [7; 32],
        1,
        RecoveryRecord::Admit {
            command: Box::new(command),
            evaluated_at: 60,
            expected,
        },
    );
    assert_eq!(result.unwrap_err(), Error::Field);
}

#[test]
fn a_query_image_cannot_claim_the_opposite_action_classification() {
    let (mut account, command) = setup();
    account.admit(&canonical(command.clone()), 60).unwrap();
    let query = canonical(action(&command, true));
    let mut expected = account.admit(&query, 60).unwrap().image();
    expected.increases = true;
    let result = CanonicalRecord::new(
        [7; 32],
        2,
        RecoveryRecord::Admit {
            command: Box::new(query),
            evaluated_at: 60,
            expected,
        },
    );
    assert_eq!(result.unwrap_err(), Error::Field);
}

#[test]
fn new_orders_still_require_the_recorded_risk_version() {
    let (mut account, mut command) = setup();
    let mut expected = account
        .admit(&canonical(command.clone()), 60)
        .unwrap()
        .image();
    command.context.strategy.risk_policy_version -= 1;
    let changed = canonical(command);
    expected.command_digest = changed.digest();
    let result = CanonicalRecord::new(
        [7; 32],
        1,
        RecoveryRecord::Admit {
            command: Box::new(changed),
            evaluated_at: 60,
            expected,
        },
    );
    assert_eq!(result.unwrap_err(), Error::Field);
}
