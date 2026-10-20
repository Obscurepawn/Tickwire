mod common;
use common::*;
use tickwire_account::*;
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
fn stale_account_still_admits_owned_cancel_and_query() {
    for query in [false, true] {
        let (mut account, command) = setup();
        account.admit(&canonical(command.clone()), 60).unwrap();
        let admission = account
            .admit(&canonical(action(&command, query)), 1000)
            .expect("an expired account baseline does not prohibit an owned cancel/query");
        assert_eq!(account.check_admission(&admission, 1001), Ok(()));
        assert!(admission.resources().is_empty());
        assert_eq!(account.asset(key(2)).unwrap().reserved, units(60));
    }
}

#[test]
fn a_previously_admitted_cancel_is_still_valid_after_account_expiry() {
    let (mut account, command) = setup();
    account.admit(&canonical(command.clone()), 60).unwrap();
    let admission = account
        .admit(&canonical(action(&command, false)), 60)
        .unwrap();
    assert_eq!(account.check_admission(&admission, 1000), Ok(()));
    assert_eq!(account.check_admission(&admission, 3000), Err(Error::Stale));
}

#[test]
fn nonincreasing_actions_do_not_require_the_old_risk_policy_version() {
    let (mut account, command) = setup();
    account.admit(&canonical(command.clone()), 60).unwrap();
    let mut query = action(&command, true);
    query.context.strategy.risk_policy_version += 1;
    let admission = account.admit(&canonical(query), 60).unwrap();
    assert_eq!(account.check_admission(&admission, 60), Ok(()));
}

#[test]
fn stale_cancel_still_requires_live_request_identity_and_ownership() {
    let (mut account, command) = setup();
    account.admit(&canonical(command.clone()), 60).unwrap();
    let mut expired = action(&command, false);
    expired.context.request.deadline = 1000;
    assert_eq!(account.admit(&canonical(expired), 1000), Err(Error::Stale));
    let mut foreign = action(&command, false);
    foreign.context.request.account += 1;
    assert_eq!(
        account.admit(&canonical(foreign), 1000),
        Err(Error::Identity)
    );
    let mut wrong_owner = action(&command, false);
    let v::CommandBody::Cancel(ref mut target) = wrong_owner.body else {
        unreachable!()
    };
    target.expected_owner_revision += 1;
    assert_eq!(
        account.admit(&canonical(wrong_owner), 1000),
        Err(Error::Identity)
    );
    let mut new = command;
    new.context.request.intent_id += 10;
    new.context.request.deadline = 3000;
    assert_eq!(account.admit(&canonical(new), 1000), Err(Error::Stale));
    assert_eq!(account.asset(key(2)).unwrap().reserved, units(60));
}

#[test]
fn explicit_freeze_preserves_liability_and_allows_only_nonincreasing_actions() {
    let (mut account, command) = setup();
    let existing = account.admit(&canonical(command.clone()), 60).unwrap();
    let before = account.asset(key(2)).unwrap();
    let sequence = account.ledger_sequence();
    account.freeze();
    account.freeze();
    assert!(account.frozen());
    assert_eq!(account.ledger_sequence(), sequence);
    assert_eq!(account.asset(key(2)).unwrap(), before);
    assert_eq!(account.check_admission(&existing, 60), Err(Error::Frozen));
    let mut new = command.clone();
    new.context.request.intent_id += 10;
    assert_eq!(account.admit(&canonical(new), 60), Err(Error::Frozen));
    let cancel = account
        .admit(&canonical(action(&command, false)), 60)
        .unwrap();
    assert_eq!(account.check_admission(&cancel, 60), Ok(()));
}
