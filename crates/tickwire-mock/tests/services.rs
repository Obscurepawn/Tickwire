use tickwire_execution::{ports::*, *};
use tickwire_mock::*;
fn gate() -> Gate {
    Gate {
        epoch: 1,
        strategy_generation: 1,
        config_revision: 1,
        now: 1,
        lease_until: 100,
        enabled: true,
        market_valid: true,
        account_valid: true,
        recovered: true,
        owner_isolated: true,
    }
}
fn req() -> OrderRequest {
    OrderRequest {
        key: OrderKey {
            venue: 7,
            account: 9,
            product: Product::Spot,
            intent: 1,
        },
        quantity: 10,
        unit_budget: 10,
        fee_budget: 2,
        epoch: 1,
        strategy_generation: 1,
        config_revision: 1,
        deadline: 90,
        commit_profile: CommitProfile::LocalDurable,
        recovery_object: 1,
    }
}
fn runtime() -> MockRuntime<8, 8> {
    MockRuntime::new(req().key.domain(), 205, gate()).unwrap()
}
#[test]
fn relational_cas_and_immutable_kv_contracts() {
    let mut sql = MockSql::new(b"a");
    assert_eq!(sql.load().unwrap().revision, 1);
    assert_eq!(sql.compare_exchange(1, b"b").unwrap().revision, 2);
    assert_eq!(sql.compare_exchange(1, b"c"), Err(ServiceError::Conflict));
    assert_eq!(sql.load().unwrap().payload, b"b");
    let mut kv = MockKv::default();
    kv.put_immutable(1, b"a").unwrap();
    kv.put_immutable(1, b"a").unwrap();
    assert_eq!(kv.put_immutable(1, b"b"), Err(ServiceError::Conflict));
    assert_eq!(kv.get(1).unwrap(), b"a");
}
#[test]
fn missing_recovery_dependency_prevents_commit() {
    let mut j = MockJournal::default();
    assert_eq!(
        j.commit(DurableRecord {
            request: req(),
            sequence: 1
        }),
        Err(ServiceError::MissingDependency)
    );
    assert!(j.committed().is_empty());
}
#[test]
fn storage_failure_never_reaches_exchange() {
    let mut r = runtime();
    r.journal.fail_next_commit = true;
    assert!(r.submit(req(), CommitProfile::LocalDurable).is_err());
    assert_eq!(r.exchange.submit_calls, 0);
    assert_eq!(r.engine.held(), 102);
    r.resume_submit(1, CommitProfile::LocalDurable).unwrap();
    assert_eq!(r.exchange.submit_calls, 1);
}
#[test]
fn kv_failure_never_reaches_journal_or_exchange() {
    let mut r = runtime();
    r.journal.objects.fail_writes = true;
    assert!(r.submit(req(), CommitProfile::LocalDurable).is_err());
    assert!(r.journal.committed().is_empty());
    assert_eq!(r.exchange.submit_calls, 0);
}
#[test]
fn accepted_order_ack_loss_and_recovery_do_not_resubmit() {
    let mut r = runtime();
    r.exchange.next_outcome = SubmitOutcome::MayHaveSent;
    r.submit(req(), CommitProfile::LocalDurable).unwrap();
    assert_eq!(r.exchange.accepted.len(), 1);
    r.engine.not_found(1).unwrap();
    r.restart().unwrap();
    assert_eq!(r.engine.held(), 102);
    assert_eq!(r.exchange.submit_calls, 1);
    assert!(r.resume_submit(1, CommitProfile::LocalDurable).is_err());
    assert_eq!(r.exchange.submit_calls, 1);
    r.record_fill(
        1,
        Fill {
            trade_id: 102,
            quantity: 4,
            cost: 39,
        },
    )
    .unwrap();
    r.record_fill(
        1,
        Fill {
            trade_id: 101,
            quantity: 2,
            cost: 19,
        },
    )
    .unwrap();
    r.record_terminal(1, Terminal::Canceled, 6).unwrap();
    r.record_fee(1, 10, 1).unwrap();
    let before = (r.engine.spent(), r.engine.held());
    r.restart().unwrap();
    assert_eq!((r.engine.spent(), r.engine.held()), before);
    r.record_fill(
        1,
        Fill {
            trade_id: 101,
            quantity: 2,
            cost: 19,
        },
    )
    .unwrap();
    assert_eq!(r.engine.spent(), 59);
}
#[test]
fn committed_but_lost_storage_receipt_is_recovered_conservatively() {
    let mut r = runtime();
    r.journal.lose_next_receipt = true;
    assert!(r.submit(req(), CommitProfile::LocalDurable).is_err());
    assert_eq!(r.journal.committed().len(), 1);
    assert_eq!(r.exchange.submit_calls, 0);
    r.restart().unwrap();
    assert_eq!(r.engine.order(1).unwrap().send, SendState::MayHaveSent);
    assert_eq!(r.engine.held(), 102);
}
#[test]
fn synchronous_replica_pending_does_not_fall_back_to_local() {
    let mut r = runtime();
    r.journal.replica_ready = false;
    assert!(
        r.submit(
            OrderRequest {
                commit_profile: CommitProfile::SyncReplica,
                ..req()
            },
            CommitProfile::SyncReplica
        )
        .is_err()
    );
    assert_eq!(r.exchange.submit_calls, 0);
    r.journal.replica_ready = true;
    r.resume_submit(1, CommitProfile::SyncReplica).unwrap();
    assert_eq!(r.exchange.submit_calls, 1);
}
#[test]
fn facts_are_not_applied_when_storage_fails() {
    let mut r = runtime();
    r.submit(req(), CommitProfile::LocalDurable).unwrap();
    r.journal.fail_next_fact = true;
    assert!(
        r.record_fill(
            1,
            Fill {
                trade_id: 1,
                quantity: 2,
                cost: 20
            }
        )
        .is_err()
    );
    assert_eq!(r.engine.spent(), 0);
    r.record_fill(
        1,
        Fill {
            trade_id: 1,
            quantity: 2,
            cost: 20,
        },
    )
    .unwrap();
    assert_eq!(r.engine.spent(), 20);
}
#[test]
fn venue_identity_is_not_binance_specific_and_domains_cannot_mix() {
    for venue in [7, 42] {
        let mut q = req();
        q.key.venue = venue;
        let mut r = MockRuntime::<8, 8>::new(q.key.domain(), 205, gate()).unwrap();
        r.submit(q, CommitProfile::LocalDurable).unwrap();
        assert_eq!(r.exchange.accepted[0].key.venue, venue);
    }
    let mut r = runtime();
    let mut q = req();
    q.key.venue = 42;
    assert!(r.submit(q, CommitProfile::LocalDurable).is_err());
    assert_eq!(r.exchange.submit_calls, 0);
}
#[test]
fn even_empty_journal_restart_requires_recovery_authorization() {
    let mut r = runtime();
    r.restart().unwrap();
    assert!(r.engine.frozen());
    assert!(r.submit(req(), CommitProfile::LocalDurable).is_err());
    assert_eq!(r.exchange.submit_calls, 0);
}
#[test]
fn healthy_async_pending_response_is_distinct_from_timeout() {
    let mut r = runtime();
    r.exchange.next_outcome = SubmitOutcome::PendingResponse;
    r.submit(req(), CommitProfile::LocalDurable).unwrap();
    assert_eq!(r.engine.order(1).unwrap().send, SendState::MayHaveSent);
    assert!(!r.engine.frozen());
    let mut second = req();
    second.key.intent = 2;
    second.recovery_object = 2;
    r.submit(second, CommitProfile::LocalDurable).unwrap();
    assert_eq!(r.exchange.submit_calls, 2);
    r.engine.uncertain(1).unwrap();
    assert!(r.engine.frozen());
    assert_eq!(r.engine.held(), 204);
}
