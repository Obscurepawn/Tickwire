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

#[test]
fn durable_abort_prevents_reholding_released_budget_on_restart() {
    let mut r = MockRuntime::<8, 8>::new(req().key.domain(), i128::MAX, gate()).unwrap();
    let mut first = req();
    first.quantity = 2;
    first.unit_budget = 1;
    first.fee_budget = 1;
    r.exchange.next_outcome = SubmitOutcome::NoBytesAccepted;
    r.submit(first, CommitProfile::LocalDurable).unwrap();
    let mut second = first;
    second.key.intent = 2;
    second.recovery_object = 2;
    r.submit(second, CommitProfile::LocalDurable).unwrap();
    r.record_fill(
        2,
        Fill {
            trade_id: 1,
            quantity: 2,
            cost: i128::MAX - 3,
        },
    )
    .unwrap();
    assert_eq!(r.engine.available(), 2);
    assert!(!r.engine.frozen());
    r.restart().unwrap();
    assert_eq!(r.engine.available(), 2);
    assert_eq!(
        r.engine.order(1).unwrap().terminal,
        Some(Terminal::Rejected)
    );
    assert!(r.engine.frozen());
    let mut third = first;
    third.key.intent = 3;
    third.recovery_object = 3;
    third.quantity = 1;
    third.fee_budget = 0;
    assert!(r.submit(third, CommitProfile::LocalDurable).is_err());
    assert_eq!(r.exchange.submit_calls, 2);
}

#[test]
fn no_bytes_abort_survives_restart() {
    let mut r = runtime();
    r.exchange.next_outcome = SubmitOutcome::NoBytesAccepted;
    r.submit(req(), CommitProfile::LocalDurable).unwrap();
    assert_eq!(r.engine.held(), 0);
    r.restart().unwrap();
    assert_eq!(r.engine.held(), 0);
    assert_eq!(r.engine.order(1).unwrap().send, SendState::NotSubmitted);
    assert_eq!(
        r.engine.order(1).unwrap().terminal,
        Some(Terminal::Rejected)
    );
    assert!(r.engine.frozen());
    assert_eq!(r.exchange.submit_calls, 1);
    assert!(r.exchange.accepted.is_empty());
}

#[test]
fn failed_abort_persistence_retains_reservation_and_freezes() {
    let mut r = runtime();
    r.exchange.next_outcome = SubmitOutcome::NoBytesAccepted;
    r.journal.fail_next_fact = true;
    assert_eq!(
        r.submit(req(), CommitProfile::LocalDurable),
        Err(RuntimeError::Service(ServiceError::Unavailable))
    );
    assert_eq!(r.engine.held(), 102);
    assert_eq!(r.engine.order(1).unwrap().send, SendState::MayHaveSent);
    assert!(r.engine.frozen());
    assert!(r.journal.facts().is_empty());
    r.restart().unwrap();
    assert_eq!(r.engine.held(), 102);
    assert!(r.engine.frozen());
    assert_eq!(r.exchange.submit_calls, 1);
}

#[test]
fn recovery_replays_terminal_release_before_later_intent() {
    let mut r = MockRuntime::<8, 8>::new(req().key.domain(), i128::MAX, gate()).unwrap();
    let first = OrderRequest {
        quantity: 1,
        unit_budget: 1,
        fee_budget: 0,
        ..req()
    };
    r.submit(first, CommitProfile::LocalDurable).unwrap();
    r.record_terminal(1, Terminal::Rejected, 0).unwrap();
    let second = OrderRequest {
        key: OrderKey {
            intent: 2,
            ..first.key
        },
        recovery_object: 2,
        unit_budget: i128::MAX,
        ..first
    };
    r.submit(second, CommitProfile::LocalDurable).unwrap();
    assert_eq!(r.engine.held(), i128::MAX);
    r.restart().unwrap();
    assert_eq!(r.engine.held(), i128::MAX);
    assert_eq!(r.engine.spent(), 0);
    assert!(r.engine.frozen());
}

#[test]
fn failed_recovery_cannot_leave_the_old_engine_authorized() {
    let mut r = runtime();
    r.submit(req(), CommitProfile::LocalDurable).unwrap();
    // A persisted but unappliable fact must stop recovery and freeze the old engine.
    r.journal
        .append_fact(Fact::Fill {
            intent: 99,
            fill: Fill {
                trade_id: 1,
                quantity: 1,
                cost: 1,
            },
        })
        .unwrap();
    assert_eq!(r.restart(), Err(RuntimeError::Domain(Error::NotFound)));
    assert_eq!(r.engine.held(), 102);
    assert!(r.engine.frozen());
    let mut second = req();
    second.key.intent = 2;
    second.recovery_object = 2;
    assert!(r.submit(second, CommitProfile::LocalDurable).is_err());
    assert_eq!(r.exchange.submit_calls, 1);
}

#[test]
fn abort_record_binds_epoch_request_and_sequence_before_releasing() {
    for field in 0..4 {
        let mut r = runtime();
        r.exchange.next_outcome = SubmitOutcome::PendingResponse;
        r.submit(req(), CommitProfile::LocalDurable).unwrap();
        let before = *r.engine.order(1).unwrap();
        let mut record = r.engine.durable_record(1).unwrap();
        match field {
            0 => record.sequence += 1,
            1 => record.request.epoch += 1,
            2 => record.request.quantity += 1,
            _ => record.request.key.account += 1,
        }
        assert_eq!(
            Fact::AbortedBeforeSend { record }.apply(&mut r.engine),
            Err(Error::ReceiptMismatch)
        );
        assert_eq!(*r.engine.order(1).unwrap(), before);
        assert_eq!((r.engine.spent(), r.engine.held()), (0, 102));
        assert!(r.engine.frozen());
    }
    let mut r = runtime();
    let record = DurableRecord {
        request: req(),
        sequence: 1,
    };
    assert_eq!(
        Fact::AbortedBeforeSend { record }.apply(&mut r.engine),
        Err(Error::NotFound)
    );
    assert!(r.engine.frozen());
    assert_eq!(r.engine.held(), 0);
}

#[test]
fn duplicate_abort_is_idempotent_but_fee_evidence_rejects_abort() {
    let mut r = runtime();
    r.exchange.next_outcome = SubmitOutcome::NoBytesAccepted;
    r.submit(req(), CommitProfile::LocalDurable).unwrap();
    let fact = r.journal.facts()[0];
    let before = *r.engine.order(1).unwrap();
    fact.apply(&mut r.engine).unwrap();
    assert_eq!(*r.engine.order(1).unwrap(), before);
    r.journal.append_fact(fact).unwrap();
    r.restart().unwrap();
    assert_eq!(*r.engine.order(1).unwrap(), before);
    assert_eq!(r.engine.held(), 0);

    for amount in [0, 1] {
        let mut r = runtime();
        r.exchange.next_outcome = SubmitOutcome::PendingResponse;
        r.submit(req(), CommitProfile::LocalDurable).unwrap();
        r.record_fee(1, 1, amount).unwrap();
        let before = *r.engine.order(1).unwrap();
        let record = r.engine.durable_record(1).unwrap();
        assert_eq!(
            Fact::AbortedBeforeSend { record }.apply(&mut r.engine),
            Err(Error::InvalidEvent)
        );
        assert_eq!(*r.engine.order(1).unwrap(), before);
        assert!(r.engine.frozen());
    }
}

#[test]
fn persisted_abort_is_recovered_even_before_live_apply() {
    let mut r = runtime();
    r.exchange.next_outcome = SubmitOutcome::PendingResponse;
    r.submit(req(), CommitProfile::LocalDurable).unwrap();
    // Simulate the durable-fact/live-apply crash boundary for a trusted NoBytes proof.
    let record = r.engine.durable_record(1).unwrap();
    r.journal
        .append_fact(Fact::AbortedBeforeSend { record })
        .unwrap();
    assert_eq!(r.engine.held(), 102);
    r.restart().unwrap();
    assert_eq!(r.engine.held(), 0);
    assert_eq!(
        r.engine.order(1).unwrap().terminal,
        Some(Terminal::Rejected)
    );
    assert!(r.engine.frozen());
}

#[test]
fn journal_events_preserve_order_without_duplicating_idempotent_commit() {
    let mut r = runtime();
    r.journal.lose_next_receipt = true;
    assert!(r.submit(req(), CommitProfile::LocalDurable).is_err());
    let first = r.engine.durable_record(1).unwrap();
    assert_eq!(r.journal.events(), &[MockJournalEvent::Intent(first)]);
    r.exchange.next_outcome = SubmitOutcome::NoBytesAccepted;
    r.resume_submit(1, CommitProfile::LocalDurable).unwrap();
    let second_request = OrderRequest {
        key: OrderKey {
            intent: 2,
            ..req().key
        },
        recovery_object: 2,
        ..req()
    };
    r.submit(second_request, CommitProfile::LocalDurable)
        .unwrap();
    let second = r.engine.durable_record(2).unwrap();
    r.record_terminal(2, Terminal::Rejected, 0).unwrap();
    let abort = Fact::AbortedBeforeSend { record: first };
    let terminal = Fact::Terminal {
        intent: 2,
        status: Terminal::Rejected,
        cumulative: 0,
    };
    assert_eq!(
        r.journal.events(),
        &[
            MockJournalEvent::Intent(first),
            MockJournalEvent::Fact(abort),
            MockJournalEvent::Intent(second),
            MockJournalEvent::Fact(terminal),
        ]
    );
    assert_eq!(r.journal.committed(), &[first, second]);
    assert_eq!(r.journal.facts(), &[abort, terminal]);
}

#[test]
fn an_abort_before_its_intent_cannot_recover_a_live_engine() {
    let mut r = runtime();
    let record = DurableRecord {
        request: req(),
        sequence: 1,
    };
    r.journal
        .append_fact(Fact::AbortedBeforeSend { record })
        .unwrap();
    assert_eq!(r.restart(), Err(RuntimeError::Domain(Error::NotFound)));
    assert!(r.engine.frozen());
    assert!(r.engine.order(1).is_err());
}
