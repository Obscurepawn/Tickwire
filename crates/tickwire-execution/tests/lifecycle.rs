use tickwire_execution::*;

fn gate() -> Gate {
    Gate {
        epoch: 1,
        strategy_generation: 1,
        config_revision: 1,
        now: 10,
        lease_until: 100,
        enabled: true,
        market_valid: true,
        account_valid: true,
        recovered: true,
        owner_isolated: true,
    }
}
fn request(id: u64) -> OrderRequest {
    OrderRequest {
        command_digest: [9; 32],
        key: OrderKey {
            venue: 1,
            account: 1,
            product: Product::Spot,
            intent: id,
        },
        quantity: 10,
        unit_budget: 10,
        fee_budget: 2,
        epoch: 1,
        strategy_generation: 1,
        config_revision: 1,
        deadline: 90,
        commit_profile: CommitProfile::LocalDurable,
        recovery_object: id,
    }
}
fn engine() -> Engine<8, 8> {
    Engine::new(
        Domain {
            venue: 1,
            account: 1,
            product: Product::Spot,
        },
        205,
        gate(),
    )
    .unwrap()
}
fn ready(e: &mut Engine<8, 8>, id: u64) {
    e.reserve(request(id)).unwrap();
    let r = e.durable_record(id).unwrap();
    e.accept_commit(
        CommitReceipt {
            record: r,
            local_durable: true,
            replica_durable: false,
        },
        CommitProfile::LocalDurable,
    )
    .unwrap();
}

#[test]
fn missing_command_digest_cannot_reserve_or_recover() {
    let mut e = engine();
    let original = e.clone();
    let mut q = request(1);
    q.command_digest = [0; 32];
    assert_eq!(e.reserve(q), Err(Error::InvalidRequest));
    assert_eq!(e, original);
    assert_eq!(
        e.recover_intent(DurableRecord {
            request: q,
            sequence: 1
        }),
        Err(Error::InvalidRequest)
    );
    assert_eq!(e, original);
}

#[test]
fn commit_receipt_for_another_command_is_rejected() {
    let mut e = engine();
    e.reserve(request(1)).unwrap();
    let mut record = e.durable_record(1).unwrap();
    record.request.command_digest[0] ^= 1;
    assert_eq!(
        e.accept_commit(
            CommitReceipt {
                record,
                local_durable: true,
                replica_durable: false,
            },
            CommitProfile::LocalDurable,
        ),
        Err(Error::ReceiptMismatch)
    );
    assert_eq!(e.begin_send(1), Err(Error::NotDurable));
}

#[test]
fn competing_strategies_share_atomic_reservation() {
    let mut e = engine();
    e.reserve(request(1)).unwrap();
    e.reserve(request(2)).unwrap();
    assert_eq!(e.available(), 1);
    let before = e.clone();
    assert_eq!(e.reserve(request(3)), Err(Error::InsufficientCapital));
    assert_eq!(e, before);
}
#[test]
fn cannot_send_before_durable_and_gate_is_checked_again() {
    let mut e = engine();
    e.reserve(request(1)).unwrap();
    assert_eq!(e.begin_send(1), Err(Error::NotDurable));
    let r = e.durable_record(1).unwrap();
    e.accept_commit(
        CommitReceipt {
            record: r,
            local_durable: true,
            replica_durable: false,
        },
        CommitProfile::LocalDurable,
    )
    .unwrap();
    let mut g = gate();
    g.config_revision = 2;
    e.update_gate(g);
    assert_eq!(e.begin_send(1), Err(Error::GateClosed));
    assert_eq!(e.order(1).unwrap().send, SendState::NotSubmitted);
}
#[test]
fn sync_profile_requires_matching_replica_receipt() {
    let mut e = engine();
    let mut q = request(1);
    q.commit_profile = CommitProfile::SyncReplica;
    e.reserve(q).unwrap();
    let r = e.durable_record(1).unwrap();
    assert_eq!(
        e.accept_commit(
            CommitReceipt {
                record: r,
                local_durable: true,
                replica_durable: false
            },
            CommitProfile::SyncReplica
        ),
        Err(Error::NotDurable)
    );
    assert_eq!(e.begin_send(1), Err(Error::NotDurable));
    e.accept_commit(
        CommitReceipt {
            record: r,
            local_durable: true,
            replica_durable: true,
        },
        CommitProfile::SyncReplica,
    )
    .unwrap();
    e.begin_send(1).unwrap();
}
#[test]
fn timeout_not_found_and_cancel_request_keep_unknown_reserved() {
    let mut e = engine();
    ready(&mut e, 1);
    e.begin_send(1).unwrap();
    e.uncertain(1).unwrap();
    e.not_found(1).unwrap();
    e.request_cancel(1).unwrap();
    e.cancel_rejected(1).unwrap();
    assert_eq!(e.available(), 103);
    assert_eq!(e.abort_unsent(1), Err(Error::MayHaveSent));
    assert_eq!(e.begin_send(1), Err(Error::AlreadySubmitted));
}
#[test]
fn fill_before_ack_duplicates_out_of_order_and_fee_tail() {
    let mut e = engine();
    ready(&mut e, 1);
    e.begin_send(1).unwrap();
    let a = Fill {
        trade_id: 102,
        quantity: 4,
        cost: 36,
    };
    e.fill(1, a).unwrap();
    e.ack(1).unwrap();
    e.fill(1, a).unwrap();
    e.fill(
        1,
        Fill {
            trade_id: 101,
            quantity: 2,
            cost: 19,
        },
    )
    .unwrap();
    assert_eq!(e.order(1).unwrap().filled, 6);
    assert_eq!(e.spent(), 55);
    e.terminal(1, Terminal::Canceled, 6).unwrap();
    e.ack(1).unwrap();
    assert_eq!(e.order(1).unwrap().terminal, Some(Terminal::Canceled));
    assert_eq!(e.held(), 2);
    e.fee(1, 7, 1).unwrap();
    e.fee(1, 7, 1).unwrap();
    assert_eq!(e.held(), 1);
    assert_eq!(e.spent(), 56);
}
#[test]
fn terminal_before_trade_details_keeps_missing_fill_budget() {
    let mut e = engine();
    ready(&mut e, 1);
    e.begin_send(1).unwrap();
    e.terminal(1, Terminal::Canceled, 4).unwrap();
    assert_eq!(e.held(), 42);
    e.fill(
        1,
        Fill {
            trade_id: 1,
            quantity: 4,
            cost: 39,
        },
    )
    .unwrap();
    assert_eq!(e.held(), 2);
    assert_eq!(e.spent(), 39);
    e.terminal(1, Terminal::Canceled, 0).unwrap();
    assert_eq!(e.order(1).unwrap().observed_filled, 4);
}
#[test]
fn invalid_fill_and_conflicting_duplicate_are_atomic() {
    let mut e = engine();
    ready(&mut e, 1);
    e.begin_send(1).unwrap();
    let f = Fill {
        trade_id: 1,
        quantity: 4,
        cost: 39,
    };
    e.fill(1, f).unwrap();
    let mut before = e.clone();
    before.freeze();
    assert_eq!(
        e.fill(1, Fill { cost: 40, ..f }),
        Err(Error::ConflictingEvent)
    );
    assert_eq!(e, before);
    assert_eq!(
        e.fill(
            1,
            Fill {
                trade_id: 2,
                quantity: 7,
                cost: 70
            }
        ),
        Err(Error::InvalidEvent)
    );
    assert_eq!(e, before);
}
#[test]
fn authoritative_spend_above_budget_is_recorded_and_freezes_admission() {
    let mut e = engine();
    ready(&mut e, 1);
    e.begin_send(1).unwrap();
    e.fill(
        1,
        Fill {
            trade_id: 1,
            quantity: 10,
            cost: 250,
        },
    )
    .unwrap();
    assert_eq!(e.spent(), 250);
    assert!(e.frozen());
    assert_eq!(e.available(), -47);
    assert_eq!(e.reserve(request(2)), Err(Error::GateClosed));
}
#[test]
fn crash_recovery_marks_every_committed_intent_as_maybe_sent() {
    let mut e = engine();
    ready(&mut e, 1);
    let record = e.durable_record(1).unwrap();
    let mut recovered = engine();
    recovered.recover_intent(record).unwrap();
    assert_eq!(recovered.order(1).unwrap().send, SendState::MayHaveSent);
    assert_eq!(recovered.begin_send(1), Err(Error::AlreadySubmitted));
    assert_eq!(recovered.held(), 102);
    assert!(recovered.frozen());
}
#[test]
fn wrong_domain_and_expired_intents_are_rejected() {
    let mut e = engine();
    let mut r = request(1);
    r.key.product = Product::UsdM;
    assert_eq!(e.reserve(r), Err(Error::WrongDomain));
    r = request(1);
    r.deadline = 10;
    assert_eq!(e.reserve(r), Err(Error::GateClosed));
}
#[test]
fn idempotent_commit_cannot_erase_live_state() {
    let mut e = engine();
    ready(&mut e, 1);
    let r = e.durable_record(1).unwrap();
    e.begin_send(1).unwrap();
    e.fill(
        1,
        Fill {
            trade_id: 1,
            quantity: 3,
            cost: 30,
        },
    )
    .unwrap();
    e.accept_commit(
        CommitReceipt {
            record: r,
            local_durable: true,
            replica_durable: false,
        },
        CommitProfile::LocalDurable,
    )
    .unwrap();
    assert_eq!(e.order(1).unwrap().filled, 3);
    assert_eq!(e.begin_send(1), Err(Error::AlreadySubmitted));
}
#[test]
fn no_bytes_release_does_not_invent_fee_payment() {
    let mut e = engine();
    ready(&mut e, 1);
    e.begin_send(1).unwrap();
    e.transport_rejected_without_bytes(1).unwrap();
    assert_eq!(e.held(), 0);
    assert_eq!(e.order(1).unwrap().fees_paid, 0);
    assert_eq!(e.spent(), 0);
}
#[test]
fn synchronous_profile_cannot_be_downgraded_by_the_receipt_caller() {
    let mut e = engine();
    let mut r = request(1);
    r.commit_profile = CommitProfile::SyncReplica;
    e.reserve(r).unwrap();
    let record = e.durable_record(1).unwrap();
    assert_eq!(
        e.accept_commit(
            CommitReceipt {
                record,
                local_durable: true,
                replica_durable: false
            },
            CommitProfile::LocalDurable
        ),
        Err(Error::ReceiptMismatch)
    );
    assert_eq!(e.begin_send(1), Err(Error::NotDurable));
}
#[test]
fn unaccountable_external_fact_freezes_new_risk() {
    let mut e = Engine::<8, 1>::new(request(1).key.domain(), 1000, gate()).unwrap();
    e.reserve(request(1)).unwrap();
    let record = e.durable_record(1).unwrap();
    e.accept_commit(
        CommitReceipt {
            record,
            local_durable: true,
            replica_durable: false,
        },
        CommitProfile::LocalDurable,
    )
    .unwrap();
    e.begin_send(1).unwrap();
    e.fill(
        1,
        Fill {
            trade_id: 1,
            quantity: 1,
            cost: 10,
        },
    )
    .unwrap();
    assert_eq!(e.fee(1, 1, 500), Err(Error::EventCapacity));
    assert!(e.frozen());
    assert_eq!(e.reserve(request(2)), Err(Error::GateClosed));
}
#[test]
fn a_fee_fact_blocks_a_contradictory_no_bytes_callback() {
    for amount in [0, 1] {
        let mut e = engine();
        ready(&mut e, 1);
        e.begin_send(1).unwrap();
        e.fee(1, 7, amount).unwrap();
        let held = e.held();
        assert_eq!(
            e.transport_rejected_without_bytes(1),
            Err(Error::InvalidEvent)
        );
        assert_eq!(e.held(), held);
        assert_eq!(e.spent(), amount);
        assert!(e.frozen());
    }
}
#[test]
fn explicit_unknown_freezes_the_shared_pool_without_releasing_reservation() {
    let mut e = engine();
    ready(&mut e, 1);
    e.begin_send(1).unwrap();
    e.uncertain(1).unwrap();
    assert!(e.frozen());
    assert_eq!(e.held(), 102);
    assert_eq!(e.reserve(request(2)), Err(Error::GateClosed));
    e.ack(1).unwrap();
    assert!(e.frozen());
}
