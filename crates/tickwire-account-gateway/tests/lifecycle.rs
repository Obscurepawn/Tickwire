mod common;
use common::*;
use std::time::Instant;
use tickwire_account::{Applied, AssetAmount, IntentKey, RecoveryRecord};
use tickwire_account_gateway::*;
use tickwire_transport::IoBudget;
use tickwire_venue as v;

fn action(c: &v::CanonicalCommand, query: bool) -> v::CanonicalCommand {
    let mut cmd = c.command().clone();
    cmd.context.request.intent_id += 1;
    cmd.context.request.recovery_object += 1;
    cmd.context.request.deadline = 3000;
    let target = v::OrderTarget {
        order_id: [9; 16],
        expected_owner_revision: 1,
        original_intent_exec_epoch: 4,
        client_id: Some(fixture::venue::text("test-1")),
        venue_order_id: None,
    };
    cmd.body = if query {
        v::CommandBody::Query(target)
    } else {
        v::CommandBody::Cancel(target)
    };
    fixture::canonical(cmd)
}

fn drive_key(g: &mut Gateway, key: IntentKey, wanted: Status) {
    let deadline = until();
    while g.status(key) != Some(wanted) {
        assert!(Instant::now() < deadline);
        g.poll(Instant::now(), IoBudget::default()).unwrap();
        std::thread::yield_now();
    }
}

#[test]
fn authority_denial_or_foreign_account_binding_never_queues_or_reserves() {
    let (c, mut g, auth, peer) = make(false, 1);
    auth.policy.lock().unwrap().deny_authorize = true;
    assert!(matches!(
        g.submit(c.clone(), 60, until()),
        Err(Error::Authority(AuthorityError::Denied))
    ));
    assert!(!g.halted());
    assert_eq!(g.entry_count(), 0);
    assert_eq!(
        g.account().asset(fixture::key(2)).unwrap().reserved,
        fixture::units(0)
    );

    let mut wrong = c.command().clone();
    wrong.context.request.account = 999;
    assert!(matches!(
        g.submit(fixture::canonical(wrong), 60, until()),
        Err(Error::Identity)
    ));
    assert_eq!(
        g.account().asset(fixture::key(2)).unwrap().reserved,
        fixture::units(0)
    );
    drop(g);
    assert!(peer.join().unwrap().is_empty());
}

#[test]
fn every_fresh_gate_requirement_is_checked_before_first_acceptance() {
    for which in 0..7 {
        let (c, mut g, auth, peer) = make(false, 1);
        g.submit(c, 60, until()).unwrap();
        {
            let mut p = auth.policy.lock().unwrap();
            match which {
                0 => p.gate.action_permitted = false,
                1 => p.gate.market_valid = false,
                2 => p.gate.writer_epoch += 1,
                3 => p.gate.strategy_generation += 1,
                4 => p.gate.config_revision += 1,
                5 => p.gate.now = 101,
                6 => p.gate.now = 59,
                _ => unreachable!(),
            }
        }
        drive(&mut g, Status::Aborted);
        assert_eq!(auth.policy.lock().unwrap().gate_calls, 1);
        drop(g);
        assert!(peer.join().unwrap().is_empty());
    }
}

#[test]
fn zero_io_budget_is_not_evidence_for_refunding_an_order() {
    let (c, mut g, _auth, peer) = make(false, 1);
    g.submit(c, 60, until()).unwrap();
    assert!(
        g.poll(
            Instant::now(),
            IoBudget {
                max_syscalls: 0,
                max_bytes: 10
            }
        )
        .is_err()
    );
    assert_eq!(g.status(intent()), Some(Status::Unknown));
    assert_eq!(
        g.account().asset(fixture::key(2)).unwrap().reserved,
        fixture::units(60)
    );
    drop(g);
    peer.join().unwrap();
}

#[test]
fn exact_retry_survives_halt_but_conflicting_retry_freezes_without_new_entry() {
    let (c, mut g, _auth, peer) = make(false, 1);
    g.submit(c.clone(), 60, until()).unwrap();
    let mut conflict = c.command().clone();
    conflict.context.request.deadline += 1;
    assert!(matches!(
        g.submit(fixture::canonical(conflict), 60, until()),
        Err(Error::Conflict)
    ));
    assert_eq!(g.submit(c, 60, until()).unwrap(), Status::Unknown);
    assert_eq!(g.entry_count(), 1);
    assert_eq!(
        g.account().asset(fixture::key(2)).unwrap().reserved,
        fixture::units(60)
    );
    drop(g);
    assert!(peer.join().unwrap().is_empty());
}

#[test]
fn public_fact_entry_cannot_forge_admission_or_zero_acceptance() {
    let (c, mut g, _auth, peer) = make(false, 1);
    g.submit(c.clone(), 60, until()).unwrap();
    let image = g.admission(intent()).unwrap().image();
    for record in [
        RecoveryRecord::Admit {
            command: Box::new(c.clone()),
            evaluated_at: 60,
            expected: image,
        },
        RecoveryRecord::Aborted {
            intent: intent(),
            command_digest: c.digest(),
        },
    ] {
        assert!(matches!(g.record_fact(record), Err(Error::Invalid)));
    }
    assert_eq!(
        g.account().asset(fixture::key(2)).unwrap().reserved,
        fixture::units(60)
    );
    drop(g);
    assert!(peer.join().unwrap().is_empty());
}

#[test]
fn repeated_trade_keeps_new_observation_and_applies_cashflow_once() {
    let (c, mut g, _auth, peer) = make(false, 16384);
    g.submit(c.clone(), 60, until()).unwrap();
    drive(&mut g, Status::Flushed);
    let mut trade = fixture::fill(c.command(), "t1", "0.0004", "50000", "20");
    assert_eq!(
        g.record_event(fixture::event(trade.clone())).unwrap(),
        Applied::New
    );
    assert_eq!(
        g.record_event(fixture::event(trade.clone())).unwrap(),
        Applied::Duplicate
    );
    let v::VenueEventBody::Fill(ref mut fill) = trade.body else {
        unreachable!()
    };
    fill.observation.status = v::OrderStatus::Canceled;
    fill.observation.cumulative_quantity = Some("0.0004".parse().unwrap());
    assert_eq!(g.record_event(fixture::event(trade)).unwrap(), Applied::New);
    let before = g.account().asset(fixture::key(2)).unwrap();
    assert_eq!(before.total, fixture::units(980));
    assert_eq!(before.reserved, fixture::units(0));
    drop(g);
    peer.join().unwrap();
}

#[test]
fn externally_frozen_account_can_still_cancel_and_query_on_healthy_connection() {
    for query in [false, true] {
        let (c, mut g, auth, peer) = make(false, 16384);
        g.submit(c.clone(), 60, until()).unwrap();
        drive(&mut g, Status::Flushed);
        g.record_event(fixture::event(fixture::fee(
            c.command(),
            "unexpected",
            c.command().context.instrument.base.clone(),
            "-0.01",
        )))
        .unwrap();
        assert!(g.account().frozen());
        assert!(!g.halted());
        let cancel = action(&c, query);
        {
            let mut p = auth.policy.lock().unwrap();
            p.gate.now = 1001;
            p.gate.market_valid = false;
        }
        g.submit(cancel, 1000, until()).unwrap();
        drive_key(
            &mut g,
            IntentKey {
                epoch: 4,
                intent_id: 4,
            },
            Status::Flushed,
        );
        assert_eq!(
            g.account().asset(fixture::key(2)).unwrap().reserved,
            fixture::units(60)
        );
        drop(g);
        assert!(!peer.join().unwrap().is_empty());
    }
}

#[test]
fn halted_gateway_accepts_reconciliation_facts_but_never_new_admission() {
    let (c, mut g, _auth, peer) = make(false, 1);
    g.submit(c, 60, until()).unwrap();
    g.stop_sending();
    assert!(g.halted() && g.account().frozen());
    g.record_fact(RecoveryRecord::UnattributedCashFlow {
        id: fixture::venue::text("external"),
        delta: AssetAmount {
            key: fixture::key(2),
            amount: fixture::units(5),
        },
    })
    .unwrap();
    assert_eq!(
        g.account().asset(fixture::key(2)).unwrap().total,
        fixture::units(1005)
    );
    drop(g);
    peer.join().unwrap();
}
