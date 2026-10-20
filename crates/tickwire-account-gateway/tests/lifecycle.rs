mod common;
use common::*;
use std::time::Instant;
use tickwire_account::{Applied, AssetAmount, IntentKey, RecoveryRecord};
use tickwire_account_gateway::*;
use tickwire_account_journal::FileBackend;
use tickwire_transport::IoBudget;
use tickwire_venue as v;

fn reopen(dir: &Directory, auth: MockAuthority) -> Gateway {
    let (b, _) = baseline(false);
    let mut cfg = config(&b);
    cfg.writer_epoch = 5;
    AccountGateway::recover(
        FileBackend::open(&dir.0).unwrap(),
        cfg,
        auth,
        TestEncoder,
        None,
    )
    .unwrap()
}
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
fn recovered_admission_is_unknown_with_no_active_receipt_or_resend() {
    let (dir, c, mut g, auth, peer) = make(false, 1);
    g.submit(c.clone(), 60, until()).unwrap();
    let required = g
        .committed(intent())
        .unwrap()
        .receipt()
        .physical_frontier()
        .clone();
    drop(g);
    assert!(peer.join().unwrap().is_empty());
    let (b, _) = baseline(false);
    let mut cfg = config(&b);
    cfg.writer_epoch = 5;
    let mut restored = Gateway::recover(
        FileBackend::open(&dir.0).unwrap(),
        cfg,
        auth.clone(),
        TestEncoder,
        Some(&required),
    )
    .unwrap();
    assert!(restored.halted() && restored.account().frozen());
    assert!(restored.committed(intent()).is_none());
    assert_eq!(
        restored.account().asset(fixture::key(2)).unwrap().reserved,
        fixture::units(60)
    );
    assert_eq!(restored.submit(c, 9000, until()).unwrap(), Status::Unknown);
    assert_eq!(auth.policy.lock().unwrap().prepare_calls, 1);
    assert!(matches!(
        restored.poll(Instant::now(), IoBudget::default()),
        Err(Error::Halted)
    ));
}
#[test]
fn aborted_admission_remains_aborted_after_reopen() {
    let (dir, c, mut g, auth, peer) = make(false, 1);
    g.submit(c.clone(), 60, until()).unwrap();
    g.cancel_pending().unwrap();
    drop(g);
    assert!(peer.join().unwrap().is_empty());
    let mut restored = reopen(&dir, auth);
    assert_eq!(restored.submit(c, 9000, until()).unwrap(), Status::Aborted);
    assert_eq!(restored.journal_position(), 2);
    assert_eq!(
        restored.account().asset(fixture::key(2)).unwrap().reserved,
        fixture::units(0)
    );
}
#[test]
fn missing_baseline_or_history_object_rejects_recovery() {
    for kind in [14, 15] {
        let (dir, c, mut g, auth, peer) = make(false, 1);
        g.submit(c, 60, until()).unwrap();
        drop(g);
        peer.join().unwrap();
        let mut blobs = auth.blobs.0.lock().unwrap();
        let manifest = blobs
            .iter()
            .find_map(|(k, b)| matches!(k, tickwire_recovery::StoreKey::Manifest(_)).then_some(b))
            .unwrap();
        let manifest = tickwire_recovery::RecoveryManifest::decode(manifest).unwrap();
        let object = manifest.objects.iter().find(|o| o.kind == kind).unwrap();
        blobs.remove(&tickwire_recovery::StoreKey::Object(object.content_hash));
        drop(blobs);
        let (b, _) = baseline(false);
        assert!(
            Gateway::recover(
                FileBackend::open(&dir.0).unwrap(),
                config(&b),
                auth,
                TestEncoder,
                None
            )
            .is_err()
        );
    }
}
#[test]
fn dependency_rejection_or_foreign_account_binding_never_queues_or_reserves() {
    for which in 0..3 {
        let (_dir, c, mut g, auth, peer) = make(false, 1);
        {
            let mut p = auth.policy.lock().unwrap();
            p.deny_prepare = which == 0;
            p.wrong_baseline = which == 1;
            p.wrong_history = which == 2;
        }
        assert!(g.submit(c, 60, until()).is_err());
        assert!(g.halted());
        assert_eq!(g.journal_position(), 0);
        assert_eq!(
            g.account().asset(fixture::key(2)).unwrap().reserved,
            fixture::units(0)
        );
        drop(g);
        assert!(peer.join().unwrap().is_empty());
    }
}
#[test]
fn every_fresh_gate_requirement_is_checked_before_first_acceptance() {
    for which in 0..9 {
        let (_dir, c, mut g, auth, peer) = make(false, 1);
        g.submit(c, 60, until()).unwrap();
        {
            let mut p = auth.policy.lock().unwrap();
            match which {
                0 => p.gate.owner_isolated = false,
                1 => p.gate.action_permitted = false,
                2 => p.gate.market_valid = false,
                3 => p.gate.writer_epoch += 1,
                4 => p.gate.strategy_generation += 1,
                5 => p.gate.config_revision += 1,
                6 => p.gate.lease_until = p.gate.now,
                7 => p.gate.now = 101,
                8 => p.gate.now = 59,
                _ => unreachable!(),
            }
        }
        drive(&mut g, Status::Aborted);
        assert_eq!(g.journal_position(), 2);
        assert_eq!(auth.policy.lock().unwrap().gate_calls, 1);
        drop(g);
        assert!(peer.join().unwrap().is_empty());
    }
}
#[test]
fn zero_io_budget_is_not_evidence_for_refunding_an_order() {
    let (_dir, c, mut g, _auth, peer) = make(false, 1);
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
    assert_eq!(g.journal_position(), 1);
    assert_eq!(
        g.account().asset(fixture::key(2)).unwrap().reserved,
        fixture::units(60)
    );
    drop(g);
    peer.join().unwrap();
}
#[test]
fn exact_retry_survives_halt_but_conflicting_retry_freezes_without_new_record() {
    let (_dir, c, mut g, _auth, peer) = make(false, 1);
    g.submit(c.clone(), 60, until()).unwrap();
    let mut conflict = c.command().clone();
    conflict.context.request.deadline += 1;
    assert!(matches!(
        g.submit(fixture::canonical(conflict), 60, until()),
        Err(Error::Conflict)
    ));
    assert_eq!(g.submit(c, 60, until()).unwrap(), Status::Unknown);
    assert_eq!(g.journal_position(), 1);
    assert_eq!(
        g.account().asset(fixture::key(2)).unwrap().reserved,
        fixture::units(60)
    );
    drop(g);
    assert!(peer.join().unwrap().is_empty());
}
#[test]
fn public_fact_entry_cannot_forge_admission_or_zero_acceptance() {
    let (_dir, c, mut g, _auth, peer) = make(false, 1);
    g.submit(c.clone(), 60, until()).unwrap();
    let image = g.committed(intent()).unwrap().admission().image();
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
    assert_eq!(g.journal_position(), 1);
    assert_eq!(
        g.account().asset(fixture::key(2)).unwrap().reserved,
        fixture::units(60)
    );
    drop(g);
    assert!(peer.join().unwrap().is_empty());
}
#[test]
fn repeated_trade_keeps_new_observation_and_replays_cashflow_once() {
    let (dir, c, mut g, auth, peer) = make(false, 16384);
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
    assert_eq!(g.journal_position(), 4);
    let before = g.account().asset(fixture::key(2)).unwrap();
    assert_eq!(before.total, fixture::units(980));
    assert_eq!(before.reserved, fixture::units(0));
    drop(g);
    peer.join().unwrap();
    let restored = reopen(&dir, auth);
    assert_eq!(restored.journal_position(), 4);
    assert_eq!(restored.account().asset(fixture::key(2)).unwrap(), before);
}
#[test]
fn externally_frozen_account_can_still_cancel_and_query_on_healthy_connection() {
    for query in [false, true] {
        let (_dir, c, mut g, auth, peer) = make(false, 16384);
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
            p.gate.lease_until = 2000;
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
fn reducer_error_preserves_external_input_and_recovery_refuses_to_skip_it() {
    let (dir, c, mut g, auth, peer) = make(false, 16384);
    g.submit(c.clone(), 60, until()).unwrap();
    drive(&mut g, Status::Flushed);
    g.record_event(fixture::event(fixture::fill(
        c.command(),
        "t1",
        "0.0004",
        "50000",
        "20",
    )))
    .unwrap();
    assert!(matches!(
        g.record_event(fixture::event(fixture::fill(
            c.command(),
            "t1",
            "0.0005",
            "50000",
            "25"
        ))),
        Err(Error::Account(tickwire_account::Error::Conflict))
    ));
    assert_eq!(g.journal_position(), 3);
    assert!(g.account().frozen());
    assert!(!g.halted());
    drop(g);
    peer.join().unwrap();
    let (b, _) = baseline(false);
    assert!(
        Gateway::recover(
            FileBackend::open(&dir.0).unwrap(),
            config(&b),
            auth,
            TestEncoder,
            None
        )
        .is_err()
    );
}
#[test]
fn frozen_recovery_accepts_durable_reconciliation_but_never_new_admission() {
    let (dir, c, mut g, auth, peer) = make(false, 1);
    g.submit(c, 60, until()).unwrap();
    drop(g);
    peer.join().unwrap();
    let mut restored = reopen(&dir, auth);
    restored
        .record_fact(RecoveryRecord::UnattributedCashFlow {
            id: fixture::venue::text("external"),
            delta: AssetAmount {
                key: fixture::key(2),
                amount: fixture::units(5),
            },
        })
        .unwrap();
    assert_eq!(restored.journal_position(), 2);
    assert_eq!(
        restored.account().asset(fixture::key(2)).unwrap().total,
        fixture::units(1005)
    );
    assert!(restored.halted() && restored.account().frozen());
}
