mod common;
use common::*;
use tickwire_account_gateway::*;

#[test]
fn commit_reserves_once_and_retry_does_not_repeat_any_side_effect() {
    let (_dir, c, mut g, auth, peer) = make(false, 16384);
    assert_eq!(g.submit(c.clone(), 60, until()).unwrap(), Status::Queued);
    assert_eq!(g.journal_position(), 1);
    assert_eq!(
        g.account().asset(fixture::key(2)).unwrap().reserved,
        fixture::units(60)
    );
    assert_eq!(
        g.committed(intent()).unwrap().admission().command_digest(),
        c.digest()
    );
    assert_eq!(g.submit(c, 69, until()).unwrap(), Status::Queued);
    assert_eq!(g.journal_position(), 1);
    assert_eq!(auth.policy.lock().unwrap().prepare_calls, 1);
    assert_eq!(auth.policy.lock().unwrap().gate_calls, 0);
    drop(g);
    assert!(peer.join().unwrap().is_empty());
}
#[test]
fn final_gate_rejection_is_durably_aborted_before_release() {
    let (_dir, c, mut g, auth, peer) = make(false, 16384);
    g.submit(c, 60, until()).unwrap();
    auth.policy.lock().unwrap().gate.action_permitted = false;
    drive(&mut g, Status::Aborted);
    assert_eq!(g.journal_position(), 2);
    assert_eq!(
        g.account().asset(fixture::key(2)).unwrap().reserved,
        fixture::units(0)
    );
    drop(g);
    assert!(peer.join().unwrap().is_empty());
}
#[test]
fn partial_acceptance_retains_account_resources_after_local_cancellation() {
    let (_dir, c, mut g, _auth, peer) = make(false, 1);
    g.submit(c, 60, until()).unwrap();
    drive(&mut g, Status::PartiallyAccepted);
    g.cancel_pending().unwrap();
    assert_eq!(g.status(intent()), Some(Status::Unknown));
    assert!(g.halted());
    assert_eq!(g.journal_position(), 1);
    assert_eq!(
        g.account().asset(fixture::key(2)).unwrap().reserved,
        fixture::units(60)
    );
    drop(g);
    let _ = peer.join().unwrap();
}
#[test]
fn futures_admission_uses_real_capital_hold_without_scalar_budget() {
    let (_dir, c, mut g, _auth, peer) = make(true, 16384);
    g.submit(c, 60, until()).unwrap();
    assert_eq!(g.journal_position(), 1);
    assert!(g.account().venue_shadow(1).unwrap().atoms() < fixture::units(1000).atoms());
    drive(&mut g, Status::Flushed);
    drop(g);
    assert!(!peer.join().unwrap().is_empty());
}
