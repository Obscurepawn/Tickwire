mod common;
use common::*;
use tickwire_account::Applied;
use tickwire_account_gateway::*;
use tickwire_venue::CanonicalEvent;

const STREAM: [u8; 16] = [7; 16];

fn event(c: &tickwire_venue::CanonicalCommand, trade: &str) -> CanonicalEvent {
    fixture::event(fixture::fill(c.command(), trade, "0.0001", "60000", "6"))
}

#[test]
fn ingress_input_retry_does_not_repeat_reducer() {
    let (c, mut g, _auth, peer) = make(false, 16384);
    g.submit(c.clone(), 60, until()).unwrap();
    let e = event(&c, "once");
    assert_eq!(
        g.record_ingress_event(STREAM, 1, e.clone()).unwrap(),
        Applied::New
    );
    let view = g.account().asset(fixture::key(2)).unwrap();
    let seq = g.account().ledger_sequence();
    assert_eq!(g.command(intent()), Some(&c));
    assert_eq!(g.baseline().digest(), baseline(false).0.digest());
    assert_eq!(
        g.record_ingress_event(STREAM, 1, e).unwrap(),
        Applied::Duplicate
    );
    assert_eq!(g.account().ledger_sequence(), seq);
    assert_eq!(g.account().asset(fixture::key(2)).unwrap(), view);
    drop(g);
    assert!(peer.join().unwrap().is_empty());
}

#[test]
fn conflicting_ingress_input_halts_without_mutating_ledger() {
    let (c, mut g, _auth, peer) = make(false, 16384);
    g.submit(c.clone(), 60, until()).unwrap();
    g.record_ingress_event(STREAM, 1, event(&c, "original"))
        .unwrap();
    let seq = g.account().ledger_sequence();
    assert!(matches!(
        g.record_ingress_event(STREAM, 1, event(&c, "changed")),
        Err(Error::Conflict)
    ));
    assert!(g.halted() && g.account().frozen());
    assert_eq!(g.account().ledger_sequence(), seq);
    drop(g);
    assert!(peer.join().unwrap().is_empty());
}

#[test]
fn invalid_event_freezes_account_without_halting_fact_processing() {
    let (c, mut g, _auth, peer) = make(false, 16384);
    assert!(matches!(
        g.record_ingress_event(STREAM, 1, event(&c, "before-admit")),
        Err(Error::Account(_))
    ));
    assert!(g.account().frozen());
    assert!(!g.halted());
    drop(g);
    assert!(peer.join().unwrap().is_empty());
}

#[test]
fn explicit_gap_freeze_preserves_late_fact_admission() {
    let (c, mut g, _auth, peer) = make(false, 16384);
    g.submit(c.clone(), 60, until()).unwrap();
    g.freeze_account();
    assert!(g.account().frozen());
    assert!(!g.halted());
    assert_eq!(
        g.record_ingress_event(STREAM, 1, event(&c, "late"))
            .unwrap(),
        Applied::New
    );
    drop(g);
    assert!(peer.join().unwrap().is_empty());
}
