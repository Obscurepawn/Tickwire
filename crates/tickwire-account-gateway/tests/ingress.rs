mod common;
#[path = "common/fault_backend.rs"]
mod fault_backend;
use common::*;
use fault_backend::*;
use std::sync::{
    Arc,
    atomic::{AtomicU8, Ordering},
};
use tickwire_account::Applied;
use tickwire_account_gateway::*;
use tickwire_account_journal::FileBackend;
use tickwire_venue::CanonicalEvent;
const STREAM: [u8; 16] = [7; 16];
fn event(c: &tickwire_venue::CanonicalCommand, trade: &str) -> CanonicalEvent {
    fixture::event(fixture::fill(c.command(), trade, "0.0001", "60000", "6"))
}
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
#[test]
fn committed_input_retry_and_restart_do_not_repeat_reducer_or_wal() {
    let (dir, c, mut g, auth, peer) = make(false, 16384);
    g.submit(c.clone(), 60, until()).unwrap();
    let e = event(&c, "once");
    assert_eq!(
        g.record_ingress_event(STREAM, 1, e.clone()).unwrap(),
        Applied::New
    );
    let view = g.account().asset(fixture::key(2)).unwrap();
    let seq = g.account().ledger_sequence();
    assert_eq!(g.ingress_event(STREAM, 1), Some(&e));
    assert_eq!(
        g.ingress_events().collect::<Vec<_>>(),
        vec![(STREAM, 1, &e)]
    );
    assert_eq!(g.journal_config().physical.stream_epoch, [2; 16]);
    assert_eq!(g.command(intent()), Some(&c));
    assert_eq!(g.baseline().digest(), baseline(false).0.digest());
    assert_eq!(
        g.record_ingress_event(STREAM, 1, e.clone()).unwrap(),
        Applied::Duplicate
    );
    assert_eq!(g.journal_position(), 2);
    assert_eq!(g.account().ledger_sequence(), seq);
    assert_eq!(g.account().asset(fixture::key(2)).unwrap(), view);
    drop(g);
    assert!(peer.join().unwrap().is_empty());
    let mut g = reopen(&dir, auth);
    assert_eq!(
        g.record_ingress_event(STREAM, 1, e).unwrap(),
        Applied::Duplicate
    );
    assert_eq!(g.account().ledger_sequence(), seq);
    assert_eq!(g.journal_position(), 2);
    assert!(g.account().frozen() && g.halted());
    assert_eq!(
        g.record_ingress_event(STREAM, 2, event(&c, "after-recovery"))
            .unwrap(),
        Applied::New
    );
    assert_eq!(g.journal_position(), 3);
}
#[test]
fn conflicting_input_halts_without_appending_or_publishing() {
    let (_dir, c, mut g, _auth, peer) = make(false, 16384);
    g.submit(c.clone(), 60, until()).unwrap();
    g.record_ingress_event(STREAM, 1, event(&c, "original"))
        .unwrap();
    let seq = g.account().ledger_sequence();
    assert!(matches!(
        g.record_ingress_event(STREAM, 1, event(&c, "changed")),
        Err(Error::Conflict)
    ));
    assert!(g.halted() && g.account().frozen());
    assert_eq!(g.journal_position(), 2);
    assert_eq!(g.account().ledger_sequence(), seq);
    drop(g);
    assert!(peer.join().unwrap().is_empty());
}
#[test]
fn invalid_event_is_not_logged_and_freeze_does_not_halt_fact_processing() {
    let (_dir, c, mut g, _auth, peer) = make(false, 16384);
    assert!(matches!(
        g.record_ingress_event(STREAM, 1, event(&c, "before-admit")),
        Err(Error::Account(_))
    ));
    assert!(g.account().frozen());
    assert!(!g.halted());
    assert_eq!(g.journal_position(), 0);
    assert!(g.ingress_event(STREAM, 1).is_none());
    drop(g);
    assert!(peer.join().unwrap().is_empty());
}
#[test]
fn explicit_gap_freeze_preserves_late_fact_admission() {
    let (_dir, c, mut g, _auth, peer) = make(false, 16384);
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
fn fault_case(mode: u8) {
    let dir = Directory::new();
    let (b, c) = baseline(false);
    let cfg = config(&b);
    let (connection, peer) = peer(&cfg.route.authority, 16384);
    let auth = MockAuthority::default();
    let fault = Arc::new(AtomicU8::new(0));
    let backend = FaultBackend {
        inner: FileBackend::open(&dir.0).unwrap(),
        next: fault.clone(),
    };
    let mut g =
        AccountGateway::create(backend, cfg, b, auth.clone(), TestEncoder, connection).unwrap();
    g.submit(c.clone(), 60, until()).unwrap();
    let before = g.account().asset(fixture::key(2)).unwrap();
    let e = event(&c, "uncertain");
    fault.store(mode, Ordering::SeqCst);
    assert!(matches!(
        g.record_ingress_event(STREAM, 1, e.clone()),
        Err(Error::Journal(_))
    ));
    assert_eq!(fault.load(Ordering::SeqCst), 0);
    assert!(g.halted() && g.account().frozen());
    assert_eq!(g.journal_position(), 1);
    assert_eq!(g.account().asset(fixture::key(2)).unwrap(), before);
    assert!(g.ingress_event(STREAM, 1).is_none());
    drop(g);
    assert!(peer.join().unwrap().is_empty());
    let mut g = reopen(&dir, auth);
    let survived = mode == LOST_SYNC_REPLY;
    assert_eq!(g.journal_position(), if survived { 2 } else { 1 });
    assert_eq!(g.ingress_event(STREAM, 1).is_some(), survived);
    assert_eq!(
        g.record_ingress_event(STREAM, 1, e).unwrap(),
        if survived {
            Applied::Duplicate
        } else {
            Applied::New
        }
    );
    assert_eq!(g.journal_position(), 2);
}
#[test]
fn lost_ingress_sync_reply_recovers_complete_record_once() {
    fault_case(LOST_SYNC_REPLY);
}
#[test]
fn torn_ingress_append_recovers_prefix_then_accepts_retry() {
    fault_case(TORN_APPEND);
}
