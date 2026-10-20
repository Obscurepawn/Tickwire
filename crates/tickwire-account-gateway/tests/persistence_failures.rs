mod common;
#[path = "common/fault_backend.rs"]
mod fault_backend;
use common::*;
use fault_backend::*;
use std::{
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
    thread,
    time::Instant,
};
use tickwire_account_gateway::*;
use tickwire_account_journal::FileBackend;
use tickwire_transport::IoBudget;
use tickwire_venue::CanonicalCommand;

type FaultGateway = AccountGateway<FaultBackend, MockAuthority, TestEncoder>;
struct Harness {
    dir: Directory,
    command: CanonicalCommand,
    gateway: FaultGateway,
    authority: MockAuthority,
    fault: Arc<AtomicU8>,
    peer: thread::JoinHandle<Vec<u8>>,
}
fn harness() -> Harness {
    let dir = Directory::new();
    let (baseline, command) = baseline(false);
    let config = config(&baseline);
    let (connection, peer) = peer(&config.route.authority, 16384);
    let authority = MockAuthority::default();
    let fault = Arc::new(AtomicU8::new(0));
    let backend = FaultBackend {
        inner: FileBackend::open(&dir.0).unwrap(),
        next: fault.clone(),
    };
    let gateway = AccountGateway::create(
        backend,
        config,
        baseline,
        authority.clone(),
        TestEncoder,
        connection,
    )
    .unwrap();
    Harness {
        dir,
        command,
        gateway,
        authority,
        fault,
        peer,
    }
}
fn reopen(dir: &Directory, authority: MockAuthority) -> Gateway {
    let (baseline, _) = baseline(false);
    let mut cfg = config(&baseline);
    cfg.writer_epoch = 5;
    AccountGateway::recover(
        FileBackend::open(&dir.0).unwrap(),
        cfg,
        authority,
        TestEncoder,
        None,
    )
    .unwrap()
}
fn assert_closed(g: &mut FaultGateway) {
    assert!(g.halted());
    assert!(g.account().frozen());
    assert!(matches!(
        g.poll(Instant::now(), IoBudget::default()),
        Err(Error::Halted)
    ));
}

fn admit_failure(mode: u8) {
    let Harness {
        dir,
        command,
        mut gateway,
        authority,
        fault,
        peer,
    } = harness();
    fault.store(mode, Ordering::SeqCst);
    assert!(matches!(
        gateway.submit(command.clone(), 60, until()),
        Err(Error::Journal(_))
    ));
    assert_eq!(fault.load(Ordering::SeqCst), 0, "fault was exercised");
    assert_closed(&mut gateway);
    assert_eq!(gateway.journal_position(), 0);
    assert_eq!(
        gateway.account().asset(fixture::key(2)).unwrap().reserved,
        fixture::units(60)
    );
    assert!(gateway.committed(intent()).is_none());
    assert_eq!(authority.policy.lock().unwrap().gate_calls, 0);
    drop(gateway);
    assert!(peer.join().unwrap().is_empty());
    let mut recovered = reopen(&dir, authority);
    let complete = mode == LOST_SYNC_REPLY;
    assert_eq!(recovered.journal_position(), u64::from(complete));
    assert_eq!(
        recovered.status(intent()),
        complete.then_some(Status::Unknown)
    );
    assert_eq!(
        recovered.account().asset(fixture::key(2)).unwrap().reserved,
        fixture::units(if complete { 60 } else { 0 })
    );
    assert!(recovered.halted() && recovered.account().frozen());
    assert!(recovered.committed(intent()).is_none());
    assert!(matches!(
        recovered.poll(Instant::now(), IoBudget::default()),
        Err(Error::Halted)
    ));
}
#[test]
fn torn_admit_retains_live_reserve_without_output_then_recovers_empty_prefix() {
    admit_failure(TORN_APPEND);
}
#[test]
fn lost_admit_sync_reply_recovers_unknown_without_issuing_send_capability() {
    admit_failure(LOST_SYNC_REPLY);
}

fn abort_failure(mode: u8) {
    let Harness {
        dir,
        command,
        mut gateway,
        authority,
        fault,
        peer,
    } = harness();
    gateway.submit(command, 60, until()).unwrap();
    let required = gateway
        .committed(intent())
        .unwrap()
        .receipt()
        .physical_frontier()
        .clone();
    fault.store(mode, Ordering::SeqCst);
    assert!(matches!(gateway.cancel_pending(), Err(Error::Journal(_))));
    assert_eq!(fault.load(Ordering::SeqCst), 0);
    assert_closed(&mut gateway);
    assert_eq!(gateway.journal_position(), 1);
    assert_eq!(gateway.status(intent()), Some(Status::Unknown));
    assert_eq!(
        gateway.account().asset(fixture::key(2)).unwrap().reserved,
        fixture::units(60)
    );
    drop(gateway);
    assert!(peer.join().unwrap().is_empty());
    let (baseline, _) = baseline(false);
    let mut cfg = config(&baseline);
    cfg.writer_epoch = 5;
    let recovered = Gateway::recover(
        FileBackend::open(&dir.0).unwrap(),
        cfg,
        authority,
        TestEncoder,
        Some(&required),
    )
    .unwrap();
    let complete = mode == LOST_SYNC_REPLY;
    assert_eq!(recovered.journal_position(), if complete { 2 } else { 1 });
    assert_eq!(
        recovered.status(intent()),
        Some(if complete {
            Status::Aborted
        } else {
            Status::Unknown
        })
    );
    assert_eq!(
        recovered.account().asset(fixture::key(2)).unwrap().reserved,
        fixture::units(if complete { 0 } else { 60 })
    );
    assert!(recovered.halted() && recovered.account().frozen());
}
#[test]
fn torn_abort_never_releases_before_sync_and_keeps_committed_admission() {
    abort_failure(TORN_APPEND);
}
#[test]
fn lost_abort_sync_reply_releases_only_when_replayed_from_complete_tail() {
    abort_failure(LOST_SYNC_REPLY);
}

fn event_failure(mode: u8) {
    let Harness {
        dir,
        command,
        mut gateway,
        authority,
        fault,
        peer,
    } = harness();
    gateway.submit(command.clone(), 60, until()).unwrap();
    let before = gateway.account().asset(fixture::key(2)).unwrap();
    let ledger = gateway.account().ledger_sequence();
    let event = fixture::event(fixture::fill(
        command.command(),
        "persisted-fill",
        "0.0005",
        "60000",
        "30",
    ));
    fault.store(mode, Ordering::SeqCst);
    assert!(matches!(
        gateway.record_event(event.clone()),
        Err(Error::Journal(_))
    ));
    assert_eq!(fault.load(Ordering::SeqCst), 0);
    assert_closed(&mut gateway);
    assert_eq!(gateway.journal_position(), 1);
    assert_eq!(gateway.account().asset(fixture::key(2)).unwrap(), before);
    assert_eq!(gateway.account().ledger_sequence(), ledger);
    // Poisoning cannot be bypassed by accepting another fact in the same instance.
    assert!(matches!(
        gateway.record_event(event),
        Err(Error::Journal(_))
    ));
    assert_eq!(gateway.journal_position(), 1);
    drop(gateway);
    assert!(peer.join().unwrap().is_empty());
    let recovered = reopen(&dir, authority);
    let complete = mode == LOST_SYNC_REPLY;
    assert_eq!(recovered.journal_position(), if complete { 2 } else { 1 });
    let quote = recovered.account().asset(fixture::key(2)).unwrap();
    assert_eq!(
        quote.reserved,
        fixture::units(if complete { 30 } else { 60 })
    );
    assert_eq!(
        quote.total,
        fixture::units(if complete { 970 } else { 1000 })
    );
    assert_eq!(recovered.status(intent()), Some(Status::Unknown));
    assert!(recovered.halted() && recovered.account().frozen());
}
#[test]
fn torn_venue_event_is_not_applied_before_wal_completion() {
    event_failure(TORN_APPEND);
}
#[test]
fn lost_venue_event_sync_reply_replays_complete_fact_without_output() {
    event_failure(LOST_SYNC_REPLY);
}

#[test]
fn observed_order_rejects_local_abort_before_writing_an_unreplayable_record() {
    let Harness {
        dir,
        command,
        mut gateway,
        authority,
        peer,
        ..
    } = harness();
    gateway.submit(command.clone(), 60, until()).unwrap();
    gateway
        .record_event(fixture::event(fixture::fill(
            command.command(),
            "early-fill",
            "0.0005",
            "60000",
            "30",
        )))
        .unwrap();
    assert_eq!(gateway.journal_position(), 2);
    let quote = gateway.account().asset(fixture::key(2)).unwrap();
    assert!(matches!(gateway.cancel_pending(), Err(Error::Account(_))));
    assert_closed(&mut gateway);
    assert_eq!(
        gateway.journal_position(),
        2,
        "a locally known impossible abort must not contaminate the durable prefix"
    );
    assert_eq!(gateway.account().asset(fixture::key(2)).unwrap(), quote);
    drop(gateway);
    assert!(peer.join().unwrap().is_empty());
    let recovered = reopen(&dir, authority);
    assert_eq!(recovered.journal_position(), 2);
    assert_eq!(recovered.account().asset(fixture::key(2)).unwrap(), quote);
    assert_eq!(recovered.status(intent()), Some(Status::Unknown));
}
