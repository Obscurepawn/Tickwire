mod common;
use common::*;
use std::sync::atomic::Ordering;
use tickwire_gateway::*;
use tickwire_venue::CanonicalCommand;

#[test]
fn command_is_synced_before_any_application_output() {
    let d = Directory::new();
    let policy = MockAuthorization::default();
    let control = policy.state.clone();
    let (connection, peer) = peer(17);
    let mut s = JournaledSender::create(
        FailingBackend::open(&d),
        config(),
        policy,
        TestEncoder::default(),
        connection,
    )
    .unwrap();
    let c = canonical();
    assert_eq!(s.submit(record(&c), c, end()).unwrap(), Status::Queued);
    assert_eq!(s.journal_position(), 1);
    assert_eq!(control.lock().unwrap().gate_calls, 0);
    drive(&mut s, Status::AwaitingResponse);
    assert_eq!(control.lock().unwrap().gate_calls, 1);
    drop(s);
    assert!(peer.join().unwrap().starts_with(b"POST /test "));
}

#[test]
fn waiting_gate_expiry_records_abort_before_reporting_release() {
    let d = Directory::new();
    let policy = MockAuthorization::default();
    let control = policy.state.clone();
    let (connection, peer) = peer(17);
    let mut s = JournaledSender::create(
        FailingBackend::open(&d),
        config(),
        policy,
        TestEncoder::default(),
        connection,
    )
    .unwrap();
    let c = canonical();
    s.submit(record(&c), c, end()).unwrap();
    control.lock().unwrap().gate.now = 101;
    drive(&mut s, Status::Aborted);
    assert_eq!(s.journal_position(), 2);
    drop(s);
    assert!(peer.join().unwrap().is_empty());
}

#[test]
fn partial_acceptance_cancel_stays_unknown_and_duplicate_never_requeues() {
    let d = Directory::new();
    let policy = MockAuthorization::default();
    let (connection, peer) = peer(1);
    let mut s = JournaledSender::create(
        FailingBackend::open(&d),
        config(),
        policy,
        TestEncoder::default(),
        connection,
    )
    .unwrap();
    let c = canonical();
    let bytes = c.bytes().to_vec();
    let r = record(&c);
    s.submit(r, c, end()).unwrap();
    drive(&mut s, Status::Sending);
    s.cancel().unwrap();
    assert_eq!(s.status(3), Some(Status::Unknown));
    assert!(s.frozen());
    assert_eq!(s.journal_position(), 1);
    assert_eq!(
        s.submit(r, CanonicalCommand::decode(&bytes).unwrap(), end())
            .unwrap(),
        Status::Unknown
    );
    drop(s);
    let observed = peer.join().unwrap();
    assert!(observed.len() <= 1);
}

#[test]
fn failed_intent_sync_never_outputs() {
    let d = Directory::new();
    let backend = FailingBackend::open(&d);
    let failure = backend.fail.clone();
    let (connection, peer) = peer(17);
    let mut s = JournaledSender::create(
        backend,
        config(),
        MockAuthorization::default(),
        TestEncoder::default(),
        connection,
    )
    .unwrap();
    failure.store(true, Ordering::SeqCst);
    let c = canonical();
    assert!(matches!(
        s.submit(record(&c), c, end()),
        Err(Error::Journal(_))
    ));
    assert!(s.frozen());
    assert_eq!(s.journal_position(), 0);
    drop(s);
    assert!(peer.join().unwrap().is_empty());
}

#[test]
fn failed_abort_sync_does_not_publish_releasable_state() {
    let d = Directory::new();
    let backend = FailingBackend::open(&d);
    let failure = backend.fail.clone();
    let (connection, peer) = peer(17);
    let mut s = JournaledSender::create(
        backend,
        config(),
        MockAuthorization::default(),
        TestEncoder::default(),
        connection,
    )
    .unwrap();
    let c = canonical();
    s.submit(record(&c), c, end()).unwrap();
    failure.store(true, Ordering::SeqCst);
    assert!(matches!(s.cancel(), Err(Error::Journal(_))));
    assert!(s.frozen());
    assert_ne!(s.status(3), Some(Status::Aborted));
    assert_eq!(s.journal_position(), 1);
    drop(s);
    assert!(peer.join().unwrap().is_empty());
}

#[test]
fn record_and_wire_binding_mismatches_cannot_commit() {
    for variant in 0..5 {
        let d = Directory::new();
        let (connection, peer) = peer(17);
        let mut cfg = config();
        if variant == 0 {
            cfg.route.environment = tickwire_venue::Environment::Production;
        }
        let encoder = TestEncoder {
            wrong_digest: variant == 1,
            wrong_host: variant == 2,
        };
        let policy = MockAuthorization::default();
        if variant == 4 {
            policy.state.lock().unwrap().wrong_command = true;
        }
        let mut s =
            JournaledSender::create(FailingBackend::open(&d), cfg, policy, encoder, connection)
                .unwrap();
        let c = canonical();
        let mut r = record(&c);
        if variant == 3 {
            r.request.epoch += 1;
        }
        assert!(s.submit(r, c, end()).is_err());
        assert_eq!(s.journal_position(), 0);
        drop(s);
        assert!(peer.join().unwrap().is_empty());
    }
}

#[test]
fn recovered_intent_is_frozen_unknown_even_when_send_never_started() {
    let d = Directory::new();
    let policy = MockAuthorization::default();
    let saved = policy.clone();
    let (connection, peer) = peer(17);
    let mut s = JournaledSender::create(
        FailingBackend::open(&d),
        config(),
        policy,
        TestEncoder::default(),
        connection,
    )
    .unwrap();
    let c = canonical();
    let r = record(&c);
    let bytes = c.bytes().to_vec();
    s.submit(r, c, end()).unwrap();
    drop(s);
    assert!(peer.join().unwrap().is_empty());
    let mut restored = JournaledSender::recover(
        FailingBackend::open(&d),
        config(),
        saved,
        TestEncoder::default(),
        None,
    )
    .unwrap();
    assert!(restored.frozen());
    assert_eq!(restored.status(3), Some(Status::Unknown));
    assert_eq!(
        restored
            .submit(r, CanonicalCommand::decode(&bytes).unwrap(), end())
            .unwrap(),
        Status::Unknown
    );
}

#[test]
fn recovered_zero_byte_abort_stays_aborted_and_missing_command_fails() {
    let d = Directory::new();
    let policy = MockAuthorization::default();
    let saved = policy.clone();
    let (connection, peer) = peer(17);
    let mut s = JournaledSender::create(
        FailingBackend::open(&d),
        config(),
        policy,
        TestEncoder::default(),
        connection,
    )
    .unwrap();
    let c = canonical();
    let digest = c.digest();
    s.submit(record(&c), c, end()).unwrap();
    s.cancel().unwrap();
    drop(s);
    assert!(peer.join().unwrap().is_empty());
    let restored = JournaledSender::recover(
        FailingBackend::open(&d),
        config(),
        saved.clone(),
        TestEncoder::default(),
        None,
    )
    .unwrap();
    assert_eq!(restored.status(3), Some(Status::Aborted));
    drop(restored);
    saved
        .blobs
        .0
        .lock()
        .unwrap()
        .remove(&tickwire_recovery::StoreKey::Object(digest));
    assert!(
        JournaledSender::recover(
            FailingBackend::open(&d),
            config(),
            saved,
            TestEncoder::default(),
            None
        )
        .is_err()
    );
}

#[test]
fn transport_deadline_before_handshake_aborts_without_sampling_a_gate() {
    let d = Directory::new();
    let policy = MockAuthorization::default();
    let control = policy.state.clone();
    let (connection, peer) = peer(17);
    let mut s = JournaledSender::create(
        FailingBackend::open(&d),
        config(),
        policy,
        TestEncoder::default(),
        connection,
    )
    .unwrap();
    let c = canonical();
    s.submit(record(&c), c, std::time::Instant::now()).unwrap();
    drive(&mut s, Status::Aborted);
    assert_eq!(s.journal_position(), 2);
    assert_eq!(control.lock().unwrap().gate_calls, 0);
    drop(s);
    assert!(peer.join().unwrap().is_empty());
}

#[test]
fn every_final_gate_condition_is_checked_after_queueing() {
    for variant in 0..11 {
        let d = Directory::new();
        let policy = MockAuthorization::default();
        let control = policy.state.clone();
        let (connection, peer) = peer(17);
        let mut s = JournaledSender::create(
            FailingBackend::open(&d),
            config(),
            policy,
            TestEncoder::default(),
            connection,
        )
        .unwrap();
        let c = canonical();
        s.submit(record(&c), c, end()).unwrap();
        {
            let mut state = control.lock().unwrap();
            match variant {
                0 => state.gate.epoch += 1,
                1 => state.gate.strategy_generation += 1,
                2 => state.gate.config_revision += 1,
                3 => state.gate.lease_until = state.gate.now,
                4 => state.gate.enabled = false,
                5 => state.gate.market_valid = false,
                6 => state.gate.account_valid = false,
                7 => state.gate.recovered = false,
                8 => state.gate.owner_isolated = false,
                9 => state.gate.now = 100,
                10 => state.deny = true,
                _ => unreachable!(),
            }
        }
        drive(&mut s, Status::Aborted);
        assert_eq!(s.journal_position(), 2);
        assert_eq!(control.lock().unwrap().gate_calls, 1);
        drop(s);
        assert!(peer.join().unwrap().is_empty());
    }
}

#[test]
fn conflicting_duplicate_freezes_an_existing_queued_command() {
    let d = Directory::new();
    let (connection, peer) = peer(17);
    let mut s = JournaledSender::create(
        FailingBackend::open(&d),
        config(),
        MockAuthorization::default(),
        TestEncoder::default(),
        connection,
    )
    .unwrap();
    let c = canonical();
    s.submit(record(&c), c, end()).unwrap();
    let mut changed = command();
    changed.context.strategy.leg_id = 1;
    let changed = CanonicalCommand::new(changed).unwrap();
    assert!(matches!(
        s.submit(record(&changed), changed, end()),
        Err(Error::Conflict)
    ));
    assert!(s.frozen());
    assert_eq!(s.status(3), Some(Status::Unknown));
    assert_eq!(s.journal_position(), 1);
    assert!(matches!(
        s.poll(std::time::Instant::now(), Default::default()),
        Err(Error::Frozen)
    ));
    drop(s);
    assert!(peer.join().unwrap().is_empty());
}

#[test]
fn recovery_with_new_writer_keeps_original_command_epoch_and_rejects_new_submissions() {
    let d = Directory::new();
    let policy = MockAuthorization::default();
    let saved = policy.clone();
    let (connection, peer) = peer(17);
    let mut s = JournaledSender::create(
        FailingBackend::open(&d),
        config(),
        policy,
        TestEncoder::default(),
        connection,
    )
    .unwrap();
    let c = canonical();
    s.submit(record(&c), c, end()).unwrap();
    drop(s);
    assert!(peer.join().unwrap().is_empty());
    let mut cfg = config();
    cfg.writer_epoch = 5;
    let mut restored = JournaledSender::recover(
        FailingBackend::open(&d),
        cfg,
        saved,
        TestEncoder::default(),
        None,
    )
    .unwrap();
    assert_eq!(restored.status(3), Some(Status::Unknown));
    let mut c = command();
    c.context.request.intent_id = 4;
    c.context.request.intent_exec_epoch = 5;
    let c = CanonicalCommand::new(c).unwrap();
    let mut r = record(&c);
    r.request.key.intent = 4;
    r.request.epoch = 5;
    r.sequence = 2;
    assert!(matches!(restored.submit(r, c, end()), Err(Error::Frozen)));
}
