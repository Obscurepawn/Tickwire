mod common;
use common::*;
use tickwire_account::Applied;
use tickwire_account_codec::CanonicalBaseline;
use tickwire_account_gateway::AccountGateway;
use tickwire_account_journal::FileBackend;
use tickwire_venue as v;

fn reopen(
    dir: &Directory,
    authority: MockAuthority,
    baseline: &CanonicalBaseline,
) -> tickwire_account_gateway::Result<Gateway> {
    let mut config = config(baseline);
    config.writer_epoch = 5;
    AccountGateway::recover(
        FileBackend::open(&dir.0).unwrap(),
        config,
        authority,
        TestEncoder,
        None,
    )
}

fn check_recovered_metadata(initial_sequence: u64) {
    let (mut snapshot, catalog, command) = fixture::fixture();
    snapshot.ledger_sequence = initial_sequence;
    let baseline = CanonicalBaseline::new(snapshot, catalog).unwrap();
    let command = fixture::canonical(command);
    let dir = Directory::new();
    let cfg = config(&baseline);
    let (connection, peer) = peer(&cfg.route.authority, 1);
    let authority = MockAuthority::default();
    let mut gateway = AccountGateway::create(
        FileBackend::open(&dir.0).unwrap(),
        cfg,
        baseline.clone(),
        authority.clone(),
        TestEncoder,
        connection,
    )
    .unwrap();
    gateway.submit(command.clone(), 60, until()).unwrap();
    let original_image = gateway.account().admission(intent()).unwrap().image();
    let key = original_image.orders[0].clone();
    drop(gateway);
    assert!(peer.join().unwrap().is_empty());

    let mut restored = reopen(&dir, authority.clone(), &baseline).unwrap();
    assert!(restored.account().order(&key).unwrap().unknown);
    assert!(restored.committed(intent()).is_none());
    let mut trade = fixture::fill(command.command(), "same-trade", "0.0004", "50000", "20");
    assert_eq!(
        restored
            .record_event(fixture::event(trade.clone()))
            .unwrap(),
        Applied::New
    );
    let v::VenueEventBody::Fill(ref mut fill) = trade.body else {
        unreachable!()
    };
    fill.observation.status = v::OrderStatus::Unknown;
    let repeated_observation = fixture::event(trade);
    let prior_assets = [
        restored.account().asset(fixture::key(1)).unwrap(),
        restored.account().asset(fixture::key(2)).unwrap(),
    ];
    let prior_order = restored.account().order(&key).unwrap();
    let prior_image = restored.account().admission(intent()).unwrap().image();
    let result = restored.record_event(repeated_observation.clone());
    println!("initial={initial_sequence}; repeated Unknown result={result:?}");
    if initial_sequence == u64::MAX - 2 {
        assert_eq!(restored.account().ledger_sequence(), u64::MAX);
        assert_eq!(restored.journal_position(), 3);
        assert_eq!(
            restored.account().asset(fixture::key(2)).unwrap().reserved,
            fixture::units(36)
        );
        assert_eq!(
            prior_assets,
            [
                restored.account().asset(fixture::key(1)).unwrap(),
                restored.account().asset(fixture::key(2)).unwrap(),
            ]
        );
        assert_eq!(prior_order, restored.account().order(&key).unwrap());
        assert_eq!(
            prior_image,
            restored.account().admission(intent()).unwrap().image()
        );
        assert_eq!(
            restored.account().rejected_event(),
            Some(&repeated_observation)
        );
        assert!(restored.account().frozen() && restored.halted());
        drop(restored);
        let rejected = reopen(&dir, authority, &baseline);
        println!("MAX reopen error: {:?}", rejected.as_ref().err());
        assert!(
            matches!(
                rejected,
                Err(tickwire_account_gateway::Error::Journal(
                    tickwire_account_journal::Error::Account(tickwire_account::Error::Risk(
                        tickwire_risk::RiskError::ArithmeticOverflow
                    ))
                ))
            ),
            "the preserved but unapplied overflowing record must fail recovery"
        );
        assert!(
            matches!(
                result,
                Err(tickwire_account_gateway::Error::Account(
                    tickwire_account::Error::Risk(tickwire_risk::RiskError::ArithmeticOverflow)
                ))
            ),
            "new persistent observation cannot be acknowledged at the sequence limit"
        );
        return;
    }
    assert_eq!(result.unwrap(), Applied::New);
    let before_sequence = restored.account().ledger_sequence();
    let before_base = restored.account().asset(fixture::key(1)).unwrap();
    let before_quote = restored.account().asset(fixture::key(2)).unwrap();
    let before_order = restored.account().order(&key).unwrap();
    let before_image = restored.account().admission(intent()).unwrap().image();
    let before_status = restored.status(intent());
    assert_eq!(restored.journal_position(), 3);
    assert_eq!(before_image, original_image);
    drop(restored);

    let reopened = reopen(&dir, authority, &baseline).unwrap();
    let after_sequence = reopened.account().ledger_sequence();
    let after_base = reopened.account().asset(fixture::key(1)).unwrap();
    let after_quote = reopened.account().asset(fixture::key(2)).unwrap();
    let after_order = reopened.account().order(&key).unwrap();
    let after_image = reopened.account().admission(intent()).unwrap().image();
    println!("ledger sequence: {before_sequence} -> {after_sequence}");
    println!("base before={before_base:?}; after={after_base:?}");
    println!("quote before={before_quote:?}; after={after_quote:?}");
    println!("order before={before_order:?}; after={after_order:?}");
    println!("admission before={before_image:?}; after={after_image:?}");
    assert_eq!(reopened.journal_position(), 3);
    assert_eq!(before_base, after_base);
    assert_eq!(before_quote, after_quote);
    assert_eq!(before_order, after_order);
    assert_eq!(before_image, after_image);
    assert_eq!(before_status, reopened.status(intent()));
    assert!(reopened.halted() && reopened.account().frozen());
    assert!(reopened.committed(intent()).is_none());
    assert_eq!(before_sequence, after_sequence);
    let mut account = reopened.account().clone();
    assert_eq!(
        account.apply_event(&repeated_observation).unwrap(),
        Applied::Duplicate
    );
    assert_eq!(account.ledger_sequence(), after_sequence);
}

#[test]
fn repeated_unknown_after_recovery_preserves_account_and_metadata_sequence() {
    check_recovered_metadata(0);
}

#[test]
fn repeated_unknown_with_last_available_sequence_reopens_at_max() {
    check_recovered_metadata(u64::MAX - 3);
}

#[test]
fn repeated_unknown_at_max_is_not_acknowledged_as_a_duplicate() {
    check_recovered_metadata(u64::MAX - 2);
}

#[test]
fn local_freeze_does_not_hide_a_new_incomplete_terminal_observation() {
    let (snapshot, catalog, command) = fixture::fixture();
    let command = fixture::canonical(command);
    let mut live =
        tickwire_account::Account::from_snapshot(snapshot.clone(), catalog.clone()).unwrap();
    let admission = live.admit(&command, 60).unwrap();
    let mut trade = fixture::fill(command.command(), "terminal-trade", "0.0004", "50000", "20");
    let v::VenueEventBody::Fill(ref mut fill) = trade.body else {
        unreachable!()
    };
    fill.observation.status = v::OrderStatus::Canceled;
    fill.observation.cumulative_quantity = Some("0.0004".parse().unwrap());
    let first = fixture::event(trade.clone());
    live.apply_event(&first).unwrap();
    live.freeze();
    let v::VenueEventBody::Fill(ref mut fill) = trade.body else {
        unreachable!()
    };
    fill.observation.cumulative_quantity = None;
    let incomplete = fixture::event(trade);
    let applied = live.apply_event(&incomplete).unwrap();
    assert_eq!(applied, Applied::New);
    let sequence = live.ledger_sequence();
    assert_eq!(live.apply_event(&incomplete).unwrap(), Applied::Duplicate);
    assert_eq!(live.ledger_sequence(), sequence);
    let recovered = tickwire_account::Account::recover(
        snapshot,
        catalog,
        &[
            tickwire_account::RecoveryRecord::Admit {
                command: Box::new(command),
                evaluated_at: 60,
                expected: admission.image(),
            },
            tickwire_account::RecoveryRecord::VenueEvent(Box::new(first)),
            tickwire_account::RecoveryRecord::VenueEvent(Box::new(incomplete)),
        ],
    )
    .unwrap();
    println!(
        "local freeze: {applied:?}; sequence {} -> {}",
        live.ledger_sequence(),
        recovered.ledger_sequence()
    );
    assert_eq!(
        live.asset(fixture::key(1)).unwrap(),
        recovered.asset(fixture::key(1)).unwrap()
    );
    assert_eq!(
        live.asset(fixture::key(2)).unwrap(),
        recovered.asset(fixture::key(2)).unwrap()
    );
    assert_eq!(
        live.admission(admission.intent()),
        recovered.admission(admission.intent())
    );
    assert!(live.frozen() && recovered.frozen());
    assert_eq!(live.ledger_sequence(), recovered.ledger_sequence());
}
