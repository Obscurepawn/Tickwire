use super::*;

fn fixture() -> (Checkpoint, tickwire_account_codec::CanonicalBaseline) {
    let (bank, replica, _) = super::super::tests::market_fixture();
    let app = Application::new(crate::ObserveOnly, 100, vec![(bank, replica, 4)]).unwrap();
    let (snapshot, catalog, _) = crate::account_fixture::fixture();
    let baseline = CanonicalBaseline::new(snapshot, catalog).unwrap();
    let mut runtime = RuntimeState {
        now_ns: 2_000_000_000,
        config: VersionedBytes {
            revision: 1,
            bytes: b"config".to_vec(),
        },
        models: vec![],
        markets: app.market_cursors().unwrap(),
        accounts: vec![],
        external: vec![],
        timers: vec![],
        rng: Rng::Disabled,
        effects: EffectCut {
            count: 0,
            digest: [0; 32],
        },
    };
    let mut app = app;
    let stream = account::AccountReplica::stream(&baseline);
    app.apply(
        &Input::AccountCallback {
            stream: stream.clone(),
            position: 0,
            canonical: baseline.bytes().to_vec(),
        },
        &mut ReplayContext {
            runtime: &mut runtime,
            views: None,
        },
        &mut ReplayEffects {
            commands: vec![],
            allowed: true,
        },
    )
    .unwrap();
    runtime.accounts.push(Cursor {
        stream,
        sequence: 0,
        content_digest: baseline.digest(),
    });
    let checkpoint = Checkpoint::new(
        Source {
            logical_strategy: 8,
            instance: [1; 16],
            generation: 1,
            binary_digest: [2; 32],
            state_schema: 1,
            input_stream: [3; 16],
            clock: ClockDomain {
                boot_id: [4; 16],
                generation: 1,
            },
        },
        0,
        [0; 32],
        app.checkpoint().unwrap(),
        runtime,
    )
    .unwrap();
    (checkpoint, baseline)
}
fn change(c: &Checkpoint, edit: impl FnOnce(&mut Saved, &mut RuntimeState)) -> Checkpoint {
    let mut saved: Saved = serde_json::from_slice(&c.business).unwrap();
    let mut runtime = c.runtime.clone();
    edit(&mut saved, &mut runtime);
    Checkpoint::new(
        c.source.clone(),
        c.cut.sequence,
        c.cut.chain,
        canonical(&saved).unwrap(),
        runtime,
    )
    .unwrap()
}
#[test]
fn current_views_expose_real_baseline_market_and_cursors_without_strategy() {
    let (c, baseline) = fixture();
    let v = inspect(&c).unwrap();
    assert_eq!(v.accounts().len(), 1);
    assert_eq!(v.accounts()[0].baseline(), &baseline);
    assert_eq!(v.accounts()[0].cursor(), &c.runtime.accounts[0]);
    assert_eq!(v.accounts()[0].position(), 0);
    assert_eq!(
        v.accounts()[0].input_digest(),
        tickwire_account_codec::InputSeal::new(baseline.digest())
            .unwrap()
            .digest()
    );
    assert_eq!(v.markets()[0].instruments(), &[(1001, 5)]);
    assert_eq!(v.markets()[0].cursor(), &c.runtime.markets[0]);
}
#[test]
fn rehashed_current_views_reject_missing_extra_duplicate_and_cursor_mismatch() {
    let (c, _) = fixture();
    for case in 0..8 {
        let changed = change(&c, |s, r| match case {
            0 => s.accounts.clear(),
            1 => r.accounts.clear(),
            2 => s.accounts.push(SavedAccount {
                stream: s.accounts[0].stream.clone(),
                view: s.accounts[0].view.clone(),
            }),
            3 => r.accounts[0].sequence += 1,
            4 => s.markets.clear(),
            5 => r.markets.clear(),
            6 => s.markets.push(SavedMarket {
                resource: s.markets[0].resource.clone(),
                maximum_levels: 4,
                view: s.markets[0].view.clone(),
            }),
            _ => r.markets[0].sequence += 1,
        });
        assert!(inspect(&changed).is_err(), "mutation {case}");
    }
}

#[test]
fn changed_current_producer_and_baseline_are_not_replaced_by_prior_values() {
    let (c, baseline) = fixture();
    let mut changed_baseline = baseline.snapshot().clone();
    changed_baseline.scope.account += 1;
    let actual = CanonicalBaseline::new(changed_baseline, baseline.catalog().clone()).unwrap();
    let changed = change(&c, |s, r| {
        let stream = account::AccountReplica::stream(&actual);
        let replica = account::AccountReplica::from_baseline(&stream, actual.bytes()).unwrap();
        s.accounts[0] = SavedAccount {
            stream: stream.clone(),
            view: replica.checkpoint().unwrap(),
        };
        r.accounts[0].stream = stream;
        r.accounts[0].content_digest = actual.digest();
        let mut bank = ResourceHeader::decode(&s.markets[0].resource).unwrap();
        bank.producer_instance = [77; 16];
        bank.descriptor_hash = [0; 32];
        s.markets[0].resource = crate::bootstrap::wire_bytes(&bank).unwrap();
        r.markets[0].stream = market_stream(&bank).unwrap();
    });
    let views = inspect(&changed).unwrap();
    assert_eq!(views.accounts()[0].baseline(), &actual);
    assert_ne!(views.accounts()[0].baseline(), &baseline);
    assert_eq!(views.markets()[0].bank().producer_instance, [77; 16]);
}

#[test]
fn malformed_materialized_view_is_an_error_even_if_rehashed() {
    let (c, _) = fixture();
    for case in 0..5 {
        let changed = change(&c, |s, _| match case {
            0 => s.accounts[0].view.push(0),
            1 => s.markets[0].view.push(0),
            2 => s.markets[0].resource[0] ^= 1,
            3 => s.markets[0].maximum_levels += 1,
            _ => s.maximum_age_ns = 0,
        });
        assert!(inspect(&changed).is_err(), "malformed {case}");
    }
}

#[test]
fn nonzero_descriptor_digest_must_match_even_when_stream_is_rebound() {
    let (c, _) = fixture();
    let changed = change(&c, |s, r| {
        let mut bank = ResourceHeader::decode(&s.markets[0].resource).unwrap();
        bank.descriptor_hash = [88; 32];
        s.markets[0].resource = crate::bootstrap::wire_bytes(&bank).unwrap();
        r.markets[0].stream = market_stream(&bank).unwrap();
    });
    assert!(inspect(&changed).is_err());
}
