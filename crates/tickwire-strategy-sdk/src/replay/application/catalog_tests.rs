use super::*;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tickwire_instrument_catalog::Catalog;

pub(crate) fn catalog() -> Arc<Catalog> {
    let bytes = br#"{"schema":1,"venue":1,"environment":"Test","market":"Spot","catalog_version":"5","assets":[{"id":"1","version":"1","symbol":"BTC","exponent":-8},{"id":"2","version":"1","symbol":"USDT","exponent":-8}],"instruments":[{"id":"1001","version":"5","symbol":"BTCUSDT","product":"Spot","base":"1","quote":"2","settlement":"2","price_tick":"0.01","quantity_step":"0.00001","contract":{"kind":"Cash"}}]}"#;
    Arc::new(Catalog::decode(bytes, tickwire_recovery::digest(bytes)).unwrap())
}
struct Calls(Arc<AtomicUsize>);
impl crate::Strategy for Calls {
    fn checkpoint(&self) -> crate::Result<Vec<u8>> {
        Ok(vec![1])
    }
    fn on_market(
        &mut self,
        _: crate::MarketView<'_>,
        _: u64,
    ) -> crate::Result<Option<CanonicalCommand>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(None)
    }
}
fn runtime(app: &Application<Calls>) -> RuntimeState {
    RuntimeState {
        now_ns: 1_500_000_000,
        config: VersionedBytes {
            revision: 1,
            bytes: vec![1],
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
    }
}
#[test]
fn catalog_market_callback_remains_closed_until_actual_account_baseline() {
    let (bank, mut replica, record) = super::tests::market_fixture();
    replica.apply(&record, &[]).unwrap();
    assert!(replica.view(1001).unwrap().live());
    let key = market_stream(&bank).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut app = Application::new_with_catalog(
        Calls(calls.clone()),
        1_000_000_000,
        vec![(bank, replica, 4)],
        Some(catalog()),
    )
    .unwrap();
    let mut runtime = runtime(&app);
    runtime.now_ns = 1_600_000_001;
    let result = app.apply(
        &Input::MarketCallback {
            stream: key,
            instrument: 1001,
            invoked: false,
            maximum_age_ns: 1_000_000_000,
        },
        &mut ReplayContext {
            runtime: &mut runtime,
            views: None,
        },
        &mut ReplayEffects {
            allowed: true,
            commands: vec![],
        },
    );
    assert!(
        result.is_ok(),
        "pre-baseline callback must be recorded as not invoked: {result:?}"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

pub(crate) fn baseline(catalog: &Catalog) -> tickwire_account_codec::CanonicalBaseline {
    let (mut snapshot, mut policies, _) = crate::account_fixture::fixture();
    for a in &mut snapshot.assets {
        a.asset = catalog.asset(a.asset.id).unwrap().clone();
    }
    policies.entries[0].instrument = catalog.instrument(1001).unwrap().binding().clone();
    tickwire_account_codec::CanonicalBaseline::new(snapshot, policies).unwrap()
}
fn apply_report(
    app: &mut Application<Calls>,
    base: &tickwire_account_codec::CanonicalBaseline,
) -> Result<()> {
    let mut runtime = runtime(app);
    app.apply(
        &Input::AccountCallback {
            stream: account::AccountReplica::stream(base),
            position: 0,
            canonical: base.bytes().to_vec(),
        },
        &mut ReplayContext {
            runtime: &mut runtime,
            views: None,
        },
        &mut ReplayEffects {
            allowed: true,
            commands: vec![],
        },
    )
}
#[test]
fn catalog_baseline_economics_are_checked_before_callback_and_state_change() {
    for case in 0..5 {
        let catalog = catalog();
        let valid = baseline(&catalog);
        let mut snapshot = valid.snapshot().clone();
        let mut policies = valid.catalog().clone();
        match case {
            0 => {
                policies.entries[0].instrument.price_tick =
                    tickwire_venue::Decimal::new(2, -2).unwrap()
            }
            1 => {
                policies.entries[0].instrument.quantity_step =
                    tickwire_venue::Decimal::new(2, -5).unwrap()
            }
            2 => policies.entries[0].instrument.catalog_hash = [66; 32],
            3 => {
                snapshot.assets[0].asset.version += 1;
                policies.entries[0].instrument.base.version += 1;
            }
            _ => snapshot.scope.venue += 1,
        }
        let bad = tickwire_account_codec::CanonicalBaseline::new(snapshot, policies).unwrap();
        let (bank, replica, _) = super::tests::market_fixture();
        let calls = Arc::new(AtomicUsize::new(0));
        let mut app = Application::new_with_catalog(
            Calls(calls.clone()),
            1_000_000_000,
            vec![(bank, replica, 4)],
            Some(catalog),
        )
        .unwrap();
        let before = app.checkpoint().unwrap();
        assert!(apply_report(&mut app, &bad).is_err(), "mutation {case}");
        assert_eq!(app.checkpoint().unwrap(), before);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        apply_report(&mut app, &valid).unwrap();
        assert!(app.callbacks_ready());
    }
}
#[test]
fn catalog_saved_binding_requires_original_body_before_any_restore_callback() {
    let cat = catalog();
    let (bank, replica, _) = super::tests::market_fixture();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut app = Application::new_with_catalog(
        Calls(calls.clone()),
        1_000_000_000,
        vec![(bank, replica, 4)],
        Some(cat.clone()),
    )
    .unwrap();
    apply_report(&mut app, &baseline(&cat)).unwrap();
    let bytes = app.checkpoint().unwrap();
    let saved: Saved = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(saved.instrument_catalog, Some(crate::catalog::object(&cat)));
    assert!(
        Application::<Calls>::restore(&bytes, |_| panic!("missing dependency called user restore"))
            .is_err()
    );
    let mut wrong = cat.artifact();
    let mut value: serde_json::Value = serde_json::from_slice(&wrong.bytes).unwrap();
    value["instruments"][0]["price_tick"] = "0.02".into();
    wrong.bytes = serde_json::to_vec(&value).unwrap();
    wrong.digest = tickwire_recovery::digest(&wrong.bytes);
    assert!(
        Application::<Calls>::restore_with_catalog(
            &bytes,
            Some(Arc::new(wrong.catalog().unwrap())),
            |_| panic!("substituted dependency called user restore")
        )
        .is_err()
    );
    let restored =
        Application::restore_with_catalog(&bytes, Some(cat), |_| Ok(Calls(calls.clone()))).unwrap();
    assert_eq!(restored.checkpoint().unwrap(), bytes);
}
#[test]
fn catalog_does_not_embed_body_in_every_checkpoint_and_legacy_bytes_stay_unchanged() {
    let (bank, replica, _) = super::tests::market_fixture();
    let app = Application::new(
        Calls(Arc::new(AtomicUsize::new(0))),
        1_000_000_000,
        vec![(bank, replica, 4)],
    )
    .unwrap();
    let legacy = app.checkpoint().unwrap();
    assert!(
        !serde_json::from_slice::<serde_json::Value>(&legacy)
            .unwrap()
            .as_object()
            .unwrap()
            .contains_key("instrument_catalog")
    );
    let cat = catalog();
    let (bank, replica, _) = super::tests::market_fixture();
    let current = Application::new_with_catalog(
        Calls(Arc::new(AtomicUsize::new(0))),
        1_000_000_000,
        vec![(bank, replica, 4)],
        Some(cat),
    )
    .unwrap()
    .checkpoint()
    .unwrap();
    assert!(current.len() < legacy.len() + 256);
}

#[test]
fn catalog_validated_members_use_replica_index_to_reject_unknown_or_wrong_version() {
    use tickwire_ipc::Wire;
    for wrong_version in [false, true] {
        let (bank, replica, raw) = super::tests::market_fixture();
        let stream = market_stream(&bank).unwrap();
        let mut app = Application::new_with_catalog(
            Calls(Arc::new(AtomicUsize::new(0))),
            1_000_000_000,
            vec![(bank, replica, 4)],
            Some(catalog()),
        )
        .unwrap();
        let mut record = tickwire_ipc::MarketRecord::decode(&raw).unwrap();
        if wrong_version {
            record.header.instrument_version += 1;
        } else {
            record.header.instrument_id = u64::MAX;
        }
        record.header.record_crc32c = 0;
        let mut bytes = vec![0; 512];
        record.encode(&mut bytes).unwrap();
        // The full fixed ABI remains valid; rejection comes from actual member identity.
        tickwire_ipc::MarketRecord::decode(&bytes).unwrap();
        let input = Input::MarketApplied {
            stream,
            sequence: record.header.sequence,
            received_ns: record.header.ingress_mono_ns,
            record: bytes,
            payload: vec![],
        };
        let mut runtime = runtime(&app);
        let mut effects = ReplayEffects {
            allowed: true,
            commands: vec![],
        };
        assert!(
            app.apply(
                &input,
                &mut ReplayContext {
                    runtime: &mut runtime,
                    views: None
                },
                &mut effects
            )
            .is_err()
        );
        assert!(effects.commands.is_empty());
    }
}

#[test]
fn catalog_object_retains_exact_raw_digest_and_length_for_large_equivalent_body() {
    let compact = catalog();
    let mut padded = vec![b' '; tickwire_instrument_catalog::MAX_BYTES - compact.raw_bytes().len()];
    padded.extend_from_slice(compact.raw_bytes());
    let full = Catalog::decode(&padded, tickwire_recovery::digest(&padded)).unwrap();
    assert_eq!(compact.catalog_hash(), full.catalog_hash());
    assert_ne!(compact.artifact_hash(), full.artifact_hash());
    for catalog in [compact.as_ref(), &full] {
        assert_eq!(
            crate::catalog::object(catalog),
            Object::of(catalog.raw_bytes())
        );
    }
    assert_eq!(
        crate::catalog::object(&full).bytes,
        tickwire_instrument_catalog::MAX_BYTES as u64
    );
}
