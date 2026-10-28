use super::*;
use tickwire_ipc::{DepthCoverage, DepthLevel, JoinBody, MarketRecord};
use tickwire_snapshot::*;

pub(crate) fn fixture(previous: Cursor) -> (Input, ResourceHeader, crate::MarketReplica) {
    fixture_members(previous, [1001, u64::MAX], 5)
}
fn fixture_members(
    previous: Cursor,
    ids: [u64; 2],
    version: u64,
) -> (Input, ResourceHeader, crate::MarketReplica) {
    let mut bank = ResourceHeader::decode(&tests::vector("normalized_resource_header")).unwrap();
    let old_ring = bank.ring_epoch;
    bank.resource_kind = 1;
    bank.required_features |= 64;
    bank.descriptor_hash = [0; 32];
    bank.resource_id = [71; 16];
    bank.ring_epoch += 1;
    bank.feed_generation += 1;
    bank.predecessor_ring_epoch = old_ring;
    bank.cutover_seq = previous.sequence;
    bank.first_seq = previous.sequence + 1;
    let mut resource = bank.clone();
    resource.resource_id = [72; 16];
    let mut join = JoinBody::decode(&tests::vector("join_normalized_snapshot")).unwrap();
    join.snapshot_hash = [0; 32];
    join.snapshot_id = [0; 16];
    join.bank_resource_id = bank.resource_id;
    join.ring_epoch = bank.ring_epoch;
    join.feed_generation = bank.feed_generation;
    join.snapshot_seq = previous.sequence;
    join.join_seq = previous.sequence + 1;
    join.topic_instrument_set_hash = [0; 32];
    join.topic_instrument_set_version = 0;
    join.join_flags = 3;
    let states = ids.map(|id| {
        [
            "normalized_state_quote",
            "normalized_state_trade",
            "normalized_state_mark",
            "normalized_state_feed",
        ]
        .map(|name| {
            let mut r = MarketRecord::decode(&tests::vector(name)).unwrap();
            r.header.instrument_id = id;
            r.header.instrument_version = version;
            r.header.feed_generation = bank.feed_generation;
            r.header.record_crc32c = 0;
            r
        })
    });
    let bids = [DepthLevel::decode(&tests::vector("normalized_depth_1")).unwrap()];
    let asks = [DepthLevel::decode(&tests::vector("normalized_depth_2")).unwrap()];
    let specs = ids.map(|instrument_id| InstrumentSpec {
        instrument_id,
        instrument_version: version,
        required_states: QUOTE | TRADE | MARK_FUNDING,
    });
    let members = [0, 1].map(|i| InstrumentSnapshot {
        instrument_id: specs[i].instrument_id,
        instrument_version: version,
        snapshot_id: [80 + i as u8; 16],
        at_sequence: previous.sequence,
        last_event_sequence: 990,
        feed_generation: bank.feed_generation,
        catalog_version: 5,
        catalog_hash: [0x22; 32],
        created_mono_ns: 1_500_000_000,
        expires_mono_ns: 2_000_000_000,
        state_version: 990,
        coverage_depth: 1,
        coverage_flags: 3,
        states: &states[i],
        bids: &bids,
        asks: &asks,
    });
    let built = build_topic_with_coverage(
        &BuildRequest {
            resource: &resource,
            join: &join,
            catalog_hash: [0x22; 32],
            instrument_set_version: 6,
            sequence: previous.sequence,
            created_mono_ns: 1_500_000_000,
            expires_mono_ns: 2_000_000_000,
            now_mono_ns: 1_500_000_001,
            logical_strategy_id: 0,
            instruments: &specs,
            members: &members,
            byte_limit: MAX_TOPIC_SNAPSHOT_BYTES,
        },
        &vec![
            DepthCoverage {
                version: 1,
                bid_kind: 2,
                ask_kind: 2,
                bid_boundary_ticks: bids[0].price_ticks,
                ask_boundary_ticks: asks[0].price_ticks,
                source_book_generation: bank.feed_generation
            };
            2
        ],
    )
    .unwrap();
    let bootstrap = crate::bootstrap::JoinBootstrap::new(
        &bank,
        built.resource(),
        built.join(),
        &tickwire_join::TopicRegistry {
            catalog_hash: [0x22; 32],
            instrument_set_version: 6,
            instruments: specs.to_vec(),
        },
    )
    .unwrap();
    let validated = validate(
        built.bytes(),
        &JoinContext {
            expected_resource: built.resource(),
            catalog_hash: [0x22; 32],
            instruments: &specs,
            join: built.join(),
            now_mono_ns: 1_500_000_001,
        },
    )
    .unwrap();
    let mut replica =
        crate::MarketReplica::load(join.topic_id, bank.ring_epoch, &validated, 4).unwrap();
    let mut raw = states[1][0].clone();
    raw.header.sequence = previous.sequence + 1;
    raw.header.flags = 6;
    raw.header.ingress_mono_ns = 1_600_000_000;
    raw.header.publish_mono_ns = 1_600_000_001;
    let raw = crate::bootstrap::wire_bytes(&raw).unwrap();
    replica.apply(&raw, &[]).unwrap();
    let input = Input::MarketTransition {
        operation_id: [11; 16],
        approval_digest: [12; 32],
        previous: previous.clone(),
        next: market_stream(&bank).unwrap(),
        baseline_sequence: previous.sequence,
        activated_sequence: previous.sequence + 1,
        baseline: crate::recovery::snapshot_payload(&bootstrap, built.bytes(), 1_500_000_001, 4)
            .unwrap(),
        catchup: vec![MarketTransitionRecord {
            record: raw,
            payload: vec![],
            instrument: u64::MAX,
            applied_mono_ns: 1_600_000_002,
        }],
    };
    (input, bank, replica)
}

struct Count(u64);
impl crate::Strategy for Count {
    fn checkpoint(&self) -> crate::Result<Vec<u8>> {
        Ok(self.0.to_le_bytes().to_vec())
    }
    fn on_market(
        &mut self,
        _: crate::MarketView<'_>,
        _: u64,
    ) -> crate::Result<Option<CanonicalCommand>> {
        self.0 += 1;
        Ok(None)
    }
}
struct Factory;
impl BusinessFactory for Factory {
    type Running = Count;
    fn schema(&self) -> u64 {
        7
    }
    fn restore(&self, state: &[u8], _: &RuntimeState) -> Result<Count> {
        Ok(Count(u64::from_le_bytes(
            state.try_into().map_err(|_| Error::Identity)?,
        )))
    }
}
fn initial() -> (VerifiedAnchor, Application<Count>) {
    let (resource, replica, _) = tests::market_fixture();
    let app = Application::new(Count(0), 1_000_000_000, vec![(resource, replica, 4)]).unwrap();
    let mut cp = super::super::tests::anchor_with(super::super::tests::Stateful::initial())
        .checkpoint()
        .clone();
    cp.runtime.now_ns = 1_500_000_000;
    cp.runtime.markets = app.market_cursors().unwrap();
    cp.runtime.timers.clear();
    cp.runtime.models.clear();
    let cp = Checkpoint::new(cp.source, 0, [0; 32], app.checkpoint().unwrap(), cp.runtime).unwrap();
    (
        verify_anchor(
            &cp.bytes().unwrap(),
            &cp.source,
            Object::of(&cp.runtime.config.bytes),
            &[],
        )
        .unwrap(),
        app,
    )
}

#[test]
fn transition_reconstructs_high_id_and_source_replay_without_catchup_callback() {
    let (anchor, app) = initial();
    let (input, bank, expected) = fixture(anchor.checkpoint().runtime.markets[0].clone());
    let key = market_stream(&bank).unwrap();
    let mut engine = Engine::new(&anchor, app).unwrap();
    let applied = engine.apply_live(input, 1_600_000_003).unwrap();
    assert!(applied.commands.is_empty());
    assert_eq!(
        engine.strategy.business_state().unwrap(),
        0u64.to_le_bytes()
    );
    assert_eq!(
        engine.strategy.market_states().unwrap()[&key].1,
        expected.checkpoint().unwrap()
    );
    assert_eq!(
        engine.checkpoint().unwrap().runtime.markets[0].sequence,
        1001
    );
    engine
        .apply_live(
            Input::MarketCallback {
                stream: key,
                instrument: u64::MAX,
                invoked: true,
                maximum_age_ns: 1_000_000_000,
            },
            1_600_000_004,
        )
        .unwrap();
    let end = engine.checkpoint().unwrap();
    let tail = verify_tail(
        &anchor,
        &encode_tail(&anchor, engine.recorded()).unwrap(),
        &end.cut,
    )
    .unwrap();
    let replayed = replay_source(
        &ApplicationFactory {
            binary_digest: anchor.checkpoint().source.binary_digest,
            business: Factory,
        },
        &anchor,
        &tail,
    )
    .unwrap();
    assert_eq!(replayed.checkpoint(), &end);
    assert_eq!(
        engine.strategy.business_state().unwrap(),
        1u64.to_le_bytes()
    );
}

#[test]
fn invalid_transition_leaves_application_views_unchanged() {
    let (anchor, mut app) = initial();
    let (good, _, _) = fixture(anchor.checkpoint().runtime.markets[0].clone());
    for case in 0..5 {
        let mut input = good.clone();
        if let Input::MarketTransition {
            previous,
            next,
            activated_sequence,
            baseline,
            catchup,
            ..
        } = &mut input
        {
            match case {
                0 => previous.sequence += 1,
                1 => next.identity = [9; 32],
                2 => *activated_sequence += 1,
                3 => {
                    baseline[40] ^= 1;
                }
                _ => catchup[0].record[0] ^= 1,
            }
        }
        let before = app.checkpoint().unwrap();
        let mut runtime = anchor.checkpoint().runtime.clone();
        runtime.now_ns = 1_600_000_003;
        assert!(
            app.apply(
                &input,
                &mut ReplayContext {
                    views: None,
                    runtime: &mut runtime
                },
                &mut ReplayEffects {
                    allowed: false,
                    commands: vec![]
                }
            )
            .is_err()
        );
        assert_eq!(app.checkpoint().unwrap(), before);
    }
}

#[test]
fn a_valid_snapshot_cannot_remove_or_reversion_an_existing_instrument() {
    let (anchor, app) = initial();
    let before = app.checkpoint().unwrap();
    for (ids, version) in [([1002, u64::MAX], 5), ([1001, u64::MAX], 6)] {
        let (input, _, _) =
            fixture_members(anchor.checkpoint().runtime.markets[0].clone(), ids, version);
        assert!(app.transition_state(&input, 1_600_000_003).is_err());
        assert_eq!(app.checkpoint().unwrap(), before);
    }
}

#[test]
fn continuation_finds_source_cut_after_a_transition_and_retains_only_later_records() {
    let (anchor, app) = initial();
    let (input, bank, _) = fixture(anchor.checkpoint().runtime.markets[0].clone());
    let Input::MarketTransition { catchup, .. } = &input else {
        panic!()
    };
    let mut record = tickwire_ipc::MarketRecord::decode(&catchup[0].record).unwrap();
    record.header.sequence += 1;
    record.header.record_crc32c = 0;
    let record = crate::bootstrap::wire_bytes(&record).unwrap();
    let mut engine = Engine::new(&anchor, app).unwrap();
    engine.apply_live(input, 1_600_000_003).unwrap();
    let cut = engine.checkpoint().unwrap();
    let later = Input::MarketApplied {
        stream: market_stream(&bank).unwrap(),
        sequence: 1002,
        received_ns: 1_600_000_000,
        record,
        payload: vec![],
    };
    engine.apply_live(later.clone(), 1_600_000_004).unwrap();
    let plan = super::super::catchup::plan(
        &cut,
        anchor.checkpoint(),
        engine.recorded(),
        &engine.checkpoint().unwrap(),
    )
    .unwrap();
    assert_eq!(plan.inputs(), &[later]);
    let app =
        Application::restore(&cut.business, |bytes| Factory.restore(bytes, &cut.runtime)).unwrap();
    let verified = verify_anchor(
        &cut.bytes().unwrap(),
        &cut.source,
        Object::of(&cut.runtime.config.bytes),
        &[],
    )
    .unwrap();
    let mut durable =
        durable::DurableEngine::new(verified, app, tickwire_recovery::MockBlobStore::default())
            .unwrap();
    plan.apply(&mut durable, || Ok(1_700_000_000)).unwrap();
    assert_eq!(
        durable.strategy().market_states().unwrap(),
        engine.strategy.market_states().unwrap()
    );
    assert_eq!(
        durable.strategy().business_state().unwrap(),
        1u64.to_le_bytes()
    );
}

#[test]
fn transition_lineage_requires_exact_raw_cursor_at_source_previous_and_end() {
    let (anchor, app) = initial();
    let (input, _, _) = fixture(anchor.checkpoint().runtime.markets[0].clone());
    let mut engine = Engine::new(&anchor, app).unwrap();
    engine.apply_live(input, 1_600_000_003).unwrap();
    let end = engine.checkpoint().unwrap();
    for case in 0..3 {
        let mut source = anchor.checkpoint().clone();
        let mut rows = engine.recorded().to_vec();
        let mut target = end.clone();
        match case {
            0 => source.runtime.markets[0].content_digest[0] ^= 1,
            1 => {
                let Input::MarketTransition { previous, .. } = &mut rows[0].input else {
                    panic!()
                };
                previous.content_digest[0] ^= 1;
                target.cut.chain = row_chain(anchor.checkpoint().cut.chain, &rows[0]).unwrap();
            }
            _ => target.runtime.markets[0].content_digest[0] ^= 1,
        }
        let source = Checkpoint::new(
            source.source,
            source.cut.sequence,
            source.cut.chain,
            source.business,
            source.runtime,
        )
        .unwrap();
        let target = Checkpoint::new(
            target.source,
            target.cut.sequence,
            target.cut.chain,
            target.business,
            target.runtime,
        )
        .unwrap();
        assert!(
            super::super::catchup::plan(&source, anchor.checkpoint(), &rows, &target).is_err(),
            "case {case}"
        );
    }
}

#[test]
fn different_valid_depth_inputs_with_identical_final_books_do_not_prove_the_source_cut() {
    let (anchor, app) = initial();
    let stream = anchor.checkpoint().runtime.markets[0].stream.clone();
    let market = &app.markets[&stream];
    let view = market.replica.view(1001).unwrap();
    let mut record = MarketRecord::decode(&tests::vector("depth_inline_two_levels")).unwrap();
    record.header.sequence = 1001;
    record.header.topic_or_scope_id = market.resource.topic_id;
    record.header.feed_generation = market.resource.feed_generation;
    record.header.producer_generation = market.resource.producer_generation;
    record.header.instrument_id = 1001;
    record.header.instrument_version = 5;
    record.header.ingress_mono_ns = 1_600_000_000;
    record.header.publish_mono_ns = 1_600_000_001;
    record.header.flags = 6;
    record.header.record_crc32c = 0;
    record.header.payload_crc32c = 0;
    record.body.depth_flags = 0;
    record.body.inline_levels[0] = view.best_bid().unwrap().clone();
    record.body.inline_levels[1] = view.best_ask().unwrap().clone();
    let mut source = Engine::new(&anchor, app).unwrap();
    let shadow_app = Application::restore(&anchor.checkpoint().business, |bytes| {
        Factory.restore(bytes, &anchor.checkpoint().runtime)
    })
    .unwrap();
    let mut shadow = Engine::new(&anchor, shadow_app).unwrap();
    for (i, engine) in [&mut source, &mut shadow].into_iter().enumerate() {
        record.body.exchange_event_ns = 1_600_000_000 + i as u64;
        let bytes = crate::bootstrap::wire_bytes(&record).unwrap();
        tickwire_ipc::decode_record(&bytes).unwrap();
        engine
            .apply_live(
                Input::MarketApplied {
                    stream: stream.clone(),
                    sequence: 1001,
                    received_ns: 1_600_000_000,
                    payload: bytes[320..352].to_vec(),
                    record: bytes,
                },
                1_600_000_002,
            )
            .unwrap();
    }
    let source = source.checkpoint().unwrap();
    let target = shadow.checkpoint().unwrap();
    assert_eq!(source.business, target.business);
    assert_ne!(
        source.runtime.markets[0].content_digest,
        target.runtime.markets[0].content_digest
    );
    assert!(
        super::super::catchup::plan(&source, anchor.checkpoint(), shadow.recorded(), &target)
            .is_err()
    );
}

#[test]
fn transition_sync_failure_stops_callbacks_without_promoting_the_durable_cut() {
    let (anchor, app) = initial();
    let (input, bank, _) = fixture(anchor.checkpoint().runtime.markets[0].clone());
    let mut durable = durable::DurableEngine::new(
        anchor.clone(),
        app,
        tickwire_recovery::MockBlobStore::default(),
    )
    .unwrap();
    durable.store.fail_next_put = true;
    durable.apply(input, 1_600_000_003).unwrap();
    assert!(durable.flush().is_err());
    assert!(durable.stopped());
    assert_eq!(durable.last_durable_cut(), &anchor.checkpoint().cut);
    assert_eq!(
        durable.strategy().business_state().unwrap(),
        0u64.to_le_bytes()
    );
    assert!(
        durable
            .apply(
                Input::MarketCallback {
                    stream: market_stream(&bank).unwrap(),
                    instrument: u64::MAX,
                    invoked: true,
                    maximum_age_ns: 1_000_000_000
                },
                1_600_000_004
            )
            .is_err()
    );
}
