use super::*;
use crate::Strategy;
use tickwire_ipc::{DepthCoverage, DepthLevel, JoinBody, MarketRecord, ResourceHeader, Wire};
fn vector(name: &str) -> Vec<u8> {
    let all: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../specs/contracts/ipc-vectors-v1.json"
    ))
    .unwrap();
    let hex = all["vectors"]
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["name"] == name)
        .unwrap()["hex"]
        .as_str()
        .unwrap();
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
        .collect()
}
pub(crate) fn market_fixture() -> (ResourceHeader, crate::MarketReplica, Vec<u8>) {
    use tickwire_snapshot::*;
    let mut resource = ResourceHeader::decode(&vector("normalized_resource_header")).unwrap();
    resource.required_features |= 64;
    resource.descriptor_hash = [0; 32];
    let mut join = JoinBody::decode(&vector("join_normalized_snapshot")).unwrap();
    join.snapshot_hash = [0; 32];
    join.snapshot_id = [0; 16];
    let states = [
        "normalized_state_quote",
        "normalized_state_trade",
        "normalized_state_mark",
        "normalized_state_feed",
    ]
    .map(|s| MarketRecord::decode(&vector(s)).unwrap());
    let bids = [DepthLevel::decode(&vector("normalized_depth_1")).unwrap()];
    let asks = [DepthLevel::decode(&vector("normalized_depth_2")).unwrap()];
    let specs = [InstrumentSpec {
        instrument_id: 1001,
        instrument_version: 5,
        required_states: QUOTE | TRADE | MARK_FUNDING,
    }];
    let members = [InstrumentSnapshot {
        instrument_id: 1001,
        instrument_version: 5,
        snapshot_id: [18, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
        at_sequence: 1000,
        last_event_sequence: 990,
        feed_generation: 3,
        catalog_version: 5,
        catalog_hash: [0x22; 32],
        created_mono_ns: 1_000_000_000,
        expires_mono_ns: 2_000_000_000,
        state_version: 990,
        coverage_depth: 1,
        coverage_flags: 3,
        states: &states,
        bids: &bids,
        asks: &asks,
    }];
    let request = BuildRequest {
        resource: &resource,
        join: &join,
        catalog_hash: [0x22; 32],
        instrument_set_version: 5,
        sequence: 1000,
        created_mono_ns: 1_000_000_000,
        expires_mono_ns: 2_000_000_000,
        now_mono_ns: 1_500_000_000,
        logical_strategy_id: 0,
        instruments: &specs,
        members: &members,
        byte_limit: MAX_TOPIC_SNAPSHOT_BYTES,
    };
    let built = build_topic_with_coverage(
        &request,
        &[DepthCoverage {
            version: 1,
            bid_kind: 2,
            ask_kind: 2,
            bid_boundary_ticks: bids[0].price_ticks,
            ask_boundary_ticks: asks[0].price_ticks,
            source_book_generation: 3,
        }],
    )
    .unwrap();
    let validated = validate(
        built.bytes(),
        &JoinContext {
            expected_resource: built.resource(),
            catalog_hash: [0x22; 32],
            instruments: &specs,
            join: built.join(),
            now_mono_ns: 1_500_000_000,
        },
    )
    .unwrap();
    let replica =
        crate::MarketReplica::load(join.topic_id, join.ring_epoch, &validated, 4).unwrap();
    let mut next = states[0].clone();
    next.header.sequence = 1001;
    next.header.flags = 6;
    next.header.record_crc32c = 0;
    next.header.ingress_mono_ns = 1_600_000_000;
    next.header.publish_mono_ns = 1_600_000_001;
    let mut bytes = vec![0; 512];
    next.encode(&mut bytes).unwrap();
    (built.resource().clone(), replica, bytes)
}
struct Count(u64);
impl Strategy for Count {
    fn on_market_recorded(
        &mut self,
        view: crate::MarketView<'_>,
        ctx: &mut ReplayContext<'_>,
    ) -> crate::Result<Option<CanonicalCommand>> {
        let key = ctx.market_streams().next().unwrap().clone();
        assert_eq!(
            ctx.market(&key, view.instrument()).unwrap().best_bid(),
            view.best_bid()
        );
        self.on_market(view, ctx.now_ns())
    }
    fn on_report_recorded(
        &mut self,
        r: &crate::reports::AccountReport,
        ctx: &mut ReplayContext<'_>,
    ) -> crate::Result<Option<CanonicalCommand>> {
        let key = ctx.account_streams().next().unwrap().clone();
        assert!(ctx.account(&key).is_some());
        self.on_report(r, ctx.now_ns())
    }

    fn checkpoint(&self) -> crate::Result<Vec<u8>> {
        Ok(self.0.to_le_bytes().to_vec())
    }
    fn on_market(
        &mut self,
        v: crate::MarketView<'_>,
        _: u64,
    ) -> crate::Result<Option<CanonicalCommand>> {
        self.0 += v.best_bid().unwrap().price_ticks as u64;
        Ok(None)
    }
    fn on_report(
        &mut self,
        _: &crate::reports::AccountReport,
        _: u64,
    ) -> crate::Result<Option<CanonicalCommand>> {
        self.0 += 100;
        Ok(None)
    }
}
struct CountFactory;
impl BusinessFactory for CountFactory {
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
#[test]
fn actual_snapshot_ipc_record_and_account_callback_reconstruct_changed_business_state() {
    let (resource, replica, record) = market_fixture();
    let stream = market_stream(&resource).unwrap();
    let app = Application::new(Count(0), 1_000_000_000, vec![(resource, replica, 4)]).unwrap();
    let source = Source {
        logical_strategy: 8,
        instance: [2; 16],
        generation: 5,
        binary_digest: [3; 32],
        state_schema: 7,
        input_stream: [4; 16],
        clock: ClockDomain {
            boot_id: [5; 16],
            generation: 1,
        },
    };
    let runtime = RuntimeState {
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
    };
    let checkpoint = Checkpoint::new(
        source.clone(),
        0,
        [0; 32],
        app.checkpoint().unwrap(),
        runtime,
    )
    .unwrap();
    let anchor =
        verify_anchor(&checkpoint.bytes().unwrap(), &source, Object::of(&[1]), &[]).unwrap();
    let mut engine = Engine::new(&anchor, app).unwrap();
    engine
        .apply_live(
            Input::MarketApplied {
                stream: stream.clone(),
                sequence: 1001,
                received_ns: 1_600_000_000,
                record,
                payload: vec![],
            },
            1_600_000_001,
        )
        .unwrap();
    engine
        .apply_live(
            Input::MarketCallback {
                stream,
                instrument: 1001,
                invoked: true,
                maximum_age_ns: 1_000_000_000,
            },
            1_600_000_001,
        )
        .unwrap();
    let (snapshot, catalog, _) = crate::account_fixture::fixture();
    let base = tickwire_account_codec::CanonicalBaseline::new(snapshot, catalog).unwrap();
    engine
        .apply_live(
            Input::AccountCallback {
                stream: account::AccountReplica::stream(&base),
                position: 0,
                canonical: base.bytes().to_vec(),
            },
            1_600_000_002,
        )
        .unwrap();
    assert_ne!(
        engine.last_completed_cut().business,
        anchor.checkpoint().cut.business
    );
    let tail = verify_tail(
        &anchor,
        &encode_tail(&anchor, engine.recorded()).unwrap(),
        engine.last_completed_cut(),
    )
    .unwrap();
    let restored = replay_source(
        &ApplicationFactory {
            binary_digest: [3; 32],
            business: CountFactory,
        },
        &anchor,
        &tail,
    )
    .unwrap();
    assert_eq!(restored.checkpoint(), &engine.checkpoint().unwrap());
}
