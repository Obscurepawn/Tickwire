#![allow(unsafe_code)]
#[path = "../../tickwire-binance-public/tests/sbe_network.rs"]
mod network;
use tickwire_binance_public::{DepthEncoding, Notice, SubscribeMode};
use tickwire_ipc::{MarketRecord, Wire};
use tickwire_market_publisher::*;
use tickwire_shm::{Publisher, Subscriber, SubscriptionKey};
#[test]
#[cfg_attr(
    not(target_os = "linux"),
    ignore = "real memfd publication requires Linux"
)]
fn actual_binary_wss_publishes_exact_normalized_records_into_shared_memory() {
    let instrument = network::instrument();
    let mut spec: tickwire_md::config::ResourceSpec =
        serde_json::from_str(include_str!("../examples/topic-101.json")).unwrap();
    spec.identity.arch = if cfg!(target_arch = "aarch64") { 2 } else { 1 };
    spec.catalog_version = instrument.catalog_version;
    spec.first_seq = 1;
    let bank = Publisher::<32>::create(spec.header().unwrap()).unwrap();
    let bank_fd = bank.reader_fd().unwrap();
    let bank_header = bank.header().clone();
    let key = SubscriptionKey {
        bank: bank.key(),
        instance: 55,
        generation: 1,
        cursor: 56,
    };
    let registry = tickwire_join::TopicRegistry {
        catalog_hash: instrument.catalog_hash,
        instrument_set_version: 1,
        instruments: vec![tickwire_snapshot::InstrumentSpec {
            instrument_id: instrument.instrument_id,
            instrument_version: instrument.instrument_version,
            required_states: tickwire_snapshot::QUOTE,
        }],
    };
    let owner = tickwire_join::JoinOwner::new(bank, registry).unwrap();
    let id = instrument.instrument_id;
    let book_key = tickwire_book::BookKey {
        venue: 1,
        instrument: 12,
        source: 1,
    };
    let mut publisher = TopicPublisher::new(
        owner,
        vec![InstrumentConfig {
            instrument,
            key: book_key,
            market: tickwire_binance::request::Market::Spot,
            time_unit: tickwire_binance::feed::TimeUnit::Microseconds,
        }],
        Limits {
            levels_per_side: 32,
            cache_bytes: 1 << 20,
            snapshot_bytes: 1 << 20,
            max_delta_bytes: 4096,
        },
    )
    .unwrap();
    let baseline = prepare_pending(&mut publisher, spec.identity.identity(), key);
    // The real JOIN owner has reserved this exact pending subscription at S.
    // It stays pending for this test, so neither slots nor arena can be reclaimed
    // past S while the reader below accesses them.
    let reader = unsafe { Subscriber::<64>::attach(bank_fd, bank_header, key, baseline).unwrap() };
    struct Faults;
    impl FaultSink for Faults {
        fn invalidate(&mut self, f: Fault) {
            panic!("publication failed: {f:?}")
        }
    }
    let start = std::time::Instant::now();
    network::run_with(SubscribeMode::Explicit, |notice| {
        if let Notice::Sync { encoding, .. } = notice {
            assert_eq!(*encoding, DepthEncoding::SpotSbeV1);
        }
        publisher
            .on_notice_ref(id, notice, start.elapsed().as_nanos() as u64, &mut Faults)
            .unwrap();
    });
    assert!(publisher.state().live());
    let mut count = 0;
    let mut depth = None;
    let mut quote = None;
    while let Some(guard) = reader.borrow().unwrap() {
        let record = MarketRecord::decode(guard.bytes()).unwrap();
        count += 1;
        if record.header.message_type == 259 {
            depth = Some(record.clone());
            let bytes = guard.depth_payload().unwrap();
            assert_eq!(
                record.header.payload_kind, 2,
                "more than 12 levels must use the real arena"
            );
            assert_eq!(bytes.len(), 15 * 16);
            assert_eq!(i64::from_le_bytes(bytes[8..16].try_into().unwrap()), 0);
            assert_eq!(
                i64::from_le_bytes(bytes[16..24].try_into().unwrap()),
                6500100
            );
            assert_eq!(i64::from_le_bytes(bytes[24..32].try_into().unwrap()), 60000);
        }
        if record.header.message_type == 257 {
            quote = Some(record);
        }
    }
    assert_eq!(count, 5);
    let depth = depth.unwrap();
    assert_eq!((depth.body.bid_count, depth.body.ask_count), (1, 14));
    assert_eq!(
        (depth.body.venue_seq_first, depth.body.venue_seq_last),
        (45, 45)
    );
    assert_eq!(depth.body.exchange_event_ns, 2_000_000_000);
    assert_eq!(depth.header.source_timestamp_ns, 2_000_000_000);
    assert!(depth.header.ingress_mono_ns > 0);
    let quote = quote.unwrap();
    assert_eq!(quote.body.ask_price_ticks, 6500100);
    assert_eq!(quote.body.ask_qty_lots, 60000);
    assert_eq!(quote.body.bid_price_ticks, 6499900);
    assert_eq!(quote.body.bid_qty_lots, 20000);
}

fn prepare_pending(
    publisher: &mut TopicPublisher<32>,
    owner_identity: tickwire_control::Identity,
    key: SubscriptionKey,
) -> u64 {
    use tickwire_control::{ClientHandshake, MockRequestStore, Policy, ProcessIdentity, Session};
    use tickwire_ipc::{ControlEnvelope, JoinBody};
    const NOW: u64 = 1;
    const DEADLINE: u64 = 60_000_000_000;
    let mut supervisor_identity = owner_identity.clone();
    supervisor_identity.instance = [90; 16];
    supervisor_identity.role = 1;
    let process = ProcessIdentity {
        pid: std::process::id(),
        uid: 0,
        gid: 0,
    };
    let policy = |local, peer| Policy {
        local,
        peer,
        peer_process: process,
        required_features: 0x1c,
        supported_features: 0x1ff,
        control_epoch: 1,
        lease_duration_ns: DEADLINE,
        max_frame_bytes: 65536,
        nonce: [91; 32],
    };
    let handshake = ClientHandshake::new(
        policy(supervisor_identity.clone(), owner_identity.clone()),
        [92; 16],
    )
    .unwrap();
    let (mut session, welcome) = Session::accept(
        &policy(owner_identity.clone(), supervisor_identity),
        process,
        handshake.hello(),
        NOW,
    )
    .unwrap();
    let supervisor = handshake.finish(process, &welcome, NOW).unwrap();
    let mut identity = owner_identity;
    identity.instance = key.instance.to_le_bytes();
    identity.generation = key.generation;
    identity.role = 3;
    identity.logical_strategy = 7;
    let (owner, state) = publisher.split();
    let h = owner.header();
    let join = JoinBody {
        subscriber_instance: identity.instance,
        logical_strategy_id: 7,
        subscription_generation: key.generation,
        topic_id: h.topic_id,
        channel_id: h.channel_id,
        channel_epoch: h.channel_epoch,
        feed_generation: h.feed_generation,
        ring_epoch: h.ring_epoch,
        cursor_resource_id: key.cursor.to_le_bytes(),
        bank_resource_id: h.resource_id,
        readiness_deadline_mono_ns: DEADLINE,
        expected_catalog_version: h.catalog_version,
        qos: h.qos,
        role: 3,
        join_flags: 3,
        topic_instrument_set_version: owner.registry().instrument_set_version,
        topic_instrument_set_hash: tickwire_join::instrument_set_hash(
            &owner.registry().instruments,
        ),
        ..Default::default()
    };
    let mut body = vec![0; JoinBody::SIZE];
    join.encode(&mut body).unwrap();
    let request = supervisor
        .encode(
            ControlEnvelope {
                opcode: 16,
                flags: 1,
                request_id: [93; 16],
                expected_state_version: owner.membership_version(),
                ..Default::default()
            },
            &body,
        )
        .unwrap();
    let result = owner
        .prepare(
            &mut session,
            &mut MockRequestStore::new(8).unwrap(),
            &request,
            &tickwire_join::SubscriberRegistration {
                identity,
                subscription: key,
                qos: h.qos,
            },
            [94; 16],
            NOW,
            |plan| state.snapshot(plan, NOW, DEADLINE),
        )
        .unwrap();
    assert!(matches!(
        result,
        tickwire_join::PrepareOutcome::Prepared { .. }
    ));
    assert_eq!(owner.stage(key), Some(tickwire_join::Stage::Prepared));
    assert_eq!(owner.membership_version(), 2);
    owner.join(key).unwrap().snapshot_seq
}
