#![forbid(unsafe_code)]

use std::time::{Duration, Instant};

use sha2::{Digest, Sha256};
use tickwire_account::OrderKey;
use tickwire_control::{
    ClientHandshake, Identity, MockRequestStore, Policy, ProcessIdentity, Session,
};
use tickwire_control_sys::{CpuSet, current_thread_affinity, pin_current_thread};
use tickwire_ipc::{
    ALL_STRATS, DepthCoverage, DepthLevel, DopEchoBody, MarketBody, MarketExactBody,
    MarketExactRecord, MarketRecord, MarketState, MktSession, RecordHeader, ReportBody,
    ResourceHeader, Wire, intent_flags, tif_code,
};
use tickwire_join::{JoinOwner, JoinPlan, SubscriberRegistration, TopicRegistry};
use tickwire_risk::Fixed;
use tickwire_shm::{Publisher, SubscriptionKey};
use tickwire_snapshot::{
    BuildRequest, BuiltTopic, InstrumentSnapshot, InstrumentSpec, QUOTE, TRADE,
    build_topic_with_coverage,
};
use tickwire_strategy_sdk::{
    DEFAULT_MAX_ORDER_AGE_NS, DopConfig, DopSession, Error, ExecutionReportKind, FillKind,
    FillWatermarkTracker, IntentOpcode, MarketView, ModifyRequest, OrderClassificationHints,
    OutboundIntent, ReportFillOutcome, ReportIdentity, Strategy, WorkerDriver, accept_dop_login,
    arena::configure_shm_arena_header, classify_order_status, classify_venue_rejection,
    in_process_intent_pair, in_process_intent_pair_with_arena, in_process_report_pair_with_arena,
    local_seqpacket_pair, mint_self_describing_client_id, worker,
};
use tickwire_venue::{
    AssetRef, AssetSymbol, BoundedList, BoundedText, ClientId, ClientIdDomain, CommandContext,
    ContractKind, Decimal, DecodedClientId, Environment, InstrumentBinding, IntentKey, Liquidity,
    OrderStatus, OrderTarget, OrderType, Product, REPLACEMENT_ORDER_LEG, RefusalReason,
    RequestIdentity, SelfDescribingClientId, SelfTradePrevention, Side, StrategyContext, Symbol,
    TerminalReason, TimeInForce, TriggerRef, venue_id,
};

const NOW_NS: u64 = 1_500_000_000;
const DEADLINE_NS: u64 = 5_000_000_000;

fn dec(coeff: i128, exp: i8) -> Decimal {
    Decimal::new(coeff, exp).unwrap()
}

fn fixed(atoms: i128, scale: u8) -> Fixed {
    Fixed::from_atoms(atoms, scale).unwrap()
}

fn target_arch_code() -> u16 {
    if cfg!(target_arch = "x86_64") {
        1
    } else if cfg!(target_arch = "aarch64") {
        2
    } else {
        panic!("unsupported architecture")
    }
}

fn asset(id: u64, symbol: &str, exponent: i8) -> AssetRef {
    AssetRef {
        id,
        version: 1,
        symbol: AssetSymbol::new(symbol).unwrap(),
        exponent,
    }
}

fn sample_instrument() -> InstrumentBinding {
    InstrumentBinding {
        instrument_id: 1001,
        instrument_version: 5,
        catalog_version: 1,
        catalog_hash: [0x22; 32],
        symbol: Symbol::new("BTCUSDT").unwrap(),
        base: asset(1, "BTC", -8),
        quote: asset(2, "USDT", -8),
        settlement: asset(2, "USDT", -8),
        price_tick: dec(1, -2),
        quantity_step: dec(1, -5),
        contract: ContractKind::Cash,
    }
}

fn sample_context(intent_id: u64) -> CommandContext {
    CommandContext {
        request: RequestIdentity {
            risk_domain_id: [1; 16],
            venue: venue_id::BINANCE,
            environment: Environment::Test,
            account: 2,
            product: Product::Spot,
            intent_id,
            intent_exec_epoch: 4,
            strategy_generation: 5,
            config_revision: 6,
            deadline: NOW_NS + 10_000_000,
            recovery_object: 7,
        },
        strategy: StrategyContext {
            logical_strategy_id: 8,
            instance_id: [2; 16],
            strategy_intent_seq: 1,
            authorization_epoch: 9,
            risk_policy_version: 10,
            order_config_version: 11,
            created_mono_ns: NOW_NS,
            economic_intent_id: [3; 16],
            hedge_group_id: [0; 16],
            leg_id: 0,
            recovery_manifest_hash: [4; 32],
        },
        instrument: sample_instrument(),
        triggers: BoundedList::new(vec![TriggerRef {
            topic_id: 1,
            feed_generation: 1,
            ring_epoch: 1,
            sequence: 1,
            instrument_id: 1001,
            instrument_version: 5,
            observed_mono_ns: NOW_NS,
        }])
        .unwrap(),
    }
}

fn intent_bank_header(producer_instance: [u8; 16], consumer_instance: [u8; 16]) -> ResourceHeader {
    let capacity = 128u32;
    let record_bytes = 1024u32;
    ResourceHeader {
        magic: *b"TWIPC001",
        abi_major: 1,
        abi_minor: 0,
        header_bytes: 4096,
        resource_kind: 2,
        endian: 1,
        atomic_width: 8,
        page_shift: 12,
        isolation_shift: 7,
        target_arch: target_arch_code(),
        resource_id: [11; 16],
        channel_id: [12; 16],
        channel_epoch: 1,
        ring_epoch: 1,
        feed_generation: 1,
        producer_instance,
        producer_generation: 5,
        consumer_instance,
        host_boot_id: [13; 16],
        namespace_id: [14; 16],
        risk_scope: 900,
        producer_page_offset: 4096,
        slot_offset: 8192,
        capacity,
        record_bytes,
        resource_bytes: 8192 + u64::from(capacity) * u64::from(record_bytes),
        first_seq: 1,
        initial_membership_version: 1,
        max_consumers: 1,
        required_features: 0x1c | tickwire_strategy_sdk::outbound::FEATURE,
        schema_major: 1,
        clock_domain: 1,
        qos: 1,
        event_class: 8,
        ..Default::default()
    }
}

fn report_bank_header(producer_instance: [u8; 16], consumer_instance: [u8; 16]) -> ResourceHeader {
    let capacity = 128u32;
    let record_bytes = 512u32;
    ResourceHeader {
        magic: *b"TWIPC001",
        abi_major: 1,
        abi_minor: 0,
        header_bytes: 4096,
        resource_kind: 3,
        endian: 1,
        atomic_width: 8,
        page_shift: 12,
        isolation_shift: 7,
        target_arch: target_arch_code(),
        resource_id: [21; 16],
        channel_id: [22; 16],
        channel_epoch: 1,
        ring_epoch: 1,
        feed_generation: 1,
        producer_instance,
        producer_generation: 7,
        consumer_instance,
        host_boot_id: [13; 16],
        namespace_id: [14; 16],
        risk_scope: 900,
        producer_page_offset: 4096,
        slot_offset: 8192,
        capacity,
        record_bytes,
        resource_bytes: 8192 + u64::from(capacity) * u64::from(record_bytes),
        first_seq: 1,
        initial_membership_version: 1,
        max_consumers: 1,
        required_features: 0x1c | tickwire_strategy_sdk::reports::FEATURE,
        schema_major: 1,
        clock_domain: 1,
        qos: 1,
        event_class: 9,
        ..Default::default()
    }
}

fn arena_header(
    resource_id: [u8; 16],
    producer: [u8; 16],
    consumer: [u8; 16],
    generation: u64,
) -> ResourceHeader {
    let mut h = configure_shm_arena_header(
        resource_id,
        [32; 16],
        producer,
        consumer,
        1,
        generation,
        &[(16_384, 8)],
    )
    .unwrap();
    h.host_boot_id = [13; 16];
    h.namespace_id = [14; 16];
    h.risk_scope = 900;
    h
}

fn market_header() -> ResourceHeader {
    ResourceHeader {
        magic: *b"TWIPC001",
        abi_major: 1,
        header_bytes: 4096,
        resource_kind: 1,
        target_arch: target_arch_code(),
        endian: 1,
        atomic_width: 8,
        page_shift: 12,
        isolation_shift: 7,
        record_bytes: 512,
        capacity: 64,
        resource_bytes: 8192 + 64 * 512,
        resource_id: [13; 16],
        channel_id: [14; 16],
        producer_instance: [3; 16],
        producer_generation: 1,
        host_boot_id: [8; 16],
        namespace_id: [9; 16],
        channel_epoch: 1,
        ring_epoch: 1,
        feed_generation: 1,
        first_seq: 1,
        initial_membership_version: 1,
        producer_page_offset: 4096,
        slot_offset: 8192,
        max_consumers: 4,
        schema_major: 1,
        clock_domain: 1,
        required_features: 0x1c | 64 | 512,
        trust_profile: 1,
        event_class: 7,
        topic_id: 1,
        catalog_version: 1,
        qos: 1,
        ..Default::default()
    }
}

fn identity(id: u8, role: u16) -> Identity {
    Identity {
        instance: [id; 16],
        generation: 1,
        role,
        logical_strategy: if role == 3 { 7 } else { 0 },
        host_boot: [8; 16],
        namespace: [9; 16],
        build_hash: [10; 32],
        qualification_hash: [11; 32],
        manifest_hash: [12; 32],
        arch: target_arch_code(),
        trust_profile: 1,
    }
}

fn supervisor_pair() -> (Session, Session) {
    let owner = identity(3, 2);
    let sup = identity(1, 1);
    let process = ProcessIdentity {
        pid: 100,
        uid: 0,
        gid: 0,
    };
    let nonce: [u8; 32] = Sha256::digest(NOW_NS.to_le_bytes()).into();
    let server_policy = Policy {
        local: owner.clone(),
        peer: sup.clone(),
        peer_process: process,
        required_features: 0x1c,
        supported_features: 0x1f | 64 | 512,
        control_epoch: 7,
        lease_duration_ns: 60_000_000_000,
        max_frame_bytes: 65_536,
        nonce,
    };
    let client_policy = Policy {
        local: sup,
        peer: owner,
        ..server_policy.clone()
    };
    let hello = ClientHandshake::new(client_policy, [1; 16]).unwrap();
    let (server, welcome) =
        Session::accept(&server_policy, process, hello.hello(), NOW_NS).unwrap();
    let client = hello.finish(process, &welcome, NOW_NS).unwrap();
    (server, client)
}

fn load_golden_market_states() -> Vec<MarketRecord> {
    let base_hdr = |sequence: u64, message_type: u16| RecordHeader {
        sequence,
        message_type,
        schema_major: 1,
        header_bytes: 128,
        body_bytes: 384,
        flags: 6,
        topic_or_scope_id: 1,
        feed_generation: 1,
        producer_generation: 1,
        instrument_id: 1001,
        instrument_version: 5,
        ingress_mono_ns: NOW_NS,
        publish_mono_ns: NOW_NS,
        ..Default::default()
    };
    vec![
        MarketRecord {
            header: base_hdr(1, 257),
            body: MarketBody {
                book_revision: 1,
                bid_price_ticks: 6_000_000,
                bid_qty_lots: 100,
                ask_price_ticks: 6_000_100,
                ask_qty_lots: 120,
                feed_state: 2,
                ..Default::default()
            },
        },
        MarketRecord {
            header: base_hdr(2, 258),
            body: MarketBody {
                trade_id: 1,
                price_ticks: 6_000_000,
                qty_lots: 10,
                feed_state: 2,
                side: 1,
                ..Default::default()
            },
        },
        MarketRecord {
            header: base_hdr(3, 261),
            body: MarketBody {
                feed_state: 2,
                ..Default::default()
            },
        },
    ]
}

fn build_snapshot(
    states: &[MarketRecord],
    registry: &TopicRegistry,
    plan: &JoinPlan,
) -> BuiltTopic {
    let bids = [DepthLevel {
        price_ticks: 6_000_000,
        qty_lots: 100,
    }];
    let asks = [DepthLevel {
        price_ticks: 6_000_100,
        qty_lots: 120,
    }];
    let coverage = [DepthCoverage {
        version: 1,
        bid_kind: 1,
        ask_kind: 1,
        ..Default::default()
    }];
    let members = [InstrumentSnapshot {
        instrument_id: 1001,
        instrument_version: 5,
        snapshot_id: [77; 16],
        at_sequence: plan.sequence,
        last_event_sequence: plan.sequence,
        feed_generation: 1,
        catalog_version: 1,
        catalog_hash: registry.catalog_hash,
        created_mono_ns: NOW_NS,
        expires_mono_ns: DEADLINE_NS,
        state_version: 1,
        coverage_depth: 10,
        coverage_flags: 0,
        states,
        bids: &bids,
        asks: &asks,
    }];
    build_topic_with_coverage(
        &BuildRequest {
            resource: &plan.resource,
            join: &plan.join,
            catalog_hash: registry.catalog_hash,
            instrument_set_version: registry.instrument_set_version,
            sequence: plan.sequence,
            created_mono_ns: NOW_NS,
            expires_mono_ns: DEADLINE_NS,
            now_mono_ns: NOW_NS,
            logical_strategy_id: 7,
            instruments: &registry.instruments,
            members: &members,
            byte_limit: 64 * 1024 * 1024,
        },
        &coverage,
    )
    .unwrap()
}

fn make_join_registration(
    owner: &JoinOwner<4>,
    id: u8,
) -> (SubscriberRegistration, tickwire_ipc::JoinBody) {
    let h = owner.header();
    let reg = SubscriberRegistration {
        identity: identity(id, 3),
        subscription: SubscriptionKey {
            bank: tickwire_shm::BankKey {
                resource: u128::from_le_bytes(h.resource_id),
                channel: u128::from_le_bytes(h.channel_id),
                channel_epoch: h.channel_epoch,
                ring_epoch: h.ring_epoch,
                feed_generation: h.feed_generation,
            },
            instance: u128::from_le_bytes([id; 16]),
            generation: 1,
            cursor: u128::from_le_bytes([id + 100; 16]),
        },
        qos: 1,
    };
    let join = tickwire_ipc::JoinBody {
        subscriber_instance: reg.identity.instance,
        logical_strategy_id: 7,
        subscription_generation: 1,
        topic_id: h.topic_id,
        channel_id: h.channel_id,
        channel_epoch: h.channel_epoch,
        feed_generation: h.feed_generation,
        ring_epoch: h.ring_epoch,
        cursor_resource_id: reg.subscription.cursor.to_le_bytes(),
        bank_resource_id: h.resource_id,
        readiness_deadline_mono_ns: DEADLINE_NS,
        expected_catalog_version: h.catalog_version,
        qos: 1,
        role: 3,
        join_flags: 3,
        topic_instrument_set_version: owner.registry().instrument_set_version,
        topic_instrument_set_hash: tickwire_join::instrument_set_hash(
            &owner.registry().instruments,
        ),
        ..Default::default()
    };
    (reg, join)
}

#[derive(Default)]
struct QuoteObserver {
    seen_spreads: Vec<(i64, u64)>,
}

impl Strategy for QuoteObserver {
    fn on_market(
        &mut self,
        view: MarketView<'_>,
        _now_ns: u64,
    ) -> tickwire_strategy_sdk::Result<Option<OutboundIntent>> {
        match view.best_quote() {
            Some(MarketState::Exact(q)) if q.body.feed_state == 2 => {
                self.seen_spreads.push((
                    q.body.ask_price_ticks - q.body.bid_price_ticks,
                    view.tick_mono_ns(),
                ));
            }
            Some(MarketState::Legacy(q)) if q.body.feed_state == 2 => {
                self.seen_spreads.push((
                    q.body.ask_price_ticks - q.body.bid_price_ticks,
                    view.tick_mono_ns(),
                ));
            }
            _ => {}
        }
        Ok(None)
    }
}

#[test]
fn independent_shm_market_cursors_consume_market_record_and_exact_record_zero_copy() {
    let states = load_golden_market_states();
    let registry = TopicRegistry {
        catalog_hash: [0x22; 32],
        instrument_set_version: 1,
        instruments: vec![InstrumentSpec {
            instrument_id: 1001,
            instrument_version: 5,
            required_states: QUOTE | TRADE,
        }],
    };
    let mut publisher = Publisher::<4>::create(market_header()).unwrap();
    for s in &states {
        let mut raw = [0u8; MarketRecord::SIZE];
        s.encode(&mut raw).unwrap();
        publisher.publish(&[&raw]).unwrap();
    }
    let mut owner = JoinOwner::new(publisher, registry.clone()).unwrap();
    let (mut sup_server, sup_client) = supervisor_pair();
    let mut store = MockRequestStore::new(16).unwrap();

    // Join worker A (`id = 4`) and worker B (`id = 5`) at independent SHM cursors.
    let (reg_a, join_a) = make_join_registration(&owner, 4);
    let mut raw_join_a = [0u8; tickwire_ipc::JoinBody::SIZE];
    join_a.encode(&mut raw_join_a).unwrap();
    let admin_req_a = sup_client
        .encode(
            tickwire_ipc::ControlEnvelope {
                opcode: 16,
                flags: 1,
                request_id: [41; 16],
                target_instance: owner.header().producer_instance,
                expected_state_version: owner.membership_version(),
                ..Default::default()
            },
            &raw_join_a,
        )
        .unwrap();
    let (consumer_a, mut replica_a) = worker::in_process_join_consumer(
        &mut owner,
        &mut sup_server,
        &mut store,
        &admin_req_a,
        &reg_a,
        [31; 16],
        32,
        NOW_NS,
        |plan| Ok(build_snapshot(&states, &registry, plan)),
    )
    .unwrap();

    let (reg_b, join_b) = make_join_registration(&owner, 5);
    let mut raw_join_b = [0u8; tickwire_ipc::JoinBody::SIZE];
    join_b.encode(&mut raw_join_b).unwrap();
    let admin_req_b = sup_client
        .encode(
            tickwire_ipc::ControlEnvelope {
                opcode: 16,
                flags: 1,
                request_id: [42; 16],
                target_instance: owner.header().producer_instance,
                expected_state_version: owner.membership_version(),
                ..Default::default()
            },
            &raw_join_b,
        )
        .unwrap();
    let (consumer_b, mut replica_b) = worker::in_process_join_consumer(
        &mut owner,
        &mut sup_server,
        &mut store,
        &admin_req_b,
        &reg_b,
        [32; 16],
        32,
        NOW_NS,
        |plan| Ok(build_snapshot(&states, &registry, plan)),
    )
    .unwrap();

    // Publish an exact quote (`MarketExactRecord`, `message_type = 265`) onto the shared ring.
    let exact_seq = owner.published() + 1;
    let next_venue_seq = states.last().unwrap().body.venue_seq_last + 1;
    let exact_quote = MarketExactRecord {
        header: RecordHeader {
            sequence: exact_seq,
            message_type: 265,
            schema_major: 1,
            schema_minor: 0,
            header_bytes: 128,
            flags: 6,
            payload_kind: 0,
            body_bytes: 384,
            topic_or_scope_id: 1,
            feed_generation: 1,
            producer_generation: 1,
            instrument_id: 1001,
            instrument_version: 5,
            source_timestamp_ns: 0,
            ingress_mono_ns: NOW_NS + 1_234,
            publish_mono_ns: NOW_NS + 1_500,
            ..Default::default()
        },
        body: MarketExactBody {
            source_id: 1,
            source_connection_epoch: 1,
            source_encoding: 1,
            venue_seq_first: next_venue_seq,
            venue_seq_last: next_venue_seq,
            bid_price_ticks: 6_001_000,
            bid_quantity_lots: 250,
            ask_price_ticks: 6_001_250,
            ask_quantity_lots: 300,
            feed_state: 2,
            ..Default::default()
        },
    };
    let mut raw_exact = [0u8; MarketExactRecord::SIZE];
    exact_quote.encode(&mut raw_exact).unwrap();
    #[cfg(feature = "crc32")]
    {
        let mut with_crc = exact_quote.clone();
        with_crc.header.flags |= 8;
        with_crc.encode(&mut raw_exact).unwrap();
        let crc = tickwire_ipc::crc32c_masked(&raw_exact, 112..120);
        raw_exact[112..116].copy_from_slice(&crc.to_le_bytes());
    }
    owner.publish(&[&raw_exact]).unwrap();

    // Worker A drains immediately while Worker B lags at its own independent cursor.
    let mut strat_a = QuoteObserver::default();
    let drained_a = replica_a
        .drain(
            &consumer_a,
            &mut strat_a,
            NOW_NS + 2_000,
            10_000_000,
            16,
            |_| Ok(()),
        )
        .unwrap();
    assert_eq!(drained_a, 1);
    assert_eq!(strat_a.seen_spreads, vec![(250, NOW_NS + 1_234)]);

    // Worker B still sees its unread slot and observes the exact same quote independently.
    let mut strat_b = QuoteObserver::default();
    let drained_b = replica_b
        .drain(
            &consumer_b,
            &mut strat_b,
            NOW_NS + 2_000,
            10_000_000,
            16,
            |_| Ok(()),
        )
        .unwrap();
    assert_eq!(drained_b, 1);
    assert_eq!(strat_b.seen_spreads, vec![(250, NOW_NS + 1_234)]);
}

#[test]
fn dop_logon_adopts_sequence_floor_routes_accounts_checks_staleness_and_maintains_heartbeats() {
    let deadline = Instant::now() + Duration::from_secs(2);
    let dop_cfg = DopConfig {
        strat_id: 7,
        client_id: 101,
        local_instance: [2; 16],
        peer_instance: [99; 16],
        control_epoch: 3,
        expected_peer_uid: None,
        max_order_age_ns: DEFAULT_MAX_ORDER_AGE_NS,
    };

    // 1. Refused login surfaces `Error::DopRefused`.
    {
        let (mut server, client) = local_seqpacket_pair(deadline).unwrap();
        let server_handle = std::thread::spawn(move || {
            let refused_echo = DopEchoBody {
                next_source_order_seqnum: 1,
                hb_interval_ms: 100,
                strat_id: 7,
                ready_to_trade: 0,
                refusal_reason: RefusalReason::InvalidStratId.code(),
                ..Default::default()
            };
            accept_dop_login(
                &mut server,
                [99; 16],
                [2; 16],
                3,
                &refused_echo,
                &[],
                NOW_NS,
                12,
                deadline,
            )
            .unwrap()
        });
        let err =
            match DopSession::login_over_connection(client, dop_cfg.clone(), 0, NOW_NS, deadline) {
                Ok(_) => panic!("duplicate DOP login must be refused"),
                Err(e) => e,
            };
        assert!(matches!(
            err,
            Error::DopRefused {
                reason: TerminalReason::InvalidStratId,
                status: 12,
            }
        ));
        server_handle.join().unwrap();
    }

    // 2. Accepted login adopts `next_source_order_seqnum = 42`, resets `msg_seqnum = 1`,
    //    resolves `account_ref = 0 -> 3`, enforces `23 us` `max_order_age_ns`, and sends heartbeats/logout.
    let (mut server, client) = local_seqpacket_pair(deadline).unwrap();
    let sessions = vec![
        MktSession {
            local_ip: 0x7f00_0001,
            product_mask: Product::Spot.mask_bit() | Product::UsdM.mask_bit(),
            account_ref: 3,
            strat_id: 7,
            authorized: 1,
            ready: 1,
            colo: 1,
            _pad_11: [0; 5],
        },
        MktSession {
            local_ip: 0x7f00_0001,
            product_mask: Product::Spot.mask_bit(),
            account_ref: 9,
            strat_id: 7,
            authorized: 0,
            ready: 1,
            colo: 0,
            _pad_11: [0; 5],
        },
    ];
    let sessions_clone = sessions.clone();
    let login_handle = std::thread::spawn(move || {
        let echo = DopEchoBody {
            next_source_order_seqnum: 42,
            hb_interval_ms: 100,
            strat_id: 7,
            connected: 1,
            enabled: 1,
            market_connected: 1,
            ready_to_trade: 1,
            refusal_reason: 0,
            ..Default::default()
        };
        let login = accept_dop_login(
            &mut server,
            [99; 16],
            [2; 16],
            3,
            &echo,
            &sessions_clone,
            NOW_NS,
            0,
            deadline,
        )
        .unwrap();
        (server, login)
    });

    let mut dop = DopSession::login_over_connection(client, dop_cfg, 0, NOW_NS, deadline).unwrap();
    let (mut ors_server, login_body) = login_handle.join().unwrap();
    assert_eq!(login_body.strat_id, 7);
    assert_eq!(dop.next_order_seqnum(), 42);
    assert_eq!(dop.next_msg_seqnum(), 1);
    assert_eq!(dop.hb_interval_ms(), 100);
    assert_eq!(dop.resolve_account_ref(0).unwrap(), 3);
    assert_eq!(dop.resolve_account_ref(3).unwrap(), 3);
    assert!(matches!(
        dop.resolve_account_ref(9),
        Err(Error::UnauthorizedAccount { account_ref: 9 })
    ));
    assert_eq!(
        dop.resolve_ready_session(0, Some(Product::Spot))
            .unwrap()
            .account_ref,
        3
    );

    // Sync `IntentWriter` with `DopSession` and test `23 us` `max_order_age_ns` stale gate.
    let (mut writer, mut reader) =
        in_process_intent_pair(intent_bank_header([2; 16], [99; 16]), 1, [88; 16], 7, 1).unwrap();
    writer.sync_from_dop(&dop).unwrap();
    assert_eq!(writer.next_order_seqnum(), 42);
    assert_eq!(writer.next_msg_seqnum(), 1);

    let ctx = sample_context(42);
    let (tw_cid, _) = mint_self_describing_client_id(&ctx, 0).unwrap();
    let fresh_intent = OutboundIntent::new_order(
        7,
        0,
        1001,
        5,
        Product::Spot,
        Side::Buy,
        OrderType::Limit,
        Some(TimeInForce::Rpi),
        6_000_000,
        100,
        tw_cid.as_str(),
        NOW_NS + 1_000,
    )
    .unwrap();

    // Quote age `23_001 ns > 23_000 ns` is rejected before touching SHM.
    assert!(matches!(
        writer.publish_with_dop(&mut dop, &fresh_intent, NOW_NS + 1_000 + 23_001),
        Err(Error::Stale)
    ));

    // Quote age `15_000 ns <= 23_000 ns` publishes inline with `account_ref = 3`, `order_seqnum = 42`, `msg_seqnum = 1`.
    let receipt = writer
        .publish_with_dop(&mut dop, &fresh_intent, NOW_NS + 16_000)
        .unwrap();
    assert_eq!(receipt.order_seqnum, 42);
    assert_eq!(receipt.msg_seqnum, 1);
    assert_eq!(receipt.account_ref, 3);
    assert_eq!(dop.next_order_seqnum(), 43);
    assert_eq!(dop.next_msg_seqnum(), 2);

    let mut saw_zero_copy = false;
    assert!(
        reader
            .poll_zero_copy(|rec, arena_slice| {
                assert!(arena_slice.is_none());
                assert_eq!(rec.header.message_type, 513);
                assert_eq!(rec.body.order_seqnum, 42);
                assert_eq!(rec.body.msg_seqnum, 1);
                assert_eq!(rec.body.account_ref, 3);
                assert_eq!(rec.body.strat_id, 7);
                assert_eq!(rec.body.tick_mono_ns, NOW_NS + 1_000);
                assert_eq!(rec.body.time_in_force, tif_code::RPI);
                saw_zero_copy = true;
                Ok(())
            })
            .unwrap()
    );
    assert!(saw_zero_copy);

    // Heartbeat cadence (`100 ms` = `100_000_000 ns`) and clean `DopLogout`.
    assert!(
        !dop.maybe_send_heartbeat(NOW_NS + 50_000_000, deadline)
            .unwrap()
    );
    assert!(
        dop.maybe_send_heartbeat(NOW_NS + 100_000_000, deadline)
            .unwrap()
    );
    let hb_pkt = ors_server.recv(deadline).unwrap();
    let hb_frame = tickwire_ipc::ControlFrame::decode_negotiated(
        hb_pkt.bytes(),
        tickwire_ipc::DOP_PROTOCOL_FEATURE_MASK,
    )
    .unwrap();
    assert_eq!(
        hb_frame.envelope.opcode,
        tickwire_ipc::dop_opcode::DOP_HEART_BEAT
    );
    let hb_body = tickwire_ipc::DopHeartBeatBody::decode(hb_frame.body).unwrap();
    assert_eq!(hb_body.next_order_seqnum, 43);

    dop.logout(true, TerminalReason::AdminAction, deadline)
        .unwrap();
    let logout_pkt = ors_server.recv(deadline).unwrap();
    let logout_frame = tickwire_ipc::ControlFrame::decode_negotiated(
        logout_pkt.bytes(),
        tickwire_ipc::DOP_PROTOCOL_FEATURE_MASK,
    )
    .unwrap();
    assert_eq!(
        logout_frame.envelope.opcode,
        tickwire_ipc::dop_opcode::DOP_LOGOUT
    );
    let logout_body = tickwire_ipc::DopLogoutBody::decode(logout_frame.body).unwrap();
    assert_eq!(logout_body.cancel_on_logout, 1);
    assert!(dop.is_logged_out());
}

#[test]
fn inline_and_arena_intents_cover_modify_cancel_cancel_all_rfq_and_client_id_minting() {
    let ctx = sample_context(77);
    let target = OrderTarget {
        order_id: [9; 16],
        expected_owner_revision: 1,
        original_intent_exec_epoch: 4,
        client_id: Some(ClientId::new("orig-client-1").unwrap()),
        venue_order_id: Some(BoundedText::new("987654321").unwrap()),
    };

    // Reject non-positive ModifyRequest quantities and prices.
    assert!(matches!(
        ModifyRequest::new(target.clone(), Side::Buy, dec(0, -5), dec(60_000, 0))
            .build_command(ctx.clone()),
        Err(Error::Command(tickwire_venue::Error::Quantity))
    ));
    assert!(matches!(
        ModifyRequest::new(target.clone(), Side::Buy, dec(25, -3), dec(-1, 0))
            .build_command(ctx.clone()),
        Err(Error::Command(tickwire_venue::Error::Price))
    ));

    let mut usdm_ctx = ctx.clone();
    usdm_ctx.request.product = Product::UsdM;
    usdm_ctx.instrument.contract = ContractKind::Perpetual;
    let modify_req = ModifyRequest::new(target, Side::Buy, dec(25, -3), dec(6_012_525, -2))
        .with_reduce_only(true)
        .with_minted_client_id(&usdm_ctx, REPLACEMENT_ORDER_LEG)
        .unwrap();
    let modify_cmd = modify_req.build_command(usdm_ctx.clone()).unwrap();
    let domain = ClientIdDomain::from_command_context(&usdm_ctx);
    let tickwire_venue::CommandBody::Modify(ref spec) = modify_cmd.command().body else {
        panic!("expected Modify body");
    };
    assert_eq!(
        SelfDescribingClientId::decode(spec.new_client_id.as_ref().unwrap(), &domain),
        DecodedClientId::Managed(SelfDescribingClientId {
            logical_strategy_id: 8,
            leg: REPLACEMENT_ORDER_LEG,
            epoch: 4,
            intent_id: 77,
        })
    );

    let (mut writer, mut reader) = in_process_intent_pair_with_arena(
        intent_bank_header([2; 16], [99; 16]),
        Some(arena_header([31; 16], [2; 16], [99; 16], 5)),
        1,
        [88; 16],
        7,
        77,
    )
    .unwrap();

    // 1. Modify (`517`, `command_kind = 5`, `REDUCE_ONLY`).
    let modify_intent = OutboundIntent::from_canonical_command(&modify_cmd, 7, 2, NOW_NS)
        .unwrap()
        .with_orig_order_seqnum(50);
    writer.publish(&modify_intent, NOW_NS + 100).unwrap();
    let (rec_mod, mod_arena) = reader.poll().unwrap().unwrap();
    assert!(mod_arena.is_none());
    assert_eq!(rec_mod.header.message_type, 517);
    assert_eq!(rec_mod.body.command_kind, IntentOpcode::Modify.code());
    assert_eq!(rec_mod.body.orig_order_seqnum, 50);
    assert_eq!(
        rec_mod.body.intent_flags & intent_flags::REDUCE_ONLY,
        intent_flags::REDUCE_ONLY
    );

    // 2. Cancel (`514`), CancelReplace (`515`), AmendKeepPriority (`516`), CancelAll (`521` with `ALL_STRATS`), and RFQ (`522..=525`).
    let cancel = OutboundIntent::cancel(
        7,
        2,
        1001,
        5,
        Product::Spot,
        Some(Side::Buy),
        50,
        Some("orig-client-1"),
        None,
        NOW_NS,
    )
    .unwrap();
    writer.publish(&cancel, NOW_NS + 200).unwrap();
    assert_eq!(reader.poll().unwrap().unwrap().0.header.message_type, 514);

    let cancel_replace = OutboundIntent::cancel_replace(
        7,
        2,
        1001,
        5,
        Product::Spot,
        Side::Buy,
        OrderType::Limit,
        Some(TimeInForce::PostOnly),
        tickwire_venue::CancelReplaceMode::StopOnFailure,
        50,
        6_001_000,
        150,
        "repl-client-2",
        Some("orig-client-1"),
        None,
        NOW_NS,
    )
    .unwrap()
    .with_stp_mode(SelfTradePrevention::ExpireTaker);
    writer.publish(&cancel_replace, NOW_NS + 300).unwrap();
    let (rec_cr, _) = reader.poll().unwrap().unwrap();
    assert_eq!(rec_cr.header.message_type, 515);
    assert_eq!(
        rec_cr.body.intent_flags & (intent_flags::POST_ONLY | intent_flags::CHECK_STP),
        intent_flags::POST_ONLY | intent_flags::CHECK_STP
    );

    let amend = OutboundIntent::amend_keep_priority(
        7,
        2,
        1001,
        5,
        Product::Spot,
        50,
        100,
        Some("amend-client-3"),
        Some("repl-client-2"),
        None,
        NOW_NS,
    )
    .unwrap();
    writer.publish(&amend, NOW_NS + 400).unwrap();
    assert_eq!(reader.poll().unwrap().unwrap().0.header.message_type, 516);

    let cancel_all =
        OutboundIntent::cancel_all(ALL_STRATS, 2, 1001, 5, Some(Product::Spot), None, NOW_NS);
    writer.publish(&cancel_all, NOW_NS + 500).unwrap();
    let (rec_ca, _) = reader.poll().unwrap().unwrap();
    assert_eq!(rec_ca.header.message_type, 521);
    assert_eq!(rec_ca.body.strat_id, ALL_STRATS);

    let rfq_new = OutboundIntent::new_rfq_quote(
        7,
        2,
        1001,
        5,
        Product::Option,
        9001,
        10_000,
        50,
        10_200,
        50,
        "rfq-quote-1",
        NOW_NS,
    )
    .unwrap()
    .with_arena_payload(b"oversize-rfq-metadata".to_vec())
    .unwrap();
    writer.publish(&rfq_new, NOW_NS + 600).unwrap();
    let (rec_rfq, rfq_payload) = reader.poll().unwrap().unwrap();
    assert_eq!(rec_rfq.header.message_type, 522);
    assert_eq!(rec_rfq.header.payload_kind, 2);
    assert_eq!(rfq_payload.as_deref(), Some(&b"oversize-rfq-metadata"[..]));
}

#[test]
fn zero_copy_report_receiver_decodes_all_thirteen_report_kinds_and_arena_slices() {
    let identity = ReportIdentity {
        strat_id: 7,
        instance_id: [2; 16],
        strategy_generation: 5,
    };
    let (mut writer, mut receiver) = in_process_report_pair_with_arena(
        report_bank_header([99; 16], [2; 16]),
        Some(arena_header([41; 16], [99; 16], [2; 16], 7)),
        1,
        [55; 16],
        identity,
    )
    .unwrap();

    let base_body = |kind: u16| -> ReportBody {
        let mut client_id = [0u8; 40];
        client_id[..8].copy_from_slice(b"tw1order");
        let mut venue_id = [0u8; 64];
        venue_id[..6].copy_from_slice(b"123456");
        ReportBody {
            ors_order_id: 9001,
            order_seqnum: 42,
            orig_seqnum: 41,
            event_mono_ns: NOW_NS + 500,
            exchange_time_ns: NOW_NS + 400,
            instrument_id: 1001,
            quantity_lots: 100,
            cumulative_qty_lots: 40,
            projected_qty_lots: 60,
            leaves_qty_lots: 60,
            price_ticks: 6_000_000,
            report_kind: kind,
            order_status: 2,
            reason: 0,
            venue_errno: 0,
            strat_id: 7,
            account_ref: 3,
            side: 1,
            position_side: 1,
            client_id_len: 8,
            venue_order_id_len: 6,
            client_id,
            venue_order_id: venue_id,
            ..Default::default()
        }
    };

    // 1. FrontAck (`770`, `report_kind = 2`) + `to_venue_front_ack()`.
    writer
        .publish_report(base_body(2), 5, None, NOW_NS + 500)
        .unwrap();
    let rep_front = receiver.poll().unwrap().unwrap();
    assert_eq!(rep_front.kind, ExecutionReportKind::FrontAck);
    let front_ack = rep_front.to_venue_front_ack().unwrap().unwrap();
    assert_eq!(front_ack.client_id.as_ref().unwrap().as_str(), "tw1order");

    // 2. Ack (`769`, `report_kind = 1`).
    writer
        .publish_report(base_body(1), 5, None, NOW_NS + 501)
        .unwrap();
    assert_eq!(
        receiver.poll().unwrap().unwrap().kind,
        ExecutionReportKind::Ack
    );

    // 3. Nack (`771`, `report_kind = 3`) & CancelNack (`772`, `report_kind = 4`).
    let mut nack = base_body(3);
    nack.reason = TerminalReason::PostOnlyWouldTake.code();
    nack.venue_errno = -5022;
    writer
        .publish_report(nack, 5, Some(b"GTX reject detail"), NOW_NS + 502)
        .unwrap();
    let rep_nack = receiver.poll().unwrap().unwrap();
    assert_eq!(
        rep_nack.terminal_reason(),
        Some(TerminalReason::PostOnlyWouldTake)
    );
    assert!(rep_nack.is_order_lifecycle_terminal());
    assert_eq!(
        rep_nack.arena_payload.as_deref(),
        Some(&b"GTX reject detail"[..])
    );

    let mut cancel_nack = base_body(4);
    cancel_nack.reason = TerminalReason::OrderNotOpen.code();
    writer
        .publish_report(cancel_nack, 5, None, NOW_NS + 503)
        .unwrap();
    let rep_cnack = receiver.poll().unwrap().unwrap();
    assert_eq!(rep_cnack.kind, ExecutionReportKind::CancelNack);
    assert!(!rep_cnack.is_order_lifecycle_terminal());

    // 4. Fill (`773`, `report_kind = 5`): Projected, Execution, and FeeAmendment (`amendment == true`).
    let mut proj_fill = base_body(5);
    proj_fill.projected = 1;
    proj_fill.cumulative_qty_lots = 60;
    proj_fill.projected_qty_lots = 60;
    proj_fill.last_price_ticks = 6_000_000;
    writer
        .publish_report(proj_fill, 5, None, NOW_NS + 504)
        .unwrap();
    let rep_proj = receiver.poll().unwrap().unwrap();
    assert!(matches!(
        rep_proj.kind,
        ExecutionReportKind::Fill(ref f) if f.kind == FillKind::Projected && f.cumulative_qty_lots == 60
    ));

    let mut exec_fill = base_body(5);
    exec_fill.last_qty_lots = 60;
    exec_fill.last_price_ticks = 5_999_900;
    exec_fill.cumulative_qty_lots = 60;
    exec_fill.leaves_qty_lots = 40;
    exec_fill.liquidity = 1;
    exec_fill.has_fee = 1;
    exec_fill.fee_asset_id = 2;
    exec_fill.fee_units = -150;
    exec_fill.asset_exponent = -8;
    exec_fill.venue_trade_id_len = 4;
    exec_fill.venue_trade_id[..4].copy_from_slice(b"tr-1");
    writer
        .publish_report(exec_fill.clone(), 5, None, NOW_NS + 505)
        .unwrap();
    let rep_exec = receiver.poll().unwrap().unwrap();
    assert!(matches!(
        rep_exec.kind,
        ExecutionReportKind::Fill(ref f)
            if f.kind == FillKind::Execution
                && f.liquidity == Liquidity::Maker
                && f.trade_id.as_ref().map(|t| t.as_str()) == Some("tr-1")
    ));

    let mut amend_fill = exec_fill;
    amend_fill.amendment = 1;
    amend_fill.last_qty_lots = 0;
    amend_fill.fee_units = -120;
    writer
        .publish_report(amend_fill, 5, None, NOW_NS + 506)
        .unwrap();
    let rep_amend = receiver.poll().unwrap().unwrap();
    assert!(matches!(
        rep_amend.kind,
        ExecutionReportKind::Fill(ref f)
            if f.kind == FillKind::FeeAmendment && f.fee.is_some_and(|fee| fee.fee_units == -120)
    ));

    // 5. Canceled (`774`), Modified (`775`), ModifyRejected (`776`), ExchRateLimit (`777`),
    //    OrsShutDown (`778`), OrsRestricted (`779`), RfqQuoteAck (`780`), RfqQuoteNack (`781`).
    let mut canceled = base_body(6);
    canceled.reason = TerminalReason::UserCanceled.code();
    canceled.leaves_qty_lots = 0;
    writer
        .publish_report(canceled, 5, None, NOW_NS + 507)
        .unwrap();
    assert_eq!(
        receiver.poll().unwrap().unwrap().kind,
        ExecutionReportKind::Canceled
    );

    writer
        .publish_report(base_body(7), 5, None, NOW_NS + 508)
        .unwrap();
    assert_eq!(
        receiver.poll().unwrap().unwrap().kind,
        ExecutionReportKind::Modified
    );

    let mut mod_rej = base_body(8);
    mod_rej.reason = TerminalReason::NoChange.code();
    writer
        .publish_report(mod_rej, 5, None, NOW_NS + 509)
        .unwrap();
    assert_eq!(
        receiver.poll().unwrap().unwrap().kind,
        ExecutionReportKind::ModifyRejected
    );

    let mut rate = base_body(9);
    rate.strat_id = ALL_STRATS;
    rate.rate_limit_limit = 1200;
    rate.rate_limit_count = 1190;
    rate.rate_limit_interval_num = 1;
    rate.rate_limit_type = 1;
    rate.rate_limit_interval_unit = 2;
    rate.hard_limit_hit = 1;
    writer.publish_report(rate, 5, None, NOW_NS + 510).unwrap();
    assert!(matches!(
        receiver.poll().unwrap().unwrap().kind,
        ExecutionReportKind::ExchRateLimit(ref rl) if rl.hard_limit_hit && rl.count == 1190
    ));

    let mut restricted = base_body(11);
    restricted.strat_id = ALL_STRATS;
    restricted.reason = TerminalReason::RateLimited.code();
    writer
        .publish_report(restricted, 5, None, NOW_NS + 511)
        .unwrap();
    assert_eq!(
        receiver.poll().unwrap().unwrap().kind,
        ExecutionReportKind::OrsRestricted
    );

    let rfq_ack = base_body(12);
    writer
        .publish_report(rfq_ack, 5, None, NOW_NS + 512)
        .unwrap();
    assert_eq!(
        receiver.poll().unwrap().unwrap().kind,
        ExecutionReportKind::RfqQuoteAck
    );

    let mut rfq_nack = base_body(13);
    rfq_nack.reason = TerminalReason::VenueRejected.code();
    writer
        .publish_report(rfq_nack, 5, None, NOW_NS + 513)
        .unwrap();
    assert_eq!(
        receiver.poll().unwrap().unwrap().kind,
        ExecutionReportKind::RfqQuoteNack
    );

    let mut shutdown = base_body(10);
    shutdown.strat_id = ALL_STRATS;
    shutdown.reason = TerminalReason::AdminAction.code();
    writer
        .publish_report(shutdown, 5, None, NOW_NS + 514)
        .unwrap();
    assert_eq!(
        receiver.poll().unwrap().unwrap().kind,
        ExecutionReportKind::OrsShutDown
    );
}

#[test]
fn fill_watermark_tracker_separates_execution_projected_and_fee_amendment_without_double_counting()
{
    let mut tracker = FillWatermarkTracker::new();
    let order = OrderKey {
        intent: IntentKey {
            epoch: 4,
            intent_id: 42,
        },
        client_id: ClientId::new("tw1order").unwrap(),
    };

    let identity = ReportIdentity {
        strat_id: 7,
        instance_id: [2; 16],
        strategy_generation: 5,
    };
    let (mut writer, mut receiver) = in_process_report_pair_with_arena(
        report_bank_header([99; 16], [2; 16]),
        None,
        1,
        [56; 16],
        identity,
    )
    .unwrap();

    let mut client_id = [0u8; 40];
    client_id[..8].copy_from_slice(b"tw1order");
    let lot_step = dec(1, 0);
    let px_tick = dec(1, -2);

    // 1. Projected fill (`cum_qty_lots = 5`) advances `projected_filled` by `+5`, `confirmed_filled == 0`.
    let proj_body = ReportBody {
        order_seqnum: 42,
        instrument_id: 1001,
        quantity_lots: 10,
        cumulative_qty_lots: 5,
        projected_qty_lots: 5,
        leaves_qty_lots: 5,
        last_price_ticks: 6_000_000,
        report_kind: 5,
        order_status: 2,
        strat_id: 7,
        account_ref: 3,
        side: 1,
        position_side: 1,
        projected: 1,
        client_id_len: 8,
        client_id,
        ..Default::default()
    };
    writer
        .publish_report(proj_body.clone(), 5, None, NOW_NS)
        .unwrap();
    let rep_proj = receiver.poll().unwrap().unwrap();
    let out_proj = tracker
        .on_execution_report(&order, &rep_proj, lot_step, px_tick, 0, 2)
        .unwrap();
    assert!(matches!(
        out_proj,
        ReportFillOutcome::Projected(Some(ref d)) if d.delta_quantity == fixed(5, 0)
    ));
    assert_eq!(tracker.confirmed_filled(&order), Some(fixed(0, 0)));
    assert_eq!(tracker.projected_filled(&order), Some(fixed(5, 0)));

    // 2. Execution fill (`last_qty_lots = 3`) settles `3` of the `5` projected lots -> `unprojected_delta == 0`!
    let mut exec_body = ReportBody {
        projected: 0,
        last_qty_lots: 3,
        last_price_ticks: 6_000_000,
        cumulative_qty_lots: 3,
        leaves_qty_lots: 7,
        liquidity: 1,
        venue_trade_id_len: 4,
        ..proj_body
    };
    exec_body.venue_trade_id[..4].copy_from_slice(b"t-01");
    writer
        .publish_report(exec_body.clone(), 5, None, NOW_NS + 1)
        .unwrap();
    let rep_exec = receiver.poll().unwrap().unwrap();
    let out_exec = tracker
        .on_execution_report(&order, &rep_exec, lot_step, px_tick, 0, 2)
        .unwrap();
    assert!(matches!(
        out_exec,
        ReportFillOutcome::Execution(ref s)
            if !s.duplicate && s.already_projected == fixed(3, 0) && s.unprojected_delta == fixed(0, 0)
    ));
    assert_eq!(tracker.confirmed_filled(&order), Some(fixed(3, 0)));
    assert_eq!(tracker.projected_filled(&order), Some(fixed(5, 0)));

    // 3. FeeAmendment (`amendment == 1`) updates fees ONLY and NEVER mutates fill watermarks!
    let amend_body = ReportBody {
        amendment: 1,
        last_qty_lots: 0,
        has_fee: 1,
        fee_asset_id: 2,
        fee_units: -250,
        asset_exponent: -8,
        ..exec_body
    };
    writer
        .publish_report(amend_body, 5, None, NOW_NS + 2)
        .unwrap();
    let rep_amend = receiver.poll().unwrap().unwrap();
    let out_amend = tracker
        .on_execution_report(&order, &rep_amend, lot_step, px_tick, 0, 2)
        .unwrap();
    assert!(matches!(
        out_amend,
        ReportFillOutcome::FeeAmendment(ref fee_out) if fee_out.fee.fee_units == -250
    ));
    assert_eq!(tracker.confirmed_filled(&order), Some(fixed(3, 0)));
    assert_eq!(tracker.projected_filled(&order), Some(fixed(5, 0)));

    // 4. Venue rejection & status classification checks.
    assert_eq!(
        classify_venue_rejection(
            venue_id::BINANCE,
            "-5022",
            "GTX rejected",
            OrderClassificationHints::new()
        ),
        TerminalReason::PostOnlyWouldTake
    );
    assert_eq!(
        classify_order_status(
            venue_id::BINANCE,
            OrderStatus::Expired,
            OrderClassificationHints::new().with_time_in_force(TimeInForce::PostOnly),
        ),
        Some(TerminalReason::PostOnlyWouldTake)
    );
}

#[test]
fn worker_cpu_overlap_fails_fast_and_strategy_cold_escapes_single_core_taskset() {
    // 1. Overlapping `worker_cpu` and `housekeeping_cpus` fails fast.
    let err = worker::validate_affinity(Some(0), Some(&CpuSet::single(0).unwrap())).unwrap_err();
    assert!(matches!(err, Error::Transport(_)));

    // 2. Under a single-core parent thread mask (`taskset` simulation), `"strategy-cold"` spawned via
    //    `worker::spawn_housekeeping` inherits `housekeeping_cpus` (`Cpus_allowed_list`) rather than the hot core.
    let avail: Vec<u32> = current_thread_affinity().unwrap().as_slice().to_vec();
    if avail.len() >= 2 {
        let hot_cpu = avail[0];
        let cold_cpu = avail[1];
        let cold_set = CpuSet::single(cold_cpu).unwrap();

        let observed_status_list = std::thread::spawn(move || {
            pin_current_thread(&CpuSet::single(hot_cpu).unwrap()).unwrap();
            assert_eq!(
                current_thread_affinity().unwrap(),
                CpuSet::single(hot_cpu).unwrap()
            );
            let handle = worker::spawn_housekeeping(Some(&cold_set), move || {
                let status = std::fs::read_to_string("/proc/thread-self/status").unwrap();
                let allowed_line = status
                    .lines()
                    .find(|l| l.starts_with("Cpus_allowed_list:"))
                    .unwrap()
                    .split_once(':')
                    .unwrap()
                    .1
                    .trim()
                    .to_owned();
                let readback = current_thread_affinity().unwrap();
                (allowed_line, readback)
            })
            .unwrap();
            handle.join().unwrap()
        })
        .join()
        .unwrap();

        assert_eq!(observed_status_list.0, cold_cpu.to_string());
        assert_eq!(observed_status_list.1, CpuSet::single(cold_cpu).unwrap());
    }

    // 3. `WorkerDriver` halts order emission upon receiving `OrsShutDown`.
    let states = load_golden_market_states();
    let registry = TopicRegistry {
        catalog_hash: [0x22; 32],
        instrument_set_version: 1,
        instruments: vec![InstrumentSpec {
            instrument_id: 1001,
            instrument_version: 5,
            required_states: QUOTE | TRADE,
        }],
    };
    let mut publisher = Publisher::<4>::create(market_header()).unwrap();
    for s in &states {
        let mut raw = [0u8; MarketRecord::SIZE];
        s.encode(&mut raw).unwrap();
        publisher.publish(&[&raw]).unwrap();
    }
    let mut owner = JoinOwner::new(publisher, registry.clone()).unwrap();
    let (mut sup_server, sup_client) = supervisor_pair();
    let mut store = MockRequestStore::new(16).unwrap();
    let (reg, join) = make_join_registration(&owner, 6);
    let mut raw_join = [0u8; tickwire_ipc::JoinBody::SIZE];
    join.encode(&mut raw_join).unwrap();
    let admin_req = sup_client
        .encode(
            tickwire_ipc::ControlEnvelope {
                opcode: 16,
                flags: 1,
                request_id: [46; 16],
                target_instance: owner.header().producer_instance,
                expected_state_version: owner.membership_version(),
                ..Default::default()
            },
            &raw_join,
        )
        .unwrap();
    let (consumer, replica) = worker::in_process_join_consumer(
        &mut owner,
        &mut sup_server,
        &mut store,
        &admin_req,
        &reg,
        [36; 16],
        32,
        NOW_NS,
        |plan| Ok(build_snapshot(&states, &registry, plan)),
    )
    .unwrap();

    struct AlwaysBuy;
    impl Strategy for AlwaysBuy {
        fn on_market(
            &mut self,
            view: MarketView<'_>,
            _now_ns: u64,
        ) -> tickwire_strategy_sdk::Result<Option<OutboundIntent>> {
            OutboundIntent::new_order(
                7,
                3,
                view.instrument(),
                view.version(),
                Product::Spot,
                Side::Buy,
                OrderType::Limit,
                Some(TimeInForce::Gtc),
                6_000_000,
                10,
                "tw1alwaysbuy",
                view.tick_mono_ns(),
            )
            .map(Some)
        }
    }

    let (intent_writer, mut intent_reader) =
        in_process_intent_pair(intent_bank_header([2; 16], [99; 16]), 1, [89; 16], 7, 1).unwrap();
    let (mut report_writer, report_receiver) = in_process_report_pair_with_arena(
        report_bank_header([99; 16], [2; 16]),
        None,
        1,
        [59; 16],
        ReportIdentity {
            strat_id: 7,
            instance_id: [2; 16],
            strategy_generation: 5,
        },
    )
    .unwrap();
    let mut driver = WorkerDriver::new(
        replica,
        None,
        Some(intent_writer),
        Some(report_receiver),
        AlwaysBuy,
        10_000_000,
        16,
    )
    .unwrap();

    // Publish `OrsShutDown` + a market update -> `WorkerDriver::step` processes both and emits 0 intents!
    report_writer
        .publish_report(
            ReportBody {
                report_kind: 10,
                strat_id: ALL_STRATS,
                reason: TerminalReason::AdminAction.code(),
                ..Default::default()
            },
            5,
            None,
            NOW_NS + 100,
        )
        .unwrap();
    let mut next_mkt = states[0].clone();
    next_mkt.header.sequence = owner.published() + 1;
    next_mkt.body.book_revision += 1;
    let mut raw_mkt = [0u8; MarketRecord::SIZE];
    next_mkt.encode(&mut raw_mkt).unwrap();
    owner.publish(&[&raw_mkt]).unwrap();

    let step = driver.step(&consumer, NOW_NS + 200).unwrap();
    assert!(step.ors_shutdown);
    assert_eq!(step.reports_processed, 1);
    assert_eq!(step.market_records_processed, 1);
    assert_eq!(step.intents_published, 0);
    assert!(intent_reader.poll().unwrap().is_none());
}
