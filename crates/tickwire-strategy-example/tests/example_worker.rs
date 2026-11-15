#![forbid(unsafe_code)]

use std::{
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

use sha2::{Digest, Sha256};
use tickwire_control::{
    ClientHandshake, Identity, MockRequestStore, Policy, ProcessIdentity, Session,
};
use tickwire_ipc::{
    DepthCoverage, DepthLevel, DopEchoBody, MarketBody, MarketRecord, MktSession, RecordHeader,
    ReportBody, ResourceHeader, Wire,
};
use tickwire_join::{JoinOwner, JoinPlan, SubscriberRegistration, TopicRegistry};
use tickwire_shm::{Publisher, SubscriptionKey};
use tickwire_snapshot::{
    BuildRequest, BuiltTopic, InstrumentSnapshot, InstrumentSpec, QUOTE, TRADE,
    build_topic_with_coverage,
};
use tickwire_strategy_example::{Parameters, Registration, V1, V2};
use tickwire_strategy_sdk::{
    ArtifactDigest, DEFAULT_MAX_ORDER_AGE_NS, DopConfig, DopSession, ReportIdentity, WorkerDriver,
    accept_dop_login, in_process_intent_pair, in_process_report_pair, local_seqpacket_pair,
    mint_self_describing_client_id,
    registered::{FactoryBinding, TargetIdentity, configure_from_file},
    worker,
};
use tickwire_venue::{
    AssetRef, AssetSymbol, BoundedList, CanonicalCommand, Command, CommandBody, CommandContext,
    ContractKind, Decimal, Environment, InstrumentBinding, OrderSpec, OrderType, PositionSide,
    Product, ProductMode, Quantity, RequestIdentity, SelfTradePrevention, Side, StrategyContext,
    Symbol, TimeInForce, TriggerRef, venue_id,
};

const NOW_NS: u64 = 2_000_000_000;
const DEADLINE_NS: u64 = 6_000_000_000;

static NEXT_TMP: AtomicU64 = AtomicU64::new(1);

struct TempDir(PathBuf);

impl TempDir {
    fn create(prefix: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "tw-example-{prefix}-{}-{}",
            std::process::id(),
            NEXT_TMP.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&dir).unwrap();
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
        Self(dir)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn dec(coeff: i128, exp: i8) -> Decimal {
    Decimal::new(coeff, exp).unwrap()
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

fn asset(id: u64, symbol: &str) -> AssetRef {
    AssetRef {
        id,
        version: 1,
        symbol: AssetSymbol::new(symbol).unwrap(),
        exponent: -8,
    }
}

fn sample_command(target: &TargetIdentity, revision: u64) -> CanonicalCommand {
    let ctx = CommandContext {
        request: RequestIdentity {
            risk_domain_id: [1; 16],
            venue: venue_id::BINANCE,
            environment: Environment::Test,
            account: u64::from(target.account_ref.max(1)),
            product: Product::Spot,
            intent_id: 10,
            intent_exec_epoch: 1,
            strategy_generation: target.generation,
            config_revision: revision,
            deadline: DEADLINE_NS,
            recovery_object: 1,
        },
        strategy: StrategyContext {
            logical_strategy_id: target.logical_strategy_id,
            instance_id: target.instance_id,
            strategy_intent_seq: 1,
            authorization_epoch: 1,
            risk_policy_version: 1,
            order_config_version: revision,
            created_mono_ns: NOW_NS,
            economic_intent_id: [7; 16],
            hedge_group_id: [0; 16],
            leg_id: 0,
            recovery_manifest_hash: [8; 32],
        },
        instrument: InstrumentBinding {
            instrument_id: 1001,
            instrument_version: 5,
            catalog_version: 1,
            catalog_hash: [0x22; 32],
            symbol: Symbol::new("BTCUSDT").unwrap(),
            base: asset(1, "BTC"),
            quote: asset(2, "USDT"),
            settlement: asset(2, "USDT"),
            price_tick: dec(1, -2),
            quantity_step: dec(1, -5),
            contract: ContractKind::Cash,
        },
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
    };
    let (client_id, _) = mint_self_describing_client_id(&ctx, 0).unwrap();
    CanonicalCommand::new(Command {
        context: ctx,
        body: CommandBody::New(OrderSpec {
            client_id,
            side: Side::Buy,
            order_type: OrderType::Limit,
            quantity: Quantity::Base(dec(15, -4)),
            price: Some(dec(60_000, 0)),
            time_in_force: Some(TimeInForce::Gtc),
            position_side: PositionSide::Both,
            mode: ProductMode::Spot,
            reduce_only: false,
            self_trade_prevention: SelfTradePrevention::Default,
            trigger: None,
            trailing: None,
            iceberg_quantity: None,
        }),
    })
    .unwrap()
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
        required_features: 0x1c | 64,
        trust_profile: 1,
        event_class: 7,
        topic_id: 1,
        catalog_version: 1,
        qos: 1,
        ..Default::default()
    }
}

fn intent_bank_header(producer: [u8; 16], consumer: [u8; 16], generation: u64) -> ResourceHeader {
    ResourceHeader {
        magic: *b"TWIPC001",
        abi_major: 1,
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
        producer_instance: producer,
        producer_generation: generation,
        consumer_instance: consumer,
        host_boot_id: [8; 16],
        namespace_id: [9; 16],
        risk_scope: 900,
        producer_page_offset: 4096,
        slot_offset: 8192,
        capacity: 64,
        record_bytes: 1024,
        resource_bytes: 8192 + 64 * 1024,
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

fn report_bank_header(producer: [u8; 16], consumer: [u8; 16]) -> ResourceHeader {
    ResourceHeader {
        magic: *b"TWIPC001",
        abi_major: 1,
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
        producer_instance: producer,
        producer_generation: 7,
        consumer_instance: consumer,
        host_boot_id: [8; 16],
        namespace_id: [9; 16],
        risk_scope: 900,
        producer_page_offset: 4096,
        slot_offset: 8192,
        capacity: 64,
        record_bytes: 512,
        resource_bytes: 8192 + 64 * 512,
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

#[test]
fn v1_and_v2_registrations_verify_parameters_and_reject_invalid_inputs() {
    let target = TargetIdentity {
        logical_strategy_id: 7,
        strat_id: 4,
        instance_id: [4; 16],
        generation: 1,
        account_ref: 2,
    };

    // Zero minimum_observations and negative minimum_bid_ticks fail fast.
    let bad_obs = Parameters {
        schema: 1,
        enabled: false,
        minimum_observations: 0,
        minimum_bid_ticks: 0,
        command: None,
    };
    assert!(Parameters::decode(&bad_obs.bytes().unwrap()).is_err());

    let bad_ticks = Parameters {
        schema: 1,
        enabled: false,
        minimum_observations: 1,
        minimum_bid_ticks: -1,
        command: None,
    };
    assert!(Parameters::decode(&bad_ticks.bytes().unwrap()).is_err());

    // Valid V1 and V2 bindings verify SHA-256 and enforce distinct `state_schema` values.
    let dir = TempDir::create("params");
    let cmd = sample_command(&target, 3);
    let params = Parameters {
        schema: 1,
        enabled: true,
        minimum_observations: 2,
        minimum_bid_ticks: 5_000_000,
        command: Some(cmd.bytes().to_vec()),
    };
    let raw_params = params.bytes().unwrap();
    let cfg_path = dir.0.join("counting.json");
    std::fs::write(&cfg_path, &raw_params).unwrap();
    std::fs::set_permissions(&cfg_path, std::fs::Permissions::from_mode(0o600)).unwrap();

    let binding_v2 = FactoryBinding {
        id: "counting_example".into(),
        state_schema: V2,
        configuration_file: cfg_path.clone(),
        configuration: ArtifactDigest::of(&raw_params),
        revision: 3,
    };
    let configured_v2 = configure_from_file(&Registration::v2(), &binding_v2, &target).unwrap();
    let counting_v2 = configured_v2.create().unwrap();
    assert_eq!(counting_v2.schema(), V2);
    assert!(!counting_v2.spent());

    // Passing V2 binding to V1 executable fails schema verification.
    assert!(configure_from_file(&Registration::v1(), &binding_v2, &target).is_err());

    let binding_v1 = FactoryBinding {
        state_schema: V1,
        ..binding_v2
    };
    let counting_v1 = configure_from_file(&Registration::v1(), &binding_v1, &target)
        .unwrap()
        .create()
        .unwrap();
    assert_eq!(counting_v1.schema(), V1);
}

#[test]
fn counting_strategy_runs_end_to_end_over_shm_join_dop_intent_and_report_rings() {
    let target = TargetIdentity {
        logical_strategy_id: 7,
        strat_id: 4,
        instance_id: [4; 16],
        generation: 1,
        account_ref: 0,
    };
    let dir = TempDir::create("e2e");
    let cmd = sample_command(&target, 5);
    let params = Parameters {
        schema: 1,
        enabled: true,
        minimum_observations: 2,
        minimum_bid_ticks: 5_900_000,
        command: Some(cmd.bytes().to_vec()),
    };
    let raw_params = params.bytes().unwrap();
    let cfg_path = dir.0.join("counting-e2e.json");
    std::fs::write(&cfg_path, &raw_params).unwrap();
    std::fs::set_permissions(&cfg_path, std::fs::Permissions::from_mode(0o600)).unwrap();

    let binding = FactoryBinding {
        id: "counting_example".into(),
        state_schema: V2,
        configuration_file: cfg_path,
        configuration: ArtifactDigest::of(&raw_params),
        revision: 5,
    };
    let strategy = configure_from_file(&Registration::v2(), &binding, &target)
        .unwrap()
        .create()
        .unwrap();

    // 1. Join live `tickwire-md` SHM broadcast ring at an independent reader cursor.
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

    let md_identity = Identity {
        instance: [3; 16],
        generation: 1,
        role: 2,
        logical_strategy: 0,
        host_boot: [8; 16],
        namespace: [9; 16],
        build_hash: [10; 32],
        qualification_hash: [11; 32],
        manifest_hash: [12; 32],
        arch: target_arch_code(),
        trust_profile: 1,
    };
    let sup_identity = Identity {
        instance: [1; 16],
        role: 1,
        ..md_identity.clone()
    };
    let worker_identity = Identity {
        instance: target.instance_id,
        generation: target.generation,
        role: 3,
        logical_strategy: target.logical_strategy_id,
        ..md_identity.clone()
    };
    let process = ProcessIdentity {
        pid: 100,
        uid: 0,
        gid: 0,
    };
    let nonce: [u8; 32] = Sha256::digest(NOW_NS.to_le_bytes()).into();
    let sup_server_policy = Policy {
        local: md_identity.clone(),
        peer: sup_identity.clone(),
        peer_process: process,
        required_features: 0x1c,
        supported_features: 0x1f | 64 | 512,
        control_epoch: 7,
        lease_duration_ns: 60_000_000_000,
        max_frame_bytes: 65_536,
        nonce,
    };
    let sup_client_policy = Policy {
        local: sup_identity,
        peer: md_identity,
        ..sup_server_policy.clone()
    };
    let hello = ClientHandshake::new(sup_client_policy, [1; 16]).unwrap();
    let (mut sup_server, welcome) =
        Session::accept(&sup_server_policy, process, hello.hello(), NOW_NS).unwrap();
    let sup_client = hello.finish(process, &welcome, NOW_NS).unwrap();
    let mut req_store = MockRequestStore::new(16).unwrap();

    let h = owner.header();
    let reg = SubscriberRegistration {
        identity: worker_identity.clone(),
        subscription: SubscriptionKey {
            bank: tickwire_shm::BankKey {
                resource: u128::from_le_bytes(h.resource_id),
                channel: u128::from_le_bytes(h.channel_id),
                channel_epoch: h.channel_epoch,
                ring_epoch: h.ring_epoch,
                feed_generation: h.feed_generation,
            },
            instance: u128::from_le_bytes(target.instance_id),
            generation: 1,
            cursor: u128::from_le_bytes([104; 16]),
        },
        qos: 1,
    };
    let join_body = tickwire_ipc::JoinBody {
        subscriber_instance: target.instance_id,
        logical_strategy_id: target.logical_strategy_id,
        subscription_generation: 1,
        topic_id: h.topic_id,
        channel_id: h.channel_id,
        channel_epoch: h.channel_epoch,
        feed_generation: h.feed_generation,
        ring_epoch: h.ring_epoch,
        cursor_resource_id: [104; 16],
        bank_resource_id: h.resource_id,
        readiness_deadline_mono_ns: DEADLINE_NS,
        expected_catalog_version: h.catalog_version,
        qos: 1,
        role: 3,
        join_flags: 3,
        topic_instrument_set_version: 1,
        topic_instrument_set_hash: tickwire_join::instrument_set_hash(&registry.instruments),
        ..Default::default()
    };
    let mut raw_join = [0u8; tickwire_ipc::JoinBody::SIZE];
    join_body.encode(&mut raw_join).unwrap();
    let admin_req = sup_client
        .encode(
            tickwire_ipc::ControlEnvelope {
                opcode: 16,
                flags: 1,
                request_id: [77; 16],
                target_instance: h.producer_instance,
                expected_state_version: owner.membership_version(),
                ..Default::default()
            },
            &raw_join,
        )
        .unwrap();
    let build_snap = |plan: &JoinPlan| -> tickwire_ipc::Result<BuiltTopic> {
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
            snapshot_id: [32; 16],
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
            states: &states,
            bids: &bids,
            asks: &asks,
        }];
        build_topic_with_coverage(
            &BuildRequest {
                resource: &plan.resource,
                join: &plan.join,
                catalog_hash: registry.catalog_hash,
                instrument_set_version: 1,
                sequence: plan.sequence,
                created_mono_ns: NOW_NS,
                expires_mono_ns: DEADLINE_NS,
                now_mono_ns: NOW_NS,
                logical_strategy_id: target.logical_strategy_id,
                instruments: &registry.instruments,
                members: &members,
                byte_limit: 64 * 1024 * 1024,
            },
            &coverage,
        )
    };
    let (consumer, replica) = worker::in_process_join_consumer(
        &mut owner,
        &mut sup_server,
        &mut req_store,
        &admin_req,
        &reg,
        [31; 16],
        32,
        NOW_NS,
        build_snap,
    )
    .unwrap();

    // 2. Complete stateless `DopLogin` -> `DopEcho` (`next_source_order_seqnum = 10`) + `OrsSessions` (`account_ref = 2`).
    let deadline = Instant::now() + Duration::from_secs(2);
    let (mut ors_server, dop_client) = local_seqpacket_pair(deadline).unwrap();
    let dop_handle = std::thread::spawn(move || {
        let echo = DopEchoBody {
            next_source_order_seqnum: 10,
            hb_interval_ms: 500,
            strat_id: target.strat_id,
            connected: 1,
            enabled: 1,
            market_connected: 1,
            ready_to_trade: 1,
            refusal_reason: 0,
            ..Default::default()
        };
        let sessions = [MktSession {
            local_ip: 0x7f00_0001,
            product_mask: Product::Spot.mask_bit(),
            account_ref: 2,
            strat_id: target.strat_id,
            authorized: 1,
            ready: 1,
            colo: 1,
            _pad_11: [0; 5],
        }];
        accept_dop_login(
            &mut ors_server,
            [99; 16],
            target.instance_id,
            3,
            &echo,
            &sessions,
            NOW_NS,
            0,
            deadline,
        )
        .unwrap();
        ors_server
    });
    let dop = DopSession::login_over_connection(
        dop_client,
        DopConfig {
            strat_id: target.strat_id,
            client_id: 1,
            local_instance: target.instance_id,
            peer_instance: [99; 16],
            control_epoch: 3,
            expected_peer_uid: None,
            max_order_age_ns: DEFAULT_MAX_ORDER_AGE_NS,
        },
        0,
        NOW_NS,
        deadline,
    )
    .unwrap();
    let _ors_server = dop_handle.join().unwrap();

    // 3. Wire inline `IntentWriter`/`IntentReader` (`1024B`) and `ReportWriter`/`ReportReceiver` (`512B`).
    let (intent_writer, mut intent_reader) = in_process_intent_pair(
        intent_bank_header(target.instance_id, [99; 16], target.generation),
        1,
        [81; 16],
        target.strat_id,
        1,
    )
    .unwrap();
    let (mut report_writer, report_receiver) = in_process_report_pair(
        report_bank_header([99; 16], target.instance_id),
        1,
        [82; 16],
        ReportIdentity {
            strat_id: target.strat_id,
            instance_id: target.instance_id,
            strategy_generation: target.generation,
        },
    )
    .unwrap();

    let mut driver = WorkerDriver::new(
        replica,
        Some(dop),
        Some(intent_writer),
        Some(report_receiver),
        strategy,
        10_000_000,
        16,
    )
    .unwrap();

    // Publish 3 market quotes; `minimum_observations == 2` triggers exactly once on quote #2!
    for i in 1..=3u64 {
        let mut q = states[0].clone();
        q.header.sequence = owner.published() + 1;
        q.header.ingress_mono_ns = NOW_NS + i * 1_000;
        q.header.publish_mono_ns = NOW_NS + i * 1_000 + 100;
        q.body.book_revision = 1 + i;
        let mut raw = [0u8; MarketRecord::SIZE];
        q.encode(&mut raw).unwrap();
        owner.publish(&[&raw]).unwrap();

        let step = driver.step(&consumer, NOW_NS + i * 1_000 + 5_000).unwrap();
        assert_eq!(step.market_records_processed, 1);
        assert_eq!(step.intents_published, usize::from(i == 2));
    }

    assert_eq!(driver.strategy().observations(), 3);
    assert!(driver.strategy().spent());

    let (emitted, arena_bytes) = intent_reader.poll().unwrap().expect("single-shot intent");
    assert!(arena_bytes.is_none());
    assert_eq!(emitted.header.message_type, 513);
    assert_eq!(emitted.body.order_seqnum, 10);
    assert_eq!(emitted.body.msg_seqnum, 1);
    assert_eq!(emitted.body.account_ref, 2);
    assert_eq!(emitted.body.strat_id, target.strat_id);
    assert_eq!(emitted.body.tick_mono_ns, NOW_NS + 2_000);
    assert_eq!(emitted.body.price_ticks, 6_000_000);
    assert_eq!(emitted.body.qty_lots, 150);
    assert!(intent_reader.poll().unwrap().is_none());

    // Publish `FrontAck`, `Ack`, `Fill` (`Projected`), `Fill` (`Execution`), and `Fill` (`FeeAmendment`).
    let mut client_id = [0u8; 40];
    client_id[..emitted.body.client_id_len as usize]
        .copy_from_slice(&emitted.body.client_id[..emitted.body.client_id_len as usize]);
    let base_rep = ReportBody {
        order_seqnum: 10,
        instrument_id: 1001,
        quantity_lots: 150,
        leaves_qty_lots: 150,
        price_ticks: 6_000_000,
        order_status: 1,
        strat_id: target.strat_id,
        account_ref: 2,
        side: 1,
        position_side: 1,
        client_id_len: emitted.body.client_id_len,
        client_id,
        ..Default::default()
    };

    // FrontAck (`2`), Ack (`1`), Projected Fill (`5`), Execution Fill (`5`), FeeAmendment (`5`).
    report_writer
        .publish_report(
            ReportBody {
                report_kind: 2,
                ..base_rep.clone()
            },
            5,
            None,
            NOW_NS + 10_000,
        )
        .unwrap();
    report_writer
        .publish_report(
            ReportBody {
                report_kind: 1,
                ..base_rep.clone()
            },
            5,
            None,
            NOW_NS + 10_100,
        )
        .unwrap();
    report_writer
        .publish_report(
            ReportBody {
                report_kind: 5,
                projected: 1,
                cumulative_qty_lots: 150,
                projected_qty_lots: 150,
                leaves_qty_lots: 0,
                last_price_ticks: 6_000_000,
                order_status: 3,
                ..base_rep.clone()
            },
            5,
            None,
            NOW_NS + 10_200,
        )
        .unwrap();
    let mut exec_rep = ReportBody {
        report_kind: 5,
        last_qty_lots: 150,
        last_price_ticks: 6_000_000,
        cumulative_qty_lots: 150,
        leaves_qty_lots: 0,
        liquidity: 1,
        order_status: 3,
        venue_trade_id_len: 5,
        ..base_rep.clone()
    };
    exec_rep.venue_trade_id[..5].copy_from_slice(b"tr-99");
    report_writer
        .publish_report(exec_rep.clone(), 5, None, NOW_NS + 10_300)
        .unwrap();
    report_writer
        .publish_report(
            ReportBody {
                amendment: 1,
                last_qty_lots: 0,
                has_fee: 1,
                fee_asset_id: 2,
                fee_units: -50,
                asset_exponent: -8,
                ..exec_rep
            },
            5,
            None,
            NOW_NS + 10_400,
        )
        .unwrap();

    let rep_step = driver.step(&consumer, NOW_NS + 11_000).unwrap();
    assert_eq!(rep_step.reports_processed, 5);
    assert_eq!(driver.strategy().reports(), 5);
    assert_eq!(driver.strategy().fills(), 1);
}
