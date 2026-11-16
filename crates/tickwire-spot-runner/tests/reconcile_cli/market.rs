//! Verifies `TopicPublisher::new_spot_quotes` JOIN snapshot initialization, split-pair (`S` / `S+1`)
//! callback withholding when a poll budget stops between `type 265` and `type 261`, and immediate
//! unavailable gate closure (`feed_state = 3` at `S` before stale `type 265` at `S+1`).

use tickwire_book::BookKey;
use tickwire_control::{
    ClientHandshake, Identity, MockRequestStore, Policy, ProcessIdentity, Session,
};
use tickwire_ipc::{ControlEnvelope, JoinBody, MarketState};
use tickwire_join::{JoinOwner, SubscriberRegistration, TopicRegistry, instrument_set_hash};
use tickwire_market_publisher::{Limits, SpotQuote, TopicPublisher};
use tickwire_market_source::protocol::wire_bytes;
use tickwire_md::config::ResourceSpec;
use tickwire_shm::{BankKey, Publisher, SubscriptionKey};
use tickwire_snapshot::{InstrumentSpec, QUOTE};
use tickwire_strategy_sdk::{
    ExecutionReport, OutboundIntent, Strategy, worker::in_process_join_consumer,
};
use tickwire_venue::{
    AssetRef, AssetSymbol, ContractKind, Decimal, InstrumentBinding, Symbol, venue_id,
};

struct CallbackRecorder {
    callbacks: Vec<(u64, i64, i64)>,
}

impl Strategy for CallbackRecorder {
    fn on_market(
        &mut self,
        view: tickwire_strategy_sdk::MarketView<'_>,
        now_ns: u64,
    ) -> tickwire_strategy_sdk::Result<Option<OutboundIntent>> {
        if !view.callback_ready_at(now_ns, 5_000_000_000) {
            return Ok(None);
        }
        if let Some(MarketState::Exact(r)) = view.best_quote() {
            self.callbacks.push((
                r.body.venue_seq_last,
                r.body.bid_price_ticks,
                r.body.ask_price_ticks,
            ));
        }
        Ok(None)
    }

    fn on_report(
        &mut self,
        _report: &ExecutionReport,
        _now_ns: u64,
    ) -> tickwire_strategy_sdk::Result<Option<OutboundIntent>> {
        Ok(None)
    }
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

fn sample_binding() -> InstrumentBinding {
    InstrumentBinding {
        instrument_id: 201,
        instrument_version: 1,
        catalog_version: 1,
        catalog_hash: [0x33; 32],
        symbol: Symbol::new("BTCUSDT").unwrap(),
        base: AssetRef {
            id: 1,
            version: 1,
            symbol: AssetSymbol::new("BTC").unwrap(),
            exponent: -8,
        },
        quote: AssetRef {
            id: 2,
            version: 1,
            symbol: AssetSymbol::new("USDT").unwrap(),
            exponent: -8,
        },
        settlement: AssetRef {
            id: 2,
            version: 1,
            symbol: AssetSymbol::new("USDT").unwrap(),
            exponent: -8,
        },
        price_tick: Decimal::new(1, -1).unwrap(),
        quantity_step: Decimal::new(1, -6).unwrap(),
        contract: ContractKind::Cash,
    }
}

#[test]
fn join_snapshot_and_split_pair_spot_bbo_withhold_until_live_gate_completes() {
    let binding = sample_binding();
    let key = BookKey {
        venue: venue_id::BYBIT,
        source: 1,
        instrument: 201,
    };

    let spec_json = serde_json::json!({
        "identity": {
            "instance": vec![0x50u8; 16],
            "generation": 1,
            "role": 2,
            "logical_strategy": 0,
            "host_boot": vec![0x11u8; 16],
            "namespace": vec![0x22u8; 16],
            "build_hash": vec![0x44u8; 32],
            "qualification_hash": vec![0x45u8; 32],
            "manifest_hash": vec![0x46u8; 32],
            "arch": target_arch_code(),
            "trust_profile": 1
        },
        "resource_id": vec![0x61u8; 16],
        "channel_id": vec![0x62u8; 16],
        "topic_id": 201,
        "channel_epoch": 1,
        "ring_epoch": 1,
        "feed_generation": 1,
        "catalog_version": 1,
        "first_seq": 1,
        "initial_membership_version": 1,
        "capacity": 256,
        "created_mono_ns": 0,
        "arena_classes": [[65536, 32]]
    });
    let resource_spec: ResourceSpec = serde_json::from_value(spec_json).unwrap();
    let instruments = vec![InstrumentSpec {
        instrument_id: binding.instrument_id,
        instrument_version: binding.instrument_version,
        required_states: QUOTE,
    }];
    let set_hash = instrument_set_hash(&instruments);
    let registry = TopicRegistry {
        catalog_hash: binding.catalog_hash,
        instrument_set_version: 1,
        instruments,
    };
    let owner = JoinOwner::new(
        Publisher::<32>::create(resource_spec.header_with_exact_events().unwrap()).unwrap(),
        registry,
    )
    .unwrap();
    let mut topic = TopicPublisher::new_spot_quotes(
        owner,
        vec![(binding.clone(), key)],
        Limits {
            levels_per_side: 1,
            cache_bytes: 1024 * 1024,
            snapshot_bytes: 1024 * 1024,
            max_delta_bytes: 4096,
        },
    )
    .unwrap();

    // Seed an initial two-sided quote (`S = 1`, `S+1 = 2`) before the subscriber JOINs.
    let t0 = 1_000_000_000u64;
    topic
        .on_spot_quote(
            &SpotQuote {
                key,
                instrument: 201,
                version: 1,
                epoch: 1,
                sequence: 10,
                exchange_ns: t0,
                received_ns: t0,
                bid_ticks: 1_000,
                bid_lots: 10,
                ask_ticks: 1_001,
                ask_lots: 20,
            },
            t0,
        )
        .unwrap();
    assert!(topic.state().live());

    // Attach an authenticated in-memory JOIN subscriber (`0` disk writes, `0` unsafe blocks).
    let sub_identity = Identity {
        instance: [0x31; 16],
        generation: 1,
        role: 3,
        logical_strategy: 101,
        host_boot: [0x11; 16],
        namespace: [0x22; 16],
        build_hash: [0x44; 32],
        qualification_hash: [0x45; 32],
        manifest_hash: [0x46; 32],
        arch: target_arch_code(),
        trust_profile: 1,
    };
    let sup_identity = Identity {
        instance: [0x70; 16],
        generation: 1,
        role: 1,
        logical_strategy: 0,
        ..sub_identity.clone()
    };
    let owner_identity = resource_spec.identity.identity();
    let proc = ProcessIdentity {
        pid: std::process::id(),
        uid: 1000,
        gid: 1000,
    };
    let client_policy = Policy {
        local: sup_identity.clone(),
        peer: owner_identity.clone(),
        peer_process: proc,
        required_features: 0x1c,
        supported_features: 0x3ff,
        control_epoch: 1,
        lease_duration_ns: 60_000_000_000,
        max_frame_bytes: 65_536,
        nonce: [0x77; 32],
    };
    let server_policy = Policy {
        local: owner_identity,
        peer: sup_identity,
        ..client_policy.clone()
    };
    let hello = ClientHandshake::new(client_policy, [0x10; 16]).unwrap();
    let (mut sup_server, welcome) =
        Session::accept(&server_policy, proc, hello.hello(), t0).unwrap();
    let sup_client = hello.finish(proc, &welcome, t0).unwrap();

    let (join_owner, topic_state) = topic.split();
    let h = join_owner.header();
    let sub = SubscriptionKey {
        bank: BankKey {
            resource: u128::from_le_bytes(h.resource_id),
            channel: u128::from_le_bytes(h.channel_id),
            channel_epoch: h.channel_epoch,
            ring_epoch: h.ring_epoch,
            feed_generation: h.feed_generation,
        },
        instance: u128::from_le_bytes(sub_identity.instance),
        generation: 1,
        cursor: u128::from_le_bytes([0x88; 16]),
    };
    let join_req = JoinBody {
        subscriber_instance: sub_identity.instance,
        logical_strategy_id: 101,
        subscription_generation: 1,
        topic_id: h.topic_id,
        channel_id: h.channel_id,
        channel_epoch: h.channel_epoch,
        feed_generation: h.feed_generation,
        ring_epoch: h.ring_epoch,
        cursor_resource_id: [0x88; 16],
        bank_resource_id: h.resource_id,
        readiness_deadline_mono_ns: t0 + 10_000_000_000,
        expected_catalog_version: h.catalog_version,
        qos: 1,
        role: 3,
        join_flags: 3,
        topic_instrument_set_version: 1,
        topic_instrument_set_hash: set_hash,
        ..Default::default()
    };
    let admin_frame = sup_client
        .encode(
            ControlEnvelope {
                opcode: 16,
                flags: 1,
                request_id: [0x20; 16],
                target_instance: h.producer_instance,
                expected_state_version: join_owner.membership_version(),
                ..Default::default()
            },
            &wire_bytes(&join_req).unwrap(),
        )
        .unwrap();

    let mut req_store = MockRequestStore::new(16).unwrap();
    let (consumer, mut replica) = in_process_join_consumer(
        join_owner,
        &mut sup_server,
        &mut req_store,
        &admin_frame,
        &SubscriberRegistration {
            identity: sub_identity,
            subscription: sub,
            qos: 1,
        },
        [0x99; 16],
        1,
        t0,
        |plan| topic_state.snapshot(plan, t0, join_req.readiness_deadline_mono_ns),
    )
    .unwrap();

    // 1. JOIN baseline initializes the replica (`seq = 10`) without firing any callback; depth
    //    remains explicitly uncovered (`best_bid()` / `best_ask()` == `None`).
    let view = replica.view(201).unwrap();
    assert!(view.best_bid().is_none());
    assert!(view.best_ask().is_none());
    match view.best_quote().unwrap() {
        MarketState::Exact(r) => {
            assert_eq!(r.body.venue_seq_last, 10);
            assert_eq!(r.body.bid_price_ticks, 1_000);
            assert_eq!(r.body.ask_price_ticks, 1_001);
        }
        MarketState::Legacy(_) => panic!("expected exact BBO1 quote"),
    }

    // 2. Publish a live quote (`S = 3` type 265, `S+1 = 4` type 261). Stopping at budget = 1 (`S`)
    //    must withhold the strategy callback until `S+1` completes the pair.
    let t1 = t0 + 10_000_000;
    topic
        .on_spot_quote(
            &SpotQuote {
                key,
                instrument: 201,
                version: 1,
                epoch: 1,
                sequence: 11,
                exchange_ns: t1,
                received_ns: t1,
                bid_ticks: 1_005,
                bid_lots: 15,
                ask_ticks: 1_006,
                ask_lots: 25,
            },
            t1,
        )
        .unwrap();

    let mut recorder = CallbackRecorder {
        callbacks: Vec::new(),
    };
    let consumed_s = replica
        .drain(&consumer, &mut recorder, t1, 5_000_000_000, 1, |_| Ok(()))
        .unwrap();
    assert_eq!(consumed_s, 1);
    assert!(
        recorder.callbacks.is_empty(),
        "split-pair stop at S must withhold callback until S+1"
    );

    let consumed_s1 = replica
        .drain(&consumer, &mut recorder, t1, 5_000_000_000, 1, |_| Ok(()))
        .unwrap();
    assert_eq!(consumed_s1, 1);
    assert_eq!(recorder.callbacks, vec![(11, 1_005, 1_006)]);

    // 3. Unavailable transition publishes `feed_state = 3` (`type 261`) at `S` BEFORE the stale
    //    `type 265` quote at `S+1`, closing the live gate immediately with zero callbacks.
    let t2 = t1 + 10_000_000;
    topic.spot_quote_unavailable(201, 1, t2).unwrap();
    let consumed_unavail = replica
        .drain(&consumer, &mut recorder, t2, 5_000_000_000, 8, |_| Ok(()))
        .unwrap();
    assert_eq!(consumed_unavail, 2);
    assert_eq!(recorder.callbacks.len(), 1);
    assert!(
        !replica
            .view(201)
            .unwrap()
            .callback_ready_at(t2, 5_000_000_000)
    );
}
