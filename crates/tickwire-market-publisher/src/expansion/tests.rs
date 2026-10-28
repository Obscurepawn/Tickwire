use super::*;
use tickwire_book::{BinanceSpot, Book, DepthEvent, Level, PriceTicks, QtyLots, Snapshot};
use tickwire_shm::Publisher;
use tickwire_venue::{AssetRef, AssetSymbol, ContractKind, Decimal, Symbol};

fn config(id: u64, local: u32) -> InstrumentConfig {
    let asset = |id, name| AssetRef {
        id,
        version: 1,
        symbol: AssetSymbol::new(name).unwrap(),
        exponent: -8,
    };
    InstrumentConfig {
        instrument: InstrumentBinding {
            instrument_id: id,
            instrument_version: 1,
            catalog_version: 1,
            catalog_hash: [22; 32],
            symbol: Symbol::new("BTCUSDT").unwrap(),
            base: asset(1, "BTC"),
            quote: asset(2, "USDT"),
            settlement: asset(2, "USDT"),
            price_tick: Decimal::new(1, 0).unwrap(),
            quantity_step: Decimal::new(1, 0).unwrap(),
            contract: ContractKind::Cash,
        },
        key: BookKey {
            venue: 1,
            instrument: local,
            source: 1,
        },
        market: Market::Spot,
        time_unit: TimeUnit::Milliseconds,
    }
}
fn registry(ids: &[u64], version: u64) -> TopicRegistry {
    TopicRegistry {
        catalog_hash: [22; 32],
        instrument_set_version: version,
        instruments: ids
            .iter()
            .map(|&id| InstrumentSpec {
                instrument_id: id,
                instrument_version: 1,
                required_states: 1,
            })
            .collect(),
    }
}
fn header() -> ResourceHeader {
    let mut h = ResourceHeader {
        magic: *b"TWIPC001",
        abi_major: 1,
        header_bytes: 4096,
        resource_kind: 1,
        target_arch: if cfg!(target_arch = "x86_64") { 1 } else { 2 },
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
        required_features: 0x25e,
        trust_profile: 1,
        event_class: 7,
        topic_id: 1,
        catalog_version: 1,
        qos: 1,
        ..Default::default()
    };
    tickwire_shm::configure_market_arena(&mut h, &[(65536, 32)]).unwrap();
    h
}
fn limits() -> Limits {
    Limits {
        levels_per_side: 8,
        cache_bytes: 1 << 20,
        snapshot_bytes: 1 << 20,
        max_delta_bytes: 1 << 20,
    }
}
fn publisher() -> TopicPublisher<4> {
    let owner = JoinOwner::new(Publisher::create(header()).unwrap(), registry(&[20], 1)).unwrap();
    TopicPublisher::new(owner, vec![config(20, 0)], limits()).unwrap()
}
fn book(id: u32, received: u64, price: i64) -> Book<BinanceSpot, 8, 8> {
    let key = config(20, id).key;
    let mut b = Book::new(key, 1);
    b.install_snapshot(Snapshot {
        key,
        generation: 1,
        last_update_id: 1,
        bids: &[Level {
            price: PriceTicks(price),
            quantity: QtyLots(1),
        }],
        asks: &[Level {
            price: PriceTicks(price + 10),
            quantity: QtyLots(2),
        }],
        bids_complete: true,
        asks_complete: true,
    })
    .unwrap();
    b.apply(DepthEvent {
        key,
        generation: 1,
        first_update_id: 2,
        last_update_id: 2,
        previous_update_id: None,
        received_at_ns: received,
        bids: &[],
        asks: &[],
    })
    .unwrap();
    b
}
fn notice(b: &Book<BinanceSpot, 8, 8>, now: u64) -> Notice<'_> {
    Notice::Sync {
        connection_epoch: 1,
        book_generation: 1,
        received_mono_ns: Some(now),
        raw_delta: None,
        encoding: DepthEncoding::Json,
        outcome: SyncOutcome::Applied(tickwire_book::ApplyOutcome::Applied {
            last_update_id: 2,
            became_live: true,
        }),
        view: Some(b.view(now, 1000).unwrap()),
    }
}
#[derive(Default)]
struct Faults(usize);
impl FaultSink for Faults {
    fn invalidate(&mut self, _: Fault) {
        self.0 += 1;
    }
}
fn replacement(p: &TopicPublisher<4>, reg: TopicRegistry) -> JoinOwner<4> {
    let mut h = p.owner.header().clone();
    h.resource_id[0] += 1;
    h.predecessor_ring_epoch = h.ring_epoch;
    h.ring_epoch += 1;
    h.feed_generation += 1;
    h.cutover_seq = p.owner.published();
    h.first_seq = h.cutover_seq + 1;
    h.descriptor_hash = [0; 32];
    JoinOwner::new(Publisher::create(h).unwrap(), reg).unwrap()
}

#[test]
fn candidate_keeps_old_chain_live_then_commits_latest_old_and_warmed_new_rows() {
    let mut p = publisher();
    let mut faults = Faults::default();
    let old = book(0, 10, 100);
    let old_slot = p.instrument_slot(20).unwrap();
    p.on_notice(20, notice(&old, 10), 10, &mut faults).unwrap();
    let mut candidate = p
        .prepare_expansion(
            registry(&[10, 20], 2),
            vec![config(10, 1)],
            limits(),
            2 << 20,
        )
        .unwrap();
    assert!(!candidate.ready(10, 100));
    let added = book(1, 11, 200);
    candidate.warm_notice(10, &notice(&added, 11), 11).unwrap();
    assert!(candidate.ready(11, 100));
    // Old publication can advance while the candidate is private.
    let latest = book(0, 12, 110);
    p.on_notice(20, notice(&latest, 12), 12, &mut faults)
        .unwrap();
    let cut = p.owner.published();
    let retained_slot = candidate.instrument_slot(20).unwrap();
    let added_slot = candidate.instrument_slot(10).unwrap();
    assert!(candidate.instrument_slot(u64::MAX).is_err());
    assert_ne!(retained_slot.index(), old_slot.index());
    let next = replacement(&p, registry(&[10, 20], 2));
    let retired = p
        .commit_expansion(candidate, next, 12, 100, &mut faults)
        .unwrap();
    assert_eq!(retired.published(), cut);
    assert_eq!(p.owner.published(), cut + 4);
    assert_eq!(p.owner.registry().instrument_set_version, 2);
    assert!(p.state.live());
    assert_eq!(p.state.rows[0].key.instrument, 1);
    assert_eq!(p.state.rows[1].key.instrument, 0);
    assert_eq!(p.state.rows[0].bids[0].price_ticks, 200);
    assert_eq!(p.state.rows[1].bids[0].price_ticks, 110);
    assert_eq!(faults.0, 0);
    assert!(
        p.on_notice_slot(old_slot, &notice(&latest, 12), 12, &mut faults)
            .is_err()
    );
    assert_eq!(p.instrument_slot(20).unwrap().index(), 1);
    let latest_added = book(1, 14, 210);
    let latest_retained = book(0, 15, 120);
    p.on_notice_slot(added_slot, &notice(&latest_added, 14), 14, &mut faults)
        .unwrap();
    p.on_notice_slot(
        retained_slot,
        &notice(&latest_retained, 15),
        15,
        &mut faults,
    )
    .unwrap();
    assert_eq!(p.owner.published(), cut + 8);
    assert_eq!(p.state.rows[0].spec.instrument_id, 10);
    assert_eq!(p.state.rows[0].bids[0].price_ticks, 210);
    assert_eq!(p.state.rows[1].spec.instrument_id, 20);
    assert_eq!(p.state.rows[1].bids[0].price_ticks, 120);
    let built = snapshot(&p, 16);
    assert_eq!(built.bytes().len(), snapshot_bytes(&p.state.rows).unwrap());
}

fn snapshot(p: &TopicPublisher<4>, now: u64) -> BuiltTopic {
    let h = p.owner.header();
    let mut resource = h.clone();
    resource.resource_id = [95; 16];
    resource.parent_resource_id = h.resource_id;
    resource.consumer_instance = [7; 16];
    let join = JoinBody {
        subscriber_instance: [7; 16],
        logical_strategy_id: 7,
        subscription_generation: 1,
        topic_id: h.topic_id,
        channel_id: h.channel_id,
        channel_epoch: h.channel_epoch,
        feed_generation: h.feed_generation,
        ring_epoch: h.ring_epoch,
        cursor_resource_id: [94; 16],
        bank_resource_id: h.resource_id,
        readiness_deadline_mono_ns: 1000,
        expected_catalog_version: h.catalog_version,
        membership_version: p.owner.membership_version(),
        qos: h.qos,
        role: 3,
        join_flags: 3,
        topic_instrument_set_version: p.owner.registry().instrument_set_version,
        topic_instrument_set_hash: tickwire_join::instrument_set_hash(
            &p.owner.registry().instruments,
        ),
        ..Default::default()
    };
    resource.initial_membership_version = join.membership_version;
    let built = p
        .state
        .snapshot(
            &JoinPlan {
                resource,
                join,
                sequence: p.owner.published(),
            },
            now,
            1000,
        )
        .unwrap();
    tickwire_snapshot::validate(
        built.bytes(),
        &tickwire_snapshot::JoinContext {
            expected_resource: built.resource(),
            catalog_hash: p.owner.registry().catalog_hash,
            instruments: &p.owner.registry().instruments,
            join: built.join(),
            now_mono_ns: now,
        },
    )
    .unwrap();
    built
}
fn warm(p: &TopicPublisher<4>) -> TopicExpansion {
    let mut c = p
        .prepare_expansion(
            registry(&[10, 20], 2),
            vec![config(10, 1)],
            limits(),
            2 << 20,
        )
        .unwrap();
    c.warm_notice(10, &notice(&book(1, 11, 200), 11), 11)
        .unwrap();
    c
}
#[test]
fn invalid_directory_and_peak_budget_reject_without_changing_old_baseline() {
    let mut p = publisher();
    let mut faults = Faults::default();
    p.on_notice(20, notice(&book(0, 10, 100), 10), 10, &mut faults)
        .unwrap();
    let original = snapshot(&p, 10).into_bytes();
    for case in 0..7 {
        let mut r = registry(&[10, 20], 2);
        let mut cfg = config(10, 1);
        let mut bound = 2 << 20;
        match case {
            0 => r.instrument_set_version = 1,
            1 => r.catalog_hash = [99; 32],
            2 => r.instruments[1].instrument_version = 2,
            3 => r.instruments[1].required_states = 3,
            4 => cfg.key = config(20, 0).key,
            5 => cfg.instrument.catalog_version = 2,
            _ => bound = 1,
        }
        assert!(
            p.prepare_expansion(r, vec![cfg], limits(), bound).is_err(),
            "case {case}"
        );
        assert_eq!(snapshot(&p, 10).bytes(), original);
    }
    let c = warm(&p);
    let required = p.reserved_cache_bytes() + c.reserved_cache_bytes();
    assert!(matches!(
        p.prepare_expansion(
            registry(&[10, 20], 2),
            vec![config(10, 1)],
            limits(),
            required - 1
        ),
        Err(Error::Bounds)
    ));
    assert!(
        p.prepare_expansion(
            registry(&[10, 20], 2),
            vec![config(10, 1)],
            limits(),
            required
        )
        .is_ok()
    );
}
#[test]
fn invalid_bank_or_stale_old_cache_cannot_freeze_or_replace_current_owner() {
    for case in 0..6 {
        let mut p = publisher();
        let mut faults = Faults::default();
        p.on_notice(20, notice(&book(0, 10, 100), 10), 10, &mut faults)
            .unwrap();
        let mut c = warm(&p);
        let original = snapshot(&p, 11).into_bytes();
        let mut h = replacement(&p, registry(&[10, 20], 2)).header().clone();
        let mut now = 11;
        let mut max_age = 100;
        match case {
            0 => h.feed_generation = 1,
            1 => h.namespace_id = [99; 16],
            2 => {
                h.cutover_seq += 1;
                h.first_seq += 1;
            }
            3 => {
                now = 111;
                max_age = 100;
            }
            4 => c.state.limits.snapshot_bytes = 1,
            _ => {
                c.source_resource = [77; 16];
            }
        }
        h.descriptor_hash = [0; 32];
        let replacement =
            JoinOwner::new(Publisher::create(h).unwrap(), registry(&[10, 20], 2)).unwrap();
        assert!(
            p.commit_expansion(c, replacement, now, max_age, &mut faults)
                .is_err(),
            "case {case}"
        );
        assert_eq!(snapshot(&p, 11).bytes(), original);
        p.on_notice(20, notice(&book(0, 112, 101), 112), 112, &mut faults)
            .unwrap();
        assert_eq!(faults.0, 0);
    }
}
#[test]
fn disconnect_and_stale_connection_close_candidate_until_valid_rewarm() {
    let mut p = publisher();
    let mut faults = Faults::default();
    p.on_notice(20, notice(&book(0, 10, 100), 10), 10, &mut faults)
        .unwrap();
    let mut c = warm(&p);
    let gap = Notice::Gap(tickwire_binance_public::GapNotice {
        connection_epoch: 2,
        book_generation: 1,
        reason: tickwire_binance_public::Error::Closed,
    });
    c.warm_notice(10, &gap, 12).unwrap();
    assert!(!c.ready(12, 100));
    assert!(
        c.warm_notice(10, &notice(&book(1, 13, 200), 13), 13)
            .is_err()
    );
    assert!(!c.ready(13, 100));
    let fresh = book(1, 14, 201);
    let mut input = notice(&fresh, 14);
    if let Notice::Sync {
        connection_epoch, ..
    } = &mut input
    {
        *connection_epoch = 2;
    }
    c.warm_notice(10, &input, 14).unwrap();
    assert!(c.ready(14, 100));
    assert!(!c.ready(115, 100));
    let input = Notice::Sync {
        connection_epoch: 2,
        book_generation: 1,
        received_mono_ns: None,
        raw_delta: None,
        encoding: DepthEncoding::Json,
        outcome: SyncOutcome::Disconnected,
        view: None,
    };
    c.warm_notice(10, &input, 15).unwrap();
    assert!(!c.ready(15, 100));
    assert_eq!(p.state.sequence(), 2);
}
fn trade(received: u64, epoch: u64) -> Event {
    use tickwire_binance::market::{EventEncoding, EventTimes, Trade};
    Event {
        identity: StreamIdentity {
            source_id: 9,
            connection_epoch: epoch,
            instrument_id: 10,
            instrument_version: 1,
            catalog_version: 1,
            catalog_hash: [22; 32],
        },
        times: EventTimes {
            received_mono_ns: received,
            exchange_event_ns: None,
            transaction_ns: None,
        },
        encoding: EventEncoding::Json,
        value: EventValue::Trade(Trade {
            trade_id: received,
            raw_trade_range: None,
            price_ticks: 200,
            quantity_lots: 1,
            normal_quantity_lots: None,
            buyer_is_maker: false,
            aggregate: false,
        }),
    }
}
#[test]
fn required_events_keep_source_epoch_and_age_independent_from_depth() {
    let mut p = publisher();
    let mut faults = Faults::default();
    p.on_notice(20, notice(&book(0, 10, 100), 10), 10, &mut faults)
        .unwrap();
    let mut r = registry(&[10, 20], 2);
    r.instruments[0].required_states = 3;
    let mut c = p
        .prepare_expansion(r, vec![config(10, 1)], limits(), 2 << 20)
        .unwrap();
    c.configure_event(10, EventKind::Trade, 9).unwrap();
    c.warm_notice(10, &notice(&book(1, 11, 200), 11), 11)
        .unwrap();
    assert!(!c.ready(11, 100));
    c.warm_event_notice(
        10,
        &EventNotice::Event {
            event: &trade(12, 1),
            raw: &[],
            usable: true,
        },
        12,
    )
    .unwrap();
    assert!(c.ready(12, 100));
    c.warm_event_notice(
        10,
        &EventNotice::Gap(tickwire_binance_public::EventGap {
            identity: trade(13, 2).identity,
            kind: EventKind::Trade,
            reason: tickwire_binance_public::Error::Closed,
        }),
        13,
    )
    .unwrap();
    assert!(!c.ready(13, 100));
    assert!(
        c.warm_event_notice(
            10,
            &EventNotice::Event {
                event: &trade(14, 1),
                raw: &[],
                usable: true
            },
            14
        )
        .is_err()
    );
    c.warm_event_notice(
        10,
        &EventNotice::Event {
            event: &trade(15, 2),
            raw: &[],
            usable: true,
        },
        15,
    )
    .unwrap();
    assert!(c.ready(15, 100));
    c.warm_event_notice(
        10,
        &EventNotice::Event {
            event: &trade(112, 2),
            raw: &[],
            usable: true,
        },
        112,
    )
    .unwrap();
    assert!(!c.ready(112, 100)); // A fresh trade cannot refresh the depth receipt at 11.
    assert!(
        c.warm_notice(20, &notice(&book(0, 113, 200), 113), 113)
            .is_err()
    );
}

#[test]
fn candidate_publish_failure_preserves_old_owner_and_freeze_failure_closes_it() {
    for freeze_old in [false, true] {
        let mut p = publisher();
        let mut faults = Faults::default();
        p.on_notice(20, notice(&book(0, 10, 100), 10), 10, &mut faults)
            .unwrap();
        let c = warm(&p);
        let cut = p.owner.published();
        let original = p.owner.header().resource_id;
        let mut next = replacement(&p, registry(&[10, 20], 2));
        if freeze_old {
            p.owner.freeze_for_successor(2).unwrap();
        } else {
            next.freeze_for_successor(3).unwrap();
        }
        assert!(p.commit_expansion(c, next, 11, 100, &mut faults).is_err());
        assert_eq!(p.owner.published(), cut);
        assert_eq!(p.owner.header().resource_id, original);
        assert_eq!(p.state.rows.len(), 1);
        assert_eq!(p.state.rows[0].bids[0].price_ticks, 100);
        assert_eq!(p.needs_bank(), freeze_old);
        assert_eq!(faults.0, usize::from(freeze_old));
    }
}

#[test]
fn retained_exact_source_and_reserved_book_capacity_survive_expansion() {
    let mut old_registry = registry(&[20], 1);
    old_registry.instruments[0].required_states = 3;
    let owner = JoinOwner::new(Publisher::create(header()).unwrap(), old_registry).unwrap();
    let mut p = TopicPublisher::<4>::new(owner, vec![config(20, 0)], limits()).unwrap();
    p.configure_event(20, EventKind::Trade, 9).unwrap();
    let mut faults = Faults::default();
    p.on_notice(20, notice(&book(0, 10, 100), 10), 10, &mut faults)
        .unwrap();
    let mut event = trade(11, 7);
    event.identity.instrument_id = 20;
    p.on_event_notice(
        20,
        &EventNotice::Event {
            event: &event,
            raw: &[],
            usable: true,
        },
        11,
        &mut faults,
    )
    .unwrap();
    let capacities = (
        p.state.rows[0].states.capacity(),
        p.state.rows[0].bids.capacity(),
        p.state.rows[0].asks.capacity(),
    );
    let mut r = registry(&[10, 20], 2);
    r.instruments[1].required_states = 3;
    let mut c = p
        .prepare_expansion(r.clone(), vec![config(10, 1)], limits(), 2 << 20)
        .unwrap();
    assert!(c.configure_event(20, EventKind::Trade, 10).is_err());
    c.warm_notice(10, &notice(&book(1, 12, 200), 12), 12)
        .unwrap();
    let next = replacement(&p, r);
    p.commit_expansion(c, next, 12, 100, &mut faults).unwrap();
    let row = &p.state.rows[1];
    assert_eq!(row.event_sources[1].unwrap().get(), 9);
    assert!(
        row.states.capacity() >= capacities.0
            && row.bids.capacity() >= capacities.1
            && row.asks.capacity() >= capacities.2
    );
    let exact = row
        .states
        .iter()
        .find_map(|state| {
            if let MarketState::Exact(record) = state {
                Some(record)
            } else {
                None
            }
        })
        .unwrap();
    assert_eq!(
        (exact.body.source_id, exact.body.source_connection_epoch),
        (9, 7)
    );
    snapshot(&p, 13);
    event.identity.source_id = 10;
    event.times.received_mono_ns = 14;
    assert!(
        p.on_event_notice(
            20,
            &EventNotice::Event {
                event: &event,
                raw: &[],
                usable: true
            },
            14,
            &mut faults
        )
        .is_err()
    );
}
