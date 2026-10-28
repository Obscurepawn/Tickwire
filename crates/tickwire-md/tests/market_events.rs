#![allow(unsafe_code)]
#[path = "common/event_join.rs"]
mod join;
#[path = "../../tickwire-binance-public/tests/events.rs"]
mod network;
use tickwire_binance_public::{
    DepthEncoding, EventEncoding, EventKind, Notice, Source, SyncOutcome,
};
use tickwire_ipc::*;
use tickwire_market_publisher::*;
use tickwire_shm::{Publisher, Subscriber, SubscriptionKey};
#[test]
#[cfg_attr(not(target_os = "linux"), ignore = "real memfd requires Linux")]
fn real_wss_exact_events_roundtrip_publisher_shm_snapshot_and_replica() {
    for (source, kind, encoding) in [
        (
            Source::SpotProduction,
            EventKind::Trade,
            EventEncoding::Json,
        ),
        (
            Source::SpotProduction,
            EventKind::BestBidAsk,
            EventEncoding::Json,
        ),
        (
            Source::SpotProduction,
            EventKind::Trade,
            EventEncoding::SpotSbeV1,
        ),
        (
            Source::SpotProduction,
            EventKind::BestBidAsk,
            EventEncoding::SpotSbeV1,
        ),
        (
            Source::UsdmProduction,
            EventKind::Trade,
            EventEncoding::Json,
        ),
        (
            Source::UsdmProduction,
            EventKind::BestBidAsk,
            EventEncoding::Json,
        ),
        (
            Source::UsdmProduction,
            EventKind::MarkFunding,
            EventEncoding::Json,
        ),
    ] {
        let instrument = network::instrument(source);
        let mut spec: tickwire_md::config::ResourceSpec =
            serde_json::from_str(include_str!("../examples/topic-101.json")).unwrap();
        spec.identity.arch = if cfg!(target_arch = "aarch64") { 2 } else { 1 };
        spec.catalog_version = instrument.catalog_version;
        spec.first_seq = 1;
        let bank = Publisher::<32>::create(spec.header_with_exact_events().unwrap()).unwrap();
        let bank_fd = bank.reader_fd().unwrap();
        let bank_header = bank.header().clone();
        let key = SubscriptionKey {
            bank: bank.key(),
            instance: 55,
            generation: 1,
            cursor: 56,
        };
        let cat = match kind {
            EventKind::Trade => 2,
            EventKind::BestBidAsk => 1,
            EventKind::MarkFunding => 4,
        };
        let specs = vec![tickwire_snapshot::InstrumentSpec {
            instrument_id: instrument.instrument_id,
            instrument_version: instrument.instrument_version,
            required_states: 1 | cat,
        }];
        let catalog = instrument.catalog_hash;
        let id = instrument.instrument_id;
        let owner = tickwire_join::JoinOwner::new(
            bank,
            tickwire_join::TopicRegistry {
                catalog_hash: catalog,
                instrument_set_version: 1,
                instruments: specs.clone(),
            },
        )
        .unwrap();
        let book_key = tickwire_book::BookKey {
            venue: 1,
            instrument: id as u32,
            source: 1,
        };
        let market = if source == Source::SpotProduction {
            tickwire_binance::request::Market::Spot
        } else {
            tickwire_binance::request::Market::Usdm
        };
        let mut publisher = TopicPublisher::new(
            owner,
            vec![InstrumentConfig {
                instrument,
                key: book_key,
                market,
                time_unit: tickwire_binance::feed::TimeUnit::Milliseconds,
            }],
            Limits {
                levels_per_side: 32,
                cache_bytes: 1 << 20,
                snapshot_bytes: 1 << 20,
                max_delta_bytes: 4096,
            },
        )
        .unwrap();
        publisher.configure_event(id, kind, 1).unwrap();
        let (baseline, bytes, resource, join) =
            join::prepare_pending(&mut publisher, spec.identity.identity(), key);
        let validated = tickwire_snapshot::validate(
            &bytes,
            &tickwire_snapshot::JoinContext {
                expected_resource: &resource,
                catalog_hash: catalog,
                instruments: &specs,
                join: &join,
                now_mono_ns: 1,
            },
        )
        .unwrap();
        let mut replica = tickwire_market_source::replica::MarketReplica::load(
            bank_header.topic_id,
            bank_header.ring_epoch,
            &validated,
            32,
        )
        .unwrap();
        let reader =
            unsafe { Subscriber::<64>::attach(bank_fd, bank_header, key, baseline).unwrap() };
        struct Faults;
        impl FaultSink for Faults {
            fn invalidate(&mut self, f: Fault) {
                panic!("{f:?}")
            }
        }
        use tickwire_book::{BinanceSpot, Book, DepthEvent, Level, PriceTicks, QtyLots, Snapshot};
        let mut book = Book::<BinanceSpot, 32, 32>::new(book_key, 1);
        let bids = [Level {
            price: PriceTicks(9900),
            quantity: QtyLots(100),
        }];
        let asks = [Level {
            price: PriceTicks(10100),
            quantity: QtyLots(200),
        }];
        book.install_snapshot(Snapshot {
            key: book_key,
            generation: 1,
            last_update_id: 1,
            bids: &bids,
            asks: &asks,
            bids_complete: true,
            asks_complete: true,
        })
        .unwrap();
        book.apply(DepthEvent {
            key: book_key,
            generation: 1,
            first_update_id: 2,
            last_update_id: 2,
            previous_update_id: None,
            received_at_ns: 1,
            bids: &[],
            asks: &[],
        })
        .unwrap();
        publisher
            .on_notice(
                id,
                Notice::Sync {
                    connection_epoch: 1,
                    book_generation: 1,
                    received_mono_ns: Some(1),
                    raw_delta: None,
                    encoding: DepthEncoding::Json,
                    outcome: SyncOutcome::Applied(tickwire_book::ApplyOutcome::Applied {
                        last_update_id: 2,
                        became_live: true,
                    }),
                    view: Some(book.view(1, 100).unwrap()),
                },
                1,
                &mut Faults,
            )
            .unwrap();
        let slot = publisher.instrument_slot(id).unwrap();
        assert_eq!((slot.instrument(), slot.index()), (id, 0));
        assert!(publisher.instrument_slot(id ^ (1 << 63)).is_err());
        let actual = network::exercise(source, kind, encoding, |n| {
            let now = match n {
                tickwire_binance_public::EventNotice::Event { event, .. } => {
                    event.times.received_mono_ns + 1
                }
                _ => 2,
            };
            publisher.on_event_slot(slot, n, now, &mut Faults).unwrap();
        });
        assert!(publisher.state().live());
        let mut exact = Vec::new();
        while let Some(guard) = reader.borrow().unwrap() {
            let state = MarketState::decode(guard.bytes()).unwrap();
            if let MarketState::Exact(r) = &state {
                exact.push(r.clone());
            }
            replica.apply(guard.bytes(), &[]).unwrap();
        }
        assert_eq!(exact.len(), actual.len());
        for (r, event) in exact.iter().zip(actual) {
            assert_eq!(r.body.source_id, event.identity.source_id);
            assert_eq!(r.header.ingress_mono_ns, event.times.received_mono_ns);
            assert_eq!(
                r.header.source_timestamp_ns,
                event.times.exchange_event_ns.unwrap_or(0)
            );
            if let tickwire_binance::market::EventValue::MarkFunding(m) = event.value {
                assert_eq!(
                    r.body.index_price.coefficient_value(),
                    m.index_price.coefficient()
                );
                assert_eq!(r.body.index_price.exponent, m.index_price.exponent());
            }
        }
        // A later JOIN captures these heterogeneous exact states, independent of consumer replay.
        let (owner, state) = publisher.split();
        let mut next_join = join.clone();
        next_join.snapshot_id = [0; 16];
        next_join.snapshot_hash = [0; 32];
        next_join.snapshot_seq = 0;
        next_join.join_seq = 0;
        let mut template = owner.header().clone();
        template.resource_id = [95; 16];
        template.parent_resource_id = owner.header().resource_id;
        template.consumer_instance = next_join.subscriber_instance;
        template.initial_membership_version = next_join.membership_version;
        let now = exact.last().unwrap().header.publish_mono_ns + 1;
        let built = state
            .snapshot(
                &tickwire_join::JoinPlan {
                    resource: template,
                    join: next_join,
                    sequence: owner.published(),
                },
                now,
                60_000_000_000,
            )
            .unwrap();
        let validated = tickwire_snapshot::validate(
            built.bytes(),
            &tickwire_snapshot::JoinContext {
                expected_resource: built.resource(),
                catalog_hash: catalog,
                instruments: &specs,
                join: built.join(),
                now_mono_ns: now,
            },
        )
        .unwrap();
        validated.check_readiness(now).unwrap();
        let joined = tickwire_market_source::replica::MarketReplica::load(
            owner.header().topic_id,
            owner.header().ring_epoch,
            &validated,
            32,
        )
        .unwrap();
        let jview = joined.view(id).unwrap();
        assert_eq!(jview.received_mono_ns(), 1);
        let jstate = match kind {
            EventKind::Trade => jview.last_trade(),
            EventKind::BestBidAsk => jview.best_quote(),
            EventKind::MarkFunding => jview.mark_funding(),
        }
        .unwrap();
        if let MarketState::Exact(j) = jstate {
            assert_eq!(j.body, exact.last().unwrap().body);
            assert_eq!(
                j.header.ingress_mono_ns,
                exact.last().unwrap().header.ingress_mono_ns
            );
        } else {
            panic!("lost exact state");
        }
        let checkpoint = replica.checkpoint().unwrap();
        let restored =
            tickwire_market_source::replica::MarketReplica::restore_checkpoint(&checkpoint, 32)
                .unwrap();
        let view = restored.view(id).unwrap();
        let state = match kind {
            EventKind::Trade => view.last_trade(),
            EventKind::BestBidAsk => view.best_quote(),
            EventKind::MarkFunding => view.mark_funding(),
        }
        .unwrap();
        assert_eq!(state, &MarketState::Exact(exact.last().unwrap().clone()));
        assert!(view.continuous());
        assert_eq!(
            view.received_mono_ns(),
            1,
            "an independent event must not refresh depth age"
        );
    }
}
