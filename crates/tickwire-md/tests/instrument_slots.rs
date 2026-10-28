#![forbid(unsafe_code)]

#[path = "common/event_join.rs"]
mod join;
#[path = "../../tickwire-binance/tests/common/mod.rs"]
mod venue;

use std::io::{Read, Seek, SeekFrom};
use tickwire_binance::{feed::TimeUnit, market::*, request::Market};
use tickwire_binance_public::{DepthEncoding, EventNotice, Notice, SyncOutcome};
use tickwire_book::{BinanceSpot, Book, BookKey, DepthEvent, Level, PriceTicks, QtyLots, Snapshot};
use tickwire_ipc::{DepthLevel, JoinBody, MarketRecord, MarketState, MarketStateRecord, Wire};
use tickwire_join::{JoinOwner, JoinPlan, TopicRegistry};
use tickwire_market_publisher::{
    Error, Fault, FaultSink, InstrumentConfig, Limits, TopicPublisher,
};
use tickwire_shm::{Publisher, SubscriptionKey};

#[derive(Default)]
struct Faults(usize);
impl FaultSink for Faults {
    fn invalidate(&mut self, _: Fault) {
        self.0 += 1;
    }
}

struct Fixture {
    publisher: TopicPublisher<32>,
    join: JoinBody,
    bank_file: std::fs::File,
}
fn fixture(topic: u64, ids: &[u64]) -> Fixture {
    let keys: Vec<_> = ids.iter().copied().map(book_key).collect();
    fixture_with_keys(topic, ids, &keys).unwrap()
}
fn fixture_with_keys(topic: u64, ids: &[u64], keys: &[BookKey]) -> Result<Fixture, Error> {
    assert_eq!(ids.len(), keys.len());
    let mut spec: tickwire_md::config::ResourceSpec =
        serde_json::from_str(include_str!("../examples/topic-101.json")).unwrap();
    spec.identity.arch = if cfg!(target_arch = "aarch64") { 2 } else { 1 };
    spec.topic_id = topic;
    spec.resource_id = [topic as u8; 16];
    let instrument = venue::command().context.instrument;
    spec.catalog_version = instrument.catalog_version;
    let bank = Publisher::<32>::create(spec.header_with_exact_events().unwrap()).unwrap();
    let bank_file = std::fs::File::from(bank.reader_fd().unwrap());
    let key = SubscriptionKey {
        bank: bank.key(),
        instance: 55,
        generation: 1,
        cursor: 56,
    };
    let owner = JoinOwner::new(
        bank,
        TopicRegistry {
            catalog_hash: instrument.catalog_hash,
            instrument_set_version: 1,
            instruments: ids
                .iter()
                .map(|&id| tickwire_snapshot::InstrumentSpec {
                    instrument_id: id,
                    instrument_version: instrument.instrument_version,
                    required_states: tickwire_snapshot::QUOTE | tickwire_snapshot::TRADE,
                })
                .collect(),
        },
    )
    .unwrap();
    let instruments = ids
        .iter()
        .zip(keys)
        .map(|(&id, &key)| {
            let mut instrument = instrument.clone();
            instrument.instrument_id = id;
            InstrumentConfig {
                instrument,
                key,
                market: Market::Spot,
                time_unit: TimeUnit::Milliseconds,
            }
        })
        .collect();
    let mut publisher = TopicPublisher::new(
        owner,
        instruments,
        Limits {
            levels_per_side: 32,
            cache_bytes: 1 << 20,
            snapshot_bytes: 1 << 20,
            max_delta_bytes: 4096,
        },
    )?;
    for &id in ids {
        publisher.configure_event(id, EventKind::Trade, 1).unwrap();
    }
    let (_, _, _, join) = join::prepare_pending(&mut publisher, spec.identity.identity(), key);
    Ok(Fixture {
        publisher,
        join,
        bank_file,
    })
}
fn book_key(id: u64) -> BookKey {
    BookKey {
        venue: 1,
        instrument: id as u32,
        source: 1,
    }
}
fn trade(received: u64) -> Event {
    let i = venue::command().context.instrument;
    Event {
        identity: StreamIdentity {
            source_id: 1,
            connection_epoch: 1,
            instrument_id: i.instrument_id,
            instrument_version: i.instrument_version,
            catalog_version: i.catalog_version,
            catalog_hash: i.catalog_hash,
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
            price_ticks: 10000,
            quantity_lots: 100,
            normal_quantity_lots: None,
            buyer_is_maker: false,
            aggregate: false,
        }),
    }
}
fn snapshot(f: &mut Fixture) -> Vec<u8> {
    snapshot_with(f, |_| {})
}
fn snapshot_with(
    f: &mut Fixture,
    mut inspect: impl FnMut(&tickwire_snapshot::Member<'_>),
) -> Vec<u8> {
    let (owner, state) = f.publisher.split();
    let h = owner.header();
    let mut j = f.join.clone();
    j.snapshot_id = [0; 16];
    j.snapshot_hash = [0; 32];
    j.snapshot_seq = 0;
    j.join_seq = 0;
    j.bank_resource_id = h.resource_id;
    j.feed_generation = h.feed_generation;
    j.ring_epoch = h.ring_epoch;
    j.membership_version = owner.membership_version();
    let mut resource = h.clone();
    resource.resource_id = [95; 16];
    resource.parent_resource_id = h.resource_id;
    resource.consumer_instance = j.subscriber_instance;
    resource.initial_membership_version = j.membership_version;
    let built = state
        .snapshot(
            &JoinPlan {
                resource,
                join: j,
                sequence: owner.published(),
            },
            10,
            60_000_000_000,
        )
        .unwrap();
    let validated = tickwire_snapshot::validate(
        built.bytes(),
        &tickwire_snapshot::JoinContext {
            expected_resource: built.resource(),
            catalog_hash: owner.registry().catalog_hash,
            instruments: &owner.registry().instruments,
            join: built.join(),
            now_mono_ns: 10,
        },
    )
    .unwrap();
    let mut records = Vec::new();
    for (index, spec) in owner.registry().instruments.iter().enumerate() {
        let member = validated.member(index).unwrap();
        assert_eq!(member.entry.instrument_id, spec.instrument_id);
        for raw in member.state_records.as_chunks::<512>().0 {
            let record = MarketState::decode(raw).unwrap();
            assert_eq!(record.header().instrument_id, spec.instrument_id);
            assert_eq!(record.header().topic_or_scope_id, h.topic_id);
            assert_eq!(record.header().feed_generation, h.feed_generation);
        }
        inspect(&member);
        records.extend_from_slice(member.state_records);
    }
    records
}

#[test]
#[cfg_attr(not(target_os = "linux"), ignore = "real memfd requires Linux")]
fn publication_directory_rejects_duplicate_complete_book_keys() {
    let ids = [(1 << 32) | 12, (1 << 63) | 12];
    assert!(matches!(
        fixture_with_keys(101, &ids, &[book_key(0), book_key(0)]),
        Err(Error::Identity)
    ));
    // Uniqueness covers all three fields, not just the local instrument number.
    for second in [
        BookKey {
            source: 2,
            ..book_key(0)
        },
        BookKey {
            venue: 2,
            ..book_key(0)
        },
    ] {
        assert!(fixture_with_keys(101, &ids, &[book_key(0), second]).is_ok());
    }
}

fn live_book(key: BookKey, bid: i64) -> Book<BinanceSpot, 32, 32> {
    let mut book = Book::new(key, 1);
    book.install_snapshot(Snapshot {
        key,
        generation: 1,
        last_update_id: 1,
        bids: &[Level {
            price: PriceTicks(bid),
            quantity: QtyLots(100),
        }],
        asks: &[Level {
            price: PriceTicks(bid + 200),
            quantity: QtyLots(200),
        }],
        bids_complete: true,
        asks_complete: true,
    })
    .unwrap();
    book.apply(DepthEvent {
        key,
        generation: 1,
        first_update_id: 2,
        last_update_id: 2,
        previous_update_id: None,
        received_at_ns: 2,
        bids: &[],
        asks: &[],
    })
    .unwrap();
    book
}

#[test]
#[cfg_attr(not(target_os = "linux"), ignore = "real memfd requires Linux")]
fn full_width_ids_publish_local_book_views_and_reject_wrong_keys_without_row_changes() {
    check_full_width_publication();
}
fn check_full_width_publication() {
    let ids = [(1 << 32) | 12, (1 << 63) | 12];
    let keys = [book_key(0), book_key(1)];
    let mut f = fixture_with_keys(101, &ids, &keys).unwrap();
    let books = [live_book(keys[0], 9900), live_book(keys[1], 19900)];
    let mut faults = Faults::default();
    for (index, book) in books.iter().enumerate() {
        let slot = f.publisher.instrument_slot(ids[index]).unwrap();
        assert_eq!(slot.index(), index);
        let raw = serde_json::to_vec(&serde_json::json!({
            "e":"depthUpdate", "E":1, "s":"BTCUSDT", "U":2, "u":2,
            "b":[[(99 + index * 100).to_string(), "0.001".to_owned()]], "a":[],
        }))
        .unwrap();
        f.publisher
            .on_notice_slot(
                slot,
                &Notice::Sync {
                    connection_epoch: 1,
                    book_generation: 1,
                    received_mono_ns: Some(2),
                    raw_delta: Some(&raw),
                    encoding: DepthEncoding::Json,
                    outcome: SyncOutcome::Applied(tickwire_book::ApplyOutcome::Applied {
                        last_update_id: 2,
                        became_live: true,
                    }),
                    view: Some(book.view(2, 100).unwrap()),
                },
                2,
                &mut faults,
            )
            .unwrap();
    }
    assert_eq!(f.publisher.state().sequence(), 6);
    // Read the actual bank through its file descriptor while its sole writer is idle.
    // The pending JOIN retains this prefix; no mmap or unsafe attachment is needed.
    let (owner, _) = f.publisher.split();
    assert_eq!(owner.header().first_seq, 1);
    assert!(owner.header().capacity >= 6);
    f.bank_file
        .seek(SeekFrom::Start(owner.header().slot_offset))
        .unwrap();
    let mut wire = [0; 6 * MarketRecord::SIZE];
    f.bank_file.read_exact(&mut wire).unwrap();
    for (index, batch) in wire
        .as_chunks::<{ 3 * MarketRecord::SIZE }>()
        .0
        .iter()
        .enumerate()
    {
        for (record_index, raw) in batch.as_chunks::<512>().0.iter().enumerate() {
            let record = MarketRecord::decode(raw).unwrap();
            assert_eq!(record.header.instrument_id, ids[index]);
            assert_eq!(
                record.header.sequence,
                (index * 3 + record_index + 1) as u64
            );
            assert_eq!(record.header.message_type, [259, 257, 261][record_index]);
            if record.header.message_type == 257 {
                assert_eq!(record.body.bid_price_ticks, 9900 + index as i64 * 10000);
            } else if record.header.message_type == 259 {
                assert_eq!((record.body.bid_count, record.body.ask_count), (1, 0));
                assert_eq!(record.body.venue_seq_last, 2);
                assert_eq!(
                    record.body.inline_levels[0].price_ticks,
                    9900 + index as i64 * 10000
                );
                assert_eq!(record.body.inline_levels[0].qty_lots, 100);
            }
        }
    }
    let mut depth = Vec::new();
    let before = snapshot_with(&mut f, |member| {
        depth.push((member.entry.instrument_id, member.depth_levels.to_vec()));
    });
    for (index, (id, levels)) in depth.iter().enumerate() {
        assert_eq!(*id, ids[index]);
        assert_eq!(levels.len(), 2 * DepthLevel::SIZE);
        let bid = DepthLevel::decode(&levels[..DepthLevel::SIZE]).unwrap();
        let ask = DepthLevel::decode(&levels[DepthLevel::SIZE..]).unwrap();
        assert_eq!(
            (bid.price_ticks, ask.price_ticks),
            (9900 + index as i64 * 10000, 10100 + index as i64 * 10000)
        );
    }
    let quotes = |records: &[u8]| {
        records
            .as_chunks::<512>()
            .0
            .iter()
            .map(|raw| MarketState::decode(raw).unwrap())
            .filter_map(|record| match record {
                MarketState::Legacy(r) if r.header.message_type == 257 => {
                    Some((r.header.instrument_id, r.body))
                }
                _ => None,
            })
            .collect::<Vec<_>>()
    };
    let before_quotes = quotes(&before);
    assert_eq!(before_quotes.len(), 2);
    for (index, (id, body)) in before_quotes.iter().enumerate() {
        assert_eq!(*id, ids[index]);
        assert_eq!(body.bid_price_ticks, 9900 + index as i64 * 10000);
        assert_eq!(body.book_revision, 1);
    }
    let publications = f.publisher.state().publications().collect::<Vec<_>>();
    // A view for the other row, or a different venue/source, cannot replace row 0.
    for wrong in [
        keys[1],
        BookKey {
            venue: 2,
            ..keys[0]
        },
        BookKey {
            source: 2,
            ..keys[0]
        },
    ] {
        let book = live_book(wrong, 29900);
        assert!(matches!(
            f.publisher.on_notice_ref(
                ids[0],
                &Notice::Sync {
                    connection_epoch: 1,
                    book_generation: 1,
                    received_mono_ns: Some(2),
                    raw_delta: None,
                    encoding: DepthEncoding::Json,
                    outcome: SyncOutcome::Applied(tickwire_book::ApplyOutcome::Applied {
                        last_update_id: 2,
                        became_live: true
                    }),
                    view: Some(book.view(2, 100).unwrap()),
                },
                3,
                &mut faults
            ),
            Err(Error::Identity)
        ));
        assert_eq!(f.publisher.state().sequence(), 6);
        assert_eq!(
            f.publisher.state().publications().collect::<Vec<_>>(),
            publications
        );
    }
    assert_eq!(faults.0, 3);
    assert!(f.publisher.needs_bank());
    assert!(!f.publisher.state().ready_for_snapshot());
    let (owner, _) = f.publisher.split();
    let mut header = owner.header().clone();
    let registry = owner.registry().clone();
    header.resource_id = [104; 16];
    header.predecessor_ring_epoch = header.ring_epoch;
    header.ring_epoch += 1;
    header.feed_generation += 1;
    header.cutover_seq = owner.published();
    header.first_seq = header.cutover_seq + 1;
    header.descriptor_hash = [0; 32];
    let replacement = JoinOwner::new(Publisher::<32>::create(header).unwrap(), registry).unwrap();
    let _retired = f.publisher.replace_bank(replacement, 4).unwrap();
    let mut restored_depth = Vec::new();
    let after = snapshot_with(&mut f, |member| {
        restored_depth.push((member.entry.instrument_id, member.depth_levels.to_vec()));
    });
    assert_eq!(restored_depth, depth);
    assert_eq!(quotes(&after), before_quotes);
}

#[test]
#[cfg_attr(not(target_os = "linux"), ignore = "real memfd requires Linux")]
fn topic_local_slots_reject_foreign_rows_and_survive_bank_rotation() {
    let mut a = fixture(101, &[12, 20]);
    let mut b = fixture(102, &[5, 12, 20]);
    let a_slot = a.publisher.instrument_slot(12).unwrap();
    let b_slot = b.publisher.instrument_slot(12).unwrap();
    assert_eq!((a_slot.index(), b_slot.index()), (0, 1));
    let mut faults = Faults::default();
    let event = trade(2);
    let event_notice = EventNotice::Event {
        event: &event,
        raw: &[],
        usable: true,
    };
    let disconnect = Notice::Sync {
        connection_epoch: 1,
        book_generation: 1,
        received_mono_ns: None,
        raw_delta: None,
        encoding: DepthEncoding::Json,
        outcome: SyncOutcome::Disconnected,
        view: None,
    };
    for (f, foreign) in [(&mut a, b_slot), (&mut b, a_slot)] {
        let before = snapshot(f);
        let sequence = f.publisher.state().sequence();
        assert!(matches!(
            f.publisher
                .on_notice_slot(foreign, &disconnect, 1000, &mut faults),
            Err(Error::Identity)
        ));
        assert!(matches!(
            f.publisher
                .on_event_slot(foreign, &event_notice, 1000, &mut faults),
            Err(Error::Identity)
        ));
        assert!(matches!(
            f.publisher
                .on_notice_ref(999, &disconnect, 1000, &mut faults),
            Err(Error::Identity)
        ));
        assert!(matches!(
            f.publisher
                .on_event_notice(999, &event_notice, 1000, &mut faults),
            Err(Error::Identity)
        ));
        assert_eq!(snapshot(f), before);
        assert_eq!(f.publisher.state().sequence(), sequence);
        assert!(!f.publisher.needs_bank());
    }
    assert_eq!(faults.0, 0);

    let mut book = Book::<BinanceSpot, 32, 32>::new(book_key(12), 1);
    book.install_snapshot(Snapshot {
        key: book_key(12),
        generation: 1,
        last_update_id: 1,
        bids: &[Level {
            price: PriceTicks(9900),
            quantity: QtyLots(100),
        }],
        asks: &[Level {
            price: PriceTicks(10100),
            quantity: QtyLots(200),
        }],
        bids_complete: true,
        asks_complete: true,
    })
    .unwrap();
    let outcome = book
        .apply(DepthEvent {
            key: book_key(12),
            generation: 1,
            first_update_id: 2,
            last_update_id: 2,
            previous_update_id: None,
            received_at_ns: 2,
            bids: &[],
            asks: &[],
        })
        .unwrap();
    let view = book.view(2, 100).unwrap();
    let depth_notice = Notice::Sync {
        connection_epoch: 1,
        book_generation: 1,
        received_mono_ns: Some(2),
        raw_delta: None,
        encoding: DepthEncoding::Json,
        outcome: SyncOutcome::Applied(outcome),
        view: Some(view),
    };
    for (f, slot) in [(&mut a, a_slot), (&mut b, b_slot)] {
        // A rejected foreign/unknown slot must not advance the publisher clock to 1000.
        f.publisher
            .on_notice_slot(slot, &depth_notice, 2, &mut faults)
            .unwrap();
        f.publisher
            .on_event_slot(slot, &event_notice, 2, &mut faults)
            .unwrap();
        let records = snapshot(f);
        let target: Vec<_> = records
            .as_chunks::<512>()
            .0
            .iter()
            .map(|raw| MarketState::decode(raw).unwrap())
            .filter(|r| r.header().instrument_id == 12)
            .collect();
        assert!(target.iter().any(|r| r.header().message_type == 257));
        assert!(target.iter().any(|r| r.header().message_type == 264));
        assert!(
            f.publisher
                .state()
                .publications()
                .filter(|row| row.0 != 12)
                .all(|row| row.2 == 0)
        );
    }

    let (owner, _) = a.publisher.split();
    let mut header = owner.header().clone();
    let registry = owner.registry().clone();
    header.resource_id = [103; 16];
    header.predecessor_ring_epoch = header.ring_epoch;
    header.ring_epoch += 1;
    header.feed_generation += 1;
    header.cutover_seq = owner.published();
    header.first_seq = header.cutover_seq + 1;
    header.descriptor_hash = [0; 32];
    let replacement = JoinOwner::new(Publisher::<32>::create(header).unwrap(), registry).unwrap();
    a.publisher.require_rebuild();
    let _retired = a.publisher.replace_bank(replacement, 3).unwrap();
    let next_event = trade(4);
    a.publisher
        .on_event_slot(
            a_slot,
            &EventNotice::Event {
                event: &next_event,
                raw: &[],
                usable: true,
            },
            4,
            &mut faults,
        )
        .unwrap();
    let records = snapshot(&mut a);
    let latest = records
        .as_chunks::<512>()
        .0
        .iter()
        .map(|raw| MarketState::decode(raw).unwrap())
        .find(|r| r.header().message_type == 264)
        .unwrap();
    assert_eq!(latest.header().instrument_id, 12);
    assert_eq!(latest.header().feed_generation, 2);
    assert!(matches!(latest, MarketState::Exact(r) if r.body.trade_id == 4));
    assert_eq!(faults.0, 0);
}
