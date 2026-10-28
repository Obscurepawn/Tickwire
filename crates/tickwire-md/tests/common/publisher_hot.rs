use super::{join, venue};
use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
};
use tickwire_binance::{feed::TimeUnit, market::EventKind, request::Market};
use tickwire_ipc::{
    ArenaBlockHeader, DepthLevel, JoinBody, MarketRecord, MarketState, MarketStateRecord,
    ResourceHeader, Wire,
};
use tickwire_join::{JoinOwner, JoinPlan, TopicRegistry};
use tickwire_market_publisher::{
    Fault, FaultSink, InstrumentConfig, InstrumentSlot, Limits, TopicPublisher,
};
use tickwire_shm::{Publisher, SubscriptionKey};

pub const ID: u64 = (1 << 63) | 12;
pub const KEY: tickwire_book::BookKey = tickwire_book::BookKey {
    venue: 1,
    instrument: 0,
    source: 1,
};

#[derive(Default)]
pub struct Faults(pub usize);
impl FaultSink for Faults {
    fn invalidate(&mut self, _: Fault) {
        self.0 += 1;
    }
}

pub struct Fixture {
    pub publisher: TopicPublisher<32>,
    pub slot: InstrumentSlot,
    pub faults: Faults,
    pub file: File,
    pub header: ResourceHeader,
    join: JoinBody,
}
impl Fixture {
    pub fn new(kinds: &[EventKind], capacity: u32, arena_blocks: u32) -> Self {
        Self::with_budget(kinds, capacity, arena_blocks, 1 << 20).unwrap()
    }
    pub fn with_budget(
        kinds: &[EventKind],
        capacity: u32,
        arena_blocks: u32,
        cache_bytes: usize,
    ) -> tickwire_market_publisher::Result<Self> {
        let mut spec: tickwire_md::config::ResourceSpec =
            serde_json::from_str(include_str!("../../examples/topic-101.json")).unwrap();
        spec.identity.arch = if cfg!(target_arch = "aarch64") { 2 } else { 1 };
        spec.capacity = capacity;
        spec.arena_classes = vec![(65536, arena_blocks)];
        let mut instrument = venue::command().context.instrument;
        instrument.instrument_id = ID;
        let futures = kinds.contains(&EventKind::MarkFunding);
        if futures {
            instrument.contract = tickwire_venue::ContractKind::Perpetual;
        }
        spec.catalog_version = instrument.catalog_version;
        let bank = Publisher::<32>::create(spec.header_with_exact_events().unwrap()).unwrap();
        let file = File::from(bank.reader_fd().unwrap());
        let header = bank.header().clone();
        let key = SubscriptionKey {
            bank: bank.key(),
            instance: 55,
            generation: 1,
            cursor: 56,
        };
        let required_states = kinds.iter().fold(tickwire_snapshot::QUOTE, |bits, kind| {
            bits | match kind {
                EventKind::Trade => tickwire_snapshot::TRADE,
                EventKind::BestBidAsk => tickwire_snapshot::QUOTE,
                EventKind::MarkFunding => tickwire_snapshot::MARK_FUNDING,
            }
        });
        let owner = JoinOwner::new(
            bank,
            TopicRegistry {
                catalog_hash: instrument.catalog_hash,
                instrument_set_version: 1,
                instruments: vec![tickwire_snapshot::InstrumentSpec {
                    instrument_id: ID,
                    instrument_version: instrument.instrument_version,
                    required_states,
                }],
            },
        )
        .unwrap();
        let mut publisher = TopicPublisher::new(
            owner,
            vec![InstrumentConfig {
                instrument,
                key: KEY,
                market: if futures { Market::Usdm } else { Market::Spot },
                time_unit: if futures {
                    TimeUnit::Milliseconds
                } else {
                    TimeUnit::Microseconds
                },
            }],
            Limits {
                levels_per_side: 4096,
                cache_bytes,
                snapshot_bytes: 1 << 20,
                max_delta_bytes: 1 << 20,
            },
        )?;
        for &kind in kinds {
            publisher.configure_event(ID, kind, 1).unwrap();
        }
        let (_, _, _, join) = join::prepare_pending(&mut publisher, spec.identity.identity(), key);
        let slot = publisher.instrument_slot(ID).unwrap();
        Ok(Self {
            publisher,
            slot,
            faults: Faults::default(),
            file,
            header,
            join,
        })
    }
    /// The only writer is idle and pending JOIN pins every inspected record.
    pub fn record(&mut self, sequence: u64) -> MarketState {
        let slot = (sequence - self.header.first_seq) % u64::from(self.header.capacity);
        self.file
            .seek(SeekFrom::Start(self.header.slot_offset + slot * 512))
            .unwrap();
        let mut raw = [0; 512];
        self.file.read_exact(&mut raw).unwrap();
        let record = MarketState::decode(&raw).unwrap();
        assert_eq!(record.header().sequence, sequence);
        assert_eq!(record.header().instrument_id, ID);
        record
    }
    pub fn depth(&mut self, sequence: u64) -> (MarketRecord, Vec<DepthLevel>) {
        let MarketState::Legacy(record) = self.record(sequence) else {
            panic!("depth record");
        };
        assert_eq!(record.header.message_type, 259);
        let count = (record.body.bid_count + record.body.ask_count) as usize;
        let levels = if count <= 12 {
            assert_eq!(record.header.payload_kind, 1);
            assert_eq!(record.header.payload_offset, 0);
            assert!(
                record.body.inline_levels[count..]
                    .iter()
                    .all(|level| *level == DepthLevel::default())
            );
            record.body.inline_levels[..count].to_vec()
        } else {
            assert_eq!(record.header.payload_kind, 2);
            assert!(
                record
                    .body
                    .inline_levels
                    .iter()
                    .all(|level| *level == DepthLevel::default())
            );
            self.file
                .seek(SeekFrom::Start(record.header.payload_offset - 128))
                .unwrap();
            let mut block = [0; 128];
            self.file.read_exact(&mut block).unwrap();
            let block = ArenaBlockHeader::decode(&block).unwrap();
            assert_eq!(block.allocation_seq, sequence);
            assert_eq!(block.resource_id, self.header.resource_id);
            assert_eq!(block.ring_epoch, self.header.ring_epoch);
            assert_eq!(block.payload_length, record.header.payload_length);
            let mut payload = vec![0; count * DepthLevel::SIZE];
            self.file.read_exact(&mut payload).unwrap();
            assert_eq!(tickwire_ipc::crc32c(&payload), record.header.payload_crc32c);
            assert_eq!(block.payload_crc32c, record.header.payload_crc32c);
            payload
                .as_chunks::<16>()
                .0
                .iter()
                .map(|raw| DepthLevel::decode(raw).unwrap())
                .collect()
        };
        assert_eq!(record.body.level_count as usize, count);
        assert_eq!(record.header.payload_length as usize, count * 16);
        (record, levels)
    }
    pub fn rotate(&mut self, now: u64) -> JoinOwner<32> {
        self.publisher.require_rebuild();
        let (owner, _) = self.publisher.split();
        let registry = owner.registry().clone();
        let mut header = owner.header().clone();
        header.resource_id[0] += 1;
        header.predecessor_ring_epoch = header.ring_epoch;
        header.ring_epoch += 1;
        header.feed_generation += 1;
        header.cutover_seq = owner.published();
        header.first_seq = header.cutover_seq + 1;
        header.descriptor_hash = [0; 32];
        let bank = Publisher::<32>::create(header).unwrap();
        self.file = File::from(bank.reader_fd().unwrap());
        self.header = bank.header().clone();
        self.publisher
            .replace_bank(JoinOwner::new(bank, registry).unwrap(), now)
            .unwrap()
    }
    pub fn snapshot(&mut self, now: u64) -> Vec<u8> {
        let (owner, state) = self.publisher.split();
        let h = owner.header();
        let mut join = self.join.clone();
        join.snapshot_id = [0; 16];
        join.snapshot_hash = [0; 32];
        join.snapshot_seq = 0;
        join.join_seq = 0;
        join.bank_resource_id = h.resource_id;
        join.feed_generation = h.feed_generation;
        join.ring_epoch = h.ring_epoch;
        join.membership_version = owner.membership_version();
        let mut resource = h.clone();
        resource.resource_id = [95; 16];
        resource.parent_resource_id = h.resource_id;
        resource.consumer_instance = join.subscriber_instance;
        resource.initial_membership_version = join.membership_version;
        let built = state
            .snapshot(
                &JoinPlan {
                    resource,
                    join,
                    sequence: owner.published(),
                },
                now,
                60_000_000_000,
            )
            .unwrap();
        tickwire_snapshot::validate(
            built.bytes(),
            &tickwire_snapshot::JoinContext {
                expected_resource: built.resource(),
                catalog_hash: owner.registry().catalog_hash,
                instruments: &owner.registry().instruments,
                join: built.join(),
                now_mono_ns: now,
            },
        )
        .unwrap();
        built.bytes().to_vec()
    }
}

pub fn book() -> tickwire_book::Book<tickwire_book::BinanceSpot, 8, 8> {
    use tickwire_book::*;
    let mut book = Book::new(KEY, 1);
    book.install_snapshot(Snapshot {
        key: KEY,
        generation: 1,
        last_update_id: 1,
        bids: &[Level {
            price: PriceTicks(10000),
            quantity: QtyLots(100),
        }],
        asks: &[Level {
            price: PriceTicks(20000),
            quantity: QtyLots(100),
        }],
        bids_complete: true,
        asks_complete: true,
    })
    .unwrap();
    book.apply(DepthEvent {
        key: KEY,
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
