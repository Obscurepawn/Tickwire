//! Complete depth publication, exact JOIN baselines, and explicit failed-bank handoff.
#![forbid(unsafe_code)]
use sha2::{Digest, Sha256};
use tickwire_binance::{
    feed::{DepthContext, DepthWorkspace, TimeUnit, decode_diff_into, decode_sbe_diff_into},
    request::Market,
};
use tickwire_binance_public::{DepthEncoding, Notice, PublicSink, SyncOutcome};
use tickwire_book::{BestLevel, BookKey, BookView, Side, SideCoverage};
use tickwire_instrument_index::{DenseId, FixedIndex};
use tickwire_ipc::*;
use tickwire_join::{JoinOwner, JoinPlan};
use tickwire_shm::MarketMessage;
use tickwire_snapshot::{
    BuildRequest, BuiltTopic, InstrumentSnapshot, InstrumentSpec, build_topic_with_coverage,
};
use tickwire_venue::InstrumentBinding;

// One 64 KiB arena payload, with 16 bytes per normalized level.
const MAX_DELTA_LEVELS: usize = 4096;

#[derive(Debug)]
pub enum Error {
    Identity,
    Bounds,
    Clock,
    NeedsBank,
    Decode(tickwire_binance::Error),
    Wire(tickwire_ipc::Error),
    Join(tickwire_join::Error),
}
pub type Result<T> = std::result::Result<T, Error>;
impl From<tickwire_ipc::Error> for Error {
    fn from(e: tickwire_ipc::Error) -> Self {
        Self::Wire(e)
    }
}
impl From<tickwire_join::Error> for Error {
    fn from(e: tickwire_join::Error) -> Self {
        Self::Join(e)
    }
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}

#[derive(Clone)]
pub struct InstrumentConfig {
    pub instrument: InstrumentBinding,
    pub key: BookKey,
    pub market: Market,
    pub time_unit: TimeUnit,
}
/// A local, prebound row. It never replaces the stable instrument identity on wire.
#[derive(Clone, Copy, Debug)]
pub struct InstrumentSlot {
    instrument: u64,
    dense: DenseId,
}
impl InstrumentSlot {
    pub fn instrument(self) -> u64 {
        self.instrument
    }
    pub fn index(self) -> usize {
        self.dense.index()
    }
}
#[derive(Clone, Copy)]
pub struct Limits {
    pub levels_per_side: usize,
    pub cache_bytes: usize,
    pub snapshot_bytes: usize,
    pub max_delta_bytes: usize,
}
#[derive(Clone, Copy, Debug)]
pub struct Fault {
    pub topic: u64,
    pub feed_generation: u64,
    pub instrument: u64,
    pub published: u64,
}
/// Must synchronously close the execution gate independently of ring capacity.
pub trait FaultSink {
    fn invalidate(&mut self, fault: Fault);
}
struct Row {
    spec: InstrumentSpec,
    key: BookKey,
    context: DepthContext,
    source_generation: Option<u64>,
    revision: u64,
    last_event: u64,
    states: Vec<MarketState>,
    selection: EventSelection,
    event_sources: [Option<std::num::NonZeroU64>; 3],
    depth_state: u16,
    depth_received: u64,
    instrument: InstrumentBinding,
    bids: Vec<DepthLevel>,
    asks: Vec<DepthLevel>,
    coverage: DepthCoverage,
}
/// A single-threaded, complete topic cut. JOIN borrows it while the owner is exclusive.
pub struct TopicState {
    header: ResourceHeader,
    catalog: [u8; 32],
    rows: Vec<Row>,
    index: FixedIndex,
    limits: Limits,
    sequence: u64,
    dirty: bool,
}
impl TopicState {
    pub fn sequence(&self) -> u64 {
        self.sequence
    }
    pub fn ready_for_snapshot(&self) -> bool {
        !self.dirty
    }
    pub fn cached_live(&self) -> bool {
        self.rows.iter().all(|r| {
            r.states
                .iter()
                .any(|s| s.header().message_type == 261 && s.feed_state() == 2)
        })
    }
    pub fn publications(&self) -> impl Iterator<Item = (u64, u64, u64, u64)> + '_ {
        self.rows.iter().map(|r| {
            (
                r.spec.instrument_id,
                r.spec.instrument_version,
                r.last_event,
                r.states
                    .iter()
                    .map(|s| s.header().publish_mono_ns)
                    .max()
                    .unwrap_or(0),
            )
        })
    }
    pub fn live(&self) -> bool {
        !self.dirty
            && self.rows.iter().all(|r| {
                r.states
                    .iter()
                    .any(|s| s.header().message_type == 261 && s.feed_state() == 2)
            })
    }
    pub fn snapshot(
        &self,
        plan: &JoinPlan,
        now: u64,
        expires: u64,
    ) -> tickwire_ipc::Result<BuiltTopic> {
        if self.dirty
            || plan.sequence != self.sequence
            || plan.resource.channel_id != self.header.channel_id
            || plan.resource.feed_generation != self.header.feed_generation
            || plan.resource.ring_epoch != self.header.ring_epoch
            || expires <= now
        {
            return Err(tickwire_ipc::Error::ResyncRequired);
        }
        let specs = self.rows.iter().map(|r| r.spec).collect::<Vec<_>>();
        let coverage = self
            .rows
            .iter()
            .map(|r| r.coverage.clone())
            .collect::<Vec<_>>();
        let members = self
            .rows
            .iter()
            .map(|r| {
                let mut h = Sha256::new();
                h.update(b"tickwire.market-member.v2");
                h.update(plan.resource.resource_id);
                h.update(r.spec.instrument_id.to_le_bytes());
                let id: [u8; 16] = h.finalize()[..16].try_into().unwrap();
                InstrumentSnapshot {
                    instrument_id: r.spec.instrument_id,
                    instrument_version: r.spec.instrument_version,
                    snapshot_id: id,
                    at_sequence: plan.sequence,
                    last_event_sequence: r.last_event,
                    feed_generation: plan.resource.feed_generation,
                    catalog_version: self.header.catalog_version,
                    catalog_hash: self.catalog_hash(),
                    created_mono_ns: now,
                    expires_mono_ns: expires,
                    state_version: r.revision,
                    coverage_depth: self.limits.levels_per_side as u32,
                    coverage_flags: coverage_flags(&r.coverage),
                    states: &r.states,
                    bids: &r.bids,
                    asks: &r.asks,
                }
            })
            .collect::<Vec<_>>();
        build_topic_with_coverage(
            &BuildRequest {
                resource: &plan.resource,
                join: &plan.join,
                catalog_hash: self.catalog_hash(),
                instrument_set_version: plan.join.topic_instrument_set_version,
                sequence: plan.sequence,
                created_mono_ns: now,
                expires_mono_ns: expires,
                now_mono_ns: now,
                logical_strategy_id: plan.join.logical_strategy_id,
                instruments: &specs,
                members: &members,
                byte_limit: self.limits.snapshot_bytes,
            },
            &coverage,
        )
    }
    fn catalog_hash(&self) -> [u8; 32] {
        self.catalog
    }
}
fn coverage_flags(c: &DepthCoverage) -> u32 {
    if c.bid_kind == 1 && c.ask_kind == 1 {
        0
    } else {
        3
    }
}
fn side_coverage(c: SideCoverage) -> (u8, i64) {
    match c {
        SideCoverage::Unknown => (0, 0),
        SideCoverage::Complete => (1, 0),
        SideCoverage::Through(p) => (2, p.0),
    }
}
fn header(
    h: &ResourceHeader,
    spec: InstrumentSpec,
    sequence: u64,
    kind: u16,
    now: u64,
    received: u64,
) -> RecordHeader {
    RecordHeader {
        sequence,
        message_type: kind,
        schema_major: 1,
        header_bytes: 128,
        body_bytes: 384,
        flags: 2 | 4 | 8,
        topic_or_scope_id: h.topic_id,
        feed_generation: h.feed_generation,
        producer_generation: h.producer_generation,
        instrument_id: spec.instrument_id,
        instrument_version: spec.instrument_version,
        ingress_mono_ns: received,
        publish_mono_ns: now,
        ..Default::default()
    }
}
pub struct TopicPublisher<const M: usize> {
    owner: JoinOwner<M>,
    state: TopicState,
    needs_bank: bool,
    last_now: u64,
    depth_workspace: DepthWorkspace,
    depth_levels: Vec<DepthLevel>,
    reserved_cache_bytes: usize,
}
impl<const M: usize> TopicPublisher<M> {
    pub fn new(
        owner: JoinOwner<M>,
        instruments: Vec<InstrumentConfig>,
        limits: Limits,
    ) -> Result<Self> {
        if owner.header().required_features & 0x42 != 0x42
            || owner.header().arena_class_count == 0
            || owner.header().class_descriptors[owner.header().arena_class_count as usize - 1]
                .payload_limit
                != 65536
            || owner.published() != owner.header().first_seq - 1
            || owner.membership_version() != owner.header().initial_membership_version
            || instruments.len() != owner.registry().instruments.len()
            || limits.levels_per_side == 0
            || limits.levels_per_side > 5000
            || limits.snapshot_bytes == 0
            || limits.snapshot_bytes > 64 << 20
            || limits.max_delta_bytes == 0
            || limits.max_delta_bytes > 16 << 20
        {
            return Err(Error::Identity);
        }
        let per_row = limits
            .levels_per_side
            .checked_mul(2 * size_of::<DepthLevel>())
            .and_then(|n| n.checked_add(size_of::<Row>()))
            .and_then(|n| n.checked_add(4 * size_of::<MarketState>()))
            .ok_or(Error::Bounds)?;
        let estimate = instruments
            .len()
            .checked_mul(per_row)
            .and_then(|n| {
                n.checked_add(
                    MAX_DELTA_LEVELS
                        * (size_of::<tickwire_book::Level>() + size_of::<DepthLevel>()),
                )
            })
            .ok_or(Error::Bounds)?;
        if estimate > limits.cache_bytes {
            return Err(Error::Bounds);
        }
        let h = owner.header().clone();
        let mut rows = Vec::with_capacity(instruments.len());
        let mut book_keys = std::collections::BTreeSet::new();
        for (cfg, spec) in instruments
            .into_iter()
            .zip(owner.registry().instruments.iter().copied())
        {
            if cfg.instrument.instrument_id != spec.instrument_id
                || cfg.instrument.instrument_version != spec.instrument_version
                || cfg.instrument.catalog_version != h.catalog_version
                || cfg.instrument.catalog_hash != owner.registry().catalog_hash
                || !book_keys.insert((cfg.key.venue, cfg.key.instrument, cfg.key.source))
            {
                return Err(Error::Identity);
            }
            let context = DepthContext::new(
                cfg.market,
                cfg.instrument.clone(),
                cfg.key,
                0,
                cfg.time_unit,
            )
            .map_err(Error::Decode)?;
            rows.push(Row {
                spec,
                key: cfg.key,
                context,
                source_generation: None,
                revision: 0,
                last_event: owner.published(),
                selection: EventSelection::default(),
                event_sources: [None; 3],
                depth_state: 1,
                depth_received: 0,
                instrument: cfg.instrument,
                states: {
                    let mut states = Vec::with_capacity(4);
                    states.push(MarketState::Legacy(MarketRecord {
                        header: header(&h, spec, owner.published(), 261, 0, 0),
                        body: MarketBody {
                            feed_state: 1,
                            ..Default::default()
                        },
                    }));
                    states
                },
                bids: Vec::with_capacity(limits.levels_per_side),
                asks: Vec::with_capacity(limits.levels_per_side),
                coverage: DepthCoverage {
                    version: 1,
                    ..Default::default()
                },
            });
        }
        let index = FixedIndex::new(
            &rows
                .iter()
                .map(|r| r.spec.instrument_id)
                .collect::<Vec<_>>(),
        )
        .map_err(|_| Error::Bounds)?;
        let state = TopicState {
            header: h,
            rows,
            index,
            limits,
            sequence: owner.published(),
            dirty: false,
            catalog: owner.registry().catalog_hash,
        };
        let depth_workspace = DepthWorkspace::new(MAX_DELTA_LEVELS).map_err(Error::Decode)?;
        let mut depth_levels = Vec::new();
        depth_levels
            .try_reserve_exact(MAX_DELTA_LEVELS)
            .map_err(|_| Error::Bounds)?;
        let mut publisher = Self {
            owner,
            state,
            needs_bank: false,
            last_now: 0,
            depth_workspace,
            depth_levels,
            reserved_cache_bytes: 0,
        };
        publisher.reserved_cache_bytes = publisher.measure_cache_bytes()?;
        if publisher.reserved_cache_bytes > limits.cache_bytes {
            return Err(Error::Bounds);
        }
        Ok(publisher)
    }
    /// Retained heap payload, excluding allocator metadata, SHM/JOIN and call stacks.
    pub fn reserved_cache_bytes(&self) -> usize {
        self.reserved_cache_bytes
    }
    fn measure_cache_bytes(&self) -> Result<usize> {
        let mut bytes = self
            .state
            .rows
            .capacity()
            .checked_mul(size_of::<Row>())
            .ok_or(Error::Bounds)?;
        let mut add = |value: usize| -> Result<()> {
            bytes = bytes.checked_add(value).ok_or(Error::Bounds)?;
            Ok(())
        };
        add(self.state.index.memory_stats().total_bytes())?;
        add(self.depth_workspace.reserved_bytes())?;
        add(self
            .depth_levels
            .capacity()
            .checked_mul(size_of::<DepthLevel>())
            .ok_or(Error::Bounds)?)?;
        for row in &self.state.rows {
            add(row
                .states
                .capacity()
                .checked_mul(size_of::<MarketState>())
                .ok_or(Error::Bounds)?)?;
            add(row
                .bids
                .capacity()
                .checked_mul(size_of::<DepthLevel>())
                .ok_or(Error::Bounds)?)?;
            add(row
                .asks
                .capacity()
                .checked_mul(size_of::<DepthLevel>())
                .ok_or(Error::Bounds)?)?;
            add(row.context.reserved_bytes())?;
            for symbol in [
                row.instrument.symbol.as_str(),
                row.instrument.base.symbol.as_str(),
                row.instrument.quote.symbol.as_str(),
                row.instrument.settlement.symbol.as_str(),
            ] {
                add(symbol.len())?;
            }
        }
        Ok(bytes)
    }
    pub fn split(&mut self) -> (&mut JoinOwner<M>, &TopicState) {
        (&mut self.owner, &self.state)
    }
    pub fn state(&self) -> &TopicState {
        &self.state
    }
    /// Bind once at feed setup; hot notifications then use direct row access.
    pub fn instrument_slot(&self, instrument: u64) -> Result<InstrumentSlot> {
        Ok(InstrumentSlot {
            instrument,
            dense: self.state.index.lookup(instrument).ok_or(Error::Identity)?,
        })
    }
    fn slot_index(&self, slot: InstrumentSlot) -> Result<usize> {
        let index = slot.dense.index();
        if self.state.rows.get(index).map(|r| r.spec.instrument_id) != Some(slot.instrument) {
            return Err(Error::Identity);
        }
        Ok(index)
    }
    pub fn needs_bank(&self) -> bool {
        self.needs_bank
    }
    /// Control has already invalidated this generation independently of the ring.
    pub fn require_rebuild(&mut self) {
        self.needs_bank = true;
        self.state.dirty = true;
    }
    pub fn sink<'a, C: FnMut() -> u64, F: FaultSink>(
        &'a mut self,
        instrument: u64,
        now: C,
        faults: &'a mut F,
    ) -> PublicationSink<'a, M, C, F> {
        PublicationSink {
            publisher: self,
            instrument,
            now,
            faults,
            last_error: None,
        }
    }
    fn fault(&mut self, instrument: u64, faults: &mut impl FaultSink) {
        self.needs_bank = true;
        self.state.dirty = true;
        faults.invalidate(Fault {
            topic: self.state.header.topic_id,
            feed_generation: self.state.header.feed_generation,
            instrument,
            published: self.owner.published(),
        });
    }
    pub fn on_notice(
        &mut self,
        instrument: u64,
        notice: Notice<'_>,
        now: u64,
        faults: &mut impl FaultSink,
    ) -> Result<()> {
        self.on_notice_ref(instrument, &notice, now, faults)
    }
    pub fn on_notice_ref(
        &mut self,
        instrument: u64,
        notice: &Notice<'_>,
        now: u64,
        faults: &mut impl FaultSink,
    ) -> Result<()> {
        let slot = self.instrument_slot(instrument)?;
        self.on_notice_slot(slot, notice, now, faults)
    }
    pub fn on_notice_slot(
        &mut self,
        slot: InstrumentSlot,
        notice: &Notice<'_>,
        now: u64,
        faults: &mut impl FaultSink,
    ) -> Result<()> {
        let instrument = slot.instrument;
        let index = self.slot_index(slot)?;
        if now < self.last_now {
            self.fault(instrument, faults);
            return Err(Error::Clock);
        }
        self.last_now = now;
        match notice {
            Notice::Http(_) => Ok(()),
            Notice::Gap(_) => {
                self.invalidate_row(index, now, faults);
                Ok(())
            }
            Notice::Sync {
                outcome,
                view,
                raw_delta,
                received_mono_ns,
                encoding,
                ..
            } => {
                if matches!(outcome, SyncOutcome::Rebuild(_) | SyncOutcome::Disconnected) {
                    self.invalidate_row(index, now, faults);
                    return Ok(());
                }
                let Some(view) = view else {
                    return Ok(());
                };
                if matches!(
                    outcome,
                    SyncOutcome::IgnoredObsolete
                        | SyncOutcome::Applied(tickwire_book::ApplyOutcome::IgnoredStale)
                ) {
                    return Ok(());
                }
                let was_blocked = self.needs_bank;
                let result =
                    self.apply_view(index, view, *raw_delta, *encoding, *received_mono_ns, now);
                if result.is_err() || (!was_blocked && self.needs_bank) {
                    self.fault(instrument, faults);
                }
                result
            }
        }
    }
    fn invalidate_row(&mut self, index: usize, now: u64, faults: &mut impl FaultSink) {
        let row = &mut self.state.rows[index];
        if let Some(feed) = row
            .states
            .iter_mut()
            .find(|r| r.header().message_type == 261)
        {
            if let MarketState::Legacy(feed) = feed {
                feed.body.feed_state = 3;
                feed.header.publish_mono_ns = now;
            }
            row.depth_state = 3;
        }
        self.fault(self.state.rows[index].spec.instrument_id, faults);
    }
    fn apply_view(
        &mut self,
        index: usize,
        view: &BookView<'_>,
        raw: Option<&[u8]>,
        encoding: DepthEncoding,
        received: Option<u64>,
        now: u64,
    ) -> Result<()> {
        let row = &mut self.state.rows[index];
        if view.key() != row.key
            || view.received_at_ns() > now
            || view.known_bids().len() > self.state.limits.levels_per_side
            || view.known_asks().len() > self.state.limits.levels_per_side
            || row.source_generation.is_some_and(|g| view.generation() < g)
        {
            return Err(Error::Identity);
        }
        let generation_changed = row
            .source_generation
            .is_some_and(|g| g != view.generation());
        if generation_changed {
            self.needs_bank = true;
        }
        let rev = row
            .revision
            .checked_add(1)
            .filter(|x| *x < 1 << 63)
            .ok_or(Error::Bounds)?;
        let (bid_kind, bid_boundary_ticks) = side_coverage(view.coverage(Side::Bid));
        let (ask_kind, ask_boundary_ticks) = side_coverage(view.coverage(Side::Ask));
        row.coverage = DepthCoverage {
            version: 1,
            bid_kind,
            ask_kind,
            bid_boundary_ticks,
            ask_boundary_ticks,
            source_book_generation: view.generation(),
        };
        row.bids.clear();
        row.asks.clear();
        row.bids
            .extend(view.known_bids().iter().map(|l| DepthLevel {
                price_ticks: l.price.0,
                qty_lots: l.quantity.0,
            }));
        row.asks
            .extend(view.known_asks().iter().map(|l| DepthLevel {
                price_ticks: l.price.0,
                qty_lots: l.quantity.0,
            }));
        row.source_generation = Some(view.generation());
        row.revision = rev;
        let state = if matches!(view.best_bid(), BestLevel::Unknown)
            || matches!(view.best_ask(), BestLevel::Unknown)
        {
            3
        } else {
            2
        };
        if state == 3 {
            self.needs_bank = true;
        }
        let mut quote = MarketRecord {
            header: header(
                &self.state.header,
                row.spec,
                0,
                257,
                now,
                view.received_at_ns(),
            ),
            body: MarketBody {
                feed_state: state,
                book_revision: rev,
                ..Default::default()
            },
        };
        if let BestLevel::Known(l) = view.best_bid() {
            quote.body.bid_price_ticks = l.price.0;
            quote.body.bid_qty_lots = l.quantity.0;
        }
        if let BestLevel::Known(l) = view.best_ask() {
            quote.body.ask_price_ticks = l.price.0;
            quote.body.ask_qty_lots = l.quantity.0;
        }
        row.depth_state = state;
        row.depth_received = view.received_at_ns();
        if !row.selection.best_bid_ask {
            upsert(&mut row.states, quote.clone().into());
        }
        let feed = MarketRecord {
            header: header(
                &self.state.header,
                row.spec,
                0,
                261,
                now,
                view.received_at_ns(),
            ),
            body: MarketBody {
                feed_state: aggregate_state(row),
                ..Default::default()
            },
        };
        upsert(&mut row.states, feed.clone().into());
        self.state.dirty = true;
        if self.needs_bank {
            return Ok(());
        }
        // Before first Live there can be no ready JOIN: its complete baseline
        // was Bootstrapping. First Live is captured for the subsequent JOIN.
        let mut records: [MarketRecord; 3] = std::array::from_fn(|_| MarketRecord::default());
        let mut record_count = 0;
        let levels = &mut self.depth_levels;
        levels.clear();
        if let Some(raw) = raw {
            row.context.set_generation(view.generation());
            let decode = match encoding {
                DepthEncoding::Json => decode_diff_into,
                DepthEncoding::SpotSbeV1 => decode_sbe_diff_into,
            };
            let decoded = decode(
                raw,
                &row.context,
                received.unwrap_or(view.received_at_ns()),
                self.state.limits.max_delta_bytes,
                &mut self.depth_workspace,
            )
            .map_err(Error::Decode)?;
            let event = decoded.event();
            if event.last_update_id != view.last_update_id()
                || event.bids.len() + event.asks.len() > MAX_DELTA_LEVELS
            {
                return Err(Error::Bounds);
            }
            levels.extend(event.bids.iter().chain(event.asks).map(|l| DepthLevel {
                price_ticks: l.price.0,
                qty_lots: l.quantity.0,
            }));
            levels[..event.bids.len()].sort_unstable_by_key(|a| std::cmp::Reverse(a.price_ticks));
            levels[event.bids.len()..].sort_unstable_by_key(|l| l.price_ticks);
            let mut record = MarketRecord {
                header: header(
                    &self.state.header,
                    row.spec,
                    0,
                    259,
                    now,
                    view.received_at_ns(),
                ),
                body: MarketBody {
                    venue_seq_first: event.first_update_id,
                    venue_seq_last: event.last_update_id,
                    venue_seq_previous: event.previous_update_id.unwrap_or(0),
                    book_revision: rev,
                    bid_count: event.bids.len() as u32,
                    ask_count: event.asks.len() as u32,
                    depth_flags: 1 | if coverage_flags(&row.coverage) != 0 {
                        4
                    } else {
                        0
                    },
                    feed_state: state,
                    ..Default::default()
                },
            };
            if let Some(t) = decoded.event_time_ns() {
                record.body.exchange_event_ns = t;
                record.header.source_timestamp_ns = t;
                record.header.flags |= 1;
            }
            records[record_count] = record;
            record_count += 1;
        }
        if !row.selection.best_bid_ask {
            records[record_count] = quote;
            record_count += 1;
        }
        records[record_count] = feed;
        record_count += 1;
        for (i, r) in records[..record_count].iter_mut().enumerate() {
            r.header.sequence = self.owner.published() + i as u64 + 1;
        }
        let messages: [MarketMessage<'_>; 3] = std::array::from_fn(|i| {
            let r = &records[i];
            MarketMessage {
                record: r,
                levels: if r.header.message_type == 259 {
                    levels
                } else {
                    &[]
                },
            }
        });
        let published = self.owner.publish_market(&messages[..record_count])?;
        for record in records
            .into_iter()
            .take(record_count)
            .filter(|r| r.header.message_type != 259)
        {
            upsert(&mut row.states, record.into());
        }
        row.last_event = published;
        self.state.sequence = published;
        self.state.dirty = false;
        Ok(())
    }
    /// Adopt a fresh real bank after control has arranged re-JOIN. Return the
    /// old owner so its mappings and legitimate outstanding borrowers remain alive.
    pub fn replace_bank(
        &mut self,
        mut replacement: JoinOwner<M>,
        now: u64,
    ) -> Result<JoinOwner<M>> {
        let h = replacement.header().clone();
        let old = self.owner.header();
        if !self.needs_bank
            || now < self.last_now
            || h.channel_id != old.channel_id
            || h.topic_id != old.topic_id
            || h.catalog_version != old.catalog_version
            || h.producer_instance != old.producer_instance
            || h.producer_generation != old.producer_generation
            || h.feed_generation <= old.feed_generation
            || h.predecessor_ring_epoch != old.ring_epoch
            || h.ring_epoch <= old.ring_epoch
            || h.cutover_seq != self.owner.published()
            || h.first_seq != self.owner.published() + 1
            || replacement.published() != h.first_seq - 1
            || replacement.membership_version() != h.initial_membership_version
            || h.required_features & 0x42 != 0x42
            || h.required_features != old.required_features
            || replacement.registry().catalog_hash != self.state.catalog
            || replacement
                .registry()
                .instruments
                .iter()
                .map(|x| (x.instrument_id, x.instrument_version, x.required_states))
                .ne(self.state.rows.iter().map(|x| {
                    (
                        x.spec.instrument_id,
                        x.spec.instrument_version,
                        x.spec.required_states,
                    )
                }))
        {
            return Err(Error::Identity);
        }
        self.state.header = h.clone();
        for row in &mut self.state.rows {
            for state in &mut row.states {
                state.header_mut().feed_generation = h.feed_generation;
                state.header_mut().sequence = replacement.published() + 1;
                state.header_mut().publish_mono_ns = now;
                let mut raw = [0; 512];
                encode_state(state, &mut raw)?;
                replacement.publish(&[&raw])?;
            }
            row.last_event = replacement.published();
        }
        self.owner
            .freeze_for_successor(replacement.header().ring_epoch)?;
        self.state.sequence = replacement.published();
        self.state.dirty = false;
        self.needs_bank = false;
        self.last_now = now;
        Ok(std::mem::replace(&mut self.owner, replacement))
    }
}
pub struct PublicationSink<'a, const M: usize, C, F> {
    publisher: &'a mut TopicPublisher<M>,
    instrument: u64,
    now: C,
    faults: &'a mut F,
    last_error: Option<Error>,
}
impl<const M: usize, C, F> PublicationSink<'_, M, C, F> {
    pub fn take_error(&mut self) -> Option<Error> {
        self.last_error.take()
    }
}
impl<const M: usize, C: FnMut() -> u64, F: FaultSink> PublicSink for PublicationSink<'_, M, C, F> {
    fn on_notice(&mut self, notice: Notice<'_>) -> bool {
        if let Err(e) = self
            .publisher
            .on_notice(self.instrument, notice, (self.now)(), self.faults)
        {
            self.last_error = Some(e);
        }
        // A failed publication bank does not destroy the independent healthy
        // source: keep its latest image for the next bank's complete JOIN.
        true
    }
}
mod events;
pub use events::{EventSelection, event_record};
use events::{aggregate_state, encode_state, upsert};
