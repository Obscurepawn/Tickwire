//! Complete depth publication, exact JOIN baselines, and explicit failed-bank handoff.
#![forbid(unsafe_code)]
use sha2::{Digest, Sha256};
use tickwire_binance::{
    feed::{DepthContext, TimeUnit, decode_diff},
    request::Market,
};
use tickwire_binance_public::{Notice, PublicSink, SyncOutcome};
use tickwire_book::{BestLevel, BookKey, BookView, Side, SideCoverage};
use tickwire_ipc::*;
use tickwire_join::{JoinOwner, JoinPlan};
use tickwire_shm::MarketMessage;
use tickwire_snapshot::{
    BuildRequest, BuiltTopic, InstrumentSnapshot, InstrumentSpec, build_topic_with_coverage,
};
use tickwire_venue::InstrumentBinding;

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
    states: Vec<MarketRecord>,
    bids: Vec<DepthLevel>,
    asks: Vec<DepthLevel>,
    coverage: DepthCoverage,
}
/// A single-threaded, complete topic cut. JOIN borrows it while the owner is exclusive.
pub struct TopicState {
    header: ResourceHeader,
    catalog: [u8; 32],
    rows: Vec<Row>,
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
                .any(|s| s.header.message_type == 261 && s.body.feed_state == 2)
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
                    .map(|s| s.header.publish_mono_ns)
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
                    .any(|s| s.header.message_type == 261 && s.body.feed_state == 2)
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
        let estimate = instruments
            .len()
            .checked_mul(
                limits
                    .levels_per_side
                    .checked_mul(32)
                    .and_then(|n| n.checked_add(2048))
                    .ok_or(Error::Bounds)?,
            )
            .ok_or(Error::Bounds)?;
        if estimate > limits.cache_bytes {
            return Err(Error::Bounds);
        }
        let h = owner.header().clone();
        let mut rows = Vec::with_capacity(instruments.len());
        for (cfg, spec) in instruments
            .into_iter()
            .zip(owner.registry().instruments.iter().copied())
        {
            if cfg.instrument.instrument_id != spec.instrument_id
                || cfg.instrument.instrument_version != spec.instrument_version
                || cfg.instrument.catalog_version != h.catalog_version
                || cfg.instrument.catalog_hash != owner.registry().catalog_hash
            {
                return Err(Error::Identity);
            }
            let context = DepthContext::new(cfg.market, cfg.instrument, cfg.key, 0, cfg.time_unit)
                .map_err(Error::Decode)?;
            rows.push(Row {
                spec,
                key: cfg.key,
                context,
                source_generation: None,
                revision: 0,
                last_event: owner.published(),
                states: vec![MarketRecord {
                    header: header(&h, spec, owner.published(), 261, 0, 0),
                    body: MarketBody {
                        feed_state: 1,
                        ..Default::default()
                    },
                }],
                bids: Vec::with_capacity(limits.levels_per_side),
                asks: Vec::with_capacity(limits.levels_per_side),
                coverage: DepthCoverage {
                    version: 1,
                    ..Default::default()
                },
            });
        }
        let state = TopicState {
            header: h,
            rows,
            limits,
            sequence: owner.published(),
            dirty: false,
            catalog: owner.registry().catalog_hash,
        };
        Ok(Self {
            owner,
            state,
            needs_bank: false,
            last_now: 0,
        })
    }
    pub fn split(&mut self) -> (&mut JoinOwner<M>, &TopicState) {
        (&mut self.owner, &self.state)
    }
    pub fn state(&self) -> &TopicState {
        &self.state
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
        if now < self.last_now {
            self.fault(instrument, faults);
            return Err(Error::Clock);
        }
        self.last_now = now;
        let index = self
            .state
            .rows
            .binary_search_by_key(&instrument, |r| r.spec.instrument_id)
            .map_err(|_| Error::Identity)?;
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
                let result = self.apply_view(index, view, *raw_delta, *received_mono_ns, now);
                if result.is_err() || (!was_blocked && self.needs_bank) {
                    self.fault(instrument, faults);
                }
                result
            }
        }
    }
    fn invalidate_row(&mut self, index: usize, now: u64, faults: &mut impl FaultSink) {
        let row = &mut self.state.rows[index];
        if let Some(feed) = row.states.iter_mut().find(|r| r.header.message_type == 261) {
            feed.body.feed_state = 3;
            feed.header.publish_mono_ns = now;
        }
        self.fault(self.state.rows[index].spec.instrument_id, faults);
    }
    fn apply_view(
        &mut self,
        index: usize,
        view: &BookView<'_>,
        raw: Option<&[u8]>,
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
                feed_state: state,
                ..Default::default()
            },
        };
        row.states = vec![quote, feed];
        self.state.dirty = true;
        if self.needs_bank {
            return Ok(());
        }
        // Before first Live there can be no ready JOIN: its complete baseline
        // was Bootstrapping. First Live is captured for the subsequent JOIN.
        let mut records = Vec::with_capacity(3);
        let mut levels = Vec::new();
        if let Some(raw) = raw {
            let decoded = decode_diff::<4096>(
                raw,
                &row.context,
                received.unwrap_or(view.received_at_ns()),
                self.state.limits.max_delta_bytes,
            )
            .map_err(Error::Decode)?;
            let event = decoded.event();
            if event.last_update_id != view.last_update_id()
                || event.bids.len() + event.asks.len() > 4096
            {
                return Err(Error::Bounds);
            }
            levels.extend(event.bids.iter().chain(event.asks).map(|l| DepthLevel {
                price_ticks: l.price.0,
                qty_lots: l.quantity.0,
            }));
            levels[..event.bids.len()].sort_unstable_by(|a, b| b.price_ticks.cmp(&a.price_ticks));
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
            records.push(record);
        }
        records.extend(row.states.iter().cloned());
        for (i, r) in records.iter_mut().enumerate() {
            r.header.sequence = self.owner.published() + i as u64 + 1;
        }
        let messages = records
            .iter()
            .map(|r| MarketMessage {
                record: r,
                levels: if r.header.message_type == 259 {
                    &levels
                } else {
                    &[]
                },
            })
            .collect::<Vec<_>>();
        let published = self.owner.publish_market(&messages)?;
        let end = records.len();
        row.states.clone_from_slice(&records[end - 2..]);
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
                state.header.feed_generation = h.feed_generation;
                state.header.sequence = replacement.published() + 1;
                state.header.publish_mono_ns = now;
                replacement.publish_market(&[MarketMessage {
                    record: state,
                    levels: &[],
                }])?;
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
