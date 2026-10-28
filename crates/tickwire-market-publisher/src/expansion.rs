//! Private cold candidates; no new bank exists until the final topic cut is known.
use super::*;
use tickwire_binance::market::{Event, EventKind, EventValue, StreamIdentity};
use tickwire_binance_public::EventNotice;
use tickwire_join::TopicRegistry;

struct Warming {
    instrument: u64,
    connection: u64,
    generation: u64,
    event_epochs: [u64; 3],
}
/// A private cache for a strict topic superset. It cannot publish or admit JOIN.
pub struct TopicExpansion {
    state: TopicState,
    registry: TopicRegistry,
    source_resource: [u8; 16],
    source_set_version: u64,
    warming: Vec<Warming>,
    last_now: u64,
    peak_cache_bytes: usize,
    depth_workspace: DepthWorkspace,
    depth_levels: Vec<DepthLevel>,
    reserved_cache_bytes: usize,
}
impl TopicExpansion {
    pub fn reserved_cache_bytes(&self) -> usize {
        self.reserved_cache_bytes
    }
    /// Only added rows are warmed here. Commit refreshes and checks all retained rows.
    pub fn ready(&self, now: u64, max_age_ns: u64) -> bool {
        now >= self.last_now
            && self.warming.iter().all(|w| {
                self.state
                    .index
                    .lookup(w.instrument)
                    .is_some_and(|id| fresh(&self.state.rows[id.index()], now, max_age_ns))
            })
    }
    fn added(&self, instrument: u64) -> Result<(usize, usize)> {
        let warm = self
            .warming
            .binary_search_by_key(&instrument, |w| w.instrument)
            .map_err(|_| Error::Identity)?;
        let row = self
            .state
            .index
            .lookup(instrument)
            .ok_or(Error::Identity)?
            .index();
        Ok((row, warm))
    }
    pub fn configure_event(
        &mut self,
        instrument: u64,
        kind: EventKind,
        source_id: u64,
    ) -> Result<()> {
        let (i, _) = self.added(instrument)?;
        let row = &mut self.state.rows[i];
        let cat = events::category(kind);
        let slot = events::source_index(cat);
        if source_id == 0
            || row.revision != 0
            || self.state.header.required_features & EXACT_MARKET_EVENTS == 0
            || row.spec.required_states & cat == 0
            || row.event_sources[slot].is_some()
            || (cat == 4 && matches!(row.instrument.contract, tickwire_venue::ContractKind::Cash))
        {
            return Err(Error::Identity);
        }
        row.event_sources[slot] = std::num::NonZeroU64::new(source_id);
        match kind {
            EventKind::Trade => row.selection.trade = true,
            EventKind::BestBidAsk => row.selection.best_bid_ask = true,
            EventKind::MarkFunding => row.selection.mark_funding = true,
        }
        Ok(())
    }
    pub fn warm_notice(&mut self, instrument: u64, notice: &Notice<'_>, now: u64) -> Result<()> {
        let (i, warm) = self.added(instrument)?;
        let result = self.warm_depth(i, warm, notice, now);
        if result.is_err() {
            invalidate_depth(&mut self.state.rows[i], now);
        }
        result
    }
    fn warm_depth(&mut self, i: usize, warm: usize, notice: &Notice<'_>, now: u64) -> Result<()> {
        if now < self.last_now {
            return Err(Error::Clock);
        }
        self.last_now = now;
        let (connection, generation) = match notice {
            Notice::Http(_) => return Ok(()),
            Notice::Gap(g) => (g.connection_epoch, g.book_generation),
            Notice::Sync {
                connection_epoch,
                book_generation,
                ..
            } => (*connection_epoch, *book_generation),
        };
        let w = &mut self.warming[warm];
        if connection == 0 || connection < w.connection || generation < w.generation {
            return Err(Error::Identity);
        }
        if connection > w.connection || generation > w.generation {
            invalidate_depth(&mut self.state.rows[i], now);
        }
        w.connection = connection;
        w.generation = generation;
        match notice {
            Notice::Gap(_) => invalidate_depth(&mut self.state.rows[i], now),
            Notice::Sync { outcome, view, .. } => {
                if matches!(outcome, SyncOutcome::Rebuild(_) | SyncOutcome::Disconnected) {
                    invalidate_depth(&mut self.state.rows[i], now);
                } else if !matches!(
                    outcome,
                    SyncOutcome::IgnoredObsolete
                        | SyncOutcome::Applied(tickwire_book::ApplyOutcome::IgnoredStale)
                ) && let Some(view) = view
                {
                    if view.generation() != generation {
                        return Err(Error::Identity);
                    }
                    cache_view(
                        &mut self.state.rows[i],
                        &self.state.header,
                        self.state.limits,
                        view,
                        now,
                    )?;
                }
            }
            Notice::Http(_) => unreachable!(),
        }
        Ok(())
    }
    pub fn warm_event_notice(
        &mut self,
        instrument: u64,
        notice: &EventNotice<'_>,
        now: u64,
    ) -> Result<()> {
        let (i, warm) = self.added(instrument)?;
        let kind = match notice {
            EventNotice::Http(_) => None,
            EventNotice::Gap(g) => Some(g.kind),
            EventNotice::Event { event, .. } => Some(event_kind(event)),
        };
        let result = self.warm_event(i, warm, notice, now);
        if result.is_err() {
            if let Some(kind) = kind {
                invalidate_event(&mut self.state.rows[i], events::category(kind), now);
            } else {
                invalidate_depth(&mut self.state.rows[i], now);
            }
        }
        result
    }
    fn warm_event(
        &mut self,
        i: usize,
        warm: usize,
        notice: &EventNotice<'_>,
        now: u64,
    ) -> Result<()> {
        if now < self.last_now {
            return Err(Error::Clock);
        }
        self.last_now = now;
        let (identity, kind) = match notice {
            EventNotice::Http(_) => return Ok(()),
            EventNotice::Gap(g) => (g.identity, g.kind),
            EventNotice::Event { event, .. } => (event.identity, event_kind(event)),
        };
        let cat = events::category(kind);
        let slot = events::source_index(cat);
        let row = &mut self.state.rows[i];
        check_identity(
            row,
            self.state.header.catalog_version,
            self.state.catalog,
            identity,
            cat,
        )?;
        if identity.connection_epoch == 0
            || identity.connection_epoch < self.warming[warm].event_epochs[slot]
        {
            return Err(Error::Identity);
        }
        self.warming[warm].event_epochs[slot] = identity.connection_epoch;
        match notice {
            EventNotice::Gap(_) => invalidate_event(row, cat, now),
            EventNotice::Event { event, usable, .. } => {
                if event.times.received_mono_ns > now {
                    return Err(Error::Clock);
                }
                let revision = row
                    .revision
                    .checked_add(1)
                    .filter(|n| *n < 1 << 63)
                    .ok_or(Error::Bounds)?;
                let h = header(
                    &self.state.header,
                    row.spec,
                    1,
                    264,
                    now,
                    event.times.received_mono_ns,
                );
                let mut state = MarketState::Exact(event_record(h, event, *usable)?);
                let mut raw = [0; 512];
                encode_state(&mut state, &mut raw)?;
                upsert(&mut row.states, state);
                row.revision = revision;
                refresh_feed(row, &self.state.header, now);
            }
            EventNotice::Http(_) => unreachable!(),
        }
        Ok(())
    }
    fn measure(&self) -> Result<usize> {
        measure_cache_bytes(&self.state, &self.depth_workspace, &self.depth_levels)?
            .checked_add(
                self.registry
                    .instruments
                    .capacity()
                    .checked_mul(size_of::<InstrumentSpec>())
                    .ok_or(Error::Bounds)?,
            )
            .and_then(|n| {
                self.warming
                    .capacity()
                    .checked_mul(size_of::<Warming>())
                    .and_then(|extra| n.checked_add(extra))
            })
            .ok_or(Error::Bounds)
    }
}
fn event_kind(event: &Event) -> EventKind {
    match event.value {
        EventValue::Trade(_) => EventKind::Trade,
        EventValue::BestBidAsk(_) => EventKind::BestBidAsk,
        EventValue::MarkFunding(_) => EventKind::MarkFunding,
    }
}
fn check_identity(
    row: &Row,
    catalog_version: u64,
    catalog: [u8; 32],
    id: StreamIdentity,
    cat: u8,
) -> Result<()> {
    if id.instrument_id != row.spec.instrument_id
        || id.instrument_version != row.spec.instrument_version
        || id.catalog_version != catalog_version
        || id.catalog_hash != catalog
        || row.event_sources[events::source_index(cat)].map(std::num::NonZeroU64::get)
            != Some(id.source_id)
    {
        return Err(Error::Identity);
    }
    Ok(())
}
fn refresh_feed(row: &mut Row, h: &ResourceHeader, now: u64) {
    let feed = MarketRecord {
        header: header(h, row.spec, 0, 261, now, row.depth_received),
        body: MarketBody {
            feed_state: aggregate_state(row),
            ..Default::default()
        },
    };
    upsert(&mut row.states, feed.into());
}
fn invalidate_depth(row: &mut Row, now: u64) {
    row.depth_state = 3;
    if let Some(MarketState::Legacy(feed)) = row
        .states
        .iter_mut()
        .find(|s| s.header().message_type == 261)
    {
        feed.body.feed_state = 3;
        feed.header.publish_mono_ns = now;
    }
}
fn invalidate_event(row: &mut Row, cat: u8, now: u64) {
    for state in &mut row.states {
        if market_state_category(state.header().message_type) == Ok(cat)
            || state.header().message_type == 261
        {
            match state {
                MarketState::Legacy(r) => r.body.feed_state = 3,
                MarketState::Exact(r) => r.body.feed_state = 3,
            }
            state.header_mut().publish_mono_ns = now;
        }
    }
}
fn fresh(row: &Row, now: u64, max_age: u64) -> bool {
    let age = |received| max_age > 0 && now.checked_sub(received).is_some_and(|n| n <= max_age);
    row.depth_state == 2
        && row.source_generation.is_some()
        && age(row.depth_received)
        && [1, 2, 4, 8].into_iter().all(|cat| {
            (cat != 8 && row.spec.required_states & cat == 0)
                || row.states.iter().any(|s| {
                    market_state_category(s.header().message_type) == Ok(cat)
                        && s.feed_state() == 2
                        && age(s.header().ingress_mono_ns)
                })
        })
}
// ExactDepthCoverage snapshot layout: page-aligned outer manifest and members.
fn snapshot_bytes(rows: &[Row]) -> Result<usize> {
    let page = |n: usize| n.checked_add(4095).map(|n| n & !4095).ok_or(Error::Bounds);
    let mut total = page(
        4352usize
            .checked_add(rows.len().checked_mul(128).ok_or(Error::Bounds)?)
            .ok_or(Error::Bounds)?,
    )?;
    for row in rows {
        let levels = row
            .bids
            .len()
            .checked_add(row.asks.len())
            .and_then(|n| n.checked_mul(16))
            .ok_or(Error::Bounds)?;
        let member = 480usize
            .checked_add(row.states.len().checked_mul(512).ok_or(Error::Bounds)?)
            .and_then(|n| n.checked_add(levels))
            .ok_or(Error::Bounds)?;
        total = total.checked_add(page(member)?).ok_or(Error::Bounds)?;
    }
    Ok(total)
}
fn cloned_row(row: &Row, limits: Limits) -> Row {
    let mut out = row.clone();
    out.states
        .reserve_exact(row.states.capacity().max(4) - out.states.len());
    out.bids
        .reserve_exact(row.bids.capacity().max(limits.levels_per_side) - out.bids.len());
    out.asks
        .reserve_exact(row.asks.capacity().max(limits.levels_per_side) - out.asks.len());
    out
}
fn copy_cache(to: &mut Row, from: &Row) {
    to.states.clear();
    to.states.extend_from_slice(&from.states);
    to.bids.clear();
    to.bids.extend_from_slice(&from.bids);
    to.asks.clear();
    to.asks.extend_from_slice(&from.asks);
    to.source_generation = from.source_generation;
    to.revision = from.revision;
    to.last_event = from.last_event;
    to.depth_state = from.depth_state;
    to.depth_received = from.depth_received;
    to.coverage = from.coverage.clone();
    if let Some(g) = from.source_generation {
        to.context.set_generation(g);
    }
}
impl<const M: usize> TopicPublisher<M> {
    pub fn prepare_expansion(
        &self,
        registry: TopicRegistry,
        additions: Vec<InstrumentConfig>,
        limits: Limits,
        peak_cache_bytes: usize,
    ) -> Result<TopicExpansion> {
        let old = self.owner.registry();
        if registry.catalog_hash != old.catalog_hash
            || registry.instrument_set_version <= old.instrument_set_version
            || registry.instrument_set_version >= 1 << 63
            || registry.instruments.len() > 4096
            || registry.instruments.len()
                != old
                    .instruments
                    .len()
                    .checked_add(additions.len())
                    .ok_or(Error::Bounds)?
            || additions.is_empty()
            || limits.levels_per_side < self.state.limits.levels_per_side
            || limits.levels_per_side > 5000
            || limits.snapshot_bytes == 0
            || limits.snapshot_bytes > 64 << 20
            || limits.max_delta_bytes == 0
            || limits.max_delta_bytes > 16 << 20
        {
            return Err(Error::Identity);
        }
        let minimum = registry
            .instruments
            .len()
            .checked_mul(
                limits
                    .levels_per_side
                    .checked_mul(2 * size_of::<DepthLevel>())
                    .and_then(|n| n.checked_add(size_of::<Row>() + 4 * size_of::<MarketState>()))
                    .ok_or(Error::Bounds)?,
            )
            .and_then(|n| {
                n.checked_add(
                    MAX_DELTA_LEVELS
                        * (size_of::<tickwire_book::Level>() + size_of::<DepthLevel>()),
                )
            })
            .ok_or(Error::Bounds)?;
        if minimum > limits.cache_bytes
            || minimum
                .checked_add(self.reserved_cache_bytes)
                .ok_or(Error::Bounds)?
                > peak_cache_bytes
        {
            return Err(Error::Bounds);
        }
        let mut prior = 0;
        for spec in &registry.instruments {
            if spec.instrument_id <= prior
                || !(1..1 << 63).contains(&spec.instrument_version)
                || spec.required_states & !7 != 0
            {
                return Err(Error::Identity);
            }
            prior = spec.instrument_id;
        }
        let mut additions = additions;
        additions.sort_unstable_by_key(|cfg| cfg.instrument.instrument_id);
        if additions
            .windows(2)
            .any(|p| p[0].instrument.instrument_id == p[1].instrument.instrument_id)
        {
            return Err(Error::Identity);
        }
        let mut rows = Vec::with_capacity(registry.instruments.len());
        let mut warming = Vec::with_capacity(additions.len());
        let mut additions = additions.into_iter().peekable();
        for spec in &registry.instruments {
            if let Some(id) = self.state.index.lookup(spec.instrument_id) {
                let old = &self.state.rows[id.index()];
                if old.spec.instrument_version != spec.instrument_version
                    || old.spec.required_states != spec.required_states
                {
                    return Err(Error::Identity);
                }
                rows.push(cloned_row(old, limits));
            } else {
                let cfg = additions.next().ok_or(Error::Identity)?;
                if cfg.instrument.instrument_id != spec.instrument_id
                    || cfg.instrument.instrument_version != spec.instrument_version
                    || cfg.instrument.catalog_version != self.state.header.catalog_version
                    || cfg.instrument.catalog_hash != registry.catalog_hash
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
                let mut states = Vec::with_capacity(4);
                states.push(MarketState::Legacy(MarketRecord {
                    header: header(&self.state.header, *spec, 0, 261, 0, 0),
                    body: MarketBody {
                        feed_state: 1,
                        ..Default::default()
                    },
                }));
                rows.push(Row {
                    spec: *spec,
                    key: cfg.key,
                    context,
                    source_generation: None,
                    revision: 0,
                    last_event: self.owner.published(),
                    states,
                    selection: EventSelection::default(),
                    event_sources: [None; 3],
                    depth_state: 1,
                    depth_received: 0,
                    instrument: cfg.instrument,
                    bids: Vec::with_capacity(limits.levels_per_side),
                    asks: Vec::with_capacity(limits.levels_per_side),
                    coverage: DepthCoverage {
                        version: 1,
                        ..Default::default()
                    },
                });
                warming.push(Warming {
                    instrument: spec.instrument_id,
                    connection: 0,
                    generation: 0,
                    event_epochs: [0; 3],
                });
            }
        }
        if additions.next().is_some() || rows.len() - warming.len() != self.state.rows.len() {
            return Err(Error::Identity);
        }
        let mut keys = std::collections::BTreeSet::new();
        if rows
            .iter()
            .any(|r| !keys.insert((r.key.venue, r.key.instrument, r.key.source)))
        {
            return Err(Error::Identity);
        }
        let index = FixedIndex::new(
            &rows
                .iter()
                .map(|r| r.spec.instrument_id)
                .collect::<Vec<_>>(),
        )
        .map_err(|_| Error::Bounds)?;
        let depth_workspace = DepthWorkspace::new(MAX_DELTA_LEVELS).map_err(Error::Decode)?;
        let mut depth_levels = Vec::new();
        depth_levels
            .try_reserve_exact(MAX_DELTA_LEVELS)
            .map_err(|_| Error::Bounds)?;
        let mut candidate = TopicExpansion {
            state: TopicState {
                header: self.state.header.clone(),
                catalog: self.state.catalog,
                rows,
                index,
                limits,
                sequence: self.state.sequence,
                dirty: true,
            },
            registry,
            source_resource: self.owner.header().resource_id,
            source_set_version: old.instrument_set_version,
            warming,
            last_now: self.last_now,
            peak_cache_bytes,
            depth_workspace,
            depth_levels,
            reserved_cache_bytes: 0,
        };
        candidate.reserved_cache_bytes = candidate.measure()?;
        if candidate.reserved_cache_bytes > limits.cache_bytes
            || candidate
                .reserved_cache_bytes
                .checked_add(self.reserved_cache_bytes)
                .ok_or(Error::Bounds)?
                > peak_cache_bytes
        {
            return Err(Error::Bounds);
        }
        Ok(candidate)
    }
    pub fn commit_expansion(
        &mut self,
        mut candidate: TopicExpansion,
        mut replacement: JoinOwner<M>,
        now: u64,
        max_age_ns: u64,
        faults: &mut impl FaultSink,
    ) -> Result<JoinOwner<M>> {
        let h = replacement.header().clone();
        let old = self.owner.header();
        if candidate.source_resource != old.resource_id
            || candidate.state.header != *old
            || candidate.source_set_version != self.owner.registry().instrument_set_version
            || !self.owner.all_detached()
            || self.needs_bank
            || !self.state.live()
            || now < self.last_now
            || now < candidate.last_now
            || h.resource_id == old.resource_id
            || h.channel_id != old.channel_id
            || h.channel_epoch != old.channel_epoch
            || h.topic_id != old.topic_id
            || h.catalog_version != old.catalog_version
            || h.producer_instance != old.producer_instance
            || h.producer_generation != old.producer_generation
            || h.host_boot_id != old.host_boot_id
            || h.namespace_id != old.namespace_id
            || h.clock_domain != old.clock_domain
            || h.target_arch != old.target_arch
            || h.trust_profile != old.trust_profile
            || h.schema_major != old.schema_major
            || h.schema_minor != old.schema_minor
            || h.required_features != old.required_features
            || h.optional_features != old.optional_features
            || h.qos != old.qos
            || h.feed_generation <= old.feed_generation
            || h.ring_epoch <= old.ring_epoch
            || h.predecessor_ring_epoch != old.ring_epoch
            || h.cutover_seq != self.owner.published()
            || h.first_seq != self.owner.published().checked_add(1).ok_or(Error::Bounds)?
            || replacement.published() != h.first_seq - 1
            || replacement.membership_version() != h.initial_membership_version
            || !replacement.all_detached()
            || replacement.registry().catalog_hash != candidate.registry.catalog_hash
            || replacement.registry().instrument_set_version
                != candidate.registry.instrument_set_version
            || replacement.registry().instruments != candidate.registry.instruments
        {
            return Err(Error::Identity);
        }
        for old in &self.state.rows {
            let row = &mut candidate.state.rows[candidate
                .state
                .index
                .lookup(old.spec.instrument_id)
                .ok_or(Error::Identity)?
                .index()];
            if row.key != old.key || row.event_sources != old.event_sources {
                return Err(Error::Identity);
            }
            copy_cache(row, old);
        }
        if !candidate
            .state
            .rows
            .iter()
            .all(|r| fresh(r, now, max_age_ns))
        {
            return Err(Error::NeedsBank);
        }
        if snapshot_bytes(&candidate.state.rows)? > candidate.state.limits.snapshot_bytes {
            return Err(Error::Bounds);
        }
        let retained = candidate.measure()?;
        if retained > candidate.state.limits.cache_bytes
            || retained
                .checked_add(self.reserved_cache_bytes)
                .ok_or(Error::Bounds)?
                > candidate.peak_cache_bytes
        {
            return Err(Error::Bounds);
        }
        candidate.state.header = h.clone();
        for row in &mut candidate.state.rows {
            for state in &mut row.states {
                state.header_mut().feed_generation = h.feed_generation;
                state.header_mut().sequence = replacement
                    .published()
                    .checked_add(1)
                    .filter(|n| *n < 1 << 63)
                    .ok_or(Error::Bounds)?;
                state.header_mut().publish_mono_ns = now;
                let mut raw = [0; 512];
                encode_state(state, &mut raw)?;
                replacement.publish(&[&raw])?;
            }
            row.last_event = replacement.published();
        }
        let reserved = measure_cache_bytes(
            &candidate.state,
            &candidate.depth_workspace,
            &candidate.depth_levels,
        )?;
        if let Err(error) = self.owner.freeze_for_successor(h.ring_epoch) {
            self.fault(self.state.rows[0].spec.instrument_id, faults);
            return Err(Error::Join(error));
        }
        candidate.state.sequence = replacement.published();
        candidate.state.dirty = false;
        self.state = candidate.state;
        self.depth_workspace = candidate.depth_workspace;
        self.depth_levels = candidate.depth_levels;
        self.reserved_cache_bytes = reserved;
        self.needs_bank = false;
        self.last_now = now;
        Ok(std::mem::replace(&mut self.owner, replacement))
    }
}
#[cfg(all(test, target_os = "linux"))]
mod tests;
