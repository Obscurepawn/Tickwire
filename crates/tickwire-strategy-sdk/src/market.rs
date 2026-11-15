//! Strategy-private local `MarketReplica` and `MarketView` consuming `tickwire-md` SHM
//! broadcast rings (`MarketRecord` / `MarketExactRecord`) in place at an independent cursor.
#![forbid(unsafe_code)]

use tickwire_ipc::{DepthLevel, MarketRecord, MarketStateRecord, MarketStateRef};
use tickwire_join::JoinConsumer;
use tickwire_market_source::replica as core;
pub use tickwire_market_source::replica::DenseId;
use tickwire_snapshot::ValidatedTopic;

use crate::{Error, Result, Strategy, outbound::OutboundIntent};

/// Strategy-local order-book and quote replica maintained exclusively in private memory.
pub struct MarketReplica(core::MarketReplica);

/// Borrowed view of a single instrument's live quote/depth state inside [`MarketReplica`].
pub struct MarketView<'a>(core::MarketView<'a>);

impl MarketView<'_> {
    pub fn last_trade(&self) -> Option<&tickwire_ipc::MarketState> {
        self.0.last_trade()
    }

    pub fn best_quote(&self) -> Option<&tickwire_ipc::MarketState> {
        self.0.best_quote()
    }

    pub fn mark_funding(&self) -> Option<&tickwire_ipc::MarketState> {
        self.0.mark_funding()
    }

    pub fn state_age_ns(&self, category: u8, now: u64) -> Option<u64> {
        self.0.state_age_ns(category, now)
    }

    pub fn instrument(&self) -> u64 {
        self.0.instrument()
    }

    pub fn version(&self) -> u64 {
        self.0.version()
    }

    pub fn quote_only(&self) -> bool {
        self.0.quote_only()
    }

    pub fn live(&self) -> bool {
        self.0.live()
    }

    pub fn callback_ready_at(&self, now: u64, maximum_age_ns: u64) -> bool {
        self.0.callback_ready_at(now, maximum_age_ns)
    }

    pub fn continuous(&self) -> bool {
        self.0.continuous()
    }

    pub fn bids(&self) -> &[DepthLevel] {
        self.0.bids()
    }

    pub fn asks(&self) -> &[DepthLevel] {
        self.0.asks()
    }

    pub fn best_bid(&self) -> Option<&DepthLevel> {
        self.0.best_bid()
    }

    pub fn best_ask(&self) -> Option<&DepthLevel> {
        self.0.best_ask()
    }

    pub fn quantity(&self, bid: bool, price: i64) -> Option<i64> {
        self.0.quantity(bid, price)
    }

    pub fn published_mono_ns(&self) -> u64 {
        self.0.published_mono_ns()
    }

    pub fn received_mono_ns(&self) -> u64 {
        self.0.received_mono_ns()
    }

    /// Monotonic quote timestamp propagated into `IntentBody::tick_mono_ns` for ORS `max_order_age_ns`
    /// (`23 µs` default) staleness checks. Takes the freshest ingress timestamp across the book
    /// depth and live `MarketRecord` / `MarketExactRecord` state slots (`best_quote`, `last_trade`,
    /// `mark_funding`), falling back to `published_mono_ns` when ingress is unset.
    #[inline]
    pub fn tick_mono_ns(&self) -> u64 {
        let mut ingress = self.0.received_mono_ns();
        for state in [
            self.0.best_quote(),
            self.0.last_trade(),
            self.0.mark_funding(),
        ]
        .into_iter()
        .flatten()
        {
            let h = state.header();
            if h.flags & 2 != 0 {
                ingress = ingress.max(h.ingress_mono_ns);
            }
        }
        if ingress != 0 {
            ingress
        } else {
            self.0.published_mono_ns()
        }
    }

    pub fn trigger(&self) -> tickwire_venue::TriggerRef {
        self.trigger_at(self.0.published_mono_ns())
    }

    pub fn trigger_at(&self, now: u64) -> tickwire_venue::TriggerRef {
        let m = self.0.trigger_at(now);
        tickwire_venue::TriggerRef {
            topic_id: m.topic_id,
            feed_generation: m.feed_generation,
            ring_epoch: m.ring_epoch,
            sequence: m.sequence,
            instrument_id: m.instrument_id,
            instrument_version: m.instrument_version,
            observed_mono_ns: m.observed_mono_ns,
        }
    }

    pub fn ipc_trigger_at(
        &self,
        now: u64,
        product: Option<tickwire_venue::Product>,
    ) -> tickwire_ipc::TriggerRef {
        let m = self.0.trigger_at(now);
        tickwire_ipc::TriggerRef {
            topic_id: m.topic_id,
            feed_generation: m.feed_generation,
            ring_epoch: m.ring_epoch,
            sequence: m.sequence,
            instrument_id: m.instrument_id,
            instrument_version: m.instrument_version,
            observed_mono_ns: m.observed_mono_ns,
            required_feed_state: 2,
            product: product.map_or(0, |p| u16::from(p.code())),
            _pad_60: [0; 4],
        }
    }
}

impl MarketReplica {
    pub fn instruments(&self) -> impl Iterator<Item = u64> + '_ {
        self.0.instruments()
    }

    pub fn from_core(core: core::MarketReplica) -> Self {
        Self(core)
    }

    pub fn load(
        topic: u64,
        ring: u64,
        snapshot: &ValidatedTopic<'_>,
        maximum_levels: usize,
    ) -> tickwire_ipc::Result<Self> {
        Ok(Self(core::MarketReplica::load(
            topic,
            ring,
            snapshot,
            maximum_levels,
        )?))
    }

    pub fn invalidate(&mut self) {
        self.0.invalidate();
    }

    pub fn sequence(&self) -> u64 {
        self.0.sequence()
    }

    pub fn view(&self, instrument: u64) -> Option<MarketView<'_>> {
        self.0.view(instrument).map(MarketView)
    }

    /// Dense IDs are local to this replica's current instrument set.
    pub fn view_dense(&self, instrument: DenseId) -> Option<MarketView<'_>> {
        self.0.view_dense(instrument).map(MarketView)
    }

    pub fn apply(&mut self, bytes: &[u8], payload: &[u8]) -> Result<u64> {
        Ok(self.0.apply(bytes, payload)?)
    }

    pub fn apply_dense(&mut self, bytes: &[u8], payload: &[u8]) -> Result<DenseId> {
        Ok(self.0.apply_dense(bytes, payload)?)
    }

    pub fn apply_market_record(&mut self, record: &MarketRecord, payload: &[u8]) -> Result<u64> {
        Ok(self.0.apply_market_record(record, payload)?)
    }

    pub fn apply_market_record_dense(
        &mut self,
        record: &MarketRecord,
        payload: &[u8],
    ) -> Result<DenseId> {
        Ok(self.0.apply_market_record_dense(record, payload)?)
    }

    pub fn apply_state(&mut self, state: MarketStateRef<'_>, payload: &[u8]) -> Result<u64> {
        Ok(self.0.apply_state(state, payload)?)
    }

    pub fn apply_state_dense(
        &mut self,
        state: MarketStateRef<'_>,
        payload: &[u8],
    ) -> Result<DenseId> {
        Ok(self.0.apply_state_dense(state, payload)?)
    }

    /// Consume up to `budget` market ring records in place via `RecordBorrow::as_market_state`,
    /// releasing each ring borrow *before* invoking `Strategy::on_market` (`0` disk writes).
    pub fn drain<const B: usize, S: Strategy>(
        &mut self,
        consumer: &JoinConsumer<B>,
        strategy: &mut S,
        now: u64,
        maximum_age_ns: u64,
        budget: usize,
        mut emit: impl FnMut(OutboundIntent) -> Result<()>,
    ) -> Result<usize> {
        if budget == 0 {
            return Err(Error::Bounds);
        }
        let mut count = 0;
        while count < budget {
            let Some(guard) = consumer.borrow()? else {
                break;
            };
            let state = guard.as_market_state()?;
            let payload = if state.header().message_type == 259 {
                guard.depth_payload()?
            } else {
                &[]
            };
            let dense = self.apply_state_dense(state, payload)?;
            let seq = guard.sequence();
            drop(guard);
            consumer.apply(seq)?;
            let view = self.view_dense(dense).ok_or(Error::Identity)?;
            let ready = if view.quote_only() {
                view.callback_ready_at(now, maximum_age_ns)
            } else {
                true
            };
            if ready && let Some(intent) = strategy.on_market(view, now)? {
                emit(intent)?;
            }
            count += 1;
        }
        Ok(count)
    }
}
