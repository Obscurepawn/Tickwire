use crate::{Error, Result, Strategy};
use tickwire_ipc::{DepthLevel, RecordHeader, Wire};
use tickwire_join::JoinConsumer;
use tickwire_market_source::replica as core;
pub use tickwire_market_source::replica::DenseId;
use tickwire_snapshot::ValidatedTopic;
use tickwire_venue::CanonicalCommand;
pub struct MarketReplica(core::MarketReplica);
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
    pub fn live(&self) -> bool {
        self.0.live()
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
    pub fn received_mono_ns(&self) -> u64 {
        self.0.received_mono_ns()
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
}
impl MarketReplica {
    pub fn checkpoint(&self) -> Result<Vec<u8>> {
        Ok(self.0.checkpoint()?)
    }
    pub fn restore_checkpoint(bytes: &[u8], maximum_levels: usize) -> Result<Self> {
        Ok(Self(core::MarketReplica::restore_checkpoint(
            bytes,
            maximum_levels,
        )?))
    }
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
    /// Consume complete ring records, ending each slot/block borrow before strategy code.
    pub fn drain<const B: usize, S: Strategy>(
        &mut self,
        consumer: &JoinConsumer<B>,
        strategy: &mut S,
        now: u64,
        budget: usize,
        mut emit: impl FnMut(CanonicalCommand) -> Result<()>,
    ) -> Result<usize> {
        self.drain_recorded(
            consumer,
            strategy,
            now,
            budget,
            &mut crate::recovery::NoJournal,
            |c, _| emit(c),
        )
    }
    pub fn drain_recorded<const B: usize, S: Strategy, J: crate::recovery::InputJournal>(
        &mut self,
        consumer: &JoinConsumer<B>,
        strategy: &mut S,
        now: u64,
        budget: usize,
        journal: &mut J,
        mut emit: impl FnMut(CanonicalCommand, &mut J) -> Result<()>,
    ) -> Result<usize> {
        if budget == 0 {
            return Err(Error::Bounds);
        }
        let mut count = 0;
        while count < budget {
            let Some(guard) = consumer.borrow()? else {
                break;
            };
            let h = RecordHeader::decode(&guard.bytes()[..128])?;
            let payload = if h.message_type == 259 {
                guard.depth_payload()?
            } else {
                &[]
            };
            journal.market(guard.bytes(), payload)?;
            let dense = self.apply_dense(guard.bytes(), payload)?;
            let seq = guard.sequence();
            drop(guard);
            consumer.apply(seq)?;
            let view = self.view_dense(dense).ok_or(Error::Identity)?;
            journal.decision(view.instrument(), now)?;
            if let Some(command) = strategy.on_market(view, now)? {
                emit(command, journal)?;
            }
            count += 1;
        }
        Ok(count)
    }
}
