use crate::{Error, Result, Strategy};
use tickwire_ipc::{DepthLevel, RecordHeader, Wire};
use tickwire_join::JoinConsumer;
use tickwire_market_source::replica as core;
use tickwire_snapshot::ValidatedTopic;
use tickwire_venue::CanonicalCommand;
pub struct MarketReplica(core::MarketReplica);
pub struct MarketView<'a>(core::MarketView<'a>);
impl MarketView<'_> {
    pub fn instrument(&self) -> u64 {
        self.0.instrument()
    }
    pub fn live(&self) -> bool {
        self.0.live()
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
    pub fn apply(&mut self, bytes: &[u8], payload: &[u8]) -> Result<u64> {
        Ok(self.0.apply(bytes, payload)?)
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
            let id = self.apply(guard.bytes(), payload)?;
            let seq = guard.sequence();
            drop(guard);
            consumer.apply(seq)?;
            journal.decision(id, now)?;
            if let Some(view) = self.view(id)
                && let Some(command) = strategy.on_market(view, now)?
            {
                emit(command, journal)?;
            }
            count += 1;
        }
        Ok(count)
    }
}
