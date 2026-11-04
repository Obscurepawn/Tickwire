//! Independently observed top-of-book snapshots, with no depth coverage.
use super::*;

#[derive(Clone, Copy, Debug)]
pub struct SpotQuote {
    pub key: BookKey,
    pub instrument: u64,
    pub version: u64,
    pub epoch: u64,
    pub sequence: u64,
    pub exchange_ns: u64,
    pub received_ns: u64,
    pub bid_ticks: i64,
    pub bid_lots: i64,
    pub ask_ticks: i64,
    pub ask_lots: i64,
}

impl<const M: usize> TopicPublisher<M> {
    pub fn on_spot_quote(&mut self, q: &SpotQuote, now: u64) -> Result<()> {
        let slot = self.instrument_slot(q.instrument)?;
        let row = &self.state.rows[slot.index()];
        if row.key != q.key
            || row.spec.instrument_version != q.version
            || q.bid_ticks <= 0
            || q.ask_ticks <= q.bid_ticks
            || q.bid_lots <= 0
            || q.ask_lots <= 0
            || q.exchange_ns == 0
        {
            return Err(Error::Identity);
        }
        let mut h = header(&self.state.header, row.spec, 0, 265, now, q.received_ns);
        h.flags |= 1;
        h.source_timestamp_ns = q.exchange_ns;
        let quote = MarketExactRecord {
            header: h,
            body: MarketExactBody {
                source_id: u64::from(row.key.source),
                source_connection_epoch: q.epoch,
                source_encoding: 1,
                feed_state: 2,
                value_validity: 1,
                exchange_event_ns: q.exchange_ns,
                venue_seq_first: q.sequence,
                venue_seq_last: q.sequence,
                bid_price_ticks: q.bid_ticks,
                bid_quantity_lots: q.bid_lots,
                ask_price_ticks: q.ask_ticks,
                ask_quantity_lots: q.ask_lots,
                ..Default::default()
            },
        };
        self.publish_spot(slot.index(), q.epoch, Some(quote), true, q.received_ns, now)
    }

    /// Preserve the previous observation's identity and age, while marking it unusable.
    pub fn spot_quote_unavailable(&mut self, instrument: u64, epoch: u64, now: u64) -> Result<()> {
        self.spot_quote_unavailable_at(instrument, epoch, now, now)
    }

    pub fn spot_quote_unavailable_at(
        &mut self,
        instrument: u64,
        epoch: u64,
        received: u64,
        now: u64,
    ) -> Result<()> {
        let index = self.instrument_slot(instrument)?.index();
        let quote = self.state.rows[index].states.iter().find_map(|s| match s {
            MarketState::Exact(r) if r.header.message_type == 265 => Some(r.clone()),
            _ => None,
        });
        self.publish_spot(index, epoch, quote, false, received, now)
    }

    fn publish_spot(
        &mut self,
        index: usize,
        epoch: u64,
        quote: Option<MarketExactRecord>,
        live: bool,
        received: u64,
        now: u64,
    ) -> Result<()> {
        let row = &mut self.state.rows[index];
        if self.needs_bank {
            return Err(Error::NeedsBank);
        }
        if row.context.is_some()
            || epoch == 0
            || epoch >= 1 << 63
            || row.source_generation.is_some_and(|g| epoch < g)
            || received > now
            || received < row.depth_received
            || now < self.last_now
        {
            return Err(Error::Identity);
        }
        let feed_state = if live { 2 } else { 3 };
        let seq = self.owner.published().checked_add(1).ok_or(Error::Bounds)?;
        let rev = row
            .revision
            .checked_add(1)
            .filter(|r| *r < 1 << 63)
            .ok_or(Error::Bounds)?;
        let mut quote = quote.map(|mut q| {
            // Close the aggregate gate BEFORE a subscriber can see a stale quote.
            q.header.sequence = seq + u64::from(!live);
            q.header.publish_mono_ns = now;
            q.body.feed_state = feed_state;
            MarketState::Exact(q)
        });
        let mut feed = MarketState::Legacy(MarketRecord {
            header: header(
                &self.state.header,
                row.spec,
                seq + u64::from(live && quote.is_some()),
                261,
                now,
                received,
            ),
            body: MarketBody {
                feed_state,
                source_schema_id: SPOT_BBO_FEED_SCHEMA,
                ..Default::default()
            },
        });
        let mut feed_raw = [0; 512];
        encode_state(&mut feed, &mut feed_raw)?;
        let result = if let Some(q) = quote.as_mut() {
            let mut quote_raw = [0; 512];
            encode_state(q, &mut quote_raw)?;
            if live {
                self.owner.publish(&[&quote_raw, &feed_raw])
            } else {
                self.owner.publish(&[&feed_raw, &quote_raw])
            }
        } else {
            self.owner.publish(&[&feed_raw])
        };
        let published = match result {
            Ok(s) => s,
            Err(e) => {
                self.require_rebuild();
                return Err(e.into());
            }
        };
        if let Some(q) = quote {
            upsert(&mut row.states, q);
        }
        upsert(&mut row.states, feed);
        row.source_generation = Some(epoch);
        row.last_event = published;
        row.revision = rev;
        row.depth_state = feed_state;
        row.depth_received = received;
        self.last_now = now;
        self.state.sequence = published;
        Ok(())
    }
}
