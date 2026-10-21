use super::*;
use tickwire_binance::market::{Event, EventValue};
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct EventSelection {
    pub trade: bool,
    pub best_bid_ask: bool,
    pub mark_funding: bool,
}
pub fn event_record(
    mut header: RecordHeader,
    event: &Event,
    usable: bool,
) -> Result<MarketExactRecord> {
    if header.instrument_id != event.identity.instrument_id
        || header.instrument_version != event.identity.instrument_version
        || header.ingress_mono_ns != event.times.received_mono_ns
    {
        return Err(Error::Identity);
    }
    let mut b = MarketExactBody {
        source_id: event.identity.source_id,
        source_connection_epoch: event.identity.connection_epoch,
        source_encoding: match event.encoding {
            tickwire_binance::market::EventEncoding::Json => 1,
            _ => 2,
        },
        source_schema_id: match event.encoding {
            tickwire_binance::market::EventEncoding::Json => 0,
            _ => 1,
        },
        feed_state: if usable { 2 } else { 3 },
        ..Default::default()
    };
    header.flags &= !1;
    header.source_timestamp_ns = 0;
    header.record_crc32c = 0;
    if let Some(t) = event.times.exchange_event_ns {
        b.value_validity |= 1;
        b.exchange_event_ns = t;
        header.source_timestamp_ns = t;
        header.flags |= 1;
    }
    if let Some(t) = event.times.transaction_ns {
        b.value_validity |= 2;
        b.transaction_time_ns = t;
    }
    let dec = |d: tickwire_venue::Decimal| DecimalWire::new(d.coefficient(), d.exponent());
    match event.value {
        EventValue::Trade(t) => {
            header.message_type = 264;
            b.trade_id = t.trade_id;
            b.price_ticks = t.price_ticks;
            b.quantity_lots = t.quantity_lots;
            b.side = if t.buyer_is_maker { 2 } else { 1 };
            b.trade_flags = u8::from(t.buyer_is_maker) | (u8::from(t.aggregate) * 2);
            if let Some((f, l)) = t.raw_trade_range {
                b.value_validity |= 128;
                b.venue_seq_first = f;
                b.venue_seq_last = l;
            }
            if let Some(n) = t.normal_quantity_lots {
                b.value_validity |= 4;
                b.normal_quantity_lots = n;
            }
        }
        EventValue::BestBidAsk(q) => {
            header.message_type = 265;
            b.venue_seq_first = q.update_id;
            b.venue_seq_last = q.update_id;
            b.bid_price_ticks = q.bid_price_ticks;
            b.bid_quantity_lots = q.bid_quantity_lots;
            b.ask_price_ticks = q.ask_price_ticks;
            b.ask_quantity_lots = q.ask_quantity_lots;
        }
        EventValue::MarkFunding(m) => {
            header.message_type = 266;
            b.mark_price = dec(m.mark_price)?;
            b.index_price = dec(m.index_price)?;
            if let Some(v) = m.estimated_settlement_price {
                b.value_validity |= 8;
                b.estimated_settlement_price = dec(v)?;
            }
            if let Some(v) = m.moving_average {
                b.value_validity |= 16;
                b.moving_average = dec(v)?;
            }
            if let Some(v) = m.funding_rate {
                b.value_validity |= 32;
                b.funding_rate = dec(v)?;
            }
            if let Some(v) = m.next_funding_utc_ns {
                b.value_validity |= 64;
                b.next_funding_utc_ns = v;
            }
        }
    }
    Ok(MarketExactRecord { header, body: b })
}
fn category(kind: tickwire_binance::market::EventKind) -> u8 {
    use tickwire_binance::market::EventKind::*;
    match kind {
        Trade => 2,
        BestBidAsk => 1,
        MarkFunding => 4,
    }
}
pub(super) fn upsert(states: &mut Vec<MarketState>, state: MarketState) {
    let cat = market_state_category(state.header().message_type).expect("validated state type");
    if let Some(old) = states
        .iter_mut()
        .find(|r| market_state_category(r.header().message_type) == Ok(cat))
    {
        *old = state;
    } else {
        states.push(state);
    }
    states.sort_unstable_by_key(|r| r.header().message_type);
}
pub(super) fn aggregate_state(row: &Row) -> u16 {
    if row.depth_state != 2 {
        return row.depth_state;
    }
    for cat in [1, 2, 4] {
        if row.spec.required_states & cat != 0
            && !row.states.iter().any(|r| {
                market_state_category(r.header().message_type) == Ok(cat) && r.feed_state() == 2
            })
        {
            return 1;
        }
    }
    2
}
pub(super) fn encode_state(state: &mut MarketState, out: &mut [u8; 512]) -> Result<()> {
    state.header_mut().record_crc32c = 0;
    state.encode(out)?;
    if state.header().flags & 8 != 0 {
        let crc = crc32c(out);
        state.header_mut().record_crc32c = crc;
        state.encode(out)?;
    }
    decode_record(out)?;
    Ok(())
}
impl<const M: usize> TopicPublisher<M> {
    /// Configure each independent event source before the first publication/JOIN.
    pub fn configure_event(
        &mut self,
        instrument: u64,
        kind: tickwire_binance::market::EventKind,
        source_id: u64,
    ) -> Result<()> {
        if self.owner.published() != self.state.header.first_seq - 1
            || self.state.header.required_features & EXACT_MARKET_EVENTS == 0
            || source_id == 0
        {
            return Err(Error::Identity);
        }
        let row = self
            .state
            .rows
            .iter_mut()
            .find(|r| r.spec.instrument_id == instrument)
            .ok_or(Error::Identity)?;
        let cat = category(kind);
        if row.spec.required_states & cat == 0
            || row.event_sources.contains_key(&cat)
            || (cat == 4 && matches!(row.instrument.contract, tickwire_venue::ContractKind::Cash))
        {
            return Err(Error::Identity);
        }
        row.event_sources.insert(cat, source_id);
        match cat {
            1 => row.selection.best_bid_ask = true,
            2 => row.selection.trade = true,
            4 => row.selection.mark_funding = true,
            _ => unreachable!(),
        };
        Ok(())
    }
    pub fn on_event_notice(
        &mut self,
        instrument: u64,
        notice: &tickwire_binance_public::EventNotice<'_>,
        now: u64,
        faults: &mut impl FaultSink,
    ) -> Result<()> {
        use tickwire_binance_public::EventNotice;
        if now < self.last_now {
            self.fault(instrument, faults);
            return Err(Error::Clock);
        }
        self.last_now = now;
        let i = self
            .state
            .rows
            .binary_search_by_key(&instrument, |r| r.spec.instrument_id)
            .map_err(|_| Error::Identity)?;
        let result = match notice {
            EventNotice::Http(_) => Ok(()),
            EventNotice::Gap(g) => {
                let row = &mut self.state.rows[i];
                let cat = category(g.kind);
                if row.event_sources.get(&cat) != Some(&g.identity.source_id)
                    || g.identity.instrument_id != instrument
                    || g.identity.instrument_version != row.spec.instrument_version
                    || g.identity.catalog_version != self.state.header.catalog_version
                    || g.identity.catalog_hash != self.state.catalog
                {
                    return Err(Error::Identity);
                }
                if let Some(MarketState::Exact(r)) = row
                    .states
                    .iter_mut()
                    .find(|s| market_state_category(s.header().message_type) == Ok(cat))
                {
                    if g.identity.connection_epoch < r.body.source_connection_epoch {
                        return Err(Error::Identity);
                    }
                    r.body.feed_state = 3;
                    r.header.publish_mono_ns = now;
                }
                if let Some(MarketState::Legacy(feed)) = row
                    .states
                    .iter_mut()
                    .find(|s| s.header().message_type == 261)
                {
                    feed.body.feed_state = 3;
                    feed.header.publish_mono_ns = now;
                }
                self.fault(instrument, faults);
                Ok(())
            }
            EventNotice::Event { event, usable, .. } => self.apply_event(i, event, *usable, now),
        };
        if result.is_err() {
            self.fault(instrument, faults);
        }
        result
    }
    fn apply_event(&mut self, index: usize, event: &Event, usable: bool, now: u64) -> Result<()> {
        let row = &mut self.state.rows[index];
        let kind = match event.value {
            EventValue::Trade(_) => 264,
            EventValue::BestBidAsk(_) => 265,
            EventValue::MarkFunding(_) => 266,
        };
        let cat = market_state_category(kind)?;
        if event.times.received_mono_ns > now
            || event.identity.instrument_id != row.spec.instrument_id
            || event.identity.instrument_version != row.spec.instrument_version
            || event.identity.catalog_version != self.state.header.catalog_version
            || event.identity.catalog_hash != self.state.catalog
            || row.event_sources.get(&cat) != Some(&event.identity.source_id)
        {
            return Err(Error::Identity);
        }
        if let Some(MarketState::Exact(old)) = row
            .states
            .iter()
            .find(|s| market_state_category(s.header().message_type) == Ok(cat))
            && event.identity.connection_epoch < old.body.source_connection_epoch
        {
            return Err(Error::Identity);
        }
        let revision = row
            .revision
            .checked_add(1)
            .filter(|n| *n < 1 << 63)
            .ok_or(Error::Bounds)?;
        let h = header(
            &self.state.header,
            row.spec,
            self.owner.published() + 1,
            kind,
            now,
            event.times.received_mono_ns,
        );
        let mut state = MarketState::Exact(event_record(h, event, usable)?);
        let mut raw = [0; 512];
        encode_state(&mut state, &mut raw)?;
        upsert(&mut row.states, state);
        row.revision = revision;
        let mut feed: MarketState = MarketRecord {
            header: header(
                &self.state.header,
                row.spec,
                self.owner.published() + 2,
                261,
                now,
                row.depth_received,
            ),
            body: MarketBody {
                feed_state: aggregate_state(row),
                ..Default::default()
            },
        }
        .into();
        let mut feed_raw = [0; 512];
        encode_state(&mut feed, &mut feed_raw)?;
        upsert(&mut row.states, feed);
        self.state.dirty = true;
        if self.needs_bank {
            return Ok(());
        }
        let published = self.owner.publish(&[&raw, &feed_raw])?;
        row.last_event = published;
        self.state.sequence = published;
        self.state.dirty = false;
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exact_publication_preserves_decimal_identity_and_absent_source_time() {
        use tickwire_binance::market::*;
        let event = Event {
            identity: StreamIdentity {
                source_id: 1,
                connection_epoch: 2,
                instrument_id: 4,
                instrument_version: 5,
                catalog_version: 6,
                catalog_hash: [7; 32],
            },
            times: EventTimes {
                received_mono_ns: 50,
                exchange_event_ns: None,
                transaction_ns: None,
            },
            encoding: EventEncoding::Json,
            value: EventValue::MarkFunding(MarkFunding {
                mark_price: tickwire_venue::Decimal::new(1179415, -2).unwrap(),
                index_price: tickwire_venue::Decimal::new(1178462659091, -8).unwrap(),
                estimated_settlement_price: None,
                moving_average: None,
                funding_rate: Some(tickwire_venue::Decimal::new(-3, -4).unwrap()),
                next_funding_utc_ns: None,
            }),
        };
        let h = RecordHeader {
            sequence: 1,
            schema_major: 1,
            header_bytes: 128,
            body_bytes: 384,
            topic_or_scope_id: 1,
            producer_generation: 1,
            feed_generation: 1,
            instrument_id: 4,
            instrument_version: 5,
            flags: 6,
            ingress_mono_ns: 50,
            publish_mono_ns: 51,
            ..Default::default()
        };
        let r = event_record(h, &event, true).unwrap();
        assert_eq!(r.body.index_price.coefficient_value(), 1178462659091);
        assert_eq!(r.body.index_price.exponent, -8);
        assert_eq!(r.body.funding_rate.coefficient_value(), -3);
        assert_eq!(r.header.source_timestamp_ns, 0);
        assert_eq!(r.body.value_validity, 32);
        let mut raw = [0; 512];
        r.encode(&mut raw).unwrap();
        decode_record(&raw).unwrap();
    }
}
