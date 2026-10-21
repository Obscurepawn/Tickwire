//! Typed public trade, best bid/ask and reference-price decoding.
use crate::{Error, Result, feed::TimeUnit, request::Market};
use tickwire_venue::{Decimal, InstrumentBinding};
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EventKind {
    Trade,
    BestBidAsk,
    MarkFunding,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EventEncoding {
    Json,
    SpotSbeV1,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StreamIdentity {
    pub source_id: u64,
    pub connection_epoch: u64,
    pub instrument_id: u64,
    pub instrument_version: u64,
    pub catalog_version: u64,
    pub catalog_hash: [u8; 32],
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EventTimes {
    pub received_mono_ns: u64,
    pub exchange_event_ns: Option<u64>,
    pub transaction_ns: Option<u64>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Trade {
    pub trade_id: u64,
    pub raw_trade_range: Option<(u64, u64)>,
    pub price_ticks: i64,
    pub quantity_lots: i64,
    pub normal_quantity_lots: Option<i64>,
    pub buyer_is_maker: bool,
    pub aggregate: bool,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BestBidAsk {
    pub update_id: u64,
    pub bid_price_ticks: i64,
    pub bid_quantity_lots: i64,
    pub ask_price_ticks: i64,
    pub ask_quantity_lots: i64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MarkFunding {
    pub mark_price: Decimal,
    pub index_price: Decimal,
    pub estimated_settlement_price: Option<Decimal>,
    pub moving_average: Option<Decimal>,
    pub funding_rate: Option<Decimal>,
    pub next_funding_utc_ns: Option<u64>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EventValue {
    Trade(Trade),
    BestBidAsk(BestBidAsk),
    MarkFunding(MarkFunding),
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Event {
    pub identity: StreamIdentity,
    pub times: EventTimes,
    pub encoding: EventEncoding,
    pub value: EventValue,
}
#[derive(Clone, Debug)]
pub struct EventContext {
    pub market: Market,
    pub instrument: InstrumentBinding,
    pub identity: StreamIdentity,
    pub kind: EventKind,
    pub time_unit: TimeUnit,
}
impl EventContext {
    pub fn validate(&self) -> Result<()> {
        let i = &self.instrument;
        let s = self.identity;
        if s.source_id == 0
            || s.connection_epoch == 0
            || s.instrument_id != i.instrument_id
            || s.instrument_id == 0
            || s.instrument_version != i.instrument_version
            || s.instrument_version == 0
            || s.catalog_version != i.catalog_version
            || s.catalog_version == 0
            || s.catalog_hash != i.catalog_hash
            || s.catalog_hash == [0; 32]
            || !matches!(
                (self.market, i.contract),
                (Market::Spot, tickwire_venue::ContractKind::Cash)
                    | (
                        Market::Usdm,
                        tickwire_venue::ContractKind::Perpetual
                            | tickwire_venue::ContractKind::Delivery { .. }
                    )
            )
            || (self.market == Market::Usdm && self.time_unit != TimeUnit::Milliseconds)
            || (self.market == Market::Spot && self.kind == EventKind::MarkFunding)
            || !i.price_tick.is_positive()
            || !i.quantity_step.is_positive()
        {
            return Err(Error::IdentityMismatch);
        }
        Ok(())
    }
    pub fn stream_name(&self, encoding: EventEncoding, fast_mark: bool) -> Result<String> {
        self.validate()?;
        let suffix = match (self.market, self.kind, encoding) {
            (Market::Spot, EventKind::Trade, _) => "trade",
            (Market::Spot, EventKind::BestBidAsk, EventEncoding::SpotSbeV1) => "bestBidAsk",
            (_, EventKind::BestBidAsk, EventEncoding::Json) => "bookTicker",
            (Market::Usdm, EventKind::Trade, EventEncoding::Json) => "aggTrade",
            (Market::Usdm, EventKind::MarkFunding, EventEncoding::Json) => {
                if fast_mark {
                    "markPrice@1s"
                } else {
                    "markPrice"
                }
            }
            _ => return Err(Error::UnsupportedCommand),
        };
        Ok(format!(
            "{}@{suffix}",
            self.instrument.symbol.as_str().to_lowercase()
        ))
    }
}
fn check(bytes: &[u8], ctx: &EventContext, max: usize) -> Result<()> {
    ctx.validate()?;
    if max == 0 || max > crate::feed::MAX_MARKET_MESSAGE_BYTES || bytes.len() > max {
        return Err(Error::Capacity);
    }
    Ok(())
}
fn decimal(s: &str) -> Result<Decimal> {
    let d = crate::decimal::Decimal::parse(s)?;
    Decimal::new(d.mantissa(), -(d.scale() as i8)).map_err(|_| Error::InvalidDecimal)
}
fn optional_decimal(s: Option<&str>) -> Result<Option<Decimal>> {
    s.filter(|v| !v.is_empty()).map(decimal).transpose()
}
fn units(s: &str, unit: Decimal) -> Result<i64> {
    crate::feed::units(
        crate::decimal::Decimal::parse(s)?,
        crate::feed::venue_decimal(unit)?,
    )
}
fn required<T>(value: Option<T>) -> Result<T> {
    value.ok_or(Error::InvalidResponse)
}
/// Parse one raw/combined JSON event. Known protocol fields reject duplicate keys.
pub fn decode_json(bytes: &[u8], ctx: &EventContext, received: u64, max: usize) -> Result<Event> {
    check(bytes, ctx, max)?;
    use serde::Deserialize;
    use serde_json::value::RawValue;
    #[derive(Deserialize)]
    struct Envelope<'a> {
        #[serde(borrow)]
        stream: Option<&'a str>,
        #[serde(borrow)]
        data: Option<&'a RawValue>,
    }
    let envelope: Envelope<'_> =
        serde_json::from_slice(bytes).map_err(|_| Error::InvalidResponse)?;
    let payload = match (envelope.stream, envelope.data) {
        (None, None) => bytes,
        (Some(s), Some(d))
            if s == ctx.stream_name(EventEncoding::Json, false)?
                || s == ctx.stream_name(EventEncoding::Json, true)? =>
        {
            d.get().as_bytes()
        }
        _ => return Err(Error::IdentityMismatch),
    };
    #[derive(Deserialize)]
    #[allow(non_snake_case)]
    struct Wire<'a> {
        e: Option<&'a str>,
        s: &'a str,
        E: Option<u64>,
        T: Option<u64>,
        st: Option<u64>,
        t: Option<u64>,
        a: Option<&'a RawValue>,
        u: Option<u64>,
        f: Option<u64>,
        l: Option<u64>,
        p: Option<&'a str>,
        q: Option<&'a str>,
        nq: Option<&'a str>,
        m: Option<bool>,
        b: Option<&'a str>,
        B: Option<&'a str>,
        A: Option<&'a str>,
        i: Option<&'a str>,
        P: Option<&'a str>,
        r: Option<&'a str>,
        ap: Option<&'a str>,
    }
    let w: Wire<'_> = serde_json::from_slice(payload).map_err(|_| Error::InvalidResponse)?;
    if w.s != ctx.instrument.symbol.as_str()
        || (ctx.market == Market::Spot && w.st.is_some())
        || (ctx.market == Market::Usdm && w.st.is_some_and(|s| s != 1))
    {
        return Err(Error::IdentityMismatch);
    }
    let times = EventTimes {
        received_mono_ns: received,
        exchange_event_ns: w.E.map(|v| ctx.time_unit.nanoseconds(v)).transpose()?,
        transaction_ns: if ctx.kind == EventKind::MarkFunding {
            None
        } else {
            w.T.map(|v| ctx.time_unit.nanoseconds(v)).transpose()?
        },
    };
    let value = match ctx.kind {
        EventKind::Trade => {
            let aggregate = ctx.market == Market::Usdm;
            if w.e != Some(if aggregate { "aggTrade" } else { "trade" })
                || times.exchange_event_ns.is_none()
                || times.transaction_ns.is_none()
            {
                return Err(Error::InvalidResponse);
            }
            let id = if aggregate {
                serde_json::from_str::<u64>(required(w.a)?.get())
                    .map_err(|_| Error::InvalidResponse)?
            } else {
                required(w.t)?
            };
            let range = if aggregate {
                let (f, l) = (required(w.f)?, required(w.l)?);
                if f > l {
                    return Err(Error::InvalidResponse);
                }
                Some((f, l))
            } else {
                if w.f.is_some() || w.l.is_some() || w.nq.is_some() {
                    return Err(Error::InvalidResponse);
                }
                None
            };
            let price = units(required(w.p)?, ctx.instrument.price_tick)?;
            let quantity = units(required(w.q)?, ctx.instrument.quantity_step)?;
            let normal =
                w.nq.map(|n| units(n, ctx.instrument.quantity_step))
                    .transpose()?;
            if price <= 0 || quantity <= 0 || normal.is_some_and(|n| n < 0 || n > quantity) {
                return Err(Error::InvalidDecimal);
            }
            EventValue::Trade(Trade {
                trade_id: id,
                raw_trade_range: range,
                price_ticks: price,
                quantity_lots: quantity,
                normal_quantity_lots: normal,
                buyer_is_maker: required(w.m)?,
                aggregate,
            })
        }
        EventKind::BestBidAsk => {
            if (ctx.market == Market::Usdm
                && (w.e != Some("bookTicker")
                    || times.exchange_event_ns.is_none()
                    || times.transaction_ns.is_none()))
                || (ctx.market == Market::Spot && w.e.is_some_and(|e| e != "bookTicker"))
            {
                return Err(Error::InvalidResponse);
            }
            let ask: &str =
                serde_json::from_str(required(w.a)?.get()).map_err(|_| Error::InvalidResponse)?;
            let q = BestBidAsk {
                update_id: required(w.u)?,
                bid_price_ticks: units(required(w.b)?, ctx.instrument.price_tick)?,
                bid_quantity_lots: units(required(w.B)?, ctx.instrument.quantity_step)?,
                ask_price_ticks: units(ask, ctx.instrument.price_tick)?,
                ask_quantity_lots: units(required(w.A)?, ctx.instrument.quantity_step)?,
            };
            if q.bid_price_ticks <= 0
                || q.ask_price_ticks <= q.bid_price_ticks
                || q.bid_quantity_lots < 0
                || q.ask_quantity_lots < 0
            {
                return Err(Error::InvalidResponse);
            }
            EventValue::BestBidAsk(q)
        }
        EventKind::MarkFunding => {
            if w.e != Some("markPriceUpdate") || times.exchange_event_ns.is_none() {
                return Err(Error::InvalidResponse);
            }
            let m = MarkFunding {
                mark_price: decimal(required(w.p)?)?,
                index_price: decimal(required(w.i)?)?,
                estimated_settlement_price: optional_decimal(w.P)?,
                moving_average: optional_decimal(w.ap)?,
                funding_rate: optional_decimal(w.r)?,
                next_funding_utc_ns: w
                    .T
                    .filter(|v| *v != 0)
                    .map(|v| ctx.time_unit.nanoseconds(v))
                    .transpose()?,
            };
            if !m.mark_price.is_positive()
                || !m.index_price.is_positive()
                || m.estimated_settlement_price
                    .is_some_and(|d| d.coefficient() < 0)
                || m.moving_average.is_some_and(|d| d.coefficient() < 0)
            {
                return Err(Error::InvalidDecimal);
            }
            EventValue::MarkFunding(m)
        }
    };
    Ok(Event {
        identity: ctx.identity,
        times,
        encoding: EventEncoding::Json,
        value,
    })
}
/// Validate and normalize every row before returning any event. The owner retains
/// this bounded batch across poll budgets instead of dropping a partially read frame.
pub fn decode_sbe(
    bytes: &[u8],
    ctx: &EventContext,
    received: u64,
    max: usize,
    max_events: usize,
) -> Result<Vec<Event>> {
    check(bytes, ctx, max)?;
    if ctx.market != Market::Spot {
        return Err(Error::IdentityMismatch);
    }
    use tickwire_binance_sbe::{DecimalUnit, Kind, Limits, decimal_to_units, decode_frame};
    let frame = decode_frame(
        bytes,
        Limits {
            max_frame_bytes: max,
            max_trades: max_events,
            ..Limits::default()
        },
    )
    .map_err(|_| Error::InvalidResponse)?;
    if frame.symbol() != ctx.instrument.symbol.as_str() {
        return Err(Error::IdentityMismatch);
    }
    let price = DecimalUnit::new(
        ctx.instrument.price_tick.coefficient(),
        ctx.instrument.price_tick.exponent(),
    )
    .map_err(|_| Error::InvalidDecimal)?;
    let qty = DecimalUnit::new(
        ctx.instrument.quantity_step.coefficient(),
        ctx.instrument.quantity_step.exponent(),
    )
    .map_err(|_| Error::InvalidDecimal)?;
    let p =
        |n| decimal_to_units(n, frame.price_exponent(), price).map_err(|_| Error::InvalidDecimal);
    let q =
        |n| decimal_to_units(n, frame.quantity_exponent(), qty).map_err(|_| Error::InvalidDecimal);
    let event = |value| Event {
        identity: ctx.identity,
        times: EventTimes {
            received_mono_ns: received,
            exchange_event_ns: Some(frame.event_time_ns()),
            transaction_ns: frame.transaction_time_ns(),
        },
        encoding: EventEncoding::SpotSbeV1,
        value,
    };
    match (ctx.kind, frame.kind()) {
        (EventKind::Trade, Kind::Trades) => frame
            .trades()
            .ok_or(Error::InvalidResponse)?
            .iter()
            .map(|t| {
                let (price, quantity) = (p(t.price_mantissa)?, q(t.quantity_mantissa)?);
                if price <= 0 || quantity <= 0 {
                    return Err(Error::InvalidDecimal);
                }
                Ok(event(EventValue::Trade(Trade {
                    trade_id: t.id,
                    raw_trade_range: None,
                    price_ticks: price,
                    quantity_lots: quantity,
                    normal_quantity_lots: None,
                    buyer_is_maker: t.is_buyer_maker,
                    aggregate: false,
                })))
            })
            .collect(),
        (EventKind::BestBidAsk, Kind::BestBidAsk) => {
            let raw = frame.quote().ok_or(Error::InvalidResponse)?;
            let quote = BestBidAsk {
                update_id: frame.last_update_id().ok_or(Error::InvalidResponse)?,
                bid_price_ticks: p(raw.bid.price_mantissa)?,
                bid_quantity_lots: q(raw.bid.quantity_mantissa)?,
                ask_price_ticks: p(raw.ask.price_mantissa)?,
                ask_quantity_lots: q(raw.ask.quantity_mantissa)?,
            };
            if quote.bid_price_ticks <= 0
                || quote.ask_price_ticks <= quote.bid_price_ticks
                || quote.bid_quantity_lots < 0
                || quote.ask_quantity_lots < 0
            {
                return Err(Error::InvalidResponse);
            }
            Ok(vec![event(EventValue::BestBidAsk(quote))])
        }
        _ => Err(Error::InvalidResponse),
    }
}
