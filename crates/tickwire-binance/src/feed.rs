//! Exact, bounded decoding of Binance public market data.
use crate::{Error, Result, decimal::Decimal, request::Market};
use serde::{
    Deserialize,
    de::{DeserializeSeed, SeqAccess, Visitor},
};
use serde_json::value::RawValue;
use std::borrow::Cow;
use tickwire_book::{BookKey, DepthEvent, Level, PriceTicks, QtyLots, Snapshot};
use tickwire_venue::{ContractKind, InstrumentBinding};
mod metadata;
pub use metadata::*;

pub const MAX_MARKET_MESSAGE_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_DEPTH_LEVELS: usize = 5000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeUnit {
    Milliseconds,
    Microseconds,
}
impl TimeUnit {
    pub fn nanoseconds(self, value: u64) -> Result<u64> {
        value
            .checked_mul(match self {
                Self::Milliseconds => 1_000_000,
                Self::Microseconds => 1_000,
            })
            .ok_or(Error::Overflow)
    }
}
#[derive(Debug, Clone)]
pub struct DepthContext {
    market: Market,
    instrument: InstrumentBinding,
    key: BookKey,
    generation: u64,
    time_unit: TimeUnit,
    stream_symbol: String,
    price_tick: Decimal,
    quantity_step: Decimal,
}
impl DepthContext {
    pub fn new(
        market: Market,
        instrument: InstrumentBinding,
        key: BookKey,
        generation: u64,
        time_unit: TimeUnit,
    ) -> Result<Self> {
        if key.venue == 0
            || key.source == 0
            || u64::from(key.instrument) != instrument.instrument_id
            || instrument.instrument_version == 0
            || instrument.catalog_version == 0
            || instrument.catalog_hash == [0; 32]
            || !matches!(
                (market, instrument.contract),
                (Market::Spot, ContractKind::Cash)
                    | (
                        Market::Usdm,
                        ContractKind::Perpetual | ContractKind::Delivery { .. }
                    )
            )
            || (market == Market::Usdm && time_unit != TimeUnit::Milliseconds)
        {
            return Err(Error::IdentityMismatch);
        }
        let price_tick = venue_decimal(instrument.price_tick)?;
        let quantity_step = venue_decimal(instrument.quantity_step)?;
        if price_tick.mantissa() <= 0 || quantity_step.mantissa() <= 0 {
            return Err(Error::InvalidParameter);
        }
        let stream_symbol = instrument.symbol.as_str().to_lowercase();
        Ok(Self {
            market,
            instrument,
            key,
            generation,
            time_unit,
            stream_symbol,
            price_tick,
            quantity_step,
        })
    }
    fn stream_matches(&self, stream: &str) -> bool {
        let Some(suffix) = stream.strip_prefix(&self.stream_symbol) else {
            return false;
        };
        matches!(
            (self.market, suffix),
            (_, "@depth" | "@depth@100ms")
                | (Market::Spot, "@depth@1000ms")
                | (Market::Usdm, "@depth@500ms")
        )
    }
}
const EMPTY_LEVEL: Level = Level {
    price: PriceTicks(0),
    quantity: QtyLots(0),
};
#[derive(Debug)]
struct Levels<const N: usize> {
    values: [Level; N],
    len: usize,
}
impl<const N: usize> Levels<N> {
    fn as_slice(&self) -> &[Level] {
        &self.values[..self.len]
    }
}
#[derive(Debug)]
struct DecodedDepth<const N: usize> {
    key: BookKey,
    generation: u64,
    bids: Levels<N>,
    asks: Levels<N>,
    first: u64,
    last: u64,
    previous: Option<u64>,
    received: u64,
    event_time_ns: Option<u64>,
    transaction_time_ns: Option<u64>,
}
#[derive(Debug)]
pub struct DepthSnapshot<const N: usize>(DecodedDepth<N>);
#[derive(Debug)]
pub struct DepthUpdate<const N: usize>(DecodedDepth<N>);
impl<const N: usize> DepthSnapshot<N> {
    pub fn snapshot(&self) -> Snapshot<'_> {
        Snapshot {
            key: self.0.key,
            generation: self.0.generation,
            last_update_id: self.0.last,
            bids: self.0.bids.as_slice(),
            asks: self.0.asks.as_slice(),
            bids_complete: false,
            asks_complete: false,
        }
    }
    pub fn event_time_ns(&self) -> Option<u64> {
        self.0.event_time_ns
    }
    pub fn transaction_time_ns(&self) -> Option<u64> {
        self.0.transaction_time_ns
    }
}
impl<const N: usize> DepthUpdate<N> {
    pub fn event(&self) -> DepthEvent<'_> {
        DepthEvent {
            key: self.0.key,
            generation: self.0.generation,
            first_update_id: self.0.first,
            last_update_id: self.0.last,
            previous_update_id: self.0.previous,
            received_at_ns: self.0.received,
            bids: self.0.bids.as_slice(),
            asks: self.0.asks.as_slice(),
        }
    }
    pub fn event_time_ns(&self) -> Option<u64> {
        self.0.event_time_ns
    }
    pub fn transaction_time_ns(&self) -> Option<u64> {
        self.0.transaction_time_ns
    }
}
pub fn decode_snapshot<const N: usize>(
    bytes: &[u8],
    context: &DepthContext,
    max_bytes: usize,
) -> Result<DepthSnapshot<N>> {
    check_input::<N>(bytes, max_bytes)?;
    #[derive(Deserialize)]
    struct Wire<'a> {
        #[serde(rename = "lastUpdateId")]
        last: u64,
        #[serde(rename = "E")]
        event: Option<u64>,
        #[serde(rename = "T")]
        transaction: Option<u64>,
        #[serde(borrow)]
        bids: &'a RawValue,
        #[serde(borrow)]
        asks: &'a RawValue,
    }
    let wire: Wire<'_> = serde_json::from_slice(bytes).map_err(|_| Error::InvalidResponse)?;
    if context.market == Market::Usdm && (wire.event.is_none() || wire.transaction.is_none()) {
        return Err(Error::InvalidResponse);
    }
    let bids = levels(wire.bids, context)?;
    let asks = levels(wire.asks, context)?;
    if bids
        .as_slice()
        .iter()
        .chain(asks.as_slice())
        .any(|l| l.quantity.0 == 0)
        || bids.as_slice().windows(2).any(|w| w[0].price <= w[1].price)
        || asks.as_slice().windows(2).any(|w| w[0].price >= w[1].price)
        || bids
            .as_slice()
            .first()
            .zip(asks.as_slice().first())
            .is_some_and(|(b, a)| b.price >= a.price)
    {
        return Err(Error::InvalidResponse);
    }
    Ok(DepthSnapshot(DecodedDepth {
        key: context.key,
        generation: context.generation,
        bids,
        asks,
        first: wire.last,
        last: wire.last,
        previous: None,
        received: 0,
        event_time_ns: wire
            .event
            .map(|v| context.time_unit.nanoseconds(v))
            .transpose()?,
        transaction_time_ns: wire
            .transaction
            .map(|v| context.time_unit.nanoseconds(v))
            .transpose()?,
    }))
}
pub fn decode_diff<const N: usize>(
    bytes: &[u8],
    context: &DepthContext,
    received_mono_ns: u64,
    max_bytes: usize,
) -> Result<DepthUpdate<N>> {
    check_input::<N>(bytes, max_bytes)?;
    #[derive(Deserialize)]
    struct Envelope<'a> {
        #[serde(borrow)]
        stream: Option<Cow<'a, str>>,
        #[serde(borrow)]
        data: Option<&'a RawValue>,
    }
    let envelope: Envelope<'_> =
        serde_json::from_slice(bytes).map_err(|_| Error::InvalidResponse)?;
    let payload = match (envelope.stream, envelope.data) {
        (Some(stream), Some(data)) if context.stream_matches(&stream) => data.get().as_bytes(),
        (None, None) => bytes,
        _ => return Err(Error::IdentityMismatch),
    };
    #[derive(Deserialize)]
    struct Wire<'a> {
        #[serde(borrow)]
        e: Cow<'a, str>,
        #[serde(borrow)]
        s: Cow<'a, str>,
        #[serde(rename = "E")]
        event: u64,
        #[serde(rename = "T")]
        transaction: Option<u64>,
        #[serde(rename = "U")]
        first: u64,
        #[serde(rename = "u")]
        last: u64,
        pu: Option<u64>,
        st: Option<u64>,
        #[serde(borrow)]
        b: &'a RawValue,
        #[serde(borrow)]
        a: &'a RawValue,
    }
    let wire: Wire<'_> = serde_json::from_slice(payload).map_err(|_| Error::InvalidResponse)?;
    if wire.e != "depthUpdate"
        || wire.s != context.instrument.symbol.as_str()
        || (context.market == Market::Spot && (wire.pu.is_some() || wire.st.is_some()))
        || (context.market == Market::Usdm && wire.st.is_some_and(|st| st != 1))
    {
        return Err(Error::IdentityMismatch);
    }
    if wire.first > wire.last
        || (context.market == Market::Usdm && (wire.pu.is_none() || wire.transaction.is_none()))
    {
        return Err(Error::InvalidResponse);
    }
    Ok(DepthUpdate(DecodedDepth {
        key: context.key,
        generation: context.generation,
        bids: levels(wire.b, context)?,
        asks: levels(wire.a, context)?,
        first: wire.first,
        last: wire.last,
        previous: wire.pu,
        received: received_mono_ns,
        event_time_ns: Some(context.time_unit.nanoseconds(wire.event)?),
        transaction_time_ns: wire
            .transaction
            .map(|v| context.time_unit.nanoseconds(v))
            .transpose()?,
    }))
}
fn check_input<const N: usize>(bytes: &[u8], max: usize) -> Result<()> {
    if N == 0
        || N > MAX_DEPTH_LEVELS
        || max == 0
        || max > MAX_MARKET_MESSAGE_BYTES
        || bytes.len() > max
    {
        return Err(Error::Capacity);
    }
    Ok(())
}
fn venue_decimal(value: tickwire_venue::Decimal) -> Result<Decimal> {
    if value.exponent() <= 0 {
        Decimal::new(value.coefficient(), (-value.exponent()) as u8)
    } else {
        Decimal::new(
            value
                .coefficient()
                .checked_mul(
                    10i128
                        .checked_pow(value.exponent() as u32)
                        .ok_or(Error::Overflow)?,
                )
                .ok_or(Error::Overflow)?,
            0,
        )
    }
}
fn gcd(mut a: u128, mut b: u128) -> u128 {
    while b != 0 {
        (a, b) = (b, a % b);
    }
    a
}
fn units(value: Decimal, step: Decimal) -> Result<i64> {
    if value.mantissa() < 0 {
        return Err(Error::InvalidDecimal);
    }
    let mut n = value.mantissa() as u128;
    let mut d = step.mantissa() as u128;
    let common = gcd(n, d);
    n /= common;
    d /= common;
    for _ in value.scale()..step.scale() {
        let common = gcd(d, 10);
        d /= common;
        n = n.checked_mul(10 / common).ok_or(Error::Overflow)?;
    }
    for _ in step.scale()..value.scale() {
        let common = gcd(n, 10);
        n /= common;
        d = d.checked_mul(10 / common).ok_or(Error::Overflow)?;
    }
    if d != 1 {
        return Err(Error::InvalidDecimal);
    }
    i64::try_from(n).map_err(|_| Error::Overflow)
}
fn levels<const N: usize>(raw: &RawValue, context: &DepthContext) -> Result<Levels<N>> {
    struct Seed<'a, const N: usize> {
        ctx: &'a DepthContext,
        error: &'a mut Option<Error>,
    }
    impl<'de, const N: usize> DeserializeSeed<'de> for Seed<'_, N> {
        type Value = Levels<N>;
        fn deserialize<D: serde::Deserializer<'de>>(
            self,
            d: D,
        ) -> std::result::Result<Self::Value, D::Error> {
            d.deserialize_seq(self)
        }
    }
    impl<'de, const N: usize> Visitor<'de> for Seed<'_, N> {
        type Value = Levels<N>;
        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("bounded price/quantity pairs")
        }
        fn visit_seq<A: SeqAccess<'de>>(
            self,
            mut seq: A,
        ) -> std::result::Result<Self::Value, A::Error> {
            let mut out = Levels {
                values: [EMPTY_LEVEL; N],
                len: 0,
            };
            while let Some([price, qty]) = seq.next_element::<[Decimal; 2]>()? {
                let result = (|| {
                    if out.len == N {
                        return Err(Error::Capacity);
                    }
                    let price = units(price, self.ctx.price_tick)?;
                    let quantity = units(qty, self.ctx.quantity_step)?;
                    if price <= 0 {
                        return Err(Error::InvalidDecimal);
                    }
                    Ok(Level {
                        price: PriceTicks(price),
                        quantity: QtyLots(quantity),
                    })
                })();
                match result {
                    Ok(level) => {
                        out.values[out.len] = level;
                        out.len += 1;
                    }
                    Err(e) => {
                        *self.error = Some(e);
                        return Err(serde::de::Error::custom("invalid depth"));
                    }
                }
            }
            Ok(out)
        }
    }
    let mut error = None;
    let mut decoder = serde_json::Deserializer::from_str(raw.get());
    let decoded = Seed {
        ctx: context,
        error: &mut error,
    }
    .deserialize(&mut decoder);
    decoded.map_err(|_| error.unwrap_or(Error::InvalidResponse))
}
