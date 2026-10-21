use crate::*;
pub const EXACT_MARKET_EVENTS: u64 = 1 << 9;
impl DecimalWire {
    pub fn new(mut coefficient: i128, mut exponent: i8) -> Result<Self> {
        if !(-38..=38).contains(&exponent) {
            return Err(Error::Bounds);
        }
        if coefficient == 0 {
            exponent = 0;
        }
        while coefficient != 0 && coefficient % 10 == 0 && exponent < 38 {
            coefficient /= 10;
            exponent += 1;
        }
        Ok(Self {
            coefficient: coefficient.to_le_bytes(),
            exponent,
        })
    }
    pub fn coefficient_value(&self) -> i128 {
        i128::from_le_bytes(self.coefficient)
    }
    pub fn validate(&self) -> Result<()> {
        if Self::new(self.coefficient_value(), self.exponent)? != *self {
            return Err(Error::Malformed);
        }
        Ok(())
    }
}
pub(crate) fn validate_exact(r: &MarketExactRecord) -> Result<()> {
    let h = &r.header;
    let b = &r.body;
    if h.feed_generation == 0
        || h.instrument_id == 0
        || h.instrument_version == 0
        || b.source_id == 0
        || b.source_connection_epoch == 0
    {
        return Err(Error::Identity);
    }
    seq(b.source_connection_epoch)?;
    if !matches!(
        (
            b.source_encoding,
            b.source_schema_id,
            b.source_schema_version
        ),
        (1, 0, 0) | (2, 1, 0)
    ) {
        return Err(Error::UnsupportedFeature);
    }
    if !(1..=5).contains(&b.feed_state) {
        return Err(Error::InvalidEnum);
    }
    let mask = match h.message_type {
        264 => 0x87,
        265 => 3,
        266 => 0x79,
        _ => return Err(Error::UnknownMessage),
    };
    if b.value_validity & !mask != 0 {
        return Err(Error::Malformed);
    }
    for (bit, zero) in [
        (0, b.exchange_event_ns == 0),
        (1, b.transaction_time_ns == 0),
        (2, b.normal_quantity_lots == 0),
        (3, b.estimated_settlement_price == DecimalWire::default()),
        (4, b.moving_average == DecimalWire::default()),
        (5, b.funding_rate == DecimalWire::default()),
        (6, b.next_funding_utc_ns == 0),
    ] {
        if b.value_validity & (1 << bit) == 0 && !zero {
            return Err(Error::Malformed);
        }
    }
    if (h.flags & 1 != 0) != (b.value_validity & 1 != 0)
        || h.source_timestamp_ns != b.exchange_event_ns
    {
        return Err(Error::Malformed);
    }
    match h.message_type {
        264 => {
            let maker = b.trade_flags & 1 != 0;
            let aggregate = b.trade_flags & 2 != 0;
            if b.trade_flags & !3 != 0
                || b.price_ticks <= 0
                || b.quantity_lots <= 0
                || b.normal_quantity_lots < 0
                || b.normal_quantity_lots > b.quantity_lots
                || b.side != if maker { 2 } else { 1 }
                || aggregate != (b.value_validity & 128 != 0)
                || (!aggregate
                    && (b.venue_seq_first != 0
                        || b.venue_seq_last != 0
                        || b.value_validity & 4 != 0))
                || (aggregate && b.venue_seq_first > b.venue_seq_last)
            {
                return Err(Error::Malformed);
            }
        }
        265 => {
            if b.venue_seq_first != b.venue_seq_last
                || b.bid_price_ticks <= 0
                || b.ask_price_ticks <= b.bid_price_ticks
                || b.bid_quantity_lots < 0
                || b.ask_quantity_lots < 0
            {
                return Err(Error::Malformed);
            }
        }
        266 => {
            for d in [
                &b.mark_price,
                &b.index_price,
                &b.estimated_settlement_price,
                &b.moving_average,
                &b.funding_rate,
            ] {
                d.validate()?;
            }
            if b.mark_price.coefficient_value() <= 0
                || b.index_price.coefficient_value() <= 0
                || b.estimated_settlement_price.coefficient_value() < 0
                || b.moving_average.coefficient_value() < 0
            {
                return Err(Error::Malformed);
            }
        }
        _ => unreachable!(),
    }
    Ok(())
}
/// A snapshot state may use a legacy body or the exact-value extension. Both
/// occupy the same 512-byte record and keep the original header and timestamps.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MarketState {
    Legacy(MarketRecord),
    Exact(MarketExactRecord),
}
impl From<MarketRecord> for MarketState {
    fn from(r: MarketRecord) -> Self {
        Self::Legacy(r)
    }
}
impl From<MarketExactRecord> for MarketState {
    fn from(r: MarketExactRecord) -> Self {
        Self::Exact(r)
    }
}
pub trait MarketStateRecord: Wire + Clone + std::fmt::Debug {
    fn header(&self) -> &RecordHeader;
    fn header_mut(&mut self) -> &mut RecordHeader;
    fn feed_state(&self) -> u16;
}
impl MarketStateRecord for MarketRecord {
    fn header(&self) -> &RecordHeader {
        &self.header
    }
    fn header_mut(&mut self) -> &mut RecordHeader {
        &mut self.header
    }
    fn feed_state(&self) -> u16 {
        self.body.feed_state
    }
}
impl MarketStateRecord for MarketExactRecord {
    fn header(&self) -> &RecordHeader {
        &self.header
    }
    fn header_mut(&mut self) -> &mut RecordHeader {
        &mut self.header
    }
    fn feed_state(&self) -> u16 {
        self.body.feed_state
    }
}
impl MarketStateRecord for MarketState {
    fn header(&self) -> &RecordHeader {
        match self {
            Self::Legacy(r) => &r.header,
            Self::Exact(r) => &r.header,
        }
    }
    fn header_mut(&mut self) -> &mut RecordHeader {
        match self {
            Self::Legacy(r) => &mut r.header,
            Self::Exact(r) => &mut r.header,
        }
    }
    fn feed_state(&self) -> u16 {
        match self {
            Self::Legacy(r) => r.body.feed_state,
            Self::Exact(r) => r.body.feed_state,
        }
    }
}
impl Wire for MarketState {
    const SIZE: usize = 512;
    fn decode(raw: &[u8]) -> Result<Self> {
        match decode_record(raw)? {
            Record::Market(r) => Ok(Self::Legacy(r)),
            Record::MarketExact(r) => Ok(Self::Exact(r)),
            _ => Err(Error::UnknownMessage),
        }
    }
    fn encode(&self, out: &mut [u8]) -> Result<()> {
        match self {
            Self::Legacy(r) => r.encode(out),
            Self::Exact(r) => r.encode(out),
        }
    }
}
/// The legacy/exact forms satisfy the same registered category, never two states.
pub fn market_state_category(kind: u16) -> Result<u8> {
    match kind {
        257 | 265 => Ok(1),
        258 | 264 => Ok(2),
        260 | 266 => Ok(4),
        261 => Ok(8),
        _ => Err(Error::UnknownMessage),
    }
}
