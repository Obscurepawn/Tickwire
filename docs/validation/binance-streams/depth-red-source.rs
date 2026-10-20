//! Exact, bounded decoding of Binance public market data.
use crate::{Error, Result, request::Market};
use tickwire_book::{BookKey, DepthEvent, Level, Snapshot};
use tickwire_venue::InstrumentBinding;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeUnit {
    Milliseconds,
    Microseconds,
}
#[derive(Debug, Clone)]
pub struct DepthContext {
    market: Market,
    instrument: InstrumentBinding,
    key: BookKey,
    generation: u64,
    time_unit: TimeUnit,
}
impl DepthContext {
    pub fn new(
        market: Market,
        instrument: InstrumentBinding,
        key: BookKey,
        generation: u64,
        time_unit: TimeUnit,
    ) -> Result<Self> {
        Ok(Self { market, instrument, key, generation, time_unit })
    }
}
#[derive(Debug)]
pub struct DecodedDepth<const N: usize> {
    context: DepthContext,
    bids: Vec<Level>,
    asks: Vec<Level>,
    first: u64,
    last: u64,
    previous: Option<u64>,
    received: u64,
    pub event_time_ns: Option<u64>,
    pub transaction_time_ns: Option<u64>,
}
impl<const N: usize> DecodedDepth<N> {
    pub fn snapshot(&self) -> Snapshot<'_> {
        Snapshot {
            key: self.context.key,
            generation: self.context.generation,
            last_update_id: self.last,
            bids: &self.bids,
            asks: &self.asks,
            bids_complete: false,
            asks_complete: false,
        }
    }
    pub fn event(&self) -> DepthEvent<'_> {
        DepthEvent {
            key: self.context.key,
            generation: self.context.generation,
            first_update_id: self.first,
            last_update_id: self.last,
            previous_update_id: self.previous,
            received_at_ns: self.received,
            bids: &self.bids,
            asks: &self.asks,
        }
    }
}
pub fn decode_snapshot<const N: usize>(
    _bytes: &[u8], _context: &DepthContext, _max_bytes: usize,
) -> Result<DecodedDepth<N>> {
    Err(Error::InvalidResponse)
}
pub fn decode_diff<const N: usize>(
    _bytes: &[u8], _context: &DepthContext, _received_mono_ns: u64, _max_bytes: usize,
) -> Result<DecodedDepth<N>> {
    Err(Error::InvalidResponse)
}
