use crate::{DecimalUnit, Error, Frame, Kind, Result, decimal_to_units};
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Identity {
    pub instrument_id: u64,
    pub instrument_version: u64,
    pub catalog_version: u64,
    pub catalog_hash: [u8; 32],
    pub source_id: u64,
    pub generation: u64,
}
#[derive(Clone, Copy, Debug)]
pub struct ExpectedInstrument<'a> {
    pub identity: Identity,
    pub symbol: &'a str,
    pub price_tick: DecimalUnit,
    pub quantity_step: DecimalUnit,
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Level {
    pub price_ticks: i64,
    pub quantity_lots: i64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DepthKind {
    Diff { first: u64, last: u64 },
    Partial { last: u64 },
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Coverage {
    Unknown,
    Through(i64),
}
#[derive(Debug)]
pub struct DepthUpdate<const N: usize> {
    identity: Identity,
    kind: DepthKind,
    received: u64,
    event: u64,
    bids: [Level; N],
    asks: [Level; N],
    bid_count: usize,
    ask_count: usize,
}
impl<const N: usize> DepthUpdate<N> {
    pub fn identity(&self) -> Identity {
        self.identity
    }
    pub fn kind(&self) -> DepthKind {
        self.kind
    }
    pub fn received_mono_ns(&self) -> u64 {
        self.received
    }
    pub fn event_time_ns(&self) -> u64 {
        self.event
    }
    pub fn bids(&self) -> &[Level] {
        &self.bids[..self.bid_count]
    }
    pub fn asks(&self) -> &[Level] {
        &self.asks[..self.ask_count]
    }
    /// Partial top-20 data never claims complete side coverage.
    pub fn coverage(&self) -> Option<(Coverage, Coverage)> {
        if matches!(self.kind, DepthKind::Diff { .. }) {
            return None;
        }
        let side = |levels: &[Level]| {
            levels
                .last()
                .map_or(Coverage::Unknown, |l| Coverage::Through(l.price_ticks))
        };
        Some((side(self.bids()), side(self.asks())))
    }
}
pub fn normalize_depth<const N: usize>(
    expected: &ExpectedInstrument<'_>,
    frame: &Frame<'_>,
    received_mono_ns: u64,
) -> Result<DepthUpdate<N>> {
    let id = expected.identity;
    if id.instrument_id == 0
        || id.instrument_version == 0
        || id.catalog_version == 0
        || id.catalog_hash == [0; 32]
        || id.source_id == 0
        || id.generation == 0
        || frame.symbol() != expected.symbol
    {
        return Err(Error::Identity);
    }
    if N == 0 || N > 5000 {
        return Err(Error::Capacity);
    }
    let kind = match frame.kind() {
        Kind::DepthDiff => DepthKind::Diff {
            first: frame.first_update_id().ok_or(Error::Kind)?,
            last: frame.last_update_id().ok_or(Error::Kind)?,
        },
        Kind::PartialDepth => DepthKind::Partial {
            last: frame.last_update_id().ok_or(Error::Kind)?,
        },
        _ => return Err(Error::Kind),
    };
    let bids = frame.bids().ok_or(Error::Kind)?;
    let asks = frame.asks().ok_or(Error::Kind)?;
    if bids.len() > N || asks.len() > N {
        return Err(Error::Capacity);
    }
    let mut result = DepthUpdate {
        identity: id,
        kind,
        received: received_mono_ns,
        event: frame.event_time_ns(),
        bids: [Level::default(); N],
        asks: [Level::default(); N],
        bid_count: bids.len(),
        ask_count: asks.len(),
    };
    for (from, to) in [(bids, &mut result.bids), (asks, &mut result.asks)] {
        for (raw, out) in from.iter().zip(to.iter_mut()) {
            *out = Level {
                price_ticks: decimal_to_units(
                    raw.price_mantissa,
                    frame.price_exponent(),
                    expected.price_tick,
                )?,
                quantity_lots: decimal_to_units(
                    raw.quantity_mantissa,
                    frame.quantity_exponent(),
                    expected.quantity_step,
                )?,
            };
            if out.price_ticks <= 0 || out.quantity_lots < 0 {
                return Err(Error::Value);
            }
        }
    }
    if matches!(kind, DepthKind::Partial { .. })
        && (result
            .bids()
            .windows(2)
            .any(|w| w[0].price_ticks <= w[1].price_ticks)
            || result
                .asks()
                .windows(2)
                .any(|w| w[0].price_ticks >= w[1].price_ticks)
            || result
                .bids()
                .first()
                .zip(result.asks().first())
                .is_some_and(|(b, a)| b.price_ticks >= a.price_ticks))
    {
        return Err(Error::Value);
    }
    Ok(result)
}
