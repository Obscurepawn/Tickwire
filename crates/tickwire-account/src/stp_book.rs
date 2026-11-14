use std::sync::atomic::{AtomicI64, Ordering};

use tickwire_risk::{Fixed, Rounding};
use tickwire_venue as v;

use crate::{Error, OrderKey, Result};

pub const DEFAULT_STP_SYMBOLS: usize = 64;
pub const DEFAULT_STP_ORDERS_PER_SIDE: usize = 128;

const EMPTY_BUCKET: u32 = u32::MAX;

#[repr(align(64))]
#[derive(Debug)]
struct AtomicBestCell {
    ticks: AtomicI64,
}

impl AtomicBestCell {
    #[inline]
    fn new(init: i64) -> Self {
        Self {
            ticks: AtomicI64::new(init),
        }
    }

    #[inline]
    fn load(&self) -> i64 {
        self.ticks.load(Ordering::Acquire)
    }

    #[inline]
    fn store(&self, val: i64) {
        self.ticks.store(val, Ordering::Release);
    }
}

impl Clone for AtomicBestCell {
    fn clone(&self) -> Self {
        Self::new(self.load())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct OrderSlot {
    key: OrderKey,
    resting_ticks: i64,
    pending_ticks: Option<i64>,
}

impl OrderSlot {
    #[inline]
    fn aggressive_ticks(&self, side: v::Side) -> i64 {
        aggressive_ticks(side, self.resting_ticks, self.pending_ticks)
    }
}

#[derive(Debug, Clone)]
struct SymbolSlot {
    instrument_id: u64,
    tick_size: Fixed,
    best_bid: AtomicBestCell,
    best_ask: AtomicBestCell,
    bids: Vec<OrderSlot>,
    asks: Vec<OrderSlot>,
}

impl SymbolSlot {
    fn new(instrument_id: u64, tick_size: Fixed, max_orders_per_side: usize) -> Self {
        Self {
            instrument_id,
            tick_size,
            best_bid: AtomicBestCell::new(i64::MIN),
            best_ask: AtomicBestCell::new(i64::MAX),
            bids: Vec::with_capacity(max_orders_per_side),
            asks: Vec::with_capacity(max_orders_per_side),
        }
    }

    fn recompute_best_bid(&self) {
        let best = self
            .bids
            .iter()
            .map(|s| s.aggressive_ticks(v::Side::Buy))
            .max()
            .unwrap_or(i64::MIN);
        self.best_bid.store(best);
    }

    fn recompute_best_ask(&self) {
        let best = self
            .asks
            .iter()
            .map(|s| s.aggressive_ticks(v::Side::Sell))
            .min()
            .unwrap_or(i64::MAX);
        self.best_ask.store(best);
    }
}

/// Preallocated per-symbol self-trade-prevention order book with cache-line-aligned
/// atomic best-bid and best-ask integer tick cells.
#[derive(Debug, Clone)]
pub struct StpBook {
    buckets: Vec<u32>,
    symbols: Vec<SymbolSlot>,
    max_orders_per_side: usize,
}

impl Default for StpBook {
    fn default() -> Self {
        Self::new()
    }
}

impl StpBook {
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_STP_SYMBOLS, DEFAULT_STP_ORDERS_PER_SIDE)
    }

    pub fn with_capacity(max_symbols: usize, max_orders_per_side: usize) -> Self {
        let sym_cap = max_symbols.max(1);
        let bucket_cap = sym_cap
            .saturating_mul(4)
            .max(16)
            .checked_next_power_of_two()
            .unwrap_or(256);
        Self {
            buckets: vec![EMPTY_BUCKET; bucket_cap],
            symbols: Vec::with_capacity(sym_cap),
            max_orders_per_side: max_orders_per_side.max(1),
        }
    }

    pub fn register_instrument(&mut self, instrument_id: u64, tick_size: Fixed) -> Result<()> {
        if instrument_id == 0 {
            return Err(Error::Catalog);
        }
        if tick_size.atoms() <= 0 {
            return Err(Error::InvalidPrice);
        }
        if let Some(idx) = self.find_slot_index(instrument_id) {
            let existing = &self.symbols[idx];
            let same = same_value(existing.tick_size, tick_size)?;
            if !same && (!existing.bids.is_empty() || !existing.asks.is_empty()) {
                return Err(Error::Catalog);
            }
            self.symbols[idx].tick_size = tick_size;
            return Ok(());
        }
        if (self.symbols.len() + 1).saturating_mul(2) >= self.buckets.len() {
            self.rehash(self.buckets.len().saturating_mul(2));
        }
        let idx = self.symbols.len();
        let slot_idx = u32::try_from(idx).map_err(|_| Error::Capacity)?;
        self.symbols.push(SymbolSlot::new(
            instrument_id,
            tick_size,
            self.max_orders_per_side,
        ));
        let mask = self.buckets.len() - 1;
        let mut probe = hash_instrument(instrument_id) & mask;
        loop {
            if self.buckets[probe] == EMPTY_BUCKET {
                self.buckets[probe] = slot_idx;
                break;
            }
            probe = (probe + 1) & mask;
        }
        Ok(())
    }

    #[inline]
    pub fn best_bid_ticks(&self, instrument_id: u64) -> Result<Option<i64>> {
        let slot = self.slot(instrument_id)?;
        let bid = slot.best_bid.load();
        Ok((bid != i64::MIN).then_some(bid))
    }

    #[inline]
    pub fn best_ask_ticks(&self, instrument_id: u64) -> Result<Option<i64>> {
        let slot = self.slot(instrument_id)?;
        let ask = slot.best_ask.load();
        Ok((ask != i64::MAX).then_some(ask))
    }

    /// Lock-free single-atomic-load self-cross check against resting and pending opposite-side orders.
    #[inline]
    pub fn self_cross(&self, instrument_id: u64, side: v::Side, price: Fixed) -> Result<bool> {
        let slot = self.slot(instrument_id)?;
        let candidate = price_to_ticks(price, slot.tick_size)?;
        Ok(match side {
            v::Side::Buy => {
                let ask = slot.best_ask.load();
                ask != i64::MAX && candidate >= ask
            }
            v::Side::Sell => {
                let bid = slot.best_bid.load();
                bid != i64::MIN && candidate <= bid
            }
        })
    }

    /// Checks whether `candidate_price` on `side` crosses any resting or pending order on the
    /// opposite side excluding `exclude`.
    pub fn self_cross_excluding(
        &self,
        instrument_id: u64,
        side: v::Side,
        candidate_price: Fixed,
        exclude: &OrderKey,
    ) -> Result<bool> {
        let slot = self.slot(instrument_id)?;
        let candidate = price_to_ticks(candidate_price, slot.tick_size)?;
        match side {
            v::Side::Buy => {
                let ask = slot.best_ask.load();
                if ask == i64::MAX || candidate < ask {
                    return Ok(false);
                }
                Ok(slot
                    .asks
                    .iter()
                    .any(|s| &s.key != exclude && candidate >= s.aggressive_ticks(v::Side::Sell)))
            }
            v::Side::Sell => {
                let bid = slot.best_bid.load();
                if bid == i64::MIN || candidate > bid {
                    return Ok(false);
                }
                Ok(slot
                    .bids
                    .iter()
                    .any(|s| &s.key != exclude && candidate <= s.aggressive_ticks(v::Side::Buy)))
            }
        }
    }

    /// Writes crossing opposite-side `OrderKey`s into `out` without heap allocation, returning
    /// the number of keys written (at most `out.len()`).
    pub fn find_crossing_orders(
        &self,
        instrument_id: u64,
        side: v::Side,
        price: Fixed,
        out: &mut [OrderKey],
    ) -> Result<usize> {
        let slot = self.slot(instrument_id)?;
        let candidate = price_to_ticks(price, slot.tick_size)?;
        if out.is_empty() {
            return Ok(0);
        }
        let mut written = 0usize;
        match side {
            v::Side::Buy => {
                let ask = slot.best_ask.load();
                if ask == i64::MAX || candidate < ask {
                    return Ok(0);
                }
                for entry in &slot.asks {
                    if candidate >= entry.aggressive_ticks(v::Side::Sell) {
                        out[written] = entry.key.clone();
                        written += 1;
                        if written == out.len() {
                            break;
                        }
                    }
                }
            }
            v::Side::Sell => {
                let bid = slot.best_bid.load();
                if bid == i64::MIN || candidate > bid {
                    return Ok(0);
                }
                for entry in &slot.bids {
                    if candidate <= entry.aggressive_ticks(v::Side::Buy) {
                        out[written] = entry.key.clone();
                        written += 1;
                        if written == out.len() {
                            break;
                        }
                    }
                }
            }
        }
        Ok(written)
    }

    pub(crate) fn tick_size(&self, instrument_id: u64) -> Result<Fixed> {
        Ok(self.slot(instrument_id)?.tick_size)
    }

    pub(crate) fn upsert_order(
        &mut self,
        instrument_id: u64,
        side: v::Side,
        key: OrderKey,
        resting_price: Fixed,
        pending_price: Option<Fixed>,
    ) -> Result<()> {
        let idx = self.find_slot_index(instrument_id).ok_or(Error::Catalog)?;
        let tick_size = self.symbols[idx].tick_size;
        let resting_ticks = price_to_ticks(resting_price, tick_size)?;
        let pending_ticks = match pending_price {
            Some(p) => Some(price_to_ticks(p, tick_size)?),
            None => None,
        };
        let new_agg = aggressive_ticks(side, resting_ticks, pending_ticks);
        let slot = &mut self.symbols[idx];
        match side {
            v::Side::Buy => {
                let cur_best = slot.best_bid.load();
                if let Some(existing) = slot.bids.iter_mut().find(|s| s.key == key) {
                    let old_agg = existing.aggressive_ticks(v::Side::Buy);
                    existing.resting_ticks = resting_ticks;
                    existing.pending_ticks = pending_ticks;
                    if new_agg >= cur_best {
                        slot.best_bid.store(new_agg);
                    } else if old_agg == cur_best && new_agg < old_agg {
                        slot.recompute_best_bid();
                    }
                } else {
                    slot.bids.push(OrderSlot {
                        key,
                        resting_ticks,
                        pending_ticks,
                    });
                    if new_agg > cur_best {
                        slot.best_bid.store(new_agg);
                    }
                }
            }
            v::Side::Sell => {
                let cur_best = slot.best_ask.load();
                if let Some(existing) = slot.asks.iter_mut().find(|s| s.key == key) {
                    let old_agg = existing.aggressive_ticks(v::Side::Sell);
                    existing.resting_ticks = resting_ticks;
                    existing.pending_ticks = pending_ticks;
                    if new_agg <= cur_best {
                        slot.best_ask.store(new_agg);
                    } else if old_agg == cur_best && new_agg > old_agg {
                        slot.recompute_best_ask();
                    }
                } else {
                    slot.asks.push(OrderSlot {
                        key,
                        resting_ticks,
                        pending_ticks,
                    });
                    if new_agg < cur_best {
                        slot.best_ask.store(new_agg);
                    }
                }
            }
        }
        Ok(())
    }

    pub(crate) fn remove_order(
        &mut self,
        instrument_id: u64,
        side: v::Side,
        key: &OrderKey,
    ) -> Result<()> {
        let Some(idx) = self.find_slot_index(instrument_id) else {
            return Ok(());
        };
        let slot = &mut self.symbols[idx];
        match side {
            v::Side::Buy => {
                if let Some(pos) = slot.bids.iter().position(|s| &s.key == key) {
                    let removed = slot.bids.swap_remove(pos);
                    let old_agg = removed.aggressive_ticks(v::Side::Buy);
                    if slot.bids.is_empty() {
                        slot.best_bid.store(i64::MIN);
                    } else if old_agg == slot.best_bid.load() {
                        slot.recompute_best_bid();
                    }
                }
            }
            v::Side::Sell => {
                if let Some(pos) = slot.asks.iter().position(|s| &s.key == key) {
                    let removed = slot.asks.swap_remove(pos);
                    let old_agg = removed.aggressive_ticks(v::Side::Sell);
                    if slot.asks.is_empty() {
                        slot.best_ask.store(i64::MAX);
                    } else if old_agg == slot.best_ask.load() {
                        slot.recompute_best_ask();
                    }
                }
            }
        }
        Ok(())
    }

    #[inline]
    fn slot(&self, instrument_id: u64) -> Result<&SymbolSlot> {
        let idx = self.find_slot_index(instrument_id).ok_or(Error::Catalog)?;
        Ok(&self.symbols[idx])
    }

    #[inline]
    fn find_slot_index(&self, instrument_id: u64) -> Option<usize> {
        if self.buckets.is_empty() {
            return None;
        }
        let mask = self.buckets.len() - 1;
        let mut probe = hash_instrument(instrument_id) & mask;
        for _ in 0..self.buckets.len() {
            let entry = self.buckets[probe];
            if entry == EMPTY_BUCKET {
                return None;
            }
            let idx = entry as usize;
            if self.symbols[idx].instrument_id == instrument_id {
                return Some(idx);
            }
            probe = (probe + 1) & mask;
        }
        None
    }

    fn rehash(&mut self, new_bucket_count: usize) {
        let cap = new_bucket_count.checked_next_power_of_two().unwrap_or(256);
        self.buckets = vec![EMPTY_BUCKET; cap];
        let mask = cap - 1;
        for (idx, slot) in self.symbols.iter().enumerate() {
            let mut probe = hash_instrument(slot.instrument_id) & mask;
            loop {
                if self.buckets[probe] == EMPTY_BUCKET {
                    self.buckets[probe] = idx as u32;
                    break;
                }
                probe = (probe + 1) & mask;
            }
        }
    }
}

/// Dual-price pending-modify aggressiveness rule (`human-docs/3.8`):
/// `Buy` takes `max(resting, pending)`, `Sell` takes `min(resting, pending)`.
#[inline]
pub fn aggressive_ticks(side: v::Side, resting_ticks: i64, pending_ticks: Option<i64>) -> i64 {
    match side {
        v::Side::Buy => pending_ticks.map_or(resting_ticks, |p| resting_ticks.max(p)),
        v::Side::Sell => pending_ticks.map_or(resting_ticks, |p| resting_ticks.min(p)),
    }
}

/// Converts a positive `Fixed` price into exact `i64` integer ticks of `tick_size` without floats.
#[inline]
pub fn price_to_ticks(price: Fixed, tick_size: Fixed) -> Result<i64> {
    if price.atoms() <= 0 || tick_size.atoms() <= 0 {
        return Err(Error::InvalidPrice);
    }
    let scale = price.scale().max(tick_size.scale());
    let p = price
        .rescale(scale, Rounding::Floor)
        .map_err(|_| Error::InvalidPrice)?;
    let t = tick_size
        .rescale(scale, Rounding::Floor)
        .map_err(|_| Error::InvalidPrice)?;
    if t.atoms() <= 0 || p.atoms() <= 0 || p.atoms() % t.atoms() != 0 {
        return Err(Error::InvalidPrice);
    }
    let q = p.atoms() / t.atoms();
    i64::try_from(q).map_err(|_| Error::InvalidPrice)
}

fn same_value(a: Fixed, b: Fixed) -> Result<bool> {
    let scale = a.scale().max(b.scale());
    let aa = a
        .rescale(scale, Rounding::Floor)
        .map_err(|_| Error::InvalidPrice)?;
    let bb = b
        .rescale(scale, Rounding::Floor)
        .map_err(|_| Error::InvalidPrice)?;
    Ok(aa == bb)
}

#[inline]
fn hash_instrument(id: u64) -> usize {
    let mut x = id;
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51_afd7_ed55_8ccd);
    x ^= x >> 33;
    x as usize
}
