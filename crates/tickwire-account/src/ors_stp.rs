//! Lock-free atomic-BBO ORS Self-Trade Prevention book (`OrsStpBook`, `human-docs/2`, `3.6`–`3.8`).
//!
//! - Send-path `self_cross` reads a single per-symbol `AtomicI64` (`best_bid_ticks` or
//!   `best_ask_ticks`) with `Ordering::Acquire` and **zero spinlocks**.
//! - Book mutations (`insert_order`, `stage_pending_modify`, `commit_modify_price`,
//!   `rollback_pending_modify`, `unlink_order`) require [`OrderLockProof`], statically guaranteeing
//!   that a symbol's STP spinlock is acquired **only while already holding** the caller's single
//!   order spinlock (`LockedOrder`).
//! - Zero logging, zero heap allocation, and zero protocol parsing occur under the symbol lock.

use crate::spinlock::{CachePadded, SpinLockGuard, TtasSpinLock};
use core::marker::PhantomData;
use core::sync::atomic::{AtomicI64, Ordering};
use tickwire_venue as v;

/// Sentinel stored in `best_bid_ticks` when no resting buy orders exist on a symbol.
pub const EMPTY_BID_TICKS: i64 = i64::MIN;

/// Sentinel stored in `best_ask_ticks` when no resting sell orders exist on a symbol.
pub const EMPTY_ASK_TICKS: i64 = i64::MAX;

/// Zero-sized compile-time token proving the caller holds a single `LockedOrder` spinlock.
#[derive(Debug, Clone, Copy)]
pub struct OrderLockProof<'a>(PhantomData<&'a ()>);

impl<'a> OrderLockProof<'a> {
    #[inline]
    pub(crate) const fn new() -> Self {
        Self(PhantomData)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct StpOrderEntry {
    ors_order_id: u64,
    slot_idx: u32,
    side: v::Side,
    active_price_ticks: i64,
    /// Dual-price registration during in-flight strategy (`3.8`) or venue (`3.7`) modify.
    pending_price_ticks: Option<i64>,
}

impl Default for StpOrderEntry {
    fn default() -> Self {
        Self {
            ors_order_id: 0,
            slot_idx: 0,
            side: v::Side::Buy,
            active_price_ticks: 0,
            pending_price_ticks: None,
        }
    }
}

impl StpOrderEntry {
    #[inline]
    fn max_bid_contribution(&self) -> i64 {
        match self.pending_price_ticks {
            Some(p) if p > self.active_price_ticks => p,
            _ => self.active_price_ticks,
        }
    }

    #[inline]
    fn min_ask_contribution(&self) -> i64 {
        match self.pending_price_ticks {
            Some(p) if p < self.active_price_ticks => p,
            _ => self.active_price_ticks,
        }
    }

    #[inline]
    fn crosses_incoming(&self, incoming_side: v::Side, incoming_price_ticks: i64) -> bool {
        match (incoming_side, self.side) {
            (v::Side::Buy, v::Side::Sell) => incoming_price_ticks >= self.min_ask_contribution(),
            (v::Side::Sell, v::Side::Buy) => incoming_price_ticks <= self.max_bid_contribution(),
            _ => false,
        }
    }
}

#[derive(Debug)]
struct SymbolBookInner {
    entries: Box<[StpOrderEntry]>,
    len: usize,
}

impl SymbolBookInner {
    fn with_capacity(max_orders_per_symbol: usize) -> Self {
        let cap = max_orders_per_symbol.max(1);
        Self {
            entries: vec![StpOrderEntry::default(); cap].into_boxed_slice(),
            len: 0,
        }
    }

    #[inline]
    fn active_slice(&self) -> &[StpOrderEntry] {
        &self.entries[..self.len]
    }

    #[inline]
    fn find_idx(&self, ors_order_id: u64) -> Option<usize> {
        self.entries[..self.len]
            .iter()
            .position(|e| e.ors_order_id == ors_order_id)
    }

    #[inline]
    fn compute_bbo(&self) -> (i64, i64) {
        let mut best_bid = EMPTY_BID_TICKS;
        let mut best_ask = EMPTY_ASK_TICKS;
        for entry in &self.entries[..self.len] {
            match entry.side {
                v::Side::Buy => {
                    let bid = entry.max_bid_contribution();
                    if bid > best_bid {
                        best_bid = bid;
                    }
                }
                v::Side::Sell => {
                    let ask = entry.min_ask_contribution();
                    if ask < best_ask {
                        best_ask = ask;
                    }
                }
            }
        }
        (best_bid, best_ask)
    }
}

#[derive(Debug)]
struct SymbolStpState {
    best_bid_ticks: AtomicI64,
    best_ask_ticks: AtomicI64,
    book: TtasSpinLock<SymbolBookInner>,
}

impl SymbolStpState {
    fn new(max_orders_per_symbol: usize) -> Self {
        Self {
            best_bid_ticks: AtomicI64::new(EMPTY_BID_TICKS),
            best_ask_ticks: AtomicI64::new(EMPTY_ASK_TICKS),
            book: TtasSpinLock::new(SymbolBookInner::with_capacity(max_orders_per_symbol)),
        }
    }
}

/// Flat per-symbol ORS Self-Trade Prevention book indexed by compact `symbol_id: usize`.
#[derive(Debug)]
pub struct OrsStpBook {
    symbols: Box<[CachePadded<SymbolStpState>]>,
}

impl OrsStpBook {
    /// Preallocates per-symbol STP state for `0..num_symbols` with fixed per-symbol capacity.
    pub fn new(num_symbols: usize, max_orders_per_symbol: usize) -> Self {
        let symbols = (0..num_symbols)
            .map(|_| CachePadded::new(SymbolStpState::new(max_orders_per_symbol)))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self { symbols }
    }

    #[inline]
    pub fn num_symbols(&self) -> usize {
        self.symbols.len()
    }

    /// Lock-free send-path self-trade check (`human-docs/2`, `3.6`): reads a single atomic word on
    /// the opposite side without acquiring any spinlock.
    #[inline]
    pub fn self_cross(&self, symbol_id: usize, side: v::Side, price_ticks: i64) -> bool {
        let Some(sym) = self.symbols.get(symbol_id) else {
            return false;
        };
        match side {
            v::Side::Buy => {
                let best_ask = sym.best_ask_ticks.load(Ordering::Acquire);
                best_ask != EMPTY_ASK_TICKS && price_ticks >= best_ask
            }
            v::Side::Sell => {
                let best_bid = sym.best_bid_ticks.load(Ordering::Acquire);
                best_bid != EMPTY_BID_TICKS && price_ticks <= best_bid
            }
        }
    }

    /// Lock-free read of the published best resting/pending bid price in integer ticks.
    #[inline]
    pub fn best_bid_ticks(&self, symbol_id: usize) -> Option<i64> {
        let val = self
            .symbols
            .get(symbol_id)?
            .best_bid_ticks
            .load(Ordering::Acquire);
        (val != EMPTY_BID_TICKS).then_some(val)
    }

    /// Lock-free read of the published best resting/pending ask price in integer ticks.
    #[inline]
    pub fn best_ask_ticks(&self, symbol_id: usize) -> Option<i64> {
        let val = self
            .symbols
            .get(symbol_id)?
            .best_ask_ticks
            .load(Ordering::Acquire);
        (val != EMPTY_ASK_TICKS).then_some(val)
    }

    /// Acquires the per-symbol STP spinlock while proving the caller already holds its single
    /// `LockedOrder` spinlock (`Order -> StpBook` strict hierarchy).
    #[inline]
    pub fn lock_symbol<'a>(
        &'a self,
        symbol_id: usize,
        _proof: OrderLockProof<'_>,
    ) -> Option<LockedSymbolBook<'a>> {
        let sym = &self.symbols.get(symbol_id)?.0;
        let guard = sym.book.lock();
        Some(LockedSymbolBook { state: sym, guard })
    }
}

/// RAII guard for a single symbol's `OrsStpBook` spinlock, acquirable only while holding a single
/// `LockedOrder`.
#[derive(Debug)]
pub struct LockedSymbolBook<'a> {
    state: &'a SymbolStpState,
    guard: SpinLockGuard<'a, SymbolBookInner>,
}

impl LockedSymbolBook<'_> {
    #[inline]
    pub fn resting_count(&self) -> usize {
        self.guard.len
    }

    /// Checks whether modifying `ors_order_id` to `new_price_ticks` would cross any *other* order
    /// resting or pending-modify on the opposite side of this symbol.
    #[inline]
    pub fn self_cross_excluding_order(
        &self,
        ors_order_id: u64,
        side: v::Side,
        new_price_ticks: i64,
    ) -> bool {
        self.guard
            .active_slice()
            .iter()
            .any(|e| e.ors_order_id != ors_order_id && e.crosses_incoming(side, new_price_ticks))
    }

    /// Inserts or updates a resting order in the STP book and republishes atomic BBO.
    #[inline]
    pub fn insert_order(
        &mut self,
        ors_order_id: u64,
        slot_idx: u32,
        side: v::Side,
        price_ticks: i64,
    ) -> Result<(), v::RefusalReason> {
        if let Some(idx) = self.guard.find_idx(ors_order_id) {
            let entry = &mut self.guard.entries[idx];
            entry.slot_idx = slot_idx;
            entry.side = side;
            entry.active_price_ticks = price_ticks;
            entry.pending_price_ticks = None;
            self.publish_bbo();
            return Ok(());
        }
        if self.guard.len >= self.guard.entries.len() {
            return Err(v::RefusalReason::ALLOCATION_ERROR);
        }
        let idx = self.guard.len;
        self.guard.entries[idx] = StpOrderEntry {
            ors_order_id,
            slot_idx,
            side,
            active_price_ticks: price_ticks,
            pending_price_ticks: None,
        };
        self.guard.len = idx + 1;
        self.publish_bbo();
        Ok(())
    }

    /// Registers a dual-price pending modify (`old` + `new` simultaneously active in STP) and
    /// republishes atomic BBO (`human-docs/3.8`).
    #[inline]
    pub fn stage_pending_modify(&mut self, ors_order_id: u64, new_price_ticks: i64) -> bool {
        let Some(idx) = self.guard.find_idx(ors_order_id) else {
            return false;
        };
        self.guard.entries[idx].pending_price_ticks = Some(new_price_ticks);
        self.publish_bbo();
        true
    }

    /// Commits a confirmed strategy (`3.8`) or venue (`3.7`) modify price, collapsing any pending
    /// dual-price entry to `new_price_ticks` and republishing atomic BBO.
    #[inline]
    pub fn commit_modify_price(
        &mut self,
        ors_order_id: u64,
        slot_idx: u32,
        side: v::Side,
        new_price_ticks: i64,
    ) -> Result<(), v::RefusalReason> {
        if let Some(idx) = self.guard.find_idx(ors_order_id) {
            let entry = &mut self.guard.entries[idx];
            entry.active_price_ticks = new_price_ticks;
            entry.pending_price_ticks = None;
            self.publish_bbo();
            return Ok(());
        }
        self.insert_order(ors_order_id, slot_idx, side, new_price_ticks)
    }

    /// Rolls back a rejected pending modify (`3.8`), restoring single-price `active_price_ticks`
    /// and republishing atomic BBO.
    #[inline]
    pub fn rollback_pending_modify(&mut self, ors_order_id: u64) -> bool {
        let Some(idx) = self.guard.find_idx(ors_order_id) else {
            return false;
        };
        self.guard.entries[idx].pending_price_ticks = None;
        self.publish_bbo();
        true
    }

    /// Immediately removes `ors_order_id` from the STP book via O(1) swap-remove and republishes
    /// atomic BBO. Called in the same critical section as full fill, cancel, or reject retirement.
    #[inline]
    pub fn unlink_order(&mut self, ors_order_id: u64) -> bool {
        let Some(idx) = self.guard.find_idx(ors_order_id) else {
            return false;
        };
        let last = self.guard.len - 1;
        if idx != last {
            self.guard.entries[idx] = self.guard.entries[last];
        }
        self.guard.entries[last] = StpOrderEntry::default();
        self.guard.len = last;
        self.publish_bbo();
        true
    }

    #[inline]
    fn publish_bbo(&self) {
        let (best_bid, best_ask) = self.guard.compute_bbo();
        self.state.best_bid_ticks.store(best_bid, Ordering::Release);
        self.state.best_ask_ticks.store(best_ask, Ordering::Release);
    }
}
