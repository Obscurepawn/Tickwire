//! Per-account ORS `OrsQueryDesk` state machine (`human-docs/3.11`, `13.8`).
//!
//! - Schedules `StatusAsk` queries on ambiguous venue errors (`-1007`, `-1000`, `MKT_DELAYED`) or
//!   venue socket disconnects.
//! - Coalesces `Place` + `Cancel` via `cancel_after` on a single per-order entry.
//! - Exponentially backs off `Cancel` retries (`1s..=64s`, capped at `8` attempts).
//! - Evicts **only** non-`Live` entries when the table reaches capacity, never dropping an active
//!   in-doubt `Live` order.

use crate::spinlock::{CachePadded, TtasSpinLock};
use tickwire_venue as v;

/// Maximum `Cancel` retry attempts before exhausting exponential backoff (`human-docs/3.11`).
pub const MAX_CANCEL_BACKOFF_ATTEMPTS: u8 = 8;

/// Default base retry interval (`1s`) used for `StatusAsk` and `Cancel` exponential backoff.
pub const DEFAULT_QUERY_BASE_BACKOFF_NS: u64 = 1_000_000_000;

/// Trigger origin for an [`OrsQueryEntry`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrsQueryOrigin {
    Place,
    Cancel,
    Modify,
    SocketDrop,
}

/// Active per-order reconciliation record inside [`OrsQueryDesk`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrsQueryEntry {
    pub occupied: bool,
    pub ors_order_id: u64,
    pub slot_idx: u32,
    pub strat_id: u8,
    pub account_ref: u8,
    pub symbol_id: u32,
    pub order_seqnum: u64,
    pub origin: OrsQueryOrigin,
    /// Set when a strategy `Cancel` arrives while a `Place`/`SocketDrop` query is already pending.
    pub cancel_after: bool,
    /// Number of `StatusAsk` attempts dispatched so far.
    pub query_attempts: u8,
    /// Number of `Cancel` backoff attempts dispatched (`0..=8`).
    pub cancel_attempts: u8,
    pub cancel_exhausted: bool,
    pub next_due_mono_ns: u64,
}

impl Default for OrsQueryEntry {
    fn default() -> Self {
        Self {
            occupied: false,
            ors_order_id: 0,
            slot_idx: 0,
            strat_id: 0,
            account_ref: 0,
            symbol_id: 0,
            order_seqnum: 0,
            origin: OrsQueryOrigin::Place,
            cancel_after: false,
            query_attempts: 0,
            cancel_attempts: 0,
            cancel_exhausted: false,
            next_due_mono_ns: 0,
        }
    }
}

/// Action emitted by [`OrsQueryDesk::poll_due`] for the cold `QueryDesk` worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DueQueryAction {
    StatusAsk {
        ors_order_id: u64,
        account_ref: u8,
        symbol_id: u32,
        attempt: u8,
        cancel_after: bool,
    },
    RetryCancel {
        ors_order_id: u64,
        account_ref: u8,
        symbol_id: u32,
        attempt: u8,
    },
}

#[derive(Debug)]
struct AccountDeskInner {
    entries: Box<[OrsQueryEntry]>,
    base_backoff_ns: u64,
}

impl AccountDeskInner {
    fn new(capacity: usize, base_backoff_ns: u64) -> Self {
        let cap = capacity.max(1);
        Self {
            entries: vec![OrsQueryEntry::default(); cap].into_boxed_slice(),
            base_backoff_ns: base_backoff_ns.max(1),
        }
    }

    #[inline]
    fn find_idx(&self, ors_order_id: u64) -> Option<usize> {
        self.entries
            .iter()
            .position(|e| e.occupied && e.ors_order_id == ors_order_id)
    }

    #[inline]
    fn find_free_idx(&self) -> Option<usize> {
        self.entries.iter().position(|e| !e.occupied)
    }
}

/// Returns `true` when `(error_code, reason)` indicates an ambiguous send/cancel outcome requiring
/// `OrsQueryDesk` `StatusAsk` reconciliation (`-1007`, `-1000`, `-1001`, `-1006`, `408`, or
/// `MKT_DELAYED`).
#[inline]
pub const fn is_ambiguous_venue_outcome(
    error_code: Option<i64>,
    reason: Option<v::TerminalReason>,
) -> bool {
    if matches!(error_code, Some(-1000 | -1001 | -1006 | -1007 | 408)) {
        return true;
    }
    matches!(reason, Some(v::TerminalReason::MarketDelayed))
}

/// Computes exponential cancel backoff (`base * 2^min(attempt, 6)`, i.e. `1s..=64s` for `1s` base).
#[inline]
pub const fn cancel_backoff_delay_ns(base_backoff_ns: u64, attempt: u8) -> u64 {
    let shift = if attempt > 6 { 6 } else { attempt };
    base_backoff_ns.saturating_mul(1u64 << shift)
}

/// Per-account preallocated `QueryDesk` table (`account_ref` `1..=255`).
#[derive(Debug)]
pub struct OrsQueryDesk {
    accounts: Box<[CachePadded<TtasSpinLock<AccountDeskInner>>]>,
}

impl OrsQueryDesk {
    pub fn new(capacity_per_account: usize, base_backoff_ns: u64) -> Self {
        let accounts = (0..256)
            .map(|_| {
                CachePadded::new(TtasSpinLock::new(AccountDeskInner::new(
                    capacity_per_account,
                    base_backoff_ns,
                )))
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self { accounts }
    }

    /// Schedules or coalesces a `StatusAsk` for `ors_order_id`.
    ///
    /// - If an entry for `ors_order_id` already exists:
    ///   * When `origin == OrsQueryOrigin::Cancel`, coalesces by setting `cancel_after = true`!
    ///   * Moves `next_due_mono_ns` earlier if `due_mono_ns` is sooner.
    /// - If the table is full, runs `is_order_live(slot_idx, ors_order_id)` to evict **only**
    ///   non-`Live` entries before inserting; refuses with `TooManyPendingActions` if all entries
    ///   are still `Live`.
    #[allow(clippy::too_many_arguments)]
    pub fn schedule_status_ask<F>(
        &self,
        account_ref: u8,
        ors_order_id: u64,
        slot_idx: u32,
        strat_id: u8,
        symbol_id: u32,
        order_seqnum: u64,
        origin: OrsQueryOrigin,
        due_mono_ns: u64,
        is_order_live: F,
    ) -> Result<OrsQueryEntry, v::RefusalReason>
    where
        F: Fn(u32, u64) -> bool,
    {
        if account_ref == 0 || ors_order_id == 0 {
            return Err(v::RefusalReason::INVALID_ACCOUNT_ID);
        }
        let mut desk = self.accounts[account_ref as usize].lock();
        if let Some(idx) = desk.find_idx(ors_order_id) {
            let entry = &mut desk.entries[idx];
            if origin == OrsQueryOrigin::Cancel {
                entry.cancel_after = true;
            }
            if due_mono_ns < entry.next_due_mono_ns {
                entry.next_due_mono_ns = due_mono_ns;
            }
            return Ok(*entry);
        }

        let free_idx = match desk.find_free_idx() {
            Some(idx) => idx,
            None => {
                // Full-table cleanup: evict ONLY non-LIVE entries!
                for entry in desk.entries.iter_mut() {
                    if entry.occupied && !is_order_live(entry.slot_idx, entry.ors_order_id) {
                        *entry = OrsQueryEntry::default();
                    }
                }
                desk.find_free_idx()
                    .ok_or(v::RefusalReason::TooManyPendingActions)?
            }
        };

        let entry = OrsQueryEntry {
            occupied: true,
            ors_order_id,
            slot_idx,
            strat_id,
            account_ref,
            symbol_id,
            order_seqnum,
            origin,
            cancel_after: origin == OrsQueryOrigin::Cancel,
            query_attempts: 0,
            cancel_attempts: 0,
            cancel_exhausted: false,
            next_due_mono_ns: due_mono_ns,
        };
        desk.entries[free_idx] = entry;
        Ok(entry)
    }

    /// Coalesces `cancel_after = true` onto an existing pending `Place`/`SocketDrop` entry if one
    /// exists, returning `true` when coalesced so the caller avoids sending a duplicate blind
    /// cancel before status resolution.
    pub fn mark_cancel_after(&self, account_ref: u8, ors_order_id: u64) -> bool {
        if account_ref == 0 {
            return false;
        }
        let mut desk = self.accounts[account_ref as usize].lock();
        let Some(idx) = desk.find_idx(ors_order_id) else {
            return false;
        };
        desk.entries[idx].cancel_after = true;
        true
    }

    /// Schedules an exponential-backoff `Cancel` retry (`1s..=64s` up to `8` attempts).
    /// Returns `Ok(Some(entry))` when scheduled, or `Ok(None)` once all `8` attempts are exhausted.
    #[allow(clippy::too_many_arguments)]
    pub fn schedule_cancel_backoff<F>(
        &self,
        account_ref: u8,
        ors_order_id: u64,
        slot_idx: u32,
        strat_id: u8,
        symbol_id: u32,
        order_seqnum: u64,
        now_mono_ns: u64,
        is_order_live: F,
    ) -> Result<Option<OrsQueryEntry>, v::RefusalReason>
    where
        F: Fn(u32, u64) -> bool,
    {
        if account_ref == 0 || ors_order_id == 0 {
            return Err(v::RefusalReason::INVALID_ACCOUNT_ID);
        }
        let mut desk = self.accounts[account_ref as usize].lock();
        let base_ns = desk.base_backoff_ns;
        if let Some(idx) = desk.find_idx(ors_order_id) {
            let entry = &mut desk.entries[idx];
            entry.origin = OrsQueryOrigin::Cancel;
            entry.cancel_after = true;
            if entry.cancel_attempts >= MAX_CANCEL_BACKOFF_ATTEMPTS {
                entry.cancel_exhausted = true;
                return Ok(None);
            }
            let delay = cancel_backoff_delay_ns(base_ns, entry.cancel_attempts);
            entry.cancel_attempts += 1;
            entry.next_due_mono_ns = now_mono_ns.saturating_add(delay);
            return Ok(Some(*entry));
        }

        let free_idx = match desk.find_free_idx() {
            Some(idx) => idx,
            None => {
                for entry in desk.entries.iter_mut() {
                    if entry.occupied && !is_order_live(entry.slot_idx, entry.ors_order_id) {
                        *entry = OrsQueryEntry::default();
                    }
                }
                desk.find_free_idx()
                    .ok_or(v::RefusalReason::TooManyPendingActions)?
            }
        };

        let delay = cancel_backoff_delay_ns(base_ns, 0);
        let entry = OrsQueryEntry {
            occupied: true,
            ors_order_id,
            slot_idx,
            strat_id,
            account_ref,
            symbol_id,
            order_seqnum,
            origin: OrsQueryOrigin::Cancel,
            cancel_after: true,
            query_attempts: 0,
            cancel_attempts: 1,
            cancel_exhausted: false,
            next_due_mono_ns: now_mono_ns.saturating_add(delay),
        };
        desk.entries[free_idx] = entry;
        Ok(Some(entry))
    }

    /// Polls due `StatusAsk` and `RetryCancel` actions into caller-supplied `out` slice without
    /// heap allocation, pruning any non-`Live` entries encountered along the way.
    pub fn poll_due<F>(
        &self,
        account_ref: u8,
        now_mono_ns: u64,
        out: &mut [DueQueryAction],
        is_order_live: F,
    ) -> usize
    where
        F: Fn(u32, u64) -> bool,
    {
        if account_ref == 0 || out.is_empty() {
            return 0;
        }
        let mut desk = self.accounts[account_ref as usize].lock();
        let base_ns = desk.base_backoff_ns;
        let mut count = 0usize;
        for entry in desk.entries.iter_mut() {
            if count >= out.len() {
                break;
            }
            if !entry.occupied {
                continue;
            }
            if !is_order_live(entry.slot_idx, entry.ors_order_id) {
                *entry = OrsQueryEntry::default();
                continue;
            }
            if entry.cancel_exhausted || now_mono_ns < entry.next_due_mono_ns {
                continue;
            }
            if entry.origin == OrsQueryOrigin::Cancel && entry.cancel_attempts > 0 {
                let attempt = entry.cancel_attempts;
                out[count] = DueQueryAction::RetryCancel {
                    ors_order_id: entry.ors_order_id,
                    account_ref: entry.account_ref,
                    symbol_id: entry.symbol_id,
                    attempt,
                };
                count += 1;
                if entry.cancel_attempts >= MAX_CANCEL_BACKOFF_ATTEMPTS {
                    entry.cancel_exhausted = true;
                } else {
                    let delay = cancel_backoff_delay_ns(base_ns, entry.cancel_attempts);
                    entry.cancel_attempts += 1;
                    entry.next_due_mono_ns = now_mono_ns.saturating_add(delay);
                }
            } else {
                entry.query_attempts = entry.query_attempts.saturating_add(1);
                out[count] = DueQueryAction::StatusAsk {
                    ors_order_id: entry.ors_order_id,
                    account_ref: entry.account_ref,
                    symbol_id: entry.symbol_id,
                    attempt: entry.query_attempts,
                    cancel_after: entry.cancel_after,
                };
                count += 1;
                entry.next_due_mono_ns = now_mono_ns.saturating_add(base_ns);
            }
        }
        count
    }

    /// Removes a resolved entry from `account_ref`'s query desk, returning the removed entry.
    pub fn complete_order(&self, account_ref: u8, ors_order_id: u64) -> Option<OrsQueryEntry> {
        if account_ref == 0 {
            return None;
        }
        let mut desk = self.accounts[account_ref as usize].lock();
        let idx = desk.find_idx(ors_order_id)?;
        let removed = desk.entries[idx];
        desk.entries[idx] = OrsQueryEntry::default();
        Some(removed)
    }

    /// Sweeps all non-`Live` entries from `account_ref`'s query desk, returning evicted count.
    pub fn cleanup_non_live<F>(&self, account_ref: u8, is_order_live: F) -> usize
    where
        F: Fn(u32, u64) -> bool,
    {
        if account_ref == 0 {
            return 0;
        }
        let mut desk = self.accounts[account_ref as usize].lock();
        let mut evicted = 0usize;
        for entry in desk.entries.iter_mut() {
            if entry.occupied && !is_order_live(entry.slot_idx, entry.ors_order_id) {
                *entry = OrsQueryEntry::default();
                evicted += 1;
            }
        }
        evicted
    }

    /// Reads a copy of the active `OrsQueryEntry` for `(account_ref, ors_order_id)`.
    pub fn get_entry(&self, account_ref: u8, ors_order_id: u64) -> Option<OrsQueryEntry> {
        if account_ref == 0 {
            return None;
        }
        let desk = self.accounts[account_ref as usize].lock();
        let idx = desk.find_idx(ors_order_id)?;
        Some(desk.entries[idx])
    }

    /// Returns the number of occupied entries on `account_ref`.
    pub fn occupied_count(&self, account_ref: u8) -> usize {
        if account_ref == 0 {
            return 0;
        }
        let desk = self.accounts[account_ref as usize].lock();
        desk.entries.iter().filter(|e| e.occupied).count()
    }
}
