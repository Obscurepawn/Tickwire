//! ORS strategy session table (`DopState`): dual-sequence login handshake, `account_ref` routing
//! & authorization, and `(account_ref, strat_id)` enablement masks (`human-docs/3.2`, `11`).

use crate::spinlock::{CachePadded, TtasSpinLock};
use tickwire_venue as v;

/// Maximum concrete strategy slots (`strat_id` `0..=254`; `255 = ALL_ORS_STRATS` is wildcard-only).
pub const MAX_ORS_STRATS: usize = 255;

/// Account reference table size (`0` = default routing sentinel; `1..=255` = concrete accounts).
pub const ACCOUNT_REF_SLOTS: usize = 256;

/// Inline 256-bit mask over `0..=255` indices with zero heap allocation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BitMask256 {
    words: [u64; 4],
}

impl BitMask256 {
    #[inline]
    pub const fn empty() -> Self {
        Self { words: [0; 4] }
    }

    #[inline]
    pub fn insert(&mut self, bit: u8) {
        let idx = (bit >> 6) as usize;
        let shift = bit & 63;
        self.words[idx] |= 1u64 << shift;
    }

    #[inline]
    pub fn remove(&mut self, bit: u8) {
        let idx = (bit >> 6) as usize;
        let shift = bit & 63;
        self.words[idx] &= !(1u64 << shift);
    }

    #[inline]
    pub const fn contains(&self, bit: u8) -> bool {
        let idx = (bit >> 6) as usize;
        let shift = bit & 63;
        (self.words[idx] & (1u64 << shift)) != 0
    }

    #[inline]
    pub fn first_nonzero_bit(&self) -> Option<u8> {
        for (word_idx, &word) in self.words.iter().enumerate() {
            let masked = if word_idx == 0 { word & !1u64 } else { word };
            if masked != 0 {
                let bit = (word_idx as u8) * 64 + (masked.trailing_zeros() as u8);
                return Some(bit);
            }
        }
        None
    }
}

/// Login handshake reply returned by [`DopState::login`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DopLoginAck {
    pub strat_id: u8,
    pub next_order_seqnum: u64,
    pub default_account_ref: u8,
}

#[derive(Debug, Clone, Copy)]
struct StratEntry {
    configured: bool,
    logged_in: bool,
    /// Persistent across disconnects and seeded by cold REST `tw1...` bootstrap.
    next_order_seqnum: u64,
    /// Reset to `0` on login/logout; first request sets baseline, subsequent require `+1`.
    last_recv_seqnum: u64,
    authorized_accounts: BitMask256,
    default_account_ref: u8,
}

impl Default for StratEntry {
    fn default() -> Self {
        Self {
            configured: false,
            logged_in: false,
            next_order_seqnum: 1,
            last_recv_seqnum: 0,
            authorized_accounts: BitMask256::empty(),
            default_account_ref: 0,
        }
    }
}

/// Per-strategy and per-account ORS session and authorization table (`DopState`).
#[derive(Debug)]
pub struct DopState {
    registered_accounts: BitMask256,
    strats: Box<[CachePadded<TtasSpinLock<StratEntry>>]>,
    /// Per-`account_ref` (`1..=255`) enabled strategy mask (`strat_id` `0..=254`).
    account_enabled_strats: Box<[CachePadded<TtasSpinLock<BitMask256>>]>,
}

impl Default for DopState {
    fn default() -> Self {
        Self::new(&[1])
    }
}

impl DopState {
    /// Creates a `DopState` with the given registered concrete `account_ref`s (`1..=255`).
    ///
    /// By default, all `0..=254` strategies are enabled on every registered `account_ref`; callers
    /// bind per-strategy account authorizations at cold startup via [`Self::configure_strategy`].
    pub fn new(registered_account_refs: &[u8]) -> Self {
        let mut registered_accounts = BitMask256::empty();
        for &acc in registered_account_refs {
            if acc != 0 {
                registered_accounts.insert(acc);
            }
        }
        let mut all_strats_enabled = BitMask256::empty();
        for sid in 0..=254u8 {
            all_strats_enabled.insert(sid);
        }

        let strats = (0..MAX_ORS_STRATS)
            .map(|_| CachePadded::new(TtasSpinLock::new(StratEntry::default())))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let account_enabled_strats = (0..ACCOUNT_REF_SLOTS)
            .map(|idx| {
                let init = if idx > 0 && registered_accounts.contains(idx as u8) {
                    all_strats_enabled
                } else {
                    BitMask256::empty()
                };
                CachePadded::new(TtasSpinLock::new(init))
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();

        Self {
            registered_accounts,
            strats,
            account_enabled_strats,
        }
    }

    /// Cold-startup configuration binding `strat_id` (`0..=254`) to its ordered list of authorized
    /// `account_ref`s (`1..=255`). The first entry becomes the `account_ref == 0` default target.
    pub fn configure_strategy(
        &self,
        strat_id: u8,
        authorized_account_refs: &[u8],
    ) -> Result<(), v::RefusalReason> {
        if strat_id == v::ALL_ORS_STRATS {
            return Err(v::RefusalReason::INVALID_STRAT_ID);
        }
        if authorized_account_refs.is_empty() {
            return Err(v::RefusalReason::INVALID_ACCOUNT_ID);
        }
        let mut mask = BitMask256::empty();
        let mut default_acc = 0u8;
        for (idx, &acc) in authorized_account_refs.iter().enumerate() {
            if acc == 0 || !self.registered_accounts.contains(acc) {
                return Err(v::RefusalReason::INVALID_ACCOUNT_ID);
            }
            if idx == 0 {
                default_acc = acc;
            }
            mask.insert(acc);
        }
        let mut entry = self.strats[strat_id as usize].lock();
        entry.configured = true;
        entry.authorized_accounts = mask;
        entry.default_account_ref = default_acc;
        if entry.next_order_seqnum == 0 {
            entry.next_order_seqnum = 1;
        }
        Ok(())
    }

    /// Returns `true` when `account_ref` (`1..=255`) is registered on this ORS instance.
    #[inline]
    pub const fn is_account_registered(&self, account_ref: u8) -> bool {
        account_ref != 0 && self.registered_accounts.contains(account_ref)
    }

    /// Dual-sequence login handshake (`human-docs/11`):
    /// - Rejects `strat_id == 255`, unconfigured strategies, or duplicate active logins with
    ///   `INVALID_STRAT_ID`.
    /// - Sets `next_order_seqnum = max(login_order_seqnum, next_order_seqnum, 1)`.
    /// - Resets `last_recv_seqnum = 0` so the next request establishes the session baseline.
    pub fn login(
        &self,
        strat_id: u8,
        login_order_seqnum: u64,
    ) -> Result<DopLoginAck, v::RefusalReason> {
        if strat_id == v::ALL_ORS_STRATS {
            return Err(v::RefusalReason::INVALID_STRAT_ID);
        }
        let mut entry = self.strats[strat_id as usize].lock();
        if !entry.configured || entry.logged_in {
            return Err(v::RefusalReason::INVALID_STRAT_ID);
        }
        entry.logged_in = true;
        entry.next_order_seqnum = entry.next_order_seqnum.max(login_order_seqnum).max(1);
        entry.last_recv_seqnum = 0;
        Ok(DopLoginAck {
            strat_id,
            next_order_seqnum: entry.next_order_seqnum,
            default_account_ref: entry.default_account_ref,
        })
    }

    /// Marks `strat_id` logged out and resets `last_recv_seqnum = 0` while preserving
    /// `next_order_seqnum` across disconnects.
    pub fn logout(&self, strat_id: u8) -> Result<(), v::RefusalReason> {
        if strat_id == v::ALL_ORS_STRATS {
            return Err(v::RefusalReason::INVALID_STRAT_ID);
        }
        let mut entry = self.strats[strat_id as usize].lock();
        if !entry.configured {
            return Err(v::RefusalReason::INVALID_STRAT_ID);
        }
        entry.logged_in = false;
        entry.last_recv_seqnum = 0;
        Ok(())
    }

    /// Seeds `next_order_seqnum` from an open order recovered during cold Exchange REST bootstrap.
    pub fn observe_recovered_order(
        &self,
        strat_id: u8,
        recovered_order_seqnum: u64,
    ) -> Result<(), v::RefusalReason> {
        if strat_id == v::ALL_ORS_STRATS {
            return Err(v::RefusalReason::INVALID_STRAT_ID);
        }
        let mut entry = self.strats[strat_id as usize].lock();
        let next = recovered_order_seqnum.saturating_add(1).max(1);
        if next > entry.next_order_seqnum {
            entry.next_order_seqnum = next;
        }
        Ok(())
    }

    /// Reads the current `next_order_seqnum` watermark for `strat_id`.
    pub fn next_order_seqnum(&self, strat_id: u8) -> Option<u64> {
        if strat_id == v::ALL_ORS_STRATS {
            return None;
        }
        Some(self.strats[strat_id as usize].lock().next_order_seqnum)
    }

    /// Reads the current `last_recv_seqnum` transport cursor for `strat_id`.
    pub fn last_recv_seqnum(&self, strat_id: u8) -> Option<u64> {
        if strat_id == v::ALL_ORS_STRATS {
            return None;
        }
        Some(self.strats[strat_id as usize].lock().last_recv_seqnum)
    }

    /// Validates a non-order management request (`Cancel` / `Modify`) from `strat_id`:
    /// verifies login, checks/advances `msg_seqnum` (without advancing on mismatch!), and resolves
    /// `account_ref` (`0` -> strategy's first authorized account).
    pub fn admit_management_request(
        &self,
        strat_id: u8,
        msg_seqnum: u64,
        requested_account_ref: u8,
    ) -> Result<u8, v::RefusalReason> {
        if strat_id == v::ALL_ORS_STRATS {
            return Err(v::RefusalReason::INVALID_STRAT_ID);
        }
        let mut entry = self.strats[strat_id as usize].lock();
        if !entry.configured || !entry.logged_in {
            return Err(v::RefusalReason::INVALID_STRAT_ID);
        }
        Self::check_msg_seqnum(&entry, msg_seqnum)?;
        entry.last_recv_seqnum = msg_seqnum;
        self.resolve_account_locked(&entry, requested_account_ref)
    }

    /// Validates a `NewOrder` request from `strat_id`:
    /// verifies login, checks `msg_seqnum` (refusing `UNEXPECTED_MSG_SEQNUM` without advancing
    /// `last_recv_seqnum`), checks `order_seqnum >= next_order_seqnum`, resolves `account_ref`,
    /// checks `(account_ref, strat_id)` enablement (`RESTRICTED_CANCEL_ONLY`), and advances both
    /// sequence cursors atomically.
    pub fn admit_new_order_request(
        &self,
        strat_id: u8,
        msg_seqnum: u64,
        order_seqnum: u64,
        requested_account_ref: u8,
    ) -> Result<u8, v::RefusalReason> {
        if strat_id == v::ALL_ORS_STRATS {
            return Err(v::RefusalReason::INVALID_STRAT_ID);
        }
        let mut entry = self.strats[strat_id as usize].lock();
        if !entry.configured || !entry.logged_in {
            return Err(v::RefusalReason::INVALID_STRAT_ID);
        }
        Self::check_msg_seqnum(&entry, msg_seqnum)?;
        entry.last_recv_seqnum = msg_seqnum;

        if order_seqnum == 0 || order_seqnum < entry.next_order_seqnum {
            return Err(v::RefusalReason::UNEXPECTED_MSG_SEQNUM);
        }
        let resolved_account = self.resolve_account_locked(&entry, requested_account_ref)?;
        if !self.is_strat_enabled(resolved_account, strat_id) {
            return Err(v::RefusalReason::RESTRICTED_CANCEL_ONLY);
        }
        entry.next_order_seqnum = order_seqnum.saturating_add(1);
        Ok(resolved_account)
    }

    /// Resolves `requested_account_ref` (`0` -> first authorized account) for `strat_id` without
    /// mutating sequence numbers.
    pub fn resolve_account_ref(
        &self,
        strat_id: u8,
        requested_account_ref: u8,
    ) -> Result<u8, v::RefusalReason> {
        if strat_id == v::ALL_ORS_STRATS {
            return Err(v::RefusalReason::INVALID_STRAT_ID);
        }
        let entry = self.strats[strat_id as usize].lock();
        if !entry.configured {
            return Err(v::RefusalReason::INVALID_STRAT_ID);
        }
        self.resolve_account_locked(&entry, requested_account_ref)
    }

    /// Checks whether `(account_ref, strat_id)` is currently enabled for new/risk-increasing
    /// orders.
    #[inline]
    pub fn is_strat_enabled(&self, account_ref: u8, strat_id: u8) -> bool {
        if account_ref == 0
            || strat_id == v::ALL_ORS_STRATS
            || !self.registered_accounts.contains(account_ref)
        {
            return false;
        }
        self.account_enabled_strats[account_ref as usize]
            .lock()
            .contains(strat_id)
    }

    /// Granular `(account_ref, strat_id)` enable/disable toggle (`human-docs/11`):
    /// - `account_ref == 0` applies across all registered accounts; `1..=255` targets one account.
    /// - `strat_id == 255` (`ALL_ORS_STRATS`) applies across all `0..=254` strategies on the
    ///   selected account(s).
    pub fn set_strat_enabled(
        &self,
        account_ref: u8,
        strat_id: u8,
        enabled: bool,
    ) -> Result<(), v::RefusalReason> {
        if account_ref != 0 && !self.registered_accounts.contains(account_ref) {
            return Err(v::RefusalReason::INVALID_ACCOUNT_ID);
        }
        if account_ref == 0 {
            for acc in 1..=255u8 {
                if self.registered_accounts.contains(acc) {
                    self.apply_enabled_on_account(acc, strat_id, enabled);
                }
            }
        } else {
            self.apply_enabled_on_account(account_ref, strat_id, enabled);
        }
        Ok(())
    }

    #[inline]
    fn apply_enabled_on_account(&self, account_ref: u8, strat_id: u8, enabled: bool) {
        let mut mask = self.account_enabled_strats[account_ref as usize].lock();
        if strat_id == v::ALL_ORS_STRATS {
            for sid in 0..=254u8 {
                if enabled {
                    mask.insert(sid);
                } else {
                    mask.remove(sid);
                }
            }
        } else if enabled {
            mask.insert(strat_id);
        } else {
            mask.remove(strat_id);
        }
    }

    #[inline]
    fn check_msg_seqnum(entry: &StratEntry, msg_seqnum: u64) -> Result<(), v::RefusalReason> {
        if msg_seqnum == 0 {
            return Err(v::RefusalReason::UNEXPECTED_MSG_SEQNUM);
        }
        if entry.last_recv_seqnum != 0 {
            let expected = entry
                .last_recv_seqnum
                .checked_add(1)
                .ok_or(v::RefusalReason::UNEXPECTED_MSG_SEQNUM)?;
            if msg_seqnum != expected {
                return Err(v::RefusalReason::UNEXPECTED_MSG_SEQNUM);
            }
        }
        Ok(())
    }

    #[inline]
    fn resolve_account_locked(
        &self,
        entry: &StratEntry,
        requested_account_ref: u8,
    ) -> Result<u8, v::RefusalReason> {
        if requested_account_ref == 0 {
            if entry.default_account_ref == 0 {
                return Err(v::RefusalReason::INVALID_ACCOUNT_ID);
            }
            return Ok(entry.default_account_ref);
        }
        if !self.registered_accounts.contains(requested_account_ref) {
            return Err(v::RefusalReason::INVALID_ACCOUNT_ID);
        }
        if !entry.authorized_accounts.contains(requested_account_ref) {
            return Err(v::RefusalReason::REJECT_ROUTING_ID_MISMATCH);
        }
        Ok(requested_account_ref)
    }
}
