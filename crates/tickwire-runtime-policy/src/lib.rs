//! Stateless, venue-agnostic ORS runtime control policy (`OrsRuntimePolicy` / `ControlPlane`).
//!
//! Enforces process-wide `Mode` (`Trading`, `ManageOnly` / `RestrictedCancelOnly`, `Stopped`),
//! strategy whitelist (`0..=254`), `account_ref = 0` default-account resolution in
//! `enabled_accounts` order, DOP single-session `connected` gating, `active` flags, and lock-free
//! `(account_ref, strat_id)` granular enable/disable state with zero `unsafe`, zero floating-point
//! math, zero owner leases, zero WAL reconciliation digests, and zero SHM market watermarks.
#![forbid(unsafe_code)]

pub mod bootstrap;

pub use bootstrap::{AccountAuthSpec, PolicyConfig, PolicyConfigError};

use core::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use serde::{Deserialize, Serialize};
use tickwire_venue::{ALL_ORS_STRATS, RefusalReason};

/// Maximum valid strategy identifier (`0..=254`; `255` is reserved for [`ALL_ORS_STRATS`]).
pub const MAX_STRAT_ID: u8 = 254;

/// Maximum valid `1`-based account reference (`1..=255`; `0` selects first authorized account).
pub const MAX_ACCOUNT_REF: u8 = 255;

/// Process-wide ORS command admission mode.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    /// Full trading enabled for active, authorized, enabled `(account_ref, strat_id)` pairs.
    #[default]
    Trading = 0,
    /// Restricted cancel-only mode (`OrsRestricted`): refuses new orders, modifies, and RFQ quotes
    /// with [`RefusalReason::RestrictedCancelOnly`] while admitting cancellations and queries.
    ManageOnly = 1,
    /// Process stopped or draining shutdown: refuses new orders, modifies, and strategy cancels
    /// with [`RefusalReason::MarketRejectMarketDown`] while permitting administrative sweeps.
    Stopped = 2,
}

impl Mode {
    /// Alias matching ORS wire `RESTRICTED_CANCEL_ONLY` / `OrsRestricted` terminology.
    #[allow(non_upper_case_globals)]
    pub const RestrictedCancelOnly: Self = Self::ManageOnly;

    #[inline]
    const fn from_u8(raw: u8) -> Self {
        match raw {
            0 => Self::Trading,
            1 => Self::ManageOnly,
            _ => Self::Stopped,
        }
    }

    #[inline]
    pub const fn is_trading(self) -> bool {
        matches!(self, Self::Trading)
    }

    #[inline]
    pub const fn is_restricted(self) -> bool {
        matches!(self, Self::ManageOnly)
    }

    #[inline]
    pub const fn is_stopped(self) -> bool {
        matches!(self, Self::Stopped)
    }
}

/// Command category evaluated by [`OrsRuntimePolicy::evaluate`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PolicyAction {
    /// Risk-creating `NewOrder` intent.
    NewOrder,
    /// In-place `Modify` / `AmendKeepPriority` / `CancelReplace` intent.
    Modify,
    /// Strategy-originated `Cancel` or `CancelAll` intent.
    Cancel,
    /// Read-only order status query intent.
    Query,
    /// Risk-creating `NewRfqQuote` or `ModifyRfqQuote` intent.
    RfqQuote,
    /// Strategy-originated `CancelRfqQuote` or `CancelAllRfqQuotes` intent.
    CancelRfqQuote,
    /// Operator, logout, disablement, or graceful-shutdown cancel sweep (`strat_id` may be `255`).
    AdminCancel,
}

impl PolicyAction {
    #[inline]
    pub const fn is_risk_increasing(self) -> bool {
        matches!(self, Self::NewOrder | Self::Modify | Self::RfqQuote)
    }

    #[inline]
    pub const fn is_cancel_or_query(self) -> bool {
        matches!(
            self,
            Self::Cancel | Self::Query | Self::CancelRfqQuote | Self::AdminCancel
        )
    }
}

/// Four `u64` words covering all 256 byte values (`0..=255`).
#[derive(Debug)]
struct AtomicStratBitset {
    words: [AtomicU64; 4],
}

impl Default for AtomicStratBitset {
    fn default() -> Self {
        Self::new_empty()
    }
}

impl AtomicStratBitset {
    const fn new_empty() -> Self {
        Self {
            words: [
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
            ],
        }
    }

    fn from_words(init: [u64; 4]) -> Self {
        Self {
            words: [
                AtomicU64::new(init[0]),
                AtomicU64::new(init[1]),
                AtomicU64::new(init[2]),
                AtomicU64::new(init[3]),
            ],
        }
    }

    #[inline]
    fn contains(&self, id: u8) -> bool {
        let word_idx = usize::from(id >> 6);
        let bit = 1u64 << (id & 63);
        (self.words[word_idx].load(Ordering::Acquire) & bit) != 0
    }

    /// Sets or clears `id` and returns the previous boolean value.
    #[inline]
    fn set(&self, id: u8, enabled: bool) -> bool {
        let word_idx = usize::from(id >> 6);
        let bit = 1u64 << (id & 63);
        let prev = if enabled {
            self.words[word_idx].fetch_or(bit, Ordering::AcqRel)
        } else {
            self.words[word_idx].fetch_and(!bit, Ordering::AcqRel)
        };
        (prev & bit) != 0
    }

    /// Atomically sets `id` iff currently `false`, returning `true` when acquired.
    #[inline]
    fn try_claim(&self, id: u8) -> bool {
        let word_idx = usize::from(id >> 6);
        let bit = 1u64 << (id & 63);
        let prev = self.words[word_idx].fetch_or(bit, Ordering::AcqRel);
        (prev & bit) == 0
    }
}

#[inline]
fn bit_in_words(words: &[u64; 4], id: u8) -> bool {
    let word_idx = usize::from(id >> 6);
    let bit = 1u64 << (id & 63);
    (words[word_idx] & bit) != 0
}

#[inline]
fn set_bit_in_words(words: &mut [u64; 4], id: u8) {
    let word_idx = usize::from(id >> 6);
    words[word_idx] |= 1u64 << (id & 63);
}

/// Lock-free, thread-safe (`Send + Sync`) stateless ORS control policy.
#[derive(Debug)]
pub struct OrsRuntimePolicy {
    mode: AtomicU8,
    configured_strats: [u64; 4],
    account_present: [bool; 256],
    enabled_account_order: Vec<u8>,
    account_authorized: Box<[[u64; 4]; 256]>,
    default_account_for_strat: [u8; 256],
    active: AtomicStratBitset,
    connected: AtomicStratBitset,
    global_strat_enabled: AtomicStratBitset,
    account_strat_enabled: Box<[AtomicStratBitset]>,
}

/// Ergonomic alias for [`OrsRuntimePolicy`].
pub type ControlPlane = OrsRuntimePolicy;

impl OrsRuntimePolicy {
    /// Builds a validated stateless ORS policy from static startup configuration.
    pub fn from_config(config: &PolicyConfig) -> Result<Self, PolicyConfigError> {
        config.validate()?;

        let mut configured_strats = [0u64; 4];
        for &sid in &config.strategies {
            set_bit_in_words(&mut configured_strats, sid);
        }
        for acct in &config.enabled_accounts {
            for &sid in &acct.strategies {
                set_bit_in_words(&mut configured_strats, sid);
            }
        }

        let mut account_present = [false; 256];
        let mut enabled_account_order = Vec::with_capacity(config.enabled_accounts.len());
        let mut account_authorized = Box::new([[0u64; 4]; 256]);
        let mut default_account_for_strat = [0u8; 256];

        for acct in &config.enabled_accounts {
            let idx = usize::from(acct.account_ref);
            account_present[idx] = true;
            enabled_account_order.push(acct.account_ref);
            for &sid in &acct.strategies {
                set_bit_in_words(&mut account_authorized[idx], sid);
                if default_account_for_strat[usize::from(sid)] == 0 {
                    default_account_for_strat[usize::from(sid)] = acct.account_ref;
                }
            }
        }

        let active_init = if config.default_active {
            configured_strats
        } else {
            [0u64; 4]
        };
        let global_enabled_init = if config.default_enabled {
            configured_strats
        } else {
            [0u64; 4]
        };

        let mut account_strat_enabled = Vec::with_capacity(256);
        for idx in 0..256 {
            if account_present[idx] && config.default_enabled {
                account_strat_enabled.push(AtomicStratBitset::from_words(account_authorized[idx]));
            } else {
                account_strat_enabled.push(AtomicStratBitset::new_empty());
            }
        }

        Ok(Self {
            mode: AtomicU8::new(config.initial_mode as u8),
            configured_strats,
            account_present,
            enabled_account_order,
            account_authorized,
            default_account_for_strat,
            active: AtomicStratBitset::from_words(active_init),
            connected: AtomicStratBitset::new_empty(),
            global_strat_enabled: AtomicStratBitset::from_words(global_enabled_init),
            account_strat_enabled: account_strat_enabled.into_boxed_slice(),
        })
    }

    /// Current process-wide operating [`Mode`].
    #[inline]
    pub fn mode(&self) -> Mode {
        Mode::from_u8(self.mode.load(Ordering::Acquire))
    }

    /// Atomically updates the process-wide [`Mode`], returning the previous mode.
    #[inline]
    pub fn set_mode(&self, next: Mode) -> Mode {
        Mode::from_u8(self.mode.swap(next as u8, Ordering::AcqRel))
    }

    /// Suspends the ORS into [`Mode::ManageOnly`] (`RESTRICTED_CANCEL_ONLY`), returning previous mode.
    #[inline]
    pub fn suspend(&self) -> Mode {
        self.set_mode(Mode::ManageOnly)
    }

    /// Resumes full [`Mode::Trading`], returning previous mode.
    #[inline]
    pub fn resume(&self) -> Mode {
        self.set_mode(Mode::Trading)
    }

    /// Transitions the ORS into [`Mode::Stopped`], returning previous mode.
    #[inline]
    pub fn stop(&self) -> Mode {
        self.set_mode(Mode::Stopped)
    }

    /// Ordered slice of configured `account_ref` values in `enabled_accounts` declaration order.
    #[inline]
    pub fn enabled_accounts(&self) -> &[u8] {
        &self.enabled_account_order
    }

    /// Returns `true` when `account_ref` (`1..=255`) is loaded in this ORS process.
    #[inline]
    pub fn has_account(&self, account_ref: u8) -> bool {
        account_ref != 0 && self.account_present[usize::from(account_ref)]
    }

    /// Returns `true` when `strat_id` (`0..=254`) is configured on this ORS process.
    #[inline]
    pub fn is_whitelisted(&self, strat_id: u8) -> bool {
        strat_id != ALL_ORS_STRATS && bit_in_words(&self.configured_strats, strat_id)
    }

    /// Returns `true` when `strat_id` is authorized in `account_ref`'s static `strategies` list.
    #[inline]
    pub fn is_authorized(&self, account_ref: u8, strat_id: u8) -> bool {
        if account_ref == 0
            || strat_id == ALL_ORS_STRATS
            || !self.account_present[usize::from(account_ref)]
        {
            return false;
        }
        bit_in_words(&self.account_authorized[usize::from(account_ref)], strat_id)
    }

    /// Returns the default `1`-based `account_ref` for `strat_id` (`0` if unauthorized everywhere).
    #[inline]
    pub fn default_account_ref(&self, strat_id: u8) -> Option<u8> {
        if strat_id == ALL_ORS_STRATS {
            return None;
        }
        let acct = self.default_account_for_strat[usize::from(strat_id)];
        (acct != 0).then_some(acct)
    }

    /// Attempts to register a live DOP session for `strat_id` (`DopLogin`).
    ///
    /// Refuses `ALL_ORS_STRATS` (`255`), unconfigured strategy IDs, or a second concurrent login
    /// while `strat_id` is already connected with [`RefusalReason::InvalidStratId`].
    pub fn try_login(&self, strat_id: u8) -> Result<(), RefusalReason> {
        if !self.is_whitelisted(strat_id) {
            return Err(RefusalReason::InvalidStratId);
        }
        if !self.connected.try_claim(strat_id) {
            return Err(RefusalReason::InvalidStratId);
        }
        Ok(())
    }

    /// Clears the live DOP `connected` bit for `strat_id` (`DopLogout` or socket EOF).
    #[inline]
    pub fn logout(&self, strat_id: u8) -> bool {
        if strat_id == ALL_ORS_STRATS {
            return false;
        }
        self.connected.set(strat_id, false)
    }

    /// Returns `true` if `strat_id` currently holds a connected DOP session.
    #[inline]
    pub fn is_connected(&self, strat_id: u8) -> bool {
        strat_id != ALL_ORS_STRATS && self.connected.contains(strat_id)
    }

    /// Sets or clears the administrative `active` bit for `strat_id` (`SetActive`).
    ///
    /// Returns `Err(RefusalReason::InvalidStratId)` if `strat_id` is not configured.
    pub fn set_active(&self, strat_id: u8, active: bool) -> Result<bool, RefusalReason> {
        if !self.is_whitelisted(strat_id) {
            return Err(RefusalReason::InvalidStratId);
        }
        Ok(self.active.set(strat_id, active))
    }

    /// Returns `true` when `strat_id` is marked `active`.
    #[inline]
    pub fn is_active(&self, strat_id: u8) -> bool {
        self.is_whitelisted(strat_id) && self.active.contains(strat_id)
    }

    /// Enables or disables `strat_id` across all accounts (`enable(strat, flag)`).
    pub fn set_global_strat_enabled(
        &self,
        strat_id: u8,
        enabled: bool,
    ) -> Result<bool, RefusalReason> {
        if !self.is_whitelisted(strat_id) {
            return Err(RefusalReason::InvalidStratId);
        }
        Ok(self.global_strat_enabled.set(strat_id, enabled))
    }

    /// Returns the strategy-wide `enabled` flag echoed at `DopLogin` (`DopEcho`).
    #[inline]
    pub fn is_global_strat_enabled(&self, strat_id: u8) -> bool {
        self.is_whitelisted(strat_id) && self.global_strat_enabled.contains(strat_id)
    }

    /// Enables or disables `strat_id` on a single `account_ref` (`set_strat_enabled`).
    ///
    /// Validates `strat_id`, `account_ref`, and authorization before mutating the `(account_ref, strat_id)`
    /// bit, returning the previous enablement state on that account.
    pub fn set_strat_enabled(
        &self,
        account_ref: u8,
        strat_id: u8,
        enabled: bool,
    ) -> Result<bool, RefusalReason> {
        let resolved = self.resolve_account_ref(account_ref, strat_id)?;
        Ok(self.account_strat_enabled[usize::from(resolved)].set(strat_id, enabled))
    }

    /// Returns `true` iff `strat_id` is enabled both globally and on `account_ref`.
    #[inline]
    pub fn is_strat_enabled(&self, account_ref: u8, strat_id: u8) -> bool {
        if !self.is_authorized(account_ref, strat_id) {
            return false;
        }
        self.global_strat_enabled.contains(strat_id)
            && self.account_strat_enabled[usize::from(account_ref)].contains(strat_id)
    }

    /// Resolves `requested_account_ref` (`0` = first authorized block in `enabled_accounts` order;
    /// `1..=255` = explicit account) for `strat_id` in `O(1)` without heap allocation.
    #[inline]
    pub fn resolve_account_ref(
        &self,
        requested_account_ref: u8,
        strat_id: u8,
    ) -> Result<u8, RefusalReason> {
        if strat_id == ALL_ORS_STRATS {
            return Err(RefusalReason::InvalidStratId);
        }
        if requested_account_ref == 0 {
            if !self.is_whitelisted(strat_id) {
                return Err(RefusalReason::InvalidStratId);
            }
            let default_acct = self.default_account_for_strat[usize::from(strat_id)];
            if default_acct == 0 {
                return Err(RefusalReason::InvalidStratId);
            }
            return Ok(default_acct);
        }

        let idx = usize::from(requested_account_ref);
        if !self.account_present[idx] {
            return Err(RefusalReason::InvalidAccountId);
        }
        if !bit_in_words(&self.account_authorized[idx], strat_id) {
            return Err(RefusalReason::RejectRoutingIdMismatch);
        }
        Ok(requested_account_ref)
    }

    /// Evaluates `action` on `(requested_account_ref, strat_id)` and returns the resolved
    /// `1`-based `account_ref` (`0` only for `AdminCancel` across all accounts when requested `0`).
    #[inline]
    pub fn evaluate(
        &self,
        action: PolicyAction,
        requested_account_ref: u8,
        strat_id: u8,
    ) -> Result<u8, RefusalReason> {
        if action == PolicyAction::AdminCancel {
            if requested_account_ref != 0 && !self.has_account(requested_account_ref) {
                return Err(RefusalReason::InvalidAccountId);
            }
            if strat_id != ALL_ORS_STRATS && !self.is_whitelisted(strat_id) {
                return Err(RefusalReason::InvalidStratId);
            }
            return Ok(requested_account_ref);
        }

        if strat_id == ALL_ORS_STRATS {
            if matches!(action, PolicyAction::Cancel | PolicyAction::CancelRfqQuote) {
                if requested_account_ref != 0 && !self.has_account(requested_account_ref) {
                    return Err(RefusalReason::InvalidAccountId);
                }
                if self.mode() == Mode::Stopped {
                    return Err(RefusalReason::MarketRejectMarketDown);
                }
                return Ok(requested_account_ref);
            }
            return Err(RefusalReason::InvalidStratId);
        }

        let resolved_acct = self.resolve_account_ref(requested_account_ref, strat_id)?;
        if !self.active.contains(strat_id) {
            return Err(RefusalReason::InvalidStratId);
        }

        match self.mode() {
            Mode::Stopped => Err(RefusalReason::MarketRejectMarketDown),
            Mode::ManageOnly => {
                if action.is_risk_increasing() {
                    Err(RefusalReason::RestrictedCancelOnly)
                } else {
                    Ok(resolved_acct)
                }
            }
            Mode::Trading => {
                if action.is_risk_increasing()
                    && (!self.global_strat_enabled.contains(strat_id)
                        || !self.account_strat_enabled[usize::from(resolved_acct)]
                            .contains(strat_id))
                {
                    return Err(RefusalReason::AdminAction);
                }
                Ok(resolved_acct)
            }
        }
    }
}

#[cfg(test)]
mod tests;
