//! Multi-source-IP and action-class-isolated hot-path request rate limiter.
//!
//! Models fixed epoch-aligned counters and preallocated sliding timestamp rings
//! keyed by [`RateLimitId`] (`{ local_ip, interval_s, account, flags, symbol }`),
//! with transactional multi-window `put_back` rollback, per-IP HTTP `429`
//! cooldown isolation, conservative exchange header reconciliation, and compiled
//! institutional [`VenuePreset`] budgets.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

/// Maximum number of window debits permitted in a single [`RateLimiter::allow`] call.
pub const MAX_SPENDS_PER_CALL: usize = 8;

/// Default percentage of `limit` at which exchange-reported usage raises a warning.
pub const DEFAULT_EXCH_WARN_PCT: u8 = 80;

/// Default maximum number of distinct windows in one [`RateLimiter`].
pub const MAX_RATE_WINDOWS: usize = 1024;

/// Maximum number of concurrently tracked per-IP cooldown entries.
pub const MAX_TRACKED_IPS: usize = 64;

/// Maximum number of concurrently tracked per-account cooldown/freeze entries.
pub const MAX_TRACKED_ACCOUNTS: usize = 64;

/// Minimum preallocated ring capacity for a [`RateFlags::ROLLING`] window.
pub const MIN_ROLLING_RING_CAPACITY: usize = 64;

/// Default upper bound on preallocated ring slots for a [`RateFlags::ROLLING`] window.
pub const MAX_ROLLING_RING_CAPACITY: usize = 4096;

/// Maximum number of local egress source IPs bound to one [`VenueRateLimiter`].
pub const MAX_VENUE_IPS: usize = 16;

/// Maximum number of distinct symbol/instrument buckets in one [`VenueRateLimiter`].
pub const MAX_VENUE_SYMBOLS: usize = 256;

const NS_PER_SEC: u64 = 1_000_000_000;

const IP_TAG_NONE: u8 = 0;
const IP_TAG_V4: u8 = 4;
const IP_TAG_V6: u8 = 6;

/// Compact, zero-allocation source-IP identity for [`RateLimitId`].
///
/// Losslessly encodes [`IpKey::NONE`] (IP-independent windows), every IPv4
/// address (including `0.0.0.0`), and every IPv6 address (including `::`)
/// without heap allocation.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IpKey {
    tag: u8,
    bits: u128,
}

impl IpKey {
    /// Sentinel representing an account-wide or symbol-scoped window with no source-IP component.
    pub const NONE: Self = Self {
        tag: IP_TAG_NONE,
        bits: 0,
    };

    /// Returns `true` when this key is [`IpKey::NONE`].
    #[inline]
    pub const fn is_none(self) -> bool {
        self.tag == IP_TAG_NONE
    }

    /// Encodes an IPv4 address losslessly.
    #[inline]
    pub const fn from_v4(ip: Ipv4Addr) -> Self {
        Self {
            tag: IP_TAG_V4,
            bits: ip.to_bits() as u128,
        }
    }

    /// Encodes an IPv6 address losslessly.
    #[inline]
    pub const fn from_v6(ip: Ipv6Addr) -> Self {
        Self {
            tag: IP_TAG_V6,
            bits: ip.to_bits(),
        }
    }

    /// Encodes any [`IpAddr`] losslessly.
    #[inline]
    pub const fn from_ip(ip: IpAddr) -> Self {
        match ip {
            IpAddr::V4(v4) => Self::from_v4(v4),
            IpAddr::V6(v6) => Self::from_v6(v6),
        }
    }

    /// Encodes `Some(ip)` as its address key and `None` as [`IpKey::NONE`].
    #[inline]
    pub const fn from_opt_ip(ip: Option<IpAddr>) -> Self {
        match ip {
            Some(addr) => Self::from_ip(addr),
            None => Self::NONE,
        }
    }

    /// Decodes back into the original [`IpAddr`], or `None` for [`IpKey::NONE`].
    #[inline]
    pub const fn to_ip(self) -> Option<IpAddr> {
        match self.tag {
            IP_TAG_V4 => Some(IpAddr::V4(Ipv4Addr::from_bits(self.bits as u32))),
            IP_TAG_V6 => Some(IpAddr::V6(Ipv6Addr::from_bits(self.bits))),
            _ => None,
        }
    }
}

impl From<IpAddr> for IpKey {
    #[inline]
    fn from(ip: IpAddr) -> Self {
        Self::from_ip(ip)
    }
}

impl From<Ipv4Addr> for IpKey {
    #[inline]
    fn from(ip: Ipv4Addr) -> Self {
        Self::from_v4(ip)
    }
}

impl From<Ipv6Addr> for IpKey {
    #[inline]
    fn from(ip: Ipv6Addr) -> Self {
        Self::from_v6(ip)
    }
}

/// Bitflags describing a window's action class, counting mode, and scope dimensions.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RateFlags(pub u16);

impl RateFlags {
    pub const EMPTY: Self = Self(0);
    /// Order creation (and venue-counted order modifications).
    pub const ORDER: Self = Self(1 << 0);
    /// Order cancellation — strictly isolated from [`Self::ORDER`].
    pub const CANCEL: Self = Self(1 << 1);
    /// General REST/WebSocket request or query budget.
    pub const REQUEST: Self = Self(1 << 2);
    /// Units represent multi-unit endpoint weight rather than 1-per-send counts.
    pub const WEIGHT: Self = Self(1 << 3);
    /// Sliding timestamp window (`0` = epoch-aligned fixed window).
    pub const ROLLING: Self = Self(1 << 4);
    /// Scoped to a specific `account`.
    pub const PER_ACCOUNT: Self = Self(1 << 5);
    /// Scoped to a specific egress `local_ip`.
    pub const PER_IP: Self = Self(1 << 6);
    /// Scoped to a specific `symbol` or instrument family.
    pub const PER_SYMBOL: Self = Self(1 << 7);

    #[inline]
    pub const fn contains(self, other: Self) -> bool {
        (self.0 & other.0) == other.0
    }

    #[inline]
    pub const fn intersects(self, other: Self) -> bool {
        (self.0 & other.0) != 0
    }

    #[inline]
    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    #[inline]
    pub const fn is_rolling(self) -> bool {
        self.contains(Self::ROLLING)
    }

    #[inline]
    pub const fn has_action_class(self) -> bool {
        self.intersects(Self(Self::ORDER.0 | Self::CANCEL.0 | Self::REQUEST.0))
    }
}

impl core::ops::BitOr for RateFlags {
    type Output = Self;

    #[inline]
    fn bitor(self, rhs: Self) -> Self::Output {
        self.union(rhs)
    }
}

impl core::ops::BitOrAssign for RateFlags {
    #[inline]
    fn bitor_assign(&mut self, rhs: Self) {
        self.0 |= rhs.0;
    }
}

impl core::ops::BitAnd for RateFlags {
    type Output = Self;

    #[inline]
    fn bitand(self, rhs: Self) -> Self::Output {
        Self(self.0 & rhs.0)
    }
}

/// Canonical composite key identifying one rate-limit counter.
///
/// Two requests share a counter if and only if their `RateLimitId` values are equal.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RateLimitId {
    /// Bound local egress IP, or [`IpKey::NONE`] when IP-independent.
    pub local_ip: IpKey,
    /// Window duration in whole seconds (`> 0`).
    pub interval_s: u32,
    /// Account identifier, or `0` when shared across all accounts on `local_ip`.
    pub account: u64,
    /// Action class (`ORDER`, `CANCEL`, `REQUEST`), window kind (`ROLLING`), and scope flags.
    pub flags: RateFlags,
    /// Symbol, product, or instrument family code, or `0` when symbol-independent.
    pub symbol: u64,
}

/// Complete window rule pairing a [`RateLimitId`] with its cap `limit`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RateRule {
    pub id: RateLimitId,
    pub limit: u64,
}

impl RateRule {
    /// Account-scoped rule: `{ NONE, interval_s, account, PER_ACCOUNT | flags, 0 }`.
    #[inline]
    pub const fn account(account: u64, interval_s: u32, flags: RateFlags, limit: u64) -> Self {
        Self {
            id: RateLimitId {
                local_ip: IpKey::NONE,
                interval_s,
                account,
                flags: flags.union(RateFlags::PER_ACCOUNT),
                symbol: 0,
            },
            limit,
        }
    }

    /// Source-IP-scoped rule shared by all accounts on `local_ip`:
    /// `{ local_ip, interval_s, 0, PER_IP | flags, 0 }`.
    #[inline]
    pub const fn ip(local_ip: IpKey, interval_s: u32, flags: RateFlags, limit: u64) -> Self {
        Self {
            id: RateLimitId {
                local_ip,
                interval_s,
                account: 0,
                flags: flags.union(RateFlags::PER_IP),
                symbol: 0,
            },
            limit,
        }
    }

    /// Account + symbol rule:
    /// `{ NONE, interval_s, account, PER_ACCOUNT | PER_SYMBOL | flags, symbol }`.
    #[inline]
    pub const fn symbol(
        account: u64,
        interval_s: u32,
        flags: RateFlags,
        limit: u64,
        symbol: u64,
    ) -> Self {
        Self {
            id: RateLimitId {
                local_ip: IpKey::NONE,
                interval_s,
                account,
                flags: flags
                    .union(RateFlags::PER_ACCOUNT)
                    .union(RateFlags::PER_SYMBOL),
                symbol,
            },
            limit,
        }
    }

    /// Account + product rule (`PER_SYMBOL` omitted, `product` stored in `symbol`).
    #[inline]
    pub const fn product(
        account: u64,
        interval_s: u32,
        flags: RateFlags,
        limit: u64,
        product: u64,
    ) -> Self {
        Self {
            id: RateLimitId {
                local_ip: IpKey::NONE,
                interval_s,
                account,
                flags: flags.union(RateFlags::PER_ACCOUNT),
                symbol: product,
            },
            limit,
        }
    }

    /// Instrument-family rule (`PER_ACCOUNT` omitted when `account == 0`).
    #[inline]
    pub const fn family(
        account: u64,
        interval_s: u32,
        flags: RateFlags,
        limit: u64,
        family: u64,
    ) -> Self {
        let scoped = if account == 0 {
            flags.union(RateFlags::PER_SYMBOL)
        } else {
            flags
                .union(RateFlags::PER_ACCOUNT)
                .union(RateFlags::PER_SYMBOL)
        };
        Self {
            id: RateLimitId {
                local_ip: IpKey::NONE,
                interval_s,
                account,
                flags: scoped,
                symbol: family,
            },
            limit,
        }
    }
}

/// Registration specification for a fixed or rolling window.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WindowSpec {
    pub id: RateLimitId,
    pub limit: u64,
    pub ring_capacity: Option<usize>,
}

impl WindowSpec {
    #[inline]
    pub const fn new(id: RateLimitId, limit: u64) -> Self {
        Self {
            id,
            limit,
            ring_capacity: None,
        }
    }

    #[inline]
    pub const fn with_ring_capacity(id: RateLimitId, limit: u64, ring_capacity: usize) -> Self {
        Self {
            id,
            limit,
            ring_capacity: Some(ring_capacity),
        }
    }

    #[inline]
    pub const fn from_rule(rule: RateRule) -> Self {
        Self::new(rule.id, rule.limit)
    }
}

impl From<RateRule> for WindowSpec {
    #[inline]
    fn from(rule: RateRule) -> Self {
        Self::from_rule(rule)
    }
}

/// Resolved handle returned by [`RateLimiter::register_window`] and [`RateLimiter::add`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct WindowHandle {
    index: usize,
    id: RateLimitId,
}

impl WindowHandle {
    #[inline]
    pub const fn id(self) -> RateLimitId {
        self.id
    }

    #[inline]
    pub const fn index(self) -> usize {
        self.index
    }
}

/// Single window debit within a transactional [`RateLimiter::allow`] batch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Spend {
    pub id: RateLimitId,
    pub units: u32,
}

impl Spend {
    #[inline]
    pub const fn new(id: RateLimitId, units: u32) -> Self {
        Self { id, units }
    }

    #[inline]
    pub const fn from_handle(handle: WindowHandle, units: u32) -> Self {
        Self {
            id: handle.id,
            units,
        }
    }
}

/// Proof of an admitted hot-path rate-limit debit at `granted_at_ns`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RatePermit {
    pub granted_at_ns: u64,
    pub spends_applied: u8,
}

/// Rejection reason returned when [`RateLimiter`] refuses an operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RateReject {
    Invalid,
    Capacity,
    ClockRegression,
    UnknownWindow {
        id: RateLimitId,
    },
    IpCooldown {
        local_ip: IpKey,
        until_ns: u64,
        retry_after_ns: u64,
    },
    AccountCooldown {
        account: u64,
        until_ns: u64,
        retry_after_ns: u64,
    },
    Frozen {
        account: u64,
    },
    Exhausted {
        id: RateLimitId,
        limit: u64,
        used: u64,
        requested: u32,
        retry_after_ns: u64,
    },
}

/// Result of reconciling an authoritative exchange rate-limit header or WS `rateLimits[]` row.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExchangeUsageReport {
    pub id: RateLimitId,
    pub limit: u64,
    pub previous_used: u64,
    pub effective_used: u64,
    pub reported_used: u64,
    pub warn: bool,
    /// `true` on the rising edge into warning state (`< exch_warn_pct` -> `>= exch_warn_pct`).
    pub newly_warned: bool,
}

/// Computes the default preallocated ring capacity `min(limit.max(64), 4096)` for rolling windows.
#[inline]
pub const fn default_rolling_ring_capacity(limit: u64) -> usize {
    let min = MIN_ROLLING_RING_CAPACITY as u64;
    let max = MAX_ROLLING_RING_CAPACITY as u64;
    let at_least_min = if limit < min { min } else { limit };
    let clamped = if at_least_min > max {
        max
    } else {
        at_least_min
    };
    clamped as usize
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct RollingEntry {
    ts_ns: u64,
    units: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WindowUndo {
    Fixed { prev_used: u64 },
    RollingPushed { units: u64 },
    RollingCoalesced { units: u64 },
}

#[derive(Debug)]
struct FixedWindow {
    interval_ns: u64,
    limit: u64,
    current_epoch_idx: Option<u64>,
    used: u64,
    warn_active: bool,
}

impl FixedWindow {
    fn new(interval_ns: u64, limit: u64) -> Self {
        Self {
            interval_ns,
            limit,
            current_epoch_idx: None,
            used: 0,
            warn_active: false,
        }
    }

    #[inline]
    fn advance_to(&mut self, now_ns: u64) -> core::result::Result<u64, RateReject> {
        let epoch_idx = now_ns / self.interval_ns;
        match self.current_epoch_idx {
            None => {
                self.current_epoch_idx = Some(epoch_idx);
                self.used = 0;
                self.warn_active = false;
            }
            Some(cur) if epoch_idx > cur => {
                self.current_epoch_idx = Some(epoch_idx);
                self.used = 0;
                self.warn_active = false;
            }
            Some(cur) if epoch_idx < cur => {
                return Err(RateReject::ClockRegression);
            }
            Some(_) => {}
        }
        Ok(epoch_idx)
    }

    #[inline]
    fn used_at(&self, now_ns: u64) -> u64 {
        let epoch_idx = now_ns / self.interval_ns;
        match self.current_epoch_idx {
            Some(cur) if cur == epoch_idx => self.used,
            _ => 0,
        }
    }

    #[inline]
    fn can_take_readonly(
        &self,
        id: RateLimitId,
        prior_units: u64,
        request_units: u32,
        now_ns: u64,
    ) -> core::result::Result<(), RateReject> {
        let epoch_idx = now_ns / self.interval_ns;
        if let Some(cur) = self.current_epoch_idx
            && epoch_idx < cur
        {
            return Err(RateReject::ClockRegression);
        }
        let cur_used = self.used_at(now_ns);
        let used_before = cur_used.saturating_add(prior_units);
        let req = u64::from(request_units);
        let fits = cur_used
            .checked_add(prior_units)
            .and_then(|u| u.checked_add(req))
            .is_some_and(|sum| sum <= self.limit);
        if !fits {
            let next_epoch_ns = epoch_idx.saturating_add(1).saturating_mul(self.interval_ns);
            return Err(RateReject::Exhausted {
                id,
                limit: self.limit,
                used: used_before,
                requested: request_units,
                retry_after_ns: next_epoch_ns.saturating_sub(now_ns),
            });
        }
        Ok(())
    }

    #[inline]
    fn try_take(
        &mut self,
        id: RateLimitId,
        units: u32,
        now_ns: u64,
    ) -> core::result::Result<WindowUndo, RateReject> {
        let epoch_idx = self.advance_to(now_ns)?;
        let req = u64::from(units);
        let Some(next_used) = self.used.checked_add(req).filter(|&sum| sum <= self.limit) else {
            let next_epoch_ns = epoch_idx.saturating_add(1).saturating_mul(self.interval_ns);
            return Err(RateReject::Exhausted {
                id,
                limit: self.limit,
                used: self.used,
                requested: units,
                retry_after_ns: next_epoch_ns.saturating_sub(now_ns),
            });
        };
        let prev_used = self.used;
        self.used = next_used;
        Ok(WindowUndo::Fixed { prev_used })
    }

    #[inline]
    fn put_back(&mut self, undo: WindowUndo) {
        if let WindowUndo::Fixed { prev_used } = undo {
            self.used = prev_used;
        }
    }
}

#[derive(Debug)]
struct RollingWindow {
    interval_ns: u64,
    limit: u64,
    /// Fixed-length boxed slice allocated once during registration; never resized on hot path.
    entries: Box<[RollingEntry]>,
    head: usize,
    len: usize,
    used: u64,
    observed_exchange_peak: u64,
    warn_active: bool,
}

impl RollingWindow {
    fn with_entries(interval_ns: u64, limit: u64, entries: Box<[RollingEntry]>) -> Self {
        Self {
            interval_ns,
            limit,
            entries,
            head: 0,
            len: 0,
            used: 0,
            observed_exchange_peak: 0,
            warn_active: false,
        }
    }

    fn new(interval_ns: u64, limit: u64, ring_capacity: usize) -> Self {
        let entries = vec![RollingEntry::default(); ring_capacity].into_boxed_slice();
        Self::with_entries(interval_ns, limit, entries)
    }

    #[inline]
    fn evict_expired(&mut self, now_ns: u64) {
        let cap = self.entries.len();
        while self.len > 0 {
            let front = self.entries[self.head];
            if now_ns.saturating_sub(front.ts_ns) >= self.interval_ns {
                self.used = self.used.saturating_sub(front.units);
                self.head = (self.head + 1) % cap;
                self.len -= 1;
            } else {
                break;
            }
        }
        if self.used == 0 {
            self.observed_exchange_peak = 0;
            self.warn_active = false;
        }
    }

    #[inline]
    fn state_at(&self, now_ns: u64) -> (usize, usize, u64) {
        let cap = self.entries.len();
        let mut expired_count = 0usize;
        let mut expired_units = 0u64;
        while expired_count < self.len {
            let entry = self.entries[(self.head + expired_count) % cap];
            if now_ns.saturating_sub(entry.ts_ns) >= self.interval_ns {
                expired_units = expired_units.saturating_add(entry.units);
                expired_count += 1;
            } else {
                break;
            }
        }
        let eff_head = (self.head + expired_count) % cap;
        let eff_len = self.len - expired_count;
        let eff_used = self.used.saturating_sub(expired_units);
        (eff_head, eff_len, eff_used)
    }

    #[inline]
    fn used_at(&self, now_ns: u64) -> u64 {
        self.state_at(now_ns).2
    }

    #[inline]
    fn retry_after_for_units(
        &self,
        eff_head: usize,
        eff_len: usize,
        eff_used: u64,
        req: u64,
        now_ns: u64,
    ) -> u64 {
        if req > self.limit {
            return self.interval_ns;
        }
        let need_to_free =
            (u128::from(eff_used) + u128::from(req)).saturating_sub(u128::from(self.limit));
        let cap = self.entries.len();
        let mut freed = 0u128;
        for i in 0..eff_len {
            let entry = self.entries[(eff_head + i) % cap];
            freed += u128::from(entry.units);
            if freed >= need_to_free {
                let expire_at_ns = entry.ts_ns.saturating_add(self.interval_ns);
                return expire_at_ns.saturating_sub(now_ns);
            }
        }
        self.interval_ns
    }

    #[inline]
    fn can_take_readonly(
        &self,
        id: RateLimitId,
        prior_units: u64,
        request_units: u32,
        now_ns: u64,
    ) -> core::result::Result<(), RateReject> {
        let (eff_head, eff_len, eff_used) = self.state_at(now_ns);
        let used_before = eff_used.saturating_add(prior_units);
        let req = u64::from(request_units);
        let fits = eff_used
            .checked_add(prior_units)
            .and_then(|u| u.checked_add(req))
            .is_some_and(|sum| sum <= self.limit);
        if !fits {
            let retry_after_ns = match prior_units.checked_add(req) {
                Some(total_units) if req <= self.limit => {
                    self.retry_after_for_units(eff_head, eff_len, eff_used, total_units, now_ns)
                }
                _ => self.interval_ns,
            };
            return Err(RateReject::Exhausted {
                id,
                limit: self.limit,
                used: used_before,
                requested: request_units,
                retry_after_ns,
            });
        }
        let cap = self.entries.len();
        let coalesces = prior_units > 0
            || (eff_len > 0 && {
                let tail_idx = (eff_head + eff_len - 1) % cap;
                self.entries[tail_idx].ts_ns == now_ns
            });
        if !coalesces && eff_len == cap {
            let front = self.entries[eff_head];
            let retry_after_ns = self
                .interval_ns
                .saturating_sub(now_ns.saturating_sub(front.ts_ns));
            return Err(RateReject::Exhausted {
                id,
                limit: self.limit,
                used: used_before,
                requested: request_units,
                retry_after_ns,
            });
        }
        Ok(())
    }

    #[inline]
    fn try_take(
        &mut self,
        id: RateLimitId,
        units: u32,
        now_ns: u64,
    ) -> core::result::Result<WindowUndo, RateReject> {
        self.evict_expired(now_ns);
        let req = u64::from(units);
        let Some(next_used) = self.used.checked_add(req).filter(|&sum| sum <= self.limit) else {
            let retry_after_ns =
                self.retry_after_for_units(self.head, self.len, self.used, req, now_ns);
            return Err(RateReject::Exhausted {
                id,
                limit: self.limit,
                used: self.used,
                requested: units,
                retry_after_ns,
            });
        };
        let cap = self.entries.len();
        if self.len > 0 {
            let tail_idx = (self.head + self.len - 1) % cap;
            if self.entries[tail_idx].ts_ns == now_ns {
                let merged = self.entries[tail_idx]
                    .units
                    .checked_add(req)
                    .ok_or(RateReject::Invalid)?;
                self.entries[tail_idx].units = merged;
                self.used = next_used;
                return Ok(WindowUndo::RollingCoalesced { units: req });
            }
        }
        if self.len == cap {
            let front = self.entries[self.head];
            let retry_after_ns = self
                .interval_ns
                .saturating_sub(now_ns.saturating_sub(front.ts_ns));
            return Err(RateReject::Exhausted {
                id,
                limit: self.limit,
                used: self.used,
                requested: units,
                retry_after_ns,
            });
        }
        let write_idx = (self.head + self.len) % cap;
        self.entries[write_idx] = RollingEntry {
            ts_ns: now_ns,
            units: req,
        };
        self.len += 1;
        self.used = next_used;
        Ok(WindowUndo::RollingPushed { units: req })
    }

    #[inline]
    fn put_back(&mut self, undo: WindowUndo) {
        let cap = self.entries.len();
        match undo {
            WindowUndo::RollingCoalesced { units } => {
                debug_assert!(self.len > 0);
                let tail_idx = (self.head + self.len - 1) % cap;
                self.entries[tail_idx].units -= units;
                self.used -= units;
            }
            WindowUndo::RollingPushed { units } => {
                debug_assert!(self.len > 0);
                self.len -= 1;
                let tail_idx = (self.head + self.len) % cap;
                self.entries[tail_idx] = RollingEntry::default();
                self.used -= units;
            }
            WindowUndo::Fixed { .. } => {}
        }
    }
}

#[derive(Debug)]
enum WindowKind {
    Fixed(FixedWindow),
    Rolling(RollingWindow),
}

#[derive(Debug)]
struct RateWindow {
    id: RateLimitId,
    kind: WindowKind,
}

impl RateWindow {
    #[inline]
    fn limit(&self) -> u64 {
        match &self.kind {
            WindowKind::Fixed(w) => w.limit,
            WindowKind::Rolling(w) => w.limit,
        }
    }

    #[inline]
    fn used_at(&self, now_ns: u64) -> u64 {
        match &self.kind {
            WindowKind::Fixed(w) => w.used_at(now_ns),
            WindowKind::Rolling(w) => w.used_at(now_ns),
        }
    }

    #[inline]
    fn can_take_readonly(
        &self,
        prior_units: u64,
        request_units: u32,
        now_ns: u64,
    ) -> core::result::Result<(), RateReject> {
        match &self.kind {
            WindowKind::Fixed(w) => {
                w.can_take_readonly(self.id, prior_units, request_units, now_ns)
            }
            WindowKind::Rolling(w) => {
                w.can_take_readonly(self.id, prior_units, request_units, now_ns)
            }
        }
    }

    #[inline]
    fn try_take(
        &mut self,
        units: u32,
        now_ns: u64,
    ) -> core::result::Result<WindowUndo, RateReject> {
        match &mut self.kind {
            WindowKind::Fixed(w) => w.try_take(self.id, units, now_ns),
            WindowKind::Rolling(w) => w.try_take(self.id, units, now_ns),
        }
    }

    #[inline]
    fn put_back(&mut self, undo: WindowUndo) {
        match &mut self.kind {
            WindowKind::Fixed(w) => w.put_back(undo),
            WindowKind::Rolling(w) => w.put_back(undo),
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct IpCooldownEntry {
    local_ip: IpKey,
    until_ns: u64,
}

#[derive(Clone, Copy, Debug, Default)]
struct AccountCooldownEntry {
    account: u64,
    until_ns: u64,
    frozen: bool,
}

/// Preallocated single-thread multi-window rate limiter with exact transactional rollback.
#[derive(Debug)]
pub struct RateLimiter {
    windows: Vec<RateWindow>,
    /// Sorted `(RateLimitId, window_index)` table for `O(log N)` zero-allocation lookup.
    index_by_id: Vec<(RateLimitId, usize)>,
    max_windows: usize,
    ip_cooldowns: Box<[IpCooldownEntry; MAX_TRACKED_IPS]>,
    ip_cooldown_len: usize,
    account_cooldowns: Box<[AccountCooldownEntry; MAX_TRACKED_ACCOUNTS]>,
    account_cooldown_len: usize,
    last_now_ns: u64,
    exch_warn_pct: u8,
}

impl Default for RateLimiter {
    fn default() -> Self {
        Self::new()
    }
}

impl RateLimiter {
    /// Creates a [`RateLimiter`] with [`MAX_RATE_WINDOWS`] and [`DEFAULT_EXCH_WARN_PCT`] (`80%`).
    pub fn new() -> Self {
        Self::with_config(MAX_RATE_WINDOWS, DEFAULT_EXCH_WARN_PCT)
            .expect("default RateLimiter configuration must be valid")
    }

    /// Creates a [`RateLimiter`] with explicit preallocated window capacity and warning percentage (`1..=100`).
    pub fn with_config(
        max_windows: usize,
        exch_warn_pct: u8,
    ) -> core::result::Result<Self, RateReject> {
        if max_windows == 0 || !(1..=100).contains(&exch_warn_pct) {
            return Err(RateReject::Invalid);
        }
        Ok(Self {
            windows: Vec::with_capacity(max_windows),
            index_by_id: Vec::with_capacity(max_windows),
            max_windows,
            ip_cooldowns: Box::new([IpCooldownEntry::default(); MAX_TRACKED_IPS]),
            ip_cooldown_len: 0,
            account_cooldowns: Box::new([AccountCooldownEntry::default(); MAX_TRACKED_ACCOUNTS]),
            account_cooldown_len: 0,
            last_now_ns: 0,
            exch_warn_pct,
        })
    }

    #[inline]
    pub const fn exch_warn_pct(&self) -> u8 {
        self.exch_warn_pct
    }

    #[inline]
    pub const fn last_now_ns(&self) -> u64 {
        self.last_now_ns
    }

    #[inline]
    pub fn window_count(&self) -> usize {
        self.windows.len()
    }

    #[inline]
    pub fn window_capacity(&self) -> usize {
        self.windows.capacity()
    }

    /// Returns the heap address and length of a rolling window's preallocated ring slice, if rolling.
    pub fn rolling_ring_allocation_identity(&self, id: RateLimitId) -> Option<(usize, usize)> {
        let idx = self.lookup_index(id)?;
        match &self.windows[idx].kind {
            WindowKind::Fixed(_) => None,
            WindowKind::Rolling(w) => Some((w.entries.as_ptr() as usize, w.entries.len())),
        }
    }

    #[inline]
    fn lookup_index(&self, id: RateLimitId) -> Option<usize> {
        self.index_by_id
            .binary_search_by_key(&id, |&(k, _)| k)
            .ok()
            .map(|pos| self.index_by_id[pos].1)
    }

    fn validate_spec(spec: &WindowSpec) -> core::result::Result<u64, RateReject> {
        if spec.limit == 0 || spec.id.interval_s == 0 || !spec.id.flags.has_action_class() {
            return Err(RateReject::Invalid);
        }
        if spec.id.flags.contains(RateFlags::PER_IP) && spec.id.local_ip.is_none() {
            return Err(RateReject::Invalid);
        }
        u64::from(spec.id.interval_s)
            .checked_mul(NS_PER_SEC)
            .ok_or(RateReject::Invalid)
    }

    /// Installs a window rule if absent, keeping the original cap if `rule.id` was already added.
    pub fn add(&mut self, rule: RateRule) -> core::result::Result<WindowHandle, RateReject> {
        self.register_window(WindowSpec::from_rule(rule))
    }

    /// Registers a fixed or rolling window, preallocating any rolling timestamp ring once at setup.
    ///
    /// If `spec.id` is already registered, returns the existing [`WindowHandle`] without reallocating.
    pub fn register_window(
        &mut self,
        spec: WindowSpec,
    ) -> core::result::Result<WindowHandle, RateReject> {
        let interval_ns = Self::validate_spec(&spec)?;
        let pos = match self.index_by_id.binary_search_by_key(&spec.id, |&(k, _)| k) {
            Ok(existing_pos) => {
                let index = self.index_by_id[existing_pos].1;
                return Ok(WindowHandle {
                    index,
                    id: self.windows[index].id,
                });
            }
            Err(insert_pos) => insert_pos,
        };
        if self.windows.len() >= self.max_windows {
            return Err(RateReject::Capacity);
        }
        let kind = if spec.id.flags.is_rolling() {
            let cap = spec
                .ring_capacity
                .unwrap_or_else(|| default_rolling_ring_capacity(spec.limit));
            if cap == 0 {
                return Err(RateReject::Invalid);
            }
            WindowKind::Rolling(RollingWindow::new(interval_ns, spec.limit, cap))
        } else {
            WindowKind::Fixed(FixedWindow::new(interval_ns, spec.limit))
        };
        let index = self.windows.len();
        self.windows.push(RateWindow { id: spec.id, kind });
        self.index_by_id.insert(pos, (spec.id, index));
        Ok(WindowHandle { index, id: spec.id })
    }

    fn register_rolling_with_preallocated_ring(
        &mut self,
        rule: RateRule,
        ring: Box<[RollingEntry]>,
    ) -> core::result::Result<WindowHandle, RateReject> {
        let spec = WindowSpec::from_rule(rule);
        let interval_ns = Self::validate_spec(&spec)?;
        if !rule.id.flags.is_rolling() || ring.is_empty() {
            return Err(RateReject::Invalid);
        }
        let pos = match self.index_by_id.binary_search_by_key(&rule.id, |&(k, _)| k) {
            Ok(existing_pos) => {
                let index = self.index_by_id[existing_pos].1;
                return Ok(WindowHandle {
                    index,
                    id: self.windows[index].id,
                });
            }
            Err(insert_pos) => insert_pos,
        };
        if self.windows.len() >= self.max_windows {
            return Err(RateReject::Capacity);
        }
        let index = self.windows.len();
        self.windows.push(RateWindow {
            id: rule.id,
            kind: WindowKind::Rolling(RollingWindow::with_entries(interval_ns, rule.limit, ring)),
        });
        self.index_by_id.insert(pos, (rule.id, index));
        Ok(WindowHandle { index, id: rule.id })
    }

    /// Returns the configured limit for `id`, if registered.
    pub fn window_limit(&self, id: RateLimitId) -> Option<u64> {
        let idx = self.lookup_index(id)?;
        Some(self.windows[idx].limit())
    }

    /// Returns the effective consumed units for `id` at `now_ns` without mutating state.
    pub fn window_used(&self, id: RateLimitId, now_ns: u64) -> Option<u64> {
        let idx = self.lookup_index(id)?;
        Some(self.windows[idx].used_at(now_ns))
    }

    /// Isolates an HTTP `429` / IP throttle cooldown strictly to `local_ip` (`local_ip != IpKey::NONE`).
    ///
    /// Sibling source IPs remain completely unaffected.
    pub fn note_ip_cooldown(
        &mut self,
        local_ip: IpKey,
        until_ns: u64,
    ) -> core::result::Result<(), RateReject> {
        if local_ip.is_none() {
            return Err(RateReject::Invalid);
        }
        for i in 0..self.ip_cooldown_len {
            if self.ip_cooldowns[i].local_ip == local_ip {
                self.ip_cooldowns[i].until_ns = self.ip_cooldowns[i].until_ns.max(until_ns);
                return Ok(());
            }
        }
        if self.ip_cooldown_len < MAX_TRACKED_IPS {
            let slot = self.ip_cooldown_len;
            self.ip_cooldowns[slot] = IpCooldownEntry { local_ip, until_ns };
            self.ip_cooldown_len += 1;
            return Ok(());
        }
        for i in 0..self.ip_cooldown_len {
            if self.ip_cooldowns[i].until_ns <= self.last_now_ns {
                self.ip_cooldowns[i] = IpCooldownEntry { local_ip, until_ns };
                return Ok(());
            }
        }
        Err(RateReject::Capacity)
    }

    /// Applies an account-wide cooldown across all egress source IPs for `account`.
    pub fn note_account_cooldown(
        &mut self,
        account: u64,
        until_ns: u64,
    ) -> core::result::Result<(), RateReject> {
        if account == 0 {
            return Err(RateReject::Invalid);
        }
        for i in 0..self.account_cooldown_len {
            if self.account_cooldowns[i].account == account {
                self.account_cooldowns[i].until_ns =
                    self.account_cooldowns[i].until_ns.max(until_ns);
                return Ok(());
            }
        }
        if self.account_cooldown_len < MAX_TRACKED_ACCOUNTS {
            let slot = self.account_cooldown_len;
            self.account_cooldowns[slot] = AccountCooldownEntry {
                account,
                until_ns,
                frozen: false,
            };
            self.account_cooldown_len += 1;
            return Ok(());
        }
        for i in 0..self.account_cooldown_len {
            if !self.account_cooldowns[i].frozen
                && self.account_cooldowns[i].until_ns <= self.last_now_ns
            {
                self.account_cooldowns[i] = AccountCooldownEntry {
                    account,
                    until_ns,
                    frozen: false,
                };
                return Ok(());
            }
        }
        Err(RateReject::Capacity)
    }

    /// Permanently freezes `account` across all source IPs (e.g. HTTP `418` / account ban).
    pub fn freeze_account(&mut self, account: u64) -> core::result::Result<(), RateReject> {
        if account == 0 {
            return Err(RateReject::Invalid);
        }
        for i in 0..self.account_cooldown_len {
            if self.account_cooldowns[i].account == account {
                self.account_cooldowns[i].frozen = true;
                return Ok(());
            }
        }
        if self.account_cooldown_len < MAX_TRACKED_ACCOUNTS {
            let slot = self.account_cooldown_len;
            self.account_cooldowns[slot] = AccountCooldownEntry {
                account,
                until_ns: 0,
                frozen: true,
            };
            self.account_cooldown_len += 1;
            return Ok(());
        }
        for i in 0..self.account_cooldown_len {
            if !self.account_cooldowns[i].frozen
                && self.account_cooldowns[i].until_ns <= self.last_now_ns
            {
                self.account_cooldowns[i] = AccountCooldownEntry {
                    account,
                    until_ns: 0,
                    frozen: true,
                };
                return Ok(());
            }
        }
        Err(RateReject::Capacity)
    }

    #[inline]
    pub fn is_account_frozen(&self, account: u64) -> bool {
        if account == 0 {
            return false;
        }
        for i in 0..self.account_cooldown_len {
            if self.account_cooldowns[i].account == account {
                return self.account_cooldowns[i].frozen;
            }
        }
        false
    }

    #[inline]
    pub fn ip_cooldown_until(&self, local_ip: IpKey) -> Option<u64> {
        if local_ip.is_none() {
            return None;
        }
        for i in 0..self.ip_cooldown_len {
            if self.ip_cooldowns[i].local_ip == local_ip {
                return Some(self.ip_cooldowns[i].until_ns);
            }
        }
        None
    }

    #[inline]
    pub fn account_cooldown_until(&self, account: u64) -> Option<u64> {
        if account == 0 {
            return None;
        }
        for i in 0..self.account_cooldown_len {
            if self.account_cooldowns[i].account == account {
                return Some(self.account_cooldowns[i].until_ns);
            }
        }
        None
    }

    #[inline]
    pub fn check_account(&self, account: u64, now_ns: u64) -> core::result::Result<(), RateReject> {
        if account == 0 {
            return Ok(());
        }
        for i in 0..self.account_cooldown_len {
            let entry = self.account_cooldowns[i];
            if entry.account == account {
                if entry.frozen {
                    return Err(RateReject::Frozen { account });
                }
                if now_ns < entry.until_ns {
                    return Err(RateReject::AccountCooldown {
                        account,
                        until_ns: entry.until_ns,
                        retry_after_ns: entry.until_ns - now_ns,
                    });
                }
                break;
            }
        }
        Ok(())
    }

    #[inline]
    pub fn check_ip(&self, local_ip: IpKey, now_ns: u64) -> core::result::Result<(), RateReject> {
        if local_ip.is_none() {
            return Ok(());
        }
        for i in 0..self.ip_cooldown_len {
            let entry = self.ip_cooldowns[i];
            if entry.local_ip == local_ip {
                if now_ns < entry.until_ns {
                    return Err(RateReject::IpCooldown {
                        local_ip,
                        until_ns: entry.until_ns,
                        retry_after_ns: entry.until_ns - now_ns,
                    });
                }
                break;
            }
        }
        Ok(())
    }

    #[inline]
    fn precheck_spends(
        &self,
        spends: &[Spend],
        now_ns: u64,
    ) -> core::result::Result<[usize; MAX_SPENDS_PER_CALL], RateReject> {
        if spends.is_empty() || spends.len() > MAX_SPENDS_PER_CALL {
            return Err(RateReject::Invalid);
        }
        for spend in spends {
            if spend.units == 0 {
                return Err(RateReject::Invalid);
            }
        }
        if now_ns < self.last_now_ns {
            return Err(RateReject::ClockRegression);
        }
        for spend in spends {
            self.check_account(spend.id.account, now_ns)?;
            self.check_ip(spend.id.local_ip, now_ns)?;
        }
        let mut indices = [0usize; MAX_SPENDS_PER_CALL];
        for (i, spend) in spends.iter().enumerate() {
            indices[i] = self
                .lookup_index(spend.id)
                .ok_or(RateReject::UnknownWindow { id: spend.id })?;
        }
        Ok(indices)
    }

    /// Read-only probe checking whether `spends` would succeed at `now_ns` without mutating any window.
    pub fn can_afford(
        &self,
        spends: &[Spend],
        now_ns: u64,
    ) -> core::result::Result<(), RateReject> {
        let indices = self.precheck_spends(spends, now_ns)?;
        let mut accumulated = [(0usize, 0u64); MAX_SPENDS_PER_CALL];
        let mut unique_len = 0usize;
        for (i, spend) in spends.iter().enumerate() {
            let win_idx = indices[i];
            let mut found = None;
            for (slot_idx, item) in accumulated.iter().enumerate().take(unique_len) {
                if item.0 == win_idx {
                    found = Some(slot_idx);
                    break;
                }
            }
            let prior_units = found.map_or(0, |slot| accumulated[slot].1);
            self.windows[win_idx].can_take_readonly(prior_units, spend.units, now_ns)?;
            let u = u64::from(spend.units);
            match found {
                Some(slot) => {
                    accumulated[slot].1 = accumulated[slot]
                        .1
                        .checked_add(u)
                        .ok_or(RateReject::Invalid)?;
                }
                None => {
                    accumulated[unique_len] = (win_idx, u);
                    unique_len += 1;
                }
            }
        }
        Ok(())
    }

    #[inline]
    fn advance_clock_only(&mut self, now_ns: u64) -> core::result::Result<(), RateReject> {
        if now_ns < self.last_now_ns {
            return Err(RateReject::ClockRegression);
        }
        self.last_now_ns = now_ns;
        Ok(())
    }

    /// Transactionally debits all `spends` at `now_ns`, rolling back every earlier debit via
    /// `put_back` in reverse order if any window rejects.
    pub fn allow(
        &mut self,
        spends: &[Spend],
        now_ns: u64,
    ) -> core::result::Result<RatePermit, RateReject> {
        let indices = self.precheck_spends(spends, now_ns)?;
        self.last_now_ns = now_ns;
        let mut undo_stack = [(0usize, WindowUndo::Fixed { prev_used: 0 }); MAX_SPENDS_PER_CALL];
        let mut applied = 0usize;
        for (i, spend) in spends.iter().enumerate() {
            let win_idx = indices[i];
            match self.windows[win_idx].try_take(spend.units, now_ns) {
                Ok(undo) => {
                    undo_stack[applied] = (win_idx, undo);
                    applied += 1;
                }
                Err(reject) => {
                    while applied > 0 {
                        applied -= 1;
                        let (prev_win_idx, prev_undo) = undo_stack[applied];
                        self.windows[prev_win_idx].put_back(prev_undo);
                    }
                    return Err(reject);
                }
            }
        }
        Ok(RatePermit {
            granted_at_ns: now_ns,
            spends_applied: applied as u8,
        })
    }

    /// Reconciles exchange-reported usage (`X-MBX-USED-WEIGHT-*`, `X-MBX-ORDER-COUNT-*`, or WS `rateLimits[]`).
    ///
    /// Conservatively raises fixed-window `used` to `used.max(reported_used.min(limit))`, records
    /// rolling-window peak usage without mutating timestamp ring entries, and flags `warn` when
    /// `reported_used * 100 >= limit * exch_warn_pct`.
    pub fn reconcile_exchange_usage(
        &mut self,
        id: RateLimitId,
        reported_used: u64,
        now_ns: u64,
    ) -> core::result::Result<ExchangeUsageReport, RateReject> {
        if now_ns < self.last_now_ns {
            return Err(RateReject::ClockRegression);
        }
        let idx = self
            .lookup_index(id)
            .ok_or(RateReject::UnknownWindow { id })?;
        self.last_now_ns = now_ns;
        let window = &mut self.windows[idx];
        let limit = window.limit();
        let clamped = reported_used.min(limit);
        let warn =
            u128::from(reported_used) * 100 >= u128::from(limit) * u128::from(self.exch_warn_pct);
        let (previous_used, effective_used, newly_warned) = match &mut window.kind {
            WindowKind::Fixed(w) => {
                w.advance_to(now_ns)?;
                let prev = w.used;
                w.used = w.used.max(clamped);
                let rising = warn && !w.warn_active;
                w.warn_active = warn;
                (prev, w.used, rising)
            }
            WindowKind::Rolling(w) => {
                w.evict_expired(now_ns);
                let prev = w.used.max(w.observed_exchange_peak);
                w.observed_exchange_peak = w.observed_exchange_peak.max(clamped);
                let eff = w.used.max(w.observed_exchange_peak);
                let rising = warn && !w.warn_active;
                w.warn_active = warn;
                (prev, eff, rising)
            }
        };
        Ok(ExchangeUsageReport {
            id,
            limit,
            previous_used,
            effective_used,
            reported_used,
            warn,
            newly_warned,
        })
    }
}

/// Institutional exchange rate-limit preset compiled from venue specifications.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum VenuePreset {
    BinanceSpot,
    BinanceUsdm,
    BybitSpot,
    OkxSpot,
}

#[derive(Clone, Copy, Debug)]
struct SymbolHandles {
    symbol: u64,
    order_id: RateLimitId,
    cancel_id: RateLimitId,
}

/// Zero-allocation typed façade over [`RateLimiter`] for one account across `1..=16` local source IPs.
#[derive(Debug)]
pub struct VenueRateLimiter {
    preset: VenuePreset,
    account: u64,
    local_ips: Vec<IpKey>,
    ip_window_ids: Vec<RateLimitId>,
    account_order_ids: Vec<RateLimitId>,
    account_cancel_id: Option<RateLimitId>,
    account_request_id: Option<RateLimitId>,
    symbols: Vec<SymbolHandles>,
    /// Preallocated rolling rings for `OkxSpot` unseen symbols so `register_symbol` / `allow_*`
    /// never allocate on the hot path.
    symbol_ring_pool: Vec<Box<[RollingEntry]>>,
    limiter: RateLimiter,
}

impl VenueRateLimiter {
    /// Constructs a preallocated [`VenueRateLimiter`] with [`DEFAULT_EXCH_WARN_PCT`] (`80%`).
    pub fn new(
        preset: VenuePreset,
        account: u64,
        local_ips: &[IpKey],
    ) -> core::result::Result<Self, RateReject> {
        Self::with_warn_pct(preset, account, local_ips, DEFAULT_EXCH_WARN_PCT)
    }

    /// Constructs a preallocated [`VenueRateLimiter`] with an explicit `exch_warn_pct` (`1..=100`).
    pub fn with_warn_pct(
        preset: VenuePreset,
        account: u64,
        local_ips: &[IpKey],
        exch_warn_pct: u8,
    ) -> core::result::Result<Self, RateReject> {
        if account == 0 || local_ips.is_empty() || local_ips.len() > MAX_VENUE_IPS {
            return Err(RateReject::Invalid);
        }
        for (i, &ip) in local_ips.iter().enumerate() {
            if ip.is_none() || local_ips[..i].contains(&ip) {
                return Err(RateReject::Invalid);
            }
        }
        let max_windows = 8 + MAX_VENUE_IPS + (2 * MAX_VENUE_SYMBOLS);
        let mut limiter = RateLimiter::with_config(max_windows, exch_warn_pct)?;
        let mut account_order_ids = Vec::with_capacity(2);
        let mut account_cancel_id = None;
        let mut account_request_id = None;
        let mut ip_window_ids = Vec::with_capacity(local_ips.len());
        let mut symbol_ring_pool = Vec::new();

        match preset {
            VenuePreset::BinanceSpot => {
                let o10 = RateRule::account(account, 10, RateFlags::ORDER, 100);
                let oday = RateRule::account(account, 86_400, RateFlags::ORDER, 200_000);
                limiter.add(o10)?;
                limiter.add(oday)?;
                account_order_ids.push(o10.id);
                account_order_ids.push(oday.id);
                for &ip in local_ips {
                    let rule =
                        RateRule::ip(ip, 60, RateFlags::REQUEST.union(RateFlags::WEIGHT), 6_000);
                    limiter.add(rule)?;
                    ip_window_ids.push(rule.id);
                }
            }
            VenuePreset::BinanceUsdm => {
                let o10 = RateRule::account(account, 10, RateFlags::ORDER, 300);
                let o60 = RateRule::account(account, 60, RateFlags::ORDER, 1_200);
                limiter.add(o10)?;
                limiter.add(o60)?;
                account_order_ids.push(o10.id);
                account_order_ids.push(o60.id);
                for &ip in local_ips {
                    let rule =
                        RateRule::ip(ip, 60, RateFlags::REQUEST.union(RateFlags::WEIGHT), 2_400);
                    limiter.add(rule)?;
                    ip_window_ids.push(rule.id);
                }
            }
            VenuePreset::BybitSpot => {
                let ord =
                    RateRule::account(account, 1, RateFlags::ORDER.union(RateFlags::ROLLING), 20);
                let cxl =
                    RateRule::account(account, 1, RateFlags::CANCEL.union(RateFlags::ROLLING), 20);
                let req =
                    RateRule::account(account, 1, RateFlags::REQUEST.union(RateFlags::ROLLING), 50);
                limiter.add(ord)?;
                limiter.add(cxl)?;
                limiter.add(req)?;
                account_order_ids.push(ord.id);
                account_cancel_id = Some(cxl.id);
                account_request_id = Some(req.id);
                for &ip in local_ips {
                    let rule =
                        RateRule::ip(ip, 5, RateFlags::REQUEST.union(RateFlags::ROLLING), 600);
                    limiter.add(rule)?;
                    ip_window_ids.push(rule.id);
                }
            }
            VenuePreset::OkxSpot => {
                let ord = RateRule::account(
                    account,
                    2,
                    RateFlags::ORDER.union(RateFlags::ROLLING),
                    1_000,
                );
                let req =
                    RateRule::account(account, 2, RateFlags::REQUEST.union(RateFlags::ROLLING), 20);
                limiter.add(ord)?;
                limiter.add(req)?;
                account_order_ids.push(ord.id);
                account_request_id = Some(req.id);
                for &ip in local_ips {
                    let rule =
                        RateRule::ip(ip, 2, RateFlags::REQUEST.union(RateFlags::ROLLING), 600);
                    limiter.add(rule)?;
                    ip_window_ids.push(rule.id);
                }
                let ring_cap = default_rolling_ring_capacity(60);
                symbol_ring_pool.reserve_exact(2 * MAX_VENUE_SYMBOLS);
                for _ in 0..(2 * MAX_VENUE_SYMBOLS) {
                    symbol_ring_pool
                        .push(vec![RollingEntry::default(); ring_cap].into_boxed_slice());
                }
            }
        }

        Ok(Self {
            preset,
            account,
            local_ips: local_ips.to_vec(),
            ip_window_ids,
            account_order_ids,
            account_cancel_id,
            account_request_id,
            symbols: Vec::with_capacity(MAX_VENUE_SYMBOLS),
            symbol_ring_pool,
            limiter,
        })
    }

    #[inline]
    pub const fn preset(&self) -> VenuePreset {
        self.preset
    }

    #[inline]
    pub const fn account(&self) -> u64 {
        self.account
    }

    #[inline]
    pub fn local_ips(&self) -> &[IpKey] {
        &self.local_ips
    }

    #[inline]
    pub fn account_order_window_ids(&self) -> &[RateLimitId] {
        &self.account_order_ids
    }

    #[inline]
    pub fn ip_window_id(&self, local_ip: IpKey) -> Option<RateLimitId> {
        self.local_ips
            .iter()
            .position(|&ip| ip == local_ip)
            .map(|idx| self.ip_window_ids[idx])
    }

    #[inline]
    pub fn symbol_order_window_id(&self, symbol: u64) -> Option<RateLimitId> {
        self.lookup_symbol(symbol).map(|s| s.order_id)
    }

    #[inline]
    pub fn symbol_cancel_window_id(&self, symbol: u64) -> Option<RateLimitId> {
        self.lookup_symbol(symbol).map(|s| s.cancel_id)
    }

    #[inline]
    pub const fn limiter(&self) -> &RateLimiter {
        &self.limiter
    }

    #[inline]
    pub fn limiter_mut(&mut self) -> &mut RateLimiter {
        &mut self.limiter
    }

    #[inline]
    fn lookup_symbol(&self, symbol: u64) -> Option<SymbolHandles> {
        self.symbols
            .binary_search_by_key(&symbol, |s| s.symbol)
            .ok()
            .map(|pos| self.symbols[pos])
    }

    /// Registers per-symbol buckets for `symbol` (`1..=MAX_VENUE_SYMBOLS` distinct symbols).
    ///
    /// On [`VenuePreset::OkxSpot`], binds preallocated `60 / 2s` rolling `ORDER` and `CANCEL`
    /// rings with zero heap allocation. On symbol-independent presets this is an idempotent
    /// capacity-checked registration.
    pub fn register_symbol(&mut self, symbol: u64) -> core::result::Result<(), RateReject> {
        if symbol == 0 {
            return Err(RateReject::Invalid);
        }
        let insert_pos = match self.symbols.binary_search_by_key(&symbol, |s| s.symbol) {
            Ok(_) => return Ok(()),
            Err(pos) => pos,
        };
        if self.symbols.len() >= MAX_VENUE_SYMBOLS {
            return Err(RateReject::Capacity);
        }
        let handles = match self.preset {
            VenuePreset::OkxSpot => {
                let ord_rule = RateRule::symbol(
                    self.account,
                    2,
                    RateFlags::ORDER.union(RateFlags::ROLLING),
                    60,
                    symbol,
                );
                let cxl_rule = RateRule::symbol(
                    self.account,
                    2,
                    RateFlags::CANCEL.union(RateFlags::ROLLING),
                    60,
                    symbol,
                );
                let ord_ring = self.symbol_ring_pool.pop().ok_or(RateReject::Capacity)?;
                let cxl_ring = self.symbol_ring_pool.pop().ok_or(RateReject::Capacity)?;
                self.limiter
                    .register_rolling_with_preallocated_ring(ord_rule, ord_ring)?;
                self.limiter
                    .register_rolling_with_preallocated_ring(cxl_rule, cxl_ring)?;
                SymbolHandles {
                    symbol,
                    order_id: ord_rule.id,
                    cancel_id: cxl_rule.id,
                }
            }
            VenuePreset::BinanceSpot | VenuePreset::BinanceUsdm | VenuePreset::BybitSpot => {
                SymbolHandles {
                    symbol,
                    order_id: RateLimitId::default(),
                    cancel_id: RateLimitId::default(),
                }
            }
        };
        self.symbols.insert(insert_pos, handles);
        Ok(())
    }

    /// Applies an HTTP `429` / per-IP cooldown strictly to `local_ip`.
    pub fn note_ip_cooldown(
        &mut self,
        local_ip: IpKey,
        until_ns: u64,
    ) -> core::result::Result<(), RateReject> {
        if self.ip_window_id(local_ip).is_none() {
            return Err(RateReject::Invalid);
        }
        self.limiter.note_ip_cooldown(local_ip, until_ns)
    }

    /// Applies an account-wide cooldown across all bound source IPs.
    pub fn note_account_cooldown(&mut self, until_ns: u64) -> core::result::Result<(), RateReject> {
        self.limiter.note_account_cooldown(self.account, until_ns)
    }

    /// Permanently freezes the account across all bound source IPs (e.g. HTTP `418` ban).
    pub fn freeze_account(&mut self) -> core::result::Result<(), RateReject> {
        self.limiter.freeze_account(self.account)
    }

    #[inline]
    pub fn is_account_frozen(&self) -> bool {
        self.limiter.is_account_frozen(self.account)
    }

    /// Reconciles exchange-reported window usage against `id`.
    pub fn reconcile_exchange_usage(
        &mut self,
        id: RateLimitId,
        reported_used: u64,
        now_ns: u64,
    ) -> core::result::Result<ExchangeUsageReport, RateReject> {
        self.limiter
            .reconcile_exchange_usage(id, reported_used, now_ns)
    }

    #[inline]
    fn precheck_route(
        &self,
        local_ip: IpKey,
        now_ns: u64,
    ) -> core::result::Result<RateLimitId, RateReject> {
        let ip_win = self.ip_window_id(local_ip).ok_or(RateReject::Invalid)?;
        if now_ns < self.limiter.last_now_ns() {
            return Err(RateReject::ClockRegression);
        }
        self.limiter.check_account(self.account, now_ns)?;
        self.limiter.check_ip(local_ip, now_ns)?;
        Ok(ip_win)
    }

    fn build_place_spends(
        &self,
        ip_win: RateLimitId,
        symbol: u64,
        order_units: u32,
        weight: u32,
    ) -> core::result::Result<([Spend; 4], usize), RateReject> {
        if order_units == 0 {
            return Err(RateReject::Invalid);
        }
        let mut buf = [Spend::new(RateLimitId::default(), 0); 4];
        let len = match self.preset {
            VenuePreset::BinanceSpot => {
                if weight == 0 {
                    return Err(RateReject::Invalid);
                }
                buf[0] = Spend::new(self.account_order_ids[0], order_units);
                buf[1] = Spend::new(self.account_order_ids[1], order_units);
                buf[2] = Spend::new(ip_win, weight);
                3
            }
            VenuePreset::BinanceUsdm => {
                buf[0] = Spend::new(self.account_order_ids[0], order_units);
                buf[1] = Spend::new(self.account_order_ids[1], order_units);
                if weight > 0 {
                    buf[2] = Spend::new(ip_win, weight);
                    3
                } else {
                    2
                }
            }
            VenuePreset::BybitSpot => {
                if weight == 0 {
                    return Err(RateReject::Invalid);
                }
                buf[0] = Spend::new(self.account_order_ids[0], order_units);
                buf[1] = Spend::new(ip_win, 1);
                2
            }
            VenuePreset::OkxSpot => {
                if symbol == 0 || weight == 0 {
                    return Err(RateReject::Invalid);
                }
                let sym = self.lookup_symbol(symbol).ok_or(RateReject::Invalid)?;
                buf[0] = Spend::new(self.account_order_ids[0], order_units);
                buf[1] = Spend::new(sym.order_id, order_units);
                buf[2] = Spend::new(ip_win, 1);
                3
            }
        };
        Ok((buf, len))
    }

    /// Admits a single-order placement on `local_ip`.
    pub fn allow_place(
        &mut self,
        local_ip: IpKey,
        symbol: u64,
        weight: u32,
        now_ns: u64,
    ) -> core::result::Result<RatePermit, RateReject> {
        self.allow_place_orders(local_ip, symbol, 1, weight, now_ns)
    }

    /// Admits an order placement consuming `order_units` (e.g. `legs` for Spot OCO/OTO/OTOCO lists).
    pub fn allow_place_orders(
        &mut self,
        local_ip: IpKey,
        symbol: u64,
        order_units: u32,
        weight: u32,
        now_ns: u64,
    ) -> core::result::Result<RatePermit, RateReject> {
        let ip_win = self.precheck_route(local_ip, now_ns)?;
        if self.preset == VenuePreset::OkxSpot {
            if symbol == 0 {
                return Err(RateReject::Invalid);
            }
            if self.lookup_symbol(symbol).is_none() {
                self.register_symbol(symbol)?;
            }
        }
        let (spends, len) = self.build_place_spends(ip_win, symbol, order_units, weight)?;
        self.limiter.allow(&spends[..len], now_ns)
    }

    /// Read-only probe for [`Self::allow_place`].
    pub fn can_afford_place(
        &self,
        local_ip: IpKey,
        symbol: u64,
        weight: u32,
        now_ns: u64,
    ) -> core::result::Result<(), RateReject> {
        self.can_afford_place_orders(local_ip, symbol, 1, weight, now_ns)
    }

    /// Read-only probe for [`Self::allow_place_orders`].
    pub fn can_afford_place_orders(
        &self,
        local_ip: IpKey,
        symbol: u64,
        order_units: u32,
        weight: u32,
        now_ns: u64,
    ) -> core::result::Result<(), RateReject> {
        let ip_win = self.precheck_route(local_ip, now_ns)?;
        if self.preset == VenuePreset::OkxSpot && symbol == 0 {
            return Err(RateReject::Invalid);
        }
        if self.preset == VenuePreset::OkxSpot && self.lookup_symbol(symbol).is_none() {
            if self.symbols.len() >= MAX_VENUE_SYMBOLS
                || self.limiter.window_count().saturating_add(2) > self.limiter.max_windows
            {
                return Err(RateReject::Capacity);
            }
            if order_units == 0 || weight == 0 {
                return Err(RateReject::Invalid);
            }
            self.limiter.can_afford(
                &[Spend::new(self.account_order_ids[0], order_units)],
                now_ns,
            )?;
            if u64::from(order_units) > 60 {
                let sym_ord_rule = RateRule::symbol(
                    self.account,
                    2,
                    RateFlags::ORDER.union(RateFlags::ROLLING),
                    60,
                    symbol,
                );
                return Err(RateReject::Exhausted {
                    id: sym_ord_rule.id,
                    limit: 60,
                    used: 0,
                    requested: order_units,
                    retry_after_ns: 2 * NS_PER_SEC,
                });
            }
            return self.limiter.can_afford(&[Spend::new(ip_win, 1)], now_ns);
        }
        let (spends, len) = self.build_place_spends(ip_win, symbol, order_units, weight)?;
        self.limiter.can_afford(&spends[..len], now_ns)
    }

    fn build_modify_spends(
        &self,
        ip_win: RateLimitId,
        symbol: u64,
        weight: u32,
        counts_as_order: bool,
    ) -> core::result::Result<([Spend; 4], usize), RateReject> {
        let mut buf = [Spend::new(RateLimitId::default(), 0); 4];
        let mut len = 0usize;
        match self.preset {
            VenuePreset::BinanceSpot => {
                if weight == 0 {
                    return Err(RateReject::Invalid);
                }
                if counts_as_order {
                    buf[0] = Spend::new(self.account_order_ids[0], 1);
                    buf[1] = Spend::new(self.account_order_ids[1], 1);
                    buf[2] = Spend::new(ip_win, weight);
                    len = 3;
                } else {
                    buf[0] = Spend::new(ip_win, weight);
                    len = 1;
                }
            }
            VenuePreset::BinanceUsdm => {
                if counts_as_order {
                    buf[0] = Spend::new(self.account_order_ids[0], 1);
                    buf[1] = Spend::new(self.account_order_ids[1], 1);
                    len = 2;
                }
                if weight > 0 {
                    buf[len] = Spend::new(ip_win, weight);
                    len += 1;
                }
            }
            VenuePreset::BybitSpot => {
                if weight == 0 {
                    return Err(RateReject::Invalid);
                }
                let acct_win = if counts_as_order {
                    self.account_order_ids[0]
                } else {
                    self.account_request_id.ok_or(RateReject::Invalid)?
                };
                buf[0] = Spend::new(acct_win, 1);
                buf[1] = Spend::new(ip_win, 1);
                len = 2;
            }
            VenuePreset::OkxSpot => {
                if symbol == 0 || weight == 0 {
                    return Err(RateReject::Invalid);
                }
                if counts_as_order {
                    let sym = self.lookup_symbol(symbol).ok_or(RateReject::Invalid)?;
                    buf[0] = Spend::new(self.account_order_ids[0], 1);
                    buf[1] = Spend::new(sym.order_id, 1);
                    buf[2] = Spend::new(ip_win, 1);
                    len = 3;
                } else {
                    let acct_req = self.account_request_id.ok_or(RateReject::Invalid)?;
                    buf[0] = Spend::new(acct_req, 1);
                    buf[1] = Spend::new(ip_win, 1);
                    len = 2;
                }
            }
        }
        Ok((buf, len))
    }

    /// Admits an order modification / amendment on `local_ip`, charging `ORDER` windows iff `counts_as_order`.
    pub fn allow_modify(
        &mut self,
        local_ip: IpKey,
        symbol: u64,
        weight: u32,
        counts_as_order: bool,
        now_ns: u64,
    ) -> core::result::Result<RatePermit, RateReject> {
        let ip_win = self.precheck_route(local_ip, now_ns)?;
        if self.preset == VenuePreset::OkxSpot && counts_as_order {
            if symbol == 0 {
                return Err(RateReject::Invalid);
            }
            if self.lookup_symbol(symbol).is_none() {
                self.register_symbol(symbol)?;
            }
        }
        let (spends, len) = self.build_modify_spends(ip_win, symbol, weight, counts_as_order)?;
        if len == 0 {
            self.limiter.advance_clock_only(now_ns)?;
            return Ok(RatePermit {
                granted_at_ns: now_ns,
                spends_applied: 0,
            });
        }
        self.limiter.allow(&spends[..len], now_ns)
    }

    /// Read-only probe for [`Self::allow_modify`].
    pub fn can_afford_modify(
        &self,
        local_ip: IpKey,
        symbol: u64,
        weight: u32,
        counts_as_order: bool,
        now_ns: u64,
    ) -> core::result::Result<(), RateReject> {
        let ip_win = self.precheck_route(local_ip, now_ns)?;
        if self.preset == VenuePreset::OkxSpot && counts_as_order {
            if symbol == 0 {
                return Err(RateReject::Invalid);
            }
            if self.lookup_symbol(symbol).is_none() {
                if self.symbols.len() >= MAX_VENUE_SYMBOLS
                    || self.limiter.window_count().saturating_add(2) > self.limiter.max_windows
                {
                    return Err(RateReject::Capacity);
                }
                if weight == 0 {
                    return Err(RateReject::Invalid);
                }
                let spends = [
                    Spend::new(self.account_order_ids[0], 1),
                    Spend::new(ip_win, 1),
                ];
                return self.limiter.can_afford(&spends, now_ns);
            }
        }
        let (spends, len) = self.build_modify_spends(ip_win, symbol, weight, counts_as_order)?;
        if len == 0 {
            return Ok(());
        }
        self.limiter.can_afford(&spends[..len], now_ns)
    }

    /// Admits an order cancellation on `local_ip`.
    ///
    /// Strictly isolated from `ORDER` windows: never touches account or symbol `ORDER` caps!
    pub fn allow_cancel(
        &mut self,
        local_ip: IpKey,
        symbol: u64,
        weight: u32,
        now_ns: u64,
    ) -> core::result::Result<RatePermit, RateReject> {
        let ip_win = self.precheck_route(local_ip, now_ns)?;
        match self.preset {
            VenuePreset::BinanceSpot => {
                if weight == 0 {
                    return Err(RateReject::Invalid);
                }
                let spends = [Spend::new(ip_win, weight)];
                self.limiter.allow(&spends, now_ns)
            }
            VenuePreset::BinanceUsdm => {
                if weight == 0 {
                    self.limiter.advance_clock_only(now_ns)?;
                    return Ok(RatePermit {
                        granted_at_ns: now_ns,
                        spends_applied: 0,
                    });
                }
                let spends = [Spend::new(ip_win, weight)];
                self.limiter.allow(&spends, now_ns)
            }
            VenuePreset::BybitSpot => {
                if weight == 0 {
                    return Err(RateReject::Invalid);
                }
                let cxl_id = self.account_cancel_id.ok_or(RateReject::Invalid)?;
                let spends = [Spend::new(cxl_id, 1), Spend::new(ip_win, 1)];
                self.limiter.allow(&spends, now_ns)
            }
            VenuePreset::OkxSpot => {
                if symbol == 0 || weight == 0 {
                    return Err(RateReject::Invalid);
                }
                if self.lookup_symbol(symbol).is_none() {
                    self.register_symbol(symbol)?;
                }
                let sym = self.lookup_symbol(symbol).ok_or(RateReject::Invalid)?;
                let spends = [Spend::new(sym.cancel_id, 1), Spend::new(ip_win, 1)];
                self.limiter.allow(&spends, now_ns)
            }
        }
    }

    /// Read-only probe for [`Self::allow_cancel`].
    pub fn can_afford_cancel(
        &self,
        local_ip: IpKey,
        symbol: u64,
        weight: u32,
        now_ns: u64,
    ) -> core::result::Result<(), RateReject> {
        let ip_win = self.precheck_route(local_ip, now_ns)?;
        match self.preset {
            VenuePreset::BinanceSpot => {
                if weight == 0 {
                    return Err(RateReject::Invalid);
                }
                let spends = [Spend::new(ip_win, weight)];
                self.limiter.can_afford(&spends, now_ns)
            }
            VenuePreset::BinanceUsdm => {
                if weight == 0 {
                    return Ok(());
                }
                let spends = [Spend::new(ip_win, weight)];
                self.limiter.can_afford(&spends, now_ns)
            }
            VenuePreset::BybitSpot => {
                if weight == 0 {
                    return Err(RateReject::Invalid);
                }
                let cxl_id = self.account_cancel_id.ok_or(RateReject::Invalid)?;
                let spends = [Spend::new(cxl_id, 1), Spend::new(ip_win, 1)];
                self.limiter.can_afford(&spends, now_ns)
            }
            VenuePreset::OkxSpot => {
                if symbol == 0 || weight == 0 {
                    return Err(RateReject::Invalid);
                }
                if let Some(sym) = self.lookup_symbol(symbol) {
                    let spends = [Spend::new(sym.cancel_id, 1), Spend::new(ip_win, 1)];
                    self.limiter.can_afford(&spends, now_ns)
                } else {
                    if self.symbols.len() >= MAX_VENUE_SYMBOLS
                        || self.limiter.window_count().saturating_add(2) > self.limiter.max_windows
                    {
                        return Err(RateReject::Capacity);
                    }
                    let spends = [Spend::new(ip_win, 1)];
                    self.limiter.can_afford(&spends, now_ns)
                }
            }
        }
    }

    /// Admits a general REST/WebSocket query on `local_ip`.
    ///
    /// Strictly isolated from `ORDER` and `CANCEL` windows!
    pub fn allow_query(
        &mut self,
        local_ip: IpKey,
        weight: u32,
        now_ns: u64,
    ) -> core::result::Result<RatePermit, RateReject> {
        let ip_win = self.precheck_route(local_ip, now_ns)?;
        match self.preset {
            VenuePreset::BinanceSpot | VenuePreset::BinanceUsdm => {
                if weight == 0 {
                    return Err(RateReject::Invalid);
                }
                let spends = [Spend::new(ip_win, weight)];
                self.limiter.allow(&spends, now_ns)
            }
            VenuePreset::BybitSpot | VenuePreset::OkxSpot => {
                if weight == 0 {
                    return Err(RateReject::Invalid);
                }
                let req_id = self.account_request_id.ok_or(RateReject::Invalid)?;
                let spends = [Spend::new(req_id, 1), Spend::new(ip_win, 1)];
                self.limiter.allow(&spends, now_ns)
            }
        }
    }

    /// Read-only probe for [`Self::allow_query`].
    pub fn can_afford_query(
        &self,
        local_ip: IpKey,
        weight: u32,
        now_ns: u64,
    ) -> core::result::Result<(), RateReject> {
        let ip_win = self.precheck_route(local_ip, now_ns)?;
        match self.preset {
            VenuePreset::BinanceSpot | VenuePreset::BinanceUsdm => {
                if weight == 0 {
                    return Err(RateReject::Invalid);
                }
                let spends = [Spend::new(ip_win, weight)];
                self.limiter.can_afford(&spends, now_ns)
            }
            VenuePreset::BybitSpot | VenuePreset::OkxSpot => {
                if weight == 0 {
                    return Err(RateReject::Invalid);
                }
                let req_id = self.account_request_id.ok_or(RateReject::Invalid)?;
                let spends = [Spend::new(req_id, 1), Spend::new(ip_win, 1)];
                self.limiter.can_afford(&spends, now_ns)
            }
        }
    }
}
