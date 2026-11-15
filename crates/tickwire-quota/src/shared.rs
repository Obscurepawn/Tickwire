//! Process-wide cross-account lock-free atomic rate limiter (`SharedRateLimiter`) for multi-session
//! ORS execution (`REQ`, `RESP`, and `AsyncRestQuerier`).
//!
//! Shares source-IP budgets (`RateRule::ip`, `account = 0`) across all `AccountSession`s bound to
//! the same `local_ip`, isolates per-account `(account_ref, product / symbol / family)` fixed and
//! sub-bucket sliding (`RateFlags::ROLLING`) windows with atomic delta rollback, enforces
//! single-pick route admission (`pick -> spend(conn.local_ip) -> NACK immediately`), compiles
//! institutional defaults for Binance (Spot, USD-M, COIN-M, Options), OKX UTA, and Bybit UTA, and
//! emits rising-edge `ExchRateLimitEvent` notifications (`80%` warning and `100%` ceiling) from
//! venue `rateLimits[]` frames using exact integer arithmetic (`#![forbid(unsafe_code)]`).

use crate::rate_limiter::{
    DEFAULT_EXCH_WARN_PCT, IpKey, MAX_SPENDS_PER_CALL, MAX_TRACKED_ACCOUNTS, MAX_TRACKED_IPS,
    RateFlags, RateLimitId, RatePermit, RateReject, RateRule,
};
use core::sync::atomic::{AtomicBool, AtomicU8, AtomicU32, AtomicU64, Ordering};
use std::net::{IpAddr, Ipv4Addr};
use tickwire_venue::OrderRoute;

const NS_PER_SEC: u64 = 1_000_000_000;

/// Default power-of-two slot capacity of [`SharedRateLimiter`] (`4,096` slots).
pub const DEFAULT_SHARED_TABLE_SLOTS: usize = 4096;

/// Minimum power-of-two slot capacity of [`SharedRateLimiter`].
pub const MIN_SHARED_TABLE_SLOTS: usize = 64;

/// Maximum power-of-two slot capacity of [`SharedRateLimiter`].
pub const MAX_SHARED_TABLE_SLOTS: usize = 65_536;

/// Number of atomic sub-buckets partitioning each [`RateFlags::ROLLING`] interval (`20`).
pub const ROLLING_SUB_BUCKETS: usize = 20;

const SLOT_EMPTY: u8 = 0;
const SLOT_CLAIMING: u8 = 1;
const SLOT_READY: u8 = 2;

const FIXED_UNINIT: u64 = u64::MAX;
const ROLLING_COUNT_BITS: u32 = 28;
const ROLLING_COUNT_MASK: u64 = (1u64 << ROLLING_COUNT_BITS) - 1;
const ROLLING_MAX_TICK: u64 = (1u64 << (64 - ROLLING_COUNT_BITS)) - 1;

const LATCH_WARN_ACTIVE: u8 = 1 << 0;
const LATCH_CEIL_ACTIVE: u8 = 1 << 1;

/// Per-egress-IP rate-limit budget override (`ORS.RATE_LIMITER.ips = [{ ip, ip_weight_min }]`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IpBudgetSpec {
    pub ip: IpAddr,
    pub ip_weight_min: u64,
}

impl IpBudgetSpec {
    #[inline]
    pub const fn new(ip: IpAddr, ip_weight_min: u64) -> Self {
        Self { ip, ip_weight_min }
    }
}

/// Built-in ORS venue rate-limit profile covering Binance (Spot, USD-M, COIN-M, Options),
/// OKX UTA, and Bybit UTA (`human-docs/5.2 venue-limits.md`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum OrsVenueProfile {
    BinanceSpot,
    BinanceUsdm,
    BinanceCoinm,
    BinanceOptions,
    OkxUta,
    BybitUta,
}

/// Compiled default rate-limit parameters for an [`OrsVenueProfile`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct VenueRateDefaults {
    pub profile: OrsVenueProfile,
    /// Primary account order window `(interval_s, limit)`, if applicable.
    pub account_order_primary: Option<(u32, u64, RateFlags)>,
    /// Secondary account order window `(interval_s, limit)`, if applicable.
    pub account_order_secondary: Option<(u32, u64, RateFlags)>,
    /// Account-wide general request window `(interval_s, limit)`, if applicable.
    pub account_request: Option<(u32, u64, RateFlags)>,
    /// Per-`(account, symbol/family)` order & cancel windows `(interval_s, order_limit, cancel_limit, flags)`.
    pub per_symbol_order_cancel: Option<(u32, u64, u64, RateFlags)>,
    /// Per-`(account, product)` order & cancel windows `(interval_s, order_limit, cancel_limit, flags)`.
    pub per_product_order_cancel: Option<(u32, u64, u64, RateFlags)>,
    /// Shared per-IP weight/request window `(interval_s, default_ip_weight_limit, flags)`.
    pub shared_ip_budget: (u32, u64, RateFlags),
    /// Whether standard WebSocket new-order placement spends `0` IP weight by default (e.g. COIN-M).
    pub default_ws_order_ip_weight: u32,
    /// Default REST order/cancel/query IP weight.
    pub default_rest_ip_weight: u32,
}

impl OrsVenueProfile {
    /// Returns the compiled institutional rate-limit table for this venue profile.
    #[inline]
    pub const fn defaults(self) -> VenueRateDefaults {
        let ip_fixed_weight = RateFlags::REQUEST.union(RateFlags::WEIGHT);
        let ip_rolling_weight = ip_fixed_weight.union(RateFlags::ROLLING);
        match self {
            Self::BinanceSpot => VenueRateDefaults {
                profile: self,
                account_order_primary: Some((10, 100, RateFlags::ORDER)),
                account_order_secondary: Some((86_400, 200_000, RateFlags::ORDER)),
                account_request: None,
                per_symbol_order_cancel: None,
                per_product_order_cancel: None,
                shared_ip_budget: (60, 6_000, ip_fixed_weight),
                default_ws_order_ip_weight: 1,
                default_rest_ip_weight: 1,
            },
            Self::BinanceUsdm => VenueRateDefaults {
                profile: self,
                account_order_primary: Some((10, 300, RateFlags::ORDER)),
                account_order_secondary: Some((60, 1_200, RateFlags::ORDER)),
                account_request: None,
                per_symbol_order_cancel: None,
                per_product_order_cancel: None,
                shared_ip_budget: (60, 2_400, ip_fixed_weight),
                default_ws_order_ip_weight: 0,
                default_rest_ip_weight: 1,
            },
            Self::BinanceCoinm => VenueRateDefaults {
                profile: self,
                account_order_primary: Some((60, 1_200, RateFlags::ORDER)),
                account_order_secondary: None,
                account_request: None,
                per_symbol_order_cancel: None,
                per_product_order_cancel: None,
                shared_ip_budget: (60, 2_400, ip_fixed_weight),
                default_ws_order_ip_weight: 0,
                default_rest_ip_weight: 1,
            },
            Self::BinanceOptions => VenueRateDefaults {
                profile: self,
                account_order_primary: Some((10, 100, RateFlags::ORDER)),
                account_order_secondary: Some((60, 1_200, RateFlags::ORDER)),
                account_request: None,
                per_symbol_order_cancel: None,
                per_product_order_cancel: None,
                shared_ip_budget: (60, 2_400, ip_fixed_weight),
                default_ws_order_ip_weight: 0,
                default_rest_ip_weight: 1,
            },
            Self::OkxUta => VenueRateDefaults {
                profile: self,
                account_order_primary: Some((2, 1_000, RateFlags::ORDER)),
                account_order_secondary: None,
                account_request: Some((2, 20, RateFlags::REQUEST)),
                per_symbol_order_cancel: Some((2, 60, 60, RateFlags::EMPTY)),
                per_product_order_cancel: None,
                shared_ip_budget: (2, 600, ip_fixed_weight),
                default_ws_order_ip_weight: 0,
                default_rest_ip_weight: 1,
            },
            Self::BybitUta => VenueRateDefaults {
                profile: self,
                account_order_primary: None,
                account_order_secondary: None,
                account_request: Some((1, 50, RateFlags::REQUEST.union(RateFlags::ROLLING))),
                per_symbol_order_cancel: None,
                per_product_order_cancel: Some((1, 20, 20, RateFlags::ROLLING)),
                shared_ip_budget: (5, 600, ip_rolling_weight),
                default_ws_order_ip_weight: 1,
                default_rest_ip_weight: 1,
            },
        }
    }
}

/// Optional per-account cap overrides (`ACCOUNT_MANAGER.<name>.rate_limit`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AccountRateOverrides {
    pub primary_order_limit: Option<u64>,
    pub secondary_order_limit: Option<u64>,
    pub account_request_limit: Option<u64>,
    pub per_symbol_order_limit: Option<u64>,
    pub per_symbol_cancel_limit: Option<u64>,
    pub per_product_order_limit: Option<u64>,
    pub per_product_cancel_limit: Option<u64>,
}

/// Instrument scope passed when spending an [`OrsAccountQuota`] action.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum InstrumentScope {
    #[default]
    None,
    /// Per-instrument symbol key (`OKX` Spot/Swap/Futures `60/2s`).
    Symbol(u64),
    /// Per-option-family key (`OKX` Options `60/2s` family bucket).
    Family(u64),
    /// Per-product-line key (`Bybit` UTA: `1` = Spot, `2` = Derivatives, `3` = Options).
    Product(u64),
}

/// Direct O(1) slot handle returned by [`SharedRateLimiter::add`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SharedWindowHandle {
    slot: u32,
    id: RateLimitId,
}

impl SharedWindowHandle {
    #[inline]
    pub const fn id(self) -> RateLimitId {
        self.id
    }

    #[inline]
    pub const fn slot_index(self) -> usize {
        self.slot as usize
    }
}

/// Target of a [`SharedSpend`]: either a pre-resolved [`SharedWindowHandle`] (`window_ != null`)
/// or an on-the-fly [`RateRule`] (`window_ == null`, resolved/inserted on first send).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SpendTarget {
    Handle(SharedWindowHandle),
    Rule(RateRule),
}

/// Single window debit within [`SharedRateLimiter::allow`]. Entries with `units == 0` are skipped.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SharedSpend {
    pub target: SpendTarget,
    pub units: u32,
}

impl SharedSpend {
    #[inline]
    pub const fn handle(handle: SharedWindowHandle, units: u32) -> Self {
        Self {
            target: SpendTarget::Handle(handle),
            units,
        }
    }

    #[inline]
    pub const fn rule(rule: RateRule, units: u32) -> Self {
        Self {
            target: SpendTarget::Rule(rule),
            units,
        }
    }
}

/// Immediate single-pick admission rejection (`pick -> spend(conn.local_ip) -> NACK immediately`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RouteAdmissionReject {
    /// Router had no ready connection matching the requested `product_mask`.
    NoReadyConnection,
    /// Chosen connection lacked a valid bound IPv4/IPv6 `local_ip` when required.
    MissingLocalIp { conn_id: u16 },
    /// Chosen connection's account or `local_ip` rate limit rejected—MUST NACK immediately without
    /// retrying any other connection!
    RateLimitedNack(RateReject),
}

impl From<RateReject> for RouteAdmissionReject {
    #[inline]
    fn from(err: RateReject) -> Self {
        Self::RateLimitedNack(err)
    }
}

/// Rising-edge severity of an exchange-reported `rateLimits[]` threshold crossing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExchRateLimitLevel {
    /// Crossed from `< exch_warn_pct` to `>= exch_warn_pct` (default `80%`).
    WarnThreshold,
    /// Reached or exceeded `100%` (`count >= limit`).
    CeilingReached,
}

/// Rising-edge notification emitted by [`SharedRateLimiter::reconcile_venue_usage`] for strategy fan-out.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExchRateLimitEvent {
    pub id: RateLimitId,
    pub count: u64,
    pub limit: u64,
    pub warn_pct: u8,
    pub level: ExchRateLimitLevel,
    pub crossed_warn: bool,
    pub crossed_ceiling: bool,
    pub observed_at_ns: u64,
}

/// Input row for [`SharedRateLimiter::reconcile_venue_usage_batch`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct VenueRateLimitRow {
    pub id: RateLimitId,
    pub count: u64,
    pub limit: u64,
}

#[derive(Clone, Copy, Debug)]
enum SharedUndo {
    Fixed {
        slot: usize,
        epoch: u32,
        units: u32,
    },
    Rolling {
        slot: usize,
        sub_idx: usize,
        booked_tick: u64,
        units: u32,
    },
}

struct AtomicFixedWindow {
    /// Packs `(epoch: u32, used: u32)`; `FIXED_UNINIT` (`u64::MAX`) before first use.
    packed: AtomicU64,
}

impl AtomicFixedWindow {
    const fn new() -> Self {
        Self {
            packed: AtomicU64::new(FIXED_UNINIT),
        }
    }

    #[inline]
    fn used_at(&self, interval_ns: u64, now_ns: u64) -> u64 {
        let epoch = (now_ns / interval_ns) as u32;
        let raw = self.packed.load(Ordering::Acquire);
        if raw == FIXED_UNINIT {
            return 0;
        }
        let cur_epoch = (raw >> 32) as u32;
        if cur_epoch == epoch {
            raw & 0xffff_ffff
        } else {
            0
        }
    }

    #[inline]
    fn try_take(
        &self,
        slot: usize,
        id: RateLimitId,
        interval_ns: u64,
        limit: u64,
        units: u32,
        now_ns: u64,
    ) -> core::result::Result<SharedUndo, RateReject> {
        let epoch_u64 = now_ns / interval_ns;
        let epoch = epoch_u64 as u32;
        let req = u64::from(units);
        let mut raw = self.packed.load(Ordering::Acquire);
        loop {
            let eff_used = if raw == FIXED_UNINIT {
                0
            } else {
                let cur_epoch = (raw >> 32) as u32;
                if epoch < cur_epoch && (cur_epoch - epoch) < (u32::MAX / 2) {
                    return Err(RateReject::ClockRegression);
                }
                if epoch == cur_epoch {
                    raw & 0xffff_ffff
                } else {
                    0
                }
            };
            let Some(next_used) = eff_used.checked_add(req).filter(|&sum| sum <= limit) else {
                let next_epoch_ns = epoch_u64.saturating_add(1).saturating_mul(interval_ns);
                return Err(RateReject::Exhausted {
                    id,
                    limit,
                    used: eff_used,
                    requested: units,
                    retry_after_ns: next_epoch_ns.saturating_sub(now_ns),
                });
            };
            let next_raw = ((epoch as u64) << 32) | (next_used & 0xffff_ffff);
            match self.packed.compare_exchange_weak(
                raw,
                next_raw,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(SharedUndo::Fixed { slot, epoch, units }),
                Err(actual) => raw = actual,
            }
        }
    }

    #[inline]
    fn put_back(&self, epoch: u32, units: u32) {
        let mut raw = self.packed.load(Ordering::Acquire);
        while raw != FIXED_UNINIT {
            let cur_epoch = (raw >> 32) as u32;
            if cur_epoch != epoch {
                break;
            }
            let cur_used = (raw & 0xffff_ffff) as u32;
            let next_used = cur_used.saturating_sub(units);
            let next_raw = ((epoch as u64) << 32) | u64::from(next_used);
            match self.packed.compare_exchange_weak(
                raw,
                next_raw,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual) => raw = actual,
            }
        }
    }
}

struct AtomicRollingWindow {
    /// Conserved sum of active units across `buckets[0..ROLLING_SUB_BUCKETS]`.
    in_window_sum: AtomicU64,
    /// Each word packs `(sub_tick: 36 bits) << 28 | (count: 28 bits)`.
    buckets: [AtomicU64; ROLLING_SUB_BUCKETS],
}

impl AtomicRollingWindow {
    fn new() -> Self {
        Self {
            in_window_sum: AtomicU64::new(0),
            buckets: core::array::from_fn(|_| AtomicU64::new(0)),
        }
    }

    #[inline]
    fn sub_ns(interval_ns: u64) -> u64 {
        (interval_ns / (ROLLING_SUB_BUCKETS as u64)).max(1)
    }

    #[inline]
    fn evict_expired(&self, cur_tick: u64) {
        let window_ticks = ROLLING_SUB_BUCKETS as u64;
        for bucket in &self.buckets {
            let mut raw = bucket.load(Ordering::Acquire);
            while raw != 0 {
                let slot_tick = raw >> ROLLING_COUNT_BITS;
                let slot_count = raw & ROLLING_COUNT_MASK;
                if slot_count > 0 && cur_tick > slot_tick && (cur_tick - slot_tick) >= window_ticks
                {
                    match bucket.compare_exchange_weak(raw, 0, Ordering::AcqRel, Ordering::Acquire)
                    {
                        Ok(_) => {
                            self.in_window_sum.fetch_sub(slot_count, Ordering::AcqRel);
                            break;
                        }
                        Err(actual) => raw = actual,
                    }
                } else {
                    break;
                }
            }
        }
    }

    #[inline]
    fn used_at(&self, interval_ns: u64, now_ns: u64) -> u64 {
        let sub_ns = Self::sub_ns(interval_ns);
        let cur_tick = (now_ns / sub_ns) & ROLLING_MAX_TICK;
        let window_ticks = ROLLING_SUB_BUCKETS as u64;
        let mut sum = 0u64;
        for bucket in &self.buckets {
            let raw = bucket.load(Ordering::Acquire);
            if raw == 0 {
                continue;
            }
            let slot_tick = raw >> ROLLING_COUNT_BITS;
            let slot_count = raw & ROLLING_COUNT_MASK;
            if cur_tick < slot_tick || (cur_tick - slot_tick) < window_ticks {
                sum = sum.saturating_add(slot_count);
            }
        }
        sum
    }

    #[inline]
    fn retry_after_ns(&self, interval_ns: u64, cur_tick: u64, now_ns: u64) -> u64 {
        let sub_ns = Self::sub_ns(interval_ns);
        let window_ticks = ROLLING_SUB_BUCKETS as u64;
        let mut earliest_tick: Option<u64> = None;
        for bucket in &self.buckets {
            let raw = bucket.load(Ordering::Acquire);
            let slot_count = raw & ROLLING_COUNT_MASK;
            if slot_count == 0 {
                continue;
            }
            let slot_tick = raw >> ROLLING_COUNT_BITS;
            if cur_tick >= slot_tick && (cur_tick - slot_tick) < window_ticks {
                earliest_tick = Some(earliest_tick.map_or(slot_tick, |m| m.min(slot_tick)));
            }
        }
        if let Some(oldest) = earliest_tick {
            let expire_tick = oldest.saturating_add(window_ticks);
            expire_tick
                .saturating_mul(sub_ns)
                .saturating_sub(now_ns)
                .max(1)
        } else {
            sub_ns
        }
    }

    #[inline]
    fn try_take(
        &self,
        slot: usize,
        id: RateLimitId,
        interval_ns: u64,
        limit: u64,
        units: u32,
        now_ns: u64,
    ) -> core::result::Result<SharedUndo, RateReject> {
        let sub_ns = Self::sub_ns(interval_ns);
        let cur_tick = (now_ns / sub_ns) & ROLLING_MAX_TICK;
        self.evict_expired(cur_tick);

        let req = u64::from(units);
        if req > ROLLING_COUNT_MASK {
            return Err(RateReject::Invalid);
        }

        // Step 1: Atomically reserve `req` in `in_window_sum` so concurrent threads never exceed `limit`.
        let mut cur_sum = self.in_window_sum.load(Ordering::Acquire);
        loop {
            let Some(next_sum) = cur_sum.checked_add(req).filter(|&sum| sum <= limit) else {
                return Err(RateReject::Exhausted {
                    id,
                    limit,
                    used: cur_sum,
                    requested: units,
                    retry_after_ns: self.retry_after_ns(interval_ns, cur_tick, now_ns),
                });
            };
            match self.in_window_sum.compare_exchange_weak(
                cur_sum,
                next_sum,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual) => cur_sum = actual,
            }
        }

        // Step 2: Credit `req` into `buckets[sub_idx]`.
        let sub_idx = (cur_tick as usize) % ROLLING_SUB_BUCKETS;
        let bucket = &self.buckets[sub_idx];
        let mut raw = bucket.load(Ordering::Acquire);
        let booked_tick = loop {
            let slot_tick = raw >> ROLLING_COUNT_BITS;
            let slot_count = raw & ROLLING_COUNT_MASK;
            if raw == 0 || slot_tick == cur_tick {
                let merged = slot_count.saturating_add(req);
                if merged > ROLLING_COUNT_MASK {
                    self.in_window_sum.fetch_sub(req, Ordering::AcqRel);
                    return Err(RateReject::Invalid);
                }
                let next_raw = (cur_tick << ROLLING_COUNT_BITS) | merged;
                match bucket.compare_exchange_weak(
                    raw,
                    next_raw,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => break cur_tick,
                    Err(actual) => raw = actual,
                }
            } else if cur_tick > slot_tick {
                // Same `sub_idx` with `cur_tick > slot_tick` implies `cur_tick - slot_tick >= ROLLING_SUB_BUCKETS` (expired).
                let next_raw = (cur_tick << ROLLING_COUNT_BITS) | req;
                match bucket.compare_exchange_weak(
                    raw,
                    next_raw,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {
                        if slot_count > 0 {
                            self.in_window_sum.fetch_sub(slot_count, Ordering::AcqRel);
                        }
                        break cur_tick;
                    }
                    Err(actual) => raw = actual,
                }
            } else {
                // `slot_tick > cur_tick`: another thread with slightly newer timestamp already advanced this sub-bucket.
                let merged = slot_count.saturating_add(req);
                if merged > ROLLING_COUNT_MASK {
                    self.in_window_sum.fetch_sub(req, Ordering::AcqRel);
                    return Err(RateReject::Invalid);
                }
                let next_raw = (slot_tick << ROLLING_COUNT_BITS) | merged;
                match bucket.compare_exchange_weak(
                    raw,
                    next_raw,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => break slot_tick,
                    Err(actual) => raw = actual,
                }
            }
        };

        Ok(SharedUndo::Rolling {
            slot,
            sub_idx,
            booked_tick,
            units,
        })
    }

    #[inline]
    fn put_back(&self, sub_idx: usize, booked_tick: u64, units: u32) {
        let req = u64::from(units);
        let bucket = &self.buckets[sub_idx];
        let mut raw = bucket.load(Ordering::Acquire);
        while raw != 0 {
            let slot_tick = raw >> ROLLING_COUNT_BITS;
            let slot_count = raw & ROLLING_COUNT_MASK;
            if slot_tick != booked_tick || slot_count < req {
                // Already evicted by a later tick (which already subtracted the bucket's full count).
                break;
            }
            let rem = slot_count - req;
            let next_raw = if rem == 0 {
                0
            } else {
                (booked_tick << ROLLING_COUNT_BITS) | rem
            };
            match bucket.compare_exchange_weak(raw, next_raw, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => {
                    self.in_window_sum.fetch_sub(req, Ordering::AcqRel);
                    break;
                }
                Err(actual) => raw = actual,
            }
        }
    }
}

struct SharedWindowSlot {
    state: AtomicU8,
    exch_latch: AtomicU8,
    ip_tag_flags: AtomicU32,
    interval_s: AtomicU32,
    ip_lo: AtomicU64,
    ip_hi: AtomicU64,
    account: AtomicU64,
    symbol: AtomicU64,
    limit: AtomicU64,
    interval_ns: AtomicU64,
    last_exch_count: AtomicU64,
    last_exch_limit: AtomicU64,
    fixed: AtomicFixedWindow,
    rolling: AtomicRollingWindow,
}

impl SharedWindowSlot {
    fn new() -> Self {
        Self {
            state: AtomicU8::new(SLOT_EMPTY),
            exch_latch: AtomicU8::new(0),
            ip_tag_flags: AtomicU32::new(0),
            interval_s: AtomicU32::new(0),
            ip_lo: AtomicU64::new(0),
            ip_hi: AtomicU64::new(0),
            account: AtomicU64::new(0),
            symbol: AtomicU64::new(0),
            limit: AtomicU64::new(0),
            interval_ns: AtomicU64::new(0),
            last_exch_count: AtomicU64::new(0),
            last_exch_limit: AtomicU64::new(0),
            fixed: AtomicFixedWindow::new(),
            rolling: AtomicRollingWindow::new(),
        }
    }

    #[inline]
    fn encode_ip(ip: IpKey) -> (u8, u64, u64) {
        match ip.to_ip() {
            None => (0, 0, 0),
            Some(IpAddr::V4(v4)) => (4, u64::from(v4.to_bits()), 0),
            Some(IpAddr::V6(v6)) => {
                let bits = v6.to_bits();
                (6, bits as u64, (bits >> 64) as u64)
            }
        }
    }

    #[inline]
    fn matches_id(&self, id: RateLimitId) -> bool {
        let (ip_tag, lo, hi) = Self::encode_ip(id.local_ip);
        let tag_flags = u32::from(ip_tag) | (u32::from(id.flags.0) << 8);
        self.ip_tag_flags.load(Ordering::Relaxed) == tag_flags
            && self.interval_s.load(Ordering::Relaxed) == id.interval_s
            && self.ip_lo.load(Ordering::Relaxed) == lo
            && self.ip_hi.load(Ordering::Relaxed) == hi
            && self.account.load(Ordering::Relaxed) == id.account
            && self.symbol.load(Ordering::Relaxed) == id.symbol
    }

    #[inline]
    fn read_id(&self) -> RateLimitId {
        let tf = self.ip_tag_flags.load(Ordering::Relaxed);
        let ip_tag = (tf & 0xff) as u8;
        let flags = RateFlags((tf >> 8) as u16);
        let lo = self.ip_lo.load(Ordering::Relaxed);
        let hi = self.ip_hi.load(Ordering::Relaxed);
        let local_ip = match ip_tag {
            4 => IpKey::from_v4(Ipv4Addr::from_bits(lo as u32)),
            6 => IpKey::from_v6(std::net::Ipv6Addr::from_bits(
                u128::from(lo) | (u128::from(hi) << 64),
            )),
            _ => IpKey::NONE,
        };
        RateLimitId {
            local_ip,
            interval_s: self.interval_s.load(Ordering::Relaxed),
            account: self.account.load(Ordering::Relaxed),
            flags,
            symbol: self.symbol.load(Ordering::Relaxed),
        }
    }

    #[inline]
    fn write_init(&self, id: RateLimitId, limit: u64, interval_ns: u64) {
        let (ip_tag, lo, hi) = Self::encode_ip(id.local_ip);
        let tag_flags = u32::from(ip_tag) | (u32::from(id.flags.0) << 8);
        self.ip_tag_flags.store(tag_flags, Ordering::Relaxed);
        self.interval_s.store(id.interval_s, Ordering::Relaxed);
        self.ip_lo.store(lo, Ordering::Relaxed);
        self.ip_hi.store(hi, Ordering::Relaxed);
        self.account.store(id.account, Ordering::Relaxed);
        self.symbol.store(id.symbol, Ordering::Relaxed);
        self.limit.store(limit, Ordering::Relaxed);
        self.interval_ns.store(interval_ns, Ordering::Relaxed);
    }
}

struct AtomicIpCooldown {
    state: AtomicU8,
    ip_tag: AtomicU8,
    ip_lo: AtomicU64,
    ip_hi: AtomicU64,
    until_ns: AtomicU64,
}

impl AtomicIpCooldown {
    const fn new() -> Self {
        Self {
            state: AtomicU8::new(SLOT_EMPTY),
            ip_tag: AtomicU8::new(0),
            ip_lo: AtomicU64::new(0),
            ip_hi: AtomicU64::new(0),
            until_ns: AtomicU64::new(0),
        }
    }
}

struct AtomicAccountCooldown {
    state: AtomicU8,
    frozen: AtomicBool,
    account: AtomicU64,
    until_ns: AtomicU64,
}

impl AtomicAccountCooldown {
    const fn new() -> Self {
        Self {
            state: AtomicU8::new(SLOT_EMPTY),
            frozen: AtomicBool::new(false),
            account: AtomicU64::new(0),
            until_ns: AtomicU64::new(0),
        }
    }
}

#[inline]
fn hash_rate_limit_id(id: RateLimitId) -> usize {
    let (ip_tag, lo, hi) = SharedWindowSlot::encode_ip(id.local_ip);
    let mut h = 0x9e37_79b9_7f4a_7c15u64;
    h ^= lo.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    h = h.rotate_left(27) ^ hi.wrapping_mul(0x94d0_49bb_1331_11eb);
    h ^= id.account.wrapping_mul(0xd6e8_feb8_6659_fd93);
    h = h.rotate_left(31) ^ id.symbol.wrapping_mul(0xa076_1d64_78bd_642f);
    let meta = u64::from(ip_tag) | (u64::from(id.flags.0) << 8) | (u64::from(id.interval_s) << 32);
    h ^= meta.wrapping_mul(0xe703_7ed1_a0b4_28db);
    h ^= h >> 32;
    h as usize
}

#[inline]
fn atomic_fetch_max(cell: &AtomicU64, candidate: u64) {
    let mut cur = cell.load(Ordering::Acquire);
    while candidate > cur {
        match cell.compare_exchange_weak(cur, candidate, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => break,
            Err(actual) => cur = actual,
        }
    }
}

/// Process-wide cross-account lock-free atomic rate limiter (`Send + Sync`).
pub struct SharedRateLimiter {
    slots: Box<[SharedWindowSlot]>,
    mask: usize,
    max_entries: usize,
    len: AtomicU64,
    ip_cooldowns: Box<[AtomicIpCooldown; MAX_TRACKED_IPS]>,
    account_cooldowns: Box<[AtomicAccountCooldown; MAX_TRACKED_ACCOUNTS]>,
    exch_warn_pct: u8,
}

impl core::fmt::Debug for SharedRateLimiter {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("SharedRateLimiter")
            .field("table_slots", &self.slots.len())
            .field("window_count", &self.window_count())
            .field("exch_warn_pct", &self.exch_warn_pct)
            .finish()
    }
}

impl Default for SharedRateLimiter {
    fn default() -> Self {
        Self::new()
    }
}

impl SharedRateLimiter {
    /// Creates a [`SharedRateLimiter`] with [`DEFAULT_SHARED_TABLE_SLOTS`] (`4,096`) and
    /// [`DEFAULT_EXCH_WARN_PCT`] (`80%`).
    pub fn new() -> Self {
        Self::with_config(DEFAULT_SHARED_TABLE_SLOTS, DEFAULT_EXCH_WARN_PCT)
            .expect("default SharedRateLimiter config must be valid")
    }

    /// Creates a [`SharedRateLimiter`] with explicit power-of-two `table_slots` (`64..=65,536`)
    /// and `exch_warn_pct` (`1..=100`).
    pub fn with_config(
        table_slots: usize,
        exch_warn_pct: u8,
    ) -> core::result::Result<Self, RateReject> {
        if !(MIN_SHARED_TABLE_SLOTS..=MAX_SHARED_TABLE_SLOTS).contains(&table_slots)
            || !table_slots.is_power_of_two()
            || !(1..=100).contains(&exch_warn_pct)
        {
            return Err(RateReject::Invalid);
        }
        let mut vec = Vec::with_capacity(table_slots);
        for _ in 0..table_slots {
            vec.push(SharedWindowSlot::new());
        }
        Ok(Self {
            slots: vec.into_boxed_slice(),
            mask: table_slots - 1,
            // Enforce <= 50% load factor so linear probing stays bounded and O(1).
            max_entries: table_slots / 2,
            len: AtomicU64::new(0),
            ip_cooldowns: Box::new(core::array::from_fn(|_| AtomicIpCooldown::new())),
            account_cooldowns: Box::new(core::array::from_fn(|_| AtomicAccountCooldown::new())),
            exch_warn_pct,
        })
    }

    #[inline]
    pub const fn exch_warn_pct(&self) -> u8 {
        self.exch_warn_pct
    }

    #[inline]
    pub fn window_count(&self) -> usize {
        self.len.load(Ordering::Acquire) as usize
    }

    #[inline]
    pub fn table_capacity(&self) -> usize {
        self.slots.len()
    }

    fn validate_rule(rule: RateRule) -> core::result::Result<u64, RateReject> {
        if rule.limit == 0
            || rule.limit > u64::from(u32::MAX)
            || rule.id.interval_s == 0
            || !rule.id.flags.has_action_class()
        {
            return Err(RateReject::Invalid);
        }
        if rule.id.flags.contains(RateFlags::PER_IP) && rule.id.local_ip.is_none() {
            return Err(RateReject::Invalid);
        }
        if rule.id.flags.is_rolling() && rule.limit > ROLLING_COUNT_MASK {
            return Err(RateReject::Invalid);
        }
        u64::from(rule.id.interval_s)
            .checked_mul(NS_PER_SEC)
            .ok_or(RateReject::Invalid)
    }

    /// Installs `rule` if absent, returning its [`SharedWindowHandle`].
    ///
    /// As specified in `human-docs/5`, the first insert of `rule.id` keeps its `limit`; any
    /// subsequent `add` with the same `RateLimitId` returns the existing handle.
    pub fn add(&self, rule: RateRule) -> core::result::Result<SharedWindowHandle, RateReject> {
        let interval_ns = Self::validate_rule(rule)?;
        let start = hash_rate_limit_id(rule.id) & self.mask;
        for step in 0..self.slots.len() {
            let idx = (start + step) & self.mask;
            let slot = &self.slots[idx];
            loop {
                match slot.state.load(Ordering::Acquire) {
                    SLOT_READY => {
                        if slot.matches_id(rule.id) {
                            return Ok(SharedWindowHandle {
                                slot: idx as u32,
                                id: rule.id,
                            });
                        }
                        break;
                    }
                    SLOT_CLAIMING => {
                        core::hint::spin_loop();
                    }
                    SLOT_EMPTY => {
                        // Reserve capacity before claiming slot.
                        let cur_len = self.len.load(Ordering::Acquire);
                        if cur_len as usize >= self.max_entries {
                            return Err(RateReject::Capacity);
                        }
                        if slot
                            .state
                            .compare_exchange(
                                SLOT_EMPTY,
                                SLOT_CLAIMING,
                                Ordering::AcqRel,
                                Ordering::Acquire,
                            )
                            .is_ok()
                        {
                            let prev = self.len.fetch_add(1, Ordering::AcqRel);
                            if prev as usize >= self.max_entries {
                                self.len.fetch_sub(1, Ordering::AcqRel);
                                slot.state.store(SLOT_EMPTY, Ordering::Release);
                                return Err(RateReject::Capacity);
                            }
                            slot.write_init(rule.id, rule.limit, interval_ns);
                            slot.state.store(SLOT_READY, Ordering::Release);
                            return Ok(SharedWindowHandle {
                                slot: idx as u32,
                                id: rule.id,
                            });
                        }
                    }
                    _ => unreachable!(),
                }
            }
        }
        Err(RateReject::Capacity)
    }

    /// Installs shared per-IP weight budgets (`ips = [{ ip, ip_weight_min }]`) for `profile`.
    ///
    /// Every `AccountSession` bound to the same `ip` shares the installed window (`account = 0`).
    pub fn install_venue_ip_budgets(
        &self,
        profile: OrsVenueProfile,
        ips: &[IpBudgetSpec],
    ) -> core::result::Result<(), RateReject> {
        let (interval_s, default_limit, flags) = profile.defaults().shared_ip_budget;
        for entry in ips {
            let limit = if entry.ip_weight_min == 0 {
                default_limit
            } else {
                entry.ip_weight_min
            };
            let rule = RateRule::ip(IpKey::from_ip(entry.ip), interval_s, flags, limit);
            self.add(rule)?;
        }
        Ok(())
    }

    /// Resolves (or lazily installs with `profile`'s default `ip_weight_min`) the shared IP window
    /// for `local_ip`.
    #[inline]
    pub fn resolve_venue_ip_window(
        &self,
        profile: OrsVenueProfile,
        local_ip: IpKey,
    ) -> core::result::Result<SharedWindowHandle, RateReject> {
        if local_ip.is_none() {
            return Err(RateReject::Invalid);
        }
        let (interval_s, default_limit, flags) = profile.defaults().shared_ip_budget;
        self.add(RateRule::ip(local_ip, interval_s, flags, default_limit))
    }

    #[inline]
    fn lookup_slot(&self, id: RateLimitId) -> Option<usize> {
        let start = hash_rate_limit_id(id) & self.mask;
        for step in 0..self.slots.len() {
            let idx = (start + step) & self.mask;
            let slot = &self.slots[idx];
            loop {
                match slot.state.load(Ordering::Acquire) {
                    SLOT_EMPTY => return None,
                    SLOT_CLAIMING => core::hint::spin_loop(),
                    SLOT_READY => {
                        if slot.matches_id(id) {
                            return Some(idx);
                        }
                        break;
                    }
                    _ => unreachable!(),
                }
            }
        }
        None
    }

    /// Returns the configured limit for `id`, if installed.
    pub fn window_limit(&self, id: RateLimitId) -> Option<u64> {
        let idx = self.lookup_slot(id)?;
        Some(self.slots[idx].limit.load(Ordering::Relaxed))
    }

    /// Returns the effective consumed units for `id` at `now_ns` without mutating state.
    pub fn window_used(&self, id: RateLimitId, now_ns: u64) -> Option<u64> {
        let idx = self.lookup_slot(id)?;
        let slot = &self.slots[idx];
        let interval_ns = slot.interval_ns.load(Ordering::Relaxed);
        if id.flags.is_rolling() {
            Some(slot.rolling.used_at(interval_ns, now_ns))
        } else {
            Some(slot.fixed.used_at(interval_ns, now_ns))
        }
    }

    /// Isolates an HTTP `429` / IP cooldown strictly to `local_ip` across all accounts on that IP.
    pub fn note_ip_cooldown(
        &self,
        local_ip: IpKey,
        until_ns: u64,
    ) -> core::result::Result<(), RateReject> {
        if local_ip.is_none() {
            return Err(RateReject::Invalid);
        }
        let (tag, lo, hi) = SharedWindowSlot::encode_ip(local_ip);
        for entry in self.ip_cooldowns.iter() {
            loop {
                match entry.state.load(Ordering::Acquire) {
                    SLOT_READY => {
                        if entry.ip_tag.load(Ordering::Relaxed) == tag
                            && entry.ip_lo.load(Ordering::Relaxed) == lo
                            && entry.ip_hi.load(Ordering::Relaxed) == hi
                        {
                            atomic_fetch_max(&entry.until_ns, until_ns);
                            return Ok(());
                        }
                        break;
                    }
                    SLOT_CLAIMING => core::hint::spin_loop(),
                    SLOT_EMPTY => {
                        if entry
                            .state
                            .compare_exchange(
                                SLOT_EMPTY,
                                SLOT_CLAIMING,
                                Ordering::AcqRel,
                                Ordering::Acquire,
                            )
                            .is_ok()
                        {
                            entry.ip_tag.store(tag, Ordering::Relaxed);
                            entry.ip_lo.store(lo, Ordering::Relaxed);
                            entry.ip_hi.store(hi, Ordering::Relaxed);
                            entry.until_ns.store(until_ns, Ordering::Relaxed);
                            entry.state.store(SLOT_READY, Ordering::Release);
                            return Ok(());
                        }
                    }
                    _ => unreachable!(),
                }
            }
        }
        Err(RateReject::Capacity)
    }

    /// Applies an account-wide cooldown across all `local_ip` routes for `account`.
    pub fn note_account_cooldown(
        &self,
        account: u64,
        until_ns: u64,
    ) -> core::result::Result<(), RateReject> {
        if account == 0 {
            return Err(RateReject::Invalid);
        }
        let entry = self.get_or_claim_account_cooldown(account)?;
        atomic_fetch_max(&entry.until_ns, until_ns);
        Ok(())
    }

    /// Permanently freezes `account` across all `local_ip` routes (e.g. HTTP `418` ban).
    pub fn freeze_account(&self, account: u64) -> core::result::Result<(), RateReject> {
        if account == 0 {
            return Err(RateReject::Invalid);
        }
        let entry = self.get_or_claim_account_cooldown(account)?;
        entry.frozen.store(true, Ordering::Release);
        Ok(())
    }

    fn get_or_claim_account_cooldown(
        &self,
        account: u64,
    ) -> core::result::Result<&AtomicAccountCooldown, RateReject> {
        for entry in self.account_cooldowns.iter() {
            loop {
                match entry.state.load(Ordering::Acquire) {
                    SLOT_READY => {
                        if entry.account.load(Ordering::Relaxed) == account {
                            return Ok(entry);
                        }
                        break;
                    }
                    SLOT_CLAIMING => core::hint::spin_loop(),
                    SLOT_EMPTY => {
                        if entry
                            .state
                            .compare_exchange(
                                SLOT_EMPTY,
                                SLOT_CLAIMING,
                                Ordering::AcqRel,
                                Ordering::Acquire,
                            )
                            .is_ok()
                        {
                            entry.account.store(account, Ordering::Relaxed);
                            entry.until_ns.store(0, Ordering::Relaxed);
                            entry.frozen.store(false, Ordering::Relaxed);
                            entry.state.store(SLOT_READY, Ordering::Release);
                            return Ok(entry);
                        }
                    }
                    _ => unreachable!(),
                }
            }
        }
        Err(RateReject::Capacity)
    }

    #[inline]
    pub fn is_account_frozen(&self, account: u64) -> bool {
        if account == 0 {
            return false;
        }
        for entry in self.account_cooldowns.iter() {
            if entry.state.load(Ordering::Acquire) == SLOT_READY
                && entry.account.load(Ordering::Relaxed) == account
            {
                return entry.frozen.load(Ordering::Acquire);
            }
        }
        false
    }

    #[inline]
    pub fn check_account(&self, account: u64, now_ns: u64) -> core::result::Result<(), RateReject> {
        if account == 0 {
            return Ok(());
        }
        for entry in self.account_cooldowns.iter() {
            let st = entry.state.load(Ordering::Acquire);
            if st == SLOT_EMPTY {
                break;
            }
            if st == SLOT_READY && entry.account.load(Ordering::Relaxed) == account {
                if entry.frozen.load(Ordering::Acquire) {
                    return Err(RateReject::Frozen { account });
                }
                let until = entry.until_ns.load(Ordering::Acquire);
                if now_ns < until {
                    return Err(RateReject::AccountCooldown {
                        account,
                        until_ns: until,
                        retry_after_ns: until - now_ns,
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
        let (tag, lo, hi) = SharedWindowSlot::encode_ip(local_ip);
        for entry in self.ip_cooldowns.iter() {
            let st = entry.state.load(Ordering::Acquire);
            if st == SLOT_EMPTY {
                break;
            }
            if st == SLOT_READY
                && entry.ip_tag.load(Ordering::Relaxed) == tag
                && entry.ip_lo.load(Ordering::Relaxed) == lo
                && entry.ip_hi.load(Ordering::Relaxed) == hi
            {
                let until = entry.until_ns.load(Ordering::Acquire);
                if now_ns < until {
                    return Err(RateReject::IpCooldown {
                        local_ip,
                        until_ns: until,
                        retry_after_ns: until - now_ns,
                    });
                }
                break;
            }
        }
        Ok(())
    }

    #[inline]
    fn put_back_undo(&self, undo: SharedUndo) {
        match undo {
            SharedUndo::Fixed { slot, epoch, units } => {
                self.slots[slot].fixed.put_back(epoch, units);
            }
            SharedUndo::Rolling {
                slot,
                sub_idx,
                booked_tick,
                units,
            } => {
                self.slots[slot]
                    .rolling
                    .put_back(sub_idx, booked_tick, units);
            }
        }
    }

    /// Atomically spends a chain of `1..=MAX_SPENDS_PER_CALL` windows, rolling back all earlier
    /// debits in reverse order if any later bucket in the chain rejects.
    ///
    /// Entries with `units == 0` are skipped after checking their account/IP cooldowns.
    pub fn allow(
        &self,
        spends: &[SharedSpend],
        now_ns: u64,
    ) -> core::result::Result<RatePermit, RateReject> {
        if spends.is_empty() || spends.len() > MAX_SPENDS_PER_CALL {
            return Err(RateReject::Invalid);
        }

        let mut resolved = [(0usize, RateLimitId::default(), 0u32); MAX_SPENDS_PER_CALL];
        let mut active_len = 0usize;

        for spend in spends {
            let (slot_idx, id) = match spend.target {
                SpendTarget::Handle(h) => {
                    let idx = h.slot_index();
                    if idx >= self.slots.len()
                        || self.slots[idx].state.load(Ordering::Acquire) != SLOT_READY
                    {
                        return Err(RateReject::UnknownWindow { id: h.id });
                    }
                    (idx, h.id)
                }
                SpendTarget::Rule(rule) => {
                    let h = self.add(rule)?;
                    (h.slot_index(), h.id)
                }
            };
            self.check_account(id.account, now_ns)?;
            self.check_ip(id.local_ip, now_ns)?;
            if spend.units > 0 {
                resolved[active_len] = (slot_idx, id, spend.units);
                active_len += 1;
            }
        }

        let mut undo_stack = [SharedUndo::Fixed {
            slot: 0,
            epoch: 0,
            units: 0,
        }; MAX_SPENDS_PER_CALL];
        let mut applied = 0usize;

        for &(slot_idx, id, units) in &resolved[..active_len] {
            let slot = &self.slots[slot_idx];
            let limit = slot.limit.load(Ordering::Relaxed);
            let interval_ns = slot.interval_ns.load(Ordering::Relaxed);
            let take_res = if id.flags.is_rolling() {
                slot.rolling
                    .try_take(slot_idx, id, interval_ns, limit, units, now_ns)
            } else {
                slot.fixed
                    .try_take(slot_idx, id, interval_ns, limit, units, now_ns)
            };
            match take_res {
                Ok(undo) => {
                    undo_stack[applied] = undo;
                    applied += 1;
                }
                Err(reject) => {
                    while applied > 0 {
                        applied -= 1;
                        self.put_back_undo(undo_stack[applied]);
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

    /// Enforces the mandatory ORS hot-path sequence (`human-docs/5 rate-limiter.md`):
    /// `pick connection -> spend(conn.local_ip) -> on limit exceeded return NACK immediately`
    /// (invoking `pick_once` at most once so another connection is NEVER retried on rate-limit hit).
    pub fn pick_and_spend_or_nack<R, FPick, FBuild>(
        &self,
        pick_once: FPick,
        build_spends: FBuild,
        now_ns: u64,
    ) -> core::result::Result<(R, RatePermit), RouteAdmissionReject>
    where
        FPick: FnOnce() -> Option<R>,
        FBuild: FnOnce(
            &R,
            &mut [SharedSpend; MAX_SPENDS_PER_CALL],
        ) -> core::result::Result<usize, RouteAdmissionReject>,
    {
        let route = pick_once().ok_or(RouteAdmissionReject::NoReadyConnection)?;
        let mut buf = [SharedSpend::handle(
            SharedWindowHandle {
                slot: 0,
                id: RateLimitId::default(),
            },
            0,
        ); MAX_SPENDS_PER_CALL];
        let len = build_spends(&route, &mut buf)?;
        let permit = self
            .allow(&buf[..len], now_ns)
            .map_err(RouteAdmissionReject::RateLimitedNack)?;
        Ok((route, permit))
    }

    /// Evaluates a parsed venue `rateLimits[]` `(reported_count, reported_limit)` row in exact
    /// integer math without mutating local spend windows, emitting a rising-edge
    /// [`ExchRateLimitEvent`] ONLY when crossing `exch_warn_pct` (default `80%`) or `100%`
    /// ceiling, and re-arming ONLY after usage drops strictly below the respective threshold.
    pub fn reconcile_venue_usage(
        &self,
        id: RateLimitId,
        reported_count: u64,
        reported_limit: u64,
        now_ns: u64,
    ) -> core::result::Result<Option<ExchRateLimitEvent>, RateReject> {
        if reported_limit == 0 || reported_limit > u64::from(u32::MAX) {
            return Err(RateReject::Invalid);
        }
        let handle = self.add(RateRule {
            id,
            limit: reported_limit,
        })?;
        let slot = &self.slots[handle.slot_index()];
        let effective_id = slot.read_id();

        slot.last_exch_count
            .store(reported_count, Ordering::Relaxed);
        slot.last_exch_limit
            .store(reported_limit, Ordering::Relaxed);

        // Exact integer threshold comparisons (zero floating-point math):
        // warn_hit <=> count / limit >= exch_warn_pct / 100 <=> count * 100 >= limit * exch_warn_pct
        let warn_hit = u128::from(reported_count) * 100
            >= u128::from(reported_limit) * u128::from(self.exch_warn_pct);
        let ceiling_hit = reported_count >= reported_limit;

        let mut desired_latch = 0u8;
        if warn_hit {
            desired_latch |= LATCH_WARN_ACTIVE;
        }
        if ceiling_hit {
            desired_latch |= LATCH_CEIL_ACTIVE;
        }

        let mut prev_latch = slot.exch_latch.load(Ordering::Acquire);
        let (crossed_warn, crossed_ceiling) = loop {
            // A latch bit stays set while its threshold holds and clears ONLY when usage drops below threshold.
            let next_latch = desired_latch;
            match slot.exch_latch.compare_exchange_weak(
                prev_latch,
                next_latch,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    let cw = warn_hit && (prev_latch & LATCH_WARN_ACTIVE) == 0;
                    let cc = ceiling_hit && (prev_latch & LATCH_CEIL_ACTIVE) == 0;
                    break (cw, cc);
                }
                Err(actual) => prev_latch = actual,
            }
        };

        if crossed_ceiling {
            Ok(Some(ExchRateLimitEvent {
                id: effective_id,
                count: reported_count,
                limit: reported_limit,
                warn_pct: self.exch_warn_pct,
                level: ExchRateLimitLevel::CeilingReached,
                crossed_warn,
                crossed_ceiling: true,
                observed_at_ns: now_ns,
            }))
        } else if crossed_warn {
            Ok(Some(ExchRateLimitEvent {
                id: effective_id,
                count: reported_count,
                limit: reported_limit,
                warn_pct: self.exch_warn_pct,
                level: ExchRateLimitLevel::WarnThreshold,
                crossed_warn: true,
                crossed_ceiling: false,
                observed_at_ns: now_ns,
            }))
        } else {
            Ok(None)
        }
    }

    /// Reconciles a slice of parsed `rateLimits[]` rows into stack-allocated `out`, returning the
    /// number of rising-edge [`ExchRateLimitEvent`]s emitted.
    pub fn reconcile_venue_usage_batch(
        &self,
        rows: &[VenueRateLimitRow],
        now_ns: u64,
        out: &mut [Option<ExchRateLimitEvent>],
    ) -> core::result::Result<usize, RateReject> {
        let mut emitted = 0usize;
        for row in rows {
            if let Some(ev) = self.reconcile_venue_usage(row.id, row.count, row.limit, now_ns)?
                && emitted < out.len()
            {
                out[emitted] = Some(ev);
                emitted += 1;
            }
        }
        Ok(emitted)
    }
}

/// Pre-bound per-`AccountSession` handle over the process-wide [`SharedRateLimiter`].
///
/// Multiple `OrsAccountQuota` instances (for `account_ref = 1..=255`) hold their own private
/// account/symbol/product window handles while automatically sharing the same underlying
/// `RateRule::ip` window whenever routed over a connection bound to the same `local_ip`.
#[derive(Clone, Copy, Debug)]
pub struct OrsAccountQuota<'a> {
    limiter: &'a SharedRateLimiter,
    profile: OrsVenueProfile,
    defaults: VenueRateDefaults,
    overrides: AccountRateOverrides,
    account: u64,
    order_primary: Option<SharedWindowHandle>,
    order_secondary: Option<SharedWindowHandle>,
    account_request: Option<SharedWindowHandle>,
}

impl<'a> OrsAccountQuota<'a> {
    /// Registers an `AccountSession`'s venue rules in `limiter` with compiled defaults.
    pub fn register(
        limiter: &'a SharedRateLimiter,
        profile: OrsVenueProfile,
        account: u64,
    ) -> core::result::Result<Self, RateReject> {
        Self::register_with_overrides(limiter, profile, account, AccountRateOverrides::default())
    }

    /// Registers an `AccountSession`'s venue rules with optional `ACCOUNT_MANAGER.<name>.rate_limit`
    /// cap overrides.
    pub fn register_with_overrides(
        limiter: &'a SharedRateLimiter,
        profile: OrsVenueProfile,
        account: u64,
        overrides: AccountRateOverrides,
    ) -> core::result::Result<Self, RateReject> {
        if account == 0 {
            return Err(RateReject::Invalid);
        }
        let defaults = profile.defaults();
        let order_primary = match defaults.account_order_primary {
            Some((interval_s, def_limit, flags)) => {
                let cap = overrides.primary_order_limit.unwrap_or(def_limit);
                Some(limiter.add(RateRule::account(account, interval_s, flags, cap))?)
            }
            None => None,
        };
        let order_secondary = match defaults.account_order_secondary {
            Some((interval_s, def_limit, flags)) => {
                let cap = overrides.secondary_order_limit.unwrap_or(def_limit);
                Some(limiter.add(RateRule::account(account, interval_s, flags, cap))?)
            }
            None => None,
        };
        let account_request = match defaults.account_request {
            Some((interval_s, def_limit, flags)) => {
                let cap = overrides.account_request_limit.unwrap_or(def_limit);
                Some(limiter.add(RateRule::account(account, interval_s, flags, cap))?)
            }
            None => None,
        };
        Ok(Self {
            limiter,
            profile,
            defaults,
            overrides,
            account,
            order_primary,
            order_secondary,
            account_request,
        })
    }

    #[inline]
    pub const fn profile(&self) -> OrsVenueProfile {
        self.profile
    }

    #[inline]
    pub const fn account(&self) -> u64 {
        self.account
    }

    #[inline]
    pub const fn order_primary_handle(&self) -> Option<SharedWindowHandle> {
        self.order_primary
    }

    #[inline]
    pub const fn order_secondary_handle(&self) -> Option<SharedWindowHandle> {
        self.order_secondary
    }

    fn append_ip_spend(
        &self,
        local_ip: IpKey,
        ip_weight: u32,
        out: &mut [SharedSpend; MAX_SPENDS_PER_CALL],
        len: &mut usize,
    ) -> core::result::Result<(), RateReject> {
        if ip_weight == 0 {
            if !local_ip.is_none() {
                let h = self
                    .limiter
                    .resolve_venue_ip_window(self.profile, local_ip)?;
                out[*len] = SharedSpend::handle(h, 0);
                *len += 1;
            }
            return Ok(());
        }
        let h = self
            .limiter
            .resolve_venue_ip_window(self.profile, local_ip)?;
        out[*len] = SharedSpend::handle(h, ip_weight);
        *len += 1;
        Ok(())
    }

    /// Populates `out` with the exact [`SharedSpend`] chain for placing `order_units` orders on `local_ip`.
    pub fn build_place_spends(
        &self,
        local_ip: IpKey,
        scope: InstrumentScope,
        order_units: u32,
        ip_weight: u32,
        out: &mut [SharedSpend; MAX_SPENDS_PER_CALL],
    ) -> core::result::Result<usize, RateReject> {
        if order_units == 0 {
            return Err(RateReject::Invalid);
        }
        let mut len = 0usize;
        if let Some(h) = self.order_primary {
            out[len] = SharedSpend::handle(h, order_units);
            len += 1;
        }
        if let Some(h) = self.order_secondary {
            out[len] = SharedSpend::handle(h, order_units);
            len += 1;
        }
        if let Some((interval_s, def_ord, _, extra_flags)) = self.defaults.per_symbol_order_cancel {
            let cap = self.overrides.per_symbol_order_limit.unwrap_or(def_ord);
            let flags = RateFlags::ORDER.union(extra_flags);
            let rule = match scope {
                InstrumentScope::Symbol(sym) if sym != 0 => {
                    RateRule::symbol(self.account, interval_s, flags, cap, sym)
                }
                InstrumentScope::Family(fam) if fam != 0 => {
                    RateRule::family(self.account, interval_s, flags, cap, fam)
                }
                _ => return Err(RateReject::Invalid),
            };
            out[len] = SharedSpend::rule(rule, order_units);
            len += 1;
        }
        if let Some((interval_s, def_ord, _, extra_flags)) = self.defaults.per_product_order_cancel
        {
            let cap = self.overrides.per_product_order_limit.unwrap_or(def_ord);
            let prod = match scope {
                InstrumentScope::Product(p) if p != 0 => p,
                InstrumentScope::None => 1,
                _ => return Err(RateReject::Invalid),
            };
            let rule = RateRule::product(
                self.account,
                interval_s,
                RateFlags::ORDER.union(extra_flags),
                cap,
                prod,
            );
            out[len] = SharedSpend::rule(rule, order_units);
            len += 1;
        }
        self.append_ip_spend(local_ip, ip_weight, out, &mut len)?;
        Ok(len)
    }

    /// Populates `out` with the exact [`SharedSpend`] chain for cancelling an order on `local_ip`
    /// (strictly isolated from all `ORDER` windows!).
    pub fn build_cancel_spends(
        &self,
        local_ip: IpKey,
        scope: InstrumentScope,
        ip_weight: u32,
        out: &mut [SharedSpend; MAX_SPENDS_PER_CALL],
    ) -> core::result::Result<usize, RateReject> {
        let mut len = 0usize;
        if let Some((interval_s, _, def_cxl, extra_flags)) = self.defaults.per_symbol_order_cancel {
            let cap = self.overrides.per_symbol_cancel_limit.unwrap_or(def_cxl);
            let flags = RateFlags::CANCEL.union(extra_flags);
            let rule = match scope {
                InstrumentScope::Symbol(sym) if sym != 0 => {
                    RateRule::symbol(self.account, interval_s, flags, cap, sym)
                }
                InstrumentScope::Family(fam) if fam != 0 => {
                    RateRule::family(self.account, interval_s, flags, cap, fam)
                }
                _ => return Err(RateReject::Invalid),
            };
            out[len] = SharedSpend::rule(rule, 1);
            len += 1;
        }
        if let Some((interval_s, _, def_cxl, extra_flags)) = self.defaults.per_product_order_cancel
        {
            let cap = self.overrides.per_product_cancel_limit.unwrap_or(def_cxl);
            let prod = match scope {
                InstrumentScope::Product(p) if p != 0 => p,
                InstrumentScope::None => 1,
                _ => return Err(RateReject::Invalid),
            };
            let rule = RateRule::product(
                self.account,
                interval_s,
                RateFlags::CANCEL.union(extra_flags),
                cap,
                prod,
            );
            out[len] = SharedSpend::rule(rule, 1);
            len += 1;
        }
        self.append_ip_spend(local_ip, ip_weight, out, &mut len)?;
        Ok(len)
    }

    /// Populates `out` with the exact [`SharedSpend`] chain for a status/REST query on `local_ip`
    /// (strictly isolated from `ORDER` and `CANCEL` windows!).
    pub fn build_query_spends(
        &self,
        local_ip: IpKey,
        ip_weight: u32,
        out: &mut [SharedSpend; MAX_SPENDS_PER_CALL],
    ) -> core::result::Result<usize, RateReject> {
        let mut len = 0usize;
        if let Some(h) = self.account_request {
            out[len] = SharedSpend::handle(h, 1);
            len += 1;
        }
        self.append_ip_spend(local_ip, ip_weight, out, &mut len)?;
        Ok(len)
    }

    /// Admits an order placement on `local_ip`.
    pub fn allow_place(
        &self,
        local_ip: IpKey,
        scope: InstrumentScope,
        order_units: u32,
        ip_weight: u32,
        now_ns: u64,
    ) -> core::result::Result<RatePermit, RateReject> {
        let mut buf = [SharedSpend::handle(
            SharedWindowHandle {
                slot: 0,
                id: RateLimitId::default(),
            },
            0,
        ); MAX_SPENDS_PER_CALL];
        let len = self.build_place_spends(local_ip, scope, order_units, ip_weight, &mut buf)?;
        self.limiter.allow(&buf[..len], now_ns)
    }

    /// Admits an order cancellation on `local_ip` (never touches `ORDER` windows!).
    pub fn allow_cancel(
        &self,
        local_ip: IpKey,
        scope: InstrumentScope,
        ip_weight: u32,
        now_ns: u64,
    ) -> core::result::Result<RatePermit, RateReject> {
        let mut buf = [SharedSpend::handle(
            SharedWindowHandle {
                slot: 0,
                id: RateLimitId::default(),
            },
            0,
        ); MAX_SPENDS_PER_CALL];
        let len = self.build_cancel_spends(local_ip, scope, ip_weight, &mut buf)?;
        self.limiter.allow(&buf[..len], now_ns)
    }

    /// Admits a REST/WS query or listenKey refresh on `local_ip` (never touches `ORDER`/`CANCEL`!).
    pub fn allow_query(
        &self,
        local_ip: IpKey,
        ip_weight: u32,
        now_ns: u64,
    ) -> core::result::Result<RatePermit, RateReject> {
        let mut buf = [SharedSpend::handle(
            SharedWindowHandle {
                slot: 0,
                id: RateLimitId::default(),
            },
            0,
        ); MAX_SPENDS_PER_CALL];
        let len = self.build_query_spends(local_ip, ip_weight, &mut buf)?;
        self.limiter.allow(&buf[..len], now_ns)
    }

    /// Enforces `pick -> spend(route.local_ip) -> NACK immediately` for an outbound order placement
    /// over [`tickwire_venue::OrderRoute`].
    pub fn pick_route_and_place_or_nack<FPick>(
        &self,
        pick_once: FPick,
        scope: InstrumentScope,
        order_units: u32,
        ip_weight: u32,
        now_ns: u64,
    ) -> core::result::Result<(OrderRoute, RatePermit), RouteAdmissionReject>
    where
        FPick: FnOnce() -> Option<OrderRoute>,
    {
        self.limiter.pick_and_spend_or_nack(
            pick_once,
            |route, buf| {
                let local_ip = route_ip_key(route)?;
                self.build_place_spends(local_ip, scope, order_units, ip_weight, buf)
                    .map_err(RouteAdmissionReject::RateLimitedNack)
            },
            now_ns,
        )
    }

    /// Enforces `pick -> spend(route.local_ip) -> NACK immediately` for an outbound cancellation.
    pub fn pick_route_and_cancel_or_nack<FPick>(
        &self,
        pick_once: FPick,
        scope: InstrumentScope,
        ip_weight: u32,
        now_ns: u64,
    ) -> core::result::Result<(OrderRoute, RatePermit), RouteAdmissionReject>
    where
        FPick: FnOnce() -> Option<OrderRoute>,
    {
        self.limiter.pick_and_spend_or_nack(
            pick_once,
            |route, buf| {
                let local_ip = route_ip_key(route)?;
                self.build_cancel_spends(local_ip, scope, ip_weight, buf)
                    .map_err(RouteAdmissionReject::RateLimitedNack)
            },
            now_ns,
        )
    }
}

/// Extracts [`IpKey`] from a published [`tickwire_venue::OrderRoute`] (`local_ip == 0` -> `MissingLocalIp`).
#[inline]
pub fn route_ip_key(route: &OrderRoute) -> core::result::Result<IpKey, RouteAdmissionReject> {
    if route.local_ip == 0 {
        return Err(RouteAdmissionReject::MissingLocalIp {
            conn_id: route.connection_id,
        });
    }
    Ok(IpKey::from_v4(Ipv4Addr::from_bits(route.local_ip)))
}
