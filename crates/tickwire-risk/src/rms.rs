//! Flat, lock-free per-symbol Risk Management System (`SymbolLimits` & `RmsEngine`).
//!
//! Designed for the ORS `REQ` hot path:
//! - Direct `O(1)` flat-slice indexing by compact `symbol_id: usize` (zero hash/tree lookups).
//! - Three-tier `1s / 5s / 10s` rolling-window ring counters for `new_count` and `order_qty`.
//! - Exact integer parts-per-billion (`10^-9`) `FatFingerRatio` price cage around reference price
//!   (zero floating-point operations).
//! - Single-order max quantity and max notional checks plus controlled `bypass_rms` gating.
//! - Cache-line isolated atomic fee accumulator (`RESP` thread) and sequence-locked atomic limit
//!   snapshots so `SIGUSR1` off-thread `reload_limits` swaps configuration cleanly while retaining
//!   prior active limits on any parse or validation error.

use crate::fixed::Fixed;
use std::fmt;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering, fence};
use std::sync::{Arc, Mutex};

/// Parts-per-billion denominator (`10^9`) used for exact fixed-point fat-finger ratios.
pub const PPB_SCALE: u32 = 1_000_000_000;

/// Duration of one rolling-window sub-bucket (`100 ms` in monotonic nanoseconds).
pub const WINDOW_BUCKET_NS: u64 = 100_000_000;

/// Number of `100 ms` buckets spanning the `10 s` outer window.
pub const WINDOW_BUCKETS: usize = 100;

const BUCKETS_1S: u64 = 10;
const BUCKETS_5S: u64 = 50;
const BUCKETS_10S: u64 = 100;
const TICK_BIAS: u64 = WINDOW_BUCKETS as u64;

const FLAG_ENABLED: u64 = 1 << 0;
const FLAG_ALLOW_BYPASS: u64 = 1 << 1;

/// Order side evaluated by [`RmsEngine`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RmsSide {
    Buy,
    Sell,
}

/// Rolling-window horizon tier (`1s`, `5s`, `10s`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowTier {
    OneSecond,
    FiveSeconds,
    TenSeconds,
}

impl WindowTier {
    #[inline]
    pub const fn label(self) -> &'static str {
        match self {
            Self::OneSecond => "1s",
            Self::FiveSeconds => "5s",
            Self::TenSeconds => "10s",
        }
    }
}

/// Exact integer fat-finger price-cage ratio in `(0, 1]` scaled to parts-per-billion (`10^-9`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FatFingerRatio {
    ppb: u32,
}

impl FatFingerRatio {
    /// Constructs a ratio from parts per billion (`1..=1_000_000_000`).
    #[inline]
    pub const fn from_ppb(ppb: u32) -> Option<Self> {
        if ppb == 0 || ppb > PPB_SCALE {
            None
        } else {
            Some(Self { ppb })
        }
    }

    /// Constructs a ratio from parts per million (`1..=1_000_000`).
    #[inline]
    pub const fn from_ppm(ppm: u32) -> Option<Self> {
        if ppm == 0 || ppm > 1_000_000 {
            None
        } else {
            Some(Self { ppb: ppm * 1_000 })
        }
    }

    /// Constructs a ratio from basis points (`1..=10_000`, where `100 bps = 1%`).
    #[inline]
    pub const fn from_bps(bps: u32) -> Option<Self> {
        if bps == 0 || bps > 10_000 {
            None
        } else {
            Some(Self { ppb: bps * 100_000 })
        }
    }

    /// Constructs a ratio from an exact [`Fixed`] decimal in `(0, 1]` with at most 9 fractional digits.
    pub fn from_fixed(ratio: Fixed) -> Option<Self> {
        let lo = ratio.rescale(9, crate::Rounding::Floor).ok()?;
        let hi = ratio.rescale(9, crate::Rounding::Ceil).ok()?;
        if lo != hi {
            return None;
        }
        let atoms = lo.atoms();
        if atoms <= 0 || atoms > i128::from(PPB_SCALE) {
            return None;
        }
        Some(Self { ppb: atoms as u32 })
    }

    /// Parses `"0.05"`, `"500bps"`, or `"50000ppm"` into an exact integer ratio (zero floats).
    pub fn parse_str(raw: &str) -> Option<Self> {
        let trimmed = raw.trim();
        if let Some(bps_str) = trimmed.strip_suffix("bps") {
            let bps: u32 = bps_str.trim().parse().ok()?;
            return Self::from_bps(bps);
        }
        if let Some(ppm_str) = trimmed.strip_suffix("ppm") {
            let ppm: u32 = ppm_str.trim().parse().ok()?;
            return Self::from_ppm(ppm);
        }
        if let Some(ppb_str) = trimmed.strip_suffix("ppb") {
            let ppb: u32 = ppb_str.trim().parse().ok()?;
            return Self::from_ppb(ppb);
        }
        let fixed = Fixed::parse(trimmed, 9).ok()?;
        Self::from_fixed(fixed)
    }

    /// Returns the exact parts-per-billion numerator (`1..=1_000_000_000`).
    #[inline]
    pub const fn ppb(self) -> u32 {
        self.ppb
    }

    /// Computes inclusive `(min_sell_ticks, max_buy_ticks)` around `ref_price_ticks > 0`.
    ///
    /// - `max_buy_ticks = floor(ref_price_ticks * (10^9 + ppb) / 10^9)`
    /// - `min_sell_ticks = ceil(ref_price_ticks * (10^9 - ppb) / 10^9)`
    #[inline]
    pub fn bounds_ticks(self, ref_price_ticks: i64) -> Option<(i64, i64)> {
        if ref_price_ticks <= 0 {
            return None;
        }
        let ref_i128 = i128::from(ref_price_ticks);
        let scale = i128::from(PPB_SCALE);
        let ppb = i128::from(self.ppb);

        let max_buy = ref_i128.checked_mul(scale + ppb)? / scale;
        let sell_numer = ref_i128.checked_mul(scale - ppb)?;
        let min_sell = sell_numer.checked_add(scale - 1)? / scale;

        let max_buy_i64 = i64::try_from(max_buy).ok()?;
        let min_sell_i64 = i64::try_from(min_sell).ok()?;
        Some((min_sell_i64, max_buy_i64))
    }

    /// Validates `price_ticks` for `side` against `ref_price_ticks`.
    #[inline]
    pub fn check_price_ticks(
        self,
        side: RmsSide,
        price_ticks: i64,
        ref_price_ticks: i64,
    ) -> Result<(), RmsRejectReason> {
        if price_ticks <= 0 {
            return Err(RmsRejectReason::InvalidPrice);
        }
        if ref_price_ticks <= 0 {
            return Err(RmsRejectReason::MissingReferencePrice);
        }
        let (min_sell_ticks, max_buy_ticks) = self
            .bounds_ticks(ref_price_ticks)
            .ok_or(RmsRejectReason::ArithmeticOverflow)?;
        let violated = match side {
            RmsSide::Buy => price_ticks > max_buy_ticks,
            RmsSide::Sell => price_ticks < min_sell_ticks,
        };
        if violated {
            return Err(RmsRejectReason::FatFingerPrice {
                price_ticks,
                ref_price_ticks,
                min_sell_ticks,
                max_buy_ticks,
            });
        }
        Ok(())
    }
}

/// Three-tier (`1s / 5s / 10s`) sliding-window ring counter backed by 100 fixed `100 ms` buckets.
///
/// Zero heap allocation after construction; advancing monotonic timestamps updates running sums
/// incrementally in `O(1)` amortized time.
#[derive(Debug, Clone)]
pub struct RollingWindow {
    buckets: [u64; WINDOW_BUCKETS],
    last_tick: u64,
    initialized: bool,
    sum_1s: u64,
    sum_5s: u64,
    sum_10s: u64,
}

impl Default for RollingWindow {
    fn default() -> Self {
        Self::new()
    }
}

impl RollingWindow {
    /// Creates an empty three-tier rolling window.
    #[inline]
    pub const fn new() -> Self {
        Self {
            buckets: [0; WINDOW_BUCKETS],
            last_tick: 0,
            initialized: false,
            sum_1s: 0,
            sum_5s: 0,
            sum_10s: 0,
        }
    }

    /// Resets all buckets and running sums to zero.
    #[inline]
    pub fn reset(&mut self) {
        self.buckets.fill(0);
        self.last_tick = 0;
        self.initialized = false;
        self.sum_1s = 0;
        self.sum_5s = 0;
        self.sum_10s = 0;
    }

    /// Advances the ring clock to `now_mono_ns` (non-decreasing) and expires aged buckets.
    #[inline]
    pub fn advance(&mut self, now_mono_ns: u64) {
        let now_tick = (now_mono_ns / WINDOW_BUCKET_NS) + TICK_BIAS;
        if !self.initialized {
            self.last_tick = now_tick;
            self.initialized = true;
            return;
        }
        if now_tick <= self.last_tick {
            return;
        }
        let elapsed = now_tick - self.last_tick;
        if elapsed >= BUCKETS_10S {
            self.buckets.fill(0);
            self.sum_1s = 0;
            self.sum_5s = 0;
            self.sum_10s = 0;
            self.last_tick = now_tick;
            return;
        }
        for tick in (self.last_tick + 1)..=now_tick {
            let leave_1s = ((tick - BUCKETS_1S) % BUCKETS_10S) as usize;
            self.sum_1s = self.sum_1s.saturating_sub(self.buckets[leave_1s]);

            let leave_5s = ((tick - BUCKETS_5S) % BUCKETS_10S) as usize;
            self.sum_5s = self.sum_5s.saturating_sub(self.buckets[leave_5s]);

            let leave_10s = (tick % BUCKETS_10S) as usize;
            self.sum_10s = self.sum_10s.saturating_sub(self.buckets[leave_10s]);
            self.buckets[leave_10s] = 0;
        }
        self.last_tick = now_tick;
    }

    /// Returns current `[sum_1s, sum_5s, sum_10s]` after advancing to `now_mono_ns`.
    #[inline]
    pub fn sums_at(&mut self, now_mono_ns: u64) -> [u64; 3] {
        self.advance(now_mono_ns);
        [self.sum_1s, self.sum_5s, self.sum_10s]
    }

    /// Returns cached `[sum_1s, sum_5s, sum_10s]` without advancing the clock.
    #[inline]
    pub const fn current_sums(&self) -> [u64; 3] {
        [self.sum_1s, self.sum_5s, self.sum_10s]
    }

    /// Checks whether adding `delta` at `now_mono_ns` would breach `limits` (`[1s, 5s, 10s]`),
    /// returning `(tier, projected, limit)` on the tightest breached horizon without mutating
    /// bucket contents beyond clock advancement.
    #[inline]
    pub fn check_projected(
        &mut self,
        now_mono_ns: u64,
        delta: u64,
        limits: [u64; 3],
    ) -> Result<(), (WindowTier, u64, u64)> {
        let [s1, s5, s10] = self.sums_at(now_mono_ns);
        let tiers = [
            (WindowTier::OneSecond, s1, limits[0]),
            (WindowTier::FiveSeconds, s5, limits[1]),
            (WindowTier::TenSeconds, s10, limits[2]),
        ];
        for (tier, cur, limit) in tiers {
            let projected = cur.saturating_add(delta);
            if projected > limit {
                return Err((tier, projected, limit));
            }
        }
        Ok(())
    }

    /// Commits `delta` into the active bucket at `now_mono_ns`.
    #[inline]
    pub fn record(&mut self, now_mono_ns: u64, delta: u64) -> Result<[u64; 3], RmsRejectReason> {
        self.advance(now_mono_ns);
        let idx = (self.last_tick % BUCKETS_10S) as usize;
        let next_bucket = self.buckets[idx]
            .checked_add(delta)
            .ok_or(RmsRejectReason::ArithmeticOverflow)?;
        let next_1s = self
            .sum_1s
            .checked_add(delta)
            .ok_or(RmsRejectReason::ArithmeticOverflow)?;
        let next_5s = self
            .sum_5s
            .checked_add(delta)
            .ok_or(RmsRejectReason::ArithmeticOverflow)?;
        let next_10s = self
            .sum_10s
            .checked_add(delta)
            .ok_or(RmsRejectReason::ArithmeticOverflow)?;

        self.buckets[idx] = next_bucket;
        self.sum_1s = next_1s;
        self.sum_5s = next_5s;
        self.sum_10s = next_10s;
        Ok([next_1s, next_5s, next_10s])
    }
}

/// Static per-symbol RMS limit configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SymbolLimitConfig {
    /// Whether new/modify order flow is enabled for this symbol.
    pub enabled: bool,
    /// Whether an intent carrying `bypass_rms = true` is permitted to bypass soft caps.
    pub allow_bypass: bool,
    /// Maximum quantity (in integer lots) of any single order (`> 0`).
    pub max_order_qty_lots: i64,
    /// Maximum notional (`price_ticks * qty_lots * notional_multiplier`) of any single order (`> 0`).
    pub max_order_notional: i128,
    /// Contract/tick notional multiplier (`> 0`, defaults to `1`).
    pub notional_multiplier: i64,
    /// Maximum new/modify order count across `[1s, 5s, 10s]`.
    pub new_count_limits: [u64; 3],
    /// Maximum cumulative order quantity lots across `[1s, 5s, 10s]`.
    pub order_qty_limits: [i64; 3],
    /// Optional fat-finger price cage ratio around the symbol's reference price.
    pub fat_finger_ratio: Option<FatFingerRatio>,
}

impl SymbolLimitConfig {
    /// Disabled fail-closed default configuration for unconfigured symbol slots.
    #[inline]
    pub const fn disabled() -> Self {
        Self {
            enabled: false,
            allow_bypass: false,
            max_order_qty_lots: 0,
            max_order_notional: 0,
            notional_multiplier: 1,
            new_count_limits: [0, 0, 0],
            order_qty_limits: [0, 0, 0],
            fat_finger_ratio: None,
        }
    }

    /// Validates internal invariants before installation or hot reload.
    pub fn validate(&self) -> Result<(), RmsConfigErrorKind> {
        if self.notional_multiplier <= 0 {
            return Err(RmsConfigErrorKind::InvalidNotionalMultiplier);
        }
        if !self.enabled {
            // Disabled symbol slots may carry zero or positive caps; if positive caps are supplied,
            // they must still obey monotonicity.
            if self.max_order_qty_lots < 0 || self.max_order_notional < 0 {
                return Err(RmsConfigErrorKind::NonPositiveSingleLimit);
            }
            return Ok(());
        }
        if self.max_order_qty_lots <= 0 || self.max_order_notional <= 0 {
            return Err(RmsConfigErrorKind::NonPositiveSingleLimit);
        }
        let [c1, c5, c10] = self.new_count_limits;
        if c1 == 0 || c1 > c5 || c5 > c10 {
            return Err(RmsConfigErrorKind::InvalidNewCountWindows);
        }
        let [q1, q5, q10] = self.order_qty_limits;
        if q1 <= 0 || q1 > q5 || q5 > q10 {
            return Err(RmsConfigErrorKind::InvalidOrderQtyWindows);
        }
        Ok(())
    }
}

/// Reason an outbound order check was rejected by [`RmsEngine`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RmsRejectReason {
    UnknownSymbol,
    SymbolDisabled,
    InvalidQuantity,
    InvalidPrice,
    MissingReferencePrice,
    FatFingerPrice {
        price_ticks: i64,
        ref_price_ticks: i64,
        min_sell_ticks: i64,
        max_buy_ticks: i64,
    },
    MaxOrderQty {
        qty_lots: i64,
        limit_lots: i64,
    },
    MaxOrderNotional {
        notional: i128,
        limit: i128,
    },
    NewCountWindow {
        tier: WindowTier,
        projected: u64,
        limit: u64,
    },
    OrderQtyWindow {
        tier: WindowTier,
        projected_lots: i64,
        limit_lots: i64,
    },
    ArithmeticOverflow,
}

impl RmsRejectReason {
    #[inline]
    pub const fn code(self) -> &'static str {
        match self {
            Self::UnknownSymbol => "RMS_UNKNOWN_SYMBOL",
            Self::SymbolDisabled => "RMS_SYMBOL_DISABLED",
            Self::InvalidQuantity => "RMS_INVALID_QUANTITY",
            Self::InvalidPrice => "RMS_INVALID_PRICE",
            Self::MissingReferencePrice => "RMS_MISSING_REFERENCE_PRICE",
            Self::FatFingerPrice { .. } => "RMS_FAT_FINGER_PRICE",
            Self::MaxOrderQty { .. } => "RMS_MAX_ORDER_QTY",
            Self::MaxOrderNotional { .. } => "RMS_MAX_ORDER_NOTIONAL",
            Self::NewCountWindow { .. } => "RMS_NEW_COUNT_WINDOW",
            Self::OrderQtyWindow { .. } => "RMS_ORDER_QTY_WINDOW",
            Self::ArithmeticOverflow => "RMS_ARITHMETIC_OVERFLOW",
        }
    }
}

impl fmt::Display for RmsRejectReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.code())
    }
}

impl std::error::Error for RmsRejectReason {}

/// Error kind produced while parsing or validating a static RMS configuration table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RmsConfigErrorKind {
    InvalidSyntax,
    UnknownField,
    MissingField(&'static str),
    DuplicateSymbolId(usize),
    SymbolOutOfRange { symbol_id: usize, capacity: usize },
    InvalidValue(&'static str),
    NonPositiveSingleLimit,
    InvalidNotionalMultiplier,
    InvalidNewCountWindows,
    InvalidOrderQtyWindows,
}

/// Line-attributed static RMS configuration error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RmsConfigError {
    pub line: usize,
    pub kind: RmsConfigErrorKind,
}

impl fmt::Display for RmsConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RMS config error on line {}: {:?}", self.line, self.kind)
    }
}

impl std::error::Error for RmsConfigError {}

/// Parsed and validated static RMS configuration table ready for transactional swap.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RmsConfigTable {
    entries: Vec<(usize, SymbolLimitConfig)>,
}

impl RmsConfigTable {
    /// Parses a static RMS configuration table from key=value lines (`#` comments supported).
    ///
    /// Example line:
    /// `symbol_id=0 enabled=true allow_bypass=true max_qty=1000 max_notional=50000000000 new_count=50/200/350 order_qty=5000/20000/35000 fat_finger=0.05`
    pub fn parse_str(raw: &str) -> Result<Self, RmsConfigError> {
        let mut entries: Vec<(usize, SymbolLimitConfig)> = Vec::new();
        for (idx, raw_line) in raw.lines().enumerate() {
            let line_no = idx + 1;
            let content = raw_line.split('#').next().unwrap_or("").trim();
            if content.is_empty() {
                continue;
            }

            let mut symbol_id: Option<usize> = None;
            let mut enabled = true;
            let mut allow_bypass = false;
            let mut max_qty: Option<i64> = None;
            let mut max_notional: Option<i128> = None;
            let mut notional_multiplier: i64 = 1;
            let mut new_count: Option<[u64; 3]> = None;
            let mut order_qty: Option<[i64; 3]> = None;
            let mut fat_finger: Option<FatFingerRatio> = None;

            for token in content.split(|c: char| c.is_ascii_whitespace() || c == ',') {
                if token.is_empty() {
                    continue;
                }
                let Some((k, v)) = token.split_once('=') else {
                    return Err(RmsConfigError {
                        line: line_no,
                        kind: RmsConfigErrorKind::InvalidSyntax,
                    });
                };
                let key = k.trim();
                let val = v.trim();
                match key {
                    "symbol_id" => {
                        symbol_id = Some(val.parse::<usize>().map_err(|_| RmsConfigError {
                            line: line_no,
                            kind: RmsConfigErrorKind::InvalidValue("symbol_id"),
                        })?);
                    }
                    "enabled" => {
                        enabled = parse_bool(val).ok_or(RmsConfigError {
                            line: line_no,
                            kind: RmsConfigErrorKind::InvalidValue("enabled"),
                        })?;
                    }
                    "allow_bypass" => {
                        allow_bypass = parse_bool(val).ok_or(RmsConfigError {
                            line: line_no,
                            kind: RmsConfigErrorKind::InvalidValue("allow_bypass"),
                        })?;
                    }
                    "max_qty" => {
                        max_qty = Some(val.parse::<i64>().map_err(|_| RmsConfigError {
                            line: line_no,
                            kind: RmsConfigErrorKind::InvalidValue("max_qty"),
                        })?);
                    }
                    "max_notional" => {
                        max_notional = Some(val.parse::<i128>().map_err(|_| RmsConfigError {
                            line: line_no,
                            kind: RmsConfigErrorKind::InvalidValue("max_notional"),
                        })?);
                    }
                    "notional_multiplier" => {
                        notional_multiplier = val.parse::<i64>().map_err(|_| RmsConfigError {
                            line: line_no,
                            kind: RmsConfigErrorKind::InvalidValue("notional_multiplier"),
                        })?;
                    }
                    "new_count" => {
                        new_count = Some(parse_triple_u64(val).ok_or(RmsConfigError {
                            line: line_no,
                            kind: RmsConfigErrorKind::InvalidValue("new_count"),
                        })?);
                    }
                    "order_qty" => {
                        order_qty = Some(parse_triple_i64(val).ok_or(RmsConfigError {
                            line: line_no,
                            kind: RmsConfigErrorKind::InvalidValue("order_qty"),
                        })?);
                    }
                    "fat_finger" => {
                        if val.eq_ignore_ascii_case("none") || val.eq_ignore_ascii_case("off") {
                            fat_finger = None;
                        } else {
                            fat_finger =
                                Some(FatFingerRatio::parse_str(val).ok_or(RmsConfigError {
                                    line: line_no,
                                    kind: RmsConfigErrorKind::InvalidValue("fat_finger"),
                                })?);
                        }
                    }
                    _ => {
                        return Err(RmsConfigError {
                            line: line_no,
                            kind: RmsConfigErrorKind::UnknownField,
                        });
                    }
                }
            }

            let sid = symbol_id.ok_or(RmsConfigError {
                line: line_no,
                kind: RmsConfigErrorKind::MissingField("symbol_id"),
            })?;
            if entries.iter().any(|(existing, _)| *existing == sid) {
                return Err(RmsConfigError {
                    line: line_no,
                    kind: RmsConfigErrorKind::DuplicateSymbolId(sid),
                });
            }

            let cfg = if enabled {
                SymbolLimitConfig {
                    enabled: true,
                    allow_bypass,
                    max_order_qty_lots: max_qty.ok_or(RmsConfigError {
                        line: line_no,
                        kind: RmsConfigErrorKind::MissingField("max_qty"),
                    })?,
                    max_order_notional: max_notional.ok_or(RmsConfigError {
                        line: line_no,
                        kind: RmsConfigErrorKind::MissingField("max_notional"),
                    })?,
                    notional_multiplier,
                    new_count_limits: new_count.ok_or(RmsConfigError {
                        line: line_no,
                        kind: RmsConfigErrorKind::MissingField("new_count"),
                    })?,
                    order_qty_limits: order_qty.ok_or(RmsConfigError {
                        line: line_no,
                        kind: RmsConfigErrorKind::MissingField("order_qty"),
                    })?,
                    fat_finger_ratio: fat_finger,
                }
            } else {
                SymbolLimitConfig {
                    enabled: false,
                    allow_bypass,
                    max_order_qty_lots: max_qty.unwrap_or(0),
                    max_order_notional: max_notional.unwrap_or(0),
                    notional_multiplier,
                    new_count_limits: new_count.unwrap_or([0, 0, 0]),
                    order_qty_limits: order_qty.unwrap_or([0, 0, 0]),
                    fat_finger_ratio: fat_finger,
                }
            };

            cfg.validate().map_err(|kind| RmsConfigError {
                line: line_no,
                kind,
            })?;
            entries.push((sid, cfg));
        }
        Ok(Self { entries })
    }

    /// Borrowing slice of validated `(symbol_id, SymbolLimitConfig)` pairs.
    #[inline]
    pub fn entries(&self) -> &[(usize, SymbolLimitConfig)] {
        &self.entries
    }
}

fn parse_bool(raw: &str) -> Option<bool> {
    match raw {
        "true" | "1" => Some(true),
        "false" | "0" => Some(false),
        _ => None,
    }
}

fn parse_triple_u64(raw: &str) -> Option<[u64; 3]> {
    let mut parts = raw.split('/');
    let a = parts.next()?.trim().parse::<u64>().ok()?;
    let b = parts.next()?.trim().parse::<u64>().ok()?;
    let c = parts.next()?.trim().parse::<u64>().ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some([a, b, c])
}

fn parse_triple_i64(raw: &str) -> Option<[i64; 3]> {
    let mut parts = raw.split('/');
    let a = parts.next()?.trim().parse::<i64>().ok()?;
    let b = parts.next()?.trim().parse::<i64>().ok()?;
    let c = parts.next()?.trim().parse::<i64>().ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some([a, b, c])
}

/// Sequence-locked atomic configuration cell for one symbol (`REQ` reads lock-free; cold `SIGUSR1`
/// writes under the off-path reload mutex).
#[repr(align(64))]
#[derive(Debug)]
struct SymbolConfigCell {
    seq: AtomicU64,
    flags: AtomicU64,
    max_order_qty_lots: AtomicI64,
    max_order_notional_lo: AtomicU64,
    max_order_notional_hi: AtomicU64,
    notional_multiplier: AtomicI64,
    new_count_1s: AtomicU64,
    new_count_5s: AtomicU64,
    new_count_10s: AtomicU64,
    order_qty_1s: AtomicI64,
    order_qty_5s: AtomicI64,
    order_qty_10s: AtomicI64,
    fat_finger_ppb: AtomicI64,
}

impl SymbolConfigCell {
    fn new(cfg: SymbolLimitConfig) -> Self {
        let cell = Self {
            seq: AtomicU64::new(2),
            flags: AtomicU64::new(0),
            max_order_qty_lots: AtomicI64::new(0),
            max_order_notional_lo: AtomicU64::new(0),
            max_order_notional_hi: AtomicU64::new(0),
            notional_multiplier: AtomicI64::new(1),
            new_count_1s: AtomicU64::new(0),
            new_count_5s: AtomicU64::new(0),
            new_count_10s: AtomicU64::new(0),
            order_qty_1s: AtomicI64::new(0),
            order_qty_5s: AtomicI64::new(0),
            order_qty_10s: AtomicI64::new(0),
            fat_finger_ppb: AtomicI64::new(-1),
        };
        cell.store_config(cfg);
        cell
    }

    fn store_config(&self, cfg: SymbolLimitConfig) {
        let cur = self.seq.load(Ordering::Relaxed);
        self.seq.store(cur.wrapping_add(1), Ordering::Release);
        fence(Ordering::Release);

        let mut flags = 0u64;
        if cfg.enabled {
            flags |= FLAG_ENABLED;
        }
        if cfg.allow_bypass {
            flags |= FLAG_ALLOW_BYPASS;
        }
        let notional_bits = cfg.max_order_notional as u128;
        let lo = notional_bits as u64;
        let hi = (notional_bits >> 64) as u64;
        let ff_ppb = cfg.fat_finger_ratio.map_or(-1_i64, |r| i64::from(r.ppb()));

        self.flags.store(flags, Ordering::Relaxed);
        self.max_order_qty_lots
            .store(cfg.max_order_qty_lots, Ordering::Relaxed);
        self.max_order_notional_lo.store(lo, Ordering::Relaxed);
        self.max_order_notional_hi.store(hi, Ordering::Relaxed);
        self.notional_multiplier
            .store(cfg.notional_multiplier, Ordering::Relaxed);
        self.new_count_1s
            .store(cfg.new_count_limits[0], Ordering::Relaxed);
        self.new_count_5s
            .store(cfg.new_count_limits[1], Ordering::Relaxed);
        self.new_count_10s
            .store(cfg.new_count_limits[2], Ordering::Relaxed);
        self.order_qty_1s
            .store(cfg.order_qty_limits[0], Ordering::Relaxed);
        self.order_qty_5s
            .store(cfg.order_qty_limits[1], Ordering::Relaxed);
        self.order_qty_10s
            .store(cfg.order_qty_limits[2], Ordering::Relaxed);
        self.fat_finger_ppb.store(ff_ppb, Ordering::Relaxed);

        self.seq.store(cur.wrapping_add(2), Ordering::Release);
    }

    #[inline]
    fn load_config(&self) -> SymbolLimitConfig {
        loop {
            let s1 = self.seq.load(Ordering::Acquire);
            if s1 & 1 != 0 {
                core::hint::spin_loop();
                continue;
            }
            let flags = self.flags.load(Ordering::Relaxed);
            let max_order_qty_lots = self.max_order_qty_lots.load(Ordering::Relaxed);
            let lo = self.max_order_notional_lo.load(Ordering::Relaxed);
            let hi = self.max_order_notional_hi.load(Ordering::Relaxed);
            let notional_multiplier = self.notional_multiplier.load(Ordering::Relaxed);
            let c1 = self.new_count_1s.load(Ordering::Relaxed);
            let c5 = self.new_count_5s.load(Ordering::Relaxed);
            let c10 = self.new_count_10s.load(Ordering::Relaxed);
            let q1 = self.order_qty_1s.load(Ordering::Relaxed);
            let q5 = self.order_qty_5s.load(Ordering::Relaxed);
            let q10 = self.order_qty_10s.load(Ordering::Relaxed);
            let ff_ppb = self.fat_finger_ppb.load(Ordering::Relaxed);

            fence(Ordering::Acquire);
            let s2 = self.seq.load(Ordering::Relaxed);
            if s1 == s2 {
                let notional_u128 = (u128::from(hi) << 64) | u128::from(lo);
                let fat_finger_ratio = if ff_ppb > 0 {
                    u32::try_from(ff_ppb)
                        .ok()
                        .and_then(FatFingerRatio::from_ppb)
                } else {
                    None
                };
                return SymbolLimitConfig {
                    enabled: flags & FLAG_ENABLED != 0,
                    allow_bypass: flags & FLAG_ALLOW_BYPASS != 0,
                    max_order_qty_lots,
                    max_order_notional: notional_u128 as i128,
                    notional_multiplier,
                    new_count_limits: [c1, c5, c10],
                    order_qty_limits: [q1, q5, q10],
                    fat_finger_ratio,
                };
            }
            core::hint::spin_loop();
        }
    }
}

/// Cache-line isolated per-symbol runtime atomics updated from `RESP` or reference-price feeds.
#[repr(align(64))]
#[derive(Debug, Default)]
struct SymbolRuntimeAtomics {
    ref_price_ticks: AtomicI64,
    cumulative_fee_atoms: AtomicI64,
    fee_event_count: AtomicU64,
}

#[derive(Debug)]
struct RmsShared {
    configs: Box<[SymbolConfigCell]>,
    runtime: Box<[SymbolRuntimeAtomics]>,
    generation: AtomicU64,
    reload_writer_lock: Mutex<()>,
}

/// Flat per-symbol state owned by [`RmsEngine`] on the `REQ` thread.
#[derive(Debug)]
pub struct SymbolLimits {
    symbol_id: usize,
    new_count: RollingWindow,
    order_qty: RollingWindow,
}

impl SymbolLimits {
    fn new(symbol_id: usize) -> Self {
        Self {
            symbol_id,
            new_count: RollingWindow::new(),
            order_qty: RollingWindow::new(),
        }
    }

    #[inline]
    pub const fn symbol_id(&self) -> usize {
        self.symbol_id
    }

    #[inline]
    pub fn new_count_window(&self) -> &RollingWindow {
        &self.new_count
    }

    #[inline]
    pub fn order_qty_window(&self) -> &RollingWindow {
        &self.order_qty
    }
}

/// Outbound order parameters evaluated by [`RmsEngine::check_order`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RmsOrderCheck {
    pub symbol_id: usize,
    pub side: RmsSide,
    /// Order limit price in integer ticks (`None` for unpriced market orders).
    pub price_ticks: Option<i64>,
    /// Order quantity in integer lots (`> 0`).
    pub qty_lots: i64,
    /// Optional explicit reference price in integer ticks; falls back to the symbol's atomic
    /// `ref_price_ticks` when `None`.
    pub reference_price_ticks: Option<i64>,
    /// Monotonic timestamp in nanoseconds for rolling-window advancement.
    pub now_mono_ns: u64,
    /// Request controlled soft-limit bypass (`allow_bypass` must also be enabled on the symbol).
    pub bypass_rms: bool,
}

/// Thread-safe handle (`Clone + Send + Sync`) for `RESP` atomic fee accumulation, reference-price
/// updates, and off-thread `SIGUSR1` limit reloads without locking `REQ`.
#[derive(Debug, Clone)]
pub struct RmsReloadHandle {
    shared: Arc<RmsShared>,
}

impl RmsReloadHandle {
    /// Number of flat symbol slots managed by the engine.
    #[inline]
    pub fn symbol_count(&self) -> usize {
        self.shared.configs.len()
    }

    /// Current configuration reload generation (`1` at initial creation, incremented per reload).
    #[inline]
    pub fn generation(&self) -> u64 {
        self.shared.generation.load(Ordering::Acquire)
    }

    /// Reads the active [`SymbolLimitConfig`] snapshot for `symbol_id`.
    #[inline]
    pub fn config(&self, symbol_id: usize) -> Option<SymbolLimitConfig> {
        self.shared
            .configs
            .get(symbol_id)
            .map(SymbolConfigCell::load_config)
    }

    /// Publishes an updated reference price (in positive integer ticks) for `symbol_id`.
    #[inline]
    pub fn set_reference_price_ticks(
        &self,
        symbol_id: usize,
        ref_price_ticks: i64,
    ) -> Result<(), RmsRejectReason> {
        if ref_price_ticks <= 0 {
            return Err(RmsRejectReason::InvalidPrice);
        }
        let slot = self
            .shared
            .runtime
            .get(symbol_id)
            .ok_or(RmsRejectReason::UnknownSymbol)?;
        slot.ref_price_ticks
            .store(ref_price_ticks, Ordering::Release);
        Ok(())
    }

    /// Reads the latest atomic reference price (`0` if unset) for `symbol_id`.
    #[inline]
    pub fn reference_price_ticks(&self, symbol_id: usize) -> Option<i64> {
        self.shared
            .runtime
            .get(symbol_id)
            .map(|r| r.ref_price_ticks.load(Ordering::Acquire))
    }

    /// Atomically accumulates a signed fee/rebate delta (`RESP` thread) without touching `REQ`.
    ///
    /// Returns the updated cumulative fee in atoms for `symbol_id`.
    #[inline]
    pub fn record_execution_fee(
        &self,
        symbol_id: usize,
        fee_delta_atoms: i64,
    ) -> Result<i64, RmsRejectReason> {
        let slot = self
            .shared
            .runtime
            .get(symbol_id)
            .ok_or(RmsRejectReason::UnknownSymbol)?;
        let prev = slot
            .cumulative_fee_atoms
            .fetch_add(fee_delta_atoms, Ordering::Relaxed);
        slot.fee_event_count.fetch_add(1, Ordering::Relaxed);
        Ok(prev.wrapping_add(fee_delta_atoms))
    }

    /// Reads `(cumulative_fee_atoms, fee_event_count)` for `symbol_id`.
    #[inline]
    pub fn cumulative_fee(&self, symbol_id: usize) -> Option<(i64, u64)> {
        let slot = self.shared.runtime.get(symbol_id)?;
        Some((
            slot.cumulative_fee_atoms.load(Ordering::Relaxed),
            slot.fee_event_count.load(Ordering::Relaxed),
        ))
    }

    /// Parses, validates, and atomically swaps per-symbol limits (`SIGUSR1` handler).
    ///
    /// On any parse or validation error, returns `Err(RmsConfigError)` without modifying any
    /// active symbol limit or advancing `generation`.
    pub fn reload_limits(&self, raw_config: &str) -> Result<u64, RmsConfigError> {
        let table = RmsConfigTable::parse_str(raw_config)?;
        self.apply_table(&table)
    }

    /// Applies an already-parsed [`RmsConfigTable`] transactionally.
    pub fn apply_table(&self, table: &RmsConfigTable) -> Result<u64, RmsConfigError> {
        let cap = self.shared.configs.len();
        for (idx, &(sid, ref cfg)) in table.entries().iter().enumerate() {
            if sid >= cap {
                return Err(RmsConfigError {
                    line: idx + 1,
                    kind: RmsConfigErrorKind::SymbolOutOfRange {
                        symbol_id: sid,
                        capacity: cap,
                    },
                });
            }
            cfg.validate().map_err(|kind| RmsConfigError {
                line: idx + 1,
                kind,
            })?;
        }

        let _guard = self
            .shared
            .reload_writer_lock
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        for &(sid, cfg) in table.entries() {
            self.shared.configs[sid].store_config(cfg);
        }
        let next_gen = self.shared.generation.fetch_add(1, Ordering::AcqRel) + 1;
        Ok(next_gen)
    }

    /// Replaces the configuration of a single `symbol_id` after validation.
    pub fn set_symbol_config(
        &self,
        symbol_id: usize,
        cfg: SymbolLimitConfig,
    ) -> Result<u64, RmsConfigError> {
        let cap = self.shared.configs.len();
        if symbol_id >= cap {
            return Err(RmsConfigError {
                line: 1,
                kind: RmsConfigErrorKind::SymbolOutOfRange {
                    symbol_id,
                    capacity: cap,
                },
            });
        }
        cfg.validate()
            .map_err(|kind| RmsConfigError { line: 1, kind })?;
        let _guard = self
            .shared
            .reload_writer_lock
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        self.shared.configs[symbol_id].store_config(cfg);
        Ok(self.shared.generation.fetch_add(1, Ordering::AcqRel) + 1)
    }
}

/// Flat, lock-free per-symbol Risk Management System engine owned by the `REQ` thread.
#[derive(Debug)]
pub struct RmsEngine {
    symbols: Box<[SymbolLimits]>,
    shared: Arc<RmsShared>,
}

impl RmsEngine {
    /// Allocates a flat `symbol_count` RMS table with all symbols initially disabled (fail-closed).
    pub fn new(symbol_count: usize) -> Self {
        let configs: Vec<SymbolConfigCell> = (0..symbol_count)
            .map(|_| SymbolConfigCell::new(SymbolLimitConfig::disabled()))
            .collect();
        let runtime: Vec<SymbolRuntimeAtomics> = (0..symbol_count)
            .map(|_| SymbolRuntimeAtomics::default())
            .collect();
        let symbols: Vec<SymbolLimits> = (0..symbol_count).map(SymbolLimits::new).collect();
        Self {
            symbols: symbols.into_boxed_slice(),
            shared: Arc::new(RmsShared {
                configs: configs.into_boxed_slice(),
                runtime: runtime.into_boxed_slice(),
                generation: AtomicU64::new(1),
                reload_writer_lock: Mutex::new(()),
            }),
        }
    }

    /// Constructs an `RmsEngine` from validated per-symbol configurations indexed `0..configs.len()`.
    pub fn from_configs(configs: &[SymbolLimitConfig]) -> Result<Self, RmsConfigError> {
        for (idx, cfg) in configs.iter().enumerate() {
            cfg.validate().map_err(|kind| RmsConfigError {
                line: idx + 1,
                kind,
            })?;
        }
        let cells: Vec<SymbolConfigCell> =
            configs.iter().copied().map(SymbolConfigCell::new).collect();
        let runtime: Vec<SymbolRuntimeAtomics> = (0..configs.len())
            .map(|_| SymbolRuntimeAtomics::default())
            .collect();
        let symbols: Vec<SymbolLimits> = (0..configs.len()).map(SymbolLimits::new).collect();
        Ok(Self {
            symbols: symbols.into_boxed_slice(),
            shared: Arc::new(RmsShared {
                configs: cells.into_boxed_slice(),
                runtime: runtime.into_boxed_slice(),
                generation: AtomicU64::new(1),
                reload_writer_lock: Mutex::new(()),
            }),
        })
    }

    /// Constructs a `symbol_count` engine and loads initial limits from `raw_config`.
    pub fn from_static_config(
        symbol_count: usize,
        raw_config: &str,
    ) -> Result<Self, RmsConfigError> {
        let engine = Self::new(symbol_count);
        let table = RmsConfigTable::parse_str(raw_config)?;
        engine.reload_handle().apply_table(&table)?;
        // Reset initial generation back to 1 after cold bootstrap.
        engine.shared.generation.store(1, Ordering::Release);
        Ok(engine)
    }

    /// Returns a cloneable `Send + Sync` handle for `RESP` fee updates and `SIGUSR1` reloads.
    #[inline]
    pub fn reload_handle(&self) -> RmsReloadHandle {
        RmsReloadHandle {
            shared: Arc::clone(&self.shared),
        }
    }

    /// Number of flat symbol slots in the engine.
    #[inline]
    pub fn symbol_count(&self) -> usize {
        self.symbols.len()
    }

    /// Current configuration generation.
    #[inline]
    pub fn generation(&self) -> u64 {
        self.shared.generation.load(Ordering::Acquire)
    }

    /// Direct `O(1)` view of a symbol's rolling-window state.
    #[inline]
    pub fn symbol(&self, symbol_id: usize) -> Option<&SymbolLimits> {
        self.symbols.get(symbol_id)
    }

    /// Reads the active [`SymbolLimitConfig`] for `symbol_id`.
    #[inline]
    pub fn config(&self, symbol_id: usize) -> Option<SymbolLimitConfig> {
        self.shared
            .configs
            .get(symbol_id)
            .map(SymbolConfigCell::load_config)
    }

    /// Updates reference price ticks for `symbol_id`.
    #[inline]
    pub fn set_reference_price_ticks(
        &self,
        symbol_id: usize,
        ref_price_ticks: i64,
    ) -> Result<(), RmsRejectReason> {
        self.reload_handle()
            .set_reference_price_ticks(symbol_id, ref_price_ticks)
    }

    /// Atomically accumulates a signed execution fee/rebate delta (`RESP` or single-thread path).
    #[inline]
    pub fn record_execution_fee(
        &self,
        symbol_id: usize,
        fee_delta_atoms: i64,
    ) -> Result<i64, RmsRejectReason> {
        self.reload_handle()
            .record_execution_fee(symbol_id, fee_delta_atoms)
    }

    /// Reads `(cumulative_fee_atoms, fee_event_count)` for `symbol_id`.
    #[inline]
    pub fn cumulative_fee(&self, symbol_id: usize) -> Option<(i64, u64)> {
        self.reload_handle().cumulative_fee(symbol_id)
    }

    /// Reloads static RMS limits transactionally, retaining old limits on any parse/validation error.
    pub fn reload_limits(&self, raw_config: &str) -> Result<u64, RmsConfigError> {
        self.reload_handle().reload_limits(raw_config)
    }

    /// Evaluates an outbound order on the `REQ` hot path (`O(1)` flat slice index, zero locks,
    /// zero heap allocation) and commits `new_count` / `order_qty` if admitted.
    #[inline]
    pub fn check_order(&mut self, order: &RmsOrderCheck) -> Result<(), RmsRejectReason> {
        let sid = order.symbol_id;
        if sid >= self.symbols.len() {
            return Err(RmsRejectReason::UnknownSymbol);
        }
        if order.qty_lots <= 0 {
            return Err(RmsRejectReason::InvalidQuantity);
        }
        if let Some(px) = order.price_ticks
            && px <= 0
        {
            return Err(RmsRejectReason::InvalidPrice);
        }

        let cfg = self.shared.configs[sid].load_config();
        if !cfg.enabled {
            return Err(RmsRejectReason::SymbolDisabled);
        }

        let effective_bypass = order.bypass_rms && cfg.allow_bypass;
        let qty_u64 =
            u64::try_from(order.qty_lots).map_err(|_| RmsRejectReason::InvalidQuantity)?;

        if !effective_bypass {
            if order.qty_lots > cfg.max_order_qty_lots {
                return Err(RmsRejectReason::MaxOrderQty {
                    qty_lots: order.qty_lots,
                    limit_lots: cfg.max_order_qty_lots,
                });
            }

            let ref_px = order.reference_price_ticks.unwrap_or_else(|| {
                self.shared.runtime[sid]
                    .ref_price_ticks
                    .load(Ordering::Acquire)
            });

            // Valuation price: explicit order limit price, or reference price for market orders.
            let valuation_px = match order.price_ticks {
                Some(px) => px,
                None => {
                    if ref_px <= 0 {
                        return Err(RmsRejectReason::MissingReferencePrice);
                    }
                    ref_px
                }
            };

            if let Some(ff_ratio) = cfg.fat_finger_ratio
                && let Some(px) = order.price_ticks
            {
                ff_ratio.check_price_ticks(order.side, px, ref_px)?;
            }

            let notional = i128::from(valuation_px)
                .checked_mul(i128::from(order.qty_lots))
                .and_then(|v| v.checked_mul(i128::from(cfg.notional_multiplier)))
                .ok_or(RmsRejectReason::ArithmeticOverflow)?;
            if notional > cfg.max_order_notional {
                return Err(RmsRejectReason::MaxOrderNotional {
                    notional,
                    limit: cfg.max_order_notional,
                });
            }

            let sym = &mut self.symbols[sid];
            sym.new_count
                .check_projected(order.now_mono_ns, 1, cfg.new_count_limits)
                .map_err(|(tier, projected, limit)| RmsRejectReason::NewCountWindow {
                    tier,
                    projected,
                    limit,
                })?;

            let qty_limits_u64 = [
                cfg.order_qty_limits[0] as u64,
                cfg.order_qty_limits[1] as u64,
                cfg.order_qty_limits[2] as u64,
            ];
            sym.order_qty
                .check_projected(order.now_mono_ns, qty_u64, qty_limits_u64)
                .map_err(|(tier, projected, limit)| RmsRejectReason::OrderQtyWindow {
                    tier,
                    projected_lots: i64::try_from(projected).unwrap_or(i64::MAX),
                    limit_lots: i64::try_from(limit).unwrap_or(i64::MAX),
                })?;
        }

        // Commit sent order flow into rolling windows only after all active checks pass.
        let sym = &mut self.symbols[sid];
        sym.new_count.record(order.now_mono_ns, 1)?;
        sym.order_qty.record(order.now_mono_ns, qty_u64)?;
        Ok(())
    }
}
