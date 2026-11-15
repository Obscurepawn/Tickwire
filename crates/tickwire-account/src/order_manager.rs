//! Zero-allocation ORS `OrderManager<CHECK_STP>` (`human-docs/3`, `3.2`–`3.11`, `11`).
//!
//! Combines:
//! - Startup-preallocated (`MlockedSlice`) power-of-two per-strategy order slot rings plus
//!   dedicated per-strategy `dry_run` scratch slots (`slot_idx = base + (seqnum & (cap - 1))`).
//! - Fine-grained per-order spinlock (`LockedOrder`) and strict `LockedOrder -> LockedSymbolBook`
//!   lock hierarchy with lock-free atomic-BBO `self_cross` checks on the send path.
//! - Immediate `SlotState::Retired` transition and `OrsStpBook` unlinking in the same call on
//!   full fill, cancel to zero, or reject (never waiting on `fee_reconciled`).
//! - `FillRecordPool` deduplication and `TRADE_LITE` / post-trade rebate `amendment = true` fee
//!   updates without double-applying fill quantity/holds/positions or freezing the account.
//! - `One ack`, `FrontAck`, `ProjectedFill` (`on_cum_qty`), `pending_cxl` reason attribution,
//!   `cancel-before-ack`, `DopState`, `OrsQueryDesk`, `UtaWalletLedger`, and stateless cold
//!   bootstrap from Exchange REST snapshots + `SelfDescribingClientId` (`tw1...`).

use crate::dop_state::{DopState, MAX_ORS_STRATS};
use crate::fill_record::{
    FillKey, FillObservationInput, FillRecordOutcome, FillRecordPool, OrsFeeSpec,
    trade_id_to_exec_ref,
};
use crate::ors_query_desk::{
    DEFAULT_QUERY_BASE_BACKOFF_NS, OrsQueryDesk, OrsQueryOrigin, is_ambiguous_venue_outcome,
};
use crate::ors_stp::{LockedSymbolBook, OrderLockProof, OrsStpBook};
use crate::spinlock::{CachePadded, SpinLockGuard, TtasSpinLock};
use crate::uta_ledger::{OrsSymbolSpec, UtaWalletLedger};
use core::sync::atomic::{AtomicU64, Ordering};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use tickwire_shm_sys::{MlockOutcome, MlockPolicy, MlockedSlice};
use tickwire_venue as v;

const SEQNUM_MASK_56: u64 = (1u64 << 56) - 1;
const STATE_SHIFT_56: u32 = 56;
const TW1_PREFIX: &[u8; 3] = b"tw1";
const TW1_BASE36_LEN: usize = 29;
const TW1_ALPHABET: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";
const TW1_TAG_MASK: u64 = (1u64 << 30) - 1;

/// Fixed-capacity stack ASCII string (`Copy`, zero heap allocation).
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct InlineAscii<const N: usize> {
    bytes: [u8; N],
    len: u8,
}

impl<const N: usize> Default for InlineAscii<N> {
    #[inline]
    fn default() -> Self {
        Self::empty()
    }
}

impl<const N: usize> core::fmt::Debug for InlineAscii<N> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_tuple("InlineAscii").field(&self.as_str()).finish()
    }
}

impl<const N: usize> InlineAscii<N> {
    #[inline]
    pub const fn empty() -> Self {
        Self {
            bytes: [0u8; N],
            len: 0,
        }
    }

    /// Stores `s` if it fits in `N` ASCII bytes (`<= 255` and `<= N`).
    #[inline]
    pub fn try_from_str(s: &str) -> Option<Self> {
        let raw = s.as_bytes();
        if raw.len() > N || raw.len() > usize::from(u8::MAX) || !s.is_ascii() {
            return None;
        }
        let mut bytes = [0u8; N];
        bytes[..raw.len()].copy_from_slice(raw);
        Some(Self {
            bytes,
            len: raw.len() as u8,
        })
    }

    /// Truncates `s` at `N` ASCII bytes if longer, preserving prefix without panicking or
    /// allocating.
    #[inline]
    pub fn lossy_ascii(s: &str) -> Self {
        let raw = s.as_bytes();
        let cap = N.min(usize::from(u8::MAX));
        let take = raw.len().min(cap);
        let mut bytes = [0u8; N];
        for (dst, &src) in bytes[..take].iter_mut().zip(&raw[..take]) {
            *dst = if src.is_ascii() { src } else { b'?' };
        }
        Self {
            bytes,
            len: take as u8,
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline]
    pub fn clear(&mut self) {
        self.len = 0;
    }

    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes[..usize::from(self.len)]
    }

    #[inline]
    pub fn as_str(&self) -> &str {
        core::str::from_utf8(self.as_bytes()).unwrap_or("")
    }

    /// Appends `segment` separated by `" | "`, keeping the existing prefix when full
    /// (`human-docs/3.3` `appendNotes`).
    pub fn append_note(&mut self, segment: &str) {
        if segment.is_empty() {
            return;
        }
        let cap = N.min(usize::from(u8::MAX));
        let mut cur = usize::from(self.len);
        if cur >= cap {
            return;
        }
        if cur > 0 {
            let sep = b" | ";
            if cur + sep.len() >= cap {
                return;
            }
            self.bytes[cur..cur + sep.len()].copy_from_slice(sep);
            cur += sep.len();
        }
        let rem = cap - cur;
        let raw = segment.as_bytes();
        let take = raw.len().min(rem);
        for (dst, &src) in self.bytes[cur..cur + take].iter_mut().zip(&raw[..take]) {
            *dst = if src.is_ascii() { src } else { b'?' };
        }
        self.len = (cur + take) as u8;
    }
}

/// Encodes `(order_seqnum, strat_id)` into the 64-bit `ors_order_id` (`(seqnum << 8) | strat_id`).
#[inline]
pub const fn make_ors_order_id(order_seqnum: u64, strat_id: u8) -> u64 {
    (order_seqnum << 8) | (strat_id as u64)
}

/// Splits `ors_order_id` into `(order_seqnum, strat_id)`.
#[inline]
pub const fn split_ors_order_id(ors_order_id: u64) -> (u64, u8) {
    (ors_order_id >> 8, (ors_order_id & 0xff) as u8)
}

/// Atomic slot publication states (`human-docs/3.2`).
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotState {
    Empty = 0,
    Claimed = 1,
    Live = 2,
    Retired = 3,
}

impl SlotState {
    #[inline]
    const fn from_u8(raw: u8) -> Self {
        match raw {
            0 => Self::Empty,
            1 => Self::Claimed,
            2 => Self::Live,
            _ => Self::Retired,
        }
    }
}

#[inline]
const fn pack_slot_identity(state: SlotState, order_seqnum: u64) -> u64 {
    ((state as u64) << STATE_SHIFT_56) | (order_seqnum & SEQNUM_MASK_56)
}

#[inline]
const fn unpack_slot_identity(word: u64) -> (SlotState, u64) {
    let state = SlotState::from_u8((word >> STATE_SHIFT_56) as u8);
    let seq = word & SEQNUM_MASK_56;
    (state, seq)
}

/// Internal lifecycle state of an `OrsOrder` protected by its per-slot spinlock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrsOrderStatus {
    PendingNew,
    Open,
    Closed,
}

/// Per-order state (`human-docs/3.3`): zero atomic fields inside `OrsOrder`; every read/write
/// occurs under `OrderSlot::order`'s single `TtasSpinLock`.
#[derive(Debug, Clone, Copy)]
pub struct OrsOrder {
    pub ors_order_id: u64,
    pub slot_idx: u32,
    pub strat_id: u8,
    pub account_ref: u8,
    pub symbol_id: u32,
    pub order_seqnum: u64,
    pub side: v::Side,
    pub tif: v::TimeInForce,
    pub post_only: bool,
    pub reduce_only: bool,
    pub check_stp: bool,
    pub leg: u8,
    pub status: OrsOrderStatus,
    pub front_ack_emitted: bool,
    pub price_ticks: i64,
    pub original_qty_lots: i64,
    pub effective_qty_lots: i64,
    pub filled_qty_lots: i64,
    pub projected_fill_qty_lots: i64,
    pub hold_asset_key: v::AssetKey,
    pub hold_scale: u8,
    pub held_atoms: i128,
    /// Saturating count of active strategy/admin cancel requests (`human-docs/3.3`).
    pub pending_cxl: u8,
    /// Armed when a cancel arrives while `PendingNew` and `allow_cancel_before_ack == true`.
    pub cancel_on_ack: bool,
    pub admin_cancel_requested: bool,
    /// Consecutive inconclusive `StatusAsk` count (`human-docs/3.11`).
    pub query_fail_count: u8,
    pub modify_pending: bool,
    pub pending_modify_price_ticks: i64,
    pub pending_modify_qty_lots: i64,
    pub pending_modify_extra_hold_atoms: i128,
    pub created_mono_ns: u64,
    pub client_id: InlineAscii<64>,
    pub venue_order_id: InlineAscii<64>,
    pub notes: InlineAscii<128>,
}

impl Default for OrsOrder {
    fn default() -> Self {
        Self {
            ors_order_id: 0,
            slot_idx: 0,
            strat_id: 0,
            account_ref: 0,
            symbol_id: 0,
            order_seqnum: 0,
            side: v::Side::Buy,
            tif: v::TimeInForce::Gtc,
            post_only: false,
            reduce_only: false,
            check_stp: true,
            leg: 0,
            status: OrsOrderStatus::Closed,
            front_ack_emitted: false,
            price_ticks: 0,
            original_qty_lots: 0,
            effective_qty_lots: 0,
            filled_qty_lots: 0,
            projected_fill_qty_lots: 0,
            hold_asset_key: v::AssetKey::default(),
            hold_scale: 0,
            held_atoms: 0,
            pending_cxl: 0,
            cancel_on_ack: false,
            admin_cancel_requested: false,
            query_fail_count: 0,
            modify_pending: false,
            pending_modify_price_ticks: 0,
            pending_modify_qty_lots: 0,
            pending_modify_extra_hold_atoms: 0,
            created_mono_ns: 0,
            client_id: InlineAscii::empty(),
            venue_order_id: InlineAscii::empty(),
            notes: InlineAscii::empty(),
        }
    }
}

impl OrsOrder {
    #[inline]
    pub const fn is_open_or_pending(&self) -> bool {
        matches!(
            self.status,
            OrsOrderStatus::PendingNew | OrsOrderStatus::Open
        )
    }

    #[inline]
    pub const fn unfilled_qty_lots(&self) -> i64 {
        self.effective_qty_lots.saturating_sub(self.filled_qty_lots)
    }
}

/// Cache-line-isolated slot (`identity` on line 0, `TtasSpinLock<OrsOrder>` on line 1+).
#[derive(Debug)]
struct OrderSlot {
    identity: CachePadded<AtomicU64>,
    order: CachePadded<TtasSpinLock<OrsOrder>>,
}

impl Default for OrderSlot {
    fn default() -> Self {
        Self {
            identity: CachePadded::new(AtomicU64::new(pack_slot_identity(SlotState::Empty, 0))),
            order: CachePadded::new(TtasSpinLock::new(OrsOrder::default())),
        }
    }
}

/// RAII handle holding a single order's spinlock (`human-docs/3`).
///
/// Provides [`LockedOrder::lock_proof`] so [`OrsStpBook::lock_symbol`] can be called strictly
/// inside the `Order -> StpBook` lock hierarchy.
#[derive(Debug)]
pub struct LockedOrder<'a> {
    slot: &'a OrderSlot,
    guard: SpinLockGuard<'a, OrsOrder>,
}

impl<'a> LockedOrder<'a> {
    /// Returns a zero-sized proof token enabling `OrsStpBook::lock_symbol` while this order lock
    /// remains held.
    #[inline]
    pub fn lock_proof(&self) -> OrderLockProof<'_> {
        OrderLockProof::new()
    }

    #[inline]
    pub fn order(&self) -> &OrsOrder {
        &self.guard
    }

    #[inline]
    pub fn order_mut(&mut self) -> &mut OrsOrder {
        &mut self.guard
    }

    #[inline]
    fn publish_live(&self, order_seqnum: u64) {
        self.slot.identity.store(
            pack_slot_identity(SlotState::Live, order_seqnum),
            Ordering::Release,
        );
    }

    #[inline]
    fn retire_in_place(&mut self, order_seqnum: u64) {
        self.guard.status = OrsOrderStatus::Closed;
        self.guard.pending_cxl = 0;
        self.guard.cancel_on_ack = false;
        self.guard.modify_pending = false;
        self.guard.pending_modify_extra_hold_atoms = 0;
        self.guard.client_id.clear();
        self.guard.notes.clear();
        self.slot.identity.store(
            pack_slot_identity(SlotState::Retired, order_seqnum),
            Ordering::Release,
        );
    }
}

/// Per-`account_ref` domain parameters for `SelfDescribingClientId` (`tw1...`) encoding/decoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrsAccountDomainConfig {
    pub account_ref: u8,
    pub risk_domain_id: [u8; 16],
    pub venue: u32,
    pub environment: v::Environment,
    pub exchange_account_id: u64,
    pub epoch: u32,
}

impl OrsAccountDomainConfig {
    #[inline]
    pub const fn client_id_domain(&self, product: v::Product) -> v::ClientIdDomain {
        v::ClientIdDomain {
            risk_domain_id: self.risk_domain_id,
            venue: self.venue,
            environment: self.environment,
            account: self.exchange_account_id,
            product,
        }
    }
}

/// Startup configuration for [`OrderManager`].
#[derive(Debug, Clone)]
pub struct OrderManagerConfig {
    /// Power-of-two slot capacity per strategy (`e.g. 2048`).
    pub slots_per_strat: usize,
    /// Preallocated STP book capacity per symbol.
    pub max_stp_orders_per_symbol: usize,
    /// `FillRecordPool` bucket exponent (`1 << fill_map_exponent` buckets).
    pub fill_map_exponent: u8,
    /// `FillRecordPool` lazy overwrite retention window in seconds.
    pub fill_memory_s: u64,
    /// Enables `CANCEL_ON_SAME_BEFORE_ACK` (`human-docs/3`).
    pub allow_cancel_before_ack: bool,
    /// Optional dead-slot release threshold after consecutive inconclusive status queries (`3.11`).
    pub max_query_count: Option<u8>,
    /// Per-account `OrsQueryDesk` capacity.
    pub query_desk_capacity: usize,
    /// Base `OrsQueryDesk` retry/backoff interval in nanoseconds.
    pub query_base_backoff_ns: u64,
    /// Startup `mlock(2)` policy applied to preallocated order-slot and fill-record pools.
    pub mlock_policy: MlockPolicy,
    /// Concrete `account_ref` domains managed by this ORS process (`1..=255`).
    pub accounts: Vec<OrsAccountDomainConfig>,
    /// Static compact symbol table (`symbol_id == index`).
    pub symbols: Vec<OrsSymbolSpec>,
    /// Optional cold-startup `AssetRef` dictionary (`asset_id -> v::AssetRef`) for `v::Fill`
    /// conversion off the hot path.
    pub asset_refs: Vec<v::AssetRef>,
}

impl OrderManagerConfig {
    pub fn single_account_default(
        account: OrsAccountDomainConfig,
        symbols: Vec<OrsSymbolSpec>,
    ) -> Self {
        Self {
            slots_per_strat: 256,
            max_stp_orders_per_symbol: 256,
            fill_map_exponent: 8,
            fill_memory_s: 300,
            allow_cancel_before_ack: true,
            max_query_count: None,
            query_desk_capacity: 256,
            query_base_backoff_ns: DEFAULT_QUERY_BASE_BACKOFF_NS,
            mlock_policy: MlockPolicy::BestEffort,
            accounts: vec![account],
            symbols,
            asset_refs: Vec::new(),
        }
    }
}

/// Outbound `NewOrder` intent submitted by a strategy on the `REQ` hot path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrsNewOrderRequest {
    pub strat_id: u8,
    pub account_ref: u8,
    pub msg_seqnum: u64,
    pub order_seqnum: u64,
    pub symbol_id: u32,
    pub side: v::Side,
    pub price_ticks: i64,
    pub qty_lots: i64,
    pub tif: v::TimeInForce,
    pub post_only: bool,
    pub reduce_only: bool,
    pub check_stp: bool,
    pub dry_run: bool,
    pub now_mono_ns: u64,
}

/// Successful admission & slot-allocation outcome for [`OrderManager::place_order`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrsPlaceOutcome {
    pub ors_order_id: u64,
    pub slot_idx: u32,
    pub account_ref: u8,
    pub product: v::Product,
    pub client_id: InlineAscii<64>,
    pub should_send_to_venue: bool,
}

/// Synchronous refusal returned by `place_order`, `request_cancel`, or `request_modify`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrsOrderRefusal {
    pub strat_id: u8,
    pub account_ref: u8,
    pub order_seqnum: u64,
    pub reason: v::RefusalReason,
}

/// Strategy-initiated cancel request on `REQ`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrsCancelRequest {
    pub strat_id: u8,
    pub account_ref: u8,
    pub msg_seqnum: u64,
    pub ors_order_id: u64,
    pub now_mono_ns: u64,
}

/// Outcome of [`OrderManager::request_cancel`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrsCancelDispatch {
    pub ors_order_id: u64,
    pub account_ref: u8,
    pub symbol_id: u32,
    pub client_id: InlineAscii<64>,
    pub venue_order_id: InlineAscii<64>,
    /// `true` when the order is already `Open` (or immediate send is allowed); `false` when queued
    /// behind `PendingNew` (`cancel_on_ack`) or coalesced into `OrsQueryDesk` (`cancel_after`).
    pub should_send_now: bool,
    pub coalesced_in_query_desk: bool,
}

/// Strategy-initiated in-place modify request (`human-docs/3.8`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrsModifyRequest {
    pub strat_id: u8,
    pub account_ref: u8,
    pub msg_seqnum: u64,
    pub ors_order_id: u64,
    pub new_price_ticks: i64,
    pub new_qty_lots: i64,
    pub now_mono_ns: u64,
}

/// Outcome of [`OrderManager::request_modify`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrsModifyDispatch {
    pub ors_order_id: u64,
    pub account_ref: u8,
    pub symbol_id: u32,
    pub new_price_ticks: i64,
    pub new_qty_lots: i64,
    pub extra_hold_atoms: i128,
}

/// Execution fill input on `RESP` (`human-docs/3.4`, `3.9`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrsFillObservation {
    pub ors_order_id: u64,
    pub account_ref: u8,
    pub symbol_id: u32,
    pub side: v::Side,
    pub liquidity: v::Liquidity,
    /// Explicit venue trade ref (`0` derives deterministically from `trade_id`).
    pub exec_ref: u64,
    pub trade_id: InlineAscii<64>,
    pub qty_lots: i64,
    pub price_ticks: i64,
    /// `None` for `TRADE_LITE`; `Some` when commission/rebate is known or patched.
    pub fee: Option<OrsFeeSpec>,
    pub received_mono_ns: u64,
}

/// Cancel confirmation input on `RESP`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrsCancelObservation<'a> {
    pub ors_order_id: u64,
    pub cum_qty_lots: Option<i64>,
    pub error_code: Option<i64>,
    pub status_or_msg: &'a str,
    pub stp_mode_or_reason: Option<&'a str>,
    pub explicit_reason: Option<v::TerminalReason>,
    pub received_mono_ns: u64,
}

/// Stack-allocated `FrontAck` report (`Copy`), convertible to [`v::FrontAck`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrsFrontAckReport {
    pub ors_order_id: u64,
    pub strat_id: u8,
    pub account_ref: u8,
    pub order_seqnum: u64,
    pub orig_seqnum: u64,
    pub client_id: InlineAscii<64>,
    pub venue_order_id: InlineAscii<64>,
    pub received_mono_ns: u64,
}

impl OrsFrontAckReport {
    /// Converts this stack report into [`tickwire_venue::FrontAck`] outside any spinlock.
    pub fn to_venue_front_ack(&self) -> Result<v::FrontAck, v::Error> {
        let client_id = if self.client_id.is_empty() {
            None
        } else {
            Some(v::ClientId::new(self.client_id.as_str())?)
        };
        let venue_order_id = if self.venue_order_id.is_empty() {
            None
        } else {
            Some(v::VenueOrderId::new(self.venue_order_id.as_str())?)
        };
        let out = v::FrontAck {
            ors_order_id: self.ors_order_id,
            strat_id: self.strat_id,
            account_ref: self.account_ref,
            order_seqnum: self.order_seqnum,
            orig_seqnum: self.orig_seqnum,
            client_id,
            venue_order_id,
            received_mono_ns: self.received_mono_ns,
        };
        out.validate()?;
        Ok(out)
    }
}

/// Stack-allocated `Ack` report (`Copy`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrsAckReport {
    pub ors_order_id: u64,
    pub strat_id: u8,
    pub account_ref: u8,
    pub symbol_id: u32,
    pub order_seqnum: u64,
    pub price_ticks: i64,
    pub effective_qty_lots: i64,
    pub venue_modified: bool,
    pub venue_order_id: InlineAscii<64>,
    pub received_mono_ns: u64,
}

/// Outcome of [`OrderManager::on_ack`], carrying both any emitted report and whether a queued
/// `cancel-before-ack` must now be transmitted by the session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrsAckOutcome {
    pub report: Option<OrsReport>,
    pub dispatch_pending_cancel: bool,
}

impl OrsAckOutcome {
    #[inline]
    pub const fn empty() -> Self {
        Self {
            report: None,
            dispatch_pending_cancel: false,
        }
    }
}

/// Stack-allocated `Modified` report (`Copy`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrsModifiedReport {
    pub ors_order_id: u64,
    pub strat_id: u8,
    pub account_ref: u8,
    pub symbol_id: u32,
    pub order_seqnum: u64,
    pub price_ticks: i64,
    pub effective_qty_lots: i64,
    pub filled_qty_lots: i64,
    pub received_mono_ns: u64,
}

/// Stack-allocated `Fill` report (`Copy`), supporting real fills, `ProjectedFill`
/// (`projected == true`), and fee/rebate patches (`amendment == true`). Convertible to [`v::Fill`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrsFillReport {
    pub ors_order_id: u64,
    pub strat_id: u8,
    pub account_ref: u8,
    pub symbol_id: u32,
    pub order_seqnum: u64,
    pub exec_ref: u64,
    pub trade_id: InlineAscii<64>,
    pub side: v::Side,
    pub liquidity: v::Liquidity,
    pub last_qty_lots: i64,
    pub last_price_ticks: i64,
    pub cumulative_filled_lots: i64,
    pub cumulative_projected_lots: i64,
    pub fee: Option<OrsFeeSpec>,
    pub amendment: bool,
    pub projected: bool,
    pub order_retired: bool,
    pub received_mono_ns: u64,
}

impl OrsFillReport {
    /// Converts this stack report into [`tickwire_venue::Fill`] outside any spinlock.
    pub fn to_venue_fill<const CHECK_STP: bool>(
        &self,
        manager: &OrderManager<CHECK_STP>,
    ) -> Result<v::Fill, v::Error> {
        let spec = manager
            .symbol_spec(self.symbol_id as usize)
            .ok_or(v::Error::Identity)?;
        let last_quantity = spec
            .qty_decimal(self.last_qty_lots)
            .map_err(|_| v::Error::Quantity)?;
        let last_price = spec
            .price_decimal(self.last_price_ticks)
            .map_err(|_| v::Error::Price)?;
        let cumulative_filled = spec
            .qty_decimal(self.cumulative_filled_lots)
            .map_err(|_| v::Error::Quantity)?;
        let cumulative_projected = spec
            .qty_decimal(self.cumulative_projected_lots)
            .map_err(|_| v::Error::Quantity)?;
        let trade_id = if self.trade_id.is_empty() {
            None
        } else {
            Some(v::VenueTradeId::new(self.trade_id.as_str())?)
        };
        let fee = match self.fee {
            None => None,
            Some(f) => {
                let asset = manager.resolve_asset_ref(f.asset_key.asset_id, f.scale)?;
                let amount = v::Decimal::new(f.signed_atoms, -(f.scale as i8))?;
                Some(v::AssetDelta { asset, amount })
            }
        };
        let out = v::Fill {
            ors_order_id: self.ors_order_id,
            strat_id: self.strat_id,
            account_ref: self.account_ref,
            order_seqnum: self.order_seqnum,
            exec_ref: self.exec_ref,
            trade_id,
            side: self.side,
            liquidity: self.liquidity,
            last_quantity,
            last_price,
            cumulative_filled,
            cumulative_projected,
            fee,
            amendment: self.amendment,
            projected: self.projected,
            received_mono_ns: self.received_mono_ns,
        };
        out.validate()?;
        Ok(out)
    }
}

/// Stack-allocated `Canceled` report (`Copy`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrsCanceledReport {
    pub ors_order_id: u64,
    pub strat_id: u8,
    pub account_ref: u8,
    pub symbol_id: u32,
    pub order_seqnum: u64,
    pub canceled_qty_lots: i64,
    pub cumulative_filled_lots: i64,
    pub reason: v::TerminalReason,
    pub received_mono_ns: u64,
}

/// Stack-allocated `Rejected` (`NAck`) report (`Copy`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrsRejectedReport {
    pub ors_order_id: u64,
    pub strat_id: u8,
    pub account_ref: u8,
    pub symbol_id: u32,
    pub order_seqnum: u64,
    pub reason: v::TerminalReason,
    pub received_mono_ns: u64,
}

/// Stack-allocated `ModifyRejected` (`ModifyNAck`) report (`Copy`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrsModifyRejectedReport {
    pub ors_order_id: u64,
    pub strat_id: u8,
    pub account_ref: u8,
    pub symbol_id: u32,
    pub order_seqnum: u64,
    pub reason: v::TerminalReason,
    pub received_mono_ns: u64,
}

/// Stack-allocated `CancelRejected` (`CancelNAck`) report (`Copy`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrsCancelRejectedReport {
    pub ors_order_id: u64,
    pub strat_id: u8,
    pub account_ref: u8,
    pub symbol_id: u32,
    pub order_seqnum: u64,
    pub reason: v::TerminalReason,
    pub scheduled_status_query: bool,
    pub received_mono_ns: u64,
}

/// Unified zero-allocation (`Copy`) outbound strategy report emitted by [`OrderManager`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrsReport {
    FrontAck(OrsFrontAckReport),
    Ack(OrsAckReport),
    Modified(OrsModifiedReport),
    Fill(OrsFillReport),
    Canceled(OrsCanceledReport),
    Rejected(OrsRejectedReport),
    ModifyRejected(OrsModifyRejectedReport),
    CancelRejected(OrsCancelRejectedReport),
}

/// Unified inbound `RESP` venue event wrapper for callers preferring enum dispatch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrsVenueEvent<'a> {
    FrontAck {
        ors_order_id: u64,
        venue_order_id: Option<&'a str>,
        received_mono_ns: u64,
    },
    Ack {
        ors_order_id: u64,
        venue_order_id: Option<&'a str>,
        ack_price_ticks: Option<i64>,
        ack_qty_lots: Option<i64>,
        received_mono_ns: u64,
    },
    ProjectedFill {
        ors_order_id: u64,
        cum_qty_lots: i64,
        price_ticks: i64,
        received_mono_ns: u64,
    },
    Fill(OrsFillObservation),
    Modified {
        ors_order_id: u64,
        price_ticks: i64,
        qty_lots: i64,
        venue_order_id: Option<&'a str>,
        received_mono_ns: u64,
    },
    ModifyRejected {
        ors_order_id: u64,
        reason: v::TerminalReason,
        received_mono_ns: u64,
    },
    Canceled(OrsCancelObservation<'a>),
    CancelRejected {
        ors_order_id: u64,
        error_code: Option<i64>,
        reason: v::TerminalReason,
        received_mono_ns: u64,
    },
    Rejected {
        ors_order_id: u64,
        error_code: Option<i64>,
        status_or_msg: &'a str,
        explicit_reason: Option<v::TerminalReason>,
        received_mono_ns: u64,
    },
}

/// Open order descriptor returned by `set_strat_enabled(..., false, ...)` for auto-cancellation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrsAutoCancelTarget {
    pub ors_order_id: u64,
    pub strat_id: u8,
    pub account_ref: u8,
    pub symbol_id: u32,
    pub client_id: InlineAscii<64>,
    pub venue_order_id: InlineAscii<64>,
    pub reason: v::TerminalReason,
}

impl Default for OrsAutoCancelTarget {
    fn default() -> Self {
        Self {
            ors_order_id: 0,
            strat_id: 0,
            account_ref: 0,
            symbol_id: 0,
            client_id: InlineAscii::empty(),
            venue_order_id: InlineAscii::empty(),
            reason: v::TerminalReason::ADMIN_ACTION,
        }
    }
}

/// Filter passed to [`OrderManager::find_orders`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OrsOrderFilter {
    pub strat_id: Option<u8>,
    pub account_ref: Option<u8>,
    pub symbol_id: Option<u32>,
}

/// Order status returned by a `StatusAsk` venue REST query (`human-docs/3.11`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrsQueriedOrderStatus {
    Open,
    PartiallyFilled,
    Filled,
    Canceled,
    Expired,
    NotFound,
}

/// Snapshot returned by a `StatusAsk` REST query for [`OrderManager::reconcile_query_snapshot`].
#[derive(Debug, Clone)]
pub struct OrsOrderQuerySnapshot<'a> {
    pub ors_order_id: u64,
    pub account_ref: u8,
    pub symbol_id: u32,
    pub status: OrsQueriedOrderStatus,
    pub venue_order_id: Option<&'a str>,
    pub price_ticks: Option<i64>,
    pub orig_qty_lots: Option<i64>,
    pub cum_filled_lots: i64,
    pub avg_fill_price_ticks: Option<i64>,
    /// Individual trades retrieved alongside the order snapshot (applied strictly before settling
    /// remaining canceled quantity!).
    pub trades: &'a [OrsFillObservation],
    pub received_mono_ns: u64,
}

/// Follow-up action returned by [`OrderManager::reconcile_query_snapshot`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrsQueryReconcileAction {
    Resolved,
    DispatchCancel {
        ors_order_id: u64,
        account_ref: u8,
        symbol_id: u32,
    },
}

/// Wallet balance row inside an Exchange REST cold-bootstrap snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrsRestBalanceRow {
    pub key: v::AssetKey,
    pub scale: u8,
    pub free_atoms: i128,
    pub locked_atoms: i128,
}

/// Derivative position row inside an Exchange REST cold-bootstrap snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrsRestPositionRow {
    pub symbol_id: u32,
    pub signed_lots: i64,
}

/// Open order row inside an Exchange REST cold-bootstrap snapshot (`OpenOrders`).
#[derive(Debug, Clone)]
pub struct OrsRestOpenOrderRow {
    pub symbol_id: u32,
    pub client_id: String,
    pub venue_order_id: String,
    pub side: v::Side,
    pub tif: v::TimeInForce,
    pub post_only: bool,
    pub reduce_only: bool,
    pub price_ticks: i64,
    pub orig_qty_lots: i64,
    pub cum_filled_lots: i64,
    pub created_mono_ns: u64,
}

/// Complete per-account Exchange REST cold-bootstrap snapshot (`Account + OpenOrders`).
#[derive(Debug, Clone)]
pub struct OrsExchangeBootstrapSnapshot {
    pub balances: Vec<OrsRestBalanceRow>,
    pub positions: Vec<OrsRestPositionRow>,
    pub open_orders: Vec<OrsRestOpenOrderRow>,
    /// Set `true` when `balances[*].locked_atoms` from the venue REST snapshot already includes
    /// the open orders' locked margin/funds; set `false` when `free_atoms` holds unreserved total
    /// balance that should be moved into `locked_atoms` per open order.
    pub balances_include_open_order_locks: bool,
}

/// Summary returned by [`OrderManager::bootstrap_from_exchange`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OrsBootstrapSummary {
    pub managed_open_orders: usize,
    pub external_open_orders: usize,
}

/// Zero-allocation ORS `OrderManager<CHECK_STP>`.
#[derive(Debug)]
pub struct OrderManager<const CHECK_STP: bool = true> {
    slots_per_strat: usize,
    slot_mask: usize,
    regular_slots_len: usize,
    allow_cancel_before_ack: bool,
    max_query_count: Option<u8>,
    slot_mlock_outcome: MlockOutcome,
    symbols: Box<[OrsSymbolSpec]>,
    asset_refs: BTreeMap<u64, v::AssetRef>,
    account_domains: Box<[Option<OrsAccountDomainConfig>]>,
    slots: MlockedSlice<OrderSlot>,
    stp_book: OrsStpBook,
    fill_records: FillRecordPool,
    dop_state: DopState,
    wallet_ledger: UtaWalletLedger,
    query_desk: OrsQueryDesk,
}

impl<const CHECK_STP: bool> OrderManager<CHECK_STP> {
    /// Preallocates all order slots, `OrsStpBook`, `FillRecordPool`, `DopState`,
    /// `UtaWalletLedger`, and `OrsQueryDesk` at cold startup, optionally locking hot pools in RAM
    /// via `mlock(2)`.
    pub fn new(config: OrderManagerConfig) -> Result<Self, String> {
        if config.slots_per_strat == 0 || !config.slots_per_strat.is_power_of_two() {
            return Err(format!(
                "slots_per_strat ({}) must be a non-zero power of two",
                config.slots_per_strat
            ));
        }
        if config.accounts.is_empty() {
            return Err(String::from("at least one ORS account must be configured"));
        }
        for (idx, sym) in config.symbols.iter().enumerate() {
            if sym.symbol_id as usize != idx {
                return Err(format!(
                    "symbol_id {} does not match compact table index {idx}",
                    sym.symbol_id
                ));
            }
            sym.validate()
                .map_err(|r| format!("invalid OrsSymbolSpec at index {idx}: {r}"))?;
        }

        let mut account_domains = vec![None; 256];
        let mut registered_refs = Vec::with_capacity(config.accounts.len());
        for acc in &config.accounts {
            if acc.account_ref == 0 || acc.epoch == 0 {
                return Err(String::from("account_ref and epoch must be non-zero"));
            }
            if account_domains[acc.account_ref as usize].is_some() {
                return Err(format!("duplicate account_ref {}", acc.account_ref));
            }
            account_domains[acc.account_ref as usize] = Some(*acc);
            registered_refs.push(acc.account_ref);
        }

        let mut asset_refs = BTreeMap::new();
        for asset in config.asset_refs {
            asset_refs.insert(asset.id, asset);
        }

        let regular_slots_len = MAX_ORS_STRATS
            .checked_mul(config.slots_per_strat)
            .ok_or_else(|| String::from("slots_per_strat overflow"))?;
        let total_slots = regular_slots_len
            .checked_add(MAX_ORS_STRATS)
            .ok_or_else(|| String::from("total_slots overflow"))?;
        let raw_slots = (0..total_slots)
            .map(|_| OrderSlot::default())
            .collect::<Vec<_>>();
        let slots = MlockedSlice::from_vec(raw_slots, config.mlock_policy)
            .map_err(|e| format!("failed to mlock OrderManager slots: {e}"))?;
        let slot_mlock_outcome = slots.outcome();

        let stp_cap = if CHECK_STP {
            config.max_stp_orders_per_symbol.max(1)
        } else {
            1
        };
        let stp_book = OrsStpBook::new(if CHECK_STP { config.symbols.len() } else { 0 }, stp_cap);
        let fill_records = FillRecordPool::new(
            config.fill_map_exponent,
            config.fill_memory_s,
            config.mlock_policy,
        )?;
        let dop_state = DopState::new(&registered_refs);
        let wallet_ledger = UtaWalletLedger::new(config.symbols.len());
        let query_desk =
            OrsQueryDesk::new(config.query_desk_capacity, config.query_base_backoff_ns);

        Ok(Self {
            slots_per_strat: config.slots_per_strat,
            slot_mask: config.slots_per_strat - 1,
            regular_slots_len,
            allow_cancel_before_ack: config.allow_cancel_before_ack,
            max_query_count: config.max_query_count,
            slot_mlock_outcome,
            symbols: config.symbols.into_boxed_slice(),
            asset_refs,
            account_domains: account_domains.into_boxed_slice(),
            slots,
            stp_book,
            fill_records,
            dop_state,
            wallet_ledger,
            query_desk,
        })
    }

    #[inline]
    pub fn dop_state(&self) -> &DopState {
        &self.dop_state
    }

    #[inline]
    pub fn stp_book(&self) -> &OrsStpBook {
        &self.stp_book
    }

    #[inline]
    pub fn fill_records(&self) -> &FillRecordPool {
        &self.fill_records
    }

    #[inline]
    pub fn wallet_ledger(&self) -> &UtaWalletLedger {
        &self.wallet_ledger
    }

    #[inline]
    pub fn query_desk(&self) -> &OrsQueryDesk {
        &self.query_desk
    }

    #[inline]
    pub fn slot_mlock_outcome(&self) -> MlockOutcome {
        self.slot_mlock_outcome
    }

    #[inline]
    pub fn slots_per_strat(&self) -> usize {
        self.slots_per_strat
    }

    #[inline]
    pub fn symbol_spec(&self, symbol_id: usize) -> Option<&OrsSymbolSpec> {
        self.symbols.get(symbol_id)
    }

    /// Acquires `LockedOrder` for `ors_order_id` if and only if its slot is currently `Live` for
    /// that exact `order_seqnum` (`human-docs/3.2` `lockOrder`).
    #[inline]
    pub fn lock_order(&self, ors_order_id: u64) -> Option<LockedOrder<'_>> {
        let (order_seqnum, strat_id) = split_ors_order_id(ors_order_id);
        if order_seqnum == 0 || usize::from(strat_id) >= MAX_ORS_STRATS {
            return None;
        }
        let slot_idx = self.regular_slot_idx(strat_id, order_seqnum);
        let slot = &self.slots[slot_idx];
        let expected = pack_slot_identity(SlotState::Live, order_seqnum);
        if slot.identity.load(Ordering::Acquire) != expected {
            return None;
        }
        let guard = slot.order.lock();
        if slot.identity.load(Ordering::Acquire) != expected || guard.ors_order_id != ors_order_id {
            return None;
        }
        Some(LockedOrder { slot, guard })
    }

    /// Acquires `LockedSymbolBook` while proving the caller already holds `LockedOrder`.
    #[inline]
    pub fn lock_symbol_book<'a>(
        &'a self,
        symbol_id: usize,
        held_order: &LockedOrder<'_>,
    ) -> Option<LockedSymbolBook<'a>> {
        self.stp_book
            .lock_symbol(symbol_id, held_order.lock_proof())
    }

    /// Checks whether `(slot_idx, ors_order_id)` is still `Live` without taking the order spinlock.
    #[inline]
    pub fn is_slot_live(&self, slot_idx: u32, ors_order_id: u64) -> bool {
        let Some(slot) = self.slots.get(slot_idx as usize) else {
            return false;
        };
        let (seq, _) = split_ors_order_id(ors_order_id);
        slot.identity.load(Ordering::Acquire) == pack_slot_identity(SlotState::Live, seq)
    }

    /// Admits and allocates a new order on the `REQ` hot path (`human-docs/3`, `3.2`, `3.6`):
    /// - Zero heap allocation, zero logging under spinlocks.
    /// - Lock-free `OrsStpBook::self_cross` check on the opposite side's atomic BBO word.
    /// - Refuses slot collision on `Claimed`/`Live` with `RefusalReason::ALLOCATION_ERROR` without
    ///   resizing or evicting the active occupant.
    /// - Supports `dry_run` via each strategy's dedicated scratch slot (inspected **only** at slot
    ///   selection and at transport-send skip!).
    pub fn place_order(
        &self,
        req: &OrsNewOrderRequest,
    ) -> Result<OrsPlaceOutcome, OrsOrderRefusal> {
        let fail = |account_ref: u8, reason: v::RefusalReason| OrsOrderRefusal {
            strat_id: req.strat_id,
            account_ref,
            order_seqnum: req.order_seqnum,
            reason,
        };

        let resolved_account = self
            .dop_state
            .admit_new_order_request(
                req.strat_id,
                req.msg_seqnum,
                req.order_seqnum,
                req.account_ref,
            )
            .map_err(|r| fail(req.account_ref, r))?;

        let spec = self
            .symbols
            .get(req.symbol_id as usize)
            .ok_or_else(|| fail(resolved_account, v::RefusalReason::InvalidQuantity))?;
        if req.price_ticks <= 0 {
            return Err(fail(resolved_account, v::RefusalReason::InvalidPrice));
        }
        if req.qty_lots <= 0 {
            return Err(fail(resolved_account, v::RefusalReason::InvalidQuantity));
        }

        // Lock-free send-path STP check (`human-docs/2`, `3.6`).
        let use_stp = CHECK_STP && req.check_stp;
        if use_stp
            && self
                .stp_book
                .self_cross(req.symbol_id as usize, req.side, req.price_ticks)
        {
            return Err(fail(
                resolved_account,
                v::RefusalReason::MKT_CANCELED_SELFTRADE,
            ));
        }

        let (hold_key, hold_atoms) = spec
            .compute_hold_atoms(req.side, req.price_ticks, req.qty_lots, req.reduce_only)
            .map_err(|r| fail(resolved_account, r))?;
        let (_, hold_scale) = spec.hold_asset_for_side(req.side);

        let domain_cfg = self.account_domains[resolved_account as usize]
            .ok_or_else(|| fail(resolved_account, v::RefusalReason::INVALID_ACCOUNT_ID))?;
        let cid_model = v::SelfDescribingClientId::from_ors_order(
            req.strat_id,
            0,
            domain_cfg.epoch,
            req.order_seqnum,
        )
        .map_err(|_| fail(resolved_account, v::RefusalReason::INVALID_STRAT_ID))?;
        let client_id =
            encode_tw1_client_id_inline(&cid_model, &domain_cfg.client_id_domain(spec.product))
                .map_err(|r| fail(resolved_account, r))?;

        // `dry_run` read #1: select dedicated scratch slot vs regular power-of-two ring slot.
        let slot_idx = if req.dry_run {
            self.scratch_slot_idx(req.strat_id)
        } else {
            self.regular_slot_idx(req.strat_id, req.order_seqnum)
        };
        let slot = &self.slots[slot_idx];
        self.claim_slot(slot, req.order_seqnum)
            .map_err(|r| fail(resolved_account, r))?;

        let guard = slot.order.lock();
        let mut locked = LockedOrder { slot, guard };

        if let Err(reason) =
            self.wallet_ledger
                .reserve_hold(resolved_account, hold_key, hold_scale, hold_atoms)
        {
            locked.retire_in_place(req.order_seqnum);
            return Err(fail(resolved_account, reason));
        }

        let ors_order_id = make_ors_order_id(req.order_seqnum, req.strat_id);
        *locked.order_mut() = OrsOrder {
            ors_order_id,
            slot_idx: slot_idx as u32,
            strat_id: req.strat_id,
            account_ref: resolved_account,
            symbol_id: req.symbol_id,
            order_seqnum: req.order_seqnum,
            side: req.side,
            tif: req.tif,
            post_only: req.post_only,
            reduce_only: req.reduce_only,
            check_stp: use_stp,
            leg: 0,
            status: OrsOrderStatus::PendingNew,
            front_ack_emitted: false,
            price_ticks: req.price_ticks,
            original_qty_lots: req.qty_lots,
            effective_qty_lots: req.qty_lots,
            filled_qty_lots: 0,
            projected_fill_qty_lots: 0,
            hold_asset_key: hold_key,
            hold_scale,
            held_atoms: hold_atoms,
            pending_cxl: 0,
            cancel_on_ack: false,
            admin_cancel_requested: false,
            query_fail_count: 0,
            modify_pending: false,
            pending_modify_price_ticks: 0,
            pending_modify_qty_lots: 0,
            pending_modify_extra_hold_atoms: 0,
            created_mono_ns: req.now_mono_ns,
            client_id,
            venue_order_id: InlineAscii::empty(),
            notes: InlineAscii::empty(),
        };

        if use_stp
            && let Some(mut book) = self.lock_symbol_book(req.symbol_id as usize, &locked)
            && let Err(reason) =
                book.insert_order(ors_order_id, slot_idx as u32, req.side, req.price_ticks)
        {
            drop(book);
            self.wallet_ledger
                .release_hold(resolved_account, hold_key, hold_atoms);
            locked.order_mut().held_atoms = 0;
            locked.retire_in_place(req.order_seqnum);
            return Err(fail(resolved_account, reason));
        }

        locked.publish_live(req.order_seqnum);

        // `dry_run` read #2: skip venue transport send and immediately release scratch slot.
        let should_send_to_venue = if req.dry_run {
            if use_stp
                && let Some(mut book) = self.lock_symbol_book(req.symbol_id as usize, &locked)
            {
                book.unlink_order(ors_order_id);
            }
            let held = locked.order().held_atoms;
            self.wallet_ledger
                .release_hold(resolved_account, hold_key, held);
            locked.order_mut().held_atoms = 0;
            locked.retire_in_place(req.order_seqnum);
            false
        } else {
            true
        };

        Ok(OrsPlaceOutcome {
            ors_order_id,
            slot_idx: slot_idx as u32,
            account_ref: resolved_account,
            product: spec.product,
            client_id,
            should_send_to_venue,
        })
    }

    /// Applies an optional gateway `FrontAck` (`VenueEvent::FrontAck`): emitted at most once while
    /// the order is still live.
    pub fn on_front_ack(
        &self,
        ors_order_id: u64,
        venue_order_id: Option<&str>,
        received_mono_ns: u64,
    ) -> Option<OrsReport> {
        let mut locked = self.lock_order(ors_order_id)?;
        let order = locked.order_mut();
        if order.front_ack_emitted || !order.is_open_or_pending() {
            return None;
        }
        order.front_ack_emitted = true;
        if let Some(vid) = venue_order_id
            && !vid.is_empty()
        {
            order.venue_order_id = InlineAscii::lossy_ascii(vid);
        }
        Some(OrsReport::FrontAck(OrsFrontAckReport {
            ors_order_id: order.ors_order_id,
            strat_id: order.strat_id,
            account_ref: order.account_ref,
            order_seqnum: order.order_seqnum,
            orig_seqnum: order.order_seqnum,
            client_id: order.client_id,
            venue_order_id: order.venue_order_id,
            received_mono_ns,
        }))
    }

    /// Applies order book acceptance (`Ack` / `3.7` venue modify on ack):
    /// - `One ack`: whichever arrives first (order socket reply or private stream) transitions
    ///   `PendingNew -> Open` and emits the single `OrsReport::Ack`.
    /// - A second ack on an already-`Open` order is a no-op (`report: None`) unless venue-reported
    ///   price or quantity moved (`3.7`), in which case it emits `OrsReport::Modified`!
    /// - If `cancel_on_ack` was armed (`cancel-before-ack`), returns
    ///   `dispatch_pending_cancel = true`.
    pub fn on_ack(
        &self,
        ors_order_id: u64,
        venue_order_id: Option<&str>,
        ack_price_ticks: Option<i64>,
        ack_qty_lots: Option<i64>,
        received_mono_ns: u64,
    ) -> Result<OrsAckOutcome, v::RefusalReason> {
        let Some(mut locked) = self.lock_order(ors_order_id) else {
            return Ok(OrsAckOutcome::empty());
        };
        let spec = *self
            .symbols
            .get(locked.order().symbol_id as usize)
            .ok_or(v::RefusalReason::InvalidQuantity)?;

        if let Some(vid) = venue_order_id
            && !vid.is_empty()
        {
            locked.order_mut().venue_order_id = InlineAscii::lossy_ascii(vid);
        }

        let target_px = ack_price_ticks.unwrap_or(locked.order().price_ticks);
        let target_qty = ack_qty_lots.unwrap_or(locked.order().effective_qty_lots);
        if target_px <= 0 || target_qty <= 0 {
            return Err(v::RefusalReason::InvalidQuantity);
        }

        match locked.order().status {
            OrsOrderStatus::PendingNew => {
                let venue_modified = target_px != locked.order().price_ticks
                    || target_qty != locked.order().effective_qty_lots;
                if venue_modified {
                    self.reconcile_order_terms_locked(&mut locked, &spec, target_px, target_qty)?;
                }
                locked.order_mut().status = OrsOrderStatus::Open;
                let dispatch_pending_cancel =
                    locked.order().cancel_on_ack && locked.order().pending_cxl > 0;
                locked.order_mut().cancel_on_ack = false;

                let o = *locked.order();
                if o.filled_qty_lots >= o.effective_qty_lots {
                    self.close_and_retire_locked(&mut locked);
                }
                Ok(OrsAckOutcome {
                    report: Some(OrsReport::Ack(OrsAckReport {
                        ors_order_id: o.ors_order_id,
                        strat_id: o.strat_id,
                        account_ref: o.account_ref,
                        symbol_id: o.symbol_id,
                        order_seqnum: o.order_seqnum,
                        price_ticks: o.price_ticks,
                        effective_qty_lots: o.effective_qty_lots,
                        venue_modified,
                        venue_order_id: o.venue_order_id,
                        received_mono_ns,
                    })),
                    dispatch_pending_cancel,
                })
            }
            OrsOrderStatus::Open => {
                let moved = target_px != locked.order().price_ticks
                    || target_qty != locked.order().effective_qty_lots;
                if !moved {
                    return Ok(OrsAckOutcome::empty());
                }
                self.reconcile_order_terms_locked(&mut locked, &spec, target_px, target_qty)?;
                let o = *locked.order();
                if o.filled_qty_lots >= o.effective_qty_lots {
                    self.close_and_retire_locked(&mut locked);
                }
                Ok(OrsAckOutcome {
                    report: Some(OrsReport::Modified(OrsModifiedReport {
                        ors_order_id: o.ors_order_id,
                        strat_id: o.strat_id,
                        account_ref: o.account_ref,
                        symbol_id: o.symbol_id,
                        order_seqnum: o.order_seqnum,
                        price_ticks: o.price_ticks,
                        effective_qty_lots: o.effective_qty_lots,
                        filled_qty_lots: o.filled_qty_lots,
                        received_mono_ns,
                    })),
                    dispatch_pending_cancel: false,
                })
            }
            OrsOrderStatus::Closed => Ok(OrsAckOutcome::empty()),
        }
    }

    /// Registers a strategy cancel request (`REQ`), supporting `CANCEL_ON_SAME_BEFORE_ACK` and
    /// `OrsQueryDesk` `Place`+`Cancel` `cancel_after` coalescing.
    pub fn request_cancel(
        &self,
        req: &OrsCancelRequest,
    ) -> Result<OrsCancelDispatch, OrsOrderRefusal> {
        let (order_seqnum, _) = split_ors_order_id(req.ors_order_id);
        let fail = |account_ref: u8, reason: v::RefusalReason| OrsOrderRefusal {
            strat_id: req.strat_id,
            account_ref,
            order_seqnum,
            reason,
        };

        let resolved_account = self
            .dop_state
            .admit_management_request(req.strat_id, req.msg_seqnum, req.account_ref)
            .map_err(|r| fail(req.account_ref, r))?;

        let Some(mut locked) = self.lock_order(req.ors_order_id) else {
            return Err(fail(resolved_account, v::RefusalReason::OrderNotOpen));
        };
        if locked.order().strat_id != req.strat_id || locked.order().account_ref != resolved_account
        {
            return Err(fail(
                resolved_account,
                v::RefusalReason::REJECT_ROUTING_ID_MISMATCH,
            ));
        }

        if locked.order().status == OrsOrderStatus::PendingNew && !self.allow_cancel_before_ack {
            return Err(fail(
                resolved_account,
                v::RefusalReason::TooManyPendingActions,
            ));
        }

        locked.order_mut().pending_cxl = locked.order().pending_cxl.saturating_add(1);
        let coalesced_in_query_desk = self
            .query_desk
            .mark_cancel_after(resolved_account, req.ors_order_id);

        let should_send_now = if coalesced_in_query_desk {
            false
        } else if locked.order().status == OrsOrderStatus::PendingNew {
            locked.order_mut().cancel_on_ack = true;
            false
        } else {
            true
        };

        let o = *locked.order();
        Ok(OrsCancelDispatch {
            ors_order_id: o.ors_order_id,
            account_ref: o.account_ref,
            symbol_id: o.symbol_id,
            client_id: o.client_id,
            venue_order_id: o.venue_order_id,
            should_send_now,
            coalesced_in_query_desk,
        })
    }

    /// Admits a strategy-initiated in-place modify (`human-docs/3.8`), staging dual old+new STP
    /// prices and reserving worst-case upward hold delta before venue dispatch.
    pub fn request_modify(
        &self,
        req: &OrsModifyRequest,
    ) -> Result<OrsModifyDispatch, OrsOrderRefusal> {
        let (order_seqnum, _) = split_ors_order_id(req.ors_order_id);
        let fail = |account_ref: u8, reason: v::RefusalReason| OrsOrderRefusal {
            strat_id: req.strat_id,
            account_ref,
            order_seqnum,
            reason,
        };

        let resolved_account = self
            .dop_state
            .admit_management_request(req.strat_id, req.msg_seqnum, req.account_ref)
            .map_err(|r| fail(req.account_ref, r))?;

        let Some(mut locked) = self.lock_order(req.ors_order_id) else {
            return Err(fail(resolved_account, v::RefusalReason::OrderNotOpen));
        };
        let o_snap = *locked.order();
        if o_snap.strat_id != req.strat_id || o_snap.account_ref != resolved_account {
            return Err(fail(
                resolved_account,
                v::RefusalReason::REJECT_ROUTING_ID_MISMATCH,
            ));
        }
        if o_snap.status != OrsOrderStatus::Open || o_snap.modify_pending {
            return Err(fail(
                resolved_account,
                v::RefusalReason::TooManyPendingActions,
            ));
        }
        if req.new_price_ticks <= 0 {
            return Err(fail(resolved_account, v::RefusalReason::InvalidPrice));
        }
        if req.new_qty_lots <= o_snap.filled_qty_lots {
            return Err(fail(resolved_account, v::RefusalReason::InvalidQuantity));
        }
        if req.new_price_ticks == o_snap.price_ticks
            && req.new_qty_lots == o_snap.effective_qty_lots
        {
            return Err(fail(resolved_account, v::RefusalReason::NoChange));
        }

        // Pure same-price quantity decrease is allowed in cancel-only mode; price moves or qty
        // increases require the strategy to be enabled.
        let is_pure_decrease = req.new_price_ticks == o_snap.price_ticks
            && req.new_qty_lots < o_snap.effective_qty_lots;
        if !is_pure_decrease
            && !self
                .dop_state
                .is_strat_enabled(resolved_account, req.strat_id)
        {
            return Err(fail(
                resolved_account,
                v::RefusalReason::RESTRICTED_CANCEL_ONLY,
            ));
        }

        let spec = self
            .symbols
            .get(o_snap.symbol_id as usize)
            .ok_or_else(|| fail(resolved_account, v::RefusalReason::InvalidQuantity))?;

        let target_unfilled = req.new_qty_lots - o_snap.filled_qty_lots;
        let (_, target_hold) = spec
            .compute_hold_atoms(
                o_snap.side,
                req.new_price_ticks,
                target_unfilled,
                o_snap.reduce_only,
            )
            .map_err(|r| fail(resolved_account, r))?;
        let extra_hold = target_hold.saturating_sub(o_snap.held_atoms);

        if CHECK_STP && o_snap.check_stp {
            if let Some(mut book) = self.lock_symbol_book(o_snap.symbol_id as usize, &locked) {
                if book.self_cross_excluding_order(
                    o_snap.ors_order_id,
                    o_snap.side,
                    req.new_price_ticks,
                ) {
                    return Err(fail(
                        resolved_account,
                        v::RefusalReason::MKT_CANCELED_SELFTRADE,
                    ));
                }
                if extra_hold > 0 {
                    self.wallet_ledger
                        .reserve_hold(
                            resolved_account,
                            o_snap.hold_asset_key,
                            o_snap.hold_scale,
                            extra_hold,
                        )
                        .map_err(|r| fail(resolved_account, r))?;
                }
                book.stage_pending_modify(o_snap.ors_order_id, req.new_price_ticks);
            }
        } else if extra_hold > 0 {
            self.wallet_ledger
                .reserve_hold(
                    resolved_account,
                    o_snap.hold_asset_key,
                    o_snap.hold_scale,
                    extra_hold,
                )
                .map_err(|r| fail(resolved_account, r))?;
        }

        let om = locked.order_mut();
        om.held_atoms += extra_hold;
        om.modify_pending = true;
        om.pending_modify_price_ticks = req.new_price_ticks;
        om.pending_modify_qty_lots = req.new_qty_lots;
        om.pending_modify_extra_hold_atoms = extra_hold;

        Ok(OrsModifyDispatch {
            ors_order_id: o_snap.ors_order_id,
            account_ref: o_snap.account_ref,
            symbol_id: o_snap.symbol_id,
            new_price_ticks: req.new_price_ticks,
            new_qty_lots: req.new_qty_lots,
            extra_hold_atoms: extra_hold,
        })
    }

    /// Applies confirmed strategy (`3.8`) or venue (`3.7`) modification on `RESP`.
    pub fn on_modify_ack(
        &self,
        ors_order_id: u64,
        confirmed_price_ticks: i64,
        confirmed_qty_lots: i64,
        venue_order_id: Option<&str>,
        received_mono_ns: u64,
    ) -> Result<Option<OrsReport>, v::RefusalReason> {
        let Some(mut locked) = self.lock_order(ors_order_id) else {
            return Ok(None);
        };
        if confirmed_price_ticks <= 0 || confirmed_qty_lots <= 0 {
            return Err(v::RefusalReason::InvalidQuantity);
        }
        let spec = *self
            .symbols
            .get(locked.order().symbol_id as usize)
            .ok_or(v::RefusalReason::InvalidQuantity)?;
        if let Some(vid) = venue_order_id
            && !vid.is_empty()
        {
            locked.order_mut().venue_order_id = InlineAscii::lossy_ascii(vid);
        }
        locked.order_mut().modify_pending = false;
        locked.order_mut().pending_modify_extra_hold_atoms = 0;

        self.reconcile_order_terms_locked(
            &mut locked,
            &spec,
            confirmed_price_ticks,
            confirmed_qty_lots,
        )?;
        let o = *locked.order();
        if o.filled_qty_lots >= o.effective_qty_lots {
            self.close_and_retire_locked(&mut locked);
        }
        Ok(Some(OrsReport::Modified(OrsModifiedReport {
            ors_order_id: o.ors_order_id,
            strat_id: o.strat_id,
            account_ref: o.account_ref,
            symbol_id: o.symbol_id,
            order_seqnum: o.order_seqnum,
            price_ticks: o.price_ticks,
            effective_qty_lots: o.effective_qty_lots,
            filled_qty_lots: o.filled_qty_lots,
            received_mono_ns,
        })))
    }

    /// Rolls back a rejected strategy modify (`3.8`), releasing any extra upward hold and
    /// restoring single-price STP registration.
    pub fn on_modify_rejected(
        &self,
        ors_order_id: u64,
        reason: v::TerminalReason,
        received_mono_ns: u64,
    ) -> Option<OrsReport> {
        let mut locked = self.lock_order(ors_order_id)?;
        if !locked.order().modify_pending {
            return None;
        }
        let o_snap = *locked.order();
        if o_snap.pending_modify_extra_hold_atoms > 0 {
            self.wallet_ledger.release_hold(
                o_snap.account_ref,
                o_snap.hold_asset_key,
                o_snap.pending_modify_extra_hold_atoms,
            );
            locked.order_mut().held_atoms = locked
                .order()
                .held_atoms
                .saturating_sub(o_snap.pending_modify_extra_hold_atoms);
        }
        locked.order_mut().modify_pending = false;
        locked.order_mut().pending_modify_extra_hold_atoms = 0;

        if CHECK_STP
            && o_snap.check_stp
            && let Some(mut book) = self.lock_symbol_book(o_snap.symbol_id as usize, &locked)
        {
            book.rollback_pending_modify(o_snap.ors_order_id);
        }

        Some(OrsReport::ModifyRejected(OrsModifyRejectedReport {
            ors_order_id: o_snap.ors_order_id,
            strat_id: o_snap.strat_id,
            account_ref: o_snap.account_ref,
            symbol_id: o_snap.symbol_id,
            order_seqnum: o_snap.order_seqnum,
            reason,
            received_mono_ns,
        }))
    }

    /// Advances cumulative `ProjectedFill` watermark (`human-docs/3.5`):
    /// - Emits `OrsReport::Fill { projected: true, amendment: false, .. }` only when
    ///   `cum_qty_lots > max(filled_qty_lots, projected_fill_qty_lots)`.
    /// - Protects terminal cancel from double-deducting `cxl_qty = effective_qty - projected_qty`.
    pub fn on_cum_qty(
        &self,
        ors_order_id: u64,
        cum_qty_lots: i64,
        price_ticks: i64,
        received_mono_ns: u64,
    ) -> Result<Option<OrsReport>, v::RefusalReason> {
        if price_ticks <= 0 {
            return Err(v::RefusalReason::InvalidPrice);
        }
        let Some(mut locked) = self.lock_order(ors_order_id) else {
            return Ok(None);
        };
        let baseline = locked
            .order()
            .filled_qty_lots
            .max(locked.order().projected_fill_qty_lots);
        let capped_cum = cum_qty_lots.min(locked.order().effective_qty_lots);
        if capped_cum <= baseline {
            return Ok(None);
        }
        let delta_lots = capped_cum - baseline;
        locked.order_mut().projected_fill_qty_lots = capped_cum;
        let o = *locked.order();
        Ok(Some(OrsReport::Fill(OrsFillReport {
            ors_order_id: o.ors_order_id,
            strat_id: o.strat_id,
            account_ref: o.account_ref,
            symbol_id: o.symbol_id,
            order_seqnum: o.order_seqnum,
            exec_ref: 0,
            trade_id: InlineAscii::empty(),
            side: o.side,
            liquidity: v::Liquidity::Unknown,
            last_qty_lots: delta_lots,
            last_price_ticks: price_ticks,
            cumulative_filled_lots: o.filled_qty_lots,
            cumulative_projected_lots: o.projected_fill_qty_lots,
            fee: None,
            amendment: false,
            projected: true,
            order_retired: false,
            received_mono_ns,
        })))
    }

    /// Applies an execution fill or fee/rebate patch (`human-docs/3.4`, `3.9`):
    /// - Deduplicates identical executions arriving on both order reply and private stream.
    /// - Emits `amendment = true` when `TRADE_LITE` is followed by a full trade/fee patch or
    ///   post-trade rebate revision, updating wallet fee delta ONLY without double-applying
    ///   fill quantity, holds, or positions (even after the order slot has already retired!).
    /// - Immediately transitions full fills (`filled_qty_lots >= effective_qty_lots`) to
    ///   `SlotState::Retired` and unlinks from `OrsStpBook` in the **same call**!
    pub fn on_fill(&self, obs: &OrsFillObservation) -> Result<Option<OrsReport>, v::RefusalReason> {
        let exec_ref = if obs.exec_ref != 0 {
            obs.exec_ref
        } else {
            trade_id_to_exec_ref(obs.trade_id.as_str())
        };
        let (order_seqnum, strat_id) = split_ors_order_id(obs.ors_order_id);
        if order_seqnum == 0 || usize::from(strat_id) >= MAX_ORS_STRATS {
            return Err(v::RefusalReason::INVALID_STRAT_ID);
        }
        let spec = *self
            .symbols
            .get(obs.symbol_id as usize)
            .ok_or(v::RefusalReason::InvalidQuantity)?;

        let fill_key = FillKey {
            ors_order_id: obs.ors_order_id,
            exec_ref,
            side: obs.side,
            symbol_id: obs.symbol_id,
        };

        if let Some(mut locked) = self.lock_order(obs.ors_order_id) {
            let outcome = self.fill_records.record_or_amend(FillObservationInput {
                key: fill_key,
                qty_lots: obs.qty_lots,
                price_ticks: obs.price_ticks,
                fee: obs.fee,
                now_mono_ns: obs.received_mono_ns,
            })?;

            match outcome {
                FillRecordOutcome::Duplicate => Ok(None),
                FillRecordOutcome::Amended {
                    qty_lots,
                    price_ticks,
                    previous_fee,
                    new_fee,
                    reported_fee_delta,
                } => {
                    let o = *locked.order();
                    self.wallet_ledger
                        .apply_fee_amendment(o.account_ref, previous_fee, new_fee)?;
                    Ok(Some(OrsReport::Fill(OrsFillReport {
                        ors_order_id: o.ors_order_id,
                        strat_id: o.strat_id,
                        account_ref: o.account_ref,
                        symbol_id: o.symbol_id,
                        order_seqnum: o.order_seqnum,
                        exec_ref,
                        trade_id: obs.trade_id,
                        side: o.side,
                        liquidity: obs.liquidity,
                        last_qty_lots: qty_lots,
                        last_price_ticks: price_ticks,
                        cumulative_filled_lots: o.filled_qty_lots,
                        cumulative_projected_lots: o.projected_fill_qty_lots,
                        fee: Some(reported_fee_delta),
                        amendment: true,
                        projected: false,
                        order_retired: false,
                        received_mono_ns: obs.received_mono_ns,
                    })))
                }
                FillRecordOutcome::Applied { fee } => {
                    let new_filled = locked
                        .order()
                        .filled_qty_lots
                        .checked_add(obs.qty_lots)
                        .ok_or(v::RefusalReason::InvalidQuantity)?;
                    locked.order_mut().filled_qty_lots = new_filled;
                    if new_filled > locked.order().projected_fill_qty_lots {
                        locked.order_mut().projected_fill_qty_lots = new_filled;
                    }

                    let remaining_required = self.required_remaining_hold_locked(&locked, &spec)?;
                    let release_atoms =
                        locked.order().held_atoms.saturating_sub(remaining_required);
                    locked.order_mut().held_atoms -= release_atoms;

                    let o = *locked.order();
                    self.wallet_ledger.settle_applied_fill(
                        o.account_ref,
                        &spec,
                        o.side,
                        obs.qty_lots,
                        obs.price_ticks,
                        o.hold_asset_key,
                        release_atoms,
                        fee,
                    )?;

                    let fully_filled = o.filled_qty_lots >= o.effective_qty_lots;
                    if fully_filled {
                        self.close_and_retire_locked(&mut locked);
                        self.query_desk
                            .complete_order(o.account_ref, o.ors_order_id);
                    }

                    Ok(Some(OrsReport::Fill(OrsFillReport {
                        ors_order_id: o.ors_order_id,
                        strat_id: o.strat_id,
                        account_ref: o.account_ref,
                        symbol_id: o.symbol_id,
                        order_seqnum: o.order_seqnum,
                        exec_ref,
                        trade_id: obs.trade_id,
                        side: o.side,
                        liquidity: obs.liquidity,
                        last_qty_lots: obs.qty_lots,
                        last_price_ticks: obs.price_ticks,
                        cumulative_filled_lots: o.filled_qty_lots,
                        cumulative_projected_lots: o.projected_fill_qty_lots,
                        fee,
                        amendment: false,
                        projected: false,
                        order_retired: fully_filled,
                        received_mono_ns: obs.received_mono_ns,
                    })))
                }
            }
        } else {
            // Order slot is already Retired or reused: `FillRecordPool` still handles duplicate
            // suppression, late `TRADE_LITE` fee amendments, and late post-cancel fills!
            if obs.account_ref == 0 || !self.dop_state.is_account_registered(obs.account_ref) {
                return Err(v::RefusalReason::INVALID_ACCOUNT_ID);
            }
            let outcome = self.fill_records.record_or_amend(FillObservationInput {
                key: fill_key,
                qty_lots: obs.qty_lots,
                price_ticks: obs.price_ticks,
                fee: obs.fee,
                now_mono_ns: obs.received_mono_ns,
            })?;

            match outcome {
                FillRecordOutcome::Duplicate => Ok(None),
                FillRecordOutcome::Amended {
                    qty_lots,
                    price_ticks,
                    previous_fee,
                    new_fee,
                    reported_fee_delta,
                } => {
                    self.wallet_ledger.apply_fee_amendment(
                        obs.account_ref,
                        previous_fee,
                        new_fee,
                    )?;
                    Ok(Some(OrsReport::Fill(OrsFillReport {
                        ors_order_id: obs.ors_order_id,
                        strat_id,
                        account_ref: obs.account_ref,
                        symbol_id: obs.symbol_id,
                        order_seqnum,
                        exec_ref,
                        trade_id: obs.trade_id,
                        side: obs.side,
                        liquidity: obs.liquidity,
                        last_qty_lots: qty_lots,
                        last_price_ticks: price_ticks,
                        cumulative_filled_lots: qty_lots,
                        cumulative_projected_lots: qty_lots,
                        fee: Some(reported_fee_delta),
                        amendment: true,
                        projected: false,
                        order_retired: true,
                        received_mono_ns: obs.received_mono_ns,
                    })))
                }
                FillRecordOutcome::Applied { fee } => {
                    let (hold_key, _) = spec.hold_asset_for_side(obs.side);
                    self.wallet_ledger.settle_applied_fill(
                        obs.account_ref,
                        &spec,
                        obs.side,
                        obs.qty_lots,
                        obs.price_ticks,
                        hold_key,
                        0,
                        fee,
                    )?;
                    Ok(Some(OrsReport::Fill(OrsFillReport {
                        ors_order_id: obs.ors_order_id,
                        strat_id,
                        account_ref: obs.account_ref,
                        symbol_id: obs.symbol_id,
                        order_seqnum,
                        exec_ref,
                        trade_id: obs.trade_id,
                        side: obs.side,
                        liquidity: obs.liquidity,
                        last_qty_lots: obs.qty_lots,
                        last_price_ticks: obs.price_ticks,
                        cumulative_filled_lots: obs.qty_lots,
                        cumulative_projected_lots: obs.qty_lots,
                        fee,
                        amendment: false,
                        projected: false,
                        order_retired: true,
                        received_mono_ns: obs.received_mono_ns,
                    })))
                }
            }
        }
    }

    /// Applies terminal order cancellation (`human-docs/3`, `3.5`):
    /// - Computes `canceled_qty_lots = effective_qty_lots - projected_fill_qty_lots` so prior
    ///   `ProjectedFill` watermarks are never double-deducted.
    /// - Distinguishes strategy-initiated cancel (`NO_REASON` when `pending_cxl > 0`, or
    ///   `ADMIN_ACTION` when triggered by `set_strat_enabled(..., false)`) from unsolicited venue
    ///   cancels (`MKT_CANCELED`, `MKT_IOC_CANCELED`, `MKT_REJECT_POST_ONLY`,
    ///   `MKT_REDUCE_ONLY_CANCELED`, `MKT_CANCELED_SELFTRADE`).
    /// - Releases remaining hold, unlinks from `OrsStpBook`, and transitions slot to
    ///   `SlotState::Retired` in the **same call**.
    pub fn on_canceled(
        &self,
        obs: &OrsCancelObservation<'_>,
    ) -> Result<Option<OrsReport>, v::RefusalReason> {
        let Some(mut locked) = self.lock_order(obs.ors_order_id) else {
            return Ok(None);
        };
        if let Some(cum) = obs.cum_qty_lots {
            let capped = cum.min(locked.order().effective_qty_lots);
            if capped > locked.order().projected_fill_qty_lots {
                locked.order_mut().projected_fill_qty_lots = capped;
            }
        }
        let o = *locked.order();
        let canceled_qty_lots = o
            .effective_qty_lots
            .saturating_sub(o.projected_fill_qty_lots);

        let pending_strategy_cancel = o.pending_cxl > 0;
        let reason = match obs.explicit_reason {
            Some(v::TerminalReason::UserCanceled) => {
                if o.admin_cancel_requested {
                    v::TerminalReason::ADMIN_ACTION
                } else if pending_strategy_cancel {
                    v::TerminalReason::NO_REASON
                } else {
                    v::TerminalReason::MKT_CANCELED
                }
            }
            Some(explicit) => explicit,
            None => {
                let classified = v::classify_ors_cancel_reason(
                    obs.error_code,
                    obs.status_or_msg,
                    obs.stp_mode_or_reason,
                    Some(o.tif),
                    o.post_only,
                    pending_strategy_cancel,
                );
                if classified == v::TerminalReason::NO_REASON && o.admin_cancel_requested {
                    v::TerminalReason::ADMIN_ACTION
                } else {
                    classified
                }
            }
        };

        self.close_and_retire_locked(&mut locked);
        self.query_desk
            .complete_order(o.account_ref, o.ors_order_id);

        Ok(Some(OrsReport::Canceled(OrsCanceledReport {
            ors_order_id: o.ors_order_id,
            strat_id: o.strat_id,
            account_ref: o.account_ref,
            symbol_id: o.symbol_id,
            order_seqnum: o.order_seqnum,
            canceled_qty_lots,
            cumulative_filled_lots: o.filled_qty_lots,
            reason,
            received_mono_ns: obs.received_mono_ns,
        })))
    }

    /// Handles a rejected `Cancel` request (`CancelNAck`), decrementing `pending_cxl` and
    /// scheduling `OrsQueryDesk` reconciliation on ambiguous errors (`-1007`, `-1000`,
    /// `MKT_DELAYED`) or backoff when appropriate.
    pub fn on_cancel_rejected(
        &self,
        ors_order_id: u64,
        error_code: Option<i64>,
        reason: v::TerminalReason,
        received_mono_ns: u64,
    ) -> Option<OrsReport> {
        let mut locked = self.lock_order(ors_order_id)?;
        locked.order_mut().pending_cxl = locked.order().pending_cxl.saturating_sub(1);
        let o = *locked.order();
        drop(locked);

        let ambiguous = is_ambiguous_venue_outcome(error_code, Some(reason));
        let mut scheduled = false;
        if ambiguous {
            scheduled = self
                .query_desk
                .schedule_status_ask(
                    o.account_ref,
                    o.ors_order_id,
                    o.slot_idx,
                    o.strat_id,
                    o.symbol_id,
                    o.order_seqnum,
                    OrsQueryOrigin::Cancel,
                    received_mono_ns,
                    |slot_idx, oid| self.is_slot_live(slot_idx, oid),
                )
                .is_ok();
        }

        Some(OrsReport::CancelRejected(OrsCancelRejectedReport {
            ors_order_id: o.ors_order_id,
            strat_id: o.strat_id,
            account_ref: o.account_ref,
            symbol_id: o.symbol_id,
            order_seqnum: o.order_seqnum,
            reason,
            scheduled_status_query: scheduled,
            received_mono_ns,
        }))
    }

    /// Applies venue rejection (`NAck`):
    /// - On ambiguous send outcomes (`-1007`, `-1000`, `MKT_DELAYED`), keeps the slot `Live` and
    ///   schedules `OrsQueryDesk::schedule_status_ask` (`OrsQueryOrigin::Place`) instead of
    ///   prematurely releasing holds!
    /// - On definitive rejects, releases hold, unlinks from `OrsStpBook`, and transitions slot to
    ///   `SlotState::Retired` in the **same call**.
    pub fn on_rejected(
        &self,
        ors_order_id: u64,
        error_code: Option<i64>,
        status_or_msg: &str,
        explicit_reason: Option<v::TerminalReason>,
        received_mono_ns: u64,
    ) -> Option<OrsReport> {
        let mut locked = self.lock_order(ors_order_id)?;
        let o = *locked.order();
        let reason = explicit_reason.unwrap_or_else(|| {
            v::classify_ors_cancel_reason(
                error_code,
                status_or_msg,
                None,
                Some(o.tif),
                o.post_only,
                false,
            )
        });

        if is_ambiguous_venue_outcome(error_code, Some(reason)) {
            locked.order_mut().notes.append_note(status_or_msg);
            drop(locked);
            let _ = self.query_desk.schedule_status_ask(
                o.account_ref,
                o.ors_order_id,
                o.slot_idx,
                o.strat_id,
                o.symbol_id,
                o.order_seqnum,
                OrsQueryOrigin::Place,
                received_mono_ns,
                |slot_idx, oid| self.is_slot_live(slot_idx, oid),
            );
            return None;
        }

        self.close_and_retire_locked(&mut locked);
        self.query_desk
            .complete_order(o.account_ref, o.ors_order_id);

        Some(OrsReport::Rejected(OrsRejectedReport {
            ors_order_id: o.ors_order_id,
            strat_id: o.strat_id,
            account_ref: o.account_ref,
            symbol_id: o.symbol_id,
            order_seqnum: o.order_seqnum,
            reason,
            received_mono_ns,
        }))
    }

    /// Dispatches any [`OrsVenueEvent`] variant through the corresponding hot-path handler.
    pub fn apply_venue_event(
        &self,
        event: OrsVenueEvent<'_>,
    ) -> Result<Option<OrsReport>, v::RefusalReason> {
        match event {
            OrsVenueEvent::FrontAck {
                ors_order_id,
                venue_order_id,
                received_mono_ns,
            } => Ok(self.on_front_ack(ors_order_id, venue_order_id, received_mono_ns)),
            OrsVenueEvent::Ack {
                ors_order_id,
                venue_order_id,
                ack_price_ticks,
                ack_qty_lots,
                received_mono_ns,
            } => Ok(self
                .on_ack(
                    ors_order_id,
                    venue_order_id,
                    ack_price_ticks,
                    ack_qty_lots,
                    received_mono_ns,
                )?
                .report),
            OrsVenueEvent::ProjectedFill {
                ors_order_id,
                cum_qty_lots,
                price_ticks,
                received_mono_ns,
            } => self.on_cum_qty(ors_order_id, cum_qty_lots, price_ticks, received_mono_ns),
            OrsVenueEvent::Fill(obs) => self.on_fill(&obs),
            OrsVenueEvent::Modified {
                ors_order_id,
                price_ticks,
                qty_lots,
                venue_order_id,
                received_mono_ns,
            } => self.on_modify_ack(
                ors_order_id,
                price_ticks,
                qty_lots,
                venue_order_id,
                received_mono_ns,
            ),
            OrsVenueEvent::ModifyRejected {
                ors_order_id,
                reason,
                received_mono_ns,
            } => Ok(self.on_modify_rejected(ors_order_id, reason, received_mono_ns)),
            OrsVenueEvent::Canceled(obs) => self.on_canceled(&obs),
            OrsVenueEvent::CancelRejected {
                ors_order_id,
                error_code,
                reason,
                received_mono_ns,
            } => Ok(self.on_cancel_rejected(ors_order_id, error_code, reason, received_mono_ns)),
            OrsVenueEvent::Rejected {
                ors_order_id,
                error_code,
                status_or_msg,
                explicit_reason,
                received_mono_ns,
            } => Ok(self.on_rejected(
                ors_order_id,
                error_code,
                status_or_msg,
                explicit_reason,
                received_mono_ns,
            )),
        }
    }

    /// Toggles `(account_ref, strat_id)` enablement (`human-docs/11`). When `enabled == false`,
    /// performs a one-order-at-a-time lock-and-release scan in ascending slot order, marking each
    /// matching open order for admin auto-cancel (`ADMIN_ACTION`) and writing them into `out`.
    pub fn set_strat_enabled(
        &self,
        account_ref: u8,
        strat_id: u8,
        enabled: bool,
        out: &mut [OrsAutoCancelTarget],
    ) -> Result<usize, v::RefusalReason> {
        self.dop_state
            .set_strat_enabled(account_ref, strat_id, enabled)?;
        if enabled || out.is_empty() {
            return Ok(0);
        }

        let strat_range = if strat_id == v::ALL_ORS_STRATS {
            0..MAX_ORS_STRATS
        } else {
            usize::from(strat_id)..(usize::from(strat_id) + 1)
        };

        let mut count = 0usize;
        for sid in strat_range {
            let base = sid * self.slots_per_strat;
            for offset in 0..self.slots_per_strat {
                if count >= out.len() {
                    return Ok(count);
                }
                let slot = &self.slots[base + offset];
                let (state, seq) = unpack_slot_identity(slot.identity.load(Ordering::Acquire));
                if state != SlotState::Live || seq == 0 {
                    continue;
                }
                let ors_order_id = make_ors_order_id(seq, sid as u8);
                let Some(mut locked) = self.lock_order(ors_order_id) else {
                    continue;
                };
                if account_ref != 0 && locked.order().account_ref != account_ref {
                    continue;
                }
                locked.order_mut().pending_cxl = locked.order().pending_cxl.saturating_add(1);
                locked.order_mut().admin_cancel_requested = true;
                if locked.order().status == OrsOrderStatus::PendingNew {
                    locked.order_mut().cancel_on_ack = true;
                }
                let o = *locked.order();
                out[count] = OrsAutoCancelTarget {
                    ors_order_id: o.ors_order_id,
                    strat_id: o.strat_id,
                    account_ref: o.account_ref,
                    symbol_id: o.symbol_id,
                    client_id: o.client_id,
                    venue_order_id: o.venue_order_id,
                    reason: v::TerminalReason::ADMIN_ACTION,
                };
                count += 1;
            }
        }
        Ok(count)
    }

    /// Cold control-plane convenience wrapper around [`Self::set_strat_enabled`] returning all
    /// matching open orders to auto-cancel.
    pub fn set_strat_enabled_collect(
        &self,
        account_ref: u8,
        strat_id: u8,
        enabled: bool,
    ) -> Result<Vec<OrsAutoCancelTarget>, v::RefusalReason> {
        self.dop_state
            .set_strat_enabled(account_ref, strat_id, enabled)?;
        if enabled {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        let strat_range = if strat_id == v::ALL_ORS_STRATS {
            0..MAX_ORS_STRATS
        } else {
            usize::from(strat_id)..(usize::from(strat_id) + 1)
        };
        for sid in strat_range {
            let base = sid * self.slots_per_strat;
            for offset in 0..self.slots_per_strat {
                let slot = &self.slots[base + offset];
                let (state, seq) = unpack_slot_identity(slot.identity.load(Ordering::Acquire));
                if state != SlotState::Live || seq == 0 {
                    continue;
                }
                let ors_order_id = make_ors_order_id(seq, sid as u8);
                let Some(mut locked) = self.lock_order(ors_order_id) else {
                    continue;
                };
                if account_ref != 0 && locked.order().account_ref != account_ref {
                    continue;
                }
                locked.order_mut().pending_cxl = locked.order().pending_cxl.saturating_add(1);
                locked.order_mut().admin_cancel_requested = true;
                if locked.order().status == OrsOrderStatus::PendingNew {
                    locked.order_mut().cancel_on_ack = true;
                }
                let o = *locked.order();
                out.push(OrsAutoCancelTarget {
                    ors_order_id: o.ors_order_id,
                    strat_id: o.strat_id,
                    account_ref: o.account_ref,
                    symbol_id: o.symbol_id,
                    client_id: o.client_id,
                    venue_order_id: o.venue_order_id,
                    reason: v::TerminalReason::ADMIN_ACTION,
                });
            }
        }
        Ok(out)
    }

    /// Scans `Live` orders into caller-supplied `out_ids` without heap allocation
    /// (`human-docs/3.2` `findOrders`):
    /// - Unfiltered or `strat_id`-only scans read `slot.identity` atomically with **zero locks**.
    /// - Scans filtering on `account_ref` or `symbol_id` lock and release **one order at a time**.
    pub fn find_orders(&self, filter: &OrsOrderFilter, out_ids: &mut [u64]) -> usize {
        if out_ids.is_empty() {
            return 0;
        }
        if let Some(sid) = filter.strat_id
            && usize::from(sid) >= MAX_ORS_STRATS
        {
            return 0;
        }
        let strat_range = match filter.strat_id {
            Some(sid) => usize::from(sid)..(usize::from(sid) + 1),
            None => 0..MAX_ORS_STRATS,
        };
        let needs_order_lock = filter.account_ref.is_some() || filter.symbol_id.is_some();
        let mut count = 0usize;
        for sid in strat_range {
            let base = sid * self.slots_per_strat;
            for offset in 0..self.slots_per_strat {
                if count >= out_ids.len() {
                    return count;
                }
                let slot = &self.slots[base + offset];
                let (state, seq) = unpack_slot_identity(slot.identity.load(Ordering::Acquire));
                if state != SlotState::Live || seq == 0 {
                    continue;
                }
                let ors_order_id = make_ors_order_id(seq, sid as u8);
                if !needs_order_lock {
                    out_ids[count] = ors_order_id;
                    count += 1;
                    continue;
                }
                let Some(locked) = self.lock_order(ors_order_id) else {
                    continue;
                };
                if let Some(acc) = filter.account_ref
                    && locked.order().account_ref != acc
                {
                    continue;
                }
                if let Some(sym) = filter.symbol_id
                    && locked.order().symbol_id != sym
                {
                    continue;
                }
                out_ids[count] = ors_order_id;
                count += 1;
            }
        }
        count
    }

    /// Schedules `StatusAsk` queries on `OrsQueryDesk` for all in-flight orders on `account_ref`
    /// after a venue order-entry socket disconnect (`human-docs/3.11`).
    pub fn on_socket_drop(&self, account_ref: u8, now_mono_ns: u64) -> usize {
        let mut scheduled = 0usize;
        for sid in 0..MAX_ORS_STRATS {
            let base = sid * self.slots_per_strat;
            for offset in 0..self.slots_per_strat {
                let slot = &self.slots[base + offset];
                let (state, seq) = unpack_slot_identity(slot.identity.load(Ordering::Acquire));
                if state != SlotState::Live || seq == 0 {
                    continue;
                }
                let ors_order_id = make_ors_order_id(seq, sid as u8);
                let Some(locked) = self.lock_order(ors_order_id) else {
                    continue;
                };
                let o = *locked.order();
                drop(locked);
                if o.account_ref != account_ref {
                    continue;
                }
                if (o.status == OrsOrderStatus::PendingNew || o.modify_pending || o.pending_cxl > 0)
                    && self
                        .query_desk
                        .schedule_status_ask(
                            o.account_ref,
                            o.ors_order_id,
                            o.slot_idx,
                            o.strat_id,
                            o.symbol_id,
                            o.order_seqnum,
                            OrsQueryOrigin::SocketDrop,
                            now_mono_ns,
                            |s_idx, oid| self.is_slot_live(s_idx, oid),
                        )
                        .is_ok()
                {
                    scheduled += 1;
                }
            }
        }
        scheduled
    }

    /// Records an inconclusive `StatusAsk` response (`human-docs/3`, `3.11`). When
    /// `max_query_count == Some(limit)` (`limit > 0`) is reached, releases the wedged slot with
    /// `TerminalReason::INTERNAL_CANCEL`.
    pub fn on_query_inconclusive(
        &self,
        ors_order_id: u64,
        received_mono_ns: u64,
    ) -> Option<OrsReport> {
        let mut locked = self.lock_order(ors_order_id)?;
        locked.order_mut().query_fail_count = locked.order().query_fail_count.saturating_add(1);
        let limit = self.max_query_count.filter(|&n| n > 0)?;
        if locked.order().query_fail_count < limit {
            return None;
        }
        let o = *locked.order();
        let canceled_qty_lots = o
            .effective_qty_lots
            .saturating_sub(o.projected_fill_qty_lots);
        self.close_and_retire_locked(&mut locked);
        self.query_desk
            .complete_order(o.account_ref, o.ors_order_id);
        Some(OrsReport::Canceled(OrsCanceledReport {
            ors_order_id: o.ors_order_id,
            strat_id: o.strat_id,
            account_ref: o.account_ref,
            symbol_id: o.symbol_id,
            order_seqnum: o.order_seqnum,
            canceled_qty_lots,
            cumulative_filled_lots: o.filled_qty_lots,
            reason: v::TerminalReason::INTERNAL_CANCEL,
            received_mono_ns,
        }))
    }

    /// Reconciles an Exchange REST `StatusAsk` snapshot (`human-docs/3.11`):
    /// 1. Applies all reported `trades` via [`Self::on_fill`] **strictly before** settling any
    ///    remaining canceled or expired quantity!
    /// 2. Catches up missing cumulative fill quantity if no granular trade rows were included.
    /// 3. Emits missing `Ack`, retires terminal orders, or returns
    ///    [`OrsQueryReconcileAction::DispatchCancel`] when `cancel_after` was coalesced!
    pub fn reconcile_query_snapshot(
        &self,
        snapshot: &OrsOrderQuerySnapshot<'_>,
        out_reports: &mut Vec<OrsReport>,
    ) -> Result<OrsQueryReconcileAction, v::RefusalReason> {
        // 1. Apply granular trades FIRST.
        for trade in snapshot.trades {
            if let Some(rep) = self.on_fill(trade)? {
                out_reports.push(rep);
            }
        }

        // 2. If cumulative fill in snapshot still exceeds order's filled_qty_lots, synthesize the
        //    catch-up fill before any cancel settlement.
        if let Some(locked) = self.lock_order(snapshot.ors_order_id) {
            let o = *locked.order();
            drop(locked);
            if snapshot.cum_filled_lots > o.filled_qty_lots {
                let diff = snapshot.cum_filled_lots - o.filled_qty_lots;
                let px = snapshot.avg_fill_price_ticks.unwrap_or(o.price_ticks);
                let synth_exec_ref = (1u64 << 62) | (snapshot.cum_filled_lots as u64);
                let obs = OrsFillObservation {
                    ors_order_id: o.ors_order_id,
                    account_ref: o.account_ref,
                    symbol_id: o.symbol_id,
                    side: o.side,
                    liquidity: v::Liquidity::Unknown,
                    exec_ref: synth_exec_ref,
                    trade_id: InlineAscii::empty(),
                    qty_lots: diff,
                    price_ticks: px,
                    fee: None,
                    received_mono_ns: snapshot.received_mono_ns,
                };
                if let Some(rep) = self.on_fill(&obs)? {
                    out_reports.push(rep);
                }
            }
        }

        let desk_entry = self
            .query_desk
            .get_entry(snapshot.account_ref, snapshot.ors_order_id);

        match snapshot.status {
            OrsQueriedOrderStatus::Open | OrsQueriedOrderStatus::PartiallyFilled => {
                let ack = self.on_ack(
                    snapshot.ors_order_id,
                    snapshot.venue_order_id,
                    snapshot.price_ticks,
                    snapshot.orig_qty_lots,
                    snapshot.received_mono_ns,
                )?;
                if let Some(rep) = ack.report {
                    out_reports.push(rep);
                }
                let want_cancel = ack.dispatch_pending_cancel
                    || desk_entry.is_some_and(|e| e.cancel_after)
                    || self
                        .lock_order(snapshot.ors_order_id)
                        .is_some_and(|l| l.order().pending_cxl > 0);
                self.query_desk
                    .complete_order(snapshot.account_ref, snapshot.ors_order_id);
                if want_cancel && self.lock_order(snapshot.ors_order_id).is_some() {
                    Ok(OrsQueryReconcileAction::DispatchCancel {
                        ors_order_id: snapshot.ors_order_id,
                        account_ref: snapshot.account_ref,
                        symbol_id: snapshot.symbol_id,
                    })
                } else {
                    Ok(OrsQueryReconcileAction::Resolved)
                }
            }
            OrsQueriedOrderStatus::Filled => {
                self.query_desk
                    .complete_order(snapshot.account_ref, snapshot.ors_order_id);
                Ok(OrsQueryReconcileAction::Resolved)
            }
            OrsQueriedOrderStatus::Canceled | OrsQueriedOrderStatus::Expired => {
                let status_str = match snapshot.status {
                    OrsQueriedOrderStatus::Canceled => "CANCELED",
                    OrsQueriedOrderStatus::Expired => "EXPIRED",
                    _ => "CANCELED",
                };
                if let Some(rep) = self.on_canceled(&OrsCancelObservation {
                    ors_order_id: snapshot.ors_order_id,
                    cum_qty_lots: Some(snapshot.cum_filled_lots),
                    error_code: None,
                    status_or_msg: status_str,
                    stp_mode_or_reason: None,
                    explicit_reason: None,
                    received_mono_ns: snapshot.received_mono_ns,
                })? {
                    out_reports.push(rep);
                }
                Ok(OrsQueryReconcileAction::Resolved)
            }
            OrsQueriedOrderStatus::NotFound => {
                if let Some(locked) = self.lock_order(snapshot.ors_order_id) {
                    let was_open_or_filled = locked.order().status == OrsOrderStatus::Open
                        || locked.order().filled_qty_lots > 0;
                    drop(locked);
                    if was_open_or_filled {
                        if let Some(rep) = self.on_canceled(&OrsCancelObservation {
                            ors_order_id: snapshot.ors_order_id,
                            cum_qty_lots: Some(snapshot.cum_filled_lots),
                            error_code: None,
                            status_or_msg: "CANCELED",
                            stp_mode_or_reason: None,
                            explicit_reason: None,
                            received_mono_ns: snapshot.received_mono_ns,
                        })? {
                            out_reports.push(rep);
                        }
                    } else if let Some(rep) = self.on_rejected(
                        snapshot.ors_order_id,
                        Some(-2013),
                        "NO_SUCH_ORDER",
                        Some(v::TerminalReason::OrderNotOpen),
                        snapshot.received_mono_ns,
                    ) {
                        out_reports.push(rep);
                    }
                } else {
                    self.query_desk
                        .complete_order(snapshot.account_ref, snapshot.ors_order_id);
                }
                Ok(OrsQueryReconcileAction::Resolved)
            }
        }
    }

    /// Stateless cold bootstrap from authenticated Exchange REST snapshots (`Account + OpenOrders`)
    /// paired with `SelfDescribingClientId` (`tw1...`):
    /// - Seeds `UtaWalletLedger` asset balances and derivative positions.
    /// - Reconstructs open-order holds, `OrsStpBook` resting entries, strategy attribution, and
    ///   per-strategy `DopState::next_order_seqnum` watermarks with zero local disk state!
    pub fn bootstrap_from_exchange(
        &self,
        account_ref: u8,
        snapshot: &OrsExchangeBootstrapSnapshot,
    ) -> Result<OrsBootstrapSummary, v::RefusalReason> {
        let domain_cfg = self
            .account_domains
            .get(account_ref as usize)
            .and_then(|d| *d)
            .ok_or(v::RefusalReason::INVALID_ACCOUNT_ID)?;

        for b in &snapshot.balances {
            self.wallet_ledger.set_balance(
                account_ref,
                b.key,
                b.scale,
                b.free_atoms,
                b.locked_atoms,
            )?;
        }
        for p in &snapshot.positions {
            self.wallet_ledger.set_position_lots(
                account_ref,
                p.symbol_id as usize,
                p.signed_lots,
            )?;
        }

        let mut summary = OrsBootstrapSummary::default();
        for open in &snapshot.open_orders {
            let spec = self
                .symbols
                .get(open.symbol_id as usize)
                .ok_or(v::RefusalReason::InvalidQuantity)?;
            let unfilled_lots = open.orig_qty_lots.saturating_sub(open.cum_filled_lots);
            if open.price_ticks <= 0 || open.orig_qty_lots <= 0 || unfilled_lots <= 0 {
                return Err(v::RefusalReason::InvalidQuantity);
            }

            let (hold_key, hold_atoms) = spec.compute_hold_atoms(
                open.side,
                open.price_ticks,
                unfilled_lots,
                open.reduce_only,
            )?;
            let (_, hold_scale) = spec.hold_asset_for_side(open.side);

            let domain = domain_cfg.client_id_domain(spec.product);
            match v::SelfDescribingClientId::decode_str(&open.client_id, &domain) {
                v::DecodedClientId::Managed(decoded) if decoded.ors_strat_id().is_some() => {
                    let strat_id = decoded.ors_strat_id().expect("checked is_some");
                    let order_seqnum = decoded.intent_id;
                    let slot_idx = self.regular_slot_idx(strat_id, order_seqnum);
                    let slot = &self.slots[slot_idx];
                    self.claim_slot(slot, order_seqnum)?;

                    let guard = slot.order.lock();
                    let mut locked = LockedOrder { slot, guard };
                    if let Err(r) = self.wallet_ledger.reserve_bootstrap_hold(
                        account_ref,
                        hold_key,
                        hold_scale,
                        hold_atoms,
                        snapshot.balances_include_open_order_locks,
                    ) {
                        locked.retire_in_place(order_seqnum);
                        return Err(r);
                    }

                    let ors_order_id = make_ors_order_id(order_seqnum, strat_id);
                    *locked.order_mut() = OrsOrder {
                        ors_order_id,
                        slot_idx: slot_idx as u32,
                        strat_id,
                        account_ref,
                        symbol_id: open.symbol_id,
                        order_seqnum,
                        side: open.side,
                        tif: open.tif,
                        post_only: open.post_only,
                        reduce_only: open.reduce_only,
                        check_stp: CHECK_STP,
                        leg: decoded.leg,
                        status: OrsOrderStatus::Open,
                        front_ack_emitted: true,
                        price_ticks: open.price_ticks,
                        original_qty_lots: open.orig_qty_lots,
                        effective_qty_lots: open.orig_qty_lots,
                        filled_qty_lots: open.cum_filled_lots,
                        projected_fill_qty_lots: open.cum_filled_lots,
                        hold_asset_key: hold_key,
                        hold_scale,
                        held_atoms: hold_atoms,
                        pending_cxl: 0,
                        cancel_on_ack: false,
                        admin_cancel_requested: false,
                        query_fail_count: 0,
                        modify_pending: false,
                        pending_modify_price_ticks: 0,
                        pending_modify_qty_lots: 0,
                        pending_modify_extra_hold_atoms: 0,
                        created_mono_ns: open.created_mono_ns,
                        client_id: InlineAscii::lossy_ascii(&open.client_id),
                        venue_order_id: InlineAscii::lossy_ascii(&open.venue_order_id),
                        notes: InlineAscii::empty(),
                    };

                    if CHECK_STP
                        && let Some(mut book) =
                            self.lock_symbol_book(open.symbol_id as usize, &locked)
                    {
                        book.insert_order(
                            ors_order_id,
                            slot_idx as u32,
                            open.side,
                            open.price_ticks,
                        )?;
                    }
                    locked.publish_live(order_seqnum);
                    drop(locked);

                    self.dop_state
                        .observe_recovered_order(strat_id, order_seqnum)?;
                    summary.managed_open_orders += 1;
                }
                _ => {
                    self.wallet_ledger.reserve_bootstrap_hold(
                        account_ref,
                        hold_key,
                        hold_scale,
                        hold_atoms,
                        snapshot.balances_include_open_order_locks,
                    )?;
                    summary.external_open_orders += 1;
                }
            }
        }

        Ok(summary)
    }

    #[inline]
    fn regular_slot_idx(&self, strat_id: u8, order_seqnum: u64) -> usize {
        (usize::from(strat_id) * self.slots_per_strat) + ((order_seqnum as usize) & self.slot_mask)
    }

    #[inline]
    fn scratch_slot_idx(&self, strat_id: u8) -> usize {
        self.regular_slots_len + usize::from(strat_id)
    }

    /// Two-try CAS on `slot.identity` from `Empty`/`Retired` -> `Claimed` (`human-docs/3.2`).
    #[inline]
    fn claim_slot(&self, slot: &OrderSlot, order_seqnum: u64) -> Result<(), v::RefusalReason> {
        let desired = pack_slot_identity(SlotState::Claimed, order_seqnum);
        let mut cur = slot.identity.load(Ordering::Acquire);
        for _ in 0..2 {
            let (state, _) = unpack_slot_identity(cur);
            if matches!(state, SlotState::Claimed | SlotState::Live) {
                return Err(v::RefusalReason::ALLOCATION_ERROR);
            }
            match slot
                .identity
                .compare_exchange(cur, desired, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return Ok(()),
                Err(actual) => cur = actual,
            }
        }
        Err(v::RefusalReason::ALLOCATION_ERROR)
    }

    #[inline]
    fn required_remaining_hold_locked(
        &self,
        locked: &LockedOrder<'_>,
        spec: &OrsSymbolSpec,
    ) -> Result<i128, v::RefusalReason> {
        let o = locked.order();
        let active_unfilled = o.effective_qty_lots.saturating_sub(o.filled_qty_lots);
        let (_, mut req_hold) =
            spec.compute_hold_atoms(o.side, o.price_ticks, active_unfilled, o.reduce_only)?;
        if o.modify_pending {
            let pend_unfilled = o.pending_modify_qty_lots.saturating_sub(o.filled_qty_lots);
            let (_, pend_hold) = spec.compute_hold_atoms(
                o.side,
                o.pending_modify_price_ticks,
                pend_unfilled,
                o.reduce_only,
            )?;
            if pend_hold > req_hold {
                req_hold = pend_hold;
            }
        }
        Ok(req_hold)
    }

    fn reconcile_order_terms_locked(
        &self,
        locked: &mut LockedOrder<'_>,
        spec: &OrsSymbolSpec,
        new_price_ticks: i64,
        new_qty_lots: i64,
    ) -> Result<(), v::RefusalReason> {
        let o_snap = *locked.order();
        let unfilled = new_qty_lots.saturating_sub(o_snap.filled_qty_lots);
        let (_, new_hold) =
            spec.compute_hold_atoms(o_snap.side, new_price_ticks, unfilled, o_snap.reduce_only)?;
        if new_hold > o_snap.held_atoms {
            let diff = new_hold - o_snap.held_atoms;
            self.wallet_ledger.reserve_hold(
                o_snap.account_ref,
                o_snap.hold_asset_key,
                o_snap.hold_scale,
                diff,
            )?;
        } else if new_hold < o_snap.held_atoms {
            let diff = o_snap.held_atoms - new_hold;
            self.wallet_ledger
                .release_hold(o_snap.account_ref, o_snap.hold_asset_key, diff);
        }

        locked.order_mut().price_ticks = new_price_ticks;
        locked.order_mut().effective_qty_lots = new_qty_lots;
        locked.order_mut().held_atoms = new_hold;

        if CHECK_STP
            && o_snap.check_stp
            && unfilled > 0
            && let Some(mut book) = self.lock_symbol_book(o_snap.symbol_id as usize, locked)
        {
            book.commit_modify_price(
                o_snap.ors_order_id,
                o_snap.slot_idx,
                o_snap.side,
                new_price_ticks,
            )?;
        }
        Ok(())
    }

    #[inline]
    fn close_and_retire_locked(&self, locked: &mut LockedOrder<'_>) {
        let o = *locked.order();
        if CHECK_STP
            && o.check_stp
            && let Some(mut book) = self.lock_symbol_book(o.symbol_id as usize, locked)
        {
            book.unlink_order(o.ors_order_id);
        }
        if o.held_atoms > 0 {
            self.wallet_ledger
                .release_hold(o.account_ref, o.hold_asset_key, o.held_atoms);
            locked.order_mut().held_atoms = 0;
        }
        locked.retire_in_place(o.order_seqnum);
    }

    fn resolve_asset_ref(&self, asset_id: u64, scale: u8) -> Result<v::AssetRef, v::Error> {
        if let Some(found) = self.asset_refs.get(&asset_id) {
            return Ok(found.clone());
        }
        Ok(v::AssetRef {
            id: asset_id,
            version: 1,
            symbol: v::AssetSymbol::new(format!("ASSET{asset_id}"))?,
            exponent: -(scale as i8),
        })
    }
}

/// Zero-allocation stack encoder producing the exact 32-byte `[0-9a-z]` `tw1...` client ID
/// matching [`v::SelfDescribingClientId::encode`] byte-for-byte.
pub fn encode_tw1_client_id_inline(
    id: &v::SelfDescribingClientId,
    domain: &v::ClientIdDomain,
) -> Result<InlineAscii<64>, v::RefusalReason> {
    id.validate()
        .map_err(|_| v::RefusalReason::INVALID_STRAT_ID)?;

    let mut hasher = Sha256::new();
    hasher.update(b"tickwire.client-id.tw1");
    hasher.update(domain.risk_domain_id);
    hasher.update(domain.venue.to_le_bytes());
    hasher.update([match domain.environment {
        v::Environment::Production => 1,
        v::Environment::Test => 2,
    }]);
    hasher.update(domain.account.to_le_bytes());
    hasher.update([domain.product.code()]);
    hasher.update(id.logical_strategy_id.to_le_bytes());
    hasher.update([id.leg]);
    hasher.update(id.epoch.to_le_bytes());
    hasher.update(id.intent_id.to_le_bytes());
    let digest = hasher.finalize();
    let mut head = [0u8; 8];
    head.copy_from_slice(&digest[..8]);
    let tag_30 = u64::from_be_bytes(head) & TW1_TAG_MASK;

    let upper_116 = ((id.logical_strategy_id as u128) << 100)
        | ((id.leg as u128) << 96)
        | ((id.epoch as u128) << 64)
        | (id.intent_id as u128);

    let mut limbs = [
        ((upper_116 as u64) << 30) | tag_30,
        ((upper_116 >> 34) & u128::from(u64::MAX)) as u64,
        (upper_116 >> 98) as u64,
    ];

    let mut buf = [0u8; 64];
    buf[..3].copy_from_slice(TW1_PREFIX);
    for pos in (0..TW1_BASE36_LEN).rev() {
        let mut rem = 0u128;
        for limb in limbs.iter_mut().rev() {
            let cur = (rem << 64) | u128::from(*limb);
            *limb = (cur / 36) as u64;
            rem = cur % 36;
        }
        buf[3 + pos] = TW1_ALPHABET[rem as usize];
    }

    Ok(InlineAscii {
        bytes: buf,
        len: v::SELF_DESCRIBING_CLIENT_ID_LEN as u8,
    })
}
