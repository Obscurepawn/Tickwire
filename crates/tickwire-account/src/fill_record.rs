//! Preallocated `2^fill_map_exponent` bucket `FillRecordPool` (`human-docs/3.4`, `3.9`).
//!
//! - Deduplicates identical executions arriving on both order socket replies and private streams
//!   keyed by `(ors_order_id, exec_ref, side, symbol_id)`.
//! - Upgrades `TRADE_LITE` (`fee == None`) or post-trade fee/rebate revisions into
//!   `FillRecordOutcome::Amended` (`amendment = true`), updating fee delta only without
//!   re-applying fill quantity or position!
//! - Uses `tickwire_shm_sys::MlockedSlice` at startup with lazy `fill_memory_s` retention reuse
//!   and zero runtime heap allocations.

use crate::spinlock::{CachePadded, TtasSpinLock};
use tickwire_shm_sys::{MlockOutcome, MlockPolicy, MlockedSlice};
use tickwire_venue as v;

/// Number of associative ways per hash bucket (fits comfortably in two cache lines).
pub const FILL_BUCKET_WAYS: usize = 8;

/// Minimum allowed `fill_map_exponent` (`2^2 = 4` buckets).
pub const MIN_FILL_MAP_EXPONENT: u8 = 2;

/// Maximum allowed `fill_map_exponent` (`2^22 = 4,194,304` buckets).
pub const MAX_FILL_MAP_EXPONENT: u8 = 22;

/// Exact deduplication key `(ors_order_id, exec_ref, side, symbol_id)` (`human-docs/3.9`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FillKey {
    pub ors_order_id: u64,
    pub exec_ref: u64,
    pub side: v::Side,
    pub symbol_id: u32,
}

impl Default for FillKey {
    fn default() -> Self {
        Self {
            ors_order_id: 0,
            exec_ref: 0,
            side: v::Side::Buy,
            symbol_id: 0,
        }
    }
}

/// Stack-allocated (`Copy`) signed fee cashflow in integer atoms (`10^-scale`).
///
/// Commission debits are negative (`signed_atoms < 0`), maker rebates are positive
/// (`signed_atoms > 0`), and explicit zero commission is `signed_atoms == 0`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrsFeeSpec {
    pub asset_key: v::AssetKey,
    pub signed_atoms: i128,
    pub scale: u8,
}

impl OrsFeeSpec {
    #[inline]
    pub const fn new(asset_key: v::AssetKey, signed_atoms: i128, scale: u8) -> Self {
        Self {
            asset_key,
            signed_atoms,
            scale,
        }
    }
}

/// Input observation passed to [`FillRecordPool::record_or_amend`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FillObservationInput {
    pub key: FillKey,
    pub qty_lots: i64,
    pub price_ticks: i64,
    /// `None` represents a fee-omitted execution (`TRADE_LITE`); `Some` carries exact fee/rebate.
    pub fee: Option<OrsFeeSpec>,
    pub now_mono_ns: u64,
}

/// Classification returned by [`FillRecordPool::record_or_amend`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FillRecordOutcome {
    /// First observation of `key`: caller applies fill quantity, position/wallet settlement, and
    /// optional `fee`, emitting `Fill { amendment: false, .. }`.
    Applied { fee: Option<OrsFeeSpec> },
    /// `key` was already applied (`TRADE_LITE` or prior fee), and a new/revised `new_fee` arrived:
    /// caller reverses `previous_fee` (if `Some`), applies `new_fee` to wallet balances ONLY, does
    /// NOT touch fill quantity/holds/positions, and emits `Fill { amendment: true, .. }`.
    Amended {
        qty_lots: i64,
        price_ticks: i64,
        previous_fee: Option<OrsFeeSpec>,
        new_fee: OrsFeeSpec,
        /// Net fee cashflow reported on the `amendment = true` `Fill`: equals `new_fee - old_fee`
        /// when `previous_fee` shares `(asset_key, scale)`, or `new_fee` otherwise.
        reported_fee_delta: OrsFeeSpec,
    },
    /// Identical or redundant (`TRADE_LITE` after full `TRADE`) observation: caller skips silently.
    Duplicate,
}

#[derive(Debug, Clone, Copy, Default)]
struct FillSlot {
    occupied: bool,
    key: FillKey,
    qty_lots: i64,
    price_ticks: i64,
    fee: Option<OrsFeeSpec>,
    recorded_mono_ns: u64,
}

#[derive(Debug, Default)]
struct FillBucket {
    ways: [FillSlot; FILL_BUCKET_WAYS],
}

/// Startup-preallocated, optionally `mlock`ed `2^fill_map_exponent` bucket `FillRecord` pool.
#[derive(Debug)]
pub struct FillRecordPool {
    buckets: MlockedSlice<CachePadded<TtasSpinLock<FillBucket>>>,
    mask: usize,
    retention_ns: u64,
    mlock_outcome: MlockOutcome,
}

impl FillRecordPool {
    /// Allocates `1 << fill_map_exponent` associative buckets once at startup and applies
    /// `mlock_policy` via [`tickwire_shm_sys::MlockedSlice`].
    pub fn new(
        fill_map_exponent: u8,
        fill_memory_s: u64,
        mlock_policy: MlockPolicy,
    ) -> Result<Self, String> {
        if !(MIN_FILL_MAP_EXPONENT..=MAX_FILL_MAP_EXPONENT).contains(&fill_map_exponent) {
            return Err(format!(
                "fill_map_exponent {fill_map_exponent} outside [{MIN_FILL_MAP_EXPONENT}, {MAX_FILL_MAP_EXPONENT}]"
            ));
        }
        if fill_memory_s == 0 {
            return Err(String::from("fill_memory_s must be > 0"));
        }
        let num_buckets = 1usize << fill_map_exponent;
        let retention_ns = fill_memory_s.saturating_mul(1_000_000_000);
        let raw = (0..num_buckets)
            .map(|_| CachePadded::new(TtasSpinLock::new(FillBucket::default())))
            .collect::<Vec<_>>();
        let buckets = MlockedSlice::from_vec(raw, mlock_policy)
            .map_err(|e| format!("failed to mlock FillRecordPool: {e}"))?;
        let mlock_outcome = buckets.outcome();
        Ok(Self {
            buckets,
            mask: num_buckets - 1,
            retention_ns,
            mlock_outcome,
        })
    }

    #[inline]
    pub fn bucket_count(&self) -> usize {
        self.buckets.len()
    }

    #[inline]
    pub fn retention_ns(&self) -> u64 {
        self.retention_ns
    }

    #[inline]
    pub fn mlock_outcome(&self) -> MlockOutcome {
        self.mlock_outcome
    }

    /// Probes, inserts, deduplicates, or amends a fill execution in `O(1)` without heap allocation.
    pub fn record_or_amend(
        &self,
        input: FillObservationInput,
    ) -> Result<FillRecordOutcome, v::RefusalReason> {
        if input.key.ors_order_id == 0 || input.key.exec_ref == 0 || input.qty_lots <= 0 {
            return Err(v::RefusalReason::InvalidQuantity);
        }
        let bucket_idx = (hash_fill_key(&input.key) as usize) & self.mask;
        let mut bucket = self.buckets[bucket_idx].lock();

        // 1. Probe all ways for an existing matching key.
        for way in &mut bucket.ways {
            if way.occupied && way.key == input.key {
                let Some(new_fee) = input.fee else {
                    // Late or duplicate TRADE_LITE adds no fee information.
                    return Ok(FillRecordOutcome::Duplicate);
                };
                match way.fee {
                    None => {
                        way.fee = Some(new_fee);
                        if input.now_mono_ns > way.recorded_mono_ns {
                            way.recorded_mono_ns = input.now_mono_ns;
                        }
                        return Ok(FillRecordOutcome::Amended {
                            qty_lots: way.qty_lots,
                            price_ticks: way.price_ticks,
                            previous_fee: None,
                            new_fee,
                            reported_fee_delta: new_fee,
                        });
                    }
                    Some(prev_fee) => {
                        if prev_fee == new_fee {
                            return Ok(FillRecordOutcome::Duplicate);
                        }
                        let reported_fee_delta = if prev_fee.asset_key == new_fee.asset_key
                            && prev_fee.scale == new_fee.scale
                        {
                            let delta = new_fee
                                .signed_atoms
                                .checked_sub(prev_fee.signed_atoms)
                                .ok_or(v::RefusalReason::InvalidQuantity)?;
                            OrsFeeSpec {
                                asset_key: new_fee.asset_key,
                                signed_atoms: delta,
                                scale: new_fee.scale,
                            }
                        } else {
                            new_fee
                        };
                        way.fee = Some(new_fee);
                        if input.now_mono_ns > way.recorded_mono_ns {
                            way.recorded_mono_ns = input.now_mono_ns;
                        }
                        return Ok(FillRecordOutcome::Amended {
                            qty_lots: way.qty_lots,
                            price_ticks: way.price_ticks,
                            previous_fee: Some(prev_fee),
                            new_fee,
                            reported_fee_delta,
                        });
                    }
                }
            }
        }

        // 2. Key not found: prefer an empty way, else overwrite the oldest expired way.
        if let Some(empty_way) = bucket.ways.iter_mut().find(|w| !w.occupied) {
            *empty_way = FillSlot {
                occupied: true,
                key: input.key,
                qty_lots: input.qty_lots,
                price_ticks: input.price_ticks,
                fee: input.fee,
                recorded_mono_ns: input.now_mono_ns,
            };
            return Ok(FillRecordOutcome::Applied { fee: input.fee });
        }

        let mut oldest_expired_idx: Option<(usize, u64)> = None;
        for (idx, way) in bucket.ways.iter().enumerate() {
            let age_ns = input.now_mono_ns.saturating_sub(way.recorded_mono_ns);
            if age_ns >= self.retention_ns {
                match oldest_expired_idx {
                    None => oldest_expired_idx = Some((idx, way.recorded_mono_ns)),
                    Some((_, best_ts)) if way.recorded_mono_ns < best_ts => {
                        oldest_expired_idx = Some((idx, way.recorded_mono_ns));
                    }
                    _ => {}
                }
            }
        }

        let Some((reuse_idx, _)) = oldest_expired_idx else {
            return Err(v::RefusalReason::ALLOCATION_ERROR);
        };

        bucket.ways[reuse_idx] = FillSlot {
            occupied: true,
            key: input.key,
            qty_lots: input.qty_lots,
            price_ticks: input.price_ticks,
            fee: input.fee,
            recorded_mono_ns: input.now_mono_ns,
        };
        Ok(FillRecordOutcome::Applied { fee: input.fee })
    }
}

/// Deterministically maps a borrowed ASCII `trade_id` to a non-zero 64-bit `exec_ref` with zero
/// heap allocation:
/// - Canonical positive decimal integers in `1..(1 << 63)` map directly to their `u64` value.
/// - Non-numeric or wide strings hash via 64-bit FNV-1a + SplitMix64 avalanche with bit 63 set so
///   numeric and hashed identifiers live in disjoint namespaces.
#[inline]
pub fn trade_id_to_exec_ref(trade_id: &str) -> u64 {
    let bytes = trade_id.as_bytes();
    if !bytes.is_empty() && bytes[0] != b'0' && bytes.len() <= 19 {
        let mut val = 0u64;
        let mut all_digits = true;
        for &b in bytes {
            if !b.is_ascii_digit() {
                all_digits = false;
                break;
            }
            if let Some(next) = val
                .checked_mul(10)
                .and_then(|n| n.checked_add(u64::from(b - b'0')))
            {
                val = next;
            } else {
                all_digits = false;
                break;
            }
        }
        if all_digits && val > 0 && val < (1u64 << 63) {
            return val;
        }
    }

    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    splitmix64(h) | (1u64 << 63)
}

#[inline]
fn hash_fill_key(key: &FillKey) -> u64 {
    let side_tag = match key.side {
        v::Side::Buy => 0x517c_c1b7_2722_0a95u64,
        v::Side::Sell => 0x9e37_79b9_7f4a_7c15u64,
    };
    let mut x = splitmix64(key.ors_order_id);
    x ^= splitmix64(key.exec_ref.rotate_left(17));
    x ^= splitmix64(u64::from(key.symbol_id) ^ side_tag);
    splitmix64(x)
}

#[inline]
const fn splitmix64(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}
