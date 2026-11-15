//! Multi-wallet / Unified Trading Account (`UtaWalletLedger`) exact fixed-point balance, hold,
//! and position accounting keyed by `(account_ref, v::AssetKey)` across Spot, USD-M, Coin-M,
//! Options, Linear/Inverse Perpetuals, and Linear/Inverse Futures (`#![forbid(unsafe_code)]`).

use crate::fill_record::OrsFeeSpec;
use crate::spinlock::{CachePadded, TtasSpinLock};
use tickwire_risk::{Fixed, Rounding};
use tickwire_venue as v;

/// Maximum distinct `(wallet_id, asset_id)` balances per concrete `account_ref` (`1..=255`).
pub const MAX_WALLET_ASSETS_PER_ACCOUNT: usize = 64;

/// Static instrument and margin specification indexed by compact `symbol_id: usize`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrsSymbolSpec {
    pub symbol_id: u32,
    pub instrument_id: u64,
    pub product: v::Product,
    pub wallet_id: u16,
    pub base_asset_id: u64,
    pub base_scale: u8,
    pub quote_asset_id: u64,
    pub quote_scale: u8,
    pub settlement_asset_id: u64,
    pub settlement_scale: u8,
    /// Price tick in `10^-price_scale` units (`price_ticks * price_tick_atoms`).
    pub price_tick_atoms: i128,
    pub price_scale: u8,
    /// Quantity lot in `10^-qty_scale` units (`qty_lots * qty_step_atoms`).
    pub qty_step_atoms: i128,
    pub qty_scale: u8,
    /// Contract multiplier in `10^-contract_scale` units (`1` at scale `0` for standard Spot/Linear).
    pub contract_size_atoms: i128,
    pub contract_scale: u8,
    /// Initial margin rate in basis points (`10_000 == 100%`; e.g. `1_000 == 10x` leverage).
    /// Ignored for `Product::Spot` (which reserves full spent base/quote notional).
    pub initial_margin_bps: u32,
}

impl OrsSymbolSpec {
    pub fn validate(&self) -> Result<(), v::RefusalReason> {
        if self.price_tick_atoms <= 0
            || self.qty_step_atoms <= 0
            || self.contract_size_atoms <= 0
            || self.base_scale > 18
            || self.quote_scale > 18
            || self.settlement_scale > 18
            || self.price_scale > 18
            || self.qty_scale > 18
            || self.contract_scale > 18
        {
            return Err(v::RefusalReason::InvalidQuantity);
        }
        if self.product != v::Product::Spot && self.initial_margin_bps == 0 {
            return Err(v::RefusalReason::InvalidQuantity);
        }
        Ok(())
    }

    #[inline]
    pub const fn base_key(&self) -> v::AssetKey {
        v::AssetKey::new(self.wallet_id, self.base_asset_id)
    }

    #[inline]
    pub const fn quote_key(&self) -> v::AssetKey {
        v::AssetKey::new(self.wallet_id, self.quote_asset_id)
    }

    #[inline]
    pub const fn settlement_key(&self) -> v::AssetKey {
        v::AssetKey::new(self.wallet_id, self.settlement_asset_id)
    }

    /// Returns `(held_asset_key, held_scale)` for an order on `side`.
    #[inline]
    pub const fn hold_asset_for_side(&self, side: v::Side) -> (v::AssetKey, u8) {
        match self.product {
            v::Product::Spot => match side {
                v::Side::Buy => (self.quote_key(), self.quote_scale),
                v::Side::Sell => (self.base_key(), self.base_scale),
            },
            v::Product::LinearPerpetual
            | v::Product::InversePerpetual
            | v::Product::Option
            | v::Product::LinearFuture
            | v::Product::InverseFuture
            | v::Product::UsdM
            | v::Product::CoinM => (self.settlement_key(), self.settlement_scale),
        }
    }

    /// Converts integer `price_ticks` into an exact `v::Decimal`.
    #[inline]
    pub fn price_decimal(&self, price_ticks: i64) -> Result<v::Decimal, v::RefusalReason> {
        let atoms = i128::from(price_ticks)
            .checked_mul(self.price_tick_atoms)
            .ok_or(v::RefusalReason::InvalidPrice)?;
        v::Decimal::new(atoms, -(self.price_scale as i8))
            .map_err(|_| v::RefusalReason::InvalidPrice)
    }

    /// Converts integer `qty_lots` into an exact `v::Decimal`.
    #[inline]
    pub fn qty_decimal(&self, qty_lots: i64) -> Result<v::Decimal, v::RefusalReason> {
        let atoms = i128::from(qty_lots)
            .checked_mul(self.qty_step_atoms)
            .ok_or(v::RefusalReason::InvalidQuantity)?;
        v::Decimal::new(atoms, -(self.qty_scale as i8))
            .map_err(|_| v::RefusalReason::InvalidQuantity)
    }

    /// Computes exact initial hold atoms (rounded up with `Rounding::Ceil`) for `unfilled_lots`
    /// at `price_ticks` with zero floating-point operations.
    pub fn compute_hold_atoms(
        &self,
        side: v::Side,
        price_ticks: i64,
        unfilled_lots: i64,
        reduce_only: bool,
    ) -> Result<(v::AssetKey, i128), v::RefusalReason> {
        let (hold_key, hold_scale) = self.hold_asset_for_side(side);
        if unfilled_lots <= 0 {
            return Ok((hold_key, 0));
        }
        if price_ticks <= 0 {
            return Err(v::RefusalReason::InvalidPrice);
        }
        if self.product != v::Product::Spot && reduce_only {
            return Ok((hold_key, 0));
        }

        let qty = Fixed::from_atoms(
            i128::from(unfilled_lots)
                .checked_mul(self.qty_step_atoms)
                .ok_or(v::RefusalReason::InvalidQuantity)?,
            self.qty_scale,
        )
        .map_err(|_| v::RefusalReason::InvalidQuantity)?;
        let px = Fixed::from_atoms(
            i128::from(price_ticks)
                .checked_mul(self.price_tick_atoms)
                .ok_or(v::RefusalReason::InvalidPrice)?,
            self.price_scale,
        )
        .map_err(|_| v::RefusalReason::InvalidPrice)?;
        let mult = Fixed::from_atoms(self.contract_size_atoms, self.contract_scale)
            .map_err(|_| v::RefusalReason::InvalidQuantity)?;

        let hold_fixed = match self.product {
            v::Product::Spot => match side {
                v::Side::Buy => qty
                    .mul_to(px, hold_scale, Rounding::Ceil)
                    .map_err(|_| v::RefusalReason::InvalidQuantity)?,
                v::Side::Sell => qty
                    .rescale(hold_scale, Rounding::Ceil)
                    .map_err(|_| v::RefusalReason::InvalidQuantity)?,
            },
            v::Product::LinearPerpetual
            | v::Product::LinearFuture
            | v::Product::Option
            | v::Product::UsdM => {
                let eff_qty = qty
                    .mul_to(mult, 18, Rounding::Ceil)
                    .map_err(|_| v::RefusalReason::InvalidQuantity)?;
                let notional = eff_qty
                    .mul_to(px, 18, Rounding::Ceil)
                    .map_err(|_| v::RefusalReason::InvalidQuantity)?;
                let im_num = notional
                    .checked_mul_int(i128::from(self.initial_margin_bps))
                    .map_err(|_| v::RefusalReason::InvalidQuantity)?;
                let bps_denom =
                    Fixed::from_atoms(10_000, 0).map_err(|_| v::RefusalReason::InvalidQuantity)?;
                im_num
                    .div_to(bps_denom, hold_scale, Rounding::Ceil)
                    .map_err(|_| v::RefusalReason::InvalidQuantity)?
            }
            v::Product::InversePerpetual | v::Product::InverseFuture | v::Product::CoinM => {
                let contract_notional = qty
                    .mul_to(mult, 18, Rounding::Ceil)
                    .map_err(|_| v::RefusalReason::InvalidQuantity)?;
                let base_equiv = contract_notional
                    .div_to(px, 18, Rounding::Ceil)
                    .map_err(|_| v::RefusalReason::InvalidQuantity)?;
                let im_num = base_equiv
                    .checked_mul_int(i128::from(self.initial_margin_bps))
                    .map_err(|_| v::RefusalReason::InvalidQuantity)?;
                let bps_denom =
                    Fixed::from_atoms(10_000, 0).map_err(|_| v::RefusalReason::InvalidQuantity)?;
                im_num
                    .div_to(bps_denom, hold_scale, Rounding::Ceil)
                    .map_err(|_| v::RefusalReason::InvalidQuantity)?
            }
        };

        Ok((hold_key, hold_fixed.atoms()))
    }
}

/// Snapshot of a single `(wallet_id, asset_id)` balance entry.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WalletAssetBalance {
    pub occupied: bool,
    pub key: v::AssetKey,
    pub scale: u8,
    pub free_atoms: i128,
    pub locked_atoms: i128,
}

#[derive(Debug)]
struct AccountLedgerInner {
    assets: [WalletAssetBalance; MAX_WALLET_ASSETS_PER_ACCOUNT],
    asset_len: usize,
    /// Signed net position in integer lots indexed by `symbol_id` (`+` long, `-` short).
    position_lots: Box<[i64]>,
}

impl AccountLedgerInner {
    fn new(num_symbols: usize) -> Self {
        Self {
            assets: [WalletAssetBalance::default(); MAX_WALLET_ASSETS_PER_ACCOUNT],
            asset_len: 0,
            position_lots: vec![0i64; num_symbols].into_boxed_slice(),
        }
    }

    #[inline]
    fn find_asset_idx(&self, key: v::AssetKey) -> Option<usize> {
        self.assets[..self.asset_len]
            .iter()
            .position(|a| a.occupied && a.key == key)
    }

    #[inline]
    fn get_or_insert_asset(
        &mut self,
        key: v::AssetKey,
        scale: u8,
    ) -> Result<&mut WalletAssetBalance, v::RefusalReason> {
        if let Some(idx) = self.find_asset_idx(key) {
            if self.assets[idx].scale != scale {
                return Err(v::RefusalReason::InvalidQuantity);
            }
            return Ok(&mut self.assets[idx]);
        }
        if self.asset_len >= MAX_WALLET_ASSETS_PER_ACCOUNT {
            return Err(v::RefusalReason::ALLOCATION_ERROR);
        }
        let idx = self.asset_len;
        self.assets[idx] = WalletAssetBalance {
            occupied: true,
            key,
            scale,
            free_atoms: 0,
            locked_atoms: 0,
        };
        self.asset_len = idx + 1;
        Ok(&mut self.assets[idx])
    }
}

/// Preallocated multi-account, multi-wallet UTA balance and position ledger.
#[derive(Debug)]
pub struct UtaWalletLedger {
    accounts: Box<[CachePadded<TtasSpinLock<AccountLedgerInner>>]>,
}

impl UtaWalletLedger {
    pub fn new(num_symbols: usize) -> Self {
        let accounts = (0..256)
            .map(|_| CachePadded::new(TtasSpinLock::new(AccountLedgerInner::new(num_symbols))))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self { accounts }
    }

    /// Sets or seeds `(account_ref, key)` free/locked balances during cold Exchange REST bootstrap.
    pub fn set_balance(
        &self,
        account_ref: u8,
        key: v::AssetKey,
        scale: u8,
        free_atoms: i128,
        locked_atoms: i128,
    ) -> Result<(), v::RefusalReason> {
        if account_ref == 0 || free_atoms < 0 || locked_atoms < 0 || scale > 18 {
            return Err(v::RefusalReason::InvalidQuantity);
        }
        let mut acc = self.accounts[account_ref as usize].lock();
        let slot = acc.get_or_insert_asset(key, scale)?;
        slot.free_atoms = free_atoms;
        slot.locked_atoms = locked_atoms;
        Ok(())
    }

    /// Sets `(account_ref, symbol_id)` initial signed position lots during cold REST bootstrap.
    pub fn set_position_lots(
        &self,
        account_ref: u8,
        symbol_id: usize,
        signed_lots: i64,
    ) -> Result<(), v::RefusalReason> {
        if account_ref == 0 {
            return Err(v::RefusalReason::INVALID_ACCOUNT_ID);
        }
        let mut acc = self.accounts[account_ref as usize].lock();
        let pos = acc
            .position_lots
            .get_mut(symbol_id)
            .ok_or(v::RefusalReason::InvalidQuantity)?;
        *pos = signed_lots;
        Ok(())
    }

    /// Reads `(free_atoms, locked_atoms)` for `(account_ref, key)`.
    pub fn balance(&self, account_ref: u8, key: v::AssetKey) -> Option<(i128, i128)> {
        let acc = self.accounts.get(account_ref as usize)?.lock();
        let idx = acc.find_asset_idx(key)?;
        let entry = &acc.assets[idx];
        Some((entry.free_atoms, entry.locked_atoms))
    }

    /// Reads signed net position in integer lots for `(account_ref, symbol_id)`.
    pub fn position_lots(&self, account_ref: u8, symbol_id: usize) -> Option<i64> {
        let acc = self.accounts.get(account_ref as usize)?.lock();
        acc.position_lots.get(symbol_id).copied()
    }

    /// Reserves `atoms` from `free_atoms` into `locked_atoms` on `(account_ref, key)`.
    pub fn reserve_hold(
        &self,
        account_ref: u8,
        key: v::AssetKey,
        scale: u8,
        atoms: i128,
    ) -> Result<(), v::RefusalReason> {
        if atoms <= 0 {
            return Ok(());
        }
        let mut acc = self.accounts[account_ref as usize].lock();
        let Some(idx) = acc.find_asset_idx(key) else {
            return Err(v::RefusalReason::InsufficientFunds);
        };
        let entry = &mut acc.assets[idx];
        if entry.scale != scale || entry.free_atoms < atoms {
            return Err(v::RefusalReason::InsufficientFunds);
        }
        entry.free_atoms -= atoms;
        entry.locked_atoms = entry
            .locked_atoms
            .checked_add(atoms)
            .ok_or(v::RefusalReason::InvalidQuantity)?;
        Ok(())
    }

    /// Converts existing `free_atoms` into `locked_atoms` when reconstructing an open order from
    /// Exchange REST snapshots, clamping to `free_atoms` if the snapshot already reported those
    /// funds inside `locked_atoms`.
    pub fn reserve_bootstrap_hold(
        &self,
        account_ref: u8,
        key: v::AssetKey,
        scale: u8,
        atoms: i128,
        snapshot_already_locked: bool,
    ) -> Result<(), v::RefusalReason> {
        if atoms <= 0 {
            return Ok(());
        }
        let mut acc = self.accounts[account_ref as usize].lock();
        let entry = acc.get_or_insert_asset(key, scale)?;
        if snapshot_already_locked {
            if entry.locked_atoms < atoms {
                return Err(v::RefusalReason::InsufficientFunds);
            }
            return Ok(());
        }
        if entry.free_atoms < atoms {
            return Err(v::RefusalReason::InsufficientFunds);
        }
        entry.free_atoms -= atoms;
        entry.locked_atoms = entry
            .locked_atoms
            .checked_add(atoms)
            .ok_or(v::RefusalReason::InvalidQuantity)?;
        Ok(())
    }

    /// Releases `atoms` from `locked_atoms` back into `free_atoms`.
    pub fn release_hold(&self, account_ref: u8, key: v::AssetKey, atoms: i128) {
        if atoms <= 0 {
            return;
        }
        let mut acc = self.accounts[account_ref as usize].lock();
        if let Some(idx) = acc.find_asset_idx(key) {
            let entry = &mut acc.assets[idx];
            let released = atoms.min(entry.locked_atoms);
            entry.locked_atoms -= released;
            entry.free_atoms = entry.free_atoms.saturating_add(released);
        }
    }

    /// Settles a newly applied execution (`Applied`), releasing `release_hold_atoms` from the
    /// order's hold, settling Spot base/quote cashflows or Derivative position lots, and applying
    /// `fee` (if `Some`).
    #[allow(clippy::too_many_arguments)]
    pub fn settle_applied_fill(
        &self,
        account_ref: u8,
        spec: &OrsSymbolSpec,
        side: v::Side,
        fill_lots: i64,
        exec_price_ticks: i64,
        hold_key: v::AssetKey,
        release_hold_atoms: i128,
        fee: Option<OrsFeeSpec>,
    ) -> Result<(), v::RefusalReason> {
        let mut acc = self.accounts[account_ref as usize].lock();

        // 1. Release proportional order hold back to free_atoms first.
        if release_hold_atoms > 0
            && let Some(idx) = acc.find_asset_idx(hold_key)
        {
            let entry = &mut acc.assets[idx];
            let released = release_hold_atoms.min(entry.locked_atoms);
            entry.locked_atoms -= released;
            entry.free_atoms = entry.free_atoms.saturating_add(released);
        }

        // 2. Product-specific execution settlement.
        let qty = Fixed::from_atoms(
            i128::from(fill_lots)
                .checked_mul(spec.qty_step_atoms)
                .ok_or(v::RefusalReason::InvalidQuantity)?,
            spec.qty_scale,
        )
        .map_err(|_| v::RefusalReason::InvalidQuantity)?;
        let px = Fixed::from_atoms(
            i128::from(exec_price_ticks)
                .checked_mul(spec.price_tick_atoms)
                .ok_or(v::RefusalReason::InvalidPrice)?,
            spec.price_scale,
        )
        .map_err(|_| v::RefusalReason::InvalidPrice)?;

        match spec.product {
            v::Product::Spot => {
                let base_atoms = qty
                    .rescale(spec.base_scale, Rounding::Floor)
                    .map_err(|_| v::RefusalReason::InvalidQuantity)?
                    .atoms();
                let quote_rounding = match side {
                    v::Side::Buy => Rounding::Ceil,
                    v::Side::Sell => Rounding::Floor,
                };
                let quote_atoms = qty
                    .mul_to(px, spec.quote_scale, quote_rounding)
                    .map_err(|_| v::RefusalReason::InvalidQuantity)?
                    .atoms();

                match side {
                    v::Side::Buy => {
                        let q = acc.get_or_insert_asset(spec.quote_key(), spec.quote_scale)?;
                        q.free_atoms = q
                            .free_atoms
                            .checked_sub(quote_atoms)
                            .ok_or(v::RefusalReason::InvalidQuantity)?;
                        let b = acc.get_or_insert_asset(spec.base_key(), spec.base_scale)?;
                        b.free_atoms = b
                            .free_atoms
                            .checked_add(base_atoms)
                            .ok_or(v::RefusalReason::InvalidQuantity)?;
                    }
                    v::Side::Sell => {
                        let b = acc.get_or_insert_asset(spec.base_key(), spec.base_scale)?;
                        b.free_atoms = b
                            .free_atoms
                            .checked_sub(base_atoms)
                            .ok_or(v::RefusalReason::InvalidQuantity)?;
                        let q = acc.get_or_insert_asset(spec.quote_key(), spec.quote_scale)?;
                        q.free_atoms = q
                            .free_atoms
                            .checked_add(quote_atoms)
                            .ok_or(v::RefusalReason::InvalidQuantity)?;
                    }
                }
            }
            v::Product::LinearPerpetual
            | v::Product::InversePerpetual
            | v::Product::Option
            | v::Product::LinearFuture
            | v::Product::InverseFuture
            | v::Product::UsdM
            | v::Product::CoinM => {
                if let Some(pos) = acc.position_lots.get_mut(spec.symbol_id as usize) {
                    let signed_fill = match side {
                        v::Side::Buy => fill_lots,
                        v::Side::Sell => -fill_lots,
                    };
                    *pos = pos.saturating_add(signed_fill);
                }
            }
        }

        // 3. Apply commission/rebate cashflow if known.
        if let Some(f) = fee {
            let slot = acc.get_or_insert_asset(f.asset_key, f.scale)?;
            slot.free_atoms = slot
                .free_atoms
                .checked_add(f.signed_atoms)
                .ok_or(v::RefusalReason::InvalidQuantity)?;
        }

        Ok(())
    }

    /// Applies a `FillRecordOutcome::Amended` fee patch (`TRADE_LITE` -> full `TRADE` or post-trade
    /// rebate adjustment): updates wallet fee delta **ONLY**, never touching positions or order
    /// holds!
    pub fn apply_fee_amendment(
        &self,
        account_ref: u8,
        previous_fee: Option<OrsFeeSpec>,
        new_fee: OrsFeeSpec,
    ) -> Result<(), v::RefusalReason> {
        let mut acc = self.accounts[account_ref as usize].lock();
        if let Some(prev) = previous_fee {
            let prev_slot = acc.get_or_insert_asset(prev.asset_key, prev.scale)?;
            prev_slot.free_atoms = prev_slot
                .free_atoms
                .checked_sub(prev.signed_atoms)
                .ok_or(v::RefusalReason::InvalidQuantity)?;
        }
        let next_slot = acc.get_or_insert_asset(new_fee.asset_key, new_fee.scale)?;
        next_slot.free_atoms = next_slot
            .free_atoms
            .checked_add(new_fee.signed_atoms)
            .ok_or(v::RefusalReason::InvalidQuantity)?;
        Ok(())
    }
}
