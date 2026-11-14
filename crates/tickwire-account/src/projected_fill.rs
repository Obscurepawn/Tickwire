use tickwire_risk::{Fixed, RiskError, Rounding};
use tickwire_venue as v;

use crate::{Account, Error, OrderKey, Result};

pub const MAX_PROJECTED_FILLS: usize = 1024;

/// Volatile early execution projection derived from advancing cumulative exchange quantity.
/// Never mutates settled `Order.filled`, `Asset` balances, `Position`, `facts`, or `AccountJournal`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectedFill {
    pub order: OrderKey,
    pub instrument_id: u64,
    pub strategy: u64,
    pub side: v::Side,
    pub position_side: v::PositionSide,
    pub gap_quantity: Fixed,
    pub gap_price: Fixed,
    pub order_filled: Fixed,
    pub order_projected: Fixed,
    pub projected: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectedPriceInput {
    CumulativeQuote { cum_quote: Fixed },
    AveragePrice { avg_price: Fixed },
    LimitFallback,
}

impl Account {
    pub fn with_projected_fill(mut self, enabled: bool) -> Self {
        self.projected_fill_enabled = enabled;
        self
    }

    pub fn set_projected_fill(&mut self, enabled: bool) {
        self.projected_fill_enabled = enabled;
    }

    pub fn projected_fill_enabled(&self) -> bool {
        self.projected_fill_enabled
    }

    pub fn order_projected_filled(&self, key: &OrderKey) -> Option<Fixed> {
        let o = self.orders.get(key)?;
        o.projected_cum_qty.max(o.filled).ok()
    }

    pub fn pop_projected_fill(&mut self) -> Option<ProjectedFill> {
        self.projected_fills.pop_front()
    }

    pub fn drain_projected_fills(&mut self) -> Vec<ProjectedFill> {
        self.projected_fills.drain(..).collect()
    }

    /// Evaluates an exchange-reported cumulative executed quantity against the order's
    /// monotonic projection watermark without touching settled economic state.
    pub fn on_cum_qty(
        &mut self,
        key: &OrderKey,
        cum_qty: Fixed,
        price_input: ProjectedPriceInput,
    ) -> Result<Option<ProjectedFill>> {
        let (
            instrument_id,
            strategy,
            side,
            position_side,
            order_type,
            order_price,
            pending_price,
            base_scale,
            aborted,
            uncreated,
            terminal,
            prev_exch_qty,
            prev_cum_quote,
            prev_quote_qty,
            prev_avg_price,
            prev_avg_qty,
            prev_projected,
            settled_filled,
        ) = {
            let o = self.orders.get(key).ok_or(Error::Identity)?;
            (
                o.instrument,
                o.strategy,
                o.spec.side,
                o.spec.position_side,
                o.spec.order_type,
                o.price,
                o.pending_modify.as_ref().map(|pm| pm.requested_price),
                o.original.scale(),
                o.aborted,
                o.uncreated,
                o.terminal,
                o.exch_cum_qty,
                o.exch_cum_quote,
                o.exch_quote_qty,
                o.exch_avg_price,
                o.exch_avg_qty,
                o.projected_cum_qty,
                o.filled,
            )
        };

        cum_qty.nonnegative()?;
        if cum_qty.scale() != base_scale {
            return Err(Error::Risk(RiskError::ScaleMismatch));
        }

        match price_input {
            ProjectedPriceInput::CumulativeQuote { cum_quote } => {
                cum_quote.nonnegative()?;
            }
            ProjectedPriceInput::AveragePrice { avg_price } => {
                avg_price.nonnegative()?;
            }
            ProjectedPriceInput::LimitFallback => {}
        }

        // Only LIVE slots project (`human-docs/3.5`).
        if aborted || uncreated || terminal {
            return Ok(None);
        }

        // Stale report guard: older cumulative quantity is ignored.
        if cum_qty.atoms() < prev_exch_qty.atoms() {
            return Ok(None);
        }

        // CHECK_STP guard (`human-docs/3.5`, `3.6`): if this limit order crosses a resting
        // opposite-side order of ours, suppress projection until real trade confirmation.
        if order_type == v::OrderType::Limit {
            let check_price = match (side, pending_price) {
                (v::Side::Buy, Some(p)) => order_price.max(p)?,
                (v::Side::Sell, Some(p)) => order_price.min(p)?,
                _ => order_price,
            };
            if self.stp_book.self_cross(instrument_id, side, check_price)? {
                return Ok(None);
            }
        }

        let watermark = prev_projected.max(settled_filled)?;
        let gap = cum_qty.checked_sub(watermark)?;
        if gap.atoms() <= 0 {
            if cum_qty.atoms() >= prev_exch_qty.atoms() {
                let o = self.orders.get_mut(key).unwrap();
                if cum_qty.atoms() > prev_exch_qty.atoms() {
                    o.exch_cum_qty = cum_qty;
                }
                if cum_qty.atoms() > 0 {
                    match price_input {
                        ProjectedPriceInput::CumulativeQuote { cum_quote }
                            if cum_quote.atoms() > 0
                                && cum_qty.atoms() >= prev_quote_qty.atoms() =>
                        {
                            o.exch_cum_quote = Some(cum_quote);
                            o.exch_quote_qty = cum_qty;
                        }
                        ProjectedPriceInput::AveragePrice { avg_price }
                            if avg_price.atoms() > 0 && cum_qty.atoms() >= prev_avg_qty.atoms() =>
                        {
                            o.exch_avg_price = Some(avg_price);
                            o.exch_avg_qty = cum_qty;
                        }
                        _ => {}
                    }
                }
            }
            return Ok(None);
        }

        let price_scale = order_price.scale();
        let fallback_price = fallback_px(order_price, prev_avg_price, price_scale)?;

        let gap_price = match price_input {
            ProjectedPriceInput::CumulativeQuote { cum_quote } => {
                if cum_quote.atoms() == 0 {
                    fallback_price
                } else if let Some(prev_q) = prev_cum_quote
                    && prev_quote_qty.atoms() > 0
                    && cum_qty.atoms() > prev_quote_qty.atoms()
                {
                    let quote_delta_qty = cum_qty.checked_sub(prev_quote_qty)?;
                    let s = cum_quote.scale().max(prev_q.scale());
                    let cur_q = cum_quote.rescale(s, Rounding::Floor)?;
                    let old_q = prev_q.rescale(s, Rounding::Floor)?;
                    let delta_q = cur_q.checked_sub(old_q)?;
                    if delta_q.atoms() <= 0 {
                        fallback_price
                    } else {
                        let px = div_half_even(delta_q, quote_delta_qty, price_scale)?;
                        if px.atoms() > 0 { px } else { fallback_price }
                    }
                } else if prev_quote_qty.atoms() == 0 && cum_qty.atoms() > 0 {
                    let px = div_half_even(cum_quote, cum_qty, price_scale)?;
                    if px.atoms() > 0 { px } else { fallback_price }
                } else {
                    fallback_price
                }
            }
            ProjectedPriceInput::AveragePrice { avg_price } => {
                if avg_price.atoms() == 0 {
                    fallback_price
                } else if prev_avg_qty.atoms() == 0 || prev_avg_price.is_none_or(|p| p.atoms() == 0)
                {
                    let px = rescale_half_even(avg_price, price_scale)?;
                    if px.atoms() > 0 { px } else { fallback_price }
                } else if cum_qty.atoms() > prev_avg_qty.atoms() {
                    let avg_delta_qty = cum_qty.checked_sub(prev_avg_qty)?;
                    let prev_avg = prev_avg_price.unwrap();
                    let px_opt = delta_notional_div_half_even(
                        cum_qty,
                        avg_price,
                        prev_avg_qty,
                        prev_avg,
                        avg_delta_qty,
                        price_scale,
                    )?;
                    match px_opt {
                        Some(px) if px.atoms() > 0 => px,
                        _ => {
                            let avg_rescaled = rescale_half_even(avg_price, price_scale)?;
                            if avg_rescaled.atoms() > 0 {
                                avg_rescaled
                            } else {
                                fallback_price
                            }
                        }
                    }
                } else {
                    let px = rescale_half_even(avg_price, price_scale)?;
                    if px.atoms() > 0 { px } else { fallback_price }
                }
            }
            ProjectedPriceInput::LimitFallback => fallback_price,
        };

        {
            let o = self.orders.get_mut(key).unwrap();
            o.exch_cum_qty = cum_qty;
            o.projected_cum_qty = cum_qty;
            match price_input {
                ProjectedPriceInput::CumulativeQuote { cum_quote } if cum_quote.atoms() > 0 => {
                    o.exch_cum_quote = Some(cum_quote);
                    o.exch_quote_qty = cum_qty;
                }
                ProjectedPriceInput::AveragePrice { avg_price } if avg_price.atoms() > 0 => {
                    o.exch_avg_price = Some(avg_price);
                    o.exch_avg_qty = cum_qty;
                }
                _ => {}
            }
        }

        let pf = ProjectedFill {
            order: key.clone(),
            instrument_id,
            strategy,
            side,
            position_side,
            gap_quantity: gap,
            gap_price,
            order_filled: settled_filled,
            order_projected: cum_qty,
            projected: true,
        };

        if self.projected_fills.len() >= MAX_PROJECTED_FILLS {
            self.projected_fills.pop_front();
        }
        self.projected_fills.push_back(pf.clone());
        Ok(Some(pf))
    }
}

fn fallback_px(order_price: Fixed, prev_avg: Option<Fixed>, price_scale: u8) -> Result<Fixed> {
    if order_price.atoms() > 0 {
        return rescale_half_even(order_price, price_scale);
    }
    if let Some(avg) = prev_avg
        && avg.atoms() > 0
    {
        return rescale_half_even(avg, price_scale);
    }
    Ok(order_price)
}

fn rescale_half_even(val: Fixed, target_scale: u8) -> Result<Fixed> {
    div_half_even(val, Fixed::one(0)?, target_scale)
}

/// Computes `round_half_even(((cum_qty * avg_price) - (prev_qty * prev_avg)) / delta_qty)`
/// at `target_scale` using exact 256-bit integer arithmetic with zero floating-point operations.
fn delta_notional_div_half_even(
    cum_qty: Fixed,
    avg_price: Fixed,
    prev_qty: Fixed,
    prev_avg: Fixed,
    delta_qty: Fixed,
    target_scale: u8,
) -> Result<Option<Fixed>> {
    let common_px_scale = avg_price.scale().max(prev_avg.scale());
    let cur_avg = avg_price.rescale(common_px_scale, Rounding::Floor)?;
    let old_avg = prev_avg.rescale(common_px_scale, Rounding::Floor)?;

    let prod_cur = mul_u128_wide(cum_qty.atoms() as u128, cur_avg.atoms() as u128);
    let prod_old = mul_u128_wide(prev_qty.atoms() as u128, old_avg.atoms() as u128);

    let Some(diff_notional) = sub_u256_checked(prod_cur, prod_old) else {
        return Ok(None);
    };
    if diff_notional == (0, 0) {
        return Ok(None);
    }

    // `diff_notional` has implicit scale `cum_qty.scale() + common_px_scale`.
    // `delta_qty` has scale `delta_qty.scale()` (equal to `cum_qty.scale()`).
    // Therefore `(diff_notional / delta_qty.atoms())` has implicit scale `common_px_scale`!
    // To produce `target_scale`, shift by `exp_shift = target_scale - common_px_scale`.
    let exp_shift = i32::from(target_scale) - i32::from(common_px_scale);
    let den_atoms = delta_qty.atoms() as u128;
    if den_atoms == 0 {
        return Err(Error::Risk(RiskError::DivisionByZero));
    }

    let (num, den) = if exp_shift >= 0 {
        let pow = 10u128
            .checked_pow(exp_shift as u32)
            .ok_or(RiskError::ArithmeticOverflow)?;
        let scaled =
            mul_u256_u128_checked(diff_notional, pow).ok_or(RiskError::ArithmeticOverflow)?;
        (scaled, den_atoms)
    } else {
        let pow = 10u128
            .checked_pow((-exp_shift) as u32)
            .ok_or(RiskError::ArithmeticOverflow)?;
        let scaled_den = den_atoms
            .checked_mul(pow)
            .ok_or(RiskError::ArithmeticOverflow)?;
        (diff_notional, scaled_den)
    };

    let q = div_round_half_even_u256(num, den)?;
    let atoms = i128::try_from(q).map_err(|_| RiskError::ArithmeticOverflow)?;
    Ok(Some(Fixed::from_atoms(atoms, target_scale)?))
}

/// Exact integer `round_half_even((num / den) * 10^target_scale)` for nonnegative `num` and positive `den`.
pub(crate) fn div_half_even(num: Fixed, den: Fixed, target_scale: u8) -> Result<Fixed> {
    num.nonnegative()?;
    den.positive()?;
    if num.atoms() == 0 {
        return Ok(Fixed::zero(target_scale)?);
    }
    let exp_shift = i32::from(target_scale) + i32::from(den.scale()) - i32::from(num.scale());
    let u_num = num.atoms() as u128;
    let u_den = den.atoms() as u128;

    let (wide_num, eff_den) = if exp_shift >= 0 {
        let pow = 10u128
            .checked_pow(exp_shift as u32)
            .ok_or(RiskError::ArithmeticOverflow)?;
        (mul_u128_wide(u_num, pow), u_den)
    } else {
        let pow = 10u128
            .checked_pow((-exp_shift) as u32)
            .ok_or(RiskError::ArithmeticOverflow)?;
        let scaled_den = u_den
            .checked_mul(pow)
            .ok_or(RiskError::ArithmeticOverflow)?;
        ((0u128, u_num), scaled_den)
    };

    let q = div_round_half_even_u256(wide_num, eff_den)?;
    let atoms = i128::try_from(q).map_err(|_| RiskError::ArithmeticOverflow)?;
    Ok(Fixed::from_atoms(atoms, target_scale)?)
}

#[inline]
fn mul_u128_wide(a: u128, b: u128) -> (u128, u128) {
    let a_lo = a & 0xffff_ffff_ffff_ffff;
    let a_hi = a >> 64;
    let b_lo = b & 0xffff_ffff_ffff_ffff;
    let b_hi = b >> 64;

    let p0 = a_lo * b_lo;
    let p1 = a_lo * b_hi;
    let p2 = a_hi * b_lo;
    let p3 = a_hi * b_hi;

    let mid1 = (p0 >> 64) + (p1 & 0xffff_ffff_ffff_ffff) + (p2 & 0xffff_ffff_ffff_ffff);
    let hi = p3 + (p1 >> 64) + (p2 >> 64) + (mid1 >> 64);
    let lo = (p0 & 0xffff_ffff_ffff_ffff) | (mid1 << 64);
    (hi, lo)
}

#[inline]
fn sub_u256_checked(a: (u128, u128), b: (u128, u128)) -> Option<(u128, u128)> {
    if a.0 < b.0 || (a.0 == b.0 && a.1 < b.1) {
        return None;
    }
    let (lo, borrow) = a.1.overflowing_sub(b.1);
    let hi = a.0 - b.0 - u128::from(borrow);
    Some((hi, lo))
}

#[inline]
fn mul_u256_u128_checked(a: (u128, u128), m: u128) -> Option<(u128, u128)> {
    if m == 0 || a == (0, 0) {
        return Some((0, 0));
    }
    if m == 1 {
        return Some(a);
    }
    let (lo_hi, lo_lo) = mul_u128_wide(a.1, m);
    let (hi_hi, hi_lo) = mul_u128_wide(a.0, m);
    if hi_hi != 0 {
        return None;
    }
    let hi = hi_lo.checked_add(lo_hi)?;
    Some((hi, lo_lo))
}

fn div_round_half_even_u256(num: (u128, u128), den: u128) -> Result<u128> {
    if den == 0 {
        return Err(Error::Risk(RiskError::DivisionByZero));
    }
    if num.0 >= den {
        return Err(Error::Risk(RiskError::ArithmeticOverflow));
    }
    let (q, rem) = if num.0 == 0 {
        (num.1 / den, num.1 % den)
    } else {
        div_rem_u256_by_u128(num.0, num.1, den)
    };
    let half = den / 2;
    let round_up = if den & 1 == 0 {
        rem > half || (rem == half && (q & 1) == 1)
    } else {
        rem > half
    };
    if round_up {
        q.checked_add(1)
            .ok_or(Error::Risk(RiskError::ArithmeticOverflow))
    } else {
        Ok(q)
    }
}

fn div_rem_u256_by_u128(hi: u128, lo: u128, den: u128) -> (u128, u128) {
    debug_assert!(hi < den && den > 0);
    let mut rem = hi;
    let mut q = 0u128;
    for bit in (0..128).rev() {
        let carry = (rem >> 127) != 0;
        rem = (rem << 1) | ((lo >> bit) & 1);
        if carry || rem >= den {
            rem = rem.wrapping_sub(den);
            q |= 1u128 << bit;
        }
    }
    (q, rem)
}
