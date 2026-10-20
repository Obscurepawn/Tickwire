use crate::{Fixed, Result, RiskError, Rounding, fixed::sum};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartialFill {
    pub original_reserve: Fixed,
    pub actual_debit: Fixed,
    pub remaining_quantity: Fixed,
    pub remaining_reserve: Fixed,
    pub released_upper_bound: Fixed,
    pub debit_plus_reserve: Fixed,
}
pub fn partial_buy_fill(
    original_quantity: Fixed,
    buy_cap: Fixed,
    fill_quantity: Fixed,
    fill_price: Fixed,
    fee_tail: Fixed,
) -> Result<PartialFill> {
    original_quantity.nonnegative()?;
    fill_quantity.nonnegative()?;
    buy_cap.positive()?;
    fill_price.positive()?;
    fee_tail.nonnegative()?;
    buy_cap.ensure_same_scale(fill_price)?;
    if fill_price.atoms() > buy_cap.atoms() {
        return Err(RiskError::InvalidInput);
    }
    let quantity = original_quantity
        .checked_sub(fill_quantity)?
        .nonnegative()?;
    let original_reserve = original_quantity
        .mul_to(buy_cap, fee_tail.scale(), Rounding::Ceil)?
        .checked_add(fee_tail)?;
    let actual_debit = fill_quantity.mul_to(fill_price, fee_tail.scale(), Rounding::Ceil)?;
    let remaining_reserve = quantity
        .mul_to(buy_cap, fee_tail.scale(), Rounding::Ceil)?
        .checked_add(fee_tail)?;
    let debit_plus_reserve = actual_debit.checked_add(remaining_reserve)?;
    // Separate rounding may cost one atom. A negative difference is an added
    // requirement, not a value to silently clamp into a release.
    Ok(PartialFill {
        original_reserve,
        actual_debit,
        remaining_quantity: quantity,
        remaining_reserve,
        released_upper_bound: original_reserve.checked_sub(debit_plus_reserve)?,
        debit_plus_reserve,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReceivedFees {
    pub fees: [Fixed; 3],
    pub total: Fixed,
    pub net_received: Fixed,
    pub external_reserve: Fixed,
}
pub fn received_fees(received: Fixed, rates: [Fixed; 3], tail: Fixed) -> Result<ReceivedFees> {
    received.nonnegative()?;
    tail.nonnegative()?;
    received.ensure_same_scale(tail)?;
    let mut fees = [received.zero_like(); 3];
    for (slot, rate) in fees.iter_mut().zip(rates) {
        rate.nonnegative()?;
        *slot = received.mul_to(rate, received.scale(), Rounding::Ceil)?;
    }
    let total = sum(fees, received.scale())?.checked_add(tail)?;
    let net = received.checked_sub(total)?;
    Ok(ReceivedFees {
        fees,
        total,
        net_received: net.positive_part(),
        external_reserve: total.checked_sub(received)?.positive_part(),
    })
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ThirdAssetFee {
    pub quote_fee: Fixed,
    pub third_asset_estimate: Fixed,
    pub fallback_net_received: Fixed,
    pub exclusive_pool: Fixed,
    pub available_outside_pool: Fixed,
}
pub fn third_asset_fee(
    received_quote: Fixed,
    rates: [Fixed; 3],
    third_quote_low: Fixed,
    pool_balance: Fixed,
    rounding_tail: Fixed,
) -> Result<ThirdAssetFee> {
    third_quote_low.positive()?;
    pool_balance.nonnegative()?;
    rounding_tail.nonnegative()?;
    pool_balance.ensure_same_scale(rounding_tail)?;
    let quote = received_fees(received_quote, rates, received_quote.zero_like())?;
    let mut estimate = rounding_tail;
    for fee in quote.fees {
        estimate = estimate.checked_add(fee.div_to(
            third_quote_low,
            pool_balance.scale(),
            Rounding::Ceil,
        )?)?;
    }
    Ok(ThirdAssetFee {
        quote_fee: quote.total,
        third_asset_estimate: estimate,
        fallback_net_received: quote.net_received,
        exclusive_pool: pool_balance,
        available_outside_pool: pool_balance.checked_sub(pool_balance)?,
    })
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderCoverage {
    pub coverage: Fixed,
    pub actual_debit: Fixed,
    pub remaining_reserve: Fixed,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpotBaseline {
    pub free: Fixed,
    pub locked: Fixed,
    pub nontrade_locked: Fixed,
    pub net_cashflow: Fixed,
    pub proven_credits: Fixed,
    pub other_debits: Fixed,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Availability {
    pub total_assets: Fixed,
    pub local_free: Fixed,
    pub venue_cap: Fixed,
    pub available: Fixed,
}
pub fn available(baseline: SpotBaseline, orders: &[OrderCoverage]) -> Result<Availability> {
    for v in [
        baseline.free,
        baseline.locked,
        baseline.nontrade_locked,
        baseline.proven_credits,
        baseline.other_debits,
    ] {
        v.nonnegative()?;
    }
    let mut reserves = baseline.free.zero_like();
    let mut coverage = baseline.free.zero_like();
    let mut uncovered = baseline.free.zero_like();
    for order in orders {
        order.coverage.nonnegative()?;
        order.actual_debit.nonnegative()?;
        order.remaining_reserve.nonnegative()?;
        coverage = coverage.checked_add(order.coverage)?;
        reserves = reserves.checked_add(order.remaining_reserve)?;
        uncovered = uncovered.checked_add(
            order
                .actual_debit
                .checked_add(order.remaining_reserve)?
                .checked_sub(order.coverage)?
                .positive_part(),
        )?;
    }
    if coverage.checked_add(baseline.nontrade_locked)? != baseline.locked {
        return Err(RiskError::Unreconciled);
    }
    let total_assets = baseline
        .free
        .checked_add(baseline.locked)?
        .checked_add(baseline.net_cashflow)?;
    let local_free = total_assets
        .checked_sub(baseline.nontrade_locked)?
        .checked_sub(reserves)?;
    let venue_cap = baseline
        .free
        .checked_add(baseline.proven_credits)?
        .checked_sub(baseline.other_debits)?
        .checked_sub(uncovered)?;
    Ok(Availability {
        total_assets,
        local_free,
        venue_cap,
        available: local_free.min(venue_cap)?.positive_part(),
    })
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FeeRounding {
    pub aggregate_fee: Fixed,
    pub rounding_tail: Fixed,
    pub reserve: Fixed,
}
pub fn fee_rounding_tail(
    received: Fixed,
    rate: Fixed,
    maximum_fills: u64,
    fee_quantum: Fixed,
) -> Result<FeeRounding> {
    received.nonnegative()?;
    rate.nonnegative()?;
    fee_quantum.positive()?;
    let aggregate_fee = received.mul_to(rate, fee_quantum.scale(), Rounding::Ceil)?;
    let rounding_tail = fee_quantum.checked_mul_int(i128::from(maximum_fills))?;
    Ok(FeeRounding {
        aggregate_fee,
        rounding_tail,
        reserve: aggregate_fee.checked_add(rounding_tail)?,
    })
}
