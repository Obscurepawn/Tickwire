use crate::{
    Fixed, Result, RiskError, Rounding,
    fixed::{product_to, sum},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OneWayEnvelope {
    pub min: Fixed,
    pub max: Fixed,
    pub max_abs: Fixed,
    pub capital_quantity: Fixed,
    pub all_filled: Fixed,
}
pub fn one_way_envelope(position: Fixed, buys: Fixed, sells: Fixed) -> Result<OneWayEnvelope> {
    buys.nonnegative()?;
    sells.nonnegative()?;
    let min = position.checked_sub(sells)?;
    let max = position.checked_add(buys)?;
    Ok(OneWayEnvelope {
        min,
        max,
        max_abs: min.checked_abs()?.max(max.checked_abs()?)?,
        capital_quantity: position
            .checked_abs()?
            .checked_add(buys)?
            .checked_add(sells)?,
        all_filled: position.checked_add(buys)?.checked_sub(sells)?,
    })
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HedgePosition {
    pub long: Fixed,
    pub short: Fixed,
    pub open_long: Fixed,
    pub open_short: Fixed,
    pub close_long: Fixed,
    pub close_short: Fixed,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HedgeEnvelope {
    pub long_max: Fixed,
    pub short_max: Fixed,
    pub net_min: Fixed,
    pub net_max: Fixed,
    pub capital_quantity: Fixed,
    pub exit_quantity: Fixed,
}
pub fn hedge_envelope(p: HedgePosition) -> Result<HedgeEnvelope> {
    let quantities = [
        p.long,
        p.short,
        p.open_long,
        p.open_short,
        p.close_long,
        p.close_short,
    ];
    for q in quantities {
        q.nonnegative()?;
    }
    let long_max = p.long.checked_add(p.open_long)?;
    let short_max = p.short.checked_add(p.open_short)?;
    Ok(HedgeEnvelope {
        long_max,
        short_max,
        net_min: p
            .long
            .checked_sub(p.close_long)?
            .positive_part()
            .checked_sub(short_max)?,
        net_max: long_max.checked_sub(p.short.checked_sub(p.close_short)?.positive_part())?,
        capital_quantity: sum(quantities, p.long.scale())?,
        exit_quantity: long_max.checked_add(short_max)?,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BracketInterpretation {
    EffectiveAsReturned,
    Unverified,
}
pub fn qualify_brackets(coefficient: Fixed, interpretation: BracketInterpretation) -> Result<()> {
    coefficient.positive()?;
    if interpretation == BracketInterpretation::Unverified {
        return Err(RiskError::BracketSemanticsUnverified);
    }
    Ok(())
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Bracket {
    pub floor: Fixed,
    pub cap: Fixed,
    pub rate: Fixed,
    pub cumulative: Fixed,
    pub max_initial_leverage: Fixed,
}
pub struct QualifiedBrackets<'a> {
    brackets: &'a [Bracket],
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Maintenance {
    pub tier: usize,
    pub tier_maintenance: Fixed,
    pub upper_maintenance: Fixed,
    pub max_initial_leverage: Fixed,
}
impl<'a> QualifiedBrackets<'a> {
    pub fn new(
        brackets: &'a [Bracket],
        coefficient: Fixed,
        interpretation: BracketInterpretation,
    ) -> Result<Self> {
        qualify_brackets(coefficient, interpretation)?;
        if brackets.is_empty()
            || brackets[0].floor.atoms() != 0
            || brackets[0].cumulative.atoms() != 0
        {
            return Err(RiskError::InvalidBrackets);
        }
        for (j, b) in brackets.iter().enumerate() {
            b.floor.nonnegative()?;
            b.cap.positive()?;
            b.rate.nonnegative()?;
            b.cumulative.nonnegative()?;
            b.floor.ensure_same_scale(b.cap)?;
            b.floor.ensure_same_scale(b.cumulative)?;
            if b.cap.atoms() <= b.floor.atoms()
                || b.max_initial_leverage.atoms()
                    < Fixed::one(b.max_initial_leverage.scale())?.atoms()
            {
                return Err(RiskError::InvalidBrackets);
            }
            if j > 0 {
                let prior = brackets[j - 1];
                prior.rate.ensure_same_scale(b.rate)?;
                if prior.cap != b.floor || prior.rate.atoms() > b.rate.atoms() {
                    return Err(RiskError::InvalidBrackets);
                }
                let difference = b.rate.checked_sub(prior.rate)?;
                let low = b
                    .floor
                    .mul_to(difference, b.cumulative.scale(), Rounding::Floor)?;
                let high = b
                    .floor
                    .mul_to(difference, b.cumulative.scale(), Rounding::Ceil)?;
                // A published cumulative amount must represent exact continuity in
                // this scale; qualification must choose a sufficient precision.
                if low != high || prior.cumulative.checked_add(low)? != b.cumulative {
                    return Err(RiskError::InvalidBrackets);
                }
            }
        }
        Ok(Self { brackets })
    }
    pub fn maintenance(&self, notional: Fixed) -> Result<Maintenance> {
        notional.nonnegative()?;
        let mut rate = self.brackets[0].rate;
        for (j, bracket) in self.brackets.iter().enumerate() {
            notional.ensure_same_scale(bracket.floor)?;
            rate = rate.max(bracket.rate)?;
            if notional.atoms() >= bracket.floor.atoms()
                && (notional.atoms() < bracket.cap.atoms()
                    || j == self.brackets.len() - 1 && notional.atoms() == bracket.cap.atoms())
            {
                let tier_maintenance = notional
                    .mul_to(bracket.rate, notional.scale(), Rounding::Ceil)?
                    .checked_sub(bracket.cumulative)?
                    .positive_part();
                return Ok(Maintenance {
                    tier: j + 1,
                    tier_maintenance,
                    upper_maintenance: notional.mul_to(rate, notional.scale(), Rounding::Ceil)?,
                    max_initial_leverage: bracket.max_initial_leverage,
                });
            }
        }
        Err(RiskError::InvalidBrackets)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CollateralAsset {
    pub balance: Fixed,
    pub valuation_low: Fixed,
    pub valuation_high: Fixed,
    pub eligible_ratio: Fixed,
    pub eligible: bool,
}
pub fn collateral_component(asset: CollateralAsset, money_scale: u8) -> Result<Fixed> {
    if asset.balance.atoms() < 0 {
        asset.valuation_high.positive()?;
        return asset
            .balance
            .checked_abs()?
            .mul_to(asset.valuation_high, money_scale, Rounding::Ceil)?
            .checked_neg();
    }
    if !asset.eligible {
        return Fixed::zero(money_scale);
    }
    asset.valuation_low.positive()?;
    asset.valuation_high.positive()?;
    asset
        .valuation_low
        .ensure_same_scale(asset.valuation_high)?;
    if asset.valuation_low.atoms() > asset.valuation_high.atoms() {
        return Err(RiskError::InvalidInput);
    }
    asset.eligible_ratio.nonnegative()?;
    if asset.eligible_ratio.atoms() > Fixed::one(asset.eligible_ratio.scale())?.atoms() {
        return Err(RiskError::InvalidInput);
    }
    product_to(
        [asset.balance, asset.valuation_low, asset.eligible_ratio],
        money_scale,
        Rounding::Floor,
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StressPrices {
    pub low: Fixed,
    pub high: Fixed,
}
impl StressPrices {
    pub fn validate(self) -> Result<()> {
        self.low.positive()?;
        self.high.positive()?;
        self.low.ensure_same_scale(self.high)?;
        if self.low.atoms() > self.high.atoms() {
            return Err(RiskError::InvalidInput);
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PositionLoss {
    pub absolute_quantity: Fixed,
    pub stress_price: Fixed,
    pub loss: Fixed,
}
pub fn position_loss(
    quantity: Fixed,
    entry: Fixed,
    prices: StressPrices,
    money_scale: u8,
) -> Result<PositionLoss> {
    prices.validate()?;
    entry.positive()?;
    let stress_price = if quantity.atoms() >= 0 {
        prices.low
    } else {
        prices.high
    };
    let difference = if quantity.atoms() >= 0 {
        entry.checked_sub(stress_price)?
    } else {
        stress_price.checked_sub(entry)?
    }
    .positive_part();
    let absolute_quantity = quantity.checked_abs()?;
    Ok(PositionLoss {
        absolute_quantity,
        stress_price,
        loss: absolute_quantity.mul_to(difference, money_scale, Rounding::Ceil)?,
    })
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Buy,
    Sell,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PendingOrder {
    pub side: Side,
    pub quantity: Fixed,
    pub price_bound: Fixed,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CapitalParameters {
    pub prices: StressPrices,
    pub effective_leverage: Fixed,
    pub maintenance_rate_upper: Fixed,
    pub maintenance_addon: Fixed,
    pub execution_fee_rate: Fixed,
    pub exit_fee_rate: Fixed,
    pub funding_rate_abs_upper: Fixed,
    pub funding_settlements: u64,
    pub pending_outbound: Fixed,
    pub capital_buffer: Fixed,
    pub rounding_tails: Fixed,
    pub collateral_value: Fixed,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Capital {
    pub capital_quantity: Fixed,
    pub capital_notional: Fixed,
    pub initial_charge: Fixed,
    pub maintenance_upper: Fixed,
    pub position_loss: Fixed,
    pub order_loss: Fixed,
    pub execution_fee: Fixed,
    pub exit_fee: Fixed,
    pub funding_reserve: Fixed,
    pub capital: Fixed,
    pub local_headroom: Fixed,
    pub exit_quantity: Fixed,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FundingMode {
    None,
    Periodic {
        rate_abs_upper: Fixed,
        settlements: u64,
    },
}
pub fn funding_reserve(capital_notional: Fixed, mode: FundingMode) -> Result<Fixed> {
    capital_notional.nonnegative()?;
    match mode {
        FundingMode::None => Ok(capital_notional.zero_like()),
        FundingMode::Periodic {
            rate_abs_upper,
            settlements,
        } => {
            rate_abs_upper.nonnegative()?;
            product_to(
                [
                    capital_notional,
                    rate_abs_upper,
                    Fixed::from_atoms(i128::from(settlements), 0)?,
                ],
                capital_notional.scale(),
                Rounding::Ceil,
            )
        }
    }
}
impl Capital {
    pub fn admission(self) -> Result<()> {
        if self.local_headroom.atoms() < 0 {
            Err(RiskError::InsufficientCollateral)
        } else {
            Ok(())
        }
    }
}
pub fn effective_leverage(configured: Fixed, venue: Fixed, tier: Fixed) -> Result<Fixed> {
    let value = configured.min(venue)?.min(tier)?;
    if value.atoms() < Fixed::one(value.scale())?.atoms() {
        return Err(RiskError::InvalidInput);
    }
    Ok(value)
}
pub fn one_way_capital(
    position: Fixed,
    entry: Fixed,
    orders: &[PendingOrder],
    p: CapitalParameters,
) -> Result<Capital> {
    p.prices.validate()?;
    let mut buys = position.zero_like();
    let mut sells = position.zero_like();
    for order in orders {
        order.quantity.nonnegative()?;
        order.price_bound.positive()?;
        match order.side {
            Side::Buy => buys = buys.checked_add(order.quantity)?,
            Side::Sell => sells = sells.checked_add(order.quantity)?,
        }
    }
    let envelope = one_way_envelope(position, buys, sells)?;
    let loss = position_loss(position, entry, p.prices, p.collateral_value.scale())?.loss;
    capital_components(envelope.capital_quantity, envelope.max_abs, loss, orders, p)
}
/// Hedge orders are classified by their effect before admission; the envelope
/// and the execution-order set must include the same pending quantities.
pub fn hedge_capital(
    position: HedgePosition,
    long_entry: Fixed,
    short_entry: Fixed,
    orders: &[PendingOrder],
    p: CapitalParameters,
) -> Result<Capital> {
    let envelope = hedge_envelope(position)?;
    let mut buys = position.long.zero_like();
    let mut sells = position.long.zero_like();
    for order in orders {
        order.quantity.nonnegative()?;
        match order.side {
            Side::Buy => buys = buys.checked_add(order.quantity)?,
            Side::Sell => sells = sells.checked_add(order.quantity)?,
        }
    }
    if buys != position.open_long.checked_add(position.close_short)?
        || sells != position.open_short.checked_add(position.close_long)?
    {
        return Err(RiskError::InvalidInput);
    }
    let loss = position_loss(
        position.long,
        long_entry,
        p.prices,
        p.collateral_value.scale(),
    )?
    .loss
    .checked_add(
        position_loss(
            position.short.checked_neg()?,
            short_entry,
            p.prices,
            p.collateral_value.scale(),
        )?
        .loss,
    )?;
    capital_components(
        envelope.capital_quantity,
        envelope.exit_quantity,
        loss,
        orders,
        p,
    )
}
fn capital_components(
    capital_quantity: Fixed,
    exit_quantity: Fixed,
    position_loss: Fixed,
    orders: &[PendingOrder],
    p: CapitalParameters,
) -> Result<Capital> {
    p.prices.validate()?;
    let scale = p.collateral_value.scale();
    if p.effective_leverage.atoms() < Fixed::one(p.effective_leverage.scale())?.atoms() {
        return Err(RiskError::InvalidInput);
    }
    for value in [
        p.maintenance_rate_upper,
        p.maintenance_addon,
        p.execution_fee_rate,
        p.exit_fee_rate,
        p.funding_rate_abs_upper,
        p.pending_outbound,
        p.capital_buffer,
        p.rounding_tails,
    ] {
        value.nonnegative()?;
    }
    let capital_notional = capital_quantity.mul_to(p.prices.high, scale, Rounding::Ceil)?;
    let initial_charge = capital_notional.div_to(p.effective_leverage, scale, Rounding::Ceil)?;
    let maintenance_upper =
        capital_notional.mul_to(p.maintenance_rate_upper, scale, Rounding::Ceil)?;
    let mut order_loss = Fixed::zero(scale)?;
    let mut execution_fee = Fixed::zero(scale)?;
    for order in orders {
        order.quantity.nonnegative()?;
        order.price_bound.positive()?;
        let (difference, fee_price) = match order.side {
            Side::Buy => (
                order.price_bound.checked_sub(p.prices.low)?.positive_part(),
                order.price_bound,
            ),
            Side::Sell => (
                p.prices
                    .high
                    .checked_sub(order.price_bound)?
                    .positive_part(),
                p.prices.high.max(order.price_bound)?,
            ),
        };
        order_loss =
            order_loss.checked_add(order.quantity.mul_to(difference, scale, Rounding::Ceil)?)?;
        let order_notional = order.quantity.mul_to(fee_price, scale, Rounding::Ceil)?;
        execution_fee = execution_fee.checked_add(order_notional.mul_to(
            p.execution_fee_rate,
            scale,
            Rounding::Ceil,
        )?)?;
    }
    let exit_fee = product_to(
        [exit_quantity, p.prices.high, p.exit_fee_rate],
        scale,
        Rounding::Ceil,
    )?;
    let funding_reserve = funding_reserve(
        capital_notional,
        FundingMode::Periodic {
            rate_abs_upper: p.funding_rate_abs_upper,
            settlements: p.funding_settlements,
        },
    )?;
    let base = initial_charge.max(maintenance_upper.checked_add(p.maintenance_addon)?)?;
    let capital = sum(
        [
            base,
            position_loss,
            order_loss,
            execution_fee,
            exit_fee,
            funding_reserve,
            p.pending_outbound,
            p.capital_buffer,
            p.rounding_tails,
        ],
        scale,
    )?;
    Ok(Capital {
        capital_quantity,
        capital_notional,
        initial_charge,
        maintenance_upper,
        position_loss,
        order_loss,
        execution_fee,
        exit_fee,
        funding_reserve,
        capital,
        local_headroom: p.collateral_value.checked_sub(capital)?,
        exit_quantity,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RiskClass {
    AccountIncreasing,
    AccountNonincreasing,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AccountNetting {
    pub position_before: Fixed,
    pub gross_notional: Fixed,
    pub net_notional_before: Fixed,
    pub position_after: Fixed,
    pub net_notional_after: Fixed,
    pub risk_class: RiskClass,
}
pub fn account_netting(
    positions: &[Fixed],
    signed_change: Fixed,
    price: Fixed,
    money_scale: u8,
) -> Result<AccountNetting> {
    price.positive()?;
    let mut gross = signed_change.zero_like();
    let position_before = sum(positions.iter().copied(), signed_change.scale())?;
    for position in positions {
        gross = gross.checked_add(position.checked_abs()?)?;
    }
    let gross_notional = gross.mul_to(price, money_scale, Rounding::Ceil)?;
    let position_after = position_before.checked_add(signed_change)?;
    let net_notional_before =
        position_before
            .checked_abs()?
            .mul_to(price, money_scale, Rounding::Ceil)?;
    let net_notional_after =
        position_after
            .checked_abs()?
            .mul_to(price, money_scale, Rounding::Ceil)?;
    let risk_class = if net_notional_after.atoms() > net_notional_before.atoms() {
        RiskClass::AccountIncreasing
    } else {
        RiskClass::AccountNonincreasing
    };
    Ok(AccountNetting {
        position_before,
        gross_notional,
        net_notional_before,
        position_after,
        net_notional_after,
        risk_class,
    })
}
