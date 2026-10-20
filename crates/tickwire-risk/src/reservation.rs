use crate::{Fixed, Result, RiskError, Rounding, fixed::sum, futures::Side};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AssetId(pub u32);
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Money {
    pub asset: AssetId,
    pub value: Fixed,
}
impl Money {
    pub fn checked_add(self, rhs: Self) -> Result<Self> {
        if self.asset != rhs.asset {
            return Err(RiskError::AssetMismatch);
        }
        Ok(Self {
            asset: self.asset,
            value: self.value.checked_add(rhs.value)?,
        })
    }
}
/// Single-writer aggregate for one asset. Order identity, replay and journal
/// ownership belong to the executor; this type never releases on a timeout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReservationAccount {
    available: Money,
    reserved: Fixed,
    increase_open: bool,
}
impl ReservationAccount {
    pub fn new(available: Money) -> Result<Self> {
        available.value.nonnegative()?;
        Ok(Self {
            reserved: available.value.zero_like(),
            available,
            increase_open: true,
        })
    }
    pub const fn available(self) -> Money {
        self.available
    }
    pub const fn reserved(self) -> Fixed {
        self.reserved
    }
    pub const fn increase_open(self) -> bool {
        self.increase_open
    }
    pub fn reserve(&mut self, amount: Money) -> Result<()> {
        self.reserve_computed(|| Ok(amount))
    }
    /// Evaluate first and commit both amounts together. Overflow closes the
    /// increase gate while retaining every existing financial amount.
    pub fn reserve_computed(&mut self, calculate: impl FnOnce() -> Result<Money>) -> Result<()> {
        if !self.increase_open {
            return Err(RiskError::GateClosed);
        }
        let candidate = (|| {
            let amount = calculate()?;
            amount.value.nonnegative()?;
            if amount.asset != self.available.asset {
                return Err(RiskError::AssetMismatch);
            }
            self.available.value.ensure_same_scale(amount.value)?;
            if amount.value.atoms() > self.available.value.atoms() {
                return Err(RiskError::InsufficientAsset);
            }
            Ok((
                self.available.value.checked_sub(amount.value)?,
                self.reserved.checked_add(amount.value)?,
            ))
        })();
        match candidate {
            Ok((available, reserved)) => {
                self.available.value = available;
                self.reserved = reserved;
                Ok(())
            }
            Err(error) => {
                if error == RiskError::ArithmeticOverflow {
                    self.increase_open = false;
                }
                Err(error)
            }
        }
    }
    pub fn on_timeout(&self) -> Fixed {
        self.reserved.zero_like()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PotentialOrder {
    pub quantity: Fixed,
    pub buy_cap: Fixed,
}
pub fn potential_order_reserve(order: PotentialOrder, money_scale: u8) -> Result<Fixed> {
    order.quantity.nonnegative()?;
    order.buy_cap.positive()?;
    order
        .quantity
        .mul_to(order.buy_cap, money_scale, Rounding::Ceil)
}
pub fn potential_order_sum(orders: &[PotentialOrder], money_scale: u8) -> Result<Fixed> {
    orders
        .iter()
        .try_fold(Fixed::zero(money_scale)?, |total, &order| {
            total.checked_add(potential_order_reserve(order, money_scale)?)
        })
}
pub fn unknown_exposure_gate(has_unknown: bool) -> Result<()> {
    if has_unknown {
        Err(RiskError::UnknownExposure)
    } else {
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PositionSide {
    Both,
    Long,
    Short,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReduceAllocation {
    pub reducible_before: Fixed,
    pub allocated: Fixed,
    pub reducible_after: Fixed,
    pub side: Side,
    pub position_side: PositionSide,
    pub reduce_only: bool,
}
pub fn allocate_reduce(
    actual_position: Fixed,
    existing: &[Fixed],
    requested: Fixed,
    step: Fixed,
    allow_partial: bool,
    position_side: PositionSide,
) -> Result<ReduceAllocation> {
    requested.nonnegative()?;
    step.positive()?;
    for value in existing {
        value.nonnegative()?;
    }
    if position_side != PositionSide::Both && actual_position.atoms() < 0 {
        return Err(RiskError::InvalidInput);
    }
    let pending = sum(existing.iter().copied(), actual_position.scale())?;
    let actual = actual_position.checked_abs()?;
    if pending.atoms() > actual.atoms() {
        return Err(RiskError::Unreconciled);
    }
    let reducible_before = actual.checked_sub(pending)?;
    let quantity = requested
        .min(reducible_before)?
        .quantize(step, Rounding::Floor)?;
    if !allow_partial && quantity != requested {
        return Err(RiskError::InsufficientAsset);
    }
    let side = match position_side {
        PositionSide::Long => Side::Sell,
        PositionSide::Short => Side::Buy,
        PositionSide::Both => {
            if actual_position.atoms() >= 0 {
                Side::Sell
            } else {
                Side::Buy
            }
        }
    };
    Ok(ReduceAllocation {
        reducible_before,
        allocated: quantity,
        reducible_after: reducible_before.checked_sub(quantity)?,
        side,
        position_side,
        reduce_only: position_side == PositionSide::Both,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VenueShadow {
    pub available: Fixed,
}
impl VenueShadow {
    pub fn calculate(
        available0: Fixed,
        holds: &[Fixed],
        other_debits: Fixed,
        market_hold: Fixed,
    ) -> Result<Self> {
        available0.nonnegative()?;
        other_debits.nonnegative()?;
        market_hold.nonnegative()?;
        for hold in holds {
            hold.nonnegative()?;
        }
        Ok(Self {
            available: available0
                .checked_sub(sum(holds.iter().copied(), available0.scale())?)?
                .checked_sub(other_debits)?
                .checked_sub(market_hold)?
                .positive_part(),
        })
    }
    pub fn admit(self, capital_delta: Fixed) -> Result<()> {
        self.available.ensure_same_scale(capital_delta)?;
        if capital_delta.positive_part().atoms() <= self.available.atoms() {
            Ok(())
        } else {
            Err(RiskError::VenueAvailableCap)
        }
    }
}
