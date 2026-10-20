use crate::{Fixed, Result, RiskError, Rounding, fixed::product_to};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SettlementState {
    Active,
    AwaitingSettlement,
    SettledReconciled,
}
impl SettlementState {
    pub const fn name(self) -> &'static str {
        match self {
            Self::Active => "ACTIVE",
            Self::AwaitingSettlement => "AWAITING_SETTLEMENT",
            Self::SettledReconciled => "SETTLED_RECONCILED",
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IncomeKind {
    SettlementPnl,
    SettlementFee,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Income {
    pub id: u128,
    pub kind: IncomeKind,
    pub amount: Fixed,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReferenceSettlement {
    pub pnl: Fixed,
    pub fee: Fixed,
    pub net: Fixed,
}
pub fn reference_settlement(
    quantity: Fixed,
    entry: Fixed,
    actual_price: Fixed,
    rate: Fixed,
    money_scale: u8,
) -> Result<ReferenceSettlement> {
    entry.positive()?;
    actual_price.positive()?;
    rate.nonnegative()?;
    let pnl = quantity.mul_to(
        actual_price.checked_sub(entry)?,
        money_scale,
        Rounding::Floor,
    )?;
    let fee = product_to(
        [quantity.checked_abs()?, actual_price, rate],
        money_scale,
        Rounding::Ceil,
    )?;
    Ok(ReferenceSettlement {
        pnl,
        fee,
        net: pnl.checked_sub(fee)?,
    })
}
pub fn settlement_fee_reserve(
    exit_quantity: Fixed,
    high_price: Fixed,
    exit_rate: Fixed,
    settlement_rate: Fixed,
    rounding_buffer: Fixed,
) -> Result<Fixed> {
    exit_quantity.nonnegative()?;
    high_price.positive()?;
    exit_rate.nonnegative()?;
    settlement_rate.nonnegative()?;
    rounding_buffer.nonnegative()?;
    product_to(
        [exit_quantity, high_price, exit_rate.max(settlement_rate)?],
        rounding_buffer.scale(),
        Rounding::Ceil,
    )?
    .checked_add(rounding_buffer)
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReconcileFacts {
    pub complete_account_facts: bool,
    pub unresolved_orders: usize,
    pub actual_position: Fixed,
    pub observed_wallet: Fixed,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SettlementReport {
    pub actual_net_income: Fixed,
    pub expected_wallet: Fixed,
    pub wallet_difference: Fixed,
    pub reference_difference: Fixed,
    pub actual_fee: Fixed,
}
/// A bounded settlement scope. Income IDs must already be canonical account /
/// contract / settlement IDs; no allocation is performed as records arrive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Settlement<const N: usize> {
    state: SettlementState,
    position: Fixed,
    wallet_before: Fixed,
    fee_reserve: Fixed,
    records: [Option<Income>; N],
    net_income: Fixed,
    actual_fee: Fixed,
    unresolved_evidence: bool,
}
impl<const N: usize> Settlement<N> {
    pub fn new(position: Fixed, wallet_before: Fixed, fee_reserve: Fixed) -> Result<Self> {
        fee_reserve.nonnegative()?;
        wallet_before.ensure_same_scale(fee_reserve)?;
        Ok(Self {
            state: SettlementState::Active,
            position,
            wallet_before,
            fee_reserve,
            records: [None; N],
            net_income: wallet_before.zero_like(),
            actual_fee: wallet_before.zero_like(),
            unresolved_evidence: false,
        })
    }
    pub const fn state(&self) -> SettlementState {
        self.state
    }
    pub const fn position(&self) -> Fixed {
        self.position
    }
    pub const fn fee_reserve(&self) -> Fixed {
        self.fee_reserve
    }
    pub fn on_expiry_timer(&mut self) -> Fixed {
        if self.state == SettlementState::Active {
            self.state = SettlementState::AwaitingSettlement;
        }
        self.fee_reserve.zero_like()
    }
    pub fn record_income(&mut self, income: Income) -> Result<bool> {
        let candidate = (|| {
            if let Some(prior) = self
                .records
                .iter()
                .flatten()
                .find(|prior| prior.id == income.id)
            {
                return if *prior == income {
                    Ok(None)
                } else {
                    Err(RiskError::DuplicateConflict)
                };
            }
            let index = self
                .records
                .iter()
                .position(Option::is_none)
                .ok_or(RiskError::Unreconciled)?;
            self.wallet_before.ensure_same_scale(income.amount)?;
            if income.kind == IncomeKind::SettlementFee && income.amount.atoms() > 0 {
                return Err(RiskError::InvalidInput);
            }
            let net = self.net_income.checked_add(income.amount)?;
            let fee = if income.kind == IncomeKind::SettlementFee {
                self.actual_fee.checked_sub(income.amount)?
            } else {
                self.actual_fee
            };
            Ok(Some((index, net, fee)))
        })();
        match candidate {
            Ok(None) => Ok(false),
            Ok(Some((index, net, fee))) => {
                if self.state == SettlementState::SettledReconciled {
                    self.state = SettlementState::AwaitingSettlement;
                }
                self.records[index] = Some(income);
                self.net_income = net;
                self.actual_fee = fee;
                Ok(true)
            }
            Err(error) => {
                // Failed financial mutation is atomic, but newly observed
                // evidence invalidates any previous completion proof.
                self.unresolved_evidence = true;
                self.state = SettlementState::AwaitingSettlement;
                Err(error)
            }
        }
    }
    pub fn report(
        &self,
        reference: ReferenceSettlement,
        observed_wallet: Fixed,
    ) -> Result<SettlementReport> {
        let expected_wallet = self.wallet_before.checked_add(self.net_income)?;
        Ok(SettlementReport {
            actual_net_income: self.net_income,
            expected_wallet,
            wallet_difference: observed_wallet.checked_sub(expected_wallet)?,
            reference_difference: self.net_income.checked_sub(reference.net)?,
            actual_fee: self.actual_fee,
        })
    }
    pub fn reconcile(
        &mut self,
        reference: ReferenceSettlement,
        facts: ReconcileFacts,
    ) -> Result<Fixed> {
        if self.state != SettlementState::AwaitingSettlement
            || self.unresolved_evidence
            || !facts.complete_account_facts
            || facts.unresolved_orders != 0
            || facts.actual_position.atoms() != 0
        {
            return Err(RiskError::Unreconciled);
        }
        self.position.ensure_same_scale(facts.actual_position)?;
        let report = self.report(reference, facts.observed_wallet)?;
        if report.wallet_difference.atoms() != 0 || report.reference_difference.atoms() != 0 {
            return Err(RiskError::Unreconciled);
        }
        let released = self
            .fee_reserve
            .checked_sub(self.actual_fee)?
            .positive_part();
        self.position = facts.actual_position;
        self.fee_reserve = self.fee_reserve.zero_like();
        self.state = SettlementState::SettledReconciled;
        Ok(released)
    }
}
