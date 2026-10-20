use super::*;
use tickwire_risk::{futures as f, spot};

impl Account {
    pub(super) fn validate_policy(&self, p: &InstrumentPolicy) -> Result<()> {
        if p.wallet_id == 0
            || p.instrument.instrument_id == 0
            || p.fees.maximum_fills == 0
            || p.strategy_limits.is_empty()
            || p.strategy_limits.len() > MAX_STRATEGIES
        {
            return Err(Error::Catalog);
        }
        let qs = asset_scale(&p.instrument.base)?;
        p.max_absolute_position.positive()?;
        p.max_gross_quantity.positive()?;
        if p.max_absolute_position.scale() != qs || p.max_gross_quantity.scale() != qs {
            return Err(Error::Catalog);
        }
        let mut strategies = BTreeMap::new();
        for &(id, limit) in &p.strategy_limits {
            limit.nonnegative()?;
            if id == 0 || limit.scale() != qs || strategies.insert(id, limit).is_some() {
                return Err(Error::Catalog);
            }
        }
        for rate in p.fees.rates {
            rate.nonnegative()?;
        }
        if let Some(key) = p.fees.exclusive_pool
            && (key.wallet_id != p.wallet_id
                || !self
                    .assets
                    .get(&key)
                    .is_some_and(|a| a.baseline.exclusive_fee_pool))
        {
            return Err(Error::Catalog);
        }
        for other in self.catalog.values().filter(|x| x.wallet_id == p.wallet_id) {
            if (other.product == v::Product::Spot) != (p.product == v::Product::Spot) {
                return Err(Error::Catalog);
            }
        }
        let refs = match p.product {
            v::Product::Spot if p.futures.is_none() => {
                vec![&p.instrument.base, &p.instrument.quote]
            }
            v::Product::LinearPerpetual | v::Product::LinearFuture if p.futures.is_some() => {
                vec![&p.instrument.settlement]
            }
            _ => return Err(Error::Unsupported),
        };
        for asset in refs {
            if !self
                .assets
                .get(&AssetKey {
                    wallet_id: p.wallet_id,
                    asset_id: asset.id,
                })
                .is_some_and(|a| a.baseline.asset == *asset)
            {
                return Err(Error::Catalog);
            }
        }
        if let Some(fp) = &p.futures {
            if fp.brackets.len() > MAX_BRACKETS {
                return Err(Error::Capacity);
            }
            let ss = asset_scale(&p.instrument.settlement)?;
            let available = self
                .venue_available
                .get(&p.wallet_id)
                .ok_or(Error::InvalidSnapshot)?;
            if available.scale() != ss {
                return Err(Error::Catalog);
            }
            f::StressPrices {
                low: fp.stress_low,
                high: fp.stress_high,
            }
            .validate()?;
            let _ = f::QualifiedBrackets::new(
                &fp.brackets,
                Fixed::one(0)?,
                f::BracketInterpretation::EffectiveAsReturned,
            )?;
            if p.product == v::Product::LinearFuture && fp.funding_settlements != 0 {
                return Err(Error::Catalog);
            }
            for other in self.catalog.values().filter(|x| x.wallet_id == p.wallet_id) {
                if other.instrument.settlement != p.instrument.settlement
                    || other
                        .futures
                        .as_ref()
                        .is_none_or(|x| x.margin_mode != fp.margin_mode)
                    || fp.margin_mode == v::MarginMode::Isolated
                {
                    return Err(Error::Catalog);
                }
            }
        }
        Ok(())
    }
    pub(super) fn fee_rate(&self, p: &InstrumentPolicy) -> Result<Fixed> {
        let mut sum = Fixed::zero(18)?;
        for r in p.fees.rates {
            sum = sum.checked_add(r.rescale(18, Rounding::Ceil)?)?;
        }
        Ok(sum)
    }
    pub(super) fn fee_total(&self, base: Fixed, p: &InstrumentPolicy) -> Result<Fixed> {
        let mut fee = base.zero_like();
        for r in p.fees.rates {
            fee = fee.checked_add(base.mul_to(r, base.scale(), Rounding::Ceil)?)?;
        }
        Ok(fee)
    }
    pub(super) fn tail(&self, p: &InstrumentPolicy, scale: u8) -> Result<Fixed> {
        let items = p.fees.rates.iter().filter(|r| r.atoms() != 0).count() as i128;
        Ok(Fixed::from_atoms(
            i128::from(p.fees.maximum_fills) * items,
            scale,
        )?)
    }
    pub(super) fn fee_asset(&self, p: &InstrumentPolicy, o: &Order) -> AssetKey {
        let asset = if p.futures.is_some() {
            p.instrument.settlement.id
        } else if o.spec.side == v::Side::Buy {
            p.instrument.base.id
        } else {
            p.instrument.quote.id
        };
        AssetKey {
            wallet_id: p.wallet_id,
            asset_id: asset,
        }
    }
    pub(super) fn order_resources(&self, o: &Order) -> Result<BTreeMap<AssetKey, Fixed>> {
        let p = &self.catalog[&o.instrument];
        let remaining = o.remaining()?;
        let mut out = BTreeMap::new();
        if o.aborted {
            return Ok(out);
        }
        if p.futures.is_none() {
            let principal = if o.spec.side == v::Side::Buy {
                AssetAmount {
                    key: AssetKey {
                        wallet_id: p.wallet_id,
                        asset_id: p.instrument.quote.id,
                    },
                    amount: remaining.mul_to(
                        o.price,
                        asset_scale(&p.instrument.quote)?,
                        Rounding::Ceil,
                    )?,
                }
            } else {
                AssetAmount {
                    key: AssetKey {
                        wallet_id: p.wallet_id,
                        asset_id: p.instrument.base.id,
                    },
                    amount: remaining,
                }
            };
            add(&mut out, principal.key, principal.amount)?;
        }
        if !o.fees_closed {
            let key = self.fee_asset(p, o);
            let scale = asset_scale(&self.assets[&key].baseline.asset)?;
            let paid = o.paid.get(&key).copied().unwrap_or(Fixed::zero(scale)?);
            let fee = if let Some(futures) = &p.futures {
                let high = futures.stress_high.max(o.price)?;
                let future = remaining.mul_to(high, scale, Rounding::Ceil)?;
                self.fee_total(o.notional, p)?
                    .checked_add(self.fee_total(future, p)?)?
                    .checked_add(self.tail(p, scale)?)?
                    .checked_sub(paid)?
                    .positive_part()
            } else {
                let future = if o.spec.side == v::Side::Buy {
                    remaining
                } else {
                    remaining.mul_to(o.price, scale, Rounding::Floor)?
                };
                // Reported past receipts can fund their own fees only after the receipt has arrived;
                // the corresponding still-unreported commission remains a reservation.
                let past = self
                    .fee_total(o.received, p)?
                    .checked_add(self.tail(p, scale)?)?
                    .checked_sub(paid)?
                    .positive_part();
                let external = self
                    .fee_total(future, p)?
                    .checked_sub(future)?
                    .positive_part();
                past.checked_add(external)?
            };
            add(&mut out, key, fee)?;
        }
        out.retain(|_, n| n.atoms() != 0);
        Ok(out)
    }
    pub(super) fn resources(&self) -> Result<BTreeMap<AssetKey, Fixed>> {
        let mut out = BTreeMap::new();
        for o in self.orders.values() {
            for (key, n) in self.order_resources(o)? {
                add(&mut out, key, n)?;
            }
        }
        for (&key, a) in &self.assets {
            if a.baseline.exclusive_fee_pool {
                let pool = a
                    .baseline
                    .free
                    .checked_add(a.baseline.locked)?
                    .checked_add(a.cashflow)?
                    .checked_sub(a.baseline.nontrade_locked)?
                    .positive_part();
                // The entire pool is already exclusive; estimates are not added again.
                let existing = out.get(&key).copied().unwrap_or(pool.zero_like());
                out.insert(key, pool.max(existing)?);
            }
        }
        Ok(out)
    }
    pub fn asset(&self, key: AssetKey) -> Result<AssetView> {
        let a = self.assets.get(&key).ok_or(Error::Identity)?;
        let reserved = self
            .resources()?
            .get(&key)
            .copied()
            .unwrap_or(a.baseline.free.zero_like());
        let zero = a.baseline.free.zero_like();
        let calculated = spot::available(
            spot::SpotBaseline {
                free: a.baseline.free,
                locked: a.baseline.locked,
                nontrade_locked: a.baseline.nontrade_locked,
                net_cashflow: a.cashflow,
                proven_credits: a.credits,
                other_debits: a.other_debits,
            },
            &[spot::OrderCoverage {
                coverage: zero,
                actual_debit: zero,
                remaining_reserve: reserved,
            }],
        )?;
        let deficit = calculated
            .local_free
            .min(calculated.venue_cap)?
            .min(zero)?
            .checked_neg()?;
        Ok(AssetView {
            total: calculated.total_assets,
            reserved,
            available: calculated.available,
            deficit,
            ownership: a.ownership.clone(),
        })
    }
    pub(super) fn position_quantity(
        &self,
        p: &InstrumentPolicy,
        side: v::PositionSide,
    ) -> Result<Fixed> {
        Ok(self
            .positions
            .get(&(p.instrument.instrument_id, side_code(side)))
            .map(|x| x.quantity)
            .unwrap_or(Fixed::zero(asset_scale(&p.instrument.base)?)?))
    }
    pub(super) fn calculate_capital(&self) -> Result<BTreeMap<u64, (u64, Fixed)>> {
        let mut result: BTreeMap<u64, (u64, Fixed)> = BTreeMap::new();
        for p in self.catalog.values().filter(|p| p.futures.is_some()) {
            let fp = p.futures.as_ref().unwrap();
            let scale = asset_scale(&p.instrument.settlement)?;
            let zero = Fixed::zero(scale)?;
            let qzero = Fixed::zero(asset_scale(&p.instrument.base)?)?;
            let mut orders = Vec::new();
            let mut open_long = qzero;
            let mut open_short = qzero;
            let mut close_long = qzero;
            let mut close_short = qzero;
            let mut past_fees = zero;
            for o in self
                .orders
                .values()
                .filter(|o| o.instrument == p.instrument.instrument_id && !o.aborted)
            {
                let quantity = o.remaining()?;
                orders.push(f::PendingOrder {
                    side: if o.spec.side == v::Side::Buy {
                        f::Side::Buy
                    } else {
                        f::Side::Sell
                    },
                    quantity,
                    price_bound: o.price,
                });
                match (o.spec.position_side, o.spec.side) {
                    (v::PositionSide::Long, v::Side::Buy) => {
                        open_long = open_long.checked_add(quantity)?
                    }
                    (v::PositionSide::Long, v::Side::Sell) => {
                        close_long = close_long.checked_add(quantity)?
                    }
                    (v::PositionSide::Short, v::Side::Sell) => {
                        open_short = open_short.checked_add(quantity)?
                    }
                    (v::PositionSide::Short, v::Side::Buy) => {
                        close_short = close_short.checked_add(quantity)?
                    }
                    _ => {}
                }
                if !o.fees_closed {
                    let paid = o.paid.get(&self.fee_asset(p, o)).copied().unwrap_or(zero);
                    past_fees = past_fees.checked_add(
                        self.fee_total(o.notional, p)?
                            .checked_add(self.tail(p, scale)?)?
                            .checked_sub(paid)?
                            .positive_part(),
                    )?;
                }
            }
            let one = self.position_quantity(p, v::PositionSide::Both)?;
            let long = self.position_quantity(p, v::PositionSide::Long)?;
            let short = self.position_quantity(p, v::PositionSide::Short)?;
            let pending = orders
                .iter()
                .try_fold(qzero, |s, o| s.checked_add(o.quantity))?;
            let actual = if fp.position_mode == v::PositionMode::OneWay {
                one.checked_abs()?
            } else {
                long.checked_add(short)?
            };
            let notional =
                actual
                    .checked_add(pending)?
                    .mul_to(fp.stress_high, scale, Rounding::Ceil)?;
            let brackets = f::QualifiedBrackets::new(
                &fp.brackets,
                Fixed::one(0)?,
                f::BracketInterpretation::EffectiveAsReturned,
            )?;
            let tier = brackets.maintenance(notional)?;
            let rate = fp.brackets[..tier.tier]
                .iter()
                .map(|b| b.rate)
                .try_fold(fp.brackets[0].rate, |r, n| r.max(n))?;
            let parameters = f::CapitalParameters {
                prices: f::StressPrices {
                    low: fp.stress_low,
                    high: fp.stress_high,
                },
                effective_leverage: f::effective_leverage(
                    fp.configured_leverage,
                    fp.venue_leverage,
                    tier.max_initial_leverage,
                )?,
                maintenance_rate_upper: rate,
                maintenance_addon: fp.maintenance_addon,
                execution_fee_rate: self.fee_rate(p)?,
                exit_fee_rate: fp.exit_fee_rate,
                funding_rate_abs_upper: fp.funding_rate_abs_upper,
                funding_settlements: fp.funding_settlements,
                pending_outbound: fp.pending_outbound,
                capital_buffer: fp.capital_buffer,
                rounding_tails: past_fees,
                collateral_value: zero,
            };
            let entry = |side| {
                self.positions
                    .get(&(p.instrument.instrument_id, side_code(side)))
                    .map(|x| x.entry)
                    .unwrap_or(fp.stress_high)
            };
            let capital = if fp.position_mode == v::PositionMode::OneWay {
                f::one_way_capital(one, entry(v::PositionSide::Both), &orders, parameters)?
            } else {
                f::hedge_capital(
                    f::HedgePosition {
                        long,
                        short,
                        open_long,
                        open_short,
                        close_long,
                        close_short,
                    },
                    entry(v::PositionSide::Long),
                    entry(v::PositionSide::Short),
                    &orders,
                    parameters,
                )?
            };
            let (_, old) = result
                .entry(p.wallet_id)
                .or_insert((p.instrument.settlement.id, zero));
            *old = old.checked_add(capital.capital)?;
        }
        Ok(result)
    }
    pub(super) fn evaluate(&self) -> Result<()> {
        for &key in self.assets.keys() {
            if self.asset(key)?.deficit.atoms() > 0 {
                return Err(Error::InsufficientAsset);
            }
        }
        for (&wallet, &(asset, capital)) in &self.calculate_capital()? {
            let a = &self.assets[&AssetKey {
                wallet_id: wallet,
                asset_id: asset,
            }];
            let collateral = if a.baseline.exclusive_fee_pool {
                a.baseline.free.zero_like()
            } else {
                a.baseline
                    .free
                    .checked_add(a.baseline.locked)?
                    .checked_add(a.cashflow)?
                    .checked_sub(a.baseline.nontrade_locked)?
            };
            if capital.atoms() > collateral.atoms() {
                return Err(Error::InsufficientCapital);
            }
        }
        self.check_limits()
    }
    pub(super) fn check_limits(&self) -> Result<()> {
        for p in self.catalog.values() {
            let zero = Fixed::zero(asset_scale(&p.instrument.base)?)?;
            let mut buy = zero;
            let mut sell = zero;
            let mut strategies: BTreeMap<u64, Fixed> = BTreeMap::new();
            for o in self
                .orders
                .values()
                .filter(|o| o.instrument == p.instrument.instrument_id)
            {
                let q = o.remaining()?;
                if o.spec.side == v::Side::Buy {
                    buy = buy.checked_add(q)?
                } else {
                    sell = sell.checked_add(q)?
                }
                let old = strategies.get(&o.strategy).copied().unwrap_or(zero);
                strategies.insert(o.strategy, old.checked_add(q)?);
            }
            let (maximum, mut gross) = if let Some(fp) = &p.futures {
                for ((instrument, _), position) in &self.positions {
                    if *instrument == p.instrument.instrument_id {
                        for (&owner, &q) in &position.ownership {
                            let old = strategies.get(&owner).copied().unwrap_or(zero);
                            strategies.insert(owner, old.checked_add(q.checked_abs()?)?);
                        }
                    }
                }
                if fp.position_mode == v::PositionMode::OneWay {
                    let q = self.position_quantity(p, v::PositionSide::Both)?;
                    let env = f::one_way_envelope(q, buy, sell)?;
                    (env.max_abs, env.capital_quantity)
                } else {
                    let q = self
                        .position_quantity(p, v::PositionSide::Long)?
                        .checked_add(self.position_quantity(p, v::PositionSide::Short)?)?;
                    let upper = q.checked_add(buy)?.checked_add(sell)?;
                    (upper, upper)
                }
            } else {
                let a = &self.assets[&AssetKey {
                    wallet_id: p.wallet_id,
                    asset_id: p.instrument.base.id,
                }];
                for (&owner, &q) in &a.ownership {
                    let old = strategies.get(&owner).copied().unwrap_or(zero);
                    strategies.insert(owner, old.checked_add(q.checked_abs()?)?);
                }
                let actual = a
                    .baseline
                    .free
                    .checked_add(a.baseline.locked)?
                    .checked_add(a.cashflow)?;
                (
                    actual
                        .checked_add(buy)?
                        .max(actual.checked_sub(sell)?.checked_abs()?)?,
                    actual.checked_abs()?.checked_add(buy)?.checked_add(sell)?,
                )
            };
            let virtual_gross = strategies
                .values()
                .try_fold(zero, |s, n| s.checked_add(*n))?;
            gross = gross.max(virtual_gross)?;
            if maximum.atoms() > p.max_absolute_position.atoms()
                || gross.atoms() > p.max_gross_quantity.atoms()
            {
                return Err(Error::Limit);
            }
            for &(strategy, limit) in &p.strategy_limits {
                if strategies
                    .get(&strategy)
                    .is_some_and(|q| q.atoms() > limit.atoms())
                {
                    return Err(Error::Limit);
                }
            }
        }
        Ok(())
    }
}
