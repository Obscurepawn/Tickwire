use super::*;
use tickwire_risk::reservation;
impl Account {
    pub fn admit(&mut self, command: &v::CanonicalCommand, now: u64) -> Result<Admission> {
        let c = command.command();
        let key = IntentKey {
            epoch: c.context.request.intent_exec_epoch,
            intent_id: c.context.request.intent_id,
        };
        if let Some(old) = self.admissions.get(&key) {
            if old.digest == command.digest() {
                return Ok(old.clone());
            }
            return self.freeze_error(Error::Conflict);
        }
        let mut candidate = self.clone();
        match candidate.admit_candidate(command, now) {
            Ok(a) => {
                *self = candidate;
                Ok(a)
            }
            Err(e) => {
                if matches!(
                    e,
                    Error::Risk(RiskError::ArithmeticOverflow) | Error::Conflict
                ) {
                    self.frozen = true;
                }
                Err(e)
            }
        }
    }
    fn admit_candidate(&mut self, command: &v::CanonicalCommand, now: u64) -> Result<Admission> {
        let c = command.command();
        let ctx = &c.context;
        let r = &ctx.request;
        if r.risk_domain_id != self.scope.risk_domain_id
            || r.account != self.scope.account
            || r.venue != self.scope.venue
            || r.environment != self.scope.environment
        {
            return Err(Error::Identity);
        }
        if now >= self.valid_until
            || now >= r.deadline
            || ctx.strategy.risk_policy_version != self.revisions.risk
        {
            return Err(Error::Stale);
        }
        let p = self
            .catalog
            .get(&ctx.instrument.instrument_id)
            .ok_or(Error::Catalog)?
            .clone();
        if p.instrument != ctx.instrument || p.product != r.product {
            return Err(Error::Catalog);
        }
        let strategy = ctx.strategy.logical_strategy_id;
        if !p.strategy_limits.iter().any(|x| x.0 == strategy) {
            return Err(Error::Identity);
        }
        let increases = !matches!(
            c.body,
            v::CommandBody::Cancel(_)
                | v::CommandBody::Query(_)
                | v::CommandBody::CancelList(_)
                | v::CommandBody::CancelAlgo(_)
        );
        if increases && self.frozen {
            return Err(Error::Frozen);
        }
        if self.admissions.len() >= MAX_ORDERS {
            return Err(Error::Capacity);
        }
        let key = IntentKey {
            epoch: r.intent_exec_epoch,
            intent_id: r.intent_id,
        };
        if !increases {
            match &c.body {
                v::CommandBody::Cancel(target) | v::CommandBody::Query(target) => {
                    self.check_target(target, strategy, p.instrument.instrument_id)?
                }
                _ => return Err(Error::Unsupported),
            }
            self.next_sequence()?;
            let a = Admission {
                digest: command.digest(),
                key,
                epoch: self.epoch,
                sequence: self.sequence,
                revisions: self.revisions,
                deadline: r.deadline,
                resources: vec![],
                capital: vec![],
                orders: vec![],
                increases: false,
            };
            self.admissions.insert(key, a.clone());
            return Ok(a);
        }
        let before = self.resources()?;
        let before_capital = self.calculate_capital()?;
        let specs = match &c.body {
            v::CommandBody::New(o) => vec![o.clone()],
            v::CommandBody::Algo(o) => vec![o.order.clone()],
            v::CommandBody::SpotList(o) => o.legs.as_slice().to_vec(),
            v::CommandBody::CancelReplace {
                target,
                replacement,
                ..
            } => {
                self.check_target(target, strategy, p.instrument.instrument_id)?;
                vec![replacement.clone()]
            }
            v::CommandBody::Cancel(target) | v::CommandBody::Query(target) => {
                self.check_target(target, strategy, p.instrument.instrument_id)?;
                vec![]
            }
            _ => return Err(Error::Unsupported),
        };
        if self.orders.len() + specs.len() > MAX_ORDERS {
            return Err(Error::Capacity);
        }
        let mut orders = Vec::new();
        for spec in specs {
            let q = match spec.quantity {
                v::Quantity::Base(q) => exact_decimal(q, asset_scale(&p.instrument.base)?)?,
                _ => return Err(Error::UnboundedExecution),
            };
            if !matches!(
                spec.order_type,
                v::OrderType::Limit | v::OrderType::StopLimit | v::OrderType::TakeProfitLimit
            ) {
                return Err(Error::UnboundedExecution);
            }
            let price_scale = p
                .futures
                .as_ref()
                .map(|x| x.stress_high.scale())
                .unwrap_or(asset_scale(&p.instrument.quote)?);
            let price = exact_decimal(spec.price.ok_or(Error::UnboundedExecution)?, price_scale)?;
            let qzero = q.zero_like();
            let mut reduce = qzero;
            if let Some(fp) = &p.futures {
                if spec.mode
                    != (v::ProductMode::Derivative {
                        position_mode: fp.position_mode,
                        margin_mode: fp.margin_mode,
                    })
                {
                    return Err(Error::Catalog);
                }
                let reducing = spec.reduce_only
                    || matches!(
                        (spec.position_side, spec.side),
                        (v::PositionSide::Long, v::Side::Sell)
                            | (v::PositionSide::Short, v::Side::Buy)
                    );
                if reducing {
                    let actual = self.position_quantity(&p, spec.position_side)?;
                    let existing: Vec<_> = self
                        .orders
                        .values()
                        .filter(|o| {
                            o.instrument == p.instrument.instrument_id
                                && o.spec.position_side == spec.position_side
                        })
                        .map(|o| o.reduce)
                        .collect();
                    let side = match spec.position_side {
                        v::PositionSide::Both => reservation::PositionSide::Both,
                        v::PositionSide::Long => reservation::PositionSide::Long,
                        v::PositionSide::Short => reservation::PositionSide::Short,
                    };
                    let allocation = reservation::allocate_reduce(
                        actual,
                        &existing,
                        q,
                        exact_decimal(p.instrument.quantity_step, q.scale())?,
                        false,
                        side,
                    )?;
                    let buy = allocation.side == tickwire_risk::futures::Side::Buy;
                    if buy != (spec.side == v::Side::Buy)
                        || allocation.reduce_only != spec.reduce_only
                    {
                        return Err(Error::Identity);
                    }
                    reduce = allocation.allocated;
                }
            } else {
                if spec.mode != v::ProductMode::Spot {
                    return Err(Error::Unsupported);
                }
                let spending_asset = if spec.side == v::Side::Buy {
                    p.instrument.quote.id
                } else {
                    p.instrument.base.id
                };
                if self.assets[&AssetKey {
                    wallet_id: p.wallet_id,
                    asset_id: spending_asset,
                }]
                    .baseline
                    .exclusive_fee_pool
                {
                    return Err(Error::InsufficientAsset);
                }
                if spec.side == v::Side::Sell
                    && self.fee_rate(&p)?.atoms() > Fixed::one(18)?.atoms()
                {
                    return Err(Error::UnboundedExecution);
                }
            }
            let order_key = OrderKey {
                intent: key,
                client_id: spec.client_id.clone(),
            };
            if self.orders.contains_key(&order_key) {
                return Err(Error::Conflict);
            }
            if self.orders.values().any(|o| {
                o.instrument == p.instrument.instrument_id
                    && o.key.client_id == spec.client_id
                    && !o.aborted
            }) {
                return Err(Error::Conflict);
            }
            let fee_asset = if p.futures.is_some() {
                &p.instrument.settlement
            } else if spec.side == v::Side::Buy {
                &p.instrument.base
            } else {
                &p.instrument.quote
            };
            let fee_zero = Fixed::zero(asset_scale(fee_asset)?)?;
            self.orders.insert(
                order_key.clone(),
                Order {
                    key: order_key.clone(),
                    digest: command.digest(),
                    instrument: p.instrument.instrument_id,
                    strategy,
                    spec,
                    original: q,
                    filled: qzero,
                    price,
                    received: fee_zero,
                    notional: Fixed::zero(asset_scale(&p.instrument.settlement)?)?,
                    paid: BTreeMap::new(),
                    terminal_cumulative: None,
                    unknown: false,
                    terminal: false,
                    fees_closed: false,
                    aborted: false,
                    economic: false,
                    confirmed: false,
                    reduce,
                    native: None,
                    internal_id: None,
                    owner_revision: 1,
                    shadow_fee_credit: fee_zero,
                },
            );
            orders.push(order_key);
        }
        self.evaluate()?;
        let after_capital = self.calculate_capital()?;
        let mut capital = Vec::new();
        for (&wallet, &(asset, after)) in &after_capital {
            let before = before_capital
                .get(&wallet)
                .map(|x| x.1)
                .unwrap_or(after.zero_like());
            let delta = after.checked_sub(before)?.positive_part();
            let available = self.venue_shadow(wallet)?;
            if delta.atoms() > available.atoms() {
                return Err(Error::InsufficientCapital);
            }
            if delta.atoms() > 0 {
                capital.push(CapitalHold {
                    wallet_id: wallet,
                    settlement_asset: asset,
                    amount: delta,
                });
            }
        }
        let mut fee_capacity: BTreeMap<_, _> =
            capital.iter().map(|h| (h.wallet_id, h.amount)).collect();
        for order_key in &orders {
            let o = &self.orders[order_key];
            let pp = &self.catalog[&o.instrument];
            if pp.futures.is_some() {
                let key = self.fee_asset(pp, o);
                let fee = self
                    .order_resources(o)?
                    .get(&key)
                    .copied()
                    .unwrap_or(o.shadow_fee_credit.zero_like());
                let available = fee_capacity
                    .get(&pp.wallet_id)
                    .copied()
                    .unwrap_or(fee.zero_like());
                let credit = fee.min(available)?;
                fee_capacity.insert(pp.wallet_id, available.checked_sub(credit)?);
                self.orders.get_mut(order_key).unwrap().shadow_fee_credit = credit;
            }
        }
        let after = self.resources()?;
        let mut resources = Vec::new();
        for (&key, &amount) in &after {
            let delta = amount
                .checked_sub(before.get(&key).copied().unwrap_or(amount.zero_like()))?
                .positive_part();
            if delta.atoms() > 0 {
                resources.push(AssetAmount { key, amount: delta })
            }
        }
        self.next_sequence()?;
        let admission = Admission {
            digest: command.digest(),
            key,
            epoch: self.epoch,
            sequence: self.sequence,
            revisions: self.revisions,
            deadline: r.deadline,
            resources,
            capital: capital.clone(),
            orders,
            increases,
        };
        self.shadow_holds.insert(key, capital);
        self.admissions.insert(key, admission.clone());
        Ok(admission)
    }
    fn check_target(&self, target: &v::OrderTarget, strategy: u64, instrument: u64) -> Result<()> {
        let found = self.orders.values().find(|o| {
            o.instrument == instrument
                && o.key.intent.epoch == target.original_intent_exec_epoch
                && target
                    .client_id
                    .as_ref()
                    .is_none_or(|id| id == &o.key.client_id)
                && target
                    .venue_order_id
                    .as_ref()
                    .is_none_or(|id| o.native.as_ref() == Some(id))
                && o.internal_id.is_none_or(|id| id == target.order_id)
        });
        let o = found.ok_or(Error::UnknownOrder)?;
        if o.strategy != strategy || o.owner_revision != target.expected_owner_revision {
            return Err(Error::Identity);
        }
        Ok(())
    }
    pub fn check_admission(&self, a: &Admission, now: u64) -> Result<()> {
        if self.admissions.get(&a.key) != Some(a)
            || a.epoch != self.epoch
            || a.revisions != self.revisions
        {
            return Err(Error::Identity);
        }
        if now >= self.valid_until || now >= a.deadline {
            return Err(Error::Stale);
        }
        if !a.increases {
            return Ok(());
        }
        if self.frozen {
            return Err(Error::Frozen);
        }
        for key in &a.orders {
            if self
                .orders
                .get(key)
                .is_none_or(|o| o.aborted || o.unknown || o.terminal)
            {
                return Err(Error::Unreconciled);
            }
        }
        self.evaluate()
    }
    pub fn venue_shadow(&self, wallet: u64) -> Result<Fixed> {
        if self.shadow_unvalued.contains(&wallet) {
            return Err(Error::Unreconciled);
        }
        let available = *self.venue_available.get(&wallet).ok_or(Error::Catalog)?;
        let holds: Vec<_> = self
            .shadow_holds
            .values()
            .flat_map(|v| v.iter())
            .filter(|h| h.wallet_id == wallet)
            .map(|h| h.amount)
            .collect();
        Ok(reservation::VenueShadow::calculate(
            available,
            &holds,
            self.uncovered_debits
                .get(&wallet)
                .copied()
                .unwrap_or(available.zero_like()),
            available.zero_like(),
        )?
        .available)
    }
    /// Only call after the exact AbortedBeforeSend record has reached the journal completion point.
    pub fn apply_abort(&mut self, a: &Admission) -> Result<Applied> {
        if self.admissions.get(&a.key) != Some(a) {
            return self.freeze_error(Error::Identity);
        }
        if a.orders.iter().all(|key| self.orders[key].aborted) {
            return Ok(Applied::Duplicate);
        }
        if a.orders.iter().any(|key| {
            let o = &self.orders[key];
            o.economic || o.confirmed || o.terminal || o.unknown
        }) {
            return self.freeze_error(Error::Unreconciled);
        }
        let mut candidate = self.clone();
        for key in &a.orders {
            let o = candidate.orders.get_mut(key).unwrap();
            o.aborted = true;
            o.reduce = o.reduce.zero_like();
            o.fees_closed = true;
        }
        candidate.shadow_holds.remove(&a.key);
        candidate.next_sequence()?;
        *self = candidate;
        Ok(Applied::New)
    }
    /// Trusted reconciliation fact: terminal cumulative fills and all commission records are closed.
    pub fn reconcile_fees(&mut self, key: &OrderKey, cumulative: Fixed) -> Result<Applied> {
        let o = self.orders.get(key).ok_or(Error::UnknownOrder)?;
        if !o.terminal || o.terminal_cumulative != Some(cumulative) || o.filled != cumulative {
            return self.freeze_error(Error::Unreconciled);
        }
        if o.fees_closed {
            return Ok(Applied::Duplicate);
        }
        let mut candidate = self.clone();
        candidate.orders.get_mut(key).unwrap().fees_closed = true;
        candidate.next_sequence()?;
        *self = candidate;
        Ok(Applied::New)
    }
}
