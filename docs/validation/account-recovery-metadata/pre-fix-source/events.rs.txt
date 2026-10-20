use super::*;
impl Account {
    pub fn apply_event(&mut self, event: &v::CanonicalEvent) -> Result<Applied> {
        let mut candidate = self.clone();
        match candidate.apply_event_candidate(event.event()) {
            Ok(outcome) => {
                *self = candidate;
                Ok(outcome)
            }
            Err(e) => {
                self.frozen = true;
                if self.rejected_event.is_none() {
                    self.rejected_event = Some(event.clone());
                }
                Err(e)
            }
        }
    }
    fn apply_event_candidate(&mut self, event: &v::VenueEvent) -> Result<Applied> {
        let id = &event.identity;
        if id.risk_domain_id != self.scope.risk_domain_id
            || id.account != self.scope.account
            || id.venue != self.scope.venue
            || id.environment != self.scope.environment
        {
            return Err(Error::Identity);
        }
        let p = self
            .catalog
            .get(&id.instrument_id)
            .ok_or(Error::Catalog)?
            .clone();
        if id.instrument_version != p.instrument.instrument_version || id.product != p.product {
            return Err(Error::Catalog);
        }
        let identity = match &event.body {
            v::VenueEventBody::Fill(fill) => &fill.observation.order,
            v::VenueEventBody::Order(o) => &o.order,
            v::VenueEventBody::Fee { order, .. } => order,
            v::VenueEventBody::Rejected { .. } => return Err(Error::Unreconciled),
        };
        let key = self.resolve_order(identity, id.instrument_id)?;
        let prior_order = self.orders[&key].clone();
        let prior_frozen = self.frozen;
        self.bind_native(&key, identity)?;
        let economic = match &event.body {
            v::VenueEventBody::Fill(fill) => Some((
                FactKey {
                    order: key.clone(),
                    kind: 0,
                    id: fill.trade_id.as_str().to_owned(),
                },
                EconomicFact::Fill {
                    quantity: fill.last_quantity,
                    price: fill.last_price,
                    quote: fill.last_quote,
                    pnl: fill.realized_pnl.clone(),
                    fees: fill.fees.as_slice().to_vec(),
                },
            )),
            v::VenueEventBody::Fee { fee_id, deltas, .. } => Some((
                FactKey {
                    order: key.clone(),
                    kind: 1,
                    id: fee_id.as_str().to_owned(),
                },
                EconomicFact::Fee(deltas.as_slice().to_vec()),
            )),
            _ => None,
        };
        if let Some((fact_key, body)) = &economic {
            if let Some(prior) = self.facts.get(fact_key) {
                if prior != body {
                    return Err(Error::Conflict);
                }
                if let v::VenueEventBody::Fill(fill) = &event.body {
                    self.observe(&key, &fill.observation)?;
                }
                if self.orders[&key] != prior_order || self.frozen != prior_frozen {
                    self.next_sequence()?;
                    return Ok(Applied::New);
                }
                return Ok(Applied::Duplicate);
            }
            if self.facts.len() >= MAX_FACTS {
                return Err(Error::Capacity);
            }
        }
        match &event.body {
            v::VenueEventBody::Fill(fill) => {
                self.apply_fill(&key, &p, fill)?;
                self.observe(&key, &fill.observation)?;
            }
            v::VenueEventBody::Fee { deltas, .. } => {
                self.mark_economic(&key);
                for delta in deltas.as_slice() {
                    self.apply_fee(&key, &p, delta)?;
                }
            }
            v::VenueEventBody::Order(o) => self.observe(&key, o)?,
            _ => unreachable!(),
        }
        if let Some((key, body)) = economic {
            self.facts.insert(key, body);
        }
        self.next_sequence()?;
        if self.evaluate().is_err() {
            self.frozen = true;
        }
        self.check_reduce_consistency()?;
        Ok(Applied::New)
    }
    fn resolve_order(&self, id: &v::OrderIdentity, instrument: u64) -> Result<OrderKey> {
        let intent = IntentKey {
            epoch: id.original_intent_exec_epoch,
            intent_id: id.original_intent_id,
        };
        let matches: Vec<_> = self
            .orders
            .values()
            .filter(|o| {
                o.instrument == instrument
                    && o.key.intent == intent
                    && id.client_id.as_ref().is_none_or(|c| c == &o.key.client_id)
                    && id
                        .venue_order_id
                        .as_ref()
                        .is_none_or(|n| o.native.as_ref().is_none_or(|old| old == n))
            })
            .collect();
        if matches.len() != 1 {
            return Err(Error::UnknownOrder);
        }
        Ok(matches[0].key.clone())
    }
    fn bind_native(&mut self, key: &OrderKey, id: &v::OrderIdentity) -> Result<()> {
        let o = self.orders.get_mut(key).unwrap();
        if let Some(native) = &id.venue_order_id {
            if o.native.as_ref().is_some_and(|old| old != native) {
                return Err(Error::Conflict);
            }
            o.native = Some(native.clone());
        }
        if id.order_id != [0; 16] {
            if o.internal_id.is_some_and(|old| old != id.order_id) {
                return Err(Error::Conflict);
            }
            o.internal_id = Some(id.order_id);
        }
        Ok(())
    }
    fn mark_economic(&mut self, key: &OrderKey) {
        let o = self.orders.get_mut(key).unwrap();
        if o.aborted || o.fees_closed {
            self.frozen = true;
            o.aborted = false;
            o.fees_closed = false;
        }
        o.economic = true;
    }
    fn apply_fill(
        &mut self,
        key: &OrderKey,
        p: &InstrumentPolicy,
        fill: &v::FillEvent,
    ) -> Result<()> {
        let o = self.orders[key].clone();
        if fill.observation.side != o.spec.side
            || fill.observation.position_side != o.spec.position_side
        {
            return Err(Error::Identity);
        }
        let q = exact_decimal(fill.last_quantity, o.original.scale())?;
        q.positive()?;
        let price = exact_decimal(fill.last_price, o.price.scale())?;
        price.positive()?;
        self.mark_economic(key);
        let count = self
            .facts
            .keys()
            .filter(|f| f.order == *key && f.kind == 0)
            .count();
        if count >= p.fees.maximum_fills as usize {
            self.frozen = true;
        }
        let filled = o.filled.checked_add(q)?;
        if filled.atoms() > o.original.atoms()
            || o.terminal_cumulative
                .is_some_and(|cum| filled.atoms() > cum.atoms())
            || (o.spec.side == v::Side::Buy && price.atoms() > o.price.atoms())
            || (o.spec.side == v::Side::Sell && price.atoms() < o.price.atoms())
        {
            self.frozen = true;
        }
        let settlement_scale = asset_scale(&p.instrument.settlement)?;
        let notional = q.mul_to(price, settlement_scale, Rounding::Ceil)?;
        if p.futures.is_none() {
            let quote = exact_decimal(
                fill.last_quote.ok_or(Error::Unreconciled)?,
                asset_scale(&p.instrument.quote)?,
            )?;
            quote.nonnegative()?;
            let bound = q.mul_to(
                o.price,
                quote.scale(),
                if o.spec.side == v::Side::Buy {
                    Rounding::Ceil
                } else {
                    Rounding::Floor
                },
            )?;
            if (o.spec.side == v::Side::Buy && quote.atoms() > bound.atoms())
                || (o.spec.side == v::Side::Sell && quote.atoms() < bound.atoms())
            {
                self.frozen = true;
            }
            let base_key = AssetKey {
                wallet_id: p.wallet_id,
                asset_id: p.instrument.base.id,
            };
            let quote_key = AssetKey {
                wallet_id: p.wallet_id,
                asset_id: p.instrument.quote.id,
            };
            let (base, quote_delta, received) = if o.spec.side == v::Side::Buy {
                (q, quote.checked_neg()?, q)
            } else {
                (q.checked_neg()?, quote, quote)
            };
            self.cashflow(base_key, base, o.strategy)?;
            self.cashflow(quote_key, quote_delta, o.strategy)?;
            self.orders.get_mut(key).unwrap().received = o.received.checked_add(received)?;
            if fill.realized_pnl.is_some() {
                return Err(Error::Identity);
            }
        } else {
            let previous_position = self.position_quantity(p, o.spec.position_side)?;
            self.fill_position(&o, p, q, price)?;
            if let Some(pnl) = &fill.realized_pnl {
                if pnl.asset != p.instrument.settlement {
                    return Err(Error::Identity);
                }
                let amount = exact_decimal(pnl.amount, settlement_scale)?;
                self.cashflow(
                    AssetKey {
                        wallet_id: p.wallet_id,
                        asset_id: p.instrument.settlement.id,
                    },
                    amount,
                    o.strategy,
                )?;
                if amount.atoms() < 0 {
                    self.add_uncovered(p.wallet_id, amount.checked_neg()?)?;
                }
            } else {
                // Closing PnL cannot be fabricated from price or silently treated as zero.
                if o.reduce.atoms() > 0
                    || (o.spec.position_side == v::PositionSide::Both
                        && previous_position.atoms() != 0
                        && (previous_position.atoms() < 0) == (o.spec.side == v::Side::Buy))
                {
                    self.frozen = true;
                }
            }
        }
        {
            let current = self.orders.get_mut(key).unwrap();
            current.filled = filled;
            current.notional = o.notional.checked_add(notional)?;
            current.reduce = o.reduce.checked_sub(q)?.positive_part();
        }
        for delta in fill.fees.as_slice() {
            self.apply_fee(key, p, delta)?;
        }
        Ok(())
    }
    fn cashflow(&mut self, key: AssetKey, delta: Fixed, strategy: u64) -> Result<()> {
        let a = self.assets.get_mut(&key).ok_or(Error::Identity)?;
        a.cashflow = a.cashflow.checked_add(delta)?;
        if delta.atoms() >= 0 {
            a.credits = a.credits.checked_add(delta)?;
        } else {
            a.other_debits = a.other_debits.checked_add(delta.checked_neg()?)?;
        }
        add_owner(&mut a.ownership, strategy, delta)?;
        Ok(())
    }
    fn apply_fee(
        &mut self,
        key: &OrderKey,
        p: &InstrumentPolicy,
        delta: &v::AssetDelta,
    ) -> Result<()> {
        let asset_key = AssetKey {
            wallet_id: p.wallet_id,
            asset_id: delta.asset.id,
        };
        let a = self.assets.get(&asset_key).ok_or(Error::Identity)?;
        if a.baseline.asset != delta.asset {
            return Err(Error::Identity);
        }
        let amount = exact_decimal(delta.amount, asset_scale(&delta.asset)?)?;
        let o = self.orders[key].clone();
        let expected = self.fee_asset(p, &o);
        if amount.atoms() < 0 && asset_key != expected && Some(asset_key) != p.fees.exclusive_pool {
            self.frozen = true;
            if p.futures.is_some() {
                self.shadow_unvalued.insert(p.wallet_id);
            }
        }
        self.cashflow(asset_key, amount, o.strategy)?;
        if amount.atoms() < 0 {
            let debit = amount.checked_neg()?;
            add(
                &mut self.orders.get_mut(key).unwrap().paid,
                asset_key,
                debit,
            )?;
            if asset_key == expected {
                let base = if p.futures.is_some() {
                    o.notional
                } else {
                    o.received
                };
                let bound = self
                    .fee_total(base, p)?
                    .checked_add(self.tail(p, debit.scale())?)?;
                if self.orders[key].paid[&asset_key].atoms() > bound.atoms() {
                    self.frozen = true;
                }
            }
            if p.futures.is_some() && asset_key == expected {
                let covered = debit.min(o.shadow_fee_credit)?;
                self.orders.get_mut(key).unwrap().shadow_fee_credit =
                    o.shadow_fee_credit.checked_sub(covered)?;
                self.add_uncovered(p.wallet_id, debit.checked_sub(covered)?)?;
            }
        }
        Ok(())
    }
    fn add_uncovered(&mut self, wallet: u64, amount: Fixed) -> Result<()> {
        let old = self
            .uncovered_debits
            .get(&wallet)
            .copied()
            .unwrap_or(amount.zero_like());
        self.uncovered_debits
            .insert(wallet, old.checked_add(amount)?);
        Ok(())
    }
    fn observe(&mut self, key: &OrderKey, observation: &v::OrderObservation) -> Result<()> {
        let o = self.orders.get_mut(key).unwrap();
        if observation.side != o.spec.side || observation.position_side != o.spec.position_side {
            return Err(Error::Identity);
        }
        if let Some(q) = observation.original_quantity
            && exact_decimal(q, o.original.scale())? != o.original
        {
            return Err(Error::Conflict);
        }
        if matches!(
            observation.status,
            v::OrderStatus::New
                | v::OrderStatus::PartiallyFilled
                | v::OrderStatus::Filled
                | v::OrderStatus::Canceled
                | v::OrderStatus::Expired
                | v::OrderStatus::Rejected
        ) || observation.order.venue_order_id.is_some()
        {
            o.confirmed = true;
            if o.aborted {
                o.aborted = false;
                o.fees_closed = false;
                self.frozen = true;
            }
        }
        let terminal = matches!(
            observation.status,
            v::OrderStatus::Filled
                | v::OrderStatus::Canceled
                | v::OrderStatus::Expired
                | v::OrderStatus::Rejected
        );
        if observation.status == v::OrderStatus::Unknown {
            o.unknown = true;
            self.frozen = true;
        }
        if terminal {
            o.terminal = true;
            if let Some(cum) = observation.cumulative_quantity {
                let cum = exact_decimal(cum, o.original.scale())?;
                if cum.atoms() < o.filled.atoms() || cum.atoms() > o.original.atoms() {
                    self.frozen = true;
                }
                if let Some(old) = o.terminal_cumulative {
                    if old != cum {
                        self.frozen = true;
                        o.terminal_cumulative = Some(old.max(cum)?);
                    }
                } else {
                    o.terminal_cumulative = Some(cum);
                }
                if cum == o.filled {
                    o.reduce = o.reduce.zero_like();
                }
            } else {
                self.frozen = true;
            }
        }
        Ok(())
    }
    fn fill_position(
        &mut self,
        o: &Order,
        p: &InstrumentPolicy,
        q: Fixed,
        price: Fixed,
    ) -> Result<()> {
        let side = o.spec.position_side;
        let key = (o.instrument, side_code(side));
        let old = self.positions.get(&key).cloned().unwrap_or(Position {
            quantity: q.zero_like(),
            entry: price,
            ownership: BTreeMap::new(),
        });
        let change = match (side, o.spec.side) {
            (v::PositionSide::Short, v::Side::Sell)
            | (v::PositionSide::Long, v::Side::Buy)
            | (v::PositionSide::Both, v::Side::Buy) => q,
            _ => q.checked_neg()?,
        };
        let quantity = old.quantity.checked_add(change)?;
        if side != v::PositionSide::Both && quantity.atoms() < 0 {
            self.frozen = true;
        }
        let increased = if side == v::PositionSide::Both {
            (old.quantity.atoms() >= 0) == (change.atoms() > 0)
        } else {
            change.atoms() > 0
        };
        let sign_flip =
            side == v::PositionSide::Both && (old.quantity.atoms() < 0) != (quantity.atoms() < 0);
        let long =
            side == v::PositionSide::Long || side == v::PositionSide::Both && quantity.atoms() >= 0;
        let entry = if old.quantity.atoms() == 0 || sign_flip {
            price
        } else if increased {
            if long {
                old.entry.max(price)?
            } else {
                old.entry.min(price)?
            }
        } else {
            old.entry
        };
        let mut ownership = old.ownership;
        add_owner(&mut ownership, o.strategy, change)?;
        self.positions.insert(
            key,
            Position {
                quantity,
                entry,
                ownership,
            },
        );
        // p is deliberately used to recheck the asset quantity unit after attribution.
        if quantity.scale() != asset_scale(&p.instrument.base)? {
            return Err(Error::Identity);
        }
        Ok(())
    }
    fn check_reduce_consistency(&mut self) -> Result<()> {
        let mut allocated: BTreeMap<(u64, u8), Fixed> = BTreeMap::new();
        for o in self.orders.values() {
            let key = (o.instrument, side_code(o.spec.position_side));
            let old = allocated.get(&key).copied().unwrap_or(o.reduce.zero_like());
            allocated.insert(key, old.checked_add(o.reduce)?);
        }
        for ((instrument, side), sum) in allocated {
            let actual = self
                .positions
                .get(&(instrument, side))
                .map(|p| p.quantity)
                .unwrap_or(sum.zero_like())
                .checked_abs()?;
            if sum.atoms() > actual.atoms() {
                self.frozen = true;
            }
        }
        Ok(())
    }
}
