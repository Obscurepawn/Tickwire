use super::*;
impl Account {
    pub fn apply_event(&mut self, event: &v::CanonicalEvent) -> Result<Applied> {
        let prior_last_mono_ns = self.last_mono_ns;
        match self.apply_event_candidate(event.event()) {
            Ok(outcome) => Ok(outcome),
            Err(e) => {
                self.last_mono_ns = prior_last_mono_ns;
                self.frozen = true;
                if self.rejected_event.is_none() {
                    self.rejected_event = Some(event.clone());
                }
                Err(e)
            }
        }
    }
    fn validate_fee_deltas(&self, p: &InstrumentPolicy, deltas: &[v::AssetDelta]) -> Result<()> {
        for delta in deltas {
            let asset_key = AssetKey {
                wallet_id: p.wallet_id,
                asset_id: delta.asset.id,
            };
            let a = self.assets.get(&asset_key).ok_or(Error::Identity)?;
            if a.baseline.asset != delta.asset {
                return Err(Error::Identity);
            }
            let _ = exact_decimal(delta.amount, asset_scale(&delta.asset)?)?;
        }
        Ok(())
    }
    fn validate_observation_readonly(
        &self,
        key: &OrderKey,
        observation: &v::OrderObservation,
    ) -> Result<()> {
        let o = &self.orders[key];
        if let Some(q) = observation.original_quantity
            && !self.known_order_quantity(key, exact_decimal(q, o.original.scale())?)
        {
            return Err(Error::Conflict);
        }
        if observation.side != o.spec.side || observation.position_side != o.spec.position_side {
            return Err(Error::Identity);
        }
        if let Some(cum) = observation.cumulative_quantity {
            let _ = exact_decimal(cum, o.original.scale())?;
        }
        Ok(())
    }
    fn reverse_prior_fee(
        &mut self,
        key: &OrderKey,
        p: &InstrumentPolicy,
        delta: &v::AssetDelta,
    ) -> Result<()> {
        let asset_key = AssetKey {
            wallet_id: p.wallet_id,
            asset_id: delta.asset.id,
        };
        let amount = exact_decimal(delta.amount, asset_scale(&delta.asset)?)?;
        let strategy = self.orders[key].strategy;
        let expected = self.fee_asset(p, &self.orders[key]);
        {
            let a = self.assets.get_mut(&asset_key).ok_or(Error::Identity)?;
            a.cashflow = a.cashflow.checked_sub(amount)?;
            if amount.atoms() >= 0 {
                a.credits = a.credits.checked_sub(amount)?.positive_part();
            } else {
                a.other_debits = a
                    .other_debits
                    .checked_sub(amount.checked_neg()?)?
                    .positive_part();
            }
            add_owner(&mut a.ownership, strategy, amount.checked_neg()?)?;
        }
        if amount.atoms() < 0 {
            let debit = amount.checked_neg()?;
            {
                let o = self.orders.get_mut(key).unwrap();
                let cur_paid = o.paid.get(&asset_key).copied().unwrap_or(debit.zero_like());
                let next_paid = cur_paid.checked_sub(debit)?.positive_part();
                if next_paid.atoms() == 0 {
                    o.paid.remove(&asset_key);
                } else {
                    o.paid.insert(asset_key, next_paid);
                }
            }
            if p.futures.is_some()
                && asset_key == expected
                && let Some(old_uncov) = self.uncovered_debits.get(&p.wallet_id).copied()
            {
                let restored_uncov = debit.min(old_uncov)?;
                self.uncovered_debits
                    .insert(p.wallet_id, old_uncov.checked_sub(restored_uncov)?);
                let restored_credit = debit.checked_sub(restored_uncov)?;
                if restored_credit.atoms() > 0 {
                    let o = self.orders.get_mut(key).unwrap();
                    o.shadow_fee_credit = o.shadow_fee_credit.checked_add(restored_credit)?;
                }
            }
        }
        Ok(())
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
        let catalog = std::sync::Arc::clone(&self.catalog);
        let p = catalog.get(&id.instrument_id).ok_or(Error::Catalog)?;
        if id.instrument_version != p.instrument.instrument_version || id.product != p.product {
            return Err(Error::Catalog);
        }
        let now = id.received_mono_ns;
        if now > 0 {
            self.last_mono_ns = self.last_mono_ns.max(now);
            self.evict_expired(now);
        }
        if let v::VenueEventBody::Structure(s) = &event.body {
            return self.observe_structure(s, id.instrument_id);
        }
        if let v::VenueEventBody::Amendment(a) = &event.body {
            return self.observe_amendment(a, id.instrument_id);
        }
        if let v::VenueEventBody::AmendmentRejected(a) = &event.body {
            return self.reject_amendment(a, id.instrument_id);
        }
        if let v::VenueEventBody::Modified(m) = &event.body {
            return self.observe_modified(m, id.instrument_id);
        }
        if let v::VenueEventBody::ModifyRejected(r) = &event.body {
            return self.reject_modification(r, id.instrument_id);
        }
        if let v::VenueEventBody::Rejected {
            client_id,
            venue_code,
            message,
        } = &event.body
        {
            return self.observe_rejected(
                client_id.as_ref(),
                venue_code,
                message,
                id.instrument_id,
            );
        }
        let identity = match &event.body {
            v::VenueEventBody::Structure(_) => return Err(Error::Unreconciled),
            v::VenueEventBody::Amendment(_) => return Err(Error::Unreconciled),
            v::VenueEventBody::AmendmentRejected(_) => return Err(Error::Unreconciled),
            v::VenueEventBody::Modified(_) => return Err(Error::Unreconciled),
            v::VenueEventBody::ModifyRejected(_) => return Err(Error::Unreconciled),
            v::VenueEventBody::Fill(fill) => &fill.observation.order,
            v::VenueEventBody::Order(o) => &o.order,
            v::VenueEventBody::Fee { order, .. } => order,
            v::VenueEventBody::Rejected { .. } => return Err(Error::Unreconciled),
        };
        let key = self.resolve_order(identity, id.instrument_id)?;
        {
            let o = &self.orders[&key];
            if let Some(native) = &identity.venue_order_id
                && o.native
                    .as_ref()
                    .is_some_and(|old| old != native && !o.native_aliases.contains(native))
            {
                return Err(Error::Conflict);
            }
            if identity.order_id != [0; 16]
                && o.internal_id.is_some_and(|old| old != identity.order_id)
            {
                return Err(Error::Conflict);
            }
        }
        match &event.body {
            v::VenueEventBody::Fill(fill) => {
                self.validate_observation_readonly(&key, &fill.observation)?;
                let o = &self.orders[&key];
                exact_decimal(fill.last_quantity, o.original.scale())?.positive()?;
                exact_decimal(fill.last_price, o.price.scale())?.positive()?;
                if p.futures.is_none() {
                    exact_decimal(
                        fill.last_quote.ok_or(Error::Unreconciled)?,
                        asset_scale(&p.instrument.quote)?,
                    )?
                    .nonnegative()?;
                    if fill.realized_pnl.is_some() {
                        return Err(Error::Identity);
                    }
                } else if let Some(pnl) = &fill.realized_pnl {
                    if pnl.asset != p.instrument.settlement {
                        return Err(Error::Identity);
                    }
                    let _ = exact_decimal(pnl.amount, asset_scale(&p.instrument.settlement)?)?;
                }
                self.validate_fee_deltas(p, fill.fees.as_slice())?;
            }
            v::VenueEventBody::Fee { deltas, .. } => {
                self.validate_fee_deltas(p, deltas.as_slice())?;
            }
            v::VenueEventBody::Order(o) => {
                self.validate_observation_readonly(&key, o)?;
                if self.projected_fill_enabled
                    && let Some(cum_dec) = o.cumulative_quantity
                {
                    let base_scale = self.orders[&key].original.scale();
                    let _ = exact_decimal(cum_dec, base_scale)?;
                    if let Some(cq) = o.cumulative_quote {
                        let _ = exact_decimal(cq, asset_scale(&p.instrument.quote)?)?;
                    }
                }
            }
            _ => unreachable!(),
        }

        let prior_order = self.orders[&key].clone();
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
            if let Some(prior_entry) = self.facts.get(fact_key).cloned() {
                if let (
                    v::VenueEventBody::Fill(fill),
                    EconomicFact::Fill {
                        quantity: pq,
                        price: pp,
                        quote: pquote,
                        pnl: ppnl,
                        fees: pfees,
                    },
                ) = (&event.body, &prior_entry.fact)
                {
                    let core_matches = *pq == fill.last_quantity
                        && *pp == fill.last_price
                        && (pquote.is_none()
                            || fill.last_quote.is_none()
                            || *pquote == fill.last_quote)
                        && (ppnl.is_none()
                            || fill.realized_pnl.is_none()
                            || *ppnl == fill.realized_pnl);
                    if !core_matches {
                        return Err(Error::Conflict);
                    }
                    let incoming_fees = fill.fees.as_slice();
                    let pnl_added = ppnl.is_none() && fill.realized_pnl.is_some();
                    let quote_added = pquote.is_none() && fill.last_quote.is_some();
                    let fees_amended =
                        !incoming_fees.is_empty() && incoming_fees != pfees.as_slice();
                    if pnl_added || quote_added || fees_amended {
                        self.sequence
                            .checked_add(1)
                            .ok_or(RiskError::ArithmeticOverflow)?;
                        self.bind_native(&key, identity)?;
                        if self.orders[&key].fee_reconciled {
                            self.orders.get_mut(&key).unwrap().fee_reconciled = false;
                        }
                        if pnl_added && let Some(pnl) = &fill.realized_pnl {
                            let settlement_scale = asset_scale(&p.instrument.settlement)?;
                            let amount = exact_decimal(pnl.amount, settlement_scale)?;
                            let strategy = self.orders[&key].strategy;
                            self.cashflow(
                                AssetKey {
                                    wallet_id: p.wallet_id,
                                    asset_id: p.instrument.settlement.id,
                                },
                                amount,
                                strategy,
                            )?;
                            if amount.atoms() < 0 {
                                self.add_uncovered(p.wallet_id, amount.checked_neg()?)?;
                            }
                        }
                        if fees_amended {
                            for old_delta in pfees {
                                self.reverse_prior_fee(&key, p, old_delta)?;
                            }
                            for new_delta in incoming_fees {
                                self.apply_fee(&key, p, new_delta)?;
                            }
                        }
                        let _ = self.register_fill_components_mode(&key, p, fill, true)?;
                        self.observe(&key, &fill.observation)?;
                        let merged_fact = EconomicFact::Fill {
                            quantity: *pq,
                            price: *pp,
                            quote: fill.last_quote.or(*pquote),
                            pnl: fill.realized_pnl.clone().or_else(|| ppnl.clone()),
                            fees: if incoming_fees.is_empty() {
                                pfees.clone()
                            } else {
                                incoming_fees.to_vec()
                            },
                        };
                        if let Some(slot) = self.facts.get_mut(fact_key) {
                            slot.fact = merged_fact;
                            slot.recorded_mono_ns = self.last_mono_ns;
                        }
                        let stamp = self.last_mono_ns;
                        self.refresh_order_retirement(&key, stamp);
                        self.next_sequence()?;
                        if self.evaluate().is_err() {
                            self.frozen = true;
                        }
                        self.check_reduce_consistency()?;
                        return Ok(Applied::New);
                    }
                } else if &prior_entry.fact != body {
                    return Err(Error::Conflict);
                }

                self.bind_native(&key, identity)?;
                if let v::VenueEventBody::Fill(fill) = &event.body {
                    self.observe(&key, &fill.observation)?;
                }
                let components_changed = if let v::VenueEventBody::Fill(fill) = &event.body {
                    self.register_fill_components(&key, p, fill)?
                } else {
                    false
                };
                if self.orders[&key] != prior_order || components_changed {
                    self.sequence
                        .checked_add(1)
                        .ok_or(RiskError::ArithmeticOverflow)?;
                    let stamp = self.last_mono_ns;
                    self.refresh_order_retirement(&key, stamp);
                    self.next_sequence()?;
                    return Ok(Applied::New);
                }
                if self.orders[&key].in_doubt != prior_order.in_doubt {
                    let stamp = self.last_mono_ns;
                    self.refresh_order_retirement(&key, stamp);
                }
                return Ok(Applied::Duplicate);
            }
            if self.facts.len() >= MAX_FACTS {
                return Err(Error::Capacity);
            }
        }
        self.sequence
            .checked_add(1)
            .ok_or(RiskError::ArithmeticOverflow)?;
        let prior_assets_snapshot: Vec<(AssetKey, Asset)> = self
            .assets
            .iter()
            .filter(|(ak, _)| ak.wallet_id == p.wallet_id)
            .map(|(ak, av)| (*ak, av.clone()))
            .collect();
        let pos_key = (id.instrument_id, side_code(prior_order.spec.position_side));
        let prior_pos = self.positions.get(&pos_key).cloned();
        let prior_uncov = self.uncovered_debits.get(&p.wallet_id).copied();
        let prior_unval = self.shadow_unvalued.contains(&p.wallet_id);

        let step: Result<()> = (|| {
            self.bind_native(&key, identity)?;
            match &event.body {
                v::VenueEventBody::Fill(fill) => {
                    self.apply_fill(&key, p, fill)?;
                    self.register_fill_components(&key, p, fill)?;
                    self.observe(&key, &fill.observation)?;
                }
                v::VenueEventBody::Fee { deltas, .. } => {
                    self.mark_economic(&key)?;
                    for delta in deltas.as_slice() {
                        self.apply_fee(&key, p, delta)?;
                    }
                }
                v::VenueEventBody::Order(o) => {
                    if self.projected_fill_enabled
                        && let Some(cum_dec) = o.cumulative_quantity
                    {
                        let base_scale = self.orders[&key].original.scale();
                        let cum = exact_decimal(cum_dec, base_scale)?;
                        let price_input = match o.cumulative_quote {
                            Some(cq) => {
                                let quote_scale = asset_scale(&p.instrument.quote)?;
                                let cqf = exact_decimal(cq, quote_scale)?;
                                if cqf.atoms() > 0 {
                                    ProjectedPriceInput::CumulativeQuote { cum_quote: cqf }
                                } else {
                                    ProjectedPriceInput::LimitFallback
                                }
                            }
                            None => ProjectedPriceInput::LimitFallback,
                        };
                        let _ = self.on_cum_qty(&key, cum, price_input)?;
                    }
                    self.observe(&key, o)?;
                }
                _ => unreachable!(),
            }
            Ok(())
        })();

        if let Err(e) = step {
            for (ak, av) in prior_assets_snapshot {
                self.assets.insert(ak, av);
            }
            match prior_pos {
                Some(pos) => {
                    self.positions.insert(pos_key, pos);
                }
                None => {
                    self.positions.remove(&pos_key);
                }
            }
            match prior_uncov {
                Some(u) => {
                    self.uncovered_debits.insert(p.wallet_id, u);
                }
                None => {
                    self.uncovered_debits.remove(&p.wallet_id);
                }
            }
            if !prior_unval {
                self.shadow_unvalued.remove(&p.wallet_id);
            }
            self.orders.insert(key.clone(), prior_order);
            let _ = self.sync_order_in_stp_book(&key);
            return Err(e);
        }
        let stamp = self.last_mono_ns;
        if let Some((fact_key, body)) = economic {
            self.facts.insert(
                fact_key,
                FactEntry {
                    fact: body,
                    recorded_mono_ns: stamp,
                },
            );
        }
        self.refresh_order_retirement(&key, stamp);
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
        let matches_order = |o: &Order| {
            o.instrument == instrument
                && o.key.intent == intent
                && id.client_id.as_ref().is_none_or(|c| o.has_client_id(c))
                && id
                    .venue_order_id
                    .as_ref()
                    .is_none_or(|n| o.native.is_none() || o.has_native_id(n))
        };
        if let Some(cid) = &id.client_id {
            let key = OrderKey {
                intent,
                client_id: cid.clone(),
            };
            if let Some(o) = self.orders.get(&key)
                && matches_order(o)
            {
                return Ok(key);
            }
            if let Some(keys) = self.orders_by_client_id.get(&(instrument, cid.clone())) {
                let mut matched: Option<OrderKey> = None;
                for k in keys {
                    if let Some(o) = self.orders.get(k)
                        && matches_order(o)
                    {
                        if matched.is_some() {
                            return Err(Error::UnknownOrder);
                        }
                        matched = Some(k.clone());
                    }
                }
                if let Some(k) = matched {
                    return Ok(k);
                }
            }
            return Err(Error::UnknownOrder);
        }
        let Some(legs) = self.orders_by_intent.get(&intent) else {
            return Err(Error::UnknownOrder);
        };
        let mut matched: Option<OrderKey> = None;
        for leg in legs {
            if let Some(o) = self.orders.get(leg)
                && matches_order(o)
            {
                if matched.is_some() {
                    return Err(Error::UnknownOrder);
                }
                matched = Some(leg.clone());
            }
        }
        matched.ok_or(Error::UnknownOrder)
    }
    pub(super) fn bind_native(&mut self, key: &OrderKey, id: &v::OrderIdentity) -> Result<()> {
        let instrument = {
            let o = self.orders.get_mut(key).unwrap();
            if let Some(native) = &id.venue_order_id {
                if o.native
                    .as_ref()
                    .is_some_and(|old| old != native && !o.native_aliases.contains(native))
                {
                    return Err(Error::Conflict);
                }
                if o.native.is_none() {
                    o.native = Some(native.clone());
                }
            }
            if id.order_id != [0; 16] {
                if o.internal_id.is_some_and(|old| old != id.order_id) {
                    return Err(Error::Conflict);
                }
                o.internal_id = Some(id.order_id);
            }
            o.instrument
        };
        self.index_order_bind_native(
            instrument,
            key,
            id.venue_order_id.as_ref(),
            (id.order_id != [0; 16]).then_some(id.order_id),
        );
        Ok(())
    }
    fn mark_economic(&mut self, key: &OrderKey) -> Result<()> {
        let reopened = self.reopen_order(key)?;
        let o = self.orders.get_mut(key).unwrap();
        if reopened || o.fee_reconciled {
            self.frozen = true;
            o.fee_reconciled = false;
        }
        o.economic = true;
        Ok(())
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
        self.mark_economic(key)?;
        let count = self
            .facts
            .range(
                FactKey {
                    order: key.clone(),
                    kind: 0,
                    id: String::new(),
                }..,
            )
            .take_while(|(f, _)| f.order == *key && f.kind == 0)
            .count();
        if count >= p.fees.maximum_fills as usize {
            self.frozen = true;
        }
        let filled = o.filled.checked_add(q)?;
        let max_authorized_qty = self.authorized_quantity_bound(key)?;
        let price_bound = self.authorized_price_bound(key)?;
        if filled.atoms() > max_authorized_qty.atoms()
            || o.terminal_cumulative
                .is_some_and(|cum| filled.atoms() > cum.atoms())
            || (o.spec.side == v::Side::Buy && price.atoms() > price_bound.atoms())
            || (o.spec.side == v::Side::Sell && price.atoms() < price_bound.atoms())
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
                price_bound,
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
            current.projected_cum_qty = current.projected_cum_qty.max(current.filled)?;
            current.notional = o.notional.checked_add(notional)?;
            current.reduce = current.reduce.checked_sub(q)?.positive_part();
            if current.in_doubt == Some(DoubtReason::FillCatchup)
                && current
                    .terminal_cumulative
                    .is_none_or(|cum| cum == current.filled)
                && current.observed_cumulative.atoms() <= current.filled.atoms()
            {
                current.in_doubt = None;
            }
        }
        for delta in fill.fees.as_slice() {
            self.apply_fee(key, p, delta)?;
        }
        self.sync_order_in_stp_book(key)?;
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
    pub(super) fn add_uncovered(&mut self, wallet: u64, amount: Fixed) -> Result<()> {
        let old = self
            .uncovered_debits
            .get(&wallet)
            .copied()
            .unwrap_or(amount.zero_like());
        self.uncovered_debits
            .insert(wallet, old.checked_add(amount)?);
        Ok(())
    }
    pub(super) fn observe(
        &mut self,
        key: &OrderKey,
        observation: &v::OrderObservation,
    ) -> Result<()> {
        let amended = self
            .amendments
            .values()
            .any(|a| a.order == *key && a.confirmed);
        if let Some(q) = observation.original_quantity
            && !self.known_order_quantity(key, exact_decimal(q, self.orders[key].original.scale())?)
        {
            return Err(Error::Conflict);
        }
        let o = self.orders.get_mut(key).unwrap();
        if observation.side != o.spec.side || observation.position_side != o.spec.position_side {
            return Err(Error::Identity);
        }
        if amended && let Some(cum) = observation.cumulative_quantity {
            let cum = exact_decimal(cum, o.original.scale())?;
            o.observed_cumulative = o.observed_cumulative.max(cum)?;
            if cum.atoms() > o.filled.atoms() || cum.atoms() > o.effective.atoms() {
                o.observation_unreconciled = true;
                self.frozen = true;
            }
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
            if self.reopen_order(key)? {
                self.frozen = true;
            }
        }
        let max_authorized_qty = self.authorized_quantity_bound(key)?;
        let o = self.orders.get_mut(key).unwrap();
        let terminal = matches!(
            observation.status,
            v::OrderStatus::Filled
                | v::OrderStatus::Canceled
                | v::OrderStatus::Expired
                | v::OrderStatus::Rejected
        );
        if observation.status == v::OrderStatus::Unknown {
            o.observed_unknown = true;
            self.frozen = true;
        }
        if terminal {
            o.terminal = true;
            if o.terminal_reason.is_none() {
                o.terminal_reason = match observation.status {
                    v::OrderStatus::Filled => Some(v::TerminalReason::Filled),
                    v::OrderStatus::Canceled => Some(v::TerminalReason::UserCanceled),
                    v::OrderStatus::Expired => Some(v::classify_binance_reason(
                        None,
                        "EXPIRED",
                        None,
                        o.spec.time_in_force,
                        o.spec.time_in_force == Some(v::TimeInForce::PostOnly),
                    )),
                    v::OrderStatus::Rejected => Some(v::TerminalReason::VenueRejected),
                    _ => None,
                };
            }
            if let Some(cum) = observation.cumulative_quantity {
                let cum = exact_decimal(cum, o.original.scale())?;
                if cum.atoms() < o.filled.atoms() || cum.atoms() > max_authorized_qty.atoms() {
                    o.observation_unreconciled = true;
                    self.frozen = true;
                }
                if let Some(old) = o.terminal_cumulative {
                    if old != cum {
                        o.observation_unreconciled = true;
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
                o.observation_unreconciled = true;
                self.frozen = true;
            }
        }
        if observation.status != v::OrderStatus::Unknown {
            match o.in_doubt {
                Some(DoubtReason::PlaceAmbiguous) if o.confirmed => {
                    if o.terminal && o.terminal_cumulative != Some(o.filled) {
                        o.in_doubt = Some(DoubtReason::FillCatchup);
                    } else {
                        o.in_doubt = None;
                    }
                }
                Some(DoubtReason::CancelAmbiguous | DoubtReason::AmendAmbiguous) if o.terminal => {
                    if o.terminal_cumulative == Some(o.filled)
                        && o.observed_cumulative.atoms() <= o.filled.atoms()
                    {
                        o.in_doubt = None;
                    } else {
                        o.in_doubt = Some(DoubtReason::FillCatchup);
                    }
                }
                Some(DoubtReason::FillCatchup)
                    if o.terminal_cumulative == Some(o.filled)
                        && o.observed_cumulative.atoms() <= o.filled.atoms() =>
                {
                    o.in_doubt = None;
                }
                _ => {}
            }
        }
        if terminal {
            self.supersede_pending_modify_on_terminal(key)?;
        }
        self.sync_order_in_stp_book(key)?;
        Ok(())
    }
    fn authorized_quantity_bound(&self, key: &OrderKey) -> Result<Fixed> {
        let o = &self.orders[key];
        let mut bound = o.effective;
        if let Some(pm) = &o.pending_modify {
            bound = bound.max(pm.requested_quantity)?;
        }
        for m in self.modifications.values() {
            if m.order == *key && !m.confirmed && !m.aborted && m.rejected.is_none() {
                bound = bound.max(m.requested_quantity)?;
            }
        }
        for a in self.amendments.values() {
            if a.order == *key && !a.confirmed && !a.aborted && a.rejected.is_none() {
                bound = bound.max(a.after)?;
            }
        }
        Ok(bound)
    }
    fn authorized_price_bound(&self, key: &OrderKey) -> Result<Fixed> {
        let o = &self.orders[key];
        let mut bound = o.price;
        let merge = |cur: Fixed, cand: Fixed| -> Result<Fixed> {
            Ok(if o.spec.side == v::Side::Buy {
                cur.max(cand)?
            } else {
                cur.min(cand)?
            })
        };
        if let Some(pm) = &o.pending_modify {
            bound = merge(bound, pm.requested_price)?;
            bound = merge(bound, pm.prior_price)?;
        }
        for m in self.modifications.values() {
            if m.order == *key && !m.aborted && m.rejected.is_none() {
                bound = merge(bound, m.prior_price)?;
                if let Some(applied_px) = m.applied_price {
                    bound = merge(bound, applied_px)?;
                } else {
                    bound = merge(bound, m.requested_price)?;
                }
            }
        }
        Ok(bound)
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
    pub(super) fn check_reduce_consistency(&mut self) -> Result<()> {
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
    fn observe_rejected(
        &mut self,
        client_id: Option<&v::ClientId>,
        venue_code: &v::BoundedText<32>,
        message: &v::BoundedMessage<256>,
        instrument: u64,
    ) -> Result<Applied> {
        let Some(cid) = client_id else {
            return Err(Error::Unreconciled);
        };
        let matched: Vec<OrderKey> = self
            .orders_by_client_id
            .get(&(instrument, cid.clone()))
            .map(|keys| {
                keys.iter()
                    .filter(|k| self.orders.get(*k).is_some_and(|o| !o.aborted))
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        if matched.is_empty() {
            return Err(Error::UnknownOrder);
        }
        if matched.len() > 1 {
            return Err(Error::Conflict);
        }
        let key = matched[0].clone();
        let o = &self.orders[&key];
        if o.uncreated {
            return Ok(Applied::Duplicate);
        }
        if o.confirmed
            || o.economic
            || o.filled.atoms() != 0
            || !o.paid.is_empty()
            || o.terminal
            || o.native.is_some()
            || o.observed_unknown
            || o.observation_unreconciled
            || self.structures.values().any(|s| s.legs.contains(&key))
        {
            return Err(Error::Unreconciled);
        }
        let parsed_code = venue_code.as_str().parse::<i64>().ok();
        let post_only = o.spec.time_in_force == Some(v::TimeInForce::PostOnly);
        let tif = o.spec.time_in_force;
        let reason = if self.scope.venue == v::venue_id::BINANCE {
            v::classify_binance_reason(parsed_code, message.as_str(), None, tif, post_only)
        } else {
            v::classify_spot_venue_reason(
                parsed_code,
                venue_code.as_str(),
                message.as_str(),
                post_only,
                tif,
            )
        };
        self.sequence
            .checked_add(1)
            .ok_or(RiskError::ArithmeticOverflow)?;
        let o = self.orders.get_mut(&key).unwrap();
        o.uncreated = true;
        o.fee_reconciled = true;
        o.reduce = o.reduce.zero_like();
        o.in_doubt = None;
        o.recovery_unknown = false;
        o.terminal_reason = Some(reason);
        if self.orders_by_intent.get(&key.intent).is_some_and(|legs| {
            legs.iter()
                .all(|l| self.orders[l].aborted || self.orders[l].uncreated)
        }) {
            self.shadow_holds.remove(&key.intent);
        }
        self.sync_order_in_stp_book(&key)?;
        let stamp = self.last_mono_ns;
        self.refresh_order_retirement(&key, stamp);
        self.next_sequence()?;
        if self.evaluate().is_err() {
            self.frozen = true;
        }
        self.check_reduce_consistency()?;
        Ok(Applied::New)
    }
}
