use sha2::{Digest, Sha256};
use tickwire_risk::{Fixed, reservation};
use tickwire_venue as v;

use super::{
    Account, Admission, Applied, AssetAmount, CapitalHold, DoubtReason, Error, InstrumentPolicy,
    IntentKey, MAX_FACTS, MAX_MANAGEMENT_ADMISSIONS, ManagerIdentity, Order, OrderKey,
    ProjectedPriceInput, Result, asset_scale, exact_decimal, push_unique, stp_book,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PendingModify {
    pub intent: IntentKey,
    pub command_digest: v::Hash,
    pub manager: ManagerIdentity,
    pub owner_revision: u64,
    pub new_client_id: Option<v::ClientId>,
    pub prior_price: Fixed,
    pub prior_effective: Fixed,
    pub requested_price: Fixed,
    pub requested_quantity: Fixed,
    pub extra_reduce: Fixed,
    pub extra_shadow_fee_credit: Fixed,
}

#[derive(Debug, Clone)]
pub(crate) struct ModificationRecord {
    pub order: OrderKey,
    pub command_digest: v::Hash,
    pub manager: ManagerIdentity,
    pub owner_revision: u64,
    pub new_client_id: Option<v::ClientId>,
    pub prior_price: Fixed,
    pub requested_price: Fixed,
    pub requested_quantity: Fixed,
    pub applied_price: Option<Fixed>,
    pub applied_quantity: Option<Fixed>,
    pub aborted: bool,
    pub confirmed: bool,
    pub superseded: bool,
    pub rejected: Option<v::TerminalReason>,
    pub retired_mono_ns: Option<u64>,
}

impl PartialEq for ModificationRecord {
    fn eq(&self, other: &Self) -> bool {
        self.order == other.order
            && self.command_digest == other.command_digest
            && self.manager == other.manager
            && self.owner_revision == other.owner_revision
            && self.new_client_id == other.new_client_id
            && self.prior_price == other.prior_price
            && self.requested_price == other.requested_price
            && self.requested_quantity == other.requested_quantity
            && self.applied_price == other.applied_price
            && self.applied_quantity == other.applied_quantity
            && self.aborted == other.aborted
            && self.confirmed == other.confirmed
            && self.superseded == other.superseded
            && self.rejected == other.rejected
    }
}

impl Eq for ModificationRecord {}

impl Account {
    pub(super) fn admit_modify_candidate(
        &mut self,
        command: &v::CanonicalCommand,
        m: &v::ModifyOrder,
        p: &InstrumentPolicy,
        now: u64,
    ) -> Result<Admission> {
        let ctx = &command.command().context;
        let strategy = ctx.strategy.logical_strategy_id;
        let manager = ManagerIdentity {
            instance: ctx.strategy.instance_id,
            generation: ctx.request.strategy_generation,
        };
        let key = IntentKey {
            epoch: ctx.request.intent_exec_epoch,
            intent_id: ctx.request.intent_id,
        };
        let mgmt_count = self
            .admissions
            .iter()
            .filter(|(k, a)| {
                a.orders.is_empty()
                    && !self.is_confirmed_amendment(**k)
                    && !self.is_confirmed_modification(**k)
            })
            .count();
        if mgmt_count >= MAX_MANAGEMENT_ADMISSIONS
            && !self.reclaim_one_retired_management_admission()
        {
            return Err(Error::Capacity);
        }
        if self.modifications.len() >= MAX_FACTS {
            return Err(Error::Capacity);
        }

        let instrument = p.instrument.instrument_id;
        let order_key = self
            .resolve_order_target(instrument, &m.target)
            .ok_or(Error::OrderNotOpen)?;

        let o = &self.orders[&order_key];
        if o.terminal || o.aborted || o.uncreated {
            return Err(Error::OrderNotOpen);
        }
        if o.strategy != strategy
            || o.manager != manager
            || o.owner_revision != m.target.expected_owner_revision
        {
            return Err(Error::ManagementDenied);
        }
        if o.spec.order_type != v::OrderType::Limit
            || o.spec.trigger.is_some()
            || o.spec.trailing.is_some()
            || self.structures.values().any(|s| s.legs.contains(&o.key))
        {
            return Err(Error::Unsupported);
        }
        if m.side != o.spec.side
            || m.position_side != o.spec.position_side
            || m.reduce_only != o.spec.reduce_only
        {
            return Err(Error::Identity);
        }
        if o.unknown() || o.observation_unreconciled || o.in_doubt.is_some() {
            return Err(Error::Unreconciled);
        }
        if o.pending_modify.is_some()
            || self
                .amendments
                .values()
                .any(|a| a.order == order_key && !a.aborted && !a.confirmed && a.rejected.is_none())
        {
            return Err(Error::TooManyPendingActions);
        }

        let price_scale = p
            .futures
            .as_ref()
            .map(|x| x.stress_high.scale())
            .unwrap_or(asset_scale(&p.instrument.quote)?);
        let new_price = exact_decimal(m.price, price_scale).map_err(|_| Error::InvalidPrice)?;
        if new_price.atoms() <= 0 {
            return Err(Error::InvalidPrice);
        }
        let tick_size = self.stp_book.tick_size(instrument)?;
        let _ = stp_book::price_to_ticks(new_price, tick_size)?;

        let base_scale = asset_scale(&p.instrument.base)?;
        let new_qty = exact_decimal(m.quantity, base_scale).map_err(|_| Error::InvalidQuantity)?;
        if new_qty.atoms() <= 0 {
            return Err(Error::InvalidQuantity);
        }
        let step = exact_decimal(p.instrument.quantity_step, base_scale)
            .map_err(|_| Error::InvalidQuantity)?;
        if step.atoms() <= 0 || new_qty.atoms() % step.atoms() != 0 {
            return Err(Error::InvalidQuantity);
        }

        let filled_floor = o.filled.max(o.observed_cumulative)?;
        if new_qty.atoms() <= filled_floor.atoms() {
            return Err(Error::InvalidQuantity);
        }
        if new_price == o.price && new_qty == o.effective {
            return Err(Error::NoChange);
        }

        if let Some(ref cid) = m.new_client_id
            && !o.has_client_id(cid)
            && self
                .orders_by_client_id
                .get(&(instrument, cid.clone()))
                .is_some_and(|keys| {
                    keys.iter()
                        .any(|k| k != &order_key && !self.orders[k].aborted)
                })
        {
            return Err(Error::Conflict);
        }

        if self.stp_enforcement
            && self
                .stp_book
                .self_cross_excluding(instrument, o.spec.side, new_price, &order_key)?
        {
            return Err(Error::CrossesBook);
        }

        let before = self.resources()?;
        let before_capital = self.calculate_capital()?;
        let before_order_fee = if p.futures.is_some() {
            let fee_key = self.fee_asset(p, &self.orders[&order_key]);
            self.order_resources(&self.orders[&order_key])?
                .get(&fee_key)
                .copied()
                .unwrap_or(self.orders[&order_key].shadow_fee_credit.zero_like())
        } else {
            Fixed::zero(0)?
        };

        let mut extra_reduce = new_qty.zero_like();
        if p.futures.is_some() {
            let o_ref = &self.orders[&order_key];
            let reducing = o_ref.spec.reduce_only
                || matches!(
                    (o_ref.spec.position_side, o_ref.spec.side),
                    (v::PositionSide::Long, v::Side::Sell) | (v::PositionSide::Short, v::Side::Buy)
                );
            if reducing {
                let old_rem = o_ref.remaining()?;
                let new_rem = new_qty
                    .checked_sub(o_ref.filled.max(o_ref.observed_cumulative)?)?
                    .positive_part();
                if new_rem.atoms() > old_rem.atoms() {
                    let delta_rem = new_rem.checked_sub(old_rem)?;
                    let actual = self.position_quantity(p, o_ref.spec.position_side)?;
                    let existing: Vec<_> = self
                        .orders
                        .values()
                        .filter(|ord| {
                            ord.instrument == instrument
                                && ord.spec.position_side == o_ref.spec.position_side
                        })
                        .map(|ord| ord.reduce)
                        .collect();
                    let side = match o_ref.spec.position_side {
                        v::PositionSide::Both => reservation::PositionSide::Both,
                        v::PositionSide::Long => reservation::PositionSide::Long,
                        v::PositionSide::Short => reservation::PositionSide::Short,
                    };
                    let allocation = reservation::allocate_reduce(
                        actual, &existing, delta_rem, step, false, side,
                    )?;
                    let buy = allocation.side == tickwire_risk::futures::Side::Buy;
                    if buy != (o_ref.spec.side == v::Side::Buy)
                        || allocation.reduce_only != o_ref.spec.reduce_only
                    {
                        return Err(Error::Identity);
                    }
                    extra_reduce = allocation.allocated;
                }
            }
        }

        let prior_price = self.orders[&order_key].price;
        let prior_effective = self.orders[&order_key].effective;
        let fee_credit_zero = self.orders[&order_key].shadow_fee_credit.zero_like();

        {
            let o_mut = self.orders.get_mut(&order_key).unwrap();
            o_mut.reduce = o_mut.reduce.checked_add(extra_reduce)?;
            o_mut.pending_modify = Some(PendingModify {
                intent: key,
                command_digest: command.digest(),
                manager,
                owner_revision: o_mut.owner_revision,
                new_client_id: m.new_client_id.clone(),
                prior_price,
                prior_effective,
                requested_price: new_price,
                requested_quantity: new_qty,
                extra_reduce,
                extra_shadow_fee_credit: fee_credit_zero,
            });
        }

        if let Some(ref cid) = m.new_client_id
            && !self.orders[&order_key].has_client_id(cid)
        {
            push_unique(
                self.orders_by_client_id
                    .entry((instrument, cid.clone()))
                    .or_default(),
                order_key.clone(),
            );
        }

        self.sync_order_in_stp_book(&order_key)?;
        self.evaluate()?;

        let after_capital = self.calculate_capital()?;
        let mut capital = Vec::new();
        for (&wallet, &(asset, after)) in &after_capital {
            let prev = before_capital
                .get(&wallet)
                .map(|x| x.1)
                .unwrap_or(after.zero_like());
            let delta = after.checked_sub(prev)?.positive_part();
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

        if p.futures.is_some() {
            let fee_key = self.fee_asset(p, &self.orders[&order_key]);
            let after_order_fee = self
                .order_resources(&self.orders[&order_key])?
                .get(&fee_key)
                .copied()
                .unwrap_or(fee_credit_zero);
            let fee_delta = after_order_fee
                .checked_sub(before_order_fee)?
                .positive_part();
            let cap_avail = capital
                .iter()
                .find(|h| h.wallet_id == p.wallet_id)
                .map(|h| h.amount)
                .unwrap_or(fee_credit_zero);
            let extra_credit = fee_delta.min(cap_avail)?;
            if extra_credit.atoms() > 0 {
                let o_mut = self.orders.get_mut(&order_key).unwrap();
                o_mut.shadow_fee_credit = o_mut.shadow_fee_credit.checked_add(extra_credit)?;
                if let Some(pm) = o_mut.pending_modify.as_mut() {
                    pm.extra_shadow_fee_credit = extra_credit;
                }
            }
        }

        let after = self.resources()?;
        let mut resources = Vec::new();
        for (&asset_key, &amount) in &after {
            let delta = amount
                .checked_sub(
                    before
                        .get(&asset_key)
                        .copied()
                        .unwrap_or(amount.zero_like()),
                )?
                .positive_part();
            if delta.atoms() > 0 {
                resources.push(AssetAmount {
                    key: asset_key,
                    amount: delta,
                });
            }
        }

        self.next_sequence()?;
        if !capital.is_empty() {
            self.shadow_holds.insert(key, capital.clone());
        }
        self.modifications.insert(
            key,
            ModificationRecord {
                order: order_key.clone(),
                command_digest: command.digest(),
                manager,
                owner_revision: self.orders[&order_key].owner_revision,
                new_client_id: m.new_client_id.clone(),
                prior_price,
                requested_price: new_price,
                requested_quantity: new_qty,
                applied_price: None,
                applied_quantity: None,
                aborted: false,
                confirmed: false,
                superseded: false,
                rejected: None,
                retired_mono_ns: None,
            },
        );
        let r = &command.command().context.request;
        let admission = Admission {
            digest: command.digest(),
            key,
            epoch: self.epoch,
            sequence: self.sequence,
            revisions: self.revisions,
            deadline: r.deadline,
            resources,
            capital,
            orders: vec![],
            increases: true,
            retired_mono_ns: None,
        };
        self.admissions.insert(key, admission.clone());
        self.refresh_order_retirement(&order_key, now);
        Ok(admission)
    }

    pub(super) fn check_modification(
        &self,
        key: IntentKey,
        rec: &ModificationRecord,
    ) -> Result<()> {
        let o = self.orders.get(&rec.order).ok_or(Error::UnknownOrder)?;
        if o.manager != rec.manager || o.owner_revision != rec.owner_revision {
            return Err(Error::Identity);
        }
        if rec.aborted
            || rec.confirmed
            || rec.rejected.is_some()
            || o.pending_modify.as_ref().is_none_or(|pm| pm.intent != key)
            || o.observation_unreconciled
            || o.in_doubt.is_some()
            || o.unknown()
            || o.terminal
            || o.aborted
            || o.uncreated
            || o.filled.max(o.observed_cumulative)?.atoms() >= rec.requested_quantity.atoms()
        {
            return Err(Error::Unreconciled);
        }
        if self.stp_enforcement
            && self.stp_book.self_cross_excluding(
                o.instrument,
                o.spec.side,
                rec.requested_price,
                &rec.order,
            )?
        {
            return Err(Error::CrossesBook);
        }
        Ok(())
    }

    pub(super) fn abort_modification(&mut self, key: IntentKey) -> Result<Applied> {
        let rec = self.modifications.get(&key).ok_or(Error::UnknownOrder)?;
        if rec.confirmed || rec.rejected.is_some() {
            return self.freeze_error(Error::Unreconciled);
        }
        if rec.aborted {
            return Ok(Applied::Duplicate);
        }
        let order_key = rec.order.clone();
        self.rollback_pending_modify(&order_key, Some(key))?;
        self.shadow_holds.remove(&key);
        self.next_sequence()?;
        let stamp = self.last_mono_ns;
        if let Some(entry) = self.modifications.get_mut(&key) {
            entry.superseded = false;
            entry.aborted = true;
            entry.retired_mono_ns = Some(stamp);
        }
        if let Some(adm) = self.admissions.get_mut(&key) {
            adm.retired_mono_ns = Some(stamp);
        }
        self.sync_order_in_stp_book(&order_key)?;
        self.refresh_order_retirement(&order_key, stamp);
        Ok(Applied::New)
    }

    pub(super) fn observe_modified(
        &mut self,
        m: &v::ModifyObservation,
        instrument: u64,
    ) -> Result<Applied> {
        let p = self.catalog.get(&instrument).ok_or(Error::Catalog)?.clone();
        let price_scale = p
            .futures
            .as_ref()
            .map(|x| x.stress_high.scale())
            .unwrap_or(asset_scale(&p.instrument.quote)?);
        let applied_price = exact_decimal(m.new_price, price_scale)?;
        applied_price.positive()?;

        let (order_key, request_intent) = self.resolve_modify_event(
            instrument,
            &m.target,
            m.intent,
            m.prior_client_id.as_ref(),
            m.new_client_id.as_ref(),
            m.venue_order_id.as_ref(),
        )?;

        let base_scale = self.orders[&order_key].original.scale();
        let applied_qty = exact_decimal(m.new_quantity, base_scale)?;
        applied_qty.positive()?;

        if let Some(req) = request_intent
            && let Some(rec) = self.modifications.get(&req)
            && rec.confirmed
        {
            if rec.applied_price != Some(applied_price) || rec.applied_quantity != Some(applied_qty)
            {
                return Err(Error::Conflict);
            }
            if self.orders[&order_key].in_doubt == Some(DoubtReason::AmendAmbiguous) {
                self.orders.get_mut(&order_key).unwrap().in_doubt = None;
                let stamp = self.last_mono_ns;
                self.refresh_order_retirement(&order_key, stamp);
            }
            return Ok(Applied::Duplicate);
        }

        // Duplicate check for unsolicited venue modify (no local `request_intent`):
        let prior_order = self.orders[&order_key].clone();
        if request_intent.is_none()
            && prior_order.pending_modify.is_none()
            && prior_order.confirmed
            && prior_order.price == applied_price
            && prior_order.effective == applied_qty
            && m.new_client_id
                .as_ref()
                .is_none_or(|cid| &prior_order.spec.client_id == cid)
            && m.venue_order_id
                .as_ref()
                .is_none_or(|vid| prior_order.native.as_ref() == Some(vid))
            && m.cumulative_filled.is_none()
        {
            return Ok(Applied::Duplicate);
        }

        if self.reopen_order(&order_key)? {
            self.frozen = true;
        }

        // If an earlier `Order(Canceled)` for the prior native order leg arrived before a
        // Spot `cancelReplace` replacement `Modified` introducing a brand-new `venue_order_id`,
        // reopen the order slot for the live replacement leg (`evaluate()` below checks holds).
        if prior_order.terminal
            && prior_order.terminal_reason == Some(v::TerminalReason::UserCanceled)
            && m.venue_order_id
                .as_ref()
                .is_some_and(|vid| prior_order.native.is_some() && !prior_order.has_native_id(vid))
        {
            let o = self.orders.get_mut(&order_key).unwrap();
            o.terminal = false;
            o.terminal_cumulative = None;
            o.terminal_reason = None;
            o.fees_closed = false;
            o.retired_mono_ns = None;
        }

        if let Some(req) = request_intent
            && let Some(rec) = self.modifications.get(&req)
            && (rec.aborted || rec.rejected.is_some())
        {
            self.orders
                .get_mut(&order_key)
                .unwrap()
                .observation_unreconciled = true;
            self.frozen = true;
        }

        let tentative_cid = self.orders[&order_key]
            .pending_modify
            .as_ref()
            .and_then(|pm| pm.new_client_id.clone());

        if let Some(ref tent) = tentative_cid
            && m.new_client_id.as_ref() != Some(tent)
            && !self.orders[&order_key].has_client_id(tent)
        {
            self.remove_client_id_index(instrument, tent, &order_key);
        }

        let effective_new_cid = m.new_client_id.clone().or(tentative_cid);
        if let Some(ref cid) = effective_new_cid
            && cid != &self.orders[&order_key].spec.client_id
        {
            if self
                .orders_by_client_id
                .get(&(instrument, cid.clone()))
                .is_some_and(|keys| {
                    keys.iter()
                        .any(|k| k != &order_key && !self.orders[k].aborted)
                })
            {
                return Err(Error::Conflict);
            }
            let prev_cid = self.orders[&order_key].spec.client_id.clone();
            {
                let o = self.orders.get_mut(&order_key).unwrap();
                push_unique(&mut o.client_id_aliases, prev_cid);
                o.spec.client_id = cid.clone();
            }
            push_unique(
                self.orders_by_client_id
                    .entry((instrument, cid.clone()))
                    .or_default(),
                order_key.clone(),
            );
        }

        if let Some(ref vid) = m.venue_order_id {
            if self
                .orders_by_native_id
                .get(&(instrument, vid.clone()))
                .is_some_and(|keys| keys.iter().any(|k| k != &order_key))
            {
                return Err(Error::Conflict);
            }
            {
                let o = self.orders.get_mut(&order_key).unwrap();
                if let Some(prev_vid) = o.native.replace(vid.clone())
                    && prev_vid != *vid
                {
                    push_unique(&mut o.native_aliases, prev_vid);
                }
            }
            push_unique(
                self.orders_by_native_id
                    .entry((instrument, vid.clone()))
                    .or_default(),
                order_key.clone(),
            );
        }

        if m.target.order_id != [0; 16] {
            let existing_internal = self.orders[&order_key].internal_id;
            if existing_internal.is_some_and(|old| old != m.target.order_id) {
                return Err(Error::Conflict);
            }
            self.orders.get_mut(&order_key).unwrap().internal_id = Some(m.target.order_id);
            self.index_order_bind_native(instrument, &order_key, None, Some(m.target.order_id));
        }

        let cap_before_apply =
            if request_intent.is_some_and(|req| self.shadow_holds.contains_key(&req)) {
                Some(self.calculate_capital()?)
            } else {
                None
            };
        let prior_extra_credit = self.orders[&order_key]
            .pending_modify
            .as_ref()
            .map(|pm| pm.extra_shadow_fee_credit);

        {
            let o = self.orders.get_mut(&order_key).unwrap();
            let old_orig = o.original;
            let old_eff = o.effective;
            push_unique(&mut o.known_quantities, old_orig);
            push_unique(&mut o.known_quantities, old_eff);
            o.price = applied_price;
            o.effective = applied_qty;
            o.spec.price = Some(m.new_price);
            o.spec.quantity = v::Quantity::Base(m.new_quantity);
            o.pending_modify = None;
            o.confirmed = true;
            if o.filled.atoms() > applied_qty.atoms() {
                o.observation_unreconciled = true;
                self.frozen = true;
            }
            if o.in_doubt == Some(DoubtReason::AmendAmbiguous) {
                o.in_doubt = None;
            }
        }

        if let Some(cum_dec) = m.cumulative_filled {
            let cum = exact_decimal(cum_dec, base_scale)?;
            cum.nonnegative()?;
            if self.projected_fill_enabled {
                let _ = self.on_cum_qty(&order_key, cum, ProjectedPriceInput::LimitFallback)?;
            }
            if cum.atoms() > applied_qty.atoms() {
                self.orders
                    .get_mut(&order_key)
                    .unwrap()
                    .observation_unreconciled = true;
                self.frozen = true;
            }
        }

        {
            let rem = self.orders[&order_key].remaining()?;
            let o = self.orders.get_mut(&order_key).unwrap();
            o.reduce = o.reduce.min(rem)?;
            if !o.terminal && !o.observation_unreconciled && o.filled.atoms() == applied_qty.atoms()
            {
                o.terminal = true;
                o.terminal_cumulative = Some(o.filled);
                o.reduce = o.reduce.zero_like();
                o.terminal_reason = Some(v::TerminalReason::Filled);
            }
        }

        if let (Some(req), Some(cap_before)) = (request_intent, cap_before_apply) {
            let cap_after = self.calculate_capital()?;
            let mut remove_entry = false;
            let mut updated_wallet_hold = None;
            if let Some(holds) = self.shadow_holds.get_mut(&req) {
                for hold in holds.iter_mut() {
                    let w_before = cap_before
                        .get(&hold.wallet_id)
                        .map(|x| x.1)
                        .unwrap_or(hold.amount.zero_like());
                    let w_after = cap_after
                        .get(&hold.wallet_id)
                        .map(|x| x.1)
                        .unwrap_or(hold.amount.zero_like());
                    let release = w_before
                        .checked_sub(w_after)?
                        .positive_part()
                        .min(hold.amount)?;
                    hold.amount = hold.amount.checked_sub(release)?;
                    if hold.wallet_id == p.wallet_id {
                        updated_wallet_hold = Some(hold.amount);
                    }
                }
                holds.retain(|h| h.amount.atoms() > 0);
                remove_entry = holds.is_empty();
            }
            if remove_entry {
                self.shadow_holds.remove(&req);
            }
            if let Some(extra_cred) = prior_extra_credit
                && extra_cred.atoms() > 0
            {
                let kept = updated_wallet_hold.unwrap_or(extra_cred.zero_like());
                let trim = extra_cred.checked_sub(kept)?.positive_part();
                if trim.atoms() > 0 {
                    let o = self.orders.get_mut(&order_key).unwrap();
                    o.shadow_fee_credit = o.shadow_fee_credit.checked_sub(trim)?.positive_part();
                }
            }
        }

        let stamp = self.last_mono_ns;
        if let Some(req) = request_intent {
            if let Some(rec) = self.modifications.get_mut(&req) {
                rec.superseded = false;
                rec.confirmed = true;
                rec.applied_price = Some(applied_price);
                rec.applied_quantity = Some(applied_qty);
                rec.retired_mono_ns.get_or_insert(stamp);
            }
            if let Some(adm) = self.admissions.get_mut(&req) {
                adm.retired_mono_ns.get_or_insert(stamp);
            }
        }

        self.sync_order_in_stp_book(&order_key)?;
        self.refresh_order_retirement(&order_key, stamp);
        self.next_sequence()?;
        if self.evaluate().is_err() {
            self.frozen = true;
        }
        self.check_reduce_consistency()?;
        Ok(Applied::New)
    }

    pub(super) fn reject_modification(
        &mut self,
        r: &v::ModifyRejection,
        instrument: u64,
    ) -> Result<Applied> {
        let (order_key, request_intent) = self.resolve_modify_event(
            instrument,
            &r.target,
            r.intent,
            None,
            r.new_client_id.as_ref(),
            r.venue_order_id.as_ref(),
        )?;

        if let Some(req) = request_intent
            && let Some(rec) = self.modifications.get(&req)
        {
            if rec.confirmed || rec.aborted {
                return Err(Error::Conflict);
            }
            if let Some(old_reason) = rec.rejected {
                return if old_reason == r.reason {
                    if self.orders[&order_key].in_doubt == Some(DoubtReason::AmendAmbiguous) {
                        self.orders.get_mut(&order_key).unwrap().in_doubt = None;
                        let stamp = self.last_mono_ns;
                        self.refresh_order_retirement(&order_key, stamp);
                    }
                    Ok(Applied::Duplicate)
                } else {
                    Err(Error::Conflict)
                };
            }
        }

        self.rollback_pending_modify(&order_key, request_intent)?;
        if let Some(req) = request_intent {
            self.shadow_holds.remove(&req);
        }

        let stamp = self.last_mono_ns;
        {
            let o = self.orders.get_mut(&order_key).unwrap();
            o.last_modify_reject_reason = Some(r.reason);
            if o.in_doubt == Some(DoubtReason::AmendAmbiguous) {
                o.in_doubt = None;
            }
        }
        if let Some(req) = request_intent {
            if let Some(rec) = self.modifications.get_mut(&req) {
                rec.superseded = false;
                rec.rejected = Some(r.reason);
                rec.retired_mono_ns = Some(stamp);
            }
            if let Some(adm) = self.admissions.get_mut(&req) {
                adm.retired_mono_ns = Some(stamp);
            }
        }

        self.sync_order_in_stp_book(&order_key)?;
        self.refresh_order_retirement(&order_key, stamp);
        self.next_sequence()?;
        Ok(Applied::New)
    }

    pub fn order_pending_modify_intent(&self, key: &OrderKey) -> Option<IntentKey> {
        self.orders
            .get(key)?
            .pending_modify
            .as_ref()
            .map(|pm| pm.intent)
    }

    pub fn order_superseded_modify_intent(&self, key: &OrderKey) -> Option<IntentKey> {
        self.modifications.iter().rev().find_map(|(intent, m)| {
            (m.order == *key && m.superseded && !m.aborted && !m.confirmed && m.rejected.is_none())
                .then_some(*intent)
        })
    }

    pub(super) fn supersede_pending_modify_on_terminal(
        &mut self,
        order_key: &OrderKey,
    ) -> Result<()> {
        let Some(pm_intent) = self
            .orders
            .get(order_key)
            .and_then(|o| o.pending_modify.as_ref().map(|pm| pm.intent))
        else {
            return Ok(());
        };
        self.rollback_pending_modify(order_key, Some(pm_intent))?;
        let stamp = self.last_mono_ns;
        if let Some(rec) = self.modifications.get_mut(&pm_intent)
            && !rec.confirmed
            && !rec.aborted
            && rec.rejected.is_none()
        {
            rec.superseded = true;
            rec.retired_mono_ns.get_or_insert(stamp);
        }
        if let Some(adm) = self.admissions.get_mut(&pm_intent) {
            adm.retired_mono_ns.get_or_insert(stamp);
        }
        Ok(())
    }

    fn rollback_pending_modify(
        &mut self,
        order_key: &OrderKey,
        expected_intent: Option<IntentKey>,
    ) -> Result<()> {
        let should_take = self.orders[order_key]
            .pending_modify
            .as_ref()
            .is_some_and(|pm| expected_intent.is_none_or(|req| pm.intent == req));
        if !should_take {
            return Ok(());
        }
        let instrument = self.orders[order_key].instrument;
        let wallet_id = self.catalog[&instrument].wallet_id;
        let pm = self
            .orders
            .get_mut(order_key)
            .unwrap()
            .pending_modify
            .take()
            .unwrap();

        self.shadow_holds.remove(&pm.intent);

        let consumed_credit = {
            let o = self.orders.get_mut(order_key).unwrap();
            o.reduce = o.reduce.checked_sub(pm.extra_reduce)?.positive_part();
            let unused = o.shadow_fee_credit.min(pm.extra_shadow_fee_credit)?;
            o.shadow_fee_credit = o.shadow_fee_credit.checked_sub(unused)?;
            pm.extra_shadow_fee_credit.checked_sub(unused)?
        };
        if consumed_credit.atoms() > 0 {
            self.add_uncovered(wallet_id, consumed_credit)?;
        }

        if let Some(ref cid) = pm.new_client_id
            && !self.orders[order_key].has_client_id(cid)
        {
            self.remove_client_id_index(instrument, cid, order_key);
        }
        Ok(())
    }

    fn remove_client_id_index(&mut self, instrument: u64, cid: &v::ClientId, order_key: &OrderKey) {
        let map_key = (instrument, cid.clone());
        if let Some(vec) = self.orders_by_client_id.get_mut(&map_key) {
            vec.retain(|k| k != order_key);
            if vec.is_empty() {
                self.orders_by_client_id.remove(&map_key);
            }
        }
    }

    fn resolve_modify_event(
        &self,
        instrument: u64,
        target: &v::OrderTarget,
        intent: Option<IntentKey>,
        prior_cid: Option<&v::ClientId>,
        new_cid: Option<&v::ClientId>,
        venue_id: Option<&v::VenueOrderId>,
    ) -> Result<(OrderKey, Option<IntentKey>)> {
        if let Some(req) = intent
            && let Some(rec) = self.modifications.get(&req)
        {
            let o = self.orders.get(&rec.order).ok_or(Error::UnknownOrder)?;
            if o.instrument != instrument
                || o.key.intent.epoch != target.original_intent_exec_epoch
                || (target.order_id != [0; 16]
                    && o.internal_id.is_some_and(|id| id != target.order_id))
            {
                return Err(Error::Identity);
            }
            return Ok((rec.order.clone(), Some(req)));
        }

        let matches_order = |o: &Order| {
            o.instrument == instrument
                && o.key.intent.epoch == target.original_intent_exec_epoch
                && o.internal_id.is_none_or(|id| id == target.order_id)
        };

        let mut found: Option<OrderKey> = self.resolve_order_target(instrument, target);
        if found.is_none() {
            for cid in [prior_cid, new_cid].into_iter().flatten() {
                if let Some(keys) = self.orders_by_client_id.get(&(instrument, cid.clone()))
                    && let Some(k) = keys
                        .iter()
                        .find(|k| self.orders.get(*k).is_some_and(matches_order))
                {
                    found = Some(k.clone());
                    break;
                }
            }
        }
        if found.is_none()
            && let Some(vid) = venue_id
            && let Some(keys) = self.orders_by_native_id.get(&(instrument, vid.clone()))
            && let Some(k) = keys
                .iter()
                .find(|k| self.orders.get(*k).is_some_and(matches_order))
        {
            found = Some(k.clone());
        }

        let order_key = found.ok_or(Error::UnknownOrder)?;
        let resolved_intent = intent
            .or_else(|| self.order_pending_modify_intent(&order_key))
            .or_else(|| self.order_superseded_modify_intent(&order_key));
        Ok((order_key, resolved_intent))
    }

    pub(super) fn modification_state_digest(&self, strategy: Option<u64>) -> v::Hash {
        fn text(h: &mut Sha256, value: &str) {
            h.update((value.len() as u64).to_le_bytes());
            h.update(value.as_bytes());
        }
        fn fixed(h: &mut Sha256, value: Fixed) {
            h.update(value.atoms().to_le_bytes());
            h.update([value.scale()]);
        }
        let selected: Vec<_> = self
            .modifications
            .iter()
            .filter(|(_, m)| {
                strategy.is_none_or(|s| self.orders.get(&m.order).is_some_and(|o| o.strategy == s))
            })
            .collect();
        let mut h = Sha256::new();
        h.update(b"tickwire.modification-state.v1");
        h.update((selected.len() as u64).to_le_bytes());
        for (intent, m) in selected {
            for value in [
                intent.epoch,
                intent.intent_id,
                m.order.intent.epoch,
                m.order.intent.intent_id,
            ] {
                h.update(value.to_le_bytes());
            }
            text(&mut h, m.order.client_id.as_str());
            h.update(m.command_digest);
            h.update(m.manager.instance);
            h.update(m.manager.generation.to_le_bytes());
            h.update(m.owner_revision.to_le_bytes());
            fixed(&mut h, m.prior_price);
            fixed(&mut h, m.requested_price);
            fixed(&mut h, m.requested_quantity);
            h.update([u8::from(m.applied_price.is_some())]);
            if let Some(px) = m.applied_price {
                fixed(&mut h, px);
            }
            h.update([u8::from(m.applied_quantity.is_some())]);
            if let Some(q) = m.applied_quantity {
                fixed(&mut h, q);
            }
            h.update([
                u8::from(m.aborted),
                u8::from(m.confirmed),
                u8::from(m.superseded),
                u8::from(m.rejected.is_some()),
            ]);
            if let Some(reason) = m.rejected {
                text(&mut h, reason.as_str());
            }
        }
        h.finalize().into()
    }
}
