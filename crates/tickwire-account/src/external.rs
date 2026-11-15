use super::*;
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ExternalValue {
    Cash(AssetAmount),
    Position {
        instrument: u64,
        side: v::PositionSide,
        quantity: Fixed,
        entry: Fixed,
    },
}
impl Account {
    /// An unexplained external credit is booked to ownership 0 but never increases the venue cap.
    /// External debits reduce available funds immediately. Both require reconciliation.
    pub fn apply_unattributed_cashflow(
        &mut self,
        id: &v::BoundedText<64>,
        delta: AssetAmount,
    ) -> Result<Applied> {
        let value = ExternalValue::Cash(delta);
        let key = (0, id.as_str().to_owned());
        if let Some(old) = self.external_facts.get(&key) {
            if old == &value {
                return Ok(Applied::Duplicate);
            }
            return self.freeze_error(Error::Conflict);
        }
        self.frozen = true;
        if self.external_facts.len() >= MAX_FACTS {
            return Err(Error::Capacity);
        }
        self.sequence
            .checked_add(1)
            .ok_or(RiskError::ArithmeticOverflow)?;
        let (new_cashflow, new_other_debits, new_owner_0) = {
            let a = self.assets.get(&delta.key).ok_or(Error::Identity)?;
            let new_cashflow = a.cashflow.checked_add(delta.amount)?;
            let new_other_debits = if delta.amount.atoms() < 0 {
                a.other_debits.checked_add(delta.amount.checked_neg()?)?
            } else {
                a.other_debits
            };
            let old_owner = a
                .ownership
                .get(&0)
                .copied()
                .unwrap_or(delta.amount.zero_like());
            let new_owner_0 = old_owner.checked_add(delta.amount)?;
            (new_cashflow, new_other_debits, new_owner_0)
        };
        let uncovered_update = if delta.amount.atoms() < 0
            && self.venue_available.contains_key(&delta.key.wallet_id)
        {
            let wallet = delta.key.wallet_id;
            let settlement = self
                .catalog
                .values()
                .find(|p| p.wallet_id == wallet && p.futures.is_some())
                .map(|p| p.instrument.settlement.id);
            if settlement == Some(delta.key.asset_id) {
                let old = self
                    .uncovered_debits
                    .get(&wallet)
                    .copied()
                    .unwrap_or(delta.amount.zero_like());
                Some(Ok((wallet, old.checked_sub(delta.amount)?)))
            } else {
                Some(Err(wallet))
            }
        } else {
            None
        };

        let a = self.assets.get_mut(&delta.key).unwrap();
        a.cashflow = new_cashflow;
        a.other_debits = new_other_debits;
        a.ownership.insert(0, new_owner_0);
        match uncovered_update {
            Some(Ok((wallet, amount))) => {
                self.uncovered_debits.insert(wallet, amount);
            }
            Some(Err(wallet)) => {
                self.shadow_unvalued.insert(wallet);
            }
            None => {}
        }
        self.external_facts.insert(key, value);
        self.next_sequence()?;
        Ok(Applied::New)
    }
    /// Replaces the observed actual position and puts the delta into the unattributed bucket.
    /// Reservations stay in place, including reduction tickets made inconsistent by the update.
    pub fn apply_external_position(
        &mut self,
        id: &v::BoundedText<64>,
        instrument: u64,
        side: v::PositionSide,
        quantity: Fixed,
        entry: Fixed,
    ) -> Result<Applied> {
        let value = ExternalValue::Position {
            instrument,
            side,
            quantity,
            entry,
        };
        let fact = (1, id.as_str().to_owned());
        if let Some(old) = self.external_facts.get(&fact) {
            if old == &value {
                return Ok(Applied::Duplicate);
            }
            return self.freeze_error(Error::Conflict);
        }
        self.frozen = true;
        if self.external_facts.len() >= MAX_FACTS {
            return Err(Error::Capacity);
        }
        self.sequence
            .checked_add(1)
            .ok_or(RiskError::ArithmeticOverflow)?;
        let p = self.catalog.get(&instrument).ok_or(Error::Catalog)?;
        let mode = p.futures.as_ref().ok_or(Error::Unsupported)?.position_mode;
        if (side == v::PositionSide::Both) != (mode == v::PositionMode::OneWay)
            || quantity.scale() != asset_scale(&p.instrument.base)?
        {
            return Err(Error::Identity);
        }
        entry.positive()?;
        let pos_key = (instrument, side_code(side));
        let (prior_qty, prior_owner_0) = match self.positions.get(&pos_key) {
            Some(existing) => (
                existing.quantity,
                existing
                    .ownership
                    .get(&0)
                    .copied()
                    .unwrap_or(quantity.zero_like()),
            ),
            None => (quantity.zero_like(), quantity.zero_like()),
        };
        let delta_qty = quantity.checked_sub(prior_qty)?;
        let new_owner_0 = prior_owner_0.checked_add(delta_qty)?;

        let position = self.positions.entry(pos_key).or_insert(Position {
            quantity: quantity.zero_like(),
            entry,
            ownership: BTreeMap::new(),
        });
        position.ownership.insert(0, new_owner_0);
        position.quantity = quantity;
        position.entry = entry;
        self.external_facts.insert(fact, value);
        self.next_sequence()?;
        Ok(Applied::New)
    }
    /// Signed quantity moves between virtual owners; the actual position is unchanged.
    pub fn transfer_position_attribution(
        &mut self,
        instrument: u64,
        side: v::PositionSide,
        from: u64,
        to: u64,
        quantity: Fixed,
    ) -> Result<()> {
        if from == to {
            return Err(Error::Identity);
        }
        self.sequence
            .checked_add(1)
            .ok_or(RiskError::ArithmeticOverflow)?;
        let pos_key = (instrument, side_code(side));
        let (new_from, new_to) = {
            let position = self.positions.get(&pos_key).ok_or(Error::Identity)?;
            let old_from = position
                .ownership
                .get(&from)
                .copied()
                .unwrap_or(quantity.zero_like());
            let old_to = position
                .ownership
                .get(&to)
                .copied()
                .unwrap_or(quantity.zero_like());
            (
                old_from.checked_sub(quantity)?,
                old_to.checked_add(quantity)?,
            )
        };
        let position = self.positions.get_mut(&pos_key).unwrap();
        position.ownership.insert(from, new_from);
        position.ownership.insert(to, new_to);
        self.next_sequence()?;
        if self.check_limits().is_err() {
            self.frozen = true;
        }
        Ok(())
    }
}
