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
        let mut candidate = self.clone();
        let a = candidate
            .assets
            .get_mut(&delta.key)
            .ok_or(Error::Identity)?;
        a.cashflow = a.cashflow.checked_add(delta.amount)?;
        if delta.amount.atoms() < 0 {
            a.other_debits = a.other_debits.checked_add(delta.amount.checked_neg()?)?;
        }
        add_owner(&mut a.ownership, 0, delta.amount)?;
        if delta.amount.atoms() < 0 && candidate.venue_available.contains_key(&delta.key.wallet_id)
        {
            let wallet = delta.key.wallet_id;
            let settlement = candidate
                .catalog
                .values()
                .find(|p| p.wallet_id == wallet && p.futures.is_some())
                .map(|p| p.instrument.settlement.id);
            if settlement == Some(delta.key.asset_id) {
                let old = candidate
                    .uncovered_debits
                    .get(&wallet)
                    .copied()
                    .unwrap_or(delta.amount.zero_like());
                candidate
                    .uncovered_debits
                    .insert(wallet, old.checked_sub(delta.amount)?);
            } else {
                candidate.shadow_unvalued.insert(wallet);
            }
        }
        candidate.external_facts.insert(key, value);
        candidate.next_sequence()?;
        *self = candidate;
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
        let mut candidate = self.clone();
        let p = candidate.catalog.get(&instrument).ok_or(Error::Catalog)?;
        let mode = p.futures.as_ref().ok_or(Error::Unsupported)?.position_mode;
        if (side == v::PositionSide::Both) != (mode == v::PositionMode::OneWay)
            || quantity.scale() != asset_scale(&p.instrument.base)?
        {
            return Err(Error::Identity);
        }
        entry.positive()?;
        let position = candidate
            .positions
            .entry((instrument, side_code(side)))
            .or_insert(Position {
                quantity: quantity.zero_like(),
                entry,
                ownership: BTreeMap::new(),
            });
        add_owner(
            &mut position.ownership,
            0,
            quantity.checked_sub(position.quantity)?,
        )?;
        position.quantity = quantity;
        position.entry = entry;
        candidate.external_facts.insert(fact, value);
        candidate.next_sequence()?;
        *self = candidate;
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
        let mut candidate = self.clone();
        let position = candidate
            .positions
            .get_mut(&(instrument, side_code(side)))
            .ok_or(Error::Identity)?;
        transfer(&mut position.ownership, from, to, quantity)?;
        candidate.next_sequence()?;
        if candidate.check_limits().is_err() {
            candidate.frozen = true;
        }
        *self = candidate;
        Ok(())
    }
}
