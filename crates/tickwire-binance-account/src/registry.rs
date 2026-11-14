use crate::*;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Copy)]
pub(crate) struct Key64 {
    len: u8,
    bytes: [u8; 64],
}

impl Key64 {
    pub(crate) fn from_str(s: &str) -> Option<Self> {
        let b = s.as_bytes();
        if b.len() > 64 {
            return None;
        }
        let mut bytes = [0u8; 64];
        bytes[..b.len()].copy_from_slice(b);
        Some(Self {
            len: b.len() as u8,
            bytes,
        })
    }

    pub(crate) fn from_u64(mut n: u64) -> Self {
        let mut tmp = [0u8; 20];
        let mut pos = tmp.len();
        if n == 0 {
            pos -= 1;
            tmp[pos] = b'0';
        } else {
            while n > 0 {
                pos -= 1;
                tmp[pos] = b'0' + (n % 10) as u8;
                n /= 10;
            }
        }
        let slice = &tmp[pos..];
        let mut bytes = [0u8; 64];
        bytes[..slice.len()].copy_from_slice(slice);
        Self {
            len: slice.len() as u8,
            bytes,
        }
    }

    #[inline]
    fn as_bytes(&self) -> &[u8] {
        &self.bytes[..self.len as usize]
    }
}

impl PartialEq for Key64 {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}

impl Eq for Key64 {}

impl PartialOrd for Key64 {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Key64 {
    #[inline]
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_bytes().cmp(other.as_bytes())
    }
}

#[derive(Clone)]
pub(crate) struct Registration {
    pub command: CanonicalCommand,
    pub ids: Vec<[u8; 16]>,
    pub orders: Vec<v::OrderIdentity>,
}

#[derive(Clone, Default)]
pub(crate) struct OrderRegistry {
    registrations: BTreeMap<IntentKey, Registration>,
    by_order_id: BTreeMap<[u8; 16], (IntentKey, usize)>,
    by_client_id: BTreeMap<(u64, Key64), (IntentKey, usize)>,
    by_venue_id: BTreeMap<(u64, Key64), (IntentKey, usize)>,
    by_list_id: BTreeMap<[u8; 16], IntentKey>,
    by_list_client_id: BTreeMap<Key64, IntentKey>,
    by_venue_list_id: BTreeMap<(u64, Key64), IntentKey>,
    by_parent_order_id: BTreeMap<[u8; 16], IntentKey>,
    by_algo_client_id: BTreeMap<Key64, IntentKey>,
    by_venue_algo_id: BTreeMap<(u64, Key64), IntentKey>,
    symbol_instruments: BTreeMap<Key64, BTreeSet<u64>>,
    symbol_instrument_counts: BTreeMap<(Key64, u64), usize>,
}

impl OrderRegistry {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.registrations.len()
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn is_empty(&self) -> bool {
        self.registrations.is_empty()
    }

    #[inline]
    pub(crate) fn contains_key(&self, key: &IntentKey) -> bool {
        self.registrations.contains_key(key)
    }

    #[inline]
    pub(crate) fn get(&self, key: &IntentKey) -> Option<&Registration> {
        self.registrations.get(key)
    }

    #[inline]
    pub(crate) fn values(
        &self,
    ) -> std::collections::btree_map::Values<'_, IntentKey, Registration> {
        self.registrations.values()
    }

    pub(crate) fn insert(&mut self, key: IntentKey, reg: Registration) -> Option<Registration> {
        let old = self.registrations.remove(&key);
        if let Some(ref prev) = old {
            self.unindex_registration(&key, prev);
        }
        self.index_registration(key, &reg);
        self.registrations.insert(key, reg);
        old
    }

    pub(crate) fn remove(&mut self, key: &IntentKey) -> Option<Registration> {
        let old = self.registrations.remove(key)?;
        self.unindex_registration(key, &old);
        Some(old)
    }

    fn index_registration(&mut self, key: IntentKey, reg: &Registration) {
        let inst = &reg.command.command().context.instrument;
        let inst_id = inst.instrument_id;
        if let Some(sym) = Key64::from_str(inst.symbol.as_str()) {
            let count = self
                .symbol_instrument_counts
                .entry((sym, inst_id))
                .or_insert(0);
            if *count == 0 {
                self.symbol_instruments
                    .entry(sym)
                    .or_default()
                    .insert(inst_id);
            }
            *count += 1;
        }
        for (idx, order) in reg.orders.iter().enumerate() {
            self.by_order_id.insert(order.order_id, (key, idx));
            if let Some(cid) = order
                .client_id
                .as_ref()
                .and_then(|c| Key64::from_str(c.as_str()))
            {
                self.by_client_id.insert((inst_id, cid), (key, idx));
            }
            if let Some(vid) = order
                .venue_order_id
                .as_ref()
                .and_then(|v| Key64::from_str(v.as_str()))
            {
                self.by_venue_id.insert((inst_id, vid), (key, idx));
            }
            if let Some(list_id) = order.list_id {
                self.by_list_id.insert(list_id, key);
            }
            if let Some(lcid) = order
                .list_client_id
                .as_ref()
                .and_then(|c| Key64::from_str(c.as_str()))
            {
                self.by_list_client_id.insert(lcid, key);
            }
            if let Some(vlid) = order
                .venue_list_id
                .as_ref()
                .and_then(|v| Key64::from_str(v.as_str()))
            {
                self.by_venue_list_id.insert((inst_id, vlid), key);
            }
            if let Some(parent_id) = order.parent_order_id {
                self.by_parent_order_id.insert(parent_id, key);
            }
            if let Some(acid) = order
                .algo_client_id
                .as_ref()
                .and_then(|c| Key64::from_str(c.as_str()))
            {
                self.by_algo_client_id.insert(acid, key);
            }
            if let Some(vaid) = order
                .venue_algo_id
                .as_ref()
                .and_then(|v| Key64::from_str(v.as_str()))
            {
                self.by_venue_algo_id.insert((inst_id, vaid), key);
            }
        }
    }

    fn unindex_registration(&mut self, key: &IntentKey, reg: &Registration) {
        let inst = &reg.command.command().context.instrument;
        let inst_id = inst.instrument_id;
        if let Some(sym) = Key64::from_str(inst.symbol.as_str())
            && let Some(count) = self.symbol_instrument_counts.get_mut(&(sym, inst_id))
        {
            *count = count.saturating_sub(1);
            if *count == 0 {
                self.symbol_instrument_counts.remove(&(sym, inst_id));
                if let Some(set) = self.symbol_instruments.get_mut(&sym) {
                    set.remove(&inst_id);
                    if set.is_empty() {
                        self.symbol_instruments.remove(&sym);
                    }
                }
            }
        }
        for (idx, order) in reg.orders.iter().enumerate() {
            if self.by_order_id.get(&order.order_id) == Some(&(*key, idx)) {
                self.by_order_id.remove(&order.order_id);
            }
            if let Some(cid) = order
                .client_id
                .as_ref()
                .and_then(|c| Key64::from_str(c.as_str()))
                && self.by_client_id.get(&(inst_id, cid)) == Some(&(*key, idx))
            {
                self.by_client_id.remove(&(inst_id, cid));
            }
            if let Some(vid) = order
                .venue_order_id
                .as_ref()
                .and_then(|v| Key64::from_str(v.as_str()))
                && self.by_venue_id.get(&(inst_id, vid)) == Some(&(*key, idx))
            {
                self.by_venue_id.remove(&(inst_id, vid));
            }
            if let Some(list_id) = order.list_id
                && self.by_list_id.get(&list_id) == Some(key)
            {
                self.by_list_id.remove(&list_id);
            }
            if let Some(lcid) = order
                .list_client_id
                .as_ref()
                .and_then(|c| Key64::from_str(c.as_str()))
                && self.by_list_client_id.get(&lcid) == Some(key)
            {
                self.by_list_client_id.remove(&lcid);
            }
            if let Some(vlid) = order
                .venue_list_id
                .as_ref()
                .and_then(|v| Key64::from_str(v.as_str()))
                && self.by_venue_list_id.get(&(inst_id, vlid)) == Some(key)
            {
                self.by_venue_list_id.remove(&(inst_id, vlid));
            }
            if let Some(parent_id) = order.parent_order_id
                && self.by_parent_order_id.get(&parent_id) == Some(key)
            {
                self.by_parent_order_id.remove(&parent_id);
            }
            if let Some(acid) = order
                .algo_client_id
                .as_ref()
                .and_then(|c| Key64::from_str(c.as_str()))
                && self.by_algo_client_id.get(&acid) == Some(key)
            {
                self.by_algo_client_id.remove(&acid);
            }
            if let Some(vaid) = order
                .venue_algo_id
                .as_ref()
                .and_then(|v| Key64::from_str(v.as_str()))
                && self.by_venue_algo_id.get(&(inst_id, vaid)) == Some(key)
            {
                self.by_venue_algo_id.remove(&(inst_id, vaid));
            }
        }
    }

    #[inline]
    pub(crate) fn by_list_client_id(&self, client_id: &str) -> Option<&Registration> {
        let k = Key64::from_str(client_id)?;
        let intent = self.by_list_client_id.get(&k)?;
        self.registrations.get(intent)
    }

    #[inline]
    pub(crate) fn by_algo_client_id(&self, client_id: &str) -> Option<&Registration> {
        let k = Key64::from_str(client_id)?;
        let intent = self.by_algo_client_id.get(&k)?;
        self.registrations.get(intent)
    }

    #[inline]
    pub(crate) fn by_list_id(&self, list_id: [u8; 16]) -> Option<&Registration> {
        let intent = self.by_list_id.get(&list_id)?;
        self.registrations.get(intent)
    }

    #[inline]
    pub(crate) fn by_parent_order_id(&self, parent_order_id: [u8; 16]) -> Option<&Registration> {
        let intent = self.by_parent_order_id.get(&parent_order_id)?;
        self.registrations.get(intent)
    }

    /// Resolves `(Registration, OrderIdentity)` candidates for a private order frame in $O(\log N)$.
    /// Preserves exact precedence: native `order_id` matches take priority over `client_id` / `original_client_id`.
    pub(crate) fn lookup_private_order<'a>(
        &'a self,
        symbol: &str,
        order_id: u64,
        client_id: &str,
        original_client_id: Option<&str>,
    ) -> Vec<(&'a Registration, &'a v::OrderIdentity)> {
        let Some(sym_key) = Key64::from_str(symbol) else {
            return Vec::new();
        };
        let Some(inst_ids) = self.symbol_instruments.get(&sym_key) else {
            return Vec::new();
        };
        let native_key = Key64::from_u64(order_id);
        let mut native_refs: Vec<(IntentKey, usize)> = Vec::new();
        for &inst_id in inst_ids {
            if let Some(&(intent, idx)) = self.by_venue_id.get(&(inst_id, native_key))
                && let Some(reg) = self.registrations.get(&intent)
                && reg.command.command().context.instrument.symbol.as_str() == symbol
                && !native_refs.contains(&(intent, idx))
            {
                native_refs.push((intent, idx));
            }
        }
        if !native_refs.is_empty() {
            native_refs.sort_unstable();
            return native_refs
                .into_iter()
                .filter_map(|(intent, idx)| {
                    let reg = self.registrations.get(&intent)?;
                    Some((reg, reg.orders.get(idx)?))
                })
                .collect();
        }
        let cid_key = Key64::from_str(client_id);
        let orig_key = original_client_id.and_then(Key64::from_str);
        let mut client_refs: Vec<(IntentKey, usize)> = Vec::new();
        for &inst_id in inst_ids {
            for candidate_key in [cid_key, orig_key].into_iter().flatten() {
                if let Some(&(intent, idx)) = self.by_client_id.get(&(inst_id, candidate_key))
                    && let Some(reg) = self.registrations.get(&intent)
                    && reg.command.command().context.instrument.symbol.as_str() == symbol
                    && !client_refs.contains(&(intent, idx))
                {
                    client_refs.push((intent, idx));
                }
            }
        }
        client_refs.sort_unstable();
        client_refs
            .into_iter()
            .filter_map(|(intent, idx)| {
                let reg = self.registrations.get(&intent)?;
                Some((reg, reg.orders.get(idx)?))
            })
            .collect()
    }

    /// Resolves open-order snapshot row matches in $O(\log N)$ without scanning all registrations.
    pub(crate) fn lookup_open_order<'a>(
        &'a self,
        symbol: &str,
        order_id: u64,
        client_order_id: &str,
    ) -> Vec<(IntentKey, &'a Registration, usize)> {
        let Some(sym_key) = Key64::from_str(symbol) else {
            return Vec::new();
        };
        let Some(inst_ids) = self.symbol_instruments.get(&sym_key) else {
            return Vec::new();
        };
        let native_key = Key64::from_u64(order_id);
        let mut out = Vec::new();
        for &inst_id in inst_ids {
            if let Some(&(intent, idx)) = self.by_venue_id.get(&(inst_id, native_key))
                && let Some(reg) = self.registrations.get(&intent)
                && reg.command.command().context.instrument.symbol.as_str() == symbol
                && let Some(o) = reg.orders.get(idx)
                && (o.venue_algo_id.is_some()
                    || o.client_id
                        .as_ref()
                        .is_some_and(|id| id.as_str() == client_order_id))
                && !out.iter().any(|(k, _, i)| *k == intent && *i == idx)
            {
                out.push((intent, reg, idx));
            }
        }
        out.sort_by_key(|(k, _, idx)| (*k, *idx));
        out
    }

    /// Collects candidate `IntentKey`s for a REST/WS order reply in $O(\log N)$.
    pub(crate) fn candidate_intents_for_symbol(
        &self,
        symbol: &str,
        order_id: u64,
        client_id: &str,
    ) -> BTreeSet<IntentKey> {
        let mut out = BTreeSet::new();
        let Some(sym_key) = Key64::from_str(symbol) else {
            return out;
        };
        let Some(inst_ids) = self.symbol_instruments.get(&sym_key) else {
            return out;
        };
        let native_key = Key64::from_u64(order_id);
        let cid_key = Key64::from_str(client_id);
        for &inst_id in inst_ids {
            if let Some(&(intent, _)) = self.by_venue_id.get(&(inst_id, native_key)) {
                out.insert(intent);
            }
            if let Some(ck) = cid_key
                && let Some(&(intent, _)) = self.by_client_id.get(&(inst_id, ck))
            {
                out.insert(intent);
            }
        }
        out
    }

    pub(crate) fn check_learn(&self, event: &CanonicalEvent) -> Result<()> {
        let _ = validate_learn(self, None, event)?;
        Ok(())
    }

    pub(crate) fn learn(&mut self, event: &CanonicalEvent) -> Result<()> {
        let (key, updated) = validate_learn(self, None, event)?;
        self.insert(key, updated);
        Ok(())
    }

    pub(crate) fn learn_with_overrides(
        &self,
        overrides: &mut BTreeMap<IntentKey, Registration>,
        event: &CanonicalEvent,
    ) -> Result<()> {
        let (key, updated) = validate_learn(self, Some(overrides), event)?;
        overrides.insert(key, updated);
        Ok(())
    }
}

impl std::ops::Index<&IntentKey> for OrderRegistry {
    type Output = Registration;
    fn index(&self, index: &IntentKey) -> &Self::Output {
        &self.registrations[index]
    }
}

impl<'a> IntoIterator for &'a OrderRegistry {
    type Item = (&'a IntentKey, &'a Registration);
    type IntoIter = std::collections::btree_map::Iter<'a, IntentKey, Registration>;
    fn into_iter(self) -> Self::IntoIter {
        self.registrations.iter()
    }
}

pub(crate) fn prepare(
    config: &Config,
    registry: &OrderRegistry,
    command: &CanonicalCommand,
    ids: &[[u8; 16]],
    fresh: bool,
) -> Result<Registration> {
    let c = command.command();
    let r = &c.context.request;
    let scope = &config.baseline.snapshot().scope;
    if r.risk_domain_id != scope.risk_domain_id
        || r.venue != scope.venue
        || r.environment != scope.environment
        || r.account != scope.account
        || r.product != product(config)?
        || r.intent_exec_epoch > config.writer_epoch
        || (fresh && r.intent_exec_epoch != config.writer_epoch)
        || !config
            .baseline
            .catalog()
            .entries
            .iter()
            .any(|p| p.instrument == c.context.instrument && p.product == r.product)
    {
        return Err(Error::Identity);
    }
    if registry.len() >= config.max_registrations {
        return Err(Error::Capacity);
    }
    let (specs, list, algo) = match &c.body {
        v::CommandBody::New(o) | v::CommandBody::CancelReplace { replacement: o, .. } => {
            (vec![o], None, None)
        }
        v::CommandBody::SpotList(l) => (l.legs.as_slice().iter().collect(), Some(l), None),
        v::CommandBody::Algo(a) => (vec![&a.order], None, Some(a)),
        _ => return Err(Error::Unsupported),
    };
    if ids.len() != specs.len() || ids.is_empty() || ids.len() > 3 {
        return Err(Error::Identity);
    }
    let inst_id = c.context.instrument.instrument_id;
    let mut orders = Vec::with_capacity(ids.len());
    for (index, (id, spec)) in ids.iter().zip(specs).enumerate() {
        if *id == [0; 16] || ids[..index].contains(id) {
            return Err(Error::Identity);
        }
        if registry.by_order_id.contains_key(id) {
            return Err(Error::Conflict);
        }
        let cid_key = Key64::from_str(spec.client_id.as_str()).ok_or(Error::Identity)?;
        if registry.by_client_id.contains_key(&(inst_id, cid_key)) {
            return Err(Error::Conflict);
        }
        if let Some(l) = list {
            let lcid = Key64::from_str(l.client_id.as_str()).ok_or(Error::Identity)?;
            if registry.by_list_id.contains_key(&l.list_id)
                || registry.by_list_client_id.contains_key(&lcid)
            {
                return Err(Error::Conflict);
            }
        }
        if let Some(a) = algo {
            let acid = Key64::from_str(a.order.client_id.as_str()).ok_or(Error::Identity)?;
            if registry.by_parent_order_id.contains_key(&a.parent_order_id)
                || registry.by_algo_client_id.contains_key(&acid)
            {
                return Err(Error::Conflict);
            }
        }
        if orders
            .iter()
            .any(|o: &v::OrderIdentity| o.client_id.as_ref() == Some(&spec.client_id))
        {
            return Err(Error::Conflict);
        }
        orders.push(v::OrderIdentity {
            order_id: *id,
            original_intent_id: r.intent_id,
            original_intent_exec_epoch: r.intent_exec_epoch,
            client_id: Some(spec.client_id.clone()),
            venue_order_id: None,
            parent_order_id: algo.map(|a| a.parent_order_id),
            list_id: list.map(|l| l.list_id),
            venue_algo_id: None,
            venue_list_id: None,
            list_client_id: list.map(|l| l.client_id.clone()),
            algo_client_id: algo.map(|a| a.order.client_id.clone()),
        });
    }
    Ok(Registration {
        command: command.clone(),
        ids: ids.to_vec(),
        orders,
    })
}

pub(crate) fn order(event: &CanonicalEvent) -> Result<&v::OrderIdentity> {
    match &event.event().body {
        v::VenueEventBody::AmendmentRejected(a) => Ok(&a.order),
        v::VenueEventBody::Amendment(a) => Ok(&a.order),
        v::VenueEventBody::Order(o) => Ok(&o.order),
        v::VenueEventBody::Fill(f) => Ok(&f.observation.order),
        _ => Err(Error::Unsupported),
    }
}

pub(crate) fn learn(registry: &mut OrderRegistry, event: &CanonicalEvent) -> Result<()> {
    registry.learn(event)
}

fn get_reg<'a>(
    registry: &'a OrderRegistry,
    overrides: Option<&'a BTreeMap<IntentKey, Registration>>,
    key: &IntentKey,
) -> Option<&'a Registration> {
    if let Some(ov) = overrides
        && let Some(r) = ov.get(key)
    {
        return Some(r);
    }
    registry.get(key)
}

fn check_venue_order_conflict(
    registry: &OrderRegistry,
    overrides: Option<&BTreeMap<IntentKey, Registration>>,
    inst_id: u64,
    own_key: IntentKey,
    own_order_id: [u8; 16],
    venue_order_id: &v::VenueOrderId,
) -> Result<()> {
    let vid_key = Key64::from_str(venue_order_id.as_str()).ok_or(Error::Identity)?;
    if let Some(&(other_key, other_idx)) = registry.by_venue_id.get(&(inst_id, vid_key)) {
        if let Some(ov) = overrides
            && ov.contains_key(&other_key)
        {
            // Checked via `overrides` loop below.
        } else if other_key != own_key
            || registry
                .get(&other_key)
                .and_then(|r| r.orders.get(other_idx))
                .is_none_or(|o| o.order_id != own_order_id)
        {
            return Err(Error::Conflict);
        }
    }
    if let Some(ov) = overrides {
        for (other_key, other_reg) in ov {
            if other_reg.command.command().context.instrument.instrument_id == inst_id
                && other_reg.orders.iter().any(|other| {
                    other.order_id != own_order_id
                        && other.venue_order_id.as_ref() == Some(venue_order_id)
                })
            {
                let _ = other_key;
                return Err(Error::Conflict);
            }
        }
    }
    Ok(())
}

fn check_venue_parent_conflict(
    registry: &OrderRegistry,
    overrides: Option<&BTreeMap<IntentKey, Registration>>,
    inst_id: u64,
    own_key: IntentKey,
    algo: bool,
    venue_parent_id: &v::VenueOrderId,
) -> Result<()> {
    let pid_key = Key64::from_str(venue_parent_id.as_str()).ok_or(Error::Identity)?;
    let indexed = if algo {
        registry.by_venue_algo_id.get(&(inst_id, pid_key))
    } else {
        registry.by_venue_list_id.get(&(inst_id, pid_key))
    };
    if let Some(&other_key) = indexed {
        if let Some(ov) = overrides
            && ov.contains_key(&other_key)
        {
            // Checked via `overrides` loop below.
        } else if other_key != own_key {
            return Err(Error::Conflict);
        }
    }
    if let Some(ov) = overrides {
        for (other_key, other_reg) in ov {
            if *other_key != own_key
                && other_reg.command.command().context.instrument.instrument_id == inst_id
                && other_reg.orders.iter().any(|old| {
                    (if algo {
                        old.venue_algo_id.as_ref()
                    } else {
                        old.venue_list_id.as_ref()
                    }) == Some(venue_parent_id)
                })
            {
                return Err(Error::Conflict);
            }
        }
    }
    Ok(())
}

fn validate_learn(
    registry: &OrderRegistry,
    overrides: Option<&BTreeMap<IntentKey, Registration>>,
    event: &CanonicalEvent,
) -> Result<(IntentKey, Registration)> {
    if let v::VenueEventBody::Structure(s) = &event.event().body {
        return validate_learn_structure(registry, overrides, event, s);
    }
    if let v::VenueEventBody::Rejected { client_id, .. } = &event.event().body {
        return validate_learn_rejected(registry, overrides, event, client_id.as_ref());
    }
    let id = order(event)?;
    let key = IntentKey {
        epoch: id.original_intent_exec_epoch,
        intent_id: id.original_intent_id,
    };
    let reg = get_reg(registry, overrides, &key).ok_or(Error::Identity)?;
    let instrument = &reg.command.command().context.instrument;
    if instrument.instrument_id != event.event().identity.instrument_id
        || instrument.instrument_version != event.event().identity.instrument_version
    {
        return Err(Error::Identity);
    }
    let index = reg
        .orders
        .iter()
        .position(|o| o.order_id == id.order_id)
        .ok_or(Error::Identity)?;
    let old = &reg.orders[index];
    if old.client_id != id.client_id
        || old.list_id != id.list_id
        || old.list_client_id != id.list_client_id
        || old.parent_order_id != id.parent_order_id
        || old.algo_client_id != id.algo_client_id
        || old.venue_algo_id != id.venue_algo_id
        || old
            .venue_order_id
            .as_ref()
            .is_some_and(|n| Some(n) != id.venue_order_id.as_ref())
        || old
            .venue_list_id
            .as_ref()
            .is_some_and(|n| Some(n) != id.venue_list_id.as_ref())
        || id.venue_order_id.is_none()
    {
        return Err(Error::Conflict);
    }
    let vid = id.venue_order_id.as_ref().unwrap();
    if reg
        .orders
        .iter()
        .any(|other| other.order_id != id.order_id && other.venue_order_id.as_ref() == Some(vid))
    {
        return Err(Error::Conflict);
    }
    check_venue_order_conflict(
        registry,
        overrides,
        instrument.instrument_id,
        key,
        id.order_id,
        vid,
    )?;
    if let Some(vlid) = id.venue_list_id.as_ref() {
        check_venue_parent_conflict(
            registry,
            overrides,
            instrument.instrument_id,
            key,
            false,
            vlid,
        )?;
    }
    let mut updated = reg.clone();
    updated.orders[index] = id.clone();
    Ok((key, updated))
}

fn validate_learn_structure(
    registry: &OrderRegistry,
    overrides: Option<&BTreeMap<IntentKey, Registration>>,
    event: &CanonicalEvent,
    observation: &v::StructureObservation,
) -> Result<(IntentKey, Registration)> {
    let parent = &observation.parent;
    let key = IntentKey {
        epoch: parent.original_intent_exec_epoch,
        intent_id: parent.original_intent_id,
    };
    let reg = get_reg(registry, overrides, &key).ok_or(Error::Identity)?;
    let context = &reg.command.command().context;
    let identity = &event.event().identity;
    if context.request.risk_domain_id != identity.risk_domain_id
        || context.request.venue != identity.venue
        || context.request.account != identity.account
        || context.request.environment != identity.environment
        || context.request.product != identity.product
        || context.instrument.instrument_id != identity.instrument_id
        || context.instrument.instrument_version != identity.instrument_version
    {
        return Err(Error::Identity);
    }
    let algo = match &reg.command.command().body {
        v::CommandBody::SpotList(l) => {
            let kind = match l.kind {
                v::ListKind::Oco => v::StructureKind::Oco,
                v::ListKind::Oto => v::StructureKind::Oto,
                v::ListKind::Otoco => v::StructureKind::Otoco,
            };
            if parent.structure_id != l.list_id
                || parent.client_id != l.client_id
                || parent.kind != kind
            {
                return Err(Error::Identity);
            }
            false
        }
        v::CommandBody::Algo(a) => {
            if parent.structure_id != a.parent_order_id
                || parent.client_id != a.order.client_id
                || parent.kind != v::StructureKind::Algo
            {
                return Err(Error::Identity);
            }
            true
        }
        _ => return Err(Error::Identity),
    };
    let mut updated = reg.clone();
    for id in &mut updated.orders {
        let old = if algo {
            &mut id.venue_algo_id
        } else {
            &mut id.venue_list_id
        };
        if let Some(native) = &parent.venue_parent_id {
            if old.as_ref().is_some_and(|old| old != native) {
                return Err(Error::Conflict);
            }
            *old = Some(native.clone());
        }
    }
    for child in observation.children.as_slice() {
        let index = updated
            .orders
            .iter()
            .position(|o| o.order_id == child.order.order_id)
            .ok_or(Error::Identity)?;
        let old = &updated.orders[index];
        let mut expected = old.clone();
        expected.venue_order_id = child.order.venue_order_id.clone();
        if child.order != expected
            || child.order.venue_order_id.is_none()
            || old
                .venue_order_id
                .as_ref()
                .is_some_and(|old| Some(old) != child.order.venue_order_id.as_ref())
        {
            return Err(Error::Conflict);
        }
        updated.orders[index] = child.order.clone();
    }
    if let Some(native_parent) = parent.venue_parent_id.as_ref() {
        check_venue_parent_conflict(
            registry,
            overrides,
            identity.instrument_id,
            key,
            algo,
            native_parent,
        )?;
    }
    for (i, new) in updated.orders.iter().enumerate() {
        if let Some(vid) = new.venue_order_id.as_ref() {
            if updated.orders[..i].iter().any(|prev| {
                prev.order_id != new.order_id && prev.venue_order_id.as_ref() == Some(vid)
            }) {
                return Err(Error::Conflict);
            }
            check_venue_order_conflict(
                registry,
                overrides,
                identity.instrument_id,
                key,
                new.order_id,
                vid,
            )?;
        }
    }
    Ok((key, updated))
}

fn validate_learn_rejected(
    registry: &OrderRegistry,
    overrides: Option<&BTreeMap<IntentKey, Registration>>,
    event: &CanonicalEvent,
    client_id: Option<&v::ClientId>,
) -> Result<(IntentKey, Registration)> {
    let Some(cid) = client_id else {
        return Err(Error::Identity);
    };
    let identity = &event.event().identity;
    let cid_key = Key64::from_str(cid.as_str()).ok_or(Error::Identity)?;
    let &(key, idx) = registry
        .by_client_id
        .get(&(identity.instrument_id, cid_key))
        .ok_or(Error::Identity)?;
    let reg = get_reg(registry, overrides, &key).ok_or(Error::Identity)?;
    let context = &reg.command.command().context;
    if context.request.risk_domain_id != identity.risk_domain_id
        || context.request.venue != identity.venue
        || context.request.account != identity.account
        || context.request.environment != identity.environment
        || context.request.product != identity.product
        || context.instrument.instrument_id != identity.instrument_id
        || context.instrument.instrument_version != identity.instrument_version
        || !matches!(reg.command.command().body, v::CommandBody::New(_))
        || reg.orders.len() != 1
        || idx != 0
        || reg.orders[0].client_id.as_ref() != Some(cid)
        || reg.orders[0].venue_order_id.is_some()
    {
        return Err(Error::Conflict);
    }
    Ok((key, reg.clone()))
}
