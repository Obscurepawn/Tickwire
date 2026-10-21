use crate::*;

#[derive(Clone)]
pub(crate) struct Registration {
    pub command: CanonicalCommand,
    pub ids: Vec<[u8; 16]>,
    pub orders: Vec<v::OrderIdentity>,
}

pub(crate) fn prepare(
    config: &Config,
    registry: &BTreeMap<IntentKey, Registration>,
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
    let mut orders = Vec::new();
    for (index, (id, spec)) in ids.iter().zip(specs).enumerate() {
        if *id == [0; 16] || ids[..index].contains(id) {
            return Err(Error::Identity);
        }
        for reg in registry.values() {
            if reg.ids.contains(id) {
                return Err(Error::Conflict);
            }
            if reg.command.command().context.instrument.instrument_id
                == c.context.instrument.instrument_id
                && reg
                    .orders
                    .iter()
                    .any(|o| o.client_id.as_ref() == Some(&spec.client_id))
            {
                return Err(Error::Conflict);
            }
            if let Some(l) = list
                && reg.orders.iter().any(|o| {
                    o.list_id == Some(l.list_id) || o.list_client_id.as_ref() == Some(&l.client_id)
                })
            {
                return Err(Error::Conflict);
            }
            if let Some(a) = algo
                && reg.orders.iter().any(|o| {
                    o.parent_order_id == Some(a.parent_order_id)
                        || o.algo_client_id.as_ref() == Some(&a.order.client_id)
                })
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
pub(crate) fn learn(
    registry: &mut BTreeMap<IntentKey, Registration>,
    event: &CanonicalEvent,
) -> Result<()> {
    if let v::VenueEventBody::Structure(s) = &event.event().body {
        return learn_structure(registry, event, s);
    }
    let id = order(event)?;
    let key = IntentKey {
        epoch: id.original_intent_exec_epoch,
        intent_id: id.original_intent_id,
    };
    let reg = registry.get(&key).ok_or(Error::Identity)?;
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
    for candidate in registry.values() {
        if candidate.command.command().context.instrument.instrument_id == instrument.instrument_id
            && candidate.orders.iter().any(|other| {
                other.order_id != id.order_id && other.venue_order_id == id.venue_order_id
            })
        {
            return Err(Error::Conflict);
        }
    }
    registry.get_mut(&key).unwrap().orders[index] = id.clone();
    Ok(())
}

fn learn_structure(
    registry: &mut BTreeMap<IntentKey, Registration>,
    event: &CanonicalEvent,
    observation: &v::StructureObservation,
) -> Result<()> {
    let parent = &observation.parent;
    let key = IntentKey {
        epoch: parent.original_intent_exec_epoch,
        intent_id: parent.original_intent_id,
    };
    let reg = registry.get(&key).ok_or(Error::Identity)?;
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
    for (other_key, other) in registry.iter() {
        if other.command.command().context.instrument.instrument_id != identity.instrument_id {
            continue;
        }
        for old in &other.orders {
            if *other_key != key
                && parent.venue_parent_id.is_some()
                && (if algo {
                    &old.venue_algo_id
                } else {
                    &old.venue_list_id
                }) == &parent.venue_parent_id
            {
                return Err(Error::Conflict);
            }
            for new in &updated.orders {
                if old.order_id != new.order_id
                    && new.venue_order_id.is_some()
                    && old.venue_order_id == new.venue_order_id
                {
                    return Err(Error::Conflict);
                }
            }
        }
    }
    registry.insert(key, updated);
    Ok(())
}
