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
    let (specs, list) = match &c.body {
        v::CommandBody::New(o) | v::CommandBody::CancelReplace { replacement: o, .. } => {
            (vec![o], None)
        }
        v::CommandBody::SpotList(l) => (l.legs.as_slice().iter().collect(), Some(l)),
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
            parent_order_id: None,
            list_id: list.map(|l| l.list_id),
            venue_algo_id: None,
            venue_list_id: None,
            list_client_id: list.map(|l| l.client_id.clone()),
            algo_client_id: None,
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
        v::VenueEventBody::Order(o) => Ok(&o.order),
        v::VenueEventBody::Fill(f) => Ok(&f.observation.order),
        _ => Err(Error::Unsupported),
    }
}
pub(crate) fn learn(
    registry: &mut BTreeMap<IntentKey, Registration>,
    event: &CanonicalEvent,
) -> Result<()> {
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
