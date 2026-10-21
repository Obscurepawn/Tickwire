use super::*;

pub(crate) fn allowed(
    request: &CanonicalCommand,
    creating: &CanonicalCommand,
    parent: &v::StructureIdentity,
) -> bool {
    let a = &request.command().context;
    let b = &creating.command().context;
    if a.instrument != b.instrument
        || a.request.risk_domain_id != b.request.risk_domain_id
        || a.request.venue != b.request.venue
        || a.request.environment != b.request.environment
        || a.request.account != b.request.account
        || a.request.product != b.request.product
        || parent.original_intent_id != b.request.intent_id
        || parent.original_intent_exec_epoch != b.request.intent_exec_epoch
    {
        return false;
    }
    match &request.command().body {
        v::CommandBody::SpotList(_) | v::CommandBody::Algo(_) => request == creating,
        v::CommandBody::CancelList(t) => {
            parent.kind != v::StructureKind::Algo
                && t.list_id == parent.structure_id
                && t.original_intent_exec_epoch == parent.original_intent_exec_epoch
                && t.client_id.as_ref().is_none_or(|c| c == &parent.client_id)
                && t.venue_list_id
                    .as_ref()
                    .is_none_or(|id| Some(id) == parent.venue_parent_id.as_ref())
        }
        v::CommandBody::CancelAlgo(t) => {
            parent.kind == v::StructureKind::Algo
                && t.parent_order_id == parent.structure_id
                && t.original_intent_exec_epoch == parent.original_intent_exec_epoch
                && t.client_id.as_ref().is_none_or(|c| c == &parent.client_id)
                && t.venue_algo_id
                    .as_ref()
                    .is_none_or(|id| Some(id) == parent.venue_parent_id.as_ref())
        }
        _ => false,
    }
}

pub(super) fn bind<S: Store, B: Backend, A: Authority, E: Encoder>(
    input: &AccountIngress<S>,
    gateway: &AccountGateway<B, A, E>,
    record: &RestRecord,
    bytes: &[u8],
) -> Result<CanonicalEvent> {
    let scope = &input.config.baseline.snapshot().scope;
    let session = tickwire_binance::events::SessionIdentity {
        risk_domain_id: scope.risk_domain_id,
        venue: scope.venue,
        account: scope.account,
        environment: scope.environment,
        connection_epoch: record.epoch,
    };
    let mut found = None;
    for reg in input.registry.values() {
        let Ok(event) = tickwire_binance::events::bind_structure_response(
            &session,
            input.market()?,
            &reg.command,
            &reg.orders,
            bytes,
            record.received,
        ) else {
            continue;
        };
        let v::VenueEventBody::Structure(parent) = &event.event().body else {
            return Err(Error::Identity);
        };
        if !allowed(&record.command, &reg.command, &parent.parent) {
            continue;
        }
        if found.is_some() || gateway.command(intent(&reg.command)) != Some(&reg.command) {
            return Err(Error::Identity);
        }
        found = Some(event);
    }
    found.ok_or(Error::Identity)
}

/// Re-decode the actual bounded HTTP response before accepting a retained parent event.
/// This also runs while reopening the input WAL, before learning its child bindings.
pub(super) fn verify_raw<S: Store>(
    input: &AccountIngress<S>,
    record: &RestRecord,
    event: &CanonicalEvent,
) -> Result<()> {
    let v::VenueEventBody::Structure(parent) = &event.event().body else {
        return Err(Error::Identity);
    };
    let key = IntentKey {
        epoch: parent.parent.original_intent_exec_epoch,
        intent_id: parent.parent.original_intent_id,
    };
    let reg = input.registry.get(&key).ok_or(Error::Identity)?;
    let mut decoder =
        ResponseDecoder::new(input.config.max_raw_bytes, 256).map_err(Error::Transport)?;
    decoder.push(&record.raw).map_err(Error::Transport)?;
    decoder.finish();
    let response = decoder
        .next_response()
        .map_err(Error::Transport)?
        .ok_or(Error::Encoding)?;
    if response.status != record.status || !decoder.take_buffered().is_empty() {
        return Err(Error::Identity);
    }
    let decoded =
        tickwire_binance::response::decode_rest(input.market()?, response.status, &response.body)
            .map_err(|_| Error::Encoding)?;
    let body = if decoded.failure == Some(tickwire_binance::response::FailureKind::Partial) {
        decoded
            .error
            .as_ref()
            .and_then(|e| e.data)
            .ok_or(Error::Identity)?
    } else if decoded.failure.is_none() {
        decoded.result.ok_or(Error::Identity)?
    } else {
        return Err(Error::Identity);
    };
    let scope = &input.config.baseline.snapshot().scope;
    let actual = tickwire_binance::events::bind_structure_response(
        &tickwire_binance::events::SessionIdentity {
            risk_domain_id: scope.risk_domain_id,
            venue: scope.venue,
            account: scope.account,
            environment: scope.environment,
            connection_epoch: record.epoch,
        },
        input.market()?,
        &reg.command,
        &reg.orders,
        body.get().as_bytes(),
        record.received,
    )
    .map_err(|_| Error::Identity)?;
    if &actual != event {
        return Err(Error::Identity);
    }
    Ok(())
}
