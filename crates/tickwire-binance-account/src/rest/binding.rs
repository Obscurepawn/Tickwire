use super::*;
use ring::digest::{Context, SHA256};
use serde::Deserialize;
use serde_json::value::RawValue;
use tickwire_binance::response::{self, FailureKind};

#[derive(Deserialize)]
struct Reports<'a> {
    #[serde(borrow, rename = "orderReports")]
    reports: Option<Vec<&'a RawValue>>,
    #[serde(borrow, rename = "cancelResponse")]
    cancel: Option<&'a RawValue>,
    #[serde(borrow, rename = "newOrderResponse")]
    replacement: Option<&'a RawValue>,
}
#[derive(Deserialize)]
struct Extra<'a> {
    #[serde(borrow)]
    side: Option<&'a str>,
    #[serde(borrow, rename = "positionSide")]
    position: Option<&'a str>,
    #[serde(rename = "orderListId")]
    list_id: Option<i64>,
}
pub(crate) fn decide<S: Store, B: Backend, A: Authority, E: Encoder>(
    input: &AccountIngress<S>,
    gateway: &AccountGateway<B, A, E>,
    record: &RestRecord,
    body: &[u8],
) -> Result<(RestDisposition, Vec<CanonicalEvent>)> {
    let decoded =
        response::decode_rest(input.market()?, record.status, body).map_err(|_| Error::Encoding)?;
    decide_decoded(
        input,
        gateway,
        record,
        decoded,
        b"tickwire.rest.observation.v1",
        true,
        gateway.per_order_doubt_isolation(),
    )
}

pub(crate) fn bounded_rejection_message(msg: &str, code: i64) -> Result<v::BoundedMessage<256>> {
    let mut end = msg.len().min(256);
    while end > 0 && !msg.is_char_boundary(end) {
        end -= 1;
    }
    let slice = &msg[..end];
    if slice.is_empty() {
        v::BoundedMessage::new(format!("venue error {code}")).map_err(|_| Error::Encoding)
    } else {
        v::BoundedMessage::new(slice).map_err(|_| Error::Encoding)
    }
}

pub(crate) fn order_rejection<S: Store>(
    input: &AccountIngress<S>,
    record: &RestRecord,
    event_domain: &[u8],
    code: i64,
    msg: &str,
) -> Result<CanonicalEvent> {
    if record.query.is_some()
        || record.structure_query.is_some()
        || record.amendment_query.is_some()
    {
        return Err(Error::Identity);
    }
    let v::CommandBody::New(spec) = &record.command.command().body else {
        return Err(Error::Identity);
    };
    let req_intent = intent(&record.command);
    let reg = input.registry.get(&req_intent).ok_or(Error::Identity)?;
    if reg.command != record.command
        || reg.orders.len() != 1
        || reg.orders[0].client_id.as_ref() != Some(&spec.client_id)
    {
        return Err(Error::Identity);
    }
    if let Some(q) = record.order_query
        && (q.intent != req_intent || q.order_id != reg.orders[0].order_id || code != -2013)
    {
        return Err(Error::Identity);
    }
    let c = &reg.command.command().context;
    let mut h = Context::new(&SHA256);
    h.update(event_domain);
    h.update(&record.command.digest());
    h.update(&record.epoch.to_le_bytes());
    h.update(&record.raw);
    h.update(&0u64.to_le_bytes());
    let event_id: String = h
        .finish()
        .as_ref()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    CanonicalEvent::new(v::VenueEvent {
        identity: v::EventIdentity {
            risk_domain_id: c.request.risk_domain_id,
            venue: c.request.venue,
            environment: c.request.environment,
            account: c.request.account,
            product: c.request.product,
            instrument_id: c.instrument.instrument_id,
            instrument_version: c.instrument.instrument_version,
            source_connection_epoch: record.epoch,
            source_sequence: None,
            event_id: v::BoundedText::new(event_id).map_err(|_| Error::Encoding)?,
            venue_time_ns: None,
            received_mono_ns: record.received,
        },
        body: v::VenueEventBody::Rejected {
            client_id: Some(spec.client_id.clone()),
            venue_code: v::BoundedText::new(code.to_string()).map_err(|_| Error::Encoding)?,
            message: bounded_rejection_message(msg, code)?,
        },
    })
    .map_err(|_| Error::Encoding)
}

pub(crate) fn verify_rejection_raw<S: Store>(
    input: &AccountIngress<S>,
    record: &RestRecord,
    event: &CanonicalEvent,
) -> Result<()> {
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
    let decoded = response::decode_rest(input.market()?, response.status, &response.body)
        .map_err(|_| Error::Encoding)?;
    let api_err = decoded.error.as_ref().ok_or(Error::Encoding)?;
    if record.order_query.is_some() {
        if decoded.failure != Some(FailureKind::NotFound) || api_err.code != -2013 {
            return Err(Error::Identity);
        }
    } else if decoded.failure != Some(FailureKind::Rejected) {
        return Err(Error::Identity);
    }
    let actual = order_rejection(
        input,
        record,
        b"tickwire.rest.observation.v1",
        api_err.code,
        &api_err.msg,
    )?;
    if actual != *event {
        return Err(Error::Identity);
    }
    Ok(())
}

pub(crate) fn decide_decoded<S: Store, B: Backend, A: Authority, E: Encoder>(
    input: &AccountIngress<S>,
    gateway: &AccountGateway<B, A, E>,
    record: &RestRecord,
    decoded: response::Response<'_>,
    event_domain: &[u8],
    apply_candidate: bool,
    emit_order_rejection: bool,
) -> Result<(RestDisposition, Vec<CanonicalEvent>)> {
    let mut disposition = RestDisposition::Observed;
    let value = if let Some(kind) = decoded.failure {
        if kind == FailureKind::Rejected
            && matches!(
                record.command.command().body,
                v::CommandBody::AmendKeepPriority { .. }
            )
        {
            let event = super::amendment::rejection(
                input,
                gateway,
                record,
                decoded.error.as_ref().ok_or(Error::Encoding)?.code,
            )?;
            if apply_candidate {
                gateway
                    .account()
                    .clone()
                    .apply_event(&event)
                    .map_err(|_| Error::Identity)?;
            }
            return Ok((RestDisposition::Rejected, vec![event]));
        }
        if kind == FailureKind::Rejected
            && emit_order_rejection
            && record.order_query.is_none()
            && record.query.is_none()
            && record.structure_query.is_none()
            && record.amendment_query.is_none()
            && matches!(record.command.command().body, v::CommandBody::New(_))
        {
            let api_err = decoded.error.as_ref().ok_or(Error::Encoding)?;
            let event = order_rejection(input, record, event_domain, api_err.code, &api_err.msg)?;
            if apply_candidate {
                if gateway.command(intent(&record.command)) != Some(&record.command) {
                    return Err(Error::Identity);
                }
                gateway
                    .account()
                    .clone()
                    .apply_event(&event)
                    .map_err(|_| Error::Identity)?;
            }
            return Ok((RestDisposition::Rejected, vec![event]));
        }
        if kind != FailureKind::Partial {
            return Ok((
                if kind == FailureKind::Rejected {
                    RestDisposition::Rejected
                } else {
                    RestDisposition::Unknown
                },
                vec![],
            ));
        }
        disposition = RestDisposition::NeedsReconciliation;
        decoded
            .error
            .as_ref()
            .and_then(|e| e.data)
            .ok_or(Error::Encoding)?
    } else {
        decoded.result.ok_or(Error::Encoding)?
    };
    if matches!(
        record.command.command().body,
        v::CommandBody::AmendKeepPriority { .. }
    ) {
        let observed = super::amendment::bind(input, gateway, record, value.get().as_bytes())?;
        if apply_candidate {
            gateway
                .account()
                .clone()
                .apply_event(&observed)
                .map_err(|_| Error::Identity)?;
        }
        let needs = matches!(&observed.event().body, v::VenueEventBody::Amendment(a) if a.observation.as_ref().is_some_and(|o|o.cumulative_quantity.is_some_and(|q|q.coefficient()>0)));
        return Ok((
            if needs {
                RestDisposition::NeedsReconciliation
            } else {
                RestDisposition::Observed
            },
            vec![observed],
        ));
    }
    let reports: Reports<'_> = serde_json::from_str(value.get()).map_err(|_| Error::Encoding)?;
    let mut events = Vec::new();
    let mut overrides: BTreeMap<IntentKey, Registration> = BTreeMap::new();
    let mut candidate = gateway.account().clone();
    if record.order_query.is_none()
        && matches!(
            record.command.command().body,
            v::CommandBody::SpotList(_)
                | v::CommandBody::CancelList(_)
                | v::CommandBody::Algo(_)
                | v::CommandBody::CancelAlgo(_)
        )
    {
        let parent = super::structure::bind(input, gateway, record, value.get().as_bytes())?;
        input
            .registry
            .learn_with_overrides(&mut overrides, &parent)?;
        if apply_candidate {
            candidate
                .apply_event(&parent)
                .map_err(|_| Error::Identity)?;
        }
        if matches!(&parent.event().body,v::VenueEventBody::Structure(s) if s.status==v::StructureStatus::Unknown)
        {
            disposition = RestDisposition::NeedsReconciliation;
        }
        events.push(parent);
    }
    if record.structure_query.is_some() {
        if events.len() != 1 {
            return Err(Error::Identity);
        }
        return Ok((disposition, events));
    }
    let rows: Vec<&RawValue> = if record.order_query.is_some() {
        vec![value]
    } else {
        match &record.command.command().body {
            v::CommandBody::SpotList(_) | v::CommandBody::CancelList(_) => {
                // Members are identities, not reports of each leg's state.
                reports.reports.unwrap_or_default()
            }
            v::CommandBody::Algo(_) | v::CommandBody::CancelAlgo(_) => Vec::new(),
            v::CommandBody::CancelReplace { .. } => {
                let mut rows = Vec::new();
                for row in [reports.cancel, reports.replacement].into_iter().flatten() {
                    if response::decode_order(row.get().as_bytes()).is_ok() {
                        rows.push(row);
                    } else {
                        disposition = RestDisposition::NeedsReconciliation;
                    }
                }
                rows
            }
            _ => vec![value],
        }
    };
    if (rows.is_empty() && events.is_empty()) || rows.len() > 3 {
        return Err(Error::Encoding);
    }
    for (index, row) in rows.iter().enumerate() {
        let reply = response::decode_order(row.get().as_bytes()).map_err(|_| Error::Encoding)?;
        let extra: Extra<'_> = serde_json::from_str(row.get()).map_err(|_| Error::Encoding)?;
        if record.order_query.is_some()
            && (reply.status.is_none()
                || reply.original_quantity.is_none()
                || reply.executed_quantity.is_none()
                || extra.side.is_none()
                || (input.market()? == tickwire_binance::request::Market::Usdm
                    && extra.position.is_none()))
        {
            return Err(Error::Encoding);
        }

        let mut candidate_keys = input.registry.candidate_intents_for_symbol(
            &reply.symbol,
            reply.order_id,
            &reply.client_order_id,
        );
        candidate_keys.extend(overrides.keys().copied());
        let native = reply.order_id.to_string();
        let mut matches = Vec::new();
        for key in candidate_keys {
            let Some(reg) = overrides.get(&key).or_else(|| input.registry.get(&key)) else {
                continue;
            };
            if reg.command.command().context.instrument.symbol.as_str() != reply.symbol {
                continue;
            }
            for (n, id) in reg.orders.iter().enumerate() {
                if !allowed(&record.command, &reg.command, id)
                    || record.order_query.is_some_and(|q| {
                        q.order_id != id.order_id || q.intent != intent(&reg.command)
                    })
                {
                    continue;
                }
                let matches_client =
                    if matches!(reg.command.command().body, v::CommandBody::Algo(_)) {
                        id.venue_order_id
                            .as_ref()
                            .is_some_and(|id| id.as_str() == native)
                    } else {
                        id.client_id
                            .as_ref()
                            .is_some_and(|c| c.as_str() == reply.client_order_id)
                    };
                if matches_client
                    && id
                        .venue_order_id
                        .as_ref()
                        .is_none_or(|old| old.as_str() == native)
                {
                    matches.push((reg, n));
                }
            }
        }
        let [(reg, n)] = matches.as_slice() else {
            return Err(Error::Identity);
        };
        if gateway.command(intent(&reg.command)) != Some(&reg.command) {
            return Err(Error::Identity);
        }
        let spec = spec(&reg.command, *n)?;
        if extra.side.is_some_and(|s| {
            s != match spec.side {
                v::Side::Buy => "BUY",
                v::Side::Sell => "SELL",
            }
        }) || extra.position.is_some_and(|s| {
            s != match spec.position_side {
                v::PositionSide::Both => "BOTH",
                v::PositionSide::Long => "LONG",
                v::PositionSide::Short => "SHORT",
            }
        }) {
            return Err(Error::Identity);
        }
        let mut order = reg.orders[*n].clone();
        order.venue_order_id =
            Some(v::VenueOrderId::new(reply.order_id.to_string()).map_err(|_| Error::Encoding)?);
        if let Some(list) = extra.list_id.filter(|n| *n >= 0) {
            if order.list_id.is_none() {
                return Err(Error::Identity);
            }
            let list = v::VenueOrderId::new(list.to_string()).map_err(|_| Error::Encoding)?;
            if order.venue_list_id.as_ref().is_some_and(|old| *old != list) {
                return Err(Error::Conflict);
            }
            order.venue_list_id = Some(list);
        }
        let status = match reply.status.as_deref() {
            None | Some("PENDING_NEW" | "PENDING_CANCEL") => v::OrderStatus::Pending,
            Some("NEW") => v::OrderStatus::New,
            Some("PARTIALLY_FILLED") => v::OrderStatus::PartiallyFilled,
            Some("FILLED") => v::OrderStatus::Filled,
            Some("CANCELED") => v::OrderStatus::Canceled,
            Some("EXPIRED" | "EXPIRED_IN_MATCH") => v::OrderStatus::Expired,
            Some("REJECTED") => v::OrderStatus::Rejected,
            _ => v::OrderStatus::Unknown,
        };
        if status == v::OrderStatus::Unknown
            || reply.executed_quantity.is_some_and(|n| n.mantissa() > 0)
            || reply.fills.is_some_and(|raw| raw.get() != "[]")
        {
            disposition = RestDisposition::NeedsReconciliation;
        }
        let c = &reg.command.command().context;
        let mut hash = Context::new(&SHA256);
        hash.update(event_domain);
        hash.update(&record.command.digest());
        hash.update(&record.epoch.to_le_bytes());
        hash.update(&record.raw);
        hash.update(&(index as u64).to_le_bytes());
        let hex: String = hash
            .finish()
            .as_ref()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        let event = CanonicalEvent::new(v::VenueEvent {
            identity: v::EventIdentity {
                risk_domain_id: c.request.risk_domain_id,
                venue: c.request.venue,
                environment: c.request.environment,
                account: c.request.account,
                product: c.request.product,
                instrument_id: c.instrument.instrument_id,
                instrument_version: c.instrument.instrument_version,
                source_connection_epoch: record.epoch,
                source_sequence: None,
                event_id: v::BoundedText::new(hex).map_err(|_| Error::Encoding)?,
                venue_time_ns: None,
                received_mono_ns: record.received,
            },
            body: v::VenueEventBody::Order(Box::new(v::OrderObservation {
                order,
                status,
                side: spec.side,
                position_side: spec.position_side,
                original_quantity: reply.original_quantity.map(decimal).transpose()?,
                cumulative_quantity: reply.executed_quantity.map(decimal).transpose()?,
                cumulative_quote: reply.cumulative_quote.map(decimal).transpose()?,
                leaves_quantity: None,
                order_price: reply.price.map(decimal).transpose()?,
            })),
        })
        .map_err(|_| Error::Encoding)?;
        if events.iter().any(|old| {
            registry::order(old).ok().map(|o| o.order_id)
                == registry::order(&event).ok().map(|o| o.order_id)
        }) {
            return Err(Error::Conflict);
        }
        input
            .registry
            .learn_with_overrides(&mut overrides, &event)?;
        if apply_candidate {
            candidate.apply_event(&event).map_err(|_| Error::Identity)?;
        }
        events.push(event);
    }
    Ok((disposition, events))
}
fn decimal(n: tickwire_binance::decimal::Decimal) -> Result<v::Decimal> {
    v::Decimal::new(n.mantissa(), -(n.scale() as i8)).map_err(|_| Error::Encoding)
}
fn spec(command: &CanonicalCommand, index: usize) -> Result<&v::OrderSpec> {
    match &command.command().body {
        v::CommandBody::New(s) | v::CommandBody::CancelReplace { replacement: s, .. }
            if index == 0 =>
        {
            Ok(s)
        }
        v::CommandBody::SpotList(list) => list.legs.as_slice().get(index).ok_or(Error::Identity),
        v::CommandBody::Algo(a) if index == 0 => Ok(&a.order),
        _ => Err(Error::Unsupported),
    }
}
fn target(t: &v::OrderTarget, id: &v::OrderIdentity) -> bool {
    t.order_id == id.order_id
        && t.original_intent_exec_epoch == id.original_intent_exec_epoch
        && t.client_id
            .as_ref()
            .is_none_or(|n| Some(n) == id.client_id.as_ref())
        && t.venue_order_id
            .as_ref()
            .is_none_or(|n| Some(n) == id.venue_order_id.as_ref())
}
pub(super) fn allowed(
    request: &CanonicalCommand,
    creating: &CanonicalCommand,
    id: &v::OrderIdentity,
) -> bool {
    let a = &request.command().context;
    let b = &creating.command().context;
    if a.instrument != b.instrument
        || a.request.account != b.request.account
        || a.request.product != b.request.product
        || a.request.environment != b.request.environment
        || a.request.risk_domain_id != b.request.risk_domain_id
    {
        return false;
    }
    match &request.command().body {
        v::CommandBody::New(_) | v::CommandBody::SpotList(_) | v::CommandBody::Algo(_) => {
            request == creating
        }
        v::CommandBody::Cancel(t) | v::CommandBody::Query(t) => target(t, id),
        v::CommandBody::AmendKeepPriority {
            target: t,
            new_client_id,
            ..
        } => {
            target(t, id)
                && t.client_id == id.client_id
                && t.venue_order_id == id.venue_order_id
                && new_client_id == &id.client_id
                && id.parent_order_id.is_none()
                && id.list_id.is_none()
        }
        v::CommandBody::CancelReplace { target: t, .. } => request == creating || target(t, id),
        v::CommandBody::CancelList(t) => {
            id.list_id == Some(t.list_id)
                && id.original_intent_exec_epoch == t.original_intent_exec_epoch
                && t.client_id
                    .as_ref()
                    .is_none_or(|c| Some(c) == id.list_client_id.as_ref())
                && t.venue_list_id
                    .as_ref()
                    .is_none_or(|n| Some(n) == id.venue_list_id.as_ref())
        }
        v::CommandBody::CancelAlgo(t) => {
            id.parent_order_id == Some(t.parent_order_id)
                && id.original_intent_exec_epoch == t.original_intent_exec_epoch
                && t.client_id
                    .as_ref()
                    .is_none_or(|c| Some(c) == id.algo_client_id.as_ref())
                && t.venue_algo_id
                    .as_ref()
                    .is_none_or(|n| Some(n) == id.venue_algo_id.as_ref())
        }
        _ => false,
    }
}
