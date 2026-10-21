use super::*;
use ring::digest::{Context, SHA256};
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Reply<'a> {
    execution_id: u64,
    #[serde(borrow)]
    amended_order: Order<'a>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Order<'a> {
    #[serde(borrow)]
    symbol: &'a str,
    order_id: u64,
    order_list_id: i64,
    #[serde(borrow)]
    orig_client_order_id: &'a str,
    #[serde(borrow)]
    client_order_id: &'a str,
    #[serde(borrow)]
    price: &'a str,
    #[serde(borrow)]
    qty: &'a str,
    #[serde(borrow)]
    executed_qty: &'a str,
    #[serde(borrow)]
    cumulative_quote_qty: &'a str,
    #[serde(borrow)]
    status: &'a str,
    #[serde(borrow)]
    side: &'a str,
    #[serde(borrow)]
    time_in_force: &'a str,
    #[serde(borrow, rename = "type")]
    kind: &'a str,
}
pub(super) fn bind<S: Store, B: Backend, A: Authority, E: Encoder>(
    input: &AccountIngress<S>,
    gateway: &AccountGateway<B, A, E>,
    record: &RestRecord,
    body: &[u8],
) -> Result<CanonicalEvent> {
    let pending = gateway
        .account()
        .amendments()
        .find(|a| a.intent == intent(&record.command))
        .ok_or(Error::Identity)?;
    if pending.command_digest != record.command.digest() {
        return Err(Error::Identity);
    }
    let previous = v::Decimal::new(
        pending.previous_quantity.atoms(),
        -(pending.previous_quantity.scale() as i8),
    )
    .map_err(|_| Error::Encoding)?;
    decode(input, record, &pending.order, previous, body)
}
fn decode<S: Store>(
    input: &AccountIngress<S>,
    record: &RestRecord,
    key: &tickwire_account::OrderKey,
    previous: v::Decimal,
    body: &[u8],
) -> Result<CanonicalEvent> {
    if record.query.is_some() || record.order_query.is_some() || record.structure_query.is_some() {
        return Err(Error::Identity);
    }
    let reg = input.registry.get(&key.intent).ok_or(Error::Identity)?;
    let id = reg
        .orders
        .iter()
        .find(|o| o.client_id.as_ref() == Some(&key.client_id))
        .ok_or(Error::Identity)?;
    let v::CommandBody::New(spec) = &reg.command.command().body else {
        return Err(Error::Unsupported);
    };
    let v::CommandBody::AmendKeepPriority {
        new_quantity,
        new_client_id,
        ..
    } = &record.command.command().body
    else {
        return Err(Error::Identity);
    };
    if !super::binding::allowed(&record.command, &reg.command, id)
        || new_client_id.as_ref() != Some(&spec.client_id)
    {
        return Err(Error::Identity);
    }
    let reply: Reply<'_> = serde_json::from_slice(body).map_err(|_| Error::Encoding)?;
    let row = reply.amended_order;
    let decimal = |s: &str| s.parse::<v::Decimal>().map_err(|_| Error::Encoding);
    if row.symbol != reg.command.command().context.instrument.symbol.as_str()
        || row.order_list_id != -1
        || id
            .venue_order_id
            .as_ref()
            .is_none_or(|n| n.as_str() != row.order_id.to_string())
        || row.orig_client_order_id != spec.client_id.as_str()
        || row.client_order_id != spec.client_id.as_str()
        || row.kind != "LIMIT"
        || row.time_in_force != "GTC"
        || row.side
            != if spec.side == v::Side::Buy {
                "BUY"
            } else {
                "SELL"
            }
        || Some(decimal(row.price)?) != spec.price
        || decimal(row.qty)? != *new_quantity
    {
        return Err(Error::Identity);
    }
    let status = match row.status {
        "NEW" => v::OrderStatus::New,
        "PARTIALLY_FILLED" => v::OrderStatus::PartiallyFilled,
        "FILLED" => v::OrderStatus::Filled,
        "CANCELED" => v::OrderStatus::Canceled,
        "EXPIRED" | "EXPIRED_IN_MATCH" => v::OrderStatus::Expired,
        _ => return Err(Error::Unsupported),
    };
    let ctx = &reg.command.command().context;
    let mut h = Context::new(&SHA256);
    h.update(b"tickwire.rest.amendment.v1");
    h.update(&record.command.digest());
    h.update(&record.epoch.to_le_bytes());
    h.update(&record.raw);
    let event_id = h
        .finish()
        .as_ref()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
    CanonicalEvent::new(v::VenueEvent {
        identity: v::EventIdentity {
            risk_domain_id: ctx.request.risk_domain_id,
            venue: ctx.request.venue,
            environment: ctx.request.environment,
            account: ctx.request.account,
            product: ctx.request.product,
            instrument_id: ctx.instrument.instrument_id,
            instrument_version: ctx.instrument.instrument_version,
            source_connection_epoch: record.epoch,
            source_sequence: None,
            event_id: v::BoundedText::new(event_id).map_err(|_| Error::Encoding)?,
            venue_time_ns: None,
            received_mono_ns: record.received,
        },
        body: v::VenueEventBody::Amendment(Box::new(v::AmendmentObservation {
            order: id.clone(),
            request_intent_id: record.command.command().context.request.intent_id,
            request_intent_exec_epoch: record.command.command().context.request.intent_exec_epoch,
            request_digest: record.command.digest(),
            execution_source: v::AmendmentSource::RestResponse,
            execution_id: reply.execution_id,
            previous_quantity: previous,
            new_quantity: *new_quantity,
            observation: Some(v::OrderObservation {
                order: id.clone(),
                status,
                side: spec.side,
                position_side: spec.position_side,
                original_quantity: Some(*new_quantity),
                cumulative_quantity: Some(decimal(row.executed_qty)?),
                cumulative_quote: Some(decimal(row.cumulative_quote_qty)?),
                leaves_quantity: None,
                order_price: Some(decimal(row.price)?),
            }),
        })),
    })
    .map_err(|_| Error::Encoding)
}
pub(super) fn verify_raw<S: Store>(
    input: &AccountIngress<S>,
    record: &RestRecord,
    event: &CanonicalEvent,
) -> Result<()> {
    let (order, previous) = match &event.event().body {
        v::VenueEventBody::Amendment(a) => (&a.order, Some(a.previous_quantity)),
        v::VenueEventBody::AmendmentRejected(a) => (&a.order, None),
        _ => return Err(Error::Identity),
    };
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
    let key = tickwire_account::OrderKey {
        intent: IntentKey {
            epoch: order.original_intent_exec_epoch,
            intent_id: order.original_intent_id,
        },
        client_id: order.client_id.clone().ok_or(Error::Identity)?,
    };
    let actual = if let Some(previous) = previous {
        decode(input, record, &key, previous, &response.body)?
    } else {
        let decoded = tickwire_binance::response::decode_rest(
            input.market()?,
            response.status,
            &response.body,
        )
        .map_err(|_| Error::Encoding)?;
        if decoded.failure != Some(tickwire_binance::response::FailureKind::Rejected) {
            return Err(Error::Identity);
        }
        rejection_record(
            input,
            record,
            &key,
            decoded.error.as_ref().ok_or(Error::Encoding)?.code,
        )?
    };
    if actual != *event {
        return Err(Error::Identity);
    }
    Ok(())
}

pub(super) fn rejection<S: Store, B: Backend, A: Authority, E: Encoder>(
    input: &AccountIngress<S>,
    gateway: &AccountGateway<B, A, E>,
    record: &RestRecord,
    code: i64,
) -> Result<CanonicalEvent> {
    let pending = gateway
        .account()
        .amendments()
        .find(|a| a.intent == intent(&record.command))
        .ok_or(Error::Identity)?;
    if pending.command_digest != record.command.digest() {
        return Err(Error::Identity);
    }
    rejection_record(input, record, &pending.order, code)
}
fn rejection_record<S: Store>(
    input: &AccountIngress<S>,
    record: &RestRecord,
    key: &tickwire_account::OrderKey,
    code: i64,
) -> Result<CanonicalEvent> {
    let reg = input.registry.get(&key.intent).ok_or(Error::Identity)?;
    let id = reg
        .orders
        .iter()
        .find(|o| o.client_id.as_ref() == Some(&key.client_id))
        .ok_or(Error::Identity)?;
    if !matches!(
        record.command.command().body,
        v::CommandBody::AmendKeepPriority { .. }
    ) || !matches!(reg.command.command().body, v::CommandBody::New(_))
        || !super::binding::allowed(&record.command, &reg.command, id)
        || record.query.is_some()
        || record.order_query.is_some()
        || record.structure_query.is_some()
    {
        return Err(Error::Identity);
    }
    let c = &reg.command.command().context;
    let mut h = Context::new(&SHA256);
    h.update(b"tickwire.rest.amendment-rejected.v1");
    h.update(&record.command.digest());
    h.update(&record.epoch.to_le_bytes());
    h.update(&record.raw);
    let event_id = h
        .finish()
        .as_ref()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
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
        body: v::VenueEventBody::AmendmentRejected(Box::new(v::AmendmentRejection {
            order: id.clone(),
            request_intent_id: record.command.command().context.request.intent_id,
            request_intent_exec_epoch: record.command.command().context.request.intent_exec_epoch,
            request_digest: record.command.digest(),
            venue_code: v::BoundedText::new(code.to_string()).map_err(|_| Error::Encoding)?,
        })),
    })
    .map_err(|_| Error::Encoding)
}
