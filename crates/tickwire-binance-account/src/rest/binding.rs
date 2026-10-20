use super::*;
use ring::digest::{Context, SHA256};
use serde::Deserialize;
use serde_json::value::RawValue;
use tickwire_binance::response::{self, FailureKind};

#[derive(Deserialize)]
struct Reports<'a> {
    #[serde(borrow, rename = "orderReports")]
    reports: Option<Vec<&'a RawValue>>,
    #[serde(borrow)]
    orders: Option<Vec<&'a RawValue>>,
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
    let mut disposition = RestDisposition::Observed;
    let value = if let Some(kind) = decoded.failure {
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
    let reports: Reports<'_> = serde_json::from_str(value.get()).map_err(|_| Error::Encoding)?;
    let rows: Vec<&RawValue> = if record.order_query.is_some() {
        vec![value]
    } else {
        match &record.command.command().body {
            v::CommandBody::SpotList(_) | v::CommandBody::CancelList(_) => {
                reports.reports.or(reports.orders).ok_or(Error::Encoding)?
            }
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
    if rows.is_empty() || rows.len() > 3 {
        return Err(Error::Encoding);
    }
    let mut events = Vec::new();
    let mut learned = input.registry.clone();
    let mut candidate = gateway.account().clone();
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

        let mut matches = Vec::new();
        for reg in learned.values() {
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
                let native = reply.order_id.to_string();
                if id
                    .client_id
                    .as_ref()
                    .is_some_and(|c| c.as_str() == reply.client_order_id)
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
        hash.update(b"tickwire.rest.observation.v1");
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
        registry::learn(&mut learned, &event)?;
        candidate.apply_event(&event).map_err(|_| Error::Identity)?;
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
        v::CommandBody::New(_) | v::CommandBody::SpotList(_) => request == creating,
        v::CommandBody::Cancel(t) | v::CommandBody::Query(t) => target(t, id),
        v::CommandBody::CancelReplace { target: t, .. } => request == creating || target(t, id),
        v::CommandBody::CancelList(t) => {
            id.list_id == Some(t.list_id)
                && id.original_intent_exec_epoch == t.original_intent_exec_epoch
        }
        _ => false,
    }
}
