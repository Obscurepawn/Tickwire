use super::*;
use ring::digest::{SHA256, digest};

/// The mapping contains only the original, already reserved legs. Parent reports
/// bind native identities and never manufacture a child status, fill or fee.
pub fn bind_structure(
    message: &PrivateEvent<'_>,
    command: &v::CanonicalCommand,
    orders: &[v::OrderIdentity],
) -> Result<v::CanonicalEvent> {
    bind(
        message.session.identity(),
        message.session.market(),
        command,
        orders,
        message.body(),
        message.raw().get().as_bytes(),
        Some(message.event_time_ns()),
        message.received_mono_ns(),
    )
}

// Source identity and raw bytes remain explicit; a REST response is not a private-stream event.
#[allow(clippy::too_many_arguments)]
fn bind(
    session: &SessionIdentity,
    market: Market,
    command: &v::CanonicalCommand,
    orders: &[v::OrderIdentity],
    body: &PrivateBody<'_>,
    raw: &[u8],
    venue_time_ns: Option<u64>,
    received_mono_ns: u64,
) -> Result<v::CanonicalEvent> {
    let c = command.command();
    let request = &c.context.request;
    let instrument = &c.context.instrument;
    if session.risk_domain_id != request.risk_domain_id
        || session.venue != request.venue
        || session.environment != request.environment
        || session.account != request.account
        || !matches!(
            (market, request.product),
            (Market::Spot, v::Product::Spot)
                | (
                    Market::Usdm,
                    v::Product::LinearPerpetual | v::Product::LinearFuture
                )
        )
        || orders.is_empty()
        || orders.len() > 3
    {
        return Err(Error::IdentityMismatch);
    }
    for (n, id) in orders.iter().enumerate() {
        if id.order_id == [0; 16]
            || id.original_intent_id != request.intent_id
            || id.original_intent_exec_epoch != request.intent_exec_epoch
            || id.client_id.is_none()
            || orders[..n]
                .iter()
                .any(|old| old.order_id == id.order_id || old.client_id == id.client_id)
        {
            return Err(Error::IdentityMismatch);
        }
    }
    let (parent, status, raw_status, children) = match (&c.body, body) {
        (v::CommandBody::SpotList(list), PrivateBody::List(observed)) => {
            let (kind, wire_kind) = match list.kind {
                v::ListKind::Oco => (v::StructureKind::Oco, "OCO"),
                v::ListKind::Oto => (v::StructureKind::Oto, "OTO"),
                v::ListKind::Otoco => (v::StructureKind::Otoco, "OTO"),
            };
            if observed.symbol != instrument.symbol.as_str()
                || observed.list_client_id != list.client_id.as_str()
                || observed.contingency_type != wire_kind
                || orders.len() != list.legs.as_slice().len()
                || observed.orders.len() > orders.len()
            {
                return Err(Error::IdentityMismatch);
            }
            let native = native(observed.list_id)?;
            for (id, spec) in orders.iter().zip(list.legs.as_slice()) {
                if id.client_id.as_ref() != Some(&spec.client_id)
                    || id.list_id != Some(list.list_id)
                    || id.list_client_id.as_ref() != Some(&list.client_id)
                    || id.parent_order_id.is_some()
                    || id.venue_algo_id.is_some()
                    || id.algo_client_id.is_some()
                    || id.venue_list_id.as_ref().is_some_and(|old| old != &native)
                {
                    return Err(Error::IdentityMismatch);
                }
            }
            let mut children = Vec::new();
            for member in &observed.orders {
                if member.symbol != observed.symbol {
                    return Err(Error::IdentityMismatch);
                }
                let id = orders
                    .iter()
                    .find(|id| {
                        id.client_id
                            .as_ref()
                            .is_some_and(|id| id.as_str() == member.client_id)
                    })
                    .ok_or(Error::IdentityMismatch)?;
                let native_child = native_order(member.order_id)?;
                if id
                    .venue_order_id
                    .as_ref()
                    .is_some_and(|old| old != &native_child)
                {
                    return Err(Error::IdentityMismatch);
                }
                let mut child = id.clone();
                child.venue_order_id = Some(native_child);
                child.venue_list_id = Some(native.clone());
                children.push(v::StructureChild {
                    order: child,
                    reported_client_id: None,
                });
            }
            let status = match (
                observed.status_type.as_ref(),
                observed.order_status.as_ref(),
            ) {
                ("RESPONSE", "REJECT") => v::StructureStatus::Rejected,
                ("ALL_DONE", "ALL_DONE") => v::StructureStatus::Terminal,
                ("EXEC_STARTED" | "UPDATED", "EXECUTING") => v::StructureStatus::Live,
                _ => v::StructureStatus::Unknown,
            };
            (
                v::StructureIdentity {
                    structure_id: list.list_id,
                    original_intent_id: request.intent_id,
                    original_intent_exec_epoch: request.intent_exec_epoch,
                    kind,
                    client_id: list.client_id.clone(),
                    venue_parent_id: Some(native),
                },
                status,
                observed.order_status.as_ref(),
                children,
            )
        }
        (v::CommandBody::Algo(algo), PrivateBody::Algo(observed)) => {
            let spec = &algo.order;
            if orders.len() != 1
                || observed.symbol != instrument.symbol.as_str()
                || observed.client_algo_id != spec.client_id.as_str()
                || observed.algo_type != "CONDITIONAL"
                || observed.side
                    != match spec.side {
                        v::Side::Buy => "BUY",
                        v::Side::Sell => "SELL",
                    }
                || observed.position_side
                    != match spec.position_side {
                        v::PositionSide::Both => "BOTH",
                        v::PositionSide::Long => "LONG",
                        v::PositionSide::Short => "SHORT",
                    }
                || Some(observed.order_type.as_ref()) != order_type(spec.order_type)
                || Some(observed.time_in_force.as_ref()) != time_in_force(spec.time_in_force)
                || !matches!(spec.quantity,v::Quantity::Base(q) if decimal(observed.quantity)?==q)
                || observed
                    .price
                    .map(decimal)
                    .transpose()?
                    .is_some_and(|p| Some(p) != spec.price)
                || observed
                    .trigger_price
                    .map(decimal)
                    .transpose()?
                    .is_some_and(|p| spec.trigger.as_ref().is_none_or(|t| t.price != p))
            {
                return Err(Error::IdentityMismatch);
            }
            let id = &orders[0];
            let native = native(observed.algo_id)?;
            if id.client_id.as_ref() != Some(&spec.client_id)
                || id.parent_order_id != Some(algo.parent_order_id)
                || id.algo_client_id.as_ref() != Some(&spec.client_id)
                || id.list_id.is_some()
                || id.venue_list_id.is_some()
                || id.list_client_id.is_some()
                || id.venue_algo_id.as_ref().is_some_and(|old| old != &native)
            {
                return Err(Error::IdentityMismatch);
            }
            let mut children = Vec::new();
            if let Some(value) = observed
                .actual_order_id
                .as_deref()
                .filter(|v| !v.is_empty())
            {
                let child_id = value.parse::<u64>().map_err(|_| Error::InvalidResponse)?;
                let native_child = native_order(child_id)?;
                if id
                    .venue_order_id
                    .as_ref()
                    .is_some_and(|old| old != &native_child)
                {
                    return Err(Error::IdentityMismatch);
                }
                let mut child = id.clone();
                child.venue_order_id = Some(native_child);
                child.venue_algo_id = Some(native.clone());
                children.push(v::StructureChild {
                    order: child,
                    reported_client_id: None,
                });
            }
            let mut status = match observed.status.as_ref() {
                "NEW" => v::StructureStatus::Live,
                "PENDING_NEW" | "TRIGGERING" => v::StructureStatus::Pending,
                "TRIGGERED" => v::StructureStatus::Triggered,
                "FINISHED" | "CANCELED" | "EXPIRED" => v::StructureStatus::Terminal,
                "REJECTED" => v::StructureStatus::Rejected,
                _ => v::StructureStatus::Unknown,
            };
            if children.is_empty()
                && (observed
                    .actual_order_type
                    .as_deref()
                    .is_some_and(|s| !s.is_empty())
                    || observed
                        .executed_quantity
                        .is_some_and(|q| q.mantissa() != 0)
                    || observed
                        .average_fill_price
                        .is_some_and(|p| p.mantissa() != 0)
                    || observed.trigger_time.is_some_and(|t| t != 0)
                    || status == v::StructureStatus::Triggered)
            {
                status = v::StructureStatus::Unknown;
            }
            (
                v::StructureIdentity {
                    structure_id: algo.parent_order_id,
                    original_intent_id: request.intent_id,
                    original_intent_exec_epoch: request.intent_exec_epoch,
                    kind: v::StructureKind::Algo,
                    client_id: spec.client_id.clone(),
                    venue_parent_id: Some(native),
                },
                status,
                observed.status.as_ref(),
                children,
            )
        }
        _ => return Err(Error::UnsupportedCommand),
    };
    let hash = digest(&SHA256, raw);
    let event_id: String = hash.as_ref().iter().map(|b| format!("{b:02x}")).collect();
    v::CanonicalEvent::new(v::VenueEvent {
        identity: v::EventIdentity {
            risk_domain_id: request.risk_domain_id,
            venue: request.venue,
            environment: request.environment,
            account: request.account,
            product: request.product,
            instrument_id: instrument.instrument_id,
            instrument_version: instrument.instrument_version,
            source_connection_epoch: session.connection_epoch,
            source_sequence: None,
            event_id: v::BoundedText::new(event_id).map_err(|_| Error::InvalidResponse)?,
            venue_time_ns,
            received_mono_ns,
        },
        body: v::VenueEventBody::Structure(Box::new(v::StructureObservation {
            parent,
            status,
            venue_status: v::BoundedText::new(raw_status).map_err(|_| Error::InvalidResponse)?,
            children: v::BoundedList::new(children).map_err(|_| Error::Capacity)?,
        })),
    })
    .map_err(|_| Error::InvalidResponse)
}
fn native(n: u64) -> Result<v::VenueOrderId> {
    v::VenueOrderId::new(n.to_string()).map_err(|_| Error::InvalidResponse)
}
fn native_order(n: u64) -> Result<v::VenueOrderId> {
    native(n)
}
fn decimal(d: Decimal) -> Result<v::Decimal> {
    v::Decimal::new(d.mantissa(), -(d.scale() as i8)).map_err(|_| Error::InvalidDecimal)
}
fn order_type(t: v::OrderType) -> Option<&'static str> {
    Some(match t {
        v::OrderType::StopLimit => "STOP",
        v::OrderType::StopMarket => "STOP_MARKET",
        v::OrderType::TakeProfitLimit => "TAKE_PROFIT",
        v::OrderType::TakeProfitMarket => "TAKE_PROFIT_MARKET",
        v::OrderType::TrailingStop => "TRAILING_STOP_MARKET",
        _ => return None,
    })
}
fn time_in_force(t: Option<v::TimeInForce>) -> Option<&'static str> {
    Some(match t? {
        v::TimeInForce::Gtc => "GTC",
        v::TimeInForce::Ioc => "IOC",
        v::TimeInForce::Fok => "FOK",
        v::TimeInForce::PostOnly => "GTX",
        v::TimeInForce::Gtd { .. } => "GTD",
    })
}

/// Parses the parent portion of an actual REST/WS trading response. The caller
/// retains the complete response; child order reports require separate binding.
pub fn bind_structure_response(
    session: &SessionIdentity,
    market: Market,
    command: &v::CanonicalCommand,
    orders: &[v::OrderIdentity],
    bytes: &[u8],
    received: u64,
) -> Result<v::CanonicalEvent> {
    if bytes.is_empty() || bytes.len() > 32 * 1024 {
        return Err(Error::Capacity);
    }
    let body = match &command.command().body {
        v::CommandBody::SpotList(_) => {
            #[derive(Deserialize)]
            #[serde(rename_all = "camelCase")]
            struct List<'a> {
                #[serde(borrow)]
                symbol: Cow<'a, str>,
                order_list_id: u64,
                #[serde(borrow)]
                list_client_order_id: Cow<'a, str>,
                #[serde(borrow)]
                contingency_type: Cow<'a, str>,
                #[serde(borrow)]
                list_status_type: Cow<'a, str>,
                #[serde(borrow)]
                list_order_status: Cow<'a, str>,
                #[serde(borrow)]
                orders: Vec<Member<'a>>,
            }
            #[derive(Deserialize)]
            #[serde(rename_all = "camelCase")]
            struct Member<'a> {
                #[serde(borrow)]
                symbol: Cow<'a, str>,
                order_id: u64,
                #[serde(borrow)]
                client_order_id: Cow<'a, str>,
            }
            let list: List<'_> =
                serde_json::from_slice(bytes).map_err(|_| Error::InvalidResponse)?;
            if list.orders.len() > 3 {
                return Err(Error::Capacity);
            }
            PrivateBody::List(ListStatus {
                symbol: list.symbol,
                list_id: list.order_list_id,
                list_client_id: list.list_client_order_id,
                contingency_type: list.contingency_type,
                status_type: list.list_status_type,
                order_status: list.list_order_status,
                reject_reason: Cow::Borrowed(""),
                transaction_time_ns: 0,
                orders: list
                    .orders
                    .into_iter()
                    .map(|m| ListMember {
                        symbol: m.symbol,
                        order_id: m.order_id,
                        client_id: m.client_order_id,
                    })
                    .collect(),
            })
        }
        v::CommandBody::Algo(_) => {
            #[derive(Deserialize)]
            #[serde(rename_all = "camelCase")]
            struct Algo<'a> {
                #[serde(borrow)]
                symbol: Cow<'a, str>,
                algo_id: u64,
                #[serde(borrow)]
                client_algo_id: Cow<'a, str>,
                #[serde(borrow)]
                algo_type: Cow<'a, str>,
                #[serde(borrow)]
                order_type: Cow<'a, str>,
                #[serde(borrow)]
                side: Cow<'a, str>,
                #[serde(borrow)]
                position_side: Cow<'a, str>,
                #[serde(borrow)]
                time_in_force: Cow<'a, str>,
                quantity: Decimal,
                #[serde(borrow)]
                algo_status: Cow<'a, str>,
                #[serde(borrow)]
                actual_order_id: Option<Cow<'a, str>>,
                #[serde(borrow, rename = "actualType")]
                actual_order_type: Option<Cow<'a, str>>,
                actual_price: Option<Decimal>,
                actual_qty: Option<Decimal>,
                trigger_price: Option<Decimal>,
                price: Option<Decimal>,
                trigger_time: Option<u64>,
            }
            let algo: Algo<'_> =
                serde_json::from_slice(bytes).map_err(|_| Error::InvalidResponse)?;
            PrivateBody::Algo(Box::new(AlgoUpdate {
                symbol: algo.symbol,
                client_algo_id: algo.client_algo_id,
                algo_id: algo.algo_id,
                algo_type: algo.algo_type,
                order_type: algo.order_type,
                side: algo.side,
                position_side: algo.position_side,
                time_in_force: algo.time_in_force,
                quantity: algo.quantity,
                status: algo.algo_status,
                actual_order_id: algo.actual_order_id,
                actual_order_type: algo.actual_order_type,
                average_fill_price: algo.actual_price,
                executed_quantity: algo.actual_qty,
                trigger_price: algo.trigger_price,
                price: algo.price,
                trigger_time: algo.trigger_time,
                good_till_date: None,
                reject_message: None,
                activated: None,
            }))
        }
        _ => return Err(Error::UnsupportedCommand),
    };
    bind(
        session, market, command, orders, &body, bytes, None, received,
    )
}
