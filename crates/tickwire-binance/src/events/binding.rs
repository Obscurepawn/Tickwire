use super::*;
use ring::digest::{SHA256, digest};

/// `identity` is the caller's persisted mapping for the creating command (or list
/// leg). A native ID already in that mapping must agree with the message.
/// Additional assets come from the same pinned catalog as the command.
pub fn bind_order(
    message: &PrivateEvent<'_>,
    command: &v::CanonicalCommand,
    identity: &v::OrderIdentity,
    assets: &[v::AssetRef],
) -> Result<v::CanonicalEvent> {
    let PrivateBody::Order(o) = &message.body else {
        return Err(Error::UnsupportedCommand);
    };
    let c = command.command();
    let r = &c.context.request;
    let i = &c.context.instrument;
    let s = &message.session.identity;
    if assets.len() > 256 {
        return Err(Error::Capacity);
    }
    if s.risk_domain_id != r.risk_domain_id
        || s.venue != r.venue
        || s.environment != r.environment
        || s.account != r.account
        || o.symbol != i.symbol.as_str()
        || identity.order_id == [0; 16]
        || identity.original_intent_id != r.intent_id
        || identity.original_intent_exec_epoch != r.intent_exec_epoch
        || !matches!(
            (message.session.market, r.product),
            (Market::Spot, v::Product::Spot)
                | (
                    Market::Usdm,
                    v::Product::LinearPerpetual | v::Product::LinearFuture
                )
        )
    {
        return Err(Error::IdentityMismatch);
    }
    let spec = creating_spec(&c.body, identity)?;
    let side = match o.side.as_ref() {
        "BUY" => v::Side::Buy,
        "SELL" => v::Side::Sell,
        _ => return Err(Error::InvalidResponse),
    };
    let position = match o.position_side.as_deref() {
        None if message.session.market == Market::Spot => v::PositionSide::Both,
        Some("BOTH") => v::PositionSide::Both,
        Some("LONG") => v::PositionSide::Long,
        Some("SHORT") => v::PositionSide::Short,
        _ => return Err(Error::InvalidResponse),
    };
    if side != spec.side || position != spec.position_side {
        return Err(Error::IdentityMismatch);
    }
    let native = o.order_id.to_string();
    if let Some(old) = &identity.venue_order_id {
        if old.as_str() != native {
            return Err(Error::IdentityMismatch);
        }
    } else if matches!(c.body, v::CommandBody::Algo(_))
        || (o.client_id != spec.client_id.as_str()
            && o.original_client_id.as_deref() != Some(spec.client_id.as_str()))
    {
        return Err(Error::IdentityMismatch);
    }
    let mut order = identity.clone();
    order.venue_order_id = Some(v::VenueOrderId::new(native).map_err(|_| Error::InvalidResponse)?);
    if let Some(id) = o.list_id {
        if identity.list_id.is_none() {
            return Err(Error::IdentityMismatch);
        }
        let id = id.to_string();
        if identity
            .venue_list_id
            .as_ref()
            .is_some_and(|n| n.as_str() != id)
        {
            return Err(Error::IdentityMismatch);
        }
        order.venue_list_id = Some(v::VenueOrderId::new(id).map_err(|_| Error::InvalidResponse)?);
    } else if identity.venue_list_id.is_some() {
        return Err(Error::IdentityMismatch);
    }
    for (n, a) in assets.iter().enumerate() {
        if a.id == 0
            || a.version == 0
            || !(-38..=38).contains(&a.exponent)
            || assets[..n]
                .iter()
                .any(|b| b.id == a.id || b.symbol == a.symbol)
        {
            return Err(Error::IdentityMismatch);
        }
        for base in [&i.base, &i.quote, &i.settlement] {
            if (base.id == a.id || base.symbol == a.symbol) && base != a {
                return Err(Error::IdentityMismatch);
            }
        }
    }
    let status = match o.status.as_ref() {
        "PENDING_NEW" | "PENDING_CANCEL" => v::OrderStatus::Pending,
        "NEW" => v::OrderStatus::New,
        "PARTIALLY_FILLED" => v::OrderStatus::PartiallyFilled,
        "FILLED" => v::OrderStatus::Filled,
        "CANCELED" => v::OrderStatus::Canceled,
        "EXPIRED" | "EXPIRED_IN_MATCH" => v::OrderStatus::Expired,
        "REJECTED" => v::OrderStatus::Rejected,
        _ => v::OrderStatus::Unknown,
    };
    let known_execution = matches!(
        o.execution_type.as_ref(),
        "NEW"
            | "CANCELED"
            | "REPLACED"
            | "REJECTED"
            | "TRADE"
            | "EXPIRED"
            | "TRADE_PREVENTION"
            | "AMENDMENT"
            | "CALCULATED"
    );
    let observation = v::OrderObservation {
        order,
        status: if known_execution {
            status
        } else {
            v::OrderStatus::Unknown
        },
        side,
        position_side: position,
        original_quantity: Some(decimal(o.original_quantity)?),
        cumulative_quantity: Some(decimal(o.cumulative_quantity)?),
        cumulative_quote: o.cumulative_quote.map(decimal).transpose()?,
        // The stream does not report leaves quantity. A terminal cumulative is
        // distinct from unfilled quantity; no subtraction is invented here.
        leaves_quantity: None,
        order_price: Some(decimal(o.price)?),
    };
    let trade = matches!(o.execution_type.as_ref(), "TRADE" | "CALCULATED");
    let body = if trade {
        if o.last_quantity.mantissa() <= 0 || o.last_price.mantissa() <= 0 {
            return Err(Error::InvalidResponse);
        }
        if message.session.market == Market::Usdm && o.realized_pnl.is_none() {
            return Err(Error::InvalidResponse);
        }
        let mut fees = Vec::new();
        if let Some(amount) = o.commission {
            if let Some(symbol) = &o.commission_asset {
                let asset = resolve(symbol, i, assets)?;
                let amount = Decimal::new(
                    amount.mantissa().checked_neg().ok_or(Error::Overflow)?,
                    amount.scale(),
                )?;
                fees.push(v::AssetDelta {
                    asset,
                    amount: decimal(amount)?,
                });
            } else if amount.mantissa() != 0 {
                return Err(Error::IdentityMismatch);
            }
        } else {
            // A TRADE without commission information cannot stand in for a
            // complete order/trade event. TRADE_LITE is a separate raw variant.
            return Err(Error::InvalidResponse);
        }
        v::VenueEventBody::Fill(Box::new(v::FillEvent {
            observation,
            trade_id: v::VenueTradeId::new(o.trade_id.ok_or(Error::InvalidResponse)?.to_string())
                .map_err(|_| Error::InvalidResponse)?,
            last_quantity: decimal(o.last_quantity)?,
            last_price: decimal(o.last_price)?,
            last_quote: o.last_quote.map(decimal).transpose()?,
            realized_pnl: o
                .realized_pnl
                .map(|d| {
                    Ok(v::AssetDelta {
                        asset: i.settlement.clone(),
                        amount: decimal(d)?,
                    })
                })
                .transpose()?,
            liquidity: if o.maker {
                v::Liquidity::Maker
            } else {
                v::Liquidity::Taker
            },
            fees: v::BoundedList::new(fees).map_err(|_| Error::Capacity)?,
        }))
    } else {
        if o.last_quantity.mantissa() != 0
            || o.commission.is_some_and(|d| d.mantissa() != 0)
            || o.realized_pnl.is_some_and(|d| d.mantissa() != 0)
        {
            return Err(Error::InvalidResponse);
        }
        v::VenueEventBody::Order(Box::new(observation))
    };
    let hash = digest(&SHA256, message.raw.get().as_bytes());
    let event_id: String = hash.as_ref().iter().map(|b| format!("{b:02x}")).collect();
    v::CanonicalEvent::new(v::VenueEvent {
        identity: v::EventIdentity {
            risk_domain_id: s.risk_domain_id,
            venue: s.venue,
            environment: s.environment,
            account: s.account,
            product: r.product,
            instrument_id: i.instrument_id,
            instrument_version: i.instrument_version,
            source_connection_epoch: s.connection_epoch,
            source_sequence: None,
            event_id: v::BoundedText::new(event_id).map_err(|_| Error::InvalidResponse)?,
            venue_time_ns: Some(o.order_time_ns),
            received_mono_ns: message.received_mono_ns,
        },
        body,
    })
    .map_err(|_| Error::InvalidResponse)
}
fn decimal(d: Decimal) -> Result<v::Decimal> {
    v::Decimal::new(d.mantissa(), -(d.scale() as i8)).map_err(|_| Error::InvalidDecimal)
}
fn resolve(symbol: &str, i: &v::InstrumentBinding, assets: &[v::AssetRef]) -> Result<v::AssetRef> {
    [&i.base, &i.quote, &i.settlement]
        .into_iter()
        .chain(assets.iter())
        .find(|a| a.symbol.as_str() == symbol)
        .cloned()
        .ok_or(Error::IdentityMismatch)
}
fn creating_spec<'a>(body: &'a v::CommandBody, id: &v::OrderIdentity) -> Result<&'a v::OrderSpec> {
    use v::{CommandBody as C, SpotAdvancedCommand as A};
    let client = id.client_id.as_ref().map(|c| c.as_str());
    let spec = match body {
        C::New(o) | C::CancelReplace { replacement: o, .. } => o,
        C::SpotList(l) => {
            if id.list_id != Some(l.list_id)
                || id.list_client_id.as_ref().is_none_or(|c| c != &l.client_id)
            {
                return Err(Error::IdentityMismatch);
            }
            l.legs
                .as_slice()
                .iter()
                .find(|o| Some(o.client_id.as_str()) == client)
                .ok_or(Error::IdentityMismatch)?
        }
        C::Algo(a) => {
            if id.parent_order_id != Some(a.parent_order_id)
                || id.algo_client_id.as_ref() != Some(&a.order.client_id)
            {
                return Err(Error::IdentityMismatch);
            }
            return Ok(&a.order);
        }
        C::SpotAdvanced(A::New(o) | A::CancelReplace { replacement: o, .. }) => &o.order,
        C::SpotAdvanced(A::SorNew(o)) => o,
        C::SpotAdvanced(A::List(l)) => {
            if id.list_id != Some(l.list_id)
                || id.list_client_id.as_ref().is_none_or(|c| c != &l.client_id)
            {
                return Err(Error::IdentityMismatch);
            }
            &l.legs
                .as_slice()
                .iter()
                .find(|o| Some(o.order.client_id.as_str()) == client)
                .ok_or(Error::IdentityMismatch)?
                .order
        }
        _ => return Err(Error::UnsupportedCommand),
    };
    if client != Some(spec.client_id.as_str()) {
        return Err(Error::IdentityMismatch);
    }
    Ok(spec)
}
