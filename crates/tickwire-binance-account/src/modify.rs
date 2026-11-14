//! Native `CommandBody::Modify` binding, `AmendAmbiguous` requery resolution,
//! and zero-float `ProjectedFill` extraction across Binance Spot and USD-M Futures.
use crate::*;
use ring::digest::{Context, SHA256};
use serde::Deserialize;
use serde_json::value::RawValue;
use tickwire_account::{
    Account, Fixed, OrderKey, ProjectedPriceInput, classify_error_reason as classify_account_error,
};
use tickwire_account_gateway::{AccountGateway, Authority, Encoder};
use tickwire_account_journal::Backend;
use tickwire_binance::{
    events::{OrderUpdate, PrivateBody, PrivateStream, SessionContext, SessionIdentity},
    feed::TimeUnit,
    request::Market,
    response::{self, FailureKind},
};
use tickwire_binance_private::PrivateEvent;

#[derive(Deserialize)]
struct ExtraFields<'a> {
    #[serde(borrow)]
    side: Option<&'a str>,
    #[serde(borrow, rename = "positionSide")]
    position: Option<&'a str>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SpotCancelReplaceSuccess<'a> {
    #[serde(borrow)]
    cancel_result: &'a str,
    #[serde(borrow)]
    new_order_result: &'a str,
    #[serde(borrow)]
    new_order_response: &'a RawValue,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SpotCancelReplaceFailure<'a> {
    #[serde(borrow)]
    cancel_result: &'a str,
    #[serde(borrow)]
    new_order_result: &'a str,
    #[serde(borrow)]
    cancel_response: Option<SpotErrorPayload<'a>>,
}

#[derive(Deserialize)]
struct SpotErrorPayload<'a> {
    code: i64,
    #[serde(default, borrow)]
    msg: std::borrow::Cow<'a, str>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct AmendReplyQuote {
    amended_order: Option<AmendedOrderQuote>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct AmendedOrderQuote {
    executed_qty: tickwire_binance::decimal::Decimal,
    cumulative_quote_qty: tickwire_binance::decimal::Decimal,
}

fn binance_decimal_to_venue(n: tickwire_binance::decimal::Decimal) -> Result<v::Decimal> {
    v::Decimal::new(n.mantissa(), -(n.scale() as i8)).map_err(|_| Error::Encoding)
}

fn binance_decimal_to_fixed_exact(
    n: tickwire_binance::decimal::Decimal,
    target_scale: u8,
) -> Option<Fixed> {
    if n.mantissa() < 0 {
        return None;
    }
    if n.scale() <= target_scale {
        let factor = 10_i128.checked_pow(u32::from(target_scale - n.scale()))?;
        let atoms = n.mantissa().checked_mul(factor)?;
        Fixed::from_atoms(atoms, target_scale).ok()
    } else {
        let factor = 10_i128.checked_pow(u32::from(n.scale() - target_scale))?;
        if n.mantissa() % factor != 0 {
            return None;
        }
        Fixed::from_atoms(n.mantissa() / factor, target_scale).ok()
    }
}

fn binance_decimal_to_fixed_lossless(n: tickwire_binance::decimal::Decimal) -> Option<Fixed> {
    if n.mantissa() <= 0 {
        return None;
    }
    Fixed::from_atoms(n.mantissa(), n.scale()).ok()
}

pub(crate) fn is_non_freezing_account_refusal(err: &tickwire_account::Error) -> bool {
    matches!(
        err,
        tickwire_account::Error::CrossesBook
            | tickwire_account::Error::OrderNotOpen
            | tickwire_account::Error::UnknownOrder
            | tickwire_account::Error::TooManyPendingActions
            | tickwire_account::Error::InvalidPrice
            | tickwire_account::Error::InvalidQuantity
            | tickwire_account::Error::NoChange
            | tickwire_account::Error::InsufficientAsset
            | tickwire_account::Error::InsufficientCapital
            | tickwire_account::Error::Limit
            | tickwire_account::Error::ManagementDenied
            | tickwire_account::Error::Risk(
                tickwire_risk::RiskError::InsufficientAsset
                    | tickwire_risk::RiskError::InsufficientCollateral
                    | tickwire_risk::RiskError::VenueAvailableCap
            )
    )
}

pub(crate) fn classify_refusal_error(err: &Error) -> Option<v::TerminalReason> {
    match err {
        Error::RateLimited(_) => Some(v::TerminalReason::RateLimited),
        Error::Gateway(tickwire_account_gateway::Error::Account(acc_err))
            if is_non_freezing_account_refusal(acc_err) =>
        {
            let r = classify_account_error(acc_err);
            Some(if r == v::TerminalReason::Unknown {
                v::TerminalReason::AbortedBeforeSend
            } else {
                r
            })
        }
        _ => None,
    }
}

pub(crate) fn resolve_target_registration<'a>(
    registry: &'a OrderRegistry,
    overrides: Option<&'a BTreeMap<IntentKey, Registration>>,
    inst_id: u64,
    symbol: &str,
    target: &v::OrderTarget,
) -> Result<(IntentKey, &'a Registration, usize)> {
    if target.order_id == [0; 16] && target.client_id.is_none() && target.venue_order_id.is_none() {
        return Err(Error::Identity);
    }
    let mut candidate_keys = BTreeSet::new();
    if target.order_id != [0; 16]
        && let Some((k, _)) = registry.lookup_by_order_id(&target.order_id)
    {
        candidate_keys.insert(k);
    }
    if let Some(vid) = &target.venue_order_id
        && let Some((k, _)) = registry.lookup_by_venue_id(inst_id, vid.as_str())
    {
        candidate_keys.insert(k);
    }
    if let Some(cid) = &target.client_id
        && let Some((k, _)) = registry.lookup_by_client_id(inst_id, cid.as_str())
    {
        candidate_keys.insert(k);
    }
    if let Some(ov) = overrides {
        candidate_keys.extend(ov.keys().copied());
    }
    let mut found = Vec::new();
    for key in candidate_keys {
        let Some(reg) = overrides
            .and_then(|ov| ov.get(&key))
            .or_else(|| registry.get(&key))
        else {
            continue;
        };
        let ctx_inst = &reg.command.command().context.instrument;
        if ctx_inst.instrument_id != inst_id || ctx_inst.symbol.as_str() != symbol {
            continue;
        }
        for idx in 0..reg.orders.len() {
            if target_matches_registration(target, reg, idx) {
                found.push((key, reg, idx));
            }
        }
    }
    match found.as_slice() {
        [one] => Ok(*one),
        _ => Err(Error::Identity),
    }
}

pub(crate) fn target_matches_registration(
    t: &v::OrderTarget,
    reg: &Registration,
    idx: usize,
) -> bool {
    let Some(id) = reg.orders.get(idx) else {
        return false;
    };
    if id.parent_order_id.is_some() || id.list_id.is_some() {
        return false;
    }
    if t.order_id == [0; 16] && t.client_id.is_none() && t.venue_order_id.is_none() {
        return false;
    }
    (t.order_id == [0; 16] || t.order_id == id.order_id)
        && t.original_intent_exec_epoch == id.original_intent_exec_epoch
        && t.client_id
            .as_ref()
            .is_none_or(|c| reg.matches_client_id(idx, c.as_str()))
        && t.venue_order_id
            .as_ref()
            .is_none_or(|v| reg.matches_venue_order_id(idx, v.as_str()))
}

fn hex_event_id(
    prefix: &[u8],
    event_domain: &[u8],
    command_digest: &v::Hash,
    epoch: u64,
    raw: &[u8],
    sub_index: u64,
) -> Result<v::BoundedText<64>> {
    let mut h = Context::new(&SHA256);
    h.update(prefix);
    h.update(event_domain);
    h.update(command_digest);
    h.update(&epoch.to_le_bytes());
    h.update(raw);
    h.update(&sub_index.to_le_bytes());
    let hex: String = h
        .finish()
        .as_ref()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    v::BoundedText::new(hex).map_err(|_| Error::Encoding)
}

pub(crate) fn extract_spot_stop_on_failure_rejection<'a>(
    decoded: &'a response::Response<'a>,
) -> Option<(i64, std::borrow::Cow<'a, str>)> {
    if !matches!(
        decoded.failure,
        Some(FailureKind::Partial | FailureKind::Rejected)
    ) {
        return None;
    }
    let err = decoded.error.as_ref()?;
    let data = err.data?;
    let parsed: SpotCancelReplaceFailure<'a> = serde_json::from_str(data.get()).ok()?;
    if parsed.cancel_result == "FAILURE" && parsed.new_order_result == "NOT_ATTEMPTED" {
        if let Some(cancel_err) = parsed.cancel_response {
            return Some((cancel_err.code, cancel_err.msg));
        }
        return Some((err.code, err.msg.clone()));
    }
    None
}

pub(crate) fn rejection<S: Store>(
    input: &AccountIngress<S>,
    record: &crate::rest::RestRecord,
    event_domain: &[u8],
    code: i64,
    msg: &str,
) -> Result<CanonicalEvent> {
    if record.query.is_some()
        || record.order_query.is_some()
        || record.structure_query.is_some()
        || record.amendment_query.is_some()
    {
        return Err(Error::Identity);
    }
    let v::CommandBody::Modify(m) = &record.command.command().body else {
        return Err(Error::Identity);
    };
    let inst = &record.command.command().context.instrument;
    let (_reg_key, reg, idx) = resolve_target_registration(
        &input.registry,
        None,
        inst.instrument_id,
        inst.symbol.as_str(),
        &m.target,
    )?;
    if !crate::rest::binding::allowed_with_reg(&record.command, reg, idx) {
        return Err(Error::Identity);
    }
    let spec = crate::rest::binding::spec(&reg.command, idx)?;
    let post_only = spec.time_in_force == Some(v::TimeInForce::PostOnly);
    let reason = v::classify_binance_reason(Some(code), msg, None, spec.time_in_force, post_only);
    let c = &reg.command.command().context;
    let event_id = hex_event_id(
        b"tickwire.modify.rejected.v1",
        event_domain,
        &record.command.digest(),
        record.epoch,
        &record.raw,
        0,
    )?;
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
            event_id,
            venue_time_ns: None,
            received_mono_ns: record.received,
        },
        body: v::VenueEventBody::ModifyRejected(Box::new(v::ModifyRejection {
            target: m.target.clone(),
            amendment_id: None,
            intent: Some(intent(&record.command)),
            new_client_id: m.new_client_id.clone(),
            venue_order_id: m.target.venue_order_id.clone(),
            code: Some(code),
            reason,
        })),
    })
    .map_err(|_| Error::Encoding)
}

pub(crate) fn bind_reply<S: Store>(
    input: &AccountIngress<S>,
    record: &crate::rest::RestRecord,
    event_domain: &[u8],
    body: &[u8],
) -> Result<(crate::rest::RestDisposition, CanonicalEvent)> {
    if record.query.is_some()
        || record.order_query.is_some()
        || record.structure_query.is_some()
        || record.amendment_query.is_some()
    {
        return Err(Error::Identity);
    }
    let v::CommandBody::Modify(m) = &record.command.command().body else {
        return Err(Error::Identity);
    };
    let inst = &record.command.command().context.instrument;
    let (_reg_key, reg, idx) = resolve_target_registration(
        &input.registry,
        None,
        inst.instrument_id,
        inst.symbol.as_str(),
        &m.target,
    )?;
    if !crate::rest::binding::allowed_with_reg(&record.command, reg, idx) {
        return Err(Error::Identity);
    }
    let spec = crate::rest::binding::spec(&reg.command, idx)?;
    let market = input.market()?;
    let order_bytes: &[u8] = if market == Market::Spot {
        if let Ok(cr) = serde_json::from_slice::<SpotCancelReplaceSuccess<'_>>(body) {
            if cr.cancel_result != "SUCCESS" || cr.new_order_result != "SUCCESS" {
                return Err(Error::Identity);
            }
            cr.new_order_response.get().as_bytes()
        } else {
            body
        }
    } else {
        body
    };

    let reply = response::decode_order(order_bytes).map_err(|_| Error::Encoding)?;
    let extra: ExtraFields<'_> =
        serde_json::from_slice(order_bytes).map_err(|_| Error::Encoding)?;
    if reply.symbol != inst.symbol.as_str() {
        return Err(Error::Identity);
    }
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
    }) || m.side != spec.side
        || m.position_side != spec.position_side
    {
        return Err(Error::Identity);
    }
    let native_str = reply.order_id.to_string();
    if market == Market::Spot {
        if let Some(ref expected_new_cid) = m.new_client_id
            && reply.client_order_id != expected_new_cid.as_str()
        {
            return Err(Error::Identity);
        }
    } else {
        if !reg.matches_client_id(idx, &reply.client_order_id) {
            return Err(Error::Identity);
        }
        if reg.orders[idx].venue_order_id.is_some() && !reg.matches_venue_order_id(idx, &native_str)
        {
            return Err(Error::Identity);
        }
    }

    let orig_qty = reply.original_quantity.ok_or(Error::Encoding)?;
    let price = reply.price.ok_or(Error::Encoding)?;
    if orig_qty.mantissa() <= 0 || price.mantissa() <= 0 {
        return Err(Error::Encoding);
    }
    let new_quantity = binance_decimal_to_venue(orig_qty)?;
    let new_price = binance_decimal_to_venue(price)?;
    let cumulative_filled = reply
        .executed_quantity
        .map(binance_decimal_to_venue)
        .transpose()?;
    let prior_client_id = m
        .target
        .client_id
        .clone()
        .or_else(|| Some(spec.client_id.clone()));
    let new_client_id =
        Some(v::ClientId::new(&reply.client_order_id).map_err(|_| Error::Encoding)?);
    let venue_order_id = Some(v::VenueOrderId::new(native_str).map_err(|_| Error::Encoding)?);

    let needs_reconcile = reply.executed_quantity.is_some_and(|q| q.mantissa() > 0)
        || reply.fills.is_some_and(|raw| raw.get() != "[]");
    let disposition = if needs_reconcile {
        crate::rest::RestDisposition::NeedsReconciliation
    } else {
        crate::rest::RestDisposition::Observed
    };

    let c = &reg.command.command().context;
    let event_id = hex_event_id(
        b"tickwire.modify.observed.v1",
        event_domain,
        &record.command.digest(),
        record.epoch,
        &record.raw,
        0,
    )?;
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
            event_id,
            venue_time_ns: None,
            received_mono_ns: record.received,
        },
        body: v::VenueEventBody::Modified(Box::new(v::ModifyObservation {
            target: m.target.clone(),
            amendment_id: None,
            intent: Some(intent(&record.command)),
            prior_client_id,
            new_client_id,
            venue_order_id,
            new_quantity,
            new_price,
            cumulative_filled,
        })),
    })
    .map_err(|_| Error::Encoding)?;
    Ok((disposition, event))
}

pub(crate) fn find_active_pending_modify<B: Backend, A: Authority, E: Encoder>(
    gateway: &AccountGateway<B, A, E>,
    inst_id: u64,
    order_key: &OrderKey,
) -> Option<(IntentKey, v::ModifyOrder)> {
    let account = gateway.account();
    if let Some(mod_intent) = account.order_pending_modify_intent(order_key)
        && let Some(cmd) = gateway.command(mod_intent)
        && cmd.command().context.instrument.instrument_id == inst_id
        && let v::CommandBody::Modify(m) = &cmd.command().body
    {
        return Some((mod_intent, m.clone()));
    }
    for mod_intent in account.admission_keys() {
        if !account.is_intent_active(mod_intent) || account.is_confirmed_modification(mod_intent) {
            continue;
        }
        let Some(cmd) = gateway.command(mod_intent) else {
            continue;
        };
        if cmd.command().context.instrument.instrument_id != inst_id {
            continue;
        }
        if let v::CommandBody::Modify(m) = &cmd.command().body
            && account.resolve_order_target(inst_id, &m.target).as_ref() == Some(order_key)
        {
            return Some((mod_intent, m.clone()));
        }
    }
    None
}

pub(crate) fn find_unsettled_modify<B: Backend, A: Authority, E: Encoder>(
    gateway: &AccountGateway<B, A, E>,
    inst_id: u64,
    order_key: &OrderKey,
) -> Option<(IntentKey, v::ModifyOrder)> {
    if let Some(active) = find_active_pending_modify(gateway, inst_id, order_key) {
        return Some(active);
    }
    let mod_intent = gateway
        .account()
        .order_superseded_modify_intent(order_key)?;
    let cmd = gateway.command(mod_intent)?;
    if cmd.command().context.instrument.instrument_id != inst_id {
        return None;
    }
    let v::CommandBody::Modify(m) = &cmd.command().body else {
        return None;
    };
    Some((mod_intent, m.clone()))
}

pub(crate) fn find_spot_modify_registration<'a, B: Backend, A: Authority, E: Encoder>(
    registry: &'a OrderRegistry,
    gateway: &AccountGateway<B, A, E>,
    symbol: &str,
    wire_client_id: &str,
) -> Option<(&'a Registration, &'a v::OrderIdentity)> {
    let mut found = Vec::new();
    for reg in registry.values() {
        let inst = &reg.command.command().context.instrument;
        if inst.symbol.as_str() != symbol || reg.orders.len() != 1 {
            continue;
        }
        let Ok(spec) = crate::rest::binding::spec(&reg.command, 0) else {
            continue;
        };
        let order_key = OrderKey {
            intent: intent(&reg.command),
            client_id: spec.client_id.clone(),
        };
        if let Some((_, m)) = find_unsettled_modify(gateway, inst.instrument_id, &order_key)
            && m.new_client_id
                .as_ref()
                .is_some_and(|c| c.as_str() == wire_client_id)
        {
            found.push((reg, &reg.orders[0]));
        }
    }
    match found.as_slice() {
        [one] => Some(*one),
        _ => None,
    }
}

pub(crate) fn has_unconfirmed_modify<B: Backend, A: Authority, E: Encoder>(
    gateway: &AccountGateway<B, A, E>,
    order_key: &OrderKey,
) -> bool {
    let Some((inst, _)) = gateway.account().order_terms(order_key) else {
        return false;
    };
    find_active_pending_modify(gateway, inst.instrument_id, order_key).is_some()
}

fn resting_terms_differ(
    account: &Account,
    order_key: &OrderKey,
    remote_qty: tickwire_binance::decimal::Decimal,
    remote_price: tickwire_binance::decimal::Decimal,
) -> Option<bool> {
    let view = account.order(order_key)?;
    let (_inst, spec) = account.order_terms(order_key)?;
    let qty_scale = view.effective_quantity.scale();
    let remote_qty_fixed = binance_decimal_to_fixed_exact(remote_qty, qty_scale)?;
    if remote_qty_fixed != view.effective_quantity {
        return Some(true);
    }
    let cur_price_dec = spec.price?;
    let cur_price_str = cur_price_dec.as_plain_string();
    let remote_price_str = binance_decimal_to_venue(remote_price)
        .ok()?
        .as_plain_string();
    let max_scale = cur_price_dec
        .exponent()
        .unsigned_abs()
        .max(remote_price.scale())
        .min(38);
    let cur_px = Fixed::parse(&cur_price_str, max_scale).ok()?;
    let rem_px = Fixed::parse(&remote_price_str, max_scale).ok()?;
    Some(cur_px != rem_px)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn bind_order_query_resolution<B: Backend, A: Authority, E: Encoder>(
    gateway: &AccountGateway<B, A, E>,
    record: &crate::rest::RestRecord,
    reg: &Registration,
    order_idx: usize,
    reply: &response::OrderReply<'_>,
    event_domain: &[u8],
    sub_index: usize,
) -> Result<Option<CanonicalEvent>> {
    let Some(oq) = record.order_query else {
        return Ok(None);
    };
    let id = reg.orders.get(order_idx).ok_or(Error::Identity)?;
    if oq.intent != intent(&reg.command) || oq.order_id != id.order_id {
        return Err(Error::Identity);
    }
    let inst_id = reg.command.command().context.instrument.instrument_id;
    let initial_cid = crate::rest::binding::spec(&reg.command, order_idx)?
        .client_id
        .clone();
    let order_key = OrderKey {
        intent: intent(&reg.command),
        client_id: initial_cid,
    };
    if gateway.account().order(&order_key).is_none() {
        return Ok(None);
    }

    let active_modify = find_unsettled_modify(gateway, inst_id, &order_key);
    let status_str = reply.status.as_deref().unwrap_or("");
    let is_rejected_status = status_str == "REJECTED";
    let is_terminal = matches!(
        status_str,
        "FILLED" | "CANCELED" | "EXPIRED" | "EXPIRED_IN_MATCH" | "REJECTED"
    );
    let rotated_to_new_cid = active_modify
        .as_ref()
        .and_then(|(_, m)| m.new_client_id.as_ref())
        .is_some_and(|nc| nc.as_str() == reply.client_order_id.as_ref())
        && id.client_id.as_ref().map(v::ClientId::as_str) != Some(reply.client_order_id.as_ref());

    if !is_rejected_status
        && let (Some(orig_qty), Some(price)) = (reply.original_quantity, reply.price)
        && orig_qty.mantissa() > 0
        && price.mantissa() > 0
        && (resting_terms_differ(gateway.account(), &order_key, orig_qty, price) == Some(true)
            || rotated_to_new_cid)
    {
        let native =
            v::VenueOrderId::new(reply.order_id.to_string()).map_err(|_| Error::Encoding)?;
        let target = active_modify
            .as_ref()
            .map(|(_, m)| m.target.clone())
            .unwrap_or_else(|| v::OrderTarget {
                order_id: id.order_id,
                expected_owner_revision: 1,
                original_intent_exec_epoch: id.original_intent_exec_epoch,
                client_id: id.client_id.clone(),
                venue_order_id: id.venue_order_id.clone(),
            });
        let cumulative_filled = reply
            .executed_quantity
            .filter(|q| q.mantissa() > 0)
            .map(binance_decimal_to_venue)
            .transpose()?;
        let new_client_id =
            Some(v::ClientId::new(reply.client_order_id.as_ref()).map_err(|_| Error::Encoding)?);
        return build_query_modified_event(
            reg,
            record,
            event_domain,
            sub_index,
            target,
            active_modify.map(|(k, _)| k),
            id.client_id.clone(),
            new_client_id,
            Some(native),
            binance_decimal_to_venue(orig_qty)?,
            binance_decimal_to_venue(price)?,
            cumulative_filled,
        )
        .map(Some);
    }

    if let Some((mod_intent, mod_order)) = active_modify {
        let native =
            v::VenueOrderId::new(reply.order_id.to_string()).map_err(|_| Error::Encoding)?;
        let reason = if is_terminal {
            v::TerminalReason::OrderNotOpen
        } else {
            v::TerminalReason::Unknown
        };
        return build_query_modify_rejected_event(
            reg,
            record,
            event_domain,
            sub_index,
            mod_order.target,
            Some(mod_intent),
            mod_order.new_client_id,
            Some(native),
            reason,
        )
        .map(Some);
    }

    Ok(None)
}

#[allow(clippy::too_many_arguments)]
fn build_query_modified_event(
    reg: &Registration,
    record: &crate::rest::RestRecord,
    event_domain: &[u8],
    sub_index: usize,
    target: v::OrderTarget,
    mod_intent: Option<IntentKey>,
    prior_client_id: Option<v::ClientId>,
    new_client_id: Option<v::ClientId>,
    venue_order_id: Option<v::VenueOrderId>,
    new_quantity: v::Decimal,
    new_price: v::Decimal,
    cumulative_filled: Option<v::Decimal>,
) -> Result<CanonicalEvent> {
    let c = &reg.command.command().context;
    let event_id = hex_event_id(
        b"tickwire.order-query.modify.v1",
        event_domain,
        &record.command.digest(),
        record.epoch,
        &record.raw,
        sub_index as u64,
    )?;
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
            event_id,
            venue_time_ns: None,
            received_mono_ns: record.received,
        },
        body: v::VenueEventBody::Modified(Box::new(v::ModifyObservation {
            target,
            amendment_id: None,
            intent: mod_intent,
            prior_client_id,
            new_client_id,
            venue_order_id,
            new_quantity,
            new_price,
            cumulative_filled,
        })),
    })
    .map_err(|_| Error::Encoding)
}

#[allow(clippy::too_many_arguments)]
fn build_query_modify_rejected_event(
    reg: &Registration,
    record: &crate::rest::RestRecord,
    event_domain: &[u8],
    sub_index: usize,
    target: v::OrderTarget,
    mod_intent: Option<IntentKey>,
    new_client_id: Option<v::ClientId>,
    venue_order_id: Option<v::VenueOrderId>,
    reason: v::TerminalReason,
) -> Result<CanonicalEvent> {
    let c = &reg.command.command().context;
    let event_id = hex_event_id(
        b"tickwire.order-query.modify-rejected.v1",
        event_domain,
        &record.command.digest(),
        record.epoch,
        &record.raw,
        sub_index as u64,
    )?;
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
            event_id,
            venue_time_ns: None,
            received_mono_ns: record.received,
        },
        body: v::VenueEventBody::ModifyRejected(Box::new(v::ModifyRejection {
            target,
            amendment_id: None,
            intent: mod_intent,
            new_client_id,
            venue_order_id,
            code: None,
            reason,
        })),
    })
    .map_err(|_| Error::Encoding)
}

pub(crate) fn verify_raw<S: Store>(
    input: &AccountIngress<S>,
    record: &crate::rest::RestRecord,
    event: &CanonicalEvent,
) -> Result<()> {
    if record.query.is_some()
        || record.structure_query.is_some()
        || record.amendment_query.is_some()
    {
        return Err(Error::Identity);
    }
    let mut decoder =
        tickwire_transport::http::ResponseDecoder::new(input.config.max_raw_bytes, 256)
            .map_err(Error::Transport)?;
    decoder.push(&record.raw).map_err(Error::Transport)?;
    decoder.finish();
    let http_resp = decoder
        .next_response()
        .map_err(Error::Transport)?
        .ok_or(Error::Encoding)?;
    if http_resp.status != record.status || !decoder.take_buffered().is_empty() {
        return Err(Error::Identity);
    }

    if let Some(oq) = record.order_query {
        if http_resp.status != 200 {
            return Err(Error::Identity);
        }
        let reg = input.registry.get(&oq.intent).ok_or(Error::Identity)?;
        let order_idx = reg
            .orders
            .iter()
            .position(|o| o.order_id == oq.order_id)
            .ok_or(Error::Identity)?;
        let reply = response::decode_order(&http_resp.body).map_err(|_| Error::Encoding)?;
        if reply.symbol != reg.command.command().context.instrument.symbol.as_str() {
            return Err(Error::Identity);
        }
        let native =
            v::VenueOrderId::new(reply.order_id.to_string()).map_err(|_| Error::Encoding)?;
        let actual = match &event.event().body {
            v::VenueEventBody::Modified(obs) => {
                let orig_qty = reply.original_quantity.ok_or(Error::Encoding)?;
                let price = reply.price.ok_or(Error::Encoding)?;
                let expected_cum = reply
                    .executed_quantity
                    .filter(|q| q.mantissa() > 0)
                    .map(binance_decimal_to_venue)
                    .transpose()?;
                let expected_cid = Some(
                    v::ClientId::new(reply.client_order_id.as_ref())
                        .map_err(|_| Error::Encoding)?,
                );
                if !target_matches_registration(&obs.target, reg, order_idx) {
                    return Err(Error::Identity);
                }
                build_query_modified_event(
                    reg,
                    record,
                    b"tickwire.rest.observation.v1",
                    0,
                    obs.target.clone(),
                    obs.intent,
                    obs.prior_client_id.clone(),
                    expected_cid,
                    Some(native),
                    binance_decimal_to_venue(orig_qty)?,
                    binance_decimal_to_venue(price)?,
                    expected_cum,
                )?
            }
            v::VenueEventBody::ModifyRejected(rej) => {
                if !target_matches_registration(&rej.target, reg, order_idx) || rej.code.is_some() {
                    return Err(Error::Identity);
                }
                let is_terminal = matches!(
                    reply.status.as_deref().unwrap_or(""),
                    "FILLED" | "CANCELED" | "EXPIRED" | "EXPIRED_IN_MATCH" | "REJECTED"
                );
                let expected_reason = if is_terminal {
                    v::TerminalReason::OrderNotOpen
                } else {
                    v::TerminalReason::Unknown
                };
                build_query_modify_rejected_event(
                    reg,
                    record,
                    b"tickwire.rest.observation.v1",
                    0,
                    rej.target.clone(),
                    rej.intent,
                    rej.new_client_id.clone(),
                    Some(native),
                    expected_reason,
                )?
            }
            _ => return Err(Error::Identity),
        };
        if &actual != event {
            return Err(Error::Identity);
        }
        return Ok(());
    }

    let decoded = response::decode_rest(input.market()?, http_resp.status, &http_resp.body)
        .map_err(|_| Error::Encoding)?;
    let actual = match &event.event().body {
        v::VenueEventBody::Modified(_) => {
            if decoded.failure.is_some() {
                return Err(Error::Identity);
            }
            let raw_val = decoded.result.ok_or(Error::Encoding)?;
            let (_disp, ev) = bind_reply(
                input,
                record,
                b"tickwire.rest.observation.v1",
                raw_val.get().as_bytes(),
            )?;
            ev
        }
        v::VenueEventBody::ModifyRejected(_) => {
            if let Some((code, msg)) = extract_spot_stop_on_failure_rejection(&decoded) {
                rejection(input, record, b"tickwire.rest.observation.v1", code, &msg)?
            } else if matches!(
                decoded.failure,
                Some(FailureKind::Rejected | FailureKind::NotFound)
            ) {
                let api_err = decoded.error.as_ref().ok_or(Error::Encoding)?;
                rejection(
                    input,
                    record,
                    b"tickwire.rest.observation.v1",
                    api_err.code,
                    &api_err.msg,
                )?
            } else {
                return Err(Error::Identity);
            }
        }
        _ => return Err(Error::Identity),
    };
    if &actual != event {
        return Err(Error::Identity);
    }
    Ok(())
}

pub(crate) fn should_bind_private_modify(
    account: &Account,
    reg: &Registration,
    identity: &v::OrderIdentity,
    wire: &OrderUpdate<'_>,
) -> bool {
    if wire.execution_type == "AMENDMENT" {
        return true;
    }
    if matches!(
        wire.execution_type.as_ref(),
        "TRADE" | "CALCULATED" | "REPLACED"
    ) || !matches!(wire.status.as_ref(), "NEW" | "PARTIALLY_FILLED" | "FILLED")
        || wire.original_quantity.mantissa() <= 0
        || wire.price.mantissa() <= 0
    {
        return false;
    }
    let idx = reg
        .orders
        .iter()
        .position(|o| o.order_id == identity.order_id)
        .unwrap_or(0);
    let Ok(spec) = crate::rest::binding::spec(&reg.command, idx) else {
        return false;
    };
    let order_key = OrderKey {
        intent: intent(&reg.command),
        client_id: spec.client_id.clone(),
    };
    !reg.matches_client_id(idx, &wire.client_id)
        || resting_terms_differ(account, &order_key, wire.original_quantity, wire.price)
            == Some(true)
}

pub(crate) fn prepare_private_bind_identity(
    market: Market,
    reg: &Registration,
    identity: &v::OrderIdentity,
    wire: &OrderUpdate<'_>,
    allow_unseen_spot_rotation: bool,
) -> Result<v::OrderIdentity> {
    let idx = reg
        .orders
        .iter()
        .position(|o| o.order_id == identity.order_id)
        .ok_or(Error::Identity)?;
    let native = wire.order_id.to_string();
    if reg.matches_venue_order_id(idx, &native)
        || (allow_unseen_spot_rotation
            && market == Market::Spot
            && !reg.matches_client_id(idx, &wire.client_id))
    {
        if identity
            .venue_order_id
            .as_ref()
            .map(v::VenueOrderId::as_str)
            != Some(native.as_str())
        {
            let mut matched_id = identity.clone();
            matched_id.venue_order_id =
                Some(v::VenueOrderId::new(native).map_err(|_| Error::Encoding)?);
            return Ok(matched_id);
        }
        Ok(identity.clone())
    } else {
        Ok(identity.clone())
    }
}

pub(crate) fn bind_private(
    reg: &Registration,
    identity: &v::OrderIdentity,
    ordinary_order_event: &CanonicalEvent,
    wire: &OrderUpdate<'_>,
) -> Result<CanonicalEvent> {
    if matches!(wire.execution_type.as_ref(), "TRADE" | "CALCULATED")
        || wire.last_quantity.mantissa() != 0
        || wire.commission.is_some_and(|c| c.mantissa() != 0)
        || wire.original_quantity.mantissa() <= 0
        || wire.price.mantissa() <= 0
        || identity.parent_order_id.is_some()
        || identity.list_id.is_some()
    {
        return Err(Error::Identity);
    }
    let idx = reg
        .orders
        .iter()
        .position(|o| o.order_id == identity.order_id)
        .ok_or(Error::Identity)?;
    let native_str = wire.order_id.to_string();
    let client_id_rotated = !reg.matches_client_id(idx, &wire.client_id);
    if identity.venue_order_id.is_some()
        && !client_id_rotated
        && !reg.matches_venue_order_id(idx, &native_str)
    {
        return Err(Error::Identity);
    }
    let venue_order_id = Some(v::VenueOrderId::new(native_str).map_err(|_| Error::Encoding)?);
    let prior_client_id = wire
        .original_client_id
        .as_deref()
        .filter(|s| !s.is_empty())
        .map(v::ClientId::new)
        .transpose()
        .map_err(|_| Error::Encoding)?
        .or_else(|| identity.client_id.clone());
    let new_client_id =
        Some(v::ClientId::new(wire.client_id.as_ref()).map_err(|_| Error::Encoding)?);
    let amendment_id = wire
        .execution_id
        .and_then(|id| v::VenueToken::new(id.to_string()).ok());
    let cumulative_filled = (wire.cumulative_quantity.mantissa() > 0)
        .then(|| binance_decimal_to_venue(wire.cumulative_quantity))
        .transpose()?;

    let mut ev = ordinary_order_event.event().clone();
    ev.body = v::VenueEventBody::Modified(Box::new(v::ModifyObservation {
        target: v::OrderTarget {
            order_id: identity.order_id,
            expected_owner_revision: 1,
            original_intent_exec_epoch: identity.original_intent_exec_epoch,
            client_id: identity.client_id.clone(),
            venue_order_id: identity.venue_order_id.clone(),
        },
        amendment_id,
        intent: None,
        prior_client_id,
        new_client_id,
        venue_order_id,
        new_quantity: binance_decimal_to_venue(wire.original_quantity)?,
        new_price: binance_decimal_to_venue(wire.price)?,
        cumulative_filled,
    }));
    CanonicalEvent::new(ev).map_err(|_| Error::Encoding)
}

impl<S: Store> AccountIngress<S> {
    pub(crate) fn check_modify_raw(
        &self,
        meta: &SourceMeta,
        raw: &[u8],
        event: &CanonicalEvent,
    ) -> Result<()> {
        let v::VenueEventBody::Modified(obs) = &event.event().body else {
            return Err(Error::Identity);
        };
        let scope = &self.config.baseline.snapshot().scope;
        let session = SessionContext::new(
            SessionIdentity {
                risk_domain_id: scope.risk_domain_id,
                venue: scope.venue,
                environment: scope.environment,
                account: scope.account,
                connection_epoch: meta.epoch,
            },
            self.market()?,
            match meta.subscription {
                Some(subscription_id) => PrivateStream::SpotSubscription { subscription_id },
                None => PrivateStream::UsdmListenKey,
            },
            if meta.micros {
                TimeUnit::Microseconds
            } else {
                TimeUnit::Milliseconds
            },
        )
        .map_err(|_| Error::Identity)?;
        let envelope = if let Some(subscription) = meta.subscription {
            let mut bytes = format!("{{\"subscriptionId\":{subscription},\"event\":").into_bytes();
            bytes.extend_from_slice(raw);
            bytes.push(b'}');
            bytes
        } else {
            raw.to_vec()
        };
        let decoded = tickwire_binance::events::decode_private::<3>(
            &session,
            &envelope,
            self.config
                .max_raw_bytes
                .checked_add(128)
                .ok_or(Error::Capacity)?,
            meta.received,
        )
        .map_err(|_| Error::Encoding)?;
        let PrivateBody::Order(wire) = decoded.body() else {
            return Err(Error::Identity);
        };
        let candidates = self.registry.lookup_private_order(
            &wire.symbol,
            wire.order_id,
            &wire.client_id,
            wire.original_client_id.as_deref(),
        );
        let (reg, identity) = match candidates.as_slice() {
            [one] => *one,
            [] if self.market()? == Market::Spot && obs.target.order_id != [0; 16] => {
                let (intent, idx) = self
                    .registry
                    .lookup_by_order_id(&obs.target.order_id)
                    .ok_or(Error::Identity)?;
                let reg = self.registry.get(&intent).ok_or(Error::Identity)?;
                if reg.command.command().context.instrument.symbol.as_str() != wire.symbol {
                    return Err(Error::Identity);
                }
                let id = reg.orders.get(idx).ok_or(Error::Identity)?;
                (reg, id)
            }
            _ => return Err(Error::Identity),
        };
        let bind_id = prepare_private_bind_identity(self.market()?, reg, identity, wire, true)?;
        let ordinary = tickwire_binance::events::bind_order(
            &decoded,
            &reg.command,
            &bind_id,
            &self.baseline_assets,
        )
        .map_err(|_| Error::Identity)?;
        let actual = bind_private(reg, identity, &ordinary, wire)?;
        if &actual != event {
            return Err(Error::Identity);
        }
        Ok(())
    }
}

pub(crate) fn resolve_event_order_key(
    account: &Account,
    registry: &OrderRegistry,
    event: &CanonicalEvent,
) -> Option<OrderKey> {
    let inst_id = event.event().identity.instrument_id;
    match &event.event().body {
        v::VenueEventBody::Order(o) => Some(OrderKey {
            intent: IntentKey {
                epoch: o.order.original_intent_exec_epoch,
                intent_id: o.order.original_intent_id,
            },
            client_id: registry
                .get(&IntentKey {
                    epoch: o.order.original_intent_exec_epoch,
                    intent_id: o.order.original_intent_id,
                })
                .and_then(|reg| {
                    let idx = reg
                        .orders
                        .iter()
                        .position(|id| id.order_id == o.order.order_id)?;
                    crate::rest::binding::spec(&reg.command, idx)
                        .ok()
                        .map(|s| s.client_id.clone())
                })
                .or_else(|| o.order.client_id.clone())?,
        }),
        v::VenueEventBody::Amendment(a) => Some(OrderKey {
            intent: IntentKey {
                epoch: a.order.original_intent_exec_epoch,
                intent_id: a.order.original_intent_id,
            },
            client_id: a.order.client_id.clone()?,
        }),
        v::VenueEventBody::Modified(m) => {
            account
                .resolve_order_target(inst_id, &m.target)
                .or_else(|| {
                    let reg = registry
                        .lookup_by_order_id(&m.target.order_id)
                        .and_then(|(k, idx)| Some((k, registry.get(&k)?, idx)))?;
                    let spec = crate::rest::binding::spec(&reg.1.command, reg.2).ok()?;
                    Some(OrderKey {
                        intent: reg.0,
                        client_id: spec.client_id.clone(),
                    })
                })
        }
        _ => None,
    }
}

pub(crate) fn apply_projected_fill_from_private_event<
    S: Store,
    B: Backend,
    A: Authority,
    E: Encoder,
>(
    input: &AccountIngress<S>,
    gateway: &mut AccountGateway<B, A, E>,
    private_event: &PrivateEvent<'_>,
    canonical: &CanonicalEvent,
) {
    if !gateway.projected_fill_enabled() {
        return;
    }
    if !matches!(
        canonical.event().body,
        v::VenueEventBody::Order(_)
            | v::VenueEventBody::Amendment(_)
            | v::VenueEventBody::Modified(_)
    ) {
        return;
    }
    let PrivateBody::Order(wire) = private_event.body() else {
        return;
    };
    if matches!(wire.execution_type.as_ref(), "TRADE" | "CALCULATED")
        || wire.cumulative_quantity.mantissa() <= 0
    {
        return;
    }
    let Some(order_key) = resolve_event_order_key(gateway.account(), &input.registry, canonical)
    else {
        return;
    };
    let Some(view) = gateway.account().order(&order_key) else {
        return;
    };
    let Some(cum_qty) =
        binance_decimal_to_fixed_exact(wire.cumulative_quantity, view.original_quantity.scale())
    else {
        return;
    };
    let price_input = if let Some(cq) = wire
        .cumulative_quote
        .and_then(binance_decimal_to_fixed_lossless)
    {
        ProjectedPriceInput::CumulativeQuote { cum_quote: cq }
    } else if let Some(ap) = wire
        .average_price
        .and_then(binance_decimal_to_fixed_lossless)
    {
        ProjectedPriceInput::AveragePrice { avg_price: ap }
    } else {
        ProjectedPriceInput::LimitFallback
    };
    let _ = gateway.on_cum_qty(&order_key, cum_qty, price_input);
}

fn apply_projected_fill_from_order_json<S: Store, B: Backend, A: Authority, E: Encoder>(
    input: &AccountIngress<S>,
    gateway: &mut AccountGateway<B, A, E>,
    order_bytes: &[u8],
    canonical: &CanonicalEvent,
) {
    if !gateway.projected_fill_enabled() {
        return;
    }
    let Some(order_key) = resolve_event_order_key(gateway.account(), &input.registry, canonical)
    else {
        return;
    };
    let Some(view) = gateway.account().order(&order_key) else {
        return;
    };
    let base_scale = view.original_quantity.scale();
    if let v::VenueEventBody::Amendment(_) = &canonical.event().body {
        if let Ok(parsed) = serde_json::from_slice::<AmendReplyQuote>(order_bytes)
            && let Some(row) = parsed.amended_order
            && row.executed_qty.mantissa() > 0
            && let Some(cum_qty) = binance_decimal_to_fixed_exact(row.executed_qty, base_scale)
        {
            let price_input = binance_decimal_to_fixed_lossless(row.cumulative_quote_qty)
                .map_or(ProjectedPriceInput::LimitFallback, |cum_quote| {
                    ProjectedPriceInput::CumulativeQuote { cum_quote }
                });
            let _ = gateway.on_cum_qty(&order_key, cum_qty, price_input);
        }
        return;
    }

    let payload_bytes: &[u8] =
        if let Ok(cr) = serde_json::from_slice::<SpotCancelReplaceSuccess<'_>>(order_bytes) {
            cr.new_order_response.get().as_bytes()
        } else {
            order_bytes
        };
    let Ok(reply) = response::decode_order(payload_bytes) else {
        return;
    };
    let Some(exec_dec) = reply.executed_quantity.filter(|q| q.mantissa() > 0) else {
        return;
    };
    let Some(cum_qty) = binance_decimal_to_fixed_exact(exec_dec, base_scale) else {
        return;
    };
    let price_input = if let Some(cq) = reply
        .cumulative_quote
        .and_then(binance_decimal_to_fixed_lossless)
    {
        ProjectedPriceInput::CumulativeQuote { cum_quote: cq }
    } else if let Some(ap) = reply
        .average_price
        .and_then(binance_decimal_to_fixed_lossless)
    {
        ProjectedPriceInput::AveragePrice { avg_price: ap }
    } else {
        ProjectedPriceInput::LimitFallback
    };
    let _ = gateway.on_cum_qty(&order_key, cum_qty, price_input);
}

pub(crate) fn apply_projected_fill_from_rest_record<
    S: Store,
    B: Backend,
    A: Authority,
    E: Encoder,
>(
    input: &AccountIngress<S>,
    gateway: &mut AccountGateway<B, A, E>,
    record: &crate::rest::RestRecord,
    canonical: &CanonicalEvent,
) {
    if !gateway.projected_fill_enabled()
        || !matches!(
            canonical.event().body,
            v::VenueEventBody::Order(_)
                | v::VenueEventBody::Amendment(_)
                | v::VenueEventBody::Modified(_)
        )
    {
        return;
    }
    let Ok(mut decoder) =
        tickwire_transport::http::ResponseDecoder::new(input.config.max_raw_bytes, 256)
    else {
        return;
    };
    if decoder.push(&record.raw).is_err() {
        return;
    }
    decoder.finish();
    let Ok(Some(http_resp)) = decoder.next_response() else {
        return;
    };
    let Ok(market) = input.market() else {
        return;
    };
    let Ok(decoded) = response::decode_rest(market, http_resp.status, &http_resp.body) else {
        return;
    };
    let Some(result_raw) = decoded.result else {
        return;
    };
    apply_projected_fill_from_order_json(input, gateway, result_raw.get().as_bytes(), canonical);
}

pub(crate) fn apply_projected_fill_from_ws_raw<S: Store, B: Backend, A: Authority, E: Encoder>(
    input: &AccountIngress<S>,
    gateway: &mut AccountGateway<B, A, E>,
    raw: &[u8],
    canonical: &CanonicalEvent,
) {
    if !gateway.projected_fill_enabled()
        || !matches!(
            canonical.event().body,
            v::VenueEventBody::Order(_)
                | v::VenueEventBody::Amendment(_)
                | v::VenueEventBody::Modified(_)
        )
    {
        return;
    }
    let Ok(market) = input.market() else {
        return;
    };
    let Ok(decoded) = response::decode_websocket(market, raw) else {
        return;
    };
    let Some(result_raw) = decoded.result else {
        return;
    };
    apply_projected_fill_from_order_json(input, gateway, result_raw.get().as_bytes(), canonical);
}
