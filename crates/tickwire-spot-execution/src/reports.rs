//! A synchronous request acknowledgement is distinct from an observed order state.
use crate::{SpotVenue, client_ok, symbol_ok};
use serde_json::Value;
use tickwire_venue::{
    CanonicalCommand, ClientId, CommandBody, Decimal, IntentKey, ModifyObservation,
    ModifyRejection, OrderStatus, Side, TerminalReason, TimeInForce, VenueOrderId, VenueToken,
    classify_spot_venue_reason,
};

pub use tickwire_venue::{OrderSnapshot, RequestReply as Reply};

/// Decoded synchronous outcome of a native `Modify` (`POST /v5/order/amend` or `POST /api/v5/trade/amend-order`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModifyReply {
    /// Confirmed in-place price/quantity modification (with optional venue-reported clamping).
    Modified(ModifyObservation),
    /// Structured venue rejection leaving the resting order untouched unless separately closed.
    Rejected {
        rejection: ModifyRejection,
        code_str: String,
        message: String,
    },
    /// Non-2xx or ambiguous response whose remote outcome requires per-order status inquiry.
    Unknown,
}

/// Optional cumulative-quote and average-price fields extracted from a raw Bybit/OKX order JSON row.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OrderRowExtras {
    pub cum_quote: Option<Decimal>,
    pub avg_price: Option<Decimal>,
}

/// Classifies Bybit V5 and OKX V5 synchronous/query error codes into per-order vs. account-wide actions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SpotIssueKind {
    /// Definitive business rejection proving a `New` or `Modify` request was rejected on the venue.
    DeterministicReject,
    /// Timeout, clock-window, or internal error whose remote execution outcome is unknown.
    Ambiguous,
    /// Target order absent or already closed on a query, cancel, or modify endpoint.
    NotFound,
    /// Category B credential, permission, IP/WAF ban, or rate-limit hazard requiring immediate account freeze.
    AccountHazard,
}

/// Pure venue error-code classifier. Unrecognized codes default conservatively to [`SpotIssueKind::Ambiguous`]
/// so unknown venue responses never fabricate a local rejection.
pub fn classify_issue(venue: SpotVenue, code: &str) -> SpotIssueKind {
    match venue {
        SpotVenue::Bybit => match code {
            "10003" | "10004" | "10005" | "10006" | "10007" | "10009" | "10010" | "10018"
            | "33004" | "401" | "403" | "418" | "429" => SpotIssueKind::AccountHazard,
            "110001" | "170145" | "170213" => SpotIssueKind::NotFound,
            "10000" | "10002" | "10016" | "170007" => SpotIssueKind::Ambiguous,
            "110003" | "110004" | "110007" | "110017" | "110025" | "110079" | "140001"
            | "170033" | "170121" | "170124" | "170130" | "170131" | "170132" | "170133"
            | "170134" | "170135" | "170136" | "170137" | "170139" | "170140" | "170192"
            | "170193" | "170194" | "170210" => SpotIssueKind::DeterministicReject,
            _ => SpotIssueKind::Ambiguous,
        },
        SpotVenue::Okx => match code {
            "50011" | "50101" | "50103" | "50105" | "50111" | "50113" | "50119" | "50120"
            | "401" | "403" | "418" | "429" => SpotIssueKind::AccountHazard,
            "51400" | "51401" | "51402" | "51501" | "51503" | "51603" => SpotIssueKind::NotFound,
            "50000" | "50001" | "50004" | "50013" | "50026" | "50102" => SpotIssueKind::Ambiguous,
            "51000" | "51001" | "51002" | "51004" | "51006" | "51008" | "51010" | "51019"
            | "51020" | "51030" | "51059" | "51080" | "51116" | "51119" | "51137" | "51502"
            | "51506" | "51509" => SpotIssueKind::DeterministicReject,
            _ => SpotIssueKind::Ambiguous,
        },
    }
}

/// Returns `true` when an HTTP status code signals an authentication, permission, WAF, or rate-limit hazard.
pub fn is_http_account_hazard(http_status: u16) -> bool {
    matches!(http_status, 401 | 403 | 418 | 429)
}

/// Returns `true` when `code` is a venue rate-limit error code (`Bybit` `10006`/`429`, `OKX` `50011`/`429`).
pub fn is_venue_rate_limit_code(venue: SpotVenue, code: &str) -> bool {
    matches!(
        (venue, code),
        (SpotVenue::Bybit, "10006" | "429") | (SpotVenue::Okx, "50011" | "429")
    )
}

#[derive(Debug, PartialEq, Eq)]
pub enum ReportError {
    Bounds,
    Invalid,
    Identity,
}
fn string<'a>(v: &'a Value, key: &str) -> Result<&'a str, ReportError> {
    v.get(key)
        .and_then(Value::as_str)
        .ok_or(ReportError::Invalid)
}
fn ids(
    venue: SpotVenue,
    item: &Value,
    command: &CommandBody,
) -> Result<(Option<ClientId>, VenueOrderId), ReportError> {
    let (client, native) = if venue == SpotVenue::Bybit {
        ("orderLinkId", "orderId")
    } else {
        ("clOrdId", "ordId")
    };
    let native = string(item, native)?;
    if !symbol_ok(native) {
        return Err(ReportError::Invalid);
    }
    let native = VenueOrderId::new(native).map_err(|_| ReportError::Invalid)?;
    let client = string(item, client)?;
    let client = if client.is_empty() {
        None
    } else if client_ok(venue, client) {
        Some(ClientId::new(client).map_err(|_| ReportError::Invalid)?)
    } else {
        return Err(ReportError::Invalid);
    };
    match command {
        CommandBody::New(o) => {
            if Some(&o.client_id) != client.as_ref() {
                return Err(ReportError::Identity);
            }
        }
        CommandBody::Cancel(o) | CommandBody::Query(o) => {
            if o.client_id
                .as_ref()
                .is_some_and(|id| Some(id) != client.as_ref())
                || o.venue_order_id.as_ref().is_some_and(|id| id != &native)
            {
                return Err(ReportError::Identity);
            }
        }
        CommandBody::Modify(m) => {
            if let Some(ref got_cid) = client {
                let matches_target = m.target.client_id.as_ref() == Some(got_cid);
                let matches_new = m.new_client_id.as_ref() == Some(got_cid);
                if !matches_target && !matches_new {
                    return Err(ReportError::Identity);
                }
            } else if m.target.venue_order_id.is_none() {
                return Err(ReportError::Identity);
            }
            if m.target.client_id.is_none()
                && m.target
                    .venue_order_id
                    .as_ref()
                    .is_some_and(|id| id != &native)
            {
                return Err(ReportError::Identity);
            }
            if venue == SpotVenue::Okx
                && let Some(req_id) = item.get("reqId").and_then(Value::as_str)
                && !req_id.is_empty()
                && let Some(expected_req) = &m.new_client_id
                && req_id != expected_req.as_str()
            {
                return Err(ReportError::Identity);
            }
        }
        _ => return Err(ReportError::Invalid),
    }
    Ok((client, native))
}
/// Decodes one HTTP response body correlated by the single outstanding command.
pub fn decode_reply(
    command: &CanonicalCommand,
    http_status: u16,
    raw: &[u8],
) -> Result<Reply, ReportError> {
    let cmd = command.command();
    let venue = SpotVenue::from_id(cmd.context.request.venue).ok_or(ReportError::Identity)?;
    if cmd.context.request.product != tickwire_venue::Product::Spot {
        return Err(ReportError::Identity);
    }
    if raw.len() > 256 * 1024 {
        return Err(ReportError::Bounds);
    }
    if !(200..300).contains(&http_status) {
        return Ok(Reply::Unknown);
    }
    let body: Value = serde_json::from_slice(raw).map_err(|_| ReportError::Invalid)?;
    let query = matches!(cmd.body, CommandBody::Query(_));
    let bybit = venue == SpotVenue::Bybit;
    let code = if bybit {
        body.get("retCode")
            .and_then(Value::as_i64)
            .ok_or(ReportError::Invalid)?
            .to_string()
    } else {
        string(&body, "code")?.into()
    };
    if code.is_empty() || code.len() > 32 || !code.bytes().all(|b| b.is_ascii_digit() || b == b'-')
    {
        return Err(ReportError::Invalid);
    }
    if code != "0" {
        if !bybit
            && !query
            && let Some([item]) = body
                .get("data")
                .and_then(Value::as_array)
                .map(Vec::as_slice)
            && let Some(sub) = item.get("sCode").and_then(Value::as_str)
        {
            if sub.is_empty() || sub.len() > 32 || !sub.bytes().all(|b| b.is_ascii_digit()) {
                return Err(ReportError::Invalid);
            }
            if let Some(cid) = item.get("clOrdId").and_then(Value::as_str)
                && !cid.is_empty()
            {
                let matches = match &cmd.body {
                    CommandBody::New(o) => o.client_id.as_str() == cid,
                    CommandBody::Cancel(o) => {
                        o.client_id.as_ref().is_none_or(|w| w.as_str() == cid)
                    }
                    CommandBody::Modify(m) => {
                        m.target
                            .client_id
                            .as_ref()
                            .is_none_or(|w| w.as_str() == cid)
                            || m.new_client_id.as_ref().is_some_and(|w| w.as_str() == cid)
                    }
                    _ => true,
                };
                if !matches {
                    return Err(ReportError::Identity);
                }
            }
            if let CommandBody::Modify(m) = &cmd.body
                && let Some(req_id) = item.get("reqId").and_then(Value::as_str)
                && !req_id.is_empty()
                && let Some(expected_req) = &m.new_client_id
                && req_id != expected_req.as_str()
            {
                return Err(ReportError::Identity);
            }
            if sub != "0" {
                return Ok(Reply::RequestIssue { code: sub.into() });
            }
        }
        return Ok(Reply::RequestIssue { code });
    }
    let item = if bybit && !query {
        body.get("result").ok_or(ReportError::Invalid)?
    } else {
        let list = if bybit {
            if string(&body["result"], "category")? != "spot" {
                return Err(ReportError::Identity);
            }
            &body["result"]["list"]
        } else {
            &body["data"]
        };
        let rows = list.as_array().ok_or(ReportError::Invalid)?;
        match rows.as_slice() {
            [r] => r,
            [] if query => return Ok(Reply::Unknown),
            _ => return Err(ReportError::Identity),
        }
    };
    if !bybit && !query {
        let sub = string(item, "sCode")?;
        if sub.is_empty() || sub.len() > 32 || !sub.bytes().all(|b| b.is_ascii_digit()) {
            return Err(ReportError::Invalid);
        }
        if sub != "0" {
            return Ok(Reply::RequestIssue { code: sub.into() });
        }
    }
    let (client_id, venue_order_id) = ids(venue, item, &cmd.body)?;
    if !query {
        return Ok(Reply::Acknowledged {
            client_id,
            venue_order_id,
        });
    }
    let symbol = string(item, if bybit { "symbol" } else { "instId" })?;
    if symbol != cmd.context.instrument.symbol.as_str()
        || !bybit && string(item, "instType")? != "SPOT"
    {
        return Err(ReportError::Identity);
    }
    let status = match (
        venue,
        string(item, if bybit { "orderStatus" } else { "state" })?,
    ) {
        (SpotVenue::Bybit, "New") | (SpotVenue::Okx, "live") => OrderStatus::New,
        (SpotVenue::Bybit, "PartiallyFilled") | (SpotVenue::Okx, "partially_filled") => {
            OrderStatus::PartiallyFilled
        }
        (SpotVenue::Bybit, "Filled") | (SpotVenue::Okx, "filled") => OrderStatus::Filled,
        (SpotVenue::Bybit, "Cancelled" | "PartiallyFilledCanceled")
        | (SpotVenue::Okx, "canceled" | "mmp_canceled") => OrderStatus::Canceled,
        (SpotVenue::Bybit, "Rejected") => OrderStatus::Rejected,
        _ => return Ok(Reply::Unknown),
    };
    let side = match (venue, string(item, "side")?) {
        (SpotVenue::Bybit, "Buy") | (SpotVenue::Okx, "buy") => Side::Buy,
        (SpotVenue::Bybit, "Sell") | (SpotVenue::Okx, "sell") => Side::Sell,
        _ => return Err(ReportError::Invalid),
    };
    let cumulative_base: Decimal = string(item, if bybit { "cumExecQty" } else { "accFillSz" })?
        .parse()
        .map_err(|_| ReportError::Invalid)?;
    if cumulative_base.coefficient() < 0 {
        return Err(ReportError::Invalid);
    }
    let venue_time_ns = string(item, if bybit { "updatedTime" } else { "uTime" })?
        .parse::<u64>()
        .ok()
        .and_then(|v| v.checked_mul(1_000_000))
        .filter(|v| *v != 0)
        .ok_or(ReportError::Invalid)?;
    Ok(Reply::Order(OrderSnapshot {
        client_id,
        venue_order_id,
        status,
        side,
        cumulative_base,
        venue_time_ns,
    }))
}

fn decimal_leq(a: Decimal, b: Decimal) -> bool {
    let min_exp = a.exponent().min(b.exponent());
    let scale_to = |d: Decimal| -> Option<i128> {
        let shift = u32::try_from(i16::from(d.exponent()) - i16::from(min_exp)).ok()?;
        10i128
            .checked_pow(shift)
            .and_then(|m| d.coefficient().checked_mul(m))
    };
    match (scale_to(a), scale_to(b)) {
        (Some(lhs), Some(rhs)) => lhs <= rhs,
        _ => false,
    }
}

fn sanitize_message(raw: &str) -> String {
    let trimmed = raw.trim();
    let mut out = String::with_capacity(trimmed.len().min(256));
    for ch in trimmed.chars() {
        if ch.is_control() {
            continue;
        }
        if out.len() + ch.len_utf8() > 256 {
            break;
        }
        out.push(ch);
    }
    out
}

/// Extracts a bounded human-readable error message (`retMsg` on Bybit, `data[0].sMsg` or `msg` on OKX)
/// from a raw JSON response body when present.
pub fn extract_error_message(venue: SpotVenue, raw: &[u8]) -> Option<String> {
    if raw.len() > 256 * 1024 {
        return None;
    }
    let body: Value = serde_json::from_slice(raw).ok()?;
    let msg = if venue == SpotVenue::Bybit {
        body.get("retMsg").and_then(Value::as_str).unwrap_or("")
    } else {
        let sub_msg = body
            .get("data")
            .and_then(Value::as_array)
            .and_then(|arr| arr.first())
            .and_then(|item| item.get("sMsg"))
            .and_then(Value::as_str)
            .unwrap_or("");
        if !sub_msg.trim().is_empty() {
            sub_msg
        } else {
            body.get("msg").and_then(Value::as_str).unwrap_or("")
        }
    };
    let clean = sanitize_message(msg);
    (!clean.is_empty()).then_some(clean)
}

/// Decodes a synchronous Bybit `/v5/order/amend` or OKX `/api/v5/trade/amend-order` HTTP response
/// into [`ModifyReply::Modified`], [`ModifyReply::Rejected`], or [`ModifyReply::Unknown`].
///
/// Supports optional venue-reported clamped `qty`/`newSz` and `price`/`newPx` fields on `Modified`,
/// and classifies structured business rejections via [`classify_spot_venue_reason`].
pub fn decode_modify_reply(
    command: &CanonicalCommand,
    http_status: u16,
    raw: &[u8],
    post_only: bool,
    tif: Option<TimeInForce>,
) -> Result<ModifyReply, ReportError> {
    let cmd = command.command();
    let venue = SpotVenue::from_id(cmd.context.request.venue).ok_or(ReportError::Identity)?;
    if cmd.context.request.product != tickwire_venue::Product::Spot {
        return Err(ReportError::Identity);
    }
    let CommandBody::Modify(m) = &cmd.body else {
        return Err(ReportError::Identity);
    };
    if raw.len() > 256 * 1024 {
        return Err(ReportError::Bounds);
    }
    if !(200..300).contains(&http_status) {
        return Ok(ModifyReply::Unknown);
    }
    let body: Value = serde_json::from_slice(raw).map_err(|_| ReportError::Invalid)?;
    let bybit = venue == SpotVenue::Bybit;
    let code = if bybit {
        body.get("retCode")
            .and_then(Value::as_i64)
            .ok_or(ReportError::Invalid)?
            .to_string()
    } else {
        string(&body, "code")?.to_owned()
    };
    if code.is_empty() || code.len() > 32 || !code.bytes().all(|b| b.is_ascii_digit() || b == b'-')
    {
        return Err(ReportError::Invalid);
    }
    let intent = IntentKey {
        epoch: cmd.context.request.intent_exec_epoch,
        intent_id: cmd.context.request.intent_id,
    };

    if bybit {
        if code != "0" {
            let result_obj = body.get("result");
            if let Some(cid) = result_obj
                .and_then(|r| r.get("orderLinkId"))
                .and_then(Value::as_str)
                && !cid.is_empty()
            {
                let matches_target = m
                    .target
                    .client_id
                    .as_ref()
                    .is_some_and(|w| w.as_str() == cid);
                let matches_new = m.new_client_id.as_ref().is_some_and(|w| w.as_str() == cid);
                if m.target.client_id.is_some() && !matches_target && !matches_new {
                    return Err(ReportError::Identity);
                }
            }
            let venue_order_id = result_obj
                .and_then(|r| r.get("orderId"))
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty() && symbol_ok(s))
                .and_then(|s| VenueOrderId::new(s).ok())
                .or_else(|| m.target.venue_order_id.clone());
            let amendment_id = result_obj
                .and_then(|r| r.get("amendId"))
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty() && symbol_ok(s))
                .and_then(|s| VenueToken::new(s).ok());
            let message = sanitize_message(
                body.get("retMsg")
                    .and_then(Value::as_str)
                    .unwrap_or_default(),
            );
            let code_num = code.parse::<i64>().ok();
            let reason = classify_spot_venue_reason(code_num, &code, &message, post_only, tif);
            return Ok(ModifyReply::Rejected {
                rejection: ModifyRejection {
                    target: m.target.clone(),
                    amendment_id,
                    intent: Some(intent),
                    new_client_id: m.new_client_id.clone(),
                    venue_order_id,
                    code: code_num,
                    reason,
                },
                code_str: code,
                message,
            });
        }
        let item = body.get("result").ok_or(ReportError::Invalid)?;
        let (client_id, venue_order_id) = ids(venue, item, &cmd.body)?;
        let new_quantity = match item.get("qty").and_then(Value::as_str) {
            Some(s) if !s.is_empty() => {
                let d: Decimal = s.parse().map_err(|_| ReportError::Invalid)?;
                if !d.is_positive() {
                    return Err(ReportError::Invalid);
                }
                d
            }
            _ => m.quantity,
        };
        let new_price = match item.get("price").and_then(Value::as_str) {
            Some(s) if !s.is_empty() => {
                let d: Decimal = s.parse().map_err(|_| ReportError::Invalid)?;
                if !d.is_positive() {
                    return Err(ReportError::Invalid);
                }
                d
            }
            _ => m.price,
        };
        let cumulative_filled = match item.get("cumExecQty").and_then(Value::as_str) {
            Some(s) if !s.is_empty() => {
                let d: Decimal = s.parse().map_err(|_| ReportError::Invalid)?;
                if d.coefficient() < 0 {
                    return Err(ReportError::Invalid);
                }
                decimal_leq(d, new_quantity).then_some(d)
            }
            _ => None,
        };
        let amendment_id = item
            .get("amendId")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty() && symbol_ok(s))
            .map(VenueToken::new)
            .transpose()
            .map_err(|_| ReportError::Invalid)?;
        return Ok(ModifyReply::Modified(ModifyObservation {
            target: m.target.clone(),
            amendment_id,
            intent: Some(intent),
            prior_client_id: m.target.client_id.clone(),
            new_client_id: client_id,
            venue_order_id: Some(venue_order_id),
            new_quantity,
            new_price,
            cumulative_filled,
        }));
    }

    // OKX `/api/v5/trade/amend-order`
    let data_arr = body.get("data").and_then(Value::as_array);
    if code == "0" {
        let rows = data_arr.ok_or(ReportError::Invalid)?;
        if rows.len() != 1 {
            return Err(ReportError::Identity);
        }
    }
    let data_item = data_arr.and_then(|arr| match arr.as_slice() {
        [only] => Some(only),
        _ => None,
    });

    let sub_code = if let Some(item) = data_item {
        if let Some(sub) = item.get("sCode").and_then(Value::as_str) {
            if sub.is_empty() || sub.len() > 32 || !sub.bytes().all(|b| b.is_ascii_digit()) {
                return Err(ReportError::Invalid);
            }
            Some(sub)
        } else if code == "0" {
            return Err(ReportError::Invalid);
        } else {
            None
        }
    } else {
        None
    };

    if code != "0" || sub_code.is_some_and(|s| s != "0") {
        if let Some(item) = data_item {
            if let Some(cid) = item.get("clOrdId").and_then(Value::as_str)
                && !cid.is_empty()
            {
                let matches = m
                    .target
                    .client_id
                    .as_ref()
                    .is_none_or(|w| w.as_str() == cid)
                    || m.new_client_id.as_ref().is_some_and(|w| w.as_str() == cid);
                if !matches {
                    return Err(ReportError::Identity);
                }
            }
            if let Some(req_id) = item.get("reqId").and_then(Value::as_str)
                && !req_id.is_empty()
                && let Some(expected_req) = &m.new_client_id
                && req_id != expected_req.as_str()
            {
                return Err(ReportError::Identity);
            }
        }
        let eff_code = sub_code
            .filter(|s| *s != "0")
            .unwrap_or(code.as_str())
            .to_owned();
        let raw_msg = data_item
            .and_then(|i| i.get("sMsg"))
            .and_then(Value::as_str)
            .filter(|s| !s.trim().is_empty())
            .or_else(|| body.get("msg").and_then(Value::as_str))
            .unwrap_or_default();
        let message = sanitize_message(raw_msg);
        let venue_order_id = data_item
            .and_then(|i| i.get("ordId"))
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty() && symbol_ok(s))
            .and_then(|s| VenueOrderId::new(s).ok())
            .or_else(|| m.target.venue_order_id.clone());
        let amendment_id = data_item
            .and_then(|i| i.get("reqId"))
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty() && symbol_ok(s))
            .and_then(|s| VenueToken::new(s).ok())
            .or_else(|| {
                m.new_client_id
                    .as_ref()
                    .and_then(|c| VenueToken::new(c.as_str()).ok())
            });
        let code_num = eff_code.parse::<i64>().ok();
        let reason = classify_spot_venue_reason(code_num, &eff_code, &message, post_only, tif);
        return Ok(ModifyReply::Rejected {
            rejection: ModifyRejection {
                target: m.target.clone(),
                amendment_id,
                intent: Some(intent),
                new_client_id: m.new_client_id.clone(),
                venue_order_id,
                code: code_num,
                reason,
            },
            code_str: eff_code,
            message,
        });
    }

    let item = data_item.ok_or(ReportError::Invalid)?;
    let (client_id, venue_order_id) = ids(venue, item, &cmd.body)?;
    let raw_sz = item
        .get("newSz")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .or_else(|| {
            item.get("sz")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
        });
    let new_quantity = match raw_sz {
        Some(s) => {
            let d: Decimal = s.parse().map_err(|_| ReportError::Invalid)?;
            if !d.is_positive() {
                return Err(ReportError::Invalid);
            }
            d
        }
        None => m.quantity,
    };
    let raw_px = item
        .get("newPx")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .or_else(|| {
            item.get("px")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
        });
    let new_price = match raw_px {
        Some(s) => {
            let d: Decimal = s.parse().map_err(|_| ReportError::Invalid)?;
            if !d.is_positive() {
                return Err(ReportError::Invalid);
            }
            d
        }
        None => m.price,
    };
    let cumulative_filled = match item.get("accFillSz").and_then(Value::as_str) {
        Some(s) if !s.is_empty() => {
            let d: Decimal = s.parse().map_err(|_| ReportError::Invalid)?;
            if d.coefficient() < 0 {
                return Err(ReportError::Invalid);
            }
            decimal_leq(d, new_quantity).then_some(d)
        }
        _ => None,
    };
    let amendment_id = item
        .get("reqId")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty() && symbol_ok(s))
        .map(VenueToken::new)
        .transpose()
        .map_err(|_| ReportError::Invalid)?
        .or_else(|| {
            m.new_client_id
                .as_ref()
                .and_then(|c| VenueToken::new(c.as_str()).ok())
        });
    Ok(ModifyReply::Modified(ModifyObservation {
        target: m.target.clone(),
        amendment_id,
        intent: Some(intent),
        prior_client_id: m.target.client_id.clone(),
        new_client_id: client_id,
        venue_order_id: Some(venue_order_id),
        new_quantity,
        new_price,
        cumulative_filled,
    }))
}

fn parse_positive_decimal_opt(row: &Value, key: &str) -> Option<Decimal> {
    let s = row.get(key)?.as_str()?.trim();
    if s.is_empty() {
        return None;
    }
    let d: Decimal = s.parse().ok()?;
    d.is_positive().then_some(d)
}

/// Extracts optional positive cumulative-quote (`cumExecValue`) and average-price
/// (`avgPrice` / `avgPx`) fields from a raw Bybit `order.spot` or OKX `orders` JSON row.
pub fn extract_order_row_extras(venue: SpotVenue, row: &Value) -> OrderRowExtras {
    match venue {
        SpotVenue::Bybit => OrderRowExtras {
            cum_quote: parse_positive_decimal_opt(row, "cumExecValue"),
            avg_price: parse_positive_decimal_opt(row, "avgPrice"),
        },
        SpotVenue::Okx => OrderRowExtras {
            cum_quote: None,
            avg_price: parse_positive_decimal_opt(row, "avgPx"),
        },
    }
}

fn ascii_contains_ignore_case(haystack: &str, needle: &str) -> bool {
    let h = haystack.as_bytes();
    let n = needle.as_bytes();
    !n.is_empty() && h.len() >= n.len() && h.windows(n.len()).any(|w| w.eq_ignore_ascii_case(n))
}

/// Classifies the terminal [`TerminalReason`] for a Bybit V5 (`rejectReason`, `cancelType`,
/// `smpType`) or OKX V5 (`state`, `cancelSource`, `cancelSourceReason`) order JSON row via
/// [`classify_spot_venue_reason`]. Returns `None` while `status` is open or non-terminal.
pub fn classify_order_row_terminal_reason(
    venue: SpotVenue,
    row: &Value,
    status: OrderStatus,
    post_only: bool,
    tif: Option<TimeInForce>,
) -> Option<TerminalReason> {
    match status {
        OrderStatus::Filled => return Some(TerminalReason::Filled),
        OrderStatus::Canceled | OrderStatus::Expired | OrderStatus::Rejected => {}
        _ => return None,
    }

    match venue {
        SpotVenue::Bybit => {
            let smp_type = row
                .get("smpType")
                .and_then(Value::as_str)
                .unwrap_or("")
                .trim();
            let cancel_type = row
                .get("cancelType")
                .and_then(Value::as_str)
                .unwrap_or("")
                .trim();
            let reject_reason = row
                .get("rejectReason")
                .and_then(Value::as_str)
                .unwrap_or("")
                .trim();

            if (!smp_type.is_empty() && !smp_type.eq_ignore_ascii_case("None"))
                || ascii_contains_ignore_case(cancel_type, "Smp")
                || ascii_contains_ignore_case(reject_reason, "Smp")
            {
                return Some(classify_spot_venue_reason(
                    None, "SMP", smp_type, post_only, tif,
                ));
            }
            if ascii_contains_ignore_case(cancel_type, "ReduceOnly") {
                return Some(classify_spot_venue_reason(
                    None,
                    "reduce_only",
                    cancel_type,
                    post_only,
                    tif,
                ));
            }
            if let Ok(code_num) = reject_reason.parse::<i64>()
                && code_num != 0
            {
                return Some(classify_spot_venue_reason(
                    Some(code_num),
                    reject_reason,
                    cancel_type,
                    post_only,
                    tif,
                ));
            }
            if !reject_reason.is_empty()
                && !reject_reason.eq_ignore_ascii_case("EC_NoError")
                && !reject_reason.eq_ignore_ascii_case("EC_PerCancelRequest")
            {
                return Some(classify_spot_venue_reason(
                    None,
                    reject_reason,
                    cancel_type,
                    post_only,
                    tif,
                ));
            }
            let fallback_code = match status {
                OrderStatus::Rejected => "REJECTED",
                _ => {
                    if reject_reason.eq_ignore_ascii_case("EC_PerCancelRequest")
                        || cancel_type.eq_ignore_ascii_case("CancelByUser")
                    {
                        "USER_CANCELED"
                    } else {
                        "CANCELED"
                    }
                }
            };
            Some(classify_spot_venue_reason(
                None,
                fallback_code,
                cancel_type,
                post_only,
                tif,
            ))
        }
        SpotVenue::Okx => {
            let cancel_source = row
                .get("cancelSource")
                .and_then(Value::as_str)
                .unwrap_or("")
                .trim();
            let cancel_reason = row
                .get("cancelSourceReason")
                .and_then(Value::as_str)
                .unwrap_or("")
                .trim();

            if cancel_source == "32"
                || ascii_contains_ignore_case(cancel_source, "stp")
                || ascii_contains_ignore_case(cancel_source, "smp")
                || ascii_contains_ignore_case(cancel_reason, "self trade")
                || ascii_contains_ignore_case(cancel_reason, "self-trade")
                || ascii_contains_ignore_case(cancel_reason, "stp")
            {
                return Some(classify_spot_venue_reason(
                    None,
                    "SMP",
                    cancel_reason,
                    post_only,
                    tif,
                ));
            }
            if cancel_source == "20" || cancel_source == "31" {
                return Some(classify_spot_venue_reason(
                    None,
                    "EC_PostOnlyWillTakeLiquidity",
                    cancel_reason,
                    post_only,
                    tif,
                ));
            }
            if cancel_source == "13" || cancel_source == "14" {
                let eff_tif = match tif {
                    Some(TimeInForce::Ioc | TimeInForce::Fok) => tif,
                    _ => Some(TimeInForce::Ioc),
                };
                return Some(classify_spot_venue_reason(
                    None,
                    "CANCELED",
                    cancel_reason,
                    post_only,
                    eff_tif,
                ));
            }
            if cancel_source == "15" {
                return Some(classify_spot_venue_reason(
                    Some(51006),
                    "51006",
                    cancel_reason,
                    post_only,
                    tif,
                ));
            }
            if let Ok(code_num) = cancel_source.parse::<i64>()
                && (code_num == 429 || !(0..10_000).contains(&code_num))
            {
                return Some(classify_spot_venue_reason(
                    Some(code_num),
                    cancel_source,
                    cancel_reason,
                    post_only,
                    tif,
                ));
            }
            if !cancel_reason.is_empty() {
                let classified =
                    classify_spot_venue_reason(None, "", cancel_reason, post_only, tif);
                if classified != TerminalReason::Unknown {
                    return Some(classified);
                }
            }
            let fallback_code = match status {
                OrderStatus::Rejected => "REJECTED",
                _ if cancel_source == "1" || cancel_source == "2" => "USER_CANCELED",
                _ => "CANCELED",
            };
            Some(classify_spot_venue_reason(
                None,
                fallback_code,
                cancel_reason,
                post_only,
                tif,
            ))
        }
    }
}
