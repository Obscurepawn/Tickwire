//! A synchronous request acknowledgement is distinct from an observed order state.
use crate::{SpotVenue, client_ok, symbol_ok};
use serde_json::Value;
use tickwire_venue::{
    CanonicalCommand, ClientId, CommandBody, Decimal, OrderStatus, Side, VenueOrderId,
};

pub use tickwire_venue::{OrderSnapshot, RequestReply as Reply};

/// Classifies Bybit V5 and OKX V5 synchronous/query error codes into per-order vs. account-wide actions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SpotIssueKind {
    /// Definitive business rejection proving a `New` order was never accepted on the venue.
    DeterministicReject,
    /// Timeout, clock-window, or internal error whose remote execution outcome is unknown.
    Ambiguous,
    /// Target order absent on a query or cancel endpoint.
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
            "110001" | "170213" => SpotIssueKind::NotFound,
            "10000" | "10002" | "10016" | "170007" => SpotIssueKind::Ambiguous,
            "170121" | "170124" | "170130" | "170131" | "170132" | "170133" | "170134"
            | "170135" | "170136" | "170137" | "170139" | "170140" | "170192" | "170193"
            | "170194" | "170210" => SpotIssueKind::DeterministicReject,
            _ => SpotIssueKind::Ambiguous,
        },
        SpotVenue::Okx => match code {
            "50011" | "50101" | "50103" | "50105" | "50111" | "50113" | "50119" | "50120"
            | "401" | "403" | "418" | "429" => SpotIssueKind::AccountHazard,
            "51400" | "51603" => SpotIssueKind::NotFound,
            "50000" | "50001" | "50004" | "50013" | "50026" | "50102" => SpotIssueKind::Ambiguous,
            "51000" | "51001" | "51002" | "51004" | "51006" | "51008" | "51010" | "51019"
            | "51020" | "51080" => SpotIssueKind::DeterministicReject,
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
    let (wanted_client, wanted_native) = match command {
        CommandBody::New(o) => (Some(&o.client_id), None),
        CommandBody::Cancel(o) | CommandBody::Query(o) => {
            (o.client_id.as_ref(), o.venue_order_id.as_ref())
        }
        _ => return Err(ReportError::Invalid),
    };
    if wanted_client.is_some_and(|id| Some(id) != client.as_ref())
        || wanted_native.is_some_and(|id| id != &native)
    {
        return Err(ReportError::Identity);
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
                let wanted = match &cmd.body {
                    CommandBody::New(o) => Some(o.client_id.as_str()),
                    CommandBody::Cancel(o) => o.client_id.as_ref().map(ClientId::as_str),
                    _ => None,
                };
                if wanted.is_some_and(|w| w != cid) {
                    return Err(ReportError::Identity);
                }
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
