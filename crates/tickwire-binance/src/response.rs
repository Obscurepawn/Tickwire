use crate::{Error, Result, request::Market};
use serde::Deserialize;
use serde_json::value::RawValue;

pub const MAX_RESPONSE_BYTES: usize = 1024 * 1024;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FailureKind {
    Unknown,
    Rejected,
    NotFound,
    RateLimited,
    Banned,
    Authentication,
    ClockSkew,
    Partial,
}
#[derive(Debug, Deserialize)]
pub struct ApiError<'a> {
    pub code: i64,
    #[serde(borrow)]
    pub msg: std::borrow::Cow<'a, str>,
    #[serde(borrow)]
    pub data: Option<&'a RawValue>,
}
#[derive(Debug)]
pub struct Response<'a> {
    pub request_id: Option<&'a RawValue>,
    pub status: u16,
    pub result: Option<&'a RawValue>,
    pub error: Option<ApiError<'a>>,
    pub failure: Option<FailureKind>,
    pub rate_limits: Option<&'a RawValue>,
}
pub fn classify(market: Market, status: u16, code: Option<i64>, message: &str) -> FailureKind {
    if status == 408 || matches!(code, Some(-1000 | -1001 | -1006 | -1007)) {
        return FailureKind::Unknown;
    }
    if status >= 500 {
        return if market == Market::Usdm
            && status == 503
            && (code == Some(-1008)
                || message == "Service Unavailable."
                || message == "Internal error; unable to process your request. Please try again.")
        {
            FailureKind::Rejected
        } else {
            FailureKind::Unknown
        };
    }
    if status == 418 {
        FailureKind::Banned
    } else if status == 429 || matches!(code, Some(-1003 | -1015)) {
        FailureKind::RateLimited
    } else if status == 409 {
        FailureKind::Partial
    } else if matches!(code, Some(-2011 | -2013)) {
        FailureKind::NotFound
    } else if status == 401 || matches!(code, Some(-1002 | -2014 | -2015 | -1022)) {
        FailureKind::Authentication
    } else if code == Some(-1021) {
        FailureKind::ClockSkew
    } else if matches!(
        code,
        Some(
            -1013
                | -1014
                | -1020
                | -1100
                | -1101
                | -1102
                | -1103
                | -1104
                | -1105
                | -1106
                | -1108
                | -1111
                | -1114
                | -1115
                | -1116
                | -1117
                | -1118
                | -1119
                | -1120
                | -1121
                | -1128
                | -1130
                | -2010
                | -2038
        )
    ) {
        FailureKind::Rejected
    } else {
        // New server error codes do not provide evidence that execution failed.
        FailureKind::Unknown
    }
}
#[derive(Deserialize)]
struct WsEnvelope<'a> {
    #[serde(borrow)]
    id: &'a RawValue,
    status: u16,
    #[serde(borrow)]
    result: Option<&'a RawValue>,
    #[serde(borrow)]
    error: Option<ApiError<'a>>,
    #[serde(borrow, rename = "rateLimits")]
    rate_limits: Option<&'a RawValue>,
}
fn bounded(bytes: &[u8]) -> Result<()> {
    if bytes.len() > MAX_RESPONSE_BYTES {
        Err(Error::Capacity)
    } else if bytes.is_empty() {
        Err(Error::InvalidResponse)
    } else {
        Ok(())
    }
}
pub fn decode_websocket(market: Market, bytes: &[u8]) -> Result<Response<'_>> {
    bounded(bytes)?;
    let r: WsEnvelope<'_> = serde_json::from_slice(bytes).map_err(|_| Error::InvalidResponse)?;
    if !((200..300).contains(&r.status) || (400..600).contains(&r.status))
        || r.result.is_some() == r.error.is_some()
        || (r.status >= 400 && r.error.is_none())
    {
        return Err(Error::InvalidResponse);
    }
    let id: serde_json::Value =
        serde_json::from_str(r.id.get()).map_err(|_| Error::InvalidResponse)?;
    if !matches!(id, serde_json::Value::Null | serde_json::Value::String(_))
        && !id.is_u64()
        && !id.is_i64()
    {
        return Err(Error::InvalidResponse);
    }
    if r.id.get().len() > 256 {
        return Err(Error::InvalidResponse);
    }
    let failure = match &r.error {
        Some(e) => {
            if e.msg.len() > 4096 {
                return Err(Error::InvalidResponse);
            }
            Some(classify(market, r.status, Some(e.code), &e.msg))
        }
        None => None,
    };
    Ok(Response {
        request_id: Some(r.id),
        status: r.status,
        result: r.result,
        error: r.error,
        failure,
        rate_limits: r.rate_limits,
    })
}
pub fn decode_rest(market: Market, status: u16, bytes: &[u8]) -> Result<Response<'_>> {
    bounded(bytes)?;
    if !((200..300).contains(&status) || (400..600).contains(&status)) {
        return Err(Error::InvalidResponse);
    }
    let raw: &RawValue = serde_json::from_slice(bytes).map_err(|_| Error::InvalidResponse)?;
    // Inspect the error discriminator separately. A malformed error cannot fall
    // through to the success branch merely because ApiError deserialization failed.
    #[derive(Deserialize)]
    struct Probe<'a> {
        #[serde(borrow)]
        code: Option<&'a RawValue>,
    }
    let negative_code = if raw.get().starts_with('{') {
        let probe: Probe<'_> = serde_json::from_slice(bytes).map_err(|_| Error::InvalidResponse)?;
        match probe.code {
            None => false,
            Some(code) => {
                let n = serde_json::from_str::<i64>(code.get())
                    .or_else(|_| {
                        serde_json::from_str::<&str>(code.get())
                            .and_then(|s| s.parse::<i64>().map_err(serde::de::Error::custom))
                    })
                    .map_err(|_| Error::InvalidResponse)?;
                n < 0
            }
        }
    } else {
        false
    };
    let parsed = serde_json::from_slice::<ApiError<'_>>(bytes);
    if status >= 400 || negative_code {
        let error = parsed.map_err(|_| Error::InvalidResponse)?;
        if error.msg.len() > 4096 {
            return Err(Error::InvalidResponse);
        }
        let failure = classify(market, status, Some(error.code), &error.msg);
        Ok(Response {
            request_id: None,
            status,
            result: None,
            error: Some(error),
            failure: Some(failure),
            rate_limits: None,
        })
    } else {
        Ok(Response {
            request_id: None,
            status,
            result: Some(raw),
            error: None,
            failure: None,
            rate_limits: None,
        })
    }
}

#[derive(Debug, Deserialize)]
pub struct OrderReply<'a> {
    #[serde(borrow)]
    pub symbol: std::borrow::Cow<'a, str>,
    #[serde(rename = "orderId")]
    pub order_id: u64,
    #[serde(borrow, rename = "clientOrderId")]
    pub client_order_id: std::borrow::Cow<'a, str>,
    #[serde(borrow)]
    pub status: Option<std::borrow::Cow<'a, str>>,
    pub price: Option<crate::decimal::Decimal>,
    #[serde(rename = "origQty")]
    pub original_quantity: Option<crate::decimal::Decimal>,
    #[serde(rename = "executedQty")]
    pub executed_quantity: Option<crate::decimal::Decimal>,
    #[serde(rename = "cummulativeQuoteQty", alias = "cumQuote")]
    pub cumulative_quote: Option<crate::decimal::Decimal>,
    #[serde(borrow)]
    pub fills: Option<&'a RawValue>,
}
pub fn decode_order(bytes: &[u8]) -> Result<OrderReply<'_>> {
    bounded(bytes)?;
    let r: OrderReply<'_> = serde_json::from_slice(bytes).map_err(|_| Error::InvalidResponse)?;
    validate_identity(&r.symbol, 64)?;
    validate_identity(&r.client_order_id, 64)?;
    if r.order_id > i64::MAX as u64 {
        return Err(Error::InvalidResponse);
    }
    if let Some(status) = &r.status {
        validate_identity(status, 64)?;
    }
    for n in [
        r.price,
        r.original_quantity,
        r.executed_quantity,
        r.cumulative_quote,
    ]
    .into_iter()
    .flatten()
    {
        if n.mantissa() < 0 {
            return Err(Error::InvalidResponse);
        }
    }
    Ok(r)
}
#[derive(Debug, Deserialize)]
pub struct AlgoReply<'a> {
    #[serde(rename = "algoId")]
    pub algo_id: u64,
    #[serde(borrow, rename = "clientAlgoId")]
    pub client_algo_id: std::borrow::Cow<'a, str>,
    #[serde(borrow)]
    pub symbol: Option<std::borrow::Cow<'a, str>>,
    #[serde(borrow, rename = "algoStatus")]
    pub algo_status: Option<std::borrow::Cow<'a, str>>,
    #[serde(rename = "triggerPrice")]
    pub trigger_price: Option<crate::decimal::Decimal>,
}
pub fn decode_algo(bytes: &[u8]) -> Result<AlgoReply<'_>> {
    bounded(bytes)?;
    let r: AlgoReply<'_> = serde_json::from_slice(bytes).map_err(|_| Error::InvalidResponse)?;
    validate_identity(&r.client_algo_id, 64)?;
    if r.algo_id > i64::MAX as u64 {
        return Err(Error::InvalidResponse);
    }
    if let Some(symbol) = &r.symbol {
        validate_identity(symbol, 64)?;
    }
    if let Some(status) = &r.algo_status {
        validate_identity(status, 64)?;
    }
    if r.trigger_price.is_some_and(|v| v.mantissa() < 0) {
        return Err(Error::InvalidResponse);
    }
    Ok(r)
}
fn validate_identity(s: &str, max: usize) -> Result<()> {
    if s.is_empty() || s.len() > max || s.chars().any(|c| c.is_control() || c.is_whitespace()) {
        Err(Error::InvalidResponse)
    } else {
        Ok(())
    }
}
