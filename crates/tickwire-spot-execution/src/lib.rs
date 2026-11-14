//! Bybit V5 and OKX V5 cash spot REST execution, through the account gateway.
#![forbid(unsafe_code)]
mod fills;
pub mod network;
pub mod observe;
pub mod private;
pub mod recovery;
pub use fills::{FillSource, trade_id};
pub mod reports;
mod session;
pub mod warmup;
use base64::{Engine as _, engine::general_purpose::STANDARD};
pub use reports::{
    ModifyReply, OrderRowExtras, OrderSnapshot, Reply, ReportError, SpotIssueKind, classify_issue,
    classify_order_row_terminal_reason, decode_modify_reply, decode_reply, extract_error_message,
    extract_order_row_extras, is_http_account_hazard, is_venue_rate_limit_code,
};
use ring::hmac;
use serde_json::{Map, Value, json};
pub use session::{RestSession, SessionError, symbol_rate_key};
use tickwire_gateway::{EncodedRequest, Encoder, Error, Route};
use tickwire_transport::{http, rest_querier::BuiltRequest};
pub use tickwire_venue::SpotVenue;
use tickwire_venue::{
    CanonicalCommand, CommandBody, Environment, OrderSpec, OrderType, PositionSide, Product,
    ProductMode, Quantity, SelfTradePrevention, Side, TimeInForce,
};

#[derive(Clone, Copy)]
pub struct Credentials<'a> {
    venue: SpotVenue,
    key: &'a str,
    secret: &'a [u8],
    passphrase: Option<&'a str>,
}
impl<'a> Credentials<'a> {
    pub(crate) fn valid(&self) -> bool {
        let safe_header = |s: &str| {
            !s.is_empty() && s.len() <= 256 && s.bytes().all(|b| (0x21..=0x7e).contains(&b))
        };
        safe_header(self.key)
            && !self.secret.is_empty()
            && self.secret.len() <= 1024
            && self.passphrase.is_none_or(safe_header)
    }
    pub fn venue(&self) -> SpotVenue {
        self.venue
    }
    pub fn bybit(key: &'a str, secret: &'a [u8]) -> Self {
        Self {
            venue: SpotVenue::Bybit,
            key,
            secret,
            passphrase: None,
        }
    }
    pub fn okx(key: &'a str, secret: &'a [u8], passphrase: &'a str) -> Self {
        Self {
            venue: SpotVenue::Okx,
            key,
            secret,
            passphrase: Some(passphrase),
        }
    }
}
/// Supplies calibrated venue-compatible UTC milliseconds. Fail closed if unavailable.
pub struct SpotEncoder<'a, T> {
    credentials: Credentials<'a>,
    environment: Environment,
    account: u64,
    time: T,
}
pub fn authority(venue: SpotVenue, environment: Environment) -> &'static str {
    match (venue, environment) {
        (SpotVenue::Bybit, Environment::Production) => "api.bybit.com",
        (SpotVenue::Bybit, Environment::Test) => "api-testnet.bybit.com",
        (SpotVenue::Okx, _) => "openapi.okx.com",
    }
}
impl<'a, T: FnMut() -> Result<u64, Error>> SpotEncoder<'a, T> {
    pub fn new(
        credentials: Credentials<'a>,
        environment: Environment,
        account: u64,
        time: T,
    ) -> Result<Self, Error> {
        if account == 0 || !credentials.valid() {
            return Err(Error::Identity);
        }
        Ok(Self {
            credentials,
            environment,
            account,
            time,
        })
    }
    pub fn route(&self) -> Route {
        Route {
            venue: self.credentials.venue.id(),
            account: self.account,
            environment: self.environment,
            product: Product::Spot,
            authority: authority(self.credentials.venue, self.environment).to_owned(),
        }
    }
}
fn symbol_ok(symbol: &str) -> bool {
    !symbol.is_empty()
        && symbol
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'-')
}
fn client_ok(venue: SpotVenue, id: &str) -> bool {
    !id.is_empty()
        && id.len() <= if venue == SpotVenue::Okx { 32 } else { 36 }
        && id.bytes().all(|c| {
            c.is_ascii_alphanumeric() || venue == SpotVenue::Bybit && matches!(c, b'_' | b'-')
        })
}
fn terms(venue: SpotVenue, symbol: &str, order: &OrderSpec) -> Result<Map<String, Value>, Error> {
    if !client_ok(venue, order.client_id.as_str())
        || order.mode != ProductMode::Spot
        || order.position_side != PositionSide::Both
        || order.reduce_only
        || order.trigger.is_some()
        || order.trailing.is_some()
        || order.iceberg_quantity.is_some()
        || order.self_trade_prevention != SelfTradePrevention::Default
    {
        return Err(Error::Encoding);
    }
    let Quantity::Base(qty) = order.quantity else {
        return Err(Error::Encoding);
    };
    let bybit = venue == SpotVenue::Bybit;
    let (kind, tif) = match (order.order_type, order.time_in_force) {
        (OrderType::Market, None) => (if bybit { "Market" } else { "market" }, None),
        (OrderType::Limit, Some(t)) => match t {
            TimeInForce::Gtc => (if bybit { "Limit" } else { "limit" }, Some("GTC")),
            TimeInForce::Ioc => (if bybit { "Limit" } else { "ioc" }, Some("IOC")),
            TimeInForce::Fok => (if bybit { "Limit" } else { "fok" }, Some("FOK")),
            TimeInForce::PostOnly => (if bybit { "Limit" } else { "post_only" }, Some("PostOnly")),
            _ => return Err(Error::Encoding),
        },
        _ => return Err(Error::Encoding),
    };
    let buy = order.side == Side::Buy;
    let mut fields = Map::new();
    if bybit {
        fields.insert("category".into(), json!("spot"));
        fields.insert("symbol".into(), json!(symbol));
        fields.insert("orderLinkId".into(), json!(order.client_id.as_str()));
        fields.insert("orderType".into(), json!(kind));
        fields.insert("qty".into(), json!(qty.as_plain_string()));
        fields.insert("side".into(), json!(if buy { "Buy" } else { "Sell" }));
        if let Some(tif) = tif {
            fields.insert("timeInForce".into(), json!(tif));
        }
        if order.order_type == OrderType::Market {
            fields.insert("marketUnit".into(), json!("baseCoin"));
        }
    } else {
        fields.insert("instId".into(), json!(symbol));
        fields.insert("clOrdId".into(), json!(order.client_id.as_str()));
        fields.insert("ordType".into(), json!(kind));
        fields.insert("sz".into(), json!(qty.as_plain_string()));
        fields.insert("side".into(), json!(if buy { "buy" } else { "sell" }));
        fields.insert("tdMode".into(), json!("cash"));
        if order.order_type == OrderType::Market {
            fields.insert("tgtCcy".into(), json!("base_ccy"));
            fields.insert("banAmend".into(), json!(true));
        }
    }
    if let Some(px) = order.price {
        fields.insert(
            if bybit { "price" } else { "px" }.into(),
            json!(px.as_plain_string()),
        );
    }
    Ok(fields)
}
struct Request {
    method: &'static str,
    path: String,
    body: Vec<u8>,
}
fn parameters(venue: SpotVenue, cmd: &CanonicalCommand) -> Result<Request, Error> {
    let c = cmd.command();
    let symbol = c.context.instrument.symbol.as_str();
    if !symbol_ok(symbol) {
        return Err(Error::Encoding);
    }
    let bybit = venue == SpotVenue::Bybit;
    let path;
    let fields = match &c.body {
        CommandBody::New(order) => {
            path = if bybit {
                "/v5/order/create"
            } else {
                "/api/v5/trade/order"
            };
            terms(venue, symbol, order)?
        }
        CommandBody::Cancel(target) | CommandBody::Query(target) => {
            let mut fields = Map::new();
            if bybit {
                fields.insert("category".into(), json!("spot"));
            }
            fields.insert(
                if bybit { "symbol" } else { "instId" }.into(),
                json!(symbol),
            );
            if let Some(id) = &target.client_id {
                if !client_ok(venue, id.as_str()) {
                    return Err(Error::Encoding);
                }
                fields.insert(
                    if bybit { "orderLinkId" } else { "clOrdId" }.into(),
                    json!(id.as_str()),
                );
            }
            if let Some(id) = &target.venue_order_id {
                if !symbol_ok(id.as_str()) {
                    return Err(Error::Encoding);
                }
                fields.insert(
                    if bybit { "orderId" } else { "ordId" }.into(),
                    json!(id.as_str()),
                );
            }
            if matches!(&c.body, CommandBody::Query(_)) {
                let query: Vec<_> = fields
                    .iter()
                    .map(|(k, v)| format!("{k}={}", v.as_str().expect("query values are strings")))
                    .collect();
                return Ok(Request {
                    method: "GET",
                    path: format!(
                        "{}?{}",
                        if bybit {
                            "/v5/order/realtime"
                        } else {
                            "/api/v5/trade/order"
                        },
                        query.join("&")
                    ),
                    body: Vec::new(),
                });
            }
            path = if bybit {
                "/v5/order/cancel"
            } else {
                "/api/v5/trade/cancel-order"
            };
            fields
        }
        CommandBody::Modify(m) => {
            if m.position_side != PositionSide::Both
                || m.reduce_only
                || !m.quantity.is_positive()
                || !m.price.is_positive()
            {
                return Err(Error::Encoding);
            }
            if m.target.client_id.is_none() && m.target.venue_order_id.is_none() {
                return Err(Error::Encoding);
            }
            let mut fields = Map::new();
            if bybit {
                fields.insert("category".into(), json!("spot"));
            }
            fields.insert(
                if bybit { "symbol" } else { "instId" }.into(),
                json!(symbol),
            );
            if let Some(id) = &m.target.venue_order_id {
                if !symbol_ok(id.as_str()) {
                    return Err(Error::Encoding);
                }
                fields.insert(
                    if bybit { "orderId" } else { "ordId" }.into(),
                    json!(id.as_str()),
                );
            }
            if let Some(id) = &m.target.client_id {
                if !client_ok(venue, id.as_str()) {
                    return Err(Error::Encoding);
                }
                fields.insert(
                    if bybit { "orderLinkId" } else { "clOrdId" }.into(),
                    json!(id.as_str()),
                );
            }
            if let Some(req_id) = &m.new_client_id {
                if !client_ok(venue, req_id.as_str()) {
                    return Err(Error::Encoding);
                }
                if !bybit {
                    fields.insert("reqId".into(), json!(req_id.as_str()));
                }
            }
            fields.insert(
                if bybit { "qty" } else { "newSz" }.into(),
                json!(m.quantity.as_plain_string()),
            );
            fields.insert(
                if bybit { "price" } else { "newPx" }.into(),
                json!(m.price.as_plain_string()),
            );
            path = if bybit {
                "/v5/order/amend"
            } else {
                "/api/v5/trade/amend-order"
            };
            fields
        }
        _ => return Err(Error::Encoding),
    };
    Ok(Request {
        method: "POST",
        path: path.into(),
        body: serde_json::to_vec(&fields).map_err(|_| Error::Encoding)?,
    })
}
fn okx_timestamp(ms: u64) -> Result<String, Error> {
    let ts = time::OffsetDateTime::from_unix_timestamp_nanos(i128::from(ms) * 1_000_000)
        .map_err(|_| Error::Encoding)?;
    if !(1970..=9999).contains(&ts.year()) {
        return Err(Error::Encoding);
    }
    Ok(format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
        ts.year(),
        u8::from(ts.month()),
        ts.day(),
        ts.hour(),
        ts.minute(),
        ts.second(),
        ts.millisecond()
    ))
}
impl<T: FnMut() -> Result<u64, Error>> Encoder for SpotEncoder<'_, T> {
    fn encode(&mut self, cmd: &CanonicalCommand) -> Result<EncodedRequest, Error> {
        let c = &self.credentials;
        let r = &cmd.command().context.request;
        if r.venue != c.venue.id()
            || r.account != self.account
            || r.environment != self.environment
            || r.product != Product::Spot
        {
            return Err(Error::Identity);
        }
        let request = parameters(c.venue, cmd)?;
        let bytes = sign(c, self.environment, (self.time)()?, &request)?;
        EncodedRequest::new(
            cmd.digest(),
            authority(c.venue, self.environment).into(),
            bytes,
        )
    }
}
fn sign_headers(
    c: &Credentials<'_>,
    environment: Environment,
    ms: u64,
    method: &str,
    path: &str,
    body: &[u8],
) -> Result<Vec<(String, String)>, Error> {
    if !c.valid() {
        return Err(Error::Identity);
    }
    if ms == 0
        || !path.starts_with('/')
        || path.len() > 2048
        || !path.bytes().all(|b| (0x21..=0x7e).contains(&b))
    {
        return Err(Error::Encoding);
    }
    let key = hmac::Key::new(hmac::HMAC_SHA256, c.secret);
    let mut mac = hmac::Context::with_key(&key);
    let host = authority(c.venue, environment);
    let mut headers = vec![
        ("Host".to_owned(), host.to_owned()),
        ("Content-Type".to_owned(), "application/json".to_owned()),
    ];
    match c.venue {
        SpotVenue::Bybit => {
            let timestamp = ms.to_string();
            mac.update(timestamp.as_bytes());
            mac.update(c.key.as_bytes());
            mac.update(b"5000");
            match method {
                "GET" => mac.update(
                    path.split_once('?')
                        .map_or("", |(_, query)| query)
                        .as_bytes(),
                ),
                _ => mac.update(body),
            }
            let signature = hex(mac.sign().as_ref());
            headers.extend([
                ("X-BAPI-API-KEY".to_owned(), c.key.to_owned()),
                ("X-BAPI-RECV-WINDOW".to_owned(), "5000".to_owned()),
                ("X-BAPI-TIMESTAMP".to_owned(), timestamp),
                ("X-BAPI-SIGN".to_owned(), signature),
            ]);
        }
        SpotVenue::Okx => {
            let timestamp = okx_timestamp(ms)?;
            mac.update(timestamp.as_bytes());
            mac.update(method.as_bytes());
            mac.update(path.as_bytes());
            mac.update(body);
            let signature = STANDARD.encode(mac.sign().as_ref());
            headers.extend([
                ("OK-ACCESS-KEY".to_owned(), c.key.to_owned()),
                (
                    "OK-ACCESS-PASSPHRASE".to_owned(),
                    c.passphrase.ok_or(Error::Identity)?.to_owned(),
                ),
                ("OK-ACCESS-TIMESTAMP".to_owned(), timestamp),
                ("OK-ACCESS-SIGN".to_owned(), signature),
            ]);
            if environment == Environment::Test {
                headers.push(("x-simulated-trading".to_owned(), "1".to_owned()));
            }
        }
    }
    Ok(headers)
}

fn sign(
    c: &Credentials<'_>,
    environment: Environment,
    ms: u64,
    request: &Request,
) -> Result<Vec<u8>, Error> {
    let owned = sign_headers(
        c,
        environment,
        ms,
        request.method,
        &request.path,
        &request.body,
    )?;
    let refs: Vec<(&str, &str)> = owned
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    http::encode_request(
        request.method,
        &request.path,
        &refs,
        &request.body,
        16 * 1024,
    )
    .map_err(|_| Error::Encoding)
}

/// Builds a Bybit V5 spot realtime order status path (`/v5/order/realtime?category=spot...[&symbol=...]&orderLinkId=...`).
pub fn bybit_order_realtime_path(symbol: Option<&str>, client_id: &str) -> Result<String, Error> {
    if !client_ok(SpotVenue::Bybit, client_id) {
        return Err(Error::Encoding);
    }
    let mut path = String::from("/v5/order/realtime?category=spot");
    if let Some(sym) = symbol {
        if sym.len() > 64 || !symbol_ok(sym) {
            return Err(Error::Encoding);
        }
        path.push_str("&symbol=");
        path.push_str(sym);
    }
    path.push_str("&orderLinkId=");
    path.push_str(client_id);
    Ok(path)
}

/// Builds a Bybit V5 spot order history path (`/v5/order/history?category=spot...[&symbol=...]&orderLinkId=...`).
pub fn bybit_order_history_path(symbol: Option<&str>, client_id: &str) -> Result<String, Error> {
    if !client_ok(SpotVenue::Bybit, client_id) {
        return Err(Error::Encoding);
    }
    let mut path = String::from("/v5/order/history?category=spot");
    if let Some(sym) = symbol {
        if sym.len() > 64 || !symbol_ok(sym) {
            return Err(Error::Encoding);
        }
        path.push_str("&symbol=");
        path.push_str(sym);
    }
    path.push_str("&orderLinkId=");
    path.push_str(client_id);
    Ok(path)
}

/// Builds a Bybit V5 spot execution/fills query path (`/v5/execution/list?category=spot...`).
pub fn bybit_order_trades_path(
    symbol: Option<&str>,
    client_id: Option<&str>,
    order_id: Option<&str>,
) -> Result<String, Error> {
    if client_id.is_none() && order_id.is_none() {
        return Err(Error::Encoding);
    }
    let mut path = String::from("/v5/execution/list?category=spot");
    if let Some(sym) = symbol {
        if sym.len() > 64 || !symbol_ok(sym) {
            return Err(Error::Encoding);
        }
        path.push_str("&symbol=");
        path.push_str(sym);
    }
    if let Some(oid) = order_id {
        if oid.len() > 64 || !symbol_ok(oid) {
            return Err(Error::Encoding);
        }
        path.push_str("&orderId=");
        path.push_str(oid);
    }
    if let Some(cid) = client_id {
        if !client_ok(SpotVenue::Bybit, cid) {
            return Err(Error::Encoding);
        }
        path.push_str("&orderLinkId=");
        path.push_str(cid);
    }
    Ok(path)
}

/// Builds an OKX V5 spot order status query path (`/api/v5/trade/order?instId=...&clOrdId=...`).
pub fn okx_order_status_path(inst_id: &str, client_id: &str) -> Result<String, Error> {
    if inst_id.len() > 64 || !symbol_ok(inst_id) || !client_ok(SpotVenue::Okx, client_id) {
        return Err(Error::Encoding);
    }
    Ok(format!(
        "/api/v5/trade/order?instId={inst_id}&clOrdId={client_id}"
    ))
}

/// Builds an OKX V5 spot fills query path (`/api/v5/trade/fills?instType=SPOT&instId=...[&ordId=...]`).
pub fn okx_order_trades_path(inst_id: &str, order_id: Option<&str>) -> Result<String, Error> {
    if inst_id.len() > 64 || !symbol_ok(inst_id) {
        return Err(Error::Encoding);
    }
    let mut path = format!("/api/v5/trade/fills?instType=SPOT&instId={inst_id}");
    if let Some(oid) = order_id {
        if oid.len() > 64 || !symbol_ok(oid) {
            return Err(Error::Encoding);
        }
        path.push_str("&ordId=");
        path.push_str(oid);
    }
    Ok(path)
}

/// Signs an off-hot-path HTTP/1.1 `GET` request into a [`BuiltRequest`] suitable for `AsyncRestQuerier`.
pub fn build_signed_get(
    credentials: &Credentials<'_>,
    environment: Environment,
    utc_ms: u64,
    path: String,
) -> Result<BuiltRequest, Error> {
    let headers = sign_headers(credentials, environment, utc_ms, "GET", &path, &[])?;
    Ok(BuiltRequest {
        method: "GET".to_owned(),
        path,
        headers,
        body: Vec::new(),
    })
}

/// Encodes a signed off-hot-path HTTP/1.1 `GET` request directly into wire bytes.
pub fn encode_signed_get(
    credentials: &Credentials<'_>,
    environment: Environment,
    utc_ms: u64,
    path: &str,
) -> Result<Vec<u8>, Error> {
    sign(
        credentials,
        environment,
        utc_ms,
        &Request {
            method: "GET",
            path: path.to_owned(),
            body: Vec::new(),
        },
    )
}

/// Builds a signed off-hot-path `OrderStatus` `GET` request for Bybit (`realtime` or `history`) or OKX.
pub fn build_order_status_get(
    credentials: &Credentials<'_>,
    environment: Environment,
    utc_ms: u64,
    symbol: &str,
    client_id: &str,
    history_fallback: bool,
) -> Result<BuiltRequest, Error> {
    let path = match credentials.venue {
        SpotVenue::Bybit if history_fallback => bybit_order_history_path(Some(symbol), client_id)?,
        SpotVenue::Bybit => bybit_order_realtime_path(Some(symbol), client_id)?,
        SpotVenue::Okx => okx_order_status_path(symbol, client_id)?,
    };
    build_signed_get(credentials, environment, utc_ms, path)
}

/// Builds a signed off-hot-path `OrderTrades` `GET` request for Bybit (`/v5/execution/list`) or OKX (`/api/v5/trade/fills`).
pub fn build_order_trades_get(
    credentials: &Credentials<'_>,
    environment: Environment,
    utc_ms: u64,
    symbol: &str,
    client_id: Option<&str>,
    order_id: Option<&str>,
) -> Result<BuiltRequest, Error> {
    let path = match credentials.venue {
        SpotVenue::Bybit => bybit_order_trades_path(Some(symbol), client_id, order_id)?,
        SpotVenue::Okx => okx_order_trades_path(symbol, order_id)?,
    };
    build_signed_get(credentials, environment, utc_ms, path)
}

fn hex(bytes: &[u8]) -> String {
    const CHARS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(CHARS[(b >> 4) as usize] as char);
        out.push(CHARS[(b & 15) as usize] as char);
    }
    out
}
