//! Bybit V5 and OKX V5 cash spot REST execution, through the account gateway.
#![forbid(unsafe_code)]
mod fills;
pub mod network;
pub mod observe;
pub mod private;
pub mod recovery;
pub use fills::{FillSource, trade_id};
mod reports;
mod session;
use base64::{Engine as _, engine::general_purpose::STANDARD};
pub use reports::{OrderSnapshot, Reply, ReportError, decode_reply};
use ring::hmac;
use serde_json::{Map, Value, json};
pub use session::{RestSession, SessionError};
use tickwire_gateway::{EncodedRequest, Encoder, Error, Route};
use tickwire_transport::http;
pub use tickwire_venue::SpotVenue;
use tickwire_venue::{
    CanonicalCommand, CommandBody, Environment, OrderSpec, OrderType, PositionSide, Product,
    ProductMode, Quantity, SelfTradePrevention, Side, TimeInForce,
};

pub struct Credentials<'a> {
    venue: SpotVenue,
    key: &'a str,
    secret: &'a [u8],
    passphrase: Option<&'a str>,
}
impl<'a> Credentials<'a> {
    fn valid(&self) -> bool {
        let safe_header = |s: &str| {
            !s.is_empty() && s.len() <= 256 && s.bytes().all(|b| (0x21..=0x7e).contains(&b))
        };
        safe_header(self.key)
            && !self.secret.is_empty()
            && self.secret.len() <= 1024
            && self.passphrase.is_none_or(safe_header)
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
fn sign(
    c: &Credentials<'_>,
    environment: Environment,
    ms: u64,
    request: &Request,
) -> Result<Vec<u8>, Error> {
    if ms == 0 {
        return Err(Error::Encoding);
    }
    let key = hmac::Key::new(hmac::HMAC_SHA256, c.secret);
    let mut mac = hmac::Context::with_key(&key);
    let host = authority(c.venue, environment);
    let mut headers = vec![("Host", host), ("Content-Type", "application/json")];
    let timestamp;
    let signature;
    match c.venue {
        SpotVenue::Bybit => {
            timestamp = ms.to_string();
            mac.update(timestamp.as_bytes());
            mac.update(c.key.as_bytes());
            mac.update(b"5000");
            match request.method {
                "GET" => mac.update(
                    request
                        .path
                        .split_once('?')
                        .map_or("", |(_, query)| query)
                        .as_bytes(),
                ),
                _ => mac.update(&request.body),
            }
            signature = hex(mac.sign().as_ref());
            headers.extend([
                ("X-BAPI-API-KEY", c.key),
                ("X-BAPI-RECV-WINDOW", "5000"),
                ("X-BAPI-TIMESTAMP", &timestamp),
                ("X-BAPI-SIGN", &signature),
            ]);
        }
        SpotVenue::Okx => {
            timestamp = okx_timestamp(ms)?;
            mac.update(timestamp.as_bytes());
            mac.update(request.method.as_bytes());
            mac.update(request.path.as_bytes());
            mac.update(&request.body);
            signature = STANDARD.encode(mac.sign().as_ref());
            headers.extend([
                ("OK-ACCESS-KEY", c.key),
                ("OK-ACCESS-PASSPHRASE", c.passphrase.ok_or(Error::Identity)?),
                ("OK-ACCESS-TIMESTAMP", &timestamp),
                ("OK-ACCESS-SIGN", &signature),
            ]);
            if environment == Environment::Test {
                headers.push(("x-simulated-trading", "1"));
            }
        }
    }
    let bytes = http::encode_request(
        request.method,
        &request.path,
        &headers,
        &request.body,
        16 * 1024,
    )
    .map_err(|_| Error::Encoding)?;
    Ok(bytes)
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
