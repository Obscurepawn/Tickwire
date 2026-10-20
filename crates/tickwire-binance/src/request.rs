use crate::{
    Error, Result,
    auth::{Endpoint, MAX_SIGNATURE, SigningKey},
    buffer::{atomic, json_string, percent},
    params::{Params, Value},
};
use tickwire_venue::*;

pub const MAX_REQUEST_BYTES: usize = 16_384;
const MAX_PARAMS: usize = 64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Market {
    Spot,
    Usdm,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Transport {
    Rest,
    WebSocket,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Method {
    Get,
    Post,
    Put,
    Delete,
}
impl Method {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Post => "POST",
            Self::Put => "PUT",
            Self::Delete => "DELETE",
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Timestamp {
    Milliseconds(u64),
    Microseconds(u64),
}
pub struct Credentials<'a> {
    pub account: u64,
    pub environment: Environment,
    pub market: Market,
    pub api_key: &'a str,
    pub signing_key: &'a SigningKey,
}
#[derive(Clone, Copy, Debug)]
pub struct RequestOptions<'a> {
    pub transport: Transport,
    pub timestamp: Timestamp,
    pub recv_window_micros: u64,
    pub request_id: &'a str,
}
pub struct PreparedRequest {
    digest: [u8; 32],
    authority: &'static str,
    path: String,
    method: Method,
    transport: Transport,
    payload: Vec<u8>,
    api_key: Option<String>,
}
impl PreparedRequest {
    pub fn command_digest(&self) -> [u8; 32] {
        self.digest
    }
    pub fn authority(&self) -> &str {
        self.authority
    }
    pub fn path(&self) -> &str {
        &self.path
    }
    pub fn method(&self) -> Method {
        self.method
    }
    pub fn transport(&self) -> Transport {
        self.transport
    }
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
    pub fn api_key_header(&self) -> Option<&str> {
        self.api_key.as_deref()
    }
    pub fn verify_digest(&self, expected: [u8; 32]) -> Result<()> {
        if expected == self.digest {
            Ok(())
        } else {
            Err(Error::IdentityMismatch)
        }
    }
}
pub fn prepare(
    command: &CanonicalCommand,
    credentials: &Credentials<'_>,
    options: RequestOptions<'_>,
    max_bytes: usize,
) -> Result<PreparedRequest> {
    let c = command.command();
    let market = match c.context.request.product {
        Product::Spot => Market::Spot,
        Product::LinearPerpetual | Product::LinearFuture => Market::Usdm,
        _ => return Err(Error::UnsupportedCommand),
    };
    if c.context.request.venue != 1
        || c.context.request.account != credentials.account
        || c.context.request.environment != credentials.environment
        || market != credentials.market
    {
        return Err(Error::IdentityMismatch);
    }
    if max_bytes == 0 || max_bytes > MAX_REQUEST_BYTES {
        return Err(Error::Capacity);
    }
    if credentials.api_key.is_empty()
        || credentials.api_key.len() > 256
        || !credentials
            .api_key
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_".contains(&b))
        || options.request_id.is_empty()
        || options.request_id.len() > 64
        || !options.request_id.bytes().all(|b| (33..=126).contains(&b))
        || options.recv_window_micros == 0
        || options.recv_window_micros > 60_000_000
    {
        return Err(Error::InvalidParameter);
    }
    let timestamp = match (market, options.timestamp) {
        (_, Timestamp::Milliseconds(v)) | (Market::Spot, Timestamp::Microseconds(v)) if v > 0 => v,
        _ => return Err(Error::InvalidParameter),
    };
    if market == Market::Usdm && !options.recv_window_micros.is_multiple_of(1000) {
        return Err(Error::InvalidParameter);
    }
    let endpoint = match (market, options.transport) {
        (Market::Spot, Transport::Rest) => Endpoint::SpotRest,
        (Market::Spot, Transport::WebSocket) => Endpoint::SpotWebSocket,
        (Market::Usdm, Transport::Rest) => Endpoint::UsdmRest,
        (Market::Usdm, Transport::WebSocket) => Endpoint::UsdmWebSocket,
    };
    if !endpoint.permits(credentials.signing_key.algorithm()) {
        return Err(Error::UnsupportedAlgorithm);
    }
    let mut owned = OwnedParams::new();
    owned.text("symbol", c.context.instrument.symbol.as_str())?;
    let (method, rest_path, ws_method) =
        command_params(&mut owned, &c.body, market, timestamp, options.timestamp)?;
    if options.transport == Transport::WebSocket {
        owned.text("apiKey", credentials.api_key)?;
    }
    owned.integer("timestamp", timestamp)?;
    if options.recv_window_micros.is_multiple_of(1000) {
        owned.integer("recvWindow", options.recv_window_micros / 1000)?;
    } else {
        let d = crate::decimal::Decimal::new(options.recv_window_micros as i128, 3)?;
        let mut b = [0; 32];
        let n = d.write(&mut b)?;
        owned.add(
            "recvWindow",
            OwnedValue::Number(
                core::str::from_utf8(&b[..n])
                    .map_err(|_| Error::InvalidParameter)?
                    .into(),
            ),
        )?;
    }
    let params = owned.borrowed()?;
    let mut scratch = [0; MAX_REQUEST_BYTES];
    let n = match options.transport {
        Transport::Rest => params.rest(&mut scratch)?,
        Transport::WebSocket => params.websocket_preimage(&mut scratch)?,
    };
    let mut signature = [0; MAX_SIGNATURE];
    let sig_len = credentials
        .signing_key
        .sign(endpoint, &scratch[..n], &mut signature)?;
    let signature = &signature[..sig_len];
    let mut output = [0; MAX_REQUEST_BYTES];
    let (path, payload, api_key) = if options.transport == Transport::Rest {
        let size = atomic(&mut output[..max_bytes], |s| {
            s.bytes(rest_path.as_bytes())?;
            s.byte(b'?')?;
            s.bytes(&scratch[..n])?;
            s.bytes(b"&signature=")?;
            percent(s, signature)
        })?;
        (
            String::from_utf8(output[..size].to_vec()).map_err(|_| Error::InvalidParameter)?,
            Vec::new(),
            Some(credentials.api_key.to_owned()),
        )
    } else {
        let n = params.json(&mut scratch)?;
        let size = atomic(&mut output[..max_bytes], |s| {
            s.bytes(b"{\"id\":")?;
            json_string(s, options.request_id)?;
            s.bytes(b",\"method\":")?;
            json_string(s, ws_method)?;
            s.bytes(b",\"params\":")?;
            s.bytes(&scratch[..n - 1])?;
            s.bytes(b",\"signature\":")?;
            json_string(
                s,
                core::str::from_utf8(signature).map_err(|_| Error::SigningFailed)?,
            )?;
            s.bytes(b"}}")
        })?;
        (
            match market {
                Market::Spot => "/ws-api/v3",
                Market::Usdm => "/ws-fapi/v1",
            }
            .into(),
            output[..size].to_vec(),
            None,
        )
    };
    let authority = match (market, credentials.environment, options.transport) {
        (Market::Spot, Environment::Production, Transport::Rest) => "api.binance.com",
        (Market::Spot, Environment::Test, Transport::Rest) => "testnet.binance.vision",
        (Market::Spot, Environment::Production, Transport::WebSocket) => "ws-api.binance.com",
        (Market::Spot, Environment::Test, Transport::WebSocket) => "ws-api.testnet.binance.vision",
        (Market::Usdm, Environment::Production, Transport::Rest) => "fapi.binance.com",
        (Market::Usdm, Environment::Test, Transport::Rest) => "demo-fapi.binance.com",
        (Market::Usdm, Environment::Production, Transport::WebSocket) => "ws-fapi.binance.com",
        (Market::Usdm, Environment::Test, Transport::WebSocket) => "testnet.binancefuture.com",
    };
    Ok(PreparedRequest {
        digest: command.digest(),
        authority,
        path,
        method,
        transport: options.transport,
        payload,
        api_key,
    })
}
enum OwnedValue {
    Text(String),
    Number(String),
    Integer(u64),
}
struct OwnedParams(Vec<(&'static str, OwnedValue)>);
impl OwnedParams {
    fn new() -> Self {
        Self(Vec::with_capacity(MAX_PARAMS))
    }
    fn add(&mut self, name: &'static str, value: OwnedValue) -> Result<()> {
        if self.0.len() == MAX_PARAMS {
            return Err(Error::Capacity);
        }
        if self.0.iter().any(|(n, _)| *n == name) {
            return Err(Error::DuplicateParameter);
        }
        self.0.push((name, value));
        Ok(())
    }
    fn text(&mut self, name: &'static str, value: &str) -> Result<()> {
        self.add(name, OwnedValue::Text(value.into()))
    }
    fn integer(&mut self, name: &'static str, value: u64) -> Result<()> {
        self.add(name, OwnedValue::Integer(value))
    }
    fn decimal(&mut self, name: &'static str, value: Decimal) -> Result<()> {
        self.add(name, OwnedValue::Text(value.as_plain_string()))
    }
    fn borrowed(&self) -> Result<Params<'_, MAX_PARAMS>> {
        let mut p = Params::new();
        for (key, v) in &self.0 {
            p.push(
                key,
                match v {
                    OwnedValue::Text(s) => Value::String(s),
                    OwnedValue::Number(s) => Value::Number(s),
                    OwnedValue::Integer(v) => Value::Integer(*v),
                },
            )?;
        }
        Ok(p)
    }
}
fn client_id(value: &str, market: Market) -> Result<()> {
    if value.is_empty()
        || value.len() > 36
        || !value.bytes().all(|b| {
            b.is_ascii_alphanumeric()
                || match market {
                    Market::Spot => b"-_".contains(&b),
                    Market::Usdm => b".:/_-".contains(&b),
                }
        })
    {
        Err(Error::InvalidParameter)
    } else {
        Ok(())
    }
}
fn native_id(value: &str) -> Result<u64> {
    if value.is_empty() || !value.bytes().all(|b| b.is_ascii_digit()) {
        return Err(Error::InvalidParameter);
    }
    let id = value.parse::<u64>().map_err(|_| Error::InvalidParameter)?;
    if id > i64::MAX as u64 {
        return Err(Error::InvalidParameter);
    }
    Ok(id)
}
fn order_target(
    p: &mut OwnedParams,
    t: &OrderTarget,
    market: Market,
    cancel_replace: bool,
) -> Result<()> {
    if let Some(id) = &t.venue_order_id {
        p.integer(
            if cancel_replace {
                "cancelOrderId"
            } else {
                "orderId"
            },
            native_id(id.as_str())?,
        )?;
    }
    if let Some(id) = &t.client_id {
        client_id(id.as_str(), market)?;
        p.text(
            if cancel_replace {
                "cancelOrigClientOrderId"
            } else {
                "origClientOrderId"
            },
            id.as_str(),
        )?;
    }
    Ok(())
}
fn command_params(
    p: &mut OwnedParams,
    body: &CommandBody,
    market: Market,
    timestamp: u64,
    stamp: Timestamp,
) -> Result<(Method, &'static str, &'static str)> {
    let order_path = match market {
        Market::Spot => "/api/v3/order",
        Market::Usdm => "/fapi/v1/order",
    };
    Ok(match body {
        CommandBody::New(order) => {
            order_params(p, order, market, false, timestamp, stamp)?;
            (Method::Post, order_path, "order.place")
        }
        CommandBody::Cancel(t) => {
            order_target(p, t, market, false)?;
            (Method::Delete, order_path, "order.cancel")
        }
        CommandBody::Query(t) => {
            order_target(p, t, market, false)?;
            (Method::Get, order_path, "order.status")
        }
        CommandBody::CancelReplace {
            target,
            replacement,
            mode,
        } if market == Market::Spot => {
            order_target(p, target, market, true)?;
            order_params(p, replacement, market, false, timestamp, stamp)?;
            p.text(
                "cancelReplaceMode",
                match mode {
                    CancelReplaceMode::StopOnFailure => "STOP_ON_FAILURE",
                    CancelReplaceMode::AllowFailure => "ALLOW_FAILURE",
                },
            )?;
            (
                Method::Post,
                "/api/v3/order/cancelReplace",
                "order.cancelReplace",
            )
        }
        CommandBody::AmendKeepPriority {
            target,
            new_quantity,
            new_client_id,
        } if market == Market::Spot => {
            order_target(p, target, market, false)?;
            p.decimal("newQty", *new_quantity)?;
            if let Some(id) = new_client_id {
                client_id(id.as_str(), market)?;
                p.text("newClientOrderId", id.as_str())?;
            }
            (
                Method::Put,
                "/api/v3/order/amend/keepPriority",
                "order.amend.keepPriority",
            )
        }
        CommandBody::Modify(m) if market == Market::Usdm => {
            if m.new_client_id.is_some() {
                return Err(Error::UnsupportedCommand);
            }
            order_target(p, &m.target, market, false)?;
            p.text("side", side(m.side))?;
            p.decimal("quantity", m.quantity)?;
            p.decimal("price", m.price)?;
            if m.reduce_only {
                p.text("reduceOnly", "true")?;
            }
            (Method::Put, "/fapi/v1/order", "order.modify")
        }
        CommandBody::Algo(a) if market == Market::Usdm => {
            p.text("algoType", "CONDITIONAL")?;
            order_params(p, &a.order, market, true, timestamp, stamp)?;
            (Method::Post, "/fapi/v1/algoOrder", "algoOrder.place")
        }
        CommandBody::CancelAlgo(t) if market == Market::Usdm => {
            if let Some(id) = &t.venue_algo_id {
                p.integer("algoId", native_id(id.as_str())?)?;
            }
            if let Some(id) = &t.client_id {
                client_id(id.as_str(), market)?;
                p.text("clientAlgoId", id.as_str())?;
            }
            // Binance's algo cancel does not accept symbol.
            p.0.retain(|(k, _)| *k != "symbol");
            (Method::Delete, "/fapi/v1/algoOrder", "algoOrder.cancel")
        }
        CommandBody::CancelList(t) if market == Market::Spot => {
            if let Some(id) = &t.venue_list_id {
                p.integer("orderListId", native_id(id.as_str())?)?;
            }
            if let Some(id) = &t.client_id {
                client_id(id.as_str(), market)?;
                p.text("listClientOrderId", id.as_str())?;
            }
            (Method::Delete, "/api/v3/orderList", "orderList.cancel")
        }
        _ => return Err(Error::UnsupportedCommand),
    })
}
fn side(s: Side) -> &'static str {
    match s {
        Side::Buy => "BUY",
        Side::Sell => "SELL",
    }
}
fn order_params(
    p: &mut OwnedParams,
    o: &OrderSpec,
    market: Market,
    algo: bool,
    timestamp: u64,
    stamp: Timestamp,
) -> Result<()> {
    client_id(o.client_id.as_str(), market)?;
    p.text(
        if algo {
            "clientAlgoId"
        } else {
            "newClientOrderId"
        },
        o.client_id.as_str(),
    )?;
    p.text("side", side(o.side))?;
    let kind = match (market, algo, o.order_type, o.time_in_force) {
        (Market::Spot, false, OrderType::Limit, Some(TimeInForce::PostOnly)) => "LIMIT_MAKER",
        (_, false, OrderType::Limit, _) => "LIMIT",
        (_, false, OrderType::Market, _) => "MARKET",
        (Market::Spot, false, OrderType::StopLimit, _) => "STOP_LOSS_LIMIT",
        (Market::Spot, false, OrderType::StopMarket, _) => "STOP_LOSS",
        (Market::Spot, false, OrderType::TakeProfitLimit, _) => "TAKE_PROFIT_LIMIT",
        (Market::Spot, false, OrderType::TakeProfitMarket, _) => "TAKE_PROFIT",
        (Market::Usdm, true, OrderType::StopLimit, _) => "STOP",
        (Market::Usdm, true, OrderType::StopMarket, _) => "STOP_MARKET",
        (Market::Usdm, true, OrderType::TakeProfitLimit, _) => "TAKE_PROFIT",
        (Market::Usdm, true, OrderType::TakeProfitMarket, _) => "TAKE_PROFIT_MARKET",
        (Market::Usdm, true, OrderType::TrailingStop, _) => "TRAILING_STOP_MARKET",
        _ => return Err(Error::UnsupportedCommand),
    };
    p.text("type", kind)?;
    match (market, o.mode) {
        (Market::Spot, ProductMode::Spot) => {}
        (Market::Usdm, ProductMode::Derivative { position_mode, .. }) => {
            if position_mode == PositionMode::Hedge && o.reduce_only {
                return Err(Error::InvalidParameter);
            }
            p.text(
                "positionSide",
                match o.position_side {
                    PositionSide::Both => "BOTH",
                    PositionSide::Long => "LONG",
                    PositionSide::Short => "SHORT",
                },
            )?;
            if o.reduce_only {
                p.text("reduceOnly", "true")?;
            }
            if o.iceberg_quantity.is_some() {
                return Err(Error::UnsupportedCommand);
            }
        }
        _ => return Err(Error::UnsupportedCommand),
    }
    match o.quantity {
        Quantity::Base(v) => p.decimal("quantity", v)?,
        Quantity::Quote(v) if market == Market::Spot => p.decimal("quoteOrderQty", v)?,
        Quantity::ClosePosition if algo && !o.reduce_only => {
            if matches!(
                (o.side, o.position_side),
                (Side::Buy, PositionSide::Long) | (Side::Sell, PositionSide::Short)
            ) {
                return Err(Error::InvalidParameter);
            }
            p.text("closePosition", "true")?;
        }
        _ => return Err(Error::UnsupportedCommand),
    }
    if let Some(price) = o.price {
        p.decimal("price", price)?;
    }
    if let Some(tif) = o.time_in_force {
        let value = match tif {
            TimeInForce::Gtc => "GTC",
            TimeInForce::Ioc => "IOC",
            TimeInForce::Fok => "FOK",
            TimeInForce::PostOnly if market == Market::Spot && kind == "LIMIT_MAKER" => "",
            TimeInForce::PostOnly if market == Market::Usdm => "GTX",
            TimeInForce::Gtd { expire_utc_ns } if market == Market::Usdm => {
                let now_seconds = timestamp
                    / match stamp {
                        Timestamp::Milliseconds(_) => 1000,
                        Timestamp::Microseconds(_) => 1_000_000,
                    };
                let expire = expire_utc_ns / 1_000_000_000;
                if expire <= now_seconds.saturating_add(600) || expire >= 253402300799 {
                    return Err(Error::InvalidParameter);
                }
                p.integer("goodTillDate", expire * 1000)?;
                "GTD"
            }
            _ => return Err(Error::UnsupportedCommand),
        };
        if !value.is_empty() {
            p.text("timeInForce", value)?;
        }
    }
    if let Some(t) = o.trigger {
        p.decimal(if algo { "triggerPrice" } else { "stopPrice" }, t.price)?;
        if market == Market::Usdm {
            p.text("workingType", working(t.working_price)?)?;
            p.text(
                "priceProtect",
                if t.price_protect { "true" } else { "false" },
            )?;
        }
    }
    if let Some(t) = o.trailing {
        match (market, t.distance) {
            (Market::Spot, TrailingDistance::BasisPoints(n)) => {
                p.integer("trailingDelta", n as u64)?
            }
            (Market::Usdm, TrailingDistance::Percent(rate)) => {
                // The endpoint permits 0.1 through 10 percent.
                let decimal =
                    crate::decimal::Decimal::parse(&rate.as_plain_string())?.rescale_exact(8)?;
                if !(10_000_000..=1_000_000_000).contains(&decimal.mantissa()) {
                    return Err(Error::InvalidParameter);
                }
                p.decimal("callbackRate", rate)?;
                if let Some(v) = t.activation_price {
                    p.decimal("activatePrice", v)?;
                }
                p.text("workingType", working(t.working_price)?)?;
            }
            _ => return Err(Error::InvalidParameter),
        }
    }
    if let Some(v) = o.iceberg_quantity {
        p.decimal("icebergQty", v)?;
    }
    if o.self_trade_prevention != SelfTradePrevention::Default {
        p.text(
            "selfTradePreventionMode",
            match o.self_trade_prevention {
                SelfTradePrevention::None => "NONE",
                SelfTradePrevention::ExpireTaker => "EXPIRE_TAKER",
                SelfTradePrevention::ExpireMaker => "EXPIRE_MAKER",
                SelfTradePrevention::ExpireBoth => "EXPIRE_BOTH",
                SelfTradePrevention::Decrement if market == Market::Spot => "DECREMENT",
                _ => return Err(Error::UnsupportedCommand),
            },
        )?;
    }
    Ok(())
}
fn working(w: WorkingPrice) -> Result<&'static str> {
    match w {
        WorkingPrice::Contract => Ok("CONTRACT_PRICE"),
        WorkingPrice::Mark => Ok("MARK_PRICE"),
        WorkingPrice::Index => Err(Error::UnsupportedCommand),
    }
}
