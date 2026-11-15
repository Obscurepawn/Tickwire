//! Zero-allocation full-market Binance ORS wire encoders, stream URL decorators, and
//! `TerminalReason` / `explainError` classifiers across Spot, USD-M, Coin-M, and Options.

use crate::{
    Error, Result,
    auth::{Endpoint, MAX_SIGNATURE, SigningKey},
    buffer::{Sink, atomic, json_string, percent},
    params::{Params, Value},
};
use tickwire_venue::{
    OrderType, PositionSide, Product, SelfTradePrevention, Side, TerminalReason, TimeInForce,
};

const MAX_FULL_MARKET_PARAMS: usize = 32;
const MAX_PREIMAGE_SCRATCH: usize = 4096;

/// All four Binance trading product families supported by the unified stateless `BinanceSession`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum BinanceVenueMarket {
    Spot,
    Usdm,
    Coinm,
    Options,
}

impl BinanceVenueMarket {
    pub const ALL: [Self; 4] = [Self::Spot, Self::Usdm, Self::Coinm, Self::Options];

    #[inline]
    pub const fn index(self) -> usize {
        match self {
            Self::Spot => 0,
            Self::Usdm => 1,
            Self::Coinm => 2,
            Self::Options => 3,
        }
    }

    #[inline]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Spot => "spot",
            Self::Usdm => "usdm",
            Self::Coinm => "coinm",
            Self::Options => "options",
        }
    }

    #[inline]
    pub const fn from_product(product: Product) -> Option<Self> {
        match product {
            Product::Spot => Some(Self::Spot),
            Product::UsdM | Product::LinearPerpetual | Product::LinearFuture => Some(Self::Usdm),
            Product::CoinM | Product::InversePerpetual | Product::InverseFuture => {
                Some(Self::Coinm)
            }
            Product::Option => Some(Self::Options),
        }
    }

    #[inline]
    pub const fn primary_product(self) -> Product {
        match self {
            Self::Spot => Product::Spot,
            Self::Usdm => Product::UsdM,
            Self::Coinm => Product::CoinM,
            Self::Options => Product::Option,
        }
    }

    /// Bitmask of `Product`s routed to this Binance market sub-pool (`OrderRoute.product_mask`).
    #[inline]
    pub const fn product_mask(self) -> u16 {
        match self {
            Self::Spot => Product::Spot.mask_bit(),
            Self::Usdm => {
                Product::UsdM.mask_bit()
                    | Product::LinearPerpetual.mask_bit()
                    | Product::LinearFuture.mask_bit()
            }
            Self::Coinm => {
                Product::CoinM.mask_bit()
                    | Product::InversePerpetual.mask_bit()
                    | Product::InverseFuture.mask_bit()
            }
            Self::Options => Product::Option.mask_bit(),
        }
    }

    #[inline]
    pub const fn uses_ws_order_pool(self) -> bool {
        matches!(self, Self::Spot | Self::Usdm | Self::Coinm)
    }

    #[inline]
    pub const fn uses_https_order_pool(self) -> bool {
        matches!(self, Self::Options)
    }

    #[inline]
    pub const fn needs_listen_key(self) -> bool {
        matches!(self, Self::Usdm | Self::Coinm | Self::Options)
    }

    #[inline]
    pub const fn default_stream_token_refresh_s(self) -> u32 {
        match self {
            Self::Spot => 0,
            Self::Usdm | Self::Coinm | Self::Options => 1800,
        }
    }

    #[inline]
    pub const fn ws_order_path(self) -> Option<&'static str> {
        match self {
            Self::Spot => Some("/ws-api/v3"),
            Self::Usdm => Some("/ws-fapi/v1"),
            Self::Coinm => Some("/ws-dapi/v1"),
            Self::Options => None,
        }
    }

    #[inline]
    pub const fn rest_order_path(self) -> &'static str {
        match self {
            Self::Spot => "/api/v3/order",
            Self::Usdm => "/fapi/v1/order",
            Self::Coinm => "/dapi/v1/order",
            Self::Options => "/eapi/v1/order",
        }
    }

    #[inline]
    pub const fn rest_open_orders_path(self) -> &'static str {
        match self {
            Self::Spot => "/api/v3/openOrders",
            Self::Usdm => "/fapi/v1/openOrders",
            Self::Coinm => "/dapi/v1/openOrders",
            Self::Options => "/eapi/v1/openOrders",
        }
    }

    #[inline]
    pub const fn rest_account_path(self) -> &'static str {
        match self {
            Self::Spot => "/api/v3/account",
            Self::Usdm => "/fapi/v2/account",
            Self::Coinm => "/dapi/v1/account",
            Self::Options => "/eapi/v1/account",
        }
    }

    #[inline]
    pub const fn listen_key_path(self) -> Option<&'static str> {
        match self {
            Self::Spot => None,
            Self::Usdm => Some("/fapi/v1/listenKey"),
            Self::Coinm => Some("/dapi/v1/listenKey"),
            Self::Options => Some("/eapi/v1/listenKey"),
        }
    }

    #[inline]
    pub const fn logon_endpoint(self) -> Option<Endpoint> {
        match self {
            Self::Spot => Some(Endpoint::SpotSessionLogon),
            Self::Usdm => Some(Endpoint::UsdmSessionLogon),
            Self::Coinm => Some(Endpoint::CoinmSessionLogon),
            Self::Options => None,
        }
    }

    #[inline]
    pub const fn ws_signing_endpoint(self) -> Option<Endpoint> {
        match self {
            Self::Spot => Some(Endpoint::SpotWebSocket),
            Self::Usdm => Some(Endpoint::UsdmWebSocket),
            Self::Coinm => Some(Endpoint::CoinmWebSocket),
            Self::Options => None,
        }
    }

    #[inline]
    pub const fn rest_signing_endpoint(self) -> Endpoint {
        match self {
            Self::Spot => Endpoint::SpotRest,
            Self::Usdm => Endpoint::UsdmRestQuery,
            Self::Coinm => Endpoint::CoinmRest,
            Self::Options => Endpoint::OptionsRest,
        }
    }
}

/// Formats `atoms * 10^-scale` into canonical plain decimal ASCII in `out` with zero floating-point
/// arithmetic and zero heap allocation (stripping redundant trailing fractional zeros).
pub fn format_scaled_i64(atoms: i64, scale: u8, out: &mut [u8]) -> Result<usize> {
    if scale > 18 {
        return Err(Error::InvalidDecimal);
    }
    atomic(out, |s| write_scaled_i64_sink(s, atoms, scale))
}

fn write_scaled_i64_sink(s: &mut Sink<'_>, atoms: i64, scale: u8) -> Result<()> {
    if atoms < 0 {
        s.byte(b'-')?;
    }
    let abs = atoms.unsigned_abs() as u128;
    if scale == 0 {
        return s.integer(abs);
    }
    let mut pow10 = 1u128;
    for _ in 0..scale {
        pow10 = pow10.checked_mul(10).ok_or(Error::Overflow)?;
    }
    let whole = abs / pow10;
    let frac = abs % pow10;
    s.integer(whole)?;
    if frac == 0 {
        return Ok(());
    }
    s.byte(b'.')?;
    let mut digits = [b'0'; 18];
    let scale_usize = usize::from(scale);
    let mut rem = frac;
    for idx in (0..scale_usize).rev() {
        digits[idx] = b'0' + (rem % 10) as u8;
        rem /= 10;
    }
    let mut end = scale_usize;
    while end > 0 && digits[end - 1] == b'0' {
        end -= 1;
    }
    s.bytes(&digits[..end])
}

fn validate_request_id(request_id: &str) -> Result<()> {
    if request_id.is_empty()
        || request_id.len() > 64
        || !request_id.bytes().all(|b| (33..=126).contains(&b))
    {
        return Err(Error::InvalidParameter);
    }
    Ok(())
}

fn validate_api_key(api_key: &str) -> Result<()> {
    if api_key.is_empty()
        || api_key.len() > 256
        || !api_key
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_".contains(&b))
    {
        return Err(Error::InvalidParameter);
    }
    Ok(())
}

fn validate_symbol(symbol: &str) -> Result<()> {
    if symbol.is_empty()
        || symbol.len() > 64
        || !symbol
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
    {
        return Err(Error::InvalidParameter);
    }
    Ok(())
}

fn validate_client_id(cid: &str) -> Result<()> {
    if cid.is_empty()
        || cid.len() > 36
        || !cid
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b".:/_-".contains(&b))
    {
        return Err(Error::InvalidParameter);
    }
    Ok(())
}

/// Encodes an Ed25519 `session.logon` WebSocket frame for Spot, USD-M, or Coin-M into `out`
/// with zero heap allocation.
pub fn encode_ws_session_logon(
    request_id: &str,
    api_key: &str,
    timestamp_ms: u64,
    recv_window_ms: Option<u64>,
    signer: &SigningKey,
    market: BinanceVenueMarket,
    out: &mut [u8],
) -> Result<usize> {
    validate_request_id(request_id)?;
    validate_api_key(api_key)?;
    if timestamp_ms == 0 {
        return Err(Error::InvalidParameter);
    }
    let endpoint = market.logon_endpoint().ok_or(Error::UnsupportedCommand)?;
    let mut params = Params::<8>::new();
    params.push("apiKey", Value::String(api_key))?;
    if let Some(rw) = recv_window_ms {
        if rw == 0 || rw > 60_000 {
            return Err(Error::InvalidParameter);
        }
        params.push("recvWindow", Value::Integer(rw))?;
    }
    params.push("timestamp", Value::Integer(timestamp_ms))?;

    let mut preimage = [0u8; 512];
    let pre_len = params.websocket_preimage(&mut preimage)?;
    let mut sig = [0u8; MAX_SIGNATURE];
    let sig_len = signer.sign(endpoint, &preimage[..pre_len], &mut sig)?;
    let sig_str = core::str::from_utf8(&sig[..sig_len]).map_err(|_| Error::SigningFailed)?;

    let mut json_params = [0u8; 768];
    let json_len = params.json(&mut json_params)?;

    atomic(out, |s| {
        s.bytes(b"{\"id\":")?;
        json_string(s, request_id)?;
        s.bytes(b",\"method\":\"session.logon\",\"params\":")?;
        s.bytes(&json_params[..json_len - 1])?;
        s.bytes(b",\"signature\":")?;
        json_string(s, sig_str)?;
        s.bytes(b"}}")
    })
}

/// Encodes Spot's `userDataStream.subscribe` WebSocket frame after `session.logon`.
pub fn encode_ws_user_stream_subscribe(request_id: &str, out: &mut [u8]) -> Result<usize> {
    validate_request_id(request_id)?;
    atomic(out, |s| {
        s.bytes(b"{\"id\":")?;
        json_string(s, request_id)?;
        s.bytes(b",\"method\":\"userDataStream.subscribe\"}")
    })
}

/// Decorates a listen-key stream URL (`/ws/<key>` on colocation hosts vs `?listenKey=<key>` on
/// public `/private/ws` hosts) without heap allocation.
pub fn decorate_listen_key_stream_url(
    base_url: &str,
    colo: bool,
    listen_key: &str,
    out: &mut [u8],
) -> Result<usize> {
    if base_url.is_empty()
        || listen_key.is_empty()
        || listen_key.len() > 128
        || !listen_key
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_".contains(&b))
    {
        return Err(Error::InvalidParameter);
    }
    let trimmed = base_url.trim_end_matches('/');
    atomic(out, |s| {
        s.bytes(trimmed.as_bytes())?;
        if colo {
            s.byte(b'/')?;
        } else {
            s.bytes(b"?listenKey=")?;
        }
        s.bytes(listen_key.as_bytes())
    })
}

/// Optional per-frame signature credentials when a WebSocket link has not run `session.logon`.
#[derive(Clone, Copy)]
pub struct WsPerFrameAuth<'a> {
    pub api_key: &'a str,
    pub signer: &'a SigningKey,
}

/// Zero-allocation WebSocket `order.place` input across Spot, USD-M, and Coin-M.
#[derive(Clone, Copy)]
pub struct WsOrderNewParams<'a> {
    pub market: BinanceVenueMarket,
    pub request_id: &'a str,
    pub symbol: &'a str,
    pub client_order_id: &'a str,
    pub side: Side,
    pub order_type: OrderType,
    pub tif: TimeInForce,
    pub post_only: bool,
    pub reduce_only: bool,
    pub position_side: PositionSide,
    pub stp: SelfTradePrevention,
    pub price_ticks: i64,
    pub price_scale: u8,
    pub qty_lots: i64,
    pub qty_scale: u8,
    pub timestamp_ms: u64,
    pub recv_window_ms: Option<u64>,
    pub per_frame_auth: Option<WsPerFrameAuth<'a>>,
}

/// Encodes a WebSocket `order.place` JSON request for Spot (`/ws-api/v3`), USD-M (`/ws-fapi/v1`),
/// or Coin-M (`/ws-dapi/v1`) into `out` with zero heap allocation.
pub fn encode_ws_order_place(req: &WsOrderNewParams<'_>, out: &mut [u8]) -> Result<usize> {
    if !req.market.uses_ws_order_pool() {
        return Err(Error::UnsupportedCommand);
    }
    validate_request_id(req.request_id)?;
    validate_symbol(req.symbol)?;
    validate_client_id(req.client_order_id)?;
    if req.qty_lots <= 0 || req.timestamp_ms == 0 {
        return Err(Error::InvalidParameter);
    }
    if req.order_type == OrderType::Limit && req.price_ticks <= 0 {
        return Err(Error::InvalidParameter);
    }

    let mut price_buf = [0u8; 32];
    let mut qty_buf = [0u8; 32];
    let qty_len = format_scaled_i64(req.qty_lots, req.qty_scale, &mut qty_buf)?;
    let qty_str = core::str::from_utf8(&qty_buf[..qty_len]).map_err(|_| Error::InvalidDecimal)?;

    let effective_tif = if req.post_only && req.tif != TimeInForce::Rpi {
        TimeInForce::PostOnly
    } else {
        req.tif
    };

    let (wire_type, wire_tif): (&'static str, Option<&'static str>) =
        match (req.market, req.order_type, effective_tif) {
            (BinanceVenueMarket::Spot, OrderType::Limit, TimeInForce::PostOnly) => {
                ("LIMIT_MAKER", None)
            }
            (_, OrderType::Limit, TimeInForce::PostOnly) => ("LIMIT", Some("GTX")),
            (_, OrderType::Limit, TimeInForce::Rpi) => ("LIMIT", Some("RPI")),
            (_, OrderType::Limit, TimeInForce::Gtc) => ("LIMIT", Some("GTC")),
            (_, OrderType::Limit, TimeInForce::Ioc) => ("LIMIT", Some("IOC")),
            (_, OrderType::Limit, TimeInForce::Fok) => ("LIMIT", Some("FOK")),
            (_, OrderType::Market, _) => ("MARKET", None),
            _ => return Err(Error::UnsupportedCommand),
        };

    let mut params = Params::<MAX_FULL_MARKET_PARAMS>::new();
    if let Some(auth) = req.per_frame_auth {
        validate_api_key(auth.api_key)?;
        params.push("apiKey", Value::String(auth.api_key))?;
    }
    params.push("newClientOrderId", Value::String(req.client_order_id))?;
    if matches!(
        req.market,
        BinanceVenueMarket::Usdm | BinanceVenueMarket::Coinm
    ) && req.position_side != PositionSide::Both
    {
        params.push(
            "positionSide",
            Value::String(match req.position_side {
                PositionSide::Both => "BOTH",
                PositionSide::Long => "LONG",
                PositionSide::Short => "SHORT",
            }),
        )?;
    }
    if req.order_type == OrderType::Limit {
        let price_len = format_scaled_i64(req.price_ticks, req.price_scale, &mut price_buf)?;
        let price_str =
            core::str::from_utf8(&price_buf[..price_len]).map_err(|_| Error::InvalidDecimal)?;
        params.push("price", Value::String(price_str))?;
    }
    params.push("quantity", Value::String(qty_str))?;
    if let Some(rw) = req.recv_window_ms {
        if rw == 0 || rw > 60_000 {
            return Err(Error::InvalidParameter);
        }
        params.push("recvWindow", Value::Integer(rw))?;
    }
    if req.reduce_only
        && matches!(
            req.market,
            BinanceVenueMarket::Usdm | BinanceVenueMarket::Coinm
        )
    {
        params.push("reduceOnly", Value::String("true"))?;
    }
    if req.stp != SelfTradePrevention::Default {
        let stp_str = match req.stp {
            SelfTradePrevention::None => "NONE",
            SelfTradePrevention::ExpireTaker => "EXPIRE_TAKER",
            SelfTradePrevention::ExpireMaker => "EXPIRE_MAKER",
            SelfTradePrevention::ExpireBoth => "EXPIRE_BOTH",
            SelfTradePrevention::Decrement if req.market == BinanceVenueMarket::Spot => "DECREMENT",
            _ => return Err(Error::UnsupportedCommand),
        };
        params.push("selfTradePreventionMode", Value::String(stp_str))?;
    }
    params.push(
        "side",
        Value::String(match req.side {
            Side::Buy => "BUY",
            Side::Sell => "SELL",
        }),
    )?;
    params.push("symbol", Value::String(req.symbol))?;
    if let Some(tif_str) = wire_tif {
        params.push("timeInForce", Value::String(tif_str))?;
    }
    params.push("timestamp", Value::Integer(req.timestamp_ms))?;
    params.push("type", Value::String(wire_type))?;

    encode_ws_envelope(
        req.market,
        req.request_id,
        "order.place",
        &params,
        req.per_frame_auth,
        out,
    )
}

/// Zero-allocation WebSocket `order.cancel` input across Spot, USD-M, and Coin-M.
#[derive(Clone, Copy)]
pub struct WsOrderCancelParams<'a> {
    pub market: BinanceVenueMarket,
    pub request_id: &'a str,
    pub symbol: &'a str,
    pub orig_client_order_id: Option<&'a str>,
    pub venue_order_id: Option<u64>,
    pub timestamp_ms: u64,
    pub recv_window_ms: Option<u64>,
    pub per_frame_auth: Option<WsPerFrameAuth<'a>>,
}

/// Encodes a WebSocket `order.cancel` JSON request for Spot, USD-M, or Coin-M into `out`.
pub fn encode_ws_order_cancel(req: &WsOrderCancelParams<'_>, out: &mut [u8]) -> Result<usize> {
    if !req.market.uses_ws_order_pool() {
        return Err(Error::UnsupportedCommand);
    }
    validate_request_id(req.request_id)?;
    validate_symbol(req.symbol)?;
    if (req.orig_client_order_id.is_none() && req.venue_order_id.is_none()) || req.timestamp_ms == 0
    {
        return Err(Error::InvalidParameter);
    }
    let mut params = Params::<MAX_FULL_MARKET_PARAMS>::new();
    if let Some(auth) = req.per_frame_auth {
        validate_api_key(auth.api_key)?;
        params.push("apiKey", Value::String(auth.api_key))?;
    }
    if let Some(oid) = req.venue_order_id {
        params.push("orderId", Value::Integer(oid))?;
    }
    if let Some(cid) = req.orig_client_order_id {
        validate_client_id(cid)?;
        params.push("origClientOrderId", Value::String(cid))?;
    }
    if let Some(rw) = req.recv_window_ms {
        if rw == 0 || rw > 60_000 {
            return Err(Error::InvalidParameter);
        }
        params.push("recvWindow", Value::Integer(rw))?;
    }
    params.push("symbol", Value::String(req.symbol))?;
    params.push("timestamp", Value::Integer(req.timestamp_ms))?;

    encode_ws_envelope(
        req.market,
        req.request_id,
        "order.cancel",
        &params,
        req.per_frame_auth,
        out,
    )
}

/// Zero-allocation WebSocket modify input (`order.modify` on USD-M/Coin-M,
/// `order.cancelReplace` with `STOP_ON_FAILURE` on Spot).
#[derive(Clone, Copy)]
pub struct WsOrderModifyParams<'a> {
    pub market: BinanceVenueMarket,
    pub request_id: &'a str,
    pub symbol: &'a str,
    pub orig_client_order_id: Option<&'a str>,
    pub venue_order_id: Option<u64>,
    pub new_client_order_id: Option<&'a str>,
    pub side: Side,
    pub new_price_ticks: i64,
    pub price_scale: u8,
    pub new_qty_lots: i64,
    pub qty_scale: u8,
    pub timestamp_ms: u64,
    pub recv_window_ms: Option<u64>,
    pub per_frame_auth: Option<WsPerFrameAuth<'a>>,
}

/// Encodes a WebSocket `order.modify` (USD-M / Coin-M) or `order.cancelReplace` (Spot) frame.
pub fn encode_ws_order_modify(req: &WsOrderModifyParams<'_>, out: &mut [u8]) -> Result<usize> {
    if !req.market.uses_ws_order_pool() {
        return Err(Error::UnsupportedCommand);
    }
    validate_request_id(req.request_id)?;
    validate_symbol(req.symbol)?;
    if (req.orig_client_order_id.is_none() && req.venue_order_id.is_none())
        || req.new_price_ticks <= 0
        || req.new_qty_lots <= 0
        || req.timestamp_ms == 0
    {
        return Err(Error::InvalidParameter);
    }
    let mut price_buf = [0u8; 32];
    let mut qty_buf = [0u8; 32];
    let price_len = format_scaled_i64(req.new_price_ticks, req.price_scale, &mut price_buf)?;
    let qty_len = format_scaled_i64(req.new_qty_lots, req.qty_scale, &mut qty_buf)?;
    let price_str =
        core::str::from_utf8(&price_buf[..price_len]).map_err(|_| Error::InvalidDecimal)?;
    let qty_str = core::str::from_utf8(&qty_buf[..qty_len]).map_err(|_| Error::InvalidDecimal)?;

    let mut params = Params::<MAX_FULL_MARKET_PARAMS>::new();
    if let Some(auth) = req.per_frame_auth {
        validate_api_key(auth.api_key)?;
        params.push("apiKey", Value::String(auth.api_key))?;
    }

    let ws_method = if req.market == BinanceVenueMarket::Spot {
        if let Some(oid) = req.venue_order_id {
            params.push("cancelOrderId", Value::Integer(oid))?;
        }
        if let Some(cid) = req.orig_client_order_id {
            validate_client_id(cid)?;
            params.push("cancelOrigClientOrderId", Value::String(cid))?;
        }
        params.push("cancelReplaceMode", Value::String("STOP_ON_FAILURE"))?;
        if let Some(new_cid) = req.new_client_order_id {
            validate_client_id(new_cid)?;
            params.push("newClientOrderId", Value::String(new_cid))?;
        }
        params.push("price", Value::String(price_str))?;
        params.push("quantity", Value::String(qty_str))?;
        if let Some(rw) = req.recv_window_ms {
            if rw == 0 || rw > 60_000 {
                return Err(Error::InvalidParameter);
            }
            params.push("recvWindow", Value::Integer(rw))?;
        }
        params.push(
            "side",
            Value::String(match req.side {
                Side::Buy => "BUY",
                Side::Sell => "SELL",
            }),
        )?;
        params.push("symbol", Value::String(req.symbol))?;
        params.push("timeInForce", Value::String("GTC"))?;
        params.push("timestamp", Value::Integer(req.timestamp_ms))?;
        params.push("type", Value::String("LIMIT"))?;
        "order.cancelReplace"
    } else {
        if let Some(oid) = req.venue_order_id {
            params.push("orderId", Value::Integer(oid))?;
        }
        if let Some(cid) = req.orig_client_order_id {
            validate_client_id(cid)?;
            params.push("origClientOrderId", Value::String(cid))?;
        }
        params.push("price", Value::String(price_str))?;
        params.push("quantity", Value::String(qty_str))?;
        if let Some(rw) = req.recv_window_ms {
            if rw == 0 || rw > 60_000 {
                return Err(Error::InvalidParameter);
            }
            params.push("recvWindow", Value::Integer(rw))?;
        }
        params.push(
            "side",
            Value::String(match req.side {
                Side::Buy => "BUY",
                Side::Sell => "SELL",
            }),
        )?;
        params.push("symbol", Value::String(req.symbol))?;
        params.push("timestamp", Value::Integer(req.timestamp_ms))?;
        "order.modify"
    };

    encode_ws_envelope(
        req.market,
        req.request_id,
        ws_method,
        &params,
        req.per_frame_auth,
        out,
    )
}

fn encode_ws_envelope(
    market: BinanceVenueMarket,
    request_id: &str,
    ws_method: &str,
    params: &Params<'_, MAX_FULL_MARKET_PARAMS>,
    per_frame_auth: Option<WsPerFrameAuth<'_>>,
    out: &mut [u8],
) -> Result<usize> {
    let mut json_scratch = [0u8; MAX_PREIMAGE_SCRATCH];
    let json_len = params.json(&mut json_scratch)?;

    if let Some(auth) = per_frame_auth {
        let endpoint = market
            .ws_signing_endpoint()
            .ok_or(Error::UnsupportedCommand)?;
        let mut preimage = [0u8; MAX_PREIMAGE_SCRATCH];
        let pre_len = params.websocket_preimage(&mut preimage)?;
        let mut sig = [0u8; MAX_SIGNATURE];
        let sig_len = auth.signer.sign(endpoint, &preimage[..pre_len], &mut sig)?;
        let sig_str = core::str::from_utf8(&sig[..sig_len]).map_err(|_| Error::SigningFailed)?;

        atomic(out, |s| {
            s.bytes(b"{\"id\":")?;
            json_string(s, request_id)?;
            s.bytes(b",\"method\":")?;
            json_string(s, ws_method)?;
            s.bytes(b",\"params\":")?;
            s.bytes(&json_scratch[..json_len - 1])?;
            s.bytes(b",\"signature\":")?;
            json_string(s, sig_str)?;
            s.bytes(b"}}")
        })
    } else {
        atomic(out, |s| {
            s.bytes(b"{\"id\":")?;
            json_string(s, request_id)?;
            s.bytes(b",\"method\":")?;
            json_string(s, ws_method)?;
            s.bytes(b",\"params\":")?;
            s.bytes(&json_scratch[..json_len])?;
            s.byte(b'}')
        })
    }
}

/// Zero-allocation European Options HTTP/1.1 `POST /eapi/v1/order` input (`human-docs/8.5`).
#[derive(Clone, Copy)]
pub struct OptionsHttpNewParams<'a> {
    pub host: &'a str,
    pub api_key: &'a str,
    pub hmac_signer: &'a SigningKey,
    pub symbol: &'a str,
    pub client_order_id: &'a str,
    pub side: Side,
    pub tif: TimeInForce,
    pub post_only: bool,
    pub reduce_only: bool,
    pub price_ticks: i64,
    pub price_scale: u8,
    pub qty_lots: i64,
    pub qty_scale: u8,
    pub timestamp_ms: u64,
    pub recv_window_ms: Option<u64>,
}

/// Encodes a complete HTTP/1.1 keep-alive `POST /eapi/v1/order` request with form-urlencoded body
/// signed by HMAC-SHA256 + `X-MBX-APIKEY` into `out` (`human-docs/8.5`).
pub fn encode_options_http_place(req: &OptionsHttpNewParams<'_>, out: &mut [u8]) -> Result<usize> {
    validate_api_key(req.api_key)?;
    validate_symbol(req.symbol)?;
    validate_client_id(req.client_order_id)?;
    if req.host.is_empty() || req.price_ticks <= 0 || req.qty_lots <= 0 || req.timestamp_ms == 0 {
        return Err(Error::InvalidParameter);
    }

    let mut price_buf = [0u8; 32];
    let mut qty_buf = [0u8; 32];
    let price_len = format_scaled_i64(req.price_ticks, req.price_scale, &mut price_buf)?;
    let qty_len = format_scaled_i64(req.qty_lots, req.qty_scale, &mut qty_buf)?;
    let price_str =
        core::str::from_utf8(&price_buf[..price_len]).map_err(|_| Error::InvalidDecimal)?;
    let qty_str = core::str::from_utf8(&qty_buf[..qty_len]).map_err(|_| Error::InvalidDecimal)?;

    let wire_tif = if req.post_only && req.tif != TimeInForce::Rpi {
        "GTX"
    } else {
        match req.tif {
            TimeInForce::Gtc => "GTC",
            TimeInForce::Ioc => "IOC",
            TimeInForce::Fok => "FOK",
            TimeInForce::PostOnly => "GTX",
            TimeInForce::Rpi => "RPI",
            TimeInForce::Gtd { .. } => return Err(Error::UnsupportedCommand),
        }
    };

    let mut params = Params::<MAX_FULL_MARKET_PARAMS>::new();
    params.push("symbol", Value::String(req.symbol))?;
    params.push(
        "side",
        Value::String(match req.side {
            Side::Buy => "BUY",
            Side::Sell => "SELL",
        }),
    )?;
    params.push("type", Value::String("LIMIT"))?;
    params.push("quantity", Value::String(qty_str))?;
    params.push("price", Value::String(price_str))?;
    params.push("timeInForce", Value::String(wire_tif))?;
    if req.reduce_only {
        params.push("reduceOnly", Value::String("true"))?;
    }
    params.push("clientOrderId", Value::String(req.client_order_id))?;
    if let Some(rw) = req.recv_window_ms {
        if rw == 0 || rw > 60_000 {
            return Err(Error::InvalidParameter);
        }
        params.push("recvWindow", Value::Integer(rw))?;
    }
    params.push("timestamp", Value::Integer(req.timestamp_ms))?;

    encode_options_form_http(
        "POST",
        "/eapi/v1/order",
        req.host,
        req.api_key,
        req.hmac_signer,
        &params,
        out,
    )
}

/// Zero-allocation European Options HTTP/1.1 `DELETE /eapi/v1/order` input (`human-docs/8.5`).
#[derive(Clone, Copy)]
pub struct OptionsHttpCancelParams<'a> {
    pub host: &'a str,
    pub api_key: &'a str,
    pub hmac_signer: &'a SigningKey,
    pub symbol: &'a str,
    pub client_order_id: Option<&'a str>,
    pub venue_order_id: Option<u64>,
    pub timestamp_ms: u64,
    pub recv_window_ms: Option<u64>,
}

/// Encodes a complete HTTP/1.1 keep-alive `DELETE /eapi/v1/order` request with form-urlencoded
/// body signed by HMAC-SHA256 + `X-MBX-APIKEY` into `out` (`human-docs/8.5`).
pub fn encode_options_http_cancel(
    req: &OptionsHttpCancelParams<'_>,
    out: &mut [u8],
) -> Result<usize> {
    validate_api_key(req.api_key)?;
    validate_symbol(req.symbol)?;
    if req.host.is_empty()
        || (req.client_order_id.is_none() && req.venue_order_id.is_none())
        || req.timestamp_ms == 0
    {
        return Err(Error::InvalidParameter);
    }

    let mut params = Params::<MAX_FULL_MARKET_PARAMS>::new();
    params.push("symbol", Value::String(req.symbol))?;
    if let Some(oid) = req.venue_order_id {
        params.push("orderId", Value::Integer(oid))?;
    }
    if let Some(cid) = req.client_order_id {
        validate_client_id(cid)?;
        params.push("clientOrderId", Value::String(cid))?;
    }
    if let Some(rw) = req.recv_window_ms {
        if rw == 0 || rw > 60_000 {
            return Err(Error::InvalidParameter);
        }
        params.push("recvWindow", Value::Integer(rw))?;
    }
    params.push("timestamp", Value::Integer(req.timestamp_ms))?;

    encode_options_form_http(
        "DELETE",
        "/eapi/v1/order",
        req.host,
        req.api_key,
        req.hmac_signer,
        &params,
        out,
    )
}

fn encode_options_form_http(
    method: &str,
    path: &str,
    host: &str,
    api_key: &str,
    hmac_signer: &SigningKey,
    params: &Params<'_, MAX_FULL_MARKET_PARAMS>,
    out: &mut [u8],
) -> Result<usize> {
    let mut form_preimage = [0u8; MAX_PREIMAGE_SCRATCH];
    let pre_len = params.rest(&mut form_preimage)?;
    let mut sig = [0u8; MAX_SIGNATURE];
    let sig_len = hmac_signer.sign(Endpoint::OptionsRest, &form_preimage[..pre_len], &mut sig)?;
    // HMAC-SHA256 hex output is 64 lowercase hex ASCII characters (already URL-safe).
    let body_len = pre_len + "&signature=".len() + sig_len;

    atomic(out, |s| {
        s.bytes(method.as_bytes())?;
        s.byte(b' ')?;
        s.bytes(path.as_bytes())?;
        s.bytes(b" HTTP/1.1\r\nHost: ")?;
        s.bytes(host.as_bytes())?;
        s.bytes(b"\r\nX-MBX-APIKEY: ")?;
        s.bytes(api_key.as_bytes())?;
        s.bytes(b"\r\nContent-Type: application/x-www-form-urlencoded\r\nContent-Length: ")?;
        s.integer(body_len as u128)?;
        s.bytes(b"\r\nConnection: keep-alive\r\n\r\n")?;
        s.bytes(&form_preimage[..pre_len])?;
        s.bytes(b"&signature=")?;
        s.bytes(&sig[..sig_len])
    })
}

/// Encodes a signed REST `GET` order-status target (`/{api/v3,fapi/v1,dapi/v1,eapi/v1}/order?...`)
/// using `origClientOrderId` on Spot/USD-M/Coin-M and `clientOrderId` on Options (`recvWindow=60000`
/// default per `human-docs/8.5`).
pub fn encode_signed_order_status_target(
    market: BinanceVenueMarket,
    symbol: &str,
    client_order_id: &str,
    recv_window_ms: u64,
    timestamp_ms: u64,
    signer: &SigningKey,
    out: &mut [u8],
) -> Result<usize> {
    validate_symbol(symbol)?;
    validate_client_id(client_order_id)?;
    if recv_window_ms == 0 || recv_window_ms > 60_000 || timestamp_ms == 0 {
        return Err(Error::InvalidParameter);
    }
    let cid_key = if market == BinanceVenueMarket::Options {
        "clientOrderId"
    } else {
        "origClientOrderId"
    };
    let mut params = Params::<8>::new();
    params.push("symbol", Value::String(symbol))?;
    params.push(cid_key, Value::String(client_order_id))?;
    params.push("recvWindow", Value::Integer(recv_window_ms))?;
    params.push("timestamp", Value::Integer(timestamp_ms))?;

    encode_signed_get_target(
        market.rest_order_path(),
        market.rest_signing_endpoint(),
        &params,
        signer,
        out,
    )
}

/// Encodes a signed REST `GET` `openOrders` target (`/{api/v3,fapi/v1,dapi/v1,eapi/v1}/openOrders?...`).
pub fn encode_signed_open_orders_target(
    market: BinanceVenueMarket,
    symbol: Option<&str>,
    recv_window_ms: u64,
    timestamp_ms: u64,
    signer: &SigningKey,
    out: &mut [u8],
) -> Result<usize> {
    if recv_window_ms == 0 || recv_window_ms > 60_000 || timestamp_ms == 0 {
        return Err(Error::InvalidParameter);
    }
    let mut params = Params::<8>::new();
    if let Some(sym) = symbol {
        validate_symbol(sym)?;
        params.push("symbol", Value::String(sym))?;
    }
    params.push("recvWindow", Value::Integer(recv_window_ms))?;
    params.push("timestamp", Value::Integer(timestamp_ms))?;

    encode_signed_get_target(
        market.rest_open_orders_path(),
        market.rest_signing_endpoint(),
        &params,
        signer,
        out,
    )
}

/// Encodes a signed REST `GET` `account` snapshot target (`/{api/v3,fapi/v2,dapi/v1,eapi/v1}/account?...`).
pub fn encode_signed_account_target(
    market: BinanceVenueMarket,
    recv_window_ms: u64,
    timestamp_ms: u64,
    signer: &SigningKey,
    out: &mut [u8],
) -> Result<usize> {
    if recv_window_ms == 0 || recv_window_ms > 60_000 || timestamp_ms == 0 {
        return Err(Error::InvalidParameter);
    }
    let mut params = Params::<4>::new();
    params.push("recvWindow", Value::Integer(recv_window_ms))?;
    params.push("timestamp", Value::Integer(timestamp_ms))?;

    encode_signed_get_target(
        market.rest_account_path(),
        market.rest_signing_endpoint(),
        &params,
        signer,
        out,
    )
}

fn encode_signed_get_target<const CAP: usize>(
    path: &str,
    endpoint: Endpoint,
    params: &Params<'_, CAP>,
    signer: &SigningKey,
    out: &mut [u8],
) -> Result<usize> {
    let mut preimage = [0u8; MAX_PREIMAGE_SCRATCH];
    let pre_len = params.rest(&mut preimage)?;
    let mut sig = [0u8; MAX_SIGNATURE];
    let sig_len = signer.sign(endpoint, &preimage[..pre_len], &mut sig)?;

    atomic(out, |s| {
        s.bytes(path.as_bytes())?;
        s.byte(b'?')?;
        s.bytes(&preimage[..pre_len])?;
        s.bytes(b"&signature=")?;
        percent(s, &sig[..sig_len])
    })
}

/// Explains a Binance `(code, msg)` error pair into an ORS [`TerminalReason`] (`human-docs/8.5`
/// `explainError`), mapping `-1007`, `-1000` ("unknown error" / "try again later"), `-1001`,
/// `-1006`, and HTTP `408` to `TerminalReason::MKT_DELAYED`.
pub fn explain_binance_error(
    code: i64,
    msg: &str,
    tif: Option<TimeInForce>,
    post_only: bool,
) -> TerminalReason {
    match code {
        -1007 | -1006 | -1001 | 408 => TerminalReason::MKT_DELAYED,
        -1000 => TerminalReason::MKT_DELAYED,
        -1003 | -1015 | 429 | 418 => TerminalReason::RateLimited,
        -2011 | -2013 => TerminalReason::MKT_REJECT_NO_SUCH_ORDER,
        -2018 | -2019 => TerminalReason::InsufficientFunds,
        -2022 => TerminalReason::MKT_REDUCE_ONLY_CANCELED,
        -5022 => TerminalReason::MKT_REJECT_POST_ONLY,
        -5027 => TerminalReason::NoChange,
        _ => {
            if contains_ignore_case(msg, "try again later")
                || contains_ignore_case(msg, "send status unknown")
                || contains_ignore_case(msg, "execution status unknown")
                || contains_ignore_case(msg, "an unknown error occurred while processing")
            {
                return TerminalReason::MKT_DELAYED;
            }
            tickwire_venue::classify_binance_reason(Some(code), msg, None, tif, post_only)
        }
    }
}

/// Shared terminal-reason classifier across synchronous replies, private user-stream events, and
/// `StatusAsk` snapshots (`human-docs/8.5` `terminalReason`):
///
/// - `X == "EXPIRED_IN_MATCH"` or `x == "TRADE_PREVENTION"` -> `MKT_CANCELED_SELFTRADE`
/// - `X == "EXPIRED"`:
///   * `IOC` / `FOK` -> `MKT_IOC_CANCELED`
///   * `GTX` / `post_only` / `RPI` on Spot, Coin-M, or Options -> `MKT_REJECT_POST_ONLY`
///   * USD-M `EXPIRED` -> `MKT_REDUCE_ONLY_CANCELED`
///   * Spot other `EXPIRED` -> `MKT_CANCELED`
/// - Otherwise: `ADMIN_ACTION` if `admin_cancel && pending_cxl > 0`, `NO_REASON` if
///   `pending_cxl > 0`, else `MKT_CANCELED`.
#[allow(clippy::too_many_arguments)]
pub fn classify_binance_terminal_reason(
    market: BinanceVenueMarket,
    order_status: &str,
    exec_type: &str,
    wire_tif: &str,
    order_tif: Option<TimeInForce>,
    post_only: bool,
    stp_or_expired_reason: Option<&str>,
    pending_cxl: u8,
    admin_cancel: bool,
) -> TerminalReason {
    let status = order_status.trim();
    let exec = exec_type.trim();
    let tif_str = wire_tif.trim();

    if status.eq_ignore_ascii_case("EXPIRED_IN_MATCH")
        || exec.eq_ignore_ascii_case("TRADE_PREVENTION")
    {
        return TerminalReason::MKT_CANCELED_SELFTRADE;
    }

    if let Some(er) = stp_or_expired_reason
        && (contains_ignore_case(er, "SELF_TRADE")
            || contains_ignore_case(er, "EXPIRE_MAKER")
            || contains_ignore_case(er, "EXPIRE_TAKER")
            || contains_ignore_case(er, "EXPIRE_BOTH"))
        && status.eq_ignore_ascii_case("EXPIRED_IN_MATCH")
    {
        return TerminalReason::MKT_CANCELED_SELFTRADE;
    }

    if status.eq_ignore_ascii_case("EXPIRED") {
        let is_ioc_or_fok = tif_str.eq_ignore_ascii_case("IOC")
            || tif_str.eq_ignore_ascii_case("FOK")
            || matches!(order_tif, Some(TimeInForce::Ioc | TimeInForce::Fok));
        if is_ioc_or_fok {
            return TerminalReason::MKT_IOC_CANCELED;
        }

        let is_gtx_or_post_only = tif_str.eq_ignore_ascii_case("GTX")
            || tif_str.eq_ignore_ascii_case("RPI")
            || post_only
            || matches!(order_tif, Some(TimeInForce::PostOnly | TimeInForce::Rpi));
        if is_gtx_or_post_only
            && matches!(
                market,
                BinanceVenueMarket::Spot | BinanceVenueMarket::Coinm | BinanceVenueMarket::Options
            )
        {
            return TerminalReason::MKT_REJECT_POST_ONLY;
        }

        return match market {
            BinanceVenueMarket::Usdm | BinanceVenueMarket::Coinm => {
                TerminalReason::MKT_REDUCE_ONLY_CANCELED
            }
            BinanceVenueMarket::Spot | BinanceVenueMarket::Options => TerminalReason::MKT_CANCELED,
        };
    }

    if pending_cxl > 0 {
        if admin_cancel {
            TerminalReason::ADMIN_ACTION
        } else {
            TerminalReason::NO_REASON
        }
    } else {
        TerminalReason::MKT_CANCELED
    }
}

#[inline]
fn contains_ignore_case(haystack: &str, needle: &str) -> bool {
    let h = haystack.as_bytes();
    let n = needle.as_bytes();
    !n.is_empty() && h.len() >= n.len() && h.windows(n.len()).any(|w| w.eq_ignore_ascii_case(n))
}
