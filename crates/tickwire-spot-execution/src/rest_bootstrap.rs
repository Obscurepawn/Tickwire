//! Stateless cold-startup Exchange REST bootstrap (`UtaBootstrapDecoder`) and off-hot-path
//! `AsyncRestQuerier` / `OrsQueryDesk` `StatusAsk` reconciliation builders (`UtaRestRequestBuilder`)
//! for Bybit V5 and OKX V5 Unified Trading Accounts.
//!
//! Cold boot and restart recover open-order holds, derivative/option positions, `OrsStpBook`
//! entries, strategy attribution, and per-strategy `DopState::next_order_seqnum` watermarks
//! solely from authenticated Exchange REST snapshots (`Account + Positions + OpenOrders`) paired
//! with 32-character `[0-9a-z]` `SelfDescribingClientId` (`tw1...`) tokens — with zero local WAL,
//! journal, or disk state.

use crate::{
    BybitCategory, Credentials, Environment, OkxInstType, SpotVenue, UtaVenueError, client_ok,
    json_parser::{parse_positive_step_i64, parse_signed_scaled_i128},
    network::OwnedCredentials,
    symbol_ok, trade_id_inline,
};
use serde_json::Value;
use std::sync::{Arc, Mutex};
use tickwire_account::{
    InlineAscii, OrderManager, OrsBootstrapSummary, OrsExchangeBootstrapSnapshot, OrsFeeSpec,
    OrsFillObservation, OrsOrderQuerySnapshot, OrsQueriedOrderStatus, OrsRestBalanceRow,
    OrsRestOpenOrderRow, OrsRestPositionRow, OrsSymbolSpec,
};
use tickwire_transport::rest_querier::{BuiltRequest, RestRequestBuilder, RestTask, query_kind};
use tickwire_venue::{AssetKey, Liquidity, Product, Side, TimeInForce};

/// Static symbol binding mapping an ORS `symbol_id` to its venue string (`"BTCUSDT"`, `"BTC-USDT-SWAP"`,
/// `"BTC-USD-261225-90000-C"`) and fixed-point [`OrsSymbolSpec`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UtaSymbolBinding {
    pub symbol_id: u32,
    pub symbol: InlineAscii<64>,
    pub spec: OrsSymbolSpec,
}

impl UtaSymbolBinding {
    pub fn new(
        symbol_id: u32,
        symbol: &str,
        mut spec: OrsSymbolSpec,
    ) -> Result<Self, UtaVenueError> {
        if !symbol_ok(symbol) {
            return Err(UtaVenueError::Encoding);
        }
        spec.symbol_id = symbol_id;
        spec.validate().map_err(UtaVenueError::Refusal)?;
        let inline = InlineAscii::try_from_str(symbol).ok_or(UtaVenueError::Bounds)?;
        Ok(Self {
            symbol_id,
            symbol: inline,
            spec,
        })
    }
}

/// Static asset binding mapping a venue currency code (`"USDT"`, `"USDC"`, `"BTC"`) to its
/// `(wallet_id, asset_id)` [`AssetKey`] and fixed-point decimal `scale`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UtaAssetBinding {
    pub currency: InlineAscii<16>,
    pub key: AssetKey,
    pub scale: u8,
}

impl UtaAssetBinding {
    pub fn new(currency: &str, key: AssetKey, scale: u8) -> Result<Self, UtaVenueError> {
        if currency.is_empty() || currency.len() > 16 || scale > 18 {
            return Err(UtaVenueError::Encoding);
        }
        let inline = InlineAscii::try_from_str(currency).ok_or(UtaVenueError::Bounds)?;
        Ok(Self {
            currency: inline,
            key,
            scale,
        })
    }
}

/// Per-order target metadata registered before dispatching a `RestTask::status_ask` on the cold
/// `AsyncRestQuerier` worker thread.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StatusAskTarget {
    pub ors_order_id: u64,
    pub account_ref: u8,
    pub symbol_id: u32,
    pub product: Product,
    pub symbol: InlineAscii<64>,
    pub client_id: InlineAscii<64>,
    pub venue_order_id: InlineAscii<64>,
}

/// Off-hot-path registry shared between `UtaAccountSession` (`OrsQueryDesk` dispatcher) and the
/// `housekeeping_cpus`-pinned `AsyncRestQuerier` [`UtaRestRequestBuilder`].
#[derive(Debug, Default)]
pub struct StatusAskRegistry {
    entries: Mutex<Vec<StatusAskTarget>>,
}

impl StatusAskRegistry {
    #[inline]
    pub fn new() -> Self {
        Self {
            entries: Mutex::new(Vec::with_capacity(64)),
        }
    }

    pub fn upsert(&self, target: StatusAskTarget) {
        if let Ok(mut guard) = self.entries.lock() {
            if let Some(existing) = guard
                .iter_mut()
                .find(|e| e.ors_order_id == target.ors_order_id)
            {
                *existing = target;
            } else {
                guard.push(target);
            }
        }
    }

    pub fn get(&self, ors_order_id: u64) -> Option<StatusAskTarget> {
        let guard = self.entries.lock().ok()?;
        guard
            .iter()
            .copied()
            .find(|e| e.ors_order_id == ors_order_id)
    }

    pub fn remove(&self, ors_order_id: u64) {
        if let Ok(mut guard) = self.entries.lock()
            && let Some(pos) = guard.iter().position(|e| e.ors_order_id == ors_order_id)
        {
            guard.swap_remove(pos);
        }
    }
}

/// Builds the Bybit V5 or OKX V5 wallet-balance REST `GET` path for UTA cold bootstrap.
pub fn wallet_balance_path(venue: SpotVenue) -> &'static str {
    match venue {
        SpotVenue::Bybit => "/v5/account/wallet-balance?accountType=UNIFIED",
        SpotVenue::Okx => "/api/v5/account/balance",
    }
}

/// Builds a Bybit V5 derivative/option position-list `GET` path (`category = linear | inverse | option`,
/// plus optional `settleCoin` / `symbol` / `cursor`).
pub fn bybit_positions_path(
    category: BybitCategory,
    settle_coin: Option<&str>,
    symbol: Option<&str>,
    cursor: Option<&str>,
) -> Result<String, UtaVenueError> {
    if category == BybitCategory::Spot {
        return Err(UtaVenueError::Encoding);
    }
    let mut path = format!("/v5/position/list?category={}&limit=200", category.as_str());
    if let Some(sym) = symbol {
        if !symbol_ok(sym) {
            return Err(UtaVenueError::Encoding);
        }
        path.push_str("&symbol=");
        path.push_str(sym);
    } else if let Some(coin) = settle_coin {
        if !symbol_ok(coin) {
            return Err(UtaVenueError::Encoding);
        }
        path.push_str("&settleCoin=");
        path.push_str(coin);
    }
    if let Some(cur) = cursor
        && !cur.is_empty()
    {
        path.push_str("&cursor=");
        path.push_str(cur);
    }
    Ok(path)
}

/// Builds an OKX V5 positions `GET` path (`/api/v5/account/positions`, optional `instType`).
pub fn okx_positions_path(inst_type: Option<OkxInstType>) -> String {
    match inst_type {
        Some(it) if it != OkxInstType::Spot => {
            format!("/api/v5/account/positions?instType={}", it.as_str())
        }
        _ => "/api/v5/account/positions".to_owned(),
    }
}

/// Builds a Bybit V5 open-orders `GET` path (`/v5/order/realtime`) for any UTA `category`.
pub fn bybit_open_orders_path(
    category: BybitCategory,
    settle_coin: Option<&str>,
    base_coin: Option<&str>,
    symbol: Option<&str>,
    cursor: Option<&str>,
) -> Result<String, UtaVenueError> {
    let mut path = format!("/v5/order/realtime?category={}&limit=50", category.as_str());
    if let Some(sym) = symbol {
        if !symbol_ok(sym) {
            return Err(UtaVenueError::Encoding);
        }
        path.push_str("&symbol=");
        path.push_str(sym);
    } else {
        if let Some(sc) = settle_coin {
            if !symbol_ok(sc) {
                return Err(UtaVenueError::Encoding);
            }
            path.push_str("&settleCoin=");
            path.push_str(sc);
        }
        if let Some(bc) = base_coin {
            if !symbol_ok(bc) {
                return Err(UtaVenueError::Encoding);
            }
            path.push_str("&baseCoin=");
            path.push_str(bc);
        }
    }
    if let Some(cur) = cursor
        && !cur.is_empty()
    {
        path.push_str("&cursor=");
        path.push_str(cur);
    }
    Ok(path)
}

/// Builds an OKX V5 open-orders `GET` path (`/api/v5/trade/orders-pending`) for `inst_type`.
pub fn okx_open_orders_path(
    inst_type: OkxInstType,
    after_ord_id: Option<&str>,
) -> Result<String, UtaVenueError> {
    let mut path = format!(
        "/api/v5/trade/orders-pending?instType={}&limit=100",
        inst_type.as_str()
    );
    if let Some(after) = after_ord_id
        && !after.is_empty()
    {
        if !symbol_ok(after) {
            return Err(UtaVenueError::Encoding);
        }
        path.push_str("&after=");
        path.push_str(after);
    }
    Ok(path)
}

/// Builds a single-order `StatusAsk` REST `GET` path for `target`.
pub fn status_ask_order_path(
    venue: SpotVenue,
    target: &StatusAskTarget,
) -> Result<String, UtaVenueError> {
    let sym = target.symbol.as_str();
    if !symbol_ok(sym) {
        return Err(UtaVenueError::Encoding);
    }
    let cid = target.client_id.as_str();
    let vid = target.venue_order_id.as_str();
    if cid.is_empty() && vid.is_empty() {
        return Err(UtaVenueError::Encoding);
    }
    match venue {
        SpotVenue::Bybit => {
            let cat = BybitCategory::from_product(target.product).ok_or(UtaVenueError::Encoding)?;
            let mut path = format!("/v5/order/realtime?category={}&symbol={sym}", cat.as_str());
            if !vid.is_empty() {
                path.push_str("&orderId=");
                path.push_str(vid);
            } else {
                if !client_ok(SpotVenue::Bybit, cid) {
                    return Err(UtaVenueError::Encoding);
                }
                path.push_str("&orderLinkId=");
                path.push_str(cid);
            }
            Ok(path)
        }
        SpotVenue::Okx => {
            let mut path = format!("/api/v5/trade/order?instId={sym}");
            if !vid.is_empty() {
                path.push_str("&ordId=");
                path.push_str(vid);
            } else {
                if !client_ok(SpotVenue::Okx, cid) {
                    return Err(UtaVenueError::Encoding);
                }
                path.push_str("&clOrdId=");
                path.push_str(cid);
            }
            Ok(path)
        }
    }
}

/// Signs a REST `GET` path into [`BuiltRequest`] for `AsyncRestQuerier`
/// (`AsyncRestQuerier` injects `Host` automatically).
pub fn sign_get_built_request(
    credentials: &Credentials<'_>,
    environment: Environment,
    utc_ms: u64,
    path: String,
) -> Result<BuiltRequest, UtaVenueError> {
    let headers = crate::sign_headers(credentials, environment, utc_ms, "GET", &path, &[])
        .map_err(UtaVenueError::Protocol)?;
    Ok(BuiltRequest {
        method: String::from("GET"),
        path,
        headers,
        body: Vec::new(),
    })
}

/// [`RestRequestBuilder`] implementation driving `AsyncRestQuerier` off the hot path for Bybit V5
/// and OKX V5 `ACCOUNT_SNAPSHOT`, `OPEN_ORDERS_SNAPSHOT`, and `OrsQueryDesk` `STATUS_ASK` tasks.
///
/// For `OPEN_ORDERS_SNAPSHOT`, `task.key & 0x03` selects the Bybit `category` / OKX `instType` index
/// (`0 = Spot`, `1 = Linear/Swap`, `2 = Inverse/Futures`, `3 = Option`).
pub struct UtaRestRequestBuilder {
    credentials: OwnedCredentials,
    environment: Environment,
    registry: Arc<StatusAskRegistry>,
    clock_utc_ms: Box<dyn FnMut() -> u64 + Send>,
}

impl UtaRestRequestBuilder {
    pub fn new(
        credentials: OwnedCredentials,
        environment: Environment,
        registry: Arc<StatusAskRegistry>,
        clock_utc_ms: impl FnMut() -> u64 + Send + 'static,
    ) -> Self {
        Self {
            credentials,
            environment,
            registry,
            clock_utc_ms: Box::new(clock_utc_ms),
        }
    }
}

impl RestRequestBuilder for UtaRestRequestBuilder {
    fn build_request(
        &mut self,
        task: &RestTask,
        _host: &str,
        _scratch: &mut Vec<u8>,
    ) -> tickwire_transport::Result<Option<BuiltRequest>> {
        let utc_ms = (self.clock_utc_ms)();
        if utc_ms == 0 {
            return Ok(None);
        }
        let creds = self.credentials.credentials();
        let venue = creds.venue();
        let path = match task.kind {
            query_kind::ACCOUNT_SNAPSHOT => wallet_balance_path(venue).to_owned(),
            query_kind::OPEN_ORDERS_SNAPSHOT => {
                let idx = (task.key & 0x03) as usize;
                match venue {
                    SpotVenue::Bybit => {
                        let cat = BybitCategory::ALL[idx];
                        let settle = match cat {
                            BybitCategory::Linear => Some("USDT"),
                            _ => None,
                        };
                        bybit_open_orders_path(cat, settle, None, None, None)
                            .map_err(|_| tickwire_transport::Error::Protocol("open-orders path"))?
                    }
                    SpotVenue::Okx => {
                        let it = OkxInstType::ALL[idx];
                        okx_open_orders_path(it, None)
                            .map_err(|_| tickwire_transport::Error::Protocol("open-orders path"))?
                    }
                }
            }
            query_kind::STATUS_ASK => {
                let Some(target) = self.registry.get(task.key) else {
                    return Ok(None);
                };
                status_ask_order_path(venue, &target)
                    .map_err(|_| tickwire_transport::Error::Protocol("status-ask path"))?
            }
            _ => return Ok(None),
        };
        let built = sign_get_built_request(&creds, self.environment, utc_ms, path)
            .map_err(|_| tickwire_transport::Error::Protocol("sign rest request"))?;
        Ok(Some(built))
    }
}

/// Stateless cold-startup Exchange REST snapshot normalizer for Bybit V5 and OKX V5 UTA accounts.
#[derive(Clone, Debug)]
pub struct UtaBootstrapDecoder {
    venue: SpotVenue,
    symbols: Vec<UtaSymbolBinding>,
    assets: Vec<UtaAssetBinding>,
    balances: Vec<OrsRestBalanceRow>,
    positions: Vec<OrsRestPositionRow>,
    open_orders: Vec<OrsRestOpenOrderRow>,
    balances_include_open_order_locks: bool,
}

impl UtaBootstrapDecoder {
    pub fn new(
        venue: SpotVenue,
        symbols: Vec<UtaSymbolBinding>,
        assets: Vec<UtaAssetBinding>,
    ) -> Self {
        Self {
            venue,
            symbols,
            assets,
            balances: Vec::new(),
            positions: Vec::new(),
            open_orders: Vec::new(),
            balances_include_open_order_locks: true,
        }
    }

    pub fn with_balances_include_open_order_locks(mut self, include: bool) -> Self {
        self.balances_include_open_order_locks = include;
        self
    }

    #[inline]
    fn find_symbol(&self, raw_sym: &str) -> Option<&UtaSymbolBinding> {
        self.symbols.iter().find(|s| s.symbol.as_str() == raw_sym)
    }

    #[inline]
    fn find_asset(&self, raw_ccy: &str) -> Option<&UtaAssetBinding> {
        self.assets
            .iter()
            .find(|a| a.currency.as_str().eq_ignore_ascii_case(raw_ccy))
    }

    fn upsert_balance(&mut self, row: OrsRestBalanceRow) {
        if let Some(existing) = self.balances.iter_mut().find(|b| b.key == row.key) {
            *existing = row;
        } else {
            self.balances.push(row);
        }
    }

    fn accumulate_position(
        &mut self,
        symbol_id: u32,
        signed_lots: i64,
    ) -> Result<(), UtaVenueError> {
        if let Some(existing) = self.positions.iter_mut().find(|p| p.symbol_id == symbol_id) {
            existing.signed_lots = existing
                .signed_lots
                .checked_add(signed_lots)
                .ok_or(UtaVenueError::Encoding)?;
        } else {
            self.positions.push(OrsRestPositionRow {
                symbol_id,
                signed_lots,
            });
        }
        Ok(())
    }

    ///Ingests a Bybit V5 `/v5/account/wallet-balance?accountType=UNIFIED` or OKX V5
    /// `/api/v5/account/balance` JSON response page using exact fixed-point integer scaling.
    pub fn ingest_wallet_balance_json(&mut self, body: &[u8]) -> Result<usize, UtaVenueError> {
        let root: Value = serde_json::from_slice(body)
            .map_err(|_| UtaVenueError::Protocol("invalid wallet JSON"))?;
        let mut count = 0usize;
        match self.venue {
            SpotVenue::Bybit => {
                if root["retCode"].as_i64() != Some(0) {
                    return Err(UtaVenueError::Protocol(
                        "Bybit wallet-balance non-zero retCode",
                    ));
                }
                let Some(accounts) = root["result"]["list"].as_array() else {
                    return Err(UtaVenueError::Protocol("missing Bybit wallet list"));
                };
                for acct in accounts {
                    let Some(coins) = acct["coin"].as_array() else {
                        continue;
                    };
                    for coin_obj in coins {
                        let Some(ccy) = coin_obj["coin"].as_str() else {
                            continue;
                        };
                        let Some(asset) = self.find_asset(ccy).copied() else {
                            continue;
                        };
                        let wb = coin_obj["walletBalance"]
                            .as_str()
                            .filter(|s| !s.is_empty())
                            .unwrap_or("0");
                        let locked = coin_obj["locked"]
                            .as_str()
                            .filter(|s| !s.is_empty())
                            .unwrap_or("0");
                        let total_order_im = coin_obj["totalOrderIM"]
                            .as_str()
                            .filter(|s| !s.is_empty())
                            .unwrap_or("0");
                        let wallet_atoms = parse_signed_scaled_i128(wb.as_bytes(), asset.scale)
                            .ok_or(UtaVenueError::Encoding)?;
                        let spot_locked_atoms =
                            parse_signed_scaled_i128(locked.as_bytes(), asset.scale)
                                .ok_or(UtaVenueError::Encoding)?;
                        let order_im_atoms =
                            parse_signed_scaled_i128(total_order_im.as_bytes(), asset.scale)
                                .ok_or(UtaVenueError::Encoding)?;
                        let locked_atoms = spot_locked_atoms
                            .checked_add(order_im_atoms)
                            .ok_or(UtaVenueError::Encoding)?
                            .max(0);
                        let free_atoms = wallet_atoms.saturating_sub(locked_atoms).max(0);
                        self.upsert_balance(OrsRestBalanceRow {
                            key: asset.key,
                            scale: asset.scale,
                            free_atoms,
                            locked_atoms,
                        });
                        count += 1;
                    }
                }
            }
            SpotVenue::Okx => {
                if root["code"].as_str() != Some("0") {
                    return Err(UtaVenueError::Protocol("OKX account/balance non-zero code"));
                }
                let Some(data) = root["data"].as_array() else {
                    return Err(UtaVenueError::Protocol("missing OKX balance data"));
                };
                for acct in data {
                    let Some(details) = acct["details"].as_array() else {
                        continue;
                    };
                    for det in details {
                        let Some(ccy) = det["ccy"].as_str() else {
                            continue;
                        };
                        let Some(asset) = self.find_asset(ccy).copied() else {
                            continue;
                        };
                        let avail_raw = det["availBal"]
                            .as_str()
                            .filter(|s| !s.is_empty())
                            .or_else(|| det["availEq"].as_str().filter(|s| !s.is_empty()))
                            .unwrap_or("0");
                        let frozen_raw = det["ordFrozen"]
                            .as_str()
                            .filter(|s| !s.is_empty())
                            .or_else(|| det["frozenBal"].as_str().filter(|s| !s.is_empty()))
                            .unwrap_or("0");
                        let free_atoms =
                            parse_signed_scaled_i128(avail_raw.as_bytes(), asset.scale)
                                .ok_or(UtaVenueError::Encoding)?
                                .max(0);
                        let locked_atoms =
                            parse_signed_scaled_i128(frozen_raw.as_bytes(), asset.scale)
                                .ok_or(UtaVenueError::Encoding)?
                                .max(0);
                        self.upsert_balance(OrsRestBalanceRow {
                            key: asset.key,
                            scale: asset.scale,
                            free_atoms,
                            locked_atoms,
                        });
                        count += 1;
                    }
                }
            }
        }
        Ok(count)
    }

    /// Ingests a Bybit V5 `/v5/position/list` or OKX V5 `/api/v5/account/positions` JSON page.
    pub fn ingest_positions_json(&mut self, body: &[u8]) -> Result<usize, UtaVenueError> {
        let root: Value = serde_json::from_slice(body)
            .map_err(|_| UtaVenueError::Protocol("invalid positions JSON"))?;
        let mut count = 0usize;
        match self.venue {
            SpotVenue::Bybit => {
                if root["retCode"].as_i64() != Some(0) {
                    return Err(UtaVenueError::Protocol(
                        "Bybit position/list non-zero retCode",
                    ));
                }
                let Some(rows) = root["result"]["list"].as_array() else {
                    return Err(UtaVenueError::Protocol("missing Bybit position list"));
                };
                for row in rows {
                    let Some(sym_str) = row["symbol"].as_str() else {
                        continue;
                    };
                    let Some(binding) = self.find_symbol(sym_str).copied() else {
                        continue;
                    };
                    let size_raw = row["size"]
                        .as_str()
                        .filter(|s| !s.is_empty())
                        .unwrap_or("0");
                    let abs_lots = parse_positive_step_i64(
                        size_raw.as_bytes(),
                        binding.spec.qty_scale,
                        binding.spec.qty_step_atoms,
                    )
                    .ok_or(UtaVenueError::Encoding)?;
                    if abs_lots == 0 {
                        continue;
                    }
                    let side_str = row["side"].as_str().unwrap_or("");
                    let signed_lots = match side_str {
                        "Buy" => abs_lots,
                        "Sell" => -abs_lots,
                        _ => 0,
                    };
                    self.accumulate_position(binding.symbol_id, signed_lots)?;
                    count += 1;
                }
            }
            SpotVenue::Okx => {
                if root["code"].as_str() != Some("0") {
                    return Err(UtaVenueError::Protocol(
                        "OKX account/positions non-zero code",
                    ));
                }
                let Some(rows) = root["data"].as_array() else {
                    return Err(UtaVenueError::Protocol("missing OKX positions data"));
                };
                for row in rows {
                    let Some(sym_str) = row["instId"].as_str() else {
                        continue;
                    };
                    let Some(binding) = self.find_symbol(sym_str).copied() else {
                        continue;
                    };
                    let pos_raw = row["pos"].as_str().filter(|s| !s.is_empty()).unwrap_or("0");
                    let pos_atoms =
                        parse_signed_scaled_i128(pos_raw.as_bytes(), binding.spec.qty_scale)
                            .ok_or(UtaVenueError::Encoding)?;
                    if pos_atoms % binding.spec.qty_step_atoms != 0 {
                        return Err(UtaVenueError::Encoding);
                    }
                    let mut signed_lots = i64::try_from(pos_atoms / binding.spec.qty_step_atoms)
                        .map_err(|_| UtaVenueError::Encoding)?;
                    if signed_lots == 0 {
                        continue;
                    }
                    if row["posSide"].as_str() == Some("short") && signed_lots > 0 {
                        signed_lots = -signed_lots;
                    }
                    self.accumulate_position(binding.symbol_id, signed_lots)?;
                    count += 1;
                }
            }
        }
        Ok(count)
    }

    /// Ingests a Bybit V5 `/v5/order/realtime` or OKX V5 `/api/v5/trade/orders-pending` JSON page
    /// across any UTA category/instType (`Spot`, `Linear`, `Inverse`, `Swap`, `Futures`, `Option`).
    ///
    /// Returns `(ingested_orders, next_cursor_or_after_id)`.
    pub fn ingest_open_orders_json(
        &mut self,
        body: &[u8],
        created_mono_ns: u64,
    ) -> Result<(usize, Option<String>), UtaVenueError> {
        let root: Value = serde_json::from_slice(body)
            .map_err(|_| UtaVenueError::Protocol("invalid open-orders JSON"))?;
        let mut count = 0usize;
        match self.venue {
            SpotVenue::Bybit => {
                if root["retCode"].as_i64() != Some(0) {
                    return Err(UtaVenueError::Protocol(
                        "Bybit order/realtime non-zero retCode",
                    ));
                }
                let next_cursor = root["result"]["nextPageCursor"]
                    .as_str()
                    .filter(|s| !s.is_empty())
                    .map(str::to_owned);
                let Some(rows) = root["result"]["list"].as_array() else {
                    return Err(UtaVenueError::Protocol("missing Bybit open-order list"));
                };
                for row in rows {
                    let Some(sym_str) = row["symbol"].as_str() else {
                        continue;
                    };
                    let Some(binding) = self.find_symbol(sym_str).copied() else {
                        continue;
                    };
                    let side = match row["side"].as_str() {
                        Some("Buy") => Side::Buy,
                        Some("Sell") => Side::Sell,
                        _ => return Err(UtaVenueError::Protocol("invalid Bybit open-order side")),
                    };
                    let tif_raw = row["timeInForce"].as_str().unwrap_or("GTC");
                    let tif = match tif_raw {
                        "GTC" => TimeInForce::Gtc,
                        "IOC" => TimeInForce::Ioc,
                        "FOK" => TimeInForce::Fok,
                        "PostOnly" => TimeInForce::PostOnly,
                        "RPI" => TimeInForce::Rpi,
                        _ => TimeInForce::Gtc,
                    };
                    let post_only = matches!(tif, TimeInForce::PostOnly);
                    let reduce_only = row["reduceOnly"].as_bool().unwrap_or(false);
                    let px_str = row["price"].as_str().unwrap_or("0");
                    let qty_str = row["qty"].as_str().unwrap_or("0");
                    let cum_str = row["cumExecQty"]
                        .as_str()
                        .filter(|s| !s.is_empty())
                        .unwrap_or("0");
                    let price_ticks = parse_positive_step_i64(
                        px_str.as_bytes(),
                        binding.spec.price_scale,
                        binding.spec.price_tick_atoms,
                    )
                    .ok_or(UtaVenueError::Encoding)?;
                    let orig_qty_lots = parse_positive_step_i64(
                        qty_str.as_bytes(),
                        binding.spec.qty_scale,
                        binding.spec.qty_step_atoms,
                    )
                    .ok_or(UtaVenueError::Encoding)?;
                    let cum_filled_lots = parse_positive_step_i64(
                        cum_str.as_bytes(),
                        binding.spec.qty_scale,
                        binding.spec.qty_step_atoms,
                    )
                    .ok_or(UtaVenueError::Encoding)?;
                    let client_id = row["orderLinkId"].as_str().unwrap_or("").to_owned();
                    let venue_order_id = row["orderId"].as_str().unwrap_or("").to_owned();
                    self.open_orders.push(OrsRestOpenOrderRow {
                        symbol_id: binding.symbol_id,
                        client_id,
                        venue_order_id,
                        side,
                        tif,
                        post_only,
                        reduce_only,
                        price_ticks,
                        orig_qty_lots,
                        cum_filled_lots,
                        created_mono_ns,
                    });
                    count += 1;
                }
                Ok((count, next_cursor))
            }
            SpotVenue::Okx => {
                if root["code"].as_str() != Some("0") {
                    return Err(UtaVenueError::Protocol("OKX orders-pending non-zero code"));
                }
                let Some(rows) = root["data"].as_array() else {
                    return Err(UtaVenueError::Protocol("missing OKX open-order data"));
                };
                let mut last_ord_id: Option<String> = None;
                for row in rows {
                    let Some(sym_str) = row["instId"].as_str() else {
                        continue;
                    };
                    let Some(binding) = self.find_symbol(sym_str).copied() else {
                        continue;
                    };
                    let side = match row["side"].as_str() {
                        Some("buy") => Side::Buy,
                        Some("sell") => Side::Sell,
                        _ => return Err(UtaVenueError::Protocol("invalid OKX open-order side")),
                    };
                    let ord_type = row["ordType"].as_str().unwrap_or("limit");
                    let tif = match ord_type {
                        "limit" => TimeInForce::Gtc,
                        "ioc" | "optimal_limit_ioc" | "market" => TimeInForce::Ioc,
                        "fok" => TimeInForce::Fok,
                        "post_only" => TimeInForce::PostOnly,
                        "rpi" => TimeInForce::Rpi,
                        _ => TimeInForce::Gtc,
                    };
                    let post_only = ord_type == "post_only";
                    let reduce_only = match &row["reduceOnly"] {
                        Value::Bool(b) => *b,
                        Value::String(s) => s.eq_ignore_ascii_case("true"),
                        _ => false,
                    };
                    let px_str = row["px"].as_str().unwrap_or("0");
                    let qty_str = row["sz"].as_str().unwrap_or("0");
                    let cum_str = row["accFillSz"]
                        .as_str()
                        .filter(|s| !s.is_empty())
                        .unwrap_or("0");
                    let price_ticks = parse_positive_step_i64(
                        px_str.as_bytes(),
                        binding.spec.price_scale,
                        binding.spec.price_tick_atoms,
                    )
                    .ok_or(UtaVenueError::Encoding)?;
                    let orig_qty_lots = parse_positive_step_i64(
                        qty_str.as_bytes(),
                        binding.spec.qty_scale,
                        binding.spec.qty_step_atoms,
                    )
                    .ok_or(UtaVenueError::Encoding)?;
                    let cum_filled_lots = parse_positive_step_i64(
                        cum_str.as_bytes(),
                        binding.spec.qty_scale,
                        binding.spec.qty_step_atoms,
                    )
                    .ok_or(UtaVenueError::Encoding)?;
                    let client_id = row["clOrdId"].as_str().unwrap_or("").to_owned();
                    let venue_order_id = row["ordId"].as_str().unwrap_or("").to_owned();
                    if !venue_order_id.is_empty() {
                        last_ord_id = Some(venue_order_id.clone());
                    }
                    self.open_orders.push(OrsRestOpenOrderRow {
                        symbol_id: binding.symbol_id,
                        client_id,
                        venue_order_id,
                        side,
                        tif,
                        post_only,
                        reduce_only,
                        price_ticks,
                        orig_qty_lots,
                        cum_filled_lots,
                        created_mono_ns,
                    });
                    count += 1;
                }
                let next = if rows.len() >= 100 { last_ord_id } else { None };
                Ok((count, next))
            }
        }
    }

    /// Consumes accumulated REST pages into an [`OrsExchangeBootstrapSnapshot`].
    pub fn build_snapshot(self) -> OrsExchangeBootstrapSnapshot {
        OrsExchangeBootstrapSnapshot {
            balances: self.balances,
            positions: self.positions,
            open_orders: self.open_orders,
            balances_include_open_order_locks: self.balances_include_open_order_locks,
        }
    }

    /// Consumes accumulated REST pages and executes [`OrderManager::bootstrap_from_exchange`] for
    /// `account_ref`, reconstructing open-order holds, `OrsStpBook` entries, strategy attribution,
    /// and `DopState::next_order_seqnum` watermarks via `tw1...` `SelfDescribingClientId`.
    pub fn bootstrap_order_manager<const CHECK_STP: bool>(
        self,
        order_manager: &OrderManager<CHECK_STP>,
        account_ref: u8,
    ) -> Result<OrsBootstrapSummary, UtaVenueError> {
        let snapshot = self.build_snapshot();
        order_manager
            .bootstrap_from_exchange(account_ref, &snapshot)
            .map_err(UtaVenueError::Refusal)
    }
}

/// Owned `StatusAsk` decode output borrowing cleanly into [`OrsOrderQuerySnapshot`] for
/// [`OrderManager::reconcile_query_snapshot`].
#[derive(Clone, Debug)]
pub struct DecodedStatusAsk {
    pub ors_order_id: u64,
    pub account_ref: u8,
    pub symbol_id: u32,
    pub status: OrsQueriedOrderStatus,
    pub venue_order_id: Option<String>,
    pub price_ticks: Option<i64>,
    pub orig_qty_lots: Option<i64>,
    pub cum_filled_lots: i64,
    pub avg_fill_price_ticks: Option<i64>,
    pub trades: Vec<OrsFillObservation>,
    pub received_mono_ns: u64,
}

impl DecodedStatusAsk {
    #[inline]
    pub fn as_query_snapshot(&self) -> OrsOrderQuerySnapshot<'_> {
        OrsOrderQuerySnapshot {
            ors_order_id: self.ors_order_id,
            account_ref: self.account_ref,
            symbol_id: self.symbol_id,
            status: self.status,
            venue_order_id: self.venue_order_id.as_deref(),
            price_ticks: self.price_ticks,
            orig_qty_lots: self.orig_qty_lots,
            cum_filled_lots: self.cum_filled_lots,
            avg_fill_price_ticks: self.avg_fill_price_ticks,
            trades: &self.trades,
            received_mono_ns: self.received_mono_ns,
        }
    }
}

/// Decodes a Bybit V5 (`/v5/order/realtime` or `/v5/order/history` + optional `/v5/execution/list`)
/// or OKX V5 (`/api/v5/trade/order` + optional `/api/v5/trade/fills`) REST `StatusAsk` reply into
/// [`DecodedStatusAsk`] with exact integer scaling and signed fee normalization.
pub fn decode_status_ask_snapshot(
    venue: SpotVenue,
    target: &StatusAskTarget,
    spec: &OrsSymbolSpec,
    assets: &[UtaAssetBinding],
    order_body: &[u8],
    fills_body: Option<&[u8]>,
    received_mono_ns: u64,
) -> Result<DecodedStatusAsk, UtaVenueError> {
    let root: Value = serde_json::from_slice(order_body)
        .map_err(|_| UtaVenueError::Protocol("invalid StatusAsk order JSON"))?;
    let mut trades = Vec::new();

    if let Some(fb) = fills_body {
        let froot: Value = serde_json::from_slice(fb)
            .map_err(|_| UtaVenueError::Protocol("invalid StatusAsk fills JSON"))?;
        match venue {
            SpotVenue::Bybit => {
                if froot["retCode"].as_i64() == Some(0)
                    && let Some(frows) = froot["result"]["list"].as_array()
                {
                    for frow in frows {
                        let Some(exec_id) = frow["execId"].as_str().filter(|s| !s.is_empty())
                        else {
                            continue;
                        };
                        let Some((tid_inline, exec_ref)) =
                            trade_id_inline(SpotVenue::Bybit, exec_id)
                        else {
                            continue;
                        };
                        let side = match frow["side"].as_str() {
                            Some("Buy") => Side::Buy,
                            Some("Sell") => Side::Sell,
                            _ => continue,
                        };
                        let liq = match frow["isMaker"].as_bool() {
                            Some(true) => Liquidity::Maker,
                            Some(false) => Liquidity::Taker,
                            None => Liquidity::Unknown,
                        };
                        let Some(px_ticks) = frow["execPrice"].as_str().and_then(|s| {
                            parse_positive_step_i64(
                                s.as_bytes(),
                                spec.price_scale,
                                spec.price_tick_atoms,
                            )
                        }) else {
                            continue;
                        };
                        let Some(qty_lots) = frow["execQty"].as_str().and_then(|s| {
                            parse_positive_step_i64(
                                s.as_bytes(),
                                spec.qty_scale,
                                spec.qty_step_atoms,
                            )
                        }) else {
                            continue;
                        };
                        let fee = parse_rest_fee(
                            SpotVenue::Bybit,
                            assets,
                            frow["feeCurrency"].as_str(),
                            frow["execFee"].as_str(),
                        );
                        trades.push(OrsFillObservation {
                            ors_order_id: target.ors_order_id,
                            account_ref: target.account_ref,
                            symbol_id: target.symbol_id,
                            side,
                            liquidity: liq,
                            exec_ref,
                            trade_id: tid_inline,
                            qty_lots,
                            price_ticks: px_ticks,
                            fee,
                            received_mono_ns,
                        });
                    }
                }
            }
            SpotVenue::Okx => {
                if froot["code"].as_str() == Some("0")
                    && let Some(frows) = froot["data"].as_array()
                {
                    for frow in frows {
                        let Some(trade_id_raw) = frow["tradeId"]
                            .as_str()
                            .filter(|s| !s.is_empty() && *s != "0")
                        else {
                            continue;
                        };
                        let Some((tid_inline, exec_ref)) =
                            trade_id_inline(SpotVenue::Okx, trade_id_raw)
                        else {
                            continue;
                        };
                        let side = match frow["side"].as_str() {
                            Some("buy") => Side::Buy,
                            Some("sell") => Side::Sell,
                            _ => continue,
                        };
                        let liq = match frow["execType"].as_str() {
                            Some("M") => Liquidity::Maker,
                            Some("T") => Liquidity::Taker,
                            _ => Liquidity::Unknown,
                        };
                        let Some(px_ticks) = frow["fillPx"].as_str().and_then(|s| {
                            parse_positive_step_i64(
                                s.as_bytes(),
                                spec.price_scale,
                                spec.price_tick_atoms,
                            )
                        }) else {
                            continue;
                        };
                        let Some(qty_lots) = frow["fillSz"].as_str().and_then(|s| {
                            parse_positive_step_i64(
                                s.as_bytes(),
                                spec.qty_scale,
                                spec.qty_step_atoms,
                            )
                        }) else {
                            continue;
                        };
                        let fee = parse_rest_fee(
                            SpotVenue::Okx,
                            assets,
                            frow["feeCcy"].as_str(),
                            frow["fee"].as_str(),
                        );
                        trades.push(OrsFillObservation {
                            ors_order_id: target.ors_order_id,
                            account_ref: target.account_ref,
                            symbol_id: target.symbol_id,
                            side,
                            liquidity: liq,
                            exec_ref,
                            trade_id: tid_inline,
                            qty_lots,
                            price_ticks: px_ticks,
                            fee,
                            received_mono_ns,
                        });
                    }
                }
            }
        }
    }

    match venue {
        SpotVenue::Bybit => {
            let ret_code = root["retCode"].as_i64().unwrap_or(-1);
            if matches!(ret_code, 110001 | 170145 | 170213) {
                return Ok(DecodedStatusAsk {
                    ors_order_id: target.ors_order_id,
                    account_ref: target.account_ref,
                    symbol_id: target.symbol_id,
                    status: OrsQueriedOrderStatus::NotFound,
                    venue_order_id: None,
                    price_ticks: None,
                    orig_qty_lots: None,
                    cum_filled_lots: 0,
                    avg_fill_price_ticks: None,
                    trades,
                    received_mono_ns,
                });
            }
            if ret_code != 0 {
                return Err(UtaVenueError::Protocol("Bybit StatusAsk error retCode"));
            }
            let Some(rows) = root["result"]["list"].as_array() else {
                return Err(UtaVenueError::Protocol("missing Bybit StatusAsk list"));
            };
            let Some(row) = rows.first() else {
                return Ok(DecodedStatusAsk {
                    ors_order_id: target.ors_order_id,
                    account_ref: target.account_ref,
                    symbol_id: target.symbol_id,
                    status: OrsQueriedOrderStatus::NotFound,
                    venue_order_id: None,
                    price_ticks: None,
                    orig_qty_lots: None,
                    cum_filled_lots: 0,
                    avg_fill_price_ticks: None,
                    trades,
                    received_mono_ns,
                });
            };
            let status = match row["orderStatus"].as_str().unwrap_or("") {
                "New" | "Created" | "Untriggered" => OrsQueriedOrderStatus::Open,
                "PartiallyFilled" => OrsQueriedOrderStatus::PartiallyFilled,
                "Filled" => OrsQueriedOrderStatus::Filled,
                "Cancelled" | "PartiallyFilledCanceled" | "Rejected" => {
                    OrsQueriedOrderStatus::Canceled
                }
                "Deactivated" => OrsQueriedOrderStatus::Expired,
                _ => return Err(UtaVenueError::Protocol("unrecognized Bybit orderStatus")),
            };
            let venue_order_id = row["orderId"]
                .as_str()
                .filter(|s| !s.is_empty())
                .map(str::to_owned);
            let price_ticks = row["price"].as_str().and_then(|s| {
                parse_positive_step_i64(s.as_bytes(), spec.price_scale, spec.price_tick_atoms)
            });
            let orig_qty_lots = row["qty"].as_str().and_then(|s| {
                parse_positive_step_i64(s.as_bytes(), spec.qty_scale, spec.qty_step_atoms)
            });
            let cum_filled_lots = row["cumExecQty"]
                .as_str()
                .filter(|s| !s.is_empty())
                .and_then(|s| {
                    parse_positive_step_i64(s.as_bytes(), spec.qty_scale, spec.qty_step_atoms)
                })
                .unwrap_or(0);
            let avg_fill_price_ticks = row["avgPrice"]
                .as_str()
                .filter(|s| !s.is_empty() && *s != "0")
                .and_then(|s| {
                    parse_positive_step_i64(s.as_bytes(), spec.price_scale, spec.price_tick_atoms)
                });
            Ok(DecodedStatusAsk {
                ors_order_id: target.ors_order_id,
                account_ref: target.account_ref,
                symbol_id: target.symbol_id,
                status,
                venue_order_id,
                price_ticks,
                orig_qty_lots,
                cum_filled_lots,
                avg_fill_price_ticks,
                trades,
                received_mono_ns,
            })
        }
        SpotVenue::Okx => {
            let code = root["code"].as_str().unwrap_or("");
            if matches!(code, "51603" | "51400") {
                return Ok(DecodedStatusAsk {
                    ors_order_id: target.ors_order_id,
                    account_ref: target.account_ref,
                    symbol_id: target.symbol_id,
                    status: OrsQueriedOrderStatus::NotFound,
                    venue_order_id: None,
                    price_ticks: None,
                    orig_qty_lots: None,
                    cum_filled_lots: 0,
                    avg_fill_price_ticks: None,
                    trades,
                    received_mono_ns,
                });
            }
            if code != "0" {
                return Err(UtaVenueError::Protocol("OKX StatusAsk error code"));
            }
            let Some(rows) = root["data"].as_array() else {
                return Err(UtaVenueError::Protocol("missing OKX StatusAsk data"));
            };
            let Some(row) = rows.first() else {
                return Ok(DecodedStatusAsk {
                    ors_order_id: target.ors_order_id,
                    account_ref: target.account_ref,
                    symbol_id: target.symbol_id,
                    status: OrsQueriedOrderStatus::NotFound,
                    venue_order_id: None,
                    price_ticks: None,
                    orig_qty_lots: None,
                    cum_filled_lots: 0,
                    avg_fill_price_ticks: None,
                    trades,
                    received_mono_ns,
                });
            };
            let status = match row["state"].as_str().unwrap_or("") {
                "live" => OrsQueriedOrderStatus::Open,
                "partially_filled" => OrsQueriedOrderStatus::PartiallyFilled,
                "filled" => OrsQueriedOrderStatus::Filled,
                "canceled" | "mmp_canceled" => OrsQueriedOrderStatus::Canceled,
                _ => return Err(UtaVenueError::Protocol("unrecognized OKX order state")),
            };
            let venue_order_id = row["ordId"]
                .as_str()
                .filter(|s| !s.is_empty())
                .map(str::to_owned);
            let price_ticks = row["px"].as_str().and_then(|s| {
                parse_positive_step_i64(s.as_bytes(), spec.price_scale, spec.price_tick_atoms)
            });
            let orig_qty_lots = row["sz"].as_str().and_then(|s| {
                parse_positive_step_i64(s.as_bytes(), spec.qty_scale, spec.qty_step_atoms)
            });
            let cum_filled_lots = row["accFillSz"]
                .as_str()
                .filter(|s| !s.is_empty())
                .and_then(|s| {
                    parse_positive_step_i64(s.as_bytes(), spec.qty_scale, spec.qty_step_atoms)
                })
                .unwrap_or(0);
            let avg_fill_price_ticks = row["avgPx"]
                .as_str()
                .filter(|s| !s.is_empty() && *s != "0")
                .and_then(|s| {
                    parse_positive_step_i64(s.as_bytes(), spec.price_scale, spec.price_tick_atoms)
                });
            Ok(DecodedStatusAsk {
                ors_order_id: target.ors_order_id,
                account_ref: target.account_ref,
                symbol_id: target.symbol_id,
                status,
                venue_order_id,
                price_ticks,
                orig_qty_lots,
                cum_filled_lots,
                avg_fill_price_ticks,
                trades,
                received_mono_ns,
            })
        }
    }
}

pub(crate) fn parse_rest_fee(
    venue: SpotVenue,
    assets: &[UtaAssetBinding],
    fee_ccy: Option<&str>,
    fee_raw: Option<&str>,
) -> Option<OrsFeeSpec> {
    let ccy = fee_ccy.filter(|s| !s.is_empty())?;
    let raw = fee_raw.filter(|s| !s.is_empty())?;
    let asset = assets
        .iter()
        .find(|a| a.currency.as_str().eq_ignore_ascii_case(ccy))?;
    let parsed = parse_signed_scaled_i128(raw.as_bytes(), asset.scale)?;
    // Bybit `execFee` > 0 means fee paid (debit -> negate); OKX `fee` / `fillFee` is already wallet-signed.
    let signed_atoms = match venue {
        SpotVenue::Bybit => parsed.checked_neg()?,
        SpotVenue::Okx => parsed,
    };
    Some(OrsFeeSpec::new(asset.key, signed_atoms, asset.scale))
}
