//! Stateless cold-startup Exchange REST snapshot parsers (`Account + OpenOrders`) across Spot,
//! USD-M, Coin-M, and European Options (`human-docs/8.5`, `12`, `13.9`).
//!
//! Converts authenticated Binance REST JSON payloads into [`OrsExchangeBootstrapSnapshot`] rows
//! using `JsonScan` and exact integer decimal arithmetic (`#![forbid(unsafe_code)]`, zero floats,
//! zero local WAL or disk journal).

use std::collections::BTreeMap;

use tickwire_account::{
    OrderManager, OrsBootstrapSummary, OrsExchangeBootstrapSnapshot, OrsRestBalanceRow,
    OrsRestOpenOrderRow, OrsRestPositionRow, OrsSymbolSpec,
};
use tickwire_binance::full_market::BinanceVenueMarket;
use tickwire_transport::{Error as TransportError, json_scan::JsonScan};
use tickwire_venue as v;

use crate::msg_parser::parse_step_i64;

/// Startup binding between a compact ORS `symbol_id` and a Binance market symbol.
#[derive(Clone, Debug)]
pub struct BinanceSymbolBinding {
    pub symbol_id: u32,
    pub market: BinanceVenueMarket,
    pub venue_symbol: String,
    pub spec: OrsSymbolSpec,
    /// Optional venue commission asset code override for fast hot-path fee attribution.
    pub default_fee_asset_code: Option<String>,
}

/// Startup binding between a `(market, venue_asset_code)` pair and a UTA [`v::AssetKey`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BinanceAssetBinding {
    pub market: BinanceVenueMarket,
    pub asset_code: String,
    pub key: v::AssetKey,
    pub scale: u8,
}

/// Errors produced while parsing or applying cold-startup Exchange REST snapshots.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BinanceBootstrapError {
    InvalidConfig(&'static str),
    MalformedJson,
    InvalidDecimalField(&'static str),
    OrderManagerRejected(v::RefusalReason),
}

impl From<TransportError> for BinanceBootstrapError {
    #[inline]
    fn from(_: TransportError) -> Self {
        Self::MalformedJson
    }
}

/// Cold-startup symbol and asset lookup dictionary shared across all markets of a `BinanceSession`.
#[derive(Clone, Debug)]
pub struct BinanceBootstrapCatalog {
    symbols_by_id: Box<[BinanceSymbolBinding]>,
    symbol_lookup: BTreeMap<(BinanceVenueMarket, String), u32>,
    asset_lookup: BTreeMap<(BinanceVenueMarket, String), (v::AssetKey, u8)>,
}

impl BinanceBootstrapCatalog {
    /// Builds and validates a compact symbol/asset catalog at cold startup.
    pub fn new(
        symbols: Vec<BinanceSymbolBinding>,
        assets: Vec<BinanceAssetBinding>,
    ) -> Result<Self, BinanceBootstrapError> {
        let mut symbol_lookup = BTreeMap::new();
        for (idx, binding) in symbols.iter().enumerate() {
            if binding.symbol_id as usize != idx {
                return Err(BinanceBootstrapError::InvalidConfig(
                    "BinanceSymbolBinding.symbol_id must match compact slice index",
                ));
            }
            if binding.venue_symbol.is_empty() || !binding.venue_symbol.is_ascii() {
                return Err(BinanceBootstrapError::InvalidConfig(
                    "BinanceSymbolBinding.venue_symbol must be non-empty ASCII",
                ));
            }
            if binding.spec.symbol_id != binding.symbol_id {
                return Err(BinanceBootstrapError::InvalidConfig(
                    "BinanceSymbolBinding.spec.symbol_id mismatch",
                ));
            }
            if BinanceVenueMarket::from_product(binding.spec.product) != Some(binding.market) {
                return Err(BinanceBootstrapError::InvalidConfig(
                    "BinanceSymbolBinding.spec.product must match BinanceVenueMarket",
                ));
            }
            if symbol_lookup
                .insert(
                    (binding.market, binding.venue_symbol.clone()),
                    binding.symbol_id,
                )
                .is_some()
            {
                return Err(BinanceBootstrapError::InvalidConfig(
                    "duplicate (market, venue_symbol) in BinanceBootstrapCatalog",
                ));
            }
        }

        let mut asset_lookup = BTreeMap::new();
        for asset in assets {
            if asset.asset_code.is_empty() || !asset.asset_code.is_ascii() || asset.scale > 18 {
                return Err(BinanceBootstrapError::InvalidConfig(
                    "invalid BinanceAssetBinding code or scale",
                ));
            }
            if asset_lookup
                .insert((asset.market, asset.asset_code), (asset.key, asset.scale))
                .is_some()
            {
                return Err(BinanceBootstrapError::InvalidConfig(
                    "duplicate (market, asset_code) in BinanceBootstrapCatalog",
                ));
            }
        }

        Ok(Self {
            symbols_by_id: symbols.into_boxed_slice(),
            symbol_lookup,
            asset_lookup,
        })
    }

    #[inline]
    pub fn symbol(&self, symbol_id: u32) -> Option<&BinanceSymbolBinding> {
        self.symbols_by_id.get(symbol_id as usize)
    }

    #[inline]
    pub fn symbols(&self) -> &[BinanceSymbolBinding] {
        &self.symbols_by_id
    }

    #[inline]
    pub fn find_symbol_id(&self, market: BinanceVenueMarket, venue_symbol: &str) -> Option<u32> {
        for (k, &sid) in &self.symbol_lookup {
            if k.0 == market && k.1 == venue_symbol {
                return Some(sid);
            }
        }
        None
    }

    #[inline]
    pub fn find_asset(
        &self,
        market: BinanceVenueMarket,
        asset_code: &str,
    ) -> Option<(v::AssetKey, u8)> {
        for (k, v_entry) in &self.asset_lookup {
            if k.0 == market && k.1 == asset_code {
                return Some(*v_entry);
            }
        }
        None
    }
}

#[inline]
fn pow10_i128(exp: u8) -> Option<i128> {
    if exp > 36 {
        return None;
    }
    let mut out = 1i128;
    let mut i = 0u8;
    while i < exp {
        out = out.checked_mul(10)?;
        i += 1;
    }
    Some(out)
}

/// Exact integer decimal parser (`10^scale` atoms per unit) with zero floating-point operations.
pub fn parse_scaled_i128(raw: &str, scale: u8, allow_truncate_sub_atom: bool) -> Option<i128> {
    let bytes = raw.trim().as_bytes();
    if bytes.is_empty() || scale > 18 {
        return None;
    }
    let (neg, rest) = match bytes[0] {
        b'-' => (true, &bytes[1..]),
        b'+' => (false, &bytes[1..]),
        _ => (false, bytes),
    };
    if rest.is_empty() {
        return None;
    }

    let mut whole: i128 = 0;
    let mut frac: i128 = 0;
    let mut frac_digits: u8 = 0;
    let mut seen_dot = false;
    let mut seen_digit = false;

    for &b in rest {
        if b == b'.' {
            if seen_dot {
                return None;
            }
            seen_dot = true;
            continue;
        }
        if !b.is_ascii_digit() {
            return None;
        }
        seen_digit = true;
        let digit = i128::from(b - b'0');
        if !seen_dot {
            whole = whole.checked_mul(10)?.checked_add(digit)?;
        } else if frac_digits < scale {
            frac = frac.checked_mul(10)?.checked_add(digit)?;
            frac_digits += 1;
        } else if b != b'0' && !allow_truncate_sub_atom {
            return None;
        }
    }
    if !seen_digit {
        return None;
    }
    let scale_pow = pow10_i128(scale)?;
    let rem_pow = pow10_i128(scale - frac_digits)?;
    let abs_atoms = whole
        .checked_mul(scale_pow)?
        .checked_add(frac.checked_mul(rem_pow)?)?;
    if neg {
        abs_atoms.checked_neg()
    } else {
        Some(abs_atoms)
    }
}

fn scan_object_fields<'a>(
    cursor: &mut JsonScan<'a>,
    mut on_field: impl FnMut(&'a str, &mut JsonScan<'a>) -> Result<(), BinanceBootstrapError>,
) -> Result<(), BinanceBootstrapError> {
    cursor.eat_byte(b'{')?;
    if cursor.peek() == Some(b'}') {
        cursor.eat_byte(b'}')?;
        return Ok(());
    }
    loop {
        let key = cursor.read_string_raw()?;
        cursor.eat_byte(b':')?;
        on_field(key, cursor)?;
        match cursor.peek() {
            Some(b',') => cursor.eat_byte(b',')?,
            Some(b'}') => {
                cursor.eat_byte(b'}')?;
                break;
            }
            _ => return Err(BinanceBootstrapError::MalformedJson),
        }
    }
    Ok(())
}

fn scan_array_objects<'a>(
    cursor: &mut JsonScan<'a>,
    mut on_elem: impl FnMut(&mut JsonScan<'a>) -> Result<(), BinanceBootstrapError>,
) -> Result<(), BinanceBootstrapError> {
    cursor.eat_byte(b'[')?;
    if cursor.peek() == Some(b']') {
        cursor.eat_byte(b']')?;
        return Ok(());
    }
    loop {
        if cursor.peek() == Some(b'{') {
            on_elem(cursor)?;
        } else {
            let _ = cursor.skip_value()?;
        }
        match cursor.peek() {
            Some(b',') => cursor.eat_byte(b',')?,
            Some(b']') => {
                cursor.eat_byte(b']')?;
                break;
            }
            _ => return Err(BinanceBootstrapError::MalformedJson),
        }
    }
    Ok(())
}

fn parse_spot_balances_array<'a>(
    catalog: &BinanceBootstrapCatalog,
    cursor: &mut JsonScan<'a>,
    out: &mut Vec<OrsRestBalanceRow>,
) -> Result<(), BinanceBootstrapError> {
    scan_array_objects(cursor, |elem| {
        let mut asset_code: Option<&'a str> = None;
        let mut free_str: Option<&'a str> = None;
        let mut locked_str: Option<&'a str> = None;
        scan_object_fields(elem, |key, f| {
            match key {
                "asset" => asset_code = f.read_string_or_scalar()?,
                "free" => free_str = f.read_string_or_scalar()?,
                "locked" => locked_str = f.read_string_or_scalar()?,
                _ => {
                    let _ = f.skip_value()?;
                }
            }
            Ok(())
        })?;
        let Some(code) = asset_code else {
            return Ok(());
        };
        let Some((asset_key, scale)) = catalog.find_asset(BinanceVenueMarket::Spot, code) else {
            return Ok(());
        };
        let free_atoms = parse_scaled_i128(free_str.unwrap_or("0"), scale, true)
            .ok_or(BinanceBootstrapError::InvalidDecimalField("spot.free"))?;
        let locked_atoms = parse_scaled_i128(locked_str.unwrap_or("0"), scale, true)
            .ok_or(BinanceBootstrapError::InvalidDecimalField("spot.locked"))?;
        if free_atoms < 0 || locked_atoms < 0 {
            return Err(BinanceBootstrapError::InvalidDecimalField(
                "spot.negative_balance",
            ));
        }
        out.push(OrsRestBalanceRow {
            key: asset_key,
            scale,
            free_atoms,
            locked_atoms,
        });
        Ok(())
    })
}

fn parse_futures_assets_array<'a>(
    market: BinanceVenueMarket,
    catalog: &BinanceBootstrapCatalog,
    cursor: &mut JsonScan<'a>,
    out: &mut Vec<OrsRestBalanceRow>,
) -> Result<(), BinanceBootstrapError> {
    scan_array_objects(cursor, |elem| {
        let mut asset_code: Option<&'a str> = None;
        let mut wallet_balance: Option<&'a str> = None;
        let mut available_balance: Option<&'a str> = None;
        let mut open_order_im: Option<&'a str> = None;
        scan_object_fields(elem, |key, f| {
            match key {
                "asset" => asset_code = f.read_string_or_scalar()?,
                "walletBalance" => wallet_balance = f.read_string_or_scalar()?,
                "availableBalance" => available_balance = f.read_string_or_scalar()?,
                "openOrderInitialMargin" => open_order_im = f.read_string_or_scalar()?,
                _ => {
                    let _ = f.skip_value()?;
                }
            }
            Ok(())
        })?;
        let Some(code) = asset_code else {
            return Ok(());
        };
        let Some((asset_key, scale)) = catalog.find_asset(market, code) else {
            return Ok(());
        };
        let avail_atoms = match available_balance {
            Some(s) => Some(parse_scaled_i128(s, scale, true).ok_or(
                BinanceBootstrapError::InvalidDecimalField("futures.availableBalance"),
            )?),
            None => None,
        };
        let oo_im_atoms = match open_order_im {
            Some(s) => parse_scaled_i128(s, scale, true).ok_or(
                BinanceBootstrapError::InvalidDecimalField("futures.openOrderInitialMargin"),
            )?,
            None => 0,
        };
        let wallet_atoms = match wallet_balance {
            Some(s) => parse_scaled_i128(s, scale, true).ok_or(
                BinanceBootstrapError::InvalidDecimalField("futures.walletBalance"),
            )?,
            None => 0,
        };
        let unreserved_total = match avail_atoms {
            Some(avail) => avail.max(0).saturating_add(oo_im_atoms.max(0)),
            None => wallet_atoms.max(0),
        };
        out.push(OrsRestBalanceRow {
            key: asset_key,
            scale,
            free_atoms: unreserved_total,
            locked_atoms: 0,
        });
        Ok(())
    })
}

fn parse_futures_positions_array<'a>(
    market: BinanceVenueMarket,
    catalog: &BinanceBootstrapCatalog,
    cursor: &mut JsonScan<'a>,
    out: &mut Vec<OrsRestPositionRow>,
) -> Result<(), BinanceBootstrapError> {
    let mut pos_acc: BTreeMap<u32, i64> = BTreeMap::new();
    scan_array_objects(cursor, |elem| {
        let mut symbol: Option<&'a str> = None;
        let mut position_amt: Option<&'a str> = None;
        scan_object_fields(elem, |key, f| {
            match key {
                "symbol" => symbol = f.read_string_or_scalar()?,
                "positionAmt" => position_amt = f.read_string_or_scalar()?,
                _ => {
                    let _ = f.skip_value()?;
                }
            }
            Ok(())
        })?;
        let Some(sym_str) = symbol else {
            return Ok(());
        };
        let Some(symbol_id) = catalog.find_symbol_id(market, sym_str) else {
            return Ok(());
        };
        let Some(binding) = catalog.symbol(symbol_id) else {
            return Ok(());
        };
        let amt_str = position_amt.unwrap_or("0");
        let signed_lots =
            parse_step_i64(amt_str, binding.spec.qty_step_atoms, binding.spec.qty_scale).ok_or(
                BinanceBootstrapError::InvalidDecimalField("futures.positionAmt"),
            )?;
        let entry = pos_acc.entry(symbol_id).or_insert(0);
        *entry = entry.saturating_add(signed_lots);
        Ok(())
    })?;
    for (symbol_id, signed_lots) in pos_acc {
        out.push(OrsRestPositionRow {
            symbol_id,
            signed_lots,
        });
    }
    Ok(())
}

fn parse_options_assets_array<'a>(
    catalog: &BinanceBootstrapCatalog,
    cursor: &mut JsonScan<'a>,
    out: &mut Vec<OrsRestBalanceRow>,
) -> Result<(), BinanceBootstrapError> {
    scan_array_objects(cursor, |elem| {
        let mut asset_code: Option<&'a str> = None;
        let mut available: Option<&'a str> = None;
        let mut locked: Option<&'a str> = None;
        let mut equity: Option<&'a str> = None;
        scan_object_fields(elem, |key, f| {
            match key {
                "asset" => asset_code = f.read_string_or_scalar()?,
                "available" => available = f.read_string_or_scalar()?,
                "locked" => locked = f.read_string_or_scalar()?,
                "equity" => equity = f.read_string_or_scalar()?,
                _ => {
                    let _ = f.skip_value()?;
                }
            }
            Ok(())
        })?;
        let Some(code) = asset_code else {
            return Ok(());
        };
        let Some((asset_key, scale)) = catalog.find_asset(BinanceVenueMarket::Options, code) else {
            return Ok(());
        };
        let avail_atoms = match available {
            Some(s) => Some(parse_scaled_i128(s, scale, true).ok_or(
                BinanceBootstrapError::InvalidDecimalField("options.available"),
            )?),
            None => None,
        };
        let locked_atoms = match locked {
            Some(s) => parse_scaled_i128(s, scale, true)
                .ok_or(BinanceBootstrapError::InvalidDecimalField("options.locked"))?,
            None => 0,
        };
        let equity_atoms = match equity {
            Some(s) => parse_scaled_i128(s, scale, true)
                .ok_or(BinanceBootstrapError::InvalidDecimalField("options.equity"))?,
            None => 0,
        };
        let unreserved_total = match avail_atoms {
            Some(avail) => avail.max(0).saturating_add(locked_atoms.max(0)),
            None => equity_atoms.max(0),
        };
        out.push(OrsRestBalanceRow {
            key: asset_key,
            scale,
            free_atoms: unreserved_total,
            locked_atoms: 0,
        });
        Ok(())
    })
}

fn parse_options_positions_array<'a>(
    catalog: &BinanceBootstrapCatalog,
    cursor: &mut JsonScan<'a>,
    out: &mut Vec<OrsRestPositionRow>,
) -> Result<(), BinanceBootstrapError> {
    let mut pos_acc: BTreeMap<u32, i64> = BTreeMap::new();
    scan_array_objects(cursor, |elem| {
        let mut symbol: Option<&'a str> = None;
        let mut quantity: Option<&'a str> = None;
        let mut side_str: Option<&'a str> = None;
        scan_object_fields(elem, |key, f| {
            match key {
                "symbol" => symbol = f.read_string_or_scalar()?,
                "quantity" | "positionAmt" => quantity = f.read_string_or_scalar()?,
                "side" => side_str = f.read_string_or_scalar()?,
                _ => {
                    let _ = f.skip_value()?;
                }
            }
            Ok(())
        })?;
        let Some(sym_str) = symbol else {
            return Ok(());
        };
        let Some(symbol_id) = catalog.find_symbol_id(BinanceVenueMarket::Options, sym_str) else {
            return Ok(());
        };
        let Some(binding) = catalog.symbol(symbol_id) else {
            return Ok(());
        };
        let qty_str = quantity.unwrap_or("0");
        let mut signed_lots =
            parse_step_i64(qty_str, binding.spec.qty_step_atoms, binding.spec.qty_scale).ok_or(
                BinanceBootstrapError::InvalidDecimalField("options.quantity"),
            )?;
        if signed_lots > 0 && matches!(side_str, Some("SHORT" | "SELL")) {
            signed_lots = -signed_lots;
        }
        let entry = pos_acc.entry(symbol_id).or_insert(0);
        *entry = entry.saturating_add(signed_lots);
        Ok(())
    })?;
    for (symbol_id, signed_lots) in pos_acc {
        out.push(OrsRestPositionRow {
            symbol_id,
            signed_lots,
        });
    }
    Ok(())
}

/// Parses an authenticated Binance `Account` REST response (`Spot /api/v3/account`,
/// `USD-M /fapi/v2/account`, `Coin-M /dapi/v1/account`, or `Options /eapi/v1/account`).
///
/// Returns `(balances, positions, balances_include_open_order_locks)`.
pub fn parse_account_snapshot(
    market: BinanceVenueMarket,
    catalog: &BinanceBootstrapCatalog,
    json: &[u8],
) -> Result<(Vec<OrsRestBalanceRow>, Vec<OrsRestPositionRow>, bool), BinanceBootstrapError> {
    let mut cursor = JsonScan::new(json);
    let mut balances = Vec::new();
    let mut positions = Vec::new();

    match market {
        BinanceVenueMarket::Spot => {
            scan_object_fields(&mut cursor, |key, f| {
                if key == "balances" && f.peek() == Some(b'[') {
                    parse_spot_balances_array(catalog, f, &mut balances)?;
                } else {
                    let _ = f.skip_value()?;
                }
                Ok(())
            })?;
            Ok((balances, positions, true))
        }
        BinanceVenueMarket::Usdm | BinanceVenueMarket::Coinm => {
            scan_object_fields(&mut cursor, |key, f| {
                if key == "assets" && f.peek() == Some(b'[') {
                    parse_futures_assets_array(market, catalog, f, &mut balances)?;
                } else if key == "positions" && f.peek() == Some(b'[') {
                    parse_futures_positions_array(market, catalog, f, &mut positions)?;
                } else {
                    let _ = f.skip_value()?;
                }
                Ok(())
            })?;
            Ok((balances, positions, false))
        }
        BinanceVenueMarket::Options => {
            scan_object_fields(&mut cursor, |key, f| {
                if (key == "asset" || key == "assets") && f.peek() == Some(b'[') {
                    parse_options_assets_array(catalog, f, &mut balances)?;
                } else if (key == "position" || key == "positions") && f.peek() == Some(b'[') {
                    parse_options_positions_array(catalog, f, &mut positions)?;
                } else {
                    let _ = f.skip_value()?;
                }
                Ok(())
            })?;
            Ok((balances, positions, false))
        }
    }
}

fn parse_open_order_tif(
    tif_str: Option<&str>,
    order_type_str: Option<&str>,
) -> (v::TimeInForce, bool) {
    if matches!(order_type_str, Some("LIMIT_MAKER")) {
        return (v::TimeInForce::Gtc, true);
    }
    match tif_str {
        Some("IOC") => (v::TimeInForce::Ioc, false),
        Some("FOK") => (v::TimeInForce::Fok, false),
        Some("GTX") => (v::TimeInForce::PostOnly, true),
        Some("RPI") => (v::TimeInForce::Rpi, true),
        _ => (v::TimeInForce::Gtc, false),
    }
}

/// Parses an authenticated Binance `openOrders` REST JSON array across Spot, USD-M, Coin-M, and
/// Options (`GET /{api/v3,fapi/v1,dapi/v1,eapi/v1}/openOrders`).
pub fn parse_open_orders_snapshot(
    market: BinanceVenueMarket,
    catalog: &BinanceBootstrapCatalog,
    json: &[u8],
    created_mono_ns: u64,
) -> Result<Vec<OrsRestOpenOrderRow>, BinanceBootstrapError> {
    let mut cursor = JsonScan::new(json);
    let mut out = Vec::new();
    scan_array_objects(&mut cursor, |elem| {
        let mut symbol: Option<&str> = None;
        let mut client_id: Option<&str> = None;
        let mut venue_order_id: Option<&str> = None;
        let mut side_str: Option<&str> = None;
        let mut tif_str: Option<&str> = None;
        let mut order_type_str: Option<&str> = None;
        let mut price_str: Option<&str> = None;
        let mut orig_qty_str: Option<&str> = None;
        let mut exec_qty_str: Option<&str> = None;
        let mut reduce_only = false;
        let mut post_only_flag = false;

        scan_object_fields(elem, |key, f| {
            match key {
                "symbol" => symbol = f.read_string_or_scalar()?,
                "clientOrderId" | "clientId" => client_id = f.read_string_or_scalar()?,
                "orderId" => venue_order_id = f.read_string_or_scalar()?,
                "side" => side_str = f.read_string_or_scalar()?,
                "timeInForce" => tif_str = f.read_string_or_scalar()?,
                "type" => order_type_str = f.read_string_or_scalar()?,
                "price" => price_str = f.read_string_or_scalar()?,
                "origQty" | "quantity" => orig_qty_str = f.read_string_or_scalar()?,
                "executedQty" => exec_qty_str = f.read_string_or_scalar()?,
                "reduceOnly" => {
                    if let Some(s) = f.read_string_or_scalar()? {
                        reduce_only = s.eq_ignore_ascii_case("true");
                    }
                }
                "postOnly" => {
                    if let Some(s) = f.read_string_or_scalar()? {
                        post_only_flag = s.eq_ignore_ascii_case("true");
                    }
                }
                _ => {
                    let _ = f.skip_value()?;
                }
            }
            Ok(())
        })?;

        let Some(sym_str) = symbol else {
            return Ok(());
        };
        let Some(symbol_id) = catalog.find_symbol_id(market, sym_str) else {
            return Ok(());
        };
        let Some(binding) = catalog.symbol(symbol_id) else {
            return Ok(());
        };

        let side = match side_str {
            Some("BUY") => v::Side::Buy,
            Some("SELL") => v::Side::Sell,
            _ => {
                return Err(BinanceBootstrapError::InvalidDecimalField(
                    "openOrders.side",
                ));
            }
        };
        let (tif, derived_post_only) = parse_open_order_tif(tif_str, order_type_str);
        let post_only = derived_post_only || post_only_flag;

        let price_ticks = parse_step_i64(
            price_str.unwrap_or("0"),
            binding.spec.price_tick_atoms,
            binding.spec.price_scale,
        )
        .ok_or(BinanceBootstrapError::InvalidDecimalField(
            "openOrders.price",
        ))?;
        let orig_qty_lots = parse_step_i64(
            orig_qty_str.unwrap_or("0"),
            binding.spec.qty_step_atoms,
            binding.spec.qty_scale,
        )
        .ok_or(BinanceBootstrapError::InvalidDecimalField(
            "openOrders.origQty",
        ))?;
        let cum_filled_lots = parse_step_i64(
            exec_qty_str.unwrap_or("0"),
            binding.spec.qty_step_atoms,
            binding.spec.qty_scale,
        )
        .ok_or(BinanceBootstrapError::InvalidDecimalField(
            "openOrders.executedQty",
        ))?;

        if orig_qty_lots <= cum_filled_lots {
            return Ok(());
        }

        out.push(OrsRestOpenOrderRow {
            symbol_id,
            client_id: client_id.map(String::from).unwrap_or_default(),
            venue_order_id: venue_order_id.map(String::from).unwrap_or_default(),
            side,
            tif,
            post_only,
            reduce_only,
            price_ticks,
            orig_qty_lots,
            cum_filled_lots,
            created_mono_ns,
        });
        Ok(())
    })?;
    Ok(out)
}

/// Builds a complete per-market [`OrsExchangeBootstrapSnapshot`] from raw `Account`, `OpenOrders`,
/// and optional separate Options `Position` (`GET /eapi/v1/position`) REST JSON responses.
pub fn build_market_bootstrap_snapshot(
    market: BinanceVenueMarket,
    catalog: &BinanceBootstrapCatalog,
    account_json: &[u8],
    open_orders_json: &[u8],
    extra_positions_json: Option<&[u8]>,
    created_mono_ns: u64,
) -> Result<OrsExchangeBootstrapSnapshot, BinanceBootstrapError> {
    let (balances, mut positions, balances_include_open_order_locks) =
        parse_account_snapshot(market, catalog, account_json)?;
    if let Some(pos_json) = extra_positions_json {
        let mut cursor = JsonScan::new(pos_json);
        match market {
            BinanceVenueMarket::Options => {
                parse_options_positions_array(catalog, &mut cursor, &mut positions)?;
            }
            BinanceVenueMarket::Usdm | BinanceVenueMarket::Coinm => {
                parse_futures_positions_array(market, catalog, &mut cursor, &mut positions)?;
            }
            BinanceVenueMarket::Spot => {}
        }
    }
    let open_orders =
        parse_open_orders_snapshot(market, catalog, open_orders_json, created_mono_ns)?;
    Ok(OrsExchangeBootstrapSnapshot {
        balances,
        positions,
        open_orders,
        balances_include_open_order_locks,
    })
}

/// Applies a single market's Exchange REST `Account + OpenOrders` snapshot directly into
/// `OrderManager<CHECK_STP>`, reconstructing `SelfDescribingClientId` (`tw1...`) open orders,
/// `UtaWalletLedger` holds/positions, `OrsStpBook`, and `DopState` per-strategy sequence floors.
#[allow(clippy::too_many_arguments)]
pub fn bootstrap_market_into_order_manager<const CHECK_STP: bool>(
    order_manager: &OrderManager<CHECK_STP>,
    account_ref: u8,
    market: BinanceVenueMarket,
    catalog: &BinanceBootstrapCatalog,
    account_json: &[u8],
    open_orders_json: &[u8],
    extra_positions_json: Option<&[u8]>,
    created_mono_ns: u64,
) -> Result<OrsBootstrapSummary, BinanceBootstrapError> {
    let snapshot = build_market_bootstrap_snapshot(
        market,
        catalog,
        account_json,
        open_orders_json,
        extra_positions_json,
        created_mono_ns,
    )?;
    order_manager
        .bootstrap_from_exchange(account_ref, &snapshot)
        .map_err(BinanceBootstrapError::OrderManagerRejected)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_scaled_i128_handles_exact_and_sub_atom_decimals() {
        assert_eq!(
            parse_scaled_i128("10000.50000000", 8, false),
            Some(1_000_050_000_000)
        );
        assert_eq!(parse_scaled_i128("-0.0025", 4, false), Some(-25));
        assert_eq!(parse_scaled_i128("12.345", 2, false), None);
        assert_eq!(parse_scaled_i128("12.345", 2, true), Some(1234));
    }
}
