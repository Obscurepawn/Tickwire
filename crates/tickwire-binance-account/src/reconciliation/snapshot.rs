//! Compare authenticated REST observations with the current replayed account.
//! Match every ordinary live order and position against the replayed account.
//! Unknown remote orders or unsupported conditional liabilities retain the freeze.
use crate::registry::Registration;
use crate::{Error, Result};
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};
use tickwire_account::{Account, Fixed};
use tickwire_account::{IntentKey, OrderKey};
use tickwire_account_codec::CanonicalBaseline;
use tickwire_binance::{decimal::Decimal, request::Market};
use tickwire_venue as v;
use tickwire_venue::PositionSide;

const MAX_BODY: usize = 32 * 1024;
const MAX_ROWS: usize = 1024;

#[cfg(test)]
fn verify_snapshot(
    market: Market,
    baseline: &CanonicalBaseline,
    account: &Account,
    account_body: &[u8],
    open_orders_body: &[u8],
    conditional_orders_body: &[u8],
) -> Result<()> {
    verify_snapshot_with_orders(
        market,
        baseline,
        account,
        account_body,
        open_orders_body,
        conditional_orders_body,
        &BTreeMap::new(),
    )
}

pub(crate) fn verify_snapshot_with_orders(
    market: Market,
    baseline: &CanonicalBaseline,
    account: &Account,
    account_body: &[u8],
    open_orders_body: &[u8],
    conditional_orders_body: &[u8],
    registry: &BTreeMap<IntentKey, Registration>,
) -> Result<()> {
    if [account_body, open_orders_body, conditional_orders_body]
        .iter()
        .any(|b| b.is_empty() || b.len() > MAX_BODY)
    {
        return Err(Error::Capacity);
    }
    empty_orders(conditional_orders_body)?;
    if account.rejected_event().is_some()
        || account.account_epoch() != baseline.snapshot().account_epoch
    {
        return Err(Error::Conflict);
    }
    ordinary_orders(market, account, open_orders_body, registry)?;
    // One REST account has one value per asset symbol. Ambiguous local wallets
    // cannot be reconciled by copying that remote value into multiple balances.
    let mut symbols = BTreeSet::new();
    for a in &baseline.snapshot().assets {
        if !symbols.insert(a.asset.symbol.as_str()) {
            return Err(Error::Unsupported);
        }
        let view = account.asset(a.key).map_err(|_| Error::Conflict)?;
        if view.deficit.atoms() != 0 {
            return Err(Error::Conflict);
        }
    }
    for p in &baseline.catalog().entries {
        let matches = match market {
            Market::Spot => p.product == tickwire_venue::Product::Spot,
            Market::Usdm => matches!(
                p.product,
                tickwire_venue::Product::LinearPerpetual | tickwire_venue::Product::LinearFuture
            ),
        };
        if !matches {
            return Err(Error::Identity);
        }
        if let Some(futures) = &p.futures
            && futures.margin_mode != v::MarginMode::Cross
        {
            return Err(Error::Unsupported);
        }
    }
    match market {
        Market::Spot => spot(baseline, account, account_body),
        Market::Usdm => usdm(baseline, account, account_body),
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct OpenOrder<'a> {
    symbol: &'a str,
    order_id: u64,
    client_order_id: &'a str,
    status: &'a str,
    side: &'a str,
    #[serde(rename = "type")]
    kind: &'a str,
    orig_qty: Decimal,
    executed_qty: Decimal,
    price: Decimal,
    time_in_force: &'a str,
    position_side: Option<&'a str>,
    reduce_only: Option<bool>,
    order_list_id: Option<i64>,
}
fn ordinary_orders(
    market: Market,
    account: &Account,
    body: &[u8],
    registry: &BTreeMap<IntentKey, Registration>,
) -> Result<()> {
    let rows: Vec<OpenOrder<'_>> = serde_json::from_slice(body).map_err(|_| Error::Encoding)?;
    if rows.len() > MAX_ROWS {
        return Err(Error::Capacity);
    }
    let mut seen = BTreeSet::new();
    let mut native = BTreeSet::new();
    for row in rows {
        if row.order_id > i64::MAX as u64
            || !native.insert((row.symbol, row.order_id))
            || !matches!(row.status, "NEW" | "PARTIALLY_FILLED")
        {
            return Err(Error::Conflict);
        }
        let mut matching = registry.iter().filter_map(|(intent, reg)| {
            (reg.command.command().context.instrument.symbol.as_str() == row.symbol)
                .then_some(())?;
            let index = reg.orders.iter().position(|o| {
                o.client_id
                    .as_ref()
                    .is_some_and(|id| id.as_str() == row.client_order_id)
            })?;
            Some((*intent, reg, index))
        });
        let (intent, reg, index) = matching.next().ok_or(Error::Identity)?;
        if matching.next().is_some() {
            return Err(Error::Identity);
        }
        let id = &reg.orders[index];
        let key = OrderKey {
            intent,
            client_id: id.client_id.clone().ok_or(Error::Identity)?,
        };
        if !seen.insert(key.clone())
            || id
                .venue_order_id
                .as_ref()
                .and_then(|id| id.as_str().parse::<u64>().ok())
                != Some(row.order_id)
            || id.list_id.is_some()
            || id.venue_algo_id.is_some()
            || row.order_list_id.is_some_and(|n| n != -1)
        {
            return Err(Error::Identity);
        }
        let order = account.order(&key).ok_or(Error::Identity)?;
        if order.aborted || order.terminal || !order.confirmed {
            return Err(Error::Conflict);
        }
        let spec = match &reg.command.command().body {
            v::CommandBody::New(spec)
            | v::CommandBody::CancelReplace {
                replacement: spec, ..
            } => spec,
            _ => return Err(Error::Unsupported),
        };
        let kind = match (market, spec.order_type, spec.time_in_force) {
            (Market::Spot, v::OrderType::Limit, Some(v::TimeInForce::PostOnly)) => "LIMIT_MAKER",
            (_, v::OrderType::Limit, _) => "LIMIT",
            (_, v::OrderType::Market, _) => "MARKET",
            _ => return Err(Error::Unsupported),
        };
        let tif = match spec.time_in_force {
            Some(v::TimeInForce::Gtc) | None => "GTC",
            Some(v::TimeInForce::Ioc) => "IOC",
            Some(v::TimeInForce::Fok) => "FOK",
            Some(v::TimeInForce::PostOnly) if market == Market::Usdm => "GTX",
            Some(v::TimeInForce::PostOnly) => "GTC",
            _ => return Err(Error::Unsupported),
        };
        let side = match spec.side {
            v::Side::Buy => "BUY",
            v::Side::Sell => "SELL",
        };
        let position = match spec.position_side {
            PositionSide::Both => "BOTH",
            PositionSide::Long => "LONG",
            PositionSide::Short => "SHORT",
        };
        if row.kind != kind
            || row.side != side
            || row.time_in_force != tif
            || (market == Market::Usdm
                && (row.position_side != Some(position)
                    || row.reduce_only != Some(spec.reduce_only)))
        {
            return Err(Error::Conflict);
        }
        exact(row.orig_qty, order.original_quantity)?;
        exact(row.executed_qty, order.filled_quantity)?;
        if (row.status == "NEW") != (order.filled_quantity.atoms() == 0) {
            return Err(Error::Conflict);
        }
        if let Some(price) = spec.price {
            exact_venue(row.price, price)?;
        } else {
            zero(row.price)?;
        }
    }
    for key in account.order_keys() {
        let order = account.order(key).ok_or(Error::Conflict)?;
        if order.aborted {
            continue;
        }
        if order.terminal {
            if order.remaining_quantity.atoms() != 0
                || !order.fee_reconciled
                || order.reduce_allocation.atoms() != 0
            {
                return Err(Error::Conflict);
            }
        } else if !seen.contains(key) {
            return Err(Error::Conflict);
        }
    }
    Ok(())
}
fn fixed(value: Decimal, scale: u8) -> Result<Fixed> {
    let value = value.rescale_exact(scale).map_err(|_| Error::Encoding)?;
    Fixed::from_atoms(value.mantissa(), scale).map_err(|_| Error::Encoding)
}
fn exact_venue(value: Decimal, expected: v::Decimal) -> Result<()> {
    let other: Decimal =
        Decimal::parse(&expected.as_plain_string()).map_err(|_| Error::Encoding)?;
    let scale = value.scale().max(other.scale());
    if value
        .rescale_exact(scale)
        .map_err(|_| Error::Encoding)?
        .mantissa()
        != other
            .rescale_exact(scale)
            .map_err(|_| Error::Encoding)?
            .mantissa()
    {
        return Err(Error::Conflict);
    }
    Ok(())
}

// Reject the first element without retaining unbounded arbitrary JSON objects.
fn empty_orders(body: &[u8]) -> Result<()> {
    struct Empty;
    impl<'de> serde::de::Visitor<'de> for Empty {
        type Value = ();
        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("an empty order array")
        }
        fn visit_seq<A: serde::de::SeqAccess<'de>>(
            self,
            mut seq: A,
        ) -> core::result::Result<(), A::Error> {
            if seq.next_element::<serde::de::IgnoredAny>()?.is_some() {
                return Err(serde::de::Error::custom(
                    "open order requires reconciliation",
                ));
            }
            Ok(())
        }
    }
    let mut de = serde_json::Deserializer::from_slice(body);
    serde::Deserializer::deserialize_seq(&mut de, Empty).map_err(|_| Error::Conflict)?;
    de.end().map_err(|_| Error::Encoding)
}
fn exact(value: Decimal, expected: Fixed) -> Result<()> {
    let value = value
        .rescale_exact(expected.scale())
        .map_err(|_| Error::Encoding)?;
    if value.mantissa() != expected.atoms() {
        return Err(Error::Conflict);
    }
    Ok(())
}
fn zero(value: Decimal) -> Result<()> {
    if value.mantissa() != 0 {
        return Err(Error::Conflict);
    }
    Ok(())
}
fn valid_symbol(s: &str) -> bool {
    !s.is_empty() && s.len() <= 64 && !s.chars().any(|c| c.is_control() || c.is_whitespace())
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Spot<'a> {
    can_trade: bool,
    account_type: &'a str,
    #[serde(borrow)]
    balances: Vec<SpotBalance<'a>>,
}
#[derive(Deserialize)]
struct SpotBalance<'a> {
    asset: &'a str,
    free: Decimal,
    locked: Decimal,
}
fn spot(baseline: &CanonicalBaseline, account: &Account, body: &[u8]) -> Result<()> {
    let remote: Spot<'_> = serde_json::from_slice(body).map_err(|_| Error::Encoding)?;
    if !remote.can_trade || remote.account_type != "SPOT" {
        return Err(Error::Unsupported);
    }
    if remote.balances.len() > MAX_ROWS {
        return Err(Error::Capacity);
    }
    let mut seen = BTreeSet::new();
    for row in remote.balances {
        if !valid_symbol(row.asset)
            || !seen.insert(row.asset)
            || row.free.mantissa() < 0
            || row.locked.mantissa() < 0
        {
            return Err(Error::Encoding);
        }
        let Some(local) = baseline
            .snapshot()
            .assets
            .iter()
            .find(|a| a.asset.symbol.as_str() == row.asset)
        else {
            zero(row.free)?;
            zero(row.locked)?;
            continue;
        };
        let view = account.asset(local.key).map_err(|_| Error::Conflict)?;
        let free = fixed(row.free, view.total.scale())?;
        let locked = fixed(row.locked, view.total.scale())?;
        if free.checked_add(locked).map_err(|_| Error::Capacity)? != view.total
            || locked.atoms() < local.nontrade_locked.atoms()
            || locked.atoms()
                > local
                    .nontrade_locked
                    .checked_add(view.reserved)
                    .map_err(|_| Error::Capacity)?
                    .atoms()
            || free.atoms() < view.available.atoms()
        {
            return Err(Error::Conflict);
        }
    }
    // Missing zero assets are acceptable; omitting any tracked balance is not.
    for local in &baseline.snapshot().assets {
        if !seen.contains(local.asset.symbol.as_str()) {
            let view = account.asset(local.key).map_err(|_| Error::Conflict)?;
            if view.total.atoms() != 0 || local.nontrade_locked.atoms() != 0 {
                return Err(Error::Conflict);
            }
        }
    }
    Ok(())
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Usdm<'a> {
    can_trade: Option<bool>,
    multi_assets_margin: Option<bool>,
    total_initial_margin: Decimal,
    total_maint_margin: Decimal,
    total_unrealized_profit: Decimal,
    total_position_initial_margin: Decimal,
    total_open_order_initial_margin: Decimal,
    total_cross_un_pnl: Decimal,
    #[serde(borrow)]
    assets: Vec<UsdmAsset<'a>>,
    #[serde(borrow)]
    positions: Vec<UsdmPosition<'a>>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct UsdmAsset<'a> {
    asset: &'a str,
    wallet_balance: Decimal,
    available_balance: Decimal,
    margin_balance: Decimal,
    unrealized_profit: Decimal,
    maint_margin: Decimal,
    initial_margin: Decimal,
    position_initial_margin: Decimal,
    open_order_initial_margin: Decimal,
    cross_wallet_balance: Decimal,
    cross_un_pnl: Decimal,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct UsdmPosition<'a> {
    symbol: &'a str,
    position_side: &'a str,
    position_amt: Decimal,
    entry_price: Option<Decimal>,
    unrealized_profit: Decimal,
    isolated_margin: Decimal,
    notional: Decimal,
    isolated_wallet: Decimal,
    initial_margin: Decimal,
    maint_margin: Decimal,
}
fn usdm(baseline: &CanonicalBaseline, account: &Account, body: &[u8]) -> Result<()> {
    let remote: Usdm<'_> = serde_json::from_slice(body).map_err(|_| Error::Encoding)?;
    if remote.can_trade == Some(false) || remote.multi_assets_margin == Some(true) {
        return Err(Error::Unsupported);
    }
    if remote.assets.len() > MAX_ROWS || remote.positions.len() > MAX_ROWS {
        return Err(Error::Capacity);
    }
    for v in [
        remote.total_initial_margin,
        remote.total_maint_margin,
        remote.total_position_initial_margin,
        remote.total_open_order_initial_margin,
    ] {
        if v.mantissa() < 0 {
            return Err(Error::Encoding);
        }
    }
    let _ = (remote.total_unrealized_profit, remote.total_cross_un_pnl);
    let mut seen = BTreeSet::new();
    for row in remote.assets {
        if !valid_symbol(row.asset) || !seen.insert(row.asset) {
            return Err(Error::Encoding);
        }
        for v in [
            row.maint_margin,
            row.initial_margin,
            row.position_initial_margin,
            row.open_order_initial_margin,
        ] {
            if v.mantissa() < 0 {
                return Err(Error::Encoding);
            }
        }
        let values = [
            row.wallet_balance,
            row.available_balance,
            row.margin_balance,
            row.cross_wallet_balance,
            row.unrealized_profit,
            row.cross_un_pnl,
        ];
        let Some(local) = baseline
            .snapshot()
            .assets
            .iter()
            .find(|a| a.asset.symbol.as_str() == row.asset)
        else {
            for v in values {
                zero(v)?;
            }
            continue;
        };
        let view = account.asset(local.key).map_err(|_| Error::Conflict)?;
        exact(row.wallet_balance, view.total)?;
        exact(row.cross_wallet_balance, view.total)?;
        let unrealized = fixed(row.unrealized_profit, view.total.scale())?;
        exact(row.cross_un_pnl, unrealized)?;
        exact(
            row.margin_balance,
            view.total
                .checked_add(unrealized)
                .map_err(|_| Error::Capacity)?,
        )?;
        if baseline.catalog().entries.iter().any(|p| {
            p.wallet_id == local.key.wallet_id && p.instrument.settlement.id == local.asset.id
        }) {
            if fixed(row.available_balance, view.total.scale())?.atoms() < 0 {
                return Err(Error::Conflict);
            }
        } else {
            exact(row.available_balance, view.available)?;
        }
    }
    for local in &baseline.snapshot().assets {
        if !seen.contains(local.asset.symbol.as_str())
            && account
                .asset(local.key)
                .map_err(|_| Error::Conflict)?
                .total
                .atoms()
                != 0
        {
            return Err(Error::Conflict);
        }
    }
    let mut positions = BTreeSet::new();
    for row in remote.positions {
        if !valid_symbol(row.symbol) || !positions.insert((row.symbol, row.position_side)) {
            return Err(Error::Encoding);
        }
        let side = match row.position_side {
            "BOTH" => PositionSide::Both,
            "LONG" => PositionSide::Long,
            "SHORT" => PositionSide::Short,
            _ => return Err(Error::Encoding),
        };
        zero(row.isolated_margin)?;
        zero(row.isolated_wallet)?;
        if row.initial_margin.mantissa() < 0 || row.maint_margin.mantissa() < 0 {
            return Err(Error::Encoding);
        }
        let _ = (row.notional, row.unrealized_profit);
        let Some(policy) = baseline
            .catalog()
            .entries
            .iter()
            .find(|p| p.instrument.symbol.as_str() == row.symbol)
        else {
            zero(row.position_amt)?;
            continue;
        };
        let futures = policy.futures.as_ref().ok_or(Error::Identity)?;
        if (futures.position_mode == v::PositionMode::OneWay) != (side == PositionSide::Both) {
            return Err(Error::Conflict);
        }
        let expected = account
            .position(policy.instrument.instrument_id, side)
            .map_err(|_| Error::Conflict)?
            .0;
        let actual = fixed(row.position_amt, expected.scale())?;
        let actual = if side == PositionSide::Short {
            if actual.atoms() > 0 {
                return Err(Error::Conflict);
            }
            actual.checked_neg().map_err(|_| Error::Capacity)?
        } else {
            actual
        };
        if actual != expected {
            return Err(Error::Conflict);
        }
        if let Some(entry) = row.entry_price
            && expected.atoms() != 0
        {
            let bound = account
                .position_entry_bound(policy.instrument.instrument_id, side)
                .map_err(|_| Error::Conflict)?
                .ok_or(Error::Conflict)?;
            let entry = fixed(entry, bound.scale())?;
            let short =
                side == PositionSide::Short || (side == PositionSide::Both && expected.atoms() < 0);
            if (short && entry.atoms() < bound.atoms()) || (!short && entry.atoms() > bound.atoms())
            {
                return Err(Error::Conflict);
            }
        }
    }
    for policy in &baseline.catalog().entries {
        for (side, name) in [
            (PositionSide::Both, "BOTH"),
            (PositionSide::Long, "LONG"),
            (PositionSide::Short, "SHORT"),
        ] {
            if !positions.contains(&(policy.instrument.symbol.as_str(), name))
                && account
                    .position(policy.instrument.instrument_id, side)
                    .map_err(|_| Error::Conflict)?
                    .0
                    .atoms()
                    != 0
            {
                return Err(Error::Conflict);
            }
        }
    }
    Ok(())
}

/// Values become effective only as part of the verified durable reactivation.
pub(crate) fn venue_available(
    baseline: &CanonicalBaseline,
    body: &[u8],
) -> Result<Vec<(u64, Fixed)>> {
    if baseline
        .catalog()
        .entries
        .iter()
        .all(|p| p.product == v::Product::Spot)
    {
        return Ok(Vec::new());
    }
    let remote: Usdm<'_> = serde_json::from_slice(body).map_err(|_| Error::Encoding)?;
    let mut values = BTreeMap::new();
    for policy in &baseline.catalog().entries {
        let asset = &policy.instrument.settlement;
        let row = remote
            .assets
            .iter()
            .find(|a| a.asset == asset.symbol.as_str())
            .ok_or(Error::Conflict)?;
        let scale = u8::try_from(-i16::from(asset.exponent)).map_err(|_| Error::Encoding)?;
        let amount = fixed(row.available_balance, scale)?;
        if amount.atoms() < 0 {
            return Err(Error::Conflict);
        }
        if values
            .insert(policy.wallet_id, amount)
            .is_some_and(|previous| previous != amount)
        {
            return Err(Error::Conflict);
        }
    }
    Ok(values.into_iter().collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn baseline() -> (CanonicalBaseline, Account) {
        let (s, c, _) = crate::unit_fixture::fixture();
        (
            CanonicalBaseline::new(s.clone(), c.clone()).unwrap(),
            Account::from_snapshot(s, c).unwrap(),
        )
    }
    const SPOT: &[u8] = br#"{"canTrade":true,"accountType":"SPOT","balances":[{"asset":"BTC","free":"1.00000000","locked":"0"},{"asset":"USDT","free":"1000","locked":"0"}]}"#;
    #[test]
    fn spot_snapshot_requires_exact_remote_balances_and_both_order_sets() {
        let (b, a) = baseline();
        assert!(verify_snapshot(Market::Spot, &b, &a, SPOT, b"[]", b"[]").is_ok());
        let mismatch = String::from_utf8(SPOT.to_vec())
            .unwrap()
            .replace("1000", "999.999999999");
        assert!(verify_snapshot(Market::Spot, &b, &a, mismatch.as_bytes(), b"[]", b"[]").is_err());
        assert!(
            verify_snapshot(Market::Spot, &b, &a, SPOT, b"[]", br#"[{"orderListId":1}]"#).is_err()
        );
        assert!(verify_snapshot(Market::Spot, &b, &a, SPOT, b"{}", b"[]").is_err());
    }
    #[test]
    fn missing_duplicate_and_unknown_nonzero_balances_fail() {
        let (b, a) = baseline();
        for suffix in [
            r#",{"asset":"XXX","free":"1","locked":"0"}"#,
            r#",{"asset":"BTC","free":"0","locked":"0"}"#,
        ] {
            let body = String::from_utf8(SPOT.to_vec())
                .unwrap()
                .replace("]}", &format!("{suffix}]}}"));
            assert!(verify_snapshot(Market::Spot, &b, &a, body.as_bytes(), b"[]", b"[]").is_err());
        }
        let body = br#"{"canTrade":true,"accountType":"SPOT","balances":[]}"#;
        assert!(verify_snapshot(Market::Spot, &b, &a, body, b"[]", b"[]").is_err());
    }
    #[test]
    fn usdm_flat_snapshot_checks_all_wallets_and_rejects_nonzero_positions() {
        let (s, c, _) = crate::unit_fixture::futures_fixture();
        let b = CanonicalBaseline::new(s.clone(), c.clone()).unwrap();
        let a = Account::from_snapshot(s, c).unwrap();
        let assets: Vec<_> = [("BTC", "1"), ("USDT", "1000")]
            .into_iter()
            .map(|(asset, value)| {
                serde_json::json!({"asset":asset,"walletBalance":value,"availableBalance":value,
                "marginBalance":value,"crossWalletBalance":value,"unrealizedProfit":"0",
                "maintMargin":"0","initialMargin":"0","positionInitialMargin":"0",
                "openOrderInitialMargin":"0","crossUnPnl":"0"})
            })
            .collect();
        let mut body = serde_json::json!({"totalInitialMargin":"0","totalMaintMargin":"0",
            "totalUnrealizedProfit":"0","totalPositionInitialMargin":"0",
            "totalOpenOrderInitialMargin":"0","totalCrossUnPnl":"0","assets":assets,"positions":[]});
        assert!(
            verify_snapshot(
                Market::Usdm,
                &b,
                &a,
                &serde_json::to_vec(&body).unwrap(),
                b"[]",
                b"[]"
            )
            .is_ok()
        );
        body["positions"] = serde_json::json!([{"symbol":"BTCUSDT","positionSide":"BOTH",
            "positionAmt":"1","unrealizedProfit":"0","isolatedMargin":"0","notional":"0",
            "isolatedWallet":"0","initialMargin":"0","maintMargin":"0"}]);
        assert!(
            verify_snapshot(
                Market::Usdm,
                &b,
                &a,
                &serde_json::to_vec(&body).unwrap(),
                b"[]",
                b"[]"
            )
            .is_err()
        );
    }
    #[test]
    fn recovered_known_spot_order_matches_remote_open_liability() {
        let (snapshot, catalog, command) = crate::unit_fixture::fixture();
        let baseline = CanonicalBaseline::new(snapshot.clone(), catalog.clone()).unwrap();
        let canonical = crate::unit_fixture::canonical(command.clone());
        let mut account = Account::from_snapshot(snapshot.clone(), catalog.clone()).unwrap();
        let admission = account.admit(&canonical, 20).unwrap();
        let mut event = crate::unit_fixture::fill(&command, "unused", "0.001", "60000", "60");
        let v::VenueEventBody::Fill(fill) = event.body else {
            unreachable!()
        };
        let mut observation = fill.observation;
        observation.status = v::OrderStatus::New;
        observation.cumulative_quantity = Some("0".parse().unwrap());
        observation.order.venue_order_id = Some(v::VenueOrderId::new("77").unwrap());
        let identity = observation.order.clone();
        event.body = v::VenueEventBody::Order(Box::new(observation));
        let event = crate::unit_fixture::event(event);
        let account = Account::recover(
            snapshot,
            catalog,
            &[
                tickwire_account::RecoveryRecord::Admit {
                    command: Box::new(canonical.clone()),
                    evaluated_at: 20,
                    expected: admission.image(),
                },
                tickwire_account::RecoveryRecord::VenueEvent(Box::new(event)),
            ],
        )
        .unwrap();
        let registry = BTreeMap::from([(
            admission.intent(),
            Registration {
                command: canonical,
                ids: vec![identity.order_id],
                orders: vec![identity],
            },
        )]);
        let balance=br#"{"canTrade":true,"accountType":"SPOT","balances":[{"asset":"BTC","free":"1","locked":"0"},{"asset":"USDT","free":"940","locked":"60"}]}"#;
        let orders=br#"[{"symbol":"BTCUSDT","orderId":77,"clientOrderId":"test-1","status":"NEW","side":"BUY","type":"LIMIT","origQty":"0.001","executedQty":"0","price":"60000","timeInForce":"GTC","orderListId":-1}]"#;
        assert!(
            verify_snapshot_with_orders(
                Market::Spot,
                &baseline,
                &account,
                balance,
                orders,
                b"[]",
                &registry
            )
            .is_ok()
        );
        assert!(
            verify_snapshot_with_orders(
                Market::Spot,
                &baseline,
                &account,
                balance,
                b"[]",
                b"[]",
                &registry
            )
            .is_err()
        );
        let wrong = String::from_utf8(orders.to_vec())
            .unwrap()
            .replace("0.001", "0.002");
        assert!(
            verify_snapshot_with_orders(
                Market::Spot,
                &baseline,
                &account,
                balance,
                wrong.as_bytes(),
                b"[]",
                &registry
            )
            .is_err()
        );
    }
    #[test]
    fn nonzero_cross_position_and_native_available_are_exact() {
        let (mut snapshot, catalog, _) = crate::unit_fixture::futures_fixture();
        snapshot.positions.push(tickwire_account::PositionSnapshot {
            instrument_id: 12,
            side: PositionSide::Both,
            quantity: crate::unit_fixture::units(1),
            entry_bound: crate::unit_fixture::units(100),
            ownership: vec![],
        });
        snapshot.venue_available = vec![(1, crate::unit_fixture::units(990))];
        let baseline = CanonicalBaseline::new(snapshot.clone(), catalog.clone()).unwrap();
        let account = Account::from_snapshot(snapshot, catalog).unwrap();
        let assets:Vec<_>=[("BTC","1","1"),("USDT","1000","990")].into_iter().map(|(asset,wallet,available)|serde_json::json!({"asset":asset,"walletBalance":wallet,"crossWalletBalance":wallet,"marginBalance":wallet,"availableBalance":available,"unrealizedProfit":"0","maintMargin":"0","initialMargin":"0","positionInitialMargin":"0","openOrderInitialMargin":"0","crossUnPnl":"0"})).collect();
        let mut body = serde_json::json!({"totalInitialMargin":"10","totalMaintMargin":"0","totalUnrealizedProfit":"0","totalPositionInitialMargin":"10","totalOpenOrderInitialMargin":"0","totalCrossUnPnl":"0","assets":assets,"positions":[{"symbol":"BTCUSDT","positionSide":"BOTH","positionAmt":"1","entryPrice":"100","unrealizedProfit":"0","isolatedMargin":"0","isolatedWallet":"0","notional":"100","initialMargin":"10","maintMargin":"0"}]});
        let raw = serde_json::to_vec(&body).unwrap();
        assert!(verify_snapshot(Market::Usdm, &baseline, &account, &raw, b"[]", b"[]").is_ok());
        assert_eq!(
            venue_available(&baseline, &raw).unwrap(),
            vec![(1, crate::unit_fixture::units(990))]
        );
        body["positions"][0]["positionAmt"] = serde_json::json!("2");
        assert!(
            verify_snapshot(
                Market::Usdm,
                &baseline,
                &account,
                &serde_json::to_vec(&body).unwrap(),
                b"[]",
                b"[]"
            )
            .is_err()
        );
    }
}
