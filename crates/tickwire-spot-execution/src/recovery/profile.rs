//! Exclude noncash positions, loans, conditional orders and native bots.
use super::*;
fn bybit_pages(
    path: &str,
    category: &str,
    read: &mut Read<'_>,
    mut visit: impl FnMut(&Value) -> Result<()>,
) -> Result<()> {
    let mut next = String::new();
    let mut seen = BTreeSet::new();
    loop {
        let raw = read(&(path.to_owned() + &next))?;
        let d = payload(SpotVenue::Bybit, &raw)?;
        let rows = items(SpotVenue::Bybit, d, Some(category))?;
        for row in rows {
            visit(row)?;
        }
        let c = d["nextPageCursor"]
            .as_str()
            .ok_or("missing noncash continuation")?;
        if c.is_empty() {
            return Ok(());
        }
        if rows.is_empty() || !seen.insert(c.to_owned()) || seen.len() > 64 {
            return Err("noncash page cursor failed to advance".into());
        }
        next.clear();
        crate::observe::append_cursor(&mut next, true, c)?;
    }
}
pub(super) fn check(v: SpotVenue, read: &mut Read<'_>) -> Result<()> {
    if v == SpotVenue::Okx {
        for kind in [
            "conditional",
            "oco",
            "trigger",
            "move_order_stop",
            "iceberg",
            "twap",
            "smart_iceberg",
        ] {
            let raw = read(&format!(
                "/api/v5/trade/orders-algo-pending?instType=SPOT&ordType={kind}&limit=1"
            ))?;
            if !items(v, payload(v, &raw)?, None)?.is_empty() {
                return Err("active spot algorithm in exclusive account".into());
            }
        }
        for path in [
            "/api/v5/tradingBot/grid/orders-algo-pending?algoOrdType=grid&limit=1",
            "/api/v5/tradingBot/recurring/orders-algo-pending?limit=1",
        ] {
            let raw = read(path)?;
            if !items(v, payload(v, &raw)?, None)?.is_empty() {
                return Err("active spot bot in exclusive account".into());
            }
        }
        return Ok(());
    }
    let a = read("/v5/account/info")?;
    let profile = payload(v, &a)?;
    if profile["marginMode"] != "REGULAR_MARGIN"
        || !matches!(profile["unifiedMarginStatus"].as_u64(), Some(5 | 6))
    {
        return Err(
            "Bybit recovery requires UTA 2 regular margin account with no margin exposure".into(),
        );
    }
    let m = read("/v5/spot-margin-trade/state")?;
    if payload(v, &m)?["spotMarginMode"] != "0" {
        return Err("Bybit spot margin borrowing is enabled".into());
    }
    // REST requires a settlement filter for linear positions and orders. Discover active and
    // pre-market settlement currencies from the venue instead of assuming USDT/USDC forever.
    let mut settlements = BTreeSet::new();
    for status in ["", "&status=PreLaunch"] {
        bybit_pages(
            &format!("/v5/market/instruments-info?category=linear&limit=250{status}"),
            "linear",
            read,
            |r| {
                let s = r["settleCoin"]
                    .as_str()
                    .filter(|s| s.len() <= 32 && crate::symbol_ok(s))
                    .ok_or("invalid linear settlement currency")?;
                settlements.insert(s.to_owned());
                Ok(())
            },
        )?;
    }
    if settlements.is_empty() {
        return Err("empty linear instrument catalog".into());
    }
    let scopes = settlements
        .into_iter()
        .map(|s| ("linear", format!("&settleCoin={s}")))
        .chain([("inverse", String::new()), ("option", String::new())]);
    for (category, filter) in scopes {
        bybit_pages(
            &format!("/v5/order/realtime?category={category}{filter}&openOnly=0&limit=1"),
            category,
            read,
            |_| Err("noncash open order in exclusive account".into()),
        )?;
        bybit_pages(
            &format!("/v5/position/list?category={category}{filter}&limit=200"),
            category,
            read,
            |r| {
                if rows::decimal(r["size"].as_str().ok_or("unknown noncash position")?)?
                    .coefficient()
                    != 0
                {
                    return Err("noncash position in exclusive account".into());
                }
                Ok(())
            },
        )?;
    }
    Ok(())
}
