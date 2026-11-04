use super::*;
use serde_json::json;
use std::collections::BTreeSet;

fn field<'a>(v: &'a Value, key: &str) -> Result<&'a str> {
    v.get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("invalid or missing recovery field {key}").into())
}
pub(super) fn decimal(s: &str) -> Result<Decimal> {
    s.parse()
        .map_err(|_| "invalid exact decimal in recovery response".into())
}
fn number(v: &Value, k: &str) -> Result<Decimal> {
    decimal(field(v, k)?)
}
fn time(v: &Value, k: &str) -> Result<u64> {
    field(v, k)?
        .parse::<u64>()
        .ok()
        .filter(|n| *n > 0)
        .ok_or_else(|| "invalid venue timestamp".into())
}
fn zero(v: &Value, k: &str) -> Result<()> {
    if number(v, k)?.coefficient() != 0 {
        return Err(format!("unsupported account exposure in {k}").into());
    }
    Ok(())
}
fn difference(a: Decimal, b: Decimal) -> Result<Decimal> {
    let e = a.exponent().min(b.exponent());
    let project = |v: Decimal| {
        10i128
            .checked_pow((i16::from(v.exponent()) - i16::from(e)) as u32)
            .and_then(|scale| v.coefficient().checked_mul(scale))
    };
    Decimal::new(
        project(a)
            .and_then(|x| x.checked_sub(project(b)?))
            .ok_or("balance precision overflow")?,
        e,
    )
    .map_err(|_| "balance precision out of range".into())
}
fn name<'a>(row: &'a Value, k: &str) -> Result<&'a str> {
    let s = field(row, k)?;
    if s.is_empty() || s.len() > 64 || !crate::symbol_ok(s) {
        return Err("invalid native identifier".into());
    }
    Ok(s)
}
fn ordinary_side(v: SpotVenue, row: &Value) -> Result<Side> {
    match (v, field(row, "side")?) {
        (SpotVenue::Bybit, "Buy") | (SpotVenue::Okx, "buy") => Ok(Side::Buy),
        (SpotVenue::Bybit, "Sell") | (SpotVenue::Okx, "sell") => Ok(Side::Sell),
        _ => Err("invalid native order side".into()),
    }
}
pub(super) fn balances(v: SpotVenue, rows: &[Value]) -> Result<Vec<RemoteBalance>> {
    let [wallet] = rows else {
        return Err("expected exactly one cash wallet".into());
    };
    let bybit = v == SpotVenue::Bybit;
    if bybit {
        if wallet["accountType"] != "UNIFIED" {
            return Err("unexpected wallet profile".into());
        }
        for k in [
            "totalPerpUPL",
            "totalInitialMargin",
            "totalMaintenanceMargin",
        ] {
            zero(wallet, k)?;
        }
    }
    let mut seen = BTreeSet::new();
    let mut balances = vec![];
    for r in wallet[if bybit { "coin" } else { "details" }]
        .as_array()
        .ok_or("missing complete wallet assets")?
    {
        let symbol = name(r, if bybit { "coin" } else { "ccy" })?.to_owned();
        if !seen.insert(symbol.clone()) {
            return Err("duplicate wallet currency".into());
        }
        let total = number(r, if bybit { "walletBalance" } else { "cashBal" })?;
        let locked = number(r, if bybit { "locked" } else { "frozenBal" })?;
        let available = difference(total, locked)?;
        if total.coefficient() < 0 || locked.coefficient() < 0 || available.coefficient() < 0 {
            return Err("negative cash balance".into());
        }
        if bybit {
            for k in ["borrowAmount", "spotBorrow"] {
                zero(r, k)?;
            }
            for k in [
                "unrealisedPnl",
                "accruedInterest",
                "spotHedgingQty",
                "bonus",
                "totalPositionIM",
                "totalPositionMM",
                "totalOrderIM",
            ] {
                if r.get(k).is_some_and(|x| x.as_str() != Some("")) {
                    zero(r, k)?;
                }
            }
            if number(r, "equity")? != total {
                return Err("wallet includes noncash equity".into());
            }
        } else {
            // Under independently verified acctLv=1, OKX uses an empty string for
            // unavailable margin metrics. Positive debt and malformed values still fail.
            let liab = field(r, "liab")?;
            if !liab.is_empty() {
                zero(r, "liab")?;
            }
            for k in [
                "rewardBal",
                "fixedBal",
                "spotIsoBal",
                "spotInUseAmt",
                "clSpotInUseAmt",
                "stgyEq",
            ] {
                if r.get(k).is_some_and(|x| x.as_str() != Some("")) {
                    zero(r, k)?;
                }
            }
            if number(r, "eq")? != total || number(r, "availBal")? != available {
                return Err("cash wallet does not reconcile".into());
            }
        }
        balances.push(RemoteBalance {
            symbol,
            total,
            available,
            locked,
        });
    }
    balances.sort_by(|a, b| a.symbol.cmp(&b.symbol));
    Ok(balances)
}
pub(super) fn order(v: SpotVenue, r: &Value) -> Result<RemoteOrder> {
    let b = v == SpotVenue::Bybit;
    let k = |a, bk| if b { a } else { bk };
    if (b
        && (field(r, "isLeverage")? != "0"
            || r.get("stopOrderType")
                .is_some_and(|x| !matches!(x.as_str(), Some("" | "UNKNOWN")))))
        || (!b
            && (field(r, "instType")? != "SPOT"
                || field(r, "tdMode")? != "cash"
                || r.get("algoId").is_some_and(|s| s.as_str() != Some(""))))
    {
        return Err("conditional, margin, or noncash order in recovery".into());
    }
    if b {
        if r.get("category").is_some_and(|c| c != "spot") {
            return Err("noncash order event".into());
        }
        for key in [
            "triggerPrice",
            "takeProfit",
            "stopLoss",
            "tpLimitPrice",
            "slLimitPrice",
        ] {
            if let Some(value) = r.get(key)
                && value.as_str() != Some("")
            {
                zero(r, key)?;
            }
        }
    } else if r
        .get("attachAlgoOrds")
        .is_some_and(|v| !v.as_array().is_some_and(Vec::is_empty))
    {
        return Err("attached algorithm on cash order".into());
    }
    let status = match (v, field(r, k("orderStatus", "state"))?) {
        (SpotVenue::Bybit, "New") | (SpotVenue::Okx, "live") => OrderStatus::New,
        (SpotVenue::Bybit, "PartiallyFilled") | (SpotVenue::Okx, "partially_filled") => {
            OrderStatus::PartiallyFilled
        }
        (SpotVenue::Bybit, "Filled") | (SpotVenue::Okx, "filled") => OrderStatus::Filled,
        (SpotVenue::Bybit, "Cancelled" | "PartiallyFilledCanceled")
        | (SpotVenue::Okx, "canceled" | "mmp_canceled") => OrderStatus::Canceled,
        (SpotVenue::Bybit, "Rejected") => OrderStatus::Rejected,
        _ => return Err("unsupported order state in recovery".into()),
    };
    let (order_type, time_in_force) = match (v, field(r, k("orderType", "ordType"))?) {
        (SpotVenue::Bybit, "Limit") => {
            let tif = match field(r, "timeInForce")? {
                "GTC" => TimeInForce::Gtc,
                "IOC" => TimeInForce::Ioc,
                "FOK" => TimeInForce::Fok,
                "PostOnly" => TimeInForce::PostOnly,
                _ => return Err("unsupported Bybit limit time in force".into()),
            };
            (OrderType::Limit, Some(tif))
        }
        (SpotVenue::Bybit, "Market") if field(r, "timeInForce")? == "IOC" => {
            (OrderType::Market, None)
        }
        (SpotVenue::Okx, "market") => (OrderType::Market, None),
        (SpotVenue::Okx, "limit") => (OrderType::Limit, Some(TimeInForce::Gtc)),
        (SpotVenue::Okx, "ioc") => (OrderType::Limit, Some(TimeInForce::Ioc)),
        (SpotVenue::Okx, "fok") => (OrderType::Limit, Some(TimeInForce::Fok)),
        (SpotVenue::Okx, "post_only") => (OrderType::Limit, Some(TimeInForce::PostOnly)),
        _ => return Err("unsupported cash order type or time in force".into()),
    };
    let price = if order_type == OrderType::Limit {
        let px = number(r, k("price", "px"))?;
        if !px.is_positive() {
            return Err("invalid limit price".into());
        }
        Some(px)
    } else {
        if field(r, k("marketUnit", "tgtCcy"))? != k("baseCoin", "base_ccy") {
            return Err("cash recovery requires base-sized market orders".into());
        }
        None
    };
    let client_id = field(r, k("orderLinkId", "clOrdId"))?;
    if !crate::client_ok(v, client_id) {
        return Err("order has no managed client identity".into());
    }
    let qty = number(r, k("qty", "sz"))?;
    let cumulative = number(r, k("cumExecQty", "accFillSz"))?;
    let left = difference(qty, cumulative)?;
    if !qty.is_positive()
        || cumulative.coefficient() < 0
        || left.coefficient() < 0
        || match status {
            OrderStatus::New | OrderStatus::Rejected => cumulative.coefficient() != 0,
            OrderStatus::Filled => left.coefficient() != 0,
            OrderStatus::PartiallyFilled => !cumulative.is_positive() || !left.is_positive(),
            _ => false,
        }
    {
        return Err("inconsistent native order quantity".into());
    }
    let created_ms = time(r, k("createdTime", "cTime"))?;
    let updated_ms = time(r, k("updatedTime", "uTime"))?;
    if updated_ms < created_ms {
        return Err("order update precedes creation".into());
    }
    Ok(RemoteOrder {
        symbol: name(r, k("symbol", "instId"))?.into(),
        native_id: name(r, k("orderId", "ordId"))?.into(),
        client_id: client_id.into(),
        side: ordinary_side(v, r)?,
        status,
        order_type,
        time_in_force,
        qty,
        cumulative,
        price,
        created_ms,
        updated_ms,
    })
}
pub(super) fn fill(v: SpotVenue, r: &Value) -> Result<(RemoteFill, u64)> {
    let b = v == SpotVenue::Bybit;
    let k = |a, bk| if b { a } else { bk };
    if b && (field(r, "execType")? != "Trade"
        || r.get("extraFees")
            .is_some_and(|x| !(x.as_str() == Some("") || x.as_array().is_some_and(Vec::is_empty))))
        || !b && (field(r, "instType")? != "SPOT" || !matches!(field(r, "subType")?, "1" | "2"))
    {
        return Err("unsupported native trade or additional fees".into());
    }
    let client_id = field(r, k("orderLinkId", "clOrdId"))?;
    if !crate::client_ok(v, client_id) {
        return Err("unmanaged execution in cash account".into());
    }
    let trade_id = name(r, k("execId", "tradeId"))?.to_owned();
    crate::fills::trade_id(v, &trade_id).map_err(|_| "invalid trade ID for venue")?;
    let price = number(r, k("execPrice", "fillPx"))?;
    let qty = number(r, k("execQty", "fillSz"))?;
    if !qty.is_positive() || !price.is_positive() {
        return Err("nonpositive execution size or price".into());
    }
    let fee = number(r, k("execFee", "fee"))?;
    let fee = if b {
        Decimal::new(
            fee.coefficient().checked_neg().ok_or("fee overflow")?,
            fee.exponent(),
        )
        .map_err(|_| "invalid fee")?
    } else {
        fee
    };
    let fee_currency = field(r, k("feeCurrency", "feeCcy"))?;
    if (!fee_currency.is_empty() && !crate::symbol_ok(fee_currency))
        || (fee.coefficient() != 0 && fee_currency.is_empty())
    {
        return Err("unknown fee currency".into());
    }
    let mut wire = r.clone();
    if b {
        wire["category"] = json!("spot");
        if !number(r, "execValue")?.is_positive() || r["isMaker"].as_bool().is_none() {
            return Err("missing reported trade notional or liquidity".into());
        }
    } else {
        if !matches!(field(r, "execType")?, "M" | "T") {
            return Err("unknown execution liquidity".into());
        }
        wire["fillFee"] = r["fee"].clone();
        wire["fillFeeCcy"] = r["feeCcy"].clone();
    }
    if !b
        && r.get("tradeQuoteCcy").is_some_and(|q| {
            q.as_str().filter(|x| !x.is_empty()).is_some_and(|x| {
                r["instId"]
                    .as_str()
                    .and_then(|s| s.rsplit_once('-'))
                    .is_none_or(|(_, ccy)| ccy != x)
            })
        })
    {
        return Err("alternate trade settlement currency unsupported".into());
    }
    let time_ms = time(r, k("execTime", "fillTime"))?;
    let cursor_ms = if b { time_ms } else { time(r, "ts")? };
    Ok((
        RemoteFill {
            symbol: name(r, k("symbol", "instId"))?.into(),
            native_id: name(r, k("orderId", "ordId"))?.into(),
            client_id: client_id.into(),
            trade_id,
            side: ordinary_side(v, r)?,
            qty,
            price,
            fee,
            fee_currency: fee_currency.into(),
            time_ms,
            wire_row: serde_json::to_vec(&wire)?,
        },
        cursor_ms,
    ))
}
