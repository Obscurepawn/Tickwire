//! Match the exchange inventory against the economic ledger without assigning remote cash to a strategy.
use super::*;
use std::collections::{BTreeMap, BTreeSet};
use tickwire_account::{Account, Fixed, OrderKey};
use tickwire_account_codec::CanonicalBaseline;
use tickwire_spot_execution::recovery::RemoteOrder;
use tickwire_venue::{Decimal, OrderStatus, OrderType, Side};
pub(super) fn amount(d: Decimal, scale: u8) -> Result<Fixed> {
    Fixed::parse(&d.as_plain_string(), scale)
        .map_err(|_| Error::Origin("amount outside the asset scale"))
}
fn quote(
    venue: tickwire_spot_execution::SpotVenue,
    row: &serde_json::Value,
    qty: Decimal,
    price: Decimal,
) -> Result<Decimal> {
    if venue == tickwire_spot_execution::SpotVenue::Bybit {
        row["execValue"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .ok_or(Error::Origin("missing exact native trade quote"))
    } else {
        Decimal::new(
            qty.coefficient()
                .checked_mul(price.coefficient())
                .ok_or(Error::Origin("trade value overflow"))?,
            qty.exponent()
                .checked_add(price.exponent())
                .ok_or(Error::Origin("trade value scale"))?,
        )
        .map_err(|_| Error::Origin("trade value scale"))
    }
}
pub(super) fn open(s: OrderStatus) -> bool {
    matches!(s, OrderStatus::New | OrderStatus::PartiallyFilled)
}
pub(super) fn orders(e: &RestEvidence) -> Result<BTreeMap<&str, &RemoteOrder>> {
    let mut all = BTreeMap::new();
    let mut native = BTreeMap::new();
    let mut open_set = BTreeSet::new();
    for o in &e.open_orders {
        if !open(o.status) || !open_set.insert(o.client_id.as_str()) {
            return Err(Error::Origin("invalid or duplicate open order"));
        }
    }
    for o in e
        .history
        .iter()
        .chain(&e.target_orders)
        .chain(&e.open_orders)
    {
        if all
            .insert(o.client_id.as_str(), o)
            .is_some_and(|prior| prior != o)
            || native
                .insert(
                    (o.symbol.as_str(), o.native_id.as_str()),
                    o.client_id.as_str(),
                )
                .is_some_and(|prior| prior != o.client_id)
            || open(o.status) != open_set.contains(o.client_id.as_str())
        {
            return Err(Error::Origin(
                "conflicting history, targeted lookup or open order inventory",
            ));
        }
    }
    Ok(all)
}

pub(super) fn admitted_terms(account: &Account, k: &OrderKey, remote: &RemoteOrder) -> Result<()> {
    let (instrument, order) = account
        .order_terms(k)
        .ok_or(Error::Origin("unknown remote order"))?;
    if remote.symbol != instrument.symbol.as_str()
        || remote.client_id != k.client_id.as_str()
        || remote.side != order.side
        || remote.order_type != order.order_type
        || remote.time_in_force != order.time_in_force
        || order.price != remote.price
        || order.quantity != tickwire_venue::Quantity::Base(remote.qty)
        || account
            .venue_order_id(k)
            .is_some_and(|id| id.as_str() != remote.native_id)
    {
        return Err(Error::Origin(
            "remote order terms differ from the admitted command",
        ));
    }
    Ok(())
}
pub(super) fn terms(account: &Account, k: &OrderKey, remote: &RemoteOrder) -> Result<()> {
    admitted_terms(account, k, remote)?;
    let view = account
        .order(k)
        .ok_or(Error::Origin("missing local order"))?;
    let cum = amount(remote.cumulative, view.filled_quantity.scale())?;
    let valid = match remote.status {
        OrderStatus::New | OrderStatus::Rejected => cum.atoms() == 0,
        OrderStatus::PartiallyFilled => {
            cum.atoms() > 0 && cum.atoms() < view.original_quantity.atoms()
        }
        OrderStatus::Filled => cum == view.original_quantity,
        OrderStatus::Canceled | OrderStatus::Expired => {
            cum.atoms() >= 0 && cum.atoms() <= view.original_quantity.atoms()
        }
        _ => false,
    };
    if !valid || view.aborted || (view.terminal && open(remote.status)) {
        return Err(Error::Origin(
            "unknown or contradictory native order lifecycle",
        ));
    }
    Ok(())
}
pub(super) fn account(
    baseline: &CanonicalBaseline,
    a: &Account,
    e: &RestEvidence,
) -> Result<Vec<(u64, Fixed)>> {
    let remote = orders(e)?;
    let mut matched = BTreeSet::new();
    let mut holds: BTreeMap<String, Fixed> = BTreeMap::new();
    for k in a.order_keys() {
        let local = a.order(k).ok_or(Error::Origin("missing order state"))?;
        let Some(o) = remote.get(k.client_id.as_str()).copied() else {
            if local.aborted || (local.terminal && local.fee_reconciled) {
                continue;
            }
            return Err(Error::Origin(
                "outstanding admission missing from venue evidence",
            ));
        };
        terms(a, k, o)?;
        if !matched.insert(k.client_id.as_str())
            || local.terminal == open(o.status)
            || amount(o.cumulative, local.filled_quantity.scale())? != local.filled_quantity
            || a.venue_order_id(k)
                .is_none_or(|id| id.as_str() != o.native_id)
        {
            return Err(Error::Origin(
                "order lifecycle or fills do not close the ledger",
            ));
        }
        if open(o.status) {
            let (i, _) = a.order_terms(k).unwrap();
            let (asset, locked) = match o.side {
                Side::Sell => (&i.base, local.remaining_quantity),
                Side::Buy if o.order_type == OrderType::Limit => {
                    let price = o.price.ok_or(Error::Origin("open buy without price"))?;
                    let qty = o.qty.coefficient();
                    let scale = local.original_quantity.scale();
                    let rem = local.remaining_quantity;
                    if qty <= 0 || rem.atoms() <= 0 {
                        return Err(Error::Origin("invalid open quantity"));
                    }
                    let notional = Decimal::new(
                        rem.atoms()
                            .checked_mul(price.coefficient())
                            .ok_or(Error::Origin("open notional overflow"))?,
                        price
                            .exponent()
                            .checked_sub(scale as i8)
                            .ok_or(Error::Origin("open notional scale"))?,
                    )
                    .map_err(|_| Error::Origin("open notional out of range"))?;
                    (&i.quote, amount(notional, (-i.quote.exponent) as u8)?)
                }
                _ => return Err(Error::Origin("cannot establish a live market buy lock")),
            };
            let current = holds
                .entry(asset.symbol.as_str().into())
                .or_insert(locked.zero_like());
            *current = current
                .checked_add(locked)
                .map_err(|_| Error::Origin("venue lock overflow"))?;
        }
    }
    for f in &e.fills {
        let mut owned = a
            .order_keys()
            .filter(|k| k.client_id.as_str() == f.client_id);
        let key = owned.next().ok_or(Error::Origin("unowned execution"))?;
        let (instrument, spec) = a
            .order_terms(key)
            .ok_or(Error::Origin("missing executed order terms"))?;
        if owned.next().is_some()
            || instrument.symbol.as_str() != f.symbol
            || spec.side != f.side
            || a.venue_order_id(key)
                .is_none_or(|id| id.as_str() != f.native_id)
        {
            return Err(Error::Origin("execution owner differs"));
        }
        let fees = if f.fee.coefficient() == 0 {
            vec![]
        } else {
            let asset = baseline
                .snapshot()
                .assets
                .iter()
                .find(|a| a.asset.symbol.as_str() == f.fee_currency)
                .ok_or(Error::Origin("unknown fee currency"))?;
            vec![tickwire_venue::AssetDelta {
                asset: asset.asset.clone(),
                amount: f.fee,
            }]
        };
        let trade = tickwire_spot_execution::trade_id(e.venue, &f.trade_id)
            .map_err(|_| Error::Origin("invalid venue trade ID"))?;
        let wire =
            serde_json::from_slice(&f.wire_row).map_err(|_| Error::Origin("invalid saved fill"))?;
        let value = quote(e.venue, &wire, f.qty, f.price)?;
        if !a.matches_spot_fill(key, &trade, f.qty, f.price, value, &fees) {
            return Err(Error::Origin(
                "execution price, quantity or fee not recorded by the ledger",
            ));
        }
    }
    if matched.len() != remote.len() {
        return Err(Error::Origin("unowned venue order"));
    }
    let mut balances = BTreeMap::new();
    for r in &e.balances {
        if balances.insert(r.symbol.as_str(), r).is_some() {
            return Err(Error::Origin("duplicate cash balance"));
        }
    }
    let s = baseline.snapshot();
    if !s.venue_available.is_empty() || !s.positions.is_empty() {
        return Err(Error::Origin("expected a dedicated cash spot baseline"));
    }
    let mut symbols = BTreeSet::new();
    for b in &s.assets {
        let sym = b.asset.symbol.as_str();
        if !symbols.insert(sym) || b.nontrade_locked.atoms() != 0 || b.locked.atoms() != 0 {
            return Err(Error::Origin(
                "ambiguous asset or pre-existing baseline holds",
            ));
        }
        let v = a
            .asset(b.key)
            .map_err(|_| Error::Origin("missing local wallet"))?;
        let want_locked = holds.remove(sym).unwrap_or(v.total.zero_like());
        let Some(r) = balances.remove(sym) else {
            if v.total.atoms() == 0 && want_locked.atoms() == 0 {
                continue;
            }
            return Err(Error::Origin("funded asset missing on venue"));
        };
        let free = amount(r.available, v.total.scale())?;
        let locked = amount(r.locked, v.total.scale())?;
        if amount(r.total, v.total.scale())? != v.total
            || free.atoms() < 0
            || locked != want_locked
            || free.checked_add(locked).ok() != Some(v.total)
            || v.available.atoms() > free.atoms()
            || v.deficit.atoms() != 0
        {
            return Err(Error::Origin(
                "wallet total, spendable cash or open-order locks differ",
            ));
        }
    }
    if !holds.is_empty()
        || balances.values().any(|r| {
            r.total.coefficient() != 0
                || r.locked.coefficient() != 0
                || r.available.coefficient() != 0
        })
    {
        return Err(Error::Origin("venue contains an unmanaged asset or hold"));
    }
    // The account's per-wallet collateral cap applies to derivatives. Spot tracks each asset.
    Ok(vec![])
}

pub(super) fn private_row(
    baseline: &CanonicalBaseline,
    account: &Account,
    channel: tickwire_spot_execution::private::stream::Channel,
    row: &serde_json::Value,
) -> Result<()> {
    use tickwire_spot_execution::{
        SpotVenue, private::stream::Channel, recovery::decode_order_row,
    };
    let venue =
        SpotVenue::from_id(account.scope().venue).ok_or(Error::Origin("invalid cash venue"))?;
    let bybit = venue == SpotVenue::Bybit;
    let field = |a, b| {
        row[if bybit { a } else { b }].as_str().ok_or(Error::Origin(
            "private row lacks mandatory identity or economics",
        ))
    };
    if channel == Channel::Balance {
        return Ok(());
    }
    let client = field("orderLinkId", "clOrdId")?;
    let mut owners = account
        .order_keys()
        .filter(|k| k.client_id.as_str() == client);
    let key = owners
        .next()
        .ok_or(Error::Origin("private row has no admitted order"))?;
    let (instrument, spec) = account
        .order_terms(key)
        .ok_or(Error::Origin("missing private order terms"))?;
    if owners.next().is_some()
        || field("symbol", "instId")? != instrument.symbol.as_str()
        || account
            .venue_order_id(key)
            .is_none_or(|n| n.as_str() != field("orderId", "ordId").unwrap_or(""))
    {
        return Err(Error::Origin(
            "private native order does not match local origin",
        ));
    }
    if channel == Channel::Orders {
        let o =
            decode_order_row(venue, row).map_err(|_| Error::Origin("unexplained private order"))?;
        let v = account.order(key).unwrap();
        if o.side != spec.side
            || o.price != spec.price
            || o.order_type != spec.order_type
            || o.time_in_force != spec.time_in_force
            || v::Quantity::Base(o.qty) != spec.quantity
            || amount(o.cumulative, v.filled_quantity.scale())?.atoms() > v.filled_quantity.atoms()
            || !open(o.status) && !v.terminal
        {
            return Err(Error::Origin("private order not closed by REST and WAL"));
        }
    }
    if channel == Channel::Fills
        || (!bybit
            && row["tradeId"]
                .as_str()
                .is_some_and(|s| !s.is_empty() && s != "0"))
    {
        let decimal = |a, b| {
            field(a, b)?
                .parse::<Decimal>()
                .map_err(|_| Error::Origin("invalid private trade amount"))
        };
        let native = tickwire_spot_execution::trade_id(venue, field("execId", "tradeId")?)
            .map_err(|_| Error::Origin("invalid private trade ID"))?;
        let fee = decimal("execFee", "fillFee")?;
        let signed = if bybit {
            Decimal::new(
                fee.coefficient()
                    .checked_neg()
                    .ok_or(Error::Origin("fee overflow"))?,
                fee.exponent(),
            )
            .map_err(|_| Error::Origin("fee overflow"))?
        } else {
            fee
        };
        let fees = if signed.coefficient() == 0 {
            vec![]
        } else {
            let ccy = field("feeCurrency", "fillFeeCcy")?;
            let asset = baseline
                .snapshot()
                .assets
                .iter()
                .find(|b| b.asset.symbol.as_str() == ccy)
                .ok_or(Error::Origin("unknown private fee currency"))?;
            vec![v::AssetDelta {
                asset: asset.asset.clone(),
                amount: signed,
            }]
        };
        let expected_side = match (bybit, spec.side) {
            (true, Side::Buy) => "Buy",
            (true, Side::Sell) => "Sell",
            (false, Side::Buy) => "buy",
            (false, Side::Sell) => "sell",
        };
        if field("side", "side")? != expected_side
            || (bybit
                && (field("category", "instType")? != "spot"
                    || field("execType", "execType")? != "Trade"
                    || row.get("extraFees").is_some_and(|v| {
                        v.as_str() != Some("") && !v.as_array().is_some_and(Vec::is_empty)
                    })))
            || (!bybit && field("category", "instType")? != "SPOT")
        {
            return Err(Error::Origin(
                "private trade side, product or fee type differs",
            ));
        }
        let qty = decimal("execQty", "fillSz")?;
        let price = decimal("execPrice", "fillPx")?;
        let value = quote(venue, row, qty, price)?;
        if !account.matches_spot_fill(key, &native, qty, price, value, &fees) {
            return Err(Error::Origin(
                "private trade missing or conflicting with ledger",
            ));
        }
    }
    Ok(())
}
use tickwire_venue as v;

#[cfg(test)]
#[path = "compare_tests.rs"]
mod tests;
