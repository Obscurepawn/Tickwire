//! Detect cash movements even when a deposit and a withdrawal leave the final balance unchanged.
use super::{RemoteFill, Result, SpotVenue, Value, items, payload};
use std::collections::{BTreeMap, BTreeSet};

pub(super) fn verify(
    venue: SpotVenue,
    start: u64,
    end: u64,
    fills: &[RemoteFill],
    read: &mut impl FnMut(&str) -> Result<Value>,
) -> Result<BTreeSet<String>> {
    let bybit = venue == SpotVenue::Bybit;
    let base = if bybit {
        format!(
            "/v5/account/transaction-log?accountType=UNIFIED&startTime={start}&endTime={end}&limit=50"
        )
    } else {
        format!("/api/v5/account/bills?begin={start}&end={end}&limit=100")
    };
    let expected: BTreeMap<_, _> = fills
        .iter()
        .enumerate()
        .map(|(i, f)| {
            (
                (f.symbol.as_str(), f.native_id.as_str(), f.trade_id.as_str()),
                i,
            )
        })
        .collect();
    let mut matched = BTreeSet::new();
    let mut ids = BTreeSet::new();
    let mut cursors = BTreeSet::new();
    let mut path = base.clone();
    for _ in 0..128 {
        let body = read(&path)?;
        let data = payload(venue, &body)?;
        let page = items(venue, data, None)?;
        if page.len() > if bybit { 50 } else { 100 } {
            return Err("oversized cash bill page".into());
        }
        for row in page {
            let field = |key: &str| {
                row[key]
                    .as_str()
                    .filter(|s| !s.is_empty())
                    .ok_or("invalid cash bill identity")
            };
            let (symbol, order, trade, time, id) = if bybit {
                if field("category")? != "spot" || field("type")? != "TRADE" {
                    return Err("cash activity outside the managed spot trades".into());
                }
                (
                    field("symbol")?,
                    field("orderId")?,
                    field("tradeId")?,
                    field("transactionTime")?,
                    field("id")?,
                )
            } else {
                if field("type")? != "2"
                    || field("instType")? != "SPOT"
                    || !matches!(field("subType")?, "1" | "2")
                {
                    return Err("cash activity outside the managed spot trades".into());
                }
                (
                    field("instId")?,
                    field("ordId")?,
                    field("tradeId")?,
                    field("ts")?,
                    field("billId")?,
                )
            };
            // Bybit can reuse a transaction ID. Each currency leg still belongs to one trade.
            let identity = if bybit {
                serde_json::to_string(&(symbol, order, trade, field("currency")?, id))?
            } else {
                id.to_owned()
            };
            if !(start..=end).contains(&time.parse()?) || !ids.insert(identity) {
                return Err("duplicate or out-of-window cash bill".into());
            }
            let index = expected
                .get(&(symbol, order, trade))
                .ok_or("cash bill missing its native fill")?;
            matched.insert(*index);
        }
        let cursor = if bybit {
            let next = data["nextPageCursor"]
                .as_str()
                .ok_or("missing cash bill continuation")?;
            (!next.is_empty()).then_some(next)
        } else if !page.is_empty() {
            Some(
                page.last().unwrap()["billId"]
                    .as_str()
                    .ok_or("missing last bill ID")?,
            )
        } else {
            None
        };
        let Some(cursor) = cursor else {
            if matched.len() != expected.len() {
                return Err("a native fill is missing its cash bill".into());
            }
            return Ok(ids);
        };
        if page.is_empty() || !cursors.insert(cursor.to_owned()) {
            return Err("cash bill pagination made no progress".into());
        }
        path.clone_from(&base);
        crate::observe::append_cursor(&mut path, bybit, cursor)?;
    }
    Err("cash bill pagination limit reached".into())
}
