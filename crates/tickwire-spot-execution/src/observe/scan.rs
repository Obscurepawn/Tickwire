//! Collect bounded REST evidence for operator review. No credentials are persisted and the
//! observations never become an account capability or a contiguous private-event frontier.
use super::{Query, SpotVenue};
use serde_json::{Value, json};
use std::{collections::BTreeSet, error::Error};
type Result<T> = std::result::Result<T, Box<dyn Error>>;
fn checked(page: Value) -> Result<Value> {
    if page["success"] != true {
        return Err(format!(
            "{} failed with HTTP {} and exchange code {}",
            page["query"], page["http_status"], page["venue_code"]
        )
        .into());
    }
    Ok(page)
}
/// The caller authenticates each read on the same fixed venue, account keys and environment.
/// `end` is sampled exchange time. Refuse a wider interval than either native recent-fill API.
pub fn scan(
    venue: SpotVenue,
    expected_uid: &str,
    start: u64,
    end: u64,
    mut read: impl FnMut(Query<'_>) -> Result<Value>,
) -> Result<Value> {
    if expected_uid.is_empty()
        || expected_uid.len() > 32
        || !expected_uid.bytes().all(|c| c.is_ascii_digit())
        || start == 0
        || start >= end
        || end - start > 3 * 86_400_000
    {
        return Err("invalid expected account UID or fill window (up to three days)".into());
    }
    let check_uid = |page: Value| -> Result<()> {
        if checked(page)?["rows"][0]["uid"] != expected_uid {
            return Err("authenticated user does not match expected venue UID".into());
        }
        Ok(())
    };
    check_uid(read(Query::Identity)?)?;
    let balance = checked(read(Query::Balance)?)?;
    if balance["rows"]
        .as_array()
        .is_none_or(|rows| rows.len() != 1)
    {
        return Err("missing account balance snapshot".into());
    }
    let mut pages = |fills: bool| -> Result<Value> {
        let mut next = None;
        let mut cursors = BTreeSet::new();
        let mut ids = BTreeSet::new();
        let mut rows = Vec::new();
        let mut size = 0;
        for n in 1..=128 {
            let cursor = next.as_deref();
            let query = if fills {
                Query::Fills {
                    symbol: None,
                    window: Some((start, end)),
                    cursor,
                }
            } else {
                Query::OpenOrders {
                    symbol: None,
                    cursor,
                }
            };
            let page = checked(read(query)?)?;
            for row in page["rows"].as_array().ok_or("missing evidence rows")? {
                let bybit = venue == SpotVenue::Bybit;
                let idkey = match (bybit, fills) {
                    (true, true) => "execId",
                    (false, true) => "billId",
                    (true, false) => "orderId",
                    (false, false) => "ordId",
                };
                let symbol = row[if bybit { "symbol" } else { "instId" }]
                    .as_str()
                    .ok_or("missing instrument identifier")?;
                let id = row[idkey]
                    .as_str()
                    .filter(|v| !v.is_empty() && v.len() <= 128)
                    .ok_or("missing record identifier")?;
                if !ids.insert((symbol.to_owned(), id.to_owned())) {
                    return Err("repeated record between REST pages".into());
                }
                if fills {
                    let ts = row[if bybit { "execTime" } else { "ts" }]
                        .as_str()
                        .and_then(|v| v.parse::<u64>().ok())
                        .ok_or("missing execution timestamp")?;
                    if !(start..=end).contains(&ts) {
                        return Err("trade outside requested time window".into());
                    }
                }
                size += serde_json::to_vec(row)?.len();
                if size > 8 * 1024 * 1024 {
                    return Err("observation size budget exceeded".into());
                }
                rows.push(row.clone());
            }
            match page["next_cursor"].as_str() {
                None => return Ok(json!({"pages":n,"rows":rows,"pagination_exhausted":true})),
                Some(token) if token.len() <= 1024 && cursors.insert(token.to_owned()) => {
                    next = Some(token.to_owned())
                }
                _ => return Err("invalid or repeated continuation token".into()),
            }
        }
        Err("observation page budget exceeded".into())
    };
    let open = pages(false)?;
    let fills = pages(true)?;
    check_uid(read(Query::Identity)?)?;
    Ok(
        json!({"success":true,"venue":venue.id(),"account_uid":expected_uid,"coverage":"non_atomic_rest_scan","fresh_risk_qualified":false,
        "fill_filter_start_ms":start,"fill_filter_end_ms":end,"balance":balance,"open_orders":open,"fills":fills}),
    )
}
