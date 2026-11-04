#[path = "cash_flows.rs"]
mod cash_flows;
#[path = "profile.rs"]
mod profile;
use super::*;
use crate::observe::Query;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::json;
use std::collections::BTreeSet;
const MAX_CAPTURE: usize = 24 * 1024 * 1024;
const MAX_PAGES: usize = 512;
const HOUR: u64 = 3_600_000;
fn version(v: SpotVenue) -> &'static str {
    match v {
        SpotVenue::Bybit => "bybit",
        SpotVenue::Okx => "okx",
    }
}
fn env_name(env: Environment) -> &'static str {
    if env == Environment::Production {
        "production"
    } else {
        "test"
    }
}
fn identity_path(v: SpotVenue) -> &'static str {
    if v == SpotVenue::Bybit {
        "/v5/user/query-api"
    } else {
        "/api/v5/account/config"
    }
}
fn payload(v: SpotVenue, b: &Value) -> Result<&Value> {
    if v == SpotVenue::Bybit {
        if b["retCode"].as_i64() != Some(0) {
            return Err(format!(
                "Bybit recovery endpoint failed: {:?}",
                b["retCode"].as_i64()
            )
            .into());
        }
        Ok(&b["result"])
    } else {
        let c = b["code"].as_str().ok_or("missing OKX status")?;
        if c != "0" {
            return Err(format!(
                "OKX recovery endpoint failed: {}",
                c.parse::<i64>().unwrap_or(-1)
            )
            .into());
        }
        Ok(&b["data"])
    }
}
fn items<'a>(v: SpotVenue, data: &'a Value, category: Option<&str>) -> Result<&'a [Value]> {
    let rows = if v == SpotVenue::Bybit {
        if category.is_some_and(|c| data["category"] != c) {
            return Err("unexpected Bybit result category".into());
        }
        &data["list"]
    } else {
        data
    };
    Ok(rows.as_array().ok_or("missing private result list")?)
}
fn identity(v: SpotVenue, data: &Value, expected: &str) -> Result<()> {
    let ok = if v == SpotVenue::Bybit {
        data["userID"]
            .as_u64()
            .is_some_and(|n| n.to_string() == expected)
    } else {
        data.as_array().is_some_and(|rows| {
            rows.len() == 1 && rows[0]["uid"] == expected && rows[0]["acctLv"] == "1"
        })
    };
    if !ok {
        return Err("authenticated account/profile mismatch".into());
    }
    Ok(())
}
// Bybit's query-api returns the API key and a remark. Persist only the native UID and status.
// OKX account config can contain labels and a main UID; only the account mode and UID are needed.
fn discard_sensitive_fields(v: SpotVenue, raw: &Value) -> Result<Value> {
    let data = payload(v, raw)?;
    Ok(if v == SpotVenue::Bybit {
        json!({"retCode":0,"result":{"userID":data["userID"]}})
    } else {
        let [r] = items(v, data, None)? else {
            return Err("invalid credential subject".into());
        };
        json!({"code":"0","data":[{"uid":r["uid"],"acctLv":r["acctLv"]}]})
    })
}
/// Read callback is the trusted signed transport. The returned document is reusable by replay_evidence.
pub fn acquire(
    v: SpotVenue,
    e: Environment,
    uid: &str,
    start: u64,
    end: u64,
    now: u64,
    read: impl FnMut(&str) -> Result<Response>,
) -> Result<RestEvidence> {
    acquire_for(v, e, uid, (start, end), now, &[], read)
}
pub fn acquire_for(
    v: SpotVenue,
    e: Environment,
    uid: &str,
    window: (u64, u64),
    now: u64,
    targets: &[OrderLookup],
    mut read: impl FnMut(&str) -> Result<Response>,
) -> Result<RestEvidence> {
    let mut pages = vec![];
    let mut size = 0;
    let mut facts = collect(v, e, uid, window, now, targets, |path| {
        if pages.len() >= MAX_PAGES {
            return Err("REST recovery page budget exceeded".into());
        }
        let response = read(path)?;
        if response.status != 200 {
            return Err(format!("REST recovery HTTP {}", response.status).into());
        }
        if response.body.len() > 2 * 1024 * 1024 {
            return Err("oversized recovery response".into());
        }
        let mut val: Value = serde_json::from_slice(&response.body)?;
        let bytes = if path == identity_path(v) {
            val = discard_sensitive_fields(v, &val)?;
            serde_json::to_vec(&val)?
        } else {
            response.body
        };
        size += bytes.len();
        if size > MAX_CAPTURE / 2 {
            return Err("REST recovery byte budget exceeded".into());
        }
        pages.push(json!([path, response.status, STANDARD.encode(bytes)]));
        Ok(val)
    })?;
    let encoded = serde_json::to_vec(
        &json!({"format":"tickwire.spot-recovery.v1","venue":version(v),"environment":env_name(e),"uid":uid,"start_ms":window.0,"end_ms":window.1,"venue_now_ms":now,
        "targets":targets.iter().map(|x|json!([x.symbol,x.client_id])).collect::<Vec<_>>(),"pages":pages}),
    )?;
    if encoded.len() > MAX_CAPTURE {
        return Err("REST recovery capture too large".into());
    }
    facts.evidence_bytes = encoded;
    Ok(facts)
}
pub fn replay_evidence(bytes: &[u8]) -> Result<RestEvidence> {
    if bytes.len() > MAX_CAPTURE {
        return Err("invalid REST capture size".into());
    }
    let d: Value = serde_json::from_slice(bytes)?;
    if d["format"] != "tickwire.spot-recovery.v1" {
        return Err("unknown REST capture".into());
    }
    let v = match d["venue"].as_str() {
        Some("bybit") => SpotVenue::Bybit,
        Some("okx") => SpotVenue::Okx,
        _ => return Err("unknown venue".into()),
    };
    let e = match d["environment"].as_str() {
        Some("production") => Environment::Production,
        Some("test") => Environment::Test,
        _ => return Err("unknown environment".into()),
    };
    let n = |k: &str| d[k].as_u64().ok_or("missing coverage time");
    let targets = d["targets"]
        .as_array()
        .ok_or("missing target list")?
        .iter()
        .map(|x| {
            Ok(OrderLookup {
                symbol: x[0].as_str().ok_or("invalid lookup")?.into(),
                client_id: x[1].as_str().ok_or("invalid lookup")?.into(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let pages = d["pages"]
        .as_array()
        .filter(|v| v.len() <= MAX_PAGES)
        .ok_or("invalid page count")?;
    let mut pages = pages.iter();
    let mut result = collect(
        v,
        e,
        d["uid"].as_str().ok_or("missing UID")?,
        (n("start_ms")?, n("end_ms")?),
        n("venue_now_ms")?,
        &targets,
        |path| {
            let p = pages.next().ok_or("truncated REST recording")?;
            if p[0] != path || p[1] != 200 {
                return Err("unexpected request, pagination or HTTP status".into());
            }
            let raw = STANDARD.decode(p[2].as_str().ok_or("missing response body")?)?;
            if raw.len() > 2 * 1024 * 1024 {
                return Err("oversized persisted REST page".into());
            }
            Ok(serde_json::from_slice(&raw)?)
        },
    )?;
    if pages.next().is_some() {
        return Err("unexpected unused REST pages".into());
    }
    result.evidence_bytes = bytes.to_vec();
    Ok(result)
}
type Read<'a> = dyn FnMut(&str) -> Result<Value> + 'a;
fn collect(
    v: SpotVenue,
    e: Environment,
    uid: &str,
    (start, end): (u64, u64),
    now: u64,
    targets: &[OrderLookup],
    mut read: impl FnMut(&str) -> Result<Value>,
) -> Result<RestEvidence> {
    let max_age = if v == SpotVenue::Okx {
        2 * HOUR
    } else {
        24 * HOUR
    };
    if uid.is_empty()
        || uid.len() > 32
        || !uid.bytes().all(|x| x.is_ascii_digit())
        || uid.bytes().all(|x| x == b'0')
        || start == 0
        || start >= end
        || end > now
        || now - start >= max_age
        || targets.len() > 128
    {
        return Err("invalid account or recovery coverage outside venue retention".into());
    }
    identity(v, payload(v, &read(identity_path(v))?)?, uid)?;
    profile::check(v, &mut read)?;
    let raw = read(&Query::Balance.path(v)?)?;
    let balances = rows::balances(v, items(v, payload(v, &raw)?, None)?)?;
    let mut opens = vec![];
    let mut history = vec![];
    let mut fills = vec![];
    let mut requested = vec![];
    for kind in ["open", "history", "fills"] {
        let mut cursor = None;
        let mut tokens = BTreeSet::new();
        let mut ids = BTreeSet::new();
        let mut bills = BTreeSet::new();
        loop {
            let q = match kind {
                "open" => Query::OpenOrders {
                    symbol: None,
                    cursor: cursor.as_deref(),
                }
                .path(v)?,
                "fills" => Query::Fills {
                    symbol: None,
                    window: Some((start, end)),
                    cursor: cursor.as_deref(),
                }
                .path(v)?,
                _ => {
                    let bybit = v == SpotVenue::Bybit;
                    let mut history_path = if bybit {
                        format!(
                            "/v5/order/history?category=spot&startTime={start}&endTime={end}&limit=50"
                        )
                    } else {
                        format!(
                            "/api/v5/trade/orders-history?instType=SPOT&begin={start}&end={end}&limit=100"
                        )
                    };
                    if let Some(c) = cursor.as_deref() {
                        crate::observe::append_cursor(&mut history_path, bybit, c)?;
                    }
                    history_path
                }
            };
            let raw = read(&q)?;
            let data = payload(v, &raw)?;
            let page = items(v, data, Some("spot"))?;
            let max = if v == SpotVenue::Bybit && kind != "fills" {
                50
            } else {
                100
            };
            if page.len() > max {
                return Err("venue exceeded requested page size".into());
            }
            for r in page {
                let id = if kind == "fills" {
                    let (f, filtered_ms) = rows::fill(v, r)?;
                    if !(start..=end).contains(&filtered_ms) {
                        return Err("trade outside native filter".into());
                    }
                    if v == SpotVenue::Okx
                        && !bills.insert(
                            r["billId"]
                                .as_str()
                                .filter(|s| !s.is_empty() && s.bytes().all(|c| c.is_ascii_digit()))
                                .ok_or("invalid bill ID")?
                                .to_owned(),
                        )
                    {
                        return Err("duplicate fill bill".into());
                    }
                    let id = (f.symbol.clone(), f.trade_id.clone());
                    fills.push(f);
                    id
                } else {
                    let o = rows::order(v, r)?;
                    let id = (o.symbol.clone(), o.native_id.clone());
                    if kind == "open" {
                        if !matches!(o.status, OrderStatus::New | OrderStatus::PartiallyFilled) {
                            return Err("unexpected state in open-order list".into());
                        }
                        opens.push(o);
                    } else {
                        if !(start..=end).contains(&o.created_ms) {
                            return Err("order outside creation-time filter".into());
                        }
                        history.push(o);
                    }
                    id
                };
                if !ids.insert(id) {
                    return Err("duplicate record during pagination".into());
                }
            }
            let next = if v == SpotVenue::Bybit {
                let s = data["nextPageCursor"]
                    .as_str()
                    .ok_or("missing Bybit continuation field")?;
                (!s.is_empty()).then_some(s)
            } else if !page.is_empty() {
                Some(
                    page.last().unwrap()[if kind == "fills" { "billId" } else { "ordId" }]
                        .as_str()
                        .ok_or("missing OKX page key")?,
                )
            } else {
                None
            };
            let Some(next) = next else { break };
            if next.len() > 1024
                || page.is_empty()
                || !tokens.insert(next.to_owned())
                || tokens.len() >= 128
            {
                return Err("invalid or repeated page token, or page budget exhausted".into());
            }
            if v == SpotVenue::Okx
                && cursor.as_deref().is_some_and(|old: &str| {
                    old.parse::<u128>()
                        .ok()
                        .zip(next.parse::<u128>().ok())
                        .is_none_or(|(a, b)| b >= a)
                })
            {
                return Err("nondecreasing OKX pagination frontier".into());
            }
            cursor = Some(next.to_owned());
        }
    }
    let bills = cash_flows::verify(v, start, end, &fills, &mut read)?;
    let mut lookup_ids = BTreeSet::new();
    for t in targets {
        if !lookup_ids.insert((&t.symbol, &t.client_id)) {
            return Err("duplicate local order lookup".into());
        }
        let p = Query::Order {
            symbol: &t.symbol,
            client_id: &t.client_id,
        }
        .path(v)?;
        let raw = read(&p)?;
        let data = payload(v, &raw)?;
        let page = items(v, data, Some("spot"))?;
        let found = if page.is_empty() && v == SpotVenue::Bybit {
            let fallback = p.replace("/v5/order/realtime", "/v5/order/history") + "&limit=50";
            let body = read(&fallback)?;
            let data = payload(v, &body)?;
            if data["nextPageCursor"] != "" {
                return Err("ambiguous historical client ID".into());
            }
            let [row] = items(v, data, Some("spot"))? else {
                return Err("missing known order history".into());
            };
            rows::order(v, row)?
        } else {
            let [r] = page else {
                return Err("missing or ambiguous known order".into());
            };
            rows::order(v, r)?
        };
        if found.client_id != t.client_id || found.symbol != t.symbol {
            return Err("incorrect targeted order identity".into());
        }
        requested.push(found);
    }
    identity(v, payload(v, &read(identity_path(v))?)?, uid)?;
    for orders in [&mut opens, &mut history, &mut requested] {
        orders.sort_by(|a, b| (&a.symbol, &a.native_id).cmp(&(&b.symbol, &b.native_id)));
    }
    fills.sort_by(|a, b| {
        (a.time_ms, &a.symbol, &a.trade_id).cmp(&(b.time_ms, &b.symbol, &b.trade_id))
    });
    // Client IDs are never reused within a managed account. When two native endpoints
    // disagree on the same order during a scan the next quiet cut must establish it.
    let mut clients = std::collections::BTreeMap::new();
    let mut native = std::collections::BTreeMap::new();
    for o in opens.iter().chain(history.iter()).chain(requested.iter()) {
        if clients
            .insert(&o.client_id, (&o.symbol, &o.native_id))
            .is_some_and(|old| old != (&o.symbol, &o.native_id))
            || native
                .insert((&o.symbol, &o.native_id), o)
                .is_some_and(|old| old != o)
        {
            return Err("inconsistent client or native order identity across endpoints".into());
        }
    }
    let ords = |orders: &[RemoteOrder]| {
        orders
            .iter()
            .map(|o| {
                json!([
                    o.symbol,
                    o.native_id,
                    o.client_id,
                    format!("{:?}", o.side),
                    format!("{:?}", o.status),
                    format!("{:?}", o.order_type),
                    o.time_in_force.map(|tif| format!("{tif:?}")),
                    o.qty.as_plain_string(),
                    o.cumulative.as_plain_string(),
                    o.price.map(Decimal::as_plain_string),
                    o.created_ms,
                    o.updated_ms
                ])
            })
            .collect::<Vec<_>>()
    };
    let view = json!([
        bills,
        version(v),
        env_name(e),
        uid,
        balances
            .iter()
            .map(|o| json!([
                o.symbol,
                o.total.as_plain_string(),
                o.available.as_plain_string(),
                o.locked.as_plain_string()
            ]))
            .collect::<Vec<_>>(),
        ords(&opens),
        ords(&history),
        ords(&requested),
        fills
            .iter()
            .map(|f| {
                let raw: Value = serde_json::from_slice(&f.wire_row).expect("validated fill");
                json!([
                    f.symbol,
                    f.native_id,
                    f.client_id,
                    f.trade_id,
                    format!("{:?}", f.side),
                    f.qty.as_plain_string(),
                    f.price.as_plain_string(),
                    f.fee.as_plain_string(),
                    f.fee_currency,
                    f.time_ms,
                    raw["execValue"],
                    raw["isMaker"],
                    raw["execType"]
                ])
            })
            .collect::<Vec<_>>()
    ]);
    let fingerprint = ring::digest::digest(&ring::digest::SHA256, &serde_json::to_vec(&view)?)
        .as_ref()
        .try_into()
        .expect("sha256");
    Ok(RestEvidence {
        venue: v,
        environment: e,
        uid: uid.into(),
        start_ms: start,
        end_ms: end,
        balances,
        open_orders: opens,
        history,
        target_orders: requested,
        fills,
        fingerprint,
        evidence_bytes: vec![],
    })
}
