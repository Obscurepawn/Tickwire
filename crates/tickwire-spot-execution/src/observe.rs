//! Explicit allowlist of private read endpoints. Output retains one page of exchange evidence.
use crate::{Credentials, Environment, Request, SpotVenue, client_ok, sign, symbol_ok};
use serde_json::{Value, json};
use tickwire_transport::http::Response;
mod scan;
pub use scan::scan;

#[derive(Clone, Copy)]
pub enum Query<'a> {
    Order {
        symbol: &'a str,
        client_id: &'a str,
    },
    OpenOrders {
        symbol: Option<&'a str>,
        cursor: Option<&'a str>,
    },
    Fills {
        window: Option<(u64, u64)>,
        symbol: Option<&'a str>,
        cursor: Option<&'a str>,
    },
    Balance,
    Identity,
}
impl Query<'_> {
    pub fn name(self) -> &'static str {
        match self {
            Self::Order { .. } => "order",
            Self::OpenOrders { .. } => "open-orders",
            Self::Fills { .. } => "fills",
            Self::Balance => "balance",
            Self::Identity => "identity",
        }
    }
    pub fn path(self, venue: SpotVenue) -> Result<String, &'static str> {
        let bybit = venue == SpotVenue::Bybit;
        let (symbol, cursor, path) = match self {
            Self::Order { symbol, client_id } => {
                if symbol.len() > 64 || !symbol_ok(symbol) || !client_ok(venue, client_id) {
                    return Err("invalid instrument or client order identifier");
                }
                return Ok(if bybit {
                    format!(
                        "/v5/order/realtime?category=spot&symbol={symbol}&orderLinkId={client_id}"
                    )
                } else {
                    format!("/api/v5/trade/order?instId={symbol}&clOrdId={client_id}")
                });
            }
            Self::Identity => {
                return Ok(if bybit {
                    "/v5/user/query-api"
                } else {
                    "/api/v5/account/config"
                }
                .into());
            }
            Self::Balance => {
                return Ok(if bybit {
                    "/v5/account/wallet-balance?accountType=UNIFIED"
                } else {
                    "/api/v5/account/balance"
                }
                .into());
            }
            Self::OpenOrders { symbol, cursor } => (
                symbol,
                cursor,
                if bybit {
                    "/v5/order/realtime"
                } else {
                    "/api/v5/trade/orders-pending"
                },
            ),
            Self::Fills { symbol, cursor, .. } => (
                symbol,
                cursor,
                if bybit {
                    "/v5/execution/list"
                } else {
                    "/api/v5/trade/fills"
                },
            ),
        };
        let mut target = if bybit {
            format!("{path}?category=spot")
        } else {
            format!("{path}?instType=SPOT")
        };
        if let Some(symbol) = symbol {
            if symbol.len() > 64 || !symbol_ok(symbol) {
                return Err("invalid instrument");
            }
            target.push_str(if bybit { "&symbol=" } else { "&instId=" });
            target.push_str(symbol);
        }
        if let Self::Fills {
            window: Some((start, end)),
            ..
        } = self
        {
            if start == 0 || start > end || end - start > 3 * 86400000 {
                return Err("invalid trade history window");
            }
            use std::fmt::Write;
            if bybit {
                write!(target, "&startTime={start}&endTime={end}").expect("String");
            } else {
                write!(target, "&begin={start}&end={end}").expect("String");
            }
        }
        if bybit && matches!(self, Self::OpenOrders { .. }) {
            target.push_str("&openOnly=0");
        }
        target.push_str(if bybit && matches!(self, Self::OpenOrders { .. }) {
            "&limit=50"
        } else {
            "&limit=100"
        });
        if let Some(cursor) = cursor {
            if cursor.is_empty()
                || cursor.len() > 1024
                || !cursor.bytes().all(|b| (0x21..=0x7e).contains(&b))
            {
                return Err("invalid page token");
            }
            if !bybit && !cursor.bytes().all(|b| b.is_ascii_digit()) {
                return Err("invalid OKX page token");
            }
            target.push_str(if bybit { "&cursor=" } else { "&after=" });
            let mut bytes = cursor.bytes().peekable();
            while let Some(b) = bytes.next() {
                // Bybit returns ready-escaped tokens such as 132766%3A2%2C132766%3A2.
                if b == b'%' && bytes.clone().take(2).filter(u8::is_ascii_hexdigit).count() == 2 {
                    target.push('%');
                    for h in bytes.by_ref().take(2) {
                        target.push(h as char);
                    }
                } else if b.is_ascii_alphanumeric() || b"-_.~".contains(&b) {
                    target.push(b as char)
                } else {
                    use std::fmt::Write;
                    write!(&mut target, "%{b:02X}").expect("String")
                }
            }
        }
        Ok(target)
    }
    pub fn signed_get(
        self,
        credentials: &Credentials<'_>,
        environment: Environment,
        utc_ms: u64,
    ) -> Result<Vec<u8>, &'static str> {
        if !credentials.valid() {
            return Err("invalid credential material");
        }
        sign(
            credentials,
            environment,
            utc_ms,
            &Request {
                method: "GET",
                path: self.path(credentials.venue)?,
                body: vec![],
            },
        )
        .map_err(|_| "could not sign private read request")
    }
    /// Retains the actual HTTP and venue error codes, without echoing request headers,
    /// credential material, or remote diagnostic messages. 'Single page' is a coverage boundary.
    pub fn evidence(self, venue: SpotVenue, http: Response) -> Result<Value, &'static str> {
        let mut page = json!({"venue":venue.id(),"query":self.name(),"http_status":http.status,"success":false,"coverage":"one_response","rows":[]});
        if !(200..300).contains(&http.status) {
            return Ok(page);
        }
        if http.body.len() > 2 * 1024 * 1024 {
            return Err("response exceeds evidence limit");
        }
        let body: Value =
            serde_json::from_slice(&http.body).map_err(|_| "invalid JSON response")?;
        let bybit = venue == SpotVenue::Bybit;
        let code = if bybit {
            body["retCode"].as_i64().map(|n| n.to_string())
        } else {
            body["code"].as_str().map(str::to_owned)
        }
        .ok_or("missing venue status")?;
        if code.is_empty()
            || code.len() > 32
            || !code.bytes().all(|b| b.is_ascii_digit() || b == b'-')
        {
            return Err("invalid venue status");
        }
        page["venue_code"] = json!(code);
        if code != "0" {
            return Ok(page);
        }
        if matches!(self, Self::Identity) {
            let uid = if bybit {
                body["result"]["userID"].as_u64().map(|v| v.to_string())
            } else {
                body["data"]
                    .as_array()
                    .filter(|r| r.len() == 1)
                    .and_then(|r| r[0]["uid"].as_str())
                    .map(str::to_owned)
            };
            let uid = uid
                .filter(|s| {
                    !s.is_empty()
                        && s.len() <= 32
                        && s.bytes().all(|c| c.is_ascii_digit())
                        && !s.bytes().all(|c| c == b'0')
                })
                .ok_or("invalid authenticated user identity")?;
            page["rows"] = json!([{"uid":uid}]);
            page["success"] = json!(true);
            return Ok(page);
        }
        let rows = if bybit {
            &body["result"]["list"]
        } else {
            &body["data"]
        };
        let rows = rows.as_array().ok_or("missing result rows")?;
        let expected_symbol = match self {
            Self::Order { symbol, .. } => Some(symbol),
            Self::OpenOrders { symbol, .. } | Self::Fills { symbol, .. } => symbol,
            _ => None,
        };
        let trade_query = !matches!(self, Self::Balance);
        if bybit && trade_query && body["result"]["category"].as_str() != Some("spot") {
            return Err("unexpected result category");
        }
        if matches!(self, Self::Order { .. }) && rows.len() > 1 {
            return Err("unexpected number of order matches");
        }
        for row in rows {
            if trade_query {
                let symbol = row[if bybit { "symbol" } else { "instId" }]
                    .as_str()
                    .ok_or("missing result instrument")?;
                if !symbol_ok(symbol)
                    || symbol.len() > 64
                    || expected_symbol.is_some_and(|s| s != symbol)
                    || (!bybit && row["instType"] != "SPOT")
                {
                    return Err("unexpected result instrument");
                }
            }
            if let Self::Order { client_id, .. } = self
                && row[if bybit { "orderLinkId" } else { "clOrdId" }].as_str() != Some(client_id)
            {
                return Err("order identity mismatch");
            }
            if matches!(self, Self::Balance)
                && bybit
                && row["accountType"].as_str() != Some("UNIFIED")
            {
                return Err("unexpected wallet account");
            }
        }
        let cursor = if matches!(self, Self::OpenOrders { .. } | Self::Fills { .. }) {
            if bybit {
                let value = body["result"]["nextPageCursor"]
                    .as_str()
                    .ok_or("missing continuation field")?;
                (!value.is_empty()).then_some(value)
            } else if rows.len() == 100 {
                Some(
                    rows.last()
                        .and_then(|r| {
                            r[if matches!(self, Self::Fills { .. }) {
                                "billId"
                            } else {
                                "ordId"
                            }]
                            .as_str()
                        })
                        .ok_or("missing continuation identifier")?,
                )
            } else {
                None
            }
        } else {
            None
        };
        page["next_cursor"] = json!(cursor);
        page["rows"] = Value::Array(
            rows.iter()
                .map(|r| selected_row(self, venue, r))
                .collect::<Result<_, _>>()?,
        );
        page["success"] = json!(true);
        Ok(page)
    }
}

fn fields(row: &Value, names: &[&str]) -> Result<Value, &'static str> {
    let mut out = serde_json::Map::new();
    for name in names {
        if let Some(v) = row.get(*name) {
            if !(v.is_string() || v.is_number() || v.is_boolean() || v.is_null()) {
                return Err("unexpected evidence value");
            }
            out.insert((*name).into(), v.clone());
        }
    }
    Ok(Value::Object(out))
}
pub(crate) fn selected_row(
    query: Query<'_>,
    venue: SpotVenue,
    row: &Value,
) -> Result<Value, &'static str> {
    if !row.is_object() {
        return Err("invalid evidence row");
    }
    let bybit = venue == SpotVenue::Bybit;
    if matches!(query, Query::Balance) {
        let (key, wallet, coins): (&str, &[&str], &[&str]) = if bybit {
            (
                "coin",
                &[
                    "accountType",
                    "totalEquity",
                    "totalWalletBalance",
                    "totalAvailableBalance",
                ],
                &[
                    "coin",
                    "walletBalance",
                    "equity",
                    "locked",
                    "borrowAmount",
                    "spotBorrow",
                ],
            )
        } else {
            (
                "details",
                &["uTime", "totalEq", "adjEq", "availEq"],
                &[
                    "ccy",
                    "cashBal",
                    "availBal",
                    "frozenBal",
                    "ordFrozen",
                    "eq",
                    "liab",
                    "uTime",
                ],
            )
        };
        let mut out = fields(row, wallet)?;
        let assets = row[key].as_array().ok_or("missing balance details")?;
        out[key] = Value::Array(
            assets
                .iter()
                .map(|r| fields(r, coins))
                .collect::<Result<_, _>>()?,
        );
        return Ok(out);
    }
    let keys: &[&str] = match (bybit, query) {
        (true, Query::Fills { .. }) => &[
            "symbol",
            "orderId",
            "orderLinkId",
            "execId",
            "execTime",
            "side",
            "execType",
            "execQty",
            "execPrice",
            "execValue",
            "execFee",
            "feeCurrency",
            "isMaker",
            "seq",
        ],
        (false, Query::Fills { .. }) => &[
            "instType", "instId", "ordId", "clOrdId", "tradeId", "billId", "fillTime", "ts",
            "side", "execType", "fillSz", "fillPx", "fee", "feeCcy",
        ],
        (true, _) => &[
            "symbol",
            "orderId",
            "orderLinkId",
            "orderStatus",
            "orderType",
            "side",
            "price",
            "qty",
            "cumExecQty",
            "cumExecValue",
            "leavesQty",
            "createdTime",
            "updatedTime",
            "isLeverage",
        ],
        (false, _) => &[
            "instType",
            "instId",
            "ordId",
            "clOrdId",
            "tdMode",
            "state",
            "ordType",
            "side",
            "sz",
            "px",
            "accFillSz",
            "avgPx",
            "fee",
            "feeCcy",
            "cTime",
            "uTime",
        ],
    };
    let mut out = fields(row, keys)?;
    if bybit && matches!(query, Query::Fills { .. }) {
        // Expose the presence of regional charges without echoing opaque JSON strings.
        out["has_extra_fees"] =
            json!(row.get("extraFees").is_some_and(
                |v| !(v.as_str() == Some("") || v.as_array().is_some_and(Vec::is_empty))
            ));
    }
    Ok(out)
}
