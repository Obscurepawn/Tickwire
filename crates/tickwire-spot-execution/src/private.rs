//! Authenticated observation protocol for one Bybit or OKX private WebSocket connection.
//! A confirmed subscription is not permission to emit orders or a remote history watermark.
use crate::{
    Credentials, SpotVenue, hex,
    observe::{Query, selected_row},
    symbol_ok,
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use ring::hmac;
use serde_json::{Value, json};
use tickwire_venue::Environment;
type Result<T> = std::result::Result<T, &'static str>;
pub fn endpoint(venue: SpotVenue, env: Environment) -> (&'static str, &'static str) {
    match (venue, env) {
        (SpotVenue::Bybit, Environment::Test) => ("stream-testnet.bybit.com", "/v5/private"),
        (SpotVenue::Bybit, Environment::Production) => ("stream.bybit.com", "/v5/private"),
        (SpotVenue::Okx, Environment::Test) => ("wspap.okx.com", "/ws/v5/private"),
        (SpotVenue::Okx, Environment::Production) => ("ws.okx.com", "/ws/v5/private"),
    }
}
pub struct Batch {
    pub channel: &'static str,
    /// Selected operator-visible fields, excluding raw credentials or diagnostic text.
    pub rows: Vec<Value>,
    raw: Vec<Value>,
}
impl Batch {
    /// Individual, unmodified private records for the caller's durable intake.
    /// On OKX a row of the orders channel may also contain the last individual trade.
    pub fn records(&self) -> &[Value] {
        &self.raw
    }
}
pub enum Reaction {
    Control,
    Subscribe(Value),
    Data(Batch),
}
pub struct PrivateFeed {
    venue: SpotVenue,
    login: Value,
    connection: Option<String>,
    orders: bool,
    wallet: bool,
    failed: bool,
}
impl PrivateFeed {
    pub fn new(credentials: &Credentials<'_>, utc_ms: u64) -> Result<Self> {
        if !credentials.valid() || utc_ms == 0 {
            return Err("invalid private login material or clock");
        }
        let key = hmac::Key::new(hmac::HMAC_SHA256, credentials.secret);
        let venue = credentials.venue;
        let login = if venue == SpotVenue::Bybit {
            let expires = utc_ms
                .checked_add(5000)
                .ok_or("private timestamp overflow")?;
            let sig = hex(hmac::sign(&key, format!("GET/realtime{expires}").as_bytes()).as_ref());
            json!({"op":"auth","args":[credentials.key,expires,sig]})
        } else {
            let ts = (utc_ms / 1000).to_string();
            let sig = STANDARD
                .encode(hmac::sign(&key, format!("{ts}GET/users/self/verify").as_bytes()).as_ref());
            json!({"op":"login","args":[{"apiKey":credentials.key,"passphrase":credentials.passphrase,"timestamp":ts,"sign":sig}]})
        };
        Ok(Self {
            venue,
            login,
            connection: None,
            orders: false,
            wallet: false,
            failed: false,
        })
    }
    /// Sensitive wire message; transmit inside validated TLS only.
    pub fn login(&self) -> &Value {
        &self.login
    }
    pub fn subscribed(&self) -> bool {
        !self.failed && self.orders && self.wallet
    }
    pub fn on_text(&mut self, text: &[u8]) -> Result<Reaction> {
        if self.failed {
            return Err("private session has been invalidated");
        }
        let result = self.decode(text);
        self.failed = result.is_err();
        result
    }
    fn decode(&mut self, text: &[u8]) -> Result<Reaction> {
        const MAX: usize = 1024 * 1024;
        if text.len() > MAX {
            return Err("private message exceeded limit");
        }
        let bybit = self.venue == SpotVenue::Bybit;
        if !bybit && text == b"pong" && self.connection.is_some() {
            return Ok(Reaction::Control);
        }
        let msg: Value =
            serde_json::from_slice(text).map_err(|_| "invalid private JSON message")?;
        let event = msg[if bybit { "op" } else { "event" }].as_str();
        let native_conn = msg[if bybit { "conn_id" } else { "connId" }].as_str();
        if self.connection.is_none() {
            if event != Some(if bybit { "auth" } else { "login" })
                || if bybit {
                    msg["success"] != true
                } else {
                    msg["code"] != "0"
                }
            {
                return Err("private WebSocket authentication failed");
            }
            let conn = native_conn
                .filter(|s| !s.is_empty() && s.len() <= 128)
                .ok_or("missing authenticated connection identity")?;
            self.connection = Some(conn.into());
            self.login = Value::Null;
            let args = if bybit {
                json!(["order.spot", "execution.spot", "wallet"])
            } else {
                json!([{"channel":"orders","instType":"SPOT"},{"channel":"account"}])
            };
            let mut request = json!({"op":"subscribe","args":args});
            request[if bybit { "req_id" } else { "id" }] = json!("twaccount");
            return Ok(Reaction::Subscribe(request));
        }
        if native_conn.is_some() && native_conn != self.connection.as_deref() {
            return Err("private connection identity changed");
        }
        match event {
            Some("subscribe") => {
                if msg[if bybit { "req_id" } else { "id" }] != "twaccount" {
                    return Err("unrecognized private subscription acknowledgement");
                }
                if bybit {
                    if msg["success"] != true || self.subscribed() {
                        return Err("private subscription rejected or repeated");
                    }
                    self.orders = true;
                    self.wallet = true;
                } else {
                    match msg["arg"]["channel"].as_str() {
                        Some("orders") if !self.orders && msg["arg"]["instType"] == "SPOT" => {
                            self.orders = true
                        }
                        Some("account") if !self.wallet => self.wallet = true,
                        _ => return Err("unexpected private subscription"),
                    }
                }
                return Ok(Reaction::Control);
            }
            Some("pong" | "ping") if bybit => return Ok(Reaction::Control),
            Some(_) => return Err("private WebSocket control error"),
            None => {}
        }
        let topic = if bybit {
            msg["topic"].as_str()
        } else {
            msg["arg"]["channel"].as_str()
        };
        let channel = match (self.venue, topic) {
            (SpotVenue::Bybit, Some("order.spot" | "order")) | (SpotVenue::Okx, Some("orders"))
                if self.orders =>
            {
                "orders"
            }
            (SpotVenue::Bybit, Some("execution.spot" | "execution")) if self.orders => "fills",
            (SpotVenue::Bybit, Some("wallet")) | (SpotVenue::Okx, Some("account"))
                if self.wallet =>
            {
                "balance"
            }
            _ => return Err("message arrived on an unconfirmed private channel"),
        };
        if !bybit && channel == "orders" && msg["arg"]["instType"] != "SPOT" {
            return Err("unexpected private channel product");
        }
        let raw = msg["data"]
            .as_array()
            .filter(|r| !r.is_empty())
            .ok_or("missing private data")?;
        let mut rows = Vec::with_capacity(raw.len());
        for row in raw {
            let q = if channel == "balance" {
                if bybit && row["accountType"] != "UNIFIED" {
                    return Err("unexpected private wallet scope");
                }
                Query::Balance
            } else {
                let (scope, instrument) = if bybit {
                    ("category", "symbol")
                } else {
                    ("instType", "instId")
                };
                if row[scope] != if bybit { "spot" } else { "SPOT" }
                    || row[instrument]
                        .as_str()
                        .is_none_or(|s| s.len() > 64 || !symbol_ok(s))
                {
                    return Err("unexpected private order instrument");
                }
                if channel == "orders" {
                    Query::OpenOrders {
                        symbol: None,
                        cursor: None,
                    }
                } else {
                    Query::Fills {
                        symbol: None,
                        cursor: None,
                        window: None,
                    }
                }
            };
            let mut out = selected_row(q, self.venue, row)?;
            if !bybit && channel == "orders" {
                for k in [
                    "tradeId",
                    "fillSz",
                    "fillPx",
                    "fillFee",
                    "fillFeeCcy",
                    "fillTime",
                    "execType",
                ] {
                    if let Some(v) = row.get(k) {
                        if !v.is_string() {
                            return Err("invalid individual fill field");
                        }
                        out[k] = v.clone();
                    }
                }
            }
            rows.push(out);
        }
        Ok(Reaction::Data(Batch {
            channel,
            rows,
            raw: raw.clone(),
        }))
    }
}
