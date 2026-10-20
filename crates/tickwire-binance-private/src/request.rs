use crate::*;
use sha2::{Digest, Sha256};
use tickwire_binance::{
    auth::{Endpoint, MAX_SIGNATURE},
    params::{Params, Value},
};
pub const MAX_REQUEST_BYTES: usize = 16_384;
pub struct PreparedControlRequest {
    scope: AccountScope,
    token: RequestToken,
    authority: &'static str,
    path: &'static str,
    method: Method,
    payload: Vec<u8>,
    api_key: Option<String>,
    digest: [u8; 32],
    created: u64,
    signed_time: Option<u128>,
    stamp: Option<SignedTime>,
    domain: ClockDomain,
    window_micros: u64,
}
impl std::fmt::Debug for PreparedControlRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreparedControlRequest")
            .field("token", &self.token)
            .field("payload", &"[REDACTED]")
            .finish()
    }
}
impl PreparedControlRequest {
    pub fn scope(&self) -> &AccountScope {
        &self.scope
    }
    pub fn token(&self) -> RequestToken {
        self.token
    }
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
    pub fn path(&self) -> &str {
        self.path
    }
    pub fn authority(&self) -> &str {
        self.authority
    }
    pub fn method(&self) -> Method {
        self.method
    }
    pub fn api_key_header(&self) -> Option<&str> {
        self.api_key.as_deref()
    }
    pub fn digest(&self) -> [u8; 32] {
        self.digest
    }
    pub fn signed_time(&self) -> Option<&SignedTime> {
        self.stamp.as_ref()
    }
    pub fn clock_domain(&self) -> ClockDomain {
        self.domain
    }
    pub fn recv_window_micros(&self) -> u64 {
        self.window_micros
    }
    pub fn fresh(&self, now: &ClockSample) -> bool {
        if now.monotonic.domain() != self.domain || now.monotonic_ns() < self.created {
            return false;
        }
        let Some(signed) = self.signed_time else {
            return true;
        };
        if now.monotonic_ns() - self.created >= self.window_micros * 1000 {
            return false;
        }
        if !now
            .signing_time
            .is_some_and(|t| t.domain() == self.domain && t.source() == clock_source(&self.scope))
        {
            return false;
        }
        let Some(current) = now.signing_timestamp().and_then(timestamp_us) else {
            return false;
        };
        // Binance rejects signatures >=1 second ahead of server time. A calibrated
        // Clock supplies the trusted estimate; its uncertainty policy is external.
        signed < current + 1_000_000
            && current.saturating_sub(signed) < u128::from(self.window_micros)
    }
}
pub(crate) fn timestamp_us(t: Timestamp) -> Option<u128> {
    match t {
        Timestamp::Milliseconds(v) if v > 0 => Some(u128::from(v) * 1000),
        Timestamp::Microseconds(v) if v > 0 => Some(u128::from(v)),
        _ => None,
    }
}
pub(crate) fn auth_key<'a>(auth: &'a Authentication<'_>) -> &'a str {
    match auth {
        Authentication::SpotLogon { api_key, .. }
        | Authentication::SpotSignature { api_key, .. }
        | Authentication::UsdmApiKey { api_key } => api_key,
    }
}
pub(crate) fn validate_auth(scope: &AccountScope, auth: &Authentication<'_>) -> Result<()> {
    scope.validate()?;
    if !matches!(
        (scope.market, auth),
        (
            Market::Spot,
            Authentication::SpotLogon { .. } | Authentication::SpotSignature { .. }
        ) | (Market::Usdm, Authentication::UsdmApiKey { .. })
    ) {
        return Err(Error::Identity);
    }
    let key = auth_key(auth);
    if key.is_empty()
        || key.len() > 256
        || !key
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_".contains(&b))
    {
        return Err(Error::Authentication);
    }
    if let Authentication::SpotLogon { signing_key, .. } = auth
        && !Endpoint::SpotSessionLogon.permits(signing_key.algorithm())
    {
        return Err(Error::Unsupported);
    }
    Ok(())
}
pub(crate) fn authority(market: Market, environment: Environment, stream: bool) -> &'static str {
    match (market, environment, stream) {
        (Market::Spot, Environment::Production, _) => "ws-api.binance.com",
        (Market::Spot, Environment::Test, _) => "ws-api.testnet.binance.vision",
        (Market::Usdm, Environment::Production, false) => "fapi.binance.com",
        (Market::Usdm, Environment::Test, false) => "demo-fapi.binance.com",
        (Market::Usdm, Environment::Production, true) => "fstream.binance.com",
        (Market::Usdm, Environment::Test, true) => "demo-fstream.binance.com",
    }
}
pub fn prepare(
    scope: &AccountScope,
    auth: &Authentication<'_>,
    token: RequestToken,
    now: &ClockSample,
    window_micros: u64,
    max: usize,
) -> Result<PreparedControlRequest> {
    validate_auth(scope, auth)?;
    if max == 0 || max > MAX_REQUEST_BYTES || window_micros == 0 || window_micros > 60_000_000 {
        return Err(Error::Bounds);
    }
    let op = token.operation();
    let mut params = Params::<5>::new();
    let mut signed_time = None;
    let (method, ws_method, signed) = match op {
        Operation::SpotLogon => (Method::Get, "session.logon", true),
        Operation::SpotSubscribe => (Method::Get, "userDataStream.subscribe", false),
        Operation::SpotSubscribeSignature => {
            (Method::Get, "userDataStream.subscribe.signature", true)
        }
        Operation::SpotStatus => (Method::Get, "session.status", false),
        Operation::SpotSubscriptions => (Method::Get, "session.subscriptions", false),
        Operation::SpotUnsubscribe { .. } => (Method::Get, "userDataStream.unsubscribe", false),
        Operation::SpotLogout => (Method::Get, "session.logout", false),
        Operation::UsdmCreate => (Method::Post, "", false),
        Operation::UsdmKeepalive => (Method::Put, "", false),
        Operation::UsdmClose => (Method::Delete, "", false),
    };
    if (scope.market == Market::Usdm) != ws_method.is_empty() {
        return Err(Error::Identity);
    }
    if matches!(op, Operation::SpotLogon | Operation::SpotSubscribe)
        && !matches!(auth, Authentication::SpotLogon { .. })
    {
        return Err(Error::Unsupported);
    }
    if op == Operation::SpotSubscribeSignature
        && !matches!(auth, Authentication::SpotSignature { .. })
    {
        return Err(Error::Unsupported);
    }
    let mut payload = Vec::new();
    if scope.market == Market::Spot {
        let win = if window_micros.is_multiple_of(1000) {
            (window_micros / 1000).to_string()
        } else {
            format!("{}.{:03}", window_micros / 1000, window_micros % 1000)
                .trim_end_matches('0')
                .to_owned()
        };
        if signed || op == Operation::SpotSubscribe {
            params
                .push("apiKey", Value::String(auth_key(auth)))
                .map_err(|_| Error::Bounds)?;
        }
        if signed {
            let stamp = now.signing_time.ok_or(Error::Clock)?;
            if stamp.domain() != now.monotonic.domain() || stamp.source() != clock_source(scope) {
                return Err(Error::Identity);
            }
            let time = now.signing_timestamp().ok_or(Error::Clock)?;
            signed_time = Some(timestamp_us(time).ok_or(Error::Clock)?);
            let value = match time {
                Timestamp::Milliseconds(n) | Timestamp::Microseconds(n) => n,
            };
            params
                .push("timestamp", Value::Integer(value))
                .map_err(|_| Error::Bounds)?;
            params
                .push("recvWindow", Value::Number(&win))
                .map_err(|_| Error::Bounds)?;
        }
        if let Operation::SpotUnsubscribe { subscription_id } = op {
            params
                .push("subscriptionId", Value::Integer(subscription_id))
                .map_err(|_| Error::Bounds)?;
        }
        let mut scratch = [0; MAX_REQUEST_BYTES];
        let n = params.json(&mut scratch).map_err(|_| Error::Bounds)?;
        let mut value: serde_json::Value =
            serde_json::from_slice(&scratch[..n]).map_err(|_| Error::Protocol)?;
        if signed {
            let n = params
                .websocket_preimage(&mut scratch)
                .map_err(|_| Error::Bounds)?;
            let key = match auth {
                Authentication::SpotLogon { signing_key, .. }
                | Authentication::SpotSignature { signing_key, .. } => signing_key,
                _ => return Err(Error::Identity),
            };
            let endpoint = if op == Operation::SpotLogon {
                Endpoint::SpotSessionLogon
            } else {
                Endpoint::SpotWebSocket
            };
            let mut signature = [0; MAX_SIGNATURE];
            let n = key
                .sign(endpoint, &scratch[..n], &mut signature)
                .map_err(|_| Error::Authentication)?;
            value["signature"] = serde_json::Value::String(
                std::str::from_utf8(&signature[..n])
                    .map_err(|_| Error::Authentication)?
                    .to_owned(),
            );
        }
        let envelope = serde_json::json!({"id":token.id(),"method":ws_method,"params":value});
        payload = serde_json::to_vec(&envelope).map_err(|_| Error::Protocol)?;
    }
    let path = if scope.market == Market::Spot {
        "/ws-api/v3"
    } else {
        "/fapi/v1/listenKey"
    };
    let authority = authority(scope.market, scope.environment, false);
    let api_key = if scope.market == Market::Usdm {
        Some(auth_key(auth).to_owned())
    } else {
        None
    };
    if payload.len() > max {
        return Err(Error::Capacity);
    }
    // HTTP bounds include the actual header, not only the empty request body.
    if let Some(api_key) = &api_key {
        tickwire_transport::http::encode_request(
            method.as_str(),
            path,
            &[("Host", authority), ("X-MBX-APIKEY", api_key)],
            &[],
            max,
        )
        .map_err(|_| Error::Capacity)?;
    }
    let mut h = Sha256::new();
    h.update(b"tickwire.private-control.v1\0");
    h.update(scope.risk_domain_id);
    h.update(scope.venue.to_le_bytes());
    h.update(scope.account.to_le_bytes());
    h.update(scope.credential_generation.to_le_bytes());
    h.update(token.connection_epoch.to_le_bytes());
    h.update(token.serial.to_le_bytes());
    for bytes in [
        authority.as_bytes(),
        path.as_bytes(),
        method.as_str().as_bytes(),
        &payload,
        api_key.as_deref().unwrap_or("").as_bytes(),
    ] {
        h.update((bytes.len() as u64).to_le_bytes());
        h.update(bytes);
    }
    Ok(PreparedControlRequest {
        scope: scope.clone(),
        token,
        authority,
        path,
        method,
        payload,
        api_key,
        digest: h.finalize().into(),
        created: now.monotonic_ns(),
        signed_time,
        stamp: if signed { now.signing_time } else { None },
        domain: now.monotonic.domain(),
        window_micros,
    })
}

pub(crate) fn clock_source(scope: &AccountScope) -> tickwire_clock::Source {
    match (scope.market, scope.environment) {
        (Market::Spot, Environment::Production) => tickwire_clock::Source::SpotProduction,
        (Market::Spot, Environment::Test) => tickwire_clock::Source::SpotTest,
        (Market::Usdm, Environment::Production) => tickwire_clock::Source::UsdmProduction,
        (Market::Usdm, Environment::Test) => tickwire_clock::Source::UsdmTest,
    }
}
pub(crate) fn check_clock(
    request: &PreparedControlRequest,
    now: &ClockSample,
    clock: &mut impl Clock,
) -> Result<()> {
    if request.clock_domain() != now.monotonic.domain() {
        return Err(Error::ClockDomain);
    }
    if !request.fresh(now) {
        return Err(Error::Clock);
    }
    let Some(stamp) = request.signed_time() else {
        return Ok(());
    };
    if stamp.domain() != now.monotonic.domain() {
        return Err(Error::ClockDomain);
    }
    if stamp.source() != clock_source(request.scope()) {
        return Err(Error::Identity);
    }
    clock.check_signed(request.scope(), stamp, now, request.recv_window_micros())
}
