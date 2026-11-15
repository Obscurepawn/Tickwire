//! Zero-allocation WebSocket login, subscription step state machine, and heartbeat (`ping`/`pong`)
//! frame builders and single-pass `JsonScan` reply verifiers for Bybit V5 and OKX V5 UTA streams.

use crate::{Credentials, SpotVenue, UtaVenueError, wire_encode::BufWriter};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use ring::hmac;
use tickwire_transport::json_scan::{JsonScan, parse_i64_ascii};

/// Standard UTA WebSocket heartbeat interval (`20` seconds).
pub const WS_PING_PERIOD_S: u64 = 20;
/// Standard UTA WebSocket heartbeat interval in nanoseconds (`20_000_000_000` ns).
pub const WS_PING_PERIOD_NS: u64 = WS_PING_PERIOD_S * 1_000_000_000;

/// Result of inspecting an inbound WebSocket text payload for an authentication/login reply.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WsLoginOutcome {
    /// Login/auth succeeded (`event == "login" && code == "0"` on OKX, `op == "auth" && (success || retCode == 0)` on Bybit).
    Accepted,
    /// Login/auth explicitly rejected by the venue.
    Rejected,
    /// Frame is not a login/auth control reply.
    NotLoginFrame,
}

/// Encodes an OKX V5 WebSocket `op: "login"` JSON payload into `out` with zero heap allocation.
///
/// Signs `"{unix_sec}GET/users/self/verify"` with HMAC-SHA256 and Base64-encodes the digest onto the stack.
pub fn encode_okx_login_into(
    credentials: &Credentials<'_>,
    utc_ms: u64,
    out: &mut [u8],
) -> Result<usize, UtaVenueError> {
    if credentials.venue() != SpotVenue::Okx || !credentials.valid() || utc_ms == 0 {
        return Err(UtaVenueError::Identity);
    }
    let passphrase = credentials.passphrase().ok_or(UtaVenueError::Identity)?;
    let ts_sec = utc_ms / 1_000;
    let mut ts_buf = [0u8; 20];
    let ts_len = crate::wire_encode::write_u64_into(&mut ts_buf, ts_sec)?;
    let ts_bytes = &ts_buf[..ts_len];

    let mut mac = hmac::Context::with_key(credentials.hmac_key());
    mac.update(ts_bytes);
    mac.update(b"GET/users/self/verify");
    let tag = mac.sign();

    let mut sig_b64 = [0u8; 44];
    let sig_len = STANDARD
        .encode_slice(tag.as_ref(), &mut sig_b64)
        .map_err(|_| UtaVenueError::Encoding)?;

    let mut w = BufWriter::new(out);
    w.put_bytes(b"{\"op\":\"login\",\"args\":[{\"apiKey\":\"")?;
    w.put_bytes(credentials.key().as_bytes())?;
    w.put_bytes(b"\",\"passphrase\":\"")?;
    w.put_bytes(passphrase.as_bytes())?;
    w.put_bytes(b"\",\"timestamp\":\"")?;
    w.put_bytes(ts_bytes)?;
    w.put_bytes(b"\",\"sign\":\"")?;
    w.put_bytes(&sig_b64[..sig_len])?;
    w.put_bytes(b"\"}]}")?;
    Ok(w.written())
}

/// Inspects an inbound OKX WebSocket text frame in one zero-allocation `JsonScan` pass for a login reply.
pub fn check_okx_login_reply(payload: &[u8]) -> WsLoginOutcome {
    let mut scan = JsonScan::new(payload);
    if scan.eat_byte(b'{').is_err() {
        return WsLoginOutcome::NotLoginFrame;
    }
    let mut is_login_event = false;
    let mut is_error_event = false;
    let mut code_ok = false;
    let mut first = true;
    loop {
        if scan.eat_byte(b'}').is_ok() {
            break;
        }
        if !first && scan.eat_byte(b',').is_err() {
            return WsLoginOutcome::NotLoginFrame;
        }
        first = false;
        let Ok(key) = scan.read_string_raw() else {
            return WsLoginOutcome::NotLoginFrame;
        };
        if scan.eat_byte(b':').is_err() {
            return WsLoginOutcome::NotLoginFrame;
        }
        match key {
            "event" => {
                let Ok(ev) = scan.read_string_raw() else {
                    return WsLoginOutcome::NotLoginFrame;
                };
                if ev == "login" {
                    is_login_event = true;
                } else if ev == "error" {
                    is_error_event = true;
                }
            }
            "code" => {
                let Ok(c_opt) = scan.read_string_or_scalar() else {
                    return WsLoginOutcome::NotLoginFrame;
                };
                code_ok = c_opt == Some("0");
            }
            _ => {
                if scan.skip_value().is_err() {
                    return WsLoginOutcome::NotLoginFrame;
                }
            }
        }
    }
    if is_login_event {
        if code_ok {
            WsLoginOutcome::Accepted
        } else {
            WsLoginOutcome::Rejected
        }
    } else if is_error_event {
        WsLoginOutcome::Rejected
    } else {
        WsLoginOutcome::NotLoginFrame
    }
}

/// Encodes step `step` of the OKX V5 UTA private WebSocket subscription sequence into `out`:
/// - `step == 0`: `orders` (`instType: "ANY"`, covering `SPOT`, `SWAP`, `FUTURES`, and `OPTION`)
/// - `step == 1`: `balance_and_position`
/// - `step >= 2`: returns `Ok(0)` to signal subscription completion.
pub fn encode_okx_subscribe_step_into(step: u8, out: &mut [u8]) -> Result<usize, UtaVenueError> {
    let mut w = BufWriter::new(out);
    match step {
        0 => {
            w.put_bytes(
                b"{\"id\":\"twsub0\",\"op\":\"subscribe\",\"args\":[{\"channel\":\"orders\",\"instType\":\"ANY\"}]}",
            )?;
            Ok(w.written())
        }
        1 => {
            w.put_bytes(
                b"{\"id\":\"twsub1\",\"op\":\"subscribe\",\"args\":[{\"channel\":\"balance_and_position\"}]}",
            )?;
            Ok(w.written())
        }
        _ => Ok(0),
    }
}

/// Encodes a Bybit V5 WebSocket `op: "auth"` JSON payload into `out` with zero heap allocation.
///
/// Signs `"GET/realtime{expires_ms}"` (`expires_ms = utc_ms + 5000`) with HMAC-SHA256 lowercase hex.
pub fn encode_bybit_auth_into(
    credentials: &Credentials<'_>,
    utc_ms: u64,
    out: &mut [u8],
) -> Result<usize, UtaVenueError> {
    if credentials.venue() != SpotVenue::Bybit || !credentials.valid() || utc_ms == 0 {
        return Err(UtaVenueError::Identity);
    }
    let expires = utc_ms.checked_add(5_000).ok_or(UtaVenueError::Encoding)?;
    let mut exp_buf = [0u8; 20];
    let exp_len = crate::wire_encode::write_u64_into(&mut exp_buf, expires)?;
    let exp_bytes = &exp_buf[..exp_len];

    let mut mac = hmac::Context::with_key(credentials.hmac_key());
    mac.update(b"GET/realtime");
    mac.update(exp_bytes);
    let tag = mac.sign();

    let mut sig_hex = [0u8; 64];
    crate::wire_encode::write_hex_lower_32(tag.as_ref(), &mut sig_hex);

    let mut w = BufWriter::new(out);
    w.put_bytes(b"{\"req_id\":\"twauth\",\"op\":\"auth\",\"args\":[\"")?;
    w.put_bytes(credentials.key().as_bytes())?;
    w.put_bytes(b"\",")?;
    w.put_bytes(exp_bytes)?;
    w.put_bytes(b",\"")?;
    w.put_bytes(&sig_hex)?;
    w.put_bytes(b"\"]}")?;
    Ok(w.written())
}

/// Inspects an inbound Bybit V5 WebSocket text frame (`private` or `trade` stream) in one `JsonScan` pass.
///
/// Accepts `"op":"auth"` with either `"success":true` (`/v5/private`) or `"retCode":0` (`/v5/trade`).
pub fn check_bybit_auth_reply(payload: &[u8]) -> WsLoginOutcome {
    let mut scan = JsonScan::new(payload);
    if scan.eat_byte(b'{').is_err() {
        return WsLoginOutcome::NotLoginFrame;
    }
    let mut is_auth = false;
    let mut success_val: Option<bool> = None;
    let mut ret_code_val: Option<i64> = None;
    let mut first = true;
    loop {
        if scan.eat_byte(b'}').is_ok() {
            break;
        }
        if !first && scan.eat_byte(b',').is_err() {
            return WsLoginOutcome::NotLoginFrame;
        }
        first = false;
        let Ok(key) = scan.read_string_raw() else {
            return WsLoginOutcome::NotLoginFrame;
        };
        if scan.eat_byte(b':').is_err() {
            return WsLoginOutcome::NotLoginFrame;
        }
        match key {
            "op" => {
                let Ok(op) = scan.read_string_raw() else {
                    return WsLoginOutcome::NotLoginFrame;
                };
                is_auth = op == "auth";
            }
            "success" => {
                let Ok(prim) = scan.read_primitive_bytes() else {
                    return WsLoginOutcome::NotLoginFrame;
                };
                if prim == b"true" {
                    success_val = Some(true);
                } else if prim == b"false" {
                    success_val = Some(false);
                }
            }
            "retCode" => {
                let Ok(raw_opt) = scan.read_string_or_scalar() else {
                    return WsLoginOutcome::NotLoginFrame;
                };
                ret_code_val = raw_opt.and_then(|s| parse_i64_ascii(s.as_bytes()));
            }
            _ => {
                if scan.skip_value().is_err() {
                    return WsLoginOutcome::NotLoginFrame;
                }
            }
        }
    }
    if !is_auth {
        return WsLoginOutcome::NotLoginFrame;
    }
    if success_val == Some(true) || (success_val.is_none() && ret_code_val == Some(0)) {
        WsLoginOutcome::Accepted
    } else {
        WsLoginOutcome::Rejected
    }
}

/// Encodes step `step` of the Bybit V5 UTA private WebSocket subscription sequence into `out`:
/// - `step == 0`: `["order","execution","execution.fast","wallet"]`
/// - `step >= 1`: returns `Ok(0)`.
pub fn encode_bybit_private_subscribe_step_into(
    step: u8,
    out: &mut [u8],
) -> Result<usize, UtaVenueError> {
    if step > 0 {
        return Ok(0);
    }
    let mut w = BufWriter::new(out);
    w.put_bytes(
        b"{\"req_id\":\"twsub0\",\"op\":\"subscribe\",\"args\":[\"order\",\"execution\",\"execution.fast\",\"wallet\"]}",
    )?;
    Ok(w.written())
}

/// Encodes a venue-specific WebSocket text heartbeat request (`{"op":"ping"}` on Bybit, `"ping"` on OKX).
pub fn encode_ws_ping_into(venue: SpotVenue, out: &mut [u8]) -> Result<usize, UtaVenueError> {
    let mut w = BufWriter::new(out);
    match venue {
        SpotVenue::Bybit => w.put_bytes(b"{\"op\":\"ping\"}")?,
        SpotVenue::Okx => w.put_bytes(b"ping")?,
    }
    Ok(w.written())
}

/// Encodes a masked RFC 6455 client WebSocket frame (`FIN = 1`) into `out` with zero heap
/// allocations and zero syscalls, deriving the 4-byte XOR mask from `mask_seed` via SplitMix64.
pub fn encode_ws_client_frame_into(
    opcode: tickwire_transport::ws::Opcode,
    payload: &[u8],
    mask_seed: u64,
    out: &mut [u8],
) -> Result<usize, UtaVenueError> {
    if matches!(
        opcode,
        tickwire_transport::ws::Opcode::Close
            | tickwire_transport::ws::Opcode::Ping
            | tickwire_transport::ws::Opcode::Pong
    ) && payload.len() > 125
    {
        return Err(UtaVenueError::Encoding);
    }
    let header_len = if payload.len() < 126 {
        6
    } else if payload.len() <= 65_535 {
        8
    } else {
        14
    };
    let total_len = payload
        .len()
        .checked_add(header_len)
        .ok_or(UtaVenueError::Bounds)?;
    if total_len > out.len() {
        return Err(UtaVenueError::Bounds);
    }

    let mut z = mask_seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^= z >> 31;
    let mask = (z as u32).to_le_bytes();

    let mut pos = 0usize;
    out[pos] = 0x80 | (opcode as u8);
    pos += 1;
    if payload.len() < 126 {
        out[pos] = 0x80 | (payload.len() as u8);
        pos += 1;
    } else if payload.len() <= 65_535 {
        out[pos] = 0x80 | 126;
        pos += 1;
        let len_be = (payload.len() as u16).to_be_bytes();
        out[pos..pos + 2].copy_from_slice(&len_be);
        pos += 2;
    } else {
        out[pos] = 0x80 | 127;
        pos += 1;
        let len_be = (payload.len() as u64).to_be_bytes();
        out[pos..pos + 8].copy_from_slice(&len_be);
        pos += 8;
    }
    out[pos..pos + 4].copy_from_slice(&mask);
    pos += 4;
    for (idx, &byte) in payload.iter().enumerate() {
        out[pos + idx] = byte ^ mask[idx & 3];
    }
    Ok(total_len)
}

/// Borrowed unmasked RFC 6455 server frame decoded in place from a preallocated `RESP` buffer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WsServerFrame<'a> {
    pub fin: bool,
    pub opcode_raw: u8,
    pub payload: &'a [u8],
    pub consumed: usize,
}

/// Zero-allocation unmasked server WebSocket frame decoder over `buf`.
///
/// Returns `Ok(None)` when `buf` holds an incomplete frame header or payload.
pub fn try_decode_ws_server_frame(buf: &[u8]) -> Result<Option<WsServerFrame<'_>>, UtaVenueError> {
    if buf.len() < 2 {
        return Ok(None);
    }
    let b0 = buf[0];
    let b1 = buf[1];
    let fin = (b0 & 0x80) != 0;
    let opcode_raw = b0 & 0x0f;
    if (b0 & 0x70) != 0 || (b1 & 0x80) != 0 || !matches!(opcode_raw, 0 | 1 | 2 | 8 | 9 | 10) {
        return Err(UtaVenueError::Protocol("invalid server websocket flags"));
    }
    let small_len = usize::from(b1 & 0x7f);
    if opcode_raw >= 8 && (!fin || small_len > 125) {
        return Err(UtaVenueError::Protocol("invalid websocket control frame"));
    }
    let (hdr_len, payload_len) = match small_len {
        126 => {
            if buf.len() < 4 {
                return Ok(None);
            }
            let n = usize::from(u16::from_be_bytes([buf[2], buf[3]]));
            if n < 126 {
                return Err(UtaVenueError::Protocol("non-minimal websocket length"));
            }
            (4usize, n)
        }
        127 => {
            if buf.len() < 10 {
                return Ok(None);
            }
            let mut raw8 = [0u8; 8];
            raw8.copy_from_slice(&buf[2..10]);
            let n64 = u64::from_be_bytes(raw8);
            if n64 < 65_536 || (n64 >> 63) != 0 {
                return Err(UtaVenueError::Protocol("invalid 64-bit websocket length"));
            }
            let n = usize::try_from(n64).map_err(|_| UtaVenueError::Bounds)?;
            (10usize, n)
        }
        n => (2usize, n),
    };
    let end = hdr_len
        .checked_add(payload_len)
        .ok_or(UtaVenueError::Bounds)?;
    if buf.len() < end {
        return Ok(None);
    }
    Ok(Some(WsServerFrame {
        fin,
        opcode_raw,
        payload: &buf[hdr_len..end],
        consumed: end,
    }))
}

/// Borrowed HTTP/1.1 keep-alive response decoded in place from a preallocated `RESP` buffer,
/// extracting status, body slice, and rate-limit telemetry headers with zero heap allocation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HttpScannedResponse<'a> {
    pub status: u16,
    pub body: &'a [u8],
    pub consumed: usize,
    pub bapi_limit: Option<u64>,
    pub bapi_limit_status: Option<u64>,
    pub retry_after_secs: Option<u64>,
}

/// Zero-allocation HTTP/1.1 `Content-Length` response scanner over `buf`.
///
/// Returns `Ok(None)` when `buf` does not yet hold complete headers + `Content-Length` body.
pub fn try_decode_http_response(
    buf: &[u8],
) -> Result<Option<HttpScannedResponse<'_>>, UtaVenueError> {
    let Some(hdr_end_rel) = buf.windows(4).position(|w| w == b"\r\n\r\n") else {
        return Ok(None);
    };
    let hdr_bytes = &buf[..hdr_end_rel];
    let body_start = hdr_end_rel + 4;

    let mut lines = hdr_bytes.split(|&b| b == b'\n');
    let status_line = lines
        .next()
        .ok_or(UtaVenueError::Protocol("missing HTTP status line"))?
        .strip_suffix(b"\r")
        .unwrap_or(hdr_bytes);
    if !status_line.starts_with(b"HTTP/1.1 ") && !status_line.starts_with(b"HTTP/1.0 ") {
        return Err(UtaVenueError::Protocol("invalid HTTP version"));
    }
    let after_ver = &status_line[9..];
    if after_ver.len() < 3
        || !after_ver[0].is_ascii_digit()
        || !after_ver[1].is_ascii_digit()
        || !after_ver[2].is_ascii_digit()
    {
        return Err(UtaVenueError::Protocol("invalid HTTP status code"));
    }
    let status = u16::from(after_ver[0] - b'0') * 100
        + u16::from(after_ver[1] - b'0') * 10
        + u16::from(after_ver[2] - b'0');

    let mut content_length: Option<usize> = None;
    let mut bapi_limit: Option<u64> = None;
    let mut bapi_limit_status: Option<u64> = None;
    let mut retry_after_secs: Option<u64> = None;

    for raw_line in lines {
        let line = raw_line.strip_suffix(b"\r").unwrap_or(raw_line);
        if line.is_empty() {
            continue;
        }
        let Some(colon) = line.iter().position(|&b| b == b':') else {
            return Err(UtaVenueError::Protocol("malformed HTTP header"));
        };
        let name = line[..colon].trim_ascii();
        let val = line[colon + 1..].trim_ascii();
        if name.eq_ignore_ascii_case(b"Content-Length") {
            let n64 = tickwire_transport::json_scan::parse_u64_ascii(val)
                .ok_or(UtaVenueError::Protocol("invalid Content-Length"))?;
            let n = usize::try_from(n64).map_err(|_| UtaVenueError::Bounds)?;
            content_length = Some(n);
        } else if name.eq_ignore_ascii_case(b"X-Bapi-Limit") {
            bapi_limit = tickwire_transport::json_scan::parse_u64_ascii(val);
        } else if name.eq_ignore_ascii_case(b"X-Bapi-Limit-Status") {
            bapi_limit_status = tickwire_transport::json_scan::parse_u64_ascii(val);
        } else if name.eq_ignore_ascii_case(b"Retry-After") {
            retry_after_secs = tickwire_transport::json_scan::parse_u64_ascii(val);
        }
    }

    let body_len = match content_length {
        Some(n) => n,
        None if matches!(status, 100..=199 | 204 | 304) => 0,
        None => {
            return Err(UtaVenueError::Protocol(
                "missing Content-Length on HTTP response",
            ));
        }
    };
    let end = body_start
        .checked_add(body_len)
        .ok_or(UtaVenueError::Bounds)?;
    if buf.len() < end {
        return Ok(None);
    }
    Ok(Some(HttpScannedResponse {
        status,
        body: &buf[body_start..end],
        consumed: end,
        bapi_limit,
        bapi_limit_status,
        retry_after_secs,
    }))
}
