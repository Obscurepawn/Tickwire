//! Side-effect-free idle-window cache tracker and isolated in-memory TLS 1.3 loopback warmer.
#![forbid(unsafe_code)]

use crate::{Error, Result, ws::Opcode};
use std::{
    io::{Cursor, Read, Write},
    sync::{Arc, OnceLock},
};

/// Inline scratch slice capacity (`2 KiB`) for single-record TLS 1.3 and wire dry-runs.
pub const WARMUP_SCRATCH_BYTES: usize = 2048;

/// Maximum plaintext chunk encrypted per pass so TLS 1.3 header + AEAD tag always fit in `WARMUP_SCRATCH_BYTES`.
const MAX_TLS_CHUNK_PLAIN: usize = 1536;

/// Default synthetic HTTP/1.1 request used to prime internal `rustls` buffers at startup.
const DEFAULT_SAMPLE_HTTP_REQ: &[u8] = b"POST /v5/order/create HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: 27\r\n\r\n{\"symbol\":\"BTCUSDT\",\"qty\":1}";

/// Default synthetic HTTP/1.1 `200 OK` acknowledgement echoed from `dummy_server` to `dummy_client`.
pub const DEFAULT_DUMMY_HTTP_ACK: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 45\r\n\r\n{\"retCode\":0,\"code\":\"0\",\"orderId\":\"10000001\"}";

/// Self-signed ECDSA P-256 `localhost` server certificate (`CA:FALSE`, `DNS:localhost`, valid 2020-01-01..2049-12-31).
const LOOPBACK_CERT_DER: &[u8] = &[
    48, 130, 1, 129, 48, 130, 1, 39, 160, 3, 2, 1, 2, 2, 1, 1, 48, 10, 6, 8, 42, 134, 72, 206, 61,
    4, 3, 2, 48, 20, 49, 18, 48, 16, 6, 3, 85, 4, 3, 12, 9, 108, 111, 99, 97, 108, 104, 111, 115,
    116, 48, 30, 23, 13, 50, 48, 48, 49, 48, 49, 48, 48, 48, 48, 48, 48, 90, 23, 13, 52, 57, 49,
    50, 51, 49, 50, 51, 53, 57, 53, 57, 90, 48, 20, 49, 18, 48, 16, 6, 3, 85, 4, 3, 12, 9, 108,
    111, 99, 97, 108, 104, 111, 115, 116, 48, 89, 48, 19, 6, 7, 42, 134, 72, 206, 61, 2, 1, 6, 8,
    42, 134, 72, 206, 61, 3, 1, 7, 3, 66, 0, 4, 39, 93, 203, 126, 207, 147, 204, 236, 190, 204, 45,
    219, 102, 160, 216, 105, 56, 16, 192, 138, 65, 191, 27, 181, 35, 103, 87, 15, 134, 184, 106,
    226, 155, 40, 174, 32, 0, 34, 136, 25, 184, 108, 226, 226, 229, 61, 176, 10, 11, 191, 87, 58,
    129, 61, 244, 165, 141, 123, 216, 229, 40, 37, 13, 255, 163, 106, 48, 104, 48, 12, 6, 3, 85,
    29, 19, 1, 1, 255, 4, 2, 48, 0, 48, 14, 6, 3, 85, 29, 15, 1, 1, 255, 4, 4, 3, 2, 7, 128, 48,
    19, 6, 3, 85, 29, 37, 4, 12, 48, 10, 6, 8, 43, 6, 1, 5, 5, 7, 3, 1, 48, 20, 6, 3, 85, 29, 17,
    4, 13, 48, 11, 130, 9, 108, 111, 99, 97, 108, 104, 111, 115, 116, 48, 29, 6, 3, 85, 29, 14, 4,
    22, 4, 20, 85, 147, 170, 53, 128, 255, 155, 141, 89, 108, 101, 146, 46, 250, 42, 120, 72, 68,
    199, 158, 48, 10, 6, 8, 42, 134, 72, 206, 61, 4, 3, 2, 3, 72, 0, 48, 69, 2, 32, 8, 13, 116,
    199, 120, 197, 184, 239, 211, 121, 235, 35, 103, 36, 110, 244, 214, 40, 255, 229, 194, 121, 15,
    132, 192, 94, 144, 224, 90, 236, 38, 127, 2, 33, 0, 146, 193, 74, 116, 228, 253, 164, 136, 175,
    25, 178, 213, 52, 147, 74, 209, 202, 77, 163, 50, 172, 249, 135, 187, 238, 116, 172, 80, 57,
    93, 195, 96,
];

/// PKCS#8 DER private key matching [`LOOPBACK_CERT_DER`].
const LOOPBACK_KEY_DER: &[u8] = &[
    48, 129, 135, 2, 1, 0, 48, 19, 6, 7, 42, 134, 72, 206, 61, 2, 1, 6, 8, 42, 134, 72, 206, 61, 3,
    1, 7, 4, 109, 48, 107, 2, 1, 1, 4, 32, 105, 186, 29, 116, 239, 57, 105, 233, 129, 130, 64, 175,
    135, 133, 198, 9, 117, 9, 173, 177, 220, 215, 143, 126, 16, 245, 23, 221, 143, 86, 98, 114,
    161, 68, 3, 66, 0, 4, 39, 93, 203, 126, 207, 147, 204, 236, 190, 204, 45, 219, 102, 160, 216,
    105, 56, 16, 192, 138, 65, 191, 27, 181, 35, 103, 87, 15, 134, 184, 106, 226, 155, 40, 174, 32,
    0, 34, 136, 25, 184, 108, 226, 226, 229, 61, 176, 10, 11, 191, 87, 58, 129, 61, 244, 165, 141,
    123, 216, 229, 40, 37, 13, 255,
];

/// Idle-window threshold configuration (`0` disables hot-path warming).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WarmupConfig {
    pub warmup_interval_ns: u64,
}

impl WarmupConfig {
    pub const DISABLED: Self = Self {
        warmup_interval_ns: 0,
    };

    #[inline]
    pub const fn new(warmup_interval_ns: u64) -> Self {
        Self { warmup_interval_ns }
    }

    #[inline]
    pub const fn is_enabled(&self) -> bool {
        self.warmup_interval_ns > 0
    }
}

/// Syscall-free monotonic tracker gating cache warming strictly to idle windows `>= warmup_interval_ns`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WarmupTracker {
    config: WarmupConfig,
    last_activity_ns: u64,
    last_warm_ns: u64,
    warm_count: u64,
}

impl WarmupTracker {
    #[inline]
    pub const fn new(config: WarmupConfig) -> Self {
        Self {
            config,
            last_activity_ns: 0,
            last_warm_ns: 0,
            warm_count: 0,
        }
    }

    #[inline]
    pub const fn disabled() -> Self {
        Self::new(WarmupConfig::DISABLED)
    }

    #[inline]
    pub const fn config(&self) -> WarmupConfig {
        self.config
    }

    #[inline]
    pub const fn warmup_interval_ns(&self) -> u64 {
        self.config.warmup_interval_ns
    }

    #[inline]
    pub fn set_warmup_interval_ns(&mut self, warmup_interval_ns: u64) {
        self.config.warmup_interval_ns = warmup_interval_ns;
    }

    #[inline]
    pub const fn last_activity_ns(&self) -> u64 {
        self.last_activity_ns
    }

    #[inline]
    pub const fn last_warm_ns(&self) -> u64 {
        self.last_warm_ns
    }

    #[inline]
    pub const fn warm_count(&self) -> u64 {
        self.warm_count
    }

    /// Records real market or order activity, resetting the idle warmup countdown.
    #[inline]
    pub fn record_activity(&mut self, now_ns: u64) {
        if now_ns > self.last_activity_ns {
            self.last_activity_ns = now_ns;
        }
    }

    /// Marks an explicit warmup iteration at `now_ns` (used by unconditional `warm_hot_path` calls).
    #[inline]
    pub fn record_warm(&mut self, now_ns: u64) {
        if now_ns > self.last_warm_ns {
            self.last_warm_ns = now_ns;
        }
        self.warm_count = self.warm_count.saturating_add(1);
    }

    /// Returns the next monotonic timestamp at which warming becomes due when enabled.
    #[inline]
    pub const fn next_due_ns(&self) -> Option<u64> {
        if self.config.warmup_interval_ns == 0 {
            return None;
        }
        let anchor = if self.last_activity_ns >= self.last_warm_ns {
            self.last_activity_ns
        } else {
            self.last_warm_ns
        };
        Some(anchor.saturating_add(self.config.warmup_interval_ns))
    }

    /// Pure check returning `true` iff warming is enabled and idle time `>= warmup_interval_ns`.
    #[inline]
    pub const fn can_warm(&self, now_ns: u64) -> bool {
        let interval = self.config.warmup_interval_ns;
        if interval == 0 {
            return false;
        }
        let anchor = if self.last_activity_ns >= self.last_warm_ns {
            self.last_activity_ns
        } else {
            self.last_warm_ns
        };
        now_ns.saturating_sub(anchor) >= interval
    }

    /// Returns `true` and advances `last_warm_ns` iff warming is enabled and idle time `>= warmup_interval_ns`.
    #[inline]
    pub fn should_warm(&mut self, now_ns: u64) -> bool {
        if !self.can_warm(now_ns) {
            return false;
        }
        self.record_warm(now_ns);
        true
    }
}

fn loopback_tls_configs() -> Result<&'static (Arc<rustls::ClientConfig>, Arc<rustls::ServerConfig>)>
{
    static CONFIGS: OnceLock<(Arc<rustls::ClientConfig>, Arc<rustls::ServerConfig>)> =
        OnceLock::new();
    if let Some(pair) = CONFIGS.get() {
        return Ok(pair);
    }
    let cert = rustls::pki_types::CertificateDer::from(LOOPBACK_CERT_DER);
    let key = rustls::pki_types::PrivatePkcs8KeyDer::from(LOOPBACK_KEY_DER);

    let mut server =
        rustls::ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
            .with_no_client_auth()
            .with_single_cert(vec![cert.clone()], key.into())?;
    server.send_tls13_tickets = 0;

    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert)?;
    let client = rustls::ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
        .with_root_certificates(roots)
        .with_no_client_auth();
    if client.enable_early_data {
        return Err(Error::Protocol("early data disabled"));
    }
    Ok(CONFIGS.get_or_init(|| (Arc::new(client), Arc::new(server))))
}

/// Isolated in-memory TLS 1.3 client/server loopback pair keeping AEAD encrypt and decrypt paths warm
/// without touching any live socket or live `rustls::ClientConnection` record sequence nonce.
pub struct TlsLoopbackWarmer {
    dummy_client: rustls::ClientConnection,
    dummy_server: rustls::ServerConnection,
    tls_scratch: [u8; WARMUP_SCRATCH_BYTES],
    plain_scratch: [u8; WARMUP_SCRATCH_BYTES],
    ack_payload: &'static [u8],
}

impl std::fmt::Debug for TlsLoopbackWarmer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsLoopbackWarmer")
            .field("client_handshaking", &self.dummy_client.is_handshaking())
            .field("server_handshaking", &self.dummy_server.is_handshaking())
            .finish_non_exhaustive()
    }
}

impl TlsLoopbackWarmer {
    /// Constructs a connected in-memory TLS 1.3 loopback pair and primes internal buffers at startup.
    pub fn new() -> Result<Self> {
        let (client_cfg, server_cfg) = loopback_tls_configs()?;
        let server_name = rustls::pki_types::ServerName::try_from("localhost")
            .map_err(|_| Error::Protocol("invalid loopback server name"))?;
        let mut dummy_client = rustls::ClientConnection::new(Arc::clone(client_cfg), server_name)?;
        let mut dummy_server = rustls::ServerConnection::new(Arc::clone(server_cfg))?;
        dummy_client.set_buffer_limit(Some(16 * 1024));
        dummy_server.set_buffer_limit(Some(16 * 1024));

        let mut warmer = Self {
            dummy_client,
            dummy_server,
            tls_scratch: [0u8; WARMUP_SCRATCH_BYTES],
            plain_scratch: [0u8; WARMUP_SCRATCH_BYTES],
            ack_payload: DEFAULT_DUMMY_HTTP_ACK,
        };
        warmer.complete_handshake()?;
        // Prime internal rustls deque/deframer capacities during cold startup.
        let _ = warmer.try_warm_step(DEFAULT_SAMPLE_HTTP_REQ)?;
        Ok(warmer)
    }

    pub fn with_ack_payload(mut self, ack_payload: &'static [u8]) -> Self {
        self.ack_payload = ack_payload;
        self
    }

    #[inline]
    pub fn scratch_addr(&self) -> usize {
        self.tls_scratch.as_ptr() as usize
    }

    fn complete_handshake(&mut self) -> Result<()> {
        let mut steps = 0usize;
        while self.dummy_client.is_handshaking()
            || self.dummy_server.is_handshaking()
            || self.dummy_client.wants_write()
            || self.dummy_server.wants_write()
        {
            steps += 1;
            if steps > 64 {
                return Err(Error::Protocol("loopback tls handshake did not converge"));
            }
            self.flush_client_to_server_tls()?;
            self.flush_server_to_client_tls()?;
        }
        Ok(())
    }

    fn flush_client_to_server_tls(&mut self) -> Result<usize> {
        let mut transferred = 0usize;
        while self.dummy_client.wants_write() {
            let enc_len = {
                let mut dst = Cursor::new(&mut self.tls_scratch[..]);
                self.dummy_client.write_tls(&mut dst)?
            };
            if enc_len == 0 {
                break;
            }
            transferred += enc_len;
            let mut offset = 0usize;
            while offset < enc_len {
                let mut src = Cursor::new(&self.tls_scratch[offset..enc_len]);
                let read = self.dummy_server.read_tls(&mut src)?;
                if read == 0 {
                    return Err(Error::Protocol("loopback server short tls read"));
                }
                offset += read;
                self.dummy_server.process_new_packets()?;
            }
        }
        Ok(transferred)
    }

    fn flush_server_to_client_tls(&mut self) -> Result<usize> {
        let mut transferred = 0usize;
        while self.dummy_server.wants_write() {
            let enc_len = {
                let mut dst = Cursor::new(&mut self.tls_scratch[..]);
                self.dummy_server.write_tls(&mut dst)?
            };
            if enc_len == 0 {
                break;
            }
            transferred += enc_len;
            let mut offset = 0usize;
            while offset < enc_len {
                let mut src = Cursor::new(&self.tls_scratch[offset..enc_len]);
                let read = self.dummy_client.read_tls(&mut src)?;
                if read == 0 {
                    return Err(Error::Protocol("loopback client short tls read"));
                }
                offset += read;
                self.dummy_client.process_new_packets()?;
            }
        }
        Ok(transferred)
    }

    /// Encrypts `sample_plaintext` on `dummy_client`, decrypts and verifies AEAD on `dummy_server`,
    /// and echoes the configured dummy HTTP/WS acknowledgement back through `dummy_client`.
    ///
    /// Returns total decrypted plaintext bytes across both directions (`0` if `sample_plaintext` is empty).
    #[inline]
    pub fn warm_step(&mut self, sample_plaintext: &[u8]) -> usize {
        self.try_warm_step(sample_plaintext).unwrap_or(0)
    }

    /// Fallible variant of [`Self::warm_step`].
    #[inline]
    pub fn try_warm_step(&mut self, sample_plaintext: &[u8]) -> Result<usize> {
        let ack = self.ack_payload;
        let mut ack_sink = [0u8; 0];
        let (req_plain, ack_plain) =
            self.warm_step_exchange(sample_plaintext, ack, &mut ack_sink)?;
        Ok(req_plain.saturating_add(ack_plain))
    }

    /// Drives a full client -> server -> client TLS 1.3 record roundtrip entirely in memory and copies up to
    /// `ack_out.len()` decrypted server acknowledgement bytes into `ack_out`.
    ///
    /// Returns `(request_decrypted_bytes, ack_decrypted_bytes)`.
    pub fn warm_step_exchange(
        &mut self,
        sample_plaintext: &[u8],
        ack_plaintext: &[u8],
        ack_out: &mut [u8],
    ) -> Result<(usize, usize)> {
        if sample_plaintext.is_empty() {
            return Ok((0, 0));
        }
        let mut req_decrypted = 0usize;
        for chunk in sample_plaintext.chunks(MAX_TLS_CHUNK_PLAIN) {
            self.dummy_client.writer().write_all(chunk)?;
            self.flush_client_to_server_tls()?;
            loop {
                match self.dummy_server.reader().read(&mut self.plain_scratch) {
                    Ok(0) => break,
                    Ok(n) => req_decrypted = req_decrypted.saturating_add(n),
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                    Err(e) => return Err(Error::Io(e)),
                }
            }
            if self.dummy_server.wants_write() {
                self.flush_server_to_client_tls()?;
            }
        }
        if req_decrypted != sample_plaintext.len() {
            return Err(Error::Protocol(
                "loopback request plaintext length mismatch",
            ));
        }

        let mut ack_decrypted = 0usize;
        for chunk in ack_plaintext.chunks(MAX_TLS_CHUNK_PLAIN) {
            self.dummy_server.writer().write_all(chunk)?;
            self.flush_server_to_client_tls()?;
            loop {
                match self.dummy_client.reader().read(&mut self.plain_scratch) {
                    Ok(0) => break,
                    Ok(n) => {
                        if ack_decrypted < ack_out.len() {
                            let avail = ack_out.len() - ack_decrypted;
                            let copy_len = avail.min(n);
                            ack_out[ack_decrypted..ack_decrypted + copy_len]
                                .copy_from_slice(&self.plain_scratch[..copy_len]);
                        }
                        ack_decrypted = ack_decrypted.saturating_add(n);
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                    Err(e) => return Err(Error::Io(e)),
                }
            }
            if self.dummy_client.wants_write() {
                self.flush_client_to_server_tls()?;
            }
        }
        if ack_decrypted != ack_plaintext.len() {
            return Err(Error::Protocol("loopback ack plaintext length mismatch"));
        }
        Ok((req_decrypted, ack_decrypted))
    }
}

/// Formats `value` as decimal ASCII into `out`, returning the number of bytes written.
#[inline]
pub fn write_u64_ascii(out: &mut [u8], mut value: u64) -> Result<usize> {
    let mut digits = [0u8; 20];
    let mut pos = digits.len();
    if value == 0 {
        pos -= 1;
        digits[pos] = b'0';
    } else {
        while value > 0 {
            pos -= 1;
            digits[pos] = b'0' + (value % 10) as u8;
            value /= 10;
        }
    }
    let len = digits.len() - pos;
    if out.len() < len {
        return Err(Error::Bounds);
    }
    out[..len].copy_from_slice(&digits[pos..]);
    Ok(len)
}

fn push_bytes(out: &mut [u8], cursor: &mut usize, src: &[u8]) -> Result<()> {
    let end = cursor.checked_add(src.len()).ok_or(Error::Bounds)?;
    if end > out.len() {
        return Err(Error::Bounds);
    }
    out[*cursor..end].copy_from_slice(src);
    *cursor = end;
    Ok(())
}

fn valid_http_token(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b)
}

/// Allocation-free HTTP/1.1 request encoder writing directly into caller-provided `out`.
pub fn encode_http_request_into(
    out: &mut [u8],
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> Result<usize> {
    if method.is_empty()
        || !method.bytes().all(valid_http_token)
        || !path.starts_with('/')
        || !path.bytes().all(|b| (0x21..=0x7e).contains(&b))
        || headers.len() > 32
    {
        return Err(Error::Protocol("request line"));
    }
    let mut cursor = 0usize;
    push_bytes(out, &mut cursor, method.as_bytes())?;
    push_bytes(out, &mut cursor, b" ")?;
    push_bytes(out, &mut cursor, path.as_bytes())?;
    push_bytes(out, &mut cursor, b" HTTP/1.1\r\n")?;
    for &(name, value) in headers {
        if name.is_empty()
            || !name.bytes().all(valid_http_token)
            || !value
                .bytes()
                .all(|b| b == b'\t' || (0x20..=0x7e).contains(&b))
            || name.eq_ignore_ascii_case("Content-Length")
            || name.eq_ignore_ascii_case("Transfer-Encoding")
        {
            return Err(Error::Protocol("request header"));
        }
        push_bytes(out, &mut cursor, name.as_bytes())?;
        push_bytes(out, &mut cursor, b": ")?;
        push_bytes(out, &mut cursor, value.as_bytes())?;
        push_bytes(out, &mut cursor, b"\r\n")?;
    }
    push_bytes(out, &mut cursor, b"Content-Length: ")?;
    let len_digits = write_u64_ascii(&mut out[cursor..], body.len() as u64)?;
    cursor += len_digits;
    push_bytes(out, &mut cursor, b"\r\n\r\n")?;
    push_bytes(out, &mut cursor, body)?;
    Ok(cursor)
}

/// Allocation-free HTTP/1.1 response parser returning `(status, body_slice)` from a contiguous buffer.
pub fn decode_http_response_slice(raw: &[u8]) -> Result<(u16, &[u8])> {
    let mut headers = [httparse::EMPTY_HEADER; 16];
    let mut resp = httparse::Response::new(&mut headers);
    let body_start = match resp
        .parse(raw)
        .map_err(|_| Error::Protocol("HTTP header"))?
    {
        httparse::Status::Complete(n) => n,
        httparse::Status::Partial => return Err(Error::Protocol("incomplete HTTP response")),
    };
    let status = resp.code.ok_or(Error::Protocol("HTTP status"))?;
    let mut content_len: Option<usize> = None;
    for h in resp.headers.iter() {
        if h.name.eq_ignore_ascii_case("content-length") {
            let text =
                std::str::from_utf8(h.value).map_err(|_| Error::Protocol("Content-Length"))?;
            let parsed = text
                .trim()
                .parse::<usize>()
                .map_err(|_| Error::Protocol("Content-Length"))?;
            if content_len.replace(parsed).is_some() {
                return Err(Error::Protocol("duplicate Content-Length"));
            }
        }
    }
    let body = &raw[body_start..];
    if let Some(expected) = content_len {
        if body.len() < expected {
            return Err(Error::Protocol("truncated HTTP body"));
        }
        Ok((status, &body[..expected]))
    } else {
        Ok((status, body))
    }
}

/// Allocation-free RFC 6455 masked client frame encoder using a deterministic 4-byte `mask` (zero syscalls).
pub fn encode_ws_client_frame_into(
    out: &mut [u8],
    opcode: Opcode,
    mask: [u8; 4],
    payload: &[u8],
) -> Result<usize> {
    if matches!(opcode, Opcode::Close | Opcode::Ping | Opcode::Pong) && payload.len() > 125 {
        return Err(Error::Protocol("control length"));
    }
    if opcode == Opcode::Text && std::str::from_utf8(payload).is_err() {
        return Err(Error::Protocol("text utf8"));
    }
    let header_len: usize = if payload.len() < 126 {
        6
    } else if payload.len() <= u16::MAX as usize {
        8
    } else {
        14
    };
    let total = header_len.checked_add(payload.len()).ok_or(Error::Bounds)?;
    if out.len() < total {
        return Err(Error::Bounds);
    }
    out[0] = 0x80 | (opcode as u8);
    let mask_offset = if payload.len() < 126 {
        out[1] = 0x80 | (payload.len() as u8);
        2
    } else if payload.len() <= u16::MAX as usize {
        out[1] = 0x80 | 126;
        out[2..4].copy_from_slice(&(payload.len() as u16).to_be_bytes());
        4
    } else {
        out[1] = 0x80 | 127;
        out[2..10].copy_from_slice(&(payload.len() as u64).to_be_bytes());
        10
    };
    out[mask_offset..mask_offset + 4].copy_from_slice(&mask);
    let body_offset = mask_offset + 4;
    for (i, &b) in payload.iter().enumerate() {
        out[body_offset + i] = b ^ mask[i & 3];
    }
    Ok(total)
}

/// Allocation-free RFC 6455 unmasked server frame encoder for loopback synthetic acknowledgements.
pub fn encode_ws_server_frame_into(
    out: &mut [u8],
    opcode: Opcode,
    payload: &[u8],
) -> Result<usize> {
    let header_len: usize = if payload.len() < 126 {
        2
    } else if payload.len() <= u16::MAX as usize {
        4
    } else {
        10
    };
    let total = header_len.checked_add(payload.len()).ok_or(Error::Bounds)?;
    if out.len() < total {
        return Err(Error::Bounds);
    }
    out[0] = 0x80 | (opcode as u8);
    let body_offset = if payload.len() < 126 {
        out[1] = payload.len() as u8;
        2
    } else if payload.len() <= u16::MAX as usize {
        out[1] = 126;
        out[2..4].copy_from_slice(&(payload.len() as u16).to_be_bytes());
        4
    } else {
        out[1] = 127;
        out[2..10].copy_from_slice(&(payload.len() as u64).to_be_bytes());
        10
    };
    out[body_offset..total].copy_from_slice(payload);
    Ok(total)
}

/// Allocation-free single-frame RFC 6455 unmasked server frame decoder.
pub fn decode_ws_server_frame_slice(raw: &[u8]) -> Result<(Opcode, &[u8])> {
    if raw.len() < 2 {
        return Err(Error::Protocol("short ws frame"));
    }
    if raw[0] & 0x70 != 0 || raw[0] & 0x80 == 0 || raw[1] & 0x80 != 0 {
        return Err(Error::Protocol("ws flags"));
    }
    let opcode = match raw[0] & 0x0f {
        1 => Opcode::Text,
        2 => Opcode::Binary,
        8 => Opcode::Close,
        9 => Opcode::Ping,
        10 => Opcode::Pong,
        _ => return Err(Error::Protocol("ws opcode")),
    };
    let (len, offset) = match raw[1] & 0x7f {
        n @ 0..=125 => (usize::from(n), 2usize),
        126 => {
            if raw.len() < 4 {
                return Err(Error::Protocol("short ws len16"));
            }
            (usize::from(u16::from_be_bytes([raw[2], raw[3]])), 4usize)
        }
        _ => {
            if raw.len() < 10 {
                return Err(Error::Protocol("short ws len64"));
            }
            let n = u64::from_be_bytes(
                raw[2..10]
                    .try_into()
                    .map_err(|_| Error::Protocol("ws len64"))?,
            );
            (usize::try_from(n).map_err(|_| Error::Bounds)?, 10usize)
        }
    };
    let end = offset.checked_add(len).ok_or(Error::Bounds)?;
    if raw.len() < end {
        return Err(Error::Protocol("truncated ws payload"));
    }
    let payload = &raw[offset..end];
    if opcode == Opcode::Text && std::str::from_utf8(payload).is_err() {
        return Err(Error::Protocol("ws utf8"));
    }
    Ok((opcode, payload))
}

/// Allocation-free extractor for an unescaped JSON `"key":"value"` or `"key":123` token in `json`.
pub fn find_json_field<'a>(json: &'a [u8], quoted_key: &[u8]) -> Option<&'a [u8]> {
    if quoted_key.len() < 2 || json.len() <= quoted_key.len() {
        return None;
    }
    let mut i = 0usize;
    while i + quoted_key.len() < json.len() {
        if &json[i..i + quoted_key.len()] == quoted_key {
            let mut j = i + quoted_key.len();
            while j < json.len() && matches!(json[j], b' ' | b'\t' | b'\r' | b'\n') {
                j += 1;
            }
            if j < json.len() && json[j] == b':' {
                j += 1;
                while j < json.len() && matches!(json[j], b' ' | b'\t' | b'\r' | b'\n') {
                    j += 1;
                }
                if j >= json.len() {
                    return None;
                }
                if json[j] == b'"' {
                    let start = j + 1;
                    let mut end = start;
                    while end < json.len() && json[end] != b'"' {
                        if json[end] == b'\\' {
                            end = end.saturating_add(2);
                        } else {
                            end += 1;
                        }
                    }
                    return (end < json.len()).then_some(&json[start..end]);
                }
                let start = j;
                let mut end = start;
                while end < json.len()
                    && !matches!(json[end], b',' | b'}' | b']' | b' ' | b'\r' | b'\n')
                {
                    end += 1;
                }
                return (end > start).then_some(&json[start..end]);
            }
        }
        i += 1;
    }
    None
}

/// Parses an unsigned decimal ASCII string (e.g. `b"31020.1"` with `frac_digits = 1` -> `310201`) into integer ticks/lots.
pub fn parse_decimal_units(text: &[u8], frac_digits: u32) -> Option<i64> {
    if text.is_empty() || frac_digits > 18 {
        return None;
    }
    let mut whole: i64 = 0;
    let mut frac: i64 = 0;
    let mut seen_dot = false;
    let mut frac_seen: u32 = 0;
    let mut any_digit = false;
    for &b in text {
        if b == b'.' {
            if seen_dot {
                return None;
            }
            seen_dot = true;
            continue;
        }
        if !b.is_ascii_digit() {
            return None;
        }
        any_digit = true;
        let d = i64::from(b - b'0');
        if !seen_dot {
            whole = whole.checked_mul(10)?.checked_add(d)?;
        } else if frac_seen < frac_digits {
            frac = frac.checked_mul(10)?.checked_add(d)?;
            frac_seen += 1;
        } else if d != 0 {
            return None;
        }
    }
    if !any_digit {
        return None;
    }
    while frac_seen < frac_digits {
        frac = frac.checked_mul(10)?;
        frac_seen += 1;
    }
    let scale = 10_i64.checked_pow(frac_digits)?;
    whole.checked_mul(scale)?.checked_add(frac)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tracker_respects_zero_disabled_idle_window_and_activity_reset() {
        let mut disabled = WarmupTracker::disabled();
        assert!(!disabled.should_warm(0));
        assert!(!disabled.should_warm(1_000_000_000));
        assert_eq!(disabled.warm_count(), 0);

        let mut tracker = WarmupTracker::new(WarmupConfig::new(1_000_000));
        assert!(!tracker.should_warm(999_999));
        assert!(tracker.should_warm(1_000_000));
        assert_eq!(tracker.warm_count(), 1);
        assert_eq!(tracker.last_warm_ns(), 1_000_000);

        // Not yet idle for another full interval.
        assert!(!tracker.should_warm(1_500_000));
        // Real activity at 1_800_000 resets the idle window.
        tracker.record_activity(1_800_000);
        assert!(!tracker.should_warm(2_000_000));
        assert!(!tracker.should_warm(2_799_999));
        assert!(tracker.should_warm(2_800_000));
        assert_eq!(tracker.warm_count(), 2);
    }

    #[test]
    fn loopback_tls_warmer_completes_handshake_and_hundred_roundtrips() {
        let mut warmer = TlsLoopbackWarmer::new().expect("in-memory TLS 1.3 loopback handshake");
        let base_addr = warmer.scratch_addr();
        let mut ack_buf = [0u8; 512];
        for i in 0..100u64 {
            let mut req_buf = [0u8; WARMUP_SCRATCH_BYTES];
            let mut seq_ascii = [0u8; 20];
            let n_seq = write_u64_ascii(&mut seq_ascii, 1_700_000_000_000 + i).unwrap();
            let seq_str = std::str::from_utf8(&seq_ascii[..n_seq]).unwrap();
            let req_len = encode_http_request_into(
                &mut req_buf,
                "POST",
                "/v5/order/create",
                &[
                    ("Host", "api.bybit.com"),
                    ("Content-Type", "application/json"),
                    ("X-BAPI-TIMESTAMP", seq_str),
                ],
                br#"{"category":"spot","symbol":"BTCUSDT","side":"Buy","orderType":"Limit","qty":"0.010","price":"31020.1"}"#,
            )
            .unwrap();
            let (req_dec, ack_dec) = warmer
                .warm_step_exchange(&req_buf[..req_len], DEFAULT_DUMMY_HTTP_ACK, &mut ack_buf)
                .unwrap();
            assert_eq!(req_dec, req_len);
            assert_eq!(ack_dec, DEFAULT_DUMMY_HTTP_ACK.len());
            let (status, body) = decode_http_response_slice(&ack_buf[..ack_dec]).unwrap();
            assert_eq!(status, 200);
            assert_eq!(
                find_json_field(body, b"\"orderId\""),
                Some(&b"10000001"[..])
            );
            assert_eq!(warmer.scratch_addr(), base_addr);
        }
    }
}
