//! Blocking, deadline-bounded TLS, pre-warmed background dialer handoff (`LiveNetwork`), and
//! credential helpers for spot account connections.
use crate::{Credentials, SpotVenue};
use serde_json::Value;
use std::{
    env,
    error::Error,
    fs::File,
    io::{self, BufReader, Read, Write},
    net::{IpAddr, SocketAddr, TcpStream, ToSocketAddrs},
    path::PathBuf,
    sync::Arc,
    thread,
    time::{Duration, Instant},
};
use tickwire_transport::{
    Connection, IoBudget, Limits, PreparedSend, Security, SendState,
    dialer::{DialBackoff, DialBaton, DialSecurity, DialSpec, DialTransport, DialedLink, Dialer},
    http::{self, Response, ResponseDecoder},
    rustls::{
        self,
        pki_types::{CertificateDer, ServerName, pem::PemObject},
    },
};

pub type Result<T> = std::result::Result<T, Box<dyn Error>>;

/// Non-blocking `tickwire_transport::Connection` wrapper supporting both direct gateway/reactor
/// handoff (`into_connection`) and deadline-bounded `Read + Write` streaming (`PrivateStream` / REST).
pub struct SocketConnection {
    connection: Connection,
    peer: SocketAddr,
    connect_rtt_us: Option<u32>,
    max_send_bytes: usize,
    read_timeout: Option<Duration>,
    write_timeout: Duration,
}

impl SocketConnection {
    pub fn from_dialed(link: DialedLink, max_send_bytes: usize) -> Self {
        Self {
            connection: link.connection,
            peer: link.peer,
            connect_rtt_us: Some(link.connect_rtt_us),
            max_send_bytes: max_send_bytes.max(1),
            read_timeout: Some(Duration::from_secs(3)),
            write_timeout: Duration::from_secs(3),
        }
    }

    pub fn connect(
        host: &str,
        address: SocketAddr,
        local_ip: Option<IpAddr>,
        tls: Arc<rustls::ClientConfig>,
        limits: Limits,
        deadline: Instant,
    ) -> Result<Self> {
        let server_name = ServerName::try_from(host.to_owned())?;
        let started = Instant::now();
        let mut connection = Connection::connect_from(
            address,
            local_ip,
            Security::Tls {
                server_name,
                config: tls,
            },
            limits,
            deadline,
        )?;
        while !connection.is_ready() {
            if Instant::now() >= deadline {
                return Err("exchange TLS handshake timeout".into());
            }
            let report = connection.drive(Instant::now(), IoBudget::default())?;
            if report.peer_closed {
                return Err("exchange peer closed during TLS handshake".into());
            }
            if !connection.is_ready() && report.network_read == 0 && report.network_written == 0 {
                thread::sleep(Duration::from_micros(200));
            }
        }
        let rtt_us = started.elapsed().as_micros().min(u128::from(u32::MAX)) as u32;
        Ok(Self {
            connection,
            peer: address,
            connect_rtt_us: Some(rtt_us),
            max_send_bytes: limits.max_send_bytes.max(1),
            read_timeout: Some(Duration::from_secs(3)),
            write_timeout: Duration::from_secs(3),
        })
    }

    pub fn set_read_timeout(&mut self, timeout: Option<Duration>) {
        self.read_timeout = timeout;
    }

    pub fn set_write_timeout(&mut self, timeout: Duration) {
        self.write_timeout = timeout;
    }

    pub fn peer(&self) -> SocketAddr {
        self.peer
    }

    pub fn connect_rtt_us(&self) -> Option<u32> {
        self.connect_rtt_us
    }

    pub fn is_ready(&self) -> bool {
        self.connection.is_ready()
    }

    pub fn connection(&self) -> &Connection {
        &self.connection
    }

    pub fn connection_mut(&mut self) -> &mut Connection {
        &mut self.connection
    }

    pub fn into_connection(self) -> Connection {
        self.connection
    }
}

impl Read for SocketConnection {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }
        let n = self.connection.read(out);
        if n > 0 {
            return Ok(n);
        }
        let deadline = self.read_timeout.map(|d| Instant::now() + d);
        loop {
            let driven = self.connection.drive(Instant::now(), IoBudget::default());
            let n = self.connection.read(out);
            if n > 0 {
                return Ok(n);
            }
            let report = driven.map_err(|e| io::Error::other(e.to_string()))?;
            if report.peer_closed {
                return Ok(0);
            }
            if deadline.is_some_and(|d| Instant::now() >= d) {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "socket read timeout",
                ));
            }
            if report.network_read == 0 && report.network_written == 0 {
                thread::sleep(Duration::from_micros(200));
            }
        }
    }
}

impl Write for SocketConnection {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        let chunk = &buf[..buf.len().min(self.max_send_bytes)];
        let deadline = Instant::now() + self.write_timeout;
        while !self.connection.is_ready() {
            if Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "socket handshake timeout",
                ));
            }
            let report = self
                .connection
                .drive(Instant::now(), IoBudget::default())
                .map_err(|e| io::Error::other(e.to_string()))?;
            if report.peer_closed {
                return Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "peer closed before write",
                ));
            }
            if !self.connection.is_ready()
                && report.network_read == 0
                && report.network_written == 0
            {
                thread::sleep(Duration::from_micros(200));
            }
        }
        let prepared = PreparedSend::new(chunk, deadline, self.max_send_bytes)
            .map_err(|e| io::Error::other(e.to_string()))?;
        self.connection
            .queue(prepared)
            .map_err(|e| io::Error::other(e.to_string()))?;
        loop {
            let drv = self
                .connection
                .drive(Instant::now(), IoBudget::default())
                .map_err(|e| io::Error::other(e.to_string()))?;
            let _ = self
                .connection
                .try_send(Instant::now(), || true)
                .map_err(|e| io::Error::other(e.to_string()))?;
            let drv2 = self
                .connection
                .drive(Instant::now(), IoBudget::default())
                .map_err(|e| io::Error::other(e.to_string()))?;
            if let Some(finished) = self.connection.take_finished() {
                return match finished.state {
                    SendState::Flushed => Ok(chunk.len()),
                    other => Err(io::Error::other(format!("socket send {other:?}"))),
                };
            }
            if drv.peer_closed || drv2.peer_closed {
                let _ = self.connection.cancel();
                let _ = self.connection.take_finished();
                return Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "peer closed during write",
                ));
            }
            if Instant::now() >= deadline {
                let _ = self.connection.cancel();
                let _ = self.connection.take_finished();
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "socket write timeout",
                ));
            }
            if drv.network_written == 0 && drv2.network_written == 0 {
                thread::sleep(Duration::from_micros(200));
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Live spot endpoint dialer integrating optional background [`Dialer`] + [`DialBaton`] pre-warming
/// so TCP+TLS reconnects establish off the `AccountRunner` hot path.
#[derive(Clone)]
pub struct LiveNetwork {
    host: String,
    port: u16,
    peers: Vec<SocketAddr>,
    local_ip: Option<IpAddr>,
    tls: Arc<rustls::ClientConfig>,
    limits: Limits,
    connect_timeout: Duration,
    backoff: DialBackoff,
    dialer: Option<Arc<Dialer>>,
    slot_id: u64,
    baton: Option<Arc<DialBaton>>,
}

impl LiveNetwork {
    pub fn new(host: impl Into<String>, port: u16, tls: Arc<rustls::ClientConfig>) -> Self {
        Self {
            host: host.into(),
            port,
            peers: Vec::new(),
            local_ip: None,
            tls,
            limits: Limits::default(),
            connect_timeout: Duration::from_secs(3),
            backoff: DialBackoff::default(),
            dialer: None,
            slot_id: 0,
            baton: None,
        }
    }

    pub fn with_peers(mut self, peers: Vec<SocketAddr>) -> Self {
        self.peers = peers;
        self
    }

    pub fn with_local_ip(mut self, local_ip: Option<IpAddr>) -> Self {
        self.local_ip = local_ip;
        self
    }

    pub fn with_limits(mut self, limits: Limits) -> Self {
        self.limits = limits;
        self
    }

    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    pub fn with_backoff(mut self, backoff: DialBackoff) -> Self {
        self.backoff = backoff;
        self
    }

    pub fn with_dial_baton(mut self, baton: Arc<DialBaton>) -> Self {
        self.baton = Some(baton);
        self
    }

    pub fn attach_dialer(&mut self, dialer: Arc<Dialer>, slot_id: u64) -> Result<Arc<DialBaton>> {
        let server_name = ServerName::try_from(self.host.clone())?;
        let spec = DialSpec {
            host: self.host.clone(),
            port: self.port,
            initial_peers: self.peers.clone(),
            local_ip: self.local_ip,
            security: DialSecurity::Tls {
                server_name,
                config: self.tls.clone(),
            },
            limits: self.limits,
            transport: DialTransport::PlainOrTls,
            connect_timeout: self.connect_timeout,
            dial_timeout: self.connect_timeout.max(Duration::from_secs(5)),
            backoff: self.backoff,
        };
        let baton = dialer.add_slot(slot_id, spec)?;
        self.dialer = Some(dialer);
        self.slot_id = slot_id;
        self.baton = Some(Arc::clone(&baton));
        Ok(baton)
    }

    pub fn baton(&self) -> Option<&Arc<DialBaton>> {
        self.baton.as_ref()
    }

    /// Requests the background [`Dialer`] to establish the next TCP+TLS connection off the hot path.
    pub fn request_prewarm(&self, due_mono_ns: u64) -> bool {
        self.baton
            .as_ref()
            .is_some_and(|baton| baton.want(due_mono_ns))
    }

    /// Non-blocking hot-path handoff: takes a pre-warmed ready [`SocketConnection`] if available.
    pub fn try_take_ready(&self) -> Option<SocketConnection> {
        let link = self.baton.as_ref()?.take_ready()?;
        Some(SocketConnection::from_dialed(
            link,
            self.limits.max_send_bytes,
        ))
    }

    /// Notifies the background [`Dialer`] that the active socket closed and schedules a replacement.
    pub fn notify_disconnected(&self, due_mono_ns: u64) {
        if let Some(dialer) = &self.dialer {
            let _ = dialer.release_slot_peer(self.slot_id);
        }
        if let Some(baton) = &self.baton {
            let _ = baton.want(due_mono_ns);
        }
    }

    /// Takes a background pre-warmed [`SocketConnection`] if ready, falling back to bounded direct connect.
    pub fn connect_or_take(&self) -> Result<SocketConnection> {
        if let Some(ready) = self.try_take_ready() {
            return Ok(ready);
        }
        let deadline = Instant::now() + Duration::from_secs(10);
        let resolved: Vec<SocketAddr> = if !self.peers.is_empty() {
            self.peers.clone()
        } else {
            (self.host.as_str(), self.port).to_socket_addrs()?.collect()
        };
        for addr in resolved {
            let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                break;
            };
            let attempt_deadline = Instant::now() + remaining.min(self.connect_timeout);
            if let Ok(conn) = SocketConnection::connect(
                &self.host,
                addr,
                self.local_ip,
                self.tls.clone(),
                self.limits,
                attempt_deadline,
            ) {
                return Ok(conn);
            }
        }
        Err("cannot connect to exchange TLS endpoint".into())
    }

    pub fn get(&self, wire: &[u8]) -> Result<Response> {
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut io = self.connect_or_take()?;
        io.write_all(wire)?;
        io.flush()?;
        let mut decoder = ResponseDecoder::new(2 * 1024 * 1024, 128)?;
        let mut chunk = [0; 8192];
        while Instant::now() < deadline {
            let n = io.read(&mut chunk)?;
            if n == 0 {
                decoder.finish();
                return decoder
                    .next_response()?
                    .ok_or_else(|| "incomplete HTTP response".into());
            }
            decoder.push(&chunk[..n])?;
            if let Some(response) = decoder.next_response()? {
                return Ok(response);
            }
        }
        Err("exchange response timeout".into())
    }
}

pub fn roots() -> Result<Arc<rustls::ClientConfig>> {
    let path = env::var_os("SSL_CERT_FILE")
        .map(PathBuf::from)
        .unwrap_or_else(|| "/etc/ssl/certs/ca-certificates.crt".into());
    let f = File::open(path).map_err(|_| "cannot open configured TLS root bundle")?;
    let mut store = rustls::RootCertStore::empty();
    for cert in CertificateDer::pem_reader_iter(&mut BufReader::new(f.take(4 * 1024 * 1024))) {
        store.add(cert?)?;
    }
    if store.is_empty() {
        return Err("no TLS trust anchors loaded".into());
    }
    Ok(Arc::new(
        rustls::ClientConfig::builder()
            .with_root_certificates(store)
            .with_no_client_auth(),
    ))
}
pub fn get(host: &'static str, tls: Arc<rustls::ClientConfig>, wire: &[u8]) -> Result<Response> {
    get_at(host, (host, 443).to_socket_addrs()?, tls, wire)
}
pub fn get_at(
    host: &'static str,
    addrs: impl IntoIterator<Item = SocketAddr>,
    tls: Arc<rustls::ClientConfig>,
    wire: &[u8],
) -> Result<Response> {
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut io = tls_at(host, addrs, tls)?;
    io.write_all(wire)?;
    io.flush()?;
    let mut decoder = ResponseDecoder::new(2 * 1024 * 1024, 128)?;
    let mut chunk = [0; 8192];
    while Instant::now() < deadline {
        let n = io.read(&mut chunk)?;
        if n == 0 {
            decoder.finish();
            return decoder
                .next_response()?
                .ok_or_else(|| "incomplete HTTP response".into());
        }
        decoder.push(&chunk[..n])?;
        if let Some(response) = decoder.next_response()? {
            return Ok(response);
        }
    }
    Err("exchange response timeout".into())
}
pub fn tls_at(
    host: &'static str,
    addrs: impl IntoIterator<Item = SocketAddr>,
    tls: Arc<rustls::ClientConfig>,
) -> Result<rustls::StreamOwned<rustls::ClientConnection, TcpStream>> {
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut connected = None;
    for addr in addrs {
        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
            break;
        };
        if let Ok(socket) = TcpStream::connect_timeout(&addr, remaining.min(Duration::from_secs(3)))
        {
            connected = Some(socket);
            break;
        }
    }
    let socket = connected.ok_or("cannot connect to exchange TLS endpoint")?;
    socket.set_read_timeout(Some(Duration::from_secs(3)))?;
    socket.set_write_timeout(Some(Duration::from_secs(3)))?;
    Ok(rustls::StreamOwned::new(
        rustls::ClientConnection::new(tls, ServerName::try_from(host)?)?,
        socket,
    ))
}
pub fn venue_time(
    venue: SpotVenue,
    host: &'static str,
    tls: Arc<rustls::ClientConfig>,
) -> Result<(u64, Instant)> {
    let path = if venue == SpotVenue::Bybit {
        "/v5/market/time"
    } else {
        "/api/v5/public/time"
    };
    let wire = http::encode_request("GET", path, &[("Host", host)], &[], 4096)?;
    let start = Instant::now();
    let response = get(host, tls, &wire)?;
    let received = Instant::now();
    let rtt = received - start;
    if response.status != 200 {
        return Err(format!("public time query HTTP {}", response.status).into());
    }
    if rtt > Duration::from_secs(2) {
        return Err("public time sample exceeded two-second round-trip budget".into());
    }
    let body: Value =
        serde_json::from_slice(&response.body).map_err(|_| "invalid public time response")?;
    let ms = if venue == SpotVenue::Bybit && body["retCode"].as_i64() == Some(0) {
        body["result"]["timeNano"]
            .as_str()
            .and_then(|v| v.parse::<u64>().ok())
            .map(|ns| ns / 1_000_000)
    } else if venue == SpotVenue::Okx && body["code"].as_str() == Some("0") {
        body["data"][0]["ts"]
            .as_str()
            .and_then(|v| v.parse::<u64>().ok())
    } else {
        None
    }
    .filter(|t| *t != 0)
    .ok_or("unusable venue clock sample")?;
    Ok((ms, start + rtt / 2))
}
pub fn credential(name: &'static str) -> Result<String> {
    let value = env::var_os(name);
    let file = env::var_os(format!("{name}_FILE"));
    match (value, file) {
        (Some(v), None) => v
            .into_string()
            .map_err(|_| format!("invalid {name}").into()),
        (None, Some(path)) => {
            let mut value = String::new();
            File::open(path)
                .map_err(|_| format!("cannot open {name}_FILE"))?
                .take(1027)
                .read_to_string(&mut value)
                .map_err(|_| format!("cannot read {name}_FILE"))?;
            if value.ends_with('\n') {
                value.pop();
                if value.ends_with('\r') {
                    value.pop();
                }
            }
            if value.is_empty() || value.len() > 1024 || value.contains(['\r', '\n']) {
                return Err(format!("invalid {name}_FILE").into());
            }
            Ok(value)
        }
        (None, None) => Err(format!("missing {name} or {name}_FILE").into()),
        _ => Err(format!("configure only one source for {name}").into()),
    }
}

/// Owned credential material. Debug and serialization are intentionally unavailable.
#[derive(Clone)]
pub struct OwnedCredentials {
    venue: SpotVenue,
    key: String,
    secret: String,
    pass: Option<String>,
}
impl OwnedCredentials {
    pub fn bybit(key: impl Into<String>, secret: impl Into<String>) -> Result<Self> {
        let out = Self {
            venue: SpotVenue::Bybit,
            key: key.into(),
            secret: secret.into(),
            pass: None,
        };
        if !out.credentials().valid() {
            return Err("invalid Bybit credential material".into());
        }
        Ok(out)
    }
    pub fn okx(
        key: impl Into<String>,
        secret: impl Into<String>,
        passphrase: impl Into<String>,
    ) -> Result<Self> {
        let out = Self {
            venue: SpotVenue::Okx,
            key: key.into(),
            secret: secret.into(),
            pass: Some(passphrase.into()),
        };
        if !out.credentials().valid() {
            return Err("invalid OKX credential material".into());
        }
        Ok(out)
    }
    pub fn load(venue: SpotVenue) -> Result<Self> {
        Ok(Self {
            venue,
            key: credential("TICKWIRE_SPOT_KEY")?,
            secret: credential("TICKWIRE_SPOT_SECRET")?,
            pass: if venue == SpotVenue::Okx {
                Some(credential("TICKWIRE_SPOT_PASSPHRASE")?)
            } else {
                None
            },
        })
    }
    pub fn credentials(&self) -> Credentials<'_> {
        if let Some(pass) = &self.pass {
            Credentials::okx(&self.key, self.secret.as_bytes(), pass)
        } else {
            Credentials::bybit(&self.key, self.secret.as_bytes())
        }
    }
    pub fn venue(&self) -> SpotVenue {
        self.venue
    }
}
