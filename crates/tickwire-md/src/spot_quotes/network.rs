use super::*;
use std::{
    io::{self, BufReader, Read, Write},
    net::{TcpStream, ToSocketAddrs},
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};
use tickwire_transport::{
    http::ResponseDecoder,
    rustls::{
        self,
        pki_types::{CertificateDer, ServerName, pem::PemObject},
    },
    ws::{ClientHandshake, Decoder, Opcode, PollMessage, encode_client},
};

pub(super) const MAX_MESSAGE: usize = 16 * 1024;
const IO_TIMEOUT: Duration = Duration::from_secs(1);
const SETUP_TIMEOUT: Duration = Duration::from_secs(15);
const PING_PERIOD: Duration = Duration::from_secs(15);
const IDLE_TIMEOUT: Duration = Duration::from_secs(35);

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Notice<'a> {
    Quote(Quote<'a>),
    Unavailable {
        venue_id: u32,
        symbol: &'a str,
        instrument_id: u64,
        instrument_version: u64,
        connection_epoch: u64,
        received_mono_ns: u64,
        reason: String,
    },
}

fn emit(output: &mut impl Write, notice: &Notice<'_>) -> Result<()> {
    serde_json::to_writer(&mut *output, notice)?;
    output.write_all(b"\n")?;
    output.flush()?;
    Ok(())
}

fn at(origin: Instant) -> Result<u64> {
    Ok(origin.elapsed().as_nanos().try_into()?)
}

fn endpoint(venue: SpotVenue) -> (&'static str, u16, &'static str) {
    match venue {
        SpotVenue::Bybit => ("stream.bybit.com", 443, "/v5/public/spot"),
        SpotVenue::Okx => ("ws.okx.com", 443, "/ws/v5/public"),
    }
}

fn tls_config(file: &Path) -> Result<Arc<rustls::ClientConfig>> {
    let bytes = crate::config::read(file, 4 * 1024 * 1024)?;
    let mut roots = rustls::RootCertStore::empty();
    for cert in CertificateDer::pem_reader_iter(&mut BufReader::new(bytes.as_slice())) {
        roots.add(cert?)?;
    }
    if roots.is_empty() {
        return Err(invalid("no TLS trust anchors in PEM file"));
    }
    Ok(Arc::new(
        rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    ))
}

fn connect(
    host: &str,
    port: u16,
    proxy: Option<std::net::SocketAddr>,
    tls: &Arc<rustls::ClientConfig>,
) -> Result<rustls::StreamOwned<rustls::ClientConnection, TcpStream>> {
    let addresses = match proxy {
        Some(address) => vec![address],
        None => (host, port).to_socket_addrs()?.collect(),
    };
    let mut last = None;
    for address in addresses {
        let mut stream = match TcpStream::connect_timeout(&address, Duration::from_secs(5)) {
            Ok(stream) => stream,
            Err(error) => {
                last = Some(error);
                continue;
            }
        };
        stream.set_read_timeout(Some(SETUP_TIMEOUT))?;
        stream.set_write_timeout(Some(SETUP_TIMEOUT))?;
        stream.set_nodelay(true)?;
        if proxy.is_some() {
            open_tunnel(&mut stream, host, port)?;
        }
        let mut conn =
            rustls::ClientConnection::new(tls.clone(), ServerName::try_from(host.to_owned())?)?;
        conn.complete_io(&mut stream)?;
        stream.set_read_timeout(Some(IO_TIMEOUT))?;
        return Ok(rustls::StreamOwned::new(conn, stream));
    }
    Err(last.map_or_else(|| invalid("no public WebSocket address"), Into::into))
}

fn open_tunnel(stream: &mut TcpStream, host: &str, port: u16) -> Result<()> {
    write!(
        stream,
        "CONNECT {host}:{port} HTTP/1.1\r\nHost: {host}:{port}\r\n\r\n"
    )?;
    let mut decoder = ResponseDecoder::new(8192, 32)?;
    let mut buf = [0; 1024];
    let start = Instant::now();
    loop {
        if start.elapsed() >= SETUP_TIMEOUT {
            return Err(invalid("spot CONNECT timeout"));
        }
        let n = stream.read(&mut buf)?;
        if n == 0 {
            return Err(invalid("proxy closed CONNECT"));
        }
        decoder.push(&buf[..n])?;
        if let Some(response) = decoder.next_head_response()? {
            if response.status != 200 || !decoder.take_buffered().is_empty() {
                return Err(invalid("spot CONNECT rejected"));
            }
            return Ok(());
        }
    }
}

fn read(stream: &mut impl Read, buffer: &mut [u8]) -> Result<usize> {
    match stream.read(buffer) {
        Ok(0) => Err(invalid("public WebSocket peer closed")),
        Err(e)
            if matches!(
                e.kind(),
                io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut | io::ErrorKind::Interrupted
            ) =>
        {
            Ok(0)
        }
        result => Ok(result?),
    }
}

fn frame(stream: &mut impl Write, opcode: Opcode, payload: &[u8]) -> Result<()> {
    stream.write_all(&encode_client(opcode, payload, MAX_MESSAGE)?)?;
    stream.flush()?;
    Ok(())
}

fn session<S: Read + Write>(
    config: &QuoteConfig,
    epoch: u64,
    clock: &mut impl FnMut() -> Result<u64>,
    mut stream: S,
    sink: &mut impl FnMut(Notice<'_>) -> Result<()>,
    healthy: &mut bool,
) -> Result<()> {
    let (host, port, path) = endpoint(config.venue);
    let authority = if port == 443 {
        host.to_owned()
    } else {
        format!("{host}:{port}")
    };
    let handshake = ClientHandshake::new(&authority, path, &[], 4096)?;
    let mut http = ResponseDecoder::new(32 * 1024, 64)?;
    let mut ws = Decoder::new(MAX_MESSAGE, MAX_MESSAGE)?;
    let mut state = Session::new(config, epoch)?;
    let mut buf = [0; 8192];
    stream.write_all(handshake.request())?;
    stream.flush()?;
    let started = Instant::now();
    let mut received;
    loop {
        if started.elapsed() >= SETUP_TIMEOUT {
            return Err(invalid("public WebSocket upgrade timed out"));
        }
        let n = read(&mut stream, &mut buf)?;
        received = clock()?;
        http.push(&buf[..n])?;
        if let Some(response) = http.next_response()? {
            handshake.validate(&response)?;
            ws.push(&http.take_buffered())?;
            break;
        }
    }
    frame(&mut stream, Opcode::Text, &state.subscription())?;
    let subscribed = Instant::now();
    let mut last_seen = subscribed;
    let mut last_ping = subscribed;
    loop {
        if !*healthy && subscribed.elapsed() >= SETUP_TIMEOUT {
            return Err(invalid("spot first snapshot timed out"));
        }
        if last_seen.elapsed() >= IDLE_TIMEOUT {
            return Err(invalid("spot public heartbeat expired"));
        }
        if last_ping.elapsed() >= PING_PERIOD {
            let ping: &[u8] = match config.venue {
                SpotVenue::Bybit => br#"{"op":"ping"}"#,
                SpotVenue::Okx => b"ping",
            };
            frame(&mut stream, Opcode::Text, ping)?;
            last_ping = Instant::now();
        }
        match ws.poll_message()? {
            PollMessage::NeedInput => {
                let n = read(&mut stream, &mut buf)?;
                if n > 0 {
                    received = clock()?;
                    ws.push(&buf[..n])?;
                }
            }
            PollMessage::Yielded => continue,
            PollMessage::Message(msg) => {
                last_seen = Instant::now();
                match msg.opcode {
                    Opcode::Text => {
                        if let Some(q) = state.on_text(&msg.payload, received)? {
                            sink(Notice::Quote(q))?;
                            *healthy = true;
                        }
                    }
                    Opcode::Ping => frame(&mut stream, Opcode::Pong, &msg.payload)?,
                    Opcode::Pong => (),
                    Opcode::Close => return Err(invalid("public WebSocket closed")),
                    Opcode::Binary => return Err(invalid("unexpected binary spot quote frame")),
                }
            }
        }
    }
}

/// Runs an authenticated public quote stream until the output or local configuration fails.
pub fn run(config: &QuoteConfig, mut output: impl Write) -> Result<()> {
    let origin = Instant::now();
    run_with(config, || at(origin), |notice| emit(&mut output, &notice))
}

/// Typed input for the shared-memory publisher. The caller supplies the same
/// native clock domain used for IPC publication and strategy freshness checks.
pub fn run_with(
    config: &QuoteConfig,
    mut clock: impl FnMut() -> Result<u64>,
    mut sink: impl FnMut(Notice<'_>) -> Result<()>,
) -> Result<()> {
    config.validate()?;
    let roots = config
        .ca_pem
        .as_deref()
        .unwrap_or_else(|| Path::new("/etc/ssl/certs/ca-certificates.crt"));
    let tls = tls_config(roots)?;
    let (host, port, _) = endpoint(config.venue);
    let mut epoch = 1u64;
    let mut backoff = Duration::from_secs(1);
    loop {
        let mut healthy = false;
        let mut sink_error = None;
        let result = connect(host, port, config.proxy_addr, &tls).and_then(|s| {
            session(
                config,
                epoch,
                &mut clock,
                s,
                &mut |n| match sink(n) {
                    Ok(()) => Ok(()),
                    Err(e) => {
                        sink_error = Some(e);
                        Err(invalid("spot quote sink closed"))
                    }
                },
                &mut healthy,
            )
        });
        if let Some(e) = sink_error {
            return Err(e);
        }
        let reason = result
            .err()
            .map_or_else(|| "connection ended".to_owned(), |e| e.to_string());
        sink(Notice::Unavailable {
            venue_id: config.venue.id(),
            symbol: &config.symbol,
            instrument_id: config.instrument_id,
            instrument_version: config.instrument_version,
            connection_epoch: epoch,
            received_mono_ns: clock()?,
            reason,
        })?;
        if healthy {
            backoff = Duration::from_secs(1);
        }
        std::thread::sleep(backoff);
        backoff = (backoff * 2).min(Duration::from_secs(30));
        epoch = epoch
            .checked_add(1)
            .ok_or_else(|| invalid("quote connection epoch exhausted"))?;
    }
}

#[cfg(test)]
mod tests;
