//! Reading the exchange never submits commands or changes local account state.
#[path = "tickwire-spot-observe/watch.rs"]
mod watch;
use serde_json::Value;
use std::{
    env,
    error::Error,
    fs::File,
    io::{self, BufReader, Read, Write},
    net::{SocketAddr, TcpStream, ToSocketAddrs},
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};
use tickwire_spot_execution::{Credentials, SpotVenue, authority, observe::Query};
use tickwire_transport::{
    http::{self, Response, ResponseDecoder},
    rustls::{
        self,
        pki_types::{CertificateDer, ServerName, pem::PemObject},
    },
};
use tickwire_venue::Environment;

const HELP: &str = "tickwire-spot-observe <bybit|okx> <test|production> <command>\n\nCommands\n  order SYMBOL CLIENT_ID\n  open-orders SYMBOL [CURSOR]\n  fills SYMBOL [CURSOR]\n  balance\n  scan EXPECTED_UID START_UTC_MS\n  watch EXPECTED_UID SECONDS (1..60)\n\nRead credentials from TICKWIRE_SPOT_KEY, TICKWIRE_SPOT_SECRET, plus TICKWIRE_SPOT_PASSPHRASE for OKX. Each variable may instead be supplied with its _FILE counterpart. Use read-only credentials. Set SSL_CERT_FILE to a trusted PEM CA bundle if the Linux system bundle is unavailable.\nSingle reads return one page. Scan follows standard spot order/fill pages for every instrument, checks the UID twice, and never authorizes trading. A query miss does not prove the order was never accepted.";
type Result<T> = std::result::Result<T, Box<dyn Error>>;
fn roots() -> Result<Arc<rustls::ClientConfig>> {
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
fn get(host: &'static str, tls: Arc<rustls::ClientConfig>, wire: &[u8]) -> Result<Response> {
    get_at(host, (host, 443).to_socket_addrs()?, tls, wire)
}
fn get_at(
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
fn tls_at(
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
fn venue_time(
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
fn credential(name: &'static str) -> Result<String> {
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
fn run() -> Result<()> {
    let args: Vec<String> = env::args().skip(1).collect();
    if args.as_slice() == ["--help"] || args.as_slice() == ["-h"] {
        println!("{HELP}");
        return Ok(());
    }
    let a: Vec<_> = args.iter().map(String::as_str).collect();
    let [venue, environment, rest @ ..] = a.as_slice() else {
        return Err(HELP.into());
    };
    let venue = match *venue {
        "bybit" => SpotVenue::Bybit,
        "okx" => SpotVenue::Okx,
        _ => return Err(HELP.into()),
    };
    let environment = match *environment {
        "test" => Environment::Test,
        "production" => Environment::Production,
        _ => return Err(HELP.into()),
    };
    enum Action<'a> {
        Page(Query<'a>),
        Scan(&'a str, u64),
        Watch(&'a str, u64),
    }
    let action = match rest {
        ["order", symbol, client_id] => Action::Page(Query::Order { symbol, client_id }),
        ["open-orders", symbol, rest @ ..] if rest.len() <= 1 => Action::Page(Query::OpenOrders {
            symbol: Some(symbol),
            cursor: rest.first().copied(),
        }),
        ["fills", symbol, rest @ ..] if rest.len() <= 1 => Action::Page(Query::Fills {
            symbol: Some(symbol),
            cursor: rest.first().copied(),
            window: None,
        }),
        ["balance"] => Action::Page(Query::Balance),
        ["scan", uid, start] => Action::Scan(
            uid,
            start.parse().map_err(|_| "invalid fill coverage start")?,
        ),
        ["watch", uid, seconds] => {
            let duration = seconds
                .parse::<u64>()
                .map_err(|_| "invalid watch duration")?;
            if !(1..=60).contains(&duration) {
                return Err("watch duration must be between 1 and 60 seconds".into());
            }
            Action::Watch(uid, duration)
        }
        ["new" | "cancel", ..] => {
            return Err("spot mutation requires a qualified private account frontier and an isolated live writer".into());
        }
        _ => return Err(HELP.into()),
    };
    if let Action::Page(query) = action {
        query.path(venue)?;
    }
    let key = credential("TICKWIRE_SPOT_KEY")?;
    let secret = credential("TICKWIRE_SPOT_SECRET")?;
    let pass;
    let credentials = if venue == SpotVenue::Bybit {
        Credentials::bybit(&key, secret.as_bytes())
    } else {
        pass = credential("TICKWIRE_SPOT_PASSPHRASE")?;
        Credentials::okx(&key, secret.as_bytes(), &pass)
    };
    let host = authority(venue, environment);
    let tls = roots()?;
    let (ms, at) = venue_time(venue, host, tls.clone())?;
    let read = |query: Query<'_>| -> Result<Value> {
        if at.elapsed() > Duration::from_secs(120) {
            return Err("exchange clock sample expired during scan".into());
        }
        let stamp = ms
            .checked_add(at.elapsed().as_millis().try_into()?)
            .ok_or("venue timestamp overflow")?;
        let signed = query.signed_get(&credentials, environment, stamp)?;
        let response = get(host, tls.clone(), &signed)?;
        let status = response.status;
        query
            .evidence(venue, response)
            .map_err(|failure| format!("HTTP {status}, {failure}").into())
    };
    let mut page = match action {
        Action::Page(query) => read(query)?,
        Action::Watch(uid, seconds) => {
            let identity = read(Query::Identity)?;
            if identity["success"] != true {
                return Err(format!(
                    "identity HTTP {} venue code {}",
                    identity["http_status"], identity["venue_code"]
                )
                .into());
            }
            if identity["rows"][0]["uid"] != uid {
                return Err("authenticated user does not match expected venue UID".into());
            }
            let (host, path) = tickwire_spot_execution::private::endpoint(venue, environment);
            let stream = tls_at(host, (host, 443).to_socket_addrs()?, tls.clone())?;
            stream
                .sock
                .set_read_timeout(Some(Duration::from_millis(500)))?;
            let mut sink = io::stdout().lock();
            watch::capture(
                stream,
                host,
                path,
                &credentials,
                || ms.checked_add(at.elapsed().as_millis().try_into().ok()?),
                Duration::from_secs(seconds),
                |value| {
                    serde_json::to_writer(&mut sink, &value)?;
                    writeln!(sink)?;
                    sink.flush()?;
                    Ok(())
                },
            )?
        }
        Action::Scan(uid, start) => {
            tickwire_spot_execution::observe::scan(venue, uid, start, ms, read)?
        }
    };
    page["environment"] = serde_json::json!(if environment == Environment::Test {
        "test"
    } else {
        "production"
    });
    page["sampled_venue_ms"] = ms.into();
    let ok = page["success"] == true;
    let mut out = io::stdout().lock();
    serde_json::to_writer(&mut out, &page)?;
    writeln!(out)?;
    if !ok {
        return Err("exchange did not return a successful observation".into());
    }
    Ok(())
}
fn main() -> std::process::ExitCode {
    match run() {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("spot observation: {e}");
            std::process::ExitCode::FAILURE
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::{io::BufRead, net::TcpListener, thread};
    #[test]
    fn trusted_local_exchange_receives_a_signed_get_and_only_status_is_reported_on_denial() {
        for venue in [SpotVenue::Bybit, SpotVenue::Okx] {
            let host = authority(venue, Environment::Test);
            let cert = rcgen::generate_simple_self_signed(vec![host.into()]).unwrap();
            let key = rustls::pki_types::PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
            let server = rustls::ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(vec![cert.cert.der().clone()], key.into())
                .unwrap();
            let mut anchors = rustls::RootCertStore::empty();
            anchors.add(cert.cert.der().clone()).unwrap();
            let client = Arc::new(
                rustls::ClientConfig::builder()
                    .with_root_certificates(anchors)
                    .with_no_client_auth(),
            );
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();
            let job = thread::spawn(move || {
                let (sock, _) = listener.accept().unwrap();
                sock.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
                let conn = rustls::ServerConnection::new(Arc::new(server)).unwrap();
                let mut wire = BufReader::new(rustls::StreamOwned::new(conn, sock));
                let mut head = String::new();
                loop {
                    let len = wire.read_line(&mut head).unwrap();
                    assert!(len > 0);
                    if head.ends_with("\r\n\r\n") {
                        break;
                    }
                }
                assert!(head.starts_with("GET "));
                assert!(head.contains(if venue == SpotVenue::Bybit {
                    "X-BAPI-SIGN:"
                } else {
                    "OK-ACCESS-SIGN:"
                }));
                wire.get_mut()
                    .write_all(b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 6\r\n\r\nsecret")
                    .unwrap();
                wire.get_mut().flush().unwrap();
            });
            let q = Query::Balance;
            let cred = if venue == SpotVenue::Bybit {
                Credentials::bybit("key", b"secret")
            } else {
                Credentials::okx("key", b"secret", "pass")
            };
            let request = q
                .signed_get(&cred, Environment::Test, 1700000000123)
                .unwrap();
            let response = get_at(host, [addr], client, &request).unwrap();
            let report = q.evidence(venue, response).unwrap();
            assert_eq!(report["http_status"], 401);
            assert_eq!(report["success"], false);
            assert!(!report.to_string().contains("secret"));
            job.join().unwrap();
        }
    }
}
