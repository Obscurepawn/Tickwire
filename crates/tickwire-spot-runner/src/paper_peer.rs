//! Fresh, loopback-only exchange. Its certificate and private key never leave this process.
use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};
use tickwire_venue::{Environment, SpotVenue};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
pub(crate) const FIXED_PRICE: &str = "60000";
pub(crate) const SECRET: &[u8] = b"local-paper-secret";
use base64::{Engine as _, engine::general_purpose::STANDARD};
use ring::hmac;
use serde_json::{Value, json};
use std::{
    io::{BufRead, BufReader, Read, Write},
    net::TcpListener,
    sync::Arc,
    thread,
};
use tickwire_transport::rustls;
type Peer = thread::JoinHandle<std::result::Result<(), String>>;
pub fn start(
    venue: SpotVenue,
    count: u8,
) -> Result<(tickwire_account_gateway::ConnectionConfig, Peer)> {
    let host = tickwire_spot_execution::authority(venue, Environment::Test);
    let cert = rcgen::generate_simple_self_signed(vec![host.into()])?;
    let key = rustls::pki_types::PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
    let server = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert.cert.der().clone()], key.into())?;
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone())?;
    let tls = Arc::new(
        rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    );
    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))?;
    let address = listener.local_addr()?;
    let job =
        thread::spawn(move || serve(venue, server, listener, count).map_err(|e| e.to_string()));
    Ok((
        tickwire_account_gateway::ConnectionConfig {
            address,
            tls,
            limits: Default::default(),
            connect_deadline: Instant::now() + Duration::from_secs(5),
        },
        job,
    ))
}
fn serve(
    venue: SpotVenue,
    server: rustls::ServerConfig,
    listener: TcpListener,
    count: u8,
) -> Result<()> {
    let (sock, _) = listener.accept()?;
    sock.set_read_timeout(Some(Duration::from_secs(5)))?;
    let mut wire = BufReader::new(rustls::StreamOwned::new(
        rustls::ServerConnection::new(Arc::new(server))?,
        sock,
    ));
    let bybit = venue == SpotVenue::Bybit;
    let path = if bybit {
        "/v5/order/create"
    } else {
        "/api/v5/trade/order"
    };
    let key = hmac::Key::new(hmac::HMAC_SHA256, SECRET);
    let mut seen = std::collections::BTreeSet::new();
    for native in 1..=count {
        let mut line = String::new();
        wire.read_line(&mut line)?;
        if line != format!("POST {path} HTTP/1.1\r\n") {
            return Err("unexpected paper path".into());
        }
        let mut head = BTreeMap::new();
        let mut size = 0;
        loop {
            line.clear();
            let n = wire.read_line(&mut line)?;
            size += n;
            if n == 0 || size > 8192 {
                return Err("invalid native request headers".into());
            }
            if line == "\r\n" {
                break;
            }
            let (k, v) = line
                .trim_end()
                .split_once(": ")
                .ok_or("invalid native header")?;
            if head.insert(k.to_ascii_lowercase(), v.to_string()).is_some() {
                return Err("duplicate native header".into());
            }
        }
        let header = |name: &str| {
            head.get(name)
                .map(String::as_str)
                .ok_or("missing native header")
        };
        let n: usize = header("content-length")?.parse()?;
        if !(1..=8192).contains(&n) {
            return Err("invalid paper body".into());
        }
        let mut b = vec![0; n];
        wire.read_exact(&mut b)?;
        let body: Value = serde_json::from_slice(&b)?;
        let mut mac = hmac::Context::with_key(&key);
        let valid = if bybit {
            if header("x-bapi-api-key")? != "paper" {
                return Err("unknown paper key".into());
            }
            mac.update(header("x-bapi-timestamp")?.as_bytes());
            mac.update(b"paper");
            mac.update(header("x-bapi-recv-window")?.as_bytes());
            mac.update(&b);
            let expected: String = mac
                .sign()
                .as_ref()
                .iter()
                .map(|v| format!("{v:02x}"))
                .collect();
            header("x-bapi-sign")? == expected
                && body["category"] == "spot"
                && body["orderType"] == "Limit"
                && body["price"] == FIXED_PRICE
        } else {
            if header("ok-access-key")? != "paper"
                || header("ok-access-passphrase")? != "paper-pass"
                || header("x-simulated-trading")? != "1"
            {
                return Err("unknown paper key or environment".into());
            }
            mac.update(header("ok-access-timestamp")?.as_bytes());
            mac.update(b"POST");
            mac.update(path.as_bytes());
            mac.update(&b);
            header("ok-access-sign")? == STANDARD.encode(mac.sign().as_ref())
                && body["tdMode"] == "cash"
                && body["ordType"] == "limit"
                && body["px"] == FIXED_PRICE
        };
        if !valid {
            return Err("invalid paper signature or order terms".into());
        }
        let client = body[if bybit { "orderLinkId" } else { "clOrdId" }]
            .as_str()
            .ok_or("missing client ID")?;
        if !matches!(client, "paper8" | "paper9") || !seen.insert(client.to_owned()) {
            return Err("unexpected paper order identity".into());
        }
        let result = if bybit {
            json!({"retCode":0,"result":{"orderLinkId":client,"orderId":native.to_string()}})
        } else {
            json!({"code":"0","data":[{"sCode":"0","clOrdId":client,"ordId":native.to_string()}]})
        };
        let b = serde_json::to_vec(&result)?;
        write!(
            wire.get_mut(),
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n",
            b.len()
        )?;
        wire.get_mut().write_all(&b)?;
        wire.get_mut().flush()?;
    }
    // A persistent HTTP connection lets the owner finish the next strategy's admission.
    // Any additional application bytes after the expected requests are a test failure.
    let mut extra = [0; 1];
    match wire.read(&mut extra) {
        Ok(0) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => Ok(()),
        Ok(_) => Err("local peer received an extra native request".into()),
        Err(error) => Err(error.into()),
    }
}
