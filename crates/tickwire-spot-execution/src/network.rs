//! Blocking, deadline-bounded TLS and credentials for cold spot account reads.
use crate::{Credentials, SpotVenue};
use serde_json::Value;
use std::{
    env,
    error::Error,
    fs::File,
    io::{BufReader, Read, Write},
    net::{SocketAddr, TcpStream, ToSocketAddrs},
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};
use tickwire_transport::{
    http::{self, Response, ResponseDecoder},
    rustls::{
        self,
        pki_types::{CertificateDer, ServerName, pem::PemObject},
    },
};

pub type Result<T> = std::result::Result<T, Box<dyn Error>>;
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
