//! Certificate-backed identities for the control HTTPS endpoint.
use crate::{Error, Hash, Result};
use rustls::{
    pki_types::{CertificateDer, CertificateRevocationListDer, PrivateKeyDer, UnixTime},
    server::danger::ClientCertVerifier,
};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    io::{self, Read, Write},
    net::TcpStream,
    sync::Arc,
    time::{Duration, Instant},
};

const ALPN: &[u8] = b"http/1.1";
const MAX_CERT_BYTES: usize = 65536;
const MAX_CONNECTION_BYTES: usize = 32 * 1024 * 1024;

#[derive(Clone)]
pub struct IdentityMaterial {
    pub chain_der: Vec<Vec<u8>>,
    pub private_key_der: Vec<u8>,
    pub roots_der: Vec<Vec<u8>>,
    pub crls_der: Vec<Vec<u8>>,
}
#[derive(Debug, Clone)]
pub struct Peer {
    pub subject: String,
    pub leaf_sha256: Hash,
}
#[derive(Debug, Clone)]
pub struct AuthenticatedPrincipal {
    subject: String,
    leaf_sha256: Hash,
    chain: Vec<CertificateDer<'static>>,
}
impl AuthenticatedPrincipal {
    pub fn subject(&self) -> &str {
        &self.subject
    }
    pub fn leaf_sha256(&self) -> Hash {
        self.leaf_sha256
    }
}
#[derive(Clone)]
pub struct ServerTls {
    config: Arc<rustls::ServerConfig>,
    verifier: Arc<dyn ClientCertVerifier>,
    peers: BTreeMap<Hash, String>,
    crl_windows: Vec<(u64, u64)>,
}
impl ServerTls {
    pub fn new(material: IdentityMaterial, peers: Vec<Peer>) -> Result<Self> {
        bounded_der(&material.chain_der, 8, MAX_CERT_BYTES)?;
        bounded_der(&material.roots_der, 32, MAX_CERT_BYTES)?;
        bounded_der(&material.crls_der, 32, 4 * 1024 * 1024)?;
        if material.private_key_der.is_empty()
            || material.private_key_der.len() > MAX_CERT_BYTES
            || peers.is_empty()
            || peers.len() > 4096
        {
            return Err(Error::Bounds);
        }
        let mut allowed = BTreeMap::new();
        for peer in peers {
            if !valid_subject(&peer.subject)
                || peer.leaf_sha256 == [0; 32]
                || allowed.insert(peer.leaf_sha256, peer.subject).is_some()
            {
                return Err(Error::Identity);
            }
        }
        let mut roots = rustls::RootCertStore::empty();
        for root in material.roots_der {
            roots
                .add(CertificateDer::from(root))
                .map_err(|_| Error::Authentication)?;
        }
        let mut crl_windows = Vec::with_capacity(material.crls_der.len());
        for bytes in &material.crls_der {
            let (rest, crl) =
                x509_parser::parse_x509_crl(bytes).map_err(|_| Error::Authentication)?;
            let start =
                u64::try_from(crl.last_update().timestamp()).map_err(|_| Error::Authentication)?;
            let end = u64::try_from(crl.next_update().ok_or(Error::Authentication)?.timestamp())
                .map_err(|_| Error::Authentication)?;
            if !rest.is_empty() || start >= end {
                return Err(Error::Authentication);
            }
            crl_windows.push((start, end));
        }
        let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
            .with_crls(
                material
                    .crls_der
                    .into_iter()
                    .map(CertificateRevocationListDer::from),
            )
            .enforce_revocation_expiration()
            .build()
            .map_err(|_| Error::Authentication)?;
        let chain = material
            .chain_der
            .into_iter()
            .map(CertificateDer::from)
            .collect();
        let private_key =
            PrivateKeyDer::try_from(material.private_key_der).map_err(|_| Error::Authentication)?;
        let mut config =
            rustls::ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
                .with_client_cert_verifier(verifier.clone())
                .with_single_cert(chain, private_key)
                .map_err(|_| Error::Authentication)?;
        config.alpn_protocols = vec![ALPN.to_vec()];
        config.send_tls13_tickets = 0;
        config.max_early_data_size = 0;
        Ok(Self {
            config: Arc::new(config),
            verifier,
            peers: allowed,
            crl_windows,
        })
    }
    pub fn accept(
        &self,
        socket: TcpStream,
        timeout: Duration,
        max_bytes: usize,
    ) -> Result<AuthenticatedStream> {
        if timeout.is_zero() || timeout > Duration::from_secs(60) {
            return Err(Error::Bounds);
        }
        self.accept_until(
            socket,
            Instant::now().checked_add(timeout).ok_or(Error::Bounds)?,
            max_bytes,
        )
    }
    /// Use the accept-time deadline without extending it after scheduling.
    pub fn accept_until(
        &self,
        socket: TcpStream,
        deadline: Instant,
        max_bytes: usize,
    ) -> Result<AuthenticatedStream> {
        let mut socket = DeadlineSocket::new(socket, deadline, max_bytes)?;
        let mut connection = rustls::ServerConnection::new(self.config.clone())
            .map_err(|_| Error::Authentication)?;
        while connection.is_handshaking() {
            connection.complete_io(&mut socket).map_err(|error| {
                if error.kind() == io::ErrorKind::InvalidData {
                    Error::Authentication
                } else {
                    Error::Transport
                }
            })?;
        }
        if connection.alpn_protocol() != Some(ALPN) {
            return Err(Error::Authentication);
        }
        let chain = connection
            .peer_certificates()
            .ok_or(Error::Authentication)?
            .to_vec();
        let (subject, leaf_sha256) = self.verify_chain(&chain)?;
        let principal = AuthenticatedPrincipal {
            subject,
            leaf_sha256,
            chain,
        };
        Ok(AuthenticatedStream {
            stream: rustls::StreamOwned::new(connection, socket),
            principal,
        })
    }
    /// Run before every request, using the current trust configuration. This
    /// checks certificate authorization, not the subject's current RBAC grants.
    pub fn revalidate(&self, principal: &AuthenticatedPrincipal) -> Result<()> {
        let (subject, leaf_sha256) = self.verify_chain(&principal.chain)?;
        if subject != principal.subject || leaf_sha256 != principal.leaf_sha256 {
            return Err(Error::Authentication);
        }
        Ok(())
    }
    fn verify_chain(&self, chain: &[CertificateDer<'static>]) -> Result<(String, Hash)> {
        if chain.is_empty()
            || chain.len() > 8
            || chain
                .iter()
                .any(|c| c.is_empty() || c.len() > MAX_CERT_BYTES)
        {
            return Err(Error::Authentication);
        }
        let now = UnixTime::now();
        if self
            .crl_windows
            .iter()
            .any(|&(start, end)| now.as_secs() < start || now.as_secs() >= end)
        {
            return Err(Error::Authentication);
        }
        self.verifier
            .verify_client_cert(&chain[0], &chain[1..], now)
            .map_err(|_| Error::Authentication)?;
        let hash: Hash = Sha256::digest(chain[0].as_ref()).into();
        let subject = self.peers.get(&hash).ok_or(Error::Authentication)?;
        let (rest, certificate) = x509_parser::parse_x509_certificate(chain[0].as_ref())
            .map_err(|_| Error::Authentication)?;
        if !rest.is_empty() {
            return Err(Error::Authentication);
        }
        let san = certificate
            .subject_alternative_name()
            .map_err(|_| Error::Authentication)?
            .ok_or(Error::Authentication)?;
        let mut uris = san
            .value
            .general_names
            .iter()
            .filter_map(|name| match name {
                x509_parser::extensions::GeneralName::URI(uri) => Some(*uri),
                _ => None,
            });
        if uris.next() != Some(subject.as_str()) || uris.next().is_some() {
            return Err(Error::Authentication);
        }
        Ok((subject.clone(), hash))
    }
}
pub struct AuthenticatedStream {
    stream: rustls::StreamOwned<rustls::ServerConnection, DeadlineSocket>,
    principal: AuthenticatedPrincipal,
}
impl AuthenticatedStream {
    pub fn principal(&self) -> &AuthenticatedPrincipal {
        &self.principal
    }
}
impl Read for AuthenticatedStream {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        self.stream.sock.remaining()?;
        self.stream.read(bytes)
    }
}
impl Write for AuthenticatedStream {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.stream.sock.remaining()?;
        self.stream.write(bytes)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.stream.sock.remaining()?;
        self.stream.flush()
    }
}
fn bounded_der(values: &[Vec<u8>], count: usize, size: usize) -> Result<()> {
    if values.is_empty()
        || values.len() > count
        || values.iter().any(|v| v.is_empty() || v.len() > size)
    {
        return Err(Error::Bounds);
    }
    Ok(())
}
pub(crate) fn valid_subject(subject: &str) -> bool {
    if subject.len() > 256 {
        return false;
    }
    let Some((domain, path)) = subject
        .strip_prefix("spiffe://")
        .and_then(|s| s.split_once('/'))
    else {
        return false;
    };
    !domain.is_empty()
        && !path.is_empty()
        && domain
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b".-".contains(&b))
        && path
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"/_-".contains(&b))
}
struct DeadlineSocket {
    socket: TcpStream,
    deadline: Instant,
    max_bytes: usize,
    read: usize,
    written: usize,
}
impl DeadlineSocket {
    fn new(socket: TcpStream, deadline: Instant, max_bytes: usize) -> Result<Self> {
        let remaining = deadline.checked_duration_since(Instant::now());
        if remaining.is_none_or(|time| time.is_zero() || time > Duration::from_secs(60))
            || !(1..=MAX_CONNECTION_BYTES).contains(&max_bytes)
        {
            return Err(Error::Bounds);
        }
        // Accepted sockets may inherit the listener's nonblocking flag. This
        // socket belongs to a connection worker whose I/O is deadline bounded.
        socket
            .set_nonblocking(false)
            .map_err(|_| Error::Transport)?;
        socket.set_nodelay(true).map_err(|_| Error::Transport)?;
        Ok(Self {
            socket,
            deadline,
            max_bytes,
            read: 0,
            written: 0,
        })
    }
    fn remaining(&self) -> io::Result<Duration> {
        self.deadline
            .checked_duration_since(Instant::now())
            .filter(|d| !d.is_zero())
            .ok_or_else(|| io::Error::new(io::ErrorKind::TimedOut, "control request deadline"))
    }
}
impl Read for DeadlineSocket {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        if bytes.is_empty() {
            return Ok(0);
        }
        let capacity = self.max_bytes.saturating_sub(self.read);
        if capacity == 0 {
            return Err(io::Error::other("control encrypted input bound"));
        }
        self.socket.set_read_timeout(Some(self.remaining()?))?;
        let limit = bytes.len().min(capacity);
        let count = self.socket.read(&mut bytes[..limit])?;
        self.read += count;
        Ok(count)
    }
}
impl Write for DeadlineSocket {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.is_empty() {
            return Ok(0);
        }
        let capacity = self.max_bytes.saturating_sub(self.written);
        if capacity == 0 {
            return Err(io::Error::other("control encrypted output bound"));
        }
        self.socket.set_write_timeout(Some(self.remaining()?))?;
        let count = self.socket.write(&bytes[..bytes.len().min(capacity)])?;
        self.written += count;
        Ok(count)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.socket.set_write_timeout(Some(self.remaining()?))?;
        self.socket.flush()
    }
}
