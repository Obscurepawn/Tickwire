//! Mutual certificate authentication, exact peer leaf pinning, and a whole-request deadline.
use crate::{Error, Hash, MAX_FRAME_BYTES, Result};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use std::{
    io::{self, Read, Write},
    net::TcpStream,
    sync::Arc,
    time::{Duration, Instant},
};

const ALPN: &[u8] = b"tickwire-replica/1";
pub struct TlsIdentity {
    pub certificate_chain_der: Vec<Vec<u8>>,
    pub private_key_der: Vec<u8>,
    pub trust_anchors_der: Vec<Vec<u8>>,
    pub peer_certificate_sha256: Hash,
}
fn material(
    i: TlsIdentity,
) -> Result<(
    Vec<CertificateDer<'static>>,
    PrivateKeyDer<'static>,
    rustls::RootCertStore,
    Hash,
)> {
    if i.certificate_chain_der.is_empty()
        || i.certificate_chain_der.len() > 8
        || i.trust_anchors_der.is_empty()
        || i.trust_anchors_der.len() > 8
        || i.private_key_der.is_empty()
        || i.private_key_der.len() > 65536
        || i.peer_certificate_sha256 == [0; 32]
        || i.certificate_chain_der
            .iter()
            .chain(&i.trust_anchors_der)
            .any(|v| v.is_empty() || v.len() > 65536)
    {
        return Err(Error::Invalid("TLS identity"));
    }
    let mut roots = rustls::RootCertStore::empty();
    for c in i.trust_anchors_der {
        roots
            .add(CertificateDer::from(c))
            .map_err(|_| Error::Invalid("TLS root"))?;
    }
    let key = PrivateKeyDer::try_from(i.private_key_der)
        .map_err(|_| Error::Invalid("TLS private key"))?;
    Ok((
        i.certificate_chain_der
            .into_iter()
            .map(CertificateDer::from)
            .collect(),
        key,
        roots,
        i.peer_certificate_sha256,
    ))
}
#[derive(Clone)]
pub struct ClientTls {
    config: Arc<rustls::ClientConfig>,
    name: ServerName<'static>,
    pin: Hash,
}
impl ClientTls {
    pub fn new(identity: TlsIdentity, server_name: String) -> Result<Self> {
        let (certs, key, roots, pin) = material(identity)?;
        let mut config =
            rustls::ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
                .with_root_certificates(roots)
                .with_client_auth_cert(certs, key)
                .map_err(|_| Error::Invalid("TLS client identity"))?;
        config.alpn_protocols = vec![ALPN.to_vec()];
        config.resumption = rustls::client::Resumption::disabled();
        config.enable_early_data = false;
        let name =
            ServerName::try_from(server_name).map_err(|_| Error::Invalid("TLS server name"))?;
        Ok(Self {
            config: Arc::new(config),
            name,
            pin,
        })
    }
    pub(crate) fn connect(
        &self,
        socket: TcpStream,
        timeout: Duration,
    ) -> Result<rustls::StreamOwned<rustls::ClientConnection, DeadlineSocket>> {
        let mut conn = rustls::ClientConnection::new(self.config.clone(), self.name.clone())
            .map_err(|_| Error::Invalid("TLS connection"))?;
        let mut socket = DeadlineSocket::new(socket, timeout)?;
        while conn.is_handshaking() {
            conn.complete_io(&mut socket)?;
        }
        check_peer(conn.peer_certificates(), conn.alpn_protocol(), self.pin)?;
        Ok(rustls::StreamOwned::new(conn, socket))
    }
}
#[derive(Clone)]
pub struct ServerTls {
    config: Arc<rustls::ServerConfig>,
    pin: Hash,
}
impl ServerTls {
    pub fn new(identity: TlsIdentity) -> Result<Self> {
        let (certs, key, roots, pin) = material(identity)?;
        let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
            .build()
            .map_err(|_| Error::Invalid("TLS client verifier"))?;
        let mut config =
            rustls::ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
                .with_client_cert_verifier(verifier)
                .with_single_cert(certs, key)
                .map_err(|_| Error::Invalid("TLS server identity"))?;
        config.alpn_protocols = vec![ALPN.to_vec()];
        config.send_tls13_tickets = 0;
        Ok(Self {
            config: Arc::new(config),
            pin,
        })
    }
    pub(crate) fn accept(
        &self,
        socket: TcpStream,
        timeout: Duration,
    ) -> Result<rustls::StreamOwned<rustls::ServerConnection, DeadlineSocket>> {
        let mut conn = rustls::ServerConnection::new(self.config.clone())
            .map_err(|_| Error::Invalid("TLS connection"))?;
        let mut socket = DeadlineSocket::new(socket, timeout)?;
        while conn.is_handshaking() {
            conn.complete_io(&mut socket)?;
        }
        check_peer(conn.peer_certificates(), conn.alpn_protocol(), self.pin)?;
        Ok(rustls::StreamOwned::new(conn, socket))
    }
}
fn check_peer(certs: Option<&[CertificateDer<'_>]>, alpn: Option<&[u8]>, pin: Hash) -> Result<()> {
    if alpn != Some(ALPN)
        || certs
            .and_then(|v| v.first())
            .map(|c| tickwire_recovery::digest(c.as_ref()))
            != Some(pin)
    {
        return Err(Error::Signature);
    }
    Ok(())
}
pub(crate) struct DeadlineSocket {
    socket: TcpStream,
    deadline: Instant,
    read: usize,
    written: usize,
}
impl DeadlineSocket {
    fn new(socket: TcpStream, timeout: Duration) -> Result<Self> {
        socket.set_nodelay(true)?;
        Ok(Self {
            socket,
            deadline: Instant::now().checked_add(timeout).ok_or(Error::Bounds)?,
            read: 0,
            written: 0,
        })
    }
    fn remaining(&self) -> io::Result<Duration> {
        self.deadline
            .checked_duration_since(Instant::now())
            .filter(|d| !d.is_zero())
            .ok_or_else(|| io::Error::new(io::ErrorKind::TimedOut, "replica request deadline"))
    }
}
impl Read for DeadlineSocket {
    fn read(&mut self, b: &mut [u8]) -> io::Result<usize> {
        let limit = (MAX_FRAME_BYTES + 262144).saturating_sub(self.read);
        if limit == 0 {
            return Err(io::Error::other("replica encrypted input bound"));
        }
        self.socket.set_read_timeout(Some(self.remaining()?))?;
        let len = b.len().min(limit);
        let n = self.socket.read(&mut b[..len])?;
        self.read += n;
        Ok(n)
    }
}
impl Write for DeadlineSocket {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        let limit = (MAX_FRAME_BYTES + 262144).saturating_sub(self.written);
        if limit == 0 {
            return Err(io::Error::other("replica encrypted output bound"));
        }
        self.socket.set_write_timeout(Some(self.remaining()?))?;
        let n = self.socket.write(&b[..b.len().min(limit)])?;
        self.written += n;
        Ok(n)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.socket.set_write_timeout(Some(self.remaining()?))?;
        self.socket.flush()
    }
}
