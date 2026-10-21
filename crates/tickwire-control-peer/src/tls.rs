use crate::{Error, Result};
use rustls::{
    client::danger::ServerCertVerifier,
    pki_types::{
        CertificateDer, CertificateRevocationListDer, PrivateKeyDer, ServerName, UnixTime,
    },
    server::danger::ClientCertVerifier,
};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, sync::Arc};
pub const ALPN: &[u8] = b"tickwire-control-raft/1";
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Scope {
    pub cluster: [u8; 16],
    pub group: [u8; 16],
}
#[derive(Clone)]
pub struct IdentityMaterial {
    pub chain_der: Vec<Vec<u8>>,
    pub private_key_der: Vec<u8>,
    pub roots_der: Vec<Vec<u8>>,
    pub crls_der: Vec<Vec<u8>>,
}
#[derive(Debug, Clone)]
pub struct PeerCertificate {
    pub node_id: u64,
    pub server_name: String,
    pub leaf_sha256: [u8; 32],
}
#[derive(Debug)]
pub struct AuthenticatedPeer {
    node_id: u64,
    leaf_sha256: [u8; 32],
    chain: Vec<CertificateDer<'static>>,
    server_name: Option<ServerName<'static>>,
}
impl AuthenticatedPeer {
    pub fn node_id(&self) -> u64 {
        self.node_id
    }
    pub fn leaf_sha256(&self) -> [u8; 32] {
        self.leaf_sha256
    }
}
#[derive(Clone)]
pub struct PeerTls {
    scope: Scope,
    node_id: u64,
    client: Arc<rustls::ClientConfig>,
    server: Arc<rustls::ServerConfig>,
    server_verifier: Arc<rustls::client::WebPkiServerVerifier>,
    client_verifier: Arc<dyn ClientCertVerifier>,
    peers: BTreeMap<u64, PeerCertificate>,
    crl_windows: Vec<(u64, u64)>,
}
pub fn peer_uri(scope: Scope, node_id: u64) -> String {
    let hex = |id: [u8; 16]| id.iter().map(|b| format!("{b:02x}")).collect::<String>();
    format!(
        "spiffe://tickwire/raft/{}/{}/{node_id}",
        hex(scope.cluster),
        hex(scope.group)
    )
}
impl PeerTls {
    pub fn new(
        scope: Scope,
        node_id: u64,
        material: IdentityMaterial,
        peers: Vec<PeerCertificate>,
    ) -> Result<Self> {
        if scope.cluster == [0; 16] || scope.group == [0; 16] || node_id == 0 {
            return Err(Error::Identity);
        }
        bounded(&material.chain_der, 8, 65536)?;
        bounded(&material.roots_der, 32, 65536)?;
        bounded(&material.crls_der, 32, 4 * 1024 * 1024)?;
        if material.private_key_der.is_empty()
            || material.private_key_der.len() > 65536
            || peers.is_empty()
            || peers.len() > 16
        {
            return Err(Error::Bounds);
        }
        check_uri(&material.chain_der[0], &peer_uri(scope, node_id))?;
        let mut allowed = BTreeMap::new();
        let mut pins = std::collections::BTreeSet::new();
        for peer in peers {
            if peer.node_id == 0
                || peer.node_id == node_id
                || peer.leaf_sha256 == [0; 32]
                || peer.server_name.len() > 253
                || ServerName::try_from(peer.server_name.clone()).is_err()
                || !pins.insert(peer.leaf_sha256)
                || allowed.insert(peer.node_id, peer).is_some()
            {
                return Err(Error::Identity);
            }
        }
        let mut roots = rustls::RootCertStore::empty();
        for bytes in material.roots_der {
            roots
                .add(CertificateDer::from(bytes))
                .map_err(|_| Error::Authentication)?;
        }
        let roots = Arc::new(roots);
        let mut windows = Vec::new();
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
            windows.push((start, end));
        }
        let crls: Vec<_> = material
            .crls_der
            .into_iter()
            .map(CertificateRevocationListDer::from)
            .collect();
        let server_verifier = rustls::client::WebPkiServerVerifier::builder(roots.clone())
            .with_crls(crls.clone())
            .enforce_revocation_expiration()
            .build()
            .map_err(|_| Error::Authentication)?;
        let client_verifier = rustls::server::WebPkiClientVerifier::builder(roots)
            .with_crls(crls)
            .enforce_revocation_expiration()
            .build()
            .map_err(|_| Error::Authentication)?;
        let certs: Vec<_> = material
            .chain_der
            .into_iter()
            .map(CertificateDer::from)
            .collect();
        let key =
            PrivateKeyDer::try_from(material.private_key_der).map_err(|_| Error::Authentication)?;
        let mut client =
            rustls::ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
                .with_webpki_verifier(server_verifier.clone())
                .with_client_auth_cert(certs.clone(), key.clone_key())
                .map_err(|_| Error::Authentication)?;
        client.alpn_protocols = vec![ALPN.to_vec()];
        client.enable_early_data = false;
        client.resumption = rustls::client::Resumption::disabled();
        let mut server =
            rustls::ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
                .with_client_cert_verifier(client_verifier.clone())
                .with_single_cert(certs, key)
                .map_err(|_| Error::Authentication)?;
        server.alpn_protocols = vec![ALPN.to_vec()];
        server.send_tls13_tickets = 0;
        server.max_early_data_size = 0;
        Ok(Self {
            scope,
            node_id,
            client: Arc::new(client),
            server: Arc::new(server),
            server_verifier,
            client_verifier,
            peers: allowed,
            crl_windows: windows,
        })
    }
    pub fn scope(&self) -> Scope {
        self.scope
    }
    pub fn node_id(&self) -> u64 {
        self.node_id
    }
    pub fn peer_ids(&self) -> impl Iterator<Item = u64> + '_ {
        self.peers.keys().copied()
    }
    pub fn client(&self, peer: u64) -> Result<rustls::Connection> {
        let name = self
            .peers
            .get(&peer)
            .ok_or(Error::Identity)?
            .server_name
            .clone();
        let c = rustls::ClientConnection::new(
            self.client.clone(),
            ServerName::try_from(name).map_err(|_| Error::Identity)?,
        )
        .map_err(|_| Error::Authentication)?;
        let mut c = rustls::Connection::Client(c);
        c.set_buffer_limit(Some(128 * 1024));
        Ok(c)
    }
    pub fn server(&self) -> Result<rustls::Connection> {
        let mut c = rustls::Connection::Server(
            rustls::ServerConnection::new(self.server.clone())
                .map_err(|_| Error::Authentication)?,
        );
        c.set_buffer_limit(Some(128 * 1024));
        Ok(c)
    }
    pub fn authenticate(
        &self,
        connection: &rustls::Connection,
        expected: Option<u64>,
    ) -> Result<AuthenticatedPeer> {
        if connection.is_handshaking() || connection.alpn_protocol() != Some(ALPN) {
            return Err(Error::Authentication);
        }
        let chain = connection
            .peer_certificates()
            .ok_or(Error::Authentication)?
            .to_vec();
        let leaf = chain.first().ok_or(Error::Authentication)?;
        let hash: [u8; 32] = Sha256::digest(leaf.as_ref()).into();
        let id = self
            .peers
            .values()
            .find(|peer| peer.leaf_sha256 == hash)
            .ok_or(Error::Authentication)?
            .node_id;
        if expected.is_some_and(|e| e != id) {
            return Err(Error::Identity);
        }
        let name = if matches!(connection, rustls::Connection::Client(_)) {
            if expected.is_none() {
                return Err(Error::Identity);
            }
            Some(
                ServerName::try_from(
                    self.peers
                        .get(&id)
                        .ok_or(Error::Identity)?
                        .server_name
                        .clone(),
                )
                .map_err(|_| Error::Identity)?,
            )
        } else {
            None
        };
        let leaf_sha256 = self.verify(id, &chain, name.as_ref())?;
        Ok(AuthenticatedPeer {
            node_id: id,
            leaf_sha256,
            chain,
            server_name: name,
        })
    }
    pub fn revalidate(&self, peer: &AuthenticatedPeer) -> Result<()> {
        self.verify(peer.node_id, &peer.chain, peer.server_name.as_ref())
            .map(|_| ())
    }
    fn verify(
        &self,
        id: u64,
        chain: &[CertificateDer<'static>],
        name: Option<&ServerName<'static>>,
    ) -> Result<[u8; 32]> {
        if chain.is_empty()
            || chain.len() > 8
            || chain.iter().any(|c| c.is_empty() || c.len() > 65536)
        {
            return Err(Error::Authentication);
        }
        let now = UnixTime::now();
        if self
            .crl_windows
            .iter()
            .any(|&(s, e)| now.as_secs() < s || now.as_secs() >= e)
        {
            return Err(Error::Authentication);
        }
        let expected = self.peers.get(&id).ok_or(Error::Identity)?;
        if let Some(name) = name
            && &ServerName::try_from(expected.server_name.clone()).map_err(|_| Error::Identity)?
                != name
        {
            return Err(Error::Authentication);
        }
        if let Some(name) = name {
            self.server_verifier
                .verify_server_cert(&chain[0], &chain[1..], name, &[], now)
                .map_err(|_| Error::Authentication)?;
        } else {
            self.client_verifier
                .verify_client_cert(&chain[0], &chain[1..], now)
                .map_err(|_| Error::Authentication)?;
        }
        let pin: [u8; 32] = Sha256::digest(chain[0].as_ref()).into();
        let expected = self.peers.get(&id).ok_or(Error::Identity)?;
        if expected.leaf_sha256 != pin {
            return Err(Error::Authentication);
        }
        check_uri(chain[0].as_ref(), &peer_uri(self.scope, id))?;
        Ok(pin)
    }
}
fn bounded(values: &[Vec<u8>], count: usize, size: usize) -> Result<()> {
    if values.is_empty()
        || values.len() > count
        || values.iter().any(|v| v.is_empty() || v.len() > size)
    {
        Err(Error::Bounds)
    } else {
        Ok(())
    }
}
fn check_uri(bytes: &[u8], expected: &str) -> Result<()> {
    let (rest, cert) =
        x509_parser::parse_x509_certificate(bytes).map_err(|_| Error::Authentication)?;
    if !rest.is_empty() {
        return Err(Error::Authentication);
    }
    let san = cert
        .subject_alternative_name()
        .map_err(|_| Error::Authentication)?
        .ok_or(Error::Authentication)?;
    let mut uris = san.value.general_names.iter().filter_map(|n| match n {
        x509_parser::extensions::GeneralName::URI(uri) => Some(*uri),
        _ => None,
    });
    if uris.next() != Some(expected) || uris.next().is_some() {
        return Err(Error::Authentication);
    }
    Ok(())
}
