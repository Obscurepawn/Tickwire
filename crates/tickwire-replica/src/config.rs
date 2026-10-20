//! Explicit deployment files. Keys and peer pins have no permissive defaults.
use crate::*;
use ring::rand::{SecureRandom, SystemRandom};
use serde::Deserialize;
use std::{
    io::Read,
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NetworkConfig {
    pub timeout_ms: u64,
    pub requests_per_poll: usize,
    pub bytes_per_poll: usize,
}
impl NetworkConfig {
    pub fn build(&self) -> Result<NetworkLimits> {
        NetworkLimits {
            timeout: Duration::from_millis(self.timeout_ms),
            max_requests_per_poll: self.requests_per_poll,
            max_bytes_per_poll: self.bytes_per_poll,
        }
        .validate()
    }
}
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsFiles {
    pub certificate_chain_der: Vec<PathBuf>,
    pub private_key_der: PathBuf,
    pub trust_anchors_der: Vec<PathBuf>,
    pub peer_certificate_sha256: Hash,
}
impl TlsFiles {
    fn identity(&self) -> Result<TlsIdentity> {
        if self.certificate_chain_der.len() > 8 || self.trust_anchors_der.len() > 8 {
            return Err(Error::Bounds);
        }
        Ok(TlsIdentity {
            certificate_chain_der: self
                .certificate_chain_der
                .iter()
                .map(|p| read_bounded(p, 65536))
                .collect::<Result<_>>()?,
            private_key_der: read_bounded(&self.private_key_der, 65536)?,
            trust_anchors_der: self
                .trust_anchors_der
                .iter()
                .map(|p| read_bounded(p, 65536))
                .collect::<Result<_>>()?,
            peer_certificate_sha256: self.peer_certificate_sha256,
        })
    }
    pub fn client(&self, server_name: String) -> Result<ClientTls> {
        ClientTls::new(self.identity()?, server_name)
    }
    pub fn server(&self) -> Result<ServerTls> {
        ServerTls::new(self.identity()?)
    }
}
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReceiverConfig {
    pub listen: SocketAddr,
    pub stream_file: PathBuf,
    pub journal_dir: PathBuf,
    pub object_dir: PathBuf,
    pub receiver_id: [u8; 16],
    pub mirror_assignment_epoch: u64,
    pub profile_version: u64,
    pub max_records: usize,
    pub physical: tickwire_journal::Config,
    pub tls: TlsFiles,
    pub signing_key_pkcs8: PathBuf,
    pub source_signing_public_key: Hash,
    pub network: NetworkConfig,
    #[serde(default)]
    pub create: bool,
    pub required_prefix: Option<Prefix>,
}
impl ReceiverConfig {
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self> {
        serde_json::from_slice(&read_bounded(path, 65536)?)
            .map_err(|_| Error::Invalid("receiver configuration"))
    }
    pub fn open(
        &self,
    ) -> Result<MirrorListener<tickwire_journal::store::FileStore, FileBlobStore>> {
        let mirror = self.open_store()?;
        MirrorListener::bind(
            self.listen,
            mirror,
            Signer::from_pkcs8(&read_bounded(&self.signing_key_pkcs8, 65536)?)?,
            self.source_signing_public_key,
            self.tls.server()?,
            self.network.build()?,
        )
    }
    /// Exclusive local access for a stopped receiver's recovery operation.
    pub fn open_store(&self) -> Result<FileMirror> {
        let stream = Stream::decode(&read_bounded(&self.stream_file, 65536)?)?;
        let mut boot = [0; 16];
        SystemRandom::new()
            .fill(&mut boot)
            .map_err(|_| Error::Signature)?;
        let config = MirrorConfig {
            stream,
            receiver_id: self.receiver_id,
            receiver_boot_id: boot,
            mirror_assignment_epoch: self.mirror_assignment_epoch,
            profile_version: self.profile_version,
            max_records: self.max_records,
            physical: self.physical.clone(),
        };
        let mirror = if self.create {
            if self.required_prefix.is_some() {
                return Err(Error::Invalid("create with required prefix"));
            }
            FileMirror::create_files(config, &self.journal_dir, &self.object_dir)?
        } else {
            FileMirror::recover_files(
                config,
                &self.journal_dir,
                &self.object_dir,
                self.required_prefix,
            )?
        };
        Ok(mirror)
    }
}
pub fn read_bounded(path: impl AsRef<Path>, max: usize) -> Result<Vec<u8>> {
    let file = std::fs::File::open(path)?;
    if file.metadata()?.len() > max as u64 {
        return Err(Error::Bounds);
    }
    let mut bytes = Vec::new();
    file.take(max as u64 + 1).read_to_end(&mut bytes)?;
    if bytes.len() > max {
        return Err(Error::Bounds);
    }
    Ok(bytes)
}
