//! A single application/consensus owner suitable for the CLI or authenticated API assembly.
use crate::service::PeerService;
use serde::Deserialize;
use std::{
    collections::BTreeSet,
    net::SocketAddr,
    path::{Path, PathBuf},
};
use tickwire_config_service::Replica;
use tickwire_control_raft::generation::GenerationStore;
pub type Server = PeerService<Replica<GenerationStore>>;
#[derive(Debug)]
pub enum Error {
    Configuration,
    Io,
    Transport(crate::Error),
    Consensus(tickwire_control_raft::Error),
    Application(tickwire_config_service::Error),
    Owner,
}
pub type Result<T> = core::result::Result<T, Error>;
#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    Create,
    Join,
    Recover,
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Configuration {
    pub cluster: [u8; 16],
    pub group: [u8; 16],
    pub node_id: u64,
    pub initial_voters: [u64; 3],
    pub writer_epoch: u64,
    pub stream_epoch: [u8; 16],
    pub mode: Mode,
    pub directory: PathBuf,
    pub listen: SocketAddr,
    pub identity: IdentityFiles,
    pub peers: Vec<Peer>,
    pub application: Application,
    pub max_command_bytes: usize,
    pub max_retained_bytes: usize,
    pub bootstrap: Option<Bootstrap>,
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IdentityFiles {
    pub chain_der: Vec<PathBuf>,
    pub private_key_der: PathBuf,
    pub roots_der: Vec<PathBuf>,
    pub crls_der: Vec<PathBuf>,
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Peer {
    pub node_id: u64,
    pub address: SocketAddr,
    pub server_name: String,
    pub leaf_sha256: [u8; 32],
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Application {
    pub max_objects: usize,
    pub max_inflight: usize,
    pub max_payload_bytes: usize,
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Bootstrap {
    pub namespace: [u8; 16],
    pub cluster_name: String,
    pub public_key: [u8; 32],
}
impl Configuration {
    pub fn parse(bytes: &[u8]) -> Result<Self> {
        if bytes.is_empty() || bytes.len() > 65536 {
            return Err(Error::Configuration);
        }
        let c: Self = serde_json::from_slice(bytes).map_err(|_| Error::Configuration)?;
        c.validate()?;
        Ok(c)
    }
    pub fn validate(&self) -> Result<()> {
        let mut ids = BTreeSet::from([self.node_id]);
        let mut pins = BTreeSet::new();
        let mut addresses = BTreeSet::from([self.listen]);
        if self.cluster == [0; 16]
            || self.group == [0; 16]
            || self.stream_epoch == [0; 16]
            || self.node_id == 0
            || self.writer_epoch == 0
            || !self.directory.is_absolute()
            || self.initial_voters[0] == 0
            || self.initial_voters.windows(2).any(|w| w[0] >= w[1])
            || self.peers.is_empty()
            || self.peers.len() > 15
        {
            return Err(Error::Configuration);
        }
        if (self.mode == Mode::Create && !self.initial_voters.contains(&self.node_id))
            || (self.mode == Mode::Join && self.initial_voters.contains(&self.node_id))
        {
            return Err(Error::Configuration);
        }
        for p in &self.peers {
            if p.node_id == 0
                || !ids.insert(p.node_id)
                || !addresses.insert(p.address)
                || p.leaf_sha256 == [0; 32]
                || !pins.insert(p.leaf_sha256)
                || p.server_name.is_empty()
                || p.server_name.len() > 253
            {
                return Err(Error::Configuration);
            }
        }
        if self.mode == Mode::Create && self.initial_voters.iter().any(|n| !ids.contains(n)) {
            return Err(Error::Configuration);
        }
        if !(1..=16 * 1024 * 1024).contains(&self.max_command_bytes)
            || !(1024 * 1024..=1024 * 1024 * 1024).contains(&self.max_retained_bytes)
            || self.max_command_bytes > self.max_retained_bytes / 4
        {
            return Err(Error::Configuration);
        }
        let a = &self.application;
        if !(1..=4096).contains(&a.max_objects)
            || a.max_inflight == 0
            || a.max_inflight > a.max_objects.min(64)
            || !(1..=512 * 1024 * 1024).contains(&a.max_payload_bytes)
        {
            return Err(Error::Configuration);
        }
        for (files, max) in [
            (&self.identity.chain_der, 8),
            (&self.identity.roots_der, 32),
            (&self.identity.crls_der, 32),
        ] {
            if files.is_empty() || files.len() > max || files.iter().any(|p| !p.is_absolute()) {
                return Err(Error::Configuration);
            }
        }
        if !self.identity.private_key_der.is_absolute() {
            return Err(Error::Configuration);
        }
        self.bootstrap_trust()?;
        Ok(())
    }
    fn bootstrap_trust(
        &self,
    ) -> Result<Option<tickwire_config_service::bootstrap::BootstrapTrust>> {
        self.bootstrap
            .as_ref()
            .map(|b| {
                tickwire_config_service::bootstrap::BootstrapTrust::new(
                    self.cluster,
                    b.namespace,
                    b.cluster_name.clone(),
                    b.public_key,
                )
                .map_err(Error::Application)
            })
            .transpose()
    }
}
/// Owned peer trust and fixed startup configuration. No credential paths are
/// reopened by assembly, and no listener/WAL is created by preparation.
pub struct Prepared {
    config: Configuration,
    tls: crate::tls::PeerTls,
    bootstrap: Option<tickwire_config_service::bootstrap::BootstrapTrust>,
}
impl Prepared {
    pub fn node_id(&self) -> u64 {
        self.config.node_id
    }
    pub fn assemble(self) -> Result<Server> {
        assemble_prepared(self)
    }
}
pub fn prepare(config: &Configuration) -> Result<Prepared> {
    use crate::tls::{IdentityMaterial, PeerCertificate, PeerTls, Scope};
    config.validate()?;
    let read_many = |paths: &[PathBuf], max| {
        paths
            .iter()
            .map(|p| read_bounded(p, max, false))
            .collect::<Result<Vec<_>>>()
    };
    let tls = PeerTls::new(
        Scope {
            cluster: config.cluster,
            group: config.group,
        },
        config.node_id,
        IdentityMaterial {
            chain_der: read_many(&config.identity.chain_der, 65536)?,
            private_key_der: read_bounded(&config.identity.private_key_der, 65536, true)?,
            roots_der: read_many(&config.identity.roots_der, 65536)?,
            crls_der: read_many(&config.identity.crls_der, 4 * 1024 * 1024)?,
        },
        config
            .peers
            .iter()
            .map(|p| PeerCertificate {
                node_id: p.node_id,
                server_name: p.server_name.clone(),
                leaf_sha256: p.leaf_sha256,
            })
            .collect(),
    )
    .map_err(Error::Transport)?;
    Ok(Prepared {
        config: config.clone(),
        tls,
        bootstrap: config.bootstrap_trust()?,
    })
}
/// Returns the actual single owner. Authenticated APIs use `with_control`
/// between polls and consume every application result; no second Node is made.
pub fn assemble(config: &Configuration) -> Result<Server> {
    prepare(config)?.assemble()
}
fn assemble_prepared(prepared: Prepared) -> Result<Server> {
    use crate::service::Limits;
    use tickwire_control_raft::{
        Node,
        generation::SnapshotDirectory,
        snapshot::{MAX_CLOSURE_BYTES, SnapshotLimits},
    };
    let Prepared {
        config,
        tls,
        bootstrap,
    } = prepared;
    let listener = std::net::TcpListener::bind(config.listen).map_err(|_| Error::Io)?;
    let directory = SnapshotDirectory::open(
        &config.directory,
        config.group,
        config.node_id,
        SnapshotLimits {
            max_closure_bytes: MAX_CLOSURE_BYTES,
            ..Default::default()
        },
    )
    .map_err(Error::Consensus)?;
    let node_config = tickwire_control_raft::Config {
        physical: tickwire_journal::Config::new(config.stream_epoch, config.group),
        node_id: config.node_id,
        voters: config.initial_voters,
        writer_epoch: config.writer_epoch,
        max_command_bytes: config.max_command_bytes,
        max_retained_bytes: config.max_retained_bytes,
    };
    let node = match config.mode {
        Mode::Create => Node::create_in(directory, node_config),
        Mode::Join => Node::create_joining_in(directory, node_config),
        Mode::Recover => Node::recover_in(directory, node_config),
    }
    .map_err(Error::Consensus)?;
    let app = tickwire_config_service::Config {
        cluster: config.cluster,
        objects: tickwire_config_service::ObjectLimits {
            max_objects: config.application.max_objects as u32,
            max_inflight: config.application.max_inflight as u32,
            max_payload_bytes: config.application.max_payload_bytes as u64,
        },
    };
    let replica = Replica::recover_generation(node, app, bootstrap).map_err(Error::Application)?;
    let mut incarnation = [0; 16];
    getrandom::getrandom(&mut incarnation).map_err(|_| Error::Io)?;
    PeerService::new(
        replica,
        listener,
        incarnation,
        config
            .peers
            .iter()
            .map(|p| (p.node_id, p.address))
            .collect(),
        tls,
        Limits::default(),
    )
    .map_err(|_| Error::Owner)
}
pub(crate) fn read_bounded(path: &Path, max: usize, secret: bool) -> Result<Vec<u8>> {
    read_bounded_inner(
        path,
        max,
        secret,
        #[cfg(test)]
        || {},
    )
}
fn read_bounded_inner(
    path: &Path,
    max: usize,
    secret: bool,
    #[cfg(test)] before_read: impl FnOnce(),
) -> Result<Vec<u8>> {
    use std::io::Read;
    let file = open_input(path)?;
    let metadata = file.metadata().map_err(|_| Error::Io)?;
    if !metadata.file_type().is_file() || metadata.len() > max as u64 {
        return Err(Error::Configuration);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        if secret && (metadata.permissions().mode() & 0o077 != 0 || metadata.nlink() != 1) {
            return Err(Error::Configuration);
        }
    }
    #[cfg(not(unix))]
    if secret {
        return Err(Error::Configuration);
    }
    #[cfg(test)]
    before_read();
    let mut bytes = Vec::new();
    file.take(max as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| Error::Io)?;
    if bytes.is_empty() || bytes.len() > max {
        return Err(Error::Configuration);
    }
    Ok(bytes)
}
#[cfg(unix)]
fn open_input(path: &Path) -> Result<std::fs::File> {
    use std::os::unix::fs::OpenOptionsExt;
    std::fs::OpenOptions::new()
        .read(true)
        // Never follow a replacement leaf symlink or wait for a FIFO writer.
        // All type, length and secret checks refer to this opened descriptor.
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC)
        .open(path)
        .map_err(|_| Error::Io)
}
#[cfg(not(unix))]
fn open_input(_path: &Path) -> Result<std::fs::File> {
    Err(Error::Configuration)
}
pub fn load(path: &Path) -> Result<Configuration> {
    Configuration::parse(&read_bounded(path, 65536, false)?)
}
pub fn run(config: &Configuration, turns: Option<u64>) -> Result<()> {
    use std::time::{Duration, Instant};
    let mut server = assemble(config)?;
    let mut round = 0u64;
    let mut last_snapshot = server.status().raft_applied;
    println!(
        "{}",
        serde_json::json!({"event":"raft_peer_started","node_id":config.node_id,"group":config.group,"applied":last_snapshot,"voters":server.current_membership().voters()})
    );
    loop {
        if turns.is_some_and(|n| round >= n) {
            return Ok(());
        }
        round = round.saturating_add(1);
        let status = server
            .poll(Instant::now(), crate::service::PeerBudget::default())
            .map_err(|_| Error::Owner)?;
        for result in server.take_applied(8192, 128 * 1024 * 1024) {
            println!(
                "{}",
                serde_json::json!({"event":"raft_applied","node_id":config.node_id,"index":result.index(),"result":format!("{:?}",result.result())})
            );
        }
        for (peer, error) in status.faults {
            eprintln!(
                "{}",
                serde_json::json!({"event":"raft_peer_fault","peer":peer,"error":format!("{error:?}")})
            );
        }
        let applied = server.status().raft_applied;
        if applied.saturating_sub(last_snapshot) >= 256 {
            server
                .with_control(|mut r| r.compact_snapshot())
                .map_err(|_| Error::Owner)?
                .map_err(Error::Application)?;
            last_snapshot = applied;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn value() -> serde_json::Value {
        serde_json::json!({"cluster":vec![1;16],"group":vec![2;16],"node_id":1,"initial_voters":[1,2,3],"writer_epoch":1,"stream_epoch":vec![3;16],"mode":"create","directory":"/tmp/trusted-raft","listen":"127.0.0.1:7001","identity":{"chain_der":["/cert/node.der"],"private_key_der":"/cert/key.der","roots_der":["/cert/root.der"],"crls_der":["/cert/revocation.der"]},"peers":[{"node_id":2,"address":"127.0.0.1:7002","server_name":"localhost","leaf_sha256":vec![2;32]},{"node_id":3,"address":"127.0.0.1:7003","server_name":"localhost","leaf_sha256":vec![3;32]}],"application":{"max_objects":8,"max_inflight":4,"max_payload_bytes":4194304},"max_command_bytes":1048576,"max_retained_bytes":33554432,"bootstrap":null})
    }
    #[test]
    fn explicit_create_and_join_configuration_are_distinct() {
        let v = value();
        assert!(Configuration::parse(&serde_json::to_vec(&v).unwrap()).is_ok());
        let mut join = v;
        join["node_id"] = 4.into();
        join["mode"] = "join".into();
        join["listen"] = "127.0.0.1:7004".into();
        join["peers"].as_array_mut().unwrap().push(serde_json::json!({"node_id":1,"address":"127.0.0.1:7001","server_name":"localhost","leaf_sha256":vec![1;32]}));
        assert!(Configuration::parse(&serde_json::to_vec(&join).unwrap()).is_ok());
        join["mode"] = "create".into();
        assert!(Configuration::parse(&serde_json::to_vec(&join).unwrap()).is_err());
        join["mode"] = "join".into();
        join["node_id"] = 5.into();
        join["listen"] = "127.0.0.1:7005".into();
        join["peers"] = serde_json::json!([{"node_id":4,"address":"127.0.0.1:7004","server_name":"localhost","leaf_sha256":vec![4;32]}]);
        assert!(Configuration::parse(&serde_json::to_vec(&join).unwrap()).is_ok());
    }
    #[test]
    fn malformed_bounds_duplicates_and_relative_paths_fail_before_io() {
        let mut v = value();
        v["identity"]["private_key_der"] = "relative.key".into();
        assert!(Configuration::parse(&serde_json::to_vec(&v).unwrap()).is_err());
        let mut v = value();
        v["peers"][1]["node_id"] = 2.into();
        assert!(Configuration::parse(&serde_json::to_vec(&v).unwrap()).is_err());
        let mut v = value();
        v["extra"] = true.into();
        assert!(Configuration::parse(&serde_json::to_vec(&v).unwrap()).is_err());
        let mut v = value();
        v["max_command_bytes"] = 0.into();
        assert!(Configuration::parse(&serde_json::to_vec(&v).unwrap()).is_err());
    }
    #[cfg(unix)]
    struct Files(PathBuf);
    #[cfg(unix)]
    impl Files {
        fn new() -> Self {
            use std::sync::atomic::{AtomicU64, Ordering};
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let p = std::env::temp_dir().join(format!(
                "tickwire-runner-files-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir(&p).unwrap();
            Self(p)
        }
        fn file(&self, name: &str, bytes: &[u8], mode: u32) -> PathBuf {
            use std::os::unix::fs::PermissionsExt;
            let p = self.0.join(name);
            std::fs::write(&p, bytes).unwrap();
            std::fs::set_permissions(&p, std::fs::Permissions::from_mode(mode)).unwrap();
            p
        }
    }
    #[cfg(unix)]
    impl Drop for Files {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    #[cfg(unix)]
    #[test]
    fn opened_file_is_not_replaced_by_path_swap_before_read() {
        let f = Files::new();
        let path = f.file("key", b"original", 0o600);
        let replacement = f.file("replacement", b"substitute", 0o644);
        let bytes = read_bounded_inner(&path, 32, true, || {
            std::fs::rename(&replacement, &path).unwrap();
        })
        .unwrap();
        assert_eq!(bytes, b"original");
    }
    #[cfg(unix)]
    #[test]
    fn bounded_files_reject_fifo_symlink_and_nonprivate_keys() {
        use std::os::unix::fs::{PermissionsExt, symlink};
        let f = Files::new();
        let key = f.file("key", b"secret", 0o600);
        assert_eq!(read_bounded(&key, 32, true).unwrap(), b"secret");
        let link = f.0.join("link");
        symlink(&key, &link).unwrap();
        assert!(read_bounded(&link, 32, true).is_err());
        let fifo = f.0.join("fifo");
        assert!(
            std::process::Command::new("mkfifo")
                .arg(&fifo)
                .status()
                .unwrap()
                .success()
        );
        assert!(read_bounded(&fifo, 32, false).is_err());
        std::fs::set_permissions(&key, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(read_bounded(&key, 32, true).is_err());
        assert_eq!(read_bounded(&key, 32, false).unwrap(), b"secret");
        assert!(read_bounded(&key, 5, false).is_err());
        assert!(read_bounded(&f.file("empty", b"", 0o600), 32, false).is_err());
        assert!(read_bounded(&f.0, 32, false).is_err());
    }
    #[cfg(unix)]
    #[test]
    fn secret_file_rejects_multiple_hardlinks() {
        let f = Files::new();
        let key = f.file("key", b"secret", 0o600);
        std::fs::hard_link(&key, f.0.join("alias")).unwrap();
        assert!(read_bounded(&key, 32, true).is_err());
    }
}
