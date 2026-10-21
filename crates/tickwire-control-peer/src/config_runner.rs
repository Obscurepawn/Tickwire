//! Executable configuration service assembly; source trust comes only from startup.
use crate::{api_runner, runner};
use serde::Deserialize;
use std::{
    collections::BTreeSet,
    net::SocketAddr,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};
use tickwire_config_service::evidence::{Kind, Trust};
pub type Result<T> = core::result::Result<T, Error>;
#[derive(Debug)]
pub enum Error {
    Configuration,
    Input(runner::Error),
    Service(tickwire_config_service::Error),
    Owner(api_runner::Error),
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Configuration {
    pub schema: u16,
    pub peer_config_file: PathBuf,
    pub http: Http,
    pub deployment: Option<Deployment>,
    pub stop_file: Option<PathBuf>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Http {
    pub listen: SocketAddr,
    pub identity: runner::IdentityFiles,
    pub clients: Vec<Client>,
    pub max_connections: usize,
    pub max_body_bytes: usize,
    pub timeout_ms: u64,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Client {
    pub subject: String,
    pub leaf_sha256: [u8; 32],
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Deployment {
    pub bundle_file: PathBuf,
    pub evidence_trust_file: PathBuf,
    pub max_bundle_bytes: usize,
    pub max_jobs: usize,
    pub account_max_age_ms: u64,
    pub credential_max_age_ms: u64,
    pub resource_max_age_ms: u64,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvidenceTrust {
    pub schema: u16,
    pub cluster_id: String,
    pub namespace_id: String,
    pub revision: u64,
    pub issuers: Vec<Issuer>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Issuer {
    pub id: String,
    pub public_key: [u8; 32],
    pub kinds: Vec<Kind>,
}
impl Configuration {
    pub fn parse(bytes: &[u8]) -> Result<Self> {
        let value: Self = parse(bytes)?;
        value.validate()?;
        Ok(value)
    }
    pub fn validate(&self) -> Result<()> {
        if self.schema != 1
            || !self.peer_config_file.is_absolute()
            || self.stop_file.as_ref().is_some_and(|p| !p.is_absolute())
            || !(1..=64).contains(&self.http.max_connections)
            || !(1..=tickwire_config_service::api::MAX_BODY_BYTES)
                .contains(&self.http.max_body_bytes)
            || !(1..=60_000).contains(&self.http.timeout_ms)
            || self.http.clients.is_empty()
            || self.http.clients.len() > 4096
        {
            return Err(Error::Configuration);
        }
        let mut pins = BTreeSet::new();
        for client in &self.http.clients {
            if client.subject.is_empty()
                || client.subject.len() > 256
                || client.leaf_sha256 == [0; 32]
                || !pins.insert(client.leaf_sha256)
            {
                return Err(Error::Configuration);
            }
        }
        let i = &self.http.identity;
        for (paths, max) in [(&i.chain_der, 8), (&i.roots_der, 32), (&i.crls_der, 32)] {
            if paths.is_empty() || paths.len() > max || paths.iter().any(|p| !p.is_absolute()) {
                return Err(Error::Configuration);
            }
        }
        if !i.private_key_der.is_absolute() {
            return Err(Error::Configuration);
        }
        if let Some(d) = &self.deployment
            && (!d.bundle_file.is_absolute()
                || !d.evidence_trust_file.is_absolute()
                || !(12..=528 * 1024 * 1024 + 12).contains(&d.max_bundle_bytes)
                || !(1..=8).contains(&d.max_jobs)
                || [
                    d.account_max_age_ms,
                    d.credential_max_age_ms,
                    d.resource_max_age_ms,
                ]
                .iter()
                .any(|n| !(1..=60_000).contains(n)))
        {
            return Err(Error::Configuration);
        }
        Ok(())
    }
}
fn parse<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    if bytes.is_empty() || bytes.len() > 65_536 {
        return Err(Error::Configuration);
    }
    serde_json::from_slice(bytes).map_err(|_| Error::Configuration)
}
impl EvidenceTrust {
    pub fn parse(bytes: &[u8]) -> Result<Trust> {
        let value: Self = parse(bytes)?;
        if value.schema != 1 {
            return Err(Error::Configuration);
        }
        let issuers = value
            .issuers
            .into_iter()
            .map(|issuer| {
                let n = issuer.kinds.len();
                let kinds: BTreeSet<_> = issuer.kinds.into_iter().collect();
                if kinds.len() != n {
                    return Err(Error::Configuration);
                }
                Ok(tickwire_config_service::evidence::Issuer {
                    id: issuer.id,
                    public_key: issuer.public_key,
                    kinds,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Trust::new(
            value.cluster_id,
            value.namespace_id,
            value.revision,
            issuers,
        )
        .map_err(Error::Service)
    }
}
/// The owned startup material is fully decoded before any listener is bound.
/// It contains independent HTTPS client trust and evidence issuer trust.
pub struct Prepared {
    peer: runner::Prepared,
    address: SocketAddr,
    tls: tickwire_config_service::auth::ServerTls,
    limits: tickwire_config_service::https::Limits,
    deployment: Option<tickwire_config_service::deployment_owner::Owner>,
}
pub fn load(path: &Path) -> Result<Configuration> {
    if !path.is_absolute() {
        return Err(Error::Configuration);
    }
    Configuration::parse(&runner::read_bounded(path, 65_536, false).map_err(Error::Input)?)
}
pub fn prepare(config: &Configuration) -> Result<Prepared> {
    use tickwire_config_service::{
        auth::{IdentityMaterial, Peer, ServerTls},
        deployment,
        deployment_owner::Owner,
        source_bundle::{Bundle, Limits},
    };
    config.validate()?;
    let peer_config = runner::load(&config.peer_config_file).map_err(Error::Input)?;
    let peer = runner::prepare(&peer_config).map_err(Error::Input)?;
    let read_many = |paths: &[PathBuf], maximum| {
        paths
            .iter()
            .map(|path| runner::read_bounded(path, maximum, false).map_err(Error::Input))
            .collect::<Result<Vec<_>>>()
    };
    let i = &config.http.identity;
    let tls = ServerTls::new(
        IdentityMaterial {
            chain_der: read_many(&i.chain_der, 65_536)?,
            private_key_der: runner::read_bounded(&i.private_key_der, 65_536, true)
                .map_err(Error::Input)?,
            roots_der: read_many(&i.roots_der, 65_536)?,
            crls_der: read_many(&i.crls_der, 4 * 1024 * 1024)?,
        },
        config
            .http
            .clients
            .iter()
            .map(|c| Peer {
                subject: c.subject.clone(),
                leaf_sha256: c.leaf_sha256,
            })
            .collect(),
    )
    .map_err(Error::Service)?;
    let deployment = config
        .deployment
        .as_ref()
        .map(|d| {
            let trust = EvidenceTrust::parse(
                &runner::read_bounded(&d.evidence_trust_file, 65_536, false)
                    .map_err(Error::Input)?,
            )?;
            let bytes = runner::read_bounded(&d.bundle_file, d.max_bundle_bytes, false)
                .map_err(Error::Input)?;
            let bundle = Bundle::decode(
                &bytes,
                Limits {
                    max_bundle_bytes: d.max_bundle_bytes,
                    ..Default::default()
                },
            )
            .map_err(Error::Service)?;
            let mut limits = deployment::Limits::default();
            limits.accounts.max_age_ms = d.account_max_age_ms;
            limits.credentials.max_age_ms = d.credential_max_age_ms;
            limits.resources.max_age_ms = d.resource_max_age_ms;
            Owner::new(bundle, trust, limits, d.max_jobs).map_err(Error::Service)
        })
        .transpose()?;
    Ok(Prepared {
        peer,
        address: config.http.listen,
        tls,
        limits: tickwire_config_service::https::Limits {
            max_connections: config.http.max_connections,
            max_body_bytes: config.http.max_body_bytes,
            timeout: Duration::from_millis(config.http.timeout_ms),
        },
        deployment,
    })
}
impl Prepared {
    pub fn assemble(self) -> Result<api_runner::ApiOwner> {
        let mut owner =
            api_runner::ApiOwner::assemble_prepared(self.peer, self.address, self.tls, self.limits)
                .map_err(Error::Owner)?;
        if let Some(deployment) = self.deployment {
            let result = owner.with_control(|mut c| c.install_deployment_owner(deployment));
            match result {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    let _ = owner.shutdown();
                    return Err(Error::Service(e));
                }
                Err(e) => {
                    let _ = owner.shutdown();
                    return Err(Error::Owner(e));
                }
            }
        }
        Ok(owner)
    }
}
pub fn run(config: &Configuration, turns: Option<u64>) -> Result<()> {
    if turns == Some(0) {
        return Err(Error::Configuration);
    }
    let prepared = prepare(config)?;
    let node_id = prepared.peer.node_id();
    let mut owner = prepared.assemble()?;
    let result = run_owner(&mut owner, config, node_id, turns);
    let shutdown = owner.shutdown().map_err(Error::Owner);
    result.and(shutdown)
}
fn run_owner(
    owner: &mut api_runner::ApiOwner,
    config: &Configuration,
    node_id: u64,
    turns: Option<u64>,
) -> Result<()> {
    let mut round = 0u64;
    let mut last_snapshot = owner.status().raft_applied;
    println!(
        "{}",
        serde_json::json!({"event":"config_server_started","node_id":node_id,"http":owner.http_addr().map_err(Error::Owner)?.to_string(),"applied":last_snapshot,"deployment_sources":config.deployment.is_some()})
    );
    loop {
        if turns.is_some_and(|n| round >= n)
            || match &config.stop_file {
                Some(path) => path.try_exists().map_err(|_| Error::Configuration)?,
                None => false,
            }
        {
            return Ok(());
        }
        round = round.saturating_add(1);
        let status = owner
            .poll(Instant::now(), crate::service::PeerBudget::default(), 8)
            .map_err(Error::Owner)?;
        for applied in owner.take_applied(8192, 128 * 1024 * 1024) {
            println!(
                "{}",
                serde_json::json!({"event":"config_applied","node_id":node_id,"index":applied.index(),"result":format!("{:?}",applied.result())})
            );
        }
        for (peer, error) in status.peer.faults {
            eprintln!(
                "{}",
                serde_json::json!({"event":"config_peer_fault","peer":peer,"error":format!("{error:?}")})
            );
        }
        let applied = owner.status().raft_applied;
        if applied.saturating_sub(last_snapshot) >= 256 {
            owner
                .with_control(|mut c| c.compact_snapshot())
                .map_err(Error::Owner)?
                .map_err(Error::Service)?;
            last_snapshot = applied;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
}
