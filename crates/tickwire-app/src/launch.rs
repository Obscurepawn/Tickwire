//! Qualified strategy construction and durable dynamic Shadow registration.
use crate::{Result, config::read_file, invalid, strategy::AppStrategy};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
};
use tickwire_runtime::files;
use tickwire_runtime_policy::ControlPlane;

pub fn build(
    config: &crate::strategy::Config,
    settings: &files::Configuration,
    plane: tickwire_runtime_policy::ControlPlane,
) -> Result<Box<dyn crate::strategy::AppStrategy>> {
    use crate::strategy::{Config, MultiStrategy};
    use tickwire_ipc::Wire;
    match config {
        Config::Hosted { config: host } => crate::hosted::build(host, settings, plane),
        Config::Multi { members } => {
            if members.is_empty() || members.len() > 32 {
                return Err("multi strategy requires 1 through 32 members".into());
            }
            let mut seen = std::collections::BTreeSet::new();
            let mut topics = std::collections::BTreeSet::new();
            let mut children = Vec::with_capacity(members.len());
            for member in members {
                if member.logical_strategy_id == 0
                    || !seen.insert((member.logical_strategy_id, member.shadow))
                    || matches!(member.strategy.as_ref(), Config::Multi { .. })
                {
                    return Err(
                        "multi strategy requires unique active/shadow slots and cannot nest".into(),
                    );
                }
                if let Config::Hosted { config: host } = member.strategy.as_ref() {
                    if host.shadow != member.shadow {
                        return Err("hosted shadow mode must match its multi strategy slot".into());
                    }
                    let bootstrap: tickwire_strategy_sdk::bootstrap::Bootstrap =
                        serde_json::from_slice(&crate::config::read_file(
                            &host.bootstrap,
                            1024 * 1024,
                        )?)?;
                    let logical = if let Some(external) = &bootstrap.external_market {
                        tickwire_ipc::ResourceHeader::decode(&external.bank_header)
                            .map_err(|e| format!("external market resource: {e:?}"))?;
                        external.endpoint.local.logical_strategy
                    } else {
                        let header = tickwire_ipc::ResourceHeader::decode(
                            &crate::config::read_file(&host.market_resource, 4096)?,
                        )
                        .map_err(|e| format!("market resource: {e:?}"))?;
                        if !topics.insert(header.topic_id) {
                            return Err(
                                "independent local publishers require distinct topic IDs".into()
                            );
                        }
                        bootstrap.market.local.logical_strategy
                    };
                    if logical != member.logical_strategy_id {
                        return Err("strategy member does not match its process identity".into());
                    }
                }
                let strategy = build(&member.strategy, settings, plane.clone())?;
                children.push((member.logical_strategy_id, member.shadow, strategy));
            }
            Ok(Box::new(MultiStrategy::new_modes(children, plane)?))
        }
        other => Ok(Box::new(crate::strategy::FileStrategy::new(other)?)),
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub inbox_directory: PathBuf,
    pub journal_directory: PathBuf,
    pub stream_epoch: [u8; 16],
    pub recover: bool,
    pub segment_max_bytes: u64,
    pub maximum_requests: usize,
    pub maximum_per_poll: usize,
}
/// Publish as `<sequence:020>.json` by atomic rename in the private inbox.
/// These hashes bind the exact HostConfig and Bootstrap read for this attempt.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub version: u16,
    pub sequence: u64,
    pub request_id: [u8; 16],
    pub exec_instance: [u8; 16],
    pub writer_epoch: u64,
    pub logical_strategy_id: u64,
    pub instance_id: [u8; 16],
    pub strategy_generation: u64,
    pub host_config: PathBuf,
    pub host_config_digest: [u8; 32],
    pub bootstrap_digest: [u8; 32],
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Event {
    Bootstrap {
        binding: [u8; 32],
    },
    Decision {
        request: Request,
        digest: [u8; 32],
        eligible: bool,
    },
    Outcome {
        sequence: u64,
        digest: [u8; 32],
        registered: bool,
        error: Option<String>,
    },
}
#[derive(Default)]
struct History {
    next: u64,
    requests: BTreeSet<[u8; 16]>,
    instances: BTreeSet<[u8; 16]>,
    highest: BTreeMap<u64, u64>,
    decisions: BTreeMap<u64, ([u8; 32], bool)>,
    outcomes: BTreeSet<u64>,
}
impl History {
    fn new() -> Self {
        Self {
            next: 1,
            ..Self::default()
        }
    }
    fn eligible(&self, r: &Request) -> Result<bool> {
        if r.version != 1
            || r.sequence != self.next
            || r.request_id == [0; 16]
            || self.requests.contains(&r.request_id)
        {
            return Err(invalid("shadow request sequence or identity"));
        }
        Ok(r.logical_strategy_id != 0
            && r.instance_id != [0; 16]
            && r.strategy_generation != 0
            && self.instances.len() < 4096
            && !self.instances.contains(&r.instance_id)
            && self
                .highest
                .get(&r.logical_strategy_id)
                .is_none_or(|g| r.strategy_generation > *g))
    }
    fn decision(&mut self, r: &Request, hash: [u8; 32], eligible: bool) -> Result<()> {
        if self.eligible(r)? != eligible
            || hash != tickwire_recovery::digest(&serde_json::to_vec(r)?)
        {
            return Err(invalid("shadow decision history"));
        }
        self.requests.insert(r.request_id);
        self.decisions.insert(r.sequence, (hash, eligible));
        if eligible {
            self.instances.insert(r.instance_id);
            self.highest
                .insert(r.logical_strategy_id, r.strategy_generation);
        }
        self.next = self
            .next
            .checked_add(1)
            .ok_or_else(|| invalid("shadow sequence exhausted"))?;
        Ok(())
    }
    fn outcome(
        &mut self,
        sequence: u64,
        hash: [u8; 32],
        registered: bool,
        error: &Option<String>,
    ) -> Result<()> {
        let expected = self
            .decisions
            .get(&sequence)
            .ok_or_else(|| invalid("shadow outcome without decision"))?;
        if expected.0 != hash
            || registered && !expected.1
            || registered == error.is_some()
            || !self.outcomes.insert(sequence)
        {
            return Err(invalid("shadow outcome history"));
        }
        Ok(())
    }
}
pub struct ShadowLoader {
    config: Config,
    wal: tickwire_lifecycle::EventJournal<tickwire_journal::store::FileStore>,
    history: History,
    binding: [u8; 32],
    poisoned: bool,
}
fn binding(settings: &files::Configuration) -> Result<[u8; 32]> {
    let s = &settings.account.journal.scope;
    use tickwire_venue::{Environment, Product};
    let environment = match s.environment {
        Environment::Production => 1u8,
        Environment::Test => 2,
    };
    let product = match settings.account.route.product {
        Product::Spot => 1u8,
        Product::LinearPerpetual => 2,
        Product::InversePerpetual => 3,
        Product::Option => 4,
        Product::LinearFuture => 5,
        Product::InverseFuture => 6,
    };
    Ok(tickwire_recovery::digest(&serde_json::to_vec(&(
        1u16,
        s.risk_domain_id,
        s.venue,
        s.account,
        environment,
        settings.account.journal.physical.stream_epoch,
        settings.account.journal.baseline_digest,
        product,
    ))?))
}
fn private_directory(path: &Path) -> Result<()> {
    use std::os::unix::fs::{DirBuilderExt, MetadataExt};
    if !path.is_absolute() {
        return Err(invalid("shadow directory must be absolute"));
    }
    match fs::DirBuilder::new().mode(0o700).create(path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(e.into()),
    }
    let m = fs::symlink_metadata(path)?;
    if !m.is_dir() || m.mode() & 0o077 != 0 {
        return Err(invalid("shadow directory must be private"));
    }
    fs::File::open(
        path.parent()
            .ok_or_else(|| invalid("shadow directory parent"))?,
    )?
    .sync_all()?;
    Ok(())
}
impl ShadowLoader {
    /// Recovered decisions retain their consumed identities. Historical registered
    /// outcomes never construct a process or restore trading permission.
    pub fn open(config: Config, settings: &files::Configuration) -> Result<Self> {
        if config.maximum_requests == 0
            || config.maximum_requests > 1000
            || config.maximum_per_poll == 0
            || config.maximum_per_poll > 32
            || config.stream_epoch == [0; 16]
            || config.inbox_directory == config.journal_directory
        {
            return Err(invalid("shadow loader bounds"));
        }
        private_directory(&config.inbox_directory)?;
        private_directory(&config.journal_directory)?;
        let binding = binding(settings)?;
        let physical = tickwire_journal::Config {
            domain: settings.account.journal.scope.risk_domain_id,
            stream_epoch: config.stream_epoch,
            segment_max_bytes: config.segment_max_bytes,
        };
        let store = tickwire_journal::store::FileStore::open(&config.journal_directory)?;
        let (mut wal, events) = if config.recover {
            tickwire_lifecycle::EventJournal::recover::<Event>(
                store,
                &physical,
                settings.account.writer_epoch,
                None,
            )?
        } else {
            (
                tickwire_lifecycle::EventJournal::create(
                    store,
                    physical,
                    settings.account.writer_epoch,
                )?,
                vec![],
            )
        };
        let mut history = History::new();
        if config.recover {
            if !matches!(events.first(),Some(Event::Bootstrap{binding:b})if *b==binding) {
                return Err(invalid("shadow loader binding"));
            }
            for event in events.into_iter().skip(1) {
                match event {
                    Event::Bootstrap { .. } => return Err(invalid("shadow bootstrap repeated")),
                    Event::Decision {
                        request,
                        digest,
                        eligible,
                    } => history.decision(&request, digest, eligible)?,
                    Event::Outcome {
                        sequence,
                        digest,
                        registered,
                        error,
                    } => history.outcome(sequence, digest, registered, &error)?,
                }
            }
            if history.requests.len() > config.maximum_requests {
                return Err(invalid("shadow request history capacity"));
            }
            let interrupted = history
                .decisions
                .iter()
                .filter(|(seq, _)| !history.outcomes.contains(seq))
                .map(|(seq, (hash, _))| (*seq, *hash))
                .collect::<Vec<_>>();
            for (sequence, digest) in interrupted {
                let error =
                    Some("previous process stopped before registration completed".to_owned());
                wal.append(&Event::Outcome {
                    sequence,
                    digest,
                    registered: false,
                    error: error.clone(),
                })?;
                history.outcome(sequence, digest, false, &error)?;
            }
        } else {
            wal.append(&Event::Bootstrap { binding })?;
        }
        Ok(Self {
            config,
            wal,
            history,
            binding,
            poisoned: false,
        })
    }
    pub fn next_sequence(&self) -> u64 {
        self.history.next
    }
    /// Returns processed requests, including explicit rejections. One failed child
    /// registration does not change another instance's grant or risk state.
    pub fn poll(
        &mut self,
        target: &mut dyn AppStrategy,
        settings: &files::Configuration,
        plane: ControlPlane,
    ) -> Result<usize> {
        if self.poisoned || binding(settings)? != self.binding {
            return Err(invalid("shadow loader unavailable or binding changed"));
        }
        let mut count = 0;
        while count < self.config.maximum_per_poll {
            let path = self
                .config
                .inbox_directory
                .join(format!("{:020}.json", self.history.next));
            let bytes = match read_file(&path, 128 * 1024) {
                Ok(b) => b,
                Err(e)
                    if e.downcast_ref::<std::io::Error>()
                        .is_some_and(|e| e.kind() == std::io::ErrorKind::NotFound) =>
                {
                    break;
                }
                Err(e) => return Err(e),
            };
            if self.history.requests.len() >= self.config.maximum_requests {
                return Err(invalid("shadow request history exhausted"));
            }
            let request: Request = serde_json::from_slice(&bytes)?;
            let hash = tickwire_recovery::digest(&serde_json::to_vec(&request)?);
            let eligible = self.history.eligible(&request)?;
            self.append(&Event::Decision {
                request: request.clone(),
                digest: hash,
                eligible,
            })?;
            self.history.decision(&request, hash, eligible)?;
            let outcome = if eligible {
                register(&request, target, settings, plane.clone())
            } else {
                Err(invalid("strategy identity or generation already consumed"))
            };
            let registered = outcome.is_ok();
            let error = outcome
                .err()
                .map(|e| e.to_string().chars().take(1024).collect::<String>());
            if let Err(e) = self.append(&Event::Outcome {
                sequence: request.sequence,
                digest: hash,
                registered,
                error: error.clone(),
            }) {
                if registered {
                    target.discard_shadow(request.instance_id, request.strategy_generation);
                }
                return Err(e);
            }
            self.history
                .outcome(request.sequence, hash, registered, &error)?;
            eprintln!(
                "{}",
                serde_json::json!({"event":"shadow_registration","sequence":request.sequence,"request_id":request.request_id,
                "logical_strategy_id":request.logical_strategy_id,"instance":request.instance_id,"generation":request.strategy_generation,
                "registered":registered,"error":error})
            );
            count += 1;
        }
        Ok(count)
    }
    fn append(&mut self, event: &Event) -> Result<()> {
        if let Err(e) = self.wal.append(event) {
            self.poisoned = true;
            return Err(e.into());
        }
        Ok(())
    }
}
fn register(
    r: &Request,
    target: &mut dyn AppStrategy,
    settings: &files::Configuration,
    plane: ControlPlane,
) -> Result<()> {
    if r.exec_instance != settings.command_binding.owner_id
        || r.writer_epoch != settings.account.writer_epoch
        || !r.host_config.is_absolute()
        || r.host_config_digest == [0; 32]
        || r.bootstrap_digest == [0; 32]
    {
        return Err(invalid("shadow request executor or configuration identity"));
    }
    let bytes = read_file(&r.host_config, 128 * 1024)?;
    if tickwire_recovery::digest(&bytes) != r.host_config_digest {
        return Err(invalid("shadow HostConfig digest"));
    }
    let host: crate::hosted::HostConfig = serde_json::from_slice(&bytes)?;
    if !host.shadow {
        return Err(invalid("dynamic registration requires Shadow mode"));
    }
    let bytes = read_file(&host.bootstrap, 2 * 1024 * 1024)?;
    if tickwire_recovery::digest(&bytes) != r.bootstrap_digest {
        return Err(invalid("shadow Bootstrap digest"));
    }
    let bootstrap: tickwire_strategy_sdk::bootstrap::Bootstrap = serde_json::from_slice(&bytes)?;
    let identity = &bootstrap.market.local;
    if identity.logical_strategy != r.logical_strategy_id
        || identity.instance != r.instance_id
        || identity.generation != r.strategy_generation
    {
        return Err(invalid("shadow request process identity"));
    }
    if let Some(job_path) = &bootstrap.preflight_job {
        let job: tickwire_strategy_sdk::preflight::Job =
            serde_json::from_slice(&read_file(job_path, 2 * 1024 * 1024)?)?;
        let bundle = job.bundle.load(&job.operation)?;
        let release = bundle.verify_operation(&job.operation, &job.member)?;
        let loaded = tickwire_release::files::load(&release.target.files, &job.target_files)?;
        if loaded
            .object(&tickwire_release::Role::HostConfig)
            .map(|o| o.digest)
            != Some(r.host_config_digest)
            || loaded
                .object(&tickwire_release::Role::Bootstrap)
                .map(|o| o.digest)
                != Some(r.bootstrap_digest)
            || job.member.new_instance != r.instance_id
            || job.member.new_generation != r.strategy_generation
            || job.operation.logical_strategy_id != r.logical_strategy_id
        {
            return Err(invalid("release target differs from actual loader files"));
        }
        let source = job.source.objects()?;
        let seal = tickwire_recovery::HandoverSeal::decode(&source[0])?;
        let want = tickwire_runtime::node::HandoverReadiness {
            instance_id: seal.instance,
            strategy_generation: seal.generation,
            checkpoint_digest: seal.checkpoint_digest,
            exec_report_sequence: seal.report_position,
            orders_digest: seal.orders_digest,
            ledger_sequence: seal.ledger_sequence,
            input_seal_digest: job.source.proof.object.digest,
        };
        let actual = target.handover_objects(&want).ok_or_else(|| {
            invalid("fixed migration source was not confirmed by the owned old process")
        })?;
        if source.iter().any(|b| !actual.contains(b))
            || seal.instance != job.member.old_instance
            || seal.generation != job.member.old_generation
        {
            return Err(invalid(
                "fixed migration files differ from actual old process input",
            ));
        }
    }
    let strategy = crate::hosted::build(&host, settings, plane)?;
    if strategy.instance() != Some((r.instance_id, r.strategy_generation)) {
        return Err(invalid("shadow process changed during construction"));
    }
    target.add_shadow(r.logical_strategy_id, strategy)
}
#[cfg(test)]
mod tests {
    use super::*;
    fn request(sequence: u64, id: u8, generation: u64) -> Request {
        Request {
            version: 1,
            sequence,
            request_id: [sequence as u8; 16],
            exec_instance: [1; 16],
            writer_epoch: 1,
            logical_strategy_id: 8,
            instance_id: [id; 16],
            strategy_generation: generation,
            host_config: PathBuf::from("/trusted/host.json"),
            host_config_digest: [2; 32],
            bootstrap_digest: [3; 32],
        }
    }
    fn decide(h: &mut History, r: &Request) -> bool {
        let e = h.eligible(r).unwrap();
        h.decision(
            r,
            tickwire_recovery::digest(&serde_json::to_vec(r).unwrap()),
            e,
        )
        .unwrap();
        e
    }
    #[test]
    fn interrupted_and_rejected_attempts_never_reuse_consumed_instances() {
        let mut h = History::new();
        assert!(decide(&mut h, &request(1, 3, 2)));
        assert!(!decide(&mut h, &request(2, 3, 3)));
        assert!(!decide(&mut h, &request(3, 4, 2)));
        assert!(decide(&mut h, &request(4, 4, 3)));
        assert_eq!(h.next, 5);
        assert_eq!(h.highest.get(&8), Some(&3));
        assert_eq!(h.instances.len(), 2);
    }
    #[test]
    fn replay_checks_decision_hash_and_outcome_identity() {
        let mut h = History::new();
        let r = request(1, 3, 2);
        assert!(h.decision(&r, [9; 32], true).is_err());
        let hash = tickwire_recovery::digest(&serde_json::to_vec(&r).unwrap());
        h.decision(&r, hash, true).unwrap();
        assert!(h.outcome(1, [9; 32], true, &None).is_err());
        h.outcome(1, hash, true, &None).unwrap();
        assert!(h.outcome(1, hash, true, &None).is_err());
        assert!(h.eligible(&r).is_err());
    }
}
