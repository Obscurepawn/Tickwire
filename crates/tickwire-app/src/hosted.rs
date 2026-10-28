//! File-configured qualified child process assembly. Runtime owns all real I/O.
use crate::{
    Result,
    config::read_file,
    invalid,
    strategy::{AppStrategy, RecoveryFiles},
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, path::PathBuf};
use tickwire_ipc::{ResourceHeader, Wire};
use tickwire_runtime::files;
use tickwire_runtime_policy::ControlPlane;
use tickwire_strategy_host::{
    HostedStrategy, Launch, RotationPlan, RotationSource, SharedPublisher,
};
use tickwire_strategy_sdk::{
    bootstrap::{Bootstrap, IdentityConfig},
    runtime::{RecoveryResolver, ResolvedRecovery},
};
use tickwire_venue::CanonicalCommand;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HostConfig {
    #[serde(default)]
    pub shadow: bool,
    pub bootstrap: PathBuf,
    #[serde(default)]
    pub market_resource: PathBuf,
    /// Private MD admission directory shared with the trusted launcher.
    #[serde(default)]
    pub md_admission_directory: Option<PathBuf>,
    pub executable: PathBuf,
    pub working_directory: PathBuf,
    pub uid: u32,
    pub gid: u32,
    pub supervisor: IdentityConfig,
    pub directory_parent: PathBuf,
    pub control_directory: PathBuf,
    pub control_stream: [u8; 16],
    pub control_domain: [u8; 16],
    pub control_writer_epoch: u64,
    pub snapshot_id: [u8; 16],
    pub market_cursor_id: [u8; 16],
    pub subscription_generation: u64,
    pub instrument_set_version: u64,
    pub maximum_levels_per_side: usize,
    pub maximum_cache_bytes: usize,
    pub maximum_snapshot_bytes: usize,
    pub maximum_delta_bytes: usize,
    /// Immutable `<lowercase command SHA256>.json` descriptors containing actual input and dependency files.
    pub recovery_directory: PathBuf,
    /// Optional control-provided numbered HostConfig files (`00000000000000000001.json`).
    #[serde(default)]
    pub rotation_directory: Option<PathBuf>,
    #[serde(default = "first_rotation")]
    pub first_rotation_sequence: u64,
}
fn first_rotation() -> u64 {
    1
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryDescriptor {
    pub command_digest: [u8; 32],
    pub order_ids: Vec<[u8; 16]>,
    pub recovery: RecoveryFiles,
}
struct Files {
    directory: PathBuf,
}
impl RecoveryResolver for Files {
    fn resolve(
        &mut self,
        command: &CanonicalCommand,
    ) -> tickwire_strategy_sdk::Result<Option<ResolvedRecovery>> {
        let name = command
            .digest()
            .iter()
            .map(|v| format!("{v:02x}"))
            .collect::<String>();
        let path = self.directory.join(format!("{name}.json"));
        match std::fs::symlink_metadata(&path) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
            Ok(_) => {}
        }
        let bytes = read_file(&path, 1024 * 1024)
            .map_err(|e| tickwire_strategy_sdk::Error::Io(std::io::Error::other(e.to_string())))?;
        let descriptor: RecoveryDescriptor = serde_json::from_slice(&bytes)?;
        if descriptor.command_digest != command.digest()
            || descriptor.order_ids.len() != tickwire_strategy_sdk::recovery::order_count(command)
        {
            return Err(tickwire_strategy_sdk::Error::Identity);
        }
        let bundle = descriptor
            .recovery
            .load(command.digest())
            .map_err(|e| tickwire_strategy_sdk::Error::Io(std::io::Error::other(e.to_string())))?;
        Ok(Some(ResolvedRecovery {
            order_ids: descriptor.order_ids,
            bundle,
        }))
    }
    fn on_submission(
        &mut self,
        command: &CanonicalCommand,
        result: &std::result::Result<
            tickwire_account_gateway::Status,
            tickwire_binance_account::Error,
        >,
    ) {
        eprintln!(
            "{}",
            serde_json::json!({"event":"hosted_submission","intent":command.command().context.request.intent_id,"result":format!("{result:?}")})
        );
    }
}
struct Strategy(HostedStrategy<Files, 1>);
impl tickwire_runtime::node::Strategy for Strategy {
    fn handover_finish_replay(
        &mut self,
        installed: tickwire_runtime::node::InstalledReplay,
    ) -> Option<tickwire_runtime::node::PreparedReplay> {
        tickwire_runtime::node::Strategy::handover_finish_replay(&mut self.0, installed)
    }
    fn handover_install_replay(
        &mut self,
        source: &tickwire_release::replay::source::VerifiedSourceReplay,
    ) -> Option<tickwire_runtime::node::InstalledReplay> {
        tickwire_runtime::node::Strategy::handover_install_replay(&mut self.0, source)
    }
    fn handover_replay_source(
        &mut self,
        operation: &tickwire_release::Operation,
        member: &tickwire_release::Member,
        fixed: &tickwire_runtime::node::HandoverReadiness,
    ) -> Option<tickwire_release::replay::source::VerifiedSourceReplay> {
        tickwire_runtime::node::Strategy::handover_replay_source(
            &mut self.0,
            operation,
            member,
            fixed,
        )
    }
    fn handover_checkpoint(
        &mut self,
        instance: [u8; 16],
        generation: u64,
    ) -> Option<(tickwire_runtime::node::HandoverReadiness, Vec<Vec<u8>>)> {
        tickwire_runtime::node::Strategy::handover_checkpoint(&mut self.0, instance, generation)
    }
    fn release_preflight(
        &mut self,
        instance: [u8; 16],
        generation: u64,
        roots: [[u8; 32]; 3],
    ) -> Option<Vec<Vec<u8>>> {
        tickwire_runtime::node::Strategy::release_preflight(
            &mut self.0,
            instance,
            generation,
            roots,
        )
    }
    fn handover_ready(&mut self, r: &tickwire_runtime::node::HandoverReadiness) -> bool {
        tickwire_runtime::node::Strategy::handover_ready(&mut self.0, r)
    }
    fn handover_objects(
        &mut self,
        r: &tickwire_runtime::node::HandoverReadiness,
    ) -> Option<Vec<Vec<u8>>> {
        tickwire_runtime::node::Strategy::handover_objects(&mut self.0, r)
    }
    fn handover_quiesce(&mut self, c: &tickwire_runtime::node::StrategySwitch) -> bool {
        tickwire_runtime::node::Strategy::handover_quiesce(&mut self.0, c)
    }
    fn handover_activate(&mut self, c: &tickwire_runtime::node::StrategySwitch) -> bool {
        tickwire_runtime::node::Strategy::handover_activate(&mut self.0, c)
    }
    fn handover_halt(&mut self, id: u64) -> bool {
        tickwire_runtime::node::Strategy::handover_halt(&mut self.0, id)
    }

    fn on_market(&mut self, n: &tickwire_binance_public::Notice<'_>) -> bool {
        tickwire_runtime::node::Strategy::on_market(&mut self.0, n)
    }
    fn next(
        &mut self,
        a: &tickwire_account::Account,
        h: tickwire_runtime::node::Health,
        n: tickwire_binance_clock::LocalTime,
    ) -> Option<tickwire_runtime::node::Submission> {
        tickwire_runtime::node::Strategy::next(&mut self.0, a, h, n)
    }
    fn on_account_baseline(
        &mut self,
        b: &tickwire_account_codec::CanonicalBaseline,
        a: &tickwire_account::Account,
    ) -> bool {
        tickwire_runtime::node::Strategy::on_account_baseline(&mut self.0, b, a)
    }
    fn account_report_position(&self) -> Option<u64> {
        tickwire_runtime::node::Strategy::account_report_position(&self.0)
    }
    fn on_account_record(
        &mut self,
        p: u64,
        r: &tickwire_account_codec::CanonicalRecord,
        b: &tickwire_account_codec::CanonicalBaseline,
        a: &tickwire_account::Account,
    ) -> bool {
        tickwire_runtime::node::Strategy::on_account_record(&mut self.0, p, r, b, a)
    }
    fn on_submission(
        &mut self,
        c: &CanonicalCommand,
        r: &std::result::Result<tickwire_account_gateway::Status, tickwire_binance_account::Error>,
    ) {
        tickwire_runtime::node::Strategy::on_submission(&mut self.0, c, r);
    }
}
impl AppStrategy for Strategy {
    fn instance(&self) -> Option<([u8; 16], u64)> {
        self.0.instance()
    }
    fn set_shadow(&mut self) -> bool {
        self.0.set_shadow();
        true
    }
    fn failed(&self) -> bool {
        self.0.failed()
    }
}
struct RemoteStrategy(tickwire_strategy_host::RemoteHostedStrategy<Files>);
impl tickwire_runtime::node::Strategy for RemoteStrategy {
    fn handover_finish_replay(
        &mut self,
        installed: tickwire_runtime::node::InstalledReplay,
    ) -> Option<tickwire_runtime::node::PreparedReplay> {
        tickwire_runtime::node::Strategy::handover_finish_replay(&mut self.0, installed)
    }
    fn handover_install_replay(
        &mut self,
        source: &tickwire_release::replay::source::VerifiedSourceReplay,
    ) -> Option<tickwire_runtime::node::InstalledReplay> {
        tickwire_runtime::node::Strategy::handover_install_replay(&mut self.0, source)
    }
    fn handover_replay_source(
        &mut self,
        operation: &tickwire_release::Operation,
        member: &tickwire_release::Member,
        fixed: &tickwire_runtime::node::HandoverReadiness,
    ) -> Option<tickwire_release::replay::source::VerifiedSourceReplay> {
        tickwire_runtime::node::Strategy::handover_replay_source(
            &mut self.0,
            operation,
            member,
            fixed,
        )
    }
    fn handover_checkpoint(
        &mut self,
        instance: [u8; 16],
        generation: u64,
    ) -> Option<(tickwire_runtime::node::HandoverReadiness, Vec<Vec<u8>>)> {
        tickwire_runtime::node::Strategy::handover_checkpoint(&mut self.0, instance, generation)
    }
    fn release_preflight(
        &mut self,
        instance: [u8; 16],
        generation: u64,
        roots: [[u8; 32]; 3],
    ) -> Option<Vec<Vec<u8>>> {
        tickwire_runtime::node::Strategy::release_preflight(
            &mut self.0,
            instance,
            generation,
            roots,
        )
    }
    fn handover_ready(&mut self, r: &tickwire_runtime::node::HandoverReadiness) -> bool {
        tickwire_runtime::node::Strategy::handover_ready(&mut self.0, r)
    }
    fn handover_objects(
        &mut self,
        r: &tickwire_runtime::node::HandoverReadiness,
    ) -> Option<Vec<Vec<u8>>> {
        tickwire_runtime::node::Strategy::handover_objects(&mut self.0, r)
    }
    fn handover_quiesce(&mut self, c: &tickwire_runtime::node::StrategySwitch) -> bool {
        tickwire_runtime::node::Strategy::handover_quiesce(&mut self.0, c)
    }
    fn handover_activate(&mut self, c: &tickwire_runtime::node::StrategySwitch) -> bool {
        tickwire_runtime::node::Strategy::handover_activate(&mut self.0, c)
    }
    fn handover_halt(&mut self, id: u64) -> bool {
        tickwire_runtime::node::Strategy::handover_halt(&mut self.0, id)
    }

    fn on_market(&mut self, n: &tickwire_binance_public::Notice<'_>) -> bool {
        tickwire_runtime::node::Strategy::on_market(&mut self.0, n)
    }
    fn next(
        &mut self,
        a: &tickwire_account::Account,
        h: tickwire_runtime::node::Health,
        n: tickwire_binance_clock::LocalTime,
    ) -> Option<tickwire_runtime::node::Submission> {
        tickwire_runtime::node::Strategy::next(&mut self.0, a, h, n)
    }
    fn on_account_baseline(
        &mut self,
        b: &tickwire_account_codec::CanonicalBaseline,
        a: &tickwire_account::Account,
    ) -> bool {
        tickwire_runtime::node::Strategy::on_account_baseline(&mut self.0, b, a)
    }
    fn account_report_position(&self) -> Option<u64> {
        tickwire_runtime::node::Strategy::account_report_position(&self.0)
    }
    fn on_account_record(
        &mut self,
        p: u64,
        r: &tickwire_account_codec::CanonicalRecord,
        b: &tickwire_account_codec::CanonicalBaseline,
        a: &tickwire_account::Account,
    ) -> bool {
        tickwire_runtime::node::Strategy::on_account_record(&mut self.0, p, r, b, a)
    }
    fn on_submission(
        &mut self,
        c: &CanonicalCommand,
        r: &std::result::Result<tickwire_account_gateway::Status, tickwire_binance_account::Error>,
    ) {
        tickwire_runtime::node::Strategy::on_submission(&mut self.0, c, r);
    }
}
impl AppStrategy for RemoteStrategy {
    fn market_consumers(&self, out: &mut Vec<crate::control::market::Consumer>) {
        if let Some((pid, instance, generation)) = self.0.market_consumer() {
            out.push(crate::control::market::Consumer {
                pid,
                instance,
                generation,
            });
        }
    }
    fn arm_market(
        &mut self,
        c: crate::control::market::Consumer,
        a: &tickwire_market_source::transition::Approval,
    ) -> Result<()> {
        if self.0.market_consumer() != Some((c.pid, c.instance, c.generation)) {
            return Err(invalid("remote market process identity"));
        }
        self.0
            .arm_market(a)
            .map_err(|e| invalid(&format!("remote market arm: {e:?}")))
    }
    fn commit_market(
        &mut self,
        c: crate::control::market::Consumer,
        v: &tickwire_market_source::transition::Commit,
    ) -> Result<()> {
        if self.0.market_consumer() != Some((c.pid, c.instance, c.generation)) {
            return Err(invalid("remote market process identity"));
        }
        self.0
            .commit_market(v)
            .map_err(|e| invalid(&format!("remote market commit: {e:?}")))
    }
    fn market_transition(
        &self,
        c: crate::control::market::Consumer,
    ) -> Option<&tickwire_market_source::transition::Receipt> {
        (self.0.market_consumer() == Some((c.pid, c.instance, c.generation)))
            .then(|| self.0.market_transition())
            .flatten()
    }
    fn market_rejection(&self, c: crate::control::market::Consumer) -> Option<u32> {
        (self.0.market_consumer() == Some((c.pid, c.instance, c.generation)))
            .then(|| self.0.market_rejection())
            .flatten()
    }

    fn instance(&self) -> Option<([u8; 16], u64)> {
        Some(self.0.instance())
    }
    fn set_shadow(&mut self) -> bool {
        self.0.set_shadow();
        true
    }
    fn failed(&self) -> bool {
        self.0.failed()
    }
}
#[derive(Clone)]
struct Context {
    depth: tickwire_binance::depth_sync::Config,
    source: tickwire_clock::Source,
    domain: tickwire_clock::ClockDomain,
    risk_domain: [u8; 16],
}
struct Parts {
    owner: tickwire_join::JoinOwner<1>,
    launch: Launch,
    resolver: Files,
    instruments: Vec<tickwire_market_publisher::InstrumentConfig>,
    limits: tickwire_market_publisher::Limits,
}
fn components(
    config: &HostConfig,
    context: &Context,
    cutover: Option<(&ResourceHeader, u64)>,
) -> Result<Parts> {
    for path in [
        &config.bootstrap,
        &config.market_resource,
        &config.executable,
        &config.working_directory,
        &config.directory_parent,
        &config.control_directory,
        &config.recovery_directory,
    ] {
        if !path.is_absolute() {
            return Err(invalid("hosted paths must be absolute"));
        }
    }
    let mut bootstrap: Bootstrap =
        serde_json::from_slice(&read_file(&config.bootstrap, 2 * 1024 * 1024)?)?;
    if let Some(path) = &bootstrap.preflight_job {
        let job: tickwire_strategy_sdk::preflight::Job =
            serde_json::from_slice(&read_file(path, 2 * 1024 * 1024)?)?;
        let bundle = job.bundle.load(&job.operation)?;
        let release = bundle.verify_operation(&job.operation, &job.member)?;
        let loaded = tickwire_release::files::load(&release.target.files, &job.target_files)?;
        if loaded.bytes(&tickwire_release::Role::HostConfig)
            != Some(serde_json::to_vec(config)?.as_slice())
            || loaded.bytes(&tickwire_release::Role::Bootstrap)
                != Some(read_file(&config.bootstrap, 2 * 1024 * 1024)?.as_slice())
            || !config.shadow
            || !bootstrap.handover_enabled
        {
            return Err(invalid("preflight actual HostConfig or Bootstrap"));
        }
    }
    if config.shadow {
        bootstrap.handover_enabled = true;
    }
    if !bootstrap.report_directory.is_absolute() {
        return Err(invalid("report directory must be absolute"));
    }
    if let Some(path) = &bootstrap.recovery_template {
        let template: tickwire_strategy_sdk::recovery::Template =
            serde_json::from_slice(&read_file(path, 2 * 1024 * 1024)?)?;
        if template.output_directory != config.recovery_directory {
            return Err(invalid("worker recovery output directory binding"));
        }
    } else if bootstrap.demo_enabled {
        return Err(invalid("hosted demo requires recovery template"));
    }
    let mut header = ResourceHeader::decode(&read_file(&config.market_resource, 4096)?)
        .map_err(|_| invalid("market resource header"))?;
    if let Some((previous, published)) = cutover {
        header.predecessor_ring_epoch = previous.ring_epoch;
        header.cutover_seq = published;
        header.first_seq = published
            .checked_add(1)
            .ok_or_else(|| invalid("market sequence exhausted"))?;
        header.descriptor_hash = [0; 32];
    }
    let instrument = context.depth.instrument.clone();
    let domain = tickwire_clock::ClockDomain::new(
        bootstrap.market.local.host_boot,
        bootstrap.clock_generation,
    )
    .map_err(|_| invalid("hosted clock domain"))?;
    if bootstrap.risk_domain != context.risk_domain
        || domain != context.domain
        || bootstrap.market.peer.instance != header.producer_instance
        || bootstrap.market.peer.generation != header.producer_generation
        || header.catalog_version != instrument.catalog_version
        || header.resource_kind != 1
        || header.required_features & 66 != 66
        || config.maximum_levels_per_side < context.depth.snapshot_limit
        || bootstrap.maximum_levels < config.maximum_levels_per_side
    {
        return Err(invalid("hosted channel/catalog/capacity binding"));
    }
    let registry = tickwire_join::TopicRegistry {
        catalog_hash: instrument.catalog_hash,
        instrument_set_version: config.instrument_set_version,
        instruments: vec![tickwire_snapshot::InstrumentSpec {
            instrument_id: instrument.instrument_id,
            instrument_version: instrument.instrument_version,
            required_states: tickwire_snapshot::QUOTE,
        }],
    };
    let publisher = tickwire_shm::Publisher::<1>::create(header.clone())?;
    let subscription = tickwire_shm::SubscriptionKey {
        bank: publisher.key(),
        instance: u128::from_le_bytes(bootstrap.market.local.instance),
        generation: config.subscription_generation,
        cursor: u128::from_le_bytes(config.market_cursor_id),
    };
    let owner = tickwire_join::JoinOwner::new(publisher, registry)?;
    let market = match context.source {
        tickwire_clock::Source::SpotProduction | tickwire_clock::Source::SpotTest => {
            tickwire_binance::request::Market::Spot
        }
        _ => tickwire_binance::request::Market::Usdm,
    };
    let instruments = vec![tickwire_market_publisher::InstrumentConfig {
        instrument: instrument.clone(),
        key: context.depth.key,
        market,
        time_unit: context.depth.time_unit,
    }];
    let limits = tickwire_market_publisher::Limits {
        levels_per_side: config.maximum_levels_per_side,
        cache_bytes: config.maximum_cache_bytes,
        snapshot_bytes: config.maximum_snapshot_bytes,
        max_delta_bytes: config.maximum_delta_bytes,
    };
    let identity = bootstrap.market.local.identity();
    let launch = Launch {
        spec: tickwire_supervisor::LaunchSpec {
            identity: identity.clone(),
            executable: config.executable.clone(),
            arguments: Vec::new(),
            working_directory: config.working_directory.clone(),
            environment: BTreeMap::new(),
            uid: config.uid,
            gid: config.gid,
            inherit_output: true,
        },
        bootstrap,
        bootstrap_file: config.bootstrap.clone(),
        directory_parent: config.directory_parent.clone(),
        supervisor: config.supervisor.identity(),
        parent_process: tickwire_control::ProcessIdentity {
            pid: std::process::id(),
            uid: config.uid,
            gid: config.gid,
        },
        registration: tickwire_join::SubscriberRegistration {
            identity,
            subscription,
            qos: header.qos,
        },
        snapshot_id: config.snapshot_id,
        request_directory: config.control_directory.clone(),
        requests: tickwire_control_store::Config {
            physical: tickwire_journal::Config::new(config.control_stream, config.control_domain),
            writer_epoch: config.control_writer_epoch,
            owner: header.producer_instance,
            max_requests: 1024,
            max_pending: 32,
            max_response_bytes: 65536,
            max_retained_bytes: 16 * 1024 * 1024,
        },
    };
    Ok(Parts {
        owner,
        launch,
        resolver: Files {
            directory: config.recovery_directory.clone(),
        },
        instruments,
        limits,
    })
}
struct Rotations {
    directory: PathBuf,
    next: u64,
    context: Context,
}
impl RotationSource<Files, 1> for Rotations {
    fn next(
        &mut self,
        current: &ResourceHeader,
        published: u64,
    ) -> tickwire_strategy_host::Result<Option<RotationPlan<Files, 1>>> {
        let path = self.directory.join(format!("{:020}.json", self.next));
        match std::fs::symlink_metadata(&path) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
            Ok(_) => {}
        }
        let build = (|| -> Result<_> {
            let config: HostConfig = serde_json::from_slice(&read_file(&path, 2 * 1024 * 1024)?)?;
            let parts = components(&config, &self.context, Some((current, published)))?;
            if parts.owner.header().ring_epoch <= current.ring_epoch
                || parts.owner.header().feed_generation <= current.feed_generation
            {
                return Err(invalid(
                    "rotation must supply later ring and feed generations",
                ));
            }
            Ok(parts)
        })();
        let parts = build
            .map_err(|e| tickwire_strategy_host::Error::Io(std::io::Error::other(e.to_string())))?;
        self.next = self
            .next
            .checked_add(1)
            .ok_or(tickwire_strategy_host::Error::Invalid)?;
        Ok(Some(RotationPlan {
            owner: parts.owner,
            launch: parts.launch,
            resolver: parts.resolver,
        }))
    }
}
/// Trusted files identify approved SDK processes and immutable channel identities.
/// Build/qualification hashes are checked by the supervisor before mapping admission.
pub fn build(
    config: &HostConfig,
    runtime: &files::Configuration,
    policy: ControlPlane,
) -> Result<Box<dyn AppStrategy>> {
    let mut bootstrap: Bootstrap =
        serde_json::from_slice(&read_file(&config.bootstrap, 2 * 1024 * 1024)?)?;
    if config.shadow {
        bootstrap.handover_enabled = true;
    }
    if bootstrap.external_market.is_some() {
        return build_remote(config, runtime, policy, bootstrap);
    }
    let context = Context {
        depth: runtime.market.depth.clone(),
        source: runtime.market.source,
        domain: runtime.time.domain,
        risk_domain: runtime.command_binding.risk_domain_id,
    };
    let parts = components(config, &context, None)?;
    let publisher = SharedPublisher::new(tickwire_market_publisher::TopicPublisher::new(
        parts.owner,
        parts.instruments,
        parts.limits,
    )?);
    // SAFETY: deployment files name qualified SDKs; Registry verifies executable
    // digest, owned Child PID/UID/GID, and authenticated descriptors on every launch.
    let mut strategy = unsafe {
        HostedStrategy::new(
            parts.launch,
            publisher,
            parts.resolver,
            policy,
            context.depth.instrument.instrument_id,
            context.depth.instrument.instrument_version,
        )?
    };
    if config.shadow {
        strategy.set_shadow();
    }
    if let Some(directory) = &config.rotation_directory {
        if !directory.is_absolute() || config.first_rotation_sequence == 0 {
            return Err(invalid("rotation directory/sequence"));
        }
        strategy.set_rotation_source(Box::new(Rotations {
            directory: directory.clone(),
            next: config.first_rotation_sequence,
            context,
        }));
    }
    Ok(Box::new(Strategy(strategy)))
}

fn build_remote(
    config: &HostConfig,
    runtime: &files::Configuration,
    policy: ControlPlane,
    bootstrap: Bootstrap,
) -> Result<Box<dyn AppStrategy>> {
    let external = bootstrap
        .external_market
        .as_ref()
        .ok_or_else(|| invalid("external MD configuration"))?;
    let directory = config
        .md_admission_directory
        .as_ref()
        .ok_or_else(|| invalid("MD admission directory required"))?;
    if !directory.is_absolute()
        || !config.executable.is_absolute()
        || !config.working_directory.is_absolute()
        || !config.directory_parent.is_absolute()
        || !config.recovery_directory.is_absolute()
        || external.domain()? != runtime.time.domain
        || bootstrap.risk_domain != runtime.command_binding.risk_domain_id
        || bootstrap.market.local.identity() != external.endpoint.local.identity()
    {
        return Err(invalid("remote MD launcher binding"));
    }
    if let Some(path) = &bootstrap.recovery_template {
        let template: tickwire_strategy_sdk::recovery::Template =
            serde_json::from_slice(&read_file(path, 2 * 1024 * 1024)?)?;
        if template.output_directory != config.recovery_directory {
            return Err(invalid("worker recovery directory"));
        }
    } else if bootstrap.demo_enabled {
        return Err(invalid("demo recovery template required"));
    }
    let launch = tickwire_strategy_host::RemoteLaunch {
        spec: tickwire_supervisor::LaunchSpec {
            identity: external.endpoint.local.identity(),
            executable: config.executable.clone(),
            arguments: Vec::new(),
            working_directory: config.working_directory.clone(),
            environment: BTreeMap::new(),
            uid: config.uid,
            gid: config.gid,
            inherit_output: true,
        },
        bootstrap,
        directory_parent: config.directory_parent.clone(),
        supervisor: config.supervisor.identity(),
        parent_process: tickwire_control::ProcessIdentity {
            pid: std::process::id(),
            uid: config.uid,
            gid: config.gid,
        },
        md_admission_directory: directory.clone(),
        snapshot_id: config.snapshot_id,
    };
    // SAFETY: deployment binds qualified builds and exact MD identity; Registry
    // retains the spawned child and authenticates its real intent/report peers.
    let mut strategy = unsafe {
        tickwire_strategy_host::RemoteHostedStrategy::launch(
            launch,
            Files {
                directory: config.recovery_directory.clone(),
            },
            policy,
        )?
    };
    if config.shadow {
        strategy.set_shadow();
    }
    Ok(Box::new(RemoteStrategy(strategy)))
}
