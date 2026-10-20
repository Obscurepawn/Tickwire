//! File-configured qualified child process assembly. Runtime owns all real I/O.
use crate::{
    Result,
    config::read_file,
    invalid,
    strategy::{AppStrategy, RecoveryFiles},
};
use serde::Deserialize;
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

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostConfig {
    pub bootstrap: PathBuf,
    pub market_resource: PathBuf,
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
    let bootstrap: Bootstrap =
        serde_json::from_slice(&read_file(&config.bootstrap, 2 * 1024 * 1024)?)?;
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
