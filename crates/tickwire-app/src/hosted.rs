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
use tickwire_strategy_host::{HostedStrategy, Launch, SharedPublisher};
use tickwire_strategy_sdk::{
    bootstrap::{Bootstrap, IdentityConfig},
    runtime::{RecoveryResolver, ResolvedRecovery},
};
use tickwire_venue::CanonicalCommand;
pub mod inputs;
pub mod parameters;
pub mod profile;
mod startup;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HostConfig {
    #[serde(default, skip_serializing, rename = "shadow")]
    _active: crate::strategy::ActiveSlot,
    #[serde(default, skip_serializing, rename = "capture")]
    _capture: (),
    #[serde(default, skip_serializing, rename = "rotation_directory")]
    _rotation: (),
    #[serde(default, skip_serializing, rename = "first_rotation_sequence")]
    _rotation_cursor: u64,
    /// Bind approved account and market inputs to the actual registered worker.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub projection_inputs: Option<inputs::Config>,
    /// Explicit consumption of approved strategy parameters by the real worker.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub projection_parameters: Option<parameters::Config>,
    /// Startup-approved binding of the projection's process launch fields.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub projection_launch: Option<profile::Config>,
    /// Optional cgroup, CPU, NUMA and memory limits for the worker.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<tickwire_strategy_host::ProcessResources>,
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
        result: &tickwire_runtime::node::SubmissionResult<'_>,
    ) {
        eprintln!(
            "{}",
            serde_json::json!({"event":"hosted_submission","intent":command.command().context.request.intent_id,"result":format!("{result:?}")})
        );
    }
    fn on_request_reply(
        &mut self,
        command: &CanonicalCommand,
        result: &tickwire_venue::RequestReply,
    ) {
        eprintln!(
            "{}",
            serde_json::json!({"event":"hosted_request_reply","intent":command.command().context.request.intent_id,"result":format!("{result:?}")})
        );
    }
}
struct Strategy(HostedStrategy<Files, 1>, u64, LaunchProgress);
impl Strategy {
    fn reject_startup(&mut self) {
        self.2.failed = true;
        if let Some((instance, _)) = self.0.instance()
            && let Some(child) = self.0.child()
            && let Err(error) = child.registry().terminate(instance)
        {
            eprintln!("strategy startup cleanup failed: {error:?}");
        }
    }
    fn input_account_ok(
        &mut self,
        baseline: Option<&tickwire_account_codec::CanonicalBaseline>,
        account: &tickwire_account::Account,
    ) -> bool {
        if self.2.failed {
            return false;
        }
        if self
            .2
            .startup
            .matches_account(baseline, account, self.0.instance())
        {
            return true;
        }
        self.reject_startup();
        false
    }
}
struct LaunchProgress {
    pending: Option<profile::Prepared>,
    proof: Option<profile::LaunchBindingProof>,
    startup: startup::Progress,
    failed: bool,
}
impl tickwire_runtime::node::Strategy for Strategy {
    fn on_market(&mut self, n: &tickwire_binance_public::Notice<'_>) -> bool {
        !self.2.failed && tickwire_runtime::node::Strategy::on_market(&mut self.0, n)
    }
    fn next(
        &mut self,
        a: &tickwire_account::Account,
        h: tickwire_runtime::node::Health,
        n: tickwire_binance_clock::LocalTime,
    ) -> Option<tickwire_runtime::node::Submission> {
        if !self.input_account_ok(None, a) {
            return None;
        }
        if (self.2.pending.is_some() || self.2.startup.busy()) && !self.0.prepare_start(n) {
            return None;
        }
        if self.2.pending.is_some() {
            let prepared = self.2.pending.take()?;
            let result = self
                .0
                .observe_resources()
                .map_err(|error| -> Box<dyn std::error::Error> { Box::new(error) })
                .and_then(|observed| prepared.observe(observed));
            match result {
                Ok(proof) => self.2.proof = Some(proof),
                Err(error) => {
                    self.2.failed = true;
                    eprintln!("strategy projection launch observation failed: {error}");
                    if let Some((instance, _)) = self.0.instance()
                        && let Some(child) = self.0.child()
                        && let Err(error) = child.registry().terminate(instance)
                    {
                        eprintln!("strategy projection launch cleanup failed: {error:?}");
                    }
                    return None;
                }
            }
        }
        match self.2.startup.poll(&mut self.0) {
            Ok(true) => {}
            Ok(false) => return None,
            Err(error) => {
                self.2.failed = true;
                eprintln!("strategy startup consumption failed: {error}");
                if let Some((instance, _)) = self.0.instance()
                    && let Some(child) = self.0.child()
                    && let Err(error) = child.registry().terminate(instance)
                {
                    eprintln!("strategy parameter cleanup failed: {error:?}");
                }
                return None;
            }
        }
        tickwire_runtime::node::Strategy::next(&mut self.0, a, h, n)
    }
    fn on_account_baseline(
        &mut self,
        b: &tickwire_account_codec::CanonicalBaseline,
        a: &tickwire_account::Account,
    ) -> bool {
        self.input_account_ok(Some(b), a)
            && tickwire_runtime::node::Strategy::on_account_baseline(&mut self.0, b, a)
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
        self.input_account_ok(Some(b), a)
            && tickwire_runtime::node::Strategy::on_account_record(&mut self.0, p, r, b, a)
    }
    fn on_submission(
        &mut self,
        c: &CanonicalCommand,
        r: &tickwire_runtime::node::SubmissionResult<'_>,
    ) {
        tickwire_runtime::node::Strategy::on_submission(&mut self.0, c, r);
    }
    fn on_request_reply(&mut self, c: &CanonicalCommand, r: &tickwire_venue::RequestReply) {
        tickwire_runtime::node::Strategy::on_request_reply(&mut self.0, c, r);
    }
}
impl AppStrategy for Strategy {
    fn projection_inputs(
        &self,
        target: crate::observation::Target,
    ) -> Option<&inputs::InputsProof> {
        observation_target(target, self.1, self.instance()).ok()?;
        self.2.startup.inputs_proof()
    }

    fn projection_parameters(
        &self,
        target: crate::observation::Target,
    ) -> Option<&parameters::ParametersProof> {
        observation_target(target, self.1, self.instance()).ok()?;
        self.2.startup.proof()
    }
    fn projection_launch(
        &self,
        target: crate::observation::Target,
    ) -> Option<&profile::LaunchBindingProof> {
        observation_target(target, self.1, self.instance()).ok()?;
        self.2.proof.as_ref()
    }
    fn observation_process(
        &mut self,
        t: crate::observation::Target,
    ) -> tickwire_strategy_host::Result<tickwire_supervisor::LiveChildObservation> {
        observation_target(t, self.1, self.instance())?;
        self.0.observe_process()
    }
    fn begin_observation(
        &mut self,
        t: crate::observation::Target,
        r: tickwire_strategy_host::ObservationRequest,
    ) -> tickwire_strategy_host::Result<()> {
        observation_target(t, self.1, self.instance())?;
        if self.2.startup.busy() {
            return Err(tickwire_strategy_host::Error::Busy);
        }
        self.0.begin_observation(r)
    }
    fn poll_observation(
        &mut self,
        t: crate::observation::Target,
    ) -> tickwire_strategy_host::Result<Option<tickwire_strategy_host::FreshObservation>> {
        observation_target(t, self.1, self.instance())?;
        if self.2.startup.busy() {
            return Err(tickwire_strategy_host::Error::Busy);
        }
        self.0.poll_observation()
    }
    fn cancel_observation(
        &mut self,
        t: crate::observation::Target,
        id: [u8; 16],
    ) -> tickwire_strategy_host::Result<()> {
        observation_target(t, self.1, self.instance())?;
        if self.2.startup.busy() {
            return Err(tickwire_strategy_host::Error::Busy);
        }
        self.0.cancel_observation(id)
    }

    fn instance(&self) -> Option<([u8; 16], u64)> {
        self.0.instance()
    }
    fn failed(&self) -> bool {
        self.2.failed || self.0.failed()
    }
}
struct RemoteStrategy(
    tickwire_strategy_host::RemoteHostedStrategy<Files>,
    u64,
    LaunchProgress,
);
impl RemoteStrategy {
    fn input_account_ok(
        &mut self,
        baseline: Option<&tickwire_account_codec::CanonicalBaseline>,
        account: &tickwire_account::Account,
    ) -> bool {
        if self.2.failed {
            return false;
        }
        if self
            .2
            .startup
            .matches_account(baseline, account, Some(self.0.instance()))
        {
            return true;
        }
        self.2.failed = true;
        self.0.reject_configuration();
        false
    }
}
impl tickwire_runtime::node::Strategy for RemoteStrategy {
    fn on_market(&mut self, n: &tickwire_binance_public::Notice<'_>) -> bool {
        tickwire_runtime::node::Strategy::on_market(&mut self.0, n)
    }
    fn next(
        &mut self,
        a: &tickwire_account::Account,
        h: tickwire_runtime::node::Health,
        n: tickwire_binance_clock::LocalTime,
    ) -> Option<tickwire_runtime::node::Submission> {
        if !self.input_account_ok(None, a) {
            return None;
        }
        match self.2.startup.poll(&mut self.0) {
            Ok(true) => tickwire_runtime::node::Strategy::next(&mut self.0, a, h, n),
            Ok(false) => None,
            Err(error) => {
                self.2.failed = true;
                eprintln!("strategy startup consumption failed: {error}");
                self.0.reject_configuration();
                None
            }
        }
    }
    fn on_account_baseline(
        &mut self,
        b: &tickwire_account_codec::CanonicalBaseline,
        a: &tickwire_account::Account,
    ) -> bool {
        self.input_account_ok(Some(b), a)
            && tickwire_runtime::node::Strategy::on_account_baseline(&mut self.0, b, a)
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
        self.input_account_ok(Some(b), a)
            && tickwire_runtime::node::Strategy::on_account_record(&mut self.0, p, r, b, a)
    }
    fn on_submission(
        &mut self,
        c: &CanonicalCommand,
        r: &tickwire_runtime::node::SubmissionResult<'_>,
    ) {
        tickwire_runtime::node::Strategy::on_submission(&mut self.0, c, r);
    }
    fn on_request_reply(&mut self, c: &CanonicalCommand, r: &tickwire_venue::RequestReply) {
        tickwire_runtime::node::Strategy::on_request_reply(&mut self.0, c, r);
    }
}
impl AppStrategy for RemoteStrategy {
    fn projection_inputs(
        &self,
        target: crate::observation::Target,
    ) -> Option<&inputs::InputsProof> {
        observation_target(target, self.1, self.instance()).ok()?;
        self.2.startup.inputs_proof()
    }

    fn projection_parameters(
        &self,
        target: crate::observation::Target,
    ) -> Option<&parameters::ParametersProof> {
        observation_target(target, self.1, self.instance()).ok()?;
        self.2.startup.proof()
    }
    fn projection_launch(
        &self,
        target: crate::observation::Target,
    ) -> Option<&profile::LaunchBindingProof> {
        observation_target(target, self.1, self.instance()).ok()?;
        self.2.proof.as_ref()
    }
    fn observation_process(
        &mut self,
        t: crate::observation::Target,
    ) -> tickwire_strategy_host::Result<tickwire_supervisor::LiveChildObservation> {
        observation_target(t, self.1, self.instance())?;
        self.0.observe_process()
    }
    fn begin_observation(
        &mut self,
        t: crate::observation::Target,
        r: tickwire_strategy_host::ObservationRequest,
    ) -> tickwire_strategy_host::Result<()> {
        observation_target(t, self.1, self.instance())?;
        if self.2.startup.busy() {
            return Err(tickwire_strategy_host::Error::Busy);
        }
        self.0.begin_observation(r)
    }
    fn poll_observation(
        &mut self,
        t: crate::observation::Target,
    ) -> tickwire_strategy_host::Result<Option<tickwire_strategy_host::FreshObservation>> {
        observation_target(t, self.1, self.instance())?;
        if self.2.startup.busy() {
            return Err(tickwire_strategy_host::Error::Busy);
        }
        self.0.poll_observation()
    }
    fn cancel_observation(
        &mut self,
        t: crate::observation::Target,
        id: [u8; 16],
    ) -> tickwire_strategy_host::Result<()> {
        observation_target(t, self.1, self.instance())?;
        if self.2.startup.busy() {
            return Err(tickwire_strategy_host::Error::Busy);
        }
        self.0.cancel_observation(id)
    }

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
        if self.2.startup.inputs_enabled() {
            return Err(invalid(
                "projected input scope requires a new approved startup before market changes",
            ));
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
        if self.2.startup.inputs_enabled() {
            return Err(invalid(
                "projected input scope requires a new approved startup before market changes",
            ));
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
    fn failed(&self) -> bool {
        self.2.failed || self.0.failed()
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
    bootstrap: Bootstrap,
    local_header: Option<ResourceHeader>,
) -> Result<Parts> {
    if let Some(resources) = &config.resources {
        resources.validate()?;
    }
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
    let header = match local_header {
        Some(header) => header,
        None => ResourceHeader::decode(&read_file(&config.market_resource, 4096)?)
            .map_err(|_| invalid("market resource header"))?,
    };
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
        resources: config.resources.clone(),
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
pub fn build(
    config: &HostConfig,
    runtime: &files::Configuration,
    policy: ControlPlane,
) -> Result<Box<dyn AppStrategy>> {
    let bootstrap = serde_json::from_slice(&read_file(
        &config.bootstrap,
        tickwire_strategy_sdk::bootstrap::MAX_BOOTSTRAP_BYTES,
    )?)?;
    build_prepared(config, runtime, policy, bootstrap, None)
}

pub(super) fn build_prepared(
    config: &HostConfig,
    runtime: &files::Configuration,
    policy: ControlPlane,
    bootstrap: Bootstrap,
    local_header: Option<ResourceHeader>,
) -> Result<Box<dyn AppStrategy>> {
    if let Some(resources) = &config.resources {
        resources.validate()?;
    }
    // Validate the same owned Bootstrap that will be handed to Host. A second
    // pathname read must not replace the identity checked by the projection.
    let projection = profile::prepare(config, &bootstrap)?;
    let parameters = parameters::prepare(config, &bootstrap)?;
    let inputs = inputs::prepare(config, &bootstrap, runtime)?;
    if bootstrap.preflight_job.is_some() {
        return Err(invalid("worker bootstrap selects a retired upgrade job"));
    }
    if bootstrap.external_market.is_some() {
        return build_remote(
            config, runtime, policy, bootstrap, projection, parameters, inputs,
        );
    }
    let context = Context {
        depth: runtime.market.depth.clone(),
        source: runtime.market.source,
        domain: runtime.time.domain,
        risk_domain: runtime.command_binding.risk_domain_id,
    };
    let parts = components(config, &context, bootstrap, local_header)?;
    let logical = parts.launch.spec.identity.logical_strategy;
    let publisher = SharedPublisher::new(tickwire_market_publisher::TopicPublisher::new(
        parts.owner,
        parts.instruments,
        parts.limits,
    )?);
    // SAFETY: deployment files name qualified SDKs; Registry verifies executable
    // digest, owned Child PID/UID/GID, and authenticated descriptors on every launch.
    let strategy = unsafe {
        HostedStrategy::new(
            parts.launch,
            publisher,
            parts.resolver,
            policy,
            context.depth.instrument.instrument_id,
            context.depth.instrument.instrument_version,
        )?
    };
    Ok(Box::new(Strategy(
        strategy,
        logical,
        LaunchProgress {
            pending: projection,
            proof: None,
            startup: startup::Progress::new(parameters, inputs),
            failed: false,
        },
    )))
}

fn build_remote(
    config: &HostConfig,
    runtime: &files::Configuration,
    policy: ControlPlane,
    bootstrap: Bootstrap,
    projection: Option<profile::Prepared>,
    parameters: Option<parameters::Prepared>,
    inputs: Option<inputs::Prepared>,
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
    let logical = bootstrap.market.local.identity().logical_strategy;
    let launch = tickwire_strategy_host::RemoteLaunch {
        resources: config.resources.clone(),
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
    let proof = projection
        .map(|prepared| prepared.observe(strategy.observe_resources()?))
        .transpose()?;
    Ok(Box::new(RemoteStrategy(
        strategy,
        logical,
        LaunchProgress {
            pending: None,
            proof,
            startup: startup::Progress::new(parameters, inputs),
            failed: false,
        },
    )))
}

fn observation_target(
    t: crate::observation::Target,
    logical: u64,
    actual: Option<([u8; 16], u64)>,
) -> tickwire_strategy_host::Result<()> {
    if t.logical_strategy_id != logical || actual != Some((t.instance, t.generation)) {
        return Err(tickwire_strategy_host::Error::Invalid);
    }
    Ok(())
}
