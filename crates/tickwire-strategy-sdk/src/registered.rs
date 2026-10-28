//! Statically linked user factories. Configuration remains a qualified release artifact.
use crate::restore::RestoreFactory;
use crate::{Error, Result, Strategy, restore};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tickwire_release::{Role, files::LoadedFiles, replay::VersionedBytes};
mod rebuild;
pub use rebuild::{RebuildConfigured, configure_rebuild};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FactoryBinding {
    pub id: String,
    pub configuration_file: PathBuf,
    pub configuration: tickwire_release::Object,
    pub revision: u64,
}
impl FactoryBinding {
    pub fn validate(&self) -> Result<()> {
        if self.id.is_empty()
            || self.id.len() > 64
            || !self
                .id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
            || !self.configuration_file.is_absolute()
            || self.revision == 0
        {
            return Err(Error::Identity);
        }
        self.configuration.validate(2 * 1024 * 1024)?;
        Ok(())
    }
}
/// A factory creates a fresh strategy using exactly its decoded configuration.
/// Restore/migrate/rebuild retain the default Unsupported behavior until supplied.
pub trait UserFactory: restore::RestoreFactory + Clone {
    fn create(&self) -> restore::Result<Self::Running>;
    fn begin_rebuild(
        &self,
        _input: &crate::replay::rebuild::FactoryInput,
    ) -> crate::replay::Result<Self::Running> {
        Err(crate::replay::Error::Unsupported)
    }
    fn consumed_actions(&self, _checkpoint: &[u8]) -> crate::replay::Result<Vec<[u8; 16]>> {
        Err(crate::replay::Error::Unsupported)
    }
}
pub trait Registration {
    type Factory: UserFactory;
    type Source: crate::replay::application::BusinessFactory;
    fn id(&self) -> &'static str;
    fn configure(
        &self,
        target: &restore::Target,
        config: &VersionedBytes,
    ) -> restore::Result<Self::Factory>;
    fn source(&self, schema: u64) -> crate::replay::Result<Self::Source>;
    /// Optional old-version rebuild/restore probe. The returned value can only
    /// be made by the SDK's actual callback executors, never by deserializing a receipt.
    fn rebuild_probe(
        &self,
        _input: &tickwire_release::rebuild::rollback::Verified,
        _files: &LoadedFiles,
    ) -> Result<crate::replay::rebuild::rollback::Computation> {
        Err(crate::replay::Error::Unsupported.into())
    }
}
#[derive(Clone)]
pub struct Configured<F> {
    factory: F,
    identity: ([u8; 16], u64, u64),
    parameters: VersionedBytes,
}
/// File hash/path validation precedes user code; the resulting factory retains
/// the exact immutable configuration and target identity through installation.
pub fn configure<R: Registration>(
    registration: &R,
    binding: &FactoryBinding,
    target: &restore::Target,
    files: &LoadedFiles,
) -> Result<Configured<R::Factory>> {
    binding.validate()?;
    if registration.id() != binding.id
        || files.path(&Role::StrategyConfig) != Some(binding.configuration_file.as_path())
        || target.instance == [0; 16]
        || target.generation == 0
        || target.logical_strategy == 0
        || target.command.is_some()
        || target.enabled
        || target.minimum_bid_ticks != 0
    {
        return Err(Error::Identity);
    }
    if files.object(&Role::StrategyConfig) != Some(binding.configuration) {
        return Err(Error::Identity);
    }
    let bytes = files.bytes(&Role::StrategyConfig).ok_or(Error::Identity)?;
    if bytes.is_empty() || bytes.len() > 2 * 1024 * 1024 {
        return Err(Error::Bounds);
    }
    files.recheck()?;
    let configured = configure_bytes(registration, binding, target, bytes)?;
    files.recheck()?;
    Ok(configured)
}
fn configure_bytes<R: Registration>(
    registration: &R,
    binding: &FactoryBinding,
    target: &restore::Target,
    bytes: &[u8],
) -> Result<Configured<R::Factory>> {
    binding.validate()?;
    if registration.id() != binding.id
        || target.instance == [0; 16]
        || target.generation == 0
        || target.logical_strategy == 0
        || target.command.is_some()
        || target.enabled
        || target.minimum_bid_ticks != 0
    {
        return Err(Error::Identity);
    }
    binding.configuration.verify(bytes, 2 * 1024 * 1024)?;
    let parameters = VersionedBytes {
        revision: binding.revision,
        bytes: bytes.to_vec(),
    };
    let factory = registration.configure(target, &parameters)?;
    factory.capabilities().validate()?;
    if factory.configuration(target)? != parameters {
        return Err(Error::Identity);
    }
    Ok(Configured {
        factory,
        identity: (target.instance, target.generation, target.logical_strategy),
        parameters,
    })
}

impl<F: UserFactory> Configured<F> {
    pub fn create(&self) -> Result<Running<F>> {
        let strategy = self.factory.create()?;
        self.factory.validate_checkpoint(&strategy.checkpoint()?)?;
        let running = Running {
            strategy,
            factory: self.clone(),
        };
        running.replay_registration()?.ok_or(Error::NotReady)?;
        Ok(running)
    }
    fn wrap(&self, strategy: F::Running) -> Running<F> {
        Running {
            strategy,
            factory: self.clone(),
        }
    }
}
impl<F: UserFactory> RestoreFactory for Configured<F> {
    type Running = Running<F>;
    fn capabilities(&self) -> tickwire_release::Capabilities {
        self.factory.capabilities()
    }
    fn configuration(&self, target: &restore::Target) -> restore::Result<VersionedBytes> {
        if self.identity != (target.instance, target.generation, target.logical_strategy)
            || target.command.is_some()
            || target.enabled
            || target.minimum_bid_ticks != 0
        {
            return Err(restore::Error::Identity);
        }
        Ok(self.parameters.clone())
    }
    fn restore(&self, input: &restore::VerifiedRestoreInput) -> restore::Result<Self::Running> {
        self.configuration(input.target())?;
        Ok(self.wrap(self.factory.restore(input)?))
    }
    fn migrate(&self, input: &restore::VerifiedRestoreInput) -> restore::Result<Self::Running> {
        self.configuration(input.target())?;
        Ok(self.wrap(self.factory.migrate(input)?))
    }
    fn rebuild(&self, input: &restore::VerifiedRestoreInput) -> restore::Result<Self::Running> {
        self.configuration(input.target())?;
        Ok(self.wrap(self.factory.rebuild(input)?))
    }
    fn validate_checkpoint(&self, state: &[u8]) -> restore::Result<()> {
        self.factory.validate_checkpoint(state)
    }
}
/// Preserves the actual factory for C migration; no live authority is stored here.
pub struct Running<F: UserFactory> {
    strategy: F::Running,
    factory: Configured<F>,
}
impl<F: UserFactory> Strategy for Running<F> {
    fn prepare_replayed(
        &self,
        release: &tickwire_release::MemberRelease<'_>,
        target: restore::Target,
        proof: &tickwire_release::replay::source::VerifiedSourceReplay,
        source: tickwire_release::replay::Source,
        config: VersionedBytes,
    ) -> crate::replay::Result<crate::replay::install::Candidate<Self>> {
        crate::replay::install::prepare(&self.factory, release, target, proof, source, config)
    }
    fn replay_registration(&self) -> Result<Option<crate::replay::native::Registration>> {
        let value = self
            .strategy
            .replay_registration()?
            .ok_or(Error::NotReady)?;
        if value.schema != self.factory.capabilities().state_schema
            || value.config != self.factory.parameters
        {
            return Err(Error::Identity);
        }
        Ok(Some(value))
    }
    fn checkpoint(&self) -> Result<Vec<u8>> {
        self.strategy.checkpoint()
    }
    fn on_market(
        &mut self,
        v: crate::MarketView<'_>,
        n: u64,
    ) -> Result<Option<tickwire_venue::CanonicalCommand>> {
        self.strategy.on_market(v, n)
    }
    fn on_report(
        &mut self,
        r: &crate::reports::AccountReport,
        n: u64,
    ) -> Result<Option<tickwire_venue::CanonicalCommand>> {
        self.strategy.on_report(r, n)
    }
    fn on_market_recorded(
        &mut self,
        v: crate::MarketView<'_>,
        c: &mut crate::replay::ReplayContext<'_>,
    ) -> Result<Option<tickwire_venue::CanonicalCommand>> {
        self.strategy.on_market_recorded(v, c)
    }
    fn on_report_recorded(
        &mut self,
        r: &crate::reports::AccountReport,
        c: &mut crate::replay::ReplayContext<'_>,
    ) -> Result<Option<tickwire_venue::CanonicalCommand>> {
        self.strategy.on_report_recorded(r, c)
    }
    fn on_timer(
        &mut self,
        id: u64,
        g: u64,
        c: &mut crate::replay::ReplayContext<'_>,
    ) -> Result<Option<tickwire_venue::CanonicalCommand>> {
        self.strategy.on_timer(id, g, c)
    }
    fn on_external(
        &mut self,
        s: &tickwire_release::replay::StreamKey,
        schema: u64,
        b: &[u8],
        c: &mut crate::replay::ReplayContext<'_>,
    ) -> Result<Option<tickwire_venue::CanonicalCommand>> {
        self.strategy.on_external(s, schema, b, c)
    }
    fn apply_config(&mut self, b: &[u8], c: &mut crate::replay::ReplayContext<'_>) -> Result<()> {
        self.strategy.apply_config(b, c)
    }
    fn apply_model(
        &mut self,
        id: u16,
        b: &[u8],
        c: &mut crate::replay::ReplayContext<'_>,
    ) -> Result<()> {
        self.strategy.apply_model(id, b, c)
    }
}

fn target_identity(config: &crate::bootstrap::Bootstrap) -> Result<restore::Target> {
    if config.demo_enabled
        || config.demo_command_file.is_some()
        || config.demo_minimum_bid_ticks != 0
    {
        return Err(Error::Identity);
    }
    Ok(restore::Target {
        instance: config.market.local.instance,
        generation: config.market.local.generation,
        logical_strategy: config.market.local.logical_strategy,
        command: None,
        minimum_bid_ticks: 0,
        enabled: false,
    })
}
fn qualified<R: Registration>(
    registration: &R,
    config: &crate::bootstrap::Bootstrap,
    files: &LoadedFiles,
) -> Result<Configured<R::Factory>> {
    let target = crate::preflight::target(config, files)?;
    configure(
        registration,
        config.factory.as_ref().ok_or(Error::NotReady)?,
        &target,
        files,
    )
}
/// Common executable entry for a statically registered user strategy. The same
/// compiled registry handles live, old-version replay, restore and rollback.
/// # Safety
/// The supervisor admits qualified SHM peers, keeps artifact paths immutable,
/// and enforces the exact resource/cursor lifetime contract of `worker::run`.
#[allow(unsafe_code)]
pub unsafe fn run<R: Registration>(
    registration: &R,
    args: impl IntoIterator<Item = std::ffi::OsString>,
) -> Result<()> {
    let mut args = args.into_iter();
    let mode = args.next().ok_or(Error::Identity)?;
    let path = PathBuf::from(args.next().ok_or(Error::Identity)?);
    if args.next().is_some() {
        return Err(Error::Identity);
    }
    match mode.to_str().ok_or(Error::Identity)? {
        "--capture-only" => {
            let config: crate::capture::Bootstrap =
                crate::worker::read_config(&path, crate::capture::MAX_BYTES)?;
            capture_entry(registration, &config)?;
            match crate::capture_worker::run(config)? {
                crate::capture_worker::Outcome::Stopped => {}
                crate::capture_worker::Outcome::Install { session, request } => {
                    rebuild::install(registration, *session, *request)?;
                }
            }
        }
        "--rebuild-rollback" => {
            let job = crate::worker::read_config(&path, 2 * 1024 * 1024)?;
            crate::replay::rebuild::probe::run_with(&job, |input, files| {
                registration.rebuild_probe(input, files)
            })?;
        }
        "--source-replay" => {
            let job: crate::replay::process::SourceJob =
                crate::worker::read_config(&path, 2 * 1024 * 1024)?;
            let business = registration.source(job.source.state_schema)?;
            crate::replay::process::run_source(
                &job,
                &crate::replay::application::ApplicationFactory {
                    binary_digest: job.source.binary_digest,
                    business,
                },
            )?;
        }
        "--replay-rollback" => {
            let job = crate::worker::read_config(&path, 2 * 1024 * 1024)?;
            crate::replay::rollback::run_with_factory(&job, |config, files| {
                qualified(registration, config, files)
            })?;
        }
        "--restore-probe" => {
            let job = crate::worker::read_config(&path, 2 * 1024 * 1024)?;
            crate::preflight::run_probe_with(&job, |config, files| {
                qualified(registration, config, files)
            })?;
        }
        "--bootstrap" => {
            let config: crate::bootstrap::Bootstrap =
                crate::worker::read_config(&path, 2 * 1024 * 1024)?;
            let binding = config.factory.as_ref().ok_or(Error::NotReady)?;
            binding.validate()?;
            if binding.id != registration.id() {
                return Err(Error::NotReady);
            }
            if let Some(path_job) = &config.preflight_job {
                let job = crate::worker::read_config(path_job, 2 * 1024 * 1024)?;
                let effective = std::fs::read(&path)?;
                let (strategy, completed) = crate::preflight::start_with_factory(
                    &job,
                    &config,
                    &effective,
                    |target, files| configure(registration, binding, target, files),
                )?;
                // SAFETY: the caller supplies the qualified supervisor contract.
                unsafe {
                    crate::worker::run_restored(config, strategy, completed)?;
                }
            } else {
                let target = target_identity(&config)?;
                let bytes = tickwire_release::files::read_object(
                    &binding.configuration_file,
                    binding.configuration,
                )?;
                let factory = configure_bytes(registration, binding, &target, &bytes)?;
                let strategy = factory.create()?;
                // SAFETY: the caller supplies the qualified supervisor contract.
                unsafe {
                    crate::worker::run(config, strategy)?;
                }
            }
        }
        _ => return Err(Error::Identity),
    }
    Ok(())
}

fn capture_entry<R: Registration>(
    registration: &R,
    config: &crate::capture::Bootstrap,
) -> Result<()> {
    if registration.id() != config.contract.factory {
        return Err(Error::Identity);
    }
    Ok(())
}

#[cfg(test)]
mod tests;
