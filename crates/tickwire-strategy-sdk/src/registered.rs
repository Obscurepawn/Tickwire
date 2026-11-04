//! Launch a registered strategy with the configuration bound to its worker profile.
use crate::restore::RestoreFactory;
use crate::{Error, Result, Strategy, restore};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tickwire_release::{Role, files::LoadedFiles, replay::VersionedBytes};

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
pub trait UserFactory: restore::RestoreFactory + Clone {
    fn create(&self) -> restore::Result<Self::Running>;
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
}
#[derive(Clone)]
pub struct Configured<F> {
    factory: F,
    parameters: VersionedBytes,
}
/// Verify the configuration path, digest and target before calling the factory.
pub fn configure<R: Registration>(
    registration: &R,
    binding: &FactoryBinding,
    target: &restore::Target,
    files: &LoadedFiles,
) -> Result<Configured<R::Factory>> {
    if let Some(bytes) = files.bytes(&Role::Bootstrap) {
        crate::catalog::reject_lifecycle_bytes(bytes)?;
    }
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
    if factory.configuration(target)? != parameters {
        return Err(Error::Identity);
    }
    Ok(Configured {
        factory,
        parameters,
    })
}

impl<F: UserFactory> Configured<F> {
    pub fn create(&self) -> Result<F::Running> {
        let strategy = self.factory.create()?;
        self.factory.validate_checkpoint(&strategy.checkpoint()?)?;
        let registration = strategy.replay_registration()?.ok_or(Error::NotReady)?;
        if registration.schema != self.factory.state_schema()
            || registration.config != self.parameters
        {
            return Err(Error::Identity);
        }
        Ok(strategy)
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
/// Run a statically registered strategy or replay its recorded inputs.
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
        "--source-replay" => {
            let job: crate::replay::process::SourceJob =
                crate::worker::read_config(&path, 2 * 1024 * 1024)?;
            let catalog = crate::replay::process::resolve_catalog(&job)?;
            let business = registration.source(job.source.state_schema)?;
            crate::replay::process::run_source(
                &job,
                &crate::replay::application::ApplicationFactory {
                    binary_digest: job.source.binary_digest,
                    business,
                }
                .with_catalog(catalog),
            )?;
        }
        "--bootstrap" => {
            let config: crate::bootstrap::Bootstrap =
                crate::worker::read_config(&path, crate::bootstrap::MAX_BOOTSTRAP_BYTES)?;
            crate::catalog::validate_startup(&config)?;
            let binding = config.factory.as_ref().ok_or(Error::NotReady)?;
            binding.validate()?;
            if binding.id != registration.id() {
                return Err(Error::NotReady);
            }
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
        _ => return Err(Error::Identity),
    }
    Ok(())
}

#[cfg(test)]
mod tests;
