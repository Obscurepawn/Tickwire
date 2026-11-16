//! Stateless strategy parameter validation, SHA-256 `FactoryBinding` verification, and safe
//! `Registration` worker startup (`0` runtime parameter patches or replay state).
#![forbid(unsafe_code)]

use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use tickwire_ipc::ALL_STRATS;

use crate::{
    Error, Result, Strategy,
    bootstrap::{ArtifactDigest, Bootstrap, MAX_BOOTSTRAP_BYTES, MAX_PARAM_BYTES},
};

const SEQUENCE_LIMIT: u64 = 1 << 63;

/// Immutable configuration payload paired with its positive monotonically ordered revision.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VersionedBytes {
    pub revision: u64,
    pub bytes: Vec<u8>,
}

/// Verified identity of the target strategy worker instance receiving a parameter document.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TargetIdentity {
    pub logical_strategy_id: u64,
    pub strat_id: u8,
    pub instance_id: [u8; 16],
    pub generation: u64,
    pub account_ref: u8,
}

impl TargetIdentity {
    pub fn validate(&self) -> Result<()> {
        if self.logical_strategy_id == 0
            || self.strat_id == ALL_STRATS
            || self.instance_id == [0; 16]
            || self.generation == 0
            || self.generation >= SEQUENCE_LIMIT
        {
            return Err(Error::Identity);
        }
        Ok(())
    }

    pub fn from_bootstrap(config: &Bootstrap) -> Result<Self> {
        if config.demo_enabled
            || config.demo_command_file.is_some()
            || config.demo_minimum_bid_ticks != 0
        {
            return Err(Error::Identity);
        }
        let target = Self {
            logical_strategy_id: config.logical_strategy_id,
            strat_id: config.strat_id,
            instance_id: config.instance_id,
            generation: config.generation,
            account_ref: config.account_ref,
        };
        target.validate()?;
        Ok(target)
    }
}

/// Cryptographically bound parameter descriptor inside [`Bootstrap`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FactoryBinding {
    #[serde(alias = "registration_id")]
    pub id: String,
    pub state_schema: u64,
    pub configuration_file: PathBuf,
    pub configuration: ArtifactDigest,
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
            || self.state_schema == 0
            || !self.configuration_file.is_absolute()
            || self.revision == 0
            || self.revision >= SEQUENCE_LIMIT
        {
            return Err(Error::Identity);
        }
        self.configuration.validate(MAX_PARAM_BYTES)
    }
}

/// Typed factory constructing a fresh stateless [`Strategy`] from its validated parameters.
pub trait UserFactory: Clone {
    type Running: Strategy;

    fn state_schema(&self) -> u64;

    fn configuration(&self, target: &TargetIdentity) -> Result<VersionedBytes>;

    fn create(&self) -> Result<Self::Running>;
}

/// Static registration implemented by a user strategy binary (`tickwire-strategy-example`).
pub trait Registration {
    type Factory: UserFactory;

    fn id(&self) -> &'static str;

    fn configure(&self, target: &TargetIdentity, config: &VersionedBytes) -> Result<Self::Factory>;
}

/// Validated strategy factory paired with its exact verified parameter bytes.
#[derive(Clone, Debug)]
pub struct Configured<F> {
    factory: F,
    parameters: VersionedBytes,
}

impl<F> Configured<F> {
    #[inline]
    pub fn parameters(&self) -> &VersionedBytes {
        &self.parameters
    }

    #[inline]
    pub fn factory(&self) -> &F {
        &self.factory
    }
}

impl<F: UserFactory> Configured<F> {
    pub fn create(&self) -> Result<F::Running> {
        self.factory.create()
    }
}

/// Verify `binding`, `target`, and `bytes` (`(len, sha256)`) *before* invoking user `Registration::configure`.
pub fn configure_bytes<R: Registration>(
    registration: &R,
    binding: &FactoryBinding,
    target: &TargetIdentity,
    bytes: &[u8],
) -> Result<Configured<R::Factory>> {
    binding.validate()?;
    target.validate()?;
    if registration.id() != binding.id {
        return Err(Error::Identity);
    }
    binding.configuration.verify(bytes, MAX_PARAM_BYTES)?;
    let parameters = VersionedBytes {
        revision: binding.revision,
        bytes: bytes.to_vec(),
    };
    let factory = registration.configure(target, &parameters)?;
    if factory.state_schema() != binding.state_schema
        || factory.configuration(target)? != parameters
    {
        return Err(Error::Identity);
    }
    Ok(Configured {
        factory,
        parameters,
    })
}

/// Read `&binding.configuration_file` safely (`read_trusted_bytes`) and validate via [`configure_bytes`].
pub fn configure_from_file<R: Registration>(
    registration: &R,
    binding: &FactoryBinding,
    target: &TargetIdentity,
) -> Result<Configured<R::Factory>> {
    binding.validate()?;
    target.validate()?;
    if registration.id() != binding.id {
        return Err(Error::Identity);
    }
    let bytes = crate::worker::read_trusted_bytes(&binding.configuration_file, MAX_PARAM_BYTES)?;
    configure_bytes(registration, binding, target, &bytes)
}

/// Validate `--bootstrap <PATH>`, CPU isolation, optional `Catalog`, and `FactoryBinding` before
/// launching the stateless worker runtime.
pub fn run<R: Registration>(
    registration: &R,
    args: impl IntoIterator<Item = std::ffi::OsString>,
) -> Result<()> {
    let mut args = args.into_iter();
    let mode = args.next().ok_or(Error::Identity)?;
    let path = PathBuf::from(args.next().ok_or(Error::Identity)?);
    if args.next().is_some() || mode.to_str() != Some("--bootstrap") {
        return Err(Error::Identity);
    }
    let config: Bootstrap = crate::worker::read_config(&path, MAX_BOOTSTRAP_BYTES)?;
    config.validate()?;
    crate::catalog::validate_startup(&config)?;
    let binding = config.factory.as_ref().ok_or(Error::NotReady)?;
    let target = TargetIdentity::from_bootstrap(&config)?;
    let configured = configure_from_file(registration, binding, &target)?;
    let strategy = configured.create()?;
    crate::worker::run_qualified(config, strategy)
}

#[cfg(test)]
mod tests;
