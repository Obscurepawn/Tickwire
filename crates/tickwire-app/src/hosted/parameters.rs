//! Approved parameter content and actual registered worker consumption.
use super::{profile, startup};
use crate::{Result, invalid};
use serde::{Deserialize, Serialize};
use std::{path::PathBuf, sync::Arc};
use tickwire_release::Object;
use tickwire_strategy_host::{FreshObservation, ObservationRequest};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub object_file: PathBuf,
    pub object: profile::Binding,
    pub scope: profile::Scope,
    pub schema_file: PathBuf,
    pub timeout_ns: u64,
}

/// Historical consumption at a real, fresh checkpoint; no global Applied authority.
pub struct ParametersProof {
    object: profile::Binding,
    scope: profile::Scope,
    schema_hash: [u8; 32],
    parameter_hash: [u8; 32],
    private_revision: u64,
    private_object: Object,
    observation: Arc<FreshObservation>,
}
impl ParametersProof {
    pub fn object(&self) -> &profile::Binding {
        &self.object
    }
    pub fn scope(&self) -> &profile::Scope {
        &self.scope
    }
    pub fn schema_hash(&self) -> [u8; 32] {
        self.schema_hash
    }
    pub fn parameter_hash(&self) -> [u8; 32] {
        self.parameter_hash
    }
    pub fn private_revision(&self) -> u64 {
        self.private_revision
    }
    pub fn private_object(&self) -> Object {
        self.private_object
    }
    pub fn observation(&self) -> &FreshObservation {
        &self.observation
    }
}

pub(super) struct Prepared {
    pub(super) binding: startup::Binding,
    schema_hash: [u8; 32],
    parameter_hash: [u8; 32],
    private_revision: u64,
    private_object: Object,
}
pub(super) fn prepare(
    config: &super::HostConfig,
    bootstrap: &tickwire_strategy_sdk::bootstrap::Bootstrap,
) -> Result<Option<Prepared>> {
    let Some(input) = &config.projection_parameters else {
        return Ok(None);
    };
    let binding = startup::Binding::new(
        config,
        bootstrap,
        &input.object,
        &input.scope,
        input.timeout_ns,
    )?;
    let projection = profile::read_projection(
        config,
        bootstrap,
        &input.object_file,
        &input.object,
        &input.scope,
    )?;
    let schema_hash = tickwire_config::artifacts::reference_hash(
        projection.payload()["strategy"]["parameter_schema_ref"]
            .as_str()
            .ok_or_else(|| invalid("parameter schema reference"))?,
    )
    .map_err(|e| invalid(&format!("parameter schema reference: {e:?}")))?;
    let schema = profile::read_object(&input.schema_file)?;
    let factory = bootstrap
        .factory
        .as_ref()
        .ok_or_else(|| invalid("parameter factory"))?;
    check(factory.revision < 1 << 63, "private revision bounds")?;
    let raw = profile::read_object(&factory.configuration_file)?;
    factory.configuration.verify(&raw, 2 * 1024 * 1024)?;
    let value = tickwire_config::artifacts::validate_parameter_document(&schema, schema_hash, &raw)
        .map_err(|e| invalid(&format!("parameter document: {e:?}")))?;
    check(
        value.value() == &projection.payload()["strategy"]["params"],
        "factory/projection values differ",
    )?;
    Ok(Some(Prepared {
        binding,
        schema_hash,
        parameter_hash: value.hash(),
        private_revision: factory.revision,
        private_object: factory.configuration,
    }))
}
impl Prepared {
    pub(super) fn observe(
        self,
        request: &ObservationRequest,
        observed: Arc<FreshObservation>,
    ) -> Result<ParametersProof> {
        self.binding.verify(request, &observed)?;
        let actual = observed
            .configuration()
            .ok_or_else(|| invalid("worker has no current merged configuration"))?;
        check(
            actual.revision() == self.private_revision && actual.object() == self.private_object,
            "actual worker configuration binding",
        )?;
        Ok(ParametersProof {
            object: self.binding.object,
            scope: self.binding.scope,
            schema_hash: self.schema_hash,
            parameter_hash: self.parameter_hash,
            private_revision: self.private_revision,
            private_object: self.private_object,
            observation: observed,
        })
    }
}
fn check(valid: bool, reason: &str) -> Result<()> {
    if valid {
        Ok(())
    } else {
        Err(invalid(&format!("projection parameters {reason}")))
    }
}
