//! A binding of approved process-launch fields to an actual owned child.
//! This is not an application receipt for the complete global projection.
#![forbid(unsafe_code)]
use crate::{Result, invalid};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tickwire_config::projection_object::ObjectBinding;
use tickwire_strategy_sdk::bootstrap::Bootstrap;
use tickwire_supervisor::LiveResourceObservation;

const MAX_OBJECT_BYTES: usize = 4 * 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Binding {
    pub revision: u64,
    pub global_hash: [u8; 32],
    pub policy_revision: u64,
    pub process_id: String,
    pub projection_hash: [u8; 32],
    pub object_hash: [u8; 32],
}
impl From<&ObjectBinding> for Binding {
    fn from(value: &ObjectBinding) -> Self {
        Self {
            revision: value.revision,
            global_hash: value.global_hash,
            policy_revision: value.policy_revision,
            process_id: value.process_id.clone(),
            projection_hash: value.projection_hash,
            object_hash: value.object_hash,
        }
    }
}
impl From<&Binding> for ObjectBinding {
    fn from(value: &Binding) -> Self {
        Self {
            revision: value.revision,
            global_hash: value.global_hash,
            policy_revision: value.policy_revision,
            process_id: value.process_id.clone(),
            projection_hash: value.projection_hash,
            object_hash: value.object_hash,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Scope {
    pub cluster_id: String,
    pub namespace_id: [u8; 16],
    pub host_id: String,
    pub process_id: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OsIdentity {
    pub name: String,
    pub uid: u32,
    pub gid: u32,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub object_file: PathBuf,
    pub object: Binding,
    pub scope: Scope,
    pub os_identities: Vec<OsIdentity>,
}

/// Historical startup evidence, not continuously fresh configuration authority.
#[derive(Debug)]
pub struct LaunchBindingProof {
    object: ObjectBinding,
    scope: Scope,
    observation: LiveResourceObservation,
}
impl LaunchBindingProof {
    pub fn object(&self) -> &ObjectBinding {
        &self.object
    }
    pub fn scope(&self) -> &Scope {
        &self.scope
    }
    pub fn observation(&self) -> &LiveResourceObservation {
        &self.observation
    }
}
pub(super) struct Prepared {
    object: ObjectBinding,
    scope: Scope,
    identity: tickwire_control::Identity,
    uid: u32,
    gid: u32,
    limits: tickwire_supervisor::ResourceLimits,
    prepared_at: std::time::Instant,
}
pub(super) fn prepare(
    config: &super::HostConfig,
    bootstrap: &Bootstrap,
) -> Result<Option<Prepared>> {
    let Some(profile) = &config.projection_launch else {
        return Ok(None);
    };
    let object = read_projection(
        config,
        bootstrap,
        &profile.object_file,
        &profile.object,
        &profile.scope,
    )?;
    let process = &object.payload()["process"];
    let scope = &profile.scope;
    let identity = bootstrap.market.local.identity();
    check(
        !profile.os_identities.is_empty() && profile.os_identities.len() <= 128,
        "OS identity mapping bounds",
    )?;
    let mut names = std::collections::BTreeSet::new();
    for mapping in &profile.os_identities {
        check(
            valid_name(&mapping.name) && names.insert(&mapping.name),
            "OS identity mapping name",
        )?;
    }
    let mapping = profile
        .os_identities
        .iter()
        .find(|v| process["os_identity"] == v.name)
        .ok_or_else(|| invalid("projection launch unknown OS identity"))?;
    let current = tickwire_supervisor::current_process_identity()
        .map_err(|e| invalid(&format!("projection launch current process identity: {e}")))?;
    check(
        mapping.uid == config.uid
            && mapping.gid == config.gid
            && mapping.uid == current.uid
            && mapping.gid == current.gid,
        "OS identity mismatch",
    )?;
    let resources = config
        .resources
        .as_ref()
        .ok_or_else(|| invalid("projection launch resources required"))?;
    resources
        .validate()
        .map_err(|e| invalid(&format!("projection launch resources: {e}")))?;
    let mut cpus = process["cpus"]
        .as_array()
        .ok_or_else(|| invalid("projection launch CPUs"))?
        .iter()
        .map(|v| {
            v.as_u64()
                .and_then(|v| u32::try_from(v).ok())
                .ok_or_else(|| invalid("projection launch CPU value"))
        })
        .collect::<Result<Vec<_>>>()?;
    cpus.sort_unstable();
    check(
        !cpus.is_empty() && cpus.windows(2).all(|w| w[0] < w[1]),
        "duplicate CPUs",
    )?;
    let worker = &resources.worker;
    check(
        cpus == worker.cpus
            && process["numa_node"].as_u64() == Some(u64::from(worker.numa_node))
            && process["memory_limit_bytes"].as_u64() == Some(worker.memory_max),
        "worker resources mismatch",
    )?;
    Ok(Some(Prepared {
        object: object.binding().clone(),
        scope: scope.clone(),
        identity,
        uid: config.uid,
        gid: config.gid,
        limits: tickwire_supervisor::ResourceLimits {
            cpus,
            numa_node: worker.numa_node,
            memory_max: worker.memory_max,
        },
        prepared_at: std::time::Instant::now(),
    }))
}
pub(super) fn read_projection(
    config: &super::HostConfig,
    bootstrap: &Bootstrap,
    object_file: &std::path::Path,
    binding: &Binding,
    scope: &Scope,
) -> Result<tickwire_config::projection_object::ProjectionObject> {
    check(
        config.capture.is_none()
            && config.rotation_directory.is_none()
            && bootstrap.preflight_job.is_none()
            && bootstrap.factory.is_some()
            && !bootstrap.demo_enabled
            && bootstrap.demo_command_file.is_none()
            && bootstrap.demo_minimum_bid_ticks == 0,
        "ordinary registered startup required",
    )?;
    bootstrap
        .factory
        .as_ref()
        .ok_or_else(|| invalid("projection launch factory"))?
        .validate()
        .map_err(|e| invalid(&format!("projection launch factory: {e}")))?;
    check(
        binding.global_hash != [0; 32]
            && binding.projection_hash != [0; 32]
            && binding.object_hash != [0; 32],
        "zero object binding hash",
    )?;
    let object = read_object(object_file)?;
    let object = tickwire_config::projection_object::ProjectionObject::decode(
        &object,
        &binding.into(),
        MAX_OBJECT_BYTES,
    )
    .map_err(|e| invalid(&format!("projection launch object binding: {e:?}")))?;
    let payload = object.payload();
    let process = &payload["process"];
    let strategy = &payload["strategy"];
    check(
        payload["identity"]["cluster_id"] == scope.cluster_id
            && payload["identity"]["namespace_id"] == namespace(scope.namespace_id)
            && payload["host"]["id"] == scope.host_id
            && process["host_id"] == scope.host_id
            && process["id"] == scope.process_id
            && strategy["process_id"] == scope.process_id
            && object.binding().process_id == scope.process_id,
        "scope mismatch",
    )?;
    check(
        process["role"] == "strategy"
            && process["restart_policy"] == "never"
            && strategy["trust_profile"] == "qualified_sdk"
            && process["activation"] == if config.shadow { "shadow" } else { "active" }
            && strategy["role"] == if config.shadow { "Shadow" } else { "Active" },
        "unsupported profile",
    )?;
    let identity = bootstrap.market.local.identity();
    // Match the control Identity domain before creating directories or children.
    // The role and trust profile are further narrowed to this startup profile.
    check(
        identity.instance != [0; 16]
            && (1..(1u64 << 63)).contains(&identity.generation)
            && (1..(1u64 << 63)).contains(&identity.logical_strategy)
            && identity.host_boot != [0; 16]
            && identity.namespace != [0; 16]
            && identity.build_hash != [0; 32]
            && identity.qualification_hash != [0; 32]
            && identity.manifest_hash != [0; 32]
            && (1..=2).contains(&identity.arch),
        "invalid local channel identity",
    )?;
    check(
        identity.role == 3
            && identity.trust_profile == 1
            && identity.namespace == scope.namespace_id
            && strategy["logical_strategy_id"].as_u64() == Some(identity.logical_strategy)
            && identity == bootstrap.intent.local.identity()
            && identity == bootstrap.report.local.identity()
            && bootstrap
                .external_market
                .as_ref()
                .is_none_or(|v| identity == v.endpoint.local.identity()),
        "local channel identity mismatch",
    )?;
    let artifact = process["artifact_ref"]
        .as_str()
        .ok_or_else(|| invalid("projection launch artifact"))?;
    let artifact = tickwire_config::artifacts::reference_hash(artifact)
        .map_err(|e| invalid(&format!("projection launch artifact: {e:?}")))?;
    check(
        artifact == identity.build_hash,
        "artifact identity mismatch",
    )?;
    Ok(object)
}

impl Prepared {
    pub(super) fn observe(
        self,
        observation: LiveResourceObservation,
    ) -> Result<LaunchBindingProof> {
        let process = observation.process();
        let resources = observation.resources();
        check(
            process.identity() == &self.identity
                && process.process().uid == self.uid
                && process.process().gid == self.gid
                && process.process().pid != 0
                && process.host_boot() == self.identity.host_boot
                && process.start_ticks() != 0
                && process.image_hash() == self.identity.build_hash
                && process.image_bytes() != 0
                && process.connected_epoch().is_some_and(|v| v != 0)
                && process.sampled_at() >= self.prepared_at
                && resources.sampled_at() >= self.prepared_at
                && resources.pid() == process.process().pid
                && resources.limits() == &self.limits,
            "actual process/resource observation mismatch",
        )?;
        Ok(LaunchBindingProof {
            object: self.object,
            scope: self.scope,
            observation,
        })
    }
}
fn check(valid: bool, reason: &str) -> Result<()> {
    if valid {
        Ok(())
    } else {
        Err(invalid(&format!("projection launch {reason}")))
    }
}
fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 63
        && name.as_bytes()[0].is_ascii_lowercase()
        && name
            .bytes()
            .all(|v| v.is_ascii_lowercase() || v.is_ascii_digit() || v == b'-')
}
fn namespace(bytes: [u8; 16]) -> String {
    let mut value = String::with_capacity(36);
    use std::fmt::Write;
    for (i, byte) in bytes.into_iter().enumerate() {
        if matches!(i, 4 | 6 | 8 | 10) {
            value.push('-');
        }
        write!(&mut value, "{byte:02x}").expect("writing UUID into String");
    }
    value
}
pub(super) fn read_object(path: &std::path::Path) -> Result<Vec<u8>> {
    #[cfg(target_os = "linux")]
    {
        use std::{fs::OpenOptions, io::Read, os::unix::fs::OpenOptionsExt};
        check(path.is_absolute(), "object file requires absolute path")?;
        let read = || -> std::io::Result<Vec<u8>> {
            let file = OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC)
                .open(path)?;
            let metadata = file.metadata()?;
            if !metadata.is_file() || metadata.len() > MAX_OBJECT_BYTES as u64 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "bounded regular file required",
                ));
            }
            let mut bytes = Vec::new();
            file.take((MAX_OBJECT_BYTES + 1) as u64)
                .read_to_end(&mut bytes)?;
            if bytes.len() > MAX_OBJECT_BYTES {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "object exceeds 4 MiB",
                ));
            }
            Ok(bytes)
        };
        read().map_err(|e| invalid(&format!("projection launch object file: {e}")))
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = path;
        Err(invalid("projection launch requires Linux"))
    }
}
#[cfg(all(test, target_os = "linux"))]
#[path = "profile/tests.rs"]
mod tests;
