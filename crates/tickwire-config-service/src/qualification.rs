//! Exact report graph for a candidate; runtime qualification is separate.
use crate::{
    Error, Hash, Result,
    evidence::{Catalog, Kind, Trust, VerifiedEvidence},
};
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use tickwire_config::{
    Candidate, Format, Limits,
    artifacts::{ArtifactCatalog, DependencyReport, reference_hash},
    capacity::HostInventory,
};

const FIELDS: [(Kind, &str); 6] = [
    (Kind::HostInventory, "host_inventory_ref"),
    (Kind::DeploymentManifest, "deployment_manifest_ref"),
    (Kind::ProtocolEntitlements, "protocol_entitlements_ref"),
    (Kind::StoragePowerLoss, "storage_power_loss_report_ref"),
    (Kind::NetworkIsolation, "network_isolation_report_ref"),
    (Kind::RiskAcceptance, "risk_acceptance_ref"),
];

pub fn qualification_basis(candidate: &Candidate) -> Result<Hash> {
    candidate.validate_static().map_err(Error::Configuration)?;
    let normalized = candidate
        .normalized_configuration(Limits::default())
        .map_err(Error::Configuration)?;
    if normalized.hash() != candidate.hash() {
        return Err(Error::Protocol);
    }
    let mut value = candidate.value().clone();
    let qualification = value["qualification"]
        .as_object_mut()
        .ok_or(Error::Protocol)?;
    for (_, field) in FIELDS {
        qualification.remove(field).ok_or(Error::Protocol)?;
    }
    let canonical = Candidate::parse(
        Format::Json,
        &serde_json::to_vec(&value).map_err(|_| Error::Protocol)?,
        Limits::default(),
    )
    .map_err(Error::Configuration)?;
    let mut hash = Sha256::new();
    hash.update(b"tickwire.config.deployment-basis.v1\0");
    hash.update(canonical.canonical_bytes());
    Ok(hash.finalize().into())
}
pub struct ReportClosure {
    candidate_hash: Hash,
    basis_hash: Hash,
    reports: BTreeMap<Kind, VerifiedEvidence>,
    expires_unix_ms: u64,
}
impl ReportClosure {
    pub fn check(
        candidate: &Candidate,
        artifacts: &ArtifactCatalog,
        trust: &Trust,
        signatures: &Catalog,
        now_unix_ms: u64,
    ) -> Result<Self> {
        let basis_hash = qualification_basis(candidate)?;
        let value = candidate.value();
        let cluster = string(&value["identity"], "cluster_id")?;
        let namespace = string(&value["identity"], "namespace_id")?;
        let basis_text = hash_text(basis_hash);
        let mut references = BTreeMap::new();
        let mut report_refs = BTreeSet::new();
        let mut report_hashes = BTreeSet::new();
        let mut reports = BTreeMap::new();
        let mut expires_unix_ms = u64::MAX;
        for (kind, field) in FIELDS {
            let reference = string(&value["qualification"], field)?;
            if !report_refs.insert(reference) {
                return Err(Error::Conflict);
            }
            let envelope = signatures.get(kind, reference).ok_or(Error::NotFound)?;
            let verified = trust.verify(envelope, artifacts, kind, reference, now_unix_ms)?;
            if verified.claim().cluster_id != cluster || verified.claim().namespace_id != namespace
            {
                return Err(Error::Identity);
            }
            expires_unix_ms = expires_unix_ms.min(verified.claim().expires_unix_ms);
            report_hashes.insert(verified.artifact_hash());
            references.insert(kind, reference);
            reports.insert(kind, verified);
        }
        let dependencies =
            DependencyReport::inspect(candidate, artifacts).map_err(Error::Configuration)?;
        if dependencies.missing().next().is_some() {
            return Err(Error::NotFound);
        }
        let qualification_paths: BTreeSet<_> = FIELDS
            .iter()
            .map(|(_, field)| format!("/qualification/{field}"))
            .collect();
        let required_artifacts: BTreeSet<_> = dependencies
            .artifacts
            .iter()
            .filter(|dependency| {
                dependency
                    .uses
                    .iter()
                    .any(|path| !qualification_paths.contains(path))
            })
            .map(|dependency| dependency.reference.as_str())
            .collect();
        let hosts: BTreeSet<_> = value["hosts"]
            .as_array()
            .ok_or(Error::Protocol)?
            .iter()
            .map(|host| string(host, "id"))
            .collect::<Result<_>>()?;
        let inventory_ref = references[&Kind::HostInventory];
        let inventory = HostInventory::decode(artifacts.get(inventory_ref).ok_or(Error::NotFound)?)
            .map_err(Error::Configuration)?;
        if inventory.cluster_id() != cluster
            || inventory.namespace_id() != namespace
            || inventory.host_ids().collect::<BTreeSet<_>>() != hosts
        {
            return Err(Error::Identity);
        }
        let manifest_ref = references[&Kind::DeploymentManifest];
        let manifest: Manifest = decode(artifacts, manifest_ref)?;
        scope(
            manifest.schema,
            &manifest.cluster_id,
            &manifest.namespace_id,
            &manifest.basis_hash,
            cluster,
            namespace,
            &basis_text,
        )?;
        if manifest.host_inventory_ref != inventory_ref
            || manifest.processes != *value["processes"].as_array().ok_or(Error::Protocol)?
            || unique_references(&manifest.artifacts)? != required_artifacts
        {
            return Err(Error::Conflict);
        }
        let devices = device_map(&manifest.devices, &hosts)?;
        for kind in [
            Kind::ProtocolEntitlements,
            Kind::StoragePowerLoss,
            Kind::NetworkIsolation,
        ] {
            let report: BoundReport = decode(artifacts, references[&kind])?;
            scope(
                report.schema,
                &report.cluster_id,
                &report.namespace_id,
                &report.basis_hash,
                cluster,
                namespace,
                &basis_text,
            )?;
            if !report.passed
                || report.manifest_ref != manifest_ref
                || device_map(&report.devices, &hosts)? != devices
            {
                return Err(Error::Conflict);
            }
            if report.evidence_refs.is_empty() || report.evidence_refs.len() > 256 {
                return Err(Error::Bounds);
            }
            for reference in unique_references(&report.evidence_refs)? {
                let hash = reference_hash(reference).map_err(Error::Configuration)?;
                if report_hashes.contains(&hash) {
                    return Err(Error::Conflict);
                }
                if artifacts.get(reference).is_none() {
                    return Err(Error::NotFound);
                }
            }
        }
        let risk: RiskAcceptance = decode(artifacts, references[&Kind::RiskAcceptance])?;
        scope(
            risk.schema,
            &risk.cluster_id,
            &risk.namespace_id,
            &risk.basis_hash,
            cluster,
            namespace,
            &basis_text,
        )?;
        if !risk.accepted || risk.reports.len() != 5 {
            return Err(Error::Conflict);
        }
        let mut accepted_reports = BTreeMap::new();
        for report in &risk.reports {
            if accepted_reports
                .insert(report.kind, report.artifact_ref.as_str())
                .is_some()
            {
                return Err(Error::Conflict);
            }
        }
        references.remove(&Kind::RiskAcceptance);
        if accepted_reports != references {
            return Err(Error::Conflict);
        }
        Ok(Self {
            candidate_hash: candidate.hash(),
            basis_hash,
            reports,
            expires_unix_ms,
        })
    }
    pub fn candidate_hash(&self) -> Hash {
        self.candidate_hash
    }
    pub fn basis_hash(&self) -> Hash {
        self.basis_hash
    }
    pub fn reports(&self) -> &BTreeMap<Kind, VerifiedEvidence> {
        &self.reports
    }
    pub fn expires_unix_ms(&self) -> u64 {
        self.expires_unix_ms
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Device {
    host_id: String,
    fingerprint: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    schema: u32,
    cluster_id: String,
    namespace_id: String,
    basis_hash: String,
    host_inventory_ref: String,
    artifacts: Vec<String>,
    processes: Vec<Value>,
    devices: Vec<Device>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BoundReport {
    schema: u32,
    cluster_id: String,
    namespace_id: String,
    basis_hash: String,
    manifest_ref: String,
    passed: bool,
    devices: Vec<Device>,
    evidence_refs: Vec<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RiskAcceptance {
    schema: u32,
    cluster_id: String,
    namespace_id: String,
    basis_hash: String,
    accepted: bool,
    reports: Vec<AcceptedReport>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AcceptedReport {
    kind: Kind,
    artifact_ref: String,
}
fn decode<T: serde::de::DeserializeOwned>(
    artifacts: &ArtifactCatalog,
    reference: &str,
) -> Result<T> {
    let candidate = Candidate::parse(
        Format::Json,
        artifacts.get(reference).ok_or(Error::NotFound)?,
        Limits {
            max_bytes: 4 * 1024 * 1024,
            max_depth: 16,
        },
    )
    .map_err(Error::Configuration)?;
    serde_json::from_value(candidate.value().clone()).map_err(|_| Error::Protocol)
}
fn string<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    value[field].as_str().ok_or(Error::Protocol)
}
fn scope(
    schema: u32,
    cluster: &str,
    namespace: &str,
    basis: &str,
    expected_cluster: &str,
    expected_namespace: &str,
    expected_basis: &str,
) -> Result<()> {
    if schema != 1
        || cluster != expected_cluster
        || namespace != expected_namespace
        || basis != expected_basis
    {
        Err(Error::Identity)
    } else {
        Ok(())
    }
}
fn unique_references(references: &[String]) -> Result<BTreeSet<&str>> {
    if references.len() > 4096 {
        return Err(Error::Bounds);
    }
    let mut unique = BTreeSet::new();
    for reference in references {
        reference_hash(reference).map_err(Error::Configuration)?;
        if !unique.insert(reference.as_str()) {
            return Err(Error::Conflict);
        }
    }
    Ok(unique)
}
fn device_map<'a>(
    devices: &'a [Device],
    hosts: &BTreeSet<&str>,
) -> Result<BTreeMap<&'a str, &'a str>> {
    if devices.len() != hosts.len() {
        return Err(Error::Identity);
    }
    let mut by_host = BTreeMap::new();
    for device in devices {
        let hash = device
            .fingerprint
            .strip_prefix("sha256:")
            .ok_or(Error::Digest)?;
        if hash.len() != 64
            || hash.bytes().all(|b| b == b'0')
            || !hash
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            || !hosts.contains(device.host_id.as_str())
            || by_host
                .insert(device.host_id.as_str(), device.fingerprint.as_str())
                .is_some()
        {
            return Err(Error::Identity);
        }
    }
    Ok(by_host)
}
fn hash_text(hash: Hash) -> String {
    let hex: String = hash.iter().map(|byte| format!("{byte:02x}")).collect();
    format!("sha256:{hex}")
}
