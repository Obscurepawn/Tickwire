//! One candidate and one content source closure; no runtime or commit authority.
use crate::{
    Error, Hash, Result, account_facts, credential_facts,
    evidence::{Catalog, Kind, Trust},
    qualification::ReportClosure,
    resource_facts,
};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use tickwire_config::{
    Candidate, Format,
    artifacts::{ArtifactCatalog, DependencyReport, ParametersValidated, validate_parameters},
    delivery::{ProjectionLimits, ProjectionSet},
};

#[derive(Clone, Copy, Default)]
pub struct Limits {
    pub accounts: account_facts::Limits,
    pub credentials: credential_facts::Limits,
    pub resources: resource_facts::Limits,
    pub projections: ProjectionLimits,
}
pub struct Sources<'a> {
    pub artifacts: &'a ArtifactCatalog,
    pub signatures: &'a Catalog,
    pub retained_ref: &'a str,
    pub credential_refs: &'a [String],
}
pub struct ContentValidated {
    candidate_hash: Hash,
    sources_digest: Hash,
    checked_unix_ms: u64,
    expires_unix_ms: u64,
    parameters: ParametersValidated,
    projections: ProjectionSet,
    reports: ReportClosure,
    accounts: account_facts::AccountModes,
    credentials: credential_facts::CredentialFacts,
    resources: resource_facts::RetainedCapacity,
}
impl ContentValidated {
    pub fn check(
        candidate: &Candidate,
        sources: Sources<'_>,
        trust: &Trust,
        now_unix_ms: u64,
        limits: Limits,
    ) -> Result<Self> {
        let dependencies = DependencyReport::inspect(candidate, sources.artifacts)
            .map_err(Error::Configuration)?;
        if dependencies.missing().next().is_some() {
            return Err(Error::NotFound);
        }
        let parameters =
            validate_parameters(candidate, sources.artifacts).map_err(Error::Configuration)?;
        let projections = ProjectionSet::build(candidate, sources.artifacts, limits.projections)
            .map_err(Error::Configuration)?;
        let reports = ReportClosure::check(
            candidate,
            sources.artifacts,
            trust,
            sources.signatures,
            now_unix_ms,
        )?;
        let accounts = account_facts::AccountModes::check(
            candidate,
            sources.artifacts,
            trust,
            sources.signatures,
            now_unix_ms,
            limits.accounts,
        )?;
        let credentials = credential_facts::CredentialFacts::check(
            &projections,
            sources.artifacts,
            trust,
            sources.signatures,
            sources.credential_refs,
            now_unix_ms,
            limits.credentials,
        )?;
        let resources = resource_facts::RetainedCapacity::check(
            candidate,
            sources.artifacts,
            trust,
            sources.signatures,
            sources.retained_ref,
            now_unix_ms,
            limits.resources,
        )?;
        let expires_unix_ms = reports
            .expires_unix_ms()
            .min(accounts.expires_unix_ms())
            .min(credentials.expires_unix_ms())
            .min(resources.expires_unix_ms());
        let mut references: BTreeSet<_> = dependencies
            .artifacts
            .iter()
            .map(|a| a.reference.clone())
            .collect();
        references.extend(sources.credential_refs.iter().cloned());
        references.insert(sources.retained_ref.to_owned());
        // Raw qualification evidence is reachable through the checked report graph.
        for kind in [
            Kind::ProtocolEntitlements,
            Kind::StoragePowerLoss,
            Kind::NetworkIsolation,
        ] {
            let reference = &reports.reports()[&kind].claim().artifact_ref;
            let body: serde_json::Value =
                serde_json::from_slice(sources.artifacts.get(reference).ok_or(Error::NotFound)?)
                    .map_err(|_| Error::Protocol)?;
            for raw in body["evidence_refs"].as_array().ok_or(Error::Protocol)? {
                references.insert(raw.as_str().ok_or(Error::Protocol)?.to_owned());
            }
        }
        let objects: Vec<_> = references.iter().map(|reference| {
            let bytes=sources.artifacts.get(reference).ok_or(Error::NotFound)?;
            Ok(json!({"reference":reference,"bytes":bytes.len(),"sha256":hash_text(Sha256::digest(bytes).into())}))
        }).collect::<Result<_>>()?;
        let mut evidence: Vec<_> = reports
            .reports()
            .values()
            .chain(accounts.accounts().values().map(|a| a.evidence()))
            .chain(credentials.evidence())
            .chain(std::iter::once(resources.evidence()))
            .collect();
        evidence.sort_by(|a, b| {
            (a.claim().kind, &a.claim().artifact_ref)
                .cmp(&(b.claim().kind, &b.claim().artifact_ref))
        });
        let signatures: Vec<_> = evidence
            .iter()
            .map(|e| {
                json!({
                    "kind":e.claim().kind,"reference":e.claim().artifact_ref,
                    "envelope_sha256":hash_text(Sha256::digest(e.envelope()).into())
                })
            })
            .collect();
        let description = json!({
            "schema":1,"candidate_hash":hash_text(candidate.hash()),"trust_hash":hash_text(trust.digest()),
            "objects":objects,"signatures":signatures,"retained_ref":sources.retained_ref,
            "credential_refs":sources.credential_refs.iter().collect::<BTreeSet<_>>(),
            "limits":{"account_max_age_ms":limits.accounts.max_age_ms,"credential_max_age_ms":limits.credentials.max_age_ms,
                "resource_max_age_ms":limits.resources.max_age_ms,"projection_max_targets":limits.projections.max_targets,
                "projection_max_total_bytes":limits.projections.max_total_bytes}
        });
        let canonical = Candidate::parse(
            Format::Json,
            &serde_json::to_vec(&description).map_err(|_| Error::Protocol)?,
            tickwire_config::Limits {
                max_bytes: 16 * 1024 * 1024,
                max_depth: 8,
            },
        )
        .map_err(Error::Configuration)?;
        let mut digest = Sha256::new();
        digest.update(b"tickwire.config.deployment-content.v1\0");
        digest.update(canonical.canonical_bytes());
        Ok(Self {
            candidate_hash: candidate.hash(),
            sources_digest: digest.finalize().into(),
            checked_unix_ms: now_unix_ms,
            expires_unix_ms,
            parameters,
            projections,
            reports,
            accounts,
            credentials,
            resources,
        })
    }
    pub fn candidate_hash(&self) -> Hash {
        self.candidate_hash
    }
    pub fn sources_digest(&self) -> Hash {
        self.sources_digest
    }
    pub fn checked_unix_ms(&self) -> u64 {
        self.checked_unix_ms
    }
    pub fn expires_unix_ms(&self) -> u64 {
        self.expires_unix_ms
    }
    pub fn parameters(&self) -> &ParametersValidated {
        &self.parameters
    }
    pub fn projections(&self) -> &ProjectionSet {
        &self.projections
    }
    pub fn reports(&self) -> &ReportClosure {
        &self.reports
    }
    pub fn accounts(&self) -> &account_facts::AccountModes {
        &self.accounts
    }
    pub fn credentials(&self) -> &credential_facts::CredentialFacts {
        &self.credentials
    }
    pub fn resources(&self) -> &resource_facts::RetainedCapacity {
        &self.resources
    }
}
fn hash_text(hash: Hash) -> String {
    format!(
        "sha256:{}",
        hash.iter().map(|b| format!("{b:02x}")).collect::<String>()
    )
}
