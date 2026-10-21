use ring::signature::{Ed25519KeyPair, KeyPair};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use tickwire_config::{
    Candidate, Format, Limits,
    artifacts::{ArtifactCatalog, CatalogLimits, DependencyReport},
};
use tickwire_config_service::{
    evidence::{Catalog, Claim, Issuer, Kind, Trust},
    qualification::{ReportClosure, qualification_basis},
};
const NOW: u64 = 1_800_000_000_000;
const FIELDS: [(Kind, &str); 6] = [
    (Kind::HostInventory, "host_inventory_ref"),
    (Kind::DeploymentManifest, "deployment_manifest_ref"),
    (Kind::ProtocolEntitlements, "protocol_entitlements_ref"),
    (Kind::StoragePowerLoss, "storage_power_loss_report_ref"),
    (Kind::NetworkIsolation, "network_isolation_report_ref"),
    (Kind::RiskAcceptance, "risk_acceptance_ref"),
];
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
fn hash_text(hash: [u8; 32]) -> String {
    format!("sha256:{}", hex(&hash))
}
fn parse(v: &Value) -> Candidate {
    Candidate::parse(
        Format::Json,
        &serde_json::to_vec(v).unwrap(),
        Limits::default(),
    )
    .unwrap()
    .normalized_configuration(Limits::default())
    .unwrap()
}
fn independently_derived_basis(candidate: &Candidate) -> [u8; 32] {
    let mut v = candidate.value().clone();
    for (_, field) in FIELDS {
        v["qualification"].as_object_mut().unwrap().remove(field);
    }
    let core = Candidate::parse(
        Format::Json,
        &serde_json::to_vec(&v).unwrap(),
        Limits::default(),
    )
    .unwrap();
    let mut hash = Sha256::new();
    hash.update(b"tickwire.config.deployment-basis.v1\0");
    hash.update(core.canonical_bytes());
    hash.finalize().into()
}
fn put(catalog: &mut ArtifactCatalog, name: &str, bytes: &[u8]) -> String {
    let reference = format!(
        "artifact://fixture/{name}@sha256:{}",
        hex(&Sha256::digest(bytes))
    );
    catalog.insert(&reference, bytes).unwrap();
    reference
}
fn inventory_hosts(v: &Value) -> Value {
    let hosts: Vec<_> = v["hosts"].as_array().unwrap().iter().map(|h| {
        let count = h["physical_cores"].as_u64().unwrap();
        let cpus: Vec<_> = (0..count).map(|cpu| {
            let node = v["processes"].as_array().unwrap().iter().find(|p| {
                p["host_id"] == h["id"] && p["cpus"].as_array().unwrap().contains(&cpu.into())
            }).map(|p| p["numa_node"].as_u64().unwrap())
                .unwrap_or(if cpu == count-1 {h["numa_nodes"].as_u64().unwrap()-1} else {0});
            json!({"cpu_id":cpu,"package_id":0,"core_id":cpu,"numa_node":node})
        }).collect();
        json!({
            "id":h["id"],"failure_domain":h["failure_domain"],
            "memory_bytes":h["memory_bytes"].as_u64().unwrap()+1073741824u64,
            "system_reserved_bytes":1073741824u64,"shm_capacity_bytes":h["shm_budget_bytes"],"cpus":cpus
        })
    }).collect();
    hosts.into()
}
fn claim(candidate: &Candidate, kind: Kind, reference: &str) -> Claim {
    Claim {
        schema: 1,
        cluster_id: candidate.value()["identity"]["cluster_id"]
            .as_str()
            .unwrap()
            .into(),
        namespace_id: candidate.value()["identity"]["namespace_id"]
            .as_str()
            .unwrap()
            .into(),
        issuer_id: "qualifier".into(),
        kind,
        artifact_ref: reference.into(),
        issued_unix_ms: NOW - 1000,
        expires_unix_ms: NOW + 1000,
    }
}
fn signed(claim: &Claim) -> Vec<u8> {
    let key = Ed25519KeyPair::from_seed_unchecked(&[55; 32]).unwrap();
    let signature = hex(key.sign(&claim.signing_bytes().unwrap()).as_ref());
    serde_json::to_vec(&json!({"claim":claim,"signature":signature})).unwrap()
}
fn trust(candidate: &Candidate, cluster: Option<&str>) -> Trust {
    Trust::new(
        cluster
            .unwrap_or(
                candidate.value()["identity"]["cluster_id"]
                    .as_str()
                    .unwrap(),
            )
            .into(),
        candidate.value()["identity"]["namespace_id"]
            .as_str()
            .unwrap()
            .into(),
        1,
        vec![Issuer {
            id: "qualifier".into(),
            public_key: Ed25519KeyPair::from_seed_unchecked(&[55; 32])
                .unwrap()
                .public_key()
                .as_ref()
                .try_into()
                .unwrap(),
            kinds: FIELDS.iter().map(|x| x.0).collect(),
        }],
    )
    .unwrap()
}
struct Fixture {
    candidate: Candidate,
    artifacts: ArtifactCatalog,
    signatures: Catalog,
    bodies: BTreeMap<Kind, Vec<u8>>,
    references: BTreeMap<Kind, String>,
}
impl Fixture {
    fn check(&self) -> tickwire_config_service::Result<ReportClosure> {
        ReportClosure::check(
            &self.candidate,
            &self.artifacts,
            &trust(&self.candidate, None),
            &self.signatures,
            NOW,
        )
    }
    fn new(mut mutate: impl FnMut(Kind, &mut Value)) -> Self {
        let reference = Candidate::parse(
            Format::Toml,
            include_bytes!("../../../config/reference.toml"),
            Limits::default(),
        )
        .unwrap();
        let mut v = reference.value().clone();
        let mut artifacts = ArtifactCatalog::new(CatalogLimits::default()).unwrap();
        // Replace the design's placeholder references with actual pinned bytes.
        fn replace(
            v: &mut Value,
            artifacts: &mut ArtifactCatalog,
            ids: &mut BTreeMap<String, String>,
        ) {
            match v {
                Value::String(s) if s.starts_with("artifact://") => {
                    let old = s.clone();
                    *s = ids
                        .entry(old.clone())
                        .or_insert_with(|| {
                            put(
                                artifacts,
                                "dependency",
                                &serde_json::to_vec(&json!({"source":old})).unwrap(),
                            )
                        })
                        .clone();
                }
                Value::Object(map) => {
                    for value in map.values_mut() {
                        replace(value, artifacts, ids);
                    }
                }
                Value::Array(array) => {
                    for value in array {
                        replace(value, artifacts, ids);
                    }
                }
                _ => {}
            }
        }
        replace(&mut v, &mut artifacts, &mut BTreeMap::new());
        let candidate = parse(&v);
        candidate.validate_static().unwrap();
        let basis = hash_text(independently_derived_basis(&candidate));
        let cluster = v["identity"]["cluster_id"].clone();
        let namespace = v["identity"]["namespace_id"].clone();
        let devices: Vec<_> = v["hosts"].as_array().unwrap().iter().map(|host| json!({
            "host_id":host["id"],
            "fingerprint":hash_text(Sha256::digest(host["id"].as_str().unwrap().as_bytes()).into())
        })).collect();
        let nonqualification: Vec<_> = DependencyReport::inspect(&candidate, &artifacts)
            .unwrap()
            .artifacts
            .into_iter()
            .filter(|a| a.uses.iter().any(|p| !p.starts_with("/qualification/")))
            .map(|a| a.reference)
            .collect();
        let mut bodies = BTreeMap::new();
        let mut references = BTreeMap::new();
        for (kind, field) in FIELDS {
            let mut body = match kind {
                Kind::HostInventory => json!({
                    "schema":1,"cluster_id":cluster,"namespace_id":namespace,
                    "hosts":inventory_hosts(&v)
                }),
                Kind::DeploymentManifest => json!({
                    "schema":1,"cluster_id":cluster,"namespace_id":namespace,"basis_hash":basis,
                    "host_inventory_ref":references[&Kind::HostInventory],"artifacts":nonqualification,
                    "processes":v["processes"],"devices":devices,
                }),
                Kind::RiskAcceptance => json!({
                    "schema":1,"cluster_id":cluster,"namespace_id":namespace,"basis_hash":basis,
                    "accepted":true,"reports":references.iter().map(|(k,r)| json!({"kind":k,"artifact_ref":r})).collect::<Vec<_>>()
                }),
                _ => {
                    let raw = put(
                        &mut artifacts,
                        &format!("raw/{field}"),
                        format!("test qualification evidence for {field}").as_bytes(),
                    );
                    json!({
                        "schema":1,"cluster_id":cluster,"namespace_id":namespace,"basis_hash":basis,
                        "manifest_ref":references[&Kind::DeploymentManifest],
                        "passed":true,"devices":devices,"evidence_refs":[raw],
                    })
                }
            };
            mutate(kind, &mut body);
            let bytes = serde_json::to_vec(&body).unwrap();
            let reference = put(&mut artifacts, &format!("reports/{field}"), &bytes);
            v["qualification"][field] = reference.clone().into();
            references.insert(kind, reference);
            bodies.insert(kind, bytes);
        }
        let candidate = parse(&v);
        let mut signatures = Catalog::default();
        for (kind, reference) in &references {
            signatures
                .insert(&signed(&claim(&candidate, *kind, reference)))
                .unwrap();
        }
        Self {
            candidate,
            artifacts,
            signatures,
            bodies,
            references,
        }
    }
}
#[test]
fn complete_signed_report_graph_binds_full_candidate_basis_and_each_report() {
    let f = Fixture::new(|_, _| {});
    let result = f.check().unwrap();
    assert_eq!(result.candidate_hash(), f.candidate.hash());
    assert_eq!(
        result.basis_hash(),
        independently_derived_basis(&f.candidate)
    );
    assert_eq!(
        qualification_basis(&f.candidate).unwrap(),
        result.basis_hash()
    );
    assert_eq!(result.reports().len(), 6);
    assert_eq!(result.expires_unix_ms(), NOW + 1000);
    for (kind, reference) in &f.references {
        assert_eq!(result.reports()[kind].claim().artifact_ref, *reference);
    }
}
#[test]
fn basis_ignores_only_six_report_references_and_retains_business_and_slo_content() {
    let f = Fixture::new(|_, _| {});
    let original = qualification_basis(&f.candidate).unwrap();
    for (_, field) in FIELDS {
        let mut v = f.candidate.value().clone();
        v["qualification"][field] =
            format!("artifact://other/report@sha256:{}", "1".repeat(64)).into();
        let altered = parse(&v);
        assert_eq!(qualification_basis(&altered).unwrap(), original);
        assert_ne!(altered.hash(), f.candidate.hash());
    }
    for change in 0..3 {
        let mut v = f.candidate.value().clone();
        match change {
            0 => v["qualification"]["full_send_p99_us"] = 301.into(),
            1 => v["control"]["plan_ttl_ms"] = 31000.into(),
            _ => {
                v["processes"][0]["artifact_ref"] =
                    format!("artifact://other/binary@sha256:{}", "2".repeat(64)).into()
            }
        }
        let altered = parse(&v);
        assert_ne!(qualification_basis(&altered).unwrap(), original);
        assert!(
            ReportClosure::check(
                &altered,
                &f.artifacts,
                &trust(&altered, None),
                &f.signatures,
                NOW
            )
            .is_err()
        );
    }
}
#[test]
fn omitted_dependencies_processes_hosts_and_failed_or_different_device_reports_are_rejected() {
    for bad in 0..7 {
        let f = Fixture::new(|kind, body| match (bad, kind) {
            (0, Kind::DeploymentManifest) => {
                body["artifacts"].as_array_mut().unwrap().pop();
            }
            (1, Kind::DeploymentManifest) => {
                body["processes"].as_array_mut().unwrap().pop();
            }
            (2, Kind::DeploymentManifest) => {
                body["devices"].as_array_mut().unwrap().pop();
            }
            (3, Kind::StoragePowerLoss) => body["passed"] = false.into(),
            (4, Kind::NetworkIsolation) => {
                body["devices"][0]["fingerprint"] = hash_text([9; 32]).into()
            }
            (5, Kind::ProtocolEntitlements) => body["evidence_refs"] = json!([]),
            (6, Kind::HostInventory) => {
                body["hosts"].as_array_mut().unwrap().pop();
            }
            _ => {}
        });
        assert!(f.check().is_err(), "invalid report {bad}");
    }
}
#[test]
fn replacing_a_passed_report_cannot_reuse_previous_risk_acceptance() {
    let original = Fixture::new(|_, _| {});
    original.check().unwrap();
    let mut changed = Fixture::new(|kind, body| {
        if kind == Kind::StoragePowerLoss {
            // A distinct, valid new report. Both raw objects exist.
            body["evidence_refs"]
                .as_array_mut()
                .unwrap()
                .push(Value::String(format!(
                    "artifact://fixture/raw/protocol_entitlements_ref@sha256:{}",
                    hex(&Sha256::digest(
                        b"test qualification evidence for protocol_entitlements_ref"
                    ))
                )));
        }
    });
    changed.check().unwrap();
    let reference = &original.references[&Kind::RiskAcceptance];
    changed
        .artifacts
        .insert(reference, &original.bodies[&Kind::RiskAcceptance])
        .unwrap();
    changed
        .signatures
        .insert(&signed(&claim(
            &changed.candidate,
            Kind::RiskAcceptance,
            reference,
        )))
        .unwrap();
    let mut v = changed.candidate.value().clone();
    v["qualification"]["risk_acceptance_ref"] = reference.clone().into();
    changed.candidate = parse(&v);
    assert!(changed.check().is_err());
}
#[test]
fn signatures_from_a_different_cluster_do_not_authenticate_the_report_body_cluster() {
    let f = Fixture::new(|_, _| {});
    let mut signatures = Catalog::default();
    for (kind, reference) in &f.references {
        let mut claim = claim(&f.candidate, *kind, reference);
        claim.cluster_id = "other-cluster".into();
        signatures.insert(&signed(&claim)).unwrap();
    }
    assert!(
        ReportClosure::check(
            &f.candidate,
            &f.artifacts,
            &trust(&f.candidate, Some("other-cluster")),
            &signatures,
            NOW
        )
        .is_err()
    );
}
#[test]
fn report_objects_are_strict_and_raw_evidence_must_exist_without_report_cycles() {
    for bad in 0..5 {
        let f = Fixture::new(|kind, body| match (bad, kind) {
            (0, Kind::DeploymentManifest) => body["unknown"] = true.into(),
            (1, Kind::RiskAcceptance) => {
                body["reports"].as_array_mut().unwrap().pop();
            }
            (2, Kind::NetworkIsolation) => {
                body["evidence_refs"][0] =
                    format!("artifact://missing/raw@sha256:{}", "3".repeat(64)).into()
            }
            (3, Kind::StoragePowerLoss) => body["evidence_refs"][0] = body["manifest_ref"].clone(),
            (4, Kind::RiskAcceptance) => {
                let item = body["reports"][0].clone();
                body["reports"].as_array_mut().unwrap().push(item);
            }
            _ => {}
        });
        assert!(f.check().is_err(), "invalid graph {bad}");
    }
}
#[test]
fn evidence_catalog_retry_conflict_and_fixed_capacity_are_enforced_before_verification() {
    let f = Fixture::new(|_, _| {});
    let claim = claim(
        &f.candidate,
        Kind::HostInventory,
        &f.references[&Kind::HostInventory],
    );
    let bytes = signed(&claim);
    let mut catalog = Catalog::default();
    catalog.insert(&bytes).unwrap();
    catalog.insert(&bytes).unwrap();
    let mut other = claim.clone();
    other.expires_unix_ms += 1;
    assert!(catalog.insert(&signed(&other)).is_err());
    let original: Value = serde_json::from_slice(&bytes).unwrap();
    for n in 1..4096 {
        let mut value = original.clone();
        value["claim"]["artifact_ref"] =
            format!("artifact://fixture/reports-{n}@sha256:{}", "4".repeat(64)).into();
        catalog
            .insert(&serde_json::to_vec(&value).unwrap())
            .unwrap();
    }
    catalog.insert(&bytes).unwrap();
    let mut value = original;
    value["claim"]["artifact_ref"] =
        format!("artifact://fixture/overflow@sha256:{}", "4".repeat(64)).into();
    assert!(
        catalog
            .insert(&serde_json::to_vec(&value).unwrap())
            .is_err()
    );
    assert_eq!(FIELDS.iter().map(|x| x.0).collect::<BTreeSet<_>>().len(), 6);
}

#[test]
fn a_report_content_alias_is_not_independent_raw_evidence() {
    let mut alias = String::new();
    let mut f = Fixture::new(|kind, body| {
        if kind == Kind::StoragePowerLoss {
            let digest = body["manifest_ref"]
                .as_str()
                .unwrap()
                .split_once("@sha256:")
                .unwrap()
                .1;
            alias = format!("artifact://aliased/raw@sha256:{digest}");
            body["evidence_refs"] = json!([alias]);
        }
    });
    f.artifacts
        .insert(&alias, &f.bodies[&Kind::DeploymentManifest])
        .unwrap();
    assert!(f.check().is_err());
}

#[test]
fn catalog_presence_does_not_replace_signature_time_or_current_trust_validation() {
    let f = Fixture::new(|_, _| {});
    for bad in 0..3 {
        let mut signatures = Catalog::default();
        for (kind, reference) in &f.references {
            let mut value: Value =
                serde_json::from_slice(&signed(&claim(&f.candidate, *kind, reference))).unwrap();
            if bad == 0 && *kind == Kind::DeploymentManifest {
                value["signature"] = "0".repeat(128).into();
            }
            signatures
                .insert(&serde_json::to_vec(&value).unwrap())
                .unwrap();
        }
        let current_trust = if bad == 2 {
            Trust::new(
                f.candidate.value()["identity"]["cluster_id"]
                    .as_str()
                    .unwrap()
                    .into(),
                f.candidate.value()["identity"]["namespace_id"]
                    .as_str()
                    .unwrap()
                    .into(),
                2,
                vec![Issuer {
                    id: "qualifier".into(),
                    public_key: Ed25519KeyPair::from_seed_unchecked(&[56; 32])
                        .unwrap()
                        .public_key()
                        .as_ref()
                        .try_into()
                        .unwrap(),
                    kinds: FIELDS.iter().map(|x| x.0).collect(),
                }],
            )
            .unwrap()
        } else {
            trust(&f.candidate, None)
        };
        assert!(
            ReportClosure::check(
                &f.candidate,
                &f.artifacts,
                &current_trust,
                &signatures,
                if bad == 1 { NOW + 1000 } else { NOW }
            )
            .is_err(),
            "invalid authority {bad}"
        );
    }
}
