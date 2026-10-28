use ring::signature::{Ed25519KeyPair, KeyPair};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use tickwire_config::{
    Candidate, Format, Limits,
    artifacts::{ArtifactCatalog, CatalogLimits, DependencyReport},
};
use tickwire_config_service::{
    deployment::{ContentValidated, Limits as DeploymentLimits, Sources},
    evidence::{Catalog, Claim, Issuer, Kind, Trust},
    qualification::{ReportClosure, qualification_basis},
};
pub const NOW: u64 = 1_800_000_000_000;
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
pub fn parse(v: &Value) -> Candidate {
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
fn policy(v: &Value) -> Value {
    let targets: Vec<_> = v["processes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| {
            let id = p["id"].as_str().unwrap();
            let role = p["role"].as_str().unwrap();
            let strategy = v["strategies"]
                .as_array()
                .unwrap()
                .iter()
                .find(|s| s["process_id"] == id);
            let accounts: Vec<_> = v["accounts"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|a| match role {
                    "strategy" => strategy.unwrap()["account_ids"]
                        .as_array()
                        .unwrap()
                        .contains(&a["id"]),
                    "exec" => a["exec_process_id"] == id || a["standby_exec_process_id"] == id,
                    "guardian" => a["guardian_process_ids"]
                        .as_array()
                        .unwrap()
                        .contains(&p["id"]),
                    _ => false,
                })
                .collect();
            let topics: Vec<_> = v["ipc"]["topics"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|t| match role {
                    "strategy" => strategy.unwrap()["topic_ids"]
                        .as_array()
                        .unwrap()
                        .contains(&t["id"]),
                    "md" => {
                        t["publisher_process_id"] == id || t["standby_publisher_process_id"] == id
                    }
                    _ => false,
                })
                .collect();
            let venues: Vec<_> = v["venues"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|venue| {
                    accounts.iter().any(|a| a["venue_id"] == venue["id"])
                        || topics.iter().any(|t| t["product"] == venue["product"])
                })
                .map(|v| v["id"].clone())
                .collect();
            let purposes: &[&str] = match role {
                "supervisor" => &["control"],
                "replicator" => &["replication"],
                "exec" => &["execution"],
                "guardian" => &["guardian"],
                "md" => &["market-data"],
                _ => &[],
            };
            json!({
                "process_id":id,
                "account_ids":accounts.iter().map(|a|a["id"].clone()).collect::<Vec<_>>(),
                "venue_ids":venues,
                "topic_ids":topics.iter().map(|t|t["id"].clone()).collect::<Vec<_>>(),
                "secret_purposes":purposes
            })
        })
        .collect();
    json!({"schema":1,"revision":1,"targets":targets})
}

pub struct Fixture {
    pub candidate: Candidate,
    pub artifacts: ArtifactCatalog,
    pub signatures: Catalog,
    pub retained_ref: String,
    pub credential_refs: Vec<String>,
}
impl Fixture {
    pub fn new(
        alter_candidate: impl FnOnce(&mut Value),
        alter_fact: impl FnMut(Kind, &mut Value),
    ) -> Self {
        Self::new_at(NOW, alter_candidate, alter_fact)
    }
    pub fn new_at(
        now: u64,
        alter_candidate: impl FnOnce(&mut Value),
        alter_fact: impl FnMut(Kind, &mut Value),
    ) -> Self {
        Self::new_at_with_lifetime(now, 10_000, alter_candidate, alter_fact)
    }
    pub fn new_at_with_lifetime(
        now: u64,
        lifetime_ms: u64,
        alter_candidate: impl FnOnce(&mut Value),
        mut alter_fact: impl FnMut(Kind, &mut Value),
    ) -> Self {
        let mut v = Candidate::parse(
            Format::Toml,
            include_bytes!("../../../../config/reference.toml"),
            Limits::default(),
        )
        .unwrap()
        .value()
        .clone();
        let mut artifacts = ArtifactCatalog::new(CatalogLimits::default()).unwrap();
        fn replace(v: &mut Value, a: &mut ArtifactCatalog, map: &mut BTreeMap<String, String>) {
            match v {
                Value::String(s) if s.starts_with("artifact://") => {
                    let old = s.clone();
                    *s = map
                        .entry(old.clone())
                        .or_insert_with(|| put(a, "dependency", old.as_bytes()))
                        .clone();
                }
                Value::Object(v) => v.values_mut().for_each(|v| replace(v, a, map)),
                Value::Array(v) => v.iter_mut().for_each(|v| replace(v, a, map)),
                _ => {}
            }
        }
        replace(&mut v, &mut artifacts, &mut BTreeMap::new());
        let policy_ref = put(
            &mut artifacts,
            "policy",
            &serde_json::to_vec(&policy(&v)).unwrap(),
        );
        v["local_config"]["projection_policy_ref"] = policy_ref.into();
        let parameter_schema = json!({"type":"object","required":["signal_window_events"],
            "properties":{"signal_window_events":{"type":"integer","minimum":1}}});
        let parameters_ref = put(
            &mut artifacts,
            "parameters",
            &serde_json::to_vec(&parameter_schema).unwrap(),
        );
        for s in v["strategies"].as_array_mut().unwrap() {
            s["parameter_schema_ref"] = parameters_ref.clone().into();
        }
        let mut sign_refs = vec![];
        for i in 0..v["accounts"].as_array().unwrap().len() {
            let a = v["accounts"][i].clone();
            let venue = v["venues"]
                .as_array()
                .unwrap()
                .iter()
                .find(|x| x["id"] == a["venue_id"])
                .unwrap();
            let identity = json!({"schema":1,"venue":venue["venue"],"environment":venue["environment"],
                "product":venue["product"],"exchange_account_id":format!("uid-{i}"),
                "funds_identity":format!("funds-{i}")});
            let identity_ref = put(
                &mut artifacts,
                &format!("identity-{i}"),
                &serde_json::to_vec(&identity).unwrap(),
            );
            let mut mode = json!({"schema":1,"cluster_id":v["identity"]["cluster_id"],
                "namespace_id":v["identity"]["namespace_id"],"account_id":a["id"],"venue_id":a["venue_id"],
                "environment":venue["environment"],"product":venue["product"],"account_identity_ref":identity_ref,
                "observed_exchange_account_id":identity["exchange_account_id"],"observed_funds_identity":identity["funds_identity"],
                "position_mode":a["position_mode"],"margin_mode":a["margin_mode"],"asset_mode":a["asset_mode"],
                "trading_enabled":true,"observed_unix_ms":now-1000});
            alter_fact(Kind::AccountMode, &mut mode);
            let mode_ref = put(
                &mut artifacts,
                &format!("mode-{i}"),
                &serde_json::to_vec(&mode).unwrap(),
            );
            sign_refs.push((Kind::AccountMode, mode_ref.clone()));
            v["accounts"][i]["account_identity_ref"] = identity_ref.into();
            v["accounts"][i]["mode_snapshot_ref"] = mode_ref.into();
        }
        alter_candidate(&mut v);
        // Normalize before computing the report basis, just as a real producer must.
        v = parse(&v).value().clone();
        let candidate = parse(&v);
        let basis = hash_text(independently_derived_basis(&candidate));
        assert_eq!(
            qualification_basis(&candidate).unwrap(),
            independently_derived_basis(&candidate)
        );
        let cluster = v["identity"]["cluster_id"].clone();
        let namespace = v["identity"]["namespace_id"].clone();
        let devices:Vec<_>=v["hosts"].as_array().unwrap().iter().map(|h|json!({
            "host_id":h["id"],"fingerprint":hash_text(Sha256::digest(h["id"].as_str().unwrap()).into())
        })).collect();
        let nonqualification: Vec<_> = DependencyReport::inspect(&candidate, &artifacts)
            .unwrap()
            .artifacts
            .into_iter()
            .filter(|a| a.uses.iter().any(|p| !p.starts_with("/qualification/")))
            .map(|a| a.reference)
            .collect();
        let mut references = BTreeMap::new();
        for (kind, field) in FIELDS {
            let mut body = match kind {
                Kind::HostInventory => {
                    json!({"schema":1,"cluster_id":cluster,"namespace_id":namespace,"hosts":inventory_hosts(&v)})
                }
                Kind::DeploymentManifest => {
                    json!({"schema":1,"cluster_id":cluster,"namespace_id":namespace,
                    "basis_hash":basis,"host_inventory_ref":references[&Kind::HostInventory],
                    "artifacts":nonqualification,"processes":v["processes"],"devices":devices})
                }
                Kind::RiskAcceptance => {
                    json!({"schema":1,"cluster_id":cluster,"namespace_id":namespace,
                    "basis_hash":basis,"accepted":true,"reports":references.iter().map(|(k,r)|json!({"kind":k,"artifact_ref":r})).collect::<Vec<_>>()})
                }
                _ => {
                    let raw = put(&mut artifacts, &format!("raw/{field}"), field.as_bytes());
                    json!({"schema":1,"cluster_id":cluster,"namespace_id":namespace,"basis_hash":basis,
                        "manifest_ref":references[&Kind::DeploymentManifest],"passed":true,"devices":devices,"evidence_refs":[raw]})
                }
            };
            alter_fact(kind, &mut body);
            let reference = put(
                &mut artifacts,
                &format!("reports/{field}"),
                &serde_json::to_vec(&body).unwrap(),
            );
            v["qualification"][field] = reference.clone().into();
            sign_refs.push((kind, reference.clone()));
            references.insert(kind, reference);
        }
        let candidate = parse(&v);
        let projections = tickwire_config::delivery::ProjectionSet::build(
            &candidate,
            &artifacts,
            tickwire_config::delivery::ProjectionLimits::default(),
        )
        .unwrap();
        let identities: BTreeSet<_> = projections
            .targets()
            .values()
            .map(|p| {
                p.value()["process"]["os_identity"]
                    .as_str()
                    .unwrap()
                    .to_owned()
            })
            .collect();
        let uids: BTreeMap<_, _> = identities
            .into_iter()
            .enumerate()
            .map(|(i, k)| (k, i as u32 + 1000))
            .collect();
        let mut docs: BTreeMap<String, Value> = BTreeMap::new();
        for (id, p) in projections.targets() {
            let pv = p.value();
            for b in pv["secret_bindings"].as_array().unwrap() {
                let r = b["ref"].as_str().unwrap();
                let doc=docs.entry(r.into()).or_insert_with(||json!({"schema":1,"cluster_id":cluster,"namespace_id":namespace,
                    "secret_ref":r,"secret_version":r.rsplit('/').next().unwrap(),"key_fingerprint":hash_text(Sha256::digest(r).into()),
                    "enabled":true,"observed_unix_ms":now-2000,"grants":[]}));
                let mut grant = json!({"process_id":id,"host_id":pv["process"]["host_id"],"os_identity":pv["process"]["os_identity"],
                    "os_uid":uids[pv["process"]["os_identity"].as_str().unwrap()],"purpose":b["purpose"]});
                if let Some(a) = b.get("account_id") {
                    grant["account_id"] = a.clone();
                }
                doc["grants"].as_array_mut().unwrap().push(grant);
            }
        }
        let mut credential_refs = vec![];
        for (i, mut doc) in docs.into_values().enumerate() {
            alter_fact(Kind::CredentialMetadata, &mut doc);
            let reference = put(
                &mut artifacts,
                &format!("credentials/{i}"),
                &serde_json::to_vec(&doc).unwrap(),
            );
            credential_refs.push(reference.clone());
            sign_refs.push((Kind::CredentialMetadata, reference));
        }
        let mut retained = json!({"schema":1,"cluster_id":cluster,"namespace_id":namespace,"observed_unix_ms":now-3000,
            "retained":{"schema":1,"cluster_id":cluster,"namespace_id":namespace,"revision":"17","allocations":[]}});
        alter_fact(Kind::RetainedResources, &mut retained);
        let retained_ref = put(
            &mut artifacts,
            "retained",
            &serde_json::to_vec(&retained).unwrap(),
        );
        sign_refs.push((Kind::RetainedResources, retained_ref.clone()));
        let key = Ed25519KeyPair::from_seed_unchecked(&[85; 32]).unwrap();
        let mut signatures = Catalog::default();
        for (kind, reference) in sign_refs {
            let claim = Claim {
                schema: 1,
                cluster_id: cluster.as_str().unwrap().into(),
                namespace_id: namespace.as_str().unwrap().into(),
                issuer_id: "test-source".into(),
                kind,
                artifact_ref: reference,
                issued_unix_ms: now - 500,
                expires_unix_ms: now.checked_add(lifetime_ms).unwrap(),
            };
            let signature = hex(key.sign(&claim.signing_bytes().unwrap()).as_ref());
            signatures
                .insert(&serde_json::to_vec(&json!({"claim":claim,"signature":signature})).unwrap())
                .unwrap();
        }
        Self {
            candidate,
            artifacts,
            signatures,
            retained_ref,
            credential_refs,
        }
    }
    pub fn bundle_bytes(&self) -> Vec<u8> {
        let artifacts: Vec<_> = self
            .artifacts
            .objects()
            .map(|(r, b)| json!({"reference":r,"byte_len":b.len()}))
            .collect();
        let evidence: Vec<Value> = self
            .signatures
            .envelopes()
            .map(|(_, _, b)| serde_json::from_slice(b).unwrap())
            .collect();
        let header = json!({"schema":1,"artifacts":artifacts,"evidence":evidence,"retained_ref":self.retained_ref,"credential_refs":self.credential_refs});
        let header = serde_json::to_vec(&header).unwrap();
        let mut bytes = b"TWDSRC01".to_vec();
        bytes.extend_from_slice(&(header.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&header);
        self.artifacts
            .objects()
            .for_each(|(_, b)| bytes.extend_from_slice(b));
        bytes
    }
    pub fn trust(&self, revision: u64) -> Trust {
        Trust::new(
            self.candidate.value()["identity"]["cluster_id"]
                .as_str()
                .unwrap()
                .into(),
            self.candidate.value()["identity"]["namespace_id"]
                .as_str()
                .unwrap()
                .into(),
            revision,
            vec![Issuer {
                id: "test-source".into(),
                public_key: Ed25519KeyPair::from_seed_unchecked(&[85; 32])
                    .unwrap()
                    .public_key()
                    .as_ref()
                    .try_into()
                    .unwrap(),
                kinds: FIELDS
                    .iter()
                    .map(|x| x.0)
                    .chain([
                        Kind::AccountMode,
                        Kind::CredentialMetadata,
                        Kind::RetainedResources,
                    ])
                    .collect(),
            }],
        )
        .unwrap()
    }
    pub fn sources(&self) -> Sources<'_> {
        Sources {
            artifacts: &self.artifacts,
            signatures: &self.signatures,
            retained_ref: &self.retained_ref,
            credential_refs: &self.credential_refs,
        }
    }
    pub fn check(&self, now: u64) -> tickwire_config_service::Result<ContentValidated> {
        ContentValidated::check(
            &self.candidate,
            self.sources(),
            &self.trust(1),
            now,
            DeploymentLimits::default(),
        )
    }
    pub fn check_reports(&self) -> tickwire_config_service::Result<ReportClosure> {
        ReportClosure::check(
            &self.candidate,
            &self.artifacts,
            &self.trust(1),
            &self.signatures,
            NOW,
        )
    }
}
