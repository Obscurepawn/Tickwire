use ring::signature::{Ed25519KeyPair, KeyPair};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use tickwire_config::{
    Candidate, Format, Limits,
    artifacts::{ArtifactCatalog, CatalogLimits},
    delivery::{ProjectionLimits, ProjectionSet},
};
use tickwire_config_service::{
    Error,
    credential_facts::{CredentialFacts, Limits as CredentialLimits},
    evidence::{Catalog, Claim, Issuer, Kind, Trust},
};
const NOW: u64 = 1_800_000_000_000;
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
fn base() -> Value {
    Candidate::parse(
        Format::Toml,
        include_bytes!("../../../config/reference.toml"),
        Limits::default(),
    )
    .unwrap()
    .value()
    .clone()
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
fn catalog() -> ArtifactCatalog {
    ArtifactCatalog::new(CatalogLimits::default()).unwrap()
}
fn pinned(mut value: Value, policy: Value, catalog: &mut ArtifactCatalog) -> Candidate {
    let bytes = serde_json::to_vec(&policy).unwrap();
    let hash: String = Sha256::digest(&bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    let reference = format!("artifact://fixture/config/projection-v1@sha256:{hash}");
    catalog.insert(&reference, &bytes).unwrap();
    value["local_config"]["projection_policy_ref"] = reference.into();
    value["local_config"]["projection_policy_revision"] = policy["revision"].clone();
    parse(&value)
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|v| format!("{v:02x}")).collect()
}
struct Fixture {
    candidate: Candidate,
    projections: ProjectionSet,
    artifacts: ArtifactCatalog,
    docs: Vec<Value>,
}
impl Fixture {
    fn new() -> Self {
        let v = base();
        let mut artifacts = catalog();
        let candidate = pinned(v.clone(), policy(&v), &mut artifacts);
        let projections =
            ProjectionSet::build(&candidate, &artifacts, ProjectionLimits::default()).unwrap();
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
            let v = p.value();
            for b in v["secret_bindings"].as_array().unwrap() {
                let r = b["ref"].as_str().unwrap();
                let d=docs.entry(r.into()).or_insert_with(||json!({"schema":1,"cluster_id":v["identity"]["cluster_id"],"namespace_id":v["identity"]["namespace_id"],"secret_ref":r,"secret_version":r.rsplit('/').next().unwrap(),"key_fingerprint":format!("sha256:{}",hex(&Sha256::digest(r))),"enabled":true,"observed_unix_ms":NOW-2000,"grants":[]}));
                let mut g = json!({"process_id":id,"host_id":v["process"]["host_id"],"os_identity":v["process"]["os_identity"],"os_uid":uids[v["process"]["os_identity"].as_str().unwrap()],"purpose":b["purpose"]});
                if let Some(a) = b.get("account_id") {
                    g["account_id"] = a.clone();
                }
                d["grants"].as_array_mut().unwrap().push(g);
            }
        }
        Self {
            candidate,
            projections,
            artifacts,
            docs: docs.into_values().collect(),
        }
    }
    fn check(&mut self, now: u64, limit: u64) -> Result<CredentialFacts, Error> {
        let key = Ed25519KeyPair::from_seed_unchecked(&[77; 32]).unwrap();
        let trust = Trust::new(
            self.candidate.value()["identity"]["cluster_id"]
                .as_str()
                .unwrap()
                .into(),
            self.candidate.value()["identity"]["namespace_id"]
                .as_str()
                .unwrap()
                .into(),
            1,
            vec![Issuer {
                id: "credential-owner".into(),
                public_key: key.public_key().as_ref().try_into().unwrap(),
                kinds: BTreeSet::from([Kind::CredentialMetadata]),
            }],
        )
        .unwrap();
        let mut signatures = Catalog::default();
        let mut refs = vec![];
        for (i, d) in self.docs.iter().enumerate() {
            let bytes = serde_json::to_vec(d).unwrap();
            let reference = format!(
                "artifact://fixture/credential-{i}@sha256:{}",
                hex(&Sha256::digest(&bytes))
            );
            self.artifacts.insert(&reference, &bytes).unwrap();
            let claim = Claim {
                schema: 1,
                cluster_id: self.candidate.value()["identity"]["cluster_id"]
                    .as_str()
                    .unwrap()
                    .into(),
                namespace_id: self.candidate.value()["identity"]["namespace_id"]
                    .as_str()
                    .unwrap()
                    .into(),
                issuer_id: "credential-owner".into(),
                kind: Kind::CredentialMetadata,
                artifact_ref: reference.clone(),
                issued_unix_ms: NOW - 1000,
                expires_unix_ms: NOW + 60000,
            };
            let sig = hex(key.sign(&claim.signing_bytes().unwrap()).as_ref());
            signatures
                .insert(&serde_json::to_vec(&json!({"claim":claim,"signature":sig})).unwrap())
                .unwrap();
            refs.push(reference);
        }
        CredentialFacts::check(
            &self.projections,
            &self.artifacts,
            &trust,
            &signatures,
            &refs,
            now,
            CredentialLimits { max_age_ms: limit },
        )
    }
}
#[test]
fn actual_complete_projection_uses_receive_exact_signed_authorization() {
    let mut f = Fixture::new();
    let expected = f
        .projections
        .targets()
        .values()
        .map(|p| p.value()["secret_bindings"].as_array().unwrap().len())
        .sum::<usize>();
    let result = f.check(NOW, 5000).unwrap();
    assert_eq!(result.candidate_hash(), f.candidate.hash());
    assert_eq!(result.uses().len(), expected);
    assert!(expected > 0);
    assert_eq!(result.expires_unix_ms(), NOW + 3000);
    assert_eq!(result.evidence().len(), f.docs.len());
    assert!(
        result
            .uses()
            .iter()
            .all(|u| !u.process_id.starts_with("strategy") && u.os_uid >= 1000)
    );
}
#[test]
fn actual_use_cannot_be_replaced_by_caller_selected_process_host_account_or_purpose() {
    for field in [
        "process_id",
        "host_id",
        "os_identity",
        "account_id",
        "purpose",
    ] {
        let mut f = Fixture::new();
        let d = f
            .docs
            .iter_mut()
            .find(|d| d["grants"][0]["purpose"] == "execution")
            .unwrap();
        d["grants"][0][field] = "other".into();
        assert!(
            matches!(
                f.check(NOW, 5000),
                Err(Error::Authorization)
                    | Err(Error::Identity)
                    | Err(Error::Protocol)
                    | Err(Error::Conflict)
            ),
            "{field}"
        );
    }
}
#[test]
fn exact_secret_directory_rejects_missing_unknown_and_duplicate_entries() {
    for kind in 0..3 {
        let mut f = Fixture::new();
        match kind {
            0 => {
                f.docs.pop();
            }
            1 => {
                f.docs[0]["secret_ref"] = "secret://fixture/unknown/1".into();
            }
            _ => {
                f.docs.push(f.docs[0].clone());
            }
        }
        assert!(matches!(
            f.check(NOW, 5000),
            Err(Error::Conflict) | Err(Error::NotFound) | Err(Error::Identity)
        ));
    }
}
#[test]
fn fixed_version_enabled_and_recent_observation_are_required() {
    for kind in 0..6 {
        let mut f = Fixture::new();
        match kind {
            0 => f.docs[0]["enabled"] = false.into(),
            1 => f.docs[0]["secret_version"] = "2".into(),
            2 => f.docs[0]["secret_version"] = "01".into(),
            3 => f.docs[0]["observed_unix_ms"] = (NOW - 5000).into(),
            4 => f.docs[0]["observed_unix_ms"] = NOW.into(),
            _ => f.docs[0]["observed_unix_ms"] = 0.into(),
        }
        assert!(
            matches!(
                f.check(NOW, 5000),
                Err(Error::Authentication) | Err(Error::Identity) | Err(Error::Bounds)
            ),
            "case {kind}"
        );
    }
    assert!(Fixture::new().check(NOW, 0).is_err());
    assert!(Fixture::new().check(NOW, 60001).is_err());
    assert!(Fixture::new().check(NOW + 60000, 60000).is_err());
}
#[test]
fn underlying_execution_key_cannot_be_aliased_as_guardian() {
    let mut f = Fixture::new();
    let key = f
        .docs
        .iter()
        .find(|d| d["grants"][0]["purpose"] == "execution")
        .unwrap()["key_fingerprint"]
        .clone();
    f.docs
        .iter_mut()
        .find(|d| d["grants"][0]["purpose"] == "guardian")
        .unwrap()["key_fingerprint"] = key;
    assert!(matches!(f.check(NOW, 5000), Err(Error::Conflict)));
}
#[test]
fn all_grants_participate_in_key_isolation_but_unused_grants_do_not_expand_uses() {
    let mut f = Fixture::new();
    let d = f
        .docs
        .iter_mut()
        .find(|d| d["grants"][0]["purpose"] == "execution")
        .unwrap();
    let mut extra = d["grants"][0].clone();
    extra["process_id"] = "unused".into();
    d["grants"].as_array_mut().unwrap().push(extra);
    let result = f.check(NOW, 5000).unwrap();
    assert!(!result.uses().iter().any(|u| u.process_id == "unused"));
    let d = f
        .docs
        .iter_mut()
        .find(|d| d["grants"][0]["purpose"] == "execution")
        .unwrap();
    d["grants"].as_array_mut().unwrap().last_mut().unwrap()["purpose"] = "guardian".into();
    assert!(matches!(f.check(NOW, 5000), Err(Error::Conflict)));
}
#[test]
fn uid_mapping_and_duplicate_authorization_are_unambiguous() {
    let mut f = Fixture::new();
    let g = f.docs[0]["grants"][0].clone();
    f.docs[0]["grants"].as_array_mut().unwrap().push(g);
    assert!(matches!(f.check(NOW, 5000), Err(Error::Conflict)));
    let mut f = Fixture::new();
    let d = f
        .docs
        .iter_mut()
        .find(|d| d["grants"][0]["purpose"] == "execution")
        .unwrap();
    let mut g = d["grants"][0].clone();
    g["os_uid"] = 9999.into();
    d["grants"].as_array_mut().unwrap().push(g);
    assert!(matches!(f.check(NOW, 5000), Err(Error::Conflict)));
}
#[test]
fn raw_secret_unknown_fields_wrong_scope_and_zero_fingerprint_are_rejected() {
    for kind in 0..4 {
        let mut f = Fixture::new();
        match kind {
            0 => f.docs[0]["api_secret"] = "forbidden".into(),
            1 => f.docs[0]["cluster_id"] = "other".into(),
            2 => f.docs[0]["key_fingerprint"] = format!("sha256:{}", "0".repeat(64)).into(),
            _ => f.docs[0]["grants"][0]["extra"] = 1.into(),
        }
        assert!(matches!(
            f.check(NOW, 5000),
            Err(Error::Protocol) | Err(Error::Identity) | Err(Error::Digest)
        ));
    }
}
