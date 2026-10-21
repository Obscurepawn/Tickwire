use ring::signature::{Ed25519KeyPair, KeyPair};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tickwire_config::{
    Candidate, Format, Limits,
    artifacts::{ArtifactCatalog, CatalogLimits},
};
use tickwire_config_service::{
    evidence::{Catalog, Claim, Issuer, Kind, Trust},
    resource_facts::{Limits as ObservationLimits, RetainedCapacity},
};
const NOW: u64 = 1_800_000_000_000;
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
fn put(artifacts: &mut ArtifactCatalog, name: &str, value: &Value) -> String {
    let bytes = serde_json::to_vec(value).unwrap();
    let reference = format!(
        "artifact://fixture/{name}@sha256:{}",
        hex(&Sha256::digest(&bytes))
    );
    artifacts.insert(&reference, &bytes).unwrap();
    reference
}
fn parse(value: &Value) -> Candidate {
    Candidate::parse(
        Format::Json,
        &serde_json::to_vec(value).unwrap(),
        Limits::default(),
    )
    .unwrap()
    .normalized_configuration(Limits::default())
    .unwrap()
}
fn inventory(v: &Value) -> Value {
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
    json!({"schema":1,"cluster_id":v["identity"]["cluster_id"],"namespace_id":v["identity"]["namespace_id"],"hosts":hosts})
}
struct Fixture {
    candidate: Candidate,
    artifacts: ArtifactCatalog,
    signatures: Catalog,
    trust: Trust,
    reference: String,
}
impl Fixture {
    fn new(alter: impl FnOnce(&mut Value)) -> Self {
        let mut v = Candidate::parse(
            Format::Toml,
            include_bytes!("../../../config/reference.toml"),
            Limits::default(),
        )
        .unwrap()
        .value()
        .clone();
        let mut artifacts = ArtifactCatalog::new(CatalogLimits::default()).unwrap();
        let reference = put(&mut artifacts, "inventory", &inventory(&v));
        v["qualification"]["host_inventory_ref"] = reference.into();
        let mut document = json!({
            "schema":1,"cluster_id":v["identity"]["cluster_id"],
            "namespace_id":v["identity"]["namespace_id"],"observed_unix_ms":NOW-1000,
            "retained":{
                "schema":1,"cluster_id":v["identity"]["cluster_id"],
                "namespace_id":v["identity"]["namespace_id"],"revision":"17","allocations":[]
            }
        });
        alter(&mut document);
        let reference = put(&mut artifacts, "retained", &document);
        let key = Ed25519KeyPair::from_seed_unchecked(&[75; 32]).unwrap();
        let cluster = v["identity"]["cluster_id"].as_str().unwrap().to_owned();
        let namespace = v["identity"]["namespace_id"].as_str().unwrap().to_owned();
        let claim = Claim {
            schema: 1,
            cluster_id: cluster.clone(),
            namespace_id: namespace.clone(),
            issuer_id: "resource-registry".into(),
            kind: Kind::RetainedResources,
            artifact_ref: reference.clone(),
            issued_unix_ms: NOW - 500,
            expires_unix_ms: NOW + 10000,
        };
        let signature = hex(key.sign(&claim.signing_bytes().unwrap()).as_ref());
        let mut signatures = Catalog::default();
        signatures
            .insert(&serde_json::to_vec(&json!({"claim":claim,"signature":signature})).unwrap())
            .unwrap();
        let trust = Trust::new(
            cluster,
            namespace,
            1,
            vec![Issuer {
                id: "resource-registry".into(),
                public_key: key.public_key().as_ref().try_into().unwrap(),
                kinds: [Kind::RetainedResources].into_iter().collect(),
            }],
        )
        .unwrap();
        Self {
            candidate: parse(&v),
            artifacts,
            signatures,
            trust,
            reference,
        }
    }
    fn check(&self, now: u64) -> tickwire_config_service::Result<RetainedCapacity> {
        RetainedCapacity::check(
            &self.candidate,
            &self.artifacts,
            &self.trust,
            &self.signatures,
            &self.reference,
            now,
            ObservationLimits::default(),
        )
    }
}
#[test]
fn signed_registry_keeps_revision_content_and_capacity_bindings() {
    let f = Fixture::new(|_| {});
    let result = f.check(NOW).unwrap();
    assert_eq!(result.retained_revision(), 17);
    assert_eq!(result.capacity().candidate_hash(), f.candidate.hash());
    assert_eq!(result.capacity().hosts().len(), 3);
    assert_eq!(
        result.capacity().hosts()["host-a"].shared_bytes,
        6_449_856_512
    );
    assert_eq!(result.evidence().claim().artifact_ref, f.reference);
    assert_eq!(result.expires_unix_ms(), NOW + 4000);
    let changed = Fixture::new(|d| d["retained"]["revision"] = "18".into());
    let result2 = changed.check(NOW).unwrap();
    assert_eq!(result2.retained_revision(), 18);
    assert_ne!(
        result.capacity().retained_hash(),
        result2.capacity().retained_hash()
    );
    assert_ne!(
        result.evidence().artifact_hash(),
        result2.evidence().artifact_hash()
    );
}
#[test]
fn signed_extra_allocations_are_still_included_in_real_capacity_checks() {
    for bad in 0..3 {
        let f = Fixture::new(|d| {
            d["retained"]["allocations"] = json!([{
                "resource_id":"previous-bank","host_id":"host-a",
                "shared_bytes":if bad==0 {8589934592u64} else {0},
                "private_bytes":if bad==1 {274877906944u64} else {0},
                "cpus":if bad==2 {json!([8])} else {json!([])}
            }]);
        });
        assert!(f.check(NOW).is_err(), "invalid allocation {bad}");
    }
    let f = Fixture::new(|d| {
        d["retained"]["allocations"] = json!([{
            "resource_id":"previous-bank","host_id":"host-a",
            "shared_bytes":1,"private_bytes":1,"cpus":[62]
        }]);
    });
    let result = f.check(NOW).unwrap();
    assert_eq!(
        result.capacity().hosts()["host-a"].shared_bytes,
        6_449_856_513
    );
}
#[test]
fn outer_and_inner_scope_unknown_fields_and_invalid_revisions_are_rejected() {
    for bad in 0..7 {
        let f = Fixture::new(|d| match bad {
            0 => d["cluster_id"] = "other-cluster".into(),
            1 => d["retained"]["cluster_id"] = "other-cluster".into(),
            2 => d["unknown"] = true.into(),
            3 => d["retained"]["unknown"] = true.into(),
            4 => d["retained"]["revision"] = "017".into(),
            5 => d["retained"]["revision"] = "0".into(),
            _ => d["retained"]["revision"] = "9223372036854775808".into(),
        });
        assert!(f.check(NOW).is_err(), "invalid document {bad}");
    }
}
#[test]
fn source_authorization_and_observation_freshness_are_required_for_capacity_admission() {
    let mut f = Fixture::new(|_| {});
    assert!(f.check(NOW + 3999).is_ok());
    assert!(f.check(NOW + 4000).is_err());
    f.signatures = Catalog::default();
    assert!(f.check(NOW).is_err());
    for observed in [0, NOW - 499, NOW + 1] {
        let f = Fixture::new(|d| d["observed_unix_ms"] = observed.into());
        assert!(f.check(NOW).is_err());
    }
    let f = Fixture::new(|_| {});
    for max_age_ms in [0, 60_001, u64::MAX] {
        assert!(
            RetainedCapacity::check(
                &f.candidate,
                &f.artifacts,
                &f.trust,
                &f.signatures,
                &f.reference,
                NOW,
                ObservationLimits { max_age_ms }
            )
            .is_err()
        );
    }
}
