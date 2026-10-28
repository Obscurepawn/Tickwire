#[allow(dead_code)]
mod deployment_support;
use deployment_support::{Fixture as Deployment, NOW};
use ring::signature::{Ed25519KeyPair, KeyPair};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use tickwire_config::{
    artifacts::reference_hash,
    delivery::{ProjectionLimits, ProjectionSet},
};
use tickwire_config_service::{
    evidence::{Catalog, Claim, Issuer, Kind, Trust},
    runtime_facts::{Input, IssuerScope, Limits, RuntimeTargets, RuntimeTrust, Sources},
};

const REVISION: u64 = 7;
fn key() -> Ed25519KeyPair {
    Ed25519KeyPair::from_seed_unchecked(&[73; 32]).unwrap()
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
struct Fixture {
    deployment: Deployment,
    projections: ProjectionSet,
    documents: Vec<Value>,
    trust: Trust,
}
impl Fixture {
    fn new() -> Self {
        let deployment = Deployment::new(|_| {}, |_, _| {});
        let base = &deployment.candidate;
        let projections =
            ProjectionSet::build(base, &deployment.artifacts, ProjectionLimits::default()).unwrap();
        let mut hosts = BTreeMap::<String, Vec<Value>>::new();
        for (i, p) in base.value()["processes"]
            .as_array()
            .unwrap()
            .iter()
            .enumerate()
        {
            let id = p["id"].as_str().unwrap();
            let projection = &projections.targets()[id];
            let mut identity = [0u8; 16];
            identity[..8].copy_from_slice(&(i as u64 + 1).to_le_bytes());
            let target = json!({
                "process_id":id,"host_id":p["host_id"],"role":p["role"],
                "build_hash":reference_hash(p["artifact_ref"].as_str().unwrap()).unwrap(),
                "instance":identity,"boot":identity,"pid":1000+i,"exec_generation":"1",
                "instance_slot":identity,"locator_generation":"1",
                "runtime_revision":REVISION.to_string(),"global_hash":base.hash(),
                "projection_hash":projection.hash(),"projection_policy_revision":projection.policy_revision().to_string(),
                "applied_event_seq":"9"
            });
            hosts
                .entry(p["host_id"].as_str().unwrap().into())
                .or_default()
                .push(target);
        }
        let documents = hosts.into_iter().enumerate().map(|(i,(host,targets))|json!({
            "schema":1,"cluster_id":base.value()["identity"]["cluster_id"],
            "namespace_id":base.value()["identity"]["namespace_id"],"host_id":host,
            "host_boot":vec![i as u8+1;16],"source_epoch":vec![i as u8+11;16],"source_sequence":"1",
            "observed_unix_ms":NOW-1000,"targets":targets
        })).collect();
        let trust = Trust::new(
            base.value()["identity"]["cluster_id"]
                .as_str()
                .unwrap()
                .into(),
            base.value()["identity"]["namespace_id"]
                .as_str()
                .unwrap()
                .into(),
            1,
            vec![Issuer {
                id: "runtime-reader".into(),
                public_key: key().public_key().as_ref().try_into().unwrap(),
                kinds: [Kind::RuntimeTargets].into_iter().collect(),
            }],
        )
        .unwrap();
        Self {
            deployment,
            projections,
            documents,
            trust,
        }
    }
    fn scopes(&self) -> Vec<IssuerScope> {
        let mut hosts = BTreeMap::<String, std::collections::BTreeSet<String>>::new();
        for p in self.deployment.candidate.value()["processes"]
            .as_array()
            .unwrap()
        {
            hosts
                .entry(p["host_id"].as_str().unwrap().into())
                .or_default()
                .insert(p["id"].as_str().unwrap().into());
        }
        hosts
            .into_iter()
            .map(|(host_id, process_ids)| IssuerScope {
                issuer_id: "runtime-reader".into(),
                host_id,
                process_ids,
            })
            .collect()
    }
    fn signed(&mut self, alter: impl FnMut(&mut Claim), seed: u8) -> (Catalog, Vec<String>) {
        self.signed_raw(alter, seed, |_, _| {})
    }
    fn signed_raw(
        &mut self,
        mut alter: impl FnMut(&mut Claim),
        seed: u8,
        mut raw: impl FnMut(usize, &mut Vec<u8>),
    ) -> (Catalog, Vec<String>) {
        let mut signatures = Catalog::default();
        let mut references = Vec::new();
        for (i, document) in self.documents.iter().enumerate() {
            let mut bytes = serde_json::to_vec(document).unwrap();
            raw(i, &mut bytes);
            let reference = format!(
                "artifact://runtime/{i}@sha256:{}",
                hex(&Sha256::digest(&bytes))
            );
            self.deployment
                .artifacts
                .insert(&reference, &bytes)
                .unwrap();
            let mut claim = Claim {
                schema: 1,
                cluster_id: self.deployment.candidate.value()["identity"]["cluster_id"]
                    .as_str()
                    .unwrap()
                    .into(),
                namespace_id: self.deployment.candidate.value()["identity"]["namespace_id"]
                    .as_str()
                    .unwrap()
                    .into(),
                issuer_id: "runtime-reader".into(),
                kind: Kind::RuntimeTargets,
                artifact_ref: reference.clone(),
                issued_unix_ms: NOW - 500,
                expires_unix_ms: NOW + 10_000,
            };
            alter(&mut claim);
            // Independent canonical ASCII-key envelope signing.
            let mut signing = b"tickwire.config.evidence.v1\0".to_vec();
            signing.extend_from_slice(
                &serde_json::to_vec(&serde_json::to_value(&claim).unwrap()).unwrap(),
            );
            let signer = Ed25519KeyPair::from_seed_unchecked(&[seed; 32]).unwrap();
            signatures
                .insert(
                    &serde_json::to_vec(
                        &json!({"claim":claim,"signature":hex(signer.sign(&signing).as_ref())}),
                    )
                    .unwrap(),
                )
                .unwrap();
            references.push(reference);
        }
        (signatures, references)
    }
    fn verify(
        &self,
        signatures: &Catalog,
        references: &[String],
        runtime_trust: &RuntimeTrust,
        now: u64,
    ) -> tickwire_config_service::Result<RuntimeTargets> {
        RuntimeTargets::check(
            Input {
                base: &self.deployment.candidate,
                current_revision: REVISION,
                projections: &self.projections,
            },
            Sources {
                artifacts: &self.deployment.artifacts,
                signatures,
                trust: &self.trust,
                runtime_trust,
                references,
            },
            now,
            Limits::default(),
        )
    }
    fn check(&mut self, now: u64) -> tickwire_config_service::Result<RuntimeTargets> {
        let runtime_trust = RuntimeTrust::new(&self.trust, self.scopes())?;
        let (signatures, references) = self.signed(|_| {}, 73);
        self.verify(&signatures, &references, &runtime_trust, now)
    }
}
#[test]
fn signed_complete_baseline_observations_are_verified() {
    let mut f = Fixture::new();
    let checked = f.check(NOW).unwrap();
    assert_eq!(checked.base_hash(), f.deployment.candidate.hash());
    assert_eq!(checked.current_revision(), REVISION);
    assert_eq!(
        checked.targets().len(),
        f.deployment.candidate.value()["processes"]
            .as_array()
            .unwrap()
            .len()
    );
    assert_eq!(checked.hosts().len(), f.documents.len());
    assert_eq!(checked.expires_unix_ms(), NOW + 4000);
    assert_eq!(checked.checked_unix_ms(), NOW);
    for host in checked.hosts().values() {
        assert_eq!(host.source_sequence(), 1);
        assert_eq!(host.observed_unix_ms(), NOW - 1000);
        assert_ne!(host.source_epoch(), [0; 16]);
        assert_ne!(host.host_boot(), [0; 16]);
        assert_eq!(host.evidence().claim().kind, Kind::RuntimeTargets);
    }
    let scopes = RuntimeTrust::new(&f.trust, f.scopes()).unwrap();
    let (signatures, mut refs) = f.signed(|_| {}, 73);
    refs.reverse();
    assert_eq!(
        f.verify(&signatures, &refs, &scopes, NOW + 1)
            .unwrap()
            .digest(),
        checked.digest()
    );
}
#[test]
fn source_scope_is_startup_owned_and_cannot_overlap() {
    let f = Fixture::new();
    for field in ["issuer", "host", "process", "empty"] {
        let mut scopes = f.scopes();
        match field {
            "issuer" => scopes[0].issuer_id = "unknown".into(),
            "host" => scopes[0].host_id = "*".into(),
            "process" => {
                scopes[0].process_ids.insert("*".into());
            }
            _ => scopes[0].process_ids.clear(),
        }
        assert!(RuntimeTrust::new(&f.trust, scopes).is_err(), "{field}");
    }
    let mut scopes = f.scopes();
    scopes.push(scopes[0].clone());
    assert!(RuntimeTrust::new(&f.trust, scopes).is_err());
    let mut scopes = f.scopes();
    let duplicate = scopes[0].process_ids.first().unwrap().clone();
    scopes[1].process_ids.insert(duplicate);
    assert!(RuntimeTrust::new(&f.trust, scopes).is_err());
    let mut scopes = f.scopes();
    scopes[1].host_id = scopes[0].host_id.clone();
    assert!(RuntimeTrust::new(&f.trust, scopes).is_err());
    assert!(RuntimeTrust::new(&f.trust, Vec::new()).is_err());
    assert!(RuntimeTrust::new(&f.trust, vec![f.scopes()[0].clone(); 257]).is_err());
    let mut scopes = f.scopes();
    scopes[0].process_ids = (0..4097).map(|i| format!("process-{i}")).collect();
    assert!(RuntimeTrust::new(&f.trust, scopes).is_err());
}
#[test]
fn valid_signature_does_not_grant_process_or_host_scope() {
    let mut f = Fixture::new();
    let (signatures, refs) = f.signed(|_| {}, 73);
    let mut scopes = f.scopes();
    scopes[0].process_ids.clear();
    scopes[0].process_ids.insert("unrelated-process".into());
    let trust = RuntimeTrust::new(&f.trust, scopes).unwrap();
    assert!(matches!(
        f.verify(&signatures, &refs, &trust, NOW),
        Err(tickwire_config_service::Error::Authorization)
    ));
    let mut scopes = f.scopes();
    scopes.remove(0);
    let trust = RuntimeTrust::new(&f.trust, scopes).unwrap();
    assert!(f.verify(&signatures, &refs, &trust, NOW).is_err());
}
#[test]
fn complete_baseline_rejects_missing_extra_duplicate_and_mixed_host_documents() {
    let mut f = Fixture::new();
    let original = f.documents.clone();
    for case in 0..6 {
        f.documents = original.clone();
        match case {
            0 => {
                f.documents.remove(0);
            }
            1 => {
                f.documents[0]["targets"].as_array_mut().unwrap().pop();
            }
            2 => {
                let row = f.documents[0]["targets"][0].clone();
                f.documents[0]["targets"].as_array_mut().unwrap().push(row);
            }
            3 => {
                f.documents[0]["targets"][0]["process_id"] = "unknown-process".into();
            }
            4 => {
                let mut duplicate = f.documents[0].clone();
                duplicate["source_epoch"] = json!(vec![99; 16]);
                f.documents.push(duplicate);
            }
            _ => {
                f.documents[0]["targets"][0]["host_id"] = f.documents[1]["host_id"].clone();
            }
        }
        assert!(f.check(NOW).is_err(), "case {case}");
    }
}
#[test]
fn signed_rows_bind_roles_builds_incarnations_and_exact_current_projection() {
    let mut f = Fixture::new();
    let original = f.documents.clone();
    for (field, value) in [
        ("role", json!("made-up")),
        ("build_hash", json!(vec![91; 32])),
        ("instance", json!(vec![0; 16])),
        ("boot", json!(vec![0; 16])),
        ("instance_slot", json!(vec![0; 16])),
        ("pid", json!(0)),
        ("pid", json!(2147483648u32)),
        ("exec_generation", json!("0")),
        ("exec_generation", json!("01")),
        ("locator_generation", json!("0")),
        ("runtime_revision", json!("8")),
        ("global_hash", json!(vec![95; 32])),
        ("projection_hash", json!(vec![96; 32])),
        ("projection_policy_revision", json!("2")),
        ("applied_event_seq", json!("0")),
        ("applied_event_seq", json!("9223372036854775808")),
        ("unexpected", json!(true)),
    ] {
        f.documents = original.clone();
        f.documents[0]["targets"][0][field] = value;
        assert!(f.check(NOW).is_err(), "{field}");
    }
    for field in ["instance", "boot", "instance_slot"] {
        f.documents = original.clone();
        f.documents[1]["targets"][0][field] = f.documents[0]["targets"][0][field].clone();
        assert!(f.check(NOW).is_err(), "duplicate {field}");
    }
    f.documents = original;
    for (field, value) in [
        ("host_boot", json!(vec![0; 16])),
        ("source_epoch", json!(vec![0; 16])),
        ("source_sequence", json!("0")),
    ] {
        let prior = f.documents[0][field].clone();
        f.documents[0][field] = value;
        assert!(f.check(NOW).is_err(), "{field}");
        f.documents[0][field] = prior;
    }
}
#[test]
fn observation_and_signature_expiry_are_independent_and_exact() {
    let mut f = Fixture::new();
    assert!(f.check(NOW + 3999).is_ok());
    assert!(f.check(NOW + 4000).is_err());
    let scopes = RuntimeTrust::new(&f.trust, f.scopes()).unwrap();
    let (signatures, refs) = f.signed(|claim| claim.expires_unix_ms = NOW + 1, 73);
    assert_eq!(
        f.verify(&signatures, &refs, &scopes, NOW)
            .unwrap()
            .expires_unix_ms(),
        NOW + 1
    );
    assert!(f.verify(&signatures, &refs, &scopes, NOW + 1).is_err());
    f.documents[0]["observed_unix_ms"] = json!(NOW);
    assert!(f.check(NOW).is_err()); // Observation later than its actual signed issuance.
    f.documents[0]["observed_unix_ms"] = json!(NOW - 1000);
    let (signatures, refs) = f.signed(|claim| claim.issued_unix_ms = NOW + 1, 73);
    assert!(f.verify(&signatures, &refs, &scopes, NOW).is_err());
    let (signatures, refs) = f.signed(|_| {}, 74);
    assert!(f.verify(&signatures, &refs, &scopes, NOW).is_err());
    let (signatures, refs) = f.signed(|claim| claim.kind = Kind::AccountMode, 73);
    assert!(f.verify(&signatures, &refs, &scopes, NOW).is_err());
}
#[test]
fn candidate_projection_cannot_replace_the_observed_base_and_trust_is_pinned() {
    let mut f = Fixture::new();
    let scopes = RuntimeTrust::new(&f.trust, f.scopes()).unwrap();
    let (signatures, refs) = f.signed(|_| {}, 73);
    let mut changed = f.deployment.candidate.value().clone();
    changed["observability"]["diagnostic_level"] = "debug".into();
    let candidate = deployment_support::parse(&changed);
    let candidate_set = ProjectionSet::build(
        &candidate,
        &f.deployment.artifacts,
        ProjectionLimits::default(),
    )
    .unwrap();
    assert!(
        RuntimeTargets::check(
            Input {
                base: &f.deployment.candidate,
                current_revision: REVISION,
                projections: &candidate_set
            },
            Sources {
                artifacts: &f.deployment.artifacts,
                signatures: &signatures,
                trust: &f.trust,
                runtime_trust: &scopes,
                references: &refs
            },
            NOW,
            Limits::default()
        )
        .is_err()
    );
    let base = &f.deployment.candidate;
    f.trust = Trust::new(
        base.value()["identity"]["cluster_id"]
            .as_str()
            .unwrap()
            .into(),
        base.value()["identity"]["namespace_id"]
            .as_str()
            .unwrap()
            .into(),
        2,
        vec![Issuer {
            id: "runtime-reader".into(),
            public_key: key().public_key().as_ref().try_into().unwrap(),
            kinds: [Kind::RuntimeTargets].into_iter().collect(),
        }],
    )
    .unwrap();
    assert!(f.verify(&signatures, &refs, &scopes, NOW).is_err());
}
#[test]
fn zero_oversized_and_duplicate_reference_inputs_are_bounded() {
    let mut f = Fixture::new();
    let scopes = RuntimeTrust::new(&f.trust, f.scopes()).unwrap();
    let (signatures, refs) = f.signed(|_| {}, 73);
    assert!(f.verify(&signatures, &[], &scopes, NOW).is_err());
    assert!(
        f.verify(&signatures, &vec![refs[0].clone(); 257], &scopes, NOW)
            .is_err()
    );
    let mut repeated = refs.clone();
    repeated.push(refs[0].clone());
    assert!(f.verify(&signatures, &repeated, &scopes, NOW).is_err());
    for maximum in [0, 60_001] {
        assert!(
            RuntimeTargets::check(
                Input {
                    base: &f.deployment.candidate,
                    current_revision: REVISION,
                    projections: &f.projections
                },
                Sources {
                    artifacts: &f.deployment.artifacts,
                    signatures: &signatures,
                    trust: &f.trust,
                    runtime_trust: &scopes,
                    references: &refs
                },
                NOW,
                Limits {
                    max_age_ms: maximum
                }
            )
            .is_err()
        );
    }
}

#[test]
fn independently_signed_raw_documents_obey_strict_json_and_exact_byte_budget() {
    let mut f = Fixture::new();
    let scopes = RuntimeTrust::new(&f.trust, f.scopes()).unwrap();
    let maximum = 4 * 1024 * 1024;
    let (signatures, refs) = f.signed_raw(
        |_| {},
        73,
        |i, bytes| {
            if i == 0 {
                bytes.resize(maximum, b' ');
            }
        },
    );
    assert!(f.verify(&signatures, &refs, &scopes, NOW).is_ok());
    let (signatures, refs) = f.signed_raw(
        |_| {},
        73,
        |i, bytes| {
            if i == 0 {
                bytes.resize(maximum + 1, b' ');
            }
        },
    );
    assert!(f.verify(&signatures, &refs, &scopes, NOW).is_err());
    let (signatures, refs) = f.signed_raw(
        |_| {},
        73,
        |i, bytes| {
            if i == 0 {
                bytes.pop();
                bytes.extend_from_slice(br#", "schema":1}"#);
            }
        },
    );
    assert!(f.verify(&signatures, &refs, &scopes, NOW).is_err());
}

#[test]
fn identity_counters_keep_the_full_positive_63_bit_domain() {
    let mut f = Fixture::new();
    let maximum = ((1u64 << 63) - 1).to_string();
    for document in &mut f.documents {
        document["source_sequence"] = maximum.clone().into();
        for target in document["targets"].as_array_mut().unwrap() {
            for field in ["exec_generation", "locator_generation", "applied_event_seq"] {
                target[field] = maximum.clone().into();
            }
        }
    }
    let checked = f.check(NOW).unwrap();
    assert!(
        checked
            .targets()
            .values()
            .all(|t| t.exec_generation == (1 << 63) - 1)
    );
    assert!(
        checked
            .hosts()
            .values()
            .all(|h| h.source_sequence() == (1 << 63) - 1)
    );
}
