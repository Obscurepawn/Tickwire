use ring::signature::{Ed25519KeyPair, KeyPair};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tickwire_config::{
    Candidate, Format, Limits,
    artifacts::{ArtifactCatalog, CatalogLimits},
};
use tickwire_config_service::{
    account_facts::{AccountModes, Limits as ObservationLimits},
    evidence::{Catalog, Claim, Issuer, Kind, Trust},
};

const NOW: u64 = 1_800_000_000_000;
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
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
fn put(artifacts: &mut ArtifactCatalog, name: &str, value: &Value) -> String {
    let bytes = serde_json::to_vec(value).unwrap();
    let reference = format!(
        "artifact://fixture/{name}@sha256:{}",
        hex(&Sha256::digest(&bytes))
    );
    artifacts.insert(&reference, &bytes).unwrap();
    reference
}
fn signed(claim: &Claim) -> Vec<u8> {
    let key = Ed25519KeyPair::from_seed_unchecked(&[65; 32]).unwrap();
    serde_json::to_vec(&json!({
        "claim":claim,
        "signature":hex(key.sign(&claim.signing_bytes().unwrap()).as_ref())
    }))
    .unwrap()
}
struct Fixture {
    candidate: Candidate,
    artifacts: ArtifactCatalog,
    signatures: Catalog,
    claims: Vec<Claim>,
}
impl Fixture {
    fn new(mut alter: impl FnMut(usize, bool, &mut Value)) -> Self {
        let mut value = Candidate::parse(
            Format::Toml,
            include_bytes!("../../../config/reference.toml"),
            Limits::default(),
        )
        .unwrap()
        .value()
        .clone();
        let mut artifacts = ArtifactCatalog::new(CatalogLimits::default()).unwrap();
        let mut signatures = Catalog::default();
        let mut claims = Vec::new();
        for i in 0..value["accounts"].as_array().unwrap().len() {
            let account = value["accounts"][i].clone();
            let venue = value["venues"]
                .as_array()
                .unwrap()
                .iter()
                .find(|v| v["id"] == account["venue_id"])
                .unwrap();
            let mut identity = json!({
                "schema":1,"venue":venue["venue"],"environment":venue["environment"],
                "product":venue["product"],"exchange_account_id":format!("uid-{i}"),
                "funds_identity":format!("uid-{i}/funds-{}",venue["product"].as_str().unwrap())
            });
            alter(i, true, &mut identity);
            let identity_ref = put(&mut artifacts, &format!("account/{i}/identity"), &identity);
            let mut mode = json!({
                "schema":1,
                "cluster_id":value["identity"]["cluster_id"],
                "namespace_id":value["identity"]["namespace_id"],
                "account_id":account["id"],"venue_id":account["venue_id"],
                "environment":venue["environment"],"product":venue["product"],
                "account_identity_ref":identity_ref,
                "observed_exchange_account_id":identity["exchange_account_id"],
                "observed_funds_identity":identity["funds_identity"],
                "position_mode":account["position_mode"],"margin_mode":account["margin_mode"],
                "asset_mode":account["asset_mode"],"trading_enabled":true,
                "observed_unix_ms":NOW-1000
            });
            alter(i, false, &mut mode);
            let mode_ref = put(&mut artifacts, &format!("account/{i}/mode"), &mode);
            value["accounts"][i]["account_identity_ref"] = identity_ref.into();
            value["accounts"][i]["mode_snapshot_ref"] = mode_ref.clone().into();
            let claim = Claim {
                schema: 1,
                cluster_id: value["identity"]["cluster_id"].as_str().unwrap().into(),
                namespace_id: value["identity"]["namespace_id"].as_str().unwrap().into(),
                issuer_id: "account-reader".into(),
                kind: Kind::AccountMode,
                artifact_ref: mode_ref,
                issued_unix_ms: NOW - 500,
                expires_unix_ms: NOW + 10_000,
            };
            signatures.insert(&signed(&claim)).unwrap();
            claims.push(claim);
        }
        Self {
            candidate: parse(&value),
            artifacts,
            signatures,
            claims,
        }
    }
    fn trust(&self, cluster: Option<&str>) -> Trust {
        Trust::new(
            cluster
                .unwrap_or(
                    self.candidate.value()["identity"]["cluster_id"]
                        .as_str()
                        .unwrap(),
                )
                .into(),
            self.candidate.value()["identity"]["namespace_id"]
                .as_str()
                .unwrap()
                .into(),
            1,
            vec![Issuer {
                id: "account-reader".into(),
                public_key: Ed25519KeyPair::from_seed_unchecked(&[65; 32])
                    .unwrap()
                    .public_key()
                    .as_ref()
                    .try_into()
                    .unwrap(),
                kinds: [Kind::AccountMode].into_iter().collect(),
            }],
        )
        .unwrap()
    }
    fn check(&self, now: u64) -> tickwire_config_service::Result<AccountModes> {
        AccountModes::check(
            &self.candidate,
            &self.artifacts,
            &self.trust(None),
            &self.signatures,
            now,
            ObservationLimits::default(),
        )
    }
}
#[test]
fn both_products_bind_the_actual_account_identity_modes_and_shorter_observation_expiry() {
    let f = Fixture::new(|_, _, _| {});
    let facts = f.check(NOW).unwrap();
    assert_eq!(facts.candidate_hash(), f.candidate.hash());
    assert_eq!(facts.accounts().len(), 2);
    assert_eq!(facts.expires_unix_ms(), NOW + 4000);
    for account in f.candidate.value()["accounts"].as_array().unwrap() {
        let fact = &facts.accounts()[account["id"].as_str().unwrap()];
        let identity = f
            .artifacts
            .get(account["account_identity_ref"].as_str().unwrap())
            .unwrap();
        let mode = f
            .artifacts
            .get(account["mode_snapshot_ref"].as_str().unwrap())
            .unwrap();
        assert_eq!(
            fact.identity_hash(),
            <[u8; 32]>::from(Sha256::digest(identity))
        );
        assert_eq!(fact.mode_hash(), <[u8; 32]>::from(Sha256::digest(mode)));
        assert_eq!(fact.observed_unix_ms(), NOW - 1000);
        assert!(fact.funds_identity().contains("/funds-"));
        assert_eq!(fact.evidence().claim().kind, Kind::AccountMode);
    }
}
#[test]
fn a_valid_signature_does_not_allow_different_identity_modes_or_disabled_trading() {
    for bad in 0..9 {
        let f = Fixture::new(|i, identity, value| {
            if i != 1 || identity {
                return;
            }
            match bad {
                0 => value["observed_exchange_account_id"] = "other-uid".into(),
                1 => value["observed_funds_identity"] = "other-pool".into(),
                2 => value["position_mode"] = "Hedge".into(),
                3 => value["margin_mode"] = "Isolated".into(),
                4 => value["asset_mode"] = "MultiAsset".into(),
                5 => value["trading_enabled"] = false.into(),
                6 => value["product"] = "Spot".into(),
                7 => value["environment"] = "production".into(),
                _ => value["account_id"] = "spot-main".into(),
            };
        });
        assert!(f.check(NOW).is_err(), "invalid observation {bad}");
    }
}
#[test]
fn shared_funds_cannot_gain_two_independent_risk_owners_via_identity_object_aliases() {
    let f = Fixture::new(|_, identity, value| {
        if identity {
            value["funds_identity"] = "same-parent/shared-collateral".into();
        }
    });
    assert!(f.check(NOW).is_err());
}
#[test]
fn observation_age_is_not_extended_by_the_signature_lifetime() {
    let f = Fixture::new(|_, _, _| {});
    assert!(f.check(NOW + 3999).is_ok());
    assert!(f.check(NOW + 4000).is_err());
    for observed in [NOW + 1, NOW - 499, 0] {
        let f = Fixture::new(|_, identity, value| {
            if !identity {
                value["observed_unix_ms"] = observed.into();
            }
        });
        assert!(f.check(NOW).is_err(), "observed={observed}");
    }
    for max_age_ms in [0, 60_001, u64::MAX] {
        assert!(
            AccountModes::check(
                &f.candidate,
                &f.artifacts,
                &f.trust(None),
                &f.signatures,
                NOW,
                ObservationLimits { max_age_ms }
            )
            .is_err()
        );
    }
}
#[test]
fn signed_cross_cluster_body_missing_source_and_missing_account_objects_are_rejected() {
    let mut f = Fixture::new(|_, _, _| {});
    let mut wrong_scope = Catalog::default();
    for claim in &f.claims {
        let mut claim = claim.clone();
        claim.cluster_id = "another-cluster".into();
        wrong_scope.insert(&signed(&claim)).unwrap();
    }
    assert!(
        AccountModes::check(
            &f.candidate,
            &f.artifacts,
            &f.trust(Some("another-cluster")),
            &wrong_scope,
            NOW,
            ObservationLimits::default()
        )
        .is_err()
    );
    f.signatures = Catalog::default();
    assert!(f.check(NOW).is_err());
    let f = Fixture::new(|_, _, _| {});
    let mut value = f.candidate.value().clone();
    value["accounts"][1]["account_identity_ref"] =
        format!("artifact://absent/identity@sha256:{}", "1".repeat(64)).into();
    assert!(
        AccountModes::check(
            &parse(&value),
            &f.artifacts,
            &f.trust(None),
            &f.signatures,
            NOW,
            ObservationLimits::default()
        )
        .is_err()
    );
}
#[test]
fn identities_and_observations_have_strict_bounded_shapes() {
    for bad in 0..7 {
        let f = Fixture::new(|i, identity, value| {
            if i != 0 {
                return;
            }
            match (bad, identity) {
                (0, true) => value["unknown"] = true.into(),
                (1, false) => value["unknown"] = true.into(),
                (2, true) => value["funds_identity"] = "".into(),
                (3, true) => value["exchange_account_id"] = "a".repeat(129).into(),
                (4, true) => value["funds_identity"] = "space separated".into(),
                (5, false) => value["schema"] = 2.into(),
                (6, true) => value["product"] = "Usdm".into(),
                _ => {}
            }
        });
        assert!(f.check(NOW).is_err(), "invalid shape {bad}");
    }
}
