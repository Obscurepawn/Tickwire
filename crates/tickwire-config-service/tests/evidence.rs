use ring::signature::{Ed25519KeyPair, KeyPair};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use tickwire_config::artifacts::{ArtifactCatalog, CatalogLimits};
use tickwire_config_service::{
    Error,
    evidence::{Claim, Issuer, Kind, Trust},
};
const CLUSTER: &str = "evidence-cluster";
const NAMESPACE: &str = "00000000-0000-4000-8000-000000000001";
const NOW: u64 = 1_800_000_000_000;
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
fn key(seed: u8) -> Ed25519KeyPair {
    Ed25519KeyPair::from_seed_unchecked(&[seed; 32]).unwrap()
}
fn issuer(id: &str, seed: u8, kinds: &[Kind]) -> Issuer {
    Issuer {
        id: id.to_owned(),
        public_key: key(seed).public_key().as_ref().try_into().unwrap(),
        kinds: kinds.iter().copied().collect(),
    }
}
fn trust() -> Trust {
    Trust::new(
        CLUSTER.into(),
        NAMESPACE.into(),
        1,
        vec![issuer("inventory", 7, &[Kind::HostInventory])],
    )
    .unwrap()
}
fn fixture() -> (ArtifactCatalog, Claim) {
    let bytes = br#"{"schema":1,"hosts":["a","b"]}"#;
    let reference = format!(
        "artifact://reports/inventory@sha256:{}",
        hex(&Sha256::digest(bytes))
    );
    let mut catalog = ArtifactCatalog::new(CatalogLimits::default()).unwrap();
    catalog.insert(&reference, bytes).unwrap();
    let claim = Claim {
        schema: 1,
        cluster_id: CLUSTER.into(),
        namespace_id: NAMESPACE.into(),
        issuer_id: "inventory".into(),
        kind: Kind::HostInventory,
        artifact_ref: reference,
        issued_unix_ms: NOW - 1000,
        expires_unix_ms: NOW + 1000,
    };
    (catalog, claim)
}
// Independent ASCII-key JSON ordering and fixed domain bytes. This deliberately
// does not call the production signing-byte constructor.
fn signed(claim: &Claim, seed: u8) -> Vec<u8> {
    let claim = serde_json::to_value(claim).unwrap();
    let mut bytes = b"tickwire.config.evidence.v1\0".to_vec();
    bytes.extend_from_slice(&serde_json::to_vec(&claim).unwrap());
    let signature = hex(key(seed).sign(&bytes).as_ref());
    serde_json::to_vec(&json!({"claim":claim,"signature":signature})).unwrap()
}
#[test]
fn independently_encoded_signature_binds_actual_pinned_bytes_and_current_trust() {
    let trust = trust();
    let (catalog, claim) = fixture();
    let mut expected = b"tickwire.config.evidence.v1\0".to_vec();
    expected
        .extend_from_slice(&serde_json::to_vec(&serde_json::to_value(&claim).unwrap()).unwrap());
    assert_eq!(claim.signing_bytes().unwrap(), expected);
    let evidence = trust
        .verify(
            &signed(&claim, 7),
            &catalog,
            claim.kind,
            &claim.artifact_ref,
            NOW,
        )
        .unwrap();
    assert_eq!(evidence.claim(), &claim);
    assert_eq!(
        evidence.artifact_hash(),
        Sha256::digest(catalog.get(&claim.artifact_ref).unwrap()).as_slice()
    );
    assert_eq!(evidence.trust_hash(), trust.digest());
    assert_eq!(evidence.checked_unix_ms(), NOW);
    assert_eq!(
        evidence
            .revalidate(&trust, &catalog, NOW + 1)
            .unwrap()
            .checked_unix_ms(),
        NOW + 1
    );
}
#[test]
fn wrong_signature_kind_reference_scope_and_missing_bytes_cannot_authorize_a_report() {
    let trust = trust();
    let (catalog, claim) = fixture();
    assert!(
        trust
            .verify(
                &signed(&claim, 8),
                &catalog,
                claim.kind,
                &claim.artifact_ref,
                NOW
            )
            .is_err()
    );
    assert!(
        trust
            .verify(
                &signed(&claim, 7),
                &catalog,
                Kind::RiskAcceptance,
                &claim.artifact_ref,
                NOW
            )
            .is_err()
    );
    assert!(trust.verify(&signed(&claim, 7), &catalog, claim.kind, "artifact://reports/other@sha256:0000000000000000000000000000000000000000000000000000000000000000", NOW).is_err());
    for (cluster, namespace, issuer_id) in [
        ("another-cluster", NAMESPACE, "inventory"),
        (CLUSTER, "00000000-0000-4000-8000-000000000002", "inventory"),
        (CLUSTER, NAMESPACE, "unknown"),
    ] {
        let mut other = claim.clone();
        other.cluster_id = cluster.into();
        other.namespace_id = namespace.into();
        other.issuer_id = issuer_id.into();
        assert!(
            trust
                .verify(
                    &signed(&other, 7),
                    &catalog,
                    claim.kind,
                    &claim.artifact_ref,
                    NOW
                )
                .is_err()
        );
    }
    let empty = ArtifactCatalog::new(CatalogLimits::default()).unwrap();
    assert!(matches!(
        trust.verify(
            &signed(&claim, 7),
            &empty,
            claim.kind,
            &claim.artifact_ref,
            NOW
        ),
        Err(Error::NotFound)
    ));
}
#[test]
fn revocation_and_kind_restriction_apply_when_rechecking_saved_evidence() {
    let (catalog, claim) = fixture();
    let evidence = trust()
        .verify(
            &signed(&claim, 7),
            &catalog,
            claim.kind,
            &claim.artifact_ref,
            NOW,
        )
        .unwrap();
    for issuers in [
        vec![issuer("replacement", 8, &[Kind::HostInventory])],
        vec![issuer("inventory", 8, &[Kind::HostInventory])],
        vec![issuer("inventory", 7, &[Kind::RiskAcceptance])],
    ] {
        let current = Trust::new(CLUSTER.into(), NAMESPACE.into(), 2, issuers).unwrap();
        assert!(evidence.revalidate(&current, &catalog, NOW).is_err());
    }
    let current = Trust::new(
        CLUSTER.into(),
        NAMESPACE.into(),
        2,
        vec![issuer("inventory", 7, &[Kind::HostInventory])],
    )
    .unwrap();
    let checked = evidence.revalidate(&current, &catalog, NOW).unwrap();
    assert_ne!(checked.trust_hash(), evidence.trust_hash());
}
#[test]
fn evidence_lifetime_has_exact_future_expiry_and_maximum_duration_boundaries() {
    let trust = trust();
    let (catalog, claim) = fixture();
    for now in [
        0,
        claim.issued_unix_ms - 1,
        claim.expires_unix_ms,
        claim.expires_unix_ms + 1,
    ] {
        assert!(
            trust
                .verify(
                    &signed(&claim, 7),
                    &catalog,
                    claim.kind,
                    &claim.artifact_ref,
                    now
                )
                .is_err()
        );
    }
    assert!(
        trust
            .verify(
                &signed(&claim, 7),
                &catalog,
                claim.kind,
                &claim.artifact_ref,
                claim.issued_unix_ms
            )
            .is_ok()
    );
    let mut long = claim.clone();
    long.expires_unix_ms = long.issued_unix_ms + 7 * 24 * 60 * 60 * 1000;
    assert!(
        trust
            .verify(
                &signed(&long, 7),
                &catalog,
                claim.kind,
                &claim.artifact_ref,
                NOW
            )
            .is_ok()
    );
    long.expires_unix_ms += 1;
    assert!(
        trust
            .verify(
                &signed(&long, 7),
                &catalog,
                claim.kind,
                &claim.artifact_ref,
                NOW
            )
            .is_err()
    );
    long.expires_unix_ms = long.issued_unix_ms;
    assert!(long.signing_bytes().is_err());
}
#[test]
fn strict_envelope_rejects_unknown_fields_duplicates_noncanonical_signature_and_size() {
    let trust = trust();
    let (catalog, claim) = fixture();
    let bytes = signed(&claim, 7);
    for mutate in 0..6 {
        let mut value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        match mutate {
            0 => value["extra"] = true.into(),
            1 => value["claim"]["extra"] = true.into(),
            2 => value["signature"] = value["signature"].as_str().unwrap().to_uppercase().into(),
            3 => value["signature"] = "00".into(),
            4 => value["claim"]["issued_unix_ms"] = json!(NOW as f64),
            _ => value["claim"]["schema"] = 2.into(),
        };
        assert!(
            trust
                .verify(
                    &serde_json::to_vec(&value).unwrap(),
                    &catalog,
                    claim.kind,
                    &claim.artifact_ref,
                    NOW
                )
                .is_err()
        );
    }
    let duplicate = String::from_utf8(bytes.clone()).unwrap().replacen(
        "\"schema\":1",
        "\"schema\":1,\"schema\":1",
        1,
    );
    assert!(
        trust
            .verify(
                duplicate.as_bytes(),
                &catalog,
                claim.kind,
                &claim.artifact_ref,
                NOW
            )
            .is_err()
    );
    let mut large = bytes;
    large.extend(std::iter::repeat_n(b' ', 16 * 1024));
    assert!(
        trust
            .verify(&large, &catalog, claim.kind, &claim.artifact_ref, NOW)
            .is_err()
    );
}
#[test]
fn issuer_table_is_bounded_unambiguous_and_order_independent() {
    assert!(
        Trust::new(
            CLUSTER.into(),
            NAMESPACE.into(),
            0,
            vec![issuer("inventory", 7, &[Kind::HostInventory])]
        )
        .is_err()
    );
    assert!(Trust::new(CLUSTER.into(), NAMESPACE.into(), 1, vec![]).is_err());
    assert!(
        Trust::new(
            CLUSTER.into(),
            NAMESPACE.into(),
            1,
            vec![issuer("inventory", 7, &[])]
        )
        .is_err()
    );
    assert!(
        Trust::new(
            CLUSTER.into(),
            NAMESPACE.into(),
            1,
            vec![
                issuer("inventory", 7, &[Kind::HostInventory]),
                issuer("inventory", 8, &[Kind::RiskAcceptance])
            ]
        )
        .is_err()
    );
    assert!(
        Trust::new(
            CLUSTER.into(),
            NAMESPACE.into(),
            1,
            (0..33)
                .map(|n| issuer(&format!("issuer-{n}"), n + 1, &[Kind::HostInventory]))
                .collect()
        )
        .is_err()
    );
    let a = Trust::new(
        CLUSTER.into(),
        NAMESPACE.into(),
        1,
        vec![
            issuer("a", 7, &[Kind::HostInventory]),
            issuer("b", 8, &[Kind::RiskAcceptance]),
        ],
    )
    .unwrap();
    let b = Trust::new(
        CLUSTER.into(),
        NAMESPACE.into(),
        1,
        vec![
            issuer("b", 8, &[Kind::RiskAcceptance]),
            issuer("a", 7, &[Kind::HostInventory]),
        ],
    )
    .unwrap();
    assert_eq!(a.digest(), b.digest());
    assert_ne!(a.digest(), [0; 32]);
    let empty_kinds = Issuer {
        id: "a".into(),
        public_key: [0; 32],
        kinds: BTreeSet::new(),
    };
    assert!(Trust::new(CLUSTER.into(), NAMESPACE.into(), 1, vec![empty_kinds]).is_err());
}

fn trust_digest(bytes: &[u8]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"tickwire.config.evidence-trust.v1\0");
    hash.update(bytes);
    hash.finalize().into()
}

#[test]
fn trust_archive_roundtrip_retains_existing_digest_and_real_signature_verification() {
    let original = trust();
    let bytes = original.canonical_bytes();
    let expected = serde_json::to_vec(&json!({
        "schema":1,"cluster_id":CLUSTER,"namespace_id":NAMESPACE,"revision":"1",
        "issuers":[{"id":"inventory","public_key":key(7).public_key().as_ref(),"kinds":["host_inventory"]}]
    }))
    .unwrap();
    assert_eq!(bytes, expected);
    assert_eq!(original.digest(), trust_digest(&expected));
    let decoded = Trust::decode(bytes, original.digest()).unwrap();
    assert_eq!(decoded.canonical_bytes(), bytes);
    assert_eq!(decoded.digest(), original.digest());
    let (catalog, claim) = fixture();
    assert!(
        decoded
            .verify(
                &signed(&claim, 7),
                &catalog,
                claim.kind,
                &claim.artifact_ref,
                NOW
            )
            .is_ok()
    );
    assert!(matches!(Trust::decode(bytes, [0; 32]), Err(Error::Digest)));
    let highest = Trust::new(
        CLUSTER.into(),
        NAMESPACE.into(),
        (1 << 63) - 1,
        vec![issuer("inventory", 7, &[Kind::HostInventory])],
    )
    .unwrap();
    assert!(Trust::decode(highest.canonical_bytes(), highest.digest()).is_ok());
}

#[test]
fn trust_archive_rejects_noncanonical_and_ambiguous_descriptions_even_with_matching_hash() {
    let original = Trust::new(
        CLUSTER.into(),
        NAMESPACE.into(),
        1,
        vec![
            issuer("a", 7, &[Kind::HostInventory, Kind::RuntimeTargets]),
            issuer("b", 8, &[Kind::RiskAcceptance]),
        ],
    )
    .unwrap();
    let canonical: serde_json::Value = serde_json::from_slice(original.canonical_bytes()).unwrap();
    for mutation in 0..18 {
        let mut value = canonical.clone();
        match mutation {
            0 => value["unknown"] = true.into(),
            1 => value["issuers"][0]["unknown"] = true.into(),
            2 => value["revision"] = "01".into(),
            3 => value["revision"] = "0".into(),
            4 => value["revision"] = "+1".into(),
            5 => value["revision"] = "9223372036854775808".into(),
            6 => value["revision"] = 1.into(),
            7 => value["revision"] = "1.0".into(),
            8 => value["issuers"].as_array_mut().unwrap().reverse(),
            9 => value["issuers"][0]["kinds"]
                .as_array_mut()
                .unwrap()
                .reverse(),
            10 => value["issuers"][0]["kinds"] = json!(["host_inventory", "host_inventory"]),
            11 => value["issuers"][1] = value["issuers"][0].clone(),
            12 => value["issuers"][1]["public_key"] = value["issuers"][0]["public_key"].clone(),
            13 => value["issuers"][0]["kinds"] = json!(["unknown_kind"]),
            14 => value["issuers"][0]["public_key"] = json!(vec![0; 32]),
            15 => value["schema"] = 2.into(),
            16 => value["cluster_id"] = "INVALID".into(),
            _ => value["issuers"] = json!([]),
        }
        let bytes = serde_json::to_vec(&value).unwrap();
        assert!(
            Trust::decode(&bytes, trust_digest(&bytes)).is_err(),
            "mutation {mutation}"
        );
    }
    let duplicate = String::from_utf8(original.canonical_bytes().to_vec())
        .unwrap()
        .replacen("\"schema\":1", "\"schema\":1,\"schema\":1", 1)
        .into_bytes();
    let mut whitespace = original.canonical_bytes().to_vec();
    whitespace.push(b' ');
    let escaped = String::from_utf8(original.canonical_bytes().to_vec())
        .unwrap()
        .replacen("inventory", "invent\\u006fry", 1)
        .into_bytes();
    let mut oversized = original.canonical_bytes().to_vec();
    oversized.resize(16 * 1024 + 1, b' ');
    for bytes in [duplicate, whitespace, escaped, oversized] {
        assert!(Trust::decode(&bytes, trust_digest(&bytes)).is_err());
    }
}

#[test]
fn archived_trust_at_historical_time_cannot_replace_current_time_or_revocation() {
    let original = trust();
    let restored = Trust::decode(original.canonical_bytes(), original.digest()).unwrap();
    let (catalog, claim) = fixture();
    let evidence = restored
        .verify(
            &signed(&claim, 7),
            &catalog,
            claim.kind,
            &claim.artifact_ref,
            NOW,
        )
        .unwrap();
    assert!(
        evidence
            .revalidate(&restored, &catalog, claim.expires_unix_ms)
            .is_err()
    );
    let revoked = Trust::new(
        CLUSTER.into(),
        NAMESPACE.into(),
        2,
        vec![issuer("inventory", 8, &[Kind::HostInventory])],
    )
    .unwrap();
    let current = Trust::decode(revoked.canonical_bytes(), revoked.digest()).unwrap();
    assert!(evidence.revalidate(&current, &catalog, NOW).is_err());
}

#[test]
fn trust_archive_digest_binds_every_valid_configuration_field() {
    let original = trust();
    let value: serde_json::Value = serde_json::from_slice(original.canonical_bytes()).unwrap();
    for field in 0..6 {
        let mut changed = value.clone();
        match field {
            0 => changed["cluster_id"] = "other-cluster".into(),
            1 => changed["namespace_id"] = "00000000-0000-4000-8000-000000000002".into(),
            2 => changed["revision"] = "2".into(),
            3 => changed["issuers"][0]["id"] = "other-issuer".into(),
            4 => changed["issuers"][0]["public_key"] = json!(key(8).public_key().as_ref()),
            _ => changed["issuers"][0]["kinds"] = json!(["runtime_targets"]),
        }
        let bytes = serde_json::to_vec(&changed).unwrap();
        // These are valid configurations; only the pinned original identity is wrong.
        assert!(Trust::decode(&bytes, trust_digest(&bytes)).is_ok());
        assert!(matches!(
            Trust::decode(&bytes, original.digest()),
            Err(Error::Digest)
        ));
    }
}
