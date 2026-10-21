use sha2::{Digest, Sha256};
use tickwire_config::{
    Candidate, Format, Limits,
    locator::{Fields, Locator, MAX_BYTES},
};

fn first() -> Fields {
    Fields {
        generation: 1,
        cluster_id: "cluster-a".into(),
        active_root: "/var/lib/tickwire/config-a".into(),
        migration_id: [0; 16],
        manifest_hash: [4; 32],
        previous_locator_hash: [0; 32],
    }
}
fn next(previous: &Locator) -> Fields {
    Fields {
        generation: previous.fields().generation + 1,
        active_root: "/var/lib/tickwire/config-b".into(),
        migration_id: [5; 16],
        manifest_hash: [6; 32],
        previous_locator_hash: previous.hash(),
        ..previous.fields().clone()
    }
}
#[test]
fn locator_roundtrip_and_checksum_match_independent_canonical_calculation() {
    let a = Locator::build(first()).unwrap();
    let json = Locator::decode(Format::Json, a.canonical_bytes()).unwrap();
    let toml = Locator::decode(Format::Toml, &a.toml().unwrap()).unwrap();
    assert_eq!(json.fields(), a.fields());
    assert_eq!(toml.fields(), a.fields());
    assert_eq!(json.hash(), toml.hash());
    assert_eq!(
        a.hash(),
        <[u8; 32]>::from(Sha256::digest(a.canonical_bytes()))
    );
    let mut value: serde_json::Value = serde_json::from_slice(a.canonical_bytes()).unwrap();
    let checksum = value.as_object_mut().unwrap().remove("checksum").unwrap();
    let body = Candidate::parse(
        Format::Json,
        &serde_json::to_vec(&value).unwrap(),
        Limits::default(),
    )
    .unwrap();
    assert_eq!(
        checksum,
        format!(
            "sha256:{}",
            body.hash()
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>()
        )
    );
}
#[test]
fn locator_chain_requires_exact_generation_cluster_previous_root_and_migration() {
    let a = Locator::build(first()).unwrap();
    let b = Locator::build(next(&a)).unwrap();
    a.validate_successor(&b).unwrap();
    for case in 0..5 {
        let mut f = next(&a);
        match case {
            0 => f.generation += 1,
            1 => f.cluster_id = "cluster-b".into(),
            2 => f.previous_locator_hash = [9; 32],
            3 => f.active_root = a.fields().active_root.clone(),
            _ => f.generation = 1,
        }
        let rejected = Locator::build(f).and_then(|b| a.validate_successor(&b));
        assert!(rejected.is_err(), "case {case}");
    }
    let mut c = next(&b);
    c.active_root = "/var/lib/tickwire/config-c".into();
    assert!(
        b.validate_successor(&Locator::build(c.clone()).unwrap())
            .is_err()
    );
    c.migration_id = [7; 16];
    b.validate_successor(&Locator::build(c).unwrap()).unwrap();
}
#[test]
fn locator_rejects_unsafe_paths_zero_manifest_and_invalid_generation_bindings() {
    Locator::build(first()).unwrap();
    for path in [
        "/", "relative", "/a//b", "/a/../b", "/a/./b", "/a/", "/a/\0b", "/a/b c",
    ] {
        let mut f = first();
        f.active_root = path.into();
        assert!(Locator::build(f).is_err(), "{path:?}");
    }
    for case in 0..7 {
        let mut f = first();
        match case {
            0 => f.generation = 0,
            1 => f.generation = 1 << 63,
            2 => f.cluster_id = "wrong_cluster".into(),
            3 => f.manifest_hash = [0; 32],
            4 => f.previous_locator_hash = [8; 32],
            5 => f.generation = 2,
            _ => f.active_root = format!("/{}", "a".repeat(4096)),
        }
        assert!(Locator::build(f).is_err(), "case {case}");
    }
}
#[test]
fn locator_decode_rejects_changed_content_duplicates_unknowns_and_oversize() {
    let a = Locator::build(first()).unwrap();
    let value: serde_json::Value = serde_json::from_slice(a.canonical_bytes()).unwrap();
    for case in 0..6 {
        let mut v = value.clone();
        match case {
            0 => v["active_root"] = "/var/lib/tickwire/config-other".into(),
            1 => v["checksum"] = format!("sha256:{}", "0".repeat(64)).into(),
            2 => v["extra"] = true.into(),
            3 => v["generation"] = "01".into(),
            4 => v["migration_id"] = "not-a-uuid".into(),
            _ => v["locator_version"] = 2.into(),
        }
        assert!(
            Locator::decode(Format::Json, &serde_json::to_vec(&v).unwrap()).is_err(),
            "case {case}"
        );
    }
    let duplicate = a.canonical_bytes().strip_suffix(b"}").unwrap();
    let mut duplicate = duplicate.to_vec();
    duplicate.extend_from_slice(b",\"generation\":\"1\"}");
    assert!(Locator::decode(Format::Json, &duplicate).is_err());
    assert!(Locator::decode(Format::Toml, &[b'x'; MAX_BYTES + 1]).is_err());
}
