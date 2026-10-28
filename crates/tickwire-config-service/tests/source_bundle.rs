#[allow(dead_code)]
mod deployment_support;
use deployment_support::{Fixture, NOW};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File},
    path::PathBuf,
};
use tickwire_config_service::{
    deployment::{ContentValidated, Limits as ContentLimits},
    source_bundle::{Bundle, Limits},
};

fn pack(header: &Value, objects: &[Vec<u8>]) -> Vec<u8> {
    let h = serde_json::to_vec(header).unwrap();
    let mut b = b"TWDSRC01".to_vec();
    b.extend_from_slice(&(h.len() as u32).to_le_bytes());
    b.extend_from_slice(&h);
    for o in objects {
        b.extend_from_slice(o);
    }
    b
}
fn parts(f: &Fixture) -> (Value, Vec<Vec<u8>>) {
    let entries: Vec<_> = f
        .artifacts
        .objects()
        .map(|(r, b)| json!({"reference":r,"byte_len":b.len()}))
        .collect();
    let objects = f.artifacts.objects().map(|(_, b)| b.to_vec()).collect();
    let signatures: Vec<Value> = f
        .signatures
        .envelopes()
        .map(|(_, _, b)| serde_json::from_slice(b).unwrap())
        .collect();
    (
        json!({"schema":1,"artifacts":entries,"evidence":signatures,"retained_ref":f.retained_ref,"credential_refs":f.credential_refs}),
        objects,
    )
}
struct Temporary(PathBuf);
impl Temporary {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "tickwire-source-bundle-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir(&root).unwrap();
        Self(root)
    }
}
impl Drop for Temporary {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn real_file_loads_one_complete_package_for_content_validation() {
    let f = Fixture::new(|_| {}, |_, _| {});
    let (header, objects) = parts(&f);
    let bytes = pack(&header, &objects);
    let temp = Temporary::new();
    let path = temp.0.join("sources.twdeploy");
    fs::write(&path, &bytes).unwrap();
    let bundle = Bundle::from_file(File::open(&path).unwrap(), Limits::default()).unwrap();
    assert_eq!(bundle.stored_bytes(), bytes.len());
    assert_ne!(bundle.digest(), [0; 32]);
    let checked = ContentValidated::check(
        &f.candidate,
        bundle.sources(),
        &f.trust(1),
        NOW,
        ContentLimits::default(),
    )
    .unwrap();
    assert_eq!(
        checked.sources_digest(),
        f.check(NOW).unwrap().sources_digest()
    );
    // Subsequent path replacement cannot modify the owned source snapshot.
    fs::write(&path, b"broken").unwrap();
    assert!(
        ContentValidated::check(
            &f.candidate,
            bundle.sources(),
            &f.trust(1),
            NOW,
            ContentLimits::default()
        )
        .is_ok()
    );
}
#[test]
fn object_order_changes_package_digest_without_changing_validated_content() {
    let f = Fixture::new(|_| {}, |_, _| {});
    let (mut header, mut objects) = parts(&f);
    let a = Bundle::decode(&pack(&header, &objects), Limits::default()).unwrap();
    header["artifacts"].as_array_mut().unwrap().reverse();
    objects.reverse();
    let b = Bundle::decode(&pack(&header, &objects), Limits::default()).unwrap();
    assert_ne!(a.digest(), b.digest());
    let check = |b: &Bundle| {
        ContentValidated::check(
            &f.candidate,
            b.sources(),
            &f.trust(1),
            NOW,
            ContentLimits::default(),
        )
        .unwrap()
        .sources_digest()
    };
    assert_eq!(check(&a), check(&b));
}
#[test]
fn strict_index_rejects_duplicates_missing_bindings_and_unknown_fields() {
    let f = Fixture::new(|_| {}, |_, _| {});
    let (header, objects) = parts(&f);
    for bad in 0..9 {
        let mut h = header.clone();
        let mut o = objects.clone();
        match bad {
            0=>h["schema"]=2.into(),
            1=>h["unknown"]=true.into(),
            2=>{let e=h["artifacts"][0].clone();h["artifacts"].as_array_mut().unwrap().push(e);o.push(o[0].clone());}
            3=>{let e=h["evidence"][0].clone();h["evidence"].as_array_mut().unwrap().push(e);}
            4=>h["artifacts"][0]["path"]="/etc/passwd".into(),
            5=>h["retained_ref"]="artifact://fixture/missing@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
            6=>{let r=h["credential_refs"][0].clone();h["credential_refs"].as_array_mut().unwrap().push(r);}
            7=>h["evidence"][0]["claim"]["artifact_ref"]="artifact://fixture/missing@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
            _=>h["credential_refs"][0]=h["retained_ref"].clone(),
        }
        assert!(
            Bundle::decode(&pack(&h, &o), Limits::default()).is_err(),
            "bad index {bad}"
        );
    }
    let text = serde_json::to_string(&header)
        .unwrap()
        .replacen("{", "{\"schema\":1,", 1);
    let mut raw = b"TWDSRC01".to_vec();
    raw.extend_from_slice(&(text.len() as u32).to_le_bytes());
    raw.extend_from_slice(text.as_bytes());
    objects.iter().for_each(|o| raw.extend_from_slice(o));
    assert!(Bundle::decode(&raw, Limits::default()).is_err());
}
#[test]
fn malformed_framing_digest_and_limits_cannot_publish_a_bundle() {
    let f = Fixture::new(|_| {}, |_, _| {});
    let (header, objects) = parts(&f);
    let bytes = pack(&header, &objects);
    for end in [0, 7, 8, 11, 12, bytes.len() - 1] {
        assert!(Bundle::decode(&bytes[..end], Limits::default()).is_err());
    }
    let mut corrupt = bytes.clone();
    *corrupt.last_mut().unwrap() ^= 1;
    assert!(Bundle::decode(&corrupt, Limits::default()).is_err());
    let mut extra = bytes.clone();
    extra.push(0);
    assert!(Bundle::decode(&extra, Limits::default()).is_err());
    let mut invalid = bytes.clone();
    invalid[8..12].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(Bundle::decode(&invalid, Limits::default()).is_err());
    let mut l = Limits {
        max_bundle_bytes: bytes.len() - 1,
        ..Limits::default()
    };
    assert!(Bundle::decode(&bytes, l).is_err());
    l = Limits::default();
    l.max_header_bytes = 1;
    assert!(Bundle::decode(&bytes, l).is_err());
    l = Limits::default();
    l.artifacts.max_objects = 1;
    assert!(Bundle::decode(&bytes, l).is_err());
    let temp = Temporary::new();
    assert!(Bundle::from_file(File::open(&temp.0).unwrap(), Limits::default()).is_err());
}
#[test]
fn loaded_signatures_are_still_rechecked_against_current_time_and_trust() {
    let f = Fixture::new(|_| {}, |_, _| {});
    let (mut header, objects) = parts(&f);
    header["evidence"][0]["signature"] = "00".repeat(64).into();
    let bundle = Bundle::decode(&pack(&header, &objects), Limits::default()).unwrap();
    assert!(
        ContentValidated::check(
            &f.candidate,
            bundle.sources(),
            &f.trust(1),
            NOW,
            ContentLimits::default()
        )
        .is_err()
    );
    let (header, objects) = parts(&f);
    let bundle = Bundle::decode(&pack(&header, &objects), Limits::default()).unwrap();
    assert!(
        ContentValidated::check(
            &f.candidate,
            bundle.sources(),
            &f.trust(1),
            NOW + 2000,
            ContentLimits::default()
        )
        .is_err()
    );
}

#[test]
fn canonical_encoding_preserves_all_sources_without_replacing_original_digest() {
    let mut f = Fixture::new(|_| {}, |_, _| {});
    let extra = b"unreferenced archived object\0\xff";
    let reference = format!(
        "artifact://fixture/extra@sha256:{}",
        Sha256::digest(extra)
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>()
    );
    f.artifacts.insert(&reference, extra).unwrap();
    let (mut header, mut objects) = parts(&f);
    let first = Bundle::decode(&pack(&header, &objects), Limits::default()).unwrap();
    header["artifacts"].as_array_mut().unwrap().reverse();
    header["evidence"].as_array_mut().unwrap().reverse();
    header["credential_refs"].as_array_mut().unwrap().reverse();
    objects.reverse();
    let original = pack(&header, &objects);
    let bundle = Bundle::decode(&original, Limits::default()).unwrap();
    let encoded = bundle.encode(Limits::default()).unwrap();
    let header_len = u32::from_le_bytes(encoded[8..12].try_into().unwrap()) as usize;
    let header = &encoded[12..12 + header_len];
    assert_eq!(
        tickwire_config::Candidate::parse(
            tickwire_config::Format::Json,
            header,
            tickwire_config::Limits::default()
        )
        .unwrap()
        .canonical_bytes(),
        header
    );
    assert_eq!(encoded, first.encode(Limits::default()).unwrap());
    assert_ne!(encoded, original);
    assert_eq!(
        bundle.digest().as_slice(),
        Sha256::digest(&original).as_slice()
    );
    assert_eq!(bundle.stored_bytes(), original.len());
    let restored = Bundle::decode(&encoded, Limits::default()).unwrap();
    assert_eq!(
        restored.digest().as_slice(),
        Sha256::digest(&encoded).as_slice()
    );
    assert_eq!(restored.encode(Limits::default()).unwrap(), encoded);
    let before = bundle.sources();
    let after = restored.sources();
    assert_eq!(
        before.artifacts.objects().collect::<Vec<_>>(),
        after.artifacts.objects().collect::<Vec<_>>()
    );
    assert_eq!(
        before.signatures.envelopes().collect::<Vec<_>>(),
        after.signatures.envelopes().collect::<Vec<_>>()
    );
    assert_eq!(before.retained_ref, after.retained_ref);
    let mut roots = before.credential_refs.to_vec();
    roots.sort();
    assert_eq!(roots, after.credential_refs);
    assert_eq!(after.artifacts.get(&reference).unwrap(), extra);
    assert_eq!(
        ContentValidated::check(
            &f.candidate,
            after,
            &f.trust(1),
            NOW,
            ContentLimits::default()
        )
        .unwrap()
        .sources_digest(),
        ContentValidated::check(
            &f.candidate,
            before,
            &f.trust(1),
            NOW,
            ContentLimits::default()
        )
        .unwrap()
        .sources_digest()
    );
}

#[test]
fn encoding_checks_each_output_budget_at_its_exact_boundary() {
    let f = Fixture::new(|_| {}, |_, _| {});
    let (header, objects) = parts(&f);
    let bundle = Bundle::decode(&pack(&header, &objects), Limits::default()).unwrap();
    let bytes = bundle.encode(Limits::default()).unwrap();
    let header_len = u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as usize;
    let exact = Limits {
        max_bundle_bytes: bytes.len(),
        max_header_bytes: header_len,
        artifacts: tickwire_config::artifacts::CatalogLimits {
            max_objects: objects.len(),
            max_object_bytes: objects.iter().map(Vec::len).max().unwrap(),
            max_total_bytes: objects.iter().map(Vec::len).sum(),
        },
    };
    assert_eq!(bundle.encode(exact).unwrap(), bytes);
    assert!(Bundle::decode(&bytes, exact).is_ok());
    for field in 0..5 {
        let mut limits = exact;
        match field {
            0 => limits.max_bundle_bytes -= 1,
            1 => limits.max_header_bytes -= 1,
            2 => limits.artifacts.max_objects -= 1,
            3 => limits.artifacts.max_object_bytes -= 1,
            _ => limits.artifacts.max_total_bytes -= 1,
        }
        assert!(bundle.encode(limits).is_err(), "budget field {field}");
    }
}

#[test]
fn archived_package_roundtrip_does_not_upgrade_signature_or_time_qualification() {
    let f = Fixture::new(|_| {}, |_, _| {});
    let (header, objects) = parts(&f);
    let bundle = Bundle::decode(&pack(&header, &objects), Limits::default()).unwrap();
    let bytes = bundle.encode(Limits::default()).unwrap();
    let restored = Bundle::decode(&bytes, Limits::default()).unwrap();
    let original = f.trust(1);
    let trust = tickwire_config_service::evidence::Trust::decode(
        original.canonical_bytes(),
        original.digest(),
    )
    .unwrap();
    assert!(
        ContentValidated::check(
            &f.candidate,
            restored.sources(),
            &trust,
            NOW,
            ContentLimits::default()
        )
        .is_ok()
    );
    assert!(
        ContentValidated::check(
            &f.candidate,
            restored.sources(),
            &trust,
            NOW + 2000,
            ContentLimits::default()
        )
        .is_err()
    );
    let mut header = header;
    header["evidence"][0]["signature"] = "00".repeat(64).into();
    let damaged = Bundle::decode(&pack(&header, &objects), Limits::default()).unwrap();
    let restored = Bundle::decode(
        &damaged.encode(Limits::default()).unwrap(),
        Limits::default(),
    )
    .unwrap();
    assert_eq!(
        restored
            .sources()
            .signatures
            .envelopes()
            .collect::<Vec<_>>(),
        damaged.sources().signatures.envelopes().collect::<Vec<_>>()
    );
    assert!(
        ContentValidated::check(
            &f.candidate,
            restored.sources(),
            &trust,
            NOW,
            ContentLimits::default()
        )
        .is_err()
    );
}
