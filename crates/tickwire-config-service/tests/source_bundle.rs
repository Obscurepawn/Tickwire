#[allow(dead_code)]
mod deployment_support;
use deployment_support::{Fixture, NOW};
use serde_json::{Value, json};
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
