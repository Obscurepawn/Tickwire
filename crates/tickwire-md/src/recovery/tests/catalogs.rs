use super::*;
use crate::catalogs::fixture as economic;
use sha2::{Digest, Sha256};
fn approved() -> (tickwire_control_sys::PrivateDirectory, Snapshot) {
    let (root, mut value) = fixture();
    value.catalog_objects = vec![economic::install(
        &mut value.config,
        &root.path().join("catalog.json"),
    )];
    rehash(&mut value);
    (root, value)
}
#[test]
fn catalog_dependencies_survive_path_removal_and_cannot_be_downgraded() {
    let (root, mut value) = approved();
    let path = root.path().join("history");
    let mut store = Store::open(&path).unwrap();
    let first = store.write(value.clone()).unwrap();
    std::fs::remove_file(&value.config.catalogs.as_ref().unwrap()[0].path).unwrap();
    value.sequence = 2;
    value.previous = first;
    value.phase = Phase::Ready;
    let before = listing(&path);
    for case in 0..4 {
        let mut bad = value.clone();
        match case {
            0 => bad.catalog_objects.clear(),
            1 => bad.catalog_objects[0].push(b' '),
            2 => {
                bad.catalog_objects.clear();
                bad.config.catalogs = None;
                rehash(&mut bad);
            }
            3 => {
                for f in &mut bad.config.feeds {
                    f.instrument.price_tick = "0.5".into();
                }
                let raw = economic::body(&bad.config.feeds);
                let approval_path = bad.config.catalogs.as_ref().unwrap()[0].path.clone();
                economic::approve(&mut bad.config, &approval_path, &raw);
                bad.catalog_objects = vec![raw];
                rehash(&mut bad);
                // Complete new content is valid by itself; history forbids replacement.
                crate::catalogs::Catalogs::from_objects(&bad.config, &bad.catalog_objects).unwrap();
            }
            _ => unreachable!(),
        }
        assert!(store.write(bad).is_err(), "case {case}");
        assert_eq!(listing(&path), before);
        assert_eq!(store.current_hash(), Some(first));
    }
    let second = store.write(value.clone()).unwrap();
    drop(store);
    let reopened = Store::open(&path).unwrap();
    assert_eq!(reopened.current_hash(), Some(second));
    assert_eq!(
        reopened.current().unwrap().catalog_objects,
        value.catalog_objects
    );
}
#[test]
fn self_consistent_outer_hash_cannot_hide_missing_or_changed_catalog_body() {
    for case in 0..2 {
        let (root, mut value) = approved();
        let path = root.path().join("history");
        let mut store = Store::open(&path).unwrap();
        let old = store.write(value.clone()).unwrap();
        drop(store);
        match case {
            0 => value.catalog_objects.clear(),
            1 => value.catalog_objects[0].push(b' '),
            _ => unreachable!(),
        }
        let bytes = serde_json::to_vec(&value).unwrap();
        let hash: [u8; 32] = Sha256::digest(&bytes).into();
        std::fs::remove_file(path.join(object_name(old))).unwrap();
        private_file(&path.join(object_name(hash)), &bytes);
        #[derive(serde::Serialize)]
        struct Head {
            schema: u16,
            sequence: u64,
            digest: [u8; 32],
            bytes: usize,
        }
        let head = Head {
            schema: 1,
            sequence: 1,
            digest: hash,
            bytes: bytes.len(),
        };
        std::fs::write(path.join("HEAD"), serde_json::to_vec(&head).unwrap()).unwrap();
        assert!(
            Store::open(&path).is_err(),
            "valid outer digest must not hide dependency case {case}"
        );
    }
}
