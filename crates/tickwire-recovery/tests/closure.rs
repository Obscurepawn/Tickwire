use tickwire_recovery::*;

fn chunk(stream: [u8; 16], first: u64, prior: Hash, values: &[(u8, &[u8])]) -> (Vec<u8>, Hash) {
    let mut bytes = b"TWINP001".to_vec();
    bytes.extend(stream);
    bytes.extend(first.to_be_bytes());
    bytes.extend((values.len() as u32).to_be_bytes());
    bytes.extend(prior);
    let mut last = prior;
    for (i, (kind, payload)) in values.iter().enumerate() {
        let mut row = (first + i as u64).to_be_bytes().to_vec();
        row.push(*kind);
        row.extend((payload.len() as u32).to_be_bytes());
        row.extend_from_slice(payload);
        let mut chained = b"tickwire.input-row.v1".to_vec();
        chained.extend(last);
        chained.extend_from_slice(&row);
        last = digest(&chained);
        bytes.extend(row);
    }
    (bytes, last)
}
fn fixture() -> (Persistence<MockBlobStore>, RecoveryManifest) {
    let mut p = Persistence::new(MockBlobStore::default());
    let mut objects = vec![];
    for kind in 1..=11 {
        let bytes = vec![kind; kind as usize + 1];
        objects.push(DependencyObject {
            kind,
            content_hash: p.put_object(&bytes).unwrap(),
            byte_length: bytes.len() as u64,
            schema_id: 1,
            owner_identity: [1; 16],
        });
    }
    let (bytes, last) = chunk(
        [9; 16],
        1,
        [0; 32],
        &[
            (1, b"quote"),
            (2, b"fill"),
            (3, b"timer"),
            (4, b"config"),
            (5, b"model"),
        ],
    );
    objects.push(DependencyObject {
        kind: 12,
        content_hash: p.put_object(&bytes).unwrap(),
        byte_length: bytes.len() as u64,
        schema_id: 1,
        owner_identity: [1; 16],
    });
    let m = RecoveryManifest {
        binding: ClosureBinding {
            risk_domain_id: [2; 16],
            intent_id: 3,
            intent_exec_epoch: 4,
            business_digest: objects[0].content_hash,
            strategy_generation: 5,
            config_revision: 6,
            recovery_object: 7,
        },
        logical_strategy_id: 1,
        instance_id: [3; 16],
        action_grant_ref: objects[3].content_hash,
        operation_ref: objects[6].content_hash,
        mode: RecoveryMode::DeterministicReplay,
        fault_model_id: [4; 32],
        pin_id: [5; 16],
        input_boundary: InputBoundary {
            stream: [9; 16],
            first_sequence: 1,
            previous_hash: [0; 32],
            last_sequence: 5,
            last_hash: last,
        },
        objects,
        edges: (1..12).map(|i| (0, i)).collect(),
    };
    (p, m)
}
#[test]
fn complete_closure_publishes_and_reopens_exact_binding() {
    let (mut p, m) = fixture();
    let receipt = p.publish(&m).unwrap();
    assert_eq!(receipt.binding(), &m.binding);
    let mut p = Persistence::new(p.into_store());
    assert_eq!(p.verify(&m.binding).unwrap(), receipt);
    let mut wrong = m.binding;
    wrong.intent_exec_epoch += 1;
    assert!(p.verify(&wrong).is_err());
}

#[test]
fn command_dependency_is_checked_and_preserved_in_private_capability() {
    let (mut p, mut m) = fixture();
    let bytes = b"immutable-command";
    let hash = p.put_object(bytes).unwrap();
    m.objects.push(DependencyObject {
        kind: COMMAND_DEPENDENCY_KIND,
        content_hash: hash,
        byte_length: bytes.len() as u64,
        schema_id: 1,
        owner_identity: [1; 16],
    });
    m.edges.push((0, 12));
    let cap = p.publish(&m).unwrap();
    assert_eq!(cap.command_digest(), Some(hash));
    assert_eq!(p.verify(&m.binding).unwrap(), cap);
    assert_eq!(
        RecoveryManifest::decode(&m.canonical_bytes().unwrap()).unwrap(),
        m
    );

    let (mut p, mut missing) = fixture();
    missing.objects.push(m.objects[12]);
    assert!(p.publish(&missing).is_err());
    assert!(p.verify(&missing.binding).is_err());

    let (mut p, mut ambiguous) = fixture();
    for bytes in [b"first-command".as_slice(), b"second-command".as_slice()] {
        ambiguous.objects.push(DependencyObject {
            kind: COMMAND_DEPENDENCY_KIND,
            content_hash: p.put_object(bytes).unwrap(),
            byte_length: bytes.len() as u64,
            schema_id: 1,
            owner_identity: [1; 16],
        });
    }
    ambiguous
        .objects
        .sort_unstable_by_key(|o| (o.kind, o.content_hash));
    assert!(p.publish(&ambiguous).is_err());
}
#[test]
fn binding_is_immutable_and_identical_publish_is_idempotent() {
    let (mut p, m) = fixture();
    let a = p.publish(&m).unwrap();
    assert_eq!(p.publish(&m).unwrap(), a);
    let mut conflict = m.clone();
    conflict.binding.intent_id += 1;
    assert!(p.publish(&conflict).is_err());
    assert_eq!(p.verify(&m.binding).unwrap(), a);
}
#[test]
fn missing_kind_cycles_missing_bytes_and_wrong_binding_are_rejected() {
    let (mut p, m) = fixture();
    let mut bad = m.clone();
    bad.objects.remove(8);
    bad.edges.clear();
    assert!(p.publish(&bad).is_err());
    let mut bad = m.clone();
    bad.edges.extend([(1, 2), (2, 1)]);
    bad.edges.sort_unstable();
    assert!(p.publish(&bad).is_err());
    let mut bad = m.clone();
    bad.objects[1].content_hash = [99; 32];
    assert!(p.publish(&bad).is_err());
    let mut bad = m.clone();
    bad.binding.business_digest = [8; 32];
    assert!(p.publish(&bad).is_err());
    let mut bad = m.clone();
    bad.action_grant_ref = [8; 32];
    assert!(p.publish(&bad).is_err());
    assert!(p.verify(&m.binding).is_err());
}
#[test]
fn decision_coverage_comes_from_actual_bytes() {
    let (mut p, m) = fixture();
    for change in 0..4 {
        let mut bad = m.clone();
        match change {
            0 => bad.input_boundary.last_sequence += 1,
            1 => bad.input_boundary.last_hash = [99; 32],
            2 => bad.input_boundary.first_sequence += 1,
            _ => bad.input_boundary.stream = [8; 16],
        }
        assert!(p.publish(&bad).is_err());
    }
    let mut good = m.clone();
    good.mode = RecoveryMode::DeclaredRebuild;
    assert!(p.publish(&good).is_ok());
}
#[test]
fn failed_manifest_sync_cannot_produce_binding() {
    let (p, m) = fixture();
    let mut store = p.into_store();
    store.fail_next_put = true;
    let mut p = Persistence::new(store);
    assert!(p.publish(&m).is_err());
    assert!(p.verify(&m.binding).is_err());
    assert!(p.publish(&m).is_ok());
}
#[test]
fn revalidation_reestablishes_storage_completion_before_issuing_capability() {
    let (mut p, m) = fixture();
    p.publish(&m).unwrap();
    let mut store = p.into_store();
    store.fail_next_put = true;
    let mut p = Persistence::new(store);
    assert!(p.verify(&m.binding).is_err());
}
#[test]
fn canonical_manifest_rejects_every_truncation_and_nonminimal_schema() {
    let (_, m) = fixture();
    let bytes = m.canonical_bytes().unwrap();
    let golden = include_str!("manifest.hex").trim();
    let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    assert_eq!(hex, golden, "independently encoded canonical bytes");
    assert_eq!(RecoveryManifest::decode(&bytes).unwrap(), m);
    for n in 0..bytes.len() {
        assert!(RecoveryManifest::decode(&bytes[..n]).is_err(), "cut {n}");
    }
    let mut bad = bytes.clone();
    bad.push(0);
    assert!(RecoveryManifest::decode(&bad).is_err());
    // Array header, bstr(26) header and domain precede schema=1.
    assert_eq!(bytes[29], 1);
    let mut bad = bytes.clone();
    bad.splice(29..30, [0x18, 1]);
    assert!(RecoveryManifest::decode(&bad).is_err());
    let mut bad = bytes;
    bad[0] = 0x9f;
    assert!(RecoveryManifest::decode(&bad).is_err());
}
#[test]
fn split_input_chunks_are_ordered_by_sequence_and_gaps_overlaps_fail() {
    let (mut p, mut m) = fixture();
    let a = encode_inputs(
        [9; 16],
        1,
        [0; 32],
        &[InputRecord {
            kind: 1,
            payload: b"first",
        }],
    )
    .unwrap();
    let b = encode_inputs(
        [9; 16],
        2,
        a.boundary.last_hash,
        &[InputRecord {
            kind: 2,
            payload: b"second",
        }],
    )
    .unwrap();
    m.objects.pop();
    m.edges.clear();
    for bytes in [&a.bytes, &b.bytes] {
        m.objects.push(DependencyObject {
            kind: 12,
            content_hash: p.put_object(bytes).unwrap(),
            byte_length: bytes.len() as u64,
            schema_id: 1,
            owner_identity: [1; 16],
        });
    }
    m.objects.sort_unstable_by_key(|o| (o.kind, o.content_hash));
    m.input_boundary.last_sequence = 2;
    m.input_boundary.last_hash = b.boundary.last_hash;
    assert!(p.publish(&m).is_ok());
    for first in [1, 3] {
        let mut bad = m.clone();
        bad.binding.recovery_object += first;
        let c = encode_inputs(
            [9; 16],
            first,
            a.boundary.last_hash,
            &[InputRecord {
                kind: 2,
                payload: b"second",
            }],
        )
        .unwrap();
        bad.objects.retain(|o| o.content_hash != digest(&b.bytes));
        bad.objects.push(DependencyObject {
            kind: 12,
            content_hash: p.put_object(&c.bytes).unwrap(),
            byte_length: c.bytes.len() as u64,
            schema_id: 1,
            owner_identity: [1; 16],
        });
        bad.objects
            .sort_unstable_by_key(|o| (o.kind, o.content_hash));
        bad.input_boundary.last_sequence = c.boundary.last_sequence;
        bad.input_boundary.last_hash = c.boundary.last_hash;
        assert!(p.publish(&bad).is_err(), "first {first}");
    }
}
#[test]
fn malformed_input_bytes_with_correct_object_hash_are_rejected() {
    let (mut p, m) = fixture();
    let original = chunk(
        [9; 16],
        1,
        [0; 32],
        &[
            (1, b"quote"),
            (2, b"fill"),
            (3, b"timer"),
            (4, b"config"),
            (5, b"model"),
        ],
    )
    .0;
    for n in 1..original.len() {
        let mut bad = m.clone();
        bad.objects[11].content_hash = p.put_object(&original[..n]).unwrap();
        bad.objects[11].byte_length = n as u64;
        assert!(p.publish(&bad).is_err(), "input cut {n}");
    }
    let mut raw = original;
    raw[76] = 6;
    let mut bad = m;
    bad.objects[11].content_hash = p.put_object(&raw).unwrap();
    assert!(p.publish(&bad).is_err());
}
#[test]
fn descriptor_and_graph_bounds_precede_blob_reads() {
    let (mut p, m) = fixture();
    for variant in 0..8 {
        let mut bad = m.clone();
        match variant {
            0 => bad.objects[2].byte_length = MAX_OBJECT_BYTES as u64 + 1,
            1 => bad.objects[2].schema_id = 0,
            2 => bad.objects.swap(2, 3),
            3 => bad.edges.push((1, 1)),
            4 => bad.edges.push((1, 4000)),
            5 => bad.edges.push(bad.edges[0]),
            6 => bad.input_boundary.last_sequence = u64::MAX,
            _ => bad.binding.intent_exec_epoch = 1 << 63,
        }
        assert!(p.publish(&bad).is_err(), "variant {variant}");
    }
}
#[test]
fn partial_binding_publication_retries_sync_before_receipt() {
    struct LateFailure {
        inner: MockBlobStore,
        fail: bool,
        puts: usize,
    }
    impl BlobStore for LateFailure {
        fn get(&self, k: StoreKey, n: usize) -> Result<Vec<u8>> {
            self.inner.get(k, n)
        }
        fn put_sync(&mut self, k: StoreKey, b: &[u8]) -> Result<()> {
            self.puts += 1;
            self.inner.put_sync(k, b)?;
            if matches!(k, StoreKey::Binding(_)) && std::mem::take(&mut self.fail) {
                return Err(std::io::Error::other("directory sync failure after rename").into());
            }
            Ok(())
        }
    }
    let (p, m) = fixture();
    let mut p = Persistence::new(LateFailure {
        inner: p.into_store(),
        fail: true,
        puts: 0,
    });
    assert!(p.publish(&m).is_err());
    let mut store = p.into_store();
    assert_eq!(store.puts, 14);
    store.fail = true;
    let mut p = Persistence::new(store);
    assert!(p.verify(&m.binding).is_err());
    assert!(p.verify(&m.binding).is_ok());
    assert_eq!(p.into_store().puts, 42);
}
#[test]
fn file_store_reopens_pins_and_detects_corruption_with_real_bytes() {
    use std::{
        fs,
        sync::atomic::{AtomicU64, Ordering},
    };
    static NEXT: AtomicU64 = AtomicU64::new(1);
    struct Dir(std::path::PathBuf);
    impl Drop for Dir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    let dir = Dir(std::env::temp_dir().join(format!(
        "tickwire-deps-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )));
    let (source, m) = fixture();
    let source = source.into_store();
    let mut p = Persistence::new(FileBlobStore::open(&dir.0).unwrap());
    assert!(FileBlobStore::open(&dir.0).is_err(), "exclusive lock");
    for o in &m.objects {
        p.put_object(
            &source
                .get(StoreKey::Object(o.content_hash), MAX_OBJECT_BYTES)
                .unwrap(),
        )
        .unwrap();
    }
    let receipt = p.publish(&m).unwrap();
    drop(p);
    let mut p = Persistence::new(FileBlobStore::open(&dir.0).unwrap());
    assert_eq!(p.verify(&m.binding).unwrap(), receipt);
    drop(p);
    let name: String = m.objects[2]
        .content_hash
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    fs::write(dir.0.join(format!("object-{name}")), b"bad").unwrap();
    let mut p = Persistence::new(FileBlobStore::open(&dir.0).unwrap());
    assert!(p.verify(&m.binding).is_err());
    assert_eq!(
        fs::read(dir.0.join(format!("object-{name}"))).unwrap(),
        b"bad"
    );
}
