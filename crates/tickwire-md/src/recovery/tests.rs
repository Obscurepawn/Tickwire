use super::*;
use std::path::Path;
use tickwire_market_source::protocol::wire_bytes;
fn fixture() -> (tickwire_control_sys::PrivateDirectory, Snapshot) {
    let root = tickwire_control_sys::PrivateDirectory::create_in(Path::new("/tmp"), "md-recovery")
        .unwrap();
    let mut config: crate::config::Config =
        serde_json::from_str(include_str!("../../examples/md.json")).unwrap();
    config.topics.truncate(1);
    for feed in &mut config.feeds {
        feed.topics = vec![101];
    }
    let mut spec: crate::config::ResourceSpec =
        serde_json::from_str(include_str!("../../examples/topic-101.json")).unwrap();
    config.identity.arch = if cfg!(target_arch = "x86_64") { 1 } else { 2 };
    spec.identity = config.identity.clone();
    spec.catalog_version = config.feeds[0].instrument.catalog_version;
    let bank = spec.header().unwrap();
    let snapshot = Snapshot {
        schema: 1,
        sequence: 1,
        previous: [0; 32],
        phase: Phase::Starting,
        revision: 1,
        config_hash: crate::expansion::config_hash(&config).unwrap(),
        instrument_ids: config.feeds.iter().map(|f| f.instrument.id).collect(),
        banks: vec![wire_bytes(&bank).unwrap()],
        operations: vec![],
        reserved_owners: config.feeds.iter().map(|feed| feed.owner_id).collect(),
        config,
    };
    (root, snapshot)
}
#[test]
fn actual_store_reopens_the_exact_chain_and_preserves_history() {
    fn require_send<T: Send>() {}
    require_send::<Store>();
    let (root, mut value) = fixture();
    let path = root.path().join("history");
    let mut store = Store::open(&path).unwrap();
    assert!(store.current().is_none());
    assert!(Store::open(&path).is_err());
    let first = store.write(value.clone()).unwrap();
    value.previous = first;
    value.sequence = 2;
    value.phase = Phase::Ready;
    value.operations.push(Consumed {
        operation_id: [20; 16],
        approval_digest: [21; 32],
    });
    let second = store.write(value.clone()).unwrap();
    assert_ne!(first, second);
    drop(store);
    let reopened = Store::open(&path).unwrap();
    assert_eq!(reopened.current_hash(), Some(second));
    assert_eq!(
        serde_json::to_vec(reopened.current().unwrap()).unwrap(),
        serde_json::to_vec(&value).unwrap()
    );
    assert!(reopened.seen_instance(value.config.identity.instance));
    assert!(reopened.seen_owner(value.config.feeds[0].owner_id));
    assert!(!reopened.seen_instance([99; 16]));
}

fn listing(path: &Path) -> Vec<(String, Vec<u8>)> {
    let mut entries = std::fs::read_dir(path)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            (
                entry.file_name().into_string().unwrap(),
                std::fs::read(entry.path()).unwrap(),
            )
        })
        .collect::<Vec<_>>();
    entries.sort();
    entries
}
fn object_name(hash: Hash) -> String {
    format!(
        "object-{}",
        hash.iter().map(|b| format!("{b:02x}")).collect::<String>()
    )
}
fn private_file(path: &Path, bytes: &[u8]) {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(path)
        .unwrap()
        .write_all(bytes)
        .unwrap();
}
fn rehash(value: &mut Snapshot) {
    value.config_hash = crate::expansion::config_hash(&value.config).unwrap();
}
fn bank_mutate(value: &mut Snapshot, mutate: impl FnOnce(&mut tickwire_ipc::ResourceHeader)) {
    use tickwire_ipc::Wire;
    let mut header = tickwire_ipc::ResourceHeader::decode(&value.banks[0]).unwrap();
    mutate(&mut header);
    value.banks[0] = wire_bytes(&header).unwrap();
}
#[test]
fn input_rejections_do_not_write_or_poison_a_healthy_store() {
    let (root, valid) = fixture();
    let path = root.path().join("history");
    let mut store = Store::open(&path).unwrap();
    let before = listing(&path);
    let mutations: &[fn(&mut Snapshot)] = &[
        |s| s.schema = 2,
        |s| s.sequence = 0,
        |s| s.sequence = 2,
        |s| s.sequence = 1 << 63,
        |s| s.previous = [1; 32],
        |s| s.revision = 0,
        |s| s.revision = 1 << 63,
        |s| s.config_hash = [0; 32],
        |s| {
            s.config.clock_generation += 1;
        },
        |s| {
            s.instrument_ids.pop();
        },
        |s| s.instrument_ids.push(s.instrument_ids[0]),
        |s| s.instrument_ids[0] = 0,
        |s| s.banks.clear(),
        |s| {
            s.banks[0].pop();
        },
        |s| s.banks.push(s.banks[0].clone()),
        |s| bank_mutate(s, |h| h.producer_instance = [44; 16]),
        |s| bank_mutate(s, |h| h.topic_id = 999),
        |s| bank_mutate(s, |h| h.catalog_version += 1),
        |s| {
            s.config.feeds[0].owner_id = [0; 16];
            rehash(s);
        },
        |s| {
            s.config.feeds[0].owner_id = s.config.feeds[1].owner_id;
            rehash(s);
        },
        |s| {
            s.config.feeds[0].topics.push(101);
            rehash(s);
        },
        |s| {
            s.config.feeds[0].topics.push(999);
            rehash(s);
        },
        |s| {
            s.operations.push(Consumed {
                operation_id: [0; 16],
                approval_digest: [1; 32],
            })
        },
        |s| {
            s.operations.push(Consumed {
                operation_id: [1; 16],
                approval_digest: [0; 32],
            })
        },
        |s| {
            s.operations = vec![
                Consumed {
                    operation_id: [1; 16],
                    approval_digest: [1; 32]
                };
                2
            ]
        },
        |s| {
            s.operations = (0..129u16)
                .map(|id| {
                    let mut operation_id = [1; 16];
                    operation_id[..2].copy_from_slice(&id.to_le_bytes());
                    Consumed {
                        operation_id,
                        approval_digest: [1; 32],
                    }
                })
                .collect()
        },
    ];
    for (index, mutate) in mutations.iter().enumerate() {
        let mut next = valid.clone();
        mutate(&mut next);
        assert!(store.write(next).is_err(), "mutation {index}");
        assert_eq!(listing(&path), before, "mutation {index}");
        assert!(store.current().is_none());
    }
    store.write(valid).unwrap();
}
#[test]
fn sole_owner_unlock_is_not_extended_by_an_inherited_descriptor() {
    let (root, _) = fixture();
    let path = root.path().join("history");
    let owner = Store::open(&path).unwrap();
    let inherited = owner.inherited_lock();
    assert!(Store::open(&path).is_err());
    drop(owner);
    let successor = Store::open(&path).unwrap();
    drop(inherited);
    assert!(Store::open(&path).is_err());
    drop(successor);
    Store::open(&path).unwrap();
}
#[test]
fn full_history_retains_old_instances_resources_and_source_owners() {
    use tickwire_ipc::Wire;
    let (root, mut next) = fixture();
    let path = root.path().join("history");
    let mut store = Store::open(&path).unwrap();
    next.phase = Phase::Ready;
    next.config.feeds[0].events.push(crate::config::EventFeed {
        owner_id: [90; 16],
        stream_id: 99,
        kind: crate::config::EventType::Trade,
        first_connection_epoch: 1,
        first_operation: 1,
        stream_address: "127.0.0.1:443".into(),
        sbe_api_key_file: None,
        fast_mark: false,
    });
    next.reserved_owners.push([90; 16]);
    rehash(&mut next);
    let original = next.clone();
    next.previous = store.write(next.clone()).unwrap();
    next.sequence = 2;
    next.phase = Phase::Starting;
    next.config.identity.instance = [77; 16];
    for (index, feed) in next.config.feeds.iter_mut().enumerate() {
        feed.owner_id = [80 + index as u8; 16];
        for event in &mut feed.events {
            event.owner_id = [100 + index as u8; 16];
        }
    }
    next.reserved_owners.extend([[80; 16], [81; 16], [100; 16]]);
    bank_mutate(&mut next, |h| {
        h.producer_instance = [77; 16];
        h.resource_id = [79; 16];
        h.channel_id = [78; 16];
    });
    rehash(&mut next);
    store.write(next.clone()).unwrap();
    drop(store);
    let store = Store::open(&path).unwrap();
    for snapshot in [&original, &next] {
        assert!(store.seen_instance(snapshot.config.identity.instance));
        assert!(
            store.seen_resource(
                tickwire_ipc::ResourceHeader::decode(&snapshot.banks[0])
                    .unwrap()
                    .resource_id
            )
        );
        assert!(
            store.seen_channel(
                tickwire_ipc::ResourceHeader::decode(&snapshot.banks[0])
                    .unwrap()
                    .channel_id
            )
        );
        for feed in &snapshot.config.feeds {
            assert!(store.seen_owner(feed.owner_id));
            for event in &feed.events {
                assert!(store.seen_owner(event.owner_id));
            }
        }
    }
}
#[test]
fn recovery_refuses_missing_changed_and_noncanonical_heads() {
    let mutations: &[fn(&mut Vec<u8>)] = &[
        |bytes| bytes.push(b' '),
        |bytes| bytes.clear(),
        |bytes| *bytes = b"{}".to_vec(),
        |bytes| {
            bytes
                .splice(1..1, b"\"unknown\":0,".iter().copied())
                .for_each(drop)
        },
        |bytes| {
            bytes
                .splice(1..1, b"\"schema\":1,".iter().copied())
                .for_each(drop)
        },
        |bytes| {
            let mut head: serde_json::Value = serde_json::from_slice(bytes).unwrap();
            head["bytes"] = 1.into();
            *bytes = serde_json::to_vec(&head).unwrap();
        },
        |bytes| {
            let mut head: serde_json::Value = serde_json::from_slice(bytes).unwrap();
            head["sequence"] = 2.into();
            *bytes = serde_json::to_vec(&head).unwrap();
        },
    ];
    for mutate in mutations {
        let (root, value) = fixture();
        let path = root.path().join("history");
        let mut store = Store::open(&path).unwrap();
        store.write(value).unwrap();
        drop(store);
        let mut head = std::fs::read(path.join("HEAD")).unwrap();
        mutate(&mut head);
        std::fs::write(path.join("HEAD"), head).unwrap();
        assert!(Store::open(&path).is_err());
    }
}
#[test]
fn recovery_verifies_every_predecessor_and_rejects_orphans() {
    for case in 0..5 {
        let (root, mut value) = fixture();
        let path = root.path().join("history");
        let mut store = Store::open(&path).unwrap();
        let first = store.write(value.clone()).unwrap();
        value.previous = first;
        value.sequence = 2;
        store.write(value).unwrap();
        drop(store);
        match case {
            0 => std::fs::remove_file(path.join(object_name(first))).unwrap(),
            1 => std::fs::write(path.join(object_name(first)), b"broken").unwrap(),
            2 => {
                let file = path.join(object_name(first));
                let mut bytes = std::fs::read(&file).unwrap();
                bytes[1] ^= 1;
                std::fs::write(file, bytes).unwrap();
            }
            3 => std::fs::remove_file(path.join("HEAD")).unwrap(),
            4 => private_file(&path.join(object_name([99; 32])), b"orphan"),
            _ => unreachable!(),
        }
        assert!(Store::open(&path).is_err(), "case {case}");
    }
}
#[test]
fn file_type_permissions_and_unknown_entries_are_rejected() {
    use std::os::unix::fs::{PermissionsExt, symlink};
    for case in 0..8 {
        let (root, value) = fixture();
        let path = root.path().join("history");
        let mut store = Store::open(&path).unwrap();
        let hash = store.write(value).unwrap();
        drop(store);
        let file = path.join(object_name(hash));
        match case {
            0 => private_file(&path.join("unknown"), b"x"),
            1 => std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap(),
            2 => std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o644)).unwrap(),
            3 => {
                std::fs::rename(&file, root.path().join("other")).unwrap();
                symlink(root.path().join("other"), &file).unwrap();
            }
            4 => std::fs::hard_link(&file, root.path().join("alias")).unwrap(),
            5 => std::fs::create_dir(path.join("HEAD.pending")).unwrap(),
            6 => {
                assert!(
                    std::process::Command::new("mkfifo")
                        .arg("-m")
                        .arg("600")
                        .arg(path.join("extra"))
                        .status()
                        .unwrap()
                        .success()
                );
            }
            7 => std::fs::OpenOptions::new()
                .write(true)
                .open(&file)
                .unwrap()
                .set_len(MAX_SNAPSHOT_BYTES as u64 + 1)
                .unwrap(),
            _ => unreachable!(),
        }
        assert!(Store::open(&path).is_err(), "case {case}");
    }
}
#[test]
fn directory_or_lock_replacement_poison_the_original_writer() {
    use std::os::unix::fs::DirBuilderExt;
    for replace_lock in [false, true] {
        let (root, value) = fixture();
        let path = root.path().join("history");
        let mut store = Store::open(&path).unwrap();
        if replace_lock {
            std::fs::rename(path.join("LOCK"), root.path().join("old-lock")).unwrap();
            private_file(&path.join("LOCK"), b"");
        } else {
            std::fs::rename(&path, root.path().join("old-directory")).unwrap();
            std::fs::DirBuilder::new()
                .mode(0o700)
                .create(&path)
                .unwrap();
        }
        assert!(store.write(value.clone()).is_err());
        assert!(!path.join("HEAD").exists());
        if replace_lock {
            std::fs::remove_file(path.join("LOCK")).unwrap();
            std::fs::rename(root.path().join("old-lock"), path.join("LOCK")).unwrap();
        } else {
            std::fs::remove_dir(&path).unwrap();
            std::fs::rename(root.path().join("old-directory"), &path).unwrap();
        }
        assert!(store.write(value).is_err());
        assert!(store.current().is_none());
    }
}
#[test]
fn unexpected_disk_contents_poison_without_advancing_current() {
    let (root, mut value) = fixture();
    let path = root.path().join("history");
    let mut store = Store::open(&path).unwrap();
    let first = store.write(value.clone()).unwrap();
    value.sequence = 2;
    value.previous = first;
    private_file(&path.join("HEAD.pending"), b"interrupted");
    assert!(store.write(value.clone()).is_err());
    assert_eq!(store.current_hash(), Some(first));
    std::fs::remove_file(path.join("HEAD.pending")).unwrap();
    assert!(store.write(value).is_err());
    drop(store);
    assert_eq!(Store::open(&path).unwrap().current_hash(), Some(first));
}

fn historical_base(value: &mut Snapshot) {
    value.phase = Phase::Ready;
    value.revision = 3;
    value.operations = vec![
        Consumed {
            operation_id: [20; 16],
            approval_digest: [21; 32],
        },
        Consumed {
            operation_id: [22; 16],
            approval_digest: [23; 32],
        },
    ];
    bank_mutate(value, |bank| {
        bank.ring_epoch = 3;
        bank.feed_generation = 3;
    });
}
fn continuation_mutations() -> Vec<fn(&mut Snapshot)> {
    vec![
        |s| {
            s.operations.pop();
        },
        |s| s.operations[0].approval_digest = [42; 32],
        |s| s.operations[0].operation_id = [42; 16],
        |s| s.instrument_ids.reverse(),
        |s| s.revision -= 1,
        |s| bank_mutate(s, |h| h.ring_epoch -= 1),
        |s| bank_mutate(s, |h| h.feed_generation -= 1),
        |s| bank_mutate(s, |h| h.resource_id = [49; 16]),
        |s| bank_mutate(s, |h| h.created_mono_ns += 1),
        |s| {
            s.config.identity.instance = [77; 16];
            bank_mutate(s, |h| h.producer_instance = [77; 16]);
            rehash(s);
        },
        |s| {
            s.phase = Phase::Starting;
            s.revision += 1;
            s.config.identity.instance = [77; 16];
            bank_mutate(s, |h| h.producer_instance = [77; 16]);
            rehash(s);
        },
    ]
}
#[test]
fn old_facts_cannot_be_rewritten_by_a_new_durable_snapshot() {
    for mutate in continuation_mutations() {
        let (root, mut value) = fixture();
        historical_base(&mut value);
        let path = root.path().join("history");
        let mut store = Store::open(&path).unwrap();
        value.previous = store.write(value.clone()).unwrap();
        value.sequence = 2;
        let before = listing(&path);
        let mut invalid = value.clone();
        mutate(&mut invalid);
        assert!(store.write(invalid).is_err());
        assert_eq!(listing(&path), before);
        value.operations.reverse();
        store.write(value).unwrap();
    }
}
#[derive(Serialize)]
struct TestHead {
    schema: u16,
    sequence: u64,
    digest: Hash,
    bytes: usize,
}
fn replace_head(path: &Path, snapshot: &Snapshot, previous_tip: Hash) {
    use sha2::{Digest, Sha256};
    let bytes = serde_json::to_vec(snapshot).unwrap();
    let digest = Sha256::digest(&bytes).into();
    std::fs::remove_file(path.join(object_name(previous_tip))).unwrap();
    private_file(&path.join(object_name(digest)), &bytes);
    let head = TestHead {
        schema: 1,
        sequence: snapshot.sequence,
        digest,
        bytes: bytes.len(),
    };
    std::fs::write(path.join("HEAD"), serde_json::to_vec(&head).unwrap()).unwrap();
}
#[test]
fn recovery_rechecks_continuity_even_when_all_object_hashes_are_valid() {
    for mutate in continuation_mutations() {
        let (root, mut value) = fixture();
        historical_base(&mut value);
        let path = root.path().join("history");
        let mut store = Store::open(&path).unwrap();
        value.previous = store.write(value.clone()).unwrap();
        value.sequence = 2;
        let tip = store.write(value.clone()).unwrap();
        drop(store);
        mutate(&mut value);
        replace_head(&path, &value, tip);
        assert!(Store::open(&path).is_err());
    }
}
#[test]
fn a_nonzero_bank_descriptor_must_match_its_exact_wire_content() {
    let (root, mut value) = fixture();
    let mut store = Store::open(&root.path().join("history")).unwrap();
    bank_mutate(&mut value, |h| h.descriptor_hash = [99; 32]);
    assert!(store.write(value.clone()).is_err());
    bank_mutate(&mut value, |h| {
        use sha2::{Digest, Sha256};
        h.descriptor_hash = [0; 32];
        h.descriptor_hash = Sha256::digest(wire_bytes(h).unwrap()).into();
    });
    store.write(value).unwrap();
}

#[test]
fn recovery_rejects_disk_object_count_and_byte_budgets_before_decoding() {
    for count_limit in [false, true] {
        let (root, _) = fixture();
        let path = root.path().join("history");
        drop(Store::open(&path).unwrap());
        let count = if count_limit {
            MAX_SNAPSHOTS + 1
        } else {
            MAX_TOTAL_BYTES / MAX_SNAPSHOT_BYTES + 1
        };
        for index in 0..count {
            let mut hash = [11; 32];
            hash[..8].copy_from_slice(&(index as u64).to_le_bytes());
            let file = path.join(object_name(hash));
            private_file(&file, b"x");
            if !count_limit {
                std::fs::OpenOptions::new()
                    .write(true)
                    .open(file)
                    .unwrap()
                    .set_len(MAX_SNAPSHOT_BYTES as u64)
                    .unwrap();
            }
        }
        assert!(Store::open(&path).is_err());
    }
}
#[test]
fn an_oversized_snapshot_never_creates_a_partial_object() {
    let (root, mut value) = fixture();
    let path = root.path().join("history");
    let mut store = Store::open(&path).unwrap();
    let before = listing(&path);
    value.config.feeds[0].stream_address = "x".repeat(MAX_SNAPSHOT_BYTES);
    assert!(store.write(value).is_err());
    assert_eq!(listing(&path), before);
}

#[test]
fn new_instrument_is_appended_without_reassigning_old_dense_slots() {
    let (root, mut value) = fixture();
    let path = root.path().join("history");
    let original = value.instrument_ids.clone();
    let mut store = Store::open(&path).unwrap();
    value.previous = store.write(value.clone()).unwrap();
    value.sequence += 1;
    let mut extra = value.config.feeds[0].clone();
    extra.owner_id = [201; 16];
    extra.stream_id = 77;
    extra.instrument.id = u64::MAX;
    value.config.feeds.insert(0, extra);
    value.reserved_owners.push([201; 16]);
    value.instrument_ids.push(u64::MAX);
    rehash(&mut value);
    let tip = store.write(value).unwrap();
    drop(store);
    let store = Store::open(&path).unwrap();
    assert_eq!(store.current_hash(), Some(tip));
    let current = store.current().unwrap();
    assert!(current.instrument_ids.starts_with(&original));
    assert_eq!(current.instrument_ids.last(), Some(&u64::MAX));
    let index =
        crate::config::FeedIndex::restore(&current.config.feeds, &current.instrument_ids).unwrap();
    assert_eq!(
        index.book_key(&current.config.feeds[0]).unwrap().instrument,
        original.len() as u32
    );
    for (dense, feed) in current.config.feeds[1..].iter().enumerate() {
        assert_eq!(index.book_key(feed).unwrap().instrument, dense as u32);
    }
}
#[test]
fn the_last_allowed_snapshot_reopens_but_the_next_write_is_atomic_rejection() {
    let (root, mut value) = fixture();
    let path = root.path().join("history");
    let mut store = Store::open(&path).unwrap();
    for sequence in 1..=MAX_SNAPSHOTS as u64 {
        value.sequence = sequence;
        value.previous = store.write(value.clone()).unwrap();
    }
    let tip = value.previous;
    drop(store);
    let mut store = Store::open(&path).unwrap();
    assert_eq!(store.current_hash(), Some(tip));
    assert_eq!(store.current().unwrap().sequence, MAX_SNAPSHOTS as u64);
    value.sequence += 1;
    let before = listing(&path);
    assert!(store.write(value).is_err());
    assert_eq!(store.current_hash(), Some(tip));
    assert_eq!(listing(&path), before);
}

#[test]
fn reserved_owners_cover_current_sources_and_reject_invalid_collections() {
    let (root, value) = fixture();
    let path = root.path().join("history");
    let mut store = Store::open(&path).unwrap();
    let before = listing(&path);
    let mutations: &[fn(&mut Snapshot)] = &[
        |s| {
            s.reserved_owners.pop();
        },
        |s| s.reserved_owners.push([0; 16]),
        |s| s.reserved_owners.push(s.reserved_owners[0]),
        |s| {
            s.reserved_owners.extend((0..16384u64).map(|n| {
                let mut id = [42; 16];
                id[..8].copy_from_slice(&n.to_le_bytes());
                id
            }))
        },
    ];
    for mutate in mutations {
        let mut invalid = value.clone();
        mutate(&mut invalid);
        assert!(store.write(invalid).is_err());
        assert_eq!(listing(&path), before);
    }
    store.write(value).unwrap();
}
#[test]
fn cancelled_candidate_owners_remain_consumed_across_reopen() {
    let (root, mut value) = fixture();
    value.reserved_owners.push([42; 16]);
    let path = root.path().join("history");
    let mut store = Store::open(&path).unwrap();
    value.previous = store.write(value.clone()).unwrap();
    value.sequence += 1;
    let before = listing(&path);
    let mut changed = value.clone();
    changed.reserved_owners.pop();
    assert!(store.write(changed.clone()).is_err());
    assert_eq!(listing(&path), before);
    value.reserved_owners.reverse();
    let tip = store.write(value).unwrap();
    drop(store);
    let store = Store::open(&path).unwrap();
    assert!(store.seen_owner([42; 16]));
    drop(store);
    replace_head(&path, &changed, tip);
    assert!(Store::open(&path).is_err());
}
