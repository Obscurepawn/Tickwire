use super::*;
use std::{
    collections::BTreeMap,
    io,
    sync::{Arc, Mutex},
};
use tickwire_journal::{Config, Journal, store::Store};
use tickwire_recovery::MockBlobStore;
#[derive(Default)]
struct Memory {
    manifest: Option<Vec<u8>>,
    segments: BTreeMap<u64, Vec<u8>>,
    fail_sync: bool,
}
#[derive(Clone, Default)]
struct MemoryStore(Arc<Mutex<Memory>>);
impl Store for MemoryStore {
    fn read_manifest(&mut self) -> io::Result<Option<Vec<u8>>> {
        Ok(self.0.lock().unwrap().manifest.clone())
    }
    fn publish_manifest(&mut self, b: &[u8]) -> io::Result<()> {
        self.0.lock().unwrap().manifest = Some(b.to_vec());
        Ok(())
    }
    fn segment_ids(&mut self) -> io::Result<Vec<u64>> {
        Ok(self.0.lock().unwrap().segments.keys().copied().collect())
    }
    fn read_segment(&mut self, id: u64, max: u64) -> io::Result<Vec<u8>> {
        let m = self.0.lock().unwrap();
        let b = m
            .segments
            .get(&id)
            .ok_or_else(|| io::Error::other("missing"))?;
        if b.len() as u64 > max {
            return Err(io::Error::other("bound"));
        }
        Ok(b.clone())
    }
    fn create_segment(&mut self, id: u64) -> io::Result<()> {
        let mut m = self.0.lock().unwrap();
        if m.segments.contains_key(&id) {
            return Err(io::Error::other("exists"));
        }
        m.segments.insert(id, Vec::new());
        Ok(())
    }
    fn append_segment(&mut self, id: u64, offset: u64, b: &[u8]) -> io::Result<()> {
        let mut m = self.0.lock().unwrap();
        let v = m.segments.get_mut(&id).unwrap();
        if v.len() as u64 != offset {
            return Err(io::Error::other("offset"));
        }
        v.extend_from_slice(b);
        Ok(())
    }
    fn sync_segment(&mut self, _id: u64) -> io::Result<()> {
        if self.0.lock().unwrap().fail_sync {
            Err(io::Error::other("injected sync failure"))
        } else {
            Ok(())
        }
    }
    fn truncate_segment(&mut self, id: u64, n: u64) -> io::Result<()> {
        self.0
            .lock()
            .unwrap()
            .segments
            .get_mut(&id)
            .unwrap()
            .truncate(n as usize);
        Ok(())
    }
}
fn identity() -> tickwire_fencing::ExecutionIdentity {
    tickwire_fencing::ExecutionIdentity {
        host_id: [1; 16],
        boot_id: [2; 16],
        exec_instance_id: [3; 16],
        exec_epoch: 3,
        network_identity: [4; 32],
        credential_access_identity: [5; 32],
    }
}
fn opaque() -> (Stream, Persistence<MockBlobStore>) {
    let mut p = Persistence::new(MockBlobStore::default());
    let baseline = ObjectRef::new(b"initial snapshot").unwrap();
    p.put_object(b"initial snapshot").unwrap();
    (
        Stream {
            domain: [1; 16],
            journal_epoch: [2; 16],
            source: identity(),
            source_storage_id: [3; 16],
            profile_version: 1,
            commit_mode: CommitMode::SyncReplica {
                mirror_id: [7; 16],
                assignment_epoch: 1,
            },
            format: Format::Opaque { record_type: 99 },
            initial_objects: vec![baseline],
        },
        p,
    )
}
fn source_config(mut stream: Stream, mode: CommitMode) -> SourceConfig {
    stream.commit_mode = mode.clone();
    SourceConfig {
        stream,
        profile_version: 1,
        mode,
        max_records: 100,
        max_pending_batches: 1,
        max_pending_bytes: 65536,
    }
}
fn mirror_config(stream: Stream) -> MirrorConfig {
    MirrorConfig {
        stream,
        receiver_id: [7; 16],
        receiver_boot_id: [8; 16],
        mirror_assignment_epoch: 1,
        profile_version: 1,
        max_records: 100,
        physical: Config::new([9; 16], [1; 16]),
    }
}
fn copy<S: BlobStore>(
    source: &SourceWorker<S>,
    mirror: &mut Mirror<MemoryStore, MockBlobStore>,
    batch: &AppendBatch,
) {
    for o in source
        .stream()
        .initial_objects
        .iter()
        .chain(batch.entries.iter().map(|e| &e.payload))
    {
        let b = source.read_object(o).unwrap();
        mirror
            .object_chunk(o.clone(), 0, &b, tickwire_journal::frame::crc32c(&b))
            .unwrap();
    }
}
#[test]
fn actual_export_required_and_sync_failure_cannot_ack() {
    let (s, p) = opaque();
    let mut source = SourceWorker::new(
        source_config(
            s.clone(),
            CommitMode::SyncReplica {
                mirror_id: [7; 16],
                assignment_epoch: 1,
            },
        ),
        p,
    )
    .unwrap();
    let mut journal = Journal::create(
        MemoryStore::default(),
        Config::new(s.journal_epoch, s.domain),
    )
    .unwrap();
    journal.append_batch(3, &[b"one"]).unwrap();
    let export = journal.export_durable(1, 32, 65536).unwrap();
    source.ingest(&export, &BTreeMap::new(), 0).unwrap();
    let batch = source.next_batch().unwrap().clone();
    let store = MemoryStore::default();
    let mut mirror =
        Mirror::create(mirror_config(s), store.clone(), MockBlobStore::default()).unwrap();
    let signer = Signer::from_seed([7; 32]).unwrap();
    assert!(mirror.append(&batch, &signer).is_err());
    assert_eq!(mirror.prefix().sequence, 0);
    copy(&source, &mut mirror, &batch);
    store.0.lock().unwrap().fail_sync = true;
    assert!(mirror.append(&batch, &signer).is_err());
    assert!(mirror.is_poisoned());
    assert_eq!(mirror.prefix().sequence, 0);
    assert!(mirror.ack(1, &signer).is_err());
}
#[test]
fn duplicate_prefix_ack_is_exact_and_new_boot_requires_new_ack() {
    let (s, p) = opaque();
    let mut source = SourceWorker::new(
        source_config(
            s.clone(),
            CommitMode::SyncReplica {
                mirror_id: [7; 16],
                assignment_epoch: 1,
            },
        ),
        p,
    )
    .unwrap();
    let mut journal = Journal::create(
        MemoryStore::default(),
        Config::new(s.journal_epoch, s.domain),
    )
    .unwrap();
    journal.append_batch(3, &[b"one"]).unwrap();
    let proof = source
        .ingest(
            &journal.export_durable(1, 32, 65536).unwrap(),
            &BTreeMap::new(),
            1,
        )
        .unwrap();
    let b = source.next_batch().unwrap().clone();
    let signer = Signer::from_seed([7; 32]).unwrap();
    let mut mirror = Mirror::create(
        mirror_config(s),
        MemoryStore::default(),
        MockBlobStore::default(),
    )
    .unwrap();
    copy(&source, &mut mirror, &b);
    let ack = mirror.append(&b, &signer).unwrap();
    assert_eq!(mirror.append(&b, &signer).unwrap(), ack);
    source
        .bind_peer(&mirror.hello(), signer.public_key())
        .unwrap();
    source.acknowledge(ack.clone()).unwrap();
    assert!(source.replication_ack(&proof).is_some());
    let mut reboot = mirror.hello();
    reboot.receiver_boot_id = [10; 16];
    source.bind_peer(&reboot, signer.public_key()).unwrap();
    assert!(source.replication_ack(&proof).is_none());
    assert!(source.acknowledge(ack).is_err());
    let mut conflict = b;
    conflict.entries[0].payload.hash = [99; 32];
    conflict.end = conflict
        .previous
        .advance(mirror.stream(), &conflict.entries[0])
        .unwrap();
    assert!(matches!(
        mirror.append(&conflict, &signer),
        Err(Error::Conflict)
    ));
    assert!(mirror.is_poisoned());
}
#[test]
fn restore_does_not_fill_live_pending_queue_and_late_restore_rejects() {
    let (s, p) = opaque();
    let mut source = SourceWorker::new(
        source_config(
            s.clone(),
            CommitMode::LocalDurableAsync {
                max_unreplicated_age_ns: 100,
            },
        ),
        p,
    )
    .unwrap();
    let mut journal = Journal::create(
        MemoryStore::default(),
        Config::new(s.journal_epoch, s.domain),
    )
    .unwrap();
    for seq in 1..=4 {
        journal.append_batch(3, &[&[seq as u8]]).unwrap();
        let e = journal.export_durable(seq, 1, 65536).unwrap();
        if seq < 4 {
            source.restore_prefix(&e, &BTreeMap::new(), seq).unwrap();
        } else {
            source.ingest(&e, &BTreeMap::new(), seq).unwrap();
        }
    }
    assert_eq!(source.prefix().sequence, 4);
    assert_eq!(source.oldest_pending_ns(), Some(1));
    assert!(
        source
            .restore_prefix(
                &journal.export_durable(4, 1, 65536).unwrap(),
                &BTreeMap::new(),
                5
            )
            .is_err()
    );
}
#[test]
fn canonical_batches_and_input_prefix_reject_truncation_or_gaps() {
    let (s, _) = opaque();
    let e = Entry {
        sequence: 1,
        writer_epoch: 3,
        kind: 99,
        payload: ObjectRef::new(b"a").unwrap(),
        manifest: None,
        extra_objects: vec![],
    };
    let p = Prefix::genesis(&s);
    let b = AppendBatch {
        stream_digest: s.digest(),
        previous: p,
        end: p.advance(&s, &e).unwrap(),
        entries: vec![e],
    };
    let bytes = b.bytes();
    assert_eq!(AppendBatch::decode(&bytes).unwrap(), b);
    for n in 0..bytes.len() {
        assert!(AppendBatch::decode(&bytes[..n]).is_err());
    }
    let mut trailing = bytes;
    trailing.push(0);
    assert!(AppendBatch::decode(&trailing).is_err());
    let mut input = b"TWIPFX01".to_vec();
    input.extend([1; 16]);
    input.extend([2; 16]);
    input.extend([3; 32]);
    input.extend(1u64.to_le_bytes());
    input.extend(1u32.to_le_bytes());
    input.extend(1u64.to_le_bytes());
    input.extend(3u64.to_le_bytes());
    input.extend(1u64.to_le_bytes());
    input.extend([4; 32]);
    assert_eq!(InputPrefix::decode(&input).unwrap().through_sequence, 1);
    input[84] = 2;
    assert!(InputPrefix::decode(&input).is_err());
}
#[test]
fn closed_source_retains_export_and_required_prefix_blocks_missing_tail() {
    let (s, p) = opaque();
    let mut source = SourceWorker::new(
        source_config(
            s.clone(),
            CommitMode::SyncReplica {
                mirror_id: [7; 16],
                assignment_epoch: 1,
            },
        ),
        p,
    )
    .unwrap();
    let mut journal = Journal::create(
        MemoryStore::default(),
        Config::new(s.journal_epoch, s.domain),
    )
    .unwrap();
    journal.append_batch(3, &[b"a"]).unwrap();
    let mut closed = journal.close_for_recovery().unwrap();
    source
        .ingest(
            &closed.export_durable(1, 32, 65536).unwrap(),
            &BTreeMap::new(),
            0,
        )
        .unwrap();
    let config = mirror_config(s);
    let store = MemoryStore::default();
    let objects = tickwire_recovery::SharedBlobStore::new(MockBlobStore::default());
    Persistence::new(objects.clone())
        .put_object(b"initial snapshot")
        .unwrap();
    let mirror = Mirror::create(config.clone(), store.clone(), objects.clone()).unwrap();
    drop(mirror);
    assert!(matches!(
        Mirror::recover(config, store, objects, Some(source.prefix())),
        Err(Error::Frozen)
    ));
}
#[test]
fn restart_reuses_immutable_dependency_selection() {
    let (stream, p) = opaque();
    let shared = tickwire_recovery::SharedBlobStore::new(p.into_store());
    let mut p = Persistence::new(shared.clone());
    let old = ObjectRef::new(b"earlier input cut").unwrap();
    let later = ObjectRef::new(b"later input cut").unwrap();
    p.put_object(b"earlier input cut").unwrap();
    p.put_object(b"later input cut").unwrap();
    let mut journal = Journal::create(
        MemoryStore::default(),
        Config::new(stream.journal_epoch, stream.domain),
    )
    .unwrap();
    journal
        .append_batch(3, &[b"durable account payload"])
        .unwrap();
    let export = journal.export_durable(1, 1, 65536).unwrap();
    let config = source_config(stream.clone(), stream.commit_mode.clone());
    let mut first = SourceWorker::new(config.clone(), Persistence::new(shared.clone())).unwrap();
    let original = first
        .ingest(&export, &BTreeMap::from([(1, vec![old.clone()])]), 1)
        .unwrap();
    drop(first);
    let mut restarted = SourceWorker::new(config, Persistence::new(shared.clone())).unwrap();
    let restored = restarted
        .restore_prefix(&export, &BTreeMap::from([(1, vec![later])]), 2)
        .unwrap();
    assert_eq!(restored, original.prefix());
    assert_eq!(
        restarted.batch_range(1, 1).unwrap().entries[0].extra_objects,
        vec![old]
    );
    assert!(
        Persistence::new(shared)
            .put_replica_entry(stream.digest(), 1, b"overwrite")
            .is_err()
    );
}
#[test]
fn stream_binds_commit_mode_and_profile() {
    let (stream, p) = opaque();
    let shared = tickwire_recovery::SharedBlobStore::new(p.into_store());
    assert_eq!(Stream::decode(&stream.bytes()).unwrap(), stream);
    let mut changed = stream.clone();
    changed.commit_mode = CommitMode::LocalDurableAsync {
        max_unreplicated_age_ns: 100,
    };
    assert_ne!(stream.digest(), changed.digest());
    let mut config = source_config(stream.clone(), stream.commit_mode.clone());
    config.mode = changed.commit_mode.clone();
    assert!(matches!(
        SourceWorker::new(config, Persistence::new(shared.clone())),
        Err(Error::Bounds)
    ));
    let original = source_config(stream.clone(), stream.commit_mode.clone());
    SourceWorker::new(original, Persistence::new(shared.clone())).unwrap();
    let changed = source_config(changed.clone(), changed.commit_mode.clone());
    assert!(matches!(
        SourceWorker::new(changed, Persistence::new(shared)),
        Err(Error::Recovery(tickwire_recovery::Error::Conflict))
    ));
    let mut mirror = mirror_config(stream);
    mirror.profile_version += 1;
    assert!(Mirror::create(mirror, MemoryStore::default(), MockBlobStore::default()).is_err());
}
