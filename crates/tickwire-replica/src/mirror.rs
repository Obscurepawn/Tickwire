use crate::{
    cbor::{Reader, Writer},
    source::{verify_entry, verify_objects},
    *,
};
use std::{collections::BTreeMap, path::Path};
use tickwire_journal::{
    Journal, PhysicalFrontier,
    store::{FileStore, Store},
};
use tickwire_recovery::{RecoveryManifest, digest};
#[derive(Clone, Debug)]
pub struct MirrorConfig {
    pub stream: Stream,
    pub receiver_id: [u8; 16],
    pub receiver_boot_id: [u8; 16],
    pub mirror_assignment_epoch: u64,
    pub profile_version: u64,
    pub max_records: usize,
    pub physical: tickwire_journal::Config,
}
impl MirrorConfig {
    fn validate(&self) -> Result<()> {
        self.stream.validate()?;
        if self.profile_version != self.stream.profile_version {
            return Err(Error::Conflict);
        }
        if let CommitMode::SyncReplica {
            mirror_id,
            assignment_epoch,
        } = self.stream.commit_mode
            && (self.receiver_id != mirror_id || self.mirror_assignment_epoch != assignment_epoch)
        {
            return Err(Error::Conflict);
        }
        if self.receiver_id == [0; 16]
            || self.receiver_boot_id == [0; 16]
            || self.profile_version == 0
            || self.max_records == 0
            || self.max_records > 1_000_000
            || self.physical.domain != self.stream.domain
            || self.physical.stream_epoch == [0; 16]
        {
            return Err(Error::Invalid("mirror config"));
        }
        Ok(())
    }
    fn bootstrap(&self) -> Vec<u8> {
        let mut w = Writer::new();
        w.array(6);
        w.bytes(b"tickwire.replica.mirror");
        w.uint(1);
        w.bytes(&self.stream.bytes());
        w.bytes(&self.receiver_id);
        w.uint(self.mirror_assignment_epoch);
        w.uint(self.profile_version);
        w.0
    }
}
pub type FileMirror = Mirror<FileStore, FileBlobStore>;
pub struct Mirror<T: Store, S: BlobStore> {
    config: MirrorConfig,
    journal: Journal<T>,
    persistence: Persistence<S>,
    prefix: Prefix,
    entries: Vec<Entry>,
    prefixes: Vec<Prefix>,
    input_heads: BTreeMap<[u8; 16], InputPrefix>,
    receipts: BTreeMap<u64, PhysicalFrontier>,
    closed: bool,
    poisoned: bool,
    upload: Option<Upload>,
}
struct Upload {
    object: ObjectRef,
    bytes: Vec<u8>,
}
impl FileMirror {
    pub fn create_files(
        config: MirrorConfig,
        journal_dir: impl AsRef<Path>,
        object_dir: impl AsRef<Path>,
    ) -> Result<Self> {
        let store = FileStore::open(journal_dir)?;
        let objects = FileBlobStore::open(object_dir)?;
        Self::create(config, store, objects)
    }
    pub fn recover_files(
        config: MirrorConfig,
        journal_dir: impl AsRef<Path>,
        object_dir: impl AsRef<Path>,
        required: Option<Prefix>,
    ) -> Result<Self> {
        let store = FileStore::open(journal_dir)?;
        let objects = FileBlobStore::open(object_dir)?;
        Self::recover(config, store, objects, required)
    }
}
impl<T: Store, S: BlobStore> Mirror<T, S> {
    pub fn create(config: MirrorConfig, store: T, objects: S) -> Result<Self> {
        config.validate()?;
        let mut journal = Journal::create(store, config.physical.clone())?;
        journal.append_batch(config.stream.source.exec_epoch, &[&config.bootstrap()])?;
        let prefix = Prefix::genesis(&config.stream);
        Ok(Self {
            config,
            journal,
            persistence: Persistence::new(objects),
            prefix,
            entries: Vec::new(),
            prefixes: vec![prefix],
            input_heads: BTreeMap::new(),
            receipts: BTreeMap::new(),
            closed: false,
            poisoned: false,
            upload: None,
        })
    }
    pub fn recover(
        config: MirrorConfig,
        store: T,
        objects: S,
        required: Option<Prefix>,
    ) -> Result<Self> {
        config.validate()?;
        let (journal, recovery) = Journal::recover(store, None)?;
        if journal.config() != &config.physical
            || recovery.records.is_empty()
            || recovery.records[0].payload != config.bootstrap()
        {
            return Err(Error::Conflict);
        }
        let prefix = Prefix::genesis(&config.stream);
        let mut m = Self {
            config,
            journal,
            persistence: Persistence::new(objects),
            prefix,
            entries: Vec::new(),
            prefixes: vec![prefix],
            input_heads: BTreeMap::new(),
            receipts: BTreeMap::new(),
            closed: false,
            poisoned: false,
            upload: None,
        };
        if recovery.records.len() > 1 {
            verify_objects(&mut m.persistence, &m.config.stream.initial_objects)?;
        }
        for (index, r) in recovery.records.iter().enumerate() {
            if r.seq != index as u64 + 1 || r.exec_epoch != m.config.stream.source.exec_epoch {
                return Err(Error::Conflict);
            }
            if index == 0 {
                continue;
            }
            if r.payload == m.close_marker() {
                if m.closed {
                    return Err(Error::Conflict);
                }
                m.closed = true;
                continue;
            }
            if m.closed {
                return Err(Error::Conflict);
            }
            let b = AppendBatch::decode(&r.payload)?;
            m.validate_next(&b)?;
            for (index, e) in b.entries.iter().enumerate() {
                verify_entry(&mut m.persistence, m.config.stream.clone(), e)?;
                if e.kind == 11 {
                    let mut history = m.entries.clone();
                    history.extend_from_slice(&b.entries[..index]);
                    crate::handover::verify_history(&m.persistence, m.stream(), e, &history)?;
                }
            }
            m.input_heads = crate::input::advance_heads(
                &m.persistence,
                m.stream(),
                &m.input_heads,
                b.entries.iter().map(|e| e.extra_objects.clone()),
            )?;
            m.publish(&b);
        }
        if let Some(p) = required
            && m.prefixes.get(p.sequence as usize) != Some(&p)
        {
            return Err(Error::Frozen);
        }
        // Recovery re-synchronizes the complete physical prefix. This frontier
        // covers every earlier logical position, independent of segment layout.
        if let Some(f) = m.journal.durable_frontier() {
            m.receipts.insert(m.prefix.sequence, f.clone());
        }
        Ok(m)
    }
    pub(crate) fn restore_entries(&self) -> &[Entry] {
        &self.entries
    }
    pub(crate) fn restore_persistence(&self) -> &Persistence<S> {
        &self.persistence
    }
    pub(crate) fn check_complete(&self, c: &CompletePrefix) -> Result<()> {
        if self.poisoned
            || !self.closed
            || c.stream != self.stream().digest()
            || c.prefix != self.prefix
        {
            return Err(Error::Frozen);
        }
        Ok(())
    }
    pub fn prefix(&self) -> Prefix {
        self.prefix
    }
    pub fn stream(&self) -> &Stream {
        &self.config.stream
    }
    pub fn is_closed(&self) -> bool {
        self.closed
    }
    pub fn is_poisoned(&self) -> bool {
        self.poisoned
    }
    pub fn hello(&self) -> PeerHello {
        PeerHello {
            stream_digest: self.stream().digest(),
            receiver_id: self.config.receiver_id,
            receiver_boot_id: self.config.receiver_boot_id,
            assignment_epoch: self.config.mirror_assignment_epoch,
            prefix: self.prefix,
        }
    }
    pub fn has_object(&self, o: &ObjectRef) -> Result<bool> {
        match self.persistence.read_object(o.hash, o.bytes as usize) {
            Ok(b) => Ok(b.len() as u64 == o.bytes),
            Err(tickwire_recovery::Error::Missing) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }
    /// One bounded object assembly. Chunks carry CRC; final content must have the
    /// declared SHA256. An incomplete transfer cannot advance the durable prefix.
    pub fn object_chunk(
        &mut self,
        o: ObjectRef,
        offset: u64,
        bytes: &[u8],
        crc: u32,
    ) -> Result<bool> {
        if self.poisoned {
            return Err(Error::Frozen);
        }
        if o.bytes == 0
            || o.bytes > MAX_OBJECT_BYTES as u64
            || bytes.is_empty()
            || bytes.len() > CHUNK_BYTES
            || tickwire_journal::frame::crc32c(bytes) != crc
        {
            return Err(Error::Bounds);
        }
        if offset
            .checked_add(bytes.len() as u64)
            .is_none_or(|n| n > o.bytes)
        {
            return Err(Error::Bounds);
        }
        if self.has_object(&o)? {
            let old = self.persistence.read_object(o.hash, o.bytes as usize)?;
            if old.get(offset as usize..offset as usize + bytes.len()) != Some(bytes) {
                return Err(Error::Conflict);
            }
            return Ok(true);
        }
        if let Some(u) = &self.upload
            && u.object == o
            && offset < (u.bytes.len() as u64)
        {
            if u.bytes.get(offset as usize..offset as usize + bytes.len()) == Some(bytes) {
                return Ok(false);
            }
            return Err(Error::Conflict);
        }
        if offset == 0 {
            self.upload = Some(Upload {
                object: o.clone(),
                bytes: Vec::new(),
            });
        }
        let u = self.upload.as_mut().ok_or(Error::Gap { next: 0 })?;
        if u.object != o
            || offset != u.bytes.len() as u64
            || offset
                .checked_add(bytes.len() as u64)
                .is_none_or(|n| n > o.bytes)
        {
            return Err(Error::Conflict);
        }
        u.bytes.extend_from_slice(bytes);
        if u.bytes.len() as u64 == o.bytes {
            let u = self.upload.take().unwrap();
            if digest(&u.bytes) != o.hash {
                return Err(Error::Conflict);
            }
            if let Err(e) = self.persistence.put_object(&u.bytes) {
                self.poisoned = true;
                return Err(e.into());
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }
    pub fn publish_manifest(&mut self, bytes: &[u8]) -> Result<Hash> {
        if self.poisoned {
            return Err(Error::Frozen);
        }
        let m = RecoveryManifest::decode(bytes)?;
        if m.binding.risk_domain_id != self.stream().domain {
            return Err(Error::Conflict);
        }
        match self.persistence.publish(&m) {
            Ok(c) => Ok(c.manifest_root()),
            Err(tickwire_recovery::Error::Missing) => Err(Error::Missing),
            Err(e) => {
                self.poisoned = true;
                Err(e.into())
            }
        }
    }
    fn validate_next(&self, b: &AppendBatch) -> Result<()> {
        b.validate(self.stream())?;
        if b.previous != self.prefix {
            if b.previous.sequence == self.prefix.sequence {
                return Err(Error::Conflict);
            }
            return Err(Error::Gap {
                next: self.prefix.sequence + 1,
            });
        }
        if self
            .entries
            .len()
            .checked_add(b.entries.len())
            .is_none_or(|n| n > self.config.max_records)
        {
            return Err(Error::Bounds);
        }
        Ok(())
    }
    fn publish(&mut self, b: &AppendBatch) {
        for e in &b.entries {
            self.prefix = self
                .prefix
                .advance(self.stream(), e)
                .expect("prevalidated chain");
            self.prefixes.push(self.prefix);
            self.entries.push(e.clone());
        }
    }
    pub fn append(&mut self, b: &AppendBatch, signer: &Signer) -> Result<Signed> {
        if self.poisoned || self.closed {
            return Err(Error::Frozen);
        }
        b.validate(self.stream())?;
        if b.previous.sequence < self.prefix.sequence {
            let start = b.previous.sequence as usize;
            let end = b.end.sequence as usize;
            let overlap = end.min(self.entries.len());
            if self.prefixes.get(start) != Some(&b.previous)
                || self.entries.get(start..overlap) != Some(&b.entries[..overlap - start])
            {
                self.poisoned = true;
                return Err(Error::Conflict);
            }
            if end > self.entries.len() {
                return Err(Error::Gap {
                    next: self.prefix.sequence + 1,
                });
            }
            if self.prefixes.get(end) != Some(&b.end) {
                self.poisoned = true;
                return Err(Error::Conflict);
            }
            return self.ack(b.end.sequence, signer);
        }
        if let Err(e) = self.validate_next(b) {
            if matches!(e, Error::Conflict) {
                self.poisoned = true;
            }
            return Err(e);
        }
        verify_objects(&mut self.persistence, &self.config.stream.initial_objects)?;
        for (index, e) in b.entries.iter().enumerate() {
            verify_entry(&mut self.persistence, self.config.stream.clone(), e)?;
            if e.kind == 11 {
                let mut history = self.entries.clone();
                history.extend_from_slice(&b.entries[..index]);
                crate::handover::verify_history(&self.persistence, self.stream(), e, &history)?;
            }
        }
        let heads = crate::input::advance_heads(
            &self.persistence,
            self.stream(),
            &self.input_heads,
            b.entries.iter().map(|e| e.extra_objects.clone()),
        )?;
        let bytes = b.bytes();
        if bytes.len() > MAX_BATCH_BYTES - 1024 {
            return Err(Error::Bounds);
        }
        let sync = match self
            .journal
            .append_batch(self.config.stream.source.exec_epoch, &[&bytes])
        {
            Ok(s) => s,
            Err(e) => {
                self.poisoned = true;
                return Err(e.into());
            }
        };
        self.input_heads = heads;
        self.publish(b);
        self.receipts
            .insert(b.end.sequence, sync.frontier().clone());
        self.ack(b.end.sequence, signer)
    }
    pub fn ack(&mut self, sequence: u64, signer: &Signer) -> Result<Signed> {
        if self.poisoned {
            return Err(Error::Frozen);
        }
        let prefix = *self.prefixes.get(sequence as usize).ok_or(Error::Gap {
            next: self.prefix.sequence + 1,
        })?;
        verify_objects(&mut self.persistence, &self.config.stream.initial_objects)?;
        let f = self
            .receipts
            .range(sequence..)
            .next()
            .map(|(_, f)| f)
            .or_else(|| self.journal.durable_frontier())
            .ok_or(Error::Missing)?;
        let ack = Ack {
            stream_digest: self.stream().digest(),
            domain: self.stream().domain,
            journal_epoch: self.stream().journal_epoch,
            source_exec_epoch: self.stream().source.exec_epoch,
            receiver_id: self.config.receiver_id,
            receiver_boot_id: self.config.receiver_boot_id,
            assignment_epoch: self.config.mirror_assignment_epoch,
            prefix,
            storage_batch_id: f.batch_number,
            completed_sync_id: f.seal_hash,
        };
        Ok(signer.sign(ack.bytes()))
    }
    pub fn original_records(
        &self,
        first: u64,
        max: usize,
    ) -> Result<Vec<tickwire_journal::Record>> {
        if first == 0 || max == 0 || max > MAX_BATCH_RECORDS {
            return Err(Error::Bounds);
        }
        self.entries
            .iter()
            .skip((first - 1) as usize)
            .take(max)
            .map(|e| {
                Ok(tickwire_journal::Record {
                    seq: e.sequence,
                    exec_epoch: e.writer_epoch,
                    payload: self
                        .persistence
                        .read_object(e.payload.hash, e.payload.bytes as usize)?,
                })
            })
            .collect()
    }
    pub fn verify_source_complete(
        &mut self,
        seal: &Signed,
        verifier_key: Hash,
        fence: &tickwire_fencing::VerifiedFence,
        now: tickwire_clock::MonoTime,
    ) -> Result<CompletePrefix> {
        let verified = SourceRecoverySeal::verify(seal, verifier_key, fence, now)?;
        if verified.stream != *self.stream() || verified.prefix != self.prefix {
            return Err(Error::Frozen);
        }
        verify_objects(&mut self.persistence, &self.config.stream.initial_objects)?;
        for e in &self.entries {
            verify_entry(&mut self.persistence, self.config.stream.clone(), e)?;
        }
        verify_objects(&mut self.persistence, &verified.final_inputs)?;
        let (objects, prefixes) = crate::input::input_dependencies(
            &self.persistence,
            self.stream(),
            &verified.final_inputs,
        )?;
        verify_objects(&mut self.persistence, &objects)?;
        for (id, old) in &self.input_heads {
            if !prefixes
                .iter()
                .any(|p| p.input_stream == *id && p.through_sequence >= old.through_sequence)
            {
                return Err(Error::Missing);
            }
        }
        crate::input::advance_heads(
            &self.persistence,
            self.stream(),
            &self.input_heads,
            std::iter::once(verified.final_inputs.clone()),
        )?;
        let hash = self.persistence.put_object(&seal.bytes())?;
        if !self.closed {
            let marker = self.close_marker();
            if let Err(e) = self
                .journal
                .append_batch(self.stream().source.exec_epoch, &[&marker])
            {
                self.poisoned = true;
                return Err(e.into());
            }
            self.closed = true;
        }
        Ok(CompletePrefix {
            stream: self.stream().digest(),
            prefix: self.prefix,
            seal_hash: hash,
            fence_hash: fence.evidence_digest(),
            final_inputs: verified.final_inputs,
        })
    }
    /// Recreate the exact input records in the existing physical WAL format.
    /// The caller keeps the resulting journal frozen until account recovery completes.
    pub fn materialize_input<Target: Store>(
        &self,
        prefix_root: &ObjectRef,
        store: Target,
    ) -> Result<Journal<Target>> {
        let (_, prefixes) = crate::input::input_dependencies(
            &self.persistence,
            self.stream(),
            std::slice::from_ref(prefix_root),
        )?;
        let p = prefixes.first().ok_or(Error::Missing)?;
        self.materialize_input_with_config(
            prefix_root,
            store,
            tickwire_journal::Config::new(p.input_stream, self.stream().domain),
        )
    }
    pub fn materialize_input_with_config<Target: Store>(
        &self,
        prefix_root: &ObjectRef,
        store: Target,
        config: tickwire_journal::Config,
    ) -> Result<Journal<Target>> {
        let (_, prefixes) = crate::input::input_dependencies(
            &self.persistence,
            self.stream(),
            std::slice::from_ref(prefix_root),
        )?;
        let p = prefixes.first().ok_or(Error::Missing)?;
        if config.domain != self.stream().domain || config.stream_epoch != p.input_stream {
            return Err(Error::Conflict);
        }
        let mut journal = Journal::create(store, config)?;
        for e in &p.entries {
            let bytes = self
                .persistence
                .read_object(e.object.hash, e.object.bytes as usize)?;
            if bytes.len() as u64 != e.object.bytes {
                return Err(Error::Conflict);
            }
            journal.append_batch(e.writer_epoch, &[&bytes])?;
        }
        Ok(journal)
    }
    pub fn verify_sync_complete(
        &mut self,
        signed: &Signed,
        receiver_key: Hash,
        fence: &tickwire_fencing::VerifiedFence,
        now: tickwire_clock::MonoTime,
    ) -> Result<CompletePrefix> {
        let seal = MirrorRecoverySeal::verify(signed, receiver_key, fence, now)?;
        if !self.closed
            || seal.stream != *self.stream()
            || seal.prefix != self.prefix
            || seal.receiver_id != self.config.receiver_id
            || seal.assignment_epoch != self.config.mirror_assignment_epoch
            || seal.profile_version != self.config.profile_version
        {
            return Err(Error::Frozen);
        }
        verify_objects(&mut self.persistence, &self.config.stream.initial_objects)?;
        for e in &self.entries {
            verify_entry(&mut self.persistence, self.config.stream.clone(), e)?;
        }
        let seal_hash = self.persistence.put_object(&signed.bytes())?;
        Ok(CompletePrefix {
            stream: self.stream().digest(),
            prefix: self.prefix,
            seal_hash,
            fence_hash: fence.evidence_digest(),
            final_inputs: Vec::new(),
        })
    }
    fn close_marker(&self) -> Vec<u8> {
        let mut w = Writer::new();
        w.array(4);
        w.bytes(b"tickwire.replica.ingest-closed");
        w.uint(1);
        w.bytes(&self.stream().digest());
        w.uint(self.config.mirror_assignment_epoch);
        w.0
    }
    /// SyncReplica recovery closes the old ingestion epoch durably. The verified
    /// external fence remains a separate prerequisite, never inferred from ACKs.
    pub fn close_sync(
        &mut self,
        fence: &tickwire_fencing::VerifiedFence,
        now: tickwire_clock::MonoTime,
        signer: &Signer,
    ) -> Result<Signed> {
        fence
            .check(now)
            .map_err(|_| Error::Invalid("fence validity"))?;
        if !matches!(self.stream().commit_mode,CommitMode::SyncReplica{mirror_id,assignment_epoch}if mirror_id==self.config.receiver_id&&assignment_epoch==self.config.mirror_assignment_epoch)
            || self.poisoned
            || self.config.mirror_assignment_epoch == 0
            || fence.risk_domain() != self.stream().domain
            || fence.old_identity() != &self.stream().source
        {
            return Err(Error::Frozen);
        }
        verify_objects(&mut self.persistence, &self.config.stream.initial_objects)?;
        for e in &self.entries {
            verify_entry(&mut self.persistence, self.config.stream.clone(), e)?;
        }
        if !self.closed {
            let marker = self.close_marker();
            if let Err(e) = self
                .journal
                .append_batch(self.stream().source.exec_epoch, &[&marker])
            {
                self.poisoned = true;
                return Err(e.into());
            }
            self.closed = true;
        }
        let mut w = Writer::new();
        w.array(10);
        w.bytes(b"tickwire.replica.mirror-recovery-seal");
        w.uint(1);
        w.bytes(&self.stream().bytes());
        w.bytes(&fence.evidence_digest());
        w.uint(self.config.profile_version);
        w.bytes(&self.config.receiver_id);
        w.uint(self.config.mirror_assignment_epoch);
        self.prefix.write(&mut w);
        w.uint(1);
        w.bytes(&self.config.receiver_boot_id);
        let signed = signer.sign(w.0);
        self.persistence.put_object(&signed.bytes())?;
        Ok(signed)
    }
}
pub struct CompletePrefix {
    stream: Hash,
    prefix: Prefix,
    seal_hash: Hash,
    fence_hash: Hash,
    final_inputs: Vec<ObjectRef>,
}
impl CompletePrefix {
    pub fn final_inputs(&self) -> Option<&[ObjectRef]> {
        if self.final_inputs.is_empty() {
            None
        } else {
            Some(&self.final_inputs)
        }
    }
    pub fn prefix(&self) -> Prefix {
        self.prefix
    }
    pub fn recovery_seal_hash(&self) -> Hash {
        self.seal_hash
    }
    pub fn fence_evidence_hash(&self) -> Hash {
        self.fence_hash
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeerHello {
    pub(crate) stream_digest: Hash,
    pub(crate) receiver_id: [u8; 16],
    pub(crate) receiver_boot_id: [u8; 16],
    pub(crate) assignment_epoch: u64,
    pub(crate) prefix: Prefix,
}
impl PeerHello {
    pub fn prefix(&self) -> Prefix {
        self.prefix
    }
    pub fn receiver_boot_id(&self) -> [u8; 16] {
        self.receiver_boot_id
    }
    pub(crate) fn bytes(&self) -> Vec<u8> {
        let mut w = Writer::new();
        w.array(7);
        w.bytes(b"tickwire.replica.hello");
        w.uint(1);
        w.bytes(&self.stream_digest);
        w.bytes(&self.receiver_id);
        w.bytes(&self.receiver_boot_id);
        w.uint(self.assignment_epoch);
        self.prefix.write(&mut w);
        w.0
    }
    pub(crate) fn decode(b: &[u8]) -> Result<Self> {
        let mut r = Reader::new(b);
        r.array(7)?;
        crate::protocol::tag(&mut r, b"tickwire.replica.hello")?;
        let h = Self {
            stream_digest: r.fixed()?,
            receiver_id: r.fixed()?,
            receiver_boot_id: r.fixed()?,
            assignment_epoch: r.uint()?,
            prefix: Prefix::read(&mut r)?,
        };
        r.end()?;
        if h.receiver_id == [0; 16] || h.receiver_boot_id == [0; 16] {
            return Err(Error::Invalid("peer boot"));
        }
        Ok(h)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MirrorRecoverySeal {
    pub stream: Stream,
    pub fence_ref: Hash,
    pub profile_version: u64,
    pub receiver_id: [u8; 16],
    pub assignment_epoch: u64,
    pub prefix: Prefix,
    pub receiver_boot_id: [u8; 16],
}
impl MirrorRecoverySeal {
    pub fn verify(
        signed: &Signed,
        key: Hash,
        fence: &tickwire_fencing::VerifiedFence,
        now: tickwire_clock::MonoTime,
    ) -> Result<Self> {
        signed.verify(key)?;
        fence
            .check(now)
            .map_err(|_| Error::Invalid("fence validity"))?;
        let mut r = Reader::new(signed.payload());
        r.array(10)?;
        crate::protocol::tag(&mut r, b"tickwire.replica.mirror-recovery-seal")?;
        let stream = Stream::decode(r.bytes(65536)?)?;
        let fence_ref = r.fixed()?;
        let profile_version = r.uint()?;
        let receiver_id = r.fixed()?;
        let assignment_epoch = r.uint()?;
        let prefix = Prefix::read(&mut r)?;
        if r.uint()? != 1 {
            return Err(Error::Invalid("old ingest still open"));
        }
        let receiver_boot_id = r.fixed()?;
        r.end()?;
        if stream.profile_version != profile_version
            || !matches!(stream.commit_mode,CommitMode::SyncReplica{mirror_id,assignment_epoch:epoch}if mirror_id==receiver_id&&epoch==assignment_epoch)
            || stream.domain != fence.risk_domain()
            || &stream.source != fence.old_identity()
            || fence_ref != fence.evidence_digest()
            || profile_version == 0
            || assignment_epoch == 0
            || receiver_id == [0; 16]
            || receiver_boot_id == [0; 16]
        {
            return Err(Error::Conflict);
        }
        Ok(Self {
            stream,
            fence_ref,
            profile_version,
            receiver_id,
            assignment_epoch,
            prefix,
            receiver_boot_id,
        })
    }
}
