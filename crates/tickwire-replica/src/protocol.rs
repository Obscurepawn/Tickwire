use crate::{
    cbor::{Reader, Writer},
    *,
};
use ring::signature::{ED25519, Ed25519KeyPair, KeyPair, UnparsedPublicKey};
use tickwire_fencing::ExecutionIdentity;
use tickwire_recovery::{ClosureBinding, digest};
pub const MAX_BATCH_RECORDS: usize = 32;
pub const MAX_BATCH_BYTES: usize = 64 * 1024;
pub const MAX_OBJECT_BYTES: usize = tickwire_recovery::MAX_OBJECT_BYTES;
pub const CHUNK_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_FRAME_BYTES: usize = CHUNK_BYTES + 65536;
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectRef {
    pub hash: Hash,
    pub bytes: u64,
}
impl ObjectRef {
    pub fn new(bytes: &[u8]) -> Result<Self> {
        if bytes.is_empty() || bytes.len() > MAX_OBJECT_BYTES {
            return Err(Error::Bounds);
        }
        Ok(Self {
            hash: digest(bytes),
            bytes: bytes.len() as u64,
        })
    }
    pub(crate) fn write(&self, w: &mut Writer) {
        w.array(2);
        w.bytes(&self.hash);
        w.uint(self.bytes)
    }
    pub(crate) fn read(r: &mut Reader<'_>) -> Result<Self> {
        r.array(2)?;
        let x = Self {
            hash: r.fixed()?,
            bytes: r.uint()?,
        };
        if x.hash == [0; 32] || x.bytes == 0 || x.bytes > MAX_OBJECT_BYTES as u64 {
            return Err(Error::Bounds);
        }
        Ok(x)
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Format {
    Account { baseline: Hash },
    Opaque { record_type: u64 },
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Stream {
    pub domain: [u8; 16],
    pub journal_epoch: [u8; 16],
    pub source: ExecutionIdentity,
    pub source_storage_id: [u8; 16],
    pub format: Format,
    pub profile_version: u64,
    pub commit_mode: CommitMode,
    /// Actual immutable baseline/bootstrap objects. They remain pinned forever.
    pub initial_objects: Vec<ObjectRef>,
}
impl Stream {
    pub fn validate(&self) -> Result<()> {
        if self.profile_version == 0
            || self.domain == [0; 16]
            || self.journal_epoch == [0; 16]
            || self.source_storage_id == [0; 16]
            || [
                self.source.host_id,
                self.source.boot_id,
                self.source.exec_instance_id,
            ]
            .contains(&[0; 16])
            || self.source.exec_epoch == 0
            || self.source.network_identity == [0; 32]
            || self.source.credential_access_identity == [0; 32]
            || self.initial_objects.is_empty()
            || self.initial_objects.len() > 32
        {
            return Err(Error::Invalid("stream identity"));
        }
        if self
            .initial_objects
            .windows(2)
            .any(|w| w[0].hash >= w[1].hash)
            || self
                .initial_objects
                .iter()
                .any(|o| o.bytes == 0 || o.bytes > MAX_OBJECT_BYTES as u64 || o.hash == [0; 32])
        {
            return Err(Error::Invalid("initial objects"));
        }
        match self.commit_mode {
            CommitMode::LocalDurableAsync {
                max_unreplicated_age_ns: 0,
            }
            | CommitMode::SyncReplica {
                assignment_epoch: 0,
                ..
            } => return Err(Error::Invalid("commit mode")),
            CommitMode::SyncReplica { mirror_id, .. } if mirror_id == [0; 16] => {
                return Err(Error::Invalid("mirror identity"));
            }
            _ => {}
        }
        match self.format {
            Format::Account { baseline }
                if !self.initial_objects.iter().any(|o| o.hash == baseline) =>
            {
                return Err(Error::Missing);
            }
            Format::Opaque { record_type: 0 } => return Err(Error::Invalid("opaque record type")),
            _ => {}
        }
        Ok(())
    }
    pub fn bytes(&self) -> Vec<u8> {
        let mut w = Writer::new();
        w.array(10);
        w.bytes(b"tickwire.replica.stream");
        w.uint(1);
        w.bytes(&self.domain);
        w.bytes(&self.journal_epoch);
        write_identity(&mut w, &self.source);
        w.bytes(&self.source_storage_id);
        w.array(2);
        match self.format {
            Format::Account { baseline } => {
                w.uint(1);
                w.bytes(&baseline)
            }
            Format::Opaque { record_type } => {
                w.uint(2);
                w.uint(record_type)
            }
        }
        w.array(self.initial_objects.len());
        for o in &self.initial_objects {
            o.write(&mut w)
        }
        w.uint(self.profile_version);
        match self.commit_mode {
            CommitMode::LocalDurableAsync {
                max_unreplicated_age_ns,
            } => {
                w.array(2);
                w.uint(1);
                w.uint(max_unreplicated_age_ns)
            }
            CommitMode::SyncReplica {
                mirror_id,
                assignment_epoch,
            } => {
                w.array(3);
                w.uint(2);
                w.bytes(&mirror_id);
                w.uint(assignment_epoch)
            }
        }
        w.0
    }
    pub fn decode(b: &[u8]) -> Result<Self> {
        if b.len() > 65536 {
            return Err(Error::Bounds);
        }
        let mut r = Reader::new(b);
        r.array(10)?;
        tag(&mut r, b"tickwire.replica.stream")?;
        let domain = r.fixed()?;
        let journal_epoch = r.fixed()?;
        let source = read_identity(&mut r)?;
        let source_storage_id = r.fixed()?;
        r.array(2)?;
        let format = match r.uint()? {
            1 => Format::Account {
                baseline: r.fixed()?,
            },
            2 => Format::Opaque {
                record_type: r.uint()?,
            },
            _ => return Err(Error::Invalid("format")),
        };
        let n = r.count(32)?;
        let mut initial_objects = Vec::with_capacity(n);
        for _ in 0..n {
            initial_objects.push(ObjectRef::read(&mut r)?);
        }
        let profile_version = r.uint()?;
        let arity = r.count(3)?;
        let commit_mode = match (arity, r.uint()?) {
            (2, 1) => CommitMode::LocalDurableAsync {
                max_unreplicated_age_ns: r.uint()?,
            },
            (3, 2) => CommitMode::SyncReplica {
                mirror_id: r.fixed()?,
                assignment_epoch: r.uint()?,
            },
            _ => return Err(Error::Invalid("commit mode")),
        };
        r.end()?;
        let s = Self {
            domain,
            journal_epoch,
            source,
            source_storage_id,
            format,
            initial_objects,
            profile_version,
            commit_mode,
        };
        s.validate()?;
        Ok(s)
    }
    pub fn digest(&self) -> Hash {
        digest(&self.bytes())
    }
}
pub(crate) fn write_identity(w: &mut Writer, i: &ExecutionIdentity) {
    w.array(6);
    w.bytes(&i.host_id);
    w.bytes(&i.boot_id);
    w.bytes(&i.exec_instance_id);
    w.uint(i.exec_epoch);
    w.bytes(&i.network_identity);
    w.bytes(&i.credential_access_identity)
}
pub(crate) fn read_identity(r: &mut Reader<'_>) -> Result<ExecutionIdentity> {
    r.array(6)?;
    Ok(ExecutionIdentity {
        host_id: r.fixed()?,
        boot_id: r.fixed()?,
        exec_instance_id: r.fixed()?,
        exec_epoch: r.uint()?,
        network_identity: r.fixed()?,
        credential_access_identity: r.fixed()?,
    })
}
pub(crate) fn tag(r: &mut Reader<'_>, name: &[u8]) -> Result<()> {
    if r.bytes(64)? != name || r.uint()? != 1 {
        return Err(Error::Invalid("schema domain"));
    }
    Ok(())
}
pub(crate) fn write_binding(w: &mut Writer, b: &ClosureBinding) {
    w.array(7);
    w.bytes(&b.risk_domain_id);
    w.uint(b.intent_id);
    w.uint(b.intent_exec_epoch);
    w.bytes(&b.business_digest);
    w.uint(b.strategy_generation);
    w.uint(b.config_revision);
    w.uint(b.recovery_object)
}
pub(crate) fn read_binding(r: &mut Reader<'_>) -> Result<ClosureBinding> {
    r.array(7)?;
    Ok(ClosureBinding {
        risk_domain_id: r.fixed()?,
        intent_id: r.uint()?,
        intent_exec_epoch: r.uint()?,
        business_digest: r.fixed()?,
        strategy_generation: r.uint()?,
        config_revision: r.uint()?,
        recovery_object: r.uint()?,
    })
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ManifestRef {
    pub binding: ClosureBinding,
    pub root: Hash,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub sequence: u64,
    pub writer_epoch: u64,
    pub kind: u64,
    pub payload: ObjectRef,
    pub manifest: Option<ManifestRef>,
    pub extra_objects: Vec<ObjectRef>,
}
impl Entry {
    pub fn bytes(&self) -> Vec<u8> {
        let mut w = Writer::new();
        self.write(&mut w);
        w.0
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > 65536 {
            return Err(Error::Bounds);
        }
        let mut r = Reader::new(bytes);
        let entry = Self::read(&mut r)?;
        r.end()?;
        Ok(entry)
    }
    pub(crate) fn write(&self, w: &mut Writer) {
        w.array(7);
        w.uint(self.sequence);
        w.uint(self.writer_epoch);
        w.uint(self.kind);
        self.payload.write(w);
        w.array(usize::from(self.manifest.is_some()));
        if let Some(m) = &self.manifest {
            w.array(2);
            write_binding(w, &m.binding);
            w.bytes(&m.root)
        }
        w.array(self.extra_objects.len());
        for o in &self.extra_objects {
            o.write(w)
        }
        w.uint(1)
    }
    pub(crate) fn read(r: &mut Reader<'_>) -> Result<Self> {
        r.array(7)?;
        let sequence = r.uint()?;
        let writer_epoch = r.uint()?;
        let kind = r.uint()?;
        let payload = ObjectRef::read(r)?;
        let manifest = if r.count(1)? == 1 {
            r.array(2)?;
            Some(ManifestRef {
                binding: read_binding(r)?,
                root: r.fixed()?,
            })
        } else {
            None
        };
        let n = r.count(32)?;
        let mut extra_objects = Vec::with_capacity(n);
        for _ in 0..n {
            extra_objects.push(ObjectRef::read(r)?)
        }
        if r.uint()? != 1
            || sequence == 0
            || writer_epoch == 0
            || kind == 0
            || extra_objects.windows(2).any(|w| w[0].hash >= w[1].hash)
        {
            return Err(Error::Invalid("entry"));
        }
        Ok(Self {
            sequence,
            writer_epoch,
            kind,
            payload,
            manifest,
            extra_objects,
        })
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Prefix {
    pub sequence: u64,
    pub hash: Hash,
    pub dependency_root: Hash,
    pub writer_epoch: u64,
}
impl Prefix {
    pub fn genesis(stream: &Stream) -> Self {
        let mut w = Writer::new();
        w.array(4);
        w.bytes(b"tickwire.replica.genesis");
        w.uint(1);
        w.bytes(&stream.domain);
        w.bytes(&stream.journal_epoch);
        let hash = digest(&w.0);
        w.array(2);
        w.bytes(b"tickwire.replica.dependencies");
        w.bytes(&stream.digest());
        Self {
            sequence: 0,
            hash,
            dependency_root: digest(&w.0),
            writer_epoch: 0,
        }
    }
    pub fn advance(self, stream: &Stream, e: &Entry) -> Result<Self> {
        if self.sequence.checked_add(1) != Some(e.sequence)
            || e.writer_epoch < self.writer_epoch
            || e.writer_epoch > stream.source.exec_epoch
        {
            return Err(Error::Invalid("logical sequence or writer"));
        }
        let mut payload = Writer::new();
        payload.array(4);
        payload.bytes(b"tickwire.replica.payload");
        payload.uint(1);
        payload.uint(e.writer_epoch);
        payload.bytes(&digest(&e.bytes()));
        let mut w = Writer::new();
        w.array(5);
        w.bytes(&stream.domain);
        w.bytes(&stream.journal_epoch);
        w.uint(e.sequence);
        w.uint(e.kind);
        w.bytes(&digest(&payload.0));
        let mut bytes = self.hash.to_vec();
        bytes.extend(w.0);
        let hash = digest(&bytes);
        let mut deps = self.dependency_root.to_vec();
        deps.extend(e.bytes());
        Ok(Self {
            sequence: e.sequence,
            hash,
            dependency_root: digest(&deps),
            writer_epoch: e.writer_epoch,
        })
    }
    pub(crate) fn write(self, w: &mut Writer) {
        w.array(4);
        w.uint(self.sequence);
        w.bytes(&self.hash);
        w.bytes(&self.dependency_root);
        w.uint(self.writer_epoch)
    }
    pub(crate) fn read(r: &mut Reader<'_>) -> Result<Self> {
        r.array(4)?;
        Ok(Self {
            sequence: r.uint()?,
            hash: r.fixed()?,
            dependency_root: r.fixed()?,
            writer_epoch: r.uint()?,
        })
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppendBatch {
    pub stream_digest: Hash,
    pub previous: Prefix,
    pub entries: Vec<Entry>,
    pub end: Prefix,
}
impl AppendBatch {
    pub fn bytes(&self) -> Vec<u8> {
        let mut w = Writer::new();
        w.array(6);
        w.bytes(b"tickwire.replica.append");
        w.uint(1);
        w.bytes(&self.stream_digest);
        self.previous.write(&mut w);
        w.array(self.entries.len());
        for e in &self.entries {
            e.write(&mut w)
        }
        self.end.write(&mut w);
        w.0
    }
    pub fn decode(b: &[u8]) -> Result<Self> {
        if b.len() > MAX_BATCH_BYTES {
            return Err(Error::Bounds);
        }
        let mut r = Reader::new(b);
        r.array(6)?;
        tag(&mut r, b"tickwire.replica.append")?;
        let stream_digest = r.fixed()?;
        let previous = Prefix::read(&mut r)?;
        let n = r.count(MAX_BATCH_RECORDS)?;
        if n == 0 {
            return Err(Error::Bounds);
        }
        let mut entries = Vec::with_capacity(n);
        for _ in 0..n {
            entries.push(Entry::read(&mut r)?);
        }
        let end = Prefix::read(&mut r)?;
        r.end()?;
        Ok(Self {
            stream_digest,
            previous,
            entries,
            end,
        })
    }
    pub fn validate(&self, s: &Stream) -> Result<()> {
        if self.stream_digest != s.digest()
            || self.entries.is_empty()
            || self.entries.len() > MAX_BATCH_RECORDS
            || self.bytes().len() > MAX_BATCH_BYTES
        {
            return Err(Error::Invalid("batch identity or bound"));
        }
        let mut p = self.previous;
        for e in &self.entries {
            p = p.advance(s, e)?;
        }
        if p != self.end {
            return Err(Error::Conflict);
        }
        Ok(())
    }
}
pub struct Signer(Ed25519KeyPair);
impl Signer {
    pub fn from_pkcs8(b: &[u8]) -> Result<Self> {
        Ok(Self(
            Ed25519KeyPair::from_pkcs8(b).map_err(|_| Error::Signature)?,
        ))
    }
    pub fn from_seed(seed: [u8; 32]) -> Result<Self> {
        Ok(Self(
            Ed25519KeyPair::from_seed_unchecked(&seed).map_err(|_| Error::Signature)?,
        ))
    }
    pub fn public_key(&self) -> Hash {
        self.0.public_key().as_ref().try_into().unwrap()
    }
    pub(crate) fn sign(&self, payload: Vec<u8>) -> Signed {
        Signed {
            signature: self.0.sign(&payload).as_ref().try_into().unwrap(),
            payload,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Signed {
    payload: Vec<u8>,
    signature: [u8; 64],
}
impl Signed {
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
    pub fn bytes(&self) -> Vec<u8> {
        let mut w = Writer::new();
        w.array(2);
        w.bytes(&self.payload);
        w.bytes(&self.signature);
        w.0
    }
    pub fn decode(b: &[u8]) -> Result<Self> {
        if b.len() > MAX_FRAME_BYTES {
            return Err(Error::Bounds);
        }
        let mut r = Reader::new(b);
        r.array(2)?;
        let payload = r.bytes(MAX_FRAME_BYTES - 128)?.to_vec();
        let signature = r.fixed()?;
        r.end()?;
        Ok(Self { payload, signature })
    }
    pub fn verify(&self, key: Hash) -> Result<()> {
        UnparsedPublicKey::new(&ED25519, key)
            .verify(&self.payload, &self.signature)
            .map_err(|_| Error::Signature)
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Ack {
    pub stream_digest: Hash,
    pub domain: [u8; 16],
    pub journal_epoch: [u8; 16],
    pub source_exec_epoch: u64,
    pub receiver_id: [u8; 16],
    pub receiver_boot_id: [u8; 16],
    pub assignment_epoch: u64,
    pub prefix: Prefix,
    pub storage_batch_id: u64,
    pub completed_sync_id: Hash,
}
impl Ack {
    pub(crate) fn bytes(&self) -> Vec<u8> {
        let mut w = Writer::new();
        w.array(12);
        w.bytes(b"tickwire.replica.durable-ack");
        w.uint(1);
        w.bytes(&self.stream_digest);
        w.bytes(&self.domain);
        w.bytes(&self.journal_epoch);
        w.uint(self.source_exec_epoch);
        w.bytes(&self.receiver_id);
        w.bytes(&self.receiver_boot_id);
        w.uint(self.assignment_epoch);
        self.prefix.write(&mut w);
        w.uint(self.storage_batch_id);
        w.bytes(&self.completed_sync_id);
        w.0
    }
    pub fn verify(signed: &Signed, key: Hash) -> Result<Self> {
        signed.verify(key)?;
        let mut r = Reader::new(signed.payload());
        r.array(12)?;
        tag(&mut r, b"tickwire.replica.durable-ack")?;
        let a = Self {
            stream_digest: r.fixed()?,
            domain: r.fixed()?,
            journal_epoch: r.fixed()?,
            source_exec_epoch: r.uint()?,
            receiver_id: r.fixed()?,
            receiver_boot_id: r.fixed()?,
            assignment_epoch: r.uint()?,
            prefix: Prefix::read(&mut r)?,
            storage_batch_id: r.uint()?,
            completed_sync_id: r.fixed()?,
        };
        r.end()?;
        if a.receiver_id == [0; 16]
            || a.receiver_boot_id == [0; 16]
            || a.storage_batch_id == 0
            || a.completed_sync_id == [0; 32]
        {
            return Err(Error::Invalid("ack identity"));
        }
        Ok(a)
    }
}
