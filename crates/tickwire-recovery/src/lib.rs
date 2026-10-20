//! Immutable recovery dependencies and private, locally verified closure capabilities.
#![forbid(unsafe_code)]

use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, io};
mod codec;
mod handover;
mod inputs;
pub use handover::HandoverSeal;
pub use inputs::{EncodedInputs, InputRecord, encode_inputs};
mod files;
pub use files::{FileBlobReader, FileBlobStore};

pub type Hash = [u8; 32];
pub const COMMAND_DEPENDENCY_KIND: u8 = 13;
pub const MAX_OBJECT_BYTES: usize = 64 * 1024 * 1024;
pub const MAX_CLOSURE_BYTES: u64 = 256 * 1024 * 1024;
pub const MAX_MANIFEST_BYTES: usize = 1024 * 1024;
pub const MAX_OBJECTS: usize = 4096;
pub const MAX_EDGES: usize = 16384;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Invalid(&'static str),
    Missing,
    Conflict,
    Capacity,
}
impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
pub type Result<T> = std::result::Result<T, Error>;
pub fn digest(bytes: &[u8]) -> Hash {
    Sha256::digest(bytes).into()
}
fn replica_key(stream: Hash, sequence: u64) -> StoreKey {
    let mut bytes = b"tickwire.replica.entry-locator.v1".to_vec();
    bytes.extend(stream);
    bytes.extend(sequence.to_le_bytes());
    StoreKey::ReplicaEntry(digest(&bytes))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClosureBinding {
    pub risk_domain_id: [u8; 16],
    pub intent_id: u64,
    pub intent_exec_epoch: u64,
    pub business_digest: Hash,
    pub strategy_generation: u64,
    pub config_revision: u64,
    pub recovery_object: u64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryMode {
    DeterministicReplay,
    DeclaredRebuild,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InputBoundary {
    pub stream: [u8; 16],
    pub first_sequence: u64,
    pub previous_hash: Hash,
    pub last_sequence: u64,
    pub last_hash: Hash,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DependencyObject {
    /// 1..=12 are required; 13 is a command; 14/15 are the paired account baseline/history.
    pub kind: u8,
    pub content_hash: Hash,
    pub byte_length: u64,
    pub schema_id: u64,
    pub owner_identity: [u8; 16],
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryManifest {
    pub binding: ClosureBinding,
    pub logical_strategy_id: u64,
    pub instance_id: [u8; 16],
    pub action_grant_ref: Hash,
    pub operation_ref: Hash,
    pub mode: RecoveryMode,
    pub fault_model_id: Hash,
    pub pin_id: [u8; 16],
    pub input_boundary: InputBoundary,
    pub objects: Vec<DependencyObject>,
    /// Indices into objects, each (dependent, dependency).
    pub edges: Vec<(u16, u16)>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedClosure {
    binding: ClosureBinding,
    root: Hash,
    command_digest: Option<Hash>,
    account_baseline_digest: Option<Hash>,
    account_history_digest: Option<Hash>,
}
impl VerifiedClosure {
    pub fn binding(&self) -> &ClosureBinding {
        &self.binding
    }
    pub fn manifest_root(&self) -> Hash {
        self.root
    }
    /// Present only when the manifest includes a verified, synchronized command object.
    pub fn command_digest(&self) -> Option<Hash> {
        self.command_digest
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum StoreKey {
    Object(Hash),
    Manifest(Hash),
    Binding(Hash),
    /// Immutable replication metadata locator, independently namespaced.
    ReplicaEntry(Hash),
    ReplicaStream(Hash),
}
/// A trusted service implementation must honor successful synchronization.
pub trait BlobStore {
    fn get(&self, key: StoreKey, max_bytes: usize) -> Result<Vec<u8>>;
    fn put_sync(&mut self, key: StoreKey, bytes: &[u8]) -> Result<()>;
}
/// One exclusive store owner shared by a bounded persistence/replication worker.
/// Locks cover one local file operation and are never held over network I/O.
pub struct SharedBlobStore<S>(std::sync::Arc<std::sync::Mutex<S>>);
impl<S> Clone for SharedBlobStore<S> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
impl<S> SharedBlobStore<S> {
    pub fn new(store: S) -> Self {
        Self(std::sync::Arc::new(std::sync::Mutex::new(store)))
    }
}
impl<S: BlobStore> BlobStore for SharedBlobStore<S> {
    fn get(&self, key: StoreKey, limit: usize) -> Result<Vec<u8>> {
        self.0
            .lock()
            .map_err(|_| Error::Invalid("shared recovery owner poisoned"))?
            .get(key, limit)
    }
    fn put_sync(&mut self, key: StoreKey, bytes: &[u8]) -> Result<()> {
        self.0
            .lock()
            .map_err(|_| Error::Invalid("shared recovery owner poisoned"))?
            .put_sync(key, bytes)
    }
}
/// Explicit test double: successful put models reliable persistence, not real I/O.
#[derive(Default)]
pub struct MockBlobStore {
    blobs: BTreeMap<StoreKey, Vec<u8>>,
    pub fail_next_put: bool,
}
impl BlobStore for MockBlobStore {
    fn get(&self, key: StoreKey, limit: usize) -> Result<Vec<u8>> {
        let b = self.blobs.get(&key).ok_or(Error::Missing)?;
        if b.len() > limit {
            return Err(Error::Capacity);
        }
        Ok(b.clone())
    }
    fn put_sync(&mut self, key: StoreKey, bytes: &[u8]) -> Result<()> {
        if std::mem::take(&mut self.fail_next_put) {
            return Err(io::Error::other("injected sync failure").into());
        }
        if let Some(old) = self.blobs.get(&key) {
            if old != bytes {
                return Err(Error::Conflict);
            }
        } else {
            self.blobs.insert(key, bytes.to_vec());
        }
        Ok(())
    }
}
pub struct Persistence<S: BlobStore> {
    store: S,
}
impl<S: BlobStore> Persistence<S> {
    pub fn new(store: S) -> Self {
        Self { store }
    }
    pub fn into_store(self) -> S {
        self.store
    }
    pub fn read_replica_entry(&self, stream: Hash, sequence: u64) -> Result<Vec<u8>> {
        if stream == [0; 32] || sequence == 0 {
            return Err(Error::Invalid("replica locator"));
        }
        self.store.get(replica_key(stream, sequence), 65536)
    }
    pub fn put_replica_entry(&mut self, stream: Hash, sequence: u64, bytes: &[u8]) -> Result<()> {
        if stream == [0; 32] || sequence == 0 || bytes.is_empty() || bytes.len() > 65536 {
            return Err(Error::Capacity);
        }
        self.store.put_sync(replica_key(stream, sequence), bytes)
    }
    /// Pin the configuration of one execution source. A retry may synchronize
    /// identical bytes, but cannot change its durability mode or mirror.
    pub fn bind_replica_stream(&mut self, identity: &[u8], bytes: &[u8]) -> Result<()> {
        if identity.is_empty() || identity.len() > 1024 || bytes.is_empty() || bytes.len() > 65536 {
            return Err(Error::Capacity);
        }
        let mut key = b"tickwire.replica.source-configuration.v1".to_vec();
        key.extend(identity);
        self.store
            .put_sync(StoreKey::ReplicaStream(digest(&key)), bytes)
    }
    /// Read immutable content without creating a durability capability.
    pub fn read_object(&self, hash: Hash, max_bytes: usize) -> Result<Vec<u8>> {
        if max_bytes == 0 || max_bytes > MAX_OBJECT_BYTES {
            return Err(Error::Capacity);
        }
        let bytes = self.store.get(StoreKey::Object(hash), max_bytes)?;
        if digest(&bytes) != hash {
            return Err(Error::Conflict);
        }
        Ok(bytes)
    }
    pub fn read_manifest(&self, binding: &ClosureBinding) -> Result<RecoveryManifest> {
        let root: Hash = self
            .store
            .get(binding_key(binding), 32)?
            .try_into()
            .map_err(|_| Error::Invalid("binding length"))?;
        let bytes = self
            .store
            .get(StoreKey::Manifest(root), MAX_MANIFEST_BYTES)?;
        if digest(&bytes) != root {
            return Err(Error::Conflict);
        }
        let manifest = RecoveryManifest::decode(&bytes)?;
        if manifest.binding != *binding {
            return Err(Error::Conflict);
        }
        Ok(manifest)
    }
    pub fn put_object(&mut self, bytes: &[u8]) -> Result<Hash> {
        if bytes.is_empty() || bytes.len() > MAX_OBJECT_BYTES {
            return Err(Error::Capacity);
        }
        let hash = digest(bytes);
        self.store.put_sync(StoreKey::Object(hash), bytes)?;
        Ok(hash)
    }
    pub fn publish(&mut self, manifest: &RecoveryManifest) -> Result<VerifiedClosure> {
        self.check(manifest)?;
        let bytes = manifest.canonical_bytes()?;
        let root = digest(&bytes);
        self.store.put_sync(StoreKey::Manifest(root), &bytes)?;
        // This immutable reference is also a permanent pin in this version.
        self.store.put_sync(binding_key(&manifest.binding), &root)?;
        Ok(VerifiedClosure {
            binding: manifest.binding,
            root,
            command_digest: command_digest(manifest),
            account_baseline_digest: dependency_digest(manifest, ACCOUNT_BASELINE_DEPENDENCY_KIND),
            account_history_digest: dependency_digest(manifest, ACCOUNT_HISTORY_DEPENDENCY_KIND),
        })
    }
    pub fn verify(&mut self, binding: &ClosureBinding) -> Result<VerifiedClosure> {
        let root: Hash = self
            .store
            .get(binding_key(binding), 32)?
            .try_into()
            .map_err(|_| Error::Invalid("binding root length"))?;
        let bytes = self
            .store
            .get(StoreKey::Manifest(root), MAX_MANIFEST_BYTES)?;
        if digest(&bytes) != root {
            return Err(Error::Invalid("manifest digest"));
        }
        let m = RecoveryManifest::decode(&bytes)?;
        if m.binding != *binding {
            return Err(Error::Conflict);
        }
        self.check(&m)?;
        self.store.put_sync(StoreKey::Manifest(root), &bytes)?;
        self.store.put_sync(binding_key(binding), &root)?;
        Ok(VerifiedClosure {
            binding: *binding,
            root,
            command_digest: command_digest(&m),
            account_baseline_digest: dependency_digest(&m, ACCOUNT_BASELINE_DEPENDENCY_KIND),
            account_history_digest: dependency_digest(&m, ACCOUNT_HISTORY_DEPENDENCY_KIND),
        })
    }
    fn check(&mut self, m: &RecoveryManifest) -> Result<()> {
        validate_shape(m)?;
        let mut inputs = Vec::new();
        for object in &m.objects {
            let bytes = self.store.get(
                StoreKey::Object(object.content_hash),
                object.byte_length as usize,
            )?;
            if bytes.len() as u64 != object.byte_length || digest(&bytes) != object.content_hash {
                return Err(Error::Invalid("object digest/length"));
            }
            if object.kind == 12 {
                inputs.push(inputs::inspect(&bytes)?);
            }
            // Reestablish completion even when a previous publication failed
            // after rename but before directory synchronization.
            self.store
                .put_sync(StoreKey::Object(object.content_hash), &bytes)?;
        }
        inputs.sort_unstable_by_key(|b| b.first_sequence);
        let expected = m.input_boundary;
        let mut next = expected.first_sequence;
        let mut hash = expected.previous_hash;
        let len = inputs.len();
        for b in inputs {
            if b.stream != expected.stream
                || b.first_sequence != next
                || b.previous_hash != hash
                || (b.last_sequence < b.first_sequence && len != 1)
            {
                return Err(Error::Invalid("input coverage discontinuity"));
            }
            next = b.last_sequence.checked_add(1).ok_or(Error::Capacity)?;
            hash = b.last_hash;
        }
        if next != expected.last_sequence + 1 || hash != expected.last_hash {
            return Err(Error::Invalid("input decision boundary"));
        }
        Ok(())
    }
}

fn command_digest(manifest: &RecoveryManifest) -> Option<Hash> {
    dependency_digest(manifest, COMMAND_DEPENDENCY_KIND)
}

fn dependency_digest(manifest: &RecoveryManifest, kind: u8) -> Option<Hash> {
    manifest
        .objects
        .iter()
        .find(|object| object.kind == kind)
        .map(|object| object.content_hash)
}

pub fn binding_key(binding: &ClosureBinding) -> StoreKey {
    let mut h = Sha256::new();
    h.update(b"tickwire.manifest-binding.v1");
    h.update(binding.risk_domain_id);
    h.update(binding.recovery_object.to_be_bytes());
    StoreKey::Binding(h.finalize().into())
}
fn validate_shape(m: &RecoveryManifest) -> Result<()> {
    let b = &m.binding;
    if b.risk_domain_id == [0; 16]
        || b.intent_id == 0
        || b.business_digest == [0; 32]
        || b.recovery_object == 0
        || [
            b.intent_exec_epoch,
            b.strategy_generation,
            b.config_revision,
            m.logical_strategy_id,
        ]
        .iter()
        .any(|n| *n == 0 || *n >= 1 << 63)
        || m.instance_id == [0; 16]
        || m.pin_id == [0; 16]
        || m.fault_model_id == [0; 32]
        || m.objects.is_empty()
        || m.objects.len() > MAX_OBJECTS
        || m.edges.len() > MAX_EDGES
    {
        return Err(Error::Invalid("manifest identity/capacity"));
    }
    let i = &m.input_boundary;
    if i.stream == [0; 16]
        || i.first_sequence == 0
        || i.first_sequence >= 1 << 63
        || i.last_sequence >= 1 << 63
        || i.last_sequence < i.first_sequence - 1
    {
        return Err(Error::Invalid("input boundary"));
    }
    let mut kinds = 0u16;
    let mut total = 0u64;
    let mut intent = 0;
    let mut grant = 0;
    let mut operation = 0;
    let mut commands = 0;
    let mut account_baselines = 0;
    let mut account_histories = 0;
    for (index, o) in m.objects.iter().enumerate() {
        if !(1..=ACCOUNT_HISTORY_DEPENDENCY_KIND).contains(&o.kind)
            || o.schema_id == 0
            || o.owner_identity == [0; 16]
            || o.content_hash == [0; 32]
            || o.byte_length == 0
            || o.byte_length > MAX_OBJECT_BYTES as u64
        {
            return Err(Error::Invalid("object descriptor"));
        }
        if index > 0
            && (m.objects[index - 1].kind, m.objects[index - 1].content_hash)
                >= (o.kind, o.content_hash)
        {
            return Err(Error::Invalid("object order/duplicate"));
        }
        total = total
            .checked_add(o.byte_length)
            .filter(|n| *n <= MAX_CLOSURE_BYTES)
            .ok_or(Error::Capacity)?;
        kinds |= 1 << (o.kind - 1);
        match o.kind {
            1 => {
                intent += 1;
                if o.content_hash != b.business_digest {
                    return Err(Error::Invalid("intent digest"));
                }
            }
            4 if o.content_hash == m.action_grant_ref => grant += 1,
            7 if o.content_hash == m.operation_ref => operation += 1,
            COMMAND_DEPENDENCY_KIND => commands += 1,
            ACCOUNT_BASELINE_DEPENDENCY_KIND => account_baselines += 1,
            ACCOUNT_HISTORY_DEPENDENCY_KIND => account_histories += 1,
            _ => {}
        }
    }
    if kinds & 0xfff != 0xfff || intent != 1 || grant != 1 || operation != 1 || commands > 1 {
        return Err(Error::Invalid("required dependencies"));
    }
    if (account_baselines != 0 || account_histories != 0)
        && (account_baselines != 1 || account_histories != 1 || commands != 1)
    {
        return Err(Error::Invalid("account dependency pair"));
    }
    let n = m.objects.len();
    let mut degree = vec![0usize; n];
    let mut successors = vec![Vec::new(); n];
    for (index, &(from, to)) in m.edges.iter().enumerate() {
        if from as usize >= n
            || to as usize >= n
            || from == to
            || (index > 0 && m.edges[index - 1] >= (from, to))
        {
            return Err(Error::Invalid("edge"));
        }
        degree[to as usize] += 1;
        successors[from as usize].push(to as usize);
    }
    let mut todo: Vec<_> = (0..n).filter(|i| degree[*i] == 0).collect();
    let mut visited = 0;
    while let Some(i) = todo.pop() {
        visited += 1;
        for &j in &successors[i] {
            degree[j] -= 1;
            if degree[j] == 0 {
                todo.push(j);
            }
        }
    }
    if visited != n {
        return Err(Error::Invalid("dependency cycle"));
    }
    Ok(())
}

pub const ACCOUNT_BASELINE_DEPENDENCY_KIND: u8 = 14;
pub const ACCOUNT_HISTORY_DEPENDENCY_KIND: u8 = 15;

impl VerifiedClosure {
    pub fn account_baseline_digest(&self) -> Option<Hash> {
        self.account_baseline_digest
    }
    pub fn account_history_digest(&self) -> Option<Hash> {
        self.account_history_digest
    }
}
