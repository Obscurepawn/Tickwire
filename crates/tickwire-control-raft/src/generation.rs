//! Private snapshot directories. Publication is reserved for the Node after its
//! new Raft journal has been synchronized and reopened successfully.
use crate::{Error, Result, snapshot::*};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
};

#[derive(Clone)]
pub struct SnapshotDirectory {
    inner: Arc<Directory>,
}
struct Directory {
    root: PathBuf,
    _lock: File,
    namespace: Mutex<()>,
    node_owner: Mutex<Weak<[u8; 16]>>,
    group: [u8; 16],
    node: u64,
    limits: SnapshotLimits,
    poisoned: AtomicBool,
    leases: Mutex<BTreeMap<[u8; 16], Weak<()>>>,
    #[cfg(test)]
    stage_pause: Mutex<Option<(Arc<std::sync::Barrier>, Arc<std::sync::Barrier>)>>,
    #[cfg(test)]
    fault_after: std::sync::atomic::AtomicUsize,
    #[cfg(test)]
    steps: std::sync::atomic::AtomicUsize,
}
pub struct StagingSnapshot {
    directory: Arc<Directory>,
    generation: [u8; 16],
    path: PathBuf,
    objects: BTreeMap<ObjectRef, u64>,
    sealed: BTreeSet<ObjectRef>,
    bytes: u64,
    poisoned: bool,
    lease: Arc<()>,
}
#[derive(Clone)]
pub struct VerifiedSnapshot {
    directory: Arc<Directory>,
    generation: [u8; 16],
    path: PathBuf,
    manifest: SnapshotManifest,
    _lease: Arc<()>,
}
struct Pointer {
    generation: [u8; 16],
    root: [u8; 32],
    previous: Option<([u8; 16], [u8; 32])>,
    legacy: bool,
}
impl SnapshotDirectory {
    pub fn open(
        path: impl AsRef<Path>,
        group: [u8; 16],
        node: u64,
        limits: SnapshotLimits,
    ) -> Result<Self> {
        limits.validate()?;
        if group == [0; 16] || node == 0 {
            return Err(Error::Identity);
        }
        let path = path.as_ref();
        match private_directory(path) {
            Ok(()) => {}
            Err(e) => return Err(e),
        }
        let root = fs::canonicalize(path).map_err(|_| Error::Store)?;
        let lock_path = root.join("LOCK");
        if lock_path.exists() {
            regular(&lock_path)?;
        }
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(lock_path)
            .map_err(|_| Error::Store)?;
        lock.try_lock().map_err(|_| Error::Store)?;
        lock.sync_all().map_err(|_| Error::Store)?;
        let mut identity = b"TWGD0001".to_vec();
        identity.extend_from_slice(&group);
        identity.extend_from_slice(&node.to_le_bytes());
        identity.extend_from_slice(&Sha256::digest(&identity));
        let identity_path = root.join("IDENTITY");
        if identity_path.exists() {
            if read_file(&identity_path, 64)? != identity {
                return Err(Error::Identity);
            }
        } else {
            if fs::read_dir(&root).map_err(|_| Error::Store)?.count() != 1 {
                return Err(Error::Corrupt);
            }
            write_new(&identity_path, &identity)?;
        }
        sync_dir(&root)?;
        sync_dir(root.parent().ok_or(Error::Store)?)?;
        Ok(Self {
            inner: Arc::new(Directory {
                root,
                _lock: lock,
                namespace: Mutex::new(()),
                node_owner: Mutex::new(Weak::new()),
                group,
                node,
                limits,
                poisoned: AtomicBool::new(false),
                leases: Mutex::new(BTreeMap::new()),
                #[cfg(test)]
                stage_pause: Mutex::new(None),
                #[cfg(test)]
                fault_after: std::sync::atomic::AtomicUsize::new(0),
                #[cfg(test)]
                steps: std::sync::atomic::AtomicUsize::new(0),
            }),
        })
    }
    fn claim_node(&self) -> Result<Arc<[u8; 16]>> {
        self.inner.healthy()?;
        let mut owner = self.inner.node_owner.lock().map_err(|_| Error::Store)?;
        if owner.strong_count() > 0 {
            return Err(Error::Identity);
        }
        let lease = Arc::new(random_id()?);
        *owner = Arc::downgrade(&lease);
        Ok(lease)
    }
    pub fn stage(&self) -> Result<StagingSnapshot> {
        let _namespace = self.inner.namespace.lock().map_err(|_| Error::Store)?;
        self.inner.healthy()?;
        // Retain failed/orphaned generations for explicit cleanup rather than
        // guessing that the greatest filename is the current durable state.
        let mut count = 0;
        for entry in fs::read_dir(&self.inner.root).map_err(|_| Error::Store)? {
            if entry
                .map_err(|_| Error::Store)?
                .file_name()
                .to_string_lossy()
                .starts_with("generation-")
            {
                count += 1;
            }
        }
        if count >= 16 {
            let selected = self.pointer()?.map(|p| p.generation);
            if let Err(e) = self.collect_unused(selected) {
                self.inner.poisoned.store(true, Ordering::Relaxed);
                return Err(e);
            }
            count = fs::read_dir(&self.inner.root)
                .map_err(|_| Error::Store)?
                .try_fold(0usize, |n, e| {
                    let e = e.map_err(|_| Error::Store)?;
                    Ok::<_, Error>(
                        n + usize::from(e.file_name().to_string_lossy().starts_with("generation-")),
                    )
                })?;
            if count >= 16 {
                return Err(Error::Bounds);
            }
        }
        let generation = random_id()?;
        let path = self
            .inner
            .root
            .join(format!("generation-{}", hex(&generation)));
        private_directory(&path)?;
        sync_dir(&self.inner.root)?;
        #[cfg(test)]
        {
            let pause = self
                .inner
                .stage_pause
                .lock()
                .map_err(|_| Error::Store)?
                .take();
            if let Some((created, resume)) = pause {
                created.wait();
                resume.wait();
            }
        }
        Ok(StagingSnapshot {
            directory: Arc::clone(&self.inner),
            generation,
            path,
            objects: BTreeMap::new(),
            sealed: BTreeSet::new(),
            bytes: 0,
            poisoned: false,
            lease: self.inner.lease(generation)?,
        })
    }
    pub fn current(&self) -> Result<Option<VerifiedSnapshot>> {
        Ok(self.selected()?.and_then(|(_, snapshot)| snapshot))
    }
    fn pointer(&self) -> Result<Option<Pointer>> {
        let path = self.inner.root.join("CURRENT");
        if !path.try_exists().map_err(|_| Error::Store)? {
            return Ok(None);
        }
        let bytes = read_file(&path, 160)?;
        let legacy = match (bytes.len(), bytes.get(..8)) {
            (112, Some(b"TWGC0001")) => true,
            (160, Some(b"TWGC0002")) => false,
            _ => return Err(Error::Corrupt),
        };
        let body = bytes.len() - 32;
        if Sha256::digest(&bytes[..body])[..] != bytes[body..] {
            return Err(Error::Corrupt);
        }
        if bytes[56..64] != self.inner.node.to_le_bytes() || bytes[64..80] != self.inner.group {
            return Err(Error::Identity);
        }
        let generation = bytes[8..24].try_into().map_err(|_| Error::Corrupt)?;
        let root = bytes[24..56].try_into().map_err(|_| Error::Corrupt)?;
        if generation == [0; 16] {
            return Err(Error::Corrupt);
        }
        let previous = if legacy || bytes[80..96] == [0; 16] {
            if !legacy && bytes[96..128] != [0; 32] {
                return Err(Error::Corrupt);
            }
            None
        } else {
            let old: [u8; 16] = bytes[80..96].try_into().map_err(|_| Error::Corrupt)?;
            if old == generation || root == [0; 32] {
                return Err(Error::Corrupt);
            }
            Some((old, bytes[96..128].try_into().map_err(|_| Error::Corrupt)?))
        };
        Ok(Some(Pointer {
            generation,
            root,
            previous,
            legacy,
        }))
    }
    fn selected(&self) -> Result<Option<([u8; 16], Option<VerifiedSnapshot>)>> {
        let _namespace = self.inner.namespace.lock().map_err(|_| Error::Store)?;
        self.selected_locked()
    }
    fn selected_locked(&self) -> Result<Option<([u8; 16], Option<VerifiedSnapshot>)>> {
        let Some(pointer) = self.pointer()? else {
            return Ok(None);
        };
        if let Some((generation, root)) = pointer.previous {
            self.read_generation(generation, root)?;
        }
        Ok(Some((
            pointer.generation,
            self.read_generation(pointer.generation, pointer.root)?,
        )))
    }
    fn read_generation(
        &self,
        generation: [u8; 16],
        root: [u8; 32],
    ) -> Result<Option<VerifiedSnapshot>> {
        let path = self
            .inner
            .root
            .join(format!("generation-{}", hex(&generation)));
        check_directory(&path)?;
        if root == [0; 32] {
            if path
                .join("SNAPSHOT")
                .try_exists()
                .map_err(|_| Error::Store)?
            {
                return Err(Error::Corrupt);
            }
            return Ok(None);
        }
        let manifest_bytes = read_file(&path.join("SNAPSHOT"), MAX_MANIFEST_BYTES)?;
        if Sha256::digest(&manifest_bytes)[..] != root {
            return Err(Error::Corrupt);
        }
        let manifest = SnapshotManifest::decode(&manifest_bytes, self.inner.limits)?;
        if manifest.cut().group() != self.inner.group {
            return Err(Error::Identity);
        }
        let verified = VerifiedSnapshot {
            directory: Arc::clone(&self.inner),
            generation,
            path,
            manifest,
            _lease: self.inner.lease(generation)?,
        };
        for object in verified.manifest.objects() {
            verified.read(object)?;
        }
        Ok(Some(verified))
    }
    pub(crate) fn publish(&self, snapshot: &VerifiedSnapshot) -> Result<()> {
        self.inner.healthy()?;
        if !Arc::ptr_eq(&self.inner, &snapshot.directory)
            || snapshot.manifest.cut().group() != self.inner.group
        {
            return Err(Error::Identity);
        }
        let result = (|| {
            for object in snapshot.manifest.objects() {
                snapshot.read(object)?;
            }
            let manifest = snapshot.manifest.encode()?;
            if read_file(&snapshot.path.join("SNAPSHOT"), MAX_MANIFEST_BYTES)? != manifest {
                return Err(Error::Corrupt);
            }
            self.publish_pointer(snapshot.generation, Sha256::digest(&manifest).into())
        })();
        if result.is_err() {
            self.inner.poisoned.store(true, Ordering::Relaxed);
        }
        result
    }
    fn publish_pointer(&self, generation: [u8; 16], root: [u8; 32]) -> Result<()> {
        let _namespace = self.inner.namespace.lock().map_err(|_| Error::Store)?;
        let previous = self.pointer()?;
        if let Some(old) = &previous {
            if old.generation == generation || root == [0; 32] {
                return Err(Error::Identity);
            }
            self.read_generation(old.generation, old.root)?;
        }
        let mut bytes = b"TWGC0002".to_vec();
        bytes.extend_from_slice(&generation);
        bytes.extend_from_slice(&root);
        bytes.extend_from_slice(&self.inner.node.to_le_bytes());
        bytes.extend_from_slice(&self.inner.group);
        bytes.extend_from_slice(&previous.as_ref().map_or([0; 16], |p| p.generation));
        bytes.extend_from_slice(&previous.as_ref().map_or([0; 32], |p| p.root));
        bytes.extend_from_slice(&Sha256::digest(&bytes));
        let temporary = self.inner.root.join("CURRENT.tmp");
        if temporary.try_exists().map_err(|_| Error::Store)? {
            regular(&temporary)?;
            fs::remove_file(&temporary).map_err(|_| Error::Store)?;
        }
        self.inner.write_new(&temporary, &bytes)?;
        fs::rename(&temporary, self.inner.root.join("CURRENT")).map_err(|_| Error::Store)?;
        self.inner.checkpoint()?;
        sync_dir(&self.inner.root)?;
        self.inner.checkpoint()
    }
    fn bind_config(&self, config: &crate::Config, create: bool) -> Result<()> {
        let _namespace = self.inner.namespace.lock().map_err(|_| Error::Store)?;
        if config.node_id != self.inner.node || config.physical.domain != self.inner.group {
            return Err(Error::Identity);
        }
        let path = self.inner.root.join("CONFIG");
        let bytes = config.identity();
        if path.try_exists().map_err(|_| Error::Store)? {
            if read_file(&path, 256)? != bytes {
                return Err(Error::Identity);
            }
        } else if create {
            write_new(&path, &bytes)?;
            sync_dir(&self.inner.root)?;
        } else {
            return Err(Error::Corrupt);
        }
        Ok(())
    }
    fn collect_old(&self, expected: [u8; 16]) -> Result<usize> {
        let _namespace = self.inner.namespace.lock().map_err(|_| Error::Store)?;
        self.collect_unused(Some(expected))
    }
    fn collect_unused(&self, expected: Option<[u8; 16]>) -> Result<usize> {
        self.inner.healthy()?;
        let selected = self.selected_locked()?;
        if selected.as_ref().map(|s| s.0) != expected {
            return Err(Error::Identity);
        }
        let pointer = self.pointer()?;
        let previous = pointer.as_ref().and_then(|p| p.previous.map(|p| p.0));
        let legacy = pointer.as_ref().is_some_and(|p| p.legacy);
        // Publish has already synchronized CURRENT's parent. A live reader,
        // staged writer or child WAL pins its own generation during cleanup.
        let mut leases = self.inner.leases.lock().map_err(|_| Error::Store)?;
        let mut removed = 0;
        for entry in fs::read_dir(&self.inner.root).map_err(|_| Error::Store)? {
            let entry = entry.map_err(|_| Error::Store)?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            let Some(encoded) = name.strip_prefix("generation-") else {
                continue;
            };
            let id = parse_id(encoded)?;
            if Some(id) == expected
                || Some(id) == previous
                || legacy
                    && entry
                        .path()
                        .join("SNAPSHOT")
                        .try_exists()
                        .map_err(|_| Error::Store)?
                || leases
                    .get(&id)
                    .is_some_and(|lease| lease.strong_count() > 0)
            {
                continue;
            }
            check_directory(&entry.path())?;
            fs::remove_dir_all(entry.path()).map_err(|_| Error::Store)?;
            self.inner.checkpoint()?;
            sync_dir(&self.inner.root)?;
            self.inner.checkpoint()?;
            leases.remove(&id);
            removed += 1;
        }
        Ok(removed)
    }
}

/// The parent directory lock survives every child WAL replacement.
pub struct GenerationStore {
    directory: Arc<Directory>,
    file: tickwire_journal::store::FileStore,
    _lease: Arc<()>,
}
impl GenerationStore {
    fn open(directory: &SnapshotDirectory, generation: [u8; 16]) -> Result<Self> {
        directory.inner.healthy()?;
        let path = directory
            .inner
            .root
            .join(format!("generation-{}", hex(&generation)));
        check_directory(&path)?;
        Ok(Self {
            directory: Arc::clone(&directory.inner),
            file: tickwire_journal::store::FileStore::open(path.join("wal"))
                .map_err(|_| Error::Store)?,
            _lease: directory.inner.lease(generation)?,
        })
    }
    fn healthy_io(&self) -> std::io::Result<()> {
        self.directory
            .healthy()
            .map_err(|_| std::io::Error::other("poisoned generation"))
    }
    fn checkpoint_io(&self) -> std::io::Result<()> {
        self.directory
            .checkpoint()
            .map_err(|_| std::io::Error::other("injected generation I/O failure"))
    }
}
impl tickwire_journal::store::Store for GenerationStore {
    fn read_manifest(&mut self) -> std::io::Result<Option<Vec<u8>>> {
        self.healthy_io()?;
        self.file.read_manifest()
    }
    fn publish_manifest(&mut self, b: &[u8]) -> std::io::Result<()> {
        self.healthy_io()?;
        self.file.publish_manifest(b)?;
        self.checkpoint_io()
    }
    fn segment_ids(&mut self) -> std::io::Result<Vec<u64>> {
        self.healthy_io()?;
        self.file.segment_ids()
    }
    fn read_segment(&mut self, id: u64, max: u64) -> std::io::Result<Vec<u8>> {
        self.healthy_io()?;
        self.file.read_segment(id, max)
    }
    fn create_segment(&mut self, id: u64) -> std::io::Result<()> {
        self.healthy_io()?;
        self.file.create_segment(id)?;
        self.checkpoint_io()
    }
    fn append_segment(&mut self, id: u64, offset: u64, b: &[u8]) -> std::io::Result<()> {
        self.healthy_io()?;
        self.file.append_segment(id, offset, b)?;
        self.checkpoint_io()
    }
    fn sync_segment(&mut self, id: u64) -> std::io::Result<()> {
        self.healthy_io()?;
        self.file.sync_segment(id)?;
        self.checkpoint_io()
    }
    fn truncate_segment(&mut self, id: u64, len: u64) -> std::io::Result<()> {
        self.healthy_io()?;
        self.file.truncate_segment(id, len)?;
        self.checkpoint_io()
    }
}
impl crate::Node<GenerationStore> {
    pub fn create_in(directory: SnapshotDirectory, config: crate::Config) -> Result<Self> {
        config.validate()?;
        Self::create_generation(directory, config, false)
    }
    pub fn create_joining_in(directory: SnapshotDirectory, config: crate::Config) -> Result<Self> {
        config.validate_identity()?;
        if config.voters.contains(&config.node_id) {
            return Err(Error::Identity);
        }
        Self::create_generation(directory, config, true)
    }
    fn create_generation(
        directory: SnapshotDirectory,
        mut config: crate::Config,
        joining: bool,
    ) -> Result<Self> {
        let owner = directory.claim_node()?;
        directory.inner.healthy()?;
        if directory.selected()?.is_some() {
            return Err(Error::Identity);
        }
        directory.bind_config(&config, true)?;
        let staged = directory.stage()?;
        let generation = staged.generation;
        config.physical.stream_epoch = generation;
        let store = GenerationStore::open(&directory, generation)?;
        let created = if joining {
            Self::create_joining(store, config.clone())?
        } else {
            Self::create(store, config.clone())?
        };
        drop(created);
        let mut reopened = Self::recover(GenerationStore::open(&directory, generation)?, config)?;
        if let Err(error) = directory.publish_pointer(generation, [0; 32]) {
            directory.inner.poisoned.store(true, Ordering::Relaxed);
            return Err(error);
        }
        reopened.generation_owner = Some(owner);
        reopened.generation_factory = Some(GenerationStore::open);
        reopened.generation_directory = Some(directory);
        Ok(reopened)
    }
    pub fn recover_in(directory: SnapshotDirectory, mut config: crate::Config) -> Result<Self> {
        config.validate_identity()?;
        let owner = directory.claim_node()?;
        directory.inner.healthy()?;
        directory.bind_config(&config, false)?;
        let (mut node, snapshot) = {
            let _namespace = directory.inner.namespace.lock().map_err(|_| Error::Store)?;
            let (generation, snapshot) = directory.selected_locked()?.ok_or(Error::Corrupt)?;
            config.physical.stream_epoch = generation;
            let node = Self::recover(GenerationStore::open(&directory, generation)?, config)?;
            (node, snapshot)
        };
        node.generation_owner = Some(owner);
        if node.current_snapshot() != snapshot.as_ref().map(|s| &s.manifest) {
            return Err(Error::Corrupt);
        }
        if let Some(snapshot) = snapshot {
            node.installed = Some(InstalledGeneration {
                owner: **node.generation_owner.as_ref().ok_or(Error::Identity)?,
                node_id: directory.inner.node,
                generation: snapshot.generation,
                manifest: snapshot.manifest,
            });
        }
        node.generation_factory = Some(GenerationStore::open);
        node.generation_directory = Some(directory);
        Ok(node)
    }
    pub fn collect_old_generations(&mut self) -> Result<usize> {
        self.healthy()?;
        let result = self
            .generation_directory
            .as_ref()
            .ok_or(Error::NotReady)?
            .collect_old(self.raw.store().config.physical.stream_epoch);
        if result.is_err() {
            self.poisoned = true;
            self.reads.clear();
        }
        result
    }
    pub fn compact(&mut self, snapshot: &VerifiedSnapshot) -> Result<InstalledGeneration> {
        self.healthy()?;
        let directory = self
            .generation_directory
            .as_ref()
            .ok_or(Error::NotReady)?
            .clone();
        if !Arc::ptr_eq(&directory.inner, &snapshot.directory)
            || snapshot.manifest.cut() != &self.snapshot_cut(self.applied_index())?
        {
            return Err(Error::Identity);
        }
        if snapshot.manifest.cut().index() <= self.snapshot_base_index() {
            return Err(Error::NotReady);
        }
        let result = (|| {
            let old = self.raw.store();
            let mut config = old.config.clone();
            config.physical.stream_epoch = snapshot.generation;
            let hard = old.hard.clone();
            let tail = old
                .log
                .iter()
                .filter(|e| e.index > snapshot.manifest.cut().index())
                .cloned()
                .collect::<Vec<_>>();
            let applied = old.applied;
            let seeded = crate::storage::DurableStorage::seed(
                GenerationStore::open(&directory, snapshot.generation)?,
                config.clone(),
                snapshot.manifest.clone(),
                hard.clone(),
                tail.clone(),
                applied,
            )?;
            drop(seeded);
            let reopened = crate::storage::DurableStorage::recover(
                GenerationStore::open(&directory, snapshot.generation)?,
                config,
            )?;
            if reopened.snapshot.as_ref() != Some(&snapshot.manifest)
                || reopened.hard != hard
                || reopened.log != tail
                || reopened.applied != applied
            {
                return Err(Error::Corrupt);
            }
            directory.publish(snapshot)?;
            *self.raw.mut_store() = reopened;
            let receipt = InstalledGeneration {
                owner: **self.generation_owner.as_ref().ok_or(Error::Identity)?,
                node_id: directory.inner.node,
                generation: snapshot.generation,
                manifest: snapshot.manifest.clone(),
            };
            self.installed = Some(receipt.clone());
            Ok(receipt)
        })();
        if result.is_err() {
            self.poisoned = true;
            self.reads.clear();
        }
        result
    }
}
impl<S: tickwire_journal::store::Store> crate::Node<S> {
    pub(crate) fn check_snapshot_owner(&self, snapshot: &VerifiedSnapshot) -> Result<()> {
        let directory = self.generation_directory.as_ref().ok_or(Error::NotReady)?;
        if !Arc::ptr_eq(&directory.inner, &snapshot.directory)
            || snapshot.manifest.cut().group() != self.status().group
        {
            return Err(Error::Identity);
        }
        Ok(())
    }
    pub(crate) fn install_remote_snapshot(
        &mut self,
        snapshot: &VerifiedSnapshot,
        hard: raft::eraftpb::HardState,
        tail: Vec<raft::eraftpb::Entry>,
    ) -> Result<InstalledGeneration> {
        self.check_snapshot_owner(snapshot)?;
        if snapshot.manifest.cut().index() <= self.raw.store().applied {
            return Err(Error::Protocol);
        }
        let directory = self
            .generation_directory
            .as_ref()
            .ok_or(Error::NotReady)?
            .clone();
        let factory = self.generation_factory.ok_or(Error::NotReady)?;
        let mut config = self.raw.store().config.clone();
        config.physical.stream_epoch = snapshot.generation;
        let applied = snapshot.manifest.cut().index();
        let seeded = crate::storage::DurableStorage::seed(
            factory(&directory, snapshot.generation)?,
            config.clone(),
            snapshot.manifest.clone(),
            hard.clone(),
            tail.clone(),
            applied,
        )?;
        drop(seeded);
        let reopened = crate::storage::DurableStorage::recover(
            factory(&directory, snapshot.generation)?,
            config,
        )?;
        if reopened.snapshot.as_ref() != Some(&snapshot.manifest)
            || reopened.hard != hard
            || reopened.log != tail
            || reopened.applied != applied
        {
            return Err(Error::Corrupt);
        }
        directory.publish(snapshot)?;
        *self.raw.mut_store() = reopened;
        let receipt = InstalledGeneration {
            owner: **self.generation_owner.as_ref().ok_or(Error::Identity)?,
            node_id: directory.inner.node,
            generation: snapshot.generation,
            manifest: snapshot.manifest.clone(),
        };
        self.installed = Some(receipt.clone());
        Ok(receipt)
    }
    pub fn stage_snapshot(&self) -> Result<StagingSnapshot> {
        self.healthy()?;
        self.generation_directory
            .as_ref()
            .ok_or(Error::NotReady)?
            .stage()
    }
    pub fn snapshot_reader(&self) -> Result<Option<VerifiedSnapshot>> {
        self.healthy()?;
        let selected = self
            .generation_directory
            .as_ref()
            .ok_or(Error::NotReady)?
            .selected()?
            .ok_or(Error::Corrupt)?;
        if selected.0 != self.raw.store().config.physical.stream_epoch {
            return Err(Error::Identity);
        }
        Ok(selected.1)
    }
}
impl StagingSnapshot {
    pub fn append(&mut self, object: ObjectRef, offset: u64, bytes: &[u8]) -> Result<()> {
        self.directory.healthy()?;
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        if bytes.len() > 65_536
            || self.sealed.contains(&object)
            || object.length() > self.directory.limits.max_object_bytes as u64
        {
            return Err(Error::Bounds);
        }
        let position = self.objects.get(&object).copied().unwrap_or(0);
        if offset != position
            || offset
                .checked_add(bytes.len() as u64)
                .is_none_or(|n| n > object.length())
            || (bytes.is_empty() && object.length() != 0)
        {
            return Err(Error::Protocol);
        }
        let path = object_path(&self.path, object, "part");
        let first = !self.objects.contains_key(&object);
        if first
            && (self.objects.len() >= self.directory.limits.max_objects
                || self
                    .bytes
                    .checked_add(object.length())
                    .is_none_or(|n| n > self.directory.limits.max_closure_bytes)
                || self.objects.keys().any(|o| o.digest() == object.digest()))
        {
            return Err(Error::Bounds);
        }
        let result = (|| {
            let mut file = if first {
                new_file(&path)?
            } else {
                regular(&path)?;
                OpenOptions::new()
                    .write(true)
                    .open(&path)
                    .map_err(|_| Error::Store)?
            };
            if file.metadata().map_err(|_| Error::Store)?.len() != offset {
                return Err(Error::Corrupt);
            }
            file.seek(SeekFrom::Start(offset))
                .map_err(|_| Error::Store)?;
            file.write_all(bytes).map_err(|_| Error::Store)?;
            self.directory.checkpoint()
        })();
        if result.is_err() {
            self.poisoned = true;
            return result;
        }
        if first {
            self.bytes += object.length();
        }
        self.objects.insert(object, offset + bytes.len() as u64);
        Ok(())
    }
    pub fn seal_object(&mut self, object: ObjectRef) -> Result<()> {
        self.directory.healthy()?;
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        if self.sealed.contains(&object) {
            return Ok(());
        }
        if self.objects.get(&object) != Some(&object.length()) {
            return Err(Error::NotReady);
        }
        let path = object_path(&self.path, object, "part");
        let bytes = read_file(&path, object.length() as usize)?;
        object.verify(&bytes)?;
        let result = (|| {
            File::open(&path)
                .map_err(|_| Error::Store)?
                .sync_all()
                .map_err(|_| Error::Store)?;
            self.directory.checkpoint()?;
            fs::rename(&path, object_path(&self.path, object, "bin")).map_err(|_| Error::Store)?;
            self.directory.checkpoint()?;
            sync_dir(&self.path)?;
            self.directory.checkpoint()
        })();
        if result.is_err() {
            self.poisoned = true;
            return result;
        }
        self.sealed.insert(object);
        Ok(())
    }
    pub fn finish(self, cut: SnapshotCut, root: ApplicationRoot) -> Result<VerifiedSnapshot> {
        self.directory.healthy()?;
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        if self.sealed.len() != self.objects.len() || cut.group() != self.directory.group {
            return Err(Error::NotReady);
        }
        let manifest = SnapshotManifest::new(
            cut,
            root,
            self.sealed.into_iter().collect(),
            self.directory.limits,
        )?;
        self.directory
            .write_new(&self.path.join("SNAPSHOT"), &manifest.encode()?)?;
        sync_dir(&self.path)?;
        self.directory.checkpoint()?;
        let verified = VerifiedSnapshot {
            directory: self.directory,
            generation: self.generation,
            path: self.path,
            manifest,
            _lease: self.lease,
        };
        for object in verified.manifest.objects() {
            verified.read(object)?;
        }
        Ok(verified)
    }
}
impl SnapshotWriter for StagingSnapshot {
    fn put(&mut self, bytes: &[u8]) -> Result<ObjectRef> {
        let object = ObjectRef::for_bytes(bytes)?;
        if self.sealed.contains(&object) {
            return Ok(object);
        }
        for (n, chunk) in bytes.chunks(65_536).enumerate() {
            self.append(object, (n * 65_536) as u64, chunk)?;
        }
        if bytes.is_empty() {
            self.append(object, 0, &[])?;
        }
        self.seal_object(object)?;
        Ok(object)
    }
}
impl VerifiedSnapshot {
    pub fn manifest(&self) -> &SnapshotManifest {
        &self.manifest
    }
    pub fn generation(&self) -> [u8; 16] {
        self.generation
    }
}
impl SnapshotReader for VerifiedSnapshot {
    fn read(&self, object: &ObjectRef) -> Result<Vec<u8>> {
        if self.manifest.objects().binary_search(object).is_err() {
            return Err(Error::Identity);
        }
        let bytes = read_file(
            &object_path(&self.path, *object, "bin"),
            object.length() as usize,
        )?;
        object.verify(&bytes)?;
        Ok(bytes)
    }
}
impl Directory {
    fn lease(&self, generation: [u8; 16]) -> Result<Arc<()>> {
        let mut leases = self.leases.lock().map_err(|_| Error::Store)?;
        if let Some(lease) = leases.get(&generation).and_then(Weak::upgrade) {
            return Ok(lease);
        }
        let lease = Arc::new(());
        leases.insert(generation, Arc::downgrade(&lease));
        Ok(lease)
    }
    fn checkpoint(&self) -> Result<()> {
        #[cfg(test)]
        {
            let n = self.steps.fetch_add(1, Ordering::Relaxed) + 1;
            if self.fault_after.load(Ordering::Relaxed) == n {
                return Err(Error::Store);
            }
        }
        Ok(())
    }
    fn write_new(&self, path: &Path, bytes: &[u8]) -> Result<()> {
        let mut file = new_file(path)?;
        file.write_all(bytes).map_err(|_| Error::Store)?;
        self.checkpoint()?;
        file.sync_all().map_err(|_| Error::Store)?;
        self.checkpoint()
    }
    fn healthy(&self) -> Result<()> {
        if self.poisoned.load(Ordering::Relaxed) {
            Err(Error::Poisoned)
        } else {
            Ok(())
        }
    }
}
fn random_id() -> Result<[u8; 16]> {
    let mut id = [0; 16];
    getrandom::getrandom(&mut id).map_err(|_| Error::Store)?;
    if id == [0; 16] {
        return Err(Error::Store);
    }
    Ok(id)
}
fn parse_id(s: &str) -> Result<[u8; 16]> {
    if s.len() != 32
        || !s
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(Error::Corrupt);
    }
    let mut id = [0; 16];
    for (o, p) in id.iter_mut().zip(s.as_bytes().as_chunks::<2>().0) {
        let digit = |b| if b <= b'9' { b - b'0' } else { b - b'a' + 10 };
        *o = digit(p[0]) * 16 + digit(p[1]);
    }
    if id == [0; 16] {
        return Err(Error::Corrupt);
    }
    Ok(id)
}
fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8] = b"0123456789abcdef";
    bytes
        .iter()
        .flat_map(|b| {
            [
                char::from(DIGITS[(b >> 4) as usize]),
                char::from(DIGITS[(b & 15) as usize]),
            ]
        })
        .collect()
}
fn object_path(directory: &Path, object: ObjectRef, suffix: &str) -> PathBuf {
    directory.join(format!("{}.{}", hex(&object.digest()), suffix))
}
fn regular(path: &Path) -> Result<()> {
    if !fs::symlink_metadata(path)
        .map_err(|_| Error::Store)?
        .file_type()
        .is_file()
    {
        return Err(Error::Corrupt);
    }
    Ok(())
}
fn read_file(path: &Path, maximum: usize) -> Result<Vec<u8>> {
    regular(path)?;
    let file = File::open(path).map_err(|_| Error::Store)?;
    if file.metadata().map_err(|_| Error::Store)?.len() > maximum as u64 {
        return Err(Error::Bounds);
    }
    let mut bytes = Vec::new();
    file.take(maximum as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| Error::Store)?;
    if bytes.len() > maximum {
        return Err(Error::Bounds);
    }
    Ok(bytes)
}
fn new_file(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path).map_err(|_| Error::Store)
}
fn write_new(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = new_file(path)?;
    file.write_all(bytes).map_err(|_| Error::Store)?;
    file.sync_all().map_err(|_| Error::Store)
}
fn sync_dir(path: &Path) -> Result<()> {
    File::open(path)
        .map_err(|_| Error::Store)?
        .sync_all()
        .map_err(|_| Error::Store)
}
fn check_directory(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path).map_err(|_| Error::Store)?;
    if !metadata.is_dir() {
        return Err(Error::Identity);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(Error::Identity);
        }
    }
    Ok(())
}
fn private_directory(path: &Path) -> Result<()> {
    let mut b = fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        b.mode(0o700);
    }
    match b.create(path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(_) => return Err(Error::Store),
    }
    check_directory(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::Membership;
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(1);
    struct Temp(PathBuf);
    impl Temp {
        fn new() -> Self {
            let p = std::env::temp_dir().join(format!(
                "tickwire-raft-snapshot-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir(&p).unwrap();
            Self(p)
        }
    }
    impl Drop for Temp {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    fn cut() -> SnapshotCut {
        SnapshotCut::new(
            [8; 16],
            7,
            2,
            Membership {
                voters: vec![1, 2, 3],
                voters_outgoing: vec![],
                learners: vec![],
                learners_next: vec![],
                auto_leave: false,
                applied_index: 0,
                origin: [1, 2, 3],
                retired: vec![],
                operations: vec![],
            },
        )
        .unwrap()
    }
    fn config(id: u64, writer_epoch: u64) -> crate::Config {
        crate::Config {
            physical: tickwire_journal::Config::new([id as u8; 16], [8; 16]),
            node_id: id,
            voters: [1, 2, 3],
            writer_epoch,
            max_command_bytes: 262_144,
            max_retained_bytes: 8 * 1024 * 1024,
        }
    }
    fn pump(nodes: &mut [crate::Node<GenerationStore>]) {
        for _ in 0..128 {
            let mut messages = Vec::new();
            for n in nodes.iter_mut() {
                messages.extend(n.drive().unwrap().messages);
            }
            if messages.is_empty() {
                return;
            }
            for m in messages {
                nodes[(m.to - 1) as usize].step(m.from, m).unwrap();
            }
        }
        panic!("raft did not settle");
    }
    fn copy_tree(from: &Path, to: &Path) {
        private_directory(to).unwrap();
        for entry in fs::read_dir(from).unwrap() {
            let entry = entry.unwrap();
            let target = to.join(entry.file_name());
            if entry.file_type().unwrap().is_dir() {
                copy_tree(&entry.path(), &target);
            } else {
                fs::copy(entry.path(), target).unwrap();
            }
        }
    }
    #[test]
    fn every_real_wal_and_current_write_failure_withholds_installation_and_reopens_one_cut() {
        let temp = Temp::new();
        let mut nodes = (1..=3)
            .map(|id| {
                crate::Node::create_in(
                    SnapshotDirectory::open(
                        temp.0.join(id.to_string()),
                        [8; 16],
                        id,
                        SnapshotLimits::default(),
                    )
                    .unwrap(),
                    config(id, 1),
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        nodes[0].campaign().unwrap();
        pump(&mut nodes);
        nodes[0].propose(b"complete old state".to_vec()).unwrap();
        pump(&mut nodes);
        let applied = nodes[0].applied_index();
        drop(nodes);
        let mut failures = 0;
        for failure in 1..=100 {
            let path = temp.0.join(format!("failure-{failure}"));
            copy_tree(&temp.0.join("1"), &path);
            let directory =
                SnapshotDirectory::open(&path, [8; 16], 1, SnapshotLimits::default()).unwrap();
            let mut node = crate::Node::recover_in(directory.clone(), config(1, 2)).unwrap();
            let mut stage = node.stage_snapshot().unwrap();
            let root = stage.put(b"complete old state").unwrap();
            let snapshot = stage
                .finish(
                    node.snapshot_cut(applied).unwrap(),
                    ApplicationRoot::new(1, root).unwrap(),
                )
                .unwrap();
            directory.inner.steps.store(0, Ordering::Relaxed);
            directory
                .inner
                .fault_after
                .store(failure, Ordering::Relaxed);
            let result = node.compact(&snapshot);
            let success = result.is_ok();
            if success {
                assert!(node.matches_installed(&result.unwrap()));
            } else {
                assert!(node.status().poisoned);
                assert!(node.drive().is_err());
                failures += 1;
            }
            drop(snapshot);
            drop(node);
            drop(directory);
            let recovered = crate::Node::recover_in(
                SnapshotDirectory::open(&path, [8; 16], 1, SnapshotLimits::default()).unwrap(),
                config(1, 3),
            )
            .unwrap();
            assert_eq!(recovered.applied_index(), applied, "cut {failure}");
            if recovered.snapshot_base_index() == 0 {
                let h = recovered.history();
                assert_eq!(h.len(), 1);
                assert_eq!(h[0].command(), b"complete old state");
            } else {
                assert_eq!(recovered.snapshot_base_index(), applied);
                assert!(recovered.history().is_empty());
                assert_eq!(
                    recovered
                        .snapshot_reader()
                        .unwrap()
                        .unwrap()
                        .read(&root)
                        .unwrap(),
                    b"complete old state"
                );
            }
            if success {
                break;
            }
        }
        assert!((10..100).contains(&failures));
        println!("actual failing WAL/CURRENT boundaries: {failures}");
    }
    #[test]
    fn current_missing_object_or_corrupt_pointer_never_falls_back_to_an_older_directory() {
        let temp = Temp::new();
        let directory =
            SnapshotDirectory::open(temp.0.join("node"), [8; 16], 1, SnapshotLimits::default())
                .unwrap();
        let mut stage = directory.stage().unwrap();
        let root = stage.put(b"state").unwrap();
        let snapshot = stage
            .finish(cut(), ApplicationRoot::new(1, root).unwrap())
            .unwrap();
        directory.publish(&snapshot).unwrap();
        let current = fs::read(directory.inner.root.join("CURRENT")).unwrap();
        let object = object_path(&snapshot.path, root, "bin");
        fs::remove_file(&object).unwrap();
        assert!(directory.current().is_err());
        write_new(&object, b"state").unwrap();
        let mut bad = current.clone();
        bad[8] ^= 1;
        fs::write(directory.inner.root.join("CURRENT"), bad).unwrap();
        assert!(directory.current().is_err());
        fs::write(directory.inner.root.join("CURRENT"), current).unwrap();
        assert!(directory.current().unwrap().is_some());
    }
    #[test]
    fn cleanup_keeps_current_and_every_pinned_snapshot_reader() {
        let temp = Temp::new();
        let directory =
            SnapshotDirectory::open(temp.0.join("node"), [8; 16], 1, SnapshotLimits::default())
                .unwrap();
        let mut a = directory.stage().unwrap();
        let root_a = a.put(b"old state").unwrap();
        let a = a
            .finish(cut(), ApplicationRoot::new(1, root_a).unwrap())
            .unwrap();
        directory.publish(&a).unwrap();
        let mut b = directory.stage().unwrap();
        let root_b = b.put(b"new state").unwrap();
        let b = b
            .finish(
                SnapshotCut::new([8; 16], 8, 2, cut().membership().clone()).unwrap(),
                ApplicationRoot::new(1, root_b).unwrap(),
            )
            .unwrap();
        directory.publish(&b).unwrap();
        assert_eq!(directory.collect_old(b.generation).unwrap(), 0);
        assert_eq!(a.read(&root_a).unwrap(), b"old state");
        let old_generation = a.generation;
        drop(a);
        assert_eq!(directory.collect_old(b.generation).unwrap(), 0);
        assert!(
            directory
                .inner
                .root
                .join(format!("generation-{}", hex(&old_generation)))
                .exists()
        );
        let mut c = directory.stage().unwrap();
        let root_c = c.put(b"third state").unwrap();
        let c = c
            .finish(
                SnapshotCut::new([8; 16], 9, 2, cut().membership().clone()).unwrap(),
                ApplicationRoot::new(1, root_c).unwrap(),
            )
            .unwrap();
        directory.publish(&c).unwrap();
        assert_eq!(directory.collect_old(c.generation).unwrap(), 1);
        assert_eq!(b.read(&root_b).unwrap(), b"new state");
        assert_eq!(
            directory.current().unwrap().unwrap().read(&root_c).unwrap(),
            b"third state"
        );
    }
    #[test]
    fn disconnected_staging_cannot_permanently_exhaust_generation_capacity() {
        let temp = Temp::new();
        let directory =
            SnapshotDirectory::open(temp.0.join("node"), [8; 16], 1, SnapshotLimits::default())
                .unwrap();
        let mut stage = directory.stage().unwrap();
        let root = stage.put(b"current").unwrap();
        let current = stage
            .finish(cut(), ApplicationRoot::new(1, root).unwrap())
            .unwrap();
        directory.publish(&current).unwrap();
        for _ in 0..40 {
            let mut interrupted = directory.stage().unwrap();
            let object = ObjectRef::for_bytes(&vec![5; 65537]).unwrap();
            interrupted.append(object, 0, &[5; 16]).unwrap();
            drop(interrupted);
        }
        assert_eq!(
            directory.current().unwrap().unwrap().generation,
            current.generation
        );
        assert!(
            fs::read_dir(&directory.inner.root)
                .unwrap()
                .filter(|e| e
                    .as_ref()
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .starts_with("generation-"))
                .count()
                <= 16
        );
    }
    #[test]
    fn legacy_current_is_read_without_guessing_or_deleting_the_unknown_previous() {
        let temp = Temp::new();
        let path = temp.0.join("node");
        let directory =
            SnapshotDirectory::open(&path, [8; 16], 1, SnapshotLimits::default()).unwrap();
        let mut first = directory.stage().unwrap();
        let root = first.put(b"old").unwrap();
        let first = first
            .finish(cut(), ApplicationRoot::new(1, root).unwrap())
            .unwrap();
        directory.publish(&first).unwrap();
        let first_id = first.generation;
        drop(first);
        let mut next = directory.stage().unwrap();
        let root = next.put(b"new").unwrap();
        let next = next
            .finish(cut(), ApplicationRoot::new(1, root).unwrap())
            .unwrap();
        directory.publish(&next).unwrap();
        let current = next.generation;
        drop(next);
        let mut legacy = fs::read(path.join("CURRENT")).unwrap()[..80].to_vec();
        legacy[..8].copy_from_slice(b"TWGC0001");
        legacy.extend_from_slice(&Sha256::digest(&legacy));
        fs::write(path.join("CURRENT"), legacy).unwrap();
        assert_eq!(directory.current().unwrap().unwrap().generation, current);
        assert_eq!(directory.collect_old(current).unwrap(), 0);
        assert!(path.join(format!("generation-{}", hex(&first_id))).exists());
        drop(directory);
        let reopened =
            SnapshotDirectory::open(&path, [8; 16], 1, SnapshotLimits::default()).unwrap();
        assert_eq!(reopened.current().unwrap().unwrap().generation, current);
    }
    #[test]
    fn failed_capacity_cleanup_cannot_allocate_or_publish_a_replacement() {
        let temp = Temp::new();
        let path = temp.0.join("node");
        let directory =
            SnapshotDirectory::open(&path, [8; 16], 1, SnapshotLimits::default()).unwrap();
        let mut stage = directory.stage().unwrap();
        let root = stage.put(b"current").unwrap();
        let current = stage
            .finish(cut(), ApplicationRoot::new(1, root).unwrap())
            .unwrap();
        directory.publish(&current).unwrap();
        let id = current.generation;
        drop(current);
        for _ in 0..15 {
            drop(directory.stage().unwrap());
        }
        let before = fs::read(path.join("CURRENT")).unwrap();
        directory.inner.steps.store(0, Ordering::Relaxed);
        directory.inner.fault_after.store(1, Ordering::Relaxed);
        assert!(matches!(directory.stage(), Err(Error::Store)));
        assert_eq!(fs::read(path.join("CURRENT")).unwrap(), before);
        assert!(matches!(directory.stage(), Err(Error::Poisoned)));
        drop(directory);
        let reopened =
            SnapshotDirectory::open(&path, [8; 16], 1, SnapshotLimits::default()).unwrap();
        assert_eq!(reopened.current().unwrap().unwrap().generation, id);
    }
    #[test]
    fn concurrent_clone_gc_cannot_delete_directory_before_its_lease_is_registered() {
        let temp = Temp::new();
        let directory =
            SnapshotDirectory::open(temp.0.join("node"), [8; 16], 1, SnapshotLimits::default())
                .unwrap();
        let mut stage = directory.stage().unwrap();
        let object = stage.put(b"current").unwrap();
        let current = stage
            .finish(cut(), ApplicationRoot::new(1, object).unwrap())
            .unwrap();
        directory.publish(&current).unwrap();
        for _ in 0..14 {
            drop(directory.stage().unwrap());
        }
        let created = Arc::new(std::sync::Barrier::new(2));
        let resume = Arc::new(std::sync::Barrier::new(2));
        *directory.inner.stage_pause.lock().unwrap() = Some((created.clone(), resume.clone()));
        let first_directory = directory.clone();
        let first = std::thread::spawn(move || first_directory.stage().unwrap());
        created.wait();
        let second_directory = directory.clone();
        let (sent, received) = std::sync::mpsc::channel();
        let second = std::thread::spawn(move || {
            let staged = second_directory.stage();
            sent.send(staged).unwrap();
        });
        let early = received
            .recv_timeout(std::time::Duration::from_secs(1))
            .ok();
        resume.wait();
        let mut first = first.join().unwrap();
        let mut second_stage = early
            .unwrap_or_else(|| {
                received
                    .recv_timeout(std::time::Duration::from_secs(5))
                    .unwrap()
            })
            .unwrap();
        second.join().unwrap();
        assert!(
            first.path.exists(),
            "GC deleted the newly-created unpinned directory"
        );
        first.put(b"first").unwrap();
        second_stage.put(b"second").unwrap();
        assert!(
            fs::read_dir(&directory.inner.root)
                .unwrap()
                .filter(|e| e
                    .as_ref()
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .starts_with("generation-"))
                .count()
                <= 16
        );
    }
    #[test]
    fn directory_clones_cannot_open_two_active_nodes_even_after_compaction() {
        let temp = Temp::new();
        let mut nodes = (1..=3)
            .map(|id| {
                crate::Node::create_in(
                    SnapshotDirectory::open(
                        temp.0.join(id.to_string()),
                        [8; 16],
                        id,
                        SnapshotLimits::default(),
                    )
                    .unwrap(),
                    config(id, 1),
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        let directory = nodes[0].generation_directory.as_ref().unwrap().clone();
        assert!(matches!(
            crate::Node::recover_in(directory.clone(), config(1, 2)),
            Err(Error::Identity)
        ));
        nodes[0].campaign().unwrap();
        pump(&mut nodes);
        nodes[0].propose(b"state".to_vec()).unwrap();
        pump(&mut nodes);
        let mut staged = nodes[0].stage_snapshot().unwrap();
        let object = staged.put(b"state").unwrap();
        let snapshot = staged
            .finish(
                nodes[0].snapshot_cut(nodes[0].applied_index()).unwrap(),
                ApplicationRoot::new(1, object).unwrap(),
            )
            .unwrap();
        nodes[0].compact(&snapshot).unwrap();
        let current = fs::read(directory.inner.root.join("CURRENT")).unwrap();
        assert!(matches!(
            crate::Node::recover_in(directory.clone(), config(1, 2)),
            Err(Error::Identity)
        ));
        assert_eq!(
            fs::read(directory.inner.root.join("CURRENT")).unwrap(),
            current
        );
        drop(nodes);
        crate::Node::recover_in(directory, config(1, 2)).unwrap();
    }
    #[test]
    fn reopening_same_directory_clone_never_revalidates_a_previous_node_receipt() {
        let temp = Temp::new();
        let mut nodes = (1..=3)
            .map(|id| {
                crate::Node::create_in(
                    SnapshotDirectory::open(
                        temp.0.join(id.to_string()),
                        [8; 16],
                        id,
                        SnapshotLimits::default(),
                    )
                    .unwrap(),
                    config(id, 1),
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        nodes[0].campaign().unwrap();
        pump(&mut nodes);
        nodes[0].propose(b"state".to_vec()).unwrap();
        pump(&mut nodes);
        let directory = nodes[0].generation_directory.as_ref().unwrap().clone();
        let mut staged = nodes[0].stage_snapshot().unwrap();
        let object = staged.put(b"state").unwrap();
        let snapshot = staged
            .finish(
                nodes[0].snapshot_cut(nodes[0].applied_index()).unwrap(),
                ApplicationRoot::new(1, object).unwrap(),
            )
            .unwrap();
        let old = nodes[0].compact(&snapshot).unwrap();
        drop(nodes);
        let reopened = crate::Node::recover_in(directory, config(1, 2)).unwrap();
        assert!(
            !reopened.matches_installed(&old),
            "a new Node must not accept the previous Node's receipt"
        );
    }
    #[test]
    fn every_object_write_sync_rename_and_manifest_failure_leaves_current_unchanged() {
        let temp = Temp::new();
        let directory =
            SnapshotDirectory::open(temp.0.join("node"), [8; 16], 1, SnapshotLimits::default())
                .unwrap();
        let mut first = directory.stage().unwrap();
        let root = first.put(b"initial").unwrap();
        let first = first
            .finish(cut(), ApplicationRoot::new(1, root).unwrap())
            .unwrap();
        directory.publish(&first).unwrap();
        let mut failures = 0;
        for failure in 1..=12 {
            let mut staged = directory.stage().unwrap();
            directory.inner.steps.store(0, Ordering::Relaxed);
            directory
                .inner
                .fault_after
                .store(failure, Ordering::Relaxed);
            let result = (|| {
                let root = staged.put(b"replacement")?;
                staged.finish(cut(), ApplicationRoot::new(1, root)?)
            })();
            directory.inner.fault_after.store(0, Ordering::Relaxed);
            assert_eq!(
                directory.current().unwrap().unwrap().generation,
                first.generation
            );
            if result.is_ok() {
                break;
            }
            failures += 1;
            directory.collect_old(first.generation).unwrap();
        }
        assert_eq!(failures, 7);
    }
    #[test]
    fn real_directory_stages_verified_objects_before_current_publication() {
        let temp = Temp::new();
        let directory =
            SnapshotDirectory::open(temp.0.join("node"), [8; 16], 1, SnapshotLimits::default())
                .unwrap();
        assert!(
            SnapshotDirectory::open(temp.0.join("node"), [8; 16], 1, SnapshotLimits::default())
                .is_err()
        );
        let mut staged = directory.stage().unwrap();
        let content = vec![3; 65_537];
        let object = staged.put(&content).unwrap();
        let verified = staged
            .finish(cut(), ApplicationRoot::new(1, object).unwrap())
            .unwrap();
        assert_eq!(verified.read(&object).unwrap(), content);
        assert!(directory.current().unwrap().is_none());
        directory.publish(&verified).unwrap();
        assert_eq!(
            directory.current().unwrap().unwrap().generation(),
            verified.generation()
        );
        drop(verified);
        drop(directory);
        let reopened =
            SnapshotDirectory::open(temp.0.join("node"), [8; 16], 1, SnapshotLimits::default())
                .unwrap();
        assert_eq!(
            reopened.current().unwrap().unwrap().read(&object).unwrap(),
            content
        );
    }
    #[test]
    fn missing_mismatched_and_out_of_order_chunks_do_not_publish() {
        let temp = Temp::new();
        let directory =
            SnapshotDirectory::open(temp.0.join("node"), [8; 16], 1, SnapshotLimits::default())
                .unwrap();
        let object = ObjectRef::for_bytes(b"hello").unwrap();
        let mut staged = directory.stage().unwrap();
        assert!(staged.append(object, 1, b"h").is_err());
        staged.append(object, 0, b"he").unwrap();
        assert!(staged.seal_object(object).is_err());
        assert!(
            staged
                .finish(cut(), ApplicationRoot::new(1, object).unwrap())
                .is_err()
        );
        let mut wrong = directory.stage().unwrap();
        wrong.append(object, 0, b"jello").unwrap();
        assert!(wrong.seal_object(object).is_err());
        assert!(directory.current().unwrap().is_none());
    }
    #[test]
    fn actual_raft_compacts_and_reopens_snapshot_with_only_committed_tail_history() {
        let temp = Temp::new();
        let config = |id: u64| crate::Config {
            physical: tickwire_journal::Config::new([id as u8; 16], [8; 16]),
            node_id: id,
            voters: [1, 2, 3],
            writer_epoch: 1,
            max_command_bytes: 262_144,
            max_retained_bytes: 8 * 1024 * 1024,
        };
        let open = |id: u64| {
            SnapshotDirectory::open(
                temp.0.join(id.to_string()),
                [8; 16],
                id,
                SnapshotLimits::default(),
            )
            .unwrap()
        };
        let mut nodes: Vec<_> = (1..=3)
            .map(|id| crate::Node::create_in(open(id), config(id)).unwrap())
            .collect();
        fn pump(nodes: &mut [crate::Node<GenerationStore>]) {
            for _ in 0..128 {
                let mut messages = Vec::new();
                for node in nodes.iter_mut() {
                    messages.extend(node.drive().unwrap().messages);
                }
                if messages.is_empty() {
                    return;
                }
                for m in messages {
                    nodes[(m.to - 1) as usize].step(m.from, m).unwrap();
                }
            }
            panic!("raft did not settle")
        }
        nodes[0].campaign().unwrap();
        pump(&mut nodes);
        nodes[0].propose(b"before snapshot".to_vec()).unwrap();
        pump(&mut nodes);
        let cut = nodes[0].snapshot_cut(nodes[0].applied_index()).unwrap();
        let mut staged = nodes[0].stage_snapshot().unwrap();
        let root = staged.put(b"complete application state").unwrap();
        let snapshot = staged
            .finish(cut.clone(), ApplicationRoot::new(1, root).unwrap())
            .unwrap();
        let receipt = nodes[0].compact(&snapshot).unwrap();
        assert!(nodes[0].matches_installed(&receipt));
        assert!(!nodes[1].matches_installed(&receipt));
        assert_eq!(nodes[0].snapshot_base_index(), cut.index());
        assert!(nodes[0].history().is_empty());
        assert!(nodes[0].snapshot_cut(cut.index() - 1).is_err());
        nodes[0].propose(b"after snapshot".to_vec()).unwrap();
        pump(&mut nodes);
        let tail_index = nodes[0].applied_index();
        drop(snapshot);
        drop(nodes);
        let recovered = crate::Node::recover_in(
            open(1),
            crate::Config {
                writer_epoch: 2,
                ..config(1)
            },
        )
        .unwrap();
        assert_eq!(recovered.snapshot_base_index(), cut.index());
        assert_eq!(recovered.applied_index(), tail_index);
        let history = recovered.history();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].command(), b"after snapshot");
        assert!(!recovered.matches_installed(&receipt));
        assert!(recovered.matches_installed(recovered.installed_generation().unwrap()));
        assert_eq!(
            recovered
                .snapshot_reader()
                .unwrap()
                .unwrap()
                .read(&root)
                .unwrap(),
            b"complete application state"
        );
    }
}
