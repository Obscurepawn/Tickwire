//! Versioned application snapshots installed through the owning Raft generation.
use crate::{
    Config, Error, ObjectLimits, ObjectSpec, Purpose, Replica, Result, bootstrap, objects,
    publication,
};
use tickwire_control_raft::generation::{GenerationStore, VerifiedSnapshot};
use tickwire_control_raft::snapshot::{
    ApplicationRoot, InstalledGeneration, ObjectRef, SnapshotApplication, SnapshotCut,
    SnapshotReader, SnapshotRestore, SnapshotWriter,
};
use tickwire_journal::store::Store;
const MAGIC: &[u8; 8] = b"TWCS0001";
const PUBLICATION_MAGIC: &[u8; 8] = b"TWCS0002";
const ROOT_BYTES: usize = 1024 * 1024;
const BLOCK_BYTES: usize = 16 * 1024 * 1024;

pub struct PreparedSnapshot {
    cut: SnapshotCut,
    root: ApplicationRoot,
    state: objects::State,
    bootstrap: bootstrap::State,
    publication: publication::State,
}
impl PreparedSnapshot {
    pub fn cut(&self) -> &SnapshotCut {
        &self.cut
    }
    pub fn root(&self) -> &ApplicationRoot {
        &self.root
    }
    pub fn head(&self) -> Option<bootstrap::CommittedHead> {
        self.bootstrap.head()
    }
    pub fn prepared(
        &self,
        query: publication::PreparedQuery<'_>,
    ) -> Result<Option<publication::StoredPrepared>> {
        self.publication.get(query)
    }
    pub fn object(&self, key: crate::ObjectKey) -> Option<crate::Object> {
        self.state.object(key)
    }
}
impl<S: Store> Replica<S> {
    pub fn capture_snapshot(
        &self,
        out: &mut dyn SnapshotWriter,
    ) -> Result<(SnapshotCut, ApplicationRoot)> {
        self.healthy()?;
        let cut = self.node.snapshot_cut(self.scanned_through)?;
        let (initialized, head) = self.bootstrap.snapshot_state();
        if initialized != self.state.initialized() {
            return Err(Error::Protocol);
        }
        let entries: Vec<_> = self.state.snapshot_entries().collect();
        let mut blocks = Vec::new();
        let mut pending = Vec::new();
        for (_, bytes, _) in &entries {
            let mut rest = *bytes;
            while !rest.is_empty() {
                let n = rest.len().min(BLOCK_BYTES - pending.len());
                pending.extend_from_slice(&rest[..n]);
                rest = &rest[n..];
                if pending.len() == BLOCK_BYTES {
                    blocks.push(put(out, &pending)?);
                    pending.clear();
                }
            }
        }
        if !pending.is_empty() {
            blocks.push(put(out, &pending)?);
        }
        let publication = if self.publication.is_empty() {
            None
        } else {
            Some(put(out, &self.publication.snapshot(self.config.cluster)?)?)
        };
        let schema = if publication.is_some() { 2 } else { 1 };
        let mut bytes = if schema == 2 {
            PUBLICATION_MAGIC
        } else {
            MAGIC
        }
        .to_vec();
        bytes.extend_from_slice(&self.config.cluster);
        bytes.extend_from_slice(&cut.group());
        bytes.extend_from_slice(&cut.index().to_le_bytes());
        bytes.extend_from_slice(&cut.term().to_le_bytes());
        bytes.extend_from_slice(&cut.membership().digest());
        bytes.extend_from_slice(&self.config.objects.max_objects.to_le_bytes());
        bytes.extend_from_slice(&self.config.objects.max_inflight.to_le_bytes());
        bytes.extend_from_slice(&self.config.objects.max_payload_bytes.to_le_bytes());
        bytes.push(u8::from(initialized));
        if let Some(hash) = self.bootstrap.trust_hash() {
            bytes.push(1);
            bytes.extend_from_slice(&hash);
        } else {
            bytes.push(0);
        }
        bytes.extend_from_slice(&(entries.len() as u32).to_le_bytes());
        for (spec, content, sealed) in entries {
            bytes.extend_from_slice(&spec.key);
            bytes.extend_from_slice(&spec.owner);
            bytes.push(spec.purpose as u8);
            bytes.extend_from_slice(&spec.length.to_le_bytes());
            bytes.extend_from_slice(&spec.hash);
            bytes.extend_from_slice(&(content.len() as u32).to_le_bytes());
            bytes.push(u8::from(sealed));
        }
        bytes.extend_from_slice(&(blocks.len() as u32).to_le_bytes());
        for block in blocks {
            bytes.extend_from_slice(&block.digest());
            bytes.extend_from_slice(&block.length().to_le_bytes());
        }
        if let Some(bootstrap::SnapshotHead {
            request,
            index,
            term,
            voters,
        }) = head
        {
            bytes.push(1);
            bytes.extend_from_slice(&index.to_le_bytes());
            bytes.extend_from_slice(&term.to_le_bytes());
            for voter in voters {
                bytes.extend_from_slice(&voter.to_le_bytes());
            }
            bytes.extend_from_slice(&request.candidate);
            bytes.extend_from_slice(&request.candidate_hash);
            bytes.extend_from_slice(&request.nonce);
            bytes.extend_from_slice(&request.signature);
        } else {
            bytes.push(0);
        }
        if let Some(publication) = publication {
            bytes.extend_from_slice(&publication.digest());
            bytes.extend_from_slice(&publication.length().to_le_bytes());
        }
        if bytes.len() > ROOT_BYTES {
            return Err(Error::Bounds);
        }
        let root = ApplicationRoot::new(schema, put(out, &bytes)?)?;
        Ok((cut, root))
    }
    pub fn prepare_snapshot(
        &self,
        cut: &SnapshotCut,
        root: &ApplicationRoot,
        objects: &dyn SnapshotReader,
    ) -> Result<PreparedSnapshot> {
        self.healthy()?;
        let status = self.node.status();
        if status.poisoned {
            return Err(Error::Poisoned);
        }
        SnapshotContext {
            config: self.config,
            group: status.group,
            bootstrap: &self.bootstrap,
        }
        .prepare(cut, root, objects)
    }
}
struct SnapshotContext<'a> {
    config: Config,
    group: crate::Identity,
    bootstrap: &'a bootstrap::State,
}
impl SnapshotContext<'_> {
    fn prepare(
        &self,
        cut: &SnapshotCut,
        root: &ApplicationRoot,
        objects: &dyn SnapshotReader,
    ) -> Result<PreparedSnapshot> {
        if !matches!(root.schema(), 1 | 2) || root.root().length() > ROOT_BYTES as u64 {
            return Err(Error::Protocol);
        }
        if cut.group() != self.group {
            return Err(Error::Identity);
        }
        let bytes = read(objects, &root.root())?;
        let mut r = Reader(&bytes);
        if &r.array::<8>()?
            != if root.schema() == 2 {
                PUBLICATION_MAGIC
            } else {
                MAGIC
            }
            || r.array::<16>()? != self.config.cluster
            || r.array::<16>()? != cut.group()
            || r.u64()? != cut.index()
            || r.u64()? != cut.term()
            || r.array::<32>()? != cut.membership().digest()
        {
            return Err(Error::Identity);
        }
        let config = Config {
            cluster: self.config.cluster,
            objects: ObjectLimits {
                max_objects: r.u32()?,
                max_inflight: r.u32()?,
                max_payload_bytes: r.u64()?,
            },
        };
        if config != self.config {
            return Err(Error::Identity);
        }
        let initialized = r.boolean()?;
        let trust_hash = if r.boolean()? {
            Some(r.array::<32>()?)
        } else {
            None
        };
        if trust_hash != self.bootstrap.trust_hash() {
            return Err(Error::Identity);
        }
        let count = r.u32()? as usize;
        if count > config.objects.max_objects as usize || (!initialized && count != 0) {
            return Err(Error::Bounds);
        }
        let mut entries = Vec::with_capacity(count);
        let mut reserved = 0u64;
        let mut total = 0u64;
        let mut inflight = 0u32;
        let mut previous = None;
        for _ in 0..count {
            let spec = ObjectSpec {
                key: r.array()?,
                owner: r.array()?,
                purpose: Purpose::from_byte(r.byte()?)?,
                length: r.u32()?,
                hash: r.array()?,
            };
            spec.validate()?;
            let received = r.u32()?;
            let sealed = r.boolean()?;
            if previous.is_some_and(|key| key >= spec.key)
                || received > spec.length
                || (received < spec.length
                    && !(received as usize).is_multiple_of(crate::CHUNK_BYTES))
                || (sealed && received != spec.length)
            {
                return Err(Error::Protocol);
            }
            previous = Some(spec.key);
            reserved = reserved
                .checked_add(u64::from(spec.length))
                .ok_or(Error::Bounds)?;
            total = total
                .checked_add(u64::from(received))
                .ok_or(Error::Bounds)?;
            inflight = inflight
                .checked_add(u32::from(!sealed))
                .ok_or(Error::Bounds)?;
            if reserved > config.objects.max_payload_bytes || inflight > config.objects.max_inflight
            {
                return Err(Error::Capacity);
            }
            entries.push((spec, received, sealed));
        }
        let count = r.u32()? as u64;
        if count != total.div_ceil(BLOCK_BYTES as u64) {
            return Err(Error::Protocol);
        }
        let mut blocks = Vec::with_capacity(count as usize);
        for index in 0..count {
            let block = ObjectRef::new(r.array()?, r.u64()?)?;
            if block.length() != (total - index * BLOCK_BYTES as u64).min(BLOCK_BYTES as u64) {
                return Err(Error::Protocol);
            }
            blocks.push(block);
        }
        let head = if r.boolean()? {
            if !initialized {
                return Err(Error::Protocol);
            }
            let index = r.u64()?;
            let term = r.u64()?;
            let voters = [r.u64()?, r.u64()?, r.u64()?];
            if index == 0
                || index > cut.index()
                || term == 0
                || term > cut.term()
                || voters[0] == 0
                || voters.iter().any(|v| *v >= 1 << 63)
                || !voters.windows(2).all(|pair| pair[0] < pair[1])
            {
                return Err(Error::Protocol);
            }
            Some((
                bootstrap::Bootstrap {
                    candidate: r.array()?,
                    candidate_hash: r.array()?,
                    nonce: r.array()?,
                    signature: r.array()?,
                },
                index,
                term,
                voters,
            ))
        } else {
            None
        };
        let publication_ref = if root.schema() == 2 {
            let reference = ObjectRef::new(r.array()?, r.u64()?)?;
            if reference.length() > publication::SNAPSHOT_BYTES as u64 {
                return Err(Error::Bounds);
            }
            Some(reference)
        } else {
            None
        };
        if !r.0.is_empty() {
            return Err(Error::Protocol);
        }
        let mut contents = Contents {
            reader: objects,
            blocks: &blocks,
            next: 0,
            current: Vec::new(),
            at: 0,
        };
        let entries = entries
            .into_iter()
            .map(|(spec, received, sealed)| Ok((spec, contents.take(received as usize)?, sealed)))
            .collect::<Result<Vec<_>>>()?;
        if contents.next != blocks.len() || contents.at != contents.current.len() {
            return Err(Error::Protocol);
        }
        let state = objects::State::restore(config, initialized, entries)?;
        let mut bootstrap = bootstrap::State::new(self.bootstrap.trust());
        if initialized {
            bootstrap.initialize(trust_hash)?;
        }
        if let Some((request, index, term, voters)) = head {
            bootstrap.apply(request, &state, voters, index, term)?;
        }
        let publication = if let Some(reference) = publication_ref {
            let head = bootstrap.head().ok_or(Error::Protocol)?;
            publication::State::restore(
                &read(objects, &reference)?,
                self.config.cluster,
                &state,
                &head,
                cut.index(),
                cut.term(),
            )?
        } else {
            publication::State::default()
        };
        Ok(PreparedSnapshot {
            cut: cut.clone(),
            root: root.clone(),
            state,
            bootstrap,
            publication,
        })
    }
}
/// Only Replica's drive can create this split borrow. The Node calls install
/// after matching its own durable receipt; the public application API below
/// additionally checks that match itself.
pub(crate) struct ReplicaRestore<'a> {
    config: Config,
    group: crate::Identity,
    state: &'a mut objects::State,
    bootstrap: &'a mut bootstrap::State,
    publication: &'a mut publication::State,
    scanned: &'a mut u64,
}
impl<'a> ReplicaRestore<'a> {
    pub(crate) fn new(
        config: Config,
        group: crate::Identity,
        state: &'a mut objects::State,
        bootstrap: &'a mut bootstrap::State,
        publication: &'a mut publication::State,
        scanned: &'a mut u64,
    ) -> Self {
        Self {
            config,
            group,
            state,
            bootstrap,
            publication,
            scanned,
        }
    }
}
impl SnapshotRestore for ReplicaRestore<'_> {
    type Prepared = PreparedSnapshot;
    type Error = Error;

    fn prepare(&mut self, snapshot: &VerifiedSnapshot) -> Result<PreparedSnapshot> {
        let manifest = snapshot.manifest();
        if manifest.cut().index() < *self.scanned {
            return Err(Error::NotReady);
        }
        SnapshotContext {
            config: self.config,
            group: self.group,
            bootstrap: self.bootstrap,
        }
        .prepare(manifest.cut(), manifest.application(), snapshot)
    }
    fn install(
        &mut self,
        prepared: PreparedSnapshot,
        installed: &InstalledGeneration,
    ) -> Result<()> {
        install_matched(
            self.state,
            self.bootstrap,
            self.publication,
            self.scanned,
            prepared,
            installed,
        )
    }
}
fn install_matched(
    state: &mut objects::State,
    bootstrap: &mut bootstrap::State,
    publication: &mut publication::State,
    scanned: &mut u64,
    prepared: PreparedSnapshot,
    installed: &InstalledGeneration,
) -> Result<()> {
    if &prepared.cut != installed.cut() || &prepared.root != installed.application() {
        return Err(Error::Identity);
    }
    if *scanned > prepared.cut.index() {
        return Err(Error::NotReady);
    }
    *state = prepared.state;
    *bootstrap = prepared.bootstrap;
    *publication = prepared.publication;
    *scanned = prepared.cut.index();
    Ok(())
}
impl<S: Store> SnapshotApplication for Replica<S> {
    type Prepared = PreparedSnapshot;
    type Error = Error;

    fn capture(&self, cut: &SnapshotCut, out: &mut dyn SnapshotWriter) -> Result<ApplicationRoot> {
        if cut != &self.node.snapshot_cut(self.scanned_through)? {
            return Err(Error::Identity);
        }
        self.capture_snapshot(out).map(|(_, root)| root)
    }
    fn prepare_restore(
        &self,
        cut: &SnapshotCut,
        root: &ApplicationRoot,
        objects: &dyn SnapshotReader,
    ) -> Result<PreparedSnapshot> {
        self.prepare_snapshot(cut, root, objects)
    }
    fn install_prepared(
        &mut self,
        prepared: PreparedSnapshot,
        installed: &InstalledGeneration,
    ) -> Result<()> {
        self.healthy()?;
        if !self.node.matches_installed(installed) {
            return Err(Error::Identity);
        }
        if self.node.applied_index() < prepared.cut.index() {
            return Err(Error::NotReady);
        }
        install_matched(
            &mut self.state,
            &mut self.bootstrap,
            &mut self.publication,
            &mut self.scanned_through,
            prepared,
            installed,
        )
    }
    fn scanned_through(&self) -> u64 {
        self.scanned_through
    }
}
impl Replica<GenerationStore> {
    pub fn compact_snapshot(&mut self) -> Result<InstalledGeneration> {
        self.healthy()?;
        if self.scanned_through <= self.node.snapshot_base_index() {
            return Err(Error::NotReady);
        }
        let mut stage = self.node.stage_snapshot()?;
        let (cut, root) = self.capture_snapshot(&mut stage)?;
        let snapshot = stage.finish(cut.clone(), root.clone())?;
        let prepared = self.prepare_snapshot(&cut, &root, &snapshot)?;
        let installed = self.node.compact(&snapshot)?;
        if let Err(error) = self.install_prepared(prepared, &installed) {
            self.poisoned = true;
            return Err(error);
        }
        Ok(installed)
    }
    pub fn recover_generation(
        node: tickwire_control_raft::Node<GenerationStore>,
        config: Config,
        trust: Option<bootstrap::BootstrapTrust>,
    ) -> Result<Self> {
        config.validate()?;
        if trust
            .as_ref()
            .is_some_and(|t| t.cluster() != config.cluster)
        {
            return Err(Error::Identity);
        }
        if node.snapshot_base_index() == 0 {
            return Self::open(node, config, trust);
        }
        let snapshot = node.snapshot_reader()?.ok_or(Error::Protocol)?;
        let installed = node.installed_generation().ok_or(Error::Protocol)?.clone();
        let mut out = Self {
            node,
            config,
            state: objects::State::new(config),
            bootstrap: bootstrap::State::new(trust),
            publication: publication::State::default(),
            deployment: None,
            planning_owner: std::sync::Arc::new(()),
            scanned_through: 0,
            poisoned: false,
        };
        let prepared = out.prepare_snapshot(
            snapshot.manifest().cut(),
            snapshot.manifest().application(),
            &snapshot,
        )?;
        out.install_prepared(prepared, &installed)?;
        for applied in out.node.history() {
            let _ = out.apply(&applied)?;
        }
        out.scanned_through = out.node.applied_index();
        Ok(out)
    }
}
fn put(writer: &mut dyn SnapshotWriter, bytes: &[u8]) -> Result<ObjectRef> {
    let object = writer.put(bytes)?;
    object.verify(bytes)?;
    Ok(object)
}
fn read(reader: &dyn SnapshotReader, object: &ObjectRef) -> Result<Vec<u8>> {
    let bytes = reader.read(object)?;
    object.verify(&bytes)?;
    Ok(bytes)
}
struct Reader<'a>(&'a [u8]);
impl Reader<'_> {
    fn array<const N: usize>(&mut self) -> Result<[u8; N]> {
        let (value, rest) = self.0.split_at_checked(N).ok_or(Error::Protocol)?;
        self.0 = rest;
        value.try_into().map_err(|_| Error::Protocol)
    }
    fn byte(&mut self) -> Result<u8> {
        Ok(self.array::<1>()?[0])
    }
    fn boolean(&mut self) -> Result<bool> {
        match self.byte()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(Error::Protocol),
        }
    }
    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.array()?))
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.array()?))
    }
}
struct Contents<'a> {
    reader: &'a dyn SnapshotReader,
    blocks: &'a [ObjectRef],
    next: usize,
    current: Vec<u8>,
    at: usize,
}
impl Contents<'_> {
    fn take(&mut self, length: usize) -> Result<Vec<u8>> {
        let mut bytes = Vec::with_capacity(length);
        while bytes.len() < length {
            if self.at == self.current.len() {
                let object = self.blocks.get(self.next).ok_or(Error::Protocol)?;
                self.current = read(self.reader, object)?;
                self.at = 0;
                self.next += 1;
            }
            let n = (length - bytes.len()).min(self.current.len() - self.at);
            bytes.extend_from_slice(&self.current[self.at..self.at + n]);
            self.at += n;
        }
        Ok(bytes)
    }
}
