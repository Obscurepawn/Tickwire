//! Versioned, bounded snapshot closure shared by the Raft owner and its application.
use crate::{Error, Result, driver::Membership};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

pub const MAX_MANIFEST_BYTES: usize = 65_536;
pub const MAX_OBJECT_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_OBJECTS: usize = 1024;
pub const MAX_CLOSURE_BYTES: u64 = 1024 * 1024 * 1024;

#[derive(Debug, Clone, Copy)]
pub struct SnapshotLimits {
    pub max_objects: usize,
    pub max_object_bytes: usize,
    pub max_closure_bytes: u64,
}
impl Default for SnapshotLimits {
    fn default() -> Self {
        Self {
            max_objects: MAX_OBJECTS,
            max_object_bytes: MAX_OBJECT_BYTES,
            max_closure_bytes: 256 * 1024 * 1024,
        }
    }
}
impl SnapshotLimits {
    pub fn validate(self) -> Result<()> {
        if !(1..=MAX_OBJECTS).contains(&self.max_objects)
            || !(1..=MAX_OBJECT_BYTES).contains(&self.max_object_bytes)
            || !(1..=MAX_CLOSURE_BYTES).contains(&self.max_closure_bytes)
        {
            return Err(Error::Bounds);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ObjectRef {
    digest: [u8; 32],
    length: u64,
}
impl ObjectRef {
    pub fn new(digest: [u8; 32], length: u64) -> Result<Self> {
        if digest == [0; 32] || length > MAX_OBJECT_BYTES as u64 {
            return Err(Error::Bounds);
        }
        Ok(Self { digest, length })
    }
    pub fn for_bytes(bytes: &[u8]) -> Result<Self> {
        Self::new(Sha256::digest(bytes).into(), bytes.len() as u64)
    }
    pub fn digest(self) -> [u8; 32] {
        self.digest
    }
    pub fn length(self) -> u64 {
        self.length
    }
    pub fn verify(self, bytes: &[u8]) -> Result<()> {
        if self.length != bytes.len() as u64
            || self.digest != <[u8; 32]>::from(Sha256::digest(bytes))
        {
            return Err(Error::Corrupt);
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotCut {
    group: [u8; 16],
    index: u64,
    term: u64,
    membership: Membership,
}
impl SnapshotCut {
    pub(crate) fn new(
        group: [u8; 16],
        index: u64,
        term: u64,
        membership: Membership,
    ) -> Result<Self> {
        if group == [0; 16]
            || index == 0
            || index >= 1 << 63
            || term == 0
            || term >= 1 << 63
            || membership.applied_index() > index
        {
            return Err(Error::Identity);
        }
        validate_membership(&membership)?;
        if membership
            .operations
            .last()
            .is_some_and(|r| r.term() > term)
        {
            return Err(Error::Corrupt);
        }
        Ok(Self {
            group,
            index,
            term,
            membership,
        })
    }
    pub fn group(&self) -> [u8; 16] {
        self.group
    }
    pub fn index(&self) -> u64 {
        self.index
    }
    pub fn term(&self) -> u64 {
        self.term
    }
    pub fn membership(&self) -> &Membership {
        &self.membership
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplicationRoot {
    schema: u32,
    root: ObjectRef,
}
impl ApplicationRoot {
    pub fn new(schema: u32, root: ObjectRef) -> Result<Self> {
        if schema == 0 || root.length == 0 {
            return Err(Error::Identity);
        }
        Ok(Self { schema, root })
    }
    pub fn schema(&self) -> u32 {
        self.schema
    }
    pub fn root(&self) -> ObjectRef {
        self.root
    }
}
pub trait SnapshotWriter {
    fn put(&mut self, bytes: &[u8]) -> Result<ObjectRef>;
}
pub trait SnapshotReader {
    fn read(&self, object: &ObjectRef) -> Result<Vec<u8>>;
}
/// A private application adapter borrowed by the same owner as the Node.
/// Preparation must validate the complete application closure without publishing
/// state. Installation runs only after CURRENT and its seed journal are durable.
pub trait SnapshotRestore {
    type Prepared;
    type Error;
    fn prepare(
        &mut self,
        snapshot: &crate::generation::VerifiedSnapshot,
    ) -> core::result::Result<Self::Prepared, Self::Error>;
    fn install(
        &mut self,
        prepared: Self::Prepared,
        installed: &InstalledGeneration,
    ) -> core::result::Result<(), Self::Error>;
}
#[derive(Debug, PartialEq, Eq)]
pub enum SnapshotDriveError<E> {
    Raft(Error),
    Application(E),
}
impl<E> From<Error> for SnapshotDriveError<E> {
    fn from(error: Error) -> Self {
        Self::Raft(error)
    }
}
pub trait SnapshotApplication {
    type Prepared;
    type Error;
    fn capture(
        &self,
        cut: &SnapshotCut,
        out: &mut dyn SnapshotWriter,
    ) -> core::result::Result<ApplicationRoot, Self::Error>;
    fn prepare_restore(
        &self,
        cut: &SnapshotCut,
        root: &ApplicationRoot,
        objects: &dyn SnapshotReader,
    ) -> core::result::Result<Self::Prepared, Self::Error>;
    /// The application owner must first match this receipt against its Node's
    /// active local generation, then check the prepared cut and root.
    fn install_prepared(
        &mut self,
        prepared: Self::Prepared,
        installed: &InstalledGeneration,
    ) -> core::result::Result<(), Self::Error>;
    fn scanned_through(&self) -> u64;
}
/// Created only after the local generation and CURRENT are durably installed.
/// A parsed manifest or a caller's prepared application state cannot create it.
#[derive(Clone)]
pub struct InstalledGeneration {
    pub(crate) owner: [u8; 16],
    pub(crate) node_id: u64,
    pub(crate) generation: [u8; 16],
    pub(crate) manifest: SnapshotManifest,
}
impl InstalledGeneration {
    pub fn node_id(&self) -> u64 {
        self.node_id
    }
    pub fn generation(&self) -> [u8; 16] {
        self.generation
    }
    pub fn owner(&self) -> [u8; 16] {
        self.owner
    }
    pub fn cut(&self) -> &SnapshotCut {
        self.manifest.cut()
    }
    pub fn application(&self) -> &ApplicationRoot {
        self.manifest.application()
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotManifest {
    cut: SnapshotCut,
    application: ApplicationRoot,
    objects: Vec<ObjectRef>,
}
impl SnapshotManifest {
    pub fn new(
        cut: SnapshotCut,
        application: ApplicationRoot,
        objects: Vec<ObjectRef>,
        limits: SnapshotLimits,
    ) -> Result<Self> {
        limits.validate()?;
        if objects.is_empty() || objects.len() > limits.max_objects {
            return Err(Error::Bounds);
        }
        let mut previous = None;
        let mut total = 0u64;
        for object in &objects {
            if previous.is_some_and(|p| p >= object.digest)
                || object.length > limits.max_object_bytes as u64
            {
                return Err(Error::Corrupt);
            }
            previous = Some(object.digest);
            total = total.checked_add(object.length).ok_or(Error::Bounds)?;
        }
        if total > limits.max_closure_bytes {
            return Err(Error::Bounds);
        }
        if !objects.contains(&application.root) {
            return Err(Error::Corrupt);
        }
        Ok(Self {
            cut,
            application,
            objects,
        })
    }
    pub fn raft_snapshot(&self) -> Result<raft::eraftpb::Snapshot> {
        let m = self.cut.membership();
        Ok(raft::eraftpb::Snapshot {
            data: self.encode()?.into(),
            metadata: Some(raft::eraftpb::SnapshotMetadata {
                index: self.cut.index(),
                term: self.cut.term(),
                conf_state: Some(raft::eraftpb::ConfState {
                    voters: m.voters().to_vec(),
                    voters_outgoing: m.voters_outgoing().to_vec(),
                    learners: m.learners().to_vec(),
                    learners_next: m.learners_next().to_vec(),
                    auto_leave: m.auto_leave(),
                    ..Default::default()
                })
                .into(),
                ..Default::default()
            })
            .into(),
            ..Default::default()
        })
    }
    pub fn decode(bytes: &[u8], limits: SnapshotLimits) -> Result<Self> {
        limits.validate()?;
        if bytes.len() > MAX_MANIFEST_BYTES {
            return Err(Error::Bounds);
        }
        let mut r = Reader(bytes);
        let version = match r.take(8)? {
            b"TWRS0001" => 1,
            b"TWRS0002" => 2,
            _ => return Err(Error::Corrupt),
        };
        let group = r.array()?;
        let index = r.u64()?;
        let term = r.u64()?;
        let applied_index = r.u64()?;
        let auto_leave = match r.take(1)?[0] {
            0 => false,
            1 => true,
            _ => return Err(Error::Corrupt),
        };
        if r.take(7)? != [0; 7] {
            return Err(Error::Corrupt);
        }
        let counts = [
            r.u16()? as usize,
            r.u16()? as usize,
            r.u16()? as usize,
            r.u16()? as usize,
        ];
        if counts.iter().any(|n| *n > 16) {
            return Err(Error::Bounds);
        }
        let schema = r.u32()?;
        let count = r.u32()? as usize;
        if count == 0 || count > limits.max_objects {
            return Err(Error::Bounds);
        }
        let root = ObjectRef::new(r.array()?, r.u64()?)?;
        let expected = counts.iter().sum::<usize>() * 8 + count * 40;
        if (version == 1 && r.0.len() != expected) || (version == 2 && r.0.len() < expected + 4) {
            return Err(Error::Corrupt);
        }
        let mut sets = Vec::new();
        for count in counts {
            let mut ids = Vec::with_capacity(count);
            for _ in 0..count {
                ids.push(r.u64()?);
            }
            sets.push(ids);
        }
        let mut membership = Membership {
            voters: sets.remove(0),
            voters_outgoing: sets.remove(0),
            learners: sets.remove(0),
            learners_next: sets.remove(0),
            auto_leave,
            applied_index,
            origin: [0; 3],
            retired: vec![],
            operations: vec![],
        };
        let application = ApplicationRoot::new(schema, root)?;
        let mut objects = Vec::with_capacity(count);
        for _ in 0..count {
            objects.push(ObjectRef::new(r.array()?, r.u64()?)?);
        }
        if version == 2 {
            let length = r.u32()? as usize;
            let complete = Membership::decode_history(r.take(length)?)?;
            if complete.configuration() != membership.configuration()
                || complete.applied_index() != membership.applied_index()
                || complete.operations.is_empty()
            {
                return Err(Error::Corrupt);
            }
            membership = complete;
        } else {
            if membership.voters.len() != 3
                || membership.applied_index != 0
                || !membership.voters_outgoing.is_empty()
                || !membership.learners.is_empty()
                || !membership.learners_next.is_empty()
                || membership.auto_leave
            {
                return Err(Error::Corrupt);
            }
            membership.origin = membership
                .voters
                .as_slice()
                .try_into()
                .map_err(|_| Error::Corrupt)?;
        }
        if !r.0.is_empty() {
            return Err(Error::Corrupt);
        }
        let cut = SnapshotCut::new(group, index, term, membership)?;
        Self::new(cut, application, objects, limits)
    }
    pub fn encode(&self) -> Result<Vec<u8>> {
        let version2 = !self.cut.membership.operations.is_empty();
        let mut bytes = if version2 { b"TWRS0002" } else { b"TWRS0001" }.to_vec();
        bytes.extend_from_slice(&self.cut.group);
        for n in [
            self.cut.index,
            self.cut.term,
            self.cut.membership.applied_index,
        ] {
            bytes.extend_from_slice(&n.to_le_bytes());
        }
        bytes.push(u8::from(self.cut.membership.auto_leave));
        bytes.extend_from_slice(&[0; 7]);
        let membership = &self.cut.membership;
        let sets = [
            &membership.voters,
            &membership.voters_outgoing,
            &membership.learners,
            &membership.learners_next,
        ];
        for ids in sets {
            bytes.extend_from_slice(&(ids.len() as u16).to_le_bytes());
        }
        bytes.extend_from_slice(&self.application.schema.to_le_bytes());
        bytes.extend_from_slice(&(self.objects.len() as u32).to_le_bytes());
        put_object(&mut bytes, self.application.root);
        for ids in sets {
            for id in ids {
                bytes.extend_from_slice(&id.to_le_bytes());
            }
        }
        for object in &self.objects {
            put_object(&mut bytes, *object);
        }
        if version2 {
            let history = self.cut.membership.encode_history()?;
            bytes.extend_from_slice(&(history.len() as u32).to_le_bytes());
            bytes.extend_from_slice(&history);
        }
        if bytes.len() > MAX_MANIFEST_BYTES {
            return Err(Error::Bounds);
        }
        Ok(bytes)
    }
    pub fn digest(&self) -> Result<[u8; 32]> {
        Ok(Sha256::digest(self.encode()?).into())
    }
    pub fn cut(&self) -> &SnapshotCut {
        &self.cut
    }
    pub fn application(&self) -> &ApplicationRoot {
        &self.application
    }
    pub fn objects(&self) -> &[ObjectRef] {
        &self.objects
    }
}
fn put_object(bytes: &mut Vec<u8>, object: ObjectRef) {
    bytes.extend_from_slice(&object.digest);
    bytes.extend_from_slice(&object.length.to_le_bytes());
}
fn validate_membership(m: &Membership) -> Result<()> {
    let sets = [&m.voters, &m.voters_outgoing, &m.learners, &m.learners_next];
    let mut all = std::collections::BTreeSet::<u64>::new();
    for ids in sets {
        if ids.len() > 16 || ids.contains(&0) || ids.windows(2).any(|p| p[0] >= p[1]) {
            return Err(Error::Corrupt);
        }
        all.extend(ids.iter().copied());
    }
    if m.voters.is_empty()
        || all.len() > 16
        || (m.voters_outgoing.is_empty() && (m.auto_leave || !m.learners_next.is_empty()))
        || m.learners
            .iter()
            .any(|id| m.voters.contains(id) || m.voters_outgoing.contains(id))
        || m.learners_next.iter().any(|id| {
            !m.voters_outgoing.contains(id) || m.voters.contains(id) || m.learners.contains(id)
        })
    {
        return Err(Error::Corrupt);
    }
    Ok(())
}
struct Reader<'a>(&'a [u8]);
impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if n > self.0.len() {
            return Err(Error::Corrupt);
        }
        let (a, b) = self.0.split_at(n);
        self.0 = b;
        Ok(a)
    }
    fn array<const N: usize>(&mut self) -> Result<[u8; N]> {
        self.take(N)?.try_into().map_err(|_| Error::Corrupt)
    }
    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.array()?))
    }
    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.array()?))
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.array()?))
    }
}
pub struct MemoryObjects {
    limits: SnapshotLimits,
    objects: BTreeMap<ObjectRef, Vec<u8>>,
    total: u64,
}
impl MemoryObjects {
    pub fn new(limits: SnapshotLimits) -> Result<Self> {
        limits.validate()?;
        Ok(Self {
            limits,
            objects: BTreeMap::new(),
            total: 0,
        })
    }
    pub fn objects(&self) -> Vec<ObjectRef> {
        self.objects.keys().copied().collect()
    }
    pub fn total_bytes(&self) -> u64 {
        self.total
    }
}
impl SnapshotWriter for MemoryObjects {
    fn put(&mut self, bytes: &[u8]) -> Result<ObjectRef> {
        let object = ObjectRef::for_bytes(bytes)?;
        if self.objects.contains_key(&object) {
            return Ok(object);
        }
        let total = self.total.checked_add(object.length).ok_or(Error::Bounds)?;
        if bytes.len() > self.limits.max_object_bytes
            || self.objects.len() == self.limits.max_objects
            || total > self.limits.max_closure_bytes
        {
            return Err(Error::Bounds);
        }
        self.objects.insert(object, bytes.to_vec());
        self.total = total;
        Ok(object)
    }
}
impl SnapshotReader for MemoryObjects {
    fn read(&self, object: &ObjectRef) -> Result<Vec<u8>> {
        let bytes = self.objects.get(object).ok_or(Error::Corrupt)?;
        object.verify(bytes)?;
        Ok(bytes.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn cut() -> SnapshotCut {
        SnapshotCut::new(
            [1; 16],
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
    #[test]
    fn literal_manifest_roundtrip_and_exact_tail() {
        let root = ObjectRef::for_bytes(b"state").unwrap();
        let manifest = SnapshotManifest::new(
            cut(),
            ApplicationRoot::new(4, root).unwrap(),
            vec![root],
            SnapshotLimits::default(),
        )
        .unwrap();
        let mut expected = b"TWRS0001".to_vec();
        expected.extend_from_slice(&[1; 16]);
        for n in [7u64, 2, 0] {
            expected.extend_from_slice(&n.to_le_bytes());
        }
        expected.extend_from_slice(&[0; 8]);
        expected.extend_from_slice(&[3, 0, 0, 0, 0, 0, 0, 0]);
        expected.extend_from_slice(&4u32.to_le_bytes());
        expected.extend_from_slice(&1u32.to_le_bytes());
        expected.extend_from_slice(&root.digest());
        expected.extend_from_slice(&5u64.to_le_bytes());
        for n in [1u64, 2, 3] {
            expected.extend_from_slice(&n.to_le_bytes());
        }
        expected.extend_from_slice(&root.digest());
        expected.extend_from_slice(&5u64.to_le_bytes());
        assert_eq!(manifest.encode().unwrap(), expected);
        assert_eq!(
            SnapshotManifest::decode(&expected, SnapshotLimits::default()).unwrap(),
            manifest
        );
        for len in 0..expected.len() {
            assert!(SnapshotManifest::decode(&expected[..len], SnapshotLimits::default()).is_err());
        }
        expected.push(0);
        assert!(SnapshotManifest::decode(&expected, SnapshotLimits::default()).is_err());
    }
    #[test]
    fn missing_root_duplicate_objects_and_limits_are_rejected() {
        let a = ObjectRef::for_bytes(b"state").unwrap();
        let b = ObjectRef::for_bytes(b"other").unwrap();
        assert!(
            SnapshotManifest::new(
                cut(),
                ApplicationRoot::new(1, a).unwrap(),
                vec![b],
                SnapshotLimits::default()
            )
            .is_err()
        );
        assert!(
            SnapshotManifest::new(
                cut(),
                ApplicationRoot::new(1, a).unwrap(),
                vec![a, a],
                SnapshotLimits::default()
            )
            .is_err()
        );
        assert!(
            SnapshotManifest::new(
                cut(),
                ApplicationRoot::new(1, a).unwrap(),
                vec![a],
                SnapshotLimits {
                    max_closure_bytes: 4,
                    ..SnapshotLimits::default()
                }
            )
            .is_err()
        );
        let mut objects = MemoryObjects::new(SnapshotLimits {
            max_objects: 1,
            max_object_bytes: 5,
            max_closure_bytes: 5,
        })
        .unwrap();
        assert_eq!(objects.put(b"state").unwrap(), a);
        assert_eq!(objects.put(b"state").unwrap(), a);
        assert!(objects.put(b"other").is_err());
        assert_eq!(objects.total_bytes(), 5);
    }
}
