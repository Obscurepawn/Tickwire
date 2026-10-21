use crate::{Error, Result, frame::Kind};
use tickwire_control_raft::{
    Message, MessageType,
    generation::{StagingSnapshot, VerifiedSnapshot},
    snapshot::{SnapshotLimits, SnapshotManifest, SnapshotReader},
};
const CHUNK: usize = 65_536;
pub(crate) struct Offer {
    pub term: u64,
    pub manifest: SnapshotManifest,
    pub digest: [u8; 32],
}
impl Offer {
    pub fn parse(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 40 {
            return Err(Error::Protocol);
        }
        let term = u64::from_le_bytes(bytes[..8].try_into().map_err(|_| Error::Protocol)?);
        let digest = bytes[8..40].try_into().map_err(|_| Error::Protocol)?;
        let manifest = SnapshotManifest::decode(&bytes[40..], SnapshotLimits::default())
            .map_err(|_| Error::Protocol)?;
        if term == 0
            || term >= 1 << 63
            || term < manifest.cut().term()
            || manifest.digest().map_err(|_| Error::Protocol)? != digest
        {
            return Err(Error::Identity);
        }
        Ok(Self {
            term,
            manifest,
            digest,
        })
    }
    pub fn message(&self, from: u64, to: u64) -> Result<Message> {
        let mut m = Message::default();
        m.set_msg_type(MessageType::MsgSnapshot);
        m.from = from;
        m.to = to;
        m.term = self.term;
        m.set_snapshot(self.manifest.raft_snapshot().map_err(|_| Error::Protocol)?);
        Ok(m)
    }
}
pub(crate) struct Incoming {
    pub offer: Offer,
    stage: StagingSnapshot,
    object: usize,
    offset: u64,
}
impl Incoming {
    pub fn new(offer: Offer, stage: StagingSnapshot) -> Self {
        Self {
            offer,
            stage,
            object: 0,
            offset: 0,
        }
    }
    pub fn chunk(&mut self, bytes: &[u8]) -> Result<()> {
        if bytes.len() < 44 || bytes.len() > 44 + CHUNK || bytes[..32] != self.offer.digest {
            return Err(Error::Protocol);
        }
        let ordinal =
            u32::from_le_bytes(bytes[32..36].try_into().map_err(|_| Error::Protocol)?) as usize;
        let offset = u64::from_le_bytes(bytes[36..44].try_into().map_err(|_| Error::Protocol)?);
        if ordinal != self.object || offset != self.offset {
            return Err(Error::Protocol);
        }
        let object = *self
            .offer
            .manifest
            .objects()
            .get(ordinal)
            .ok_or(Error::Protocol)?;
        let end = offset
            .checked_add((bytes.len() - 44) as u64)
            .ok_or(Error::Bounds)?;
        if end > object.length() || bytes.len() == 44 && object.length() != 0 {
            return Err(Error::Bounds);
        }
        self.stage
            .append(object, offset, &bytes[44..])
            .map_err(|_| Error::Protocol)?;
        self.offset = end;
        if end == object.length() {
            self.stage
                .seal_object(object)
                .map_err(|_| Error::Protocol)?;
            self.object += 1;
            self.offset = 0;
        }
        Ok(())
    }
    pub fn finish(self, bytes: &[u8]) -> Result<VerifiedSnapshot> {
        if bytes != self.offer.digest || self.object != self.offer.manifest.objects().len() {
            return Err(Error::Protocol);
        }
        let result = self
            .stage
            .finish(
                self.offer.manifest.cut().clone(),
                self.offer.manifest.application().clone(),
            )
            .map_err(|_| Error::Protocol)?;
        if result.manifest() != &self.offer.manifest {
            return Err(Error::Identity);
        }
        Ok(result)
    }
}
pub(crate) struct Outgoing {
    snapshot: VerifiedSnapshot,
    term: u64,
    pub digest: [u8; 32],
    pub index: u64,
    object: usize,
    offset: usize,
    bytes: Vec<u8>,
    phase: u8,
}
impl Outgoing {
    pub fn new(snapshot: VerifiedSnapshot, message: &Message) -> Result<Self> {
        if message.get_msg_type() != MessageType::MsgSnapshot
            || message.get_snapshot()
                != &snapshot
                    .manifest()
                    .raft_snapshot()
                    .map_err(|_| Error::Protocol)?
        {
            return Err(Error::Identity);
        }
        Ok(Self {
            digest: snapshot.manifest().digest().map_err(|_| Error::Protocol)?,
            index: snapshot.manifest().cut().index(),
            snapshot,
            term: message.term,
            object: 0,
            offset: 0,
            bytes: Vec::new(),
            phase: 0,
        })
    }
    pub fn next(&mut self) -> Result<Option<(Kind, Vec<u8>)>> {
        match self.phase {
            0 => {
                let mut b = self.term.to_le_bytes().to_vec();
                b.extend_from_slice(&self.digest);
                b.extend_from_slice(
                    &self
                        .snapshot
                        .manifest()
                        .encode()
                        .map_err(|_| Error::Protocol)?,
                );
                self.phase = 1;
                Ok(Some((Kind::SnapshotBegin, b)))
            }
            1 => {
                if self.object == self.snapshot.manifest().objects().len() {
                    self.phase = 2;
                    return Ok(Some((Kind::SnapshotEnd, self.digest.to_vec())));
                }
                let object = self.snapshot.manifest().objects()[self.object];
                if self.offset == 0 {
                    self.bytes = self.snapshot.read(&object).map_err(|_| Error::Protocol)?;
                }
                let end = (self.offset + CHUNK).min(self.bytes.len());
                let mut b = self.digest.to_vec();
                b.extend_from_slice(&(self.object as u32).to_le_bytes());
                b.extend_from_slice(&(self.offset as u64).to_le_bytes());
                b.extend_from_slice(&self.bytes[self.offset..end]);
                self.offset = end;
                if end == self.bytes.len() {
                    self.object += 1;
                    self.offset = 0;
                    self.bytes = Vec::new();
                }
                Ok(Some((Kind::SnapshotChunk, b)))
            }
            _ => Ok(None),
        }
    }
    pub fn accepted(&self, bytes: &[u8]) -> Result<()> {
        if bytes.len() != 40
            || bytes[..32] != self.digest
            || u64::from_le_bytes(bytes[32..].try_into().map_err(|_| Error::Protocol)?)
                != self.index
            || self.phase != 2
        {
            return Err(Error::Identity);
        }
        Ok(())
    }
}
pub(crate) fn acknowledgement(digest: [u8; 32], index: u64) -> Vec<u8> {
    let mut b = digest.to_vec();
    b.extend_from_slice(&index.to_le_bytes());
    b
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs,
        sync::atomic::{AtomicU64, Ordering},
    };
    use tickwire_control_raft::{generation::SnapshotDirectory, snapshot::SnapshotWriter};
    static N: AtomicU64 = AtomicU64::new(1);
    #[test]
    fn sequential_chunk_validation_and_complete_closure_are_required() {
        let path = std::env::temp_dir().join(format!(
            "raft-transfer-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        let mut encoded = b"TWRS0001".to_vec();
        encoded.extend_from_slice(&[8; 16]);
        for n in [7u64, 2, 0] {
            encoded.extend_from_slice(&n.to_le_bytes());
        }
        encoded.extend_from_slice(&[0; 8]);
        for n in [3u16, 0, 0, 0] {
            encoded.extend_from_slice(&n.to_le_bytes());
        }
        encoded.extend_from_slice(&1u32.to_le_bytes());
        encoded.extend_from_slice(&1u32.to_le_bytes());
        let data = vec![9; CHUNK + 1];
        let object = tickwire_control_raft::snapshot::ObjectRef::for_bytes(&data).unwrap();
        encoded.extend_from_slice(&object.digest());
        encoded.extend_from_slice(&object.length().to_le_bytes());
        for id in [1u64, 2, 3] {
            encoded.extend_from_slice(&id.to_le_bytes());
        }
        encoded.extend_from_slice(&object.digest());
        encoded.extend_from_slice(&object.length().to_le_bytes());
        let m = SnapshotManifest::decode(&encoded, SnapshotLimits::default()).unwrap();
        let source =
            SnapshotDirectory::open(path.join("source"), [8; 16], 1, SnapshotLimits::default())
                .unwrap();
        let target =
            SnapshotDirectory::open(path.join("target"), [8; 16], 2, SnapshotLimits::default())
                .unwrap();
        let mut stage = source.stage().unwrap();
        stage.put(&data).unwrap();
        let verified = stage
            .finish(m.cut().clone(), m.application().clone())
            .unwrap();
        let offer = Offer {
            term: 2,
            digest: m.digest().unwrap(),
            manifest: m,
        };
        let mut out = Outgoing::new(verified, &offer.message(1, 2).unwrap()).unwrap();
        let (_, begin) = out.next().unwrap().unwrap();
        let mut incoming = Incoming::new(Offer::parse(&begin).unwrap(), target.stage().unwrap());
        let (_, first) = out.next().unwrap().unwrap();
        let mut bad = first.clone();
        bad[36] = 1;
        assert!(incoming.chunk(&bad).is_err());
        incoming.chunk(&first).unwrap();
        assert!(incoming.chunk(&first).is_err());
        let (_, last) = out.next().unwrap().unwrap();
        incoming.chunk(&last).unwrap();
        let (_, end) = out.next().unwrap().unwrap();
        let result = incoming.finish(&end).unwrap();
        assert_eq!(result.read(&object).unwrap(), data);
        assert!(target.current().unwrap().is_none());
        drop(result);
        drop(out);
        drop(target);
        drop(source);
        fs::remove_dir_all(path).unwrap();
    }
}
