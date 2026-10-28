use super::*;
use crate::{Result, invalid};
use serde::de::DeserializeOwned;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::Write,
    path::Path,
};
use tickwire_ipc::{ResourceHeader, Wire};
mod files;
mod validate;
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Head {
    schema: u16,
    sequence: u64,
    digest: Hash,
    bytes: usize,
}
#[derive(Default)]
struct Seen {
    instances: BTreeSet<[u8; 16]>,
    resources: BTreeSet<[u8; 16]>,
    channels: BTreeSet<[u8; 16]>,
    owners: BTreeSet<[u8; 16]>,
}
impl Seen {
    fn add(&mut self, s: &Snapshot) -> Result<()> {
        self.instances.insert(s.config.identity.instance);
        self.owners.extend(s.reserved_owners.iter().copied());
        for bank in &s.banks {
            let header = ResourceHeader::decode(bank)?;
            self.resources.insert(header.resource_id);
            self.channels.insert(header.channel_id);
        }
        for feed in &s.config.feeds {
            self.owners.insert(feed.owner_id);
            for event in &feed.events {
                self.owners.insert(event.owner_id);
            }
        }
        Ok(())
    }
}
/// Sole writer of immutable snapshot history. This handle may move to a cold worker.
pub struct Store {
    files: files::Files,
    current: Option<Snapshot>,
    head: Option<Head>,
    objects: BTreeMap<Hash, usize>,
    total: usize,
    seen: Seen,
    poisoned: bool,
}
impl Store {
    pub fn open(path: &Path) -> Result<Self> {
        let files = files::Files::open(path)?;
        let mut result = Self {
            files,
            current: None,
            head: None,
            objects: BTreeMap::new(),
            total: 0,
            seen: Seen::default(),
            poisoned: false,
        };
        let entries = result.files.inventory(MAX_SNAPSHOTS + 2)?;
        let mut has_head = false;
        for (name, size) in entries {
            match name.as_str() {
                "LOCK" if size == 0 => {}
                "HEAD" if size <= 512 => has_head = true,
                _ => {
                    let hash = parse_name(&name)?;
                    if size == 0 || result.objects.insert(hash, size).is_some() {
                        return Err(invalid("MD recovery duplicate/empty object"));
                    }
                    result.total = result
                        .total
                        .checked_add(size)
                        .filter(|n| *n <= MAX_TOTAL_BYTES)
                        .ok_or_else(|| invalid("MD recovery total bytes"))?;
                }
            }
        }
        if !has_head {
            if !result.objects.is_empty() {
                return Err(invalid("MD recovery objects without HEAD"));
            }
            result.files.check()?;
            return Ok(result);
        }
        let head: Head = decode(&result.files.read("HEAD", 512)?, 512)?;
        if head.schema != 1
            || head.sequence == 0
            || head.sequence >= 1 << 63
            || head.sequence > MAX_SNAPSHOTS as u64
            || head.digest == [0; 32]
            || head.bytes == 0
            || head.bytes > MAX_SNAPSHOT_BYTES
        {
            return Err(invalid("MD recovery HEAD bounds"));
        }
        let mut hash = head.digest;
        let mut sequence = head.sequence;
        let mut visited = BTreeSet::new();
        let mut newer = None;
        loop {
            if !visited.insert(hash) {
                return Err(invalid("MD recovery chain cycle"));
            }
            let size = *result
                .objects
                .get(&hash)
                .ok_or_else(|| invalid("MD recovery missing predecessor"))?;
            if sequence == head.sequence && size != head.bytes {
                return Err(invalid("MD recovery HEAD length"));
            }
            let bytes = result.files.read(&name(hash), MAX_SNAPSHOT_BYTES)?;
            if bytes.len() != size || digest(&bytes) != hash {
                return Err(invalid("MD recovery object digest"));
            }
            let snapshot: Snapshot = decode(&bytes, MAX_SNAPSHOT_BYTES)?;
            validate::snapshot(&snapshot)?;
            let link = validate::Link::new(&snapshot)?;
            if let Some(next) = &newer {
                validate::continuation(&link, next)?;
            }
            newer = Some(link);
            if snapshot.sequence != sequence {
                return Err(invalid("MD recovery sequence chain"));
            }
            result.seen.add(&snapshot)?;
            let previous = snapshot.previous;
            if result.current.is_none() {
                result.current = Some(snapshot);
            }
            if sequence == 1 {
                if previous != [0; 32] {
                    return Err(invalid("MD recovery first predecessor"));
                }
                break;
            }
            if previous == [0; 32] {
                return Err(invalid("MD recovery truncated chain"));
            }
            hash = previous;
            sequence -= 1;
        }
        if visited.len() != result.objects.len() {
            return Err(invalid("MD recovery orphan object"));
        }
        result.head = Some(head);
        result.files.check()?;
        Ok(result)
    }
    pub fn current(&self) -> Option<&Snapshot> {
        self.current.as_ref()
    }
    pub fn current_hash(&self) -> Option<Hash> {
        self.head.as_ref().map(|h| h.digest)
    }
    pub fn seen_instance(&self, id: [u8; 16]) -> bool {
        self.seen.instances.contains(&id)
    }
    pub fn seen_resource(&self, id: [u8; 16]) -> bool {
        self.seen.resources.contains(&id)
    }
    pub fn seen_channel(&self, id: [u8; 16]) -> bool {
        self.seen.channels.contains(&id)
    }
    pub fn seen_owner(&self, id: [u8; 16]) -> bool {
        self.seen.owners.contains(&id)
    }
    pub fn write(&mut self, next: Snapshot) -> Result<Hash> {
        if self.poisoned {
            return Err(invalid("MD recovery store poisoned"));
        }
        validate::snapshot(&next)?;
        if let Some(before) = &self.current {
            validate::continuation(&validate::Link::new(before)?, &validate::Link::new(&next)?)?;
        }
        let expected = self.head.as_ref().map_or(1, |h| h.sequence + 1);
        if next.sequence != expected || next.previous != self.current_hash().unwrap_or([0; 32]) {
            return Err(invalid("MD recovery next sequence/predecessor"));
        }
        let bytes = encode(&next, MAX_SNAPSHOT_BYTES)?;
        let total = self
            .total
            .checked_add(bytes.len())
            .filter(|n| *n <= MAX_TOTAL_BYTES)
            .ok_or_else(|| invalid("MD recovery total bytes"))?;
        if self.objects.len() >= MAX_SNAPSHOTS {
            return Err(invalid("MD recovery object count"));
        }
        let hash = digest(&bytes);
        let head = Head {
            schema: 1,
            sequence: next.sequence,
            digest: hash,
            bytes: bytes.len(),
        };
        let head_bytes = encode(&head, 512)?;
        let result = (|| -> Result<()> {
            self.files.check()?;
            self.check_inventory()?;
            self.files.create(&name(hash), &bytes)?;
            self.files.check()?;
            self.files.publish_head(&head_bytes)
        })();
        if let Err(error) = result {
            self.poisoned = true;
            return Err(error);
        }
        self.seen.add(&next)?;
        self.current = Some(next);
        self.head = Some(head);
        self.objects.insert(hash, bytes.len());
        self.total = total;
        Ok(hash)
    }
    #[cfg(all(test, target_os = "linux"))]
    pub(super) fn inherited_lock(&self) -> std::fs::File {
        self.files.inherited_lock()
    }
    fn check_inventory(&self) -> Result<()> {
        let entries = self.files.inventory(MAX_SNAPSHOTS + 2)?;
        if entries.len() != self.objects.len() + 1 + usize::from(self.head.is_some()) {
            return Err(invalid("MD recovery directory contents changed"));
        }
        for (entry, size) in entries {
            if entry == "LOCK" && size == 0 {
                continue;
            }
            if entry == "HEAD" {
                let actual: Head = decode(&self.files.read("HEAD", 512)?, 512)?;
                if self.head.as_ref() != Some(&actual) {
                    return Err(invalid("MD recovery HEAD changed"));
                }
            } else if self.objects.get(&parse_name(&entry)?) != Some(&size) {
                return Err(invalid("MD recovery object inventory changed"));
            }
        }
        Ok(())
    }
}
fn digest(bytes: &[u8]) -> Hash {
    Sha256::digest(bytes).into()
}
fn name(hash: Hash) -> String {
    format!(
        "object-{}",
        hash.iter().map(|b| format!("{b:02x}")).collect::<String>()
    )
}
fn parse_name(name: &str) -> Result<Hash> {
    let bytes = name
        .strip_prefix("object-")
        .filter(|s| s.len() == 64)
        .ok_or_else(|| invalid("MD recovery unknown file"))?
        .as_bytes();
    let mut hash = [0; 32];
    for (index, pair) in bytes.as_chunks::<2>().0.iter().enumerate() {
        let digit = |b| match b {
            b'0'..=b'9' => Ok(b - b'0'),
            b'a'..=b'f' => Ok(b - b'a' + 10),
            _ => Err(invalid("MD recovery object filename")),
        };
        hash[index] = digit(pair[0])? * 16 + digit(pair[1])?;
    }
    Ok(hash)
}
struct Bounded {
    bytes: Vec<u8>,
    maximum: usize,
}
impl Write for Bounded {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if self
            .bytes
            .len()
            .checked_add(bytes.len())
            .is_none_or(|n| n > self.maximum)
        {
            return Err(std::io::Error::other("MD recovery encoded bounds"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
fn encode<T: Serialize>(value: &T, maximum: usize) -> Result<Vec<u8>> {
    let mut writer = Bounded {
        bytes: Vec::new(),
        maximum,
    };
    serde_json::to_writer(&mut writer, value)?;
    Ok(writer.bytes)
}
fn decode<T: Serialize + DeserializeOwned>(bytes: &[u8], maximum: usize) -> Result<T> {
    if bytes.is_empty() || bytes.len() > maximum {
        return Err(invalid("MD recovery encoded bounds"));
    }
    let value = serde_json::from_slice(bytes)?;
    if encode(&value, maximum)? != bytes {
        return Err(invalid("MD recovery noncanonical object"));
    }
    Ok(value)
}
