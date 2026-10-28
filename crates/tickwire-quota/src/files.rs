//! Exclusive local durable quota allocation state. Restarts retain every grant debit.
use super::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt},
    path::{Path, PathBuf},
};
const MAX_BYTES: usize = 16 * 1024 * 1024;
pub(crate) mod domain_codec {
    use super::*;
    pub fn serialize<S: serde::Serializer>(
        d: &ClockDomain,
        s: S,
    ) -> core::result::Result<S::Ok, S::Error> {
        (d.boot_id(), d.generation()).serialize(s)
    }
    pub fn deserialize<'de, D: serde::Deserializer<'de>>(
        d: D,
    ) -> core::result::Result<ClockDomain, D::Error> {
        let (b, g) = <([u8; 16], u64)>::deserialize(d)?;
        ClockDomain::new(b, g).map_err(|_| serde::de::Error::custom("invalid clock domain"))
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Image {
    revision: u64,
    supervisor_generation: u64,
    domain: ([u8; 16], u64),
    last_ns: u64,
    next_grant: u64,
    windows: Vec<WindowConfig>,
    consumers: Vec<ConsumerRecord>,
    grants: Vec<GrantRecord>,
}
fn valid(s: &Snapshot) -> Result<()> {
    let domain = s.domain.ok_or(Error::Store)?;
    if s.revision == 0
        || s.supervisor_generation == 0
        || s.windows.len() > MAX_WINDOWS
        || s.consumers.len() > MAX_CONSUMERS
        || s.grants.len() > MAX_GRANTS
    {
        return Err(Error::Store);
    }
    for (i, w) in s.windows.iter().enumerate() {
        if !valid_window(w)
            || s.windows[..i].iter().any(|v| {
                v.key == w.key
                    || (v.key.bucket == w.key.bucket
                        && v.starts_ns < w.ends_ns
                        && w.starts_ns < v.ends_ns)
            })
        {
            return Err(Error::Store);
        }
        if accounted(s, w.key, None)? > w.limit
            || accounted(s, w.key, Some(Class::Normal))? > w.limit - w.recovery_reserve
        {
            return Err(Error::Store);
        }
    }
    for (i, c) in s.consumers.iter().enumerate() {
        if c.id.instance == [0; 16]
            || c.id.generation == 0
            || c.domain != domain
            || c.supervisor_generation == 0
            || c.supervisor_generation > s.supervisor_generation
            || c.lease_until_ns == 0
            || s.consumers[..i].iter().any(|p| p.id == c.id)
        {
            return Err(Error::Store);
        }
    }
    for (i, g) in s.grants.iter().enumerate() {
        if g.id == 0
            || g.id > s.next_grant
            || g.request_id == 0
            || g.owner.domain != domain
            || g.owner.supervisor_generation > s.supervisor_generation
            || g.expires_ns == 0
            || g.expires_ns > g.owner.lease_until_ns
            || g.costs.len() != g.retained.len()
            || g.costs.len() != g.windows.len()
            || s.grants[..i].iter().any(|p| p.id == g.id)
            || !s.consumers.iter().any(|c| c.id == g.owner.id)
        {
            return Err(Error::Store);
        }
        validate_costs(&g.costs)?;
        for ((cost, held), window) in g.costs.iter().zip(&g.retained).zip(&g.windows) {
            if *held > cost.units
                || (!g.reclaimed && *held != cost.units)
                || cost.window != window.key
                || !s.windows.contains(window)
            {
                return Err(Error::Store);
            }
        }
    }
    if s.next_grant != s.grants.iter().map(|g| g.id).max().unwrap_or(0) {
        return Err(Error::Store);
    }
    Ok(())
}
fn encode(s: &Snapshot) -> Result<Vec<u8>> {
    valid(s)?;
    let domain = s.domain.ok_or(Error::Store)?;
    let image = Image {
        revision: s.revision,
        supervisor_generation: s.supervisor_generation,
        domain: (domain.boot_id(), domain.generation()),
        last_ns: s.last_ns,
        next_grant: s.next_grant,
        windows: s.windows.clone(),
        consumers: s.consumers.clone(),
        grants: s.grants.clone(),
    };
    let body = serde_json::to_vec(&image).map_err(|_| Error::Store)?;
    if body.len() > MAX_BYTES - 40 {
        return Err(Error::Capacity);
    }
    let mut bytes = b"TWQUOTA1".to_vec();
    bytes.extend(Sha256::digest(&body));
    bytes.extend(body);
    Ok(bytes)
}
fn decode(bytes: &[u8]) -> Result<Snapshot> {
    if bytes.len() < 40
        || bytes.len() > MAX_BYTES
        || &bytes[..8] != b"TWQUOTA1"
        || bytes[8..40] != Sha256::digest(&bytes[40..])[..]
    {
        return Err(Error::Store);
    }
    let i: Image = serde_json::from_slice(&bytes[40..]).map_err(|_| Error::Store)?;
    let s = Snapshot {
        revision: i.revision,
        supervisor_generation: i.supervisor_generation,
        domain: Some(ClockDomain::new(i.domain.0, i.domain.1).map_err(|_| Error::Store)?),
        last_ns: i.last_ns,
        next_grant: i.next_grant,
        windows: i.windows,
        consumers: i.consumers,
        grants: i.grants,
    };
    valid(&s)?;
    Ok(s)
}
pub struct FileQuotaStore {
    root: PathBuf,
    _lock: WriterLock,
    state: Snapshot,
    poisoned: bool,
}
struct WriterLock(File);
impl Drop for WriterLock {
    fn drop(&mut self) {
        // A pre-exec child's inherited descriptor is not a store owner.
        let _ = self.0.unlock();
    }
}
impl FileQuotaStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_inner(path.as_ref()).map_err(|_| Error::Store)
    }
    fn open_inner(path: &Path) -> std::io::Result<Self> {
        match fs::DirBuilder::new().mode(0o700).create(path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(e),
        }
        let m = fs::symlink_metadata(path)?;
        if !m.is_dir() || m.mode() & 0o077 != 0 {
            return Err(std::io::Error::other("quota directory permissions"));
        }
        let root = fs::canonicalize(path)?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(root.join("LOCK"))?;
        lock.try_lock()
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        let lock = WriterLock(lock);
        let state = match OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
            .open(root.join("STATE"))
        {
            Ok(file) => {
                let m = file.metadata()?;
                if !m.is_file() || m.len() > MAX_BYTES as u64 {
                    return Err(std::io::Error::other("quota state size/type"));
                }
                let mut b = Vec::new();
                file.try_clone()?
                    .take(MAX_BYTES as u64 + 1)
                    .read_to_end(&mut b)?;
                let state = decode(&b).map_err(|_| std::io::Error::other("quota state format"))?;
                file.sync_all()?;
                state
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Snapshot::default(),
            Err(e) => return Err(e),
        };
        lock.0.sync_all()?;
        File::open(&root)?.sync_all()?;
        File::open(root.parent().unwrap_or(Path::new("/")))?.sync_all()?;
        Ok(Self {
            root,
            _lock: lock,
            state,
            poisoned: false,
        })
    }
}
impl QuotaStore for FileQuotaStore {
    fn load(&mut self) -> Result<Snapshot> {
        if self.poisoned {
            Err(Error::Poisoned)
        } else {
            Ok(self.state.clone())
        }
    }
    fn compare_exchange(&mut self, expected: u64, next: &Snapshot) -> Result<()> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        if self.state.revision != expected || expected.checked_add(1) != Some(next.revision) {
            return Err(Error::Store);
        }
        let bytes = encode(next)?;
        let path = self.root.join(format!(
            "STATE.pending-{}-{}",
            std::process::id(),
            next.revision
        ));
        let result = (|| -> std::io::Result<()> {
            let mut file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .mode(0o600)
                .open(&path)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            fs::rename(&path, self.root.join("STATE"))?;
            File::open(&self.root)?.sync_all()?;
            Ok(())
        })();
        if result.is_err() {
            self.poisoned = true;
            return Err(Error::Store);
        }
        self.state = next.clone();
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT: AtomicU64 = AtomicU64::new(1);

    struct Temp(PathBuf);
    impl Temp {
        fn new() -> Self {
            Self(std::env::temp_dir().join(format!(
                "tickwire-quota-lock-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            )))
        }
    }
    impl Drop for Temp {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn clean_owner_drop_releases_lock_despite_inherited_descriptor() {
        let temp = Temp::new();
        let owner = FileQuotaStore::open(&temp.0).unwrap();
        // try_clone shares the open file description, like a pre-exec child.
        let inherited = owner._lock.0.try_clone().unwrap();
        assert!(FileQuotaStore::open(&temp.0).is_err());
        drop(owner);
        let replacement = FileQuotaStore::open(&temp.0)
            .expect("an unrelated inherited descriptor must not extend owner lifetime");
        drop(inherited);
        assert!(FileQuotaStore::open(&temp.0).is_err());
        drop(replacement);
        assert!(FileQuotaStore::open(&temp.0).is_ok());
    }

    #[test]
    fn snapshot_codec_preserves_closed_history_and_rejects_corruption() {
        let domain = ClockDomain::new([1; 16], 2).unwrap();
        let state = Snapshot {
            revision: 1,
            supervisor_generation: 3,
            domain: Some(domain),
            last_ns: 8,
            ..Snapshot::default()
        };
        let bytes = encode(&state).unwrap();
        assert_eq!(decode(&bytes).unwrap(), state);
        let mut bad = bytes;
        *bad.last_mut().unwrap() ^= 1;
        assert_eq!(decode(&bad), Err(Error::Store));
    }
    #[test]
    fn malformed_persisted_reservation_is_rejected() {
        let mut s = Snapshot {
            revision: 1,
            supervisor_generation: 1,
            domain: Some(ClockDomain::new([1; 16], 1).unwrap()),
            ..Snapshot::default()
        };
        s.next_grant = 1;
        assert!(encode(&s).is_err());
    }
}
