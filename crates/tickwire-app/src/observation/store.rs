use super::*;
use std::{
    fs::File,
    io::{self, Seek, SeekFrom, Write},
};
use tickwire_journal::{Journal, store::Store};
struct Wal {
    dir: files::Directory,
    lock: File,
}
impl Drop for Wal {
    fn drop(&mut self) {
        // The sole writer owns release. Other descriptors are validation or
        // fork/exec copies, not additional qualified WAL owners.
        let _ = self.lock.unlock();
    }
}
impl Wal {
    fn check(&self) -> io::Result<()> {
        self.dir.check_named("LOCK", &self.lock)
    }
}
impl Store for Wal {
    fn read_manifest(&mut self) -> io::Result<Option<Vec<u8>>> {
        self.check()?;
        self.dir.read("MANIFEST.json", 65536)
    }
    fn publish_manifest(&mut self, b: &[u8]) -> io::Result<()> {
        self.check()?;
        self.dir.replace("MANIFEST.json", b)
    }
    fn segment_ids(&mut self) -> io::Result<Vec<u64>> {
        self.check()?;
        let mut ids = Vec::new();
        for n in self.dir.names(4096)? {
            if n.starts_with("segment-") || n.ends_with(".wal") {
                let raw = n
                    .strip_prefix("segment-")
                    .and_then(|v| v.strip_suffix(".wal"))
                    .filter(|s| s.len() == 20 && s.bytes().all(|v| v.is_ascii_digit()))
                    .ok_or_else(|| io::Error::other("observation segment name"))?;
                let id = raw.parse::<u64>().map_err(io::Error::other)?;
                if id == 0 {
                    return Err(io::Error::other("zero segment"));
                }
                self.dir.open_file(&n, false, false)?;
                ids.push(id);
            }
        }
        ids.sort_unstable();
        Ok(ids)
    }
    fn read_segment(&mut self, id: u64, max: u64) -> io::Result<Vec<u8>> {
        self.check()?;
        self.dir
            .read(&segment(id), max.min(MAX_TOTAL) as usize)?
            .ok_or_else(|| io::Error::other("missing observation segment"))
    }
    fn create_segment(&mut self, id: u64) -> io::Result<()> {
        self.check()?;
        self.dir.open_file(&segment(id), true, true)?.sync_all()?;
        self.dir.sync()
    }
    fn append_segment(&mut self, id: u64, offset: u64, b: &[u8]) -> io::Result<()> {
        self.check()?;
        self.dir.reserve(b.len() as u64)?;
        let mut f = self.dir.open_file(&segment(id), true, false)?;
        if f.metadata()?.len() != offset {
            return Err(io::Error::other("observation WAL position"));
        }
        f.seek(SeekFrom::Start(offset))?;
        f.write_all(b)
    }
    fn sync_segment(&mut self, id: u64) -> io::Result<()> {
        self.check()?;
        self.dir.open_file(&segment(id), true, false)?.sync_data()
    }
    fn truncate_segment(&mut self, id: u64, len: u64) -> io::Result<()> {
        self.check()?;
        self.dir.open_file(&segment(id), true, false)?.set_len(len)
    }
}
fn segment(id: u64) -> String {
    format!("segment-{id:020}.wal")
}
pub(super) fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|v| format!("{v:02x}")).collect()
}
pub(super) struct Storage {
    journal: Journal<Wal>,
    dir: files::Directory,
    lock: File,
    epoch: u64,
    records: usize,
}
impl Storage {
    pub fn open(config: &Config, binding: &Binding) -> Result<(Self, Vec<Record>)> {
        let dir = files::Directory::open(&config.journal_directory, config.owner_uid)?;
        let lock = match dir.open_file("LOCK", true, true) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                dir.open_file("LOCK", true, false)?
            }
            Err(e) => return Err(e.into()),
        };
        lock.try_lock().map_err(io::Error::other)?;
        // Install the release guard before any fallible initialization step.
        let wal = Wal { dir, lock };
        wal.lock.sync_all()?;
        wal.dir.sync()?;
        wal.dir.reserve(0)?;
        wal.check()?;
        let retained_lock = wal.lock.try_clone()?;
        let expected = tickwire_journal::Config {
            stream_epoch: config.stream_epoch,
            domain: binding.scope.risk_domain_id,
            segment_max_bytes: 8 * 1024 * 1024,
        };
        let (journal, records) = if config.recover {
            let (j, recovery) = Journal::recover(wal, None)?;
            if j.config() != &expected || recovery.records.len() > MAX_RECORDS {
                return Err(invalid("observation WAL identity or count"));
            }
            let rows = recovery
                .records
                .iter()
                .enumerate()
                .map(|(i, r)| {
                    if r.seq != i as u64 + 1
                        || r.exec_epoch != binding.writer_epoch
                        || r.payload.len() > MAX_REQUEST + 1024
                    {
                        return Err(invalid("observation record identity or bound"));
                    }
                    Ok(serde_json::from_slice(&r.payload)?)
                })
                .collect::<Result<Vec<_>>>()?;
            (j, rows)
        } else {
            (Journal::create(wal, expected)?, Vec::new())
        };
        let dir = files::Directory::open(&config.journal_directory, config.owner_uid)?;
        dir.reserve(0)?;
        Ok((
            Self {
                journal,
                dir,
                lock: retained_lock,
                epoch: binding.writer_epoch,
                records: records.len(),
            },
            records,
        ))
    }
    pub fn record(&mut self, r: &Record) -> Result<()> {
        self.check()?;
        let b = serde_json::to_vec(r)?;
        if b.len() > MAX_REQUEST + 1024 || self.records >= MAX_RECORDS {
            return Err(invalid("observation WAL capacity"));
        }
        self.journal.append_batch(self.epoch, &[&b])?;
        self.records += 1;
        Ok(())
    }
    pub fn put(&self, b: &[u8]) -> Result<Object> {
        self.check()?;
        if b.is_empty() || b.len() > MAX_RESULT {
            return Err(invalid("observation result bound"));
        }
        let o = Object::of(b);
        self.dir.put(&format!("object-{}", hex(&o.digest)), b)?;
        Ok(o)
    }
    pub fn get(&self, o: Object) -> Result<Vec<u8>> {
        self.check()?;
        o.validate(MAX_RESULT as u64)?;
        let b = self
            .dir
            .read(&format!("object-{}", hex(&o.digest)), MAX_RESULT)?
            .ok_or_else(|| invalid("missing observation object"))?;
        o.verify(&b, MAX_RESULT as u64)?;
        Ok(b)
    }
    pub fn put_dependency(&self, b: &[u8]) -> Result<Object> {
        self.check()?;
        let o = Object::of(b);
        o.validate(tickwire_recovery::MAX_OBJECT_BYTES as u64)?;
        self.dir.put_bounded(
            &format!("object-{}", hex(&o.digest)),
            b,
            tickwire_recovery::MAX_OBJECT_BYTES,
        )?;
        Ok(o)
    }
    pub fn get_dependency(&self, o: Object) -> Result<Vec<u8>> {
        self.check()?;
        o.validate(tickwire_recovery::MAX_OBJECT_BYTES as u64)?;
        let b = self
            .dir
            .read(
                &format!("object-{}", hex(&o.digest)),
                tickwire_recovery::MAX_OBJECT_BYTES,
            )?
            .ok_or_else(|| invalid("missing observation dependency"))?;
        o.verify(&b, tickwire_recovery::MAX_OBJECT_BYTES as u64)?;
        Ok(b)
    }
    pub fn additional_dependencies(&self, objects: &[Object]) -> Result<u64> {
        self.check()?;
        let mut added = 0u64;
        for o in objects {
            o.validate(tickwire_recovery::MAX_OBJECT_BYTES as u64)?;
            match self.dir.read(
                &format!("object-{}", hex(&o.digest)),
                tickwire_recovery::MAX_OBJECT_BYTES,
            )? {
                Some(b) => o.verify(&b, tickwire_recovery::MAX_OBJECT_BYTES as u64)?,
                None => {
                    added = added
                        .checked_add(o.bytes)
                        .ok_or_else(|| invalid("observation dependency capacity"))?
                }
            }
        }
        Ok(added)
    }
    pub fn verify_dependencies(&self, o: &Observation) -> Result<()> {
        super::inbox::verify_dependency_set(o)?;
        for root in &o.objects {
            self.get_dependency(*root)?;
        }
        Ok(())
    }
    pub fn check(&self) -> Result<()> {
        self.dir.check_named("LOCK", &self.lock)?;
        Ok(())
    }
    pub fn usage(&self) -> Result<u64> {
        Ok(self.dir.usage()?)
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use std::os::unix::fs::MetadataExt;

    #[test]
    fn wal_owner_drop_releases_lock_despite_an_inherited_open_description() {
        let root = tickwire_control_sys::PrivateDirectory::create_in(
            std::path::Path::new("/tmp"),
            "tw-observe-lock",
        )
        .unwrap();
        let uid = std::fs::metadata("/proc/self").unwrap().uid();
        let dir = files::Directory::open(root.path(), uid).unwrap();
        let lock = dir.open_file("LOCK", true, true).unwrap();
        lock.try_lock().unwrap();
        let inherited = lock.try_clone().unwrap();
        let validation = lock.try_clone().unwrap();
        let owner = Wal { dir, lock };
        let directory = files::Directory::open(root.path(), uid).unwrap();
        let contender = directory.open_file("LOCK", true, false).unwrap();
        assert!(
            contender.try_lock().is_err(),
            "live owner remains exclusive"
        );
        drop(owner);
        // This safe clone has the same kernel open description as a pre-exec
        // inherited FD; its lifetime must not extend the actual writer lease.
        contender.try_lock().unwrap();
        drop(inherited);
        drop(validation);
        let third = directory.open_file("LOCK", true, false).unwrap();
        assert!(
            third.try_lock().is_err(),
            "dropping old copies cannot unlock the successor"
        );
        contender.unlock().unwrap();
        third.try_lock().unwrap();
        third.unlock().unwrap();
        std::fs::remove_file(root.path().join("LOCK")).unwrap();
    }
}
