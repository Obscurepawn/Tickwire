use std::{
    fs::{self, File, Metadata, OpenOptions},
    io::{self, Read, Write},
    os::{
        fd::AsRawFd,
        unix::fs::{MetadataExt, OpenOptionsExt},
    },
    path::{Path, PathBuf},
};
const FLAGS: i32 = libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC;
fn bad() -> io::Error {
    io::Error::other("observation file identity, permissions or bound")
}
fn identity(m: &Metadata) -> (u64, u64) {
    (m.dev(), m.ino())
}
pub(super) struct Directory {
    path: PathBuf,
    file: File,
    uid: u32,
}
impl Directory {
    pub fn open(path: &Path, uid: u32) -> io::Result<Self> {
        if !path.is_absolute() || path.canonicalize()? != path {
            return Err(bad());
        }
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(FLAGS | libc::O_DIRECTORY)
            .open(path)?;
        let value = Self {
            path: path.to_owned(),
            file,
            uid,
        };
        value.check()?;
        Ok(value)
    }
    pub fn check(&self) -> io::Result<()> {
        let m = self.file.metadata()?;
        let actual = fs::symlink_metadata(&self.path)?;
        if !m.is_dir()
            || !actual.is_dir()
            || identity(&m) != identity(&actual)
            || m.uid() != self.uid
            || m.mode() & 0o7777 != 0o700
        {
            return Err(bad());
        }
        Ok(())
    }
    fn path(&self, name: &str) -> io::Result<PathBuf> {
        if name.is_empty() || name.len() > 128 || name.contains('/') || name == "." || name == ".."
        {
            return Err(bad());
        }
        self.check()?;
        Ok(PathBuf::from(format!("/proc/self/fd/{}", self.file.as_raw_fd())).join(name))
    }
    pub fn validate(&self, file: &File, max: u64) -> io::Result<Metadata> {
        let m = file.metadata()?;
        if !m.is_file()
            || m.uid() != self.uid
            || m.mode() & 0o7777 != 0o600
            || m.nlink() != 1
            || m.len() > max
        {
            return Err(bad());
        }
        Ok(m)
    }
    pub fn open_file(&self, name: &str, write: bool, new: bool) -> io::Result<File> {
        let f = OpenOptions::new()
            .read(true)
            .write(write)
            .create_new(new)
            .mode(0o600)
            .custom_flags(FLAGS)
            .open(self.path(name)?)?;
        self.validate(&f, super::MAX_TOTAL)?;
        Ok(f)
    }
    pub fn check_named(&self, name: &str, held: &File) -> io::Result<()> {
        let before = self.validate(held, super::MAX_TOTAL)?;
        let current = self.open_file(name, false, false)?;
        let actual = self.validate(&current, super::MAX_TOTAL)?;
        if identity(&before) != identity(&actual) {
            return Err(bad());
        }
        Ok(())
    }
    pub fn read(&self, name: &str, max: usize) -> io::Result<Option<Vec<u8>>> {
        let mut f = match self.open_file(name, false, false) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };
        let before = self.validate(&f, max as u64)?;
        let mut bytes = Vec::new();
        Read::by_ref(&mut f)
            .take(max as u64 + 1)
            .read_to_end(&mut bytes)?;
        let after = self.validate(&f, max as u64)?;
        let actual = fs::symlink_metadata(self.path(name)?)?;
        if bytes.len() > max
            || before.len() != bytes.len() as u64
            || identity(&before) != identity(&actual)
            || before.ctime() != after.ctime()
            || before.ctime_nsec() != after.ctime_nsec()
        {
            return Err(bad());
        }
        Ok(Some(bytes))
    }
    pub fn names(&self, limit: usize) -> io::Result<Vec<String>> {
        self.check()?;
        let mut out = Vec::new();
        for entry in fs::read_dir(format!("/proc/self/fd/{}", self.file.as_raw_fd()))? {
            if out.len() == limit {
                return Err(bad());
            }
            out.push(entry?.file_name().into_string().map_err(|_| bad())?);
        }
        out.sort();
        Ok(out)
    }
    pub fn usage(&self) -> io::Result<u64> {
        let mut total = 0u64;
        for name in self.names(4096)? {
            let f = self.open_file(&name, false, false)?;
            total = total.checked_add(f.metadata()?.len()).ok_or_else(bad)?;
            if total > super::MAX_TOTAL {
                return Err(bad());
            }
        }
        Ok(total)
    }
    pub fn reserve(&self, additional: u64) -> io::Result<()> {
        if self
            .usage()?
            .checked_add(additional)
            .is_none_or(|n| n > super::MAX_TOTAL)
        {
            return Err(bad());
        }
        Ok(())
    }
    pub fn sync(&self) -> io::Result<()> {
        self.check()?;
        self.file.sync_all()
    }
    pub fn replace(&self, name: &str, bytes: &[u8]) -> io::Result<()> {
        self.replace_bounded(name, bytes, super::MAX_RESULT)
    }
    pub fn replace_bounded(&self, name: &str, bytes: &[u8], max: usize) -> io::Result<()> {
        if bytes.len() > max {
            return Err(bad());
        }
        self.reserve(bytes.len() as u64)?;
        // A crash may leave this private temporary file; it is never a committed object.
        let pending = format!("pending-{name}");
        if self.read(&pending, max)?.is_some() {
            fs::remove_file(self.path(&pending)?)?;
        }
        self.read(name, max)?;
        let mut file = self.open_file(&pending, true, true)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        fs::rename(self.path(&pending)?, self.path(name)?)?;
        self.sync()
    }
    pub fn put(&self, name: &str, bytes: &[u8]) -> io::Result<()> {
        self.put_bounded(name, bytes, super::MAX_RESULT)
    }
    pub fn put_bounded(&self, name: &str, bytes: &[u8], max: usize) -> io::Result<()> {
        if bytes.is_empty() || bytes.len() > max {
            return Err(bad());
        }
        if let Some(old) = self.read(name, max)? {
            if old != bytes {
                return Err(bad());
            }
            self.open_file(name, false, false)?.sync_all()?;
            return self.sync();
        }
        self.replace_bounded(name, bytes, max)
    }
}
