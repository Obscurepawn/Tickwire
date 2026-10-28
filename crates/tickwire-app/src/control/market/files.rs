use crate::{Result, invalid};
use std::{
    fs::{self, OpenOptions},
    io::{Read, Write},
    path::Path,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Stamp {
    length: u64,
    modified: std::time::SystemTime,
    #[cfg(unix)]
    identity: (u64, u64, i64, i64),
}
pub(super) fn stamp(path: &Path, maximum: usize) -> Result<Option<Stamp>> {
    let meta = match fs::symlink_metadata(path) {
        Ok(v) => v,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    if !meta.is_file() || meta.len() > maximum as u64 {
        return Err(invalid("market control file type or size"));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if meta.permissions().mode() & 0o777 != 0o600 {
            return Err(invalid("market control file permissions"));
        }
    }
    #[cfg(unix)]
    let identity = {
        use std::os::unix::fs::MetadataExt;
        (meta.dev(), meta.ino(), meta.ctime(), meta.ctime_nsec())
    };
    Ok(Some(Stamp {
        length: meta.len(),
        modified: meta.modified()?,
        #[cfg(unix)]
        identity,
    }))
}

pub(super) fn directory(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    if !path.is_absolute() || !metadata.is_dir() || path.canonicalize()? != path {
        return Err(invalid(
            "market control requires an absolute real directory",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o777 != 0o700 {
            return Err(invalid("market control directory must be private (0700)"));
        }
    }
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.uid() != fs::metadata("/proc/self")?.uid() {
            return Err(invalid("market control directory owner"));
        }
    }
    Ok(())
}
pub(super) fn read(path: &Path, maximum: usize) -> Result<Option<Vec<u8>>> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC);
    }
    let file = match options.open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.len() > maximum as u64 {
        return Err(invalid("market control file type or size"));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o777 != 0o600 {
            return Err(invalid("market control file must be private (0600)"));
        }
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.take(maximum as u64 + 1).read_to_end(&mut bytes)?;
    if bytes.len() > maximum {
        return Err(invalid("market control file bound"));
    }
    Ok(Some(bytes))
}
pub(super) fn replace(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| invalid("market control file parent"))?;
    directory(parent)?;
    if read(
        path,
        tickwire_market_source::transition::MAX_CONTROL_BYTES + 4096,
    )?
    .as_deref()
        == Some(bytes)
    {
        // Matching bytes may be left by a previous rename whose directory sync failed.
        let mut options = OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC);
        }
        options.open(path)?.sync_all()?;
        fs::File::open(parent)?.sync_all()?;
        return Ok(());
    }
    let pending = path.with_extension("pending");
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    }
    let mut file = options.open(&pending)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    fs::rename(&pending, path)?;
    fs::File::open(parent)?.sync_all()?;
    Ok(())
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::{
        fs,
        os::unix::fs::{PermissionsExt, symlink},
        path::PathBuf,
    };
    struct Directory(PathBuf);
    impl Directory {
        fn new() -> Self {
            static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let p = std::env::temp_dir().join(format!(
                "tickwire-market-files-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            ));
            fs::create_dir(&p).unwrap();
            fs::set_permissions(&p, fs::Permissions::from_mode(0o700)).unwrap();
            Self(p)
        }
    }
    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    #[test]
    fn private_files_are_bounded_and_receipts_replace_only_after_complete_write() {
        let d = Directory::new();
        directory(&d.0).unwrap();
        let p = d.0.join("receipt.json");
        assert!(read(&p, 100).unwrap().is_none());
        replace(&p, b"armed").unwrap();
        assert_eq!(read(&p, 100).unwrap().unwrap(), b"armed");
        replace(&p, b"activated").unwrap();
        assert_eq!(read(&p, 100).unwrap().unwrap(), b"activated");
        assert!(read(&p, 2).is_err());
        fs::write(p.with_extension("pending"), b"interrupted").unwrap();
        assert!(replace(&p, b"different").is_err());
        assert_eq!(read(&p, 100).unwrap().unwrap(), b"activated");
    }
    #[test]
    fn writable_directories_and_symlink_inputs_are_rejected() {
        let d = Directory::new();
        fs::set_permissions(&d.0, fs::Permissions::from_mode(0o755)).unwrap();
        assert!(directory(&d.0).is_err());
        fs::set_permissions(&d.0, fs::Permissions::from_mode(0o700)).unwrap();
        let real = d.0.join("real");
        fs::write(&real, b"approval").unwrap();
        let link = d.0.join("link");
        symlink(&real, &link).unwrap();
        assert!(read(&link, 100).is_err());
        assert!(replace(&link, b"forged").is_err());
        assert_eq!(fs::read(&real).unwrap(), b"approval");
    }
}
