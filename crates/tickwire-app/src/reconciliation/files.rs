use std::{fs, io::Read, path::Path};
/// Optional control input: open the final component without following links or
/// waiting for a FIFO peer, then validate the actual descriptor before reading.
#[cfg(unix)]
pub(super) fn read(path: &Path, maximum: usize) -> crate::Result<Option<Vec<u8>>> {
    use std::os::unix::fs::OpenOptionsExt;
    if maximum == 0 || maximum > 128 * 1024 {
        return Err(crate::invalid("income control file bound"));
    }
    let file = match fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC)
        .open(path)
    {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.len() > maximum as u64 {
        return Err(crate::invalid(
            "income control input must be a bounded regular file",
        ));
    }
    let mut bytes = Vec::new();
    file.take(maximum as u64 + 1).read_to_end(&mut bytes)?;
    if bytes.len() > maximum {
        return Err(crate::invalid("income control file too large"));
    }
    Ok(Some(bytes))
}
#[cfg(not(unix))]
pub(super) fn read(_: &Path, _: usize) -> crate::Result<Option<Vec<u8>>> {
    Err(crate::invalid(
        "income control files require Unix descriptor checks",
    ))
}
#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::{
        io::Write,
        path::PathBuf,
        sync::{
            atomic::{AtomicU64, Ordering},
            mpsc,
        },
        thread,
        time::Duration,
    };
    struct Directory(PathBuf);
    impl Directory {
        fn new() -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let path = std::env::temp_dir().join(format!(
                "tickwire-income-files-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir(&path).unwrap();
            Self(path)
        }
    }
    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    #[test]
    fn control_file_reader_rejects_real_fifo_without_waiting_for_a_writer() {
        let dir = Directory::new();
        let path = dir.0.join("fifo");
        assert!(
            std::process::Command::new("mkfifo")
                .arg(&path)
                .status()
                .unwrap()
                .success()
        );
        let reader = path.clone();
        let (tx, rx) = mpsc::channel();
        let task = thread::spawn(move || {
            tx.send(read(&reader, 4096).is_err()).unwrap();
        });
        let before_writer = rx.recv_timeout(Duration::from_secs(1));
        if before_writer.is_err() {
            // Release the old blocking implementation before asserting. This is
            // controlled reproduction, not a leaked blocked test thread.
            let mut writer = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap();
            writer.write_all(b"{}").unwrap();
            drop(writer);
            rx.recv_timeout(Duration::from_secs(1)).unwrap();
        }
        task.join().unwrap();
        assert_eq!(before_writer, Ok(true));
    }
    #[test]
    fn control_file_reader_rejects_symlink_instead_of_following_it() {
        let dir = Directory::new();
        let path = dir.0.join("approval");
        fs::write(&path, b"{}").unwrap();
        let link = dir.0.join("link");
        std::os::unix::fs::symlink(path, &link).unwrap();
        assert!(read(&link, 4096).is_err());
    }
    #[test]
    fn control_file_reader_requires_regular_bounded_bytes_but_missing_is_optional() {
        let dir = Directory::new();
        assert!(read(&dir.0, 4096).is_err());
        let path = dir.0.join("approval");
        assert_eq!(read(&path, 4).unwrap(), None);
        fs::write(&path, b"12345").unwrap();
        assert!(read(&path, 4).is_err());
        fs::write(&path, b"1234").unwrap();
        assert_eq!(read(&path, 4).unwrap(), Some(b"1234".to_vec()));
    }
}
