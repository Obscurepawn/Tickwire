//! Linux executable snapshots. Callers must authenticate the sealed content hash.
use crate::{Error, Result};
use std::{
    fs::{File, OpenOptions, Permissions},
    io::{self, Read, Write},
    os::{
        fd::{AsRawFd, FromRawFd, OwnedFd},
        unix::{
            fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
            process::CommandExt,
        },
    },
    path::{Path, PathBuf},
    process::{Child, Command},
};

const MAX_IMAGE_BYTES: u64 = 512 * 1024 * 1024;
const IMMUTABLE: i32 =
    libc::F_SEAL_WRITE | libc::F_SEAL_GROW | libc::F_SEAL_SHRINK | libc::F_SEAL_SEAL;
/// A bounded ELF snapshot whose contents cannot change before or after exec.
/// This pins the main ELF only, not its interpreter or shared libraries.
pub struct ExecutableImage {
    file: File,
    original: PathBuf,
    length: u64,
    device: u64,
    inode: u64,
}
impl ExecutableImage {
    pub fn open(path: &Path, max_bytes: u64) -> Result<Self> {
        if !path.is_absolute() || !(1..=MAX_IMAGE_BYTES).contains(&max_bytes) {
            return Err(Error::InvalidExecutable);
        }
        let source = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
            .open(path)
            .map_err(|e| system("open executable source", e))?;
        let before = source
            .metadata()
            .map_err(|e| system("stat executable source", e))?;
        if !before.is_file()
            || before.len() < 52
            || before.len() > max_bytes
            || before.mode() & 0o111 == 0
        {
            return Err(Error::InvalidExecutable);
        }
        let mut writable = create()?;
        copy(source, &mut writable, before.len(), max_bytes)?;
        writable
            .set_permissions(Permissions::from_mode(0o555))
            .map_err(|e| system("chmod executable image", e))?;
        // F_SEAL_EXEC appeared with explicit executable memfds. Older kernels
        // reject only that optional mode seal; immutable content seals remain mandatory.
        let flags = IMMUTABLE | libc::F_SEAL_EXEC;
        // SAFETY: live owned descriptor, fcntl takes an integer seal mask.
        let rc = unsafe { libc::fcntl(writable.as_raw_fd(), libc::F_ADD_SEALS, flags) };
        if rc < 0 {
            let error = io::Error::last_os_error();
            if error.raw_os_error() != Some(libc::EINVAL) {
                return Err(system("seal executable image", error));
            }
            // SAFETY: same live descriptor; failed F_ADD_SEALS made no changes.
            if unsafe { libc::fcntl(writable.as_raw_fd(), libc::F_ADD_SEALS, IMMUTABLE) } < 0 {
                return Err(system("seal executable image", io::Error::last_os_error()));
            }
        }
        check_seals(&writable)?;
        // Close the writable file description before exec (ETXTBSY). This proc
        // path refers to our owned descriptor, never the mutable source path.
        let file = read_descriptor(&writable)?;
        drop(writable);
        let metadata = file
            .metadata()
            .map_err(|e| system("stat executable image", e))?;
        if metadata.len() != before.len() {
            return Err(Error::InvalidExecutable);
        }
        Ok(Self {
            file,
            original: path.to_owned(),
            length: metadata.len(),
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }
    pub fn len(&self) -> u64 {
        self.length
    }
    pub fn is_empty(&self) -> bool {
        self.length == 0
    }
    /// An independent read-only description at offset zero, also CLOEXEC.
    pub fn reader(&self) -> Result<File> {
        read_descriptor(&self.file)
    }
    /// Executes the fixed descriptor, preserving the approved source as argv[0].
    /// The callback may set arguments/environment/credentials/stdio, but replacing
    /// Command's program is rejected before spawning any child.
    pub fn spawn(&self, configure: impl FnOnce(&mut Command)) -> Result<Child> {
        self.configured_command(configure)?
            .spawn()
            .map_err(|e| system("spawn executable image", e))
    }
    pub(crate) fn configured_command(
        &self,
        configure: impl FnOnce(&mut Command),
    ) -> Result<Command> {
        check_seals(&self.file)?;
        let program = descriptor_path(&self.file);
        let mut command = Command::new(&program);
        command.arg0(&self.original);
        configure(&mut command);
        if command.get_program() != program.as_os_str() {
            return Err(Error::InvalidExecutable);
        }
        Ok(command)
    }
    /// Only a live-child owner may interpret this sample as process evidence.
    /// A subsequent exec is detected by another sample, not prevented here.
    pub fn matches_process(&self, pid: u32) -> Result<bool> {
        if pid == 0 {
            return Err(Error::InvalidExecutable);
        }
        let actual = File::open(format!("/proc/{pid}/exe"))
            .map_err(|e| system("open process executable", e))?;
        let metadata = actual
            .metadata()
            .map_err(|e| system("stat process executable", e))?;
        if !metadata.is_file()
            || metadata.dev() != self.device
            || metadata.ino() != self.inode
            || metadata.len() != self.length
        {
            return Ok(false);
        }
        check_seals(&actual)?;
        Ok(true)
    }
}
fn system(operation: &'static str, source: io::Error) -> Error {
    Error::System { operation, source }
}
fn create() -> Result<File> {
    let flags = libc::MFD_CLOEXEC | libc::MFD_ALLOW_SEALING | libc::MFD_EXEC;
    // SAFETY: constant NUL-terminated name and documented flag bits.
    let mut raw = unsafe { libc::memfd_create(c"tickwire-executable".as_ptr(), flags) };
    if raw < 0 {
        let error = io::Error::last_os_error();
        if error.raw_os_error() != Some(libc::EINVAL) {
            return Err(system("create executable image", error));
        }
        // Only old kernels lacking MFD_EXEC take this path. A kernel policy
        // denial is never retried with weaker flags.
        // SAFETY: same constant name, supported sealing/CLOEXEC flags.
        raw = unsafe {
            libc::memfd_create(
                c"tickwire-executable".as_ptr(),
                libc::MFD_CLOEXEC | libc::MFD_ALLOW_SEALING,
            )
        };
        if raw < 0 {
            return Err(system(
                "create executable image",
                io::Error::last_os_error(),
            ));
        }
    }
    // SAFETY: memfd_create returned a new uniquely owned descriptor above.
    let fd = unsafe { OwnedFd::from_raw_fd(raw) };
    Ok(File::from(fd))
}
fn copy(mut source: File, target: &mut File, length: u64, max_bytes: u64) -> Result<()> {
    let mut buffer = [0; 65536];
    let mut count = 0u64;
    let mut header = [0; 16];
    loop {
        let read = source
            .read(&mut buffer)
            .map_err(|e| system("read executable source", e))?;
        if read == 0 {
            break;
        }
        let next = count
            .checked_add(read as u64)
            .filter(|n| *n <= length && *n <= max_bytes)
            .ok_or(Error::InvalidExecutable)?;
        if count < 16 {
            let start = count as usize;
            let used = (16 - start).min(read);
            header[start..start + used].copy_from_slice(&buffer[..used]);
        }
        target
            .write_all(&buffer[..read])
            .map_err(|e| system("copy executable image", e))?;
        count = next;
    }
    if count != length
        || &header[..4] != b"\x7fELF"
        || !matches!(header[4], 1 | 2)
        || !matches!(header[5], 1 | 2)
        || header[6] != 1
        || (header[4] == 2 && length < 64)
    {
        return Err(Error::InvalidExecutable);
    }
    Ok(())
}
fn descriptor_path(file: &File) -> PathBuf {
    PathBuf::from(format!("/proc/self/fd/{}", file.as_raw_fd()))
}
fn read_descriptor(file: &File) -> Result<File> {
    OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_CLOEXEC)
        .open(descriptor_path(file))
        .map_err(|e| system("open sealed executable reader", e))
}
fn check_seals(file: &File) -> Result<()> {
    // SAFETY: live file descriptor; F_GET_SEALS has no pointer argument.
    let seals = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GET_SEALS) };
    if seals < 0 {
        return Err(system("read executable seals", io::Error::last_os_error()));
    }
    if seals & IMMUTABLE != IMMUTABLE {
        return Err(Error::InvalidExecutable);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn growing_source_is_bounded_by_the_opened_length_before_copy() {
        let directory =
            crate::PrivateDirectory::create_in(Path::new("/tmp"), "tw-growing-elf").unwrap();
        let path = directory.path().join("source");
        let mut bytes = vec![0; 64];
        bytes[..7].copy_from_slice(b"\x7fELF\x02\x01\x01");
        std::fs::write(&path, &bytes).unwrap();
        let source = File::open(&path).unwrap();
        let length = source.metadata().unwrap().len();
        std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(&[1; 65536])
            .unwrap();
        let mut destination = create().unwrap();
        assert!(matches!(
            copy(source, &mut destination, length, MAX_IMAGE_BYTES),
            Err(Error::InvalidExecutable)
        ));
        assert!(destination.metadata().unwrap().len() <= length);
        std::fs::remove_file(path).unwrap();
    }
}
