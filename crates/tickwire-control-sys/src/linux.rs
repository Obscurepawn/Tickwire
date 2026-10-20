use super::*;
use std::ffi::CString;
use std::mem::{size_of, zeroed};
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, RawFd};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::time::Instant;

fn system(operation: &'static str) -> Error {
    Error::System {
        operation,
        source: io::Error::last_os_error(),
    }
}
fn owned(fd: RawFd, operation: &'static str) -> Result<OwnedFd> {
    if fd < 0 {
        return Err(system(operation));
    }
    // SAFETY: only called with a newly returned, uniquely owned descriptor.
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}
fn cpath(path: &Path) -> Result<CString> {
    CString::new(path.as_os_str().as_bytes()).map_err(|_| Error::InvalidPath)
}
fn directory(path: &Path) -> Result<OwnedFd> {
    let path = cpath(path)?;
    // SAFETY: the C string remains live; flags require a real directory.
    owned(
        unsafe {
            libc::open(
                path.as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            )
        },
        "open directory",
    )
}
fn stat(fd: &OwnedFd) -> Result<libc::stat> {
    // SAFETY: all-zero stat is valid writable storage for fstat.
    let mut st: libc::stat = unsafe { zeroed() };
    // SAFETY: valid descriptor and writable stat buffer.
    if unsafe { libc::fstat(fd.as_raw_fd(), &mut st) } != 0 {
        return Err(system("fstat"));
    }
    Ok(st)
}
fn same_inode(a: &libc::stat, b: &libc::stat) -> bool {
    a.st_dev == b.st_dev && a.st_ino == b.st_ino
}

/// Pins both the parent and the created inode. Never recursively removes files.
struct OwnedEntry {
    parent: OwnedFd,
    name: CString,
    pin: OwnedFd,
    is_dir: bool,
}
impl OwnedEntry {
    fn pin(parent: OwnedFd, name: CString, is_dir: bool) -> Result<Self> {
        // SAFETY: parent/name are valid; O_PATH pins the entry without following links.
        let pin = owned(
            unsafe {
                libc::openat(
                    parent.as_raw_fd(),
                    name.as_ptr(),
                    libc::O_PATH | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                )
            },
            "pin created entry",
        )?;
        Ok(Self {
            parent,
            name,
            pin,
            is_dir,
        })
    }
    fn chmod(&self, mode: u32) -> Result<()> {
        // /proc's fd link names the pinned inode even if its directory name changed.
        let path = CString::new(format!("/proc/self/fd/{}", self.pin.as_raw_fd())).unwrap();
        // SAFETY: a live pinned inode and a valid path; no process-wide umask change.
        if unsafe { libc::chmod(path.as_ptr(), mode) } != 0 {
            return Err(system("chmod owned entry"));
        }
        Ok(())
    }
}
impl Drop for OwnedEntry {
    fn drop(&mut self) {
        let Ok(ours) = stat(&self.pin) else { return };
        // SAFETY: zeroed stat is writable kernel output storage.
        let mut current: libc::stat = unsafe { zeroed() };
        // SAFETY: pinned parent, valid name, initialized output storage.
        let rc = unsafe {
            libc::fstatat(
                self.parent.as_raw_fd(),
                self.name.as_ptr(),
                &mut current,
                libc::AT_SYMLINK_NOFOLLOW,
            )
        };
        if rc == 0 && same_inode(&ours, &current) {
            // SAFETY: only the recorded name/inode in our protected parent is removed.
            // A nonempty directory is left intact. The owner serializes namespace edits.
            unsafe {
                libc::unlinkat(
                    self.parent.as_raw_fd(),
                    self.name.as_ptr(),
                    if self.is_dir { libc::AT_REMOVEDIR } else { 0 },
                )
            };
        }
    }
}

/// A 0700 directory whose entries are managed by the trusted launcher.
///
/// The caller serializes same-UID namespace edits during creation and cleanup.
/// Cleanup compares a pinned inode before unlinkat; Linux has no atomic
/// compare-and-unlink operation, so concurrent same-UID replacement is excluded.
pub struct PrivateDirectory {
    path: PathBuf,
    _entry: OwnedEntry,
}
impl PrivateDirectory {
    pub fn create_in(parent: &Path, prefix: &str) -> Result<Self> {
        if prefix.is_empty()
            || prefix.len() > 32
            || !prefix
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
        {
            return Err(Error::InvalidPath);
        }
        let absolute = std::fs::canonicalize(parent).map_err(|source| Error::System {
            operation: "canonicalize parent",
            source,
        })?;
        let parent = directory(&absolute)?;
        let st = stat(&parent)?;
        // SAFETY: geteuid has no preconditions.
        let uid = unsafe { libc::geteuid() };
        let protected_owner = st.st_uid == uid || st.st_uid == 0;
        let protected_names = st.st_mode & 0o022 == 0 || st.st_mode & libc::S_ISVTX != 0;
        // A writable non-sticky parent lets other users replace the new directory
        // between mkdirat and the O_PATH pin. Sticky parents must have a trusted owner.
        if !protected_owner || !protected_names {
            return Err(Error::InsecureDirectory);
        }
        for _ in 0..16 {
            let mut random = [0u8; 16];
            let mut used = 0;
            while used < random.len() {
                // SAFETY: writable remaining buffer, getrandom writes at most its length.
                let n = unsafe {
                    libc::getrandom(random[used..].as_mut_ptr().cast(), random.len() - used, 0)
                };
                if n < 0 {
                    if io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
                        continue;
                    }
                    return Err(system("getrandom"));
                }
                if n == 0 {
                    return Err(Error::Ancillary);
                }
                used += n as usize;
            }
            let suffix = random
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>();
            let name = CString::new(format!("{prefix}-{suffix}")).unwrap();
            // SAFETY: parent is a directory, name is a single safe component.
            let rc = unsafe { libc::mkdirat(parent.as_raw_fd(), name.as_ptr(), 0o700) };
            if rc != 0 {
                if io::Error::last_os_error().kind() == io::ErrorKind::AlreadyExists {
                    continue;
                }
                return Err(system("mkdirat private directory"));
            }
            let path = absolute.join(name.to_str().unwrap());
            let entry = OwnedEntry::pin(parent, name, true)?;
            entry.chmod(0o700)?;
            return Ok(Self {
                path,
                _entry: entry,
            });
        }
        Err(Error::InvalidPath)
    }
    pub fn path(&self) -> &Path {
        &self.path
    }
}

fn address(path: &Path) -> Result<(libc::sockaddr_un, libc::socklen_t)> {
    let bytes = path.as_os_str().as_bytes();
    if !path.is_absolute() || bytes.is_empty() || bytes.len() >= 108 || bytes.contains(&0) {
        return Err(Error::InvalidPath);
    }
    // SAFETY: sockaddr_un is a plain C record; all-zero bytes are valid.
    let mut address: libc::sockaddr_un = unsafe { zeroed() };
    address.sun_family = libc::AF_UNIX as libc::sa_family_t;
    for (dst, src) in address.sun_path.iter_mut().zip(bytes) {
        *dst = *src as libc::c_char;
    }
    let length = std::mem::offset_of!(libc::sockaddr_un, sun_path) + bytes.len() + 1;
    Ok((address, length as libc::socklen_t))
}
fn socket() -> Result<OwnedFd> {
    // SAFETY: socket has no pointer arguments and returns a new owned descriptor.
    owned(
        unsafe {
            libc::socket(
                libc::AF_UNIX,
                libc::SOCK_SEQPACKET | libc::SOCK_CLOEXEC | libc::SOCK_NONBLOCK,
                0,
            )
        },
        "socket seqpacket",
    )
}
fn remaining_ms(deadline: Instant) -> Result<i32> {
    let remaining = deadline
        .checked_duration_since(Instant::now())
        .ok_or(Error::Timeout)?;
    if remaining.is_zero() {
        return Err(Error::Timeout);
    }
    Ok(remaining
        .as_millis()
        .saturating_add(1)
        .min(i32::MAX as u128) as i32)
}
fn ready(fd: &OwnedFd, events: i16, deadline: Instant) -> Result<()> {
    loop {
        let timeout = remaining_ms(deadline)?;
        let mut pfd = libc::pollfd {
            fd: fd.as_raw_fd(),
            events,
            revents: 0,
        };
        // SAFETY: one initialized pollfd lives for the full syscall.
        let n = unsafe { libc::poll(&mut pfd, 1, timeout) };
        if n < 0 {
            if io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(system("poll"));
        }
        if n == 0 {
            continue;
        }
        if pfd.revents & libc::POLLNVAL != 0 {
            return Err(Error::System {
                operation: "poll invalid fd",
                source: io::Error::from_raw_os_error(libc::EBADF),
            });
        }
        if pfd.revents & (events | libc::POLLERR | libc::POLLHUP) != 0 {
            return Ok(());
        }
    }
}
fn retry_backlog(deadline: Instant) -> Result<()> {
    let timeout = remaining_ms(deadline)?.min(5);
    // SAFETY: zero descriptors permits a null pointer; bounded backlog retry delay.
    let n = unsafe { libc::poll(std::ptr::null_mut(), 0, timeout) };
    if n < 0 && io::Error::last_os_error().kind() != io::ErrorKind::Interrupted {
        return Err(system("poll connection backlog"));
    }
    Ok(())
}

/// A pathname listener in a directory owned by this effective UID, with no
/// group/other write permissions. The owner serializes namespace edits.
pub struct ControlListener {
    fd: OwnedFd,
    _binding: OwnedEntry,
}
impl ControlListener {
    pub fn bind(path: &Path) -> Result<Self> {
        Self::bind_with_mode(path, 0o600)
    }
    pub fn bind_with_mode(path: &Path, mode: u32) -> Result<Self> {
        address(path)?;
        if !matches!(mode, 0o600 | 0o660) {
            return Err(Error::InsecureDirectory);
        }
        let name = path.file_name().ok_or(Error::InvalidPath)?;
        let name = CString::new(name.as_bytes()).map_err(|_| Error::InvalidPath)?;
        let parent = directory(path.parent().ok_or(Error::InvalidPath)?)?;
        let st = stat(&parent)?;
        // SAFETY: geteuid has no preconditions.
        if st.st_uid != unsafe { libc::geteuid() } || st.st_mode & 0o022 != 0 {
            return Err(Error::InsecureDirectory);
        }
        let pinned = PathBuf::from(format!(
            "/proc/self/fd/{}/{}",
            parent.as_raw_fd(),
            name.to_str().map_err(|_| Error::InvalidPath)?
        ));
        let (addr, len) = address(&pinned)?;
        let fd = socket()?;
        // SAFETY: addr is a valid initialized Unix pathname address with exact length.
        if unsafe {
            libc::bind(
                fd.as_raw_fd(),
                (&addr as *const libc::sockaddr_un).cast(),
                len,
            )
        } != 0
        {
            return Err(system("bind control pathname"));
        }
        let entry = OwnedEntry::pin(parent, name, false)?;
        entry.chmod(mode)?;
        // SAFETY: fd is our successfully bound sequenced-packet socket.
        if unsafe { libc::listen(fd.as_raw_fd(), 64) } != 0 {
            return Err(system("listen"));
        }
        Ok(Self {
            fd,
            _binding: entry,
        })
    }
    pub fn accept(&self, deadline: Instant) -> Result<Connection> {
        loop {
            remaining_ms(deadline)?;
            // SAFETY: null address outputs are explicitly permitted by accept4.
            let fd = unsafe {
                libc::accept4(
                    self.fd.as_raw_fd(),
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                    libc::SOCK_CLOEXEC | libc::SOCK_NONBLOCK,
                )
            };
            if fd >= 0 {
                return Connection::from_fd(owned(fd, "accept4")?);
            }
            match io::Error::last_os_error().kind() {
                io::ErrorKind::Interrupted => continue,
                io::ErrorKind::WouldBlock => ready(&self.fd, libc::POLLIN, deadline)?,
                _ => return Err(system("accept4")),
            }
        }
    }
}
impl AsFd for ControlListener {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.fd.as_fd()
    }
}

pub struct Connection {
    fd: OwnedFd,
    peer: PeerCredentials,
}
impl Connection {
    fn from_fd(fd: OwnedFd) -> Result<Self> {
        // SAFETY: ucred is a plain C record and zero initializes writable output.
        let mut credentials: libc::ucred = unsafe { zeroed() };
        let mut len = size_of::<libc::ucred>() as libc::socklen_t;
        // SAFETY: valid connected socket and writable correctly sized output.
        if unsafe {
            libc::getsockopt(
                fd.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_PEERCRED,
                (&mut credentials as *mut libc::ucred).cast(),
                &mut len,
            )
        } != 0
        {
            return Err(system("SO_PEERCRED"));
        }
        if len as usize != size_of::<libc::ucred>() || credentials.pid <= 0 {
            return Err(Error::Ancillary);
        }
        Ok(Self {
            fd,
            peer: PeerCredentials {
                pid: credentials.pid as u32,
                uid: credentials.uid,
                gid: credentials.gid,
            },
        })
    }
    pub fn connect(path: &Path, deadline: Instant) -> Result<Self> {
        let (addr, len) = address(path)?;
        let fd = socket()?;
        loop {
            remaining_ms(deadline)?;
            // SAFETY: addr is valid for exactly len bytes; fd is our Unix socket.
            let rc = unsafe {
                libc::connect(
                    fd.as_raw_fd(),
                    (&addr as *const libc::sockaddr_un).cast(),
                    len,
                )
            };
            if rc == 0 {
                return Self::from_fd(fd);
            }
            let error = io::Error::last_os_error();
            match error.raw_os_error() {
                Some(libc::EINTR) => continue,
                Some(libc::EISCONN) => return Self::from_fd(fd),
                Some(libc::EAGAIN) => retry_backlog(deadline)?,
                Some(libc::EINPROGRESS) => {
                    ready(&fd, libc::POLLOUT, deadline)?;
                    let mut error: libc::c_int = 0;
                    let mut len = size_of::<libc::c_int>() as libc::socklen_t;
                    // SAFETY: correctly sized writable SO_ERROR output.
                    if unsafe {
                        libc::getsockopt(
                            fd.as_raw_fd(),
                            libc::SOL_SOCKET,
                            libc::SO_ERROR,
                            (&mut error as *mut libc::c_int).cast(),
                            &mut len,
                        )
                    } != 0
                    {
                        return Err(system("SO_ERROR"));
                    }
                    if error != 0 {
                        return Err(Error::System {
                            operation: "connect",
                            source: io::Error::from_raw_os_error(error),
                        });
                    }
                    return Self::from_fd(fd);
                }
                _ => {
                    return Err(Error::System {
                        operation: "connect",
                        source: error,
                    });
                }
            }
        }
    }
    pub fn peer_credentials(&self) -> PeerCredentials {
        self.peer
    }

    pub fn send(&mut self, bytes: &[u8], fds: &[BorrowedFd<'_>], deadline: Instant) -> Result<()> {
        let frame = tickwire_ipc::ControlFrame::decode(bytes).map_err(Error::Protocol)?;
        let expected = usize::from(frame.envelope.fd_count);
        if fds.len() != expected {
            return Err(Error::FdCount {
                expected,
                actual: fds.len(),
            });
        }
        if fds.len() > MAX_FDS {
            return Err(Error::Ancillary);
        }
        let mut ancillary = AncillaryBuffer::new();
        let mut iov = libc::iovec {
            iov_base: bytes.as_ptr().cast_mut().cast(),
            iov_len: bytes.len(),
        };
        // SAFETY: zero is valid for unused msghdr fields.
        let mut msg: libc::msghdr = unsafe { zeroed() };
        msg.msg_iov = &mut iov;
        msg.msg_iovlen = 1;
        if !fds.is_empty() {
            msg.msg_control = ancillary.words.as_mut_ptr().cast();
            // SAFETY: small constant count cannot overflow CMSG_SPACE arithmetic.
            msg.msg_controllen =
                unsafe { libc::CMSG_SPACE((fds.len() * size_of::<RawFd>()) as u32) } as usize;
            // SAFETY: control storage is cmsghdr-aligned and has room for header/data.
            unsafe {
                let header = libc::CMSG_FIRSTHDR(&msg);
                (*header).cmsg_level = libc::SOL_SOCKET;
                (*header).cmsg_type = libc::SCM_RIGHTS;
                (*header).cmsg_len =
                    libc::CMSG_LEN((fds.len() * size_of::<RawFd>()) as u32) as usize;
                let data = libc::CMSG_DATA(header);
                for (index, fd) in fds.iter().enumerate() {
                    data.add(index * size_of::<RawFd>())
                        .cast::<RawFd>()
                        .write_unaligned(fd.as_raw_fd());
                }
            }
        }
        loop {
            remaining_ms(deadline)?;
            // SAFETY: msghdr points to live read-only payload and initialized cmsg storage.
            let n = unsafe {
                libc::sendmsg(
                    self.fd.as_raw_fd(),
                    &msg,
                    libc::MSG_DONTWAIT | libc::MSG_NOSIGNAL,
                )
            };
            if n >= 0 {
                if n as usize != bytes.len() {
                    return Err(Error::Truncated);
                }
                return Ok(());
            }
            match io::Error::last_os_error().kind() {
                io::ErrorKind::Interrupted => continue,
                io::ErrorKind::WouldBlock => ready(&self.fd, libc::POLLOUT, deadline)?,
                _ => return Err(system("sendmsg")),
            }
        }
    }
    pub fn recv(&mut self, deadline: Instant) -> Result<ReceivedFrame> {
        let mut bytes = vec![0; MAX_FRAME_BYTES];
        loop {
            remaining_ms(deadline)?;
            let mut ancillary = AncillaryBuffer::new();
            let mut iov = libc::iovec {
                iov_base: bytes.as_mut_ptr().cast(),
                iov_len: bytes.len(),
            };
            // SAFETY: zero is valid for unused msghdr fields.
            let mut msg: libc::msghdr = unsafe { zeroed() };
            msg.msg_iov = &mut iov;
            msg.msg_iovlen = 1;
            msg.msg_control = ancillary.words.as_mut_ptr().cast();
            msg.msg_controllen = ancillary.bytes();
            // SAFETY: all output buffers are writable and live; kernel atomically applies CLOEXEC.
            let n = unsafe {
                libc::recvmsg(
                    self.fd.as_raw_fd(),
                    &mut msg,
                    libc::MSG_DONTWAIT | libc::MSG_CMSG_CLOEXEC,
                )
            };
            if n < 0 {
                match io::Error::last_os_error().kind() {
                    io::ErrorKind::Interrupted => continue,
                    io::ErrorKind::WouldBlock => {
                        ready(&self.fd, libc::POLLIN, deadline)?;
                        continue;
                    }
                    _ => return Err(system("recvmsg")),
                }
            }
            // This must precede every payload/flag rejection: all delivered FDs are owned.
            let (fds, malformed) = ancillary.take_fds(msg.msg_controllen)?;
            if msg.msg_flags & (libc::MSG_TRUNC | libc::MSG_CTRUNC) != 0 {
                return Err(Error::Truncated);
            }
            if malformed {
                return Err(Error::Ancillary);
            }
            if n == 0 {
                return Err(Error::Closed);
            }
            bytes.truncate(n as usize);
            let frame = tickwire_ipc::ControlFrame::decode(&bytes).map_err(Error::Protocol)?;
            let expected = usize::from(frame.envelope.fd_count);
            if fds.len() != expected {
                return Err(Error::FdCount {
                    expected,
                    actual: fds.len(),
                });
            }
            return Ok(ReceivedFrame { bytes, fds });
        }
    }
}
impl AsFd for Connection {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.fd.as_fd()
    }
}

// usize storage guarantees cmsghdr alignment on supported Linux Rust targets.
struct AncillaryBuffer {
    words: Vec<usize>,
}
impl AncillaryBuffer {
    fn new() -> Self {
        // SAFETY: fixed 16-descriptor payload is small and cannot overflow.
        let bytes = unsafe { libc::CMSG_SPACE((MAX_FDS * size_of::<RawFd>()) as u32) } as usize;
        Self {
            words: vec![0; bytes.div_ceil(size_of::<usize>())],
        }
    }
    fn bytes(&self) -> usize {
        self.words.len() * size_of::<usize>()
    }
    fn take_fds(&self, length: usize) -> Result<(Vec<OwnedFd>, bool)> {
        let mut fds = Vec::new();
        let mut malformed = false;
        let mut offset = 0usize;
        if length > self.bytes() {
            return Err(Error::Ancillary);
        }
        // SAFETY: zero-byte CMSG_LEN only computes the aligned header length.
        let header_bytes = unsafe { libc::CMSG_LEN(0) } as usize;
        while offset + size_of::<libc::cmsghdr>() <= length {
            // SAFETY: checked header bounds; read_unaligned avoids relying on offset alignment.
            let header = unsafe {
                self.words
                    .as_ptr()
                    .cast::<u8>()
                    .add(offset)
                    .cast::<libc::cmsghdr>()
                    .read_unaligned()
            };
            let bytes = header.cmsg_len;
            if bytes < header_bytes || bytes > length - offset {
                return Err(Error::Ancillary);
            }
            if header.cmsg_level == libc::SOL_SOCKET && header.cmsg_type == libc::SCM_RIGHTS {
                let payload = bytes - header_bytes;
                if !payload.is_multiple_of(size_of::<RawFd>()) {
                    malformed = true;
                }
                for index in 0..payload / size_of::<RawFd>() {
                    // SAFETY: kernel-generated SCM_RIGHTS payload fits the checked cmsg.
                    let raw = unsafe {
                        self.words
                            .as_ptr()
                            .cast::<u8>()
                            .add(offset + header_bytes + index * size_of::<RawFd>())
                            .cast::<RawFd>()
                            .read_unaligned()
                    };
                    if raw < 0 {
                        malformed = true;
                        continue;
                    }
                    // SAFETY: recvmsg installs a fresh unique descriptor for each received entry.
                    fds.push(unsafe { OwnedFd::from_raw_fd(raw) });
                }
            } else {
                malformed = true;
            }
            let aligned = bytes
                .checked_add(size_of::<usize>() - 1)
                .ok_or(Error::Ancillary)?
                & !(size_of::<usize>() - 1);
            offset = offset.checked_add(aligned).ok_or(Error::Ancillary)?;
        }
        if fds.len() > MAX_FDS {
            malformed = true;
        }
        Ok((fds, malformed))
    }
}

/// Nanoseconds in this Linux time namespace's CLOCK_MONOTONIC domain.
/// Cross-process deadlines require the same host boot and time namespace.
pub fn monotonic_ns() -> Result<u64> {
    // SAFETY: timespec zero initialization is valid writable output storage.
    let mut time: libc::timespec = unsafe { zeroed() };
    // SAFETY: CLOCK_MONOTONIC is a valid clock and time points to writable storage.
    if unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut time) } != 0 {
        return Err(system("clock_gettime"));
    }
    if time.tv_sec < 0 || !(0..1_000_000_000).contains(&time.tv_nsec) {
        return Err(Error::Ancillary);
    }
    (time.tv_sec as u64)
        .checked_mul(1_000_000_000)
        .and_then(|n| n.checked_add(time.tv_nsec as u64))
        .ok_or(Error::Ancillary)
}
