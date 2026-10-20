use super::*;

pub struct ImmutableMapping {
    inner: Mapping,
}
impl ImmutableMapping {
    /// Initialize a private file without a writable mapping, then seal it and
    /// retain only an O_RDONLY descriptor and read-only shared mapping.
    pub fn create(name: &str, bytes: &[u8]) -> Result<Self> {
        platform::check()?;
        validate_length(bytes.len())?;
        let fd = platform::create(name, bytes.len())?;
        os::write_all(&fd, bytes)?;
        os::seal(&fd)?;
        Self::map(fd, bytes.len())
    }
    /// Consume a descriptor on success and failure. A successful map proves
    /// physical immutability, not Tickwire identity or JOIN admission.
    pub fn map(fd: OwnedFd, expected_len: usize) -> Result<Self> {
        platform::check()?;
        validate_length(expected_len)?;
        platform::check_file(&fd, expected_len)?;
        os::check_seals(&fd)?;
        let fd = os::readonly(fd)?;
        // map checks the length again after observing locked GROW/SHRINK seals.
        Ok(Self {
            inner: platform::map(fd, expected_len, false)?,
        })
    }
    pub fn len(&self) -> usize {
        self.inner.len
    }
    pub fn is_empty(&self) -> bool {
        false
    }
    pub fn bytes(&self) -> &[u8] {
        // SAFETY: exact locked F_SEAL_WRITE/GROW/SHRINK prohibit all conflicting
        // file modifications. The reference cannot outlive this owned mapping.
        unsafe { std::slice::from_raw_parts(self.inner.ptr.as_ptr(), self.inner.len) }
    }
    pub fn read_bytes(&self, offset: usize, len: usize) -> Result<&[u8]> {
        self.inner.check_range(offset, len)?;
        Ok(&self.bytes()[offset..offset + len])
    }
    /// Every exported descriptor is O_RDONLY and atomically duplicated CLOEXEC.
    pub fn duplicate_fd(&self) -> Result<OwnedFd> {
        self.inner.duplicate_fd()
    }
    /// Expected identity/hash must come from authenticated external metadata.
    /// This validates the resource envelope; snapshot business validators still
    /// check every section, member, internal padding range and JOIN binding.
    pub fn validate_resource(
        self,
        expected: &tickwire_ipc::ResourceHeader,
        content_bytes: usize,
        object_hash: [u8; 32],
    ) -> Result<ImmutableResource> {
        resource::validate(self, expected, content_bytes, object_hash)
    }
}

#[cfg(target_os = "linux")]
mod os {
    use super::*;
    use std::ffi::CString;
    use std::os::fd::{AsRawFd, FromRawFd};

    fn system(operation: &'static str) -> Error {
        Error::System {
            operation,
            source: io::Error::last_os_error(),
        }
    }
    pub fn write_all(fd: &OwnedFd, bytes: &[u8]) -> Result<()> {
        let mut done = 0;
        while done < bytes.len() {
            // SAFETY: private writable memfd, valid remaining source bytes;
            // caller checked the complete length fits isize/off_t.
            let written = unsafe {
                libc::pwrite(
                    fd.as_raw_fd(),
                    bytes[done..].as_ptr().cast(),
                    bytes.len() - done,
                    done as libc::off_t,
                )
            };
            if written < 0 {
                if io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(system("initialize immutable memfd"));
            }
            if written == 0 {
                return Err(Error::System {
                    operation: "initialize immutable memfd",
                    source: io::Error::from(io::ErrorKind::WriteZero),
                });
            }
            done += written as usize;
        }
        Ok(())
    }
    pub fn seal(fd: &OwnedFd) -> Result<()> {
        // SAFETY: uniquely held writable descriptor; this constructor creates
        // no writable mapping and has not exported any descriptor.
        if unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_ADD_SEALS, IMMUTABLE_SEALS) } != 0 {
            return Err(system("seal immutable memfd"));
        }
        check_seals(fd)
    }
    pub fn check_seals(fd: &OwnedFd) -> Result<()> {
        // SAFETY: read-only query on a live descriptor.
        let actual = unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_GET_SEALS) };
        if actual < 0 {
            return Err(system("immutable F_GET_SEALS"));
        }
        if actual != IMMUTABLE_SEALS {
            return Err(Error::WrongSeals { actual });
        }
        Ok(())
    }
    pub fn readonly(fd: OwnedFd) -> Result<OwnedFd> {
        // SAFETY: read-only query on a live descriptor.
        let flags = unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_GETFL) };
        if flags < 0 {
            return Err(system("immutable F_GETFL"));
        }
        match flags & libc::O_ACCMODE {
            libc::O_RDONLY => fd.try_clone().map_err(|source| Error::System {
                operation: "duplicate immutable readonly fd",
                source,
            }),
            libc::O_RDWR => {
                // This path resolves our still-owned descriptor, not a mutable
                // public file name. SCM_RIGHTS itself does not reduce access.
                let path = CString::new(format!("/proc/self/fd/{}", fd.as_raw_fd())).unwrap();
                // SAFETY: valid C string; fd remains live throughout open.
                let raw = unsafe { libc::open(path.as_ptr(), libc::O_RDONLY | libc::O_CLOEXEC) };
                if raw < 0 {
                    return Err(system("reopen immutable readonly fd"));
                }
                // SAFETY: successful open returned a fresh unique descriptor.
                Ok(unsafe { OwnedFd::from_raw_fd(raw) })
            }
            _ => Err(Error::InvalidLayout("immutable fd must permit reading")),
        }
    }
}

#[cfg(not(target_os = "linux"))]
mod os {
    use super::*;
    pub fn write_all(_: &OwnedFd, _: &[u8]) -> Result<()> {
        Err(Error::UnsupportedPlatform)
    }
    pub fn seal(_: &OwnedFd) -> Result<()> {
        Err(Error::UnsupportedPlatform)
    }
    pub fn check_seals(_: &OwnedFd) -> Result<()> {
        Err(Error::UnsupportedPlatform)
    }
    pub fn readonly(_: OwnedFd) -> Result<OwnedFd> {
        Err(Error::UnsupportedPlatform)
    }
}
