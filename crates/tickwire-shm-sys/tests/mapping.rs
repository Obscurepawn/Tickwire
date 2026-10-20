use tickwire_shm_sys::{Error, WriterMapping};

#[cfg(not(target_os = "linux"))]
#[test]
fn production_mapping_rejects_non_linux() {
    assert!(matches!(
        WriterMapping::create("unsupported", 4096),
        Err(Error::UnsupportedPlatform)
    ));
}

#[cfg(target_os = "linux")]
mod linux {
    use super::*;
    use std::os::fd::AsRawFd;
    use tickwire_shm_sys::{ReaderMapping, SINGLE_WRITER_SEALS};

    #[test]
    fn writer_must_exist_before_locked_seals_and_reader_offer() {
        let mut writer = WriterMapping::create("ordered-handshake", 8192).unwrap();
        assert!(matches!(writer.duplicate_fd(), Err(Error::NotSealed)));
        // SAFETY: fresh, private mapping; no readers have been offered its FD.
        unsafe {
            writer.write_bytes(4096, b"before-seal").unwrap();
            writer.store_u64_release(128, 1).unwrap();
        }
        writer.seal_for_readers().unwrap();
        let fd = writer.duplicate_fd().unwrap();
        // SAFETY: fcntl only inspects the live owned descriptor.
        assert_eq!(
            unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_GET_SEALS) },
            SINGLE_WRITER_SEALS
        );
        let reader = ReaderMapping::map(fd, 8192).unwrap();
        // SAFETY: field 128 has only u64 atomic access; publication protects
        // bytes 4096..4107, which are not modified while this borrow exists.
        unsafe {
            assert_eq!(reader.load_u64_acquire(128).unwrap(), 1);
            assert_eq!(reader.read_bytes(4096, 11).unwrap(), b"before-seal");
            writer.write_bytes(4112, b"after-seal").unwrap();
            writer.store_u64_release(128, 2).unwrap();
            assert_eq!(reader.load_u64_acquire(128).unwrap(), 2);
            assert_eq!(reader.read_bytes(4112, 10).unwrap(), b"after-seal");
        }
        assert_eq!(reader.len(), 8192);
        assert!(!reader.is_empty());
    }

    #[test]
    fn bounds_alignment_and_lengths_fail_before_access() {
        for len in [0, 1, 4095, 4097, usize::MAX] {
            assert!(matches!(
                WriterMapping::create("bad-length", len),
                Err(Error::InvalidLength)
            ));
        }
        let mut writer = WriterMapping::create("bounds", 4096).unwrap();
        // SAFETY: only checked-invalid accesses or empty in-bounds access.
        unsafe {
            assert!(matches!(
                writer.read_bytes(usize::MAX, 2),
                Err(Error::OutOfBounds)
            ));
            assert!(matches!(
                writer.write_bytes(4095, &[1, 2]),
                Err(Error::OutOfBounds)
            ));
            assert!(matches!(writer.load_u64_acquire(1), Err(Error::Misaligned)));
            assert!(matches!(
                writer.store_u64_release(4096, 0),
                Err(Error::OutOfBounds)
            ));
            assert!(writer.read_bytes(4096, 0).unwrap().is_empty());
        }
        writer.seal_for_readers().unwrap();
        let fd = writer.duplicate_fd().unwrap();
        assert!(matches!(
            ReaderMapping::map(fd, 8192),
            Err(Error::LengthMismatch { .. })
        ));
        for name in ["", "nul\0byte"] {
            assert!(matches!(
                WriterMapping::create(name, 4096),
                Err(Error::InvalidName)
            ));
        }
        assert!(matches!(
            WriterMapping::create(&"x".repeat(250), 4096),
            Err(Error::InvalidName)
        ));
    }

    #[test]
    fn kernel_denies_new_writer_file_writes_and_resize() {
        let mut writer = WriterMapping::create("sealed", 4096).unwrap();
        writer.seal_for_readers().unwrap();
        let fd = writer.duplicate_fd().unwrap();
        let raw = fd.as_raw_fd();
        // SAFETY: valid live fd, no pointers dereferenced; all operations are
        // expected to fail under the locked sealing profile.
        unsafe {
            let map = libc::mmap(
                std::ptr::null_mut(),
                4096,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                raw,
                0,
            );
            assert_eq!(map, libc::MAP_FAILED);
            assert_eq!(libc::ftruncate(raw, 0), -1);
            assert_eq!(libc::ftruncate(raw, 8192), -1);
            assert_eq!(libc::pwrite(raw, [1u8].as_ptr().cast(), 1, 0), -1);
            assert_eq!(libc::fcntl(raw, libc::F_ADD_SEALS, libc::F_SEAL_WRITE), -1);
            assert_ne!(libc::fcntl(raw, libc::F_GETFD) & libc::FD_CLOEXEC, 0);
        }
    }

    #[test]
    fn reader_rejects_unsealed_files_and_wrong_file_types() {
        use std::os::fd::{FromRawFd, OwnedFd};
        // SAFETY: returns a new owned descriptor; no mappings/access exist.
        let raw = unsafe {
            libc::memfd_create(
                c"unsealed".as_ptr(),
                libc::MFD_CLOEXEC | libc::MFD_ALLOW_SEALING,
            )
        };
        assert!(raw >= 0);
        // SAFETY: transfer successful fresh descriptor into RAII.
        let fd = unsafe { OwnedFd::from_raw_fd(raw) };
        // SAFETY: owned unmapped file may be initialized to the required size.
        assert_eq!(unsafe { libc::ftruncate(fd.as_raw_fd(), 4096) }, 0);
        assert!(matches!(
            ReaderMapping::map(fd, 4096),
            Err(Error::WrongSeals { actual: 0 })
        ));
        let directory = std::fs::File::open("/tmp").unwrap();
        assert!(matches!(
            ReaderMapping::map(directory.into(), 4096),
            Err(Error::WrongFileType)
        ));
    }

    #[test]
    fn repeated_sealing_is_idempotent_and_creation_is_zero_filled() {
        let mut writer = WriterMapping::create("initial-zeroes", 8192).unwrap();
        // SAFETY: private newly created zero-filled mapping, no shared access.
        assert!(
            unsafe { writer.read_bytes(0, writer.len()) }
                .unwrap()
                .iter()
                .all(|&b| b == 0)
        );
        writer.seal_for_readers().unwrap();
        writer.seal_for_readers().unwrap();
        assert_eq!(writer.len(), 8192);
        assert!(!writer.is_empty());
    }

    #[test]
    fn reader_requires_every_seal_and_the_single_writer_profile() {
        use std::os::fd::{FromRawFd, OwnedFd};
        for seals in [0x16, 0x15, 0x13, 0x07, 0x0f] {
            // SAFETY: fresh fd, initialized before any mappings exist.
            let raw = unsafe {
                libc::memfd_create(
                    c"incomplete-seals".as_ptr(),
                    libc::MFD_CLOEXEC | libc::MFD_ALLOW_SEALING,
                )
            };
            assert!(raw >= 0);
            // SAFETY: fresh descriptor ownership transferred exactly once.
            let fd = unsafe { OwnedFd::from_raw_fd(raw) };
            // SAFETY: owned fd with no mappings or concurrent accesses.
            unsafe {
                assert_eq!(libc::ftruncate(fd.as_raw_fd(), 4096), 0);
                assert_eq!(libc::fcntl(fd.as_raw_fd(), libc::F_ADD_SEALS, seals), 0);
            }
            assert!(matches!(
                ReaderMapping::map(fd, 4096),
                Err(Error::WrongSeals { actual }) if actual == seals
            ));
        }
    }

    #[test]
    fn reader_sets_close_on_exec_on_received_fd() {
        let mut writer = WriterMapping::create("received-fd", 4096).unwrap();
        writer.seal_for_readers().unwrap();
        let fd = writer.duplicate_fd().unwrap();
        let raw = fd.as_raw_fd();
        // SAFETY: only changes flags of this test-owned descriptor.
        assert_eq!(unsafe { libc::fcntl(raw, libc::F_SETFD, 0) }, 0);
        let reader = ReaderMapping::map(fd, 4096).unwrap();
        // SAFETY: reader owns the live descriptor for this entire check.
        assert_ne!(
            unsafe { libc::fcntl(raw, libc::F_GETFD) } & libc::FD_CLOEXEC,
            0
        );
        drop(reader);
    }
}
