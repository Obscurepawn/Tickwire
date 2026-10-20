use tickwire_shm_sys::{Error, ImmutableMapping};

#[cfg(not(target_os = "linux"))]
#[test]
fn immutable_production_mapping_requires_linux() {
    assert!(matches!(
        ImmutableMapping::create("unsupported", &[0; 4096]),
        Err(Error::UnsupportedPlatform)
    ));
}

#[cfg(target_os = "linux")]
mod linux {
    use super::*;
    use sha2::{Digest, Sha256};
    use std::ffi::CString;
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    use tickwire_ipc::{DepthLevel, ResourceHeader, SnapshotHeader, Wire};
    use tickwire_shm_sys::{IMMUTABLE_SEALS, SINGLE_WRITER_SEALS};

    fn digest(bytes: &[u8]) -> [u8; 32] {
        Sha256::digest(bytes).into()
    }
    fn raw_memfd(len: usize, seals: i32) -> OwnedFd {
        // SAFETY: fresh descriptor, with sealing enabled and no exported mappings.
        let raw = unsafe {
            libc::memfd_create(
                c"immutable-test".as_ptr(),
                libc::MFD_CLOEXEC | libc::MFD_ALLOW_SEALING,
            )
        };
        assert!(raw >= 0);
        // SAFETY: successful memfd_create returned a unique owned descriptor.
        let fd = unsafe { OwnedFd::from_raw_fd(raw) };
        // SAFETY: file is still private; length fits off_t in these tests.
        assert_eq!(unsafe { libc::ftruncate(raw, len as libc::off_t) }, 0);
        if seals != 0 {
            // SAFETY: live writable FD, no existing writable mappings.
            assert_eq!(unsafe { libc::fcntl(raw, libc::F_ADD_SEALS, seals) }, 0);
        }
        fd
    }
    fn readonly_and_cloexec(fd: &OwnedFd) {
        // SAFETY: read-only queries on a live descriptor.
        assert_eq!(
            unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_GETFL) } & libc::O_ACCMODE,
            libc::O_RDONLY
        );
        assert_ne!(
            unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_GETFD) } & libc::FD_CLOEXEC,
            0
        );
        assert_eq!(
            unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_GET_SEALS) },
            IMMUTABLE_SEALS
        );
    }
    fn header(kind: u16, len: usize) -> ResourceHeader {
        ResourceHeader {
            magic: *b"TWIPC001",
            abi_major: 1,
            header_bytes: 4096,
            resource_kind: kind,
            target_arch: if cfg!(target_arch = "x86_64") { 1 } else { 2 },
            endian: 1,
            atomic_width: 8,
            page_shift: 12,
            isolation_shift: 7,
            resource_bytes: len as u64,
            resource_id: [1; 16],
            channel_id: [2; 16],
            producer_instance: [3; 16],
            consumer_instance: if kind == 9 { [4; 16] } else { [0; 16] },
            host_boot_id: [5; 16],
            namespace_id: [6; 16],
            channel_epoch: 1,
            ring_epoch: 1,
            feed_generation: 1,
            producer_generation: 1,
            catalog_version: 1,
            slot_offset: 4096,
            required_features: if kind == 9 { 0x3c } else { 0x1c },
            schema_major: 1,
            trust_profile: 1,
            clock_domain: 1,
            ..Default::default()
        }
    }
    fn put_header(raw: &mut [u8], header: &mut ResourceHeader) {
        header.descriptor_hash = [0; 32];
        header.encode(&mut raw[..4096]).unwrap();
        header.descriptor_hash = digest(&raw[..4096]);
        header.encode(&mut raw[..4096]).unwrap();
    }
    fn projection() -> (Vec<u8>, ResourceHeader, usize) {
        let content = b"risk_limit = 17\n";
        let mut raw = vec![0; 8192];
        raw[4096..4096 + content.len()].copy_from_slice(content);
        let mut header = header(9, raw.len());
        header.record_bytes = 1;
        header.capacity = content.len() as u32;
        put_header(&mut raw, &mut header);
        (raw, header, content.len())
    }
    fn depth_snapshot() -> (Vec<u8>, ResourceHeader, usize) {
        let mut raw = vec![0; 8192];
        DepthLevel {
            price_ticks: 100,
            qty_lots: 2,
        }
        .encode(&mut raw[4352..4368])
        .unwrap();
        DepthLevel {
            price_ticks: 101,
            qty_lots: 3,
        }
        .encode(&mut raw[4368..4384])
        .unwrap();
        SnapshotHeader {
            magic: *b"TWSNP001",
            schema_major: 1,
            snapshot_kind: 1,
            entry_bytes: 16,
            entry_count: 2,
            object_bytes: 8192,
            snapshot_id: [7; 16],
            channel_id: [2; 16],
            feed_generation: 1,
            sequence: 7,
            instrument_id: 1,
            instrument_version: 1,
            entry_offset: 4352,
            bid_count: 1,
            ask_count: 1,
            entries_hash: digest(&raw[4352..4384]),
            ..Default::default()
        }
        .encode(&mut raw[4096..4352])
        .unwrap();
        let mut header = header(5, raw.len());
        header.record_bytes = 16;
        header.capacity = 2;
        put_header(&mut raw, &mut header);
        (raw, header, 288)
    }

    #[test]
    fn complete_private_bytes_become_safe_readonly_mappings() {
        let raw = (0..8192).map(|i| (i % 251) as u8).collect::<Vec<_>>();
        let mapping = ImmutableMapping::create("complete-object", &raw).unwrap();
        assert_eq!(mapping.len(), raw.len());
        assert!(!mapping.is_empty());
        assert_eq!(mapping.bytes(), raw);
        assert_eq!(mapping.read_bytes(4093, 10).unwrap(), &raw[4093..4103]);
        assert!(mapping.read_bytes(8192, 0).unwrap().is_empty());
        assert!(matches!(
            mapping.read_bytes(8192, 1),
            Err(Error::OutOfBounds)
        ));
        assert!(matches!(
            mapping.read_bytes(usize::MAX, 2),
            Err(Error::OutOfBounds)
        ));
        let fd = mapping.duplicate_fd().unwrap();
        readonly_and_cloexec(&fd);
        let received = ImmutableMapping::map(fd, raw.len()).unwrap();
        assert_eq!(received.bytes(), raw);
        readonly_and_cloexec(&received.duplicate_fd().unwrap());
        drop(mapping);
        assert_eq!(received.bytes(), raw);
    }

    #[test]
    fn immutable_seals_deny_write_resize_new_writer_and_permission_upgrade() {
        let mapping = ImmutableMapping::create("immutable-kernel", &[0; 8192]).unwrap();
        let fd = mapping.duplicate_fd().unwrap();
        let path = CString::new(format!("/proc/self/fd/{}", fd.as_raw_fd())).unwrap();
        // Deliberately reopen O_RDWR to distinguish seal enforcement from O_RDONLY.
        // SAFETY: valid proc FD path; original FD stays live.
        let raw = unsafe { libc::open(path.as_ptr(), libc::O_RDWR | libc::O_CLOEXEC) };
        assert!(raw >= 0);
        // SAFETY: open returned a fresh descriptor.
        let writable_descriptor = unsafe { OwnedFd::from_raw_fd(raw) };
        // SAFETY: all operations target live objects and are expected to fail;
        // no returned pointer is dereferenced and no mutable Rust reference exists.
        unsafe {
            assert_eq!(libc::pwrite(raw, [1u8].as_ptr().cast(), 1, 4096), -1);
            assert_eq!(
                std::io::Error::last_os_error().raw_os_error(),
                Some(libc::EPERM)
            );
            assert_eq!(libc::ftruncate(raw, 4096), -1);
            assert_eq!(libc::ftruncate(raw, 12288), -1);
            assert_eq!(
                libc::fcntl(raw, libc::F_ADD_SEALS, libc::F_SEAL_FUTURE_WRITE),
                -1
            );
            let ptr = libc::mmap(
                std::ptr::null_mut(),
                8192,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                raw,
                0,
            );
            assert_eq!(ptr, libc::MAP_FAILED);
            assert_eq!(
                libc::mprotect(
                    mapping.bytes().as_ptr().cast_mut().cast(),
                    mapping.len(),
                    libc::PROT_READ | libc::PROT_WRITE
                ),
                -1
            );
        }
        // The receive path must normalize even an otherwise valid O_RDWR descriptor.
        let received = ImmutableMapping::map(writable_descriptor, 8192).unwrap();
        readonly_and_cloexec(&received.duplicate_fd().unwrap());
    }

    #[test]
    fn kernel_requires_all_shared_writable_mappings_to_end_before_write_seal() {
        let fd = raw_memfd(4096, 0);
        // SAFETY: uniquely owned private test file; no Rust references to this region.
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                4096,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd.as_raw_fd(),
                0,
            )
        };
        assert_ne!(ptr, libc::MAP_FAILED);
        // SAFETY: live descriptor; the existing mapping must prevent F_SEAL_WRITE.
        assert_eq!(
            unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_ADD_SEALS, IMMUTABLE_SEALS) },
            -1
        );
        assert_eq!(
            std::io::Error::last_os_error().raw_os_error(),
            Some(libc::EBUSY)
        );
        // SAFETY: exactly the owned mapping, with no outstanding references.
        assert_eq!(unsafe { libc::munmap(ptr, 4096) }, 0);
        // SAFETY: no shared writable mapping remains.
        assert_eq!(
            unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_ADD_SEALS, IMMUTABLE_SEALS) },
            0
        );
        assert_eq!(ImmutableMapping::map(fd, 4096).unwrap().bytes(), &[0; 4096]);
    }

    #[test]
    fn rejects_every_missing_seal_and_mutable_writer_profile() {
        for seals in [0, 0x0e, 0x0d, 0x0b, 0x07, SINGLE_WRITER_SEALS, 0x1f] {
            assert!(matches!(
                ImmutableMapping::map(raw_memfd(4096, seals), 4096),
                Err(Error::WrongSeals { actual }) if actual == seals
            ));
        }
    }

    #[test]
    fn invalid_lengths_names_and_file_types_are_rejected() {
        for raw in [vec![], vec![0; 1], vec![0; 4095], vec![0; 4097]] {
            assert!(matches!(
                ImmutableMapping::create("length", &raw),
                Err(Error::InvalidLength)
            ));
        }
        for len in [0, 1, 4095, 4097, usize::MAX] {
            assert!(matches!(
                ImmutableMapping::map(raw_memfd(4096, IMMUTABLE_SEALS), len),
                Err(Error::InvalidLength)
            ));
        }
        assert!(matches!(
            ImmutableMapping::map(raw_memfd(4096, IMMUTABLE_SEALS), 8192),
            Err(Error::LengthMismatch { .. })
        ));
        assert!(matches!(
            ImmutableMapping::map(std::fs::File::open("/tmp").unwrap().into(), 4096),
            Err(Error::WrongFileType)
        ));
        for name in ["", "bad\0name"] {
            assert!(matches!(
                ImmutableMapping::create(name, &[0; 4096]),
                Err(Error::InvalidName)
            ));
        }
        assert!(matches!(
            ImmutableMapping::create(&"x".repeat(250), &[0; 4096]),
            Err(Error::InvalidName)
        ));
    }

    #[test]
    fn resource_wrapper_binds_complete_header_object_hash_and_content() {
        for (raw, expected, content) in [projection(), depth_snapshot()] {
            let resource = ImmutableMapping::create("resource", &raw)
                .unwrap()
                .validate_resource(&expected, content, digest(&raw))
                .unwrap();
            assert_eq!(resource.header(), &expected);
            assert_eq!(resource.bytes(), raw);
            assert_eq!(resource.content(), &raw[4096..4096 + content]);
            let received = ImmutableMapping::map(resource.duplicate_fd().unwrap(), raw.len())
                .unwrap()
                .validate_resource(&expected, content, digest(&raw))
                .unwrap();
            assert_eq!(received.bytes(), resource.bytes());
            readonly_and_cloexec(&received.duplicate_fd().unwrap());
        }
    }

    #[test]
    fn identity_hash_reserved_bytes_and_zero_tail_are_independent_checks() {
        let (original, expected, content) = projection();
        let original_hash = digest(&original);
        let mut wrong_expected = expected.clone();
        wrong_expected.resource_id[0] ^= 1;
        assert!(
            ImmutableMapping::create("identity", &original)
                .unwrap()
                .validate_resource(&wrong_expected, content, original_hash)
                .is_err()
        );
        assert!(
            ImmutableMapping::create("hash", &original)
                .unwrap()
                .validate_resource(&expected, content, [0; 32])
                .is_err()
        );
        let mut payload_changed = original.clone();
        payload_changed[4096] ^= 1;
        assert!(
            ImmutableMapping::create("payload", &payload_changed)
                .unwrap()
                .validate_resource(&expected, content, original_hash)
                .is_err()
        );
        let mut nonzero_tail = original.clone();
        nonzero_tail[4096 + content] = 1;
        assert!(
            ImmutableMapping::create("padding", &nonzero_tail)
                .unwrap()
                .validate_resource(&expected, content, digest(&nonzero_tail))
                .is_err()
        );
        let mut reserved = original.clone();
        reserved[4095] = 1;
        assert!(
            ImmutableMapping::create("reserved", &reserved)
                .unwrap()
                .validate_resource(&expected, content, digest(&reserved))
                .is_err()
        );
        let mut wrong_header_hash = original.clone();
        wrong_header_hash[272] ^= 1;
        let expected_with_wrong_hash = ResourceHeader::decode(&wrong_header_hash[..4096]).unwrap();
        assert!(
            ImmutableMapping::create("header-hash", &wrong_header_hash)
                .unwrap()
                .validate_resource(
                    &expected_with_wrong_hash,
                    content,
                    digest(&wrong_header_hash)
                )
                .is_err()
        );
    }

    #[test]
    fn resource_ranges_and_immutable_shapes_are_checked_before_data_use() {
        let (original, expected, content) = projection();
        for wrong_content in [0, content - 1, content + 1, original.len(), usize::MAX] {
            assert!(
                ImmutableMapping::create("range", &original)
                    .unwrap()
                    .validate_resource(&expected, wrong_content, digest(&original))
                    .is_err()
            );
        }
        for case in 0..8 {
            let mut raw = original.clone();
            let mut header = expected.clone();
            match case {
                0 => header.resource_kind = 1,
                1 => header.producer_page_offset = 4096,
                2 => header.slot_offset = 8192,
                3 => header.arena_bytes = 4096,
                4 => header.record_bytes = 2,
                5 => header.capacity = 4 * 1024 * 1024 + 1,
                6 => header.target_arch = if header.target_arch == 1 { 2 } else { 1 },
                _ => header.channel_epoch = 1 << 63,
            }
            put_header(&mut raw, &mut header);
            assert!(
                ImmutableMapping::create("shape", &raw)
                    .unwrap()
                    .validate_resource(&header, content, digest(&raw))
                    .is_err(),
                "case {case}"
            );
        }
        let oversized = 64 * 1024 * 1024 + 4096;
        let mapping =
            ImmutableMapping::map(raw_memfd(oversized, IMMUTABLE_SEALS), oversized).unwrap();
        assert!(
            mapping
                .validate_resource(&expected, oversized - 4096, [0; 32])
                .is_err()
        );
        let (mut raw, mut header, content) = depth_snapshot();
        header.capacity = u32::MAX;
        put_header(&mut raw, &mut header);
        assert!(
            ImmutableMapping::create("entry-range", &raw)
                .unwrap()
                .validate_resource(&header, content, digest(&raw))
                .is_err()
        );
    }

    #[test]
    fn rejected_fds_and_mappings_are_closed_in_isolated_process() {
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "linux::immutable_rejection_child", "--nocapture"])
            .env("TICKWIRE_IMMUTABLE_REJECTION_CHILD", "1")
            .spawn()
            .unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                assert!(status.success());
                break;
            }
            if std::time::Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!("immutable rejection child timed out");
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
    }

    #[test]
    fn immutable_rejection_child() {
        if std::env::var_os("TICKWIRE_IMMUTABLE_REJECTION_CHILD").is_none() {
            return;
        }
        let count = || std::fs::read_dir("/proc/self/fd").unwrap().count();
        let maps = || {
            std::fs::read_to_string("/proc/self/maps")
                .unwrap()
                .lines()
                .filter(|line| line.contains("memfd:immutable-rejection"))
                .count()
        };
        let before = count();
        let (raw, expected, content) = projection();
        for _ in 0..64 {
            assert!(ImmutableMapping::map(raw_memfd(4096, SINGLE_WRITER_SEALS), 4096).is_err());
            assert_eq!(count(), before);
            assert!(ImmutableMapping::map(raw_memfd(4096, IMMUTABLE_SEALS), 8192).is_err());
            assert_eq!(count(), before);
            let mapping = ImmutableMapping::create("immutable-rejection", &raw).unwrap();
            assert_eq!(maps(), 1);
            assert!(
                mapping
                    .validate_resource(&expected, content, [0; 32])
                    .is_err()
            );
            assert_eq!(count(), before);
            assert_eq!(maps(), 0);
        }
    }
}
