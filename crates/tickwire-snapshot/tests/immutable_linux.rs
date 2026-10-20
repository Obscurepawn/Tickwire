//! Real SCM_RIGHTS transport and immutable snapshot receipt across exec.
//! The launcher fixes expected identity/hash from the compiled fixture, not the
//! received file or offer. This tests the data handoff, not HELLO/session/JOIN
//! admission; fixture timestamps use their own fixed test clock.
#![cfg(target_os = "linux")]

use serde_json::Value;
use sha2::{Digest, Sha256};
use std::os::fd::AsFd;
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::process::{Child, Command};
use std::time::{Duration, Instant};
use tickwire_control_sys::{Connection, ControlListener, PrivateDirectory};
use tickwire_ipc::{
    ControlEnvelope, ControlFrame, HelloBody, JoinBody, OfferPrefix, ResourceDescriptor,
    ResourceHeader, Wire, crc32c,
};
use tickwire_shm_sys::ImmutableMapping;
use tickwire_snapshot::{InstrumentSpec, JoinContext, MARK_FUNDING, QUOTE, TRADE};

const TEST_NOW: u64 = 1_500_000_000;

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(5)
}

fn hash(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

struct Fixture {
    bytes: Vec<u8>,
    expected: ResourceHeader,
    join: JoinBody,
}

impl Fixture {
    fn trusted() -> Self {
        let vectors: Value =
            serde_json::from_str(include_str!("../../../specs/contracts/ipc-vectors-v1.json"))
                .unwrap();
        let get = |name| {
            let hex = vectors["vectors"]
                .as_array()
                .unwrap()
                .iter()
                .find(|v| v["name"] == name)
                .unwrap()["hex"]
                .as_str()
                .unwrap();
            (0..hex.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
                .collect::<Vec<_>>()
        };
        let mut bytes = get("normalized_topic_snapshot");
        let mut expected = ResourceHeader::decode(&bytes[..4096]).unwrap();
        let mut join = JoinBody::decode(&get("join_normalized_snapshot")).unwrap();
        // Adapt the trusted fixture at launch to the actual qualified machine.
        // Neither expected value is derived from the received FD.
        expected.target_arch = if cfg!(target_arch = "x86_64") { 1 } else { 2 };
        expected.descriptor_hash = [0; 32];
        expected.encode(&mut bytes[..4096]).unwrap();
        expected.descriptor_hash = hash(&bytes[..4096]);
        expected.encode(&mut bytes[..4096]).unwrap();
        join.snapshot_hash = hash(&bytes);
        Self {
            bytes,
            expected,
            join,
        }
    }

    fn context(&self) -> JoinContext<'_> {
        const INSTRUMENTS: [InstrumentSpec; 1] = [InstrumentSpec {
            instrument_id: 1001,
            instrument_version: 5,
            required_states: QUOTE | TRADE | MARK_FUNDING,
        }];
        JoinContext {
            expected_resource: &self.expected,
            catalog_hash: [0x22; 32],
            instruments: &INSTRUMENTS,
            join: &self.join,
            now_mono_ns: TEST_NOW,
        }
    }

    fn descriptor(&self) -> ResourceDescriptor {
        ResourceDescriptor {
            resource_id: self.expected.resource_id,
            channel_id: self.expected.channel_id,
            resource_bytes: self.expected.resource_bytes,
            channel_epoch: self.expected.channel_epoch,
            ring_epoch: self.expected.ring_epoch,
            resource_kind: 8,
            access: 1,
            fd_index: 0,
            seal_profile: 2,
            header_hash: self.expected.descriptor_hash,
            writer_instance: self.expected.producer_instance,
            ..Default::default()
        }
    }

    fn offer(&self) -> Vec<u8> {
        let mut bytes = vec![0; 320];
        OfferPrefix {
            offer_id: [71; 16],
            descriptor_count: 1,
            descriptor_bytes: 128,
            required_seals: 0x0f,
            expires_mono_ns: 2_000_000_000,
            membership_version: self.join.membership_version,
        }
        .encode(&mut bytes[128..192])
        .unwrap();
        self.descriptor().encode(&mut bytes[192..]).unwrap();
        envelope(&mut bytes, 3, 1);
        bytes
    }
}

fn envelope(bytes: &mut [u8], opcode: u16, fd_count: u16) {
    ControlEnvelope {
        magic: *b"TWCTL001",
        abi_major: 1,
        header_bytes: 128,
        opcode,
        frame_bytes: bytes.len() as u32,
        flags: 1,
        fd_count,
        body_crc32c: crc32c(&bytes[128..]),
        ..Default::default()
    }
    .encode(&mut bytes[..128])
    .unwrap();
    let crc = crc32c(&bytes[..128]);
    bytes[116..120].copy_from_slice(&crc.to_le_bytes());
    ControlFrame::decode(bytes).unwrap();
}

fn barrier() -> Vec<u8> {
    let mut bytes = vec![0; 320];
    HelloBody {
        min_major: 1,
        max_major: 1,
        role: 1,
        target_arch: if cfg!(target_arch = "x86_64") { 1 } else { 2 },
        page_shift: 12,
        isolation_shift: 7,
        atomic_width: 8,
        trust_profile: 1,
        required_features: 28,
        max_frame_bytes: 65536,
        ..Default::default()
    }
    .encode(&mut bytes[128..])
    .unwrap();
    envelope(&mut bytes, 1, 0);
    bytes
}

struct ChildGuard(Child);
impl ChildGuard {
    fn finish(mut self) {
        let until = deadline();
        loop {
            if let Some(status) = self.0.try_wait().unwrap() {
                assert!(status.success(), "snapshot receiver exited: {status}");
                return;
            }
            assert!(Instant::now() < until, "snapshot receiver timed out");
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}
impl Drop for ChildGuard {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            let _ = self.0.kill();
        }
        let _ = self.0.wait();
    }
}

#[test]
fn immutable_snapshot_crosses_scm_rights_into_independent_exec() {
    let fixture = Fixture::trusted();
    let directory = PrivateDirectory::create_in(Path::new("/tmp"), "tw-snapshot").unwrap();
    let path = directory.path().join("control.sock");
    let listener = ControlListener::bind(&path).unwrap();
    let child = ChildGuard(
        Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "immutable_snapshot_receiver", "--nocapture"])
            .env("TICKWIRE_SNAPSHOT_SOCKET", &path)
            .env("TICKWIRE_SNAPSHOT_PARENT", std::process::id().to_string())
            .spawn()
            .unwrap(),
    );
    let mut connection = listener.accept(deadline()).unwrap();
    let credentials = connection.peer_credentials();
    let directory_metadata = std::fs::metadata(directory.path()).unwrap();
    assert_eq!(credentials.pid, child.0.id());
    assert_eq!(credentials.uid, directory_metadata.uid());
    assert_eq!(credentials.gid, directory_metadata.gid());

    for case in 0..3 {
        let mut bytes = fixture.bytes.clone();
        match case {
            0 => {}
            1 => bytes[8192 + 100] ^= 1, // Same resource header, wrong object hash.
            2 => bytes[48] ^= 1,         // Different resource identity.
            _ => unreachable!(),
        }
        let mapping = ImmutableMapping::create("snapshot-cross-process", &bytes).unwrap();
        let fd = mapping.duplicate_fd().unwrap();
        connection
            .send(&fixture.offer(), &[fd.as_fd()], deadline())
            .unwrap();
        drop(fd);
        drop(mapping);
        // Child validates only after all sender references to this memfd close.
        connection.send(&barrier(), &[], deadline()).unwrap();
        let response = connection.recv(deadline()).unwrap();
        assert!(response.fds().is_empty());
        assert_eq!(response.bytes(), barrier());
    }
    child.finish();
}

#[test]
fn immutable_snapshot_receiver() {
    let Some(path) = std::env::var_os("TICKWIRE_SNAPSHOT_SOCKET") else {
        return;
    };
    let parent: u32 = std::env::var("TICKWIRE_SNAPSHOT_PARENT")
        .unwrap()
        .parse()
        .unwrap();
    // Independent fixture/registry information, established before receiving.
    let fixture = Fixture::trusted();
    let mut connection = Connection::connect(Path::new(&path), deadline()).unwrap();
    assert_eq!(connection.peer_credentials().pid, parent);
    let baseline_fds = std::fs::read_dir("/proc/self/fd").unwrap().count();
    for case in 0..3 {
        let received = connection.recv(deadline()).unwrap();
        let (raw, mut fds) = received.into_parts();
        assert_eq!(raw, fixture.offer());
        assert_eq!(fds.len(), 1);
        let descriptor = ResourceDescriptor::decode(&raw[192..]).unwrap();
        assert_eq!(descriptor, fixture.descriptor());
        let fd = fds.pop().unwrap();
        drop(fds);
        let signal = connection.recv(deadline()).unwrap();
        assert_eq!(signal.bytes(), barrier());
        assert!(signal.fds().is_empty());
        let mapping = ImmutableMapping::map(fd, fixture.bytes.len()).unwrap();
        let result = mapping.validate_resource(
            &fixture.expected,
            fixture.bytes.len() - 4096,
            fixture.join.snapshot_hash,
        );
        match case {
            0 => {
                let resource = result.unwrap();
                assert_eq!(resource.bytes(), fixture.bytes);
                let snapshot =
                    tickwire_snapshot::validate(resource.bytes(), &fixture.context()).unwrap();
                assert_eq!(snapshot.check_readiness(TEST_NOW), Ok(()));
                assert_eq!(snapshot.len(), 1);
                let member = snapshot.member(0).unwrap();
                assert_eq!(member.entry.instrument_id, 1001);
                assert_eq!(member.header.sequence, 1000);
                assert_eq!(member.state_records.len(), 4 * 512);
                assert_eq!(member.depth_levels.len(), 2 * 16);
                assert_eq!(member.header.coverage_flags, 3);
            }
            1 => assert!(matches!(result, Err(tickwire_shm_sys::Error::Checksum))),
            2 => assert!(matches!(result, Err(tickwire_shm_sys::Error::Identity))),
            _ => unreachable!(),
        }
        assert_eq!(
            std::fs::read_dir("/proc/self/fd").unwrap().count(),
            baseline_fds,
            "received immutable descriptor leaked in case {case}"
        );
        assert!(
            !std::fs::read_to_string("/proc/self/maps")
                .unwrap()
                .contains("memfd:snapshot-cross-process"),
            "received immutable mapping leaked in case {case}"
        );
        connection.send(&barrier(), &[], deadline()).unwrap();
    }
}
