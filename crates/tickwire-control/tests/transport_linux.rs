#![cfg(target_os = "linux")]
use std::path::Path;
use std::process::{Child, Command};
use std::time::{Duration, Instant};
use tickwire_control::*;
use tickwire_control_sys::{
    Connection, ControlListener, PeerCredentials, PrivateDirectory, monotonic_ns,
};
use tickwire_ipc::{ControlEnvelope, ControlFrame, QueryBody, StatusBody, Wire};

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(5)
}
fn identity(id: u8, role: u16) -> Identity {
    Identity {
        instance: [id; 16],
        generation: 1,
        role,
        logical_strategy: if role == 3 { 7 } else { 0 },
        host_boot: [8; 16],
        namespace: [9; 16],
        build_hash: [10; 32],
        qualification_hash: [11; 32],
        manifest_hash: [12; 32],
        arch: if cfg!(target_arch = "aarch64") { 2 } else { 1 },
        trust_profile: 1,
    }
}
fn effective_ids() -> (u32, u32) {
    let status = std::fs::read_to_string("/proc/self/status").unwrap();
    let field = |key: &str| {
        status
            .lines()
            .find_map(|l| l.strip_prefix(key))
            .unwrap()
            .split_whitespace()
            .nth(1)
            .unwrap()
            .parse()
            .unwrap()
    };
    (field("Uid:"), field("Gid:"))
}
fn policy(server: bool, peer_pid: u32) -> Policy {
    let (uid, gid) = effective_ids();
    Policy {
        local: identity(if server { 1 } else { 2 }, if server { 1 } else { 3 }),
        peer: identity(if server { 2 } else { 1 }, if server { 3 } else { 1 }),
        peer_process: ProcessIdentity {
            pid: peer_pid,
            uid,
            gid,
        },
        required_features: 0x1c,
        supported_features: 0x1f,
        control_epoch: 13,
        lease_duration_ns: 3_000_000_000,
        max_frame_bytes: 65536,
        nonce: [14; 32],
    }
}
fn process(p: PeerCredentials) -> ProcessIdentity {
    ProcessIdentity {
        pid: p.pid,
        uid: p.uid,
        gid: p.gid,
    }
}
struct Running(Child);
impl Drop for Running {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
impl Running {
    fn finish(&mut self) {
        let end = deadline();
        loop {
            if let Some(status) = self.0.try_wait().unwrap() {
                assert!(status.success());
                return;
            }
            assert!(Instant::now() < end, "child timeout");
            std::thread::sleep(Duration::from_millis(2));
        }
    }
}

#[test]
fn authenticated_exec_child_retries_receive_one_saved_result() {
    let dir = PrivateDirectory::create_in(&std::env::temp_dir(), "control-session").unwrap();
    let path = dir.path().join("control.sock");
    let listener = ControlListener::bind(&path).unwrap();
    let mut child = Running(
        Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "session_child", "--ignored", "--nocapture"])
            .env("TICKWIRE_SESSION_TEST_SOCKET", &path)
            .env(
                "TICKWIRE_SESSION_TEST_PARENT",
                std::process::id().to_string(),
            )
            .spawn()
            .unwrap(),
    );
    // Holding this unreaped Child keeps the registered process identity from PID reuse.
    let p = policy(true, child.0.id());
    let mut conn = listener.accept(deadline()).unwrap();
    let hello = conn.recv(deadline()).unwrap();
    let (mut session, welcome) = Session::accept(
        &p,
        process(conn.peer_credentials()),
        hello.bytes(),
        monotonic_ns().unwrap(),
    )
    .unwrap();
    conn.send(&welcome, &[], deadline()).unwrap();
    let mut store = MockRequestStore::new(2).unwrap();
    let mut dispatches = 0;
    let mut saved = Vec::new();
    for current_version in [0, 7] {
        let request = conn.recv(deadline()).unwrap();
        let response = match session
            .admit(
                &mut store,
                request.bytes(),
                monotonic_ns().unwrap(),
                current_version,
            )
            .unwrap()
        {
            Admission::Execute(ticket) => {
                dispatches += 1;
                let mut body = vec![0; StatusBody::SIZE];
                StatusBody {
                    state_version: 1,
                    ..Default::default()
                }
                .encode(&mut body)
                .unwrap();
                let original = ControlFrame::decode(request.bytes()).unwrap().envelope;
                let response = session
                    .encode(
                        ControlEnvelope {
                            opcode: 49,
                            flags: 10,
                            ..original
                        },
                        &body,
                    )
                    .unwrap();
                ticket.complete(&mut store, &response, 1).unwrap();
                saved = response.clone();
                response
            }
            Admission::Completed(result) => {
                assert_eq!(result.response, saved);
                result.response
            }
            Admission::Pending => panic!("completed request cannot become pending"),
        };
        conn.send(&response, &[], deadline()).unwrap();
    }
    assert_eq!(dispatches, 1);
    child.finish();
}

#[test]
#[ignore = "executed only by the parent process integration test"]
fn session_child() {
    let path = std::env::var("TICKWIRE_SESSION_TEST_SOCKET").unwrap();
    let parent: u32 = std::env::var("TICKWIRE_SESSION_TEST_PARENT")
        .unwrap()
        .parse()
        .unwrap();
    let mut conn = Connection::connect(Path::new(&path), deadline()).unwrap();
    let handshake = ClientHandshake::new(policy(false, parent), [15; 16]).unwrap();
    conn.send(handshake.hello(), &[], deadline()).unwrap();
    let welcome = conn.recv(deadline()).unwrap();
    let mut session = handshake
        .finish(
            process(conn.peer_credentials()),
            welcome.bytes(),
            monotonic_ns().unwrap(),
        )
        .unwrap();
    let mut body = vec![0; QueryBody::SIZE];
    QueryBody::default().encode(&mut body).unwrap();
    let request = session
        .encode(
            ControlEnvelope {
                opcode: 48,
                flags: 1,
                request_id: [16; 16],
                ..Default::default()
            },
            &body,
        )
        .unwrap();
    let mut first = Vec::new();
    for attempt in 0..2 {
        conn.send(&request, &[], deadline()).unwrap();
        let response = conn.recv(deadline()).unwrap();
        let frame = session
            .validate(response.bytes(), monotonic_ns().unwrap())
            .unwrap();
        assert_eq!(frame.envelope.request_id, [16; 16]);
        assert_eq!(StatusBody::decode(frame.body).unwrap().state_version, 1);
        if attempt == 0 {
            first = response.bytes().to_vec();
        } else {
            assert_eq!(first, response.bytes());
        }
    }
}
