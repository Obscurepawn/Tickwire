#![cfg(target_os = "linux")]
#![allow(unsafe_code)]
mod common;
use common::*;
use std::io::{Read, Write};
use std::os::fd::AsFd;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};
use tickwire_control::*;
use tickwire_control_sys::{
    Connection, ControlListener, PeerCredentials, PrivateDirectory, monotonic_ns,
};
use tickwire_ipc::*;
use tickwire_join::*;

fn timeout() -> Instant {
    Instant::now() + Duration::from_secs(8)
}
fn now() -> u64 {
    monotonic_ns().unwrap()
}
fn hex(raw: &[u8]) -> String {
    raw.iter().map(|b| format!("{b:02x}")).collect()
}
fn unhex(raw: &str) -> Vec<u8> {
    (0..raw.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&raw[i..i + 2], 16).unwrap())
        .collect()
}
fn wire<T: Wire>(v: &T) -> String {
    let mut bytes = vec![0; T::SIZE];
    v.encode(&mut bytes).unwrap();
    hex(&bytes)
}
fn env_wire<T: Wire>(key: &str) -> T {
    T::decode(&unhex(&std::env::var(key).unwrap())).unwrap()
}
fn ids() -> (u32, u32) {
    let raw = std::fs::read_to_string("/proc/self/status").unwrap();
    let field = |key: &str| {
        raw.lines()
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
fn process(p: PeerCredentials) -> ProcessIdentity {
    ProcessIdentity {
        pid: p.pid,
        uid: p.uid,
        gid: p.gid,
    }
}
fn real_policy(local: Identity, peer: Identity, pid: u32) -> Policy {
    let (uid, gid) = ids();
    let mut p = policy(local, peer, ProcessIdentity { pid, uid, gid }, NOW);
    p.lease_duration_ns = 30_000_000_000;
    p
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
        let deadline = timeout();
        loop {
            if let Some(status) = self.0.try_wait().unwrap() {
                assert!(status.success());
                return;
            }
            assert!(Instant::now() < deadline, "child timeout");
            std::thread::sleep(Duration::from_millis(2));
        }
    }
}
fn child_command(mode: &str, path: &Path) -> Command {
    let mut c = Command::new(std::env::current_exe().unwrap());
    c.args(["--exact", "join_child", "--ignored", "--nocapture"])
        .env("TW_JOIN_MODE", mode)
        .env("TW_JOIN_SOCKET", path)
        .env("TW_JOIN_PARENT", std::process::id().to_string());
    c
}
fn accept(listener: &ControlListener, peer: Identity, child: &Running) -> (Session, Connection) {
    let mut conn = listener.accept(timeout()).unwrap();
    assert_eq!(conn.peer_credentials().pid, child.0.id());
    assert_ne!(conn.peer_credentials().pid, std::process::id());
    let policy = real_policy(identity(3, 2), peer, child.0.id());
    let hello = conn.recv(timeout()).unwrap();
    let (session, welcome) = Session::accept(
        &policy,
        process(conn.peer_credentials()),
        hello.bytes(),
        now(),
    )
    .unwrap();
    conn.send(&welcome, &[], timeout()).unwrap();
    (session, conn)
}
fn connect(role: u16, id: u8) -> (Session, Connection) {
    let path = std::env::var("TW_JOIN_SOCKET").unwrap();
    let parent = std::env::var("TW_JOIN_PARENT").unwrap().parse().unwrap();
    let mut conn = Connection::connect(Path::new(&path), timeout()).unwrap();
    let hello = ClientHandshake::new(
        real_policy(identity(id, role), identity(3, 2), parent),
        [41; 16],
    )
    .unwrap();
    conn.send(hello.hello(), &[], timeout()).unwrap();
    let welcome = conn.recv(timeout()).unwrap();
    let session = hello
        .finish(process(conn.peer_credentials()), welcome.bytes(), now())
        .unwrap();
    (session, conn)
}

#[test]
fn supervisor_owner_and_consumer_execute_real_join_and_quiesce() {
    let directory = PrivateDirectory::create_in(&std::env::temp_dir(), "tw-join-process").unwrap();
    let path = directory.path().join("control.sock");
    let listener = ControlListener::bind(&path).unwrap();
    let (mut owner, data) = owner();
    let reg = registration(&owner, 4);
    let mut join = join_request(&owner, &reg);
    join.readiness_deadline_mono_ns = now() + 10_000_000_000;
    let mut supervisor = Running(
        child_command("supervisor", &path)
            .env("TW_JOIN_REQUEST", wire(&join))
            .stdin(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    // Both Child handles remain unreaped while their exact process IDs authorize sessions.
    let (mut admin, mut admin_conn) = accept(&listener, identity(1, 1), &supervisor);
    let request = admin_conn.recv(timeout()).unwrap();
    let mut store = MockRequestStore::new(8).unwrap();
    let prepared = now();
    let result = owner
        .prepare(
            &mut admin,
            &mut store,
            request.bytes(),
            &reg,
            [31; 16],
            prepared,
            |p| data.build_at(p, prepared, join.readiness_deadline_mono_ns),
        )
        .unwrap();
    let PrepareOutcome::Prepared { response, .. } = result else {
        panic!()
    };
    admin_conn.send(&response, &[], timeout()).unwrap();
    let repeat = admin_conn.recv(timeout()).unwrap();
    let result = owner
        .prepare(
            &mut admin,
            &mut store,
            repeat.bytes(),
            &reg,
            [31; 16],
            now(),
            |_| panic!("completed request must not rerun builder"),
        )
        .unwrap();
    let PrepareOutcome::Completed(replayed) = result else {
        panic!()
    };
    assert_eq!(response, replayed);
    admin_conn.send(&replayed, &[], timeout()).unwrap();
    let mut consumer = Running(
        child_command("consumer", &path)
            // Trusted launcher metadata is independent of, and never read from, received files.
            .env("TW_JOIN_FINAL", wire(owner.join(reg.subscription).unwrap()))
            .env("TW_JOIN_BANK", wire(owner.header()))
            .env(
                "TW_JOIN_SNAPSHOT",
                wire(owner.snapshot_header(reg.subscription).unwrap()),
            )
            .spawn()
            .unwrap(),
    );
    let (mut session, mut connection) = accept(&listener, reg.identity.clone(), &consumer);
    fill_pending(&mut owner, &data);
    owner
        .deliver(
            reg.subscription,
            &mut session,
            &mut connection,
            now(),
            timeout(),
        )
        .unwrap();
    let cursor = connection.recv(timeout()).unwrap();
    // SAFETY: this child runs the compiled Subscriber SDK; PID, full registry identity,
    // immutable descriptor, and subscription are independently checked.
    unsafe {
        owner
            .receive_cursor(reg.subscription, &mut session, cursor, now())
            .unwrap();
    }
    for _ in 0..2 {
        let ack = connection.recv(timeout()).unwrap();
        owner
            .read_ready(reg.subscription, &mut session, ack.bytes(), now())
            .unwrap();
    }
    let ready = connection.recv(timeout()).unwrap();
    assert_eq!(owner.stage(reg.subscription), Some(Stage::Delivered));
    // The child has drained/applied/released through 68. Pending still protects S=4.
    assert_full(&mut owner, &data);
    assert_eq!(
        owner
            .join_ready(reg.subscription, &mut session, ready.bytes(), now())
            .unwrap()
            .applied,
        68
    );
    assert_eq!(owner.membership_version(), 2);
    owner.publish(&[&data.record(69)]).unwrap();
    let envelope = ControlFrame::decode(ready.bytes()).unwrap().envelope;
    let status = StatusBody {
        subject_id: reg.subscription.cursor.to_le_bytes(),
        original_request_id: envelope.request_id,
        original_control_epoch: envelope.control_epoch,
        state_version: owner.membership_version(),
        membership_version: owner.membership_version(),
        ring_epoch: 1,
        published_seq: owner.published(),
        released_seq: 68,
        ..Default::default()
    };
    let mut body = vec![0; StatusBody::SIZE];
    status.encode(&mut body).unwrap();
    let ack = session
        .encode(
            ControlEnvelope {
                opcode: 49,
                flags: 10,
                ..envelope
            },
            &body,
        )
        .unwrap();
    connection.send(&ack, &[], timeout()).unwrap();
    let borrowed = connection.recv(timeout()).unwrap();
    let marker = session.validate(borrowed.bytes(), now()).unwrap();
    assert_eq!(marker.envelope.opcode, 48);
    let quiesce = owner
        .quiesce(reg.subscription, &session, [49; 16], 1)
        .unwrap();
    connection.send(&quiesce, &[], timeout()).unwrap();
    let quiesced = connection.recv(timeout()).unwrap();
    owner
        .quiesced(reg.subscription, &mut session, quiesced.bytes(), now())
        .unwrap();
    assert_eq!(owner.stage(reg.subscription), Some(Stage::Detached));
    assert_eq!(owner.membership_version(), 3);
    owner.publish(&[&data.record(70)]).unwrap();
    consumer.finish();
    supervisor
        .0
        .stdin
        .take()
        .unwrap()
        .write_all(b"done\n")
        .unwrap();
    supervisor.finish();
}

#[test]
#[ignore = "independent exec helper launched only by the integration test"]
fn join_child() {
    match std::env::var("TW_JOIN_MODE").unwrap().as_str() {
        "supervisor" => {
            let (mut session, mut connection) = connect(1, 1);
            let join: JoinBody = env_wire("TW_JOIN_REQUEST");
            let raw = request(&session, &join, 42, 1);
            let mut saved = Vec::new();
            for attempt in 0..2 {
                connection.send(&raw, &[], timeout()).unwrap();
                let received = connection.recv(timeout()).unwrap();
                let parsed = session.validate(received.bytes(), now()).unwrap();
                let result = JoinBody::decode(parsed.body).unwrap();
                assert_eq!(parsed.envelope.opcode, 16);
                assert_eq!(parsed.envelope.flags, 10);
                assert_eq!(result.snapshot_seq, 4);
                assert_eq!(result.join_seq, 5);
                assert_eq!(result.membership_version, 2);
                assert_eq!(result.subscriber_instance, join.subscriber_instance);
                if attempt == 0 {
                    saved = received.bytes().to_vec();
                } else {
                    assert_eq!(saved, received.bytes());
                }
            }
            let mut done = String::new();
            std::io::stdin().read_to_string(&mut done).unwrap();
            assert_eq!(done, "done\n");
        }
        "consumer" => {
            let (mut session, mut connection) = connect(3, 4);
            let join: JoinBody = env_wire("TW_JOIN_FINAL");
            let bank: ResourceHeader = env_wire("TW_JOIN_BANK");
            let snapshot: ResourceHeader = env_wire("TW_JOIN_SNAPSHOT");
            let mut files = Vec::new();
            for (index, expected) in [&bank, &snapshot].into_iter().enumerate() {
                let packet = connection.recv(timeout()).unwrap();
                let (bytes, mut fds) = packet.into_parts();
                let frame = session.validate(&bytes, now()).unwrap();
                assert_eq!(frame.envelope.opcode, 3);
                assert_eq!(frame.envelope.flags, 32);
                assert_eq!(
                    frame.envelope.expected_state_version,
                    join.membership_version
                );
                let prefix = OfferPrefix::decode(&frame.body[..64]).unwrap();
                assert_eq!(prefix.membership_version, join.membership_version);
                assert_eq!(prefix.expires_mono_ns, join.readiness_deadline_mono_ns);
                assert_eq!(prefix.required_seals, if index == 0 { 0x17 } else { 0x0f });
                let desc = ResourceDescriptor::decode(&frame.body[64..]).unwrap();
                assert_eq!(
                    desc,
                    ResourceDescriptor {
                        resource_id: expected.resource_id,
                        channel_id: expected.channel_id,
                        parent_resource_id: expected.parent_resource_id,
                        resource_bytes: expected.resource_bytes,
                        channel_epoch: expected.channel_epoch,
                        ring_epoch: expected.ring_epoch,
                        resource_kind: expected.resource_kind,
                        access: 1,
                        fd_index: 0,
                        seal_profile: if index == 0 { 1 } else { 2 },
                        header_hash: expected.descriptor_hash,
                        writer_instance: expected.producer_instance,
                    }
                );
                assert_eq!(fds.len(), 1);
                files.push(fds.pop().unwrap());
            }
            let snapshot_fd = files.pop().unwrap();
            let bank_fd = files.pop().unwrap();
            // SAFETY: owner runs this build's Publisher SDK, owns the bank writer,
            // and retains the real pending gate until validated activation/quiescence.
            let mut consumer = unsafe {
                JoinConsumer::<64>::attach(
                    bank_fd,
                    snapshot_fd,
                    bank,
                    snapshot,
                    join.clone(),
                    registry(),
                    now(),
                )
                .unwrap()
            };
            let local = consumer
                .load_baseline(now(), |topic| {
                    let m = topic.member(0)?;
                    let states = m
                        .state_records
                        .as_chunks::<512>()
                        .0
                        .iter()
                        .map(|bytes| MarketRecord::decode(bytes))
                        .collect::<tickwire_ipc::Result<Vec<_>>>()?;
                    let levels = m
                        .depth_levels
                        .as_chunks::<16>()
                        .0
                        .iter()
                        .map(|bytes| DepthLevel::decode(bytes))
                        .collect::<tickwire_ipc::Result<Vec<_>>>()?;
                    Ok((states, levels))
                })
                .unwrap();
            assert_eq!(local.0.len(), 4);
            assert_eq!(local.1.len(), 2);
            assert_eq!(local.1[0].price_ticks, 100);
            assert_eq!(local.1[1].price_ticks, 101);
            let (offer, fd) = consumer.cursor_offer(&session, [43; 16]).unwrap();
            connection.send(&offer, &[fd.as_fd()], timeout()).unwrap();
            drop(fd);
            for ack in consumer
                .read_ready_frames(&session, [[44; 16], [45; 16]])
                .unwrap()
            {
                connection.send(&ack, &[], timeout()).unwrap();
            }
            while let Some(guard) = consumer.borrow().unwrap() {
                let Record::Market(record) = decode_record(guard.bytes()).unwrap() else {
                    panic!()
                };
                assert_eq!(record.header.sequence, guard.sequence());
                consumer.apply(guard.sequence()).unwrap();
            }
            assert_eq!(consumer.watermarks().released, 68);
            assert_eq!(consumer.watermarks().applied, 68);
            let ready = consumer
                .join_ready_frame(&session, [46; 16], now())
                .unwrap();
            connection.send(&ready, &[], timeout()).unwrap();
            let ack = connection.recv(timeout()).unwrap();
            let response = session.validate(ack.bytes(), now()).unwrap();
            assert_eq!(response.envelope.opcode, 49);
            assert_eq!(response.envelope.flags, 10);
            assert_eq!(response.envelope.request_id, [46; 16]);
            let status = StatusBody::decode(response.body).unwrap();
            assert_eq!(status.original_request_id, [46; 16]);
            assert_eq!(status.original_control_epoch, session.epoch());
            assert_eq!(status.subject_id, join.cursor_resource_id);
            assert_eq!(status.membership_version, join.membership_version);
            assert_eq!(status.released_seq, 68);
            let guard = consumer.borrow().unwrap().unwrap();
            assert_eq!(guard.sequence(), 69);
            consumer.apply(69).unwrap();
            let marker = frame(&session, 48, 1, 47, 2, &QueryBody::default());
            connection.send(&marker, &[], timeout()).unwrap();
            let request = connection.recv(timeout()).unwrap();
            assert!(
                consumer
                    .quiesce(&mut session, request.bytes(), now())
                    .is_err()
            );
            let Record::Market(record) = decode_record(guard.bytes()).unwrap() else {
                panic!()
            };
            assert_eq!(record.header.sequence, 69);
            drop(guard);
            let ack = consumer
                .quiesce(&mut session, request.bytes(), now())
                .unwrap();
            connection.send(&ack, &[], timeout()).unwrap();
        }
        _ => panic!("unknown child role"),
    }
}
