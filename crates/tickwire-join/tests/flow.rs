#![cfg(target_os = "linux")]
#![allow(unsafe_code)]
mod common;
use common::*;
use std::os::fd::AsFd;
use std::path::Path;
use std::time::{Duration, Instant};
use tickwire_control::{MockRequestStore, Session};
use tickwire_control_sys::{Connection, ControlListener, PrivateDirectory};
use tickwire_ipc::*;
use tickwire_join::*;

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(3)
}
fn sockets() -> (Connection, Connection) {
    let dir = PrivateDirectory::create_in(Path::new("/tmp"), "tw-join-flow").unwrap();
    let path = dir.path().join("control.sock");
    let listener = ControlListener::bind(&path).unwrap();
    let client = Connection::connect(&path, deadline()).unwrap();
    let server = listener.accept(deadline()).unwrap();
    (server, client)
}
struct Flow {
    owner: JoinOwner<4>,
    data: Data,
    reg: SubscriberRegistration,
    os: Session,
    cs: Session,
    oc: Connection,
    cc: Connection,
    consumer: JoinConsumer<64>,
}
impl Flow {
    fn new() -> Self {
        let (mut owner, data) = owner();
        let reg = registration(&owner, 4);
        let (mut admin, admin_client) = session_pair(identity(1, 1));
        let raw = request(&admin_client, &join_request(&owner, &reg), 2, 1);
        let mut store = MockRequestStore::new(16).unwrap();
        owner
            .prepare(&mut admin, &mut store, &raw, &reg, [31; 16], NOW, |p| {
                data.build(p)
            })
            .unwrap();
        let (mut os, mut cs) = session_pair(reg.identity.clone());
        let (mut oc, mut cc) = sockets();
        owner
            .deliver(reg.subscription, &mut os, &mut oc, NOW, deadline())
            .unwrap();
        let mut files = Vec::new();
        for expected in [
            owner.header(),
            owner.snapshot_header(reg.subscription).unwrap(),
        ] {
            let packet = cc.recv(deadline()).unwrap();
            let (bytes, mut fds) = packet.into_parts();
            let frame = cs.validate(&bytes, NOW).unwrap();
            assert_eq!(frame.envelope.opcode, 3);
            let desc = ResourceDescriptor::decode(&frame.body[64..]).unwrap();
            assert_eq!(desc.resource_id, expected.resource_id);
            assert_eq!(desc.header_hash, expected.descriptor_hash);
            assert_eq!(desc.resource_bytes, expected.resource_bytes);
            assert_eq!(fds.len(), 1);
            files.push(fds.pop().unwrap());
        }
        let snapshot = files.pop().unwrap();
        let bank = files.pop().unwrap();
        // SAFETY: both endpoints are owned SDK objects and owner retains the gate.
        let mut consumer = unsafe {
            JoinConsumer::attach(
                bank,
                snapshot,
                owner.header().clone(),
                owner.snapshot_header(reg.subscription).unwrap().clone(),
                owner.join(reg.subscription).unwrap().clone(),
                registry(),
                NOW,
            )
            .unwrap()
        };
        assert!(consumer.borrow().is_err());
        let loaded = consumer
            .load_baseline(NOW, |v| {
                let m = v.member(0)?;
                Ok((
                    m.entry.instrument_id,
                    m.state_records.to_vec(),
                    m.depth_levels.to_vec(),
                ))
            })
            .unwrap();
        assert_eq!(loaded.0, 1001);
        assert_eq!(loaded.1.len(), 4 * 512);
        assert_eq!(loaded.2.len(), 32);
        Self {
            owner,
            data,
            reg,
            os,
            cs,
            oc,
            cc,
            consumer,
        }
    }
    fn cursor(&mut self) {
        let (frame, fd) = self.consumer.cursor_offer(&self.cs, [21; 16]).unwrap();
        self.cc.send(&frame, &[fd.as_fd()], deadline()).unwrap();
        let packet = self.oc.recv(deadline()).unwrap();
        // SAFETY: exact owned SDK consumer produced this cursor.
        unsafe {
            self.owner
                .receive_cursor(self.reg.subscription, &mut self.os, packet, NOW)
                .unwrap();
        }
    }
    fn read_ready(&mut self) {
        for frame in self
            .consumer
            .read_ready_frames(&self.cs, [[22; 16], [23; 16]])
            .unwrap()
        {
            self.owner
                .read_ready(self.reg.subscription, &mut self.os, &frame, NOW)
                .unwrap();
        }
    }
    fn drain(&self) {
        while let Some(guard) = self.consumer.borrow().unwrap() {
            let Record::Market(record) = decode_record(guard.bytes()).unwrap() else {
                panic!()
            };
            assert_eq!(record.header.sequence, guard.sequence());
            self.consumer.apply(guard.sequence()).unwrap();
        }
    }
    fn ready(&mut self) -> tickwire_join::Result<tickwire_shm::Watermarks> {
        let frame = self
            .consumer
            .join_ready_frame(&self.cs, [24; 16], NOW)
            .unwrap();
        self.owner
            .join_ready(self.reg.subscription, &mut self.os, &frame, NOW)
    }
}

#[test]
fn shared_read_ready_cannot_activate_without_cursor_or_catchup() {
    let mut f = Flow::new();
    f.read_ready();
    assert!(f.ready().is_err());
    assert_eq!(f.owner.stage(f.reg.subscription), Some(Stage::Delivered));
    f.cursor();
    fill_pending(&mut f.owner, &f.data);
    assert!(f.ready().is_err());
    assert_full(&mut f.owner, &f.data);
    f.drain();
    assert_full(&mut f.owner, &f.data); // Cursor at 68 still cannot release S=4.
    assert_eq!(f.ready().unwrap().applied, 68);
    assert_eq!(f.owner.stage(f.reg.subscription), Some(Stage::Active));
    assert_eq!(f.ready().unwrap().applied, 68);
    assert_eq!(f.owner.membership_version(), 2);
    f.owner.publish(&[&f.data.record(69)]).unwrap();
    // Completed JOIN retry returns the original result even after publication.
    assert_eq!(f.ready().unwrap().applied, 68);
}

#[test]
fn wrong_read_ready_and_join_ready_do_not_overwrite_bindings() {
    let mut f = Flow::new();
    f.cursor();
    let original = f.owner.join(f.reg.subscription).unwrap().clone();
    let acks = f
        .consumer
        .read_ready_frames(&f.cs, [[22; 16], [23; 16]])
        .unwrap();
    f.owner
        .read_ready(f.reg.subscription, &mut f.os, &acks[0], NOW)
        .unwrap();
    let parsed = ControlFrame::decode(&acks[1]).unwrap();
    let mut bad = ResourceAckBody::decode(parsed.body).unwrap();
    bad.header_hash[0] ^= 1;
    let wrong = frame(&f.cs, 5, 1, 23, 2, &bad);
    assert!(
        f.owner
            .read_ready(f.reg.subscription, &mut f.os, &wrong, NOW)
            .is_err()
    );
    assert!(f.ready().is_err());
    f.owner
        .read_ready(f.reg.subscription, &mut f.os, &acks[1], NOW)
        .unwrap();
    for case in 0..4 {
        let mut wrong = original.clone();
        match case {
            0 => wrong.snapshot_hash[0] ^= 1,
            1 => wrong.subscription_generation += 1,
            2 => wrong.membership_version += 1,
            _ => wrong.cursor_resource_id = [99; 16],
        }
        let raw = frame(&f.cs, 17, 1, 24, 2, &wrong);
        assert!(
            f.owner
                .join_ready(f.reg.subscription, &mut f.os, &raw, NOW)
                .is_err()
        );
        assert_eq!(f.owner.join(f.reg.subscription).unwrap(), &original);
        assert_eq!(f.owner.stage(f.reg.subscription), Some(Stage::Delivered));
    }
    assert!(f.ready().is_ok());
}

#[test]
fn expired_join_retains_advanced_pending_cursor_gate() {
    let mut f = Flow::new();
    f.cursor();
    f.read_ready();
    let ready = f.consumer.join_ready_frame(&f.cs, [24; 16], NOW).unwrap();
    fill_pending(&mut f.owner, &f.data);
    f.drain();
    assert!(
        f.owner
            .join_ready(f.reg.subscription, &mut f.os, &ready, DEADLINE)
            .is_err()
    );
    assert_eq!(f.owner.stage(f.reg.subscription), Some(Stage::Expired));
    assert_full(&mut f.owner, &f.data);
}

#[test]
fn failed_socket_delivery_keeps_pending_membership() {
    let (mut owner, data) = owner();
    let reg = registration(&owner, 4);
    let (mut admin, client) = session_pair(identity(1, 1));
    let raw = request(&client, &join_request(&owner, &reg), 2, 1);
    owner
        .prepare(
            &mut admin,
            &mut MockRequestStore::new(16).unwrap(),
            &raw,
            &reg,
            [31; 16],
            NOW,
            |p| data.build(p),
        )
        .unwrap();
    let (mut os, _) = session_pair(reg.identity.clone());
    let (mut oc, cc) = sockets();
    drop(cc);
    assert!(
        owner
            .deliver(reg.subscription, &mut os, &mut oc, NOW, deadline())
            .is_err()
    );
    assert_eq!(owner.stage(reg.subscription), Some(Stage::Failed));
    fill_pending(&mut owner, &data);
    assert_full(&mut owner, &data);
}

#[test]
fn another_join_does_not_replace_the_first_registration_version_or_gate() {
    let mut f = Flow::new();
    f.cursor();
    f.read_ready();
    let reg2 = registration(&f.owner, 5);
    let (mut admin, client) = session_pair(identity(1, 1));
    let raw = request(&client, &join_request(&f.owner, &reg2), 3, 2);
    f.owner
        .prepare(
            &mut admin,
            &mut MockRequestStore::new(16).unwrap(),
            &raw,
            &reg2,
            [33; 16],
            NOW,
            |p| f.data.build(p),
        )
        .unwrap();
    assert_eq!(f.owner.membership_version(), 3);
    assert_eq!(
        f.owner.join(f.reg.subscription).unwrap().membership_version,
        2
    );
    fill_pending(&mut f.owner, &f.data);
    f.drain();
    let mut wrong = f.owner.join(f.reg.subscription).unwrap().clone();
    wrong.membership_version = 3;
    let raw = frame(&f.cs, 17, 1, 24, 3, &wrong);
    assert!(
        f.owner
            .join_ready(f.reg.subscription, &mut f.os, &raw, NOW)
            .is_err()
    );
    assert_eq!(f.ready().unwrap().applied, 68);
    assert_eq!(f.owner.stage(reg2.subscription), Some(Stage::Prepared));
    assert_full(&mut f.owner, &f.data);
}

#[test]
fn actual_quiescence_is_required_and_conflicting_duplicate_ack_is_rejected() {
    let mut f = Flow::new();
    f.cursor();
    f.read_ready();
    f.ready().unwrap();
    f.owner.publish(&[&f.data.record(5)]).unwrap();
    let held = f.consumer.borrow().unwrap().unwrap();
    let req = f
        .owner
        .quiesce(f.reg.subscription, &f.os, [29; 16], 1)
        .unwrap();
    assert!(f.consumer.quiesce(&mut f.cs, &req, NOW).is_err());
    assert_eq!(held.sequence(), 5);
    drop(held);
    let ack = f.consumer.quiesce(&mut f.cs, &req, NOW).unwrap();
    f.owner
        .quiesced(f.reg.subscription, &mut f.os, &ack, NOW)
        .unwrap();
    assert_eq!(f.owner.stage(f.reg.subscription), Some(Stage::Detached));
    f.owner
        .quiesced(f.reg.subscription, &mut f.os, &ack, NOW)
        .unwrap();
    let parsed = ControlFrame::decode(&ack).unwrap();
    let mut changed = QuiesceBody::decode(parsed.body).unwrap();
    changed.applied_seq += 1;
    let conflict = frame(&f.cs, 20, 10, 29, 2, &changed);
    assert!(
        f.owner
            .quiesced(f.reg.subscription, &mut f.os, &conflict, NOW)
            .is_err()
    );
}
