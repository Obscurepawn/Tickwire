#![allow(dead_code)]
use super::poll_frontier as poll_actual;
fn poll_frontier(client: &mut Client, now: MonoTime, deadline: u64) -> crate::Result<bool> {
    poll_actual(client, now, deadline, 1_000_000_000)
}
use std::os::unix::fs::MetadataExt;
use std::time::{Duration, Instant};
use tickwire_clock::MonoTime;
use tickwire_control::{ProcessIdentity, Session};
use tickwire_control_sys::{Connection, ControlListener, PrivateDirectory};
use tickwire_ipc::{ControlEnvelope, JoinBody, ResourceHeader, Wire};
use tickwire_join::TopicRegistry;
use tickwire_market_source::protocol::*;
use tickwire_market_source::{Client, ConnectConfig, PendingJoin};
#[path = "../../../../tickwire-join/tests/common/mod.rs"]
mod fixture;
#[path = "../../../../tickwire-market-source/src/shm/pending_tests/peer.rs"]
mod peer;
fn config(path: std::path::PathBuf) -> ConnectConfig {
    let mut c = crate::transition_fixture::approval().previous;
    c.endpoint.path = path;
    c.endpoint.peer_pid = std::process::id();
    c.endpoint.peer_uid = std::fs::metadata("/proc/self").unwrap().uid();
    c.endpoint.peer_gid = std::fs::metadata("/proc/self").unwrap().gid();
    c
}
fn joined(peer: &mut peer::Peer) -> Client {
    // The shared ABI vectors omit optional timestamps. A runtime-ready peer
    // supplies real observations in the active monotonic clock domain.
    for record in &mut peer.data.states {
        record.header.flags |= 6;
        record.header.ingress_mono_ns = fixture::NOW;
        record.header.publish_mono_ns = fixture::NOW;
    }
    let mut pending = peer.pending();
    for _ in 0..128 {
        if pending.poll(peer.now(), 1).unwrap().ready {
            return pending.take_client().unwrap();
        }
        peer.step();
    }
    panic!("JOIN failed");
}
#[test]
fn empty_catchup_waits_for_one_actual_record_before_a_producer_frontier_exists() {
    let mut peer = peer::Peer::new();
    let mut client = joined(&mut peer);
    let now = peer.now();
    let until = now.nanos() + 1_000_000;
    assert!(client.catchup.is_empty());
    assert!(!poll_frontier(&mut client, now, until).unwrap());
    assert_eq!(client.consumer.watermarks().applied, 4);
    peer.publish();
    assert!(poll_frontier(&mut client, now, until).unwrap());
    assert_eq!(client.consumer.watermarks().applied, 5);
    assert_eq!(client.replica.sequence(), 5);
    assert_eq!(client.catchup.len(), 1);
    assert_eq!(client.catchup[0].record, peer.data.record(5));
}

#[test]
fn waiting_frontier_rejects_deadline_lease_eof_and_authenticated_quiesce() {
    for case in 0..4 {
        let mut peer = peer::Peer::new();
        let mut client = joined(&mut peer);
        let now = peer.now();
        let until = now.nanos() + 1_000_000;
        let result = match case {
            0 => poll_frontier(&mut client, MonoTime::new(now.domain(), until), until),
            1 => {
                let expired = client.session.deadline();
                poll_frontier(
                    &mut client,
                    MonoTime::new(now.domain(), expired),
                    expired + 1,
                )
            }
            2 => {
                peer.connection = None;
                poll_frontier(&mut client, now, until)
            }
            _ => {
                peer.send_quiesce([4; 16], false);
                poll_frontier(&mut client, now, until)
            }
        };
        assert!(result.is_err(), "case {case}");
        assert!(client.catchup.is_empty());
    }
}
#[test]
fn invalidating_feed_record_cannot_complete_the_waiting_frontier() {
    let mut peer = peer::Peer::new();
    let mut client = joined(&mut peer);
    let now = peer.now();
    let mut r = peer
        .data
        .states
        .iter()
        .find(|r| r.header.message_type == 261)
        .unwrap()
        .clone();
    r.header.sequence = 5;
    r.body.feed_state = 1;
    let bytes = wire_bytes(&r).unwrap();
    tickwire_ipc::decode_record(&bytes).unwrap();
    peer.owner.publish(&[&bytes]).unwrap();
    assert!(poll_frontier(&mut client, now, now.nanos() + 1_000_000).is_err());
}

#[test]
fn obtained_frontier_still_rechecks_session_and_quiesce_before_swap() {
    for quiesce in [false, true] {
        let mut peer = peer::Peer::new();
        let mut client = joined(&mut peer);
        let now = peer.now();
        let until = now.nanos() + 1_000_000;
        peer.publish();
        assert!(poll_frontier(&mut client, now, until).unwrap());
        if quiesce {
            peer.send_quiesce([4; 16], false);
        }
        let check = if quiesce {
            now
        } else {
            MonoTime::new(now.domain(), client.session.deadline())
        };
        let deadline = client.session.deadline() + 1;
        assert!(poll_frontier(&mut client, check, deadline).is_err());
        assert_eq!(client.consumer.watermarks().applied, 5);
    }
}
