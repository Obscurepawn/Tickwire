use super::*;
use crate::transition::{Approval, Commit, Phase};

fn joined(peer: &mut Peer) -> Client {
    let mut pending = peer.pending();
    for _ in 0..128 {
        if pending.poll(peer.now(), 1).unwrap().ready {
            return pending.take_client().unwrap();
        }
        peer.step();
    }
    panic!("real JOIN did not complete");
}
fn approval(config: &ConnectConfig) -> Approval {
    let mut next = config.clone();
    let mut bank = ResourceHeader::decode(&config.bank_header).unwrap();
    bank.resource_id = [77; 16];
    bank.predecessor_ring_epoch = bank.ring_epoch;
    bank.ring_epoch += 1;
    bank.feed_generation += 1;
    bank.created_mono_ns = 0;
    bank.first_seq = 1;
    bank.cutover_seq = 0;
    bank.descriptor_hash = [0; 32];
    next.bank_header = wire_bytes(&bank).unwrap();
    next.endpoint.control_epoch += 1;
    next.endpoint.nonce = [78; 32];
    next.cursor_id = [79; 16];
    next.request_id = [80; 16];
    next.subscription_generation += 1;
    next.instrument_set_version += 1;
    next.instruments.push(Instrument {
        id: u64::MAX,
        version: 5,
        required_states: 1,
    });
    Approval {
        schema: 1,
        operation_id: [81; 16],
        approval_digest: [82; 32],
        revision: 2,
        previous: config.clone(),
        next_template: next,
        deadline_mono_ns: fixture::NOW + 100_000_000,
    }
}
#[derive(Default)]
struct Sink {
    observed: Vec<FeedObservation>,
    invalidated: Vec<Invalidation>,
}
impl MarketSink for Sink {
    fn observed(&mut self, o: FeedObservation) -> bool {
        self.observed.push(o);
        true
    }
    fn invalidated(&mut self, i: Invalidation) -> bool {
        self.invalidated.push(i);
        true
    }
}
#[test]
fn arming_preserves_old_stream_and_requires_the_actual_quiesce_before_commit() {
    let mut peer = Peer::new();
    let client = joined(&mut peer);
    let mut source = ShmSource::new(client);
    let a = approval(&peer.config);
    let now = peer.now();
    let mut sink = Sink::default();
    let was_live = source.is_live(now, 1_000_000_000);
    let receipt = source.arm_transition(a.clone(), &peer.config, now).unwrap();
    assert_eq!(receipt.phase, Phase::Armed);
    assert_eq!(receipt.pid, std::process::id());
    source.poll(now, 1_000_000_000, 1, &mut sink).unwrap();
    assert!(!sink.observed.is_empty());
    assert!(sink.invalidated.is_empty());
    assert_eq!(source.is_live(now, 1_000_000_000), was_live);
    let c = Commit {
        schema: 1,
        operation_id: a.operation_id,
        approval_digest: a.approval_digest,
        revision: 2,
        config: a.next_template,
    };
    assert!(source.commit_transition(c, now).is_err());
    assert_eq!(source.transition_receipt().unwrap().phase, Phase::Armed);
}

fn finalized(a: &Approval, published: u64) -> Commit {
    let mut config = a.next_template.clone();
    let mut bank = ResourceHeader::decode(&config.bank_header).unwrap();
    bank.created_mono_ns = fixture::NOW;
    bank.cutover_seq = published;
    bank.first_seq = published + 1;
    bank.descriptor_hash = Sha256::digest(wire_bytes(&bank).unwrap()).into();
    config.bank_header = wire_bytes(&bank).unwrap();
    Commit {
        schema: 1,
        operation_id: a.operation_id,
        approval_digest: a.approval_digest,
        revision: a.revision,
        config,
    }
}
#[test]
fn real_quiesce_successor_join_and_effective_release_keep_observations_ordered() {
    let mut peer = Peer::new();
    let client = joined(&mut peer);
    let mut source = ShmSource::new(client);
    let a = approval(&peer.config);
    let now = peer.now();
    let mut sink = Sink::default();
    source.arm_transition(a.clone(), &peer.config, now).unwrap();
    source.poll(now, 1_000_000_000, 1, &mut sink).unwrap();
    peer.send_quiesce(a.operation_id, true);
    source.poll(now, 1_000_000_000, 1, &mut sink).unwrap();
    peer.receive_quiesced();
    assert_eq!(source.transition_receipt().unwrap().phase, Phase::Quiesced);
    assert_eq!(
        source.transition_receipt().unwrap().previous_applied,
        Some(4)
    );
    assert_eq!(sink.invalidated.len(), 1);
    let c = finalized(&a, peer.owner.published());
    peer.successor(c.config.clone());
    source.commit_transition(c.clone(), now).unwrap();
    source.commit_transition(c, now).unwrap();
    let observations = sink.observed.len();
    for _ in 0..128 {
        source.poll(now, 1_000_000_000, 1, &mut sink).unwrap();
        if source.transition_receipt().unwrap().phase == Phase::Activated {
            break;
        }
        peer.step();
    }
    let receipt = source.transition_receipt().unwrap();
    assert_eq!(receipt.phase, Phase::Activated);
    assert_eq!(receipt.activated_sequence, Some(8));
    assert_eq!(sink.observed.len(), observations);
    assert!(source.client().replica.view(u64::MAX).is_some());
    assert!(!source.is_live(now, 1_000_000_000));
    source.poll(now, 1_000_000_000, 1, &mut sink).unwrap();
    assert_eq!(sink.observed.len(), observations);
    source.release_transition(now).unwrap();
    source.poll(now, 1_000_000_000, 1, &mut sink).unwrap();
    assert_eq!(sink.observed.len(), observations + 2);
    assert_eq!(sink.observed.last().unwrap().key.instrument_id, u64::MAX);
    assert!(
        sink.observed[observations..]
            .iter()
            .all(|o| o.feed_generation == 2 && o.ring_epoch == 2)
    );
    peer.send_quiesce([99; 16], false);
    assert!(source.poll(now, 1_000_000_000, 1, &mut sink).is_err());
    peer.receive_quiesced();
    assert!(source.release_transition(now).is_err());
}
#[test]
fn expired_preparation_keeps_old_stream_but_unrelated_quiesce_is_terminal() {
    for expired in [true, false] {
        let mut peer = Peer::new();
        let client = joined(&mut peer);
        let mut source = ShmSource::new(client);
        let mut sink = Sink::default();
        let a = approval(&peer.config);
        let now = peer.now();
        source.arm_transition(a.clone(), &peer.config, now).unwrap();
        if expired {
            let later = MonoTime::new(now.domain(), a.deadline_mono_ns);
            source.poll(later, 1_000_000_000, 1, &mut sink).unwrap();
            assert!(source.transition_receipt().is_none());
            assert!(sink.invalidated.is_empty());
        } else {
            peer.send_quiesce(a.operation_id, false);
            assert!(source.poll(now, 1_000_000_000, 1, &mut sink).is_err());
            assert!(source.transition_receipt().is_none());
            peer.receive_quiesced();
        }
    }
}

#[test]
fn repeated_arm_does_not_accept_a_wrong_clock_or_an_expired_receipt() {
    let mut peer = Peer::new();
    let client = joined(&mut peer);
    let mut source = ShmSource::new(client);
    let a = approval(&peer.config);
    let now = peer.now();
    source.arm_transition(a.clone(), &peer.config, now).unwrap();
    let other = MonoTime::new(ClockDomain::new([99; 16], 1).unwrap(), now.nanos());
    assert!(
        source
            .arm_transition(a.clone(), &peer.config, other)
            .is_err()
    );
    let late = MonoTime::new(now.domain(), a.deadline_mono_ns);
    assert!(
        source
            .arm_transition(a.clone(), &peer.config, late)
            .is_err()
    );
    assert!(
        source
            .arm_transition(
                a.clone(),
                &peer.config,
                MonoTime::new(now.domain(), now.nanos() - 1)
            )
            .is_err()
    );
}

#[test]
fn quiesced_response_stays_pending_under_backpressure_and_deadline_closes_it() {
    for expire in [false, true] {
        let mut peer = Peer::new();
        let client = joined(&mut peer);
        let mut source = ShmSource::new(client);
        let mut sink = Sink::default();
        let a = approval(&peer.config);
        let now = peer.now();
        source.arm_transition(a.clone(), &peer.config, now).unwrap();
        let fill = activation_ack(
            &source.client.session,
            &ControlEnvelope {
                request_id: [99; 16],
                expected_state_version: source.client.join.membership_version,
                ..Default::default()
            },
            &source.client.join,
            peer.owner.published(),
        )
        .unwrap();
        let mut queued = 0;
        while source.client.connection.try_send(&fill, &[]).unwrap() {
            queued += 1;
            assert!(queued < 1024);
        }
        peer.send_quiesce(a.operation_id, true);
        source.poll(now, 1_000_000_000, 1, &mut sink).unwrap();
        assert_eq!(source.transition_receipt().unwrap().phase, Phase::Armed);
        assert!(!sink.invalidated.is_empty());
        assert!(
            source
                .commit_transition(finalized(&a, peer.owner.published()), now)
                .is_err()
        );
        if expire {
            let late = MonoTime::new(now.domain(), a.deadline_mono_ns);
            assert!(source.poll(late, 1_000_000_000, 1, &mut sink).is_err());
            assert!(source.transition_receipt().is_none());
            assert!(!peer.owner.all_detached());
        } else {
            for _ in 0..queued {
                let packet = peer
                    .connection
                    .as_mut()
                    .unwrap()
                    .try_recv()
                    .unwrap()
                    .unwrap();
                assert_eq!(packet.bytes(), fill);
            }
            source.poll(now, 1_000_000_000, 1, &mut sink).unwrap();
            peer.receive_quiesced();
            assert_eq!(source.transition_receipt().unwrap().phase, Phase::Quiesced);
        }
    }
}
