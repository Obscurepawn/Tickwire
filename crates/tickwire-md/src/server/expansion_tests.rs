use super::*;
use tickwire_market_source::{Client, PendingJoin};
mod fixture;
use fixture::*;

#[test]
fn active_admissions_are_actual_full_bindings_and_pending_hello_blocks_plans() {
    let mut fixture = Fixture::new();
    assert!(fixture.server.active_admissions(0).unwrap().is_empty());
    let (_client, admission) = fixture.join(1);
    let actual = fixture.server.active_admissions(0).unwrap();
    assert_eq!(actual.len(), 1);
    same_admission(&actual[0], &admission).unwrap();
    let connection = Connection::connect(
        &fixture.config.socket,
        Instant::now() + Duration::from_secs(1),
    )
    .unwrap();
    fixture.server.poll(&mut fixture.topics, NOW).unwrap();
    assert!(fixture.server.active_admissions(0).is_err());
    drop(connection);
}

#[test]
fn authenticated_handshake_eof_before_join_does_not_fault_healthy_topic() {
    let mut f = Fixture::new();
    let (_active, _) = f.join(1);
    let (mut pending, admission) = f.pending(2);
    let now = tickwire_clock::MonoTime::new(admission.client.domain().unwrap(), NOW);
    pending.poll(now, 1).unwrap();
    f.server.poll(&mut f.topics, NOW).unwrap();
    pending.poll(now, 1).unwrap();
    f.server.poll(&mut f.topics, NOW).unwrap();
    assert_eq!(f.server.peers[1].stage, Stage::Request);
    assert!(f.server.active_admissions(0).is_err());
    drop(pending);
    f.server.poll(&mut f.topics, NOW).unwrap();
    assert!(!f.topics[0].needs_bank());
    assert_eq!(f.server.active_admissions(0).unwrap().len(), 1);
}

fn quiesce(client: &mut Client) -> ControlEnvelope {
    let packet = client.connection.try_recv().unwrap().unwrap();
    let request = ControlEnvelope::decode(&packet.bytes()[..128]).unwrap();
    let response = client
        .consumer
        .quiesce(&mut client.session, packet.bytes(), NOW)
        .unwrap();
    assert!(client.connection.try_send(&response, &[]).unwrap());
    request
}

fn commit_after_quiescence(f: &mut Fixture, operation: [u8; 16]) -> tickwire_join::JoinOwner<32> {
    let (mut client, admission) = f.join(1);
    f.server
        .begin_expansion(0, operation, &[admission], &mut f.topics[0], NOW)
        .unwrap();
    f.server.poll(&mut f.topics, NOW).unwrap();
    quiesce(&mut client);
    f.server.poll(&mut f.topics, NOW).unwrap();
    assert!(
        f.server
            .expansion_quiesced(0, operation, &mut f.topics[0])
            .unwrap()
    );
    f.expand()
}

#[test]
fn resume_refuses_successor_fault_after_real_commit() {
    let mut f = Fixture::new();
    let operation = [61; 16];
    let _retired = commit_after_quiescence(&mut f, operation);
    assert!(f.topics[0].state().live());
    f.topics[0].require_rebuild();
    assert!(
        f.server
            .resume_expanded_topic(0, operation, &mut f.topics[0])
            .is_err()
    );
    assert!(f.server.active_admissions(0).is_err());
    assert!(f.topics[0].needs_bank());
}

#[test]
fn resume_refuses_unwarmed_successor_even_without_rebuild_flag() {
    let mut f = Fixture::new();
    let operation = [62; 16];
    let _retired = commit_after_quiescence(&mut f, operation);
    let mut cold = f.unwarmed_successor();
    assert!(!cold.needs_bank());
    assert!(!cold.state().live());
    assert!(
        f.server
            .resume_expanded_topic(0, operation, &mut cold)
            .is_err()
    );
    assert!(f.server.active_admissions(0).is_err());
    assert!(
        f.server
            .resume_expanded_topic(0, operation, &mut f.topics[0])
            .is_err()
    );
}

#[test]
fn repeated_resume_keeps_later_successor_fault_closed() {
    let mut f = Fixture::new();
    let operation = [63; 16];
    let _retired = commit_after_quiescence(&mut f, operation);
    f.server
        .resume_expanded_topic(0, operation, &mut f.topics[0])
        .unwrap();
    f.server
        .resume_expanded_topic(0, operation, &mut f.topics[0])
        .unwrap();
    assert!(f.server.active_admissions(0).unwrap().is_empty());
    f.topics[0].require_rebuild();
    assert!(
        f.server
            .resume_expanded_topic(0, operation, &mut f.topics[0])
            .is_err()
    );
    assert!(f.server.active_admissions(0).is_err());
    assert!(f.topics[0].needs_bank());
}

#[test]
fn expansion_needs_actual_cursor_proof_and_real_successor_before_rejoin() {
    let mut f = Fixture::new();
    let (mut client, admission) = f.join(1);
    let op = [51; 16];
    let mut wrong = admission.clone();
    wrong.snapshot_id = [99; 16];
    assert!(
        f.server
            .begin_expansion(0, op, &[wrong.clone()], &mut f.topics[0], NOW)
            .is_err()
    );
    assert_eq!(f.server.active_admissions(0).unwrap().len(), 1);
    f.server
        .begin_expansion(
            0,
            op,
            std::slice::from_ref(&admission),
            &mut f.topics[0],
            NOW,
        )
        .unwrap();
    f.server
        .begin_expansion(
            0,
            op,
            std::slice::from_ref(&admission),
            &mut f.topics[0],
            NOW,
        )
        .unwrap();
    assert!(
        f.server
            .begin_expansion(0, op, &[wrong], &mut f.topics[0], NOW)
            .is_err()
    );
    assert!(
        !f.server
            .expansion_quiesced(0, op, &mut f.topics[0])
            .unwrap()
    );
    assert!(
        f.server
            .resume_expanded_topic(0, op, &mut f.topics[0])
            .is_err()
    );
    f.server.poll(&mut f.topics, NOW).unwrap();
    let request = quiesce(&mut client);
    assert_ne!(request.request_id, [0; 16]);
    assert_ne!(request.request_id, [20; 16]);
    f.server.poll(&mut f.topics, NOW).unwrap();
    assert!(!f.server.has_peers(0));
    assert!(
        f.server
            .expansion_quiesced(0, op, &mut f.topics[0])
            .unwrap()
    );
    assert!(
        f.server
            .resume_expanded_topic(0, op, &mut f.topics[0])
            .is_err()
    );
    assert!(
        f.server
            .expansion_quiesced(0, [52; 16], &mut f.topics[0])
            .is_err()
    );
    // A late old-bank handshake is rejected without poisoning the healthy publisher.
    let (mut late, old) = f.pending(2);
    let now = tickwire_clock::MonoTime::new(old.client.domain().unwrap(), NOW);
    late.poll(now, 1).unwrap();
    f.server.poll(&mut f.topics, NOW).unwrap();
    late.poll(now, 1).unwrap();
    f.server.poll(&mut f.topics, NOW).unwrap();
    assert!(late.poll(now, 1).is_err());
    assert!(!f.topics[0].needs_bank());
    assert!(
        f.server
            .expansion_quiesced(0, op, &mut f.topics[0])
            .unwrap()
    );
    let _retired = f.expand();
    f.server
        .resume_expanded_topic(0, op, &mut f.topics[0])
        .unwrap();
    f.server
        .resume_expanded_topic(0, op, &mut f.topics[0])
        .unwrap();
    let (next, _) = f.join(3);
    assert_eq!(next.bank.feed_generation, 2);
    assert_eq!(
        next.replica.instruments().collect::<Vec<_>>(),
        vec![1001, (1 << 63) | 1002]
    );
}

#[test]
fn eof_after_revocation_fails_operation_even_when_no_peers_remain() {
    let mut f = Fixture::new();
    let (client, admission) = f.join(1);
    let op = [53; 16];
    f.server
        .begin_expansion(0, op, &[admission], &mut f.topics[0], NOW)
        .unwrap();
    drop(client);
    f.server.poll(&mut f.topics, NOW).unwrap();
    assert!(!f.server.has_peers(0));
    assert!(
        f.server
            .expansion_quiesced(0, op, &mut f.topics[0])
            .is_err()
    );
    assert!(f.server.active_admissions(0).is_err());
    assert!(!f.topics[0].split().0.all_detached());
    assert!(f.topics[0].needs_bank());
}

#[test]
fn authenticated_fabricated_quiesced_body_does_not_replace_mapped_cursor() {
    let mut f = Fixture::new();
    let (mut client, admission) = f.join(1);
    let op = [54; 16];
    f.server
        .begin_expansion(0, op, &[admission], &mut f.topics[0], NOW)
        .unwrap();
    f.server.poll(&mut f.topics, NOW).unwrap();
    let packet = client.connection.try_recv().unwrap().unwrap();
    let frame = client.session.validate(packet.bytes(), NOW).unwrap();
    let mut body = tickwire_ipc::QuiesceBody::decode(frame.body).unwrap();
    body.evidence_kind = 1;
    body.applied_seq = client.consumer.watermarks().applied;
    body.released_seq = body.applied_seq;
    let mut header = frame.envelope.clone();
    header.flags = 10;
    let forged = client
        .session
        .encode(header, &wire_bytes(&body).unwrap())
        .unwrap();
    assert!(client.connection.try_send(&forged, &[]).unwrap());
    f.server.poll(&mut f.topics, NOW).unwrap();
    assert!(!f.topics[0].split().0.all_detached());
    assert!(
        f.server
            .expansion_quiesced(0, op, &mut f.topics[0])
            .is_err()
    );
    assert!(
        f.server
            .resume_expanded_topic(0, op, &mut f.topics[0])
            .is_err()
    );
}

#[test]
fn every_approved_participant_has_distinct_request_and_must_really_detach() {
    let mut f = Fixture::new();
    let (mut a, aa) = f.join(1);
    let (mut b, ab) = f.join(2);
    let operation = [55; 16];
    assert!(
        f.server
            .begin_expansion(
                0,
                operation,
                std::slice::from_ref(&aa),
                &mut f.topics[0],
                NOW
            )
            .is_err()
    );
    f.server
        .begin_expansion(0, operation, &[ab, aa], &mut f.topics[0], NOW)
        .unwrap();
    f.server.poll(&mut f.topics, NOW).unwrap();
    let first = quiesce(&mut a);
    f.server.poll(&mut f.topics, NOW).unwrap();
    assert!(
        !f.server
            .expansion_quiesced(0, operation, &mut f.topics[0])
            .unwrap()
    );
    let second = quiesce(&mut b);
    assert_ne!(first.request_id, second.request_id);
    f.server.poll(&mut f.topics, NOW).unwrap();
    assert!(
        f.server
            .expansion_quiesced(0, operation, &mut f.topics[0])
            .unwrap()
    );
}

#[test]
fn quiesce_outbox_retains_one_exact_packet_until_kernel_backpressure_clears() {
    let mut f = Fixture::new();
    let (mut client, admission) = f.join(1);
    let operation = [56; 16];
    let dummy = activation_ack(
        f.server.peers[0].session.as_ref().unwrap(),
        &ControlEnvelope {
            request_id: [99; 16],
            expected_state_version: client.join.membership_version,
            ..Default::default()
        },
        &client.join,
        f.topics[0].split().0.published(),
    )
    .unwrap();
    let mut sent = 0;
    while sent < 4096 && f.server.peers[0].connection.try_send(&dummy, &[]).unwrap() {
        sent += 1;
    }
    assert!((1..4096).contains(&sent));
    f.server
        .begin_expansion(0, operation, &[admission], &mut f.topics[0], NOW)
        .unwrap();
    let exact = f.server.peers[0].outgoing.clone().unwrap();
    f.server.poll(&mut f.topics, NOW).unwrap();
    assert_eq!(f.server.peers[0].stage, Stage::QuiesceQueued);
    assert_eq!(f.server.peers[0].outgoing.as_ref(), Some(&exact));
    assert!(
        !f.server
            .expansion_quiesced(0, operation, &mut f.topics[0])
            .unwrap()
    );
    for _ in 0..sent {
        assert_eq!(
            client.connection.try_recv().unwrap().unwrap().bytes(),
            dummy
        );
    }
    assert!(client.connection.try_recv().unwrap().is_none());
    f.server.poll(&mut f.topics, NOW).unwrap();
    assert_eq!(f.server.peers[0].stage, Stage::Quiescing);
    assert!(f.server.peers[0].outgoing.is_none());
    let packet = client.connection.try_recv().unwrap().unwrap();
    assert_eq!(packet.bytes(), exact);
    assert!(client.connection.try_recv().unwrap().is_none());
    let response = client
        .consumer
        .quiesce(&mut client.session, packet.bytes(), NOW)
        .unwrap();
    assert!(client.connection.try_send(&response, &[]).unwrap());
    f.server.poll(&mut f.topics, NOW).unwrap();
    assert!(
        f.server
            .expansion_quiesced(0, operation, &mut f.topics[0])
            .unwrap()
    );
}

#[test]
fn expired_quiescence_keeps_topic_closed_without_detach_proof() {
    let mut f = Fixture::new();
    let (_client, admission) = f.join(1);
    let operation = [57; 16];
    f.server
        .begin_expansion(0, operation, &[admission], &mut f.topics[0], NOW)
        .unwrap();
    f.server.peers[0].deadline = Instant::now();
    f.server.poll(&mut f.topics, NOW).unwrap();
    assert!(
        f.server
            .expansion_quiesced(0, operation, &mut f.topics[0])
            .is_err()
    );
    assert!(
        f.server
            .resume_expanded_topic(0, operation, &mut f.topics[0])
            .is_err()
    );
    assert!(!f.topics[0].split().0.all_detached());
}
