use tickwire_ipc::Wire;
use tickwire_market_publisher::TopicPublisher;
use tickwire_shm::SubscriptionKey;
pub fn prepare_pending(
    publisher: &mut TopicPublisher<32>,
    owner_identity: tickwire_control::Identity,
    key: SubscriptionKey,
) -> (
    u64,
    Vec<u8>,
    tickwire_ipc::ResourceHeader,
    tickwire_ipc::JoinBody,
) {
    use tickwire_control::{ClientHandshake, MockRequestStore, Policy, ProcessIdentity, Session};
    use tickwire_ipc::{ControlEnvelope, JoinBody};
    const NOW: u64 = 1;
    const DEADLINE: u64 = 60_000_000_000;
    let mut supervisor_identity = owner_identity.clone();
    supervisor_identity.instance = [90; 16];
    supervisor_identity.role = 1;
    let process = ProcessIdentity {
        pid: std::process::id(),
        uid: 0,
        gid: 0,
    };
    let policy = |local, peer| Policy {
        local,
        peer,
        peer_process: process,
        required_features: 0x1c,
        supported_features: 0x3ff,
        control_epoch: 1,
        lease_duration_ns: DEADLINE,
        max_frame_bytes: 65536,
        nonce: [91; 32],
    };
    let handshake = ClientHandshake::new(
        policy(supervisor_identity.clone(), owner_identity.clone()),
        [92; 16],
    )
    .unwrap();
    let (mut session, welcome) = Session::accept(
        &policy(owner_identity.clone(), supervisor_identity),
        process,
        handshake.hello(),
        NOW,
    )
    .unwrap();
    let supervisor = handshake.finish(process, &welcome, NOW).unwrap();
    let mut identity = owner_identity;
    identity.instance = key.instance.to_le_bytes();
    identity.generation = key.generation;
    identity.role = 3;
    identity.logical_strategy = 7;
    let (owner, state) = publisher.split();
    let h = owner.header();
    let join = JoinBody {
        subscriber_instance: identity.instance,
        logical_strategy_id: 7,
        subscription_generation: key.generation,
        topic_id: h.topic_id,
        channel_id: h.channel_id,
        channel_epoch: h.channel_epoch,
        feed_generation: h.feed_generation,
        ring_epoch: h.ring_epoch,
        cursor_resource_id: key.cursor.to_le_bytes(),
        bank_resource_id: h.resource_id,
        readiness_deadline_mono_ns: DEADLINE,
        expected_catalog_version: h.catalog_version,
        qos: h.qos,
        role: 3,
        join_flags: 3,
        topic_instrument_set_version: owner.registry().instrument_set_version,
        topic_instrument_set_hash: tickwire_join::instrument_set_hash(
            &owner.registry().instruments,
        ),
        ..Default::default()
    };
    let mut body = vec![0; JoinBody::SIZE];
    join.encode(&mut body).unwrap();
    let request = supervisor
        .encode(
            ControlEnvelope {
                opcode: 16,
                flags: 1,
                request_id: [93; 16],
                expected_state_version: owner.membership_version(),
                ..Default::default()
            },
            &body,
        )
        .unwrap();
    let mut snapshot = None;
    let result = owner
        .prepare(
            &mut session,
            &mut MockRequestStore::new(8).unwrap(),
            &request,
            &tickwire_join::SubscriberRegistration {
                identity,
                subscription: key,
                qos: h.qos,
            },
            [94; 16],
            NOW,
            |plan| {
                let built = state.snapshot(plan, NOW, DEADLINE)?;
                snapshot = Some((
                    built.bytes().to_vec(),
                    built.resource().clone(),
                    built.join().clone(),
                ));
                Ok(built)
            },
        )
        .unwrap();
    assert!(matches!(
        result,
        tickwire_join::PrepareOutcome::Prepared { .. }
    ));
    assert_eq!(owner.stage(key), Some(tickwire_join::Stage::Prepared));
    assert_eq!(owner.membership_version(), 2);
    let (bytes, resource, join) = snapshot.unwrap();
    (owner.join(key).unwrap().snapshot_seq, bytes, resource, join)
}
