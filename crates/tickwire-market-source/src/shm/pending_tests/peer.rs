use super::*;
use tickwire_control::MockRequestStore;
use tickwire_join::{JoinOwner, SubscriberRegistration};
use tickwire_shm::Publisher;

pub struct Peer {
    _directory: PrivateDirectory,
    listener: ControlListener,
    pub config: ConnectConfig,
    pub owner: JoinOwner<4>,
    pub data: fixture::Data,
    pub registration: SubscriberRegistration,
    pub connection: Option<Connection>,
    session: Option<Session>,
    pub stage: u8,
    pub force_catchup: bool,
    pub pending_replies: usize,
    pub wrong_join: bool,
    pub ahead_ack: bool,
}
impl Peer {
    pub fn new() -> Self {
        let directory =
            PrivateDirectory::create_in(std::path::Path::new("/tmp"), "tw-join-peer").unwrap();
        let path = directory.path().join("control.sock");
        let config = config(path.clone());
        let mut data = fixture::Data::new();
        data.states
            .retain(|record| matches!(record.header.message_type, 257 | 261));
        let mut publisher =
            Publisher::<4>::create(ResourceHeader::decode(&config.bank_header).unwrap()).unwrap();
        for seq in 1..=4 {
            publisher.publish(&[&data.record(seq)]).unwrap();
        }
        let owner = JoinOwner::new(publisher, registry()).unwrap();
        let registration = fixture::registration(&owner, 4);
        Self {
            listener: ControlListener::bind(&path).unwrap(),
            _directory: directory,
            config,
            owner,
            data,
            registration,
            connection: None,
            session: None,
            stage: 0,
            force_catchup: false,
            pending_replies: 0,
            wrong_join: false,
            ahead_ack: false,
        }
    }
    pub fn now(&self) -> MonoTime {
        MonoTime::new(self.config.domain().unwrap(), fixture::NOW)
    }
    pub fn pending(&self) -> PendingJoin {
        // SAFETY: this fixture owns the authenticated writer, pending gate and immutable resources.
        unsafe { PendingJoin::new(&self.config, self.now()).unwrap() }
    }
    pub fn publish(&mut self) {
        self.owner
            .publish(&[&self.data.record(self.owner.published() + 1)])
            .unwrap();
    }
    pub fn step(&mut self) {
        if self.connection.is_none() {
            self.connection = self.listener.try_accept().unwrap();
            return;
        }
        let Some(packet) = self.connection.as_mut().unwrap().try_recv().unwrap() else {
            return;
        };
        let now = fixture::NOW;
        let sub = self.registration.subscription;
        match self.stage {
            0 => {
                let mut policy = self.config.endpoint.policy();
                std::mem::swap(&mut policy.local, &mut policy.peer);
                let peer = self.connection.as_ref().unwrap().peer_credentials();
                let (session, welcome) = Session::accept(
                    &policy,
                    ProcessIdentity {
                        pid: peer.pid,
                        uid: peer.uid,
                        gid: peer.gid,
                    },
                    packet.bytes(),
                    now,
                )
                .unwrap();
                assert!(
                    self.connection
                        .as_mut()
                        .unwrap()
                        .try_send(&welcome, &[])
                        .unwrap()
                );
                self.session = Some(session);
                self.stage = 1;
            }
            1 => {
                let frame = self
                    .session
                    .as_mut()
                    .unwrap()
                    .validate(packet.bytes(), now)
                    .unwrap();
                assert_eq!(frame.envelope.opcode, 16);
                let request = JoinBody::decode(frame.body).unwrap();
                let (mut supervisor, client) = fixture::session_pair(fixture::identity(1, 1));
                let admin_request =
                    fixture::request(&client, &request, 25, self.owner.membership_version());
                self.owner
                    .prepare(
                        &mut supervisor,
                        &mut MockRequestStore::new(16).unwrap(),
                        &admin_request,
                        &self.registration,
                        [31; 16],
                        now,
                        |plan| snapshot(&self.data, plan, request.readiness_deadline_mono_ns),
                    )
                    .unwrap();
                let session = self.session.as_mut().unwrap();
                let mut join = self.owner.join(sub).unwrap().clone();
                if self.wrong_join {
                    join.ring_epoch += 1;
                }
                let response = session
                    .encode(
                        ControlEnvelope {
                            opcode: 16,
                            flags: 10,
                            request_id: self.config.request_id,
                            expected_state_version: self.owner.membership_version(),
                            ..Default::default()
                        },
                        &wire_bytes(&join).unwrap(),
                    )
                    .unwrap();
                assert!(
                    self.connection
                        .as_mut()
                        .unwrap()
                        .try_send(&response, &[])
                        .unwrap()
                );
                self.owner
                    .deliver(
                        sub,
                        session,
                        self.connection.as_mut().unwrap(),
                        now,
                        Instant::now() + Duration::from_secs(1),
                    )
                    .unwrap();
                self.stage = 2;
            }
            2 => {
                // SAFETY: packet was produced by this fixture's exact qualified PendingJoin consumer.
                unsafe {
                    self.owner
                        .receive_cursor(sub, self.session.as_mut().unwrap(), packet, now)
                        .unwrap();
                }
                self.stage = 3;
            }
            3 | 4 => {
                self.owner
                    .read_ready(sub, self.session.as_mut().unwrap(), packet.bytes(), now)
                    .unwrap();
                self.stage += 1;
            }
            5 => {
                if self.force_catchup {
                    self.publish();
                    self.force_catchup = false;
                }
                let header = ControlEnvelope::decode(&packet.bytes()[..128]).unwrap();
                let session = self.session.as_mut().unwrap();
                let response = match self.owner.join_ready(sub, session, packet.bytes(), now) {
                    Ok(_) => {
                        self.stage = 6;
                        activation_ack(
                            session,
                            &header,
                            self.owner.join(sub).unwrap(),
                            self.owner.published() + u64::from(self.ahead_ack),
                        )
                        .unwrap()
                    }
                    Err(tickwire_join::Error::Ring(tickwire_shm::MappedError::Protocol(
                        tickwire_shm::Error::Cursor,
                    ))) => {
                        self.pending_replies += 1;
                        let target = self.owner.catchup_target(sub).unwrap().unwrap();
                        activation_pending(session, &header, self.owner.join(sub).unwrap(), target)
                            .unwrap()
                    }
                    Err(error) => panic!("ready failed: {error:?}"),
                };
                assert!(
                    self.connection
                        .as_mut()
                        .unwrap()
                        .try_send(&response, &[])
                        .unwrap()
                );
            }
            _ => panic!("unexpected frame after activation"),
        }
    }
}

fn registry() -> TopicRegistry {
    let mut registry = fixture::registry();
    registry.instruments[0].required_states = tickwire_snapshot::QUOTE;
    registry
}
fn snapshot(
    data: &fixture::Data,
    plan: &tickwire_join::JoinPlan,
    deadline: u64,
) -> tickwire_ipc::Result<tickwire_snapshot::BuiltTopic> {
    use tickwire_snapshot::{BuildRequest, InstrumentSnapshot, build_topic_with_coverage};
    let registry = registry();
    let members = [InstrumentSnapshot {
        instrument_id: 1001,
        instrument_version: 5,
        snapshot_id: [32; 16],
        at_sequence: plan.sequence,
        last_event_sequence: 4,
        feed_generation: 1,
        catalog_version: 1,
        catalog_hash: registry.catalog_hash,
        created_mono_ns: fixture::NOW,
        expires_mono_ns: deadline,
        state_version: 1,
        coverage_depth: 10,
        coverage_flags: 0,
        states: &data.states,
        bids: &data.bids,
        asks: &data.asks,
    }];
    build_topic_with_coverage(
        &BuildRequest {
            resource: &plan.resource,
            join: &plan.join,
            catalog_hash: registry.catalog_hash,
            instrument_set_version: 1,
            sequence: plan.sequence,
            created_mono_ns: fixture::NOW,
            expires_mono_ns: deadline,
            now_mono_ns: fixture::NOW,
            logical_strategy_id: 7,
            instruments: &registry.instruments,
            members: &members,
            byte_limit: 64 * 1024 * 1024,
        },
        &[tickwire_ipc::DepthCoverage {
            version: 1,
            bid_kind: 1,
            ask_kind: 1,
            ..Default::default()
        }],
    )
}
