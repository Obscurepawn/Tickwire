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

    pub fn send_quiesce(&mut self, operation: [u8; 16], approved: bool) {
        let sub = self.registration.subscription;
        let id = if approved {
            crate::transition::quiesce_request_id(operation, sub)
        } else {
            [99; 16]
        };
        let bytes = self
            .owner
            .quiesce(sub, self.session.as_ref().unwrap(), id, 1)
            .unwrap();
        assert!(
            self.connection
                .as_mut()
                .unwrap()
                .try_send(&bytes, &[])
                .unwrap()
        );
    }
    pub fn receive_quiesced(&mut self) {
        let packet = self
            .connection
            .as_mut()
            .unwrap()
            .try_recv()
            .unwrap()
            .unwrap();
        self.owner
            .quiesced(
                self.registration.subscription,
                self.session.as_mut().unwrap(),
                packet.bytes(),
                fixture::NOW,
            )
            .unwrap();
        assert!(self.owner.all_detached());
    }
    pub fn successor(&mut self, config: ConnectConfig) {
        let bank = ResourceHeader::decode(&config.bank_header).unwrap();
        let frozen = self.owner.freeze_for_successor(bank.ring_epoch).unwrap();
        assert_eq!(frozen.cutover, bank.cutover_seq);
        let mut publisher = Publisher::<4>::create(bank.clone()).unwrap();
        for record in &mut self.data.states {
            record.header.feed_generation = bank.feed_generation;
            record.header.flags |= 6;
            record.header.publish_mono_ns = fixture::NOW;
            record.header.ingress_mono_ns = fixture::NOW;
        }
        for seq in bank.first_seq..bank.first_seq + 4 {
            publisher.publish(&[&self.data.record(seq)]).unwrap();
        }
        self.owner = JoinOwner::new(publisher, config.registry().unwrap()).unwrap();
        self.registration = fixture::registration(&self.owner, 4);
        self.registration.subscription.generation = config.subscription_generation;
        self.registration.subscription.cursor = u128::from_le_bytes(config.cursor_id);
        self.config = config;
        self.connection = None;
        self.session = None;
        self.stage = 0;
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
                let registry = self.config.registry().unwrap();
                self.owner
                    .prepare(
                        &mut supervisor,
                        &mut MockRequestStore::new(16).unwrap(),
                        &admin_request,
                        &self.registration,
                        [31; 16],
                        now,
                        |plan| {
                            snapshot(
                                &self.data,
                                plan,
                                request.readiness_deadline_mono_ns,
                                &registry,
                            )
                        },
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
    registry: &TopicRegistry,
) -> tickwire_ipc::Result<tickwire_snapshot::BuiltTopic> {
    use tickwire_snapshot::{BuildRequest, InstrumentSnapshot, build_topic_with_coverage};
    let states: Vec<_> = registry
        .instruments
        .iter()
        .map(|instrument| {
            let mut records = data.states.clone();
            for record in &mut records {
                record.header.instrument_id = instrument.instrument_id;
                record.header.feed_generation = plan.join.feed_generation;
                record.header.sequence = plan.sequence;
            }
            records
        })
        .collect();
    let members: Vec<_> = registry
        .instruments
        .iter()
        .zip(&states)
        .enumerate()
        .map(|(index, (instrument, records))| InstrumentSnapshot {
            instrument_id: instrument.instrument_id,
            instrument_version: instrument.instrument_version,
            snapshot_id: [32 + index as u8; 16],
            at_sequence: plan.sequence,
            last_event_sequence: plan.sequence,
            feed_generation: plan.join.feed_generation,
            catalog_version: 1,
            catalog_hash: registry.catalog_hash,
            created_mono_ns: fixture::NOW,
            expires_mono_ns: deadline,
            state_version: 1,
            coverage_depth: 10,
            coverage_flags: 0,
            states: records,
            bids: &data.bids,
            asks: &data.asks,
        })
        .collect();
    build_topic_with_coverage(
        &BuildRequest {
            resource: &plan.resource,
            join: &plan.join,
            catalog_hash: registry.catalog_hash,
            instrument_set_version: registry.instrument_set_version,
            sequence: plan.sequence,
            created_mono_ns: fixture::NOW,
            expires_mono_ns: deadline,
            now_mono_ns: fixture::NOW,
            logical_strategy_id: 7,
            instruments: &registry.instruments,
            members: &members,
            byte_limit: 64 * 1024 * 1024,
        },
        &vec![
            tickwire_ipc::DepthCoverage {
                version: 1,
                bid_kind: 1,
                ask_kind: 1,
                ..Default::default()
            };
            members.len()
        ],
    )
}
