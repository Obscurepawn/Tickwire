use super::*;
impl State {
    /// Some(true) consumed one ring record, Some(false) advanced protocol, None would block.
    pub(super) fn advance(&mut self, now: u64) -> Result<Option<bool>> {
        match self.phase {
            Phase::Connect => {
                let Some(connection) = self.pending.as_mut().unwrap().poll()? else {
                    return Ok(None);
                };
                self.pending = None;
                self.connection = Some(connection);
                self.phase = Phase::Hello;
            }
            Phase::Hello => {
                if !self.send(|s| {
                    Ok(Outgoing {
                        bytes: s.handshake.as_ref().unwrap().hello().to_vec(),
                        fd: None,
                    })
                })? {
                    return Ok(None);
                }
                self.phase = Phase::Welcome;
            }
            Phase::Welcome => {
                let Some(packet) = self.receive()? else {
                    return Ok(None);
                };
                if !packet.fds().is_empty() {
                    return Err(Error::Identity);
                }
                let peer = self.connection.as_ref().unwrap().peer_credentials();
                let session = self.handshake.take().unwrap().finish(
                    ProcessIdentity {
                        pid: peer.pid,
                        uid: peer.uid,
                        gid: peer.gid,
                    },
                    packet.bytes(),
                    now,
                )?;
                if self.bank.required_features & !session.features() != 0 {
                    return Err(Error::Identity);
                }
                self.expires = self.expires.min(session.deadline());
                if now >= self.expires {
                    return Err(Error::Deadline);
                }
                let registry = self.registry.as_ref().unwrap();
                self.request = Some(JoinBody {
                    subscriber_instance: self.config.endpoint.local.instance,
                    logical_strategy_id: self.config.endpoint.local.logical_strategy,
                    subscription_generation: self.config.subscription_generation,
                    topic_id: self.bank.topic_id,
                    channel_id: self.bank.channel_id,
                    channel_epoch: self.bank.channel_epoch,
                    feed_generation: self.bank.feed_generation,
                    ring_epoch: self.bank.ring_epoch,
                    cursor_resource_id: self.config.cursor_id,
                    bank_resource_id: self.bank.resource_id,
                    readiness_deadline_mono_ns: self.expires,
                    expected_catalog_version: self.bank.catalog_version,
                    qos: self.bank.qos,
                    role: self.config.endpoint.local.role,
                    join_flags: 3,
                    topic_instrument_set_version: registry.instrument_set_version,
                    topic_instrument_set_hash: tickwire_join::instrument_set_hash(
                        &registry.instruments,
                    ),
                    ..Default::default()
                });
                self.session = Some(session);
                self.phase = Phase::Request;
            }
            Phase::Request => {
                if !self.send(|s| {
                    Ok(Outgoing {
                        bytes: s.session.as_ref().unwrap().encode(
                            ControlEnvelope {
                                opcode: 16,
                                flags: 1,
                                request_id: s.config.request_id,
                                ..Default::default()
                            },
                            &wire_bytes(s.request.as_ref().unwrap())?,
                        )?,
                        fd: None,
                    })
                })? {
                    return Ok(None);
                }
                self.phase = Phase::Joined;
            }
            Phase::Joined => {
                let Some(packet) = self.receive()? else {
                    return Ok(None);
                };
                if !packet.fds().is_empty() {
                    return Err(Error::Identity);
                }
                let frame = self
                    .session
                    .as_mut()
                    .unwrap()
                    .validate(packet.bytes(), now)?;
                if frame.envelope.opcode != 16
                    || frame.envelope.flags != 10
                    || frame.envelope.request_id != self.config.request_id
                {
                    return Err(Error::Identity);
                }
                let join = JoinBody::decode(frame.body)?;
                let mut echo = join.clone();
                echo.snapshot_id = [0; 16];
                echo.snapshot_hash = [0; 32];
                echo.snapshot_seq = 0;
                echo.join_seq = 0;
                echo.membership_version = 0;
                if &echo != self.request.as_ref().unwrap() {
                    return Err(Error::Identity);
                }
                self.target = join.snapshot_seq;
                self.join = Some(join);
                self.phase = Phase::Bank;
            }
            Phase::Bank => {
                let Some(packet) = self.receive()? else {
                    return Ok(None);
                };
                let join = self.join.as_ref().unwrap();
                self.bank_fd = Some(accept_offer(
                    self.session.as_mut().unwrap(),
                    packet,
                    &self.bank,
                    offer_id(join, 1),
                    self.expires,
                    join.membership_version,
                    false,
                    now,
                )?);
                self.phase = Phase::Snapshot;
            }
            Phase::Snapshot => {
                let Some(packet) = self.receive()? else {
                    return Ok(None);
                };
                let snapshot = approved_header(&packet)?;
                let join = self.join.as_ref().unwrap();
                if snapshot.resource_kind != 8 || snapshot.resource_id != join.snapshot_id {
                    return Err(Error::Identity);
                }
                if snapshot.resource_bytes > 64 * 1024 * 1024 {
                    return Err(Error::Bounds);
                }
                let snapshot_fd = accept_offer(
                    self.session.as_mut().unwrap(),
                    packet,
                    &snapshot,
                    offer_id(join, 2),
                    self.expires,
                    join.membership_version,
                    true,
                    now,
                )?;
                let registry = self.registry.take().unwrap();
                self.baseline = Some(JoinBootstrap::new(&self.bank, &snapshot, join, &registry)?);
                // SAFETY: PendingJoin::new requires the qualified writer/gating contract.
                // The exact authenticated peer, JOIN and both descriptors were checked above.
                let mut consumer = unsafe {
                    JoinConsumer::attach(
                        self.bank_fd.take().unwrap(),
                        snapshot_fd,
                        self.bank.clone(),
                        snapshot,
                        join.clone(),
                        registry,
                        now,
                    )?
                };
                let (replica, bytes) = consumer.load_baseline(now, |topic| {
                    Ok((
                        MarketReplica::load(
                            join.topic_id,
                            join.ring_epoch,
                            topic,
                            self.config.maximum_levels,
                        )?,
                        topic.bytes().to_vec(),
                    ))
                })?;
                self.replica = Some(replica);
                self.baseline_bytes = bytes;
                self.baseline_now = now;
                self.consumer = Some(consumer);
                self.phase = Phase::Cursor;
            }
            Phase::Cursor => {
                if !self.send(|s| {
                    let (bytes, fd) = s
                        .consumer
                        .as_ref()
                        .unwrap()
                        .cursor_offer(s.session.as_ref().unwrap(), s.config.request_id)?;
                    Ok(Outgoing {
                        bytes,
                        fd: Some(fd),
                    })
                })? {
                    return Ok(None);
                }
                self.phase = Phase::ReadBank;
            }
            Phase::ReadBank | Phase::ReadSnapshot => {
                let index = usize::from(self.phase == Phase::ReadSnapshot);
                if !self.send(|s| {
                    let mut frames = s
                        .consumer
                        .as_ref()
                        .unwrap()
                        .read_ready_frames(s.session.as_ref().unwrap(), [s.config.request_id; 2])?;
                    Ok(Outgoing {
                        bytes: std::mem::take(&mut frames[index]),
                        fd: None,
                    })
                })? {
                    return Ok(None);
                }
                self.phase = if index == 0 {
                    Phase::ReadSnapshot
                } else {
                    Phase::Catchup
                };
            }
            Phase::Catchup => {
                if self.drain_one(now)? {
                    return Ok(Some(true));
                }
                if self.consumer.as_ref().unwrap().watermarks().applied < self.target {
                    return Ok(None);
                }
                self.phase = Phase::Ready;
            }
            Phase::Ready => {
                if !self.send(|s| {
                    Ok(Outgoing {
                        bytes: s.consumer.as_ref().unwrap().join_ready_frame(
                            s.session.as_ref().unwrap(),
                            s.config.request_id,
                            now,
                        )?,
                        fd: None,
                    })
                })? {
                    return Ok(None);
                }
                self.phase = Phase::Activation;
            }
            Phase::Activation => {
                let Some(packet) = self.receive()? else {
                    self.phase = Phase::WaitingDrain;
                    return Ok(Some(false));
                };
                match activation_status(
                    self.session.as_mut().unwrap(),
                    packet,
                    self.join.as_ref().unwrap(),
                    self.config.request_id,
                    now,
                )? {
                    ActivationStatus::Active { published } => {
                        if self.consumer.as_ref().unwrap().watermarks().applied < published {
                            return Err(Error::Identity);
                        }
                        self.phase = Phase::Complete;
                    }
                    ActivationStatus::CatchUp { published } => {
                        if published < self.target {
                            return Err(Error::Identity);
                        }
                        self.target = published;
                        self.phase = Phase::Catchup;
                    }
                }
            }
            Phase::WaitingDrain => {
                let record = self.drain_one(now)?;
                self.phase = Phase::Activation;
                return Ok(record.then_some(true));
            }
            Phase::Complete => {}
        }
        Ok(Some(false))
    }
}
