use super::*;
use tickwire_ipc::HandoverReadyBody;
pub(crate) struct ReadyProof {
    directory: Option<PathBuf>,
    identity: Identity,
    last: Option<HandoverReadyBody>,
    account_input: Option<[u8; 32]>,
    receipt: PathBuf,
    exec_identity: Identity,
    confirmed: Option<u64>,
    seal_sequence: u64,
    pause_callbacks: bool,
    preflight: Option<preflight::Proof>,
    sources: std::collections::VecDeque<(HandoverReadyBody, [u8; 32])>,
}
impl ReadyProof {
    pub fn new(config: &Bootstrap) -> Result<Self> {
        let directory = if config.handover_enabled {
            let template: tickwire_strategy_sdk::recovery::Template = worker::read_config(
                config.recovery_template.as_ref().ok_or(Error::Invalid)?,
                2 * 1024 * 1024,
            )?;
            if template.owner != config.market.local.instance {
                return Err(Error::Invalid);
            }
            Some(template.output_directory)
        } else {
            None
        };
        Ok(Self {
            directory,
            identity: config.market.local.identity(),
            last: None,
            account_input: None,
            receipt: config.report_directory.join(format!(
                "CONSUMED-{}-{}.json",
                config
                    .market
                    .local
                    .instance
                    .iter()
                    .map(|b| format!("{b:02x}"))
                    .collect::<String>(),
                config.market.local.generation
            )),
            exec_identity: config.report.peer.identity(),
            confirmed: None,
            seal_sequence: 0,
            pause_callbacks: false,
            preflight: preflight::Proof::new(config)?,
            sources: Default::default(),
        })
    }
    pub fn poll(&mut self, lane: &mut AuthenticatedChild, now: u64) -> Result<()> {
        for _ in 0..16 {
            let packet = match lane.connection.try_recv() {
                Ok(Some(p)) => p,
                Ok(None) => break,
                Err(e) => return Err(e.into()),
            };
            if !packet.fds().is_empty() {
                return Err(Error::Invalid);
            }
            let frame = lane.session.validate(packet.bytes(), now)?;
            if frame.envelope.opcode == 38 {
                let envelope = frame.envelope.clone();
                let raw = frame.body.to_vec();
                self.preflight
                    .as_mut()
                    .ok_or(Error::Invalid)?
                    .observe(&envelope, &raw, lane)?;
                continue;
            }
            if frame.envelope.opcode != 37 || frame.envelope.flags != 32 {
                return Err(Error::Invalid);
            }
            let body = HandoverReadyBody::decode(frame.body)?;
            self.observe(body, lane.process.pid)?;
        }
        Ok(())
    }
    fn observe(&mut self, body: HandoverReadyBody, pid: u32) -> Result<()> {
        if body.instance_id != self.identity.instance
            || body.strategy_generation != self.identity.generation
            || body.logical_strategy_id != self.identity.logical_strategy
        {
            return Err(Error::Invalid);
        }
        if let Some(previous) = &self.last {
            if previous == &body {
                return Ok(());
            }
            if body.ready_sequence <= previous.ready_sequence
                || body.exec_report_sequence < previous.exec_report_sequence
            {
                return Err(Error::Invalid);
            }
        }
        let account_input = tickwire_strategy_sdk::handover::verify(
            self.directory.as_ref().ok_or(Error::Invalid)?,
            &body,
        )?;
        eprintln!(
            "{}",
            serde_json::json!({"event":"strategy_ready","worker_pid":pid,"ready_sequence":body.ready_sequence,"instance":body.instance_id,"generation":body.strategy_generation,"checkpoint_digest":body.checkpoint_digest,"exec_report_sequence":body.exec_report_sequence,"orders_digest":body.orders_digest,"ledger_sequence":body.ledger_sequence,"input_seal_digest":body.input_seal_digest})
        );
        self.account_input = Some(account_input);
        self.last = Some(body);
        Ok(())
    }
    pub fn pause_on_capture(&mut self) {
        self.pause_callbacks = true;
    }
    pub fn release_objects(&mut self, roots: [[u8; 32]; 3]) -> Result<Option<Vec<Vec<u8>>>> {
        self.preflight
            .as_mut()
            .map_or(Ok(None), |p| p.objects(roots))
    }
    /// A fresh authenticated challenge executes checkpoint after real callbacks;
    /// a prior periodic READY cannot answer it.
    pub fn capture(
        &mut self,
        lane: &mut AuthenticatedChild,
        reports: &Reports,
        sub: tickwire_shm::SubscriptionKey,
        clock: &mut SystemClock,
    ) -> Result<tickwire_runtime::node::HandoverReadiness> {
        self.seal_sequence = self.seal_sequence.checked_add(1).ok_or(Error::Invalid)?;
        let mut material = self.identity.instance.to_vec();
        material.extend(self.seal_sequence.to_le_bytes());
        material.extend(lane.session.epoch().to_le_bytes());
        let hash = tickwire_recovery::digest(&material);
        let challenge: [u8; 16] = hash[..16].try_into().map_err(|_| Error::Invalid)?;
        let now = clock.sample()?.monotonic.nanos();
        let until = now
            .checked_add(3_000_000_000)
            .ok_or(Error::Invalid)?
            .min(lane.session.deadline());
        if now >= until {
            return Err(Error::NotReady);
        }
        let body = tickwire_ipc::CheckpointSealBody {
            instance_id: self.identity.instance,
            logical_strategy_id: self.identity.logical_strategy,
            strategy_generation: self.identity.generation,
            challenge,
            report_sequence: reports.last_position(),
            valid_until_mono_ns: until,
            seal_mode: u16::from(self.pause_callbacks),
            schema: 1,
        };
        let raw = lane.session.encode(
            ControlEnvelope {
                opcode: 39,
                flags: 1,
                request_id: challenge,
                expected_state_version: body.report_sequence,
                ..Default::default()
            },
            &wire_bytes(&body)?,
        )?;
        let request = ControlEnvelope::decode(&raw[..128])?;
        let previous = self.last.as_ref().map_or(0, |r| r.ready_sequence);
        let deadline = Instant::now() + Duration::from_secs(3);
        lane.connection.send(&raw, &[], deadline)?;
        loop {
            let packet = lane.connection.recv(deadline)?;
            if !packet.fds().is_empty() {
                return Err(Error::Invalid);
            }
            let now = clock.sample()?.monotonic.nanos();
            if now >= until {
                return Err(Error::NotReady);
            }
            let frame = lane.session.validate(packet.bytes(), now)?;
            if frame.envelope.opcode == 38 {
                let envelope = frame.envelope.clone();
                let raw = frame.body.to_vec();
                self.preflight
                    .as_mut()
                    .ok_or(Error::Invalid)?
                    .observe(&envelope, &raw, lane)?;
                continue;
            }
            if frame.envelope.opcode == 37 && frame.envelope.flags == 32 {
                self.observe(HandoverReadyBody::decode(frame.body)?, lane.process.pid)?;
                continue;
            }
            let reply = tickwire_strategy_sdk::checkpoint::verify_reply(
                &frame.envelope,
                frame.body,
                &request,
                &body,
                previous,
            )?;
            self.observe(reply.ready, lane.process.pid)?;
            self.confirm(reports, sub, lane, now)?;
            let want = self.current().ok_or(Error::NotReady)?;
            if !self.matches(reports, sub, &want)? {
                return Err(Error::NotReady);
            }
            return Ok(want);
        }
    }
    /// Persist only a qualified child's authenticated READY whose complete input
    /// seal and actual registered report cursor agree with the execution owner.
    pub fn confirm(
        &mut self,
        reports: &Reports,
        sub: tickwire_shm::SubscriptionKey,
        lane: &AuthenticatedChild,
        now: u64,
    ) -> Result<()> {
        let Some(body) = &self.last else {
            return Ok(());
        };
        if self.confirmed == Some(body.ready_sequence) {
            return Ok(());
        }
        if lane.session.peer() != &self.identity
            || lane.process.pid == 0
            || now >= lane.session.deadline()
        {
            return Err(Error::Invalid);
        }
        if self.account_input.is_none()
            || self.account_input != reports.account_input_digest(body.exec_report_sequence)
            || !reports.confirms_cursor(
                sub,
                body.exec_report_sequence,
                body.report_ring_sequence,
            )?
        {
            return Ok(());
        }
        let receipt = serde_json::json!({"schema":1,"host_pid":std::process::id(),"worker_pid":lane.process.pid,"worker_uid":lane.process.uid,"worker_gid":lane.process.gid,
            "exec_instance":self.exec_identity.instance,"exec_generation":self.exec_identity.generation,"host_boot":self.exec_identity.host_boot,
            "instance":self.identity.instance,"generation":self.identity.generation,"logical_strategy":self.identity.logical_strategy,"build_hash":self.identity.build_hash,
            "cursor_resource":sub.cursor.to_le_bytes(),"subscription_generation":sub.generation,"bank_resource":sub.bank.resource.to_le_bytes(),"ring_epoch":sub.bank.ring_epoch,
            "ready_sequence":body.ready_sequence,"account_position":body.exec_report_sequence,"report_sequence":body.report_ring_sequence,"account_input_digest":self.account_input,
            "ledger_sequence":body.ledger_sequence,"orders_digest":body.orders_digest,"checkpoint_digest":body.checkpoint_digest,"input_seal_digest":body.input_seal_digest,"confirmed_mono_ns":now});
        write_json(&self.receipt, &receipt)?;
        self.confirmed = Some(body.ready_sequence);
        self.sources
            .push_back((body.clone(), self.account_input.ok_or(Error::Invalid)?));
        if self.sources.len() > 64 {
            self.sources.pop_front();
        }
        eprintln!(
            "{}",
            serde_json::json!({"event":"strategy_reports_consumed","worker_pid":lane.process.pid,"instance":self.identity.instance,"generation":self.identity.generation,"account_position":body.exec_report_sequence,"report_sequence":body.report_ring_sequence,"ready_sequence":body.ready_sequence})
        );
        Ok(())
    }
    pub fn current(&self) -> Option<tickwire_runtime::node::HandoverReadiness> {
        let body = self.last.as_ref()?;
        Some(tickwire_runtime::node::HandoverReadiness {
            instance_id: body.instance_id,
            strategy_generation: body.strategy_generation,
            checkpoint_digest: body.checkpoint_digest,
            exec_report_sequence: body.exec_report_sequence,
            orders_digest: body.orders_digest,
            ledger_sequence: body.ledger_sequence,
            input_seal_digest: body.input_seal_digest,
        })
    }
    pub fn matches(
        &self,
        reports: &Reports,
        sub: tickwire_shm::SubscriptionKey,
        want: &tickwire_runtime::node::HandoverReadiness,
    ) -> Result<bool> {
        let Some(body) = &self.last else {
            return Ok(false);
        };
        Ok(
            self.account_input == reports.account_input_digest(body.exec_report_sequence)
                && self.account_input.is_some()
                && tickwire_strategy_sdk::handover::matches(body, want)
                && reports.confirms_cursor(
                    sub,
                    body.exec_report_sequence,
                    body.report_ring_sequence,
                )?,
        )
    }
    pub fn objects(
        &self,
        reports: &Reports,
        sub: tickwire_shm::SubscriptionKey,
        want: &tickwire_runtime::node::HandoverReadiness,
    ) -> Result<Option<Vec<Vec<u8>>>> {
        let historical = self
            .sources
            .iter()
            .find(|(b, _)| tickwire_strategy_sdk::handover::matches(b, want));
        let (body, expected) = if self.matches(reports, sub, want)? {
            (
                self.last.as_ref().ok_or(Error::NotReady)?,
                self.account_input.ok_or(Error::Invalid)?,
            )
        } else if let Some((b, digest)) = historical {
            (b, *digest)
        } else {
            return Ok(None);
        };
        let (digest, objects) = tickwire_strategy_sdk::handover::objects(
            self.directory.as_ref().ok_or(Error::NotReady)?,
            body,
        )?;
        if digest != expected {
            return Err(Error::Invalid);
        }
        Ok(Some(objects))
    }
}
