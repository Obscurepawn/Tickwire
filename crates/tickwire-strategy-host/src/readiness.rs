use super::*;
use tickwire_ipc::HandoverReadyBody;
mod cache;
mod fresh;
pub(crate) struct ReadyProof {
    fresh: fresh::State,
    deferred: Option<HandoverReadyBody>,
    resources: Option<ProcessResources>,
    context: Option<cache::Context>,
    cache: cache::Cache,
    directory: Option<PathBuf>,
    identity: Identity,
    last: Option<HandoverReadyBody>,
    account_input: Option<[u8; 32]>,
    receipt: PathBuf,
    exec_identity: Identity,
    confirmed: Option<u64>,
    seal_sequence: u64,
}
impl ReadyProof {
    pub fn new(config: &Bootstrap, resources: Option<ProcessResources>) -> Result<Self> {
        resources::validate(resources.as_ref())?;
        let directory = if config.publish_checkpoints {
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
            fresh: Default::default(),
            deferred: None,
            context: None,
            cache: Default::default(),
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
            resources,
        })
    }
    pub fn poll(
        &mut self,
        lane: &mut AuthenticatedChild,
        now: u64,
        clock: &mut SystemClock,
    ) -> Result<u64> {
        self.poll_with(
            lane,
            now,
            || Ok(clock.sample()?.monotonic.nanos()),
            |_, _, _| Ok(false),
        )
    }
    pub fn poll_with(
        &mut self,
        lane: &mut AuthenticatedChild,
        mut now: u64,
        mut receive_time: impl FnMut() -> Result<u64>,
        mut extra: impl FnMut(&ControlEnvelope, &[u8], u64) -> Result<bool>,
    ) -> Result<u64> {
        self.fresh.flush(lane, now)?;
        if self.fresh.active.is_none()
            && let Some(body) = self.deferred.take()
        {
            self.observe(body, lane, now)?;
        }
        for _ in 0..16 {
            let packet = match lane.connection.try_recv() {
                Ok(Some(p)) => p,
                Ok(None) => break,
                Err(e) => return Err(e.into()),
            };
            if !packet.fds().is_empty() {
                return Err(Error::Invalid);
            }
            now = receive_time()?;
            let frame = lane.session.validate(packet.bytes(), now)?;
            if self.observe_fresh_reply(&frame.envelope, frame.body)? {
                continue;
            }
            if extra(&frame.envelope, frame.body, now)? {
                continue;
            }
            if frame.envelope.opcode != 37 || frame.envelope.flags != 32 {
                return Err(Error::Invalid);
            }
            let body = HandoverReadyBody::decode(frame.body)?;
            if self.fresh.active.is_some() {
                self.fresh.announcement(&body)?;
                self.deferred = Some(body);
            } else {
                self.observe(body, lane, now)?;
            }
        }
        Ok(now)
    }
    fn context(&mut self, lane: &AuthenticatedChild, now: u64) -> Result<cache::Context> {
        let context = cache::Context::from_lane(lane);
        if context.identity != self.identity
            || !context.live(now)
            || self.context.as_ref().is_some_and(|old| old != &context)
        {
            self.cache.clear();
            self.account_input = None;
            return Err(Error::Invalid);
        }
        Ok(context)
    }
    fn observe(
        &mut self,
        body: HandoverReadyBody,
        lane: &AuthenticatedChild,
        now: u64,
    ) -> Result<()> {
        let context = match self.context(lane, now) {
            Ok(value) => value,
            Err(error) => {
                self.cache.clear();
                self.account_input = None;
                return Err(error);
            }
        };
        let pid = lane.process.pid;
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
        let (account_input, _) = self.cache.load(
            self.directory.as_ref().ok_or(Error::Invalid)?,
            &context,
            &body,
            now,
        )?;
        self.context = Some(context);
        eprintln!(
            "{}",
            serde_json::json!({"event":"strategy_ready","worker_pid":pid,"ready_sequence":body.ready_sequence,"instance":body.instance_id,"generation":body.strategy_generation,"checkpoint_digest":body.checkpoint_digest,"exec_report_sequence":body.exec_report_sequence,"orders_digest":body.orders_digest,"ledger_sequence":body.ledger_sequence,"input_seal_digest":body.input_seal_digest})
        );
        self.account_input = Some(account_input);
        self.last = Some(body);
        Ok(())
    }
    pub fn confirm(
        &mut self,
        reports: &Reports,
        sub: tickwire_shm::SubscriptionKey,
        lane: &AuthenticatedChild,
        now: u64,
    ) -> Result<()> {
        self.context(lane, now)?;
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
        eprintln!(
            "{}",
            serde_json::json!({"event":"strategy_reports_consumed","worker_pid":lane.process.pid,"instance":self.identity.instance,"generation":self.identity.generation,"account_position":body.exec_report_sequence,"report_sequence":body.report_ring_sequence,"ready_sequence":body.ready_sequence})
        );
        Ok(())
    }
}

#[cfg(test)]
pub(super) fn test_proof(identity: Identity) -> ReadyProof {
    ReadyProof {
        fresh: Default::default(),
        deferred: None,
        resources: None,
        context: None,
        cache: Default::default(),
        directory: None,
        identity: identity.clone(),
        last: None,
        account_input: None,
        receipt: PathBuf::new(),
        exec_identity: identity,
        confirmed: None,
        seal_sequence: 0,
    }
}
