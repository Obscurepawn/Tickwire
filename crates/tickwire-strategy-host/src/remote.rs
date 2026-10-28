//! Strategy connects directly to independent MD; execution owns only intent/report lanes.
use super::*;
pub struct RemoteLaunch {
    pub spec: LaunchSpec,
    pub bootstrap: Bootstrap,
    pub directory_parent: PathBuf,
    pub supervisor: Identity,
    pub parent_process: ProcessIdentity,
    pub md_admission_directory: PathBuf,
    pub snapshot_id: [u8; 16],
}
type Intake<R> = RuntimeStrategy<R, fn(&tickwire_binance_public::Notice<'_>) -> bool>;
pub struct RemoteHostedStrategy<R> {
    pub(crate) readiness: readiness::ReadyProof,
    pub(crate) market_transition: market_transition::MarketTransition,
    pub(crate) report_sub: tickwire_shm::SubscriptionKey,
    pub(crate) strategy: Intake<R>,
    pub(crate) reports: Reports,
    pub(crate) registry: Registry,
    pub(crate) identity: Identity,
    pub(crate) intent: AuthenticatedChild,
    pub(crate) report: AuthenticatedChild,
    pub(crate) clock: SystemClock,
    pub(crate) policy: ControlPlane,
    pub(crate) forwarding: bool,
    pub(crate) error: Option<Error>,
    pub(crate) _directory: PrivateDirectory,
    pub(crate) _intent_listener: ControlListener,
    pub(crate) _report_listener: ControlListener,
}
impl<R: RecoveryResolver + 'static> RemoteHostedStrategy<R> {
    /// # Safety
    /// Control-provided policies identify qualified MD, strategy and execution
    /// SDK builds. The launcher owns Child until its actual exit is observed.
    #[allow(unsafe_code)]
    pub unsafe fn launch(
        mut config: RemoteLaunch,
        resolver: R,
        policy: ControlPlane,
    ) -> Result<Self> {
        let external = config
            .bootstrap
            .external_market
            .as_ref()
            .ok_or(Error::Invalid)?;
        if config.spec.identity != external.endpoint.local.identity()
            || config.spec.identity != config.bootstrap.intent.local.identity()
            || config.spec.identity != config.bootstrap.report.local.identity()
            || config.bootstrap.market.peer.identity() != external.endpoint.peer.identity()
            || config.bootstrap.report.peer.identity() != config.bootstrap.intent.peer.identity()
            || config.parent_process.pid != std::process::id()
            || config.spec.uid != config.parent_process.uid
            || config.spec.gid != config.parent_process.gid
            || config.supervisor.role != 1
        {
            return Err(Error::Invalid);
        }
        let domain = external.domain()?;
        let mut clock = SystemClock::new(domain)?;
        let deadline = Instant::now()
            .checked_add(Duration::from_millis(config.bootstrap.startup_timeout_ms))
            .ok_or(Error::Invalid)?;
        let directory = PrivateDirectory::create_in(&config.directory_parent, "tickwire-strategy")?;
        config.bootstrap.intent.path = directory.path().join("intent.sock");
        config.bootstrap.report.path = directory.path().join("report.sock");
        config.bootstrap.market_admission_ready = Some(directory.path().join("md-ready.json"));
        let bootstrap_file = directory.path().join("worker.json");
        match std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&config.bootstrap.command_directory)
        {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(e.into()),
        }
        for endpoint in [&mut config.bootstrap.intent, &mut config.bootstrap.report] {
            endpoint.peer_pid = config.parent_process.pid;
            endpoint.peer_uid = config.parent_process.uid;
            endpoint.peer_gid = config.parent_process.gid;
        }
        let intent_listener = ControlListener::bind(&config.bootstrap.intent.path)?;
        let report_listener = ControlListener::bind(&config.bootstrap.report.path)?;
        let report_header = config.bootstrap.report_resource()?;
        if report_header.producer_instance != config.bootstrap.report.peer.instance
            || report_header.producer_generation != config.bootstrap.report.peer.generation
            || report_header.consumer_instance != config.spec.identity.instance
            || report_header.host_boot_id != config.spec.identity.host_boot
            || report_header.namespace_id != config.spec.identity.namespace
        {
            return Err(Error::Invalid);
        }
        let report_publisher = tickwire_shm::Publisher::<1>::create(report_header)?;
        config.bootstrap.report_header = wire_bytes(report_publisher.header())?;
        let report_objects =
            tickwire_recovery::FileBlobStore::open(&config.bootstrap.report_directory)?;
        preflight::prepare(&config.bootstrap, &config.spec)?;
        write_json(&bootstrap_file, &config.bootstrap)?;
        config.spec.arguments = vec![
            "--bootstrap".into(),
            bootstrap_file.to_str().ok_or(Error::Invalid)?.into(),
        ];
        let mut registry = Registry::new(1)?;
        let child = registry.spawn(config.spec.clone())?;
        let admission = tickwire_md::server::Admission {
            client: config
                .bootstrap
                .external_market
                .clone()
                .ok_or(Error::Invalid)?,
            pid: child.pid,
            uid: child.uid,
            gid: child.gid,
            supervisor: (&config.supervisor).into(),
            supervisor_pid: config.parent_process.pid,
            snapshot_id: config.snapshot_id,
        };
        tickwire_md::server::register_admission(&config.md_admission_directory, &admission)
            .map_err(|e| Error::Io(std::io::Error::other(e.to_string())))?;
        write_json(
            config
                .bootstrap
                .market_admission_ready
                .as_ref()
                .ok_or(Error::Invalid)?,
            &true,
        )?;
        // Registry owns this same child; a second lane does not manufacture another process identity.
        let authenticated = registry.accept(
            &intent_listener,
            server_policy(&config.bootstrap.intent, child),
            clock.sample()?.monotonic.nanos(),
            deadline,
        )?;
        // SAFETY: this launcher owns the qualified child authenticated above.
        let (receiver, intent) =
            unsafe { connect_intent(&config.bootstrap, authenticated, &mut clock, deadline)? };
        let expected = registry.process(config.spec.identity.instance)?;
        let mut report_connection = report_listener.accept(deadline)?;
        let credentials = report_connection.peer_credentials();
        let report_process = ProcessIdentity {
            pid: credentials.pid,
            uid: credentials.uid,
            gid: credentials.gid,
        };
        if report_process != expected {
            return Err(Error::Invalid);
        }
        let hello = report_connection.recv(deadline)?;
        if !hello.fds().is_empty() {
            return Err(Error::Invalid);
        }
        let (report_session, welcome) = Session::accept(
            &server_policy(&config.bootstrap.report, child),
            report_process,
            hello.bytes(),
            clock.sample()?.monotonic.nanos(),
        )?;
        report_connection.send(&welcome, &[], deadline)?;
        registry.process(config.spec.identity.instance)?;
        let report_sub = tickwire_shm::SubscriptionKey {
            bank: report_publisher.key(),
            instance: u128::from_le_bytes(config.spec.identity.instance),
            generation: config.bootstrap.report_subscription_generation,
            cursor: u128::from_le_bytes(config.bootstrap.report_cursor_id),
        };
        let mut report = AuthenticatedChild {
            connection: report_connection,
            session: report_session,
            process: report_process,
        };
        // SAFETY: this launcher retains the qualified owned child and authenticated lane.
        let reports = unsafe {
            connect_reports(
                report_publisher,
                report_objects,
                report_sub,
                ReportIdentity {
                    logical_strategy_id: config.spec.identity.logical_strategy,
                    instance_id: config.spec.identity.instance,
                    strategy_generation: config.spec.identity.generation,
                    risk_domain: config.bootstrap.risk_domain,
                },
                &mut report,
                &mut clock,
                deadline,
            )?
        };
        fn ignore(_: &tickwire_binance_public::Notice<'_>) -> bool {
            true
        }
        let market_transition = market_transition::MarketTransition::new(
            config
                .bootstrap
                .external_market
                .clone()
                .ok_or(Error::Invalid)?,
            config.bootstrap.command_directory.join("merged-inputs"),
            report_process,
        );
        Ok(Self {
            market_transition,
            readiness: readiness::ReadyProof::new(&config.bootstrap, &config.spec)?,
            report_sub,
            strategy: RuntimeStrategy::new(
                receiver,
                resolver,
                ignore as fn(&tickwire_binance_public::Notice<'_>) -> bool,
            ),
            reports,
            registry,
            identity: config.spec.identity,
            clock,
            policy,
            forwarding: true,
            error: None,
            intent,
            report,
            _directory: directory,
            _intent_listener: intent_listener,
            _report_listener: report_listener,
        })
    }
    /// Actual owned remote consumer; discovering it grants no market authority.
    pub fn market_consumer(&self) -> Option<(u32, [u8; 16], u64)> {
        if self.failed() {
            return None;
        }
        Some((
            self.report.process.pid,
            self.identity.instance,
            self.identity.generation,
        ))
    }
    /// Caller has accepted the operation through its trusted runtime control authority.
    pub fn arm_market(
        &mut self,
        approval: &tickwire_market_source::transition::Approval,
    ) -> Result<()> {
        self.registry.process(self.identity.instance)?;
        self.market_transition.arm(
            approval,
            &self.report,
            &mut self.reports,
            self.clock.sample()?.monotonic.nanos(),
        )
    }
    pub fn commit_market(
        &mut self,
        commit: &tickwire_market_source::transition::Commit,
    ) -> Result<()> {
        self.registry.process(self.identity.instance)?;
        self.market_transition.commit(
            commit,
            &self.report,
            &mut self.reports,
            self.clock.sample()?.monotonic.nanos(),
        )
    }
    pub fn market_transition(&self) -> Option<&tickwire_market_source::transition::Receipt> {
        self.market_transition.receipt()
    }
    pub fn market_rejection(&self) -> Option<u32> {
        self.market_transition.rejection()
    }
    pub fn set_shadow(&mut self) {
        self.forwarding = false;
    }
    pub fn instance(&self) -> ([u8; 16], u64) {
        (self.identity.instance, self.identity.generation)
    }
    pub fn failed(&self) -> bool {
        self.error.is_some() || self.strategy.error().is_some()
    }
    pub(crate) fn fail(&mut self, e: Error) {
        eprintln!(
            "{}",
            serde_json::json!({"event":"strategy_host_failed","instance":self.identity.instance,"generation":self.identity.generation,"error":e.to_string()})
        );
        let _ = self.policy.stop_strategy_instance(
            self.identity.logical_strategy,
            self.identity.instance,
            self.identity.generation,
        );
        self.strategy.start_quiesce();
        let _ = self.registry.terminate(self.identity.instance);
        self.error = Some(e);
    }
    pub(crate) fn maintain(&mut self) -> Result<()> {
        if !self.registry.observe_exits()?.is_empty() {
            return Err(Error::NotReady);
        }
        self.registry.process(self.identity.instance)?;
        let now = self.clock.sample()?.monotonic.nanos();
        self.market_transition.flush(&mut self.report, now)?;
        self.readiness
            .poll_with(&mut self.report, now, |envelope, bytes| {
                self.market_transition.observe(envelope, bytes, now)
            })?;
        self.readiness
            .confirm(&self.reports, self.report_sub, &self.report, now)?;
        if now >= self.report.session.deadline() {
            return Err(Error::NotReady);
        }
        {
            let lane = &mut self.intent;
            if now >= lane.session.deadline() {
                return Err(Error::NotReady);
            }
            match lane.connection.try_recv() {
                Ok(Some(packet)) => {
                    lane.session.validate(packet.bytes(), now)?;
                    return Err(Error::NotReady);
                }
                Ok(None) => {}
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }
    pub(crate) fn graceful_retire(&mut self) -> Result<()> {
        self.strategy.start_quiesce();
        self.registry.process(self.identity.instance)?;
        let bank = self.reports.publisher().header().clone();
        let membership = self.reports.publisher().membership_version();
        let mut id = self.identity.instance;
        id[0] ^= 0x60;
        let raw = tickwire_strategy_sdk::retirement::request(
            &self.report.session,
            &bank,
            self.report_sub,
            membership,
            id,
        )?;
        let request = ControlEnvelope::decode(&raw[..128])?;
        let expected = tickwire_ipc::QuiesceBody::decode(&raw[128..])?;
        let deadline = Instant::now() + Duration::from_secs(4);
        self.report.connection.send(&raw, &[], deadline)?;
        loop {
            let packet = self.report.connection.recv(deadline)?;
            if !packet.fds().is_empty() {
                return Err(Error::Invalid);
            }
            let frame = self
                .report
                .session
                .validate(packet.bytes(), self.clock.sample()?.monotonic.nanos())?;
            if frame.envelope.opcode == 37 && frame.envelope.flags == 32 {
                // A prior report callback may have completed before QUIESCE arrived.
                // It does not complete retirement; retain the authenticated identity check.
                let ready = tickwire_ipc::HandoverReadyBody::decode(frame.body)?;
                if ready.instance_id != self.identity.instance
                    || ready.strategy_generation != self.identity.generation
                    || ready.logical_strategy_id != self.identity.logical_strategy
                {
                    return Err(Error::Invalid);
                }
                continue;
            }
            let proof = tickwire_strategy_sdk::retirement::check_reply(
                &frame.envelope,
                frame.body,
                &request,
                &expected,
            )?;
            self.reports.publisher().detach_registered(
                self.report_sub,
                tickwire_shm::Watermarks {
                    read: proof.released_seq,
                    released: proof.released_seq,
                    applied: proof.applied_seq,
                },
            )?;
            break;
        }
        // A validated QUIESCED cursor alone is not an exit observation.
        loop {
            if let Some(exit) = self
                .registry
                .observe_exits()?
                .into_iter()
                .find(|e| e.instance() == self.identity.instance)
            {
                if !exit.status().success() {
                    return Err(Error::NotReady);
                }
                return retire_owned_child(&mut self.registry, self.identity.instance);
            }
            if Instant::now() >= deadline {
                return Err(Error::NotReady);
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    }
    fn report_result(&mut self, r: Result<()>) -> bool {
        match r {
            Ok(()) => true,
            Err(Error::Sdk(tickwire_strategy_sdk::Error::Ring(
                tickwire_shm::MappedError::Protocol(tickwire_shm::Error::Full),
            ))) => false,
            Err(e) => {
                self.fail(e);
                false
            }
        }
    }
}
impl<R: RecoveryResolver + 'static> tickwire_runtime::node::Strategy for RemoteHostedStrategy<R> {
    fn handover_quiesce(&mut self, c: &tickwire_runtime::node::StrategySwitch) -> bool {
        if self.identity.logical_strategy == c.logical_strategy_id
            && [self.instance()].iter().any(|i| {
                *i == (c.old_instance, c.old_generation) || *i == (c.new_instance, c.new_generation)
            })
        {
            self.forwarding = false;
            if self.instance() == (c.old_instance, c.old_generation) {
                self.readiness.pause_on_capture();
            }
            true
        } else {
            false
        }
    }
    fn handover_activate(&mut self, c: &tickwire_runtime::node::StrategySwitch) -> bool {
        if self.identity.logical_strategy != c.logical_strategy_id {
            return false;
        }
        if self.instance() == (c.old_instance, c.old_generation) {
            self.forwarding = false;
            self.strategy.start_quiesce();
            let result = (|| {
                self.policy.stop_strategy_instance(
                    self.identity.logical_strategy,
                    self.identity.instance,
                    self.identity.generation,
                )?;
                self.graceful_retire()
            })();
            let retired = result.is_ok();
            if let Err(error) = &result {
                eprintln!(
                    "{}",
                    serde_json::json!({"event":"strategy_retirement_failed","instance":self.identity.instance,"generation":self.identity.generation,"error":error.to_string()})
                );
            }
            self.error = Some(result.err().unwrap_or(Error::NotReady));
            return retired;
        }
        if self.instance() != (c.new_instance, c.new_generation) {
            return false;
        }
        if self.failed() {
            return false;
        }
        let allowed = self.clock.sample().ok().is_some_and(|n| {
            self.policy
                .strategy_instance_authorized(
                    self.identity.logical_strategy,
                    self.identity.instance,
                    self.identity.generation,
                    n.monotonic,
                )
                .unwrap_or(false)
        });
        if allowed {
            self.forwarding = true;
        }
        allowed
    }
    fn handover_halt(&mut self, id: u64) -> bool {
        if id != self.identity.logical_strategy {
            return false;
        }
        self.forwarding = false;
        true
    }
    fn release_preflight(
        &mut self,
        instance: [u8; 16],
        generation: u64,
        roots: [[u8; 32]; 3],
    ) -> Option<Vec<Vec<u8>>> {
        if self.instance() != (instance, generation) || self.failed() {
            return None;
        }
        let result = (|| {
            self.maintain()?;
            self.readiness.release_objects(roots)
        })();
        match result {
            Ok(v) => v,
            Err(e) => {
                self.fail(e);
                None
            }
        }
    }
    fn handover_checkpoint(
        &mut self,
        instance: [u8; 16],
        generation: u64,
    ) -> Option<(tickwire_runtime::node::HandoverReadiness, Vec<Vec<u8>>)> {
        if self.instance() != (instance, generation) || self.failed() {
            return None;
        }
        if let Err(e) = self.maintain() {
            self.fail(e);
            return None;
        }
        let want = match self.readiness.capture(
            &mut self.report,
            &self.reports,
            self.report_sub,
            &mut self.clock,
        ) {
            Ok(w) => w,
            Err(e) => {
                self.fail(e);
                return None;
            }
        };
        let objects = self.handover_objects(&want)?;
        Some((want, objects))
    }
    fn handover_replay_source(
        &mut self,
        operation: &tickwire_release::Operation,
        member: &tickwire_release::Member,
        fixed: &tickwire_runtime::node::HandoverReadiness,
    ) -> Option<tickwire_release::replay::source::VerifiedSourceReplay> {
        if self.instance() != (member.old_instance, member.old_generation)
            || self.forwarding
            || self.failed()
        {
            return None;
        }
        let fixed_objects = self.handover_objects(fixed)?;
        let (current, current_objects) =
            self.handover_checkpoint(member.old_instance, member.old_generation)?;
        match self.readiness.source_replay(
            operation,
            member,
            fixed.clone(),
            fixed_objects,
            current,
            current_objects,
        ) {
            Ok(v) => Some(v),
            Err(e) => {
                self.fail(e);
                None
            }
        }
    }
    fn handover_install_replay(
        &mut self,
        source: &tickwire_release::replay::source::VerifiedSourceReplay,
    ) -> Option<tickwire_runtime::node::InstalledReplay> {
        let member = &source.completion().member;
        if self.instance() != (member.new_instance, member.new_generation)
            || self.forwarding
            || self.failed()
        {
            return None;
        }
        let result = (|| {
            self.maintain()?;
            self.readiness.install_replay(
                &mut self.report,
                &mut self.reports,
                self.report_sub,
                &mut self.clock,
                source,
            )
        })();
        match result {
            Ok(value) => Some(value),
            Err(e) => {
                self.fail(e);
                None
            }
        }
    }
    fn handover_finish_replay(
        &mut self,
        installed: tickwire_runtime::node::InstalledReplay,
    ) -> Option<tickwire_runtime::node::PreparedReplay> {
        let member = &installed.verified().request.member;
        if self.instance() != (member.new_instance, member.new_generation)
            || self.forwarding
            || self.failed()
        {
            return None;
        }
        let result = self.readiness.finish_replay(
            &mut self.report,
            &mut self.reports,
            self.report_sub,
            &mut self.clock,
            installed,
        );
        match result {
            Ok(v) => Some(v),
            Err(e) => {
                self.fail(e);
                None
            }
        }
    }
    fn handover_objects(
        &mut self,
        want: &tickwire_runtime::node::HandoverReadiness,
    ) -> Option<Vec<Vec<u8>>> {
        if self.instance() != (want.instance_id, want.strategy_generation)
            || self.failed()
            || self.registry.process(self.identity.instance).is_err()
        {
            return None;
        }
        let now = self.clock.sample().ok()?.monotonic.nanos();
        self.readiness
            .objects(&self.report, now, &self.reports, self.report_sub, want)
            .ok()
            .flatten()
    }
    fn handover_ready(&mut self, want: &tickwire_runtime::node::HandoverReadiness) -> bool {
        if self.failed() {
            return false;
        }
        let result = (|| {
            self.maintain()?;
            self.readiness.matches(&self.reports, self.report_sub, want)
        })();
        match result {
            Ok(v) => v,
            Err(e) => {
                self.fail(e);
                false
            }
        }
    }
    fn on_market(&mut self, _: &tickwire_binance_public::Notice<'_>) -> bool {
        !self.failed()
    }
    fn next(
        &mut self,
        a: &tickwire_account::Account,
        h: tickwire_runtime::node::Health,
        n: tickwire_binance_clock::LocalTime,
    ) -> Option<tickwire_runtime::node::Submission> {
        if self.failed() {
            return None;
        }
        if let Err(e) = self.maintain() {
            self.fail(e);
            return None;
        }
        if !self.forwarding {
            if let Err(e) = self.strategy.drain_shadow(32) {
                self.fail(e.into());
            }
            return None;
        }
        let next = tickwire_runtime::node::Strategy::next(&mut self.strategy, a, h, n);
        if self.strategy.error().is_some() {
            self.fail(Error::NotReady);
            None
        } else {
            next
        }
    }
    fn on_account_baseline(
        &mut self,
        b: &tickwire_account_codec::CanonicalBaseline,
        _: &tickwire_account::Account,
    ) -> bool {
        if self.failed() {
            return false;
        }
        let r = (|| {
            self.reports
                .publish_baseline(b, self.clock.sample()?.monotonic.nanos())?;
            Ok(())
        })();
        self.report_result(r)
    }
    fn account_report_position(&self) -> Option<u64> {
        Some(self.reports.last_position())
    }
    fn on_account_record(
        &mut self,
        p: u64,
        r: &tickwire_account_codec::CanonicalRecord,
        _: &tickwire_account_codec::CanonicalBaseline,
        _: &tickwire_account::Account,
    ) -> bool {
        if self.failed() {
            return false;
        }
        let result = (|| {
            self.reports
                .publish_record(p, r, self.clock.sample()?.monotonic.nanos())?;
            Ok(())
        })();
        self.report_result(result)
    }
    fn on_submission(
        &mut self,
        c: &tickwire_venue::CanonicalCommand,
        r: &std::result::Result<tickwire_account_gateway::Status, tickwire_binance_account::Error>,
    ) {
        tickwire_runtime::node::Strategy::on_submission(&mut self.strategy, c, r)
    }
}

/// Shared by qualified launch modes only. The caller owns the authenticated
/// child and guarantees the SDK cursor and immutable mapping contracts.
#[allow(unsafe_code)]
pub(super) unsafe fn connect_reports(
    mut publisher: tickwire_shm::Publisher<1>,
    objects: tickwire_recovery::FileBlobStore,
    sub: tickwire_shm::SubscriptionKey,
    identity: ReportIdentity,
    lane: &mut AuthenticatedChild,
    clock: &mut SystemClock,
    deadline: Instant,
) -> Result<Reports> {
    if lane.session.peer().instance != identity.instance_id
        || sub.instance.to_le_bytes() != identity.instance_id
        || publisher.header().consumer_instance != identity.instance_id
    {
        return Err(Error::Invalid);
    }
    publisher.join(sub)?;
    let membership = publisher.membership_version();
    let expiry = lane.session.deadline();
    let offer = worker::resource_offer(
        &lane.session,
        publisher.header(),
        [14; 16],
        expiry,
        membership,
        false,
    )?;
    let fd = publisher.reader_fd()?;
    lane.connection.send(&offer, &[fd.as_fd()], deadline)?;
    let header = worker::cursor_header(publisher.header(), sub)?;
    let cursor = worker::accept_offer(
        &mut lane.session,
        lane.connection.recv(deadline)?,
        &header,
        [15; 16],
        expiry,
        membership,
        false,
        clock.sample()?.monotonic.nanos(),
    )?;
    // SAFETY: the qualified launcher retains this exact authenticated child's cursor.
    unsafe {
        publisher.register_cursor(sub, cursor)?;
    }
    Ok(ReportWriter::new(publisher, objects, identity)?)
}

/// Reuses the original qualified launcher intent attachment after authenticating
/// the actual owned process. No second process or synthetic cursor is created.
#[allow(unsafe_code)]
pub(crate) unsafe fn connect_intent(
    config: &Bootstrap,
    authenticated: AuthenticatedChild,
    clock: &mut SystemClock,
    deadline: Instant,
) -> Result<(IntentReceiver, AuthenticatedChild)> {
    let authenticated_uid = authenticated.process.uid;
    let logical_strategy = authenticated.session.peer().logical_strategy;
    let AuthenticatedChild {
        mut connection,
        mut session,
        process: actual,
    } = authenticated;
    let mut bank = config.intent_resource()?;
    bank.descriptor_hash = [0; 32];
    // Publisher computes this immutable digest before creating the actual mapping.
    bank.descriptor_hash = header_hash(&bank)?;
    let membership = bank
        .initial_membership_version
        .checked_add(1)
        .ok_or(Error::Invalid)?;
    let expires = session.deadline();
    let fd = worker::accept_offer(
        &mut session,
        connection.recv(deadline)?,
        &bank,
        [7; 16],
        expires,
        membership,
        false,
        clock.sample()?.monotonic.nanos(),
    )?;
    let sub = tickwire_shm::SubscriptionKey {
        bank: tickwire_shm::BankKey {
            resource: u128::from_le_bytes(bank.resource_id),
            channel: u128::from_le_bytes(bank.channel_id),
            channel_epoch: bank.channel_epoch,
            ring_epoch: bank.ring_epoch,
            feed_generation: bank.feed_generation,
        },
        instance: u128::from_le_bytes(config.intent.peer.instance),
        generation: config.intent_subscription_generation,
        cursor: u128::from_le_bytes(config.intent_cursor_id),
    };
    let objects =
        tickwire_recovery::FileBlobReader::open(&config.command_directory, authenticated_uid)?;
    // SAFETY: the same actual authenticated child is the sole writer and uses the qualified worker SDK.
    let receiver = unsafe {
        IntentReceiver::attach(
            fd,
            bank.clone(),
            sub,
            objects,
            config.risk_domain,
            logical_strategy,
        )?
    };
    let cursor_header = worker::cursor_header(&bank, sub)?;
    let cursor = receiver.cursor_fd()?;
    let offered = worker::resource_offer(
        &session,
        &cursor_header,
        [8; 16],
        session.deadline(),
        membership,
        false,
    )?;
    connection.send(&offered, &[cursor.as_fd()], deadline)?;

    Ok((
        receiver,
        AuthenticatedChild {
            connection,
            session,
            process: actual,
        },
    ))
}
