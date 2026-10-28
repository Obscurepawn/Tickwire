use super::*;
use tickwire_market_source::protocol::wire_bytes;

impl Control {
    pub(super) fn reply(
        &mut self,
        action: Action,
        result: std::result::Result<cold::Output, String>,
        runtime: &mut Runtime,
        now: u64,
    ) -> Result<()> {
        let mut output = result.map_err(|message| invalid(&message))?;
        if let cold::Output::Persisted(store) = output {
            let durable = self
                .durable
                .as_mut()
                .ok_or_else(|| invalid("unexpected MD persistence reply"))?;
            if durable.store.is_some() || !matches!(action, Action::Persist(_)) {
                return Err(invalid("MD persistence ownership"));
            }
            durable.store = Some(store);
            output = cold::Output::Written;
        }
        if matches!(action, Action::StartupEffective) {
            if !matches!(output, cold::Output::Written) {
                return Err(invalid("MD startup effective reply"));
            }
            return self.persist(After::Startup, RecoveryPhase::Ready, runtime);
        }
        if let Action::Persist(after @ (After::Startup | After::Report | After::Bank)) = action {
            if !matches!(output, cold::Output::Written) {
                return Err(invalid("MD persistence reply"));
            }
            let durable = self
                .durable
                .as_mut()
                .ok_or_else(|| invalid("MD persistence missing"))?;
            if after == After::Startup {
                durable.ready = true;
            }
            if after == After::Report {
                durable.report_phase = None;
            }
            return Ok(());
        }
        if matches!(action, Action::Report) {
            if !matches!(output, cold::Output::Written) {
                return Err(invalid("MD result reply"));
            }
            self.reporting = None;
            return Ok(());
        }
        if matches!(action, Action::Plan) {
            let cold::Output::Read(mut values) = output else {
                return Err(invalid("MD plan reply"));
            };
            if let Some(Some(bytes)) = values.pop() {
                let hash: [u8; 32] = Sha256::digest(&bytes).into();
                if self.last_bytes != Some(hash) {
                    self.last_bytes = Some(hash);
                    if let Err(error) = self.prepare(&bytes, runtime, now) {
                        if self.flight.is_some() {
                            return Err(error);
                        }
                        let plan = Plan::decode(&bytes).ok();
                        let digest = plan
                            .as_ref()
                            .map(|p| encode(p, crate::expansion::MAX_PLAN_BYTES))
                            .transpose()?
                            .map_or(hash, |bytes| Sha256::digest(bytes).into());
                        let record = Record {
                            schema: 1,
                            operation_id: plan.as_ref().map_or([0; 16], |p| p.operation_id),
                            approval_digest: digest,
                            revision: plan
                                .as_ref()
                                .map_or(runtime.revision, |p| p.target_revision),
                            config_hash: plan
                                .as_ref()
                                .map(|p| crate::expansion::config_hash(&p.target_config))
                                .transpose()?
                                .unwrap_or([0; 32]),
                            status: "rejected".into(),
                            runtime_applied: false,
                            effective_installed: false,
                            bank_descriptor_hash: None,
                            error: Some(error.to_string()),
                        };
                        // A rejected, decoded operation also consumes its identity. Preserve
                        // an existing result when a conflicting retry is itself rejected.
                        if let Some(plan) = plan
                            && plan.operation_id != [0; 16]
                            && self.operation_room(plan.operation_id)
                        {
                            self.history
                                .entry(plan.operation_id)
                                .or_insert_with(|| record.clone());
                        }
                        self.reporting = Some(record);
                        if let Some(durable) = &mut self.durable {
                            durable.report_phase = Some(if durable.closed {
                                RecoveryPhase::Revoking
                            } else {
                                RecoveryPhase::Ready
                            });
                        }
                    }
                }
            }
            return Ok(());
        }
        let Some(mut flight) = self.flight.take() else {
            return Ok(());
        };
        let result = self.reply_flight(action, output, &mut flight, runtime, now);
        if flight.record.status == "complete" && result.is_ok() {
            self.history
                .insert(flight.plan.operation_id, flight.record.clone());
            self.reporting = Some(flight.record);
        } else {
            self.flight = Some(flight);
        }
        result
    }
    fn prepare(&mut self, bytes: &[u8], runtime: &mut Runtime, now: u64) -> Result<()> {
        if self.durable.as_ref().is_some_and(|d| d.closed) {
            return Err(invalid(
                "MD recovery history is closed after an irreversible failure",
            ));
        }
        let plan = Plan::decode(bytes)?;
        let digest: [u8; 32] =
            Sha256::digest(encode(&plan, crate::expansion::MAX_PLAN_BYTES)?).into();
        if self
            .durable
            .as_ref()
            .is_some_and(|d| d.historical.contains_key(&plan.operation_id))
        {
            return Err(invalid(
                "MD operation belongs to a previous process incarnation",
            ));
        }
        if let Some(previous) = self.history.get(&plan.operation_id) {
            if previous.approval_digest != digest {
                return Err(invalid("MD operation reused with different plan"));
            }
            self.reporting = Some(previous.clone());
            return Ok(());
        }
        if !self.operation_room(plan.operation_id) {
            return Err(invalid("MD expansion operation capacity"));
        }
        let topic = runtime
            .topics
            .iter_mut()
            .position(|owner| owner.split().0.header().topic_id == plan.topic_id)
            .ok_or_else(|| invalid("MD expansion topic"))?;
        if runtime.retired[topic].len() >= 32 || runtime.topics[topic].needs_bank() {
            return Err(invalid("MD expansion old owner unavailable"));
        }
        let bank = runtime.topics[topic].split().0.header().clone();
        let admissions = runtime.server.active_admissions(topic)?;
        let retained = runtime.retired[topic].iter().try_fold(0u64, |sum, owner| {
            sum.checked_add(owner.header().resource_bytes)
                .ok_or_else(|| invalid("retained bank overflow"))
        })?;
        let prepared = plan.clone().prepare(
            &runtime.config,
            &runtime.index,
            runtime.revision,
            &bank,
            topic,
            &admissions,
            retained,
            now,
        )?;
        if let Some(store) = self.durable.as_ref().and_then(|d| d.store.as_ref())
            && store.seen_resource(prepared.bank_template().resource_id)
        {
            return Err(invalid("MD candidate bank resource was already consumed"));
        }
        if let Some(store) = self.durable.as_ref().and_then(|d| d.store.as_ref())
            && prepared.added_feeds().iter().any(|feed| {
                store.seen_owner(feed.owner_id)
                    || feed
                        .events
                        .iter()
                        .any(|event| store.seen_owner(event.owner_id))
            })
        {
            return Err(invalid("MD candidate source owner was already consumed"));
        }
        let target = &plan.target_config.topics[topic];
        let (registry, configs) = topic_registry(
            &plan.target_config,
            prepared.bank_template(),
            prepared.index(),
            target,
        )?;
        let added_ids = prepared
            .added_feeds()
            .iter()
            .map(|f| f.instrument.id)
            .collect::<BTreeSet<_>>();
        let mut candidate = runtime.topics[topic].prepare_expansion(
            registry.clone(),
            configs
                .into_iter()
                .filter(|c| added_ids.contains(&c.instrument.instrument_id))
                .collect(),
            tickwire_market_publisher::Limits {
                levels_per_side: target.maximum_levels,
                cache_bytes: target.maximum_cache_bytes,
                snapshot_bytes: target.maximum_snapshot_bytes,
                max_delta_bytes: target.maximum_delta_bytes,
            },
            plan.peak_cache_bytes,
        )?;
        for feed in prepared.added_feeds() {
            for event in &feed.events {
                candidate.configure_event(
                    feed.instrument.id,
                    event.kind.kind(),
                    event.stream_id,
                )?;
            }
        }
        let mut old_routes = Vec::new();
        let mut old_event_routes = Vec::new();
        for (i, feed) in runtime.feeds.iter().enumerate() {
            if feed.topics.iter().any(|(t, _)| *t == topic) {
                old_routes.push((i, candidate.instrument_slot(feed.instrument)?));
            }
        }
        for (i, event) in runtime.event_feeds.iter().enumerate() {
            if event.topics.iter().any(|(t, _)| *t == topic) {
                old_event_routes.push((i, candidate.instrument_slot(event.instrument)?));
            }
        }
        let approvals = plan
            .consumers
            .iter()
            .map(|consumer| {
                let approval = Approval {
                    schema: 1,
                    operation_id: plan.operation_id,
                    approval_digest: prepared.approval_digest(),
                    revision: plan.target_revision,
                    previous: consumer.previous.client.clone(),
                    next_template: consumer.next.client.clone(),
                    deadline_mono_ns: plan.warmup_deadline_mono_ns,
                };
                approval.validate(
                    &consumer.previous.client,
                    tickwire_clock::MonoTime::new(runtime.domain, now),
                )?;
                Ok(approval)
            })
            .collect::<Result<Vec<_>>>()?;
        let feeds = prepared.added_feeds().to_vec();
        let tokens = feeds
            .iter()
            .map(|feed| owners::Reserved::new(&mut runtime.tokens, feed.events.len()))
            .collect::<Result<_>>()?;
        let index = prepared.index().expanded(&plan.target_config.feeds)?;
        let record = Record {
            schema: 1,
            operation_id: plan.operation_id,
            approval_digest: digest,
            revision: plan.target_revision,
            config_hash: prepared.target_config_hash(),
            status: "warming".into(),
            runtime_applied: false,
            effective_installed: false,
            bank_descriptor_hash: None,
            error: None,
        };
        self.history.insert(plan.operation_id, record.clone());
        let build = cold::Task::Build {
            feeds,
            index,
            domain: runtime.domain,
            tokens,
        };
        self.flight = Some(Flight {
            started_at: now,
            prepared: Some(prepared),
            plan,
            topic,
            registry,
            candidate: Some(candidate),
            additions: Vec::new(),
            approvals,
            next_admissions: Vec::new(),
            old_routes,
            old_event_routes,
            stage: Stage::Reserving,
            irreversible: false,
            record,
            build: Some(build),
            installation: None,
        });
        if self.durable.is_some() {
            self.persist(After::Reserve, RecoveryPhase::Reserved, runtime)
        } else {
            let flight = self.flight.as_mut().unwrap();
            flight.stage = Stage::Building;
            let task = flight.build.take().unwrap();
            self.submit(Action::Build, task)
        }
    }
    fn reply_flight(
        &mut self,
        action: Action,
        output: cold::Output,
        flight: &mut Flight,
        runtime: &mut Runtime,
        now: u64,
    ) -> Result<()> {
        if now >= flight.plan.warmup_deadline_mono_ns {
            return Err(invalid("MD expansion deadline"));
        }
        match (action, output) {
            (Action::Persist(After::Reserve), cold::Output::Written) => {
                flight.stage = Stage::Building;
                self.submit(
                    Action::Build,
                    flight
                        .build
                        .take()
                        .ok_or_else(|| invalid("MD build task missing"))?,
                )?;
            }
            (Action::Persist(After::Revoke), cold::Output::Written) => {
                self.begin_revocation(flight, runtime, now)?;
            }
            (Action::Persist(After::Applied), cold::Output::Written) => {
                self.submit(
                    Action::Install,
                    flight
                        .installation
                        .take()
                        .ok_or_else(|| invalid("MD installation missing"))?,
                )?;
            }
            (Action::Persist(After::Complete), cold::Output::Written) => {
                if runtime.topics[flight.topic].needs_bank()
                    || !runtime.topics[flight.topic].state().live()
                {
                    return Err(invalid("MD completed topic changed during persistence"));
                }
                flight.record.status = "complete".into();
            }
            (Action::Build, cold::Output::Built(mut built)) => {
                for owner in &mut built {
                    let slot = flight
                        .candidate
                        .as_ref()
                        .ok_or_else(|| invalid("missing candidate"))?
                        .instrument_slot(owner.feed.instrument)?;
                    owner.feed.topics = vec![(flight.topic, slot)];
                    for event in &mut owner.events {
                        event.topics.clone_from(&owner.feed.topics);
                    }
                    owner.start(runtime.poll.registry(), &mut runtime.quota)?;
                }
                flight.additions = built;
                flight.stage = Stage::Warming;
            }
            (Action::Arm, cold::Output::Written) => {
                self.next_scan = now;
            }
            (Action::Armed, cold::Output::Read(values)) => {
                if !self.receipts(flight, &values, runtime, now, Phase::Armed)? {
                    return Ok(());
                }
                if self.durable.is_some() {
                    self.persist(After::Revoke, RecoveryPhase::Revoking, runtime)?;
                } else {
                    self.begin_revocation(flight, runtime, now)?;
                }
            }
            (Action::Install, cold::Output::Written) => {
                flight.record.effective_installed = true;
                runtime.server.resume_expanded_topic(
                    flight.topic,
                    flight.plan.operation_id,
                    &mut runtime.topics[flight.topic],
                )?;
                let files = flight
                    .next_admissions
                    .iter()
                    .map(|a| {
                        let commit = Commit {
                            schema: 1,
                            operation_id: flight.plan.operation_id,
                            approval_digest: flight.record.approval_digest,
                            revision: flight.plan.target_revision,
                            config: a.client.clone(),
                        };
                        Ok((
                            self.config.consumer_path(
                                a.pid,
                                a.client.endpoint.local.instance,
                                "commit",
                            ),
                            encode(&commit, MAX_CONTROL_BYTES)?,
                        ))
                    })
                    .collect::<Result<_>>()?;
                flight.stage = Stage::Committing;
                self.submit(Action::Commit, cold::Task::Write(files))?;
            }
            (Action::Commit, cold::Output::Written) => {
                flight.stage = Stage::Rejoining;
                self.next_scan = now;
            }
            (Action::Activated, cold::Output::Read(values)) => {
                if !self.receipts(flight, &values, runtime, now, Phase::Activated)? {
                    return Ok(());
                }
                let actual = match runtime.server.active_admissions(flight.topic) {
                    Ok(v) => v,
                    Err(_) => return Ok(()),
                };
                let mut actual = actual
                    .iter()
                    .map(serde_json::to_vec)
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                let mut expected = flight
                    .next_admissions
                    .iter()
                    .map(serde_json::to_vec)
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                actual.sort();
                expected.sort();
                if actual != expected {
                    return Ok(());
                }
                if runtime.topics[flight.topic].needs_bank()
                    || !runtime.topics[flight.topic].state().live()
                    || !flight.record.runtime_applied
                    || !flight.record.effective_installed
                    || runtime.revision != flight.record.revision
                    || crate::expansion::config_hash(&runtime.config)? != flight.record.config_hash
                {
                    return Err(invalid("MD expanded runtime not effective/live"));
                }
                if self.durable.is_some() {
                    self.persist(After::Complete, RecoveryPhase::Ready, runtime)?;
                } else {
                    flight.record.status = "complete".into();
                }
            }
            _ => return Err(invalid("MD cold result phase")),
        }
        Ok(())
    }
    fn begin_revocation(
        &mut self,
        flight: &mut Flight,
        runtime: &mut Runtime,
        now: u64,
    ) -> Result<()> {
        let bank = runtime.topics[flight.topic].split().0.header();
        if wire_bytes(bank)? != flight.plan.base_bank
            || runtime.revision != flight.plan.base_revision
            || crate::expansion::config_hash(&runtime.config)? != flight.plan.base_config_hash
            || !flight
                .candidate
                .as_ref()
                .is_some_and(|c| c.ready(now, flight.plan.maximum_age_ns))
        {
            return Err(invalid("MD expansion base/readiness changed"));
        }
        let expected = flight
            .plan
            .consumers
            .iter()
            .map(|c| c.previous.clone())
            .collect::<Vec<_>>();
        let result = runtime.server.begin_expansion(
            flight.topic,
            flight.plan.operation_id,
            &expected,
            &mut runtime.topics[flight.topic],
            now,
        );
        flight.irreversible = runtime.server.topic_paused(flight.topic);
        result?;
        flight.stage = Stage::Quiescing;
        Ok(())
    }
    fn receipts(
        &self,
        flight: &Flight,
        values: &[Option<Vec<u8>>],
        runtime: &mut Runtime,
        now: u64,
        phase: Phase,
    ) -> Result<bool> {
        if values.len() != flight.approvals.len() {
            return Err(invalid("MD receipt cardinality"));
        }
        let time = tickwire_clock::MonoTime::new(runtime.domain, now);
        for ((approval, consumer), bytes) in flight
            .approvals
            .iter()
            .zip(&flight.plan.consumers)
            .zip(values)
        {
            let Some(bytes) = bytes else {
                return Ok(false);
            };
            let receipt = Receipt::decode(bytes)?;
            if receipt.operation_id != flight.plan.operation_id {
                return Ok(false);
            }
            approval.validate_receipt(&receipt, consumer.previous.pid, receipt.phase, time)?;
            if receipt.phase != phase {
                return Ok(false);
            }
            if receipt.observed_mono_ns < flight.started_at {
                return Err(invalid("stale MD receipt"));
            }
            if phase == Phase::Activated {
                let pid = consumer.previous.pid;
                let instance = approval.previous.endpoint.local.instance;
                let previous = runtime.server.expansion_previous_applied(
                    flight.topic,
                    flight.plan.operation_id,
                    pid,
                    instance,
                )?;
                let Some(floor) = runtime.server.activation_floor(flight.topic, pid, instance)
                else {
                    return Ok(false);
                };
                let (owner, _) = runtime.topics[flight.topic].split();
                let bank = owner.header();
                if receipt.previous_applied != Some(previous)
                    || receipt.next_descriptor_hash != Some(bank.descriptor_hash)
                    || receipt.observed_mono_ns < bank.created_mono_ns
                    || receipt
                        .activated_sequence
                        .is_none_or(|sequence| sequence < floor || sequence > owner.published())
                {
                    return Err(invalid(
                        "MD Activated receipt differs from real cursors/bank",
                    ));
                }
            }
        }
        Ok(true)
    }
}
