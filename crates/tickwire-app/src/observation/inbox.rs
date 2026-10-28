use super::*;
impl ObservationInbox {
    pub(super) fn open_with(
        config: Config,
        policy: &policy::Config,
        plane: ControlPlane,
        mut sample: impl FnMut() -> Result<MonoTime>,
    ) -> Result<Self> {
        if !cfg!(target_os = "linux") {
            return Err(invalid("observation provider requires Linux"));
        }
        config.validate()?;
        let binding = Binding::from_config(policy);
        let binding_bytes = serde_json::to_vec(&binding)?;
        let domain = policy
            .domain()
            .map_err(|e| invalid(&format!("observation domain: {e:?}")))?;
        let now = sample()?;
        if now.domain() != domain {
            return Err(invalid("observation clock domain"));
        }
        let inbox = files::Directory::open(&config.inbox_directory, config.owner_uid)?;
        let outbox = files::Directory::open(&config.outbox_directory, config.owner_uid)?;
        let (mut storage, records) = store::Storage::open(&config, &binding)?;
        let mut canonical = config.clone();
        canonical.recover = false;
        let config_digest = tickwire_recovery::digest(&serde_json::to_vec(&(canonical, &binding))?);
        let mut entries = BTreeMap::<[u8; 16], Entry>::new();
        let mut session_number = 0;
        let mut session_id = [0; 16];
        let mut last = 0;
        for record in records {
            match record {
                Record::Session {
                    config_digest: old,
                    session: s,
                } => {
                    if old != config_digest
                        || s.schema != 1
                        || s.session_id == [0; 16]
                        || s.session_number != session_number + 1
                        || s.started_mono_ns < last
                        || serde_json::to_vec(&s.binding)? != binding_bytes
                    {
                        return Err(invalid("observation recovered session"));
                    }
                    session_number = s.session_number;
                    session_id = s.session_id;
                    last = s.started_mono_ns;
                }
                Record::Received {
                    request,
                    admitted_mono_ns,
                } => {
                    if session_number == 0
                        || admitted_mono_ns < last
                        || entries.len() >= config.maximum_requests
                        || entries.contains_key(&request.request_id)
                        || !request_shape(&request)
                    {
                        return Err(invalid("observation recovered request"));
                    }
                    // Rejections, including stale sessions/bindings/deadlines, are audit records too.
                    let digest = tickwire_recovery::digest(&serde_json::to_vec(&request)?);
                    entries.insert(
                        request.request_id,
                        Entry {
                            request,
                            digest,
                            terminal: None,
                            conflict: None,
                            pending: false,
                        },
                    );
                    last = admitted_mono_ns;
                }
                Record::Terminal {
                    request_id,
                    request_digest,
                    result,
                } => {
                    let e = entries
                        .get_mut(&request_id)
                        .ok_or_else(|| invalid("observation result without request"))?;
                    let outcome: Outcome = serde_json::from_slice(&storage.get(result)?)?;
                    verify_outcome(&outcome, &e.request, e.digest)?;
                    if let Some(observation) = &outcome.observation {
                        storage.verify_dependencies(observation)?;
                    }
                    if e.digest != request_digest || outcome.completed_mono_ns < last {
                        return Err(invalid("observation result identity/time"));
                    }
                    if let Some(old) = e.terminal {
                        let prior: Outcome = serde_json::from_slice(&storage.get(old)?)?;
                        if prior.status != Status::Observed
                            || !matches!(outcome.status, Status::Expired | Status::Rejected)
                        {
                            return Err(invalid("observation terminal changed"));
                        }
                    }
                    e.terminal = Some(result);
                    last = outcome.completed_mono_ns;
                }
                Record::Conflict { request_id, result } => {
                    let e = entries
                        .get_mut(&request_id)
                        .ok_or_else(|| invalid("observation conflict without request"))?;
                    let out: Outcome = serde_json::from_slice(&storage.get(result)?)?;
                    verify_outcome(&out, &out.request, out.request_digest)?;
                    if e.conflict.is_some()
                        || out.request.request_id != request_id
                        || out.request_digest == e.digest
                        || out.status != Status::Rejected
                        || out.completed_mono_ns < last
                    {
                        return Err(invalid("observation recovered conflict"));
                    }
                    e.conflict = Some(result);
                    last = out.completed_mono_ns;
                }
            }
        }
        if config.recover && session_number == 0 {
            return Err(invalid("observation empty recovery"));
        }
        let current = sample()?;
        if current.domain() != domain || current.nanos() < last || current.nanos() < now.nanos() {
            return Err(invalid("observation recovered clock"));
        }
        session_number = session_number
            .checked_add(1)
            .ok_or_else(|| invalid("observation session limit"))?;
        let material = serde_json::to_vec(&(
            config_digest,
            session_number,
            current.nanos(),
            std::process::id(),
            session_id,
        ))?;
        let session = Session {
            schema: 1,
            session_id: tickwire_recovery::digest(&material)[..16].try_into()?,
            session_number,
            binding: binding.clone(),
            started_mono_ns: current.nanos(),
        };
        storage.record(&Record::Session {
            config_digest,
            session: session.clone(),
        })?;
        let mut owner = Self {
            config,
            binding,
            binding_bytes,
            domain,
            plane,
            session,
            inbox,
            outbox,
            storage,
            entries,
            next_scan: 0,
            scan_cursor: 0,
            pending_cursor: 0,
            last_now: current.nanos(),
            closed: None,
        };
        owner.capacity(MAX_RESULT as u64)?;
        // Complete uncertain old requests as Interrupted, never call a runtime owner.
        let ids: Vec<_> = owner.entries.keys().copied().collect();
        for id in ids {
            if owner.entries[&id].terminal.is_none() {
                let r = owner.entries[&id].request.clone();
                let at = owner.fresh(&mut sample)?;
                let outcome = owner.failure(&r, Status::Interrupted, at.nanos());
                owner.persist(&outcome)?;
            }
            let e = &owner.entries[&id];
            let root = e
                .conflict
                .or(e.terminal)
                .ok_or_else(|| invalid("observation recovered terminal"))?;
            let mut out: Outcome = serde_json::from_slice(&owner.storage.get(root)?)?;
            out.historical = true;
            owner.publish(&out)?;
        }
        owner.capacity(8192)?;
        owner
            .outbox
            .replace("session.json", &serde_json::to_vec(&owner.session)?)?;
        owner.fresh(&mut sample)?;
        Ok(owner)
    }
    fn fresh(&mut self, sample: &mut impl FnMut() -> Result<MonoTime>) -> Result<MonoTime> {
        let n = sample()?;
        if n.domain() != self.domain || n.nanos() < self.last_now {
            return Err(invalid("observation clock changed or regressed"));
        }
        self.last_now = n.nanos();
        Ok(n)
    }
    fn binding_live(&self) -> Result<bool> {
        let live = self
            .plane
            .configuration()
            .map_err(|e| invalid(&format!("observation policy: {e:?}")))?;
        let p = self.binding.product;
        let e = self.binding.scope.environment;
        Ok(
            live.scope.risk_domain_id == self.binding.scope.risk_domain_id
                && live.scope.venue == self.binding.scope.venue
                && live.scope.account == self.binding.scope.account
                && live.scope.environment
                    == match e {
                        policy::Environment::Production => tickwire_venue::Environment::Production,
                        policy::Environment::Test => tickwire_venue::Environment::Test,
                    }
                && live.product
                    == match p {
                        policy::Product::Spot => tickwire_venue::Product::Spot,
                        policy::Product::LinearPerpetual => {
                            tickwire_venue::Product::LinearPerpetual
                        }
                        policy::Product::LinearFuture => tickwire_venue::Product::LinearFuture,
                    }
                && live.executor_id == self.binding.executor_id
                && live.owner_generation == self.binding.owner_generation
                && live.writer_epoch == self.binding.writer_epoch
                && live.domain == self.domain,
        )
    }
    fn capacity(&self, extra: u64) -> Result<()> {
        let stored = self.storage.usage()?;
        let n = self
            .inbox
            .usage()?
            .checked_add(self.outbox.usage()?)
            .and_then(|n| n.checked_add(stored))
            .and_then(|n| n.checked_add(extra));
        if n.is_none_or(|v| v > MAX_TOTAL) {
            return Err(invalid("observation total capacity"));
        }
        Ok(())
    }
    pub(super) fn poll_with(
        &mut self,
        strategy: &mut dyn AppStrategy,
        mut sample: impl FnMut() -> Result<MonoTime>,
    ) -> Result<usize> {
        if self.closed.is_some() {
            return Ok(0);
        }
        let result = (|| {
            let now = self.fresh(&mut sample)?;
            if now.nanos() < self.next_scan {
                return Ok(0);
            }
            self.inbox.check()?;
            self.outbox.check()?;
            self.storage.check()?;
            self.capacity(0)?;
            let mut count = self.drive(strategy, &mut sample)?;
            let names = self.inbox.names(self.config.maximum_requests * 2)?;
            let scan = names.len().min(self.config.maximum_per_poll);
            for step in 0..scan {
                let name = &names[(self.scan_cursor + step) % names.len()];
                if name.starts_with('.') || name.ends_with(".pending") {
                    continue;
                }
                let id = parse_name(name)?;
                let bytes = self
                    .inbox
                    .read(name, MAX_REQUEST)?
                    .ok_or_else(|| invalid("observation request disappeared"))?;
                let request: Request = serde_json::from_slice(&bytes)?;
                if request.request_id != id || !request_shape(&request) {
                    return Err(invalid("observation request shape/name"));
                }
                let digest = tickwire_recovery::digest(&serde_json::to_vec(&request)?);
                if let Some(e) = self.entries.get(&id) {
                    if digest != e.digest && e.conflict.is_none() {
                        if e.terminal.is_none() {
                            let original = e.request.clone();
                            if e.pending {
                                let _ = strategy.cancel_observation(original.target, id);
                            }
                            let n = self.fresh(&mut sample)?;
                            self.finish(
                                self.failure(&original, Status::Rejected, n.nanos()),
                                &mut sample,
                            )?;
                        }
                        let n = self.fresh(&mut sample)?;
                        let out = self.failure(&request, Status::Rejected, n.nanos());
                        self.capacity((MAX_RESULT * 2) as u64)?;
                        let root = self.storage.put(&serde_json::to_vec(&out)?)?;
                        self.storage.record(&Record::Conflict {
                            request_id: id,
                            result: root,
                        })?;
                        self.entries.get_mut(&id).unwrap().conflict = Some(root);
                        self.publish(&out)?;
                    }
                    continue;
                }
                if self.entries.len() == self.config.maximum_requests {
                    return Err(invalid("observation request history capacity"));
                }
                let admitted = self.fresh(&mut sample)?;
                self.capacity(MAX_REQUEST as u64 + 4096)?;
                self.storage.record(&Record::Received {
                    request: request.clone(),
                    admitted_mono_ns: admitted.nanos(),
                })?;
                self.entries.insert(
                    id,
                    Entry {
                        request: request.clone(),
                        digest,
                        terminal: None,
                        conflict: None,
                        pending: false,
                    },
                );
                let now = self.fresh(&mut sample)?;
                let status = if request.session_id != self.session.session_id
                    || serde_json::to_vec(&request.binding)? != self.binding_bytes
                    || !self.binding_live()?
                {
                    Some(Status::Rejected)
                } else if now.nanos() >= request.valid_until_mono_ns {
                    Some(Status::Expired)
                } else if request.valid_until_mono_ns.saturating_sub(now.nanos()) > MAX_LIFETIME {
                    Some(Status::Rejected)
                } else if self.entries.values().filter(|e| e.pending).count()
                    >= self.config.maximum_pending
                    || self
                        .entries
                        .values()
                        .any(|e| e.pending && e.request.target == request.target)
                {
                    Some(Status::Busy)
                } else {
                    None
                };
                if let Some(status) = status {
                    self.finish(self.failure(&request, status, now.nanos()), &mut sample)?;
                    count += 1;
                    continue;
                }
                let process = match strategy.observation_process(request.target) {
                    Ok(p) => p,
                    Err(e) => {
                        self.finish(
                            self.failure(&request, types::failure(&e), now.nanos()),
                            &mut sample,
                        )?;
                        count += 1;
                        continue;
                    }
                };
                let now = self.fresh(&mut sample)?;
                let status = if now.nanos() >= request.valid_until_mono_ns {
                    Some(Status::Expired)
                } else if !self.binding_live()?
                    || !request.target.matches(&process)
                    || !self
                        .config
                        .allowed_build_hashes
                        .contains(&process.image_hash())
                {
                    Some(Status::Rejected)
                } else {
                    None
                };
                if let Some(status) = status {
                    self.finish(self.failure(&request, status, now.nanos()), &mut sample)?;
                    count += 1;
                    continue;
                }
                let host = tickwire_strategy_host::ObservationRequest {
                    request_id: id,
                    deadline: MonoTime::new(self.domain, request.valid_until_mono_ns),
                    require_resources: request.require_resources,
                };
                match strategy.begin_observation(request.target, host) {
                    Ok(()) => self.entries.get_mut(&id).unwrap().pending = true,
                    Err(e) => {
                        let n = self.fresh(&mut sample)?;
                        self.finish(
                            self.failure(&request, types::failure(&e), n.nanos()),
                            &mut sample,
                        )?;
                        count += 1;
                    }
                }
            }
            if !names.is_empty() {
                self.scan_cursor = (self.scan_cursor + scan) % names.len();
            }
            let after = self.fresh(&mut sample)?;
            self.next_scan = after.nanos().saturating_add(SCAN_INTERVAL);
            Ok(count)
        })();
        if let Err(error) = &result {
            for e in self.entries.values_mut().filter(|e| e.pending) {
                let _ = strategy.cancel_observation(e.request.target, e.request.request_id);
                e.pending = false;
            }
            self.closed = Some(error.to_string());
            let raw = serde_json::to_vec(
                &serde_json::json!({"schema":1,"session_id":self.session.session_id,"error":error.to_string()}),
            )?;
            let _ = self.outbox.replace("ERROR.json", &raw);
        }
        result
    }
    fn drive(
        &mut self,
        strategy: &mut dyn AppStrategy,
        sample: &mut impl FnMut() -> Result<MonoTime>,
    ) -> Result<usize> {
        let all: Vec<_> = self
            .entries
            .values()
            .filter(|e| e.pending)
            .map(|e| e.request.clone())
            .collect();
        let limit = all.len().min(self.config.maximum_per_poll);
        let pending: Vec<_> = (0..limit)
            .map(|offset| all[(self.pending_cursor + offset) % all.len()].clone())
            .collect();
        if !all.is_empty() {
            self.pending_cursor = (self.pending_cursor + limit) % all.len();
        }
        let mut done = 0;
        for r in pending {
            let now = self.fresh(sample)?;
            if now.nanos() >= r.valid_until_mono_ns || !self.binding_live()? {
                let _ = strategy.cancel_observation(r.target, r.request_id);
                let status = if now.nanos() >= r.valid_until_mono_ns {
                    Status::Expired
                } else {
                    Status::Rejected
                };
                self.finish(self.failure(&r, status, now.nanos()), sample)?;
                done += 1;
                continue;
            }
            match strategy.poll_observation(r.target) {
                Ok(None) => {}
                Err(e) => {
                    let _ = strategy.cancel_observation(r.target, r.request_id);
                    let at = self.fresh(sample)?;
                    self.finish(self.failure(&r, types::failure(&e), at.nanos()), sample)?;
                    done += 1;
                }
                Ok(Some(fact)) => {
                    let at = self.fresh(sample)?;
                    let valid = fact.request_id() == r.request_id
                        && fact.deadline() == MonoTime::new(self.domain, r.valid_until_mono_ns)
                        && fact.request().require_resources == r.require_resources
                        && (!r.require_resources || fact.resources().is_some())
                        && fact.started_at().domain() == self.domain
                        && fact.observed_at().domain() == self.domain
                        && fact.started_at().nanos() <= fact.observed_at().nanos()
                        && fact.observed_at().nanos() <= at.nanos()
                        && fact.observed_at().nanos() < r.valid_until_mono_ns
                        && r.target.matches(fact.process())
                        && self
                            .config
                            .allowed_build_hashes
                            .contains(&fact.process().image_hash());
                    let mut out = self.failure(&r, Status::Rejected, at.nanos());
                    if valid {
                        let mut observation = Observation::verified(&fact);
                        let objects = canonical_dependencies(
                            fact.objects()
                                .iter()
                                .map(|bytes| (Object::of(bytes), bytes.as_slice()))
                                .collect(),
                        )?;
                        observation.objects = objects.iter().map(|(object, _)| *object).collect();
                        verify_dependency_set(&observation)?;
                        let additional =
                            self.storage.additional_dependencies(&observation.objects)?;
                        self.capacity(
                            additional
                                .checked_add((MAX_RESULT * 2 + 4096) as u64)
                                .ok_or_else(|| invalid("observation dependency capacity"))?,
                        )?;
                        let mut complete = true;
                        for (_, bytes) in objects {
                            self.storage.put_dependency(bytes)?;
                            let current = self.fresh(sample)?;
                            if current.nanos() >= r.valid_until_mono_ns || !self.binding_live()? {
                                out = self.failure(
                                    &r,
                                    if current.nanos() >= r.valid_until_mono_ns {
                                        Status::Expired
                                    } else {
                                        Status::Rejected
                                    },
                                    current.nanos(),
                                );
                                complete = false;
                                break;
                            }
                        }
                        if complete {
                            out.status = Status::Observed;
                            out.observation = Some(observation);
                        }
                    }
                    self.finish(out, sample)?;
                    done += 1;
                }
            }
        }
        Ok(done)
    }
    pub(super) fn failure(&self, r: &Request, status: Status, at: u64) -> Outcome {
        Outcome {
            schema: 1,
            request: r.clone(),
            request_digest: tickwire_recovery::digest(
                &serde_json::to_vec(r).expect("typed request serialization"),
            ),
            status,
            completed_mono_ns: at,
            historical: false,
            observation: None,
        }
    }
    fn persist(&mut self, out: &Outcome) -> Result<()> {
        verify_outcome(out, &out.request, out.request_digest)?;
        self.capacity((MAX_RESULT * 2) as u64 + 4096)?;
        let object = self.storage.put(&serde_json::to_vec(out)?)?;
        self.storage.record(&Record::Terminal {
            request_id: out.request.request_id,
            request_digest: out.request_digest,
            result: object,
        })?;
        let entry = self
            .entries
            .get_mut(&out.request.request_id)
            .ok_or_else(|| invalid("observation missing request"))?;
        entry.terminal = Some(object);
        entry.pending = false;
        Ok(())
    }
    fn publish(&self, out: &Outcome) -> Result<()> {
        self.capacity(MAX_RESULT as u64)?;
        self.outbox.replace(
            &format!("{}.json", store::hex(&out.request.request_id)),
            &serde_json::to_vec(out)?,
        )?;
        Ok(())
    }
    pub(super) fn finish(
        &mut self,
        mut out: Outcome,
        sample: &mut impl FnMut() -> Result<MonoTime>,
    ) -> Result<()> {
        let at = self.fresh(sample)?;
        self.current_result(&mut out, at)?;
        self.persist(&out)?;
        let at = self.fresh(sample)?;
        if self.current_result(&mut out, at)? {
            self.persist(&out)?;
        }
        self.publish(&out)?;
        let at = self.fresh(sample)?;
        if self.current_result(&mut out, at)? {
            self.persist(&out)?;
            self.publish(&out)?;
            self.fresh(sample)?;
        }
        Ok(())
    }
    fn current_result(&self, out: &mut Outcome, at: MonoTime) -> Result<bool> {
        if out.status != Status::Observed {
            return Ok(false);
        }
        let status = if at.nanos() >= out.request.valid_until_mono_ns {
            Some(Status::Expired)
        } else if !self.binding_live()? {
            Some(Status::Rejected)
        } else {
            None
        };
        if let Some(status) = status {
            out.status = status;
            out.observation = None;
            out.completed_mono_ns = at.nanos();
            return Ok(true);
        }
        Ok(false)
    }
}
fn request_shape(r: &Request) -> bool {
    r.schema == 1
        && r.request_id != [0; 16]
        && r.session_id != [0; 16]
        && r.target.valid()
        && r.valid_until_mono_ns > 0
        && r.valid_until_mono_ns < 1u64 << 63
}
fn parse_name(name: &str) -> Result<[u8; 16]> {
    let s = name
        .strip_suffix(".json")
        .filter(|s| {
            s.len() == 32
                && s.bytes()
                    .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
        })
        .ok_or_else(|| invalid("observation request filename"))?;
    let mut id = [0; 16];
    for (i, x) in id.iter_mut().enumerate() {
        *x = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16)?;
    }
    Ok(id)
}
fn verify_outcome(out: &Outcome, r: &Request, digest: [u8; 32]) -> Result<()> {
    if out.schema != 1
        || out.historical
        || out.request_digest != digest
        || tickwire_recovery::digest(&serde_json::to_vec(&out.request)?) != digest
        || serde_json::to_vec(&out.request)? != serde_json::to_vec(r)?
        || (out.status == Status::Observed) != out.observation.is_some()
    {
        return Err(invalid("observation stored result binding"));
    }
    if let Some(o) = &out.observation {
        verify_dependency_set(o)?;
        if out.completed_mono_ns >= r.valid_until_mono_ns
            || o.observed_mono_ns > out.completed_mono_ns
            || o.started_mono_ns > o.observed_mono_ns
            || o.process.instance != r.target.instance
            || o.process.generation != r.target.generation
            || o.process.logical_strategy_id != r.target.logical_strategy_id
            || o.challenge == [0; 16]
            || o.control_epoch == 0
            || o.account_position < o.report_sequence_floor
            || o.ready_sequence == 0
            || (r.require_resources && o.resources.is_none())
        {
            return Err(invalid("observation stored facts"));
        }
    }
    Ok(())
}

pub(super) fn verify_dependency_set(o: &Observation) -> Result<()> {
    if o.objects.is_empty()
        || o.objects.len() > 1024
        || o.objects
            .windows(2)
            .any(|pair| pair[0].digest >= pair[1].digest)
        || !o.objects.contains(&o.checkpoint)
        || !o.objects.contains(&o.input_seal)
    {
        return Err(invalid("observation dependency order or READY roots"));
    }
    let mut total = 0u64;
    for root in &o.objects {
        root.validate(tickwire_recovery::MAX_OBJECT_BYTES as u64)?;
        total = total
            .checked_add(root.bytes)
            .ok_or_else(|| invalid("observation dependency capacity"))?;
    }
    if total > MAX_TOTAL {
        return Err(invalid("observation dependency capacity"));
    }
    Ok(())
}

// Host closures may repeat a shared object through several verified graph paths.
pub(super) fn canonical_dependencies(
    mut objects: Vec<(Object, &[u8])>,
) -> Result<Vec<(Object, &[u8])>> {
    objects.sort_by_key(|(object, _)| object.digest);
    for pair in objects.windows(2) {
        if pair[0].0.digest == pair[1].0.digest
            && (pair[0].0 != pair[1].0 || pair[0].1 != pair[1].1)
        {
            return Err(invalid("observation conflicting dependency bytes"));
        }
    }
    objects.dedup_by_key(|(object, _)| object.digest);
    Ok(objects)
}
