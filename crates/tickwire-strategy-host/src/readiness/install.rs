//! Qualified target control. A saved request is reused verbatim after an
//! uncertain reply; a later target cut never silently replaces its original D.
use super::*;
use tickwire_release::{
    Object,
    replay::{ControlInput, canonical, install::Request, source::VerifiedSourceReplay},
};

fn save_request(path: &Path, bytes: &[u8]) -> Result<()> {
    let file = match OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(path)
    {
        Ok(mut file) => {
            file.write_all(bytes)?;
            file
        }
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            tickwire_strategy_sdk::preflight::ObjectFile {
                path: path.to_owned(),
                object: Object::of(bytes),
            }
            .read()?;
            File::open(path)?
        }
        Err(e) => return Err(e.into()),
    };
    file.sync_all()?;
    File::open(path.parent().ok_or(Error::Invalid)?)?.sync_all()?;
    Ok(())
}
fn load_request(path: &Path) -> Result<Option<ControlInput>> {
    let meta = match std::fs::symlink_metadata(path) {
        Ok(m) => m,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    if !meta.is_file() || meta.len() > tickwire_release::replay::MAX_BYTES as u64 {
        return Err(Error::Invalid);
    }
    let bytes = std::fs::read(path)?;
    if bytes.len() as u64 != meta.len() {
        return Err(Error::Invalid);
    }
    Ok(Some(ControlInput::decode(&bytes)?))
}
impl ReadyProof {
    pub fn finish_replay(
        &mut self,
        lane: &mut AuthenticatedChild,
        reports: &mut Reports,
        sub: tickwire_shm::SubscriptionKey,
        clock: &mut SystemClock,
        installed: tickwire_runtime::node::InstalledReplay,
    ) -> Result<tickwire_runtime::node::PreparedReplay> {
        self.observation_idle(clock.sample()?.monotonic.nanos())?;
        let request = self.last_install_request.clone().ok_or(Error::NotReady)?;
        if installed.worker_pid() != lane.process.pid
            || installed.verified().installation.request.digest != request.object_digest
        {
            return Err(Error::Invalid);
        }
        let done = self
            .preflight
            .as_ref()
            .ok_or(Error::Invalid)?
            .installed_probe(&installed)?;
        for bytes in &done.objects {
            reports.put_object(bytes)?;
        }
        let now = clock.sample()?.monotonic.nanos();
        let until = now
            .checked_add(3_000_000_000)
            .ok_or(Error::Invalid)?
            .min(lane.session.deadline())
            .min(installed.verified().request.member.valid_until_ns);
        if now >= until {
            return Err(Error::NotReady);
        }
        let status = tickwire_ipc::StatusBody {
            subject_id: request.instance_id,
            original_request_id: request.challenge,
            original_control_epoch: lane.session.epoch(),
            state_version: request.expected_sequence,
            state: 3,
            result_hash: done.root.digest,
            ..Default::default()
        };
        let bytes = lane.session.encode(
            ControlEnvelope {
                opcode: 49,
                flags: 10,
                request_id: request.challenge,
                expected_state_version: request.expected_sequence,
                ..Default::default()
            },
            &wire_bytes(&status)?,
        )?;
        let deadline = Instant::now() + Duration::from_nanos(until - now);
        lane.connection.send(&bytes, &[], deadline)?;
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
            if self.observe_fresh_reply(&frame.envelope, frame.body)? {
                continue;
            }
            if frame.envelope.opcode != 37 || frame.envelope.flags != 32 {
                return Err(Error::Invalid);
            }
            self.observe(HandoverReadyBody::decode(frame.body)?, lane, now)?;
            let want = self.current().ok_or(Error::NotReady)?;
            if !self.matches(reports, sub, &want)? {
                continue;
            }
            let objects = self
                .objects(lane, clock.sample()?.monotonic.nanos(), reports, sub, &want)?
                .ok_or(Error::NotReady)?;
            let seal = tickwire_recovery::HandoverSeal::decode(
                objects
                    .iter()
                    .find(|b| Object::of(b).digest == want.input_seal_digest)
                    .ok_or(Error::Invalid)?,
            )?;
            if seal.replay_rollback_digest != Some(done.root.digest) {
                continue;
            }
            self.confirm(reports, sub, lane, now)?;
            return Ok(
                tickwire_runtime::node::PreparedReplay::from_authenticated_ready(
                    installed, done.root, want, objects,
                )?,
            );
        }
    }
    pub fn install_replay(
        &mut self,
        lane: &mut AuthenticatedChild,
        reports: &mut Reports,
        sub: tickwire_shm::SubscriptionKey,
        clock: &mut SystemClock,
        source: &VerifiedSourceReplay,
    ) -> Result<tickwire_runtime::node::InstalledReplay> {
        self.observation_idle(clock.sample()?.monotonic.nanos())?;
        let completion = source.completion();
        let member = &completion.member;
        if completion.operation.schema != 3
            || self.identity.instance != member.new_instance
            || self.identity.generation != member.new_generation
            || self.identity.logical_strategy != completion.operation.logical_strategy_id
            || lane.session.peer() != &self.identity
            || lane.process.pid == 0
        {
            return Err(Error::Invalid);
        }
        let job = self.preflight.as_ref().ok_or(Error::Invalid)?.job().clone();
        if job.operation != completion.operation || job.member != *member {
            return Err(Error::Invalid);
        }
        let bundle = job.bundle.load(&job.operation)?;
        source.bind_release(&bundle, &job.operation)?;
        let directory = self.receipt.parent().ok_or(Error::Invalid)?.to_owned();
        let path = directory.join(format!(
            "REPLAY-INSTALL-{}-{}.json",
            self.identity
                .instance
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>(),
            self.identity.generation
        ));
        let input = match load_request(&path)? {
            Some(input) => input,
            None => {
                self.pause_callbacks = true;
                let cut = self.capture(lane, reports, sub, clock)?;
                let objects = self
                    .objects(lane, clock.sample()?.monotonic.nanos(), reports, sub, &cut)?
                    .ok_or(Error::NotReady)?;
                let checkpoint = source_replay::cut(&objects, cut.input_seal_digest)?;
                let mut target = checkpoint.source.clone();
                let mut material = b"tickwire.target-replay-stream.v1".to_vec();
                material.extend(source.root().digest);
                material.extend(checkpoint.bytes()?);
                target.input_stream = tickwire_recovery::digest(&material)[..16]
                    .try_into()
                    .map_err(|_| Error::Invalid)?;
                if target.input_stream == checkpoint.source.input_stream
                    || target.input_stream == source.current().source.input_stream
                {
                    return Err(Error::Invalid);
                }
                let mut source_objects = source.objects().map(Object::of).collect::<Vec<_>>();
                source_objects.sort_by_key(|o| o.digest);
                source_objects.dedup();
                let request = Request {
                    schema: 1,
                    operation: job.operation.clone(),
                    member: member.clone(),
                    source_completion: source.root(),
                    source_objects,
                    target,
                    config: checkpoint.runtime.config.clone(),
                };
                request.bytes()?;
                let input = ControlInput::Install {
                    source: checkpoint.source,
                    expected: checkpoint.cut,
                    request: Box::new(request),
                };
                for bytes in source.objects() {
                    reports.put_object(bytes)?;
                }
                for bytes in bundle.object_bytes()? {
                    reports.put_object(&bytes)?;
                }
                let bytes = input.bytes()?;
                reports.put_object(&bytes)?;
                save_request(&path, &bytes)?;
                input
            }
        };
        let ControlInput::Install { request, .. } = &input else {
            return Err(Error::Invalid);
        };
        if request.operation != job.operation
            || request.member != job.member
            || request.source_completion != source.root()
            || request.target.instance != self.identity.instance
            || request.target.generation != self.identity.generation
            || request.target.binary_digest != self.identity.build_hash
        {
            return Err(Error::Invalid);
        }
        let object = Object::of(&input.bytes()?);
        // Also reestablish the file sync completion point after a prior uncertain write.
        reports.put_object(&input.bytes()?)?;
        save_request(&path, &input.bytes()?)?;
        self.seal_sequence = self.seal_sequence.checked_add(1).ok_or(Error::Invalid)?;
        let mut challenge = canonical(&(object, self.seal_sequence, lane.session.epoch()))?;
        challenge.extend(self.identity.instance);
        let challenge = tickwire_recovery::digest(&challenge)[..16]
            .try_into()
            .map_err(|_| Error::Invalid)?;
        let now = clock.sample()?.monotonic.nanos();
        let until = now
            .checked_add(10_000_000_000)
            .ok_or(Error::Invalid)?
            .min(lane.session.deadline())
            .min(member.valid_until_ns);
        if now >= until {
            return Err(Error::NotReady);
        }
        let body = tickwire_ipc::ReplayControlBody {
            instance_id: self.identity.instance,
            strategy_generation: self.identity.generation,
            logical_strategy_id: self.identity.logical_strategy,
            challenge,
            valid_until_mono_ns: until,
            object_digest: object.digest,
            object_bytes: object.bytes,
            expected_sequence: input.expected().sequence,
            expected_cut_digest: Object::of(&canonical(input.expected())?).digest,
            operation_id: job.operation.operation_id,
            operation_revision: job.operation.operation_revision,
            schema: 1,
            replay_kind: 3,
        };
        let raw = lane.session.encode(
            ControlEnvelope {
                opcode: 41,
                flags: 1,
                request_id: challenge,
                expected_state_version: body.expected_sequence,
                ..Default::default()
            },
            &wire_bytes(&body)?,
        )?;
        let deadline = Instant::now() + Duration::from_nanos(until - now);
        lane.connection.send(&raw, &[], deadline)?;
        self.last_install_request = Some(body.clone());
        let mut installed = None;
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
            if self.observe_fresh_reply(&frame.envelope, frame.body)? {
                continue;
            }
            match frame.envelope.opcode {
                42 => {
                    let reply = tickwire_ipc::ReplayAppliedBody::decode(frame.body)?;
                    let root = tickwire_strategy_sdk::replay::control::verify_reply(
                        &frame.envelope,
                        &reply,
                        &body,
                        now,
                    )
                    .map_err(tickwire_strategy_sdk::Error::Replay)?;
                    if installed.is_some_and(|old| old != root) {
                        return Err(Error::Invalid);
                    }
                    installed = Some(root);
                }
                37 if frame.envelope.flags == 32 => {
                    self.observe(HandoverReadyBody::decode(frame.body)?, lane, now)?
                }
                _ => return Err(Error::Invalid),
            }
            let Some(root) = installed else {
                continue;
            };
            let Some(want) = self.current() else {
                continue;
            };
            if !self.matches(reports, sub, &want)? {
                continue;
            }
            let objects = self
                .objects(lane, clock.sample()?.monotonic.nanos(), reports, sub, &want)?
                .ok_or(Error::NotReady)?;
            let seal_bytes = objects
                .iter()
                .find(|b| Object::of(b).digest == want.input_seal_digest)
                .ok_or(Error::Invalid)?;
            let seal = tickwire_recovery::HandoverSeal::decode(seal_bytes)?;
            if seal.replay_installation_digest != Some(root.digest) {
                continue;
            }
            self.confirm(reports, sub, lane, now)?;
            let installed = tickwire_runtime::node::InstalledReplay::from_authenticated_child(
                root,
                source,
                want,
                lane.process.pid,
                objects,
            )?;
            self.pause_callbacks = false;
            return Ok(installed);
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn saved_install_request_cannot_be_rebound_to_a_new_cut_on_retry() {
        struct Temp(PathBuf);
        impl Drop for Temp {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let d = Temp(std::env::temp_dir().join(format!(
                "tickwire-install-request-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            )));
        std::fs::create_dir(&d.0).unwrap();
        let path = d.0.join("request.json");
        save_request(&path, b"first exact request").unwrap();
        save_request(&path, b"first exact request").unwrap();
        assert!(save_request(&path, b"replacement cut").is_err());
        assert_eq!(std::fs::read(&path).unwrap(), b"first exact request");
    }
}
