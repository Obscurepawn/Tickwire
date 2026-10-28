use super::*;
use tickwire_ipc::{ReplayAppliedBody, ReplayControlBody};
use tickwire_recovery::{BlobStore, StoreKey};
use tickwire_release::{
    Role,
    files::{Location, ObjectFile},
};
impl<R: RecoveryResolver + 'static> Hosted<R> {
    pub(super) fn finish(&mut self, installed: InstalledRebuild) -> Result<PreparedRebuild> {
        let input = installed.input();
        let protocol = self.installation.as_ref().ok_or(Error::NotReady)?;
        if protocol.request != *input.request() {
            return Err(Error::Invalid);
        }
        let plan = input
            .rollback_plans()
            .members
            .iter()
            .find(|p| p.member == input.request().member)
            .ok_or(Error::Invalid)?;
        let files: Vec<Location> = worker::read_config(
            &self.publication.join("rollback-files.json"),
            2 * 1024 * 1024,
        )?;
        let loaded = tickwire_release::files::load(&plan.files, &files)?;
        loaded.recheck()?;
        let binary = loaded.object(&Role::Binary).ok_or(Error::Invalid)?;
        let mut spec = self.spec.clone();
        spec.identity.instance = plan.probe.instance;
        spec.identity.generation = plan.probe.generation;
        spec.identity.build_hash = binary.digest;
        spec.executable = loaded.path(&Role::Binary).ok_or(Error::Invalid)?.to_owned();
        spec.arguments.clear();
        spec.environment.clear();
        let mut store = tickwire_recovery::FileBlobStore::open(&self.publication)?;
        let mut map = BTreeMap::new();
        for bytes in input.objects() {
            insert(&mut map, bytes.clone())?;
        }
        for (hash, bytes) in &map {
            store.put_sync(StoreKey::Object(*hash), bytes)?;
        }
        let file = |object: Object| ObjectFile {
            object,
            path: self
                .publication
                .join(format!("object-{}", hex(object.digest))),
        };
        let attempt = probe_attempt(input.request(), installed.root())?;
        let job = tickwire_strategy_sdk::replay::rebuild::probe::Job {
            schema: 1,
            attempt,
            operation: input.request().operation.clone(),
            member: input.request().member.clone(),
            plans: file(input.request().plans),
            manifest: file(input.request().manifest),
            contract: file(Object::of(&input.contract().bytes()?)),
            inputs: file(Object::of(&input.inputs().bytes()?)),
            rollback: file(Object::of(&input.rollback_plans().bytes()?)),
            objects: map.values().map(|v| file(Object::of(v))).collect(),
            files,
            output_directory: self.publication.join(format!("probe-{}", hex(attempt))),
        };
        let live = self.live.as_mut().ok_or(Error::NotReady)?;
        live.maintain()?;
        let now = live.clock.sample()?.monotonic;
        let deadline = Instant::now()
            + Duration::from_nanos(
                input
                    .request()
                    .deadline_mono_ns
                    .checked_sub(now.nanos())
                    .ok_or(Error::NotReady)?,
            );
        let completed = rebuild_probe::run_selected(
            &spec,
            &self.publication,
            &job,
            deadline,
            self.resources.as_ref(),
        )?;
        insert(&mut map, job.bytes()?)?;
        for bytes in completed.objects() {
            insert(&mut map, bytes.to_vec())?;
        }
        let request = install::rollback::Request {
            schema: 1,
            installation: installed.root(),
            completion: completed.completion(),
            objects: map.values().map(|v| Object::of(v)).collect(),
            deadline_mono_ns: input.request().deadline_mono_ns,
        };
        for bytes in map.values() {
            live.reports.put_object(bytes)?;
        }
        live.reports.put_object(&request.bytes()?)?;
        let wire = install::rollback::wire_request(
            &self.captured_bootstrap,
            protocol,
            &request,
            live.report.session.deadline(),
        )?;
        live.report.connection.send(
            &tickwire_strategy_sdk::capture::encode_request(&live.report.session, &wire)?,
            &[],
            deadline,
        )?;
        let mut received = None;
        while received.is_none() {
            live.registry.process(self.spec.identity.instance)?;
            let now = live.clock.sample()?.monotonic;
            if Instant::now() >= deadline || now.nanos() >= wire.valid_until_mono_ns {
                return Err(Error::NotReady);
            }
            live.readiness
                .poll_with(&mut live.report, now.nanos(), |env, body| {
                    if env.opcode != 42 {
                        return Ok(false);
                    }
                    if env.flags == 18 {
                        tickwire_strategy_sdk::capture::verify_rejection(
                            env,
                            &ReplayControlBody::decode(body)?,
                            &wire,
                            now.nanos(),
                        )?;
                        return Err(Error::NotReady);
                    }
                    let object = capture::protocol::reply(
                        env,
                        &ReplayAppliedBody::decode(body)?,
                        &wire,
                        now.nanos(),
                    )?;
                    received = Some(object);
                    Ok(true)
                })?;
            if received.is_none() {
                std::thread::sleep(Duration::from_millis(1));
            }
        }
        let root = received.ok_or(Error::Invalid)?;
        let reader = tickwire_recovery::FileBlobReader::open(
            self.bootstrap.command_directory.join("merged-inputs"),
            self.spec.uid,
        )?;
        let bytes = reader.get_object(root.digest, tickwire_strategy_sdk::capture::MAX_BYTES)?;
        root.verify(&bytes, tickwire_strategy_sdk::capture::MAX_BYTES as u64)?;
        let receipt = install::rollback::Receipt::decode(&bytes)?;
        receipt.verify(
            &request,
            live.report.process.pid,
            live.clock.sample()?.monotonic,
        )?;
        insert(&mut map, bytes)?;
        let (ready, objects) = live
            .handover_checkpoint(self.spec.identity.instance, self.spec.identity.generation)
            .ok_or(Error::NotReady)?;
        for bytes in objects {
            insert(&mut map, bytes)?;
        }
        live.registry.process(self.spec.identity.instance)?;
        if live.clock.sample()?.monotonic.nanos() >= request.deadline_mono_ns {
            return Err(Error::NotReady);
        }
        PreparedRebuild::from_authenticated_ready(
            installed,
            completed.completion(),
            ready,
            map.into_values().collect(),
        )
        .map_err(Into::into)
    }
}
fn hex<const N: usize>(bytes: [u8; N]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn probe_attempt(
    request: &tickwire_release::rebuild::install::Request,
    root: Object,
) -> Result<[u8; 16]> {
    root.validate(tickwire_release::replay::MAX_BYTES as u64)?;
    let mut input = b"tickwire.rebuild.probe.v1".to_vec();
    input.extend(request.bytes()?);
    input.extend(tickwire_release::replay::canonical(&root)?);
    let mut attempt: [u8; 16] = tickwire_recovery::digest(&input)[..16]
        .try_into()
        .map_err(|_| Error::Invalid)?;
    if attempt == [0; 16] {
        attempt[15] = 1;
    }
    Ok(attempt)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn request() -> tickwire_release::rebuild::install::Request {
        let member = tickwire_release::Member {
            risk_domain: [1; 16],
            exec_instance: [2; 16],
            exec_epoch: 1,
            old_instance: [3; 16],
            old_generation: 1,
            new_instance: [4; 16],
            new_generation: 2,
            clock_boot_id: [5; 16],
            clock_generation: 1,
            valid_until_ns: 1000,
        };
        let mut operation_id = [0; 16];
        operation_id[0] = 0xb4;
        let object = Object::of(b"fixed");
        let operation = tickwire_release::Operation {
            schema: 4,
            operation_id,
            operation_revision: 1,
            logical_strategy_id: 8,
            members: vec![member.clone()],
            artifacts_digest: object.digest,
            checkpoint_digest: object.digest,
            rollback_digest: object.digest,
            clock_boot_id: [5; 16],
            clock_generation: 1,
            valid_until_ns: 1000,
        };
        tickwire_release::rebuild::install::Request {
            schema: 1,
            operation,
            member,
            plans: object,
            manifest: object,
            capture_completion: object,
            capture_seal: object,
            capture_receipt: object,
            target_bootstrap: object,
            deadline_mono_ns: 999,
        }
    }
    #[test]
    fn probe_attempt_is_nonzero_and_binds_revision_member_and_installation() {
        let request = request();
        let root = Object::of(b"installation");
        let attempt = probe_attempt(&request, root).unwrap();
        assert_ne!(attempt, [0; 16]);
        assert_eq!(attempt, probe_attempt(&request, root).unwrap());
        let mut next = request.clone();
        next.operation.operation_revision += 1;
        assert_ne!(attempt, probe_attempt(&next, root).unwrap());
        next = request.clone();
        next.member.risk_domain = [9; 16];
        next.operation.members[0] = next.member.clone();
        assert_ne!(attempt, probe_attempt(&next, root).unwrap());
        assert_ne!(
            attempt,
            probe_attempt(&request, Object::of(b"other installation")).unwrap()
        );
    }
}
