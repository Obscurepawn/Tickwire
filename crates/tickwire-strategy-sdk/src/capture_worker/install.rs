use super::*;
use crate::Strategy;
use tickwire_release::rebuild::{Contract, Inputs, Plans, install::Installation};

impl CapturedSession {
    pub fn install_and_run<S: Strategy>(
        mut self,
        request: crate::capture::install::Request,
        mut computed: Computed<S>,
    ) -> Result<()> {
        self.check_install(&request)?;
        let config = request.bootstrap.clone();
        let directory = config.command_directory.join("merged-inputs");
        let reader = FileBlobReader::open(&directory, config.report.peer_uid)?;
        let mut objects = std::collections::BTreeMap::<[u8; 32], Vec<u8>>::new();
        for bytes in self.sealed.window.objects() {
            retain(&mut objects, bytes.to_vec())?;
        }
        for bytes in [
            self.config.bytes()?,
            self.sealed.request.bytes()?,
            self.sealed.receipt.bytes()?,
            request.bytes()?,
            request.request.bytes()?,
        ] {
            retain(&mut objects, bytes)?;
        }
        for object in &request.objects {
            let raw = self.read_object(*object)?;
            retain(&mut objects, raw)?;
        }
        for bytes in computed.history_objects {
            retain(&mut objects, bytes)?;
        }
        let head_bytes = computed.head.bytes()?;
        let head = Object::of(&head_bytes);
        retain(&mut objects, head_bytes)?;
        for bytes in objects.values() {
            computed.live.store_object(bytes)?;
        }
        self.check_install(&request)?;
        let now = sample(&self.config, &mut self.clock)?;
        let until = Instant::now()
            + Duration::from_nanos(
                request
                    .request
                    .deadline_mono_ns
                    .checked_sub(now.nanos())
                    .ok_or(Error::Deadline)?,
            )
            .min(Duration::from_millis(config.startup_timeout_ms));
        let lane = crate::worker::connect_intent(&config, &mut self.clock, until)?;
        self.check_install(&request)?;
        let checkpoint = computed.live.committed().ok_or(Error::NotReady)?.0;
        computed.checkpoint.verify(
            &checkpoint.bytes()?,
            tickwire_release::replay::MAX_BYTES as u64,
        )?;
        let installation = Installation {
            schema: 1,
            request: Object::of(&request.request.bytes()?),
            capture_completion: request.request.capture_completion,
            capture_seal: request.request.capture_seal,
            capture_receipt: request.request.capture_receipt,
            warm_completion: computed.warm_completion,
            target: checkpoint.source.clone(),
            anchor: computed.anchor,
            checkpoint: computed.checkpoint,
            genesis_commit: computed.genesis_commit,
            commit: computed.commit,
            head,
            pid: std::process::id(),
            completed_ns: sample(&self.config, &mut self.clock)?.nanos(),
        };
        let root = computed.live.store_object(&installation.bytes()?)?;
        let plans = Plans::decode(&self.read_object(request.request.plans)?)?;
        let manifest: tickwire_release::ReleaseManifest =
            serde_json::from_slice(&self.read_object(request.request.manifest)?)?;
        let contract: &Contract = self.sealed.window.contract();
        let inputs: &Inputs = self.sealed.window.inputs();
        let verified = tickwire_release::rebuild::install::Verified::verify(
            root,
            &request.request,
            &plans,
            contract,
            inputs,
            &manifest,
            self.sealed.window.seal(),
            |object| {
                reader
                    .get_object(object.digest, tickwire_recovery::MAX_OBJECT_BYTES)
                    .map_err(|_| tickwire_release::Error::Missing)
            },
        )?;
        for bytes in verified.objects() {
            retain(&mut objects, bytes.to_vec())?;
        }
        retain(&mut objects, installation.bytes()?)?;
        if objects.len() > 512
            || objects.values().map(Vec::len).sum::<usize>()
                > tickwire_recovery::MAX_CLOSURE_BYTES as usize
        {
            return Err(Error::Bounds);
        }
        let objects = objects.into_values().collect::<Vec<_>>();
        let template_object = manifest
            .targets
            .iter()
            .find(|target| target.member == request.request.member)
            .and_then(|target| {
                target
                    .files
                    .iter()
                    .find(|file| file.role == tickwire_release::Role::RecoveryTemplate)
            })
            .ok_or(Error::Identity)?
            .object;
        let template = read_template(
            config.recovery_template.as_ref().ok_or(Error::Identity)?,
            template_object,
        )?;
        let (recovery, mirror) = crate::recovery::RecoveryWriter::from_rebuild(
            template,
            &self.sealed.window,
            &computed.live,
            &verified,
            &objects,
        )?;
        self.check_install(&request)?;
        let mut output = FileBlobStore::open(&self.config.output_directory)?;
        for bytes in &objects {
            output.put_sync(StoreKey::Object(tickwire_recovery::digest(bytes)), bytes)?;
            self.check_install(&request)?;
        }
        let now = sample(&self.config, &mut self.clock)?;
        let receipt = crate::capture::install::Receipt {
            schema: 1,
            request: Object::of(&request.bytes()?),
            installation: root,
            pid: std::process::id(),
            report_ring_sequence: self.reports.last_ring_sequence()?,
            market_applied: self.client.consumer.watermarks().applied,
            observed_mono_ns: now.nanos(),
        };
        receipt.verify(
            &self.config,
            &request,
            &self.sealed.receipt,
            &verified,
            std::process::id(),
            now,
        )?;
        let bytes = receipt.bytes()?;
        let receipt_object = Object::of(&bytes);
        output.put_sync(StoreKey::Object(receipt_object.digest), &bytes)?;
        drop(output);
        self.check_install(&request)?;
        let ready = mirror.ready(
            &config.market.local,
            self.reports.last_ring_sequence()?,
            1,
            &recovery,
            &computed.live.checkpoint()?,
        )?;
        self.check_install(&request)?;
        let deadline = Instant::now() + Duration::from_secs(1);
        let reply =
            crate::capture::encode_reply(&self.session, &self.install_request, receipt_object)?;
        self.connection.send(&reply, &[], deadline)?;
        let ready_bytes = self.session.encode(
            tickwire_ipc::ControlEnvelope {
                opcode: 37,
                flags: 32,
                request_id: config.market.local.instance,
                ..Default::default()
            },
            &crate::bootstrap::wire_bytes(&ready)?,
        )?;
        self.connection.send(&ready_bytes, &[], deadline)?;
        self.check_install(&request)?;
        let sequence = computed
            .live
            .committed()
            .ok_or(Error::NotReady)?
            .0
            .cut
            .sequence;
        let state = crate::worker::Attached {
            config,
            clock: self.clock,
            market: self.client.connection,
            market_session: self.client.session,
            consumer: self.client.consumer,
            j: self.client.join,
            intent: lane.connection,
            intent_session: lane.session,
            writer: lane.writer,
            store: lane.store,
            report: self.connection,
            report_session: self.session,
            report_bank: self.bank,
            report_sub: self.subscription,
            report_membership: self.membership,
            reports: self.reports,
            recovery: Some(recovery),
            strategy: computed.live,
            account_mirror: mirror,
            merged_journal_sequence: sequence,
            ready_sequence: 1,
            rebuild_installation: Some(crate::worker::InstalledControl {
                request: self.install_request,
                receipt: receipt_object,
                verified,
                capture: self.config,
                input: request,
                rollback: None,
            }),
        };
        crate::worker::run_attached(state)
    }
}

fn retain(
    objects: &mut std::collections::BTreeMap<[u8; 32], Vec<u8>>,
    bytes: Vec<u8>,
) -> Result<()> {
    let root = Object::of(&bytes);
    root.validate(tickwire_recovery::MAX_OBJECT_BYTES as u64)?;
    if let Some(prior) = objects.get(&root.digest) {
        if prior != &bytes {
            return Err(Error::Identity);
        }
        return Ok(());
    }
    if objects.len() >= 512
        || objects
            .values()
            .map(Vec::len)
            .sum::<usize>()
            .checked_add(bytes.len())
            .is_none_or(|n| n > tickwire_recovery::MAX_CLOSURE_BYTES as usize)
    {
        return Err(Error::Bounds);
    }
    objects.insert(root.digest, bytes);
    Ok(())
}

fn read_template(path: &std::path::Path, expected: Object) -> Result<crate::recovery::Template> {
    expected.validate(crate::capture::MAX_BYTES as u64)?;
    let bytes = tickwire_release::files::read_object(path, expected)?;
    Ok(serde_json::from_slice(&bytes)?)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn install_rechecks_the_template_bytes_against_the_fixed_manifest() {
        use std::os::unix::fs::PermissionsExt;
        let directory =
            std::env::temp_dir().join(format!("tw-rebuild-template-{}", std::process::id()));
        std::fs::create_dir(&directory).unwrap();
        std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700)).unwrap();
        let path = directory.join("template.json");
        let mut template = crate::recovery::Template {
            dependencies: vec![],
            declared_rebuild: true,
            fault_model_id: [1; 32],
            pin_id: [2; 16],
            owner: [3; 16],
            input_stream: [4; 16],
            output_directory: directory.join("original"),
            maximum_input_bytes: 4096,
            order_ids: vec![],
        };
        let bytes = serde_json::to_vec(&template).unwrap();
        std::fs::write(&path, &bytes).unwrap();
        let root = Object::of(&bytes);
        assert_eq!(
            read_template(&path, root).unwrap().output_directory,
            template.output_directory
        );
        template.output_directory = directory.join("replaced");
        std::fs::write(&path, serde_json::to_vec(&template).unwrap()).unwrap();
        let rejected = read_template(&path, root).is_err();
        std::fs::remove_dir_all(directory).unwrap();
        assert!(
            rejected,
            "valid JSON from a replaced path must not replace the approved recovery template"
        );
    }
    #[test]
    fn installation_retains_opaque_dependencies_above_typed_message_limit() {
        let mut map = std::collections::BTreeMap::new();
        let bytes = vec![7; tickwire_release::replay::MAX_BYTES + 1];
        assert!(retain(&mut map, bytes).is_ok());
        assert_eq!(map.len(), 1);
    }
}
