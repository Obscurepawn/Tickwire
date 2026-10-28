use super::*;
use tickwire_ipc::{ReplayAppliedBody, ReplayControlBody};
use tickwire_recovery::{BlobStore, StoreKey};
impl<R: RecoveryResolver + 'static> Hosted<R> {
    #[allow(unsafe_code)]
    pub(super) fn promote(&mut self, input: RebuildInput) -> Result<InstalledRebuild> {
        let request = self.installation.as_ref().ok_or(Error::NotReady)?.clone();
        if request.request != *input.request() {
            return Err(Error::Invalid);
        }
        let owner = self.capture.as_mut().ok_or(Error::NotReady)?;
        let captured = owner.captured.as_ref().ok_or(Error::NotReady)?;
        request.validate(
            &owner.bootstrap,
            captured.receipt(),
            captured.window(),
            owner.clock.sample()?.monotonic,
        )?;
        let deadline = Instant::now()
            + Duration::from_nanos(
                request
                    .request
                    .deadline_mono_ns
                    .checked_sub(owner.current()?.nanos())
                    .ok_or(Error::NotReady)?,
            );
        let intent_listener = ControlListener::bind(&request.bootstrap.intent.path)?;
        let mut output = tickwire_recovery::FileBlobStore::open(&owner.bootstrap.output_directory)?;
        for bytes in input.objects() {
            let object = Object::of(bytes);
            output.put_sync(StoreKey::Object(object.digest), bytes)?;
            owner.current()?;
        }
        // Transfer the single durable writer back before the child starts installation.
        drop(output);
        let bytes = request.bytes()?;
        owner.reports.put_object(&bytes)?;
        let wire = install::wire_request(
            &owner.bootstrap,
            &request,
            input.seal(),
            owner.report.session.deadline(),
        )?;
        owner.report.connection.send(
            &tickwire_strategy_sdk::capture::encode_request(&owner.report.session, &wire)?,
            &[],
            deadline,
        )?;
        let expected = owner.registry.process(self.spec.identity.instance)?;
        let mut connection = intent_listener.accept(deadline)?;
        let c = connection.peer_credentials();
        let actual = ProcessIdentity {
            pid: c.pid,
            uid: c.uid,
            gid: c.gid,
        };
        if actual != expected {
            return Err(Error::Invalid);
        }
        let hello = connection.recv(deadline)?;
        if !hello.fds().is_empty() {
            return Err(Error::Invalid);
        }
        let (session, welcome) = Session::accept(
            &server_policy(&request.bootstrap.intent, actual),
            actual,
            hello.bytes(),
            owner.current()?.nanos(),
        )?;
        connection.send(&welcome, &[], deadline)?;
        owner.registry.process(self.spec.identity.instance)?;
        // SAFETY: this object was constructed by the qualified launcher and still
        // owns the same authenticated child; this is the original intent attach.
        let (receiver, intent) = unsafe {
            remote::connect_intent(
                &request.bootstrap,
                AuthenticatedChild {
                    connection,
                    session,
                    process: actual,
                },
                &mut owner.clock,
                deadline,
            )?
        };
        let packet = owner.report.connection.recv(deadline)?;
        if !packet.fds().is_empty() {
            return Err(Error::Invalid);
        }
        let now = owner.current()?;
        let frame = owner.report.session.validate(packet.bytes(), now.nanos())?;
        if frame.envelope.flags == 18 {
            tickwire_strategy_sdk::capture::verify_rejection(
                &frame.envelope,
                &ReplayControlBody::decode(frame.body)?,
                &wire,
                now.nanos(),
            )?;
            return Err(Error::NotReady);
        }
        let receipt_root = capture::protocol::reply(
            &frame.envelope,
            &ReplayAppliedBody::decode(frame.body)?,
            &wire,
            now.nanos(),
        )?;
        let reader = tickwire_recovery::FileBlobReader::open(
            &owner.bootstrap.output_directory,
            owner.child.uid,
        )?;
        let bytes = reader.get_object(
            receipt_root.digest,
            tickwire_strategy_sdk::capture::MAX_BYTES,
        )?;
        receipt_root.verify(&bytes, tickwire_strategy_sdk::capture::MAX_BYTES as u64)?;
        let receipt = install::Receipt::decode(&bytes)?;
        let verified = tickwire_release::rebuild::install::Verified::verify(
            receipt.installation,
            input.request(),
            input.plans(),
            input.contract(),
            input.inputs(),
            input.manifest(),
            input.seal(),
            |o| {
                let bytes = reader
                    .get_object(o.digest, tickwire_recovery::MAX_OBJECT_BYTES)
                    .map_err(|_| tickwire_release::Error::Missing)?;
                o.verify(&bytes, tickwire_recovery::MAX_OBJECT_BYTES as u64)?;
                Ok(bytes)
            },
        )?;
        let now = owner.current()?;
        receipt.verify(
            &owner.bootstrap,
            &request,
            owner.captured.as_ref().ok_or(Error::Invalid)?.receipt(),
            &verified,
            owner.child.pid,
            now,
        )?;
        if !owner.reports.confirms_cursor(
            owner.subscription,
            input
                .seal()
                .end
                .accounts
                .first()
                .ok_or(Error::Invalid)?
                .sequence,
            receipt.report_ring_sequence,
        )? {
            return Err(Error::Invalid);
        }
        let mut objects = input.objects().to_vec();
        objects.extend(verified.objects().map(<[u8]>::to_vec));
        objects.push(bytes);
        let owner = self.capture.take().ok_or(Error::NotReady)?;
        fn ignore(_: &tickwire_binance_public::Notice<'_>) -> bool {
            true
        }
        let transition = market_transition::MarketTransition::new(
            request
                .bootstrap
                .external_market
                .clone()
                .ok_or(Error::Invalid)?,
            request.bootstrap.command_directory.join("merged-inputs"),
            owner.child,
        );
        let remote = RemoteHostedStrategy {
            readiness: readiness::ReadyProof::new(&request.bootstrap, &self.spec)?,
            market_transition: transition,
            report_sub: owner.subscription,
            strategy: RuntimeStrategy::new(
                receiver,
                self.resolver.take().ok_or(Error::Invalid)?,
                ignore as fn(&tickwire_binance_public::Notice<'_>) -> bool,
            ),
            reports: owner.reports,
            registry: owner.registry,
            identity: self.spec.identity.clone(),
            intent,
            report: owner.report,
            clock: owner.clock,
            policy: self.policy.clone(),
            forwarding: false,
            error: None,
            _directory: owner._directory,
            _intent_listener: intent_listener,
            _report_listener: owner._listener,
        };
        self.live = Some(remote);
        let (ready, more) = self
            .live
            .as_mut()
            .ok_or(Error::Invalid)?
            .handover_checkpoint(self.spec.identity.instance, self.spec.identity.generation)
            .ok_or(Error::NotReady)?;
        objects.extend(more);
        let live = self.live.as_mut().ok_or(Error::NotReady)?;
        live.registry.process(self.spec.identity.instance)?;
        if live.clock.sample()?.monotonic.nanos() >= request.request.deadline_mono_ns {
            return Err(Error::NotReady);
        }
        InstalledRebuild::from_authenticated_child(input, verified.root(), ready, objects)
            .map_err(Into::into)
    }
}
