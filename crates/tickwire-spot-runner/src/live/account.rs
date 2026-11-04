use super::*;
use tickwire_account_gateway::Authority;
use tickwire_runtime_policy::ControlPlane;
use tickwire_spot_execution::private::stream::StreamReader;

pub(super) struct PrivateAccount<'a, A: Authority, T: FnMut() -> tickwire_gateway::Result<u64>> {
    pub runner: AccountRunner<'a, FileBackend, A, T, SharedBlobStore<FileBlobStore>>,
    pub verifier: CashVerifier<FileBlobStore>,
    pub reader: StreamReader,
    pub plane: ControlPlane,
    pub permission: Rc<Cell<Option<PrivateCut>>>,
    pub signing: Rc<Cell<Option<(u64, Instant)>>>,
    pub writer_epoch: u64,
}
impl<A: Authority, T: FnMut() -> tickwire_gateway::Result<u64>> PrivateAccount<'_, A, T> {
    pub fn resume(&self) -> Result<u64> {
        let prior = self.runner.qualified_cut()?;
        if let Some(cut) = prior {
            if cut.stream != self.reader.config().stream_id
                || !self.reader.has_initial_cursor(cut.epoch, cut.through)
            {
                return Err(
                    "private capture does not contain the account journal's last verified cut"
                        .into(),
                );
            }
            self.plane
                .private_connected(cut.epoch)
                .map_err(protocol_error)?;
            self.plane
                .private_gap(cut.epoch, cut.gap_generation)
                .map_err(protocol_error)?;
            self.plane
                .input_applied(cut.stream, cut.through)
                .map_err(protocol_error)?;
        }
        Ok(prior.map_or(0, |cut| cut.through))
    }
    pub fn drain(&mut self, mut after: u64) -> Result<u64> {
        if let Some(status) = self.reader.status()
            && status.durable_sequence > after
        {
            for e in self.reader.drain(after, 64)? {
                if e.sequence != after.checked_add(1).ok_or("private sequence overflow")? {
                    return Err("discontinuous private capture".into());
                }
                let mut continuous = false;
                let cut = PrivateCut {
                    stream: self.reader.config().stream_id,
                    epoch: e.epoch,
                    gap_generation: e.epoch,
                    through: e.sequence,
                };
                match e.kind {
                    EventKind::Started { .. } => {
                        self.plane
                            .private_connected(e.epoch)
                            .map_err(protocol_error)?;
                        self.plane
                            .private_gap(e.epoch, e.epoch)
                            .map_err(protocol_error)?;
                    }
                    EventKind::Ready => {
                        self.plane
                            .private_connected(e.epoch)
                            .map_err(protocol_error)?;
                    }
                    EventKind::Gap { reason } => {
                        println!(
                            "{}",
                            serde_json::json!({"event":"spot_private_gap","private_epoch":e.epoch,"private_sequence":e.sequence,"reason":reason})
                        );
                        self.plane
                            .private_gap(e.epoch, e.epoch)
                            .map_err(protocol_error)?;
                    }
                    EventKind::Data { channel, row } => {
                        let name = match channel {
                            Channel::Orders => "orders",
                            Channel::Fills => "fills",
                            Channel::Balance => "balance",
                        };
                        match self
                            .runner
                            .apply_private_cash(name, &row, cut, e.received_ns)
                        {
                            Ok(CashUpdate::Accounted) => {
                                continuous = self.permission.get().is_some_and(|old| {
                                    old.epoch == cut.epoch && old.through == after
                                }) && !self.runner.account().frozen();
                            }
                            Ok(CashUpdate::Reconcile) => {}
                            Err(err) => eprintln!(
                                "private account row requires reconciliation at {}: {}",
                                e.sequence, err
                            ),
                        }
                    }
                }
                if continuous {
                    self.permission.set(Some(cut));
                } else {
                    self.permission.set(None);
                    self.runner.freeze_cash();
                }
                self.plane
                    .input_applied(self.reader.config().stream_id, e.sequence)
                    .map_err(protocol_error)?;
                after = e.sequence;
            }
        }
        Ok(after)
    }
    pub fn current(&self, through: u64, now: MonoTime) -> Option<PrivateCut> {
        let s = self.reader.status()?;
        s.current(s.epoch, through, now.nanos(), MAX_PRIVATE_AGE_NS)
            .then(|| PrivateCut {
                stream: self.reader.config().stream_id,
                epoch: s.epoch,
                gap_generation: s.epoch,
                through,
            })
    }
    pub fn activate(
        &mut self,
        cuts: [PrivateCut; 2],
        scan: scan::Evidence,
        clock: &mut SystemClock,
        connection: &impl Fn() -> ConnectionConfig,
    ) -> Result<bool> {
        let now = clock.sample().map_err(protocol_error)?.monotonic;
        let until = MonoTime::new(
            now.domain(),
            now.nanos().checked_add(GRANT_NS).ok_or("clock overflow")?,
        );
        let replacement = if self.runner.halted() {
            Some((self.runner.next_execution_epoch()?, connection()))
        } else {
            None
        };
        let proof = self.runner.reconcile_cash(
            &mut self.verifier,
            &scan.first,
            &scan.second,
            cuts,
            now,
            until,
            self.writer_epoch,
            replacement,
        )?;
        let cut = cuts[1];
        let now = clock.sample().map_err(protocol_error)?.monotonic;
        if self.current(cut.through, now) != Some(cut) || now.nanos() >= until.nanos() {
            self.runner.freeze_cash();
            return Ok(false);
        }
        proof.install(&self.plane).map_err(protocol_error)?;
        self.signing.set(Some(scan.clock));
        self.permission.set(Some(cut));
        Ok(true)
    }
}
