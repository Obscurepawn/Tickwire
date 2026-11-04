//! A stable private input cut plus two matching authenticated venue scans can advance one cash account.
mod compare;
mod ingest;
mod private_proof;
mod proof;
use crate::{AccountRunner, Error, Result};
pub use proof::{CashProof, CashVerifier};
use tickwire_account::{Reactivation, RecoveryRecord};
use tickwire_account_gateway::{Authority, ConnectionConfig};
use tickwire_account_journal::Backend;
use tickwire_clock::MonoTime;
use tickwire_recovery::BlobStore;
use tickwire_spot_execution::{
    SessionError,
    recovery::{OrderLookup, RestEvidence},
};
use tickwire_venue::Product;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrivateCut {
    pub stream: [u8; 16],
    pub epoch: u64,
    pub gap_generation: u64,
    pub through: u64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CashUpdate {
    Accounted,
    Reconcile,
}
impl PrivateCut {
    pub fn stable([before, after]: [Self; 2]) -> Result<Self> {
        if before != after
            || before.stream == [0; 16]
            || before.epoch == 0
            || before.gap_generation == 0
            || before.through == 0
        {
            return Err(Error::Origin(
                "private source moved, was disconnected or has no durable cut",
            ));
        }
        Ok(after)
    }
}

impl<
    'a,
    B: Backend,
    A: Authority,
    T: FnMut() -> std::result::Result<u64, tickwire_gateway::Error>,
    S: BlobStore,
> AccountRunner<'a, B, A, T, S>
{
    pub fn freeze_cash(&mut self) {
        self.session.gateway_mut().freeze_account();
    }
    pub fn halted(&self) -> bool {
        self.session.gateway().halted()
    }
    pub fn has_pending(&self) -> bool {
        self.pending.is_some()
    }
    pub fn transport_needs_poll(&self) -> bool {
        self.has_pending() || (!self.halted() && !self.session.gateway().transport_ready())
    }
    pub fn execution_epoch(&self) -> u64 {
        self.session.connection_epoch()
    }
    pub fn next_execution_epoch(&self) -> Result<u64> {
        self.latest_activation()
            .map_or(self.execution_epoch(), |b| {
                b.execution_connection_epoch.max(self.execution_epoch())
            })
            .checked_add(1)
            .ok_or(Error::Origin("execution epoch exhausted"))
    }
    fn latest_activation(&self) -> Option<&Reactivation> {
        self.session.gateway().latest_reactivation()
    }
    pub fn qualified_cut(&self) -> Result<Option<PrivateCut>> {
        Ok(self.latest_activation().map(|b| PrivateCut {
            stream: b.input_stream,
            epoch: b.private_connection_epoch,
            gap_generation: b.gap_generation,
            through: b.through_sequence,
        }))
    }
    pub fn coverage_start<F: BlobStore>(&self, verifier: &CashVerifier<F>) -> Result<u64> {
        verifier.coverage_start_at(self.latest_activation())
    }
    pub fn recovery_targets(&self) -> Result<Vec<OrderLookup>> {
        let account = self.account();
        account
            .order_keys()
            .filter_map(|k| {
                let o = account.order(k)?;
                (!o.aborted && (!o.terminal || !o.fee_reconciled)).then(|| {
                    account
                        .order_terms(k)
                        .map(|(i, _)| OrderLookup {
                            symbol: i.symbol.as_str().into(),
                            client_id: k.client_id.as_str().into(),
                        })
                        .ok_or(Error::Origin("missing admitted order terms"))
                })
            })
            .collect()
    }
    #[allow(
        clippy::too_many_arguments,
        reason = "the two authenticated scans, private cut, deadlines and connection rotation belong to the same activation"
    )]
    pub fn reconcile_cash<F: BlobStore>(
        &mut self,
        verifier: &mut CashVerifier<F>,
        first: &RestEvidence,
        second: &RestEvidence,
        cuts: [PrivateCut; 2],
        now: MonoTime,
        valid_until: MonoTime,
        writer_epoch: u64,
        replacement: Option<(u64, ConnectionConfig)>,
    ) -> Result<CashProof> {
        self.freeze_cash();
        let cut = PrivateCut::stable(cuts)?;
        if self.pending.is_some()
            || self.halted() != replacement.is_some()
            || now.domain() != valid_until.domain()
            || now.nanos() == 0
            || valid_until.nanos() <= now.nanos()
        {
            return Err(Error::Origin(
                "unsettled request, invalid clock or missing execution connection",
            ));
        }
        if let Some((epoch, _)) = &replacement
            && *epoch < self.next_execution_epoch()?
        {
            return Err(Error::Origin("execution connection epoch must advance"));
        }
        if let Some(old) = self.latest_activation()
            && (old.input_stream != cut.stream
                || old.through_sequence > cut.through
                || old.private_connection_epoch > cut.epoch
                || old.writer_epoch > writer_epoch)
        {
            return Err(Error::Origin(
                "account input, private connection or writer epoch regressed",
            ));
        }
        let reader = verifier.current(self.scope(), cut, now.domain().boot_id())?;
        let start = self.coverage_start(verifier)?;
        let (one, two) = verifier.scans(first, second, self.scope(), start)?;
        self.apply_scan(&two, cut, now.nanos())?;
        let gateway = self.session.gateway();
        let available = compare::account(gateway.baseline(), self.account(), &two)?;
        let execution_epoch = replacement
            .as_ref()
            .map_or(self.execution_epoch(), |(epoch, _)| *epoch);
        let mut binding = Reactivation {
            scope: self.scope().clone(),
            product: Product::Spot,
            account_epoch: self.account().account_epoch(),
            ledger_sequence: self.account().ledger_sequence(),
            baseline_digest: gateway.baseline().digest(),
            history_digest: gateway.history_frontier().digest(),
            input_stream: cut.stream,
            private_connection_epoch: cut.epoch,
            gap_generation: cut.gap_generation,
            through_sequence: cut.through,
            query_coverage_digest: [0; 32],
            clock_boot_id: now.domain().boot_id(),
            clock_generation: now.domain().generation(),
            evaluated_at: now.nanos(),
            valid_until: valid_until.nanos(),
            writer_epoch,
            execution_connection_epoch: execution_epoch,
            venue_available: available,
        };
        verifier.archive(gateway.baseline(), self.account(), &mut binding, &one, &two)?;
        verifier.current(self.scope(), cut, now.domain().boot_id())?;
        let receipt = self
            .session
            .reactivate(
                binding.clone(),
                replacement.map(|(_, c)| c),
                &mut &*verifier,
            )
            .map_err(Error::Session)?;
        verifier.committed(&binding, two.end_ms);
        Ok(CashProof::committed(&binding, receipt, valid_until, reader))
    }
}
