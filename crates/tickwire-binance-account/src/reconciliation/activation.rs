//! Durable activation from authenticated account and both open-order observations.
use super::*;
use bootstrap::{SnapshotEvidence, SnapshotKind};
use tickwire_account::{Account, Reactivation};
use tickwire_account_codec::CanonicalBaseline;
use tickwire_account_gateway::{AuthorityError, ConnectionConfig, ReactivationVerifier};
use tickwire_account_journal::HistoryFrontier;
use tickwire_clock::MonoTime;

#[derive(Clone, Copy, Debug)]
pub struct ActivationCut {
    pub private_connection_epoch: u64,
    pub gap_generation: u64,
    pub through_sequence: u64,
    pub now: MonoTime,
    /// Also bounds the age of the oldest remote observation at activation.
    pub valid_until: MonoTime,
    pub writer_epoch: u64,
    pub execution_connection_epoch: u64,
}
/// Exists only after the complete reconciliation decision has been synchronized.
#[derive(Clone, Debug)]
pub struct ActivationProof {
    binding: Reactivation,
    ledger: u64,
    history: v::Hash,
}
impl ActivationProof {
    pub fn binding(&self) -> &Reactivation {
        &self.binding
    }
    pub fn ledger_sequence(&self) -> u64 {
        self.ledger
    }
    pub fn history_digest(&self) -> v::Hash {
        self.history
    }
    pub fn valid_until(&self) -> MonoTime {
        MonoTime::new(
            tickwire_clock::ClockDomain::new(
                self.binding.clock_boot_id,
                self.binding.clock_generation,
            )
            .expect("validated clock domain"),
            self.binding.valid_until,
        )
    }
}
/// Borrows the already-verified input WAL. Recovery does not consult a live server.
pub struct ActivationVerifier<'a, S: Store> {
    pub(super) input: &'a AccountIngress<S>,
}
impl<S: Store> AccountIngress<S> {
    pub fn activation_verifier(&self) -> ActivationVerifier<'_, S> {
        ActivationVerifier { input: self }
    }
    pub fn activation_coverage_object(&self, binding: &Reactivation) -> Result<Vec<u8>> {
        if binding.input_stream != self.input_stream()
            || binding.baseline_digest != self.config.baseline.digest()
        {
            return Err(Error::Identity);
        }
        let bytes = coverage_bytes(&evidence_at(self, binding.through_sequence)?);
        if ring::digest::digest(&ring::digest::SHA256, &bytes).as_ref()
            != binding.query_coverage_digest
        {
            return Err(Error::Identity);
        }
        Ok(bytes)
    }
}
impl<S: Store> ReactivationVerifier for ActivationVerifier<'_, S> {
    fn verify_income_baseline(
        &mut self,
        baseline: &CanonicalBaseline,
        account: &Account,
        preceding: &HistoryFrontier,
        binding: &tickwire_account::IncomeBaseline,
    ) -> std::result::Result<(), AuthorityError> {
        self.input
            .verify_income_binding(baseline, account, preceding, binding)
            .map_err(|_| AuthorityError::Denied)
    }
    fn verify(
        &mut self,
        baseline: &CanonicalBaseline,
        account: &Account,
        preceding: &HistoryFrontier,
        binding: &Reactivation,
    ) -> std::result::Result<(), AuthorityError> {
        verify(self.input, baseline, account, preceding, binding)
            .map(|_| ())
            .map_err(|_| AuthorityError::Denied)
    }
}
fn digest(evidence: &[SnapshotEvidence; 3]) -> v::Hash {
    ring::digest::digest(&ring::digest::SHA256, &coverage_bytes(evidence))
        .as_ref()
        .try_into()
        .unwrap()
}
fn coverage_bytes(evidence: &[SnapshotEvidence; 3]) -> Vec<u8> {
    let mut bytes = b"tickwire.binance.account-snapshots.v1".to_vec();
    for e in evidence {
        bytes.extend(e.input().stream);
        bytes.extend(e.input().sequence.to_le_bytes());
        bytes.extend(e.raw_hash());
    }
    bytes
}
fn evidence_at<S: Store>(input: &AccountIngress<S>, through: u64) -> Result<[SnapshotEvidence; 3]> {
    let end = usize::try_from(through).map_err(|_| Error::Identity)?;
    if end == 0 || end > input.records.len() {
        return Err(Error::Identity);
    }
    let mut found = Vec::with_capacity(3);
    for (index, record) in input.records[..end].iter().enumerate().rev() {
        if matches!(record, Record::Snapshot(_)) {
            found.push(
                input
                    .snapshot_evidence(index as u64 + 1)?
                    .ok_or(Error::Identity)?,
            );
            if found.len() == 3 {
                break;
            }
        }
    }
    found.reverse();
    let evidence: [SnapshotEvidence; 3] = found.try_into().map_err(|_| Error::Identity)?;
    if evidence.iter().map(|e| e.query().kind).ne([
        SnapshotKind::Account,
        SnapshotKind::OpenOrders,
        SnapshotKind::ConditionalOrders,
    ]) {
        return Err(Error::Identity);
    }
    Ok(evidence)
}
fn verify<S: Store>(
    input: &AccountIngress<S>,
    baseline: &CanonicalBaseline,
    account: &Account,
    preceding: &HistoryFrontier,
    binding: &Reactivation,
) -> Result<[SnapshotEvidence; 3]> {
    if input.config.baseline.digest() != baseline.digest()
        || binding.baseline_digest != baseline.digest()
        || binding.history_digest != preceding.digest()
        || binding.scope != baseline.snapshot().scope
        || binding.product != product(&input.config)?
        || binding.account_epoch != account.account_epoch()
        || binding.ledger_sequence != account.ledger_sequence()
        || binding.input_stream != input.input_stream()
        || binding.evaluated_at >= binding.valid_until
    {
        return Err(Error::Identity);
    }
    let evidence = evidence_at(input, binding.through_sequence)?;
    let age = binding.valid_until - binding.evaluated_at;
    let domain = tickwire_clock::ClockDomain::new(binding.clock_boot_id, binding.clock_generation)
        .map_err(|_| Error::Identity)?;
    for e in &evidence {
        if e.query().private_connection_epoch != binding.private_connection_epoch
            || e.query().gap_generation != binding.gap_generation
            || e.domain() != domain
            || e.ledger_sequence() != binding.ledger_sequence
            || e.baseline_digest() != binding.baseline_digest
            || e.account_stream() != input.config.account_stream
            || e.received_mono_ns() > binding.evaluated_at
            || binding.evaluated_at - e.received_mono_ns() > age
        {
            return Err(Error::Identity);
        }
    }
    let end = binding.through_sequence as usize;
    let last_gap = input.records[..end].iter().rev().find_map(|r| match r {
        Record::BeginReconciliation { epoch, generation } => Some((*epoch, *generation)),
        Record::Gap { .. } => Some((0, 0)),
        _ => None,
    });
    if last_gap != Some((binding.private_connection_epoch, binding.gap_generation)) {
        return Err(Error::Identity);
    }
    for record in &input.records[evidence[0].input().sequence as usize..end] {
        match record {
            Record::Gap { .. } | Record::BeginReconciliation { .. } => return Err(Error::Identity),
            Record::Event(meta, _, _) if meta.epoch != binding.private_connection_epoch => {
                return Err(Error::Identity);
            }
            _ => {}
        }
    }
    if digest(&evidence) != binding.query_coverage_digest {
        return Err(Error::Identity);
    }
    if binding.venue_available != snapshot::venue_available(baseline, evidence[0].body())? {
        return Err(Error::Identity);
    }
    snapshot::verify_snapshot_with_orders(
        input.market()?,
        baseline,
        account,
        evidence[0].body(),
        evidence[1].body(),
        evidence[2].body(),
        &input.registry,
    )?;
    Ok(evidence)
}
impl<S: Store, B: Backend, A: Authority, E: Encoder> RestOwner<S, B, A, E> {
    pub fn begin_reconciliation(
        &mut self,
        token: &tickwire_binance_private::GapToken,
    ) -> Result<InputKey> {
        self.gateway.freeze_account();
        self.ingress.ready(&self.gateway)?;
        let scope = &self.gateway.baseline().snapshot().scope;
        let provided = token.scope();
        if provided.risk_domain_id != scope.risk_domain_id
            || provided.venue != scope.venue
            || provided.environment != scope.environment
            || provided.account != scope.account
            || provided.market != self.ingress.market()?
            || token.connection_epoch() == 0
            || token.generation() == 0
        {
            return Err(Error::Identity);
        }
        let latest = self
            .ingress
            .records
            .iter()
            .filter_map(|r| match r {
                Record::BeginReconciliation { epoch, generation }
                | Record::Gap {
                    epoch, generation, ..
                } => Some((*epoch, *generation)),
                _ => None,
            })
            .max();
        if latest.is_some_and(|old| (token.connection_epoch(), token.generation()) < old) {
            return Err(Error::Identity);
        }
        for (index, record) in self.ingress.records.iter().enumerate().rev() {
            match record {
                Record::BeginReconciliation { epoch, generation } => {
                    if *epoch == token.connection_epoch() && *generation == token.generation() {
                        return Ok(InputKey {
                            stream: self.ingress.input_stream(),
                            sequence: index as u64 + 1,
                        });
                    }
                    break;
                }
                Record::Gap { .. } => break,
                _ => {}
            }
        }
        let input = self.ingress.append(Record::BeginReconciliation {
            epoch: token.connection_epoch(),
            generation: token.generation(),
        })?;
        self.ingress.cursor = self.ingress.records.len();
        Ok(input)
    }
    pub fn reactivate(
        &mut self,
        evidence: [SnapshotEvidence; 3],
        cut: ActivationCut,
        connection: Option<ConnectionConfig>,
    ) -> Result<ActivationProof> {
        if self.busy() {
            return Err(Error::Busy);
        }
        self.ingress.ready(&self.gateway)?;
        if cut.now.domain() != cut.valid_until.domain()
            || cut.now.nanos() >= cut.valid_until.nanos()
            || cut.through_sequence != self.applied_position()
            || cut.writer_epoch < self.ingress.config.writer_epoch
            || if connection.is_some() {
                cut.execution_connection_epoch <= self.config.connection_epoch
            } else {
                cut.execution_connection_epoch != self.config.connection_epoch
            }
        {
            return Err(Error::Identity);
        }
        let binding = Reactivation {
            scope: self.gateway.baseline().snapshot().scope.clone(),
            product: product(&self.ingress.config)?,
            account_epoch: self.account().account_epoch(),
            ledger_sequence: self.account().ledger_sequence(),
            baseline_digest: self.gateway.baseline().digest(),
            history_digest: self.gateway.history_frontier().digest(),
            input_stream: self.ingress.input_stream(),
            private_connection_epoch: cut.private_connection_epoch,
            gap_generation: cut.gap_generation,
            through_sequence: cut.through_sequence,
            query_coverage_digest: digest(&evidence),
            clock_boot_id: cut.now.domain().boot_id(),
            clock_generation: cut.now.domain().generation(),
            evaluated_at: cut.now.nanos(),
            valid_until: cut.valid_until.nanos(),
            writer_epoch: cut.writer_epoch,
            execution_connection_epoch: cut.execution_connection_epoch,
            venue_available: snapshot::venue_available(
                self.gateway.baseline(),
                evidence[0].body(),
            )?,
        };
        // Re-read the synchronized records, so a capability from another WAL cannot be reused.
        verify(
            &self.ingress,
            self.gateway.baseline(),
            self.account(),
            self.gateway.history_frontier(),
            &binding,
        )?;
        let receipt = self
            .gateway
            .reactivate(
                binding.clone(),
                connection,
                &mut self.ingress.activation_verifier(),
            )
            .map_err(Error::Gateway)?;
        self.config.connection_epoch = cut.execution_connection_epoch;
        self.ingress.config.writer_epoch = cut.writer_epoch;
        Ok(ActivationProof {
            binding,
            ledger: receipt.ledger_sequence,
            history: receipt.history_digest,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tickwire_account::RecoveryRecord;
    use tickwire_account_codec::CanonicalRecord;
    fn binding(baseline: &CanonicalBaseline) -> Reactivation {
        Reactivation {
            scope: baseline.snapshot().scope.clone(),
            product: v::Product::Spot,
            account_epoch: baseline.snapshot().account_epoch,
            ledger_sequence: baseline.snapshot().ledger_sequence,
            baseline_digest: baseline.digest(),
            history_digest: [3; 32],
            input_stream: [4; 16],
            private_connection_epoch: 2,
            gap_generation: 3,
            through_sequence: 8,
            query_coverage_digest: [5; 32],
            clock_boot_id: [6; 16],
            clock_generation: 1,
            evaluated_at: 10,
            valid_until: 2000,
            writer_epoch: 1,
            execution_connection_epoch: 1,
            venue_available: Vec::new(),
        }
    }
    #[test]
    fn activation_is_a_replayable_record_before_later_admission() {
        let (snapshot, catalog, command) = crate::unit_fixture::fixture();
        let baseline = CanonicalBaseline::new(snapshot.clone(), catalog.clone()).unwrap();
        let activation = binding(&baseline);
        let record = RecoveryRecord::Reactivated(Box::new(activation.clone()));
        let encoded = CanonicalRecord::new(baseline.digest(), 1, record.clone()).unwrap();
        assert_eq!(CanonicalRecord::decode(encoded.bytes()).unwrap(), encoded);
        let mut account = Account::from_snapshot(snapshot.clone(), catalog.clone()).unwrap();
        account.freeze();
        account.apply_reactivation(&activation).unwrap();
        assert!(!account.frozen());
        assert_eq!(account.ledger_sequence(), 1);
        let command = crate::unit_fixture::canonical(command);
        let admission = account.admit(&command, 20).unwrap();
        let replay = Account::recover(
            snapshot,
            catalog,
            &[
                record,
                RecoveryRecord::Admit {
                    command: Box::new(command),
                    evaluated_at: 20,
                    expected: admission.image(),
                },
            ],
        )
        .unwrap();
        assert!(replay.frozen());
        assert_eq!(replay.ledger_sequence(), account.ledger_sequence());
        assert_eq!(
            replay.admission(admission.intent()).unwrap().image(),
            admission.image()
        );
    }
    #[test]
    fn stale_activation_does_not_publish_account_changes() {
        let (snapshot, catalog, _) = crate::unit_fixture::fixture();
        let baseline = CanonicalBaseline::new(snapshot.clone(), catalog.clone()).unwrap();
        let mut account = Account::from_snapshot(snapshot, catalog).unwrap();
        account.freeze();
        let mut stale = binding(&baseline);
        stale.ledger_sequence += 1;
        assert!(account.apply_reactivation(&stale).is_err());
        assert!(account.frozen());
        assert_eq!(account.ledger_sequence(), 0);
    }
    #[test]
    fn durable_available_rebase_replaces_old_holds_without_double_reservation() {
        let (snapshot, catalog, command) = crate::unit_fixture::futures_fixture();
        let baseline = CanonicalBaseline::new(snapshot.clone(), catalog.clone()).unwrap();
        let command = crate::unit_fixture::canonical(command);
        let mut account = Account::from_snapshot(snapshot.clone(), catalog.clone()).unwrap();
        let admission = account.admit(&command, 20).unwrap();
        assert!(
            account.venue_shadow(1).unwrap().atoms() < crate::unit_fixture::units(1000).atoms()
        );
        let mut activation = binding(&baseline);
        activation.product = v::Product::LinearPerpetual;
        activation.ledger_sequence = account.ledger_sequence();
        activation.venue_available = vec![(1, crate::unit_fixture::units(950))];
        account.freeze();
        account.apply_reactivation(&activation).unwrap();
        assert_eq!(
            account.venue_shadow(1).unwrap(),
            crate::unit_fixture::units(950)
        );
        let records = [
            RecoveryRecord::Admit {
                command: Box::new(command),
                evaluated_at: 20,
                expected: admission.image(),
            },
            RecoveryRecord::Reactivated(Box::new(activation)),
        ];
        let replay = Account::recover(snapshot, catalog, &records).unwrap();
        assert_eq!(
            replay.venue_shadow(1).unwrap(),
            crate::unit_fixture::units(950)
        );
        assert_eq!(replay.ledger_sequence(), account.ledger_sequence());
        assert!(replay.frozen());
    }
}
