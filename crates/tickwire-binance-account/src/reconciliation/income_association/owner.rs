use super::authority::hash;
use super::encoding::{AssociationProposal, AssociationRecord, Group, Manifest};
use super::*;
use income_closure::ClosureRequest;
use tickwire_account::{Account, IncomeObservationsAssociated, ObservationAssociation};
use tickwire_account_codec::CanonicalRecord;
use tickwire_account_journal::HistoryFrontier;
use tickwire_clock::{ClockDomain, MonoTime};

pub struct AssociationRequest {
    pub request_id: [u8; 16],
    pub wallet_id: u64,
    pub associations: Vec<ObservationAssociation>,
    pub queries: ClosureRequest,
    pub certificate: SignedAssociationAuthority,
}
impl<S: Store> AccountIngress<S> {
    pub(super) fn proposal(
        &self,
        account: &Account,
        history: &HistoryFrontier,
        prior: &[CanonicalRecord],
        request: AssociationRequest,
    ) -> Result<AssociationProposal> {
        if request.associations.is_empty()
            || request.associations.len() > tickwire_account::MAX_ASSOCIATION_OBSERVATIONS
            || request.request_id == [0; 16]
            || request.associations.iter().any(|g| {
                g.incomes.is_empty() || g.incomes.len() > tickwire_account::MAX_ASSOCIATION_INCOMES
            })
            || request
                .associations
                .iter()
                .map(|g| g.incomes.len())
                .sum::<usize>()
                > tickwire_account::MAX_ASSOCIATION_INCOMES
        {
            return Err(Error::Capacity);
        }
        let (policy_index, policy) = self
            .last_association_policy(self.records.len())
            .ok_or(Error::Identity)?;
        let authority = self
            .config
            .income_association_authority
            .as_ref()
            .ok_or(Error::Identity)?;
        if policy.writer != self.config.writer_epoch || policy.authority() != *authority {
            return Err(Error::Identity);
        }
        authority.verify_certificate(&request.certificate, request.queries.cut.now, true)?;
        let claims = request.certificate.claims();
        if claims.scope != self.config.baseline.snapshot().scope
            || claims.wallet_id != request.wallet_id
            || claims.writer_epoch != self.config.writer_epoch
        {
            return Err(Error::Identity);
        }
        let mut groups = Vec::new();
        for group in request.associations {
            let (index, meta, raw, input) = self
                .records
                .iter()
                .enumerate()
                .find_map(|(i, r)| match r {
                    Record::Event(meta, raw, Decision::AccountBound(input))
                        if input.digest() == group.observation_digest =>
                    {
                        Some((i, meta, raw, input))
                    }
                    _ => None,
                })
                .ok_or(Error::Identity)?;
            groups.push(Group {
                input: self.reference(index as u64 + 1)?,
                canonical: input.digest(),
                canonical_length: input.bytes().len() as u64,
                epoch: meta.epoch,
                raw_hash: hash(raw),
                incomes: group.incomes,
            });
        }
        groups.sort_by_key(|g| g.input.sequence);
        let interval = self.association_interval(account, prior, &groups, request.wallet_id)?;
        let m = Manifest {
            query: self.query_manifest(account, history, request.queries, interval.selected)?,
            request: request.request_id,
            wallet: request.wallet_id,
            instance: authority.execution_instance,
            certificate: request.certificate.digest(),
            policy: self.reference(policy_index as u64 + 1)?,
            anchor: interval.anchor,
            state: account
                .income_association_state_digest(
                    self.config.baseline.digest(),
                    history.digest(),
                    request.wallet_id,
                )
                .map_err(|_| Error::Identity)?,
            groups,
            account_sequence: history.record_count(),
        };
        let bytes = m.bytes()?;
        let proposal = AssociationProposal { manifest: m, bytes };
        self.verify_association_manifest(account, history, prior, &proposal)?;
        let objects = self.closure_store.as_ref().ok_or(Error::Identity)?;
        objects.load(claims.authorization_digest)?;
        for bytes in [
            &proposal.bytes[..],
            &request.certificate.bytes()[..],
            &codec::encode(&Record::AssociationPolicy(policy.clone()))?[..],
        ] {
            if objects.publish(bytes)? != hash(bytes) {
                return Err(Error::Identity);
            }
        }
        Ok(proposal)
    }
    fn verify_association_manifest(
        &self,
        account: &Account,
        history: &HistoryFrontier,
        prior: &[CanonicalRecord],
        proposal: &AssociationProposal,
    ) -> Result<()> {
        let m = &proposal.manifest;
        let q = &m.query;
        let b = &q.original;
        if m.account_sequence != history.record_count()
            || prior.len() as u64 != history.record_count()
            || m.state
                != account
                    .income_association_state_digest(b.baseline_digest, b.history_digest, m.wallet)
                    .map_err(|_| Error::Identity)?
            || proposal.bytes != m.bytes()?
            || m.request == [0; 16]
            || m.instance == [0; 16]
            || m.certificate == [0; 32]
        {
            return Err(Error::Identity);
        }
        let interval = self.association_interval(account, prior, &m.groups, m.wallet)?;
        if interval.anchor != m.anchor {
            return Err(Error::Identity);
        }
        self.verify_query_manifest(account, history, q, q.writer, &interval.selected)?;
        if m.policy.sequence > b.through_sequence
            || m.policy != self.reference(m.policy.sequence)?
            || self
                .last_association_policy(b.through_sequence as usize)
                .map(|(i, _)| i as u64 + 1)
                != Some(m.policy.sequence)
        {
            return Err(Error::Identity);
        }
        let Record::AssociationPolicy(policy) = &self.records[m.policy.sequence as usize - 1]
        else {
            return Err(Error::Identity);
        };
        if policy.writer != q.writer
            || policy.execution_instance != m.instance
            || policy
                .active
                .as_ref()
                .is_none_or(|a| a.certificate_digest != m.certificate)
        {
            return Err(Error::Identity);
        }
        for g in &m.groups {
            if g.input.sequence > b.through_sequence {
                return Err(Error::Identity);
            }
            let Record::Event(_, _, Decision::AccountBound(input)) =
                &self.records[g.input.sequence as usize - 1]
            else {
                return Err(Error::Identity);
            };
            let v::AccountInputBody::Observation(o) = &input.input().body else {
                return Err(Error::Identity);
            };
            // E/T are range constraints, never uniqueness keys or a causal match.
            if !(q.start..=q.end).contains(&o.event_time_ms)
                || o.transaction_time_ms
                    .is_some_and(|t| !(q.start..=q.end).contains(&t))
            {
                return Err(Error::Identity);
            }
        }
        for r in prior {
            if let tickwire_account::RecoveryRecord::IngressAccountInput {
                input_stream,
                input_sequence,
                ..
            } = r.record()
                && *input_stream == self.input_stream()
                && *input_sequence > b.through_sequence
            {
                return Err(Error::Identity);
            }
        }
        let mut candidate = account.clone();
        candidate
            .associate_income_observations(&m.binding(proposal.digest(), [1; 32])?)
            .map_err(|_| Error::Identity)?;
        Ok(())
    }
    /// Structural acceptance is reconstructed from a real input record. This is
    /// private, and its stored writer is checked against the physical WAL owner.
    pub(crate) fn validate_association_record(
        &self,
        record: &AssociationRecord,
        sequence: u64,
        physical_writer: u64,
    ) -> Result<()> {
        let m = &record.proposal.manifest;
        let q = &m.query;
        let end = q.original.through_sequence;
        if sequence != end.checked_add(1).ok_or(Error::Identity)?
            || record.writer != physical_writer
            || record.writer != q.writer
            || q.input_prefix_digest != self.prefix_digest(end)?
            || record.proposal.bytes != m.bytes()?
            || record.accepted < q.evaluated
            || record.accepted >= q.expires
        {
            return Err(Error::Identity);
        }
        let (_, policy) = self
            .last_association_policy(end as usize)
            .filter(|(i, _)| *i as u64 + 1 == m.policy.sequence)
            .ok_or(Error::Identity)?;
        if self.reference(m.policy.sequence)? != m.policy
            || policy.writer != record.writer
            || policy.execution_instance != m.instance
        {
            return Err(Error::Identity);
        }
        let pinned = self
            .config
            .income_association_authority
            .as_ref()
            .ok_or(Error::Identity)?;
        if policy.controller_id != pinned.controller_id || policy.root != pinned.public_key {
            return Err(Error::Identity);
        }
        let domain = ClockDomain::new(q.boot, q.generation).map_err(|_| Error::Identity)?;
        let claims = policy.authority().verify(
            &record.approval,
            MonoTime::new(domain, record.accepted),
            true,
        )?;
        let certificate = record.approval.authority().claims();
        if claims.manifest != record.proposal.digest()
            || claims.request != m.request
            || claims.authority != m.certificate
            || certificate.scope != q.original.scope
            || certificate.wallet_id != m.wallet
            || certificate.writer_epoch != record.writer
            || certificate.execution_instance != m.instance
        {
            return Err(Error::Identity);
        }
        if self.records.iter().take(sequence as usize - 1).any(
            |r| matches!(r,Record::IncomeAssociation(old) if old.proposal.request_id()==m.request),
        ) {
            return Err(Error::Conflict);
        }
        Ok(())
    }
    fn association_record(
        &self,
        binding: &IncomeObservationsAssociated,
    ) -> Result<&AssociationRecord> {
        let Record::IncomeAssociation(record) = self
            .records
            .get(
                binding
                    .through_sequence
                    .checked_sub(1)
                    .ok_or(Error::Identity)? as usize,
            )
            .ok_or(Error::Identity)?
        else {
            return Err(Error::Identity);
        };
        if record
            .proposal
            .manifest
            .binding(record.proposal.digest(), record.approval.digest())?
            != *binding
        {
            return Err(Error::Identity);
        }
        Ok(record)
    }
    pub fn income_association_bindings(
        &self,
    ) -> impl Iterator<Item = Result<IncomeObservationsAssociated>> + '_ {
        self.records.iter().filter_map(|r| match r {
            Record::IncomeAssociation(r) => Some(
                r.proposal
                    .manifest
                    .binding(r.proposal.digest(), r.approval.digest()),
            ),
            _ => None,
        })
    }
    /// Returns exact, hash-checked synchronized closure objects for replication.
    pub fn income_association_objects(
        &self,
        binding: &IncomeObservationsAssociated,
    ) -> Result<Vec<Vec<u8>>> {
        let r = self.association_record(binding)?;
        self.validate_association_record(r, binding.through_sequence, r.writer)?;
        let store = self.closure_store.as_ref().ok_or(Error::Identity)?;
        let claims = authority::ApprovalClaims::decode(r.approval.payload())?;
        let policy =
            codec::encode(&self.records[r.proposal.manifest.policy.sequence as usize - 1])?;
        let expected = [
            r.proposal.bytes.clone(),
            r.approval.bytes(),
            r.approval.authority().bytes(),
            policy,
        ];
        let mut result = Vec::new();
        for e in expected {
            let b = store.load(hash(&e))?;
            if b != e {
                return Err(Error::Identity);
            }
            result.push(b);
        }
        result.push(store.load(claims.audit)?);
        result.push(store.load(r.approval.authority().claims().authorization_digest)?);
        Ok(result)
    }
    pub(crate) fn verify_income_association(
        &self,
        baseline: &CanonicalBaseline,
        account: &Account,
        history: &HistoryFrontier,
        prior: &[CanonicalRecord],
        binding: &IncomeObservationsAssociated,
        writer: u64,
    ) -> Result<()> {
        if baseline.digest() != self.config.baseline.digest() {
            return Err(Error::Identity);
        }
        let r = self.association_record(binding)?;
        if writer < r.writer || writer > self.config.writer_epoch {
            return Err(Error::Identity);
        }
        self.income_association_objects(binding)?;
        self.verify_association_manifest(account, history, prior, &r.proposal)
    }
}
impl<S: Store, B: Backend, A: Authority, E: Encoder> RestOwner<S, B, A, E> {
    /// Store bounded immutable evidence. This only publishes content-addressed
    /// bytes; authorization still requires both signatures and actual WAL cuts.
    pub fn publish_association_evidence(&self, bytes: &[u8]) -> Result<v::Hash> {
        self.ingress
            .closure_store
            .as_ref()
            .ok_or(Error::Identity)?
            .publish(bytes)
    }
    /// Persist the deployment policy before taking an approval cut. Returns the
    /// updated input position, which the controller must include in its cut.
    pub fn prepare_association_policy(&mut self) -> Result<InputKey> {
        if self.busy() {
            return Err(Error::Busy);
        }
        self.ingress.ready(&self.gateway)?;
        self.ingress.sync_association_policy()?;
        Ok(InputKey {
            stream: self.ingress.input_stream(),
            sequence: self.ingress.position(),
        })
    }
    pub fn propose_income_association(
        &self,
        request: AssociationRequest,
    ) -> Result<AssociationProposal> {
        if self.busy() {
            return Err(Error::Busy);
        }
        self.ingress.ready(&self.gateway)?;
        let prior = self
            .gateway
            .journal_records_after(
                0,
                tickwire_account::MAX_FACTS + tickwire_account::MAX_ORDERS,
            )
            .map_err(Error::Gateway)?
            .map(|(_, r)| r.clone())
            .collect::<Vec<_>>();
        self.ingress.proposal(
            self.account(),
            self.gateway.history_frontier(),
            &prior,
            request,
        )
    }
    pub fn accept_income_association(
        &mut self,
        proposal: AssociationProposal,
        approval: SignedAssociationApproval,
        now: MonoTime,
    ) -> Result<Applied> {
        let request = approval.request_id()?;
        if let Some((index, old)) =
            self.ingress
                .records
                .iter()
                .enumerate()
                .find_map(|(i, r)| match r {
                    Record::IncomeAssociation(r) if r.proposal.request_id() == request => {
                        Some((i, r))
                    }
                    _ => None,
                })
        {
            if old.proposal != proposal || old.approval != approval {
                self.gateway.freeze_account();
                return Err(Error::Conflict);
            }
            let binding = old
                .proposal
                .manifest
                .binding(old.proposal.digest(), old.approval.digest())?;
            if let Some(existing) = self.account().income_association(request) {
                return if existing == &binding {
                    Ok(Applied::Duplicate)
                } else {
                    self.gateway.freeze_account();
                    Err(Error::Conflict)
                };
            }
            if self.ingress.cursor != index {
                return Err(Error::ReplayPending);
            }
            let applied = self
                .gateway
                .associate_income_observations(binding, &mut self.ingress.activation_verifier())
                .map_err(Error::Gateway)?;
            self.ingress.cursor += 1;
            return Ok(applied);
        }
        if self.busy() {
            return Err(Error::Busy);
        }
        self.ingress.ready(&self.gateway)?;
        let authority = self
            .ingress
            .config
            .income_association_authority
            .as_ref()
            .ok_or(Error::Identity)?;
        authority.verify(&approval, now, true)?;
        let prior = self
            .gateway
            .journal_records_after(
                0,
                tickwire_account::MAX_FACTS + tickwire_account::MAX_ORDERS,
            )
            .map_err(Error::Gateway)?
            .map(|(_, r)| r.clone())
            .collect::<Vec<_>>();
        self.ingress.verify_association_manifest(
            self.account(),
            self.gateway.history_frontier(),
            &prior,
            &proposal,
        )?;
        if proposal.manifest.query.original.through_sequence != self.ingress.position()
            || proposal.manifest.query.writer != self.ingress.config.writer_epoch
            || now.domain() != approval.authority().claims().clock_domain
        {
            return Err(Error::Identity);
        }
        let record = AssociationRecord {
            proposal,
            approval,
            accepted: now.nanos(),
            writer: self.ingress.config.writer_epoch,
        };
        let sequence = self
            .ingress
            .position()
            .checked_add(1)
            .ok_or(Error::Identity)?;
        self.ingress
            .validate_association_record(&record, sequence, record.writer)?;
        let store = self.ingress.closure_store.as_ref().ok_or(Error::Identity)?;
        let claims = authority::ApprovalClaims::decode(record.approval.payload())?;
        store.load(claims.audit)?;
        store.load(record.approval.authority().claims().authorization_digest)?;
        let policy = codec::encode(
            &self.ingress.records[record.proposal.manifest.policy.sequence as usize - 1],
        )?;
        if store.load(authority::hash(&policy))? != policy {
            return Err(Error::Identity);
        }
        for bytes in [
            &record.proposal.bytes[..],
            &record.approval.bytes()[..],
            &record.approval.authority().bytes()[..],
        ] {
            if store.publish(bytes)? != hash(bytes) {
                return Err(Error::Identity);
            }
        }
        let binding = record
            .proposal
            .manifest
            .binding(record.proposal.digest(), record.approval.digest())?;
        self.ingress
            .append(Record::IncomeAssociation(Box::new(record)))?;
        match self
            .gateway
            .associate_income_observations(binding, &mut self.ingress.activation_verifier())
        {
            Ok(applied) => {
                self.ingress.cursor = self.ingress.records.len();
                Ok(applied)
            }
            Err(error) => {
                self.ingress.stopped = true;
                self.gateway.freeze_account();
                Err(Error::Gateway(error))
            }
        }
    }
}
