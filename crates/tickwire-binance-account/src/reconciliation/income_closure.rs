//! Account-input closure from existing synchronized queries and snapshot records.
use super::*;
use activation::ActivationCut;
use std::cell::RefCell;
use tickwire_account::{Account, AccountInputsReconciled, Reactivation};
use tickwire_account_journal::HistoryFrontier;
use tickwire_recovery::{BlobStore, Persistence};
pub(super) mod encoding;
use encoding::{Manifest, Reference, Selected};

pub const MAX_COVERAGE_BYTES: usize = 64 * 1024;
const MAX_REFERENCES: usize = 128;
const MAX_SELECTED: usize = 128;
/// Callers provide persisted input positions and the current owner cut.
/// Verification reloads every referenced record from this input owner.
pub struct ClosureRequest {
    pub income_leaves: Vec<u64>,
    pub snapshots: [u64; 3],
    pub cut: ActivationCut,
}
/// A verified immutable object, published before tag14. Revalidation still occurs
/// at commit, so retaining this value cannot authorize a later account prefix.
pub struct VerifiedAccountInputsClosure {
    binding: AccountInputsReconciled,
}
impl VerifiedAccountInputsClosure {
    pub fn binding(&self) -> &AccountInputsReconciled {
        &self.binding
    }
}
pub(crate) trait ClosureStore {
    fn load(&self, digest: v::Hash) -> Result<Vec<u8>>;
    fn publish(&self, bytes: &[u8]) -> Result<v::Hash>;
}
struct Objects<S: BlobStore>(RefCell<Persistence<S>>);
impl<S: BlobStore> ClosureStore for Objects<S> {
    fn load(&self, digest: v::Hash) -> Result<Vec<u8>> {
        let mut store = self.0.try_borrow_mut().map_err(|_| Error::Busy)?;
        let bytes = store
            .read_object(digest, MAX_COVERAGE_BYTES)
            .map_err(|_| Error::Identity)?;
        // A readable leftover from a failed directory sync is not a completed
        // publication. Re-sync existing bytes only after their hash was checked.
        if store.put_object(&bytes).map_err(|_| Error::Stopped)? != digest {
            return Err(Error::Identity);
        }
        Ok(bytes)
    }
    fn publish(&self, bytes: &[u8]) -> Result<v::Hash> {
        if bytes.is_empty() || bytes.len() > MAX_COVERAGE_BYTES {
            return Err(Error::Capacity);
        }
        self.0
            .try_borrow_mut()
            .map_err(|_| Error::Busy)?
            .put_object(bytes)
            .map_err(|_| Error::Stopped)
    }
}
impl<S: Store> AccountIngress<S> {
    pub fn with_closure_store(mut self, store: impl BlobStore + 'static) -> Self {
        self.closure_store = Some(Box::new(Objects(RefCell::new(Persistence::new(store)))));
        self
    }
    /// Export the existing object after strict decoding and exact binding checks.
    pub fn account_inputs_coverage_object(
        &self,
        binding: &AccountInputsReconciled,
    ) -> Result<Vec<u8>> {
        let bytes = self
            .closure_store
            .as_ref()
            .ok_or(Error::Identity)?
            .load(binding.coverage_digest)?;
        let m = Manifest::decode(&bytes)?;
        if m.binding(binding.coverage_digest) != *binding
            || m.account_stream != self.config.account_stream
        {
            return Err(Error::Identity);
        }
        Ok(bytes)
    }
    pub(crate) fn verify_account_inputs_closure(
        &self,
        baseline: &CanonicalBaseline,
        account: &Account,
        preceding: &HistoryFrontier,
        binding: &AccountInputsReconciled,
        writer: u64,
    ) -> Result<()> {
        if baseline.digest() != self.config.baseline.digest() {
            return Err(Error::Identity);
        }
        let bytes = self.account_inputs_coverage_object(binding)?;
        let manifest = Manifest::decode(&bytes)?;
        self.verify_manifest(account, preceding, &manifest, writer)
    }
    fn prepare_account_inputs_closure(
        &self,
        account: &Account,
        preceding: &HistoryFrontier,
        request: ClosureRequest,
    ) -> Result<VerifiedAccountInputsClosure> {
        let m = self.query_manifest(account, preceding, request, selected(account)?)?;
        self.verify_manifest(account, preceding, &m, m.writer)?;
        let bytes = m.bytes()?;
        let digest = self
            .closure_store
            .as_ref()
            .ok_or(Error::Identity)?
            .publish(&bytes)?;
        if digest != hash(&bytes) {
            return Err(Error::Identity);
        }
        Ok(VerifiedAccountInputsClosure {
            binding: m.binding(digest),
        })
    }
    pub(super) fn query_manifest(
        &self,
        account: &Account,
        preceding: &HistoryFrontier,
        request: ClosureRequest,
        selected: Vec<Selected>,
    ) -> Result<Manifest> {
        let cut = request.cut;
        if self.stopped
            || self.cursor != self.records.len()
            || cut.through_sequence != self.position()
            || cut.writer_epoch != self.config.writer_epoch
            || cut.now.domain() != cut.valid_until.domain()
            || cut.now.nanos() >= cut.valid_until.nanos()
            || request.income_leaves.is_empty()
            || request.income_leaves.len() + 3 > MAX_REFERENCES
        {
            return Err(Error::Identity);
        }
        let mut leaves = Vec::with_capacity(request.income_leaves.len());
        for seq in request.income_leaves {
            let evidence = self.income_evidence(seq)?.ok_or(Error::Identity)?;
            let q = evidence.query();
            leaves.push((
                self.reference(seq)?,
                q.start_time_ms,
                q.end_time_ms,
                q.limit,
            ));
        }
        let start = leaves.first().ok_or(Error::Identity)?.1;
        let end = leaves.last().ok_or(Error::Identity)?.2;
        let original = AccountInputsReconciled {
            scope: self.config.baseline.snapshot().scope.clone(),
            account_epoch: account.account_epoch(),
            ledger_sequence: account.ledger_sequence(),
            baseline_digest: self.config.baseline.digest(),
            history_digest: preceding.digest(),
            input_stream: self.input_stream(),
            through_sequence: cut.through_sequence,
            state_digest: account
                .account_inputs_state_digest(self.config.baseline.digest(), preceding.digest())
                .map_err(|_| Error::Identity)?,
            coverage_digest: [0; 32],
        };
        let m = Manifest {
            original,
            account_stream: self.config.account_stream,
            input_prefix_digest: self.prefix_digest(cut.through_sequence)?,
            writer: cut.writer_epoch,
            private_epoch: cut.private_connection_epoch,
            gap: cut.gap_generation,
            boot: cut.now.domain().boot_id(),
            generation: cut.now.domain().generation(),
            evaluated: cut.now.nanos(),
            expires: cut.valid_until.nanos(),
            start,
            end,
            leaves,
            selected,
            snapshots: [
                self.reference(request.snapshots[0])?,
                self.reference(request.snapshots[1])?,
                self.reference(request.snapshots[2])?,
            ],
        };
        Ok(m)
    }
    pub(super) fn reference(&self, sequence: u64) -> Result<Reference> {
        let index = usize::try_from(sequence)
            .ok()
            .and_then(|s| s.checked_sub(1))
            .ok_or(Error::Identity)?;
        let bytes = codec::encode(self.records.get(index).ok_or(Error::Identity)?)?;
        Ok(Reference {
            sequence,
            length: bytes.len() as u64,
            digest: hash(&bytes),
        })
    }
    pub(super) fn prefix_digest(&self, through: u64) -> Result<v::Hash> {
        let end = usize::try_from(through).map_err(|_| Error::Capacity)?;
        if end == 0 || end > self.records.len() {
            return Err(Error::Identity);
        }
        let mut h = ring::digest::Context::new(&ring::digest::SHA256);
        h.update(b"tickwire.binance.input-closure-prefix.v1");
        h.update(&self.input_stream());
        h.update(&through.to_le_bytes());
        for i in 1..=through {
            let r = self.reference(i)?;
            h.update(&r.sequence.to_le_bytes());
            h.update(&r.length.to_le_bytes());
            h.update(&r.digest)
        }
        Ok(h.finish().as_ref().try_into().unwrap())
    }
    fn verify_manifest(
        &self,
        account: &Account,
        history: &HistoryFrontier,
        m: &Manifest,
        writer: u64,
    ) -> Result<()> {
        self.verify_query_manifest(account, history, m, writer, &selected(account)?)?;
        let mut candidate = account.clone();
        candidate
            .reconcile_account_inputs(&m.binding(hash(&m.bytes()?)))
            .map_err(|_| Error::Identity)?;
        Ok(())
    }
    pub(super) fn verify_query_manifest(
        &self,
        account: &Account,
        history: &HistoryFrontier,
        m: &Manifest,
        writer: u64,
        expected_selected: &[Selected],
    ) -> Result<()> {
        let b = &m.original;
        if b.scope != self.config.baseline.snapshot().scope
            || b.account_epoch != account.account_epoch()
            || b.ledger_sequence != account.ledger_sequence()
            || b.baseline_digest != self.config.baseline.digest()
            || b.history_digest != history.digest()
            || history.baseline_digest() != b.baseline_digest
            || history.stream() != self.config.account_stream
            || b.input_stream != self.input_stream()
            || m.account_stream != self.config.account_stream
            || m.writer != writer
            || writer == 0
            || writer > self.config.writer_epoch
            || m.evaluated >= m.expires
            || m.start > m.end
            || m.leaves.is_empty()
            || m.leaves.len() + 3 > MAX_REFERENCES
            || m.selected != expected_selected
            || b.state_digest
                != account
                    .account_inputs_state_digest(b.baseline_digest, b.history_digest)
                    .map_err(|_| Error::Identity)?
            || m.input_prefix_digest != self.prefix_digest(b.through_sequence)?
        {
            return Err(Error::Identity);
        }
        let domain =
            tickwire_clock::ClockDomain::new(m.boot, m.generation).map_err(|_| Error::Identity)?;
        let end = usize::try_from(b.through_sequence).map_err(|_| Error::Identity)?;
        let begin = self.records[..end]
            .iter()
            .enumerate()
            .rev()
            .find_map(|(i, r)| match r {
                Record::BeginReconciliation { epoch, generation } => Some((i, *epoch, *generation)),
                Record::Gap { .. } => Some((i, 0, 0)),
                _ => None,
            })
            .ok_or(Error::Identity)?;
        if (begin.1, begin.2) != (m.private_epoch, m.gap) {
            return Err(Error::Identity);
        }
        let mut next = m.start;
        let mut last = begin.0 as u64 + 1;
        let mut rows = BTreeMap::new();
        let mut received = 0;
        for (reference, start, finish, limit) in &m.leaves {
            if reference.sequence <= last
                || reference.sequence > b.through_sequence
                || *reference != self.reference(reference.sequence)?
                || *start != next
                || start > finish
            {
                return Err(Error::Identity);
            }
            let e = self
                .income_evidence(reference.sequence)?
                .ok_or(Error::Identity)?;
            let q = e.query();
            if e.saturated()
                || q.purpose != income::IncomeUse::Account
                || q.private_connection_epoch != m.private_epoch
                || q.gap_generation != m.gap
                || q.start_time_ms != *start
                || q.end_time_ms != *finish
                || q.limit != *limit
                || e.domain() != domain
                || e.baseline_digest() != b.baseline_digest
                || e.received_mono_ns() < received
                || e.received_mono_ns() > m.evaluated
                || m.evaluated - e.received_mono_ns() > m.expires - m.evaluated
            {
                return Err(Error::Identity);
            }
            for row in e.rows() {
                let r = selected_row(row)?;
                let key = (r.kind.clone(), r.id.clone());
                if let Some(old) = rows.insert(key, r.clone())
                    && old != r
                {
                    return Err(Error::Conflict);
                }
            }
            received = e.received_mono_ns();
            last = reference.sequence;
            next = finish.checked_add(1).ok_or(Error::Identity)?;
        }
        if m.end.checked_add(1) != Some(next)
            || m.selected
                .iter()
                .any(|r| rows.get(&(r.kind.clone(), r.id.clone())) != Some(r))
        {
            return Err(Error::Identity);
        }
        let evidence = activation::evidence_at(self, b.through_sequence)?;
        for (r, e) in m.snapshots.iter().zip(&evidence) {
            if r.sequence <= last
                || r.sequence != e.input().sequence
                || *r != self.reference(r.sequence)?
                || e.received_mono_ns() < received
            {
                return Err(Error::Identity);
            }
            received = e.received_mono_ns();
            last = r.sequence;
        }
        let binding = Reactivation {
            scope: b.scope.clone(),
            product: product(&self.config)?,
            account_epoch: b.account_epoch,
            ledger_sequence: b.ledger_sequence,
            baseline_digest: b.baseline_digest,
            history_digest: b.history_digest,
            input_stream: b.input_stream,
            private_connection_epoch: m.private_epoch,
            gap_generation: m.gap,
            through_sequence: b.through_sequence,
            query_coverage_digest: activation::digest(&evidence),
            clock_boot_id: m.boot,
            clock_generation: m.generation,
            evaluated_at: m.evaluated,
            valid_until: m.expires,
            writer_epoch: m.writer,
            execution_connection_epoch: 1,
            venue_available: snapshot::venue_available(&self.config.baseline, evidence[0].body())?,
        };
        activation::verify(self, &self.config.baseline, account, history, &binding)?;
        Ok(())
    }
}
impl<S: Store, B: Backend, A: Authority, E: Encoder> RestOwner<S, B, A, E> {
    pub fn reconcile_account_inputs(&mut self, request: ClosureRequest) -> Result<Applied> {
        if self.busy() {
            return Err(Error::Busy);
        }
        self.ingress.ready(&self.gateway)?;
        let proof = self.ingress.prepare_account_inputs_closure(
            self.account(),
            self.gateway.history_frontier(),
            request,
        )?;
        self.gateway
            .reconcile_account_inputs(proof.binding, &mut self.ingress.activation_verifier())
            .map_err(Error::Gateway)
    }
}
fn selected(account: &Account) -> Result<Vec<Selected>> {
    account
        .account_inputs_to_reconcile()
        .map_err(|_| Error::Identity)?
        .iter()
        .map(selected_row)
        .collect()
}
pub(super) fn selected_row(input: &v::CanonicalAccountInput) -> Result<Selected> {
    let v::AccountInputBody::Income(r) = &input.input().body else {
        return Err(Error::Identity);
    };
    Ok(Selected {
        kind: r.income_type.as_str().to_owned(),
        id: r.transaction_id.as_str().to_owned(),
        length: input.bytes().len() as u64,
        digest: input.digest(),
    })
}
fn hash(bytes: &[u8]) -> v::Hash {
    ring::digest::digest(&ring::digest::SHA256, bytes)
        .as_ref()
        .try_into()
        .unwrap()
}
#[cfg(test)]
mod tests;
