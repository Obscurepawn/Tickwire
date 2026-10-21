//! A never-created Algo child is closed only by an exact parent query plus a
//! complete, fresh account/open-order cut. The account transition stays frozen.
use super::*;
use activation::ActivationCut;
use income_closure::encoding::{Reader, Reference, Writer};
use tickwire_account::{Account, Reactivation, UncreatedAlgoClosed};
use tickwire_account_journal::HistoryFrontier;

const DOMAIN: &[u8] = b"tickwire.binance.uncreated-algo-closure.v1";
const MAX_BYTES: usize = 4096;
pub struct ClosureRequest {
    pub parent_query: u64,
    pub snapshots: [u64; 3],
    pub cut: ActivationCut,
}
pub struct VerifiedUncreatedAlgo {
    binding: UncreatedAlgoClosed,
}
impl VerifiedUncreatedAlgo {
    pub fn binding(&self) -> &UncreatedAlgoClosed {
        &self.binding
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
struct Manifest {
    original: UncreatedAlgoClosed,
    account_stream: [u8; 16],
    prefix: v::Hash,
    writer: u64,
    private_epoch: u64,
    gap: u64,
    boot: [u8; 16],
    generation: u64,
    evaluated: u64,
    expires: u64,
    query: Reference,
    snapshots: [Reference; 3],
}
impl Manifest {
    fn binding(&self, digest: v::Hash) -> UncreatedAlgoClosed {
        let mut b = self.original.clone();
        b.coverage_digest = digest;
        b
    }
    fn bytes(&self) -> Result<Vec<u8>> {
        if self.original.coverage_digest != [0; 32] {
            return Err(Error::Encoding);
        }
        let b = &self.original;
        let mut w = Writer(Vec::new());
        w.array(25);
        w.blob(DOMAIN);
        w.uint(1);
        w.array(4);
        w.blob(&b.scope.risk_domain_id);
        w.uint(b.scope.venue.into());
        w.uint(match b.scope.environment {
            v::Environment::Production => 1,
            v::Environment::Test => 2,
        });
        w.uint(b.scope.account);
        w.uint(b.account_epoch);
        w.uint(b.ledger_sequence);
        w.blob(&b.baseline_digest);
        w.blob(&b.history_digest);
        w.blob(&b.input_stream);
        w.uint(b.through_sequence);
        w.blob(&b.state_digest);
        w.blob(&b.structure_id);
        w.array(2);
        w.uint(b.intent.epoch);
        w.uint(b.intent.intent_id);
        w.blob(&b.command_digest);
        w.blob(b.venue_parent_id.as_str().as_bytes());
        w.blob(&self.account_stream);
        w.blob(&self.prefix);
        w.uint(self.writer);
        w.uint(self.private_epoch);
        w.uint(self.gap);
        w.blob(&self.boot);
        w.uint(self.generation);
        w.uint(self.evaluated);
        w.uint(self.expires);
        w.array(3);
        w.reference(&self.query);
        w.array(3);
        for r in &self.snapshots {
            w.array(3);
            w.reference(r);
        }
        if w.0.len() > MAX_BYTES {
            return Err(Error::Capacity);
        }
        Ok(w.0)
    }
    fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > MAX_BYTES {
            return Err(Error::Capacity);
        }
        let mut r = Reader { bytes, pos: 0 };
        r.array(25)?;
        if r.blob(DOMAIN.len())? != DOMAIN || r.uint()? != 1 {
            return Err(Error::Encoding);
        }
        r.array(4)?;
        let scope = tickwire_account::Scope {
            risk_domain_id: r.fixed()?,
            venue: r.uint()?.try_into().map_err(|_| Error::Encoding)?,
            environment: match r.uint()? {
                1 => v::Environment::Production,
                2 => v::Environment::Test,
                _ => return Err(Error::Encoding),
            },
            account: r.uint()?,
        };
        let account_epoch = r.uint()?;
        let ledger_sequence = r.uint()?;
        let baseline_digest = r.fixed()?;
        let history_digest = r.fixed()?;
        let input_stream = r.fixed()?;
        let through_sequence = r.uint()?;
        let state_digest = r.fixed()?;
        let structure_id = r.fixed()?;
        r.array(2)?;
        let intent = IntentKey {
            epoch: r.uint()?,
            intent_id: r.uint()?,
        };
        let command_digest = r.fixed()?;
        let venue_parent_id = v::VenueOrderId::new(&r.text(64)?).map_err(|_| Error::Encoding)?;
        let original = UncreatedAlgoClosed {
            scope,
            account_epoch,
            ledger_sequence,
            baseline_digest,
            history_digest,
            input_stream,
            through_sequence,
            state_digest,
            coverage_digest: [0; 32],
            structure_id,
            intent,
            command_digest,
            venue_parent_id,
        };
        let account_stream = r.fixed()?;
        let prefix = r.fixed()?;
        let writer = r.uint()?;
        let private_epoch = r.uint()?;
        let gap = r.uint()?;
        let boot = r.fixed()?;
        let generation = r.uint()?;
        let evaluated = r.uint()?;
        let expires = r.uint()?;
        r.array(3)?;
        let query = r.reference()?;
        r.array(3)?;
        let mut next = || {
            r.array(3)?;
            r.reference()
        };
        let snapshots = [next()?, next()?, next()?];
        let m = Self {
            original,
            account_stream,
            prefix,
            writer,
            private_epoch,
            gap,
            boot,
            generation,
            evaluated,
            expires,
            query,
            snapshots,
        };
        if r.pos != bytes.len() || m.bytes()? != bytes {
            return Err(Error::Encoding);
        }
        Ok(m)
    }
}
/// Canonical dependency description only; it grants no account or send authority.
pub struct CoverageDependencies {
    pub input_stream: [u8; 16],
    pub through_sequence: u64,
    pub prefix_digest: v::Hash,
    pub references: Vec<(u64, u64, v::Hash)>,
}
pub fn coverage_dependencies(
    b: &UncreatedAlgoClosed,
    bytes: &[u8],
    writer: u64,
    account_stream: [u8; 16],
) -> Result<CoverageDependencies> {
    let m = Manifest::decode(bytes)?;
    if m.binding(hash(bytes)) != *b
        || m.writer != writer
        || m.account_stream != account_stream
        || writer == 0
        || m.evaluated >= m.expires
        || m.private_epoch == 0
        || m.gap == 0
    {
        return Err(Error::Identity);
    }
    let mut refs = vec![(m.query.sequence, m.query.length, m.query.digest)];
    refs.extend(m.snapshots.iter().map(|r| (r.sequence, r.length, r.digest)));
    if refs.iter().any(|r| r.0 == 0 || r.0 > b.through_sequence)
        || refs.windows(2).any(|w| w[0].0 >= w[1].0)
    {
        return Err(Error::Identity);
    }
    Ok(CoverageDependencies {
        input_stream: b.input_stream,
        through_sequence: b.through_sequence,
        prefix_digest: m.prefix,
        references: refs,
    })
}
impl<S: Store> AccountIngress<S> {
    pub fn uncreated_algo_coverage_object(&self, b: &UncreatedAlgoClosed) -> Result<Vec<u8>> {
        let bytes = self
            .closure_store
            .as_ref()
            .ok_or(Error::Identity)?
            .load(b.coverage_digest)?;
        let m = Manifest::decode(&bytes)?;
        if m.binding(hash(&bytes)) != *b || m.account_stream != self.config.account_stream {
            return Err(Error::Identity);
        }
        Ok(bytes)
    }
    pub(crate) fn verify_uncreated_algo(
        &self,
        baseline: &CanonicalBaseline,
        account: &Account,
        history: &HistoryFrontier,
        b: &UncreatedAlgoClosed,
        writer: u64,
    ) -> Result<()> {
        if baseline.digest() != self.config.baseline.digest() {
            return Err(Error::Identity);
        }
        self.verify_structure_manifest(
            account,
            history,
            &Manifest::decode(&self.uncreated_algo_coverage_object(b)?)?,
            writer,
        )
    }
    fn original_binding(
        &self,
        account: &Account,
        history: &HistoryFrontier,
        seq: u64,
        through: u64,
    ) -> Result<UncreatedAlgoClosed> {
        let (record, parent) = self.parent_query(seq)?;
        let query = record.structure_query.ok_or(Error::Identity)?;
        Ok(UncreatedAlgoClosed {
            scope: self.config.baseline.snapshot().scope.clone(),
            account_epoch: account.account_epoch(),
            ledger_sequence: account.ledger_sequence(),
            baseline_digest: self.config.baseline.digest(),
            history_digest: history.digest(),
            input_stream: self.input_stream(),
            through_sequence: through,
            state_digest: account
                .account_inputs_state_digest(self.config.baseline.digest(), history.digest())
                .map_err(|_| Error::Identity)?,
            coverage_digest: [0; 32],
            structure_id: query.structure_id,
            intent: query.intent,
            command_digest: record.command.digest(),
            venue_parent_id: parent
                .parent
                .venue_parent_id
                .clone()
                .ok_or(Error::Identity)?,
        })
    }
    fn eligible_queries(
        &self,
        account: &Account,
        history: &HistoryFrontier,
        epoch: u64,
        gap: u64,
        through: u64,
    ) -> Vec<(IntentKey, u64)> {
        let mut seen = std::collections::BTreeSet::new();
        let mut out = Vec::new();
        for (index, r) in self.records.iter().enumerate().take(through as usize).rev() {
            let Record::Rest(r) = r else { continue };
            let Some(q) = r.structure_query else { continue };
            if !seen.insert(q.structure_id)
                || q.private_connection_epoch != epoch
                || q.gap_generation != gap
                || account.uncreated_algo_closure(&q.structure_id).is_some()
            {
                continue;
            }
            let Ok(mut b) = self.original_binding(account, history, index as u64 + 1, through)
            else {
                continue;
            };
            b.coverage_digest = [1; 32];
            if account.clone().close_uncreated_algo(&b).is_ok() {
                out.push((q.intent, index as u64 + 1));
            }
        }
        out.reverse();
        out
    }
    fn prepare_uncreated_algo(
        &self,
        account: &Account,
        history: &HistoryFrontier,
        request: ClosureRequest,
    ) -> Result<VerifiedUncreatedAlgo> {
        let cut = request.cut;
        if self.stopped
            || self.cursor != self.records.len()
            || cut.through_sequence != self.position()
            || cut.writer_epoch != self.config.writer_epoch
            || cut.now.domain() != cut.valid_until.domain()
        {
            return Err(Error::Identity);
        }
        let b =
            self.original_binding(account, history, request.parent_query, cut.through_sequence)?;
        let m = Manifest {
            original: b,
            account_stream: self.config.account_stream,
            prefix: self.prefix_digest(cut.through_sequence)?,
            writer: cut.writer_epoch,
            private_epoch: cut.private_connection_epoch,
            gap: cut.gap_generation,
            boot: cut.now.domain().boot_id(),
            generation: cut.now.domain().generation(),
            evaluated: cut.now.nanos(),
            expires: cut.valid_until.nanos(),
            query: self.reference(request.parent_query)?,
            snapshots: [
                self.reference(request.snapshots[0])?,
                self.reference(request.snapshots[1])?,
                self.reference(request.snapshots[2])?,
            ],
        };
        self.verify_structure_manifest(account, history, &m, m.writer)?;
        let bytes = m.bytes()?;
        let digest = self
            .closure_store
            .as_ref()
            .ok_or(Error::Identity)?
            .publish(&bytes)?;
        if digest != hash(&bytes) {
            return Err(Error::Identity);
        }
        Ok(VerifiedUncreatedAlgo {
            binding: m.binding(digest),
        })
    }
    fn parent_query(&self, seq: u64) -> Result<(&RestRecord, &v::StructureObservation)> {
        let index = usize::try_from(seq)
            .ok()
            .and_then(|n| n.checked_sub(1))
            .ok_or(Error::Identity)?;
        let Record::Rest(r) = self.records.get(index).ok_or(Error::Identity)? else {
            return Err(Error::Identity);
        };
        if r.structure_query.is_none()
            || r.status != 200
            || r.events.len() != 1
            || r.disposition != RestDisposition::Observed
        {
            return Err(Error::Identity);
        }
        self.validate_rest(r)?;
        let v::VenueEventBody::Structure(parent) = &r.events[0].event().body else {
            return Err(Error::Identity);
        };
        if parent.parent.kind != v::StructureKind::Algo
            || !matches!(
                parent.status,
                v::StructureStatus::Terminal | v::StructureStatus::Rejected
            )
            || !parent.children.as_slice().is_empty()
            || !matches!(
                parent.venue_status.as_str(),
                "CANCELED" | "EXPIRED" | "REJECTED"
            )
        {
            return Err(Error::Identity);
        }
        let mut decoder =
            ResponseDecoder::new(self.config.max_raw_bytes, 256).map_err(Error::Transport)?;
        decoder.push(&r.raw).map_err(Error::Transport)?;
        decoder.finish();
        let response = decoder
            .next_response()
            .map_err(Error::Transport)?
            .ok_or(Error::Encoding)?;
        let witness: NeverCreated =
            serde_json::from_slice(&response.body).map_err(|_| Error::Encoding)?;
        if !witness.actual_order_id.is_empty()
            || witness.actual_type.is_some_and(|s| !s.is_empty())
            || witness.actual_qty.is_some_and(|n| n.mantissa() != 0)
            || witness.actual_price.mantissa() != 0
            || witness.trigger_time != 0
        {
            return Err(Error::Identity);
        }
        Ok((r, parent))
    }
    fn verify_structure_manifest(
        &self,
        account: &Account,
        history: &HistoryFrontier,
        m: &Manifest,
        writer: u64,
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
            || m.prefix != self.prefix_digest(b.through_sequence)?
            || b.state_digest
                != account
                    .account_inputs_state_digest(b.baseline_digest, b.history_digest)
                    .map_err(|_| Error::Identity)?
            || m.query.sequence > b.through_sequence
            || self.reference(m.query.sequence)? != m.query
        {
            return Err(Error::Identity);
        }
        let (r, parent) = self.parent_query(m.query.sequence)?;
        let q = r.structure_query.ok_or(Error::Identity)?;
        let domain =
            tickwire_clock::ClockDomain::new(m.boot, m.generation).map_err(|_| Error::Identity)?;
        if q.structure_id != b.structure_id
            || q.intent != b.intent
            || q.private_connection_epoch != m.private_epoch
            || q.gap_generation != m.gap
            || r.command.digest() != b.command_digest
            || parent.parent.venue_parent_id.as_ref() != Some(&b.venue_parent_id)
            || r.structure_domain != Some(domain)
            || r.received > m.evaluated
            || m.evaluated - r.received > m.expires - m.evaluated
        {
            return Err(Error::Identity);
        }
        let begin = self.records[..m.query.sequence as usize]
            .iter()
            .rev()
            .find_map(|r| match r {
                Record::BeginReconciliation { epoch, generation } => Some((*epoch, *generation)),
                Record::Gap { .. } => Some((0, 0)),
                _ => None,
            });
        if begin != Some((m.private_epoch, m.gap)) {
            return Err(Error::Identity);
        }
        let evidence = activation::evidence_at(self, b.through_sequence)?;
        let mut last = m.query.sequence;
        let mut received = r.received;
        for (reference, e) in m.snapshots.iter().zip(&evidence) {
            if reference.sequence <= last
                || reference.sequence != e.input().sequence
                || *reference != self.reference(reference.sequence)?
                || e.received_mono_ns() < received
            {
                return Err(Error::Identity);
            }
            last = reference.sequence;
            received = e.received_mono_ns();
        }
        // A repeated query after a contradictory local fact cannot erase it:
        // the reducer checks all retained trigger/child/trade/unknown witnesses.
        let mut candidate = account.clone();
        candidate
            .close_uncreated_algo(&m.binding(hash(&m.bytes()?)))
            .map_err(|_| Error::Identity)?;
        // Other never-created parents remain reserved in the real account.
        // For remote order completeness only, validate their own last parent
        // queries from the same immutable prefix and project their absent child.
        for (_, seq) in
            self.eligible_queries(account, history, m.private_epoch, m.gap, b.through_sequence)
        {
            if seq == m.query.sequence {
                continue;
            }
            let (other, _) = self.parent_query(seq)?;
            if seq >= m.snapshots[0].sequence
                || other.structure_domain != Some(domain)
                || other.received > m.evaluated
                || m.evaluated - other.received > m.expires - m.evaluated
            {
                return Err(Error::Identity);
            }
            let mut projected =
                self.original_binding(&candidate, history, seq, b.through_sequence)?;
            projected.coverage_digest = hash(&m.bytes()?);
            candidate
                .close_uncreated_algo(&projected)
                .map_err(|_| Error::Identity)?;
        }
        let activation = Reactivation {
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
        activation::verify_with_projection(
            self,
            &self.config.baseline,
            account,
            &candidate,
            history,
            &activation,
        )?;
        Ok(())
    }
}
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct NeverCreated<'a> {
    actual_order_id: &'a str,
    actual_type: Option<&'a str>,
    actual_qty: Option<tickwire_binance::decimal::Decimal>,
    actual_price: tickwire_binance::decimal::Decimal,
    trigger_time: i64,
}
impl<S: Store, B: Backend, A: Authority, E: Encoder> RestOwner<S, B, A, E> {
    /// A freshly queried, still-waiting Algo has no ordinary child endpoint yet.
    /// Its potential liability is still checked against the open-Algo snapshot.
    pub fn waiting_algo_intents(&self, epoch: u64, gap: u64) -> Vec<IntentKey> {
        let mut seen = std::collections::BTreeSet::new();
        let mut result = Vec::new();
        for record in self.ingress.records.iter().rev() {
            let Record::Rest(r) = record else { continue };
            let Some(q) = r.structure_query else { continue };
            if !seen.insert(q.structure_id)
                || q.private_connection_epoch != epoch
                || q.gap_generation != gap
                || r.disposition != RestDisposition::Observed
                || r.events.len() != 1
            {
                continue;
            }
            let v::VenueEventBody::Structure(p) = &r.events[0].event().body else {
                continue;
            };
            let Some(s) = self.account().structure(&q.structure_id) else {
                continue;
            };
            if s.kind == v::StructureKind::Algo
                && !s.terminal
                && !s.triggered
                && s.children.is_empty()
                && matches!(
                    p.status,
                    v::StructureStatus::Live | v::StructureStatus::Pending
                )
                && p.children.as_slice().is_empty()
            {
                result.push(q.intent);
            }
        }
        result
    }
    /// Scheduling hint only. The final closure revalidates the full query cut.
    pub fn uncreated_algo_queries(&self, epoch: u64, gap: u64) -> Vec<(IntentKey, u64)> {
        self.ingress.eligible_queries(
            self.account(),
            self.gateway.history_frontier(),
            epoch,
            gap,
            self.ingress.position(),
        )
    }
    pub fn close_uncreated_algo(&mut self, request: ClosureRequest) -> Result<Applied> {
        if self.busy() {
            return Err(Error::Busy);
        }
        self.ingress.ready(&self.gateway)?;
        let proof = self.ingress.prepare_uncreated_algo(
            self.account(),
            self.gateway.history_frontier(),
            request,
        )?;
        self.gateway
            .close_uncreated_algo(proof.binding, &mut self.ingress.activation_verifier())
            .map_err(Error::Gateway)
    }
}
fn hash(bytes: &[u8]) -> v::Hash {
    ring::digest::digest(&ring::digest::SHA256, bytes)
        .as_ref()
        .try_into()
        .unwrap()
}

#[cfg(test)]
mod tests;
