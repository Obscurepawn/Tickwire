use super::*;
use serde_json::{Value, json};
use tickwire_account::{Account, Scope};
use tickwire_account_codec::CanonicalBaseline;
use tickwire_account_gateway::{ActivationReceipt, AuthorityError, ReactivationVerifier};
use tickwire_account_journal::HistoryFrontier;
use tickwire_recovery::{MAX_OBJECT_BYTES, StoreKey, digest};
use tickwire_runtime_policy::{ControlPlane, ReconciliationRequest, ReconciliationVerifier};
use tickwire_spot_execution::recovery::replay_evidence;

use tickwire_spot_execution::private::stream::StreamReader;

pub struct CashVerifier<S> {
    reader: Option<StreamReader>,
    previous: Option<(Reactivation, u64)>,
    store: S,
    uid: String,
    start: u64,
}
impl<S: BlobStore> CashVerifier<S> {
    pub fn new(store: S, expected_uid: &str, coverage_start_ms: u64) -> Result<Self> {
        if coverage_start_ms == 0
            || expected_uid.is_empty()
            || expected_uid.len() > 32
            || !expected_uid.bytes().all(|b| b.is_ascii_digit())
        {
            return Err(Error::Origin(
                "invalid configured account UID or trusted baseline cut",
            ));
        }
        Ok(Self {
            store,
            uid: expected_uid.to_owned(),
            start: coverage_start_ms,
            reader: None,
            previous: None,
        })
    }
    pub fn bind_private(&mut self, r: StreamReader) {
        self.reader = Some(r);
    }
    fn reader(&self) -> Result<&StreamReader> {
        self.reader
            .as_ref()
            .filter(|r| r.config().uid == self.uid)
            .ok_or(Error::Origin("private journal not bound to UID"))
    }
    pub(super) fn current(
        &self,
        scope: &Scope,
        c: PrivateCut,
        boot: [u8; 16],
    ) -> Result<StreamReader> {
        let r = self.reader()?;
        if &r.config().scope != scope
            || r.config().boot_id != boot
            || r.config().stream_id != c.stream
            || c.gap_generation != c.epoch
            || r.failed()
            || r.status()
                .is_none_or(|s| !s.ready || s.epoch != c.epoch || s.durable_sequence != c.through)
        {
            return Err(Error::Origin(
                "private source changed during reconciliation",
            ));
        }
        Ok(r.clone())
    }
    fn overlap(&self, end: u64) -> u64 {
        self.start.max(end.saturating_sub(60_000))
    }
    pub(super) fn committed(&mut self, b: &Reactivation, end_ms: u64) {
        self.previous = Some((b.clone(), end_ms));
    }
    fn previous_cut(&self) -> Option<([u8; 32], u64, u64)> {
        self.previous
            .as_ref()
            .map(|(b, end)| (b.query_coverage_digest, *end, b.through_sequence))
    }
    fn pair(
        &self,
        first: &RestEvidence,
        second: &RestEvidence,
        scope: &Scope,
        start: u64,
    ) -> Result<()> {
        if first.uid != self.uid
            || second.uid != self.uid
            || first.venue.id() != scope.venue
            || second.venue != first.venue
            || first.environment != scope.environment
            || second.environment != first.environment
            || first.start_ms != start
            || second.start_ms != start
            || second.end_ms <= first.end_ms
            || first.fingerprint != second.fingerprint
        {
            return Err(Error::Origin(
                "venue scans differ in scope, baseline window or economic facts",
            ));
        }
        Ok(())
    }
    pub(super) fn scans(
        &self,
        first: &RestEvidence,
        second: &RestEvidence,
        scope: &Scope,
        start: u64,
    ) -> Result<(RestEvidence, RestEvidence)> {
        let a = replay_evidence(&first.evidence_bytes)
            .map_err(|_| Error::Origin("first REST transcript incomplete"))?;
        let b = replay_evidence(&second.evidence_bytes)
            .map_err(|_| Error::Origin("second REST transcript incomplete"))?;
        self.pair(&a, &b, scope, start)?;
        Ok((a, b))
    }
    fn store(&mut self, raw: &[u8]) -> Result<[u8; 32]> {
        let h = digest(raw);
        self.store
            .put_sync(StoreKey::Object(h), raw)
            .map_err(Error::Storage)?;
        Ok(h)
    }
    fn load(&self, id: [u8; 32], lim: usize) -> Result<Vec<u8>> {
        let bytes = self
            .store
            .get(StoreKey::Object(id), lim)
            .map_err(Error::Storage)?;
        if digest(&bytes) != id {
            return Err(Error::Origin("cash evidence digest mismatch"));
        }
        Ok(bytes)
    }
    fn manifest(&self, b: &Reactivation) -> Result<Value> {
        serde_json::from_slice(&self.load(b.query_coverage_digest, 8192)?)
            .map_err(|_| Error::Origin("invalid cash archive"))
    }
    pub(super) fn coverage_start_at(&self, b: Option<&Reactivation>) -> Result<u64> {
        match b {
            None => Ok(self.start),
            Some(b) => self.manifest(b)?["end_ms"]
                .as_u64()
                .map(|end| self.overlap(end))
                .ok_or(Error::Origin("no verified coverage frontier")),
        }
    }
    pub fn coverage_start<B: Backend, A: Authority, E: tickwire_gateway::Encoder>(
        &self,
        g: &tickwire_account_gateway::AccountGateway<B, A, E>,
    ) -> Result<u64> {
        self.coverage_start_at(g.latest_reactivation())
    }
    pub(super) fn archive(
        &mut self,
        baseline: &CanonicalBaseline,
        account: &Account,
        b: &mut Reactivation,
        one: &RestEvidence,
        two: &RestEvidence,
    ) -> Result<()> {
        let first = self.store(&one.evidence_bytes)?;
        let second = self.store(&two.evidence_bytes)?;
        let private = private_proof::evidence(
            self.reader()?,
            baseline,
            account,
            b,
            self.previous.as_ref().map(|(b, _)| b),
        )?;
        let m = manifest(
            b,
            [first, second],
            [two.start_ms, two.end_ms],
            self.previous_cut(),
            private,
        );
        b.query_coverage_digest = self
            .store(&serde_json::to_vec(&m).map_err(|_| Error::Origin("cash archive encoding"))?)?;
        Ok(())
    }
    fn verify_archive(
        &self,
        baseline: &CanonicalBaseline,
        account: &Account,
        prior: &HistoryFrontier,
        b: &Reactivation,
    ) -> Result<()> {
        if b.scope != baseline.snapshot().scope
            || b.product != Product::Spot
            || b.baseline_digest != baseline.digest()
            || b.history_digest != prior.digest()
            || account.scope() != &b.scope
            || b.account_epoch != account.account_epoch()
            || b.ledger_sequence != account.ledger_sequence()
        {
            return Err(Error::Origin("wrong economic journal prefix"));
        }
        let m = self.manifest(b)?;
        let hash = |k: &str| {
            serde_json::from_value::<[u8; 32]>(m[k].clone())
                .map_err(|_| Error::Origin("invalid REST evidence reference"))
        };
        let first = hash("first")?;
        let second = hash("second")?;
        let a = replay_evidence(&self.load(first, MAX_OBJECT_BYTES)?)
            .map_err(|_| Error::Origin("invalid first REST archive"))?;
        let c = replay_evidence(&self.load(second, MAX_OBJECT_BYTES)?)
            .map_err(|_| Error::Origin("invalid second REST archive"))?;
        self.pair(&a, &c, &b.scope, c.start_ms)?;
        let private = private_proof::evidence(
            self.reader()?,
            baseline,
            account,
            b,
            self.previous.as_ref().map(|(b, _)| b),
        )?;
        if c.start_ms
            != self
                .previous
                .as_ref()
                .map_or(self.start, |p| self.overlap(p.1))
            || self
                .previous
                .as_ref()
                .is_some_and(|(old, end)| c.end_ms <= *end || !follows(old, b))
            || manifest(
                b,
                [first, second],
                [c.start_ms, c.end_ms],
                self.previous_cut(),
                private,
            ) != m
            || compare::account(baseline, account, &c)? != b.venue_available
        {
            return Err(Error::Origin(
                "cash archive or observed economic state differs from journal",
            ));
        }
        Ok(())
    }
}
// A higher writer epoch fences a recovered sender. A clock restart cannot authorize
// reuse of either the previous private connection or the previous execution connection.
fn follows(previous: &Reactivation, next: &Reactivation) -> bool {
    let boot_changed = previous.clock_boot_id != next.clock_boot_id;
    previous.input_stream == next.input_stream
        && previous.through_sequence <= next.through_sequence
        && previous.private_connection_epoch <= next.private_connection_epoch
        && previous.writer_epoch <= next.writer_epoch
        && previous.execution_connection_epoch <= next.execution_connection_epoch
        && (previous.writer_epoch == next.writer_epoch
            || previous.execution_connection_epoch < next.execution_connection_epoch)
        && (previous.private_connection_epoch == next.private_connection_epoch
            || previous.through_sequence < next.through_sequence)
        && if boot_changed {
            previous.writer_epoch < next.writer_epoch
                && previous.private_connection_epoch < next.private_connection_epoch
        } else {
            previous.clock_generation <= next.clock_generation
                && (previous.clock_generation != next.clock_generation
                    || previous.evaluated_at <= next.evaluated_at)
        }
}
fn manifest(
    b: &Reactivation,
    [first, second]: [[u8; 32]; 2],
    [start, end]: [u64; 2],
    previous: Option<([u8; 32], u64, u64)>,
    private: [u8; 32],
) -> Value {
    json!({"format":"tickwire.spot.cash-proof.v1","first":first,"second":second,"previous":previous,"private_digest":private,"start_ms":start,"end_ms":end,
        "baseline":b.baseline_digest,"history":b.history_digest,"ledger":b.ledger_sequence,"account_epoch":b.account_epoch,
        "private_stream":b.input_stream,"epoch":b.private_connection_epoch,"gap":b.gap_generation,"through":b.through_sequence,
        "boot":b.clock_boot_id,"clock":b.clock_generation,"evaluated":b.evaluated_at,"until":b.valid_until,"writer":b.writer_epoch,"execution":b.execution_connection_epoch})
}
// Verification precedes WAL sync on the live path. Advancing the chain here would
// let a failed append become the predecessor of a future proof.
impl<S: BlobStore> ReactivationVerifier for &CashVerifier<S> {
    fn verify(
        &mut self,
        baseline: &CanonicalBaseline,
        account: &Account,
        prior: &HistoryFrontier,
        b: &Reactivation,
    ) -> std::result::Result<(), AuthorityError> {
        self.verify_archive(baseline, account, prior, b)
            .map_err(|_| AuthorityError::Denied)
    }
}
impl<S: BlobStore> ReactivationVerifier for CashVerifier<S> {
    fn verify(
        &mut self,
        baseline: &CanonicalBaseline,
        account: &Account,
        prior: &HistoryFrontier,
        b: &Reactivation,
    ) -> std::result::Result<(), AuthorityError> {
        self.verify_archive(baseline, account, prior, b)
            .map_err(|_| AuthorityError::Denied)?;
        let end = self.manifest(b).map_err(|_| AuthorityError::Denied)?["end_ms"]
            .as_u64()
            .ok_or(AuthorityError::Denied)?;
        self.committed(b, end);
        Ok(())
    }
}
pub struct CashProof {
    reader: StreamReader,
    request: ReconciliationRequest,
}
impl CashProof {
    pub(super) fn committed(
        b: &Reactivation,
        r: ActivationReceipt,
        until: MonoTime,
        reader: StreamReader,
    ) -> Self {
        Self {
            reader,
            request: ReconciliationRequest {
                scope: b.scope.clone(),
                product: Product::Spot,
                writer_epoch: b.writer_epoch,
                input_stream: b.input_stream,
                connection_epoch: b.private_connection_epoch,
                gap_generation: b.gap_generation,
                through_sequence: b.through_sequence,
                account_epoch: b.account_epoch,
                ledger_sequence: r.ledger_sequence,
                baseline_digest: b.baseline_digest,
                history_digest: r.history_digest,
                query_coverage_digest: b.query_coverage_digest,
                valid_until: until,
            },
        }
    }
    pub fn request(&self) -> &ReconciliationRequest {
        &self.request
    }
    fn current(&self) -> bool {
        let until = self.request.valid_until;
        let Ok(mut clock) = tickwire_binance_clock::SystemClock::new(until.domain()) else {
            return false;
        };
        let Ok(now) = clock.sample() else {
            return false;
        };
        now.monotonic.nanos() < until.nanos()
            && !self.reader.failed()
            && self.reader.status().is_some_and(|s| {
                s.ready
                    && s.epoch == self.request.connection_epoch
                    && s.durable_sequence == self.request.through_sequence
            })
    }
    pub fn install(mut self, plane: &ControlPlane) -> tickwire_runtime_policy::Result<()> {
        if !self.current() {
            return Err(tickwire_runtime_policy::Error::Unreconciled);
        }
        plane.input_applied(self.request.input_stream, self.request.through_sequence)?;
        plane.complete_reconciliation(self.request.clone(), &mut self)?;
        if !self.current() {
            plane.private_uncertain(self.request.connection_epoch)?;
            return Err(tickwire_runtime_policy::Error::Unreconciled);
        }
        Ok(())
    }
}
impl ReconciliationVerifier for CashProof {
    fn verify(&mut self, r: &ReconciliationRequest) -> tickwire_runtime_policy::Result<()> {
        if r != &self.request || !self.current() {
            Err(tickwire_runtime_policy::Error::Identity)
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn a_private_cut_and_its_writer_cannot_travel_backwards_on_replay() {
        let b = Reactivation {
            scope: Scope {
                risk_domain_id: [1; 16],
                environment: tickwire_venue::Environment::Test,
                venue: 2,
                account: 2,
            },
            product: Product::Spot,
            account_epoch: 1,
            ledger_sequence: 1,
            baseline_digest: [1; 32],
            history_digest: [2; 32],
            input_stream: [3; 16],
            private_connection_epoch: 3,
            gap_generation: 3,
            through_sequence: 100,
            query_coverage_digest: [4; 32],
            clock_boot_id: [5; 16],
            clock_generation: 2,
            evaluated_at: 100,
            valid_until: 110,
            writer_epoch: 2,
            execution_connection_epoch: 3,
            venue_available: vec![],
        };
        assert!(follows(&b, &b));
        for invalid in [
            Reactivation {
                clock_generation: 1,
                ..b.clone()
            },
            Reactivation {
                evaluated_at: 99,
                ..b.clone()
            },
            Reactivation {
                writer_epoch: 1,
                ..b.clone()
            },
            Reactivation {
                execution_connection_epoch: 2,
                ..b.clone()
            },
            Reactivation {
                through_sequence: 99,
                ..b.clone()
            },
            Reactivation {
                private_connection_epoch: 2,
                ..b.clone()
            },
            Reactivation {
                input_stream: [6; 16],
                ..b.clone()
            },
            Reactivation {
                writer_epoch: 3,
                ..b.clone()
            },
            Reactivation {
                clock_boot_id: [7; 16],
                ..b.clone()
            },
            Reactivation {
                private_connection_epoch: 4,
                ..b.clone()
            },
        ] {
            assert!(!follows(&b, &invalid));
        }
        let next = Reactivation {
            clock_boot_id: [7; 16],
            clock_generation: 1,
            evaluated_at: 4,
            valid_until: 14,
            writer_epoch: 3,
            execution_connection_epoch: 4,
            private_connection_epoch: 4,
            gap_generation: 4,
            through_sequence: 105,
            ..b.clone()
        };
        assert!(follows(&b, &next));
    }
}
