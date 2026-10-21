//! Explicit risk-admin approval of a baseline's income coverage. Query results
//! alone cannot construct this authority, and this module has no signing API.
use super::*;
use tickwire_account::{Account, IncomeBaseline};
use tickwire_account_journal::HistoryFrontier;
use tickwire_clock::{ClockDomain, MonoTime};

pub const MAX_MANIFEST_BYTES: usize = 54 * 1024;
const MAX_REFS: usize = 128;
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct IncomeBaselineAuthority {
    pub controller_id: [u8; 16],
    /// Dedicated risk-admin baseline-import key; never an ordinary grant key.
    pub public_key: [u8; 32],
}
impl IncomeBaselineAuthority {
    pub fn validate(&self) -> Result<()> {
        if self.controller_id == [0; 16] || self.public_key == [0; 32] {
            Err(Error::Identity)
        } else {
            Ok(())
        }
    }
    pub fn verify(
        &self,
        proposal: &IncomeImportProposal,
        approval: &SignedIncomeApproval,
        now: MonoTime,
        writer_epoch: u64,
    ) -> Result<VerifiedIncomeImport> {
        self.validate()?;
        let claims = ApprovalClaims::decode(&approval.payload)?;
        if claims.controller != self.controller_id
            || claims.manifest != proposal.digest()
            || claims.baseline != proposal.manifest.baseline
            || claims.account_stream != proposal.manifest.account_stream
            || claims.input_stream != proposal.manifest.input_stream
            || claims.account_epoch != proposal.manifest.account_epoch
            || claims.ledger != proposal.manifest.ledger
            || claims.input_prefix != proposal.manifest.input_prefix
            || claims.writer != writer_epoch
            || claims.domain()? != now.domain()
            || now.nanos() < claims.issued
            || now.nanos() >= claims.expires
        {
            return Err(Error::Identity);
        }
        ring::signature::UnparsedPublicKey::new(&ring::signature::ED25519, self.public_key)
            .verify(&approval.payload, &approval.signature)
            .map_err(|_| Error::Identity)?;
        Ok(VerifiedIncomeImport {
            proposal: proposal.clone(),
            approval: approval.clone(),
            authority: self.clone(),
            accepted: now.nanos(),
            writer: writer_epoch,
        })
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SignedIncomeApproval {
    payload: Vec<u8>,
    signature: [u8; 64],
}
impl SignedIncomeApproval {
    pub fn new(payload: &[u8], signature: &[u8]) -> Result<Self> {
        ApprovalClaims::decode(payload)?;
        Ok(Self {
            payload: payload.to_vec(),
            signature: signature.try_into().map_err(|_| Error::Encoding)?,
        })
    }
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
    pub fn signature(&self) -> &[u8; 64] {
        &self.signature
    }
    pub fn request_id(&self) -> Result<[u8; 16]> {
        Ok(ApprovalClaims::decode(&self.payload)?.request)
    }
}
#[derive(Clone)]
struct Manifest {
    baseline: v::Hash,
    account_stream: [u8; 16],
    input_stream: [u8; 16],
    account_epoch: u64,
    ledger: u64,
    wallet: u64,
    input_prefix: u64,
    private_epoch: u64,
    gap: u64,
    through_time_ms: u64,
    covered: Vec<v::CanonicalAccountInput>,
    references: Vec<(u64, v::Hash)>,
}
#[derive(Clone)]
pub struct IncomeImportProposal {
    manifest: Manifest,
    bytes: Vec<u8>,
}
impl IncomeImportProposal {
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub fn digest(&self) -> v::Hash {
        hash(&self.bytes)
    }
    pub fn covered(&self) -> &[v::CanonicalAccountInput] {
        &self.manifest.covered
    }
    pub fn input_prefix(&self) -> u64 {
        self.manifest.input_prefix
    }
    /// Build the exact unsigned request for a separate risk-admin signer.
    /// Possessing these bytes provides no approval.
    pub fn approval_payload(
        &self,
        controller_id: [u8; 16],
        request_id: [u8; 16],
        writer_epoch: u64,
        issued: MonoTime,
        expires: MonoTime,
    ) -> Result<Vec<u8>> {
        if issued.domain() != expires.domain() {
            return Err(Error::Identity);
        }
        let claims = ApprovalClaims {
            controller: controller_id,
            request: request_id,
            manifest: self.digest(),
            baseline: self.manifest.baseline,
            account_stream: self.manifest.account_stream,
            input_stream: self.manifest.input_stream,
            account_epoch: self.manifest.account_epoch,
            ledger: self.manifest.ledger,
            input_prefix: self.manifest.input_prefix,
            writer: writer_epoch,
            boot: issued.domain().boot_id(),
            generation: issued.domain().generation(),
            issued: issued.nanos(),
            expires: expires.nanos(),
        };
        let bytes = claims.bytes();
        ApprovalClaims::decode(&bytes)?;
        Ok(bytes)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > MAX_MANIFEST_BYTES {
            return Err(Error::Capacity);
        }
        let mut r = Reader { bytes, position: 0 };
        if r.take(8)? != b"TWIMNF01" {
            return Err(Error::Encoding);
        }
        let baseline = r.array()?;
        let account_stream = r.array()?;
        let input_stream = r.array()?;
        let account_epoch = r.u64()?;
        let ledger = r.u64()?;
        let wallet = r.u64()?;
        let input_prefix = r.u64()?;
        let private_epoch = r.u64()?;
        let gap = r.u64()?;
        let through_time_ms = r.u64()?;
        let count = r.u16()? as usize;
        if count > tickwire_account::MAX_BASELINE_INCOMES {
            return Err(Error::Capacity);
        }
        let mut covered = Vec::with_capacity(count);
        for _ in 0..count {
            covered.push(v::CanonicalAccountInput::decode(r.blob()?).map_err(|_| Error::Encoding)?)
        }
        let count = r.u16()? as usize;
        if count == 0 || count > MAX_REFS {
            return Err(Error::Capacity);
        }
        let mut references = Vec::with_capacity(count);
        for _ in 0..count {
            references.push((r.u64()?, r.array()?));
        }
        r.end()?;
        let manifest = Manifest {
            baseline,
            account_stream,
            input_stream,
            account_epoch,
            ledger,
            wallet,
            input_prefix,
            private_epoch,
            gap,
            through_time_ms,
            covered,
            references,
        };
        let canonical = manifest.bytes()?;
        if canonical != bytes {
            return Err(Error::Encoding);
        }
        Ok(Self {
            manifest,
            bytes: canonical,
        })
    }
}
pub struct VerifiedIncomeImport {
    proposal: IncomeImportProposal,
    approval: SignedIncomeApproval,
    authority: IncomeBaselineAuthority,
    accepted: u64,
    writer: u64,
}
#[derive(Clone)]
pub(crate) struct ImportRecord {
    pub proposal: IncomeImportProposal,
    pub approval: SignedIncomeApproval,
    pub accepted: u64,
    pub writer: u64,
}
pub struct ImportRequest {
    pub wallet_id: u64,
    pub through_time_ms: u64,
    pub private_connection_epoch: u64,
    pub gap_generation: u64,
    pub income_leaves: Vec<u64>,
    pub snapshots: [u64; 3],
}
struct ApprovalClaims {
    controller: [u8; 16],
    request: [u8; 16],
    manifest: v::Hash,
    baseline: v::Hash,
    account_stream: [u8; 16],
    input_stream: [u8; 16],
    account_epoch: u64,
    ledger: u64,
    input_prefix: u64,
    writer: u64,
    boot: [u8; 16],
    generation: u64,
    issued: u64,
    expires: u64,
}
impl ApprovalClaims {
    fn domain(&self) -> Result<ClockDomain> {
        ClockDomain::new(self.boot, self.generation).map_err(|_| Error::Identity)
    }
    fn bytes(&self) -> Vec<u8> {
        let mut b = b"TWIMAP01".to_vec();
        b.extend(self.controller);
        b.extend(self.request);
        b.extend(self.manifest);
        b.extend(self.baseline);
        b.extend(self.account_stream);
        b.extend(self.input_stream);
        for n in [
            self.account_epoch,
            self.ledger,
            self.input_prefix,
            self.writer,
        ] {
            b.extend(n.to_le_bytes());
        }
        b.extend(self.boot);
        for n in [self.generation, self.issued, self.expires] {
            b.extend(n.to_le_bytes());
        }
        b
    }
    fn decode(bytes: &[u8]) -> Result<Self> {
        let mut r = Reader { bytes, position: 0 };
        if r.take(8)? != b"TWIMAP01" {
            return Err(Error::Encoding);
        }
        let s = Self {
            controller: r.array()?,
            request: r.array()?,
            manifest: r.array()?,
            baseline: r.array()?,
            account_stream: r.array()?,
            input_stream: r.array()?,
            account_epoch: r.u64()?,
            ledger: r.u64()?,
            input_prefix: r.u64()?,
            writer: r.u64()?,
            boot: r.array()?,
            generation: r.u64()?,
            issued: r.u64()?,
            expires: r.u64()?,
        };
        r.end()?;
        if s.controller == [0; 16]
            || s.request == [0; 16]
            || s.manifest == [0; 32]
            || s.baseline == [0; 32]
            || s.account_stream == [0; 16]
            || s.input_stream == [0; 16]
            || s.account_epoch == 0
            || s.input_prefix == 0
            || s.writer == 0
            || s.issued >= s.expires
        {
            return Err(Error::Identity);
        }
        s.domain()?;
        Ok(s)
    }
}
impl Manifest {
    fn bytes(&self) -> Result<Vec<u8>> {
        if self.baseline == [0; 32]
            || self.account_stream == [0; 16]
            || self.input_stream == [0; 16]
            || self.account_epoch == 0
            || self.wallet == 0
            || self.input_prefix == 0
            || self.private_epoch == 0
            || self.gap == 0
            || self.through_time_ms == 0
            || self.covered.len() > tickwire_account::MAX_BASELINE_INCOMES
            || self.references.is_empty()
            || self.references.len() > MAX_REFS
        {
            return Err(Error::Identity);
        }
        let mut prior = None;
        for row in &self.covered {
            let v::AccountInputBody::Income(r) = &row.input().body else {
                return Err(Error::Identity);
            };
            let key = (r.income_type.as_str(), r.transaction_id.as_str());
            if prior.is_some_and(|p| p >= key)
                || r.venue_time_ms > self.through_time_ms
                || row.input().identity.wallet_id != self.wallet
            {
                return Err(Error::Identity);
            }
            prior = Some(key);
        }
        if self
            .references
            .iter()
            .any(|(seq, h)| *seq == 0 || *seq > self.input_prefix || *h == [0; 32])
            || self.references.windows(2).any(|w| w[0].0 >= w[1].0)
        {
            return Err(Error::Identity);
        }
        let mut b = b"TWIMNF01".to_vec();
        b.extend(self.baseline);
        b.extend(self.account_stream);
        b.extend(self.input_stream);
        for n in [
            self.account_epoch,
            self.ledger,
            self.wallet,
            self.input_prefix,
            self.private_epoch,
            self.gap,
            self.through_time_ms,
        ] {
            b.extend(n.to_le_bytes());
        }
        b.extend((self.covered.len() as u16).to_le_bytes());
        for row in &self.covered {
            b.extend((row.bytes().len() as u32).to_le_bytes());
            b.extend(row.bytes());
        }
        b.extend((self.references.len() as u16).to_le_bytes());
        for (seq, h) in &self.references {
            b.extend(seq.to_le_bytes());
            b.extend(h);
        }
        if b.len() > MAX_MANIFEST_BYTES {
            return Err(Error::Capacity);
        }
        Ok(b)
    }
}
struct Reader<'a> {
    bytes: &'a [u8],
    position: usize,
}
impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self.position.checked_add(n).ok_or(Error::Encoding)?;
        let b = self.bytes.get(self.position..end).ok_or(Error::Encoding)?;
        self.position = end;
        Ok(b)
    }
    fn array<const N: usize>(&mut self) -> Result<[u8; N]> {
        self.take(N)?.try_into().map_err(|_| Error::Encoding)
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.array()?))
    }
    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.array()?))
    }
    fn blob(&mut self) -> Result<&'a [u8]> {
        let n = u32::from_le_bytes(self.array()?) as usize;
        self.take(n)
    }
    fn end(&self) -> Result<()> {
        if self.position == self.bytes.len() {
            Ok(())
        } else {
            Err(Error::Encoding)
        }
    }
}
fn hash(bytes: &[u8]) -> v::Hash {
    ring::digest::digest(&ring::digest::SHA256, bytes)
        .as_ref()
        .try_into()
        .unwrap()
}

impl<S: Store> AccountIngress<S> {
    pub fn income_baseline_authority(&self) -> Option<&IncomeBaselineAuthority> {
        self.config.income_baseline_authority.as_ref()
    }
    pub fn propose_income_import(&self, request: ImportRequest) -> Result<IncomeImportProposal> {
        if self.config.income_baseline_authority.is_none()
            || self.stopped
            || self.cursor != self.records.len()
        {
            return Err(Error::Stopped);
        }
        self.build_income_proposal(request, self.position())
    }
    fn build_income_proposal(
        &self,
        request: ImportRequest,
        input_prefix: u64,
    ) -> Result<IncomeImportProposal> {
        if request.income_leaves.is_empty()
            || request.income_leaves.len() + 3 > MAX_REFS
            || input_prefix == 0
            || input_prefix > self.position()
            || request.through_time_ms == 0
        {
            return Err(Error::Identity);
        }
        let end = usize::try_from(input_prefix).map_err(|_| Error::Identity)?;
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
        if (begin.1, begin.2) != (request.private_connection_epoch, request.gap_generation) {
            return Err(Error::Identity);
        }
        // No private account/order mutation can be silently crossed by this initial import.
        if self.records[begin.0 + 1..end].iter().any(|r| {
            matches!(
                r,
                Record::Event(..)
                    | Record::Rest(_)
                    | Record::RestEffect { .. }
                    | Record::IncomeEffect { .. }
            )
        }) {
            return Err(Error::Conflict);
        }
        let mut references = Vec::new();
        let mut covered = BTreeMap::new();
        let mut next_start = None;
        let mut domain = None;
        let mut last_sequence = 0;
        for sequence in request.income_leaves {
            if sequence <= begin.0 as u64 + 1
                || sequence <= last_sequence
                || sequence > input_prefix
            {
                return Err(Error::Identity);
            }
            let e = self.income_evidence(sequence)?.ok_or(Error::Identity)?;
            if e.saturated()
                || e.query().purpose != income::IncomeUse::BaselineCoverage
                || e.query().private_connection_epoch != request.private_connection_epoch
                || e.query().gap_generation != request.gap_generation
                || domain.is_some_and(|d| d != e.domain())
                || next_start.is_some_and(|n| n != e.query().start_time_ms)
            {
                return Err(Error::Identity);
            }
            next_start = e.query().end_time_ms.checked_add(1);
            domain = Some(e.domain());
            last_sequence = sequence;
            for row in e.rows() {
                let v::AccountInputBody::Income(r) = &row.input().body else {
                    return Err(Error::Identity);
                };
                if row.input().identity.wallet_id != request.wallet_id
                    || covered
                        .insert(
                            (
                                r.income_type.as_str().to_owned(),
                                r.transaction_id.as_str().to_owned(),
                            ),
                            row.clone(),
                        )
                        .is_some()
                {
                    return Err(Error::Conflict);
                }
            }
            references.push((
                sequence,
                hash(&codec::encode(&self.records[sequence as usize - 1])?),
            ));
        }
        if next_start != request.through_time_ms.checked_add(1) {
            return Err(Error::Identity);
        }
        let mut evidence = Vec::new();
        let mut ledger = None;
        for (i, sequence) in request.snapshots.into_iter().enumerate() {
            if sequence <= last_sequence || sequence > input_prefix {
                return Err(Error::Identity);
            }
            let e = self.snapshot_evidence(sequence)?.ok_or(Error::Identity)?;
            let kind = [
                bootstrap::SnapshotKind::Account,
                bootstrap::SnapshotKind::OpenOrders,
                bootstrap::SnapshotKind::ConditionalOrders,
            ][i];
            if e.query().kind != kind
                || e.query().private_connection_epoch != request.private_connection_epoch
                || e.query().gap_generation != request.gap_generation
                || domain != Some(e.domain())
                || e.baseline_digest() != self.config.baseline.digest()
                || e.account_stream() != self.config.account_stream
                || ledger.is_some_and(|n| n != e.ledger_sequence())
            {
                return Err(Error::Identity);
            }
            ledger = Some(e.ledger_sequence());
            last_sequence = sequence;
            references.push((
                sequence,
                hash(&codec::encode(&self.records[sequence as usize - 1])?),
            ));
            evidence.push(e);
        }
        let account = Account::from_snapshot(
            self.config.baseline.snapshot().clone(),
            self.config.baseline.catalog().clone(),
        )
        .map_err(|_| Error::Identity)?;
        if ledger != Some(account.ledger_sequence()) {
            return Err(Error::Identity);
        }
        snapshot::verify_snapshot_with_orders(
            Market::Usdm,
            &self.config.baseline,
            &account,
            evidence[0].body(),
            evidence[1].body(),
            evidence[2].body(),
            &self.registry,
        )?;
        let manifest = Manifest {
            baseline: self.config.baseline.digest(),
            account_stream: self.config.account_stream,
            input_stream: self.input_stream(),
            account_epoch: account.account_epoch(),
            ledger: account.ledger_sequence(),
            wallet: request.wallet_id,
            input_prefix,
            private_epoch: request.private_connection_epoch,
            gap: request.gap_generation,
            through_time_ms: request.through_time_ms,
            covered: covered.into_values().collect(),
            references,
        };
        let bytes = manifest.bytes()?;
        Ok(IncomeImportProposal { manifest, bytes })
    }
    fn verify_proposal(&self, proposal: &IncomeImportProposal) -> Result<()> {
        let m = &proposal.manifest;
        if m.baseline != self.config.baseline.digest()
            || m.account_stream != self.config.account_stream
            || m.input_stream != self.input_stream()
        {
            return Err(Error::Identity);
        }
        let mut income_leaves = Vec::new();
        let mut snapshots = Vec::new();
        for (seq, digest) in &m.references {
            let record = self.records.get(*seq as usize - 1).ok_or(Error::Identity)?;
            if hash(&codec::encode(record)?) != *digest {
                return Err(Error::Identity);
            }
            match record {
                Record::Income(_) => income_leaves.push(*seq),
                Record::Snapshot(_) => snapshots.push(*seq),
                _ => return Err(Error::Identity),
            }
        }
        let request = ImportRequest {
            wallet_id: m.wallet,
            through_time_ms: m.through_time_ms,
            private_connection_epoch: m.private_epoch,
            gap_generation: m.gap,
            income_leaves,
            snapshots: snapshots.try_into().map_err(|_| Error::Identity)?,
        };
        if self.build_income_proposal(request, m.input_prefix)?.bytes != proposal.bytes {
            return Err(Error::Identity);
        }
        Ok(())
    }
    pub(crate) fn validate_import(
        &self,
        record: &ImportRecord,
        sequence: u64,
        writer: u64,
    ) -> Result<()> {
        if record.writer != writer
            || record.proposal.manifest.input_prefix.checked_add(1) != Some(sequence)
        {
            return Err(Error::Identity);
        }
        self.verify_proposal(&record.proposal)?;
        let authority = self
            .config
            .income_baseline_authority
            .as_ref()
            .ok_or(Error::Identity)?;
        let claims = ApprovalClaims::decode(record.approval.payload())?;
        authority.verify(
            &record.proposal,
            &record.approval,
            MonoTime::new(claims.domain()?, record.accepted),
            writer,
        )?;
        if self.records.iter().any(|r|matches!(r,Record::IncomeImport(old) if old.approval.request_id().ok()==Some(claims.request))){return Err(Error::Conflict)}
        Ok(())
    }
    pub fn income_import_status(&self, request: [u8; 16]) -> Option<v::Hash> {
        self.records.iter().find_map(|r| match r {
            Record::IncomeImport(r) if r.approval.request_id().ok() == Some(request) => {
                Some(r.proposal.digest())
            }
            _ => None,
        })
    }
    /// Enumerate durable approvals and their exact account effects. Reading this
    /// data neither installs a baseline nor grants new import authority.
    pub fn income_import_bindings(&self) -> impl Iterator<Item = Result<IncomeBaseline>> + '_ {
        self.records
            .iter()
            .enumerate()
            .filter_map(|(index, record)| {
                let Record::IncomeImport(record) = record else {
                    return None;
                };
                Some(self.import_binding(record, index as u64 + 1))
            })
    }
    pub fn income_coverage_object(&self, binding: &IncomeBaseline) -> Result<Vec<u8>> {
        let sequence = binding.through_sequence;
        let r = self
            .records
            .get(
                usize::try_from(sequence)
                    .ok()
                    .and_then(|n| n.checked_sub(1))
                    .ok_or(Error::Identity)?,
            )
            .ok_or(Error::Identity)?;
        let Record::IncomeImport(record) = r else {
            return Err(Error::Identity);
        };
        if self.import_binding(record, sequence)? != *binding {
            return Err(Error::Identity);
        }
        Ok(record.proposal.bytes.clone())
    }
    pub(crate) fn import_binding(
        &self,
        record: &ImportRecord,
        sequence: u64,
    ) -> Result<IncomeBaseline> {
        let m = &record.proposal.manifest;
        if m.input_prefix.checked_add(1) != Some(sequence) {
            return Err(Error::Identity);
        }
        Ok(IncomeBaseline {
            scope: self.config.baseline.snapshot().scope.clone(),
            account_epoch: m.account_epoch,
            ledger_sequence: m.ledger,
            wallet_id: m.wallet,
            through_time_ms: m.through_time_ms,
            coverage_digest: record.proposal.digest(),
            baseline_digest: m.baseline,
            input_stream: m.input_stream,
            through_sequence: sequence,
            covered: m.covered.clone(),
        })
    }
}

impl<S: Store> AccountIngress<S> {
    pub(super) fn verify_income_binding(
        &self,
        baseline: &CanonicalBaseline,
        account: &Account,
        preceding: &HistoryFrontier,
        binding: &IncomeBaseline,
    ) -> Result<()> {
        if baseline.digest() != self.config.baseline.digest()
            || binding.baseline_digest != baseline.digest()
            || binding.scope != baseline.snapshot().scope
            || binding.account_epoch != account.account_epoch()
            || binding.ledger_sequence != account.ledger_sequence()
            || preceding.baseline_digest() != baseline.digest()
            || preceding.scope() != &binding.scope
        {
            return Err(Error::Identity);
        }
        let record = self
            .records
            .get(
                usize::try_from(binding.through_sequence)
                    .ok()
                    .and_then(|n| n.checked_sub(1))
                    .ok_or(Error::Identity)?,
            )
            .ok_or(Error::Identity)?;
        let Record::IncomeImport(record) = record else {
            return Err(Error::Identity);
        };
        if self.import_binding(record, binding.through_sequence)? != *binding {
            return Err(Error::Identity);
        }
        self.verify_proposal(&record.proposal)?;
        let claims = ApprovalClaims::decode(record.approval.payload())?;
        self.config
            .income_baseline_authority
            .as_ref()
            .ok_or(Error::Identity)?
            .verify(
                &record.proposal,
                &record.approval,
                MonoTime::new(claims.domain()?, record.accepted),
                record.writer,
            )?;
        Ok(())
    }
}
impl<S: Store, B: Backend, A: Authority, E: Encoder> RestOwner<S, B, A, E> {
    pub fn import_income_baseline(
        &mut self,
        proposal: IncomeImportProposal,
        approval: SignedIncomeApproval,
        now: MonoTime,
    ) -> Result<Applied> {
        self.gateway.freeze_account();
        self.ingress.ready(&self.gateway)?;
        let request = approval.request_id()?;
        if let Some((index, old)) =
            self.ingress
                .records
                .iter()
                .enumerate()
                .find_map(|(i, r)| match r {
                    Record::IncomeImport(r) if r.approval.request_id().ok() == Some(request) => {
                        Some((i, r))
                    }
                    _ => None,
                })
        {
            if old.approval != approval || old.proposal.bytes != proposal.bytes {
                return Err(Error::Conflict);
            }
            let binding = self.ingress.import_binding(old, index as u64 + 1)?;
            if self.gateway.account().income_baseline(binding.wallet_id) == Some(&binding) {
                return Ok(Applied::Duplicate);
            }
            return self
                .gateway
                .install_income_baseline(binding, &mut self.ingress.activation_verifier())
                .map_err(Error::Gateway);
        }
        if self.busy() || proposal.input_prefix() != self.ingress.position() {
            return Err(Error::Busy);
        }
        let authority = self
            .ingress
            .config
            .income_baseline_authority
            .as_ref()
            .ok_or(Error::Identity)?;
        let proof =
            authority.verify(&proposal, &approval, now, self.ingress.config.writer_epoch)?;
        self.install_verified_income_import(proof)
    }
    fn install_verified_income_import(&mut self, proof: VerifiedIncomeImport) -> Result<Applied> {
        if self.ingress.config.income_baseline_authority.as_ref() != Some(&proof.authority)
            || proof.proposal.input_prefix() != self.ingress.position()
        {
            return Err(Error::Identity);
        }
        let record = ImportRecord {
            proposal: proof.proposal,
            approval: proof.approval,
            accepted: proof.accepted,
            writer: proof.writer,
        };
        let sequence = self
            .ingress
            .position()
            .checked_add(1)
            .ok_or(Error::Capacity)?;
        self.ingress
            .validate_import(&record, sequence, self.ingress.config.writer_epoch)?;
        let binding = self.ingress.import_binding(&record, sequence)?;
        self.account()
            .clone()
            .apply_income_baseline(&binding)
            .map_err(|_| Error::Identity)?;
        self.ingress.append(Record::IncomeImport(record))?;
        // The input is a durable approval; publishing its account effect has a
        // separate sync completion. A failed account write is retried on recovery.
        let applied = self
            .gateway
            .install_income_baseline(binding, &mut self.ingress.activation_verifier())
            .map_err(Error::Gateway)?;
        self.ingress.cursor = self.ingress.records.len();
        Ok(applied)
    }
    pub fn propose_income_import(&self, request: ImportRequest) -> Result<IncomeImportProposal> {
        if self.busy() {
            return Err(Error::Busy);
        }
        let proposal = self.ingress.propose_income_import(request)?;
        if proposal.manifest.ledger != self.account().ledger_sequence() {
            return Err(Error::Identity);
        }
        Ok(proposal)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use ring::signature::KeyPair;
    #[derive(Default)]
    pub(crate) struct Memory {
        bootstrap: Option<Vec<u8>>,
        baseline: Option<Vec<u8>>,
        manifest: Option<Vec<u8>>,
        segments: BTreeMap<u64, Vec<u8>>,
    }
    impl tickwire_account_journal::Backend for Memory {
        fn read_bootstrap(&mut self) -> std::io::Result<Option<Vec<u8>>> {
            Ok(self.bootstrap.clone())
        }
        fn publish_bootstrap(&mut self, b: &[u8]) -> std::io::Result<()> {
            self.bootstrap = Some(b.to_vec());
            Ok(())
        }
        fn read_baseline(&mut self) -> std::io::Result<Option<Vec<u8>>> {
            Ok(self.baseline.clone())
        }
        fn publish_baseline(&mut self, b: &[u8]) -> std::io::Result<()> {
            self.baseline = Some(b.to_vec());
            Ok(())
        }
    }
    impl Store for Memory {
        fn read_manifest(&mut self) -> std::io::Result<Option<Vec<u8>>> {
            Ok(self.manifest.clone())
        }
        fn publish_manifest(&mut self, b: &[u8]) -> std::io::Result<()> {
            self.manifest = Some(b.to_vec());
            Ok(())
        }
        fn segment_ids(&mut self) -> std::io::Result<Vec<u64>> {
            Ok(self.segments.keys().copied().collect())
        }
        fn read_segment(&mut self, id: u64, max: u64) -> std::io::Result<Vec<u8>> {
            let b = self.segments.get(&id).ok_or(std::io::ErrorKind::NotFound)?;
            if b.len() as u64 > max {
                return Err(std::io::ErrorKind::InvalidData.into());
            }
            Ok(b.clone())
        }
        fn create_segment(&mut self, id: u64) -> std::io::Result<()> {
            if self.segments.insert(id, Vec::new()).is_some() {
                return Err(std::io::ErrorKind::AlreadyExists.into());
            }
            Ok(())
        }
        fn append_segment(&mut self, id: u64, offset: u64, b: &[u8]) -> std::io::Result<()> {
            let old = self
                .segments
                .get_mut(&id)
                .ok_or(std::io::ErrorKind::NotFound)?;
            if old.len() as u64 != offset {
                return Err(std::io::ErrorKind::InvalidInput.into());
            }
            old.extend(b);
            Ok(())
        }
        fn sync_segment(&mut self, _: u64) -> std::io::Result<()> {
            Ok(())
        }
        fn truncate_segment(&mut self, id: u64, len: u64) -> std::io::Result<()> {
            self.segments
                .get_mut(&id)
                .ok_or(std::io::ErrorKind::NotFound)?
                .truncate(len as usize);
            Ok(())
        }
    }
    pub(crate) fn http(body: &[u8]) -> Vec<u8> {
        let mut out =
            format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", body.len()).into_bytes();
        out.extend(body);
        out
    }
    pub(crate) fn fixture() -> (
        AccountIngress<Memory>,
        ImportRequest,
        ring::signature::Ed25519KeyPair,
        ClockDomain,
    ) {
        fixture_authority(None)
    }
    fn fixture_authority(
        association: Option<crate::reconciliation::income_association::AssociationAuthority>,
    ) -> (
        AccountIngress<Memory>,
        ImportRequest,
        ring::signature::Ed25519KeyPair,
        ClockDomain,
    ) {
        let key = ring::signature::Ed25519KeyPair::from_seed_unchecked(&[42; 32]).unwrap();
        let authority = IncomeBaselineAuthority {
            controller_id: [31; 16],
            public_key: key.public_key().as_ref().try_into().unwrap(),
        };
        let (s, c, _) = crate::unit_fixture::futures_fixture();
        let baseline = CanonicalBaseline::new(s, c).unwrap();
        let mut input = AccountIngress::create(
            Memory::default(),
            Config {
                income_association_authority: association,
                income_baseline_authority: Some(authority),
                physical: tickwire_journal::Config::new([81; 16], [1; 16]),
                writer_epoch: 4,
                baseline,
                account_stream: [2; 16],
                max_records: 64,
                max_registrations: 16,
                max_raw_bytes: 32768,
                max_retained_bytes: 1 << 20,
            },
        )
        .unwrap();
        input
            .append(Record::BeginReconciliation {
                epoch: 7,
                generation: 3,
            })
            .unwrap();
        let query = income::IncomeQuery {
            start_time_ms: 1,
            end_time_ms: 100,
            limit: 2,
            private_connection_epoch: 7,
            gap_generation: 3,
            purpose: income::IncomeUse::BaselineCoverage,
        };
        let body=br#"[{"symbol":"","incomeType":"FUNDING_FEE","income":"-2","asset":"USDT","time":99,"tranId":42,"tradeId":""}]"#;
        let rows = income::bind(&input.config.baseline, query, body).unwrap();
        let income = input
            .append(Record::Income(income::IncomeRecord {
                query,
                epoch: 8,
                received: 10,
                clock_boot_id: [8; 16],
                clock_generation: 1,
                status: 200,
                accepted: true,
                raw: http(body),
                rows,
            }))
            .unwrap();
        let assets:Vec<_>=[("BTC","1"),("USDT","1000")].into_iter().map(|(asset,n)|serde_json::json!({"asset":asset,"walletBalance":n,"availableBalance":n,"marginBalance":n,"unrealizedProfit":"0","maintMargin":"0","initialMargin":"0","positionInitialMargin":"0","openOrderInitialMargin":"0","crossWalletBalance":n,"crossUnPnl":"0"})).collect();
        let body=serde_json::to_vec(&serde_json::json!({"totalInitialMargin":"0","totalMaintMargin":"0","totalUnrealizedProfit":"0","totalPositionInitialMargin":"0","totalOpenOrderInitialMargin":"0","totalCrossUnPnl":"0","assets":assets,"positions":[]})).unwrap();
        let mut snapshots = [0; 3];
        for (i, kind) in [
            bootstrap::SnapshotKind::Account,
            bootstrap::SnapshotKind::OpenOrders,
            bootstrap::SnapshotKind::ConditionalOrders,
        ]
        .into_iter()
        .enumerate()
        {
            let raw = http(if i == 0 { &body } else { b"[]" });
            snapshots[i] = input
                .append(Record::Snapshot(bootstrap::SnapshotRecord {
                    query: bootstrap::SnapshotQuery {
                        kind,
                        private_connection_epoch: 7,
                        gap_generation: 3,
                    },
                    epoch: 9 + i as u64,
                    received: 11 + i as u64,
                    clock_boot_id: [8; 16],
                    clock_generation: 1,
                    ledger: 0,
                    status: 200,
                    complete: true,
                    raw,
                }))
                .unwrap()
                .sequence;
        }
        input.cursor = input.records.len();
        (
            input,
            ImportRequest {
                wallet_id: 1,
                through_time_ms: 100,
                private_connection_epoch: 7,
                gap_generation: 3,
                income_leaves: vec![income.sequence],
                snapshots,
            },
            key,
            ClockDomain::new([8; 16], 1).unwrap(),
        )
    }
    pub(crate) fn approved_fixture() -> (
        AccountIngress<Memory>,
        Account,
        tickwire_account_journal::AccountJournal<Memory>,
        ClockDomain,
    ) {
        approved_fixture_authority(None)
    }
    pub(crate) fn approved_fixture_authority(
        association: Option<crate::reconciliation::income_association::AssociationAuthority>,
    ) -> (
        AccountIngress<Memory>,
        Account,
        tickwire_account_journal::AccountJournal<Memory>,
        ClockDomain,
    ) {
        let (mut input, request, key, domain) = fixture_authority(association);
        let proposal = input.propose_income_import(request).unwrap();
        let payload = proposal
            .approval_payload(
                [31; 16],
                [99; 16],
                4,
                MonoTime::new(domain, 14),
                MonoTime::new(domain, 100),
            )
            .unwrap();
        let approval = SignedIncomeApproval::new(&payload, key.sign(&payload).as_ref()).unwrap();
        let record = ImportRecord {
            proposal,
            approval,
            accepted: 15,
            writer: 4,
        };
        let sequence = input.position() + 1;
        input.validate_import(&record, sequence, 4).unwrap();
        let binding = input.import_binding(&record, sequence).unwrap();
        input.append(Record::IncomeImport(record)).unwrap();
        input.cursor = input.records.len();
        let mut account = Account::from_snapshot(
            input.config.baseline.snapshot().clone(),
            input.config.baseline.catalog().clone(),
        )
        .unwrap();
        account.apply_income_baseline(&binding).unwrap();
        let config = tickwire_account_journal::Config {
            physical: tickwire_journal::Config::new(
                input.config.account_stream,
                input.config.physical.domain,
            ),
            baseline_digest: input.config.baseline.digest(),
            initial_writer_epoch: 4,
            scope: input.config.baseline.snapshot().scope.clone(),
        };
        let mut journal = tickwire_account_journal::AccountJournal::create(
            Memory::default(),
            config,
            input.config.baseline.clone(),
        )
        .unwrap();
        journal
            .append(tickwire_account::RecoveryRecord::IncomeBaseline(Box::new(
                binding,
            )))
            .unwrap();
        (input, account, journal, domain)
    }
    #[test]
    fn complete_queries_require_separate_risk_admin_approval() {
        let (input, request, key, domain) = fixture();
        let proposal = input.propose_income_import(request).unwrap();
        assert_eq!(proposal.covered().len(), 1);
        let authority = input.config.income_baseline_authority.as_ref().unwrap();
        let payload = proposal
            .approval_payload(
                authority.controller_id,
                [99; 16],
                4,
                MonoTime::new(domain, 14),
                MonoTime::new(domain, 100),
            )
            .unwrap();
        let signature = key.sign(&payload);
        let approval = SignedIncomeApproval::new(&payload, signature.as_ref()).unwrap();
        assert!(
            authority
                .verify(&proposal, &approval, MonoTime::new(domain, 15), 4)
                .is_ok()
        );
        assert!(
            authority
                .verify(&proposal, &approval, MonoTime::new(domain, 100), 4)
                .is_err()
        );
        assert!(
            authority
                .verify(&proposal, &approval, MonoTime::new(domain, 15), 5)
                .is_err()
        );
        let bad = SignedIncomeApproval::new(&payload, &[0; 64]).unwrap();
        assert!(
            authority
                .verify(&proposal, &bad, MonoTime::new(domain, 15), 4)
                .is_err()
        );
    }
    #[test]
    fn risk_admin_cannot_replace_missing_query_evidence() {
        let (input, mut request, _, _) = fixture();
        request.income_leaves.clear();
        assert!(input.propose_income_import(request).is_err());
        let (mut input, request, _, _) = fixture();
        let Record::Income(r) = &mut input.records[2] else {
            panic!("income")
        };
        r.raw = http(b"[]");
        assert!(input.propose_income_import(request).is_err());
    }
    #[test]
    fn persisted_approval_reopens_with_original_acceptance_and_preserves_coverage() {
        let (mut input, request, key, domain) = fixture();
        let proposal = input.propose_income_import(request).unwrap();
        let payload = proposal
            .approval_payload(
                [31; 16],
                [99; 16],
                4,
                MonoTime::new(domain, 14),
                MonoTime::new(domain, 100),
            )
            .unwrap();
        let approval = SignedIncomeApproval::new(&payload, key.sign(&payload).as_ref()).unwrap();
        let record = ImportRecord {
            proposal: proposal.clone(),
            approval: approval.clone(),
            accepted: 15,
            writer: 4,
        };
        let sequence = input.position() + 1;
        input.validate_import(&record, sequence, 4).unwrap();
        let binding = input.import_binding(&record, sequence).unwrap();
        input.append(Record::IncomeImport(record.clone())).unwrap();
        assert_eq!(binding.through_sequence, proposal.input_prefix() + 1);

        let mut config = input.config.clone();
        config.writer_epoch = 5;
        let reopened = AccountIngress::recover(input.journal.into_store(), config, None).unwrap();
        assert_eq!(
            reopened.income_import_status([99; 16]),
            Some(proposal.digest())
        );
        assert_eq!(
            reopened.income_coverage_object(&binding).unwrap(),
            proposal.bytes()
        );
        assert!(matches!(
            reopened.validate_import(&record, sequence, 4),
            Err(Error::Conflict)
        ));
        // A new acceptance after expiry is refused, while the durable acceptance
        // at 15 was independently rechecked during reopen under writer 5.
        assert!(
            reopened
                .config
                .income_baseline_authority
                .as_ref()
                .unwrap()
                .verify(&proposal, &approval, MonoTime::new(domain, 100), 4)
                .is_err()
        );
        let baseline = &reopened.config.baseline;
        let mut account = Account::recover(
            baseline.snapshot().clone(),
            baseline.catalog().clone(),
            &[tickwire_account::RecoveryRecord::IncomeBaseline(Box::new(
                binding.clone(),
            ))],
        )
        .unwrap();
        let asset = crate::unit_fixture::key(2);
        let before = account.asset(asset).unwrap().total;
        let ledger = account.ledger_sequence();
        assert_eq!(
            account.apply_account_input(&binding.covered[0]),
            Ok(Applied::Duplicate)
        );
        assert_eq!(account.asset(asset).unwrap().total, before);
        assert_eq!(account.ledger_sequence(), ledger);

        // A previously unseen key before the approved cut remains unresolved;
        // even authentic later query data cannot silently extend the approval.
        let mut late = binding.covered[0].input().clone();
        let v::AccountInputBody::Income(row) = &mut late.body else {
            unreachable!()
        };
        row.transaction_id = v::BoundedText::new("43").unwrap();
        let late = v::CanonicalAccountInput::new(late).unwrap();
        assert_eq!(account.apply_account_input(&late), Ok(Applied::New));
        assert_eq!(account.asset(asset).unwrap().total, before);
        assert_eq!(
            account.pending_account_debit(asset).unwrap(),
            crate::unit_fixture::units(2)
        );
        assert!(account.frozen());
    }
}
