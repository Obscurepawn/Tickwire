use super::*;
use crate::reconciliation::income_closure::encoding::{
    Manifest as QueryManifest, Reader, Reference, Writer,
};
use tickwire_account::{IncomeIdentity, IncomeObservationsAssociated, ObservationAssociation};
const DOMAIN: &[u8] = b"tickwire.binance.income-association-manifest.v1";
pub const MAX_MANIFEST_BYTES: usize = 54 * 1024;
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Anchor {
    pub sequence: u64,
    pub ledger: u64,
    pub prefix: v::Hash,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Group {
    pub input: Reference,
    pub canonical: v::Hash,
    pub canonical_length: u64,
    pub epoch: u64,
    pub raw_hash: v::Hash,
    pub incomes: Vec<IncomeIdentity>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Manifest {
    pub query: QueryManifest,
    pub request: [u8; 16],
    pub wallet: u64,
    pub instance: [u8; 16],
    pub certificate: v::Hash,
    pub policy: Reference,
    pub anchor: Anchor,
    pub state: v::Hash,
    pub groups: Vec<Group>,
    pub account_sequence: u64,
}
impl Manifest {
    pub fn bytes(&self) -> Result<Vec<u8>> {
        if self.groups.is_empty()
            || self.groups.len() > tickwire_account::MAX_ASSOCIATION_OBSERVATIONS
            || self
                .groups
                .iter()
                .any(|g| g.incomes.len() > tickwire_account::MAX_ASSOCIATION_INCOMES)
            || self.groups.iter().map(|g| g.incomes.len()).sum::<usize>()
                > tickwire_account::MAX_ASSOCIATION_INCOMES
            || self.query.leaves.len() + self.groups.len() + 4 > 128
        {
            return Err(Error::Capacity);
        }
        let mut w = Writer(Vec::new());
        w.array(12);
        w.blob(DOMAIN);
        w.uint(1);
        w.blob(&self.query.bytes()?);
        w.blob(&self.request);
        w.uint(self.wallet);
        w.blob(&self.instance);
        w.blob(&self.certificate);
        w.array(3);
        w.reference(&self.policy);
        w.array(3);
        w.uint(self.anchor.sequence);
        w.uint(self.anchor.ledger);
        w.blob(&self.anchor.prefix);
        w.blob(&self.state);
        w.array(self.groups.len());
        for g in &self.groups {
            if g.incomes.is_empty() || g.incomes.windows(2).any(|s| s[0] >= s[1]) {
                return Err(Error::Identity);
            }
            w.array(6);
            w.array(3);
            w.reference(&g.input);
            w.blob(&g.canonical);
            w.uint(g.canonical_length);
            w.uint(g.epoch);
            w.blob(&g.raw_hash);
            w.array(g.incomes.len());
            for key in &g.incomes {
                w.array(2);
                w.blob(key.income_type.as_str().as_bytes());
                w.blob(key.transaction_id.as_str().as_bytes())
            }
        }
        w.uint(self.account_sequence);
        if w.0.len() > MAX_MANIFEST_BYTES {
            return Err(Error::Capacity);
        }
        Ok(w.0)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > MAX_MANIFEST_BYTES {
            return Err(Error::Capacity);
        }
        let mut r = Reader { bytes, pos: 0 };
        r.array(12)?;
        if r.blob(64)? != DOMAIN || r.uint()? != 1 {
            return Err(Error::Encoding);
        }
        let query = QueryManifest::decode(r.blob(MAX_MANIFEST_BYTES)?)?;
        let request = r.fixed()?;
        let wallet = r.uint()?;
        let instance = r.fixed()?;
        let certificate = r.fixed()?;
        r.array(3)?;
        let policy = r.reference()?;
        r.array(3)?;
        let anchor = Anchor {
            sequence: r.uint()?,
            ledger: r.uint()?,
            prefix: r.fixed()?,
        };
        let state = r.fixed()?;
        let count = r.count(tickwire_account::MAX_ASSOCIATION_OBSERVATIONS)?;
        let mut groups = Vec::with_capacity(count);
        let mut total = 0;
        for _ in 0..count {
            r.array(6)?;
            r.array(3)?;
            let input = r.reference()?;
            let canonical = r.fixed()?;
            let canonical_length = r.uint()?;
            let epoch = r.uint()?;
            let raw_hash = r.fixed()?;
            let count = r.count(tickwire_account::MAX_ASSOCIATION_INCOMES)?;
            total += count;
            if total > tickwire_account::MAX_ASSOCIATION_INCOMES {
                return Err(Error::Capacity);
            }
            let mut incomes = Vec::with_capacity(count);
            for _ in 0..count {
                r.array(2)?;
                incomes.push(IncomeIdentity {
                    income_type: v::BoundedText::new(&r.text(64)?).map_err(|_| Error::Encoding)?,
                    transaction_id: v::BoundedText::new(&r.text(32)?)
                        .map_err(|_| Error::Encoding)?,
                })
            }
            groups.push(Group {
                input,
                canonical,
                canonical_length,
                epoch,
                raw_hash,
                incomes,
            });
        }
        let account_sequence = r.uint()?;
        let out = Self {
            query,
            request,
            wallet,
            instance,
            certificate,
            policy,
            anchor,
            state,
            groups,
            account_sequence,
        };
        if r.pos != bytes.len() || out.bytes()? != bytes {
            return Err(Error::Encoding);
        }
        Ok(out)
    }
    pub fn binding(
        &self,
        manifest: v::Hash,
        approval: v::Hash,
    ) -> Result<IncomeObservationsAssociated> {
        let b = &self.query.original;
        Ok(IncomeObservationsAssociated {
            scope: b.scope.clone(),
            account_epoch: b.account_epoch,
            ledger_sequence: b.ledger_sequence,
            wallet_id: self.wallet,
            request_id: self.request,
            baseline_digest: b.baseline_digest,
            history_digest: b.history_digest,
            input_stream: b.input_stream,
            through_sequence: b.through_sequence.checked_add(1).ok_or(Error::Identity)?,
            state_digest: self.state,
            manifest_digest: manifest,
            approval_digest: approval,
            associations: self
                .groups
                .iter()
                .map(|g| ObservationAssociation {
                    observation_digest: g.canonical,
                    incomes: g.incomes.clone(),
                })
                .collect(),
        })
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AssociationProposal {
    pub(crate) manifest: Manifest,
    pub(super) bytes: Vec<u8>,
}
impl AssociationProposal {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        Ok(Self {
            manifest: Manifest::decode(bytes)?,
            bytes: bytes.to_vec(),
        })
    }
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub fn digest(&self) -> v::Hash {
        authority::hash(&self.bytes)
    }
    pub fn request_id(&self) -> [u8; 16] {
        self.manifest.request
    }
    pub fn input_prefix(&self) -> u64 {
        self.manifest.query.original.through_sequence
    }
    pub fn ledger_sequence(&self) -> u64 {
        self.manifest.query.original.ledger_sequence
    }
    pub fn history_digest(&self) -> v::Hash {
        self.manifest.query.original.history_digest
    }
    pub fn valid_until(&self) -> Result<tickwire_clock::MonoTime> {
        Ok(tickwire_clock::MonoTime::new(
            tickwire_clock::ClockDomain::new(
                self.manifest.query.boot,
                self.manifest.query.generation,
            )
            .map_err(|_| Error::Identity)?,
            self.manifest.query.expires,
        ))
    }
    pub fn approval_payload(
        &self,
        certificate: &SignedAssociationAuthority,
        audit: v::Hash,
        issued: u64,
        expires: u64,
    ) -> Result<Vec<u8>> {
        if certificate.digest() != self.manifest.certificate {
            return Err(Error::Identity);
        }
        SignedAssociationApproval::payload_to_sign(
            certificate,
            self.request_id(),
            self.digest(),
            audit,
            issued,
            expires,
        )
    }
}
#[derive(Clone)]
pub(crate) struct AssociationRecord {
    pub proposal: AssociationProposal,
    pub approval: SignedAssociationApproval,
    pub accepted: u64,
    pub writer: u64,
}
