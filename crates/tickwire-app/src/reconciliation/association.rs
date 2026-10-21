//! Explicit association selections and externally signed approval files. This
//! owner stores no signing key and cannot activate a leaf from these files.
use super::*;
use std::path::PathBuf;
use tickwire_binance_account::reconciliation::income_association::{
    AssociationProposal, AssociationRequest, SignedAssociationApproval, SignedAssociationAuthority,
};
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub selection_file: PathBuf,
    pub proposal_file: PathBuf,
    pub approval_file: PathBuf,
    pub file_poll_interval_ns: u64,
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Key {
    income_type: String,
    transaction_id: String,
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Group {
    observation_digest_hex: String,
    incomes: Vec<Key>,
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Selection {
    format: String,
    request_id_hex: String,
    wallet_id: u64,
    certificate_payload_hex: String,
    certificate_signature_hex: String,
    authorization_evidence_hex: String,
    associations: Vec<Group>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Approval {
    format: String,
    payload_hex: String,
    signature_hex: String,
    audit_evidence_hex: String,
}
pub(super) struct Work {
    config: Config,
    selection: Option<Selection>,
    selection_bytes: Vec<u8>,
    proposal: Option<(AssociationProposal, SignedAssociationAuthority)>,
    poll_at: u64,
    approval_poll_at: u64,
}
impl Work {
    pub fn new(mut c: Config) -> crate::Result<Self> {
        if c.file_poll_interval_ns == 0 {
            return Err(crate::invalid("association poll interval"));
        }
        c.selection_file = income::fixed_path(&c.selection_file)?;
        c.proposal_file = income::fixed_path(&c.proposal_file)?;
        c.approval_file = income::fixed_path(&c.approval_file)?;
        if c.selection_file == c.proposal_file
            || c.selection_file == c.approval_file
            || c.proposal_file == c.approval_file
        {
            return Err(crate::invalid("association file paths"));
        }
        Ok(Self {
            config: c,
            selection: None,
            selection_bytes: Vec::new(),
            proposal: None,
            poll_at: 0,
            approval_poll_at: 0,
        })
    }
    pub fn reset(&mut self) {
        self.proposal = None;
        self.poll_at = 0;
        self.approval_poll_at = 0;
    }
    pub fn poll_selection(&mut self, now: u64) -> crate::Result<bool> {
        if now < self.poll_at {
            return Ok(self.selection.is_some());
        }
        self.poll_at = now.saturating_add(self.config.file_poll_interval_ns);
        let Some(bytes) = read(&self.config.selection_file, 128 * 1024)? else {
            return Ok(false);
        };
        if bytes != self.selection_bytes {
            let s: Selection = serde_json::from_slice(&bytes)?;
            if s.format != "tickwire.income-association-selection.v1"
                || s.wallet_id == 0
                || s.associations.is_empty()
                || s.associations.len() > 16
                || s.associations
                    .iter()
                    .any(|g| g.incomes.is_empty() || g.incomes.len() > 128)
                || s.associations
                    .iter()
                    .map(|g| g.incomes.len())
                    .sum::<usize>()
                    > 128
            {
                return Err(crate::invalid("association selection bounds"));
            }
            self.selection = Some(s);
            self.selection_bytes = bytes;
            self.proposal = None;
        }
        Ok(true)
    }
    pub fn cached(
        &self,
        ledger: u64,
        history: [u8; 32],
        position: u64,
        now: MonoTime,
    ) -> Option<AssociationProposal> {
        let (p, _) = self.proposal.as_ref()?;
        let until = p.valid_until().ok()?;
        (p.ledger_sequence() == ledger
            && p.history_digest() == history
            && p.input_prefix() == position
            && until.domain() == now.domain()
            && now.nanos() < until.nanos())
        .then(|| p.clone())
    }
    pub fn request(
        &self,
        queries: tickwire_binance_account::reconciliation::income_closure::ClosureRequest,
    ) -> crate::Result<(AssociationRequest, Vec<u8>)> {
        let s = self
            .selection
            .as_ref()
            .ok_or_else(|| crate::invalid("no association selection"))?;
        let certificate = SignedAssociationAuthority::new(
            &income::unhex(&s.certificate_payload_hex, 1024)?,
            &income::unhex(&s.certificate_signature_hex, 64)?,
        )?;
        let evidence = income::unhex(&s.authorization_evidence_hex, 32 * 1024)?;
        let request_id = income::unhex(&s.request_id_hex, 16)?
            .try_into()
            .map_err(|_| crate::invalid("association request id"))?;
        let mut associations = Vec::new();
        for g in &s.associations {
            let observation_digest = income::unhex(&g.observation_digest_hex, 32)?
                .try_into()
                .map_err(|_| crate::invalid("association observation digest"))?;
            let mut incomes = Vec::new();
            for k in &g.incomes {
                incomes.push(tickwire_account::IncomeIdentity {
                    income_type: tickwire_venue::BoundedText::new(&k.income_type)
                        .map_err(|_| crate::invalid("association income type"))?,
                    transaction_id: tickwire_venue::BoundedText::new(&k.transaction_id)
                        .map_err(|_| crate::invalid("association income id"))?,
                })
            }
            associations.push(tickwire_account::ObservationAssociation {
                observation_digest,
                incomes,
            });
        }
        Ok((
            AssociationRequest {
                request_id,
                wallet_id: s.wallet_id,
                associations,
                queries,
                certificate,
            },
            evidence,
        ))
    }
    pub fn publish(
        &mut self,
        p: AssociationProposal,
        certificate: SignedAssociationAuthority,
    ) -> crate::Result<()> {
        let body = serde_json::to_vec_pretty(
            &serde_json::json!({"format":"tickwire.income-association-proposal.v1","manifest_hex":income::hex(p.bytes()),"manifest_sha256":income::hex(&p.digest()),"request_id":income::hex(&p.request_id()),"certificate_sha256":income::hex(&certificate.digest()),"ledger_sequence":p.ledger_sequence(),"input_prefix":p.input_prefix(),"expires_mono_ns":p.valid_until()?.nanos(),"approval_meaning":"scoped risk-admin assertion associating exactly these persisted observations and income keys; this does not alter cash, baseline coverage or retained risk liabilities"}),
        )?;
        income::publish(&self.config.proposal_file, &body)?;
        self.proposal = Some((p, certificate));
        self.approval_poll_at = 0;
        Ok(())
    }
    pub fn approval(
        &mut self,
        now: u64,
    ) -> crate::Result<Option<(AssociationProposal, SignedAssociationApproval, Vec<u8>)>> {
        if now < self.approval_poll_at {
            return Ok(None);
        }
        self.approval_poll_at = now.saturating_add(self.config.file_poll_interval_ns);
        let Some((p, certificate)) = &self.proposal else {
            return Ok(None);
        };
        let Some(bytes) = read(&self.config.approval_file, 128 * 1024)? else {
            return Ok(None);
        };
        let a: Approval = serde_json::from_slice(&bytes)?;
        if a.format != "tickwire.income-association-approval.v1" {
            return Err(crate::invalid("association approval format"));
        }
        let approval = SignedAssociationApproval::new(
            certificate.clone(),
            &income::unhex(&a.payload_hex, 1024)?,
            &income::unhex(&a.signature_hex, 64)?,
        )?;
        if approval.manifest_digest().ok() != Some(p.digest())
            || approval.request_id().ok() != Some(p.request_id())
        {
            return Ok(None);
        }
        Ok(Some((
            p.clone(),
            approval,
            income::unhex(&a.audit_evidence_hex, 32 * 1024)?,
        )))
    }
}
fn read(path: &std::path::Path, maximum: usize) -> crate::Result<Option<Vec<u8>>> {
    super::files::read(path, maximum)
}
