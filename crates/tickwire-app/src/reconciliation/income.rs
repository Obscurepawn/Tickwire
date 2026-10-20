//! Operator-approved initial income coverage. Files carry unsigned proposals and
//! separately signed approvals; the trading process never owns the signing key.
use super::*;
use std::{
    fs,
    io::{Read, Write},
    path::{Path, PathBuf},
};
use tickwire_binance_account::reconciliation::{
    income::{IncomeQuery, IncomeReader, IncomeScan, IncomeUse},
    income_import::{ImportRequest, IncomeImportProposal, SignedIncomeApproval},
};

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub wallet_id: u64,
    pub start_time_ms: u64,
    pub through_time_ms: u64,
    pub limit: u16,
    pub maximum_queries: usize,
    pub request_id: [u8; 16],
    pub approval_valid_for_ns: u64,
    pub file_poll_interval_ns: u64,
    pub proposal_file: PathBuf,
    pub approval_file: PathBuf,
}
pub(super) struct Work {
    pub reader: IncomeReader,
    pub config: Config,
    pub scan: Option<IncomeScan>,
    proposal: Option<(IncomeImportProposal, Vec<u8>, u64)>,
    poll_at: u64,
}
impl Work {
    pub fn new(mut config: Config, reader: ReconcileConfig) -> crate::Result<Self> {
        config.proposal_file = fixed_path(&config.proposal_file)?;
        config.approval_file = fixed_path(&config.approval_file)?;
        if config.wallet_id == 0
            || config.request_id == [0; 16]
            || config.approval_valid_for_ns == 0
            || config.file_poll_interval_ns == 0
            || config.proposal_file == config.approval_file
            || config.proposal_file.file_name().is_none()
            || config.approval_file.file_name().is_none()
        {
            return Err(crate::invalid("income import configuration"));
        }
        IncomeScan::new(
            IncomeQuery {
                start_time_ms: config.start_time_ms,
                end_time_ms: config.through_time_ms,
                limit: config.limit,
                private_connection_epoch: 1,
                gap_generation: 1,
                purpose: IncomeUse::BaselineCoverage,
            },
            config.maximum_queries,
        )?;
        Ok(Self {
            reader: IncomeReader::new(reader)?,
            config,
            scan: None,
            proposal: None,
            poll_at: 0,
        })
    }
    pub fn reset(&mut self) {
        self.reader.cancel();
        self.scan = None;
        self.proposal = None;
        self.poll_at = 0;
    }
    pub fn query(
        &mut self,
        cut: (u64, u64),
    ) -> tickwire_binance_account::Result<Option<IncomeQuery>> {
        if self.scan.is_none() {
            self.scan = Some(IncomeScan::new(
                IncomeQuery {
                    start_time_ms: self.config.start_time_ms,
                    end_time_ms: self.config.through_time_ms,
                    limit: self.config.limit,
                    private_connection_epoch: cut.0,
                    gap_generation: cut.1,
                    purpose: IncomeUse::BaselineCoverage,
                },
                self.config.maximum_queries,
            )?);
        }
        self.scan.as_ref().expect("initialized scan").next_query()
    }
    pub fn request(
        &self,
        cut: (u64, u64),
        snapshots: &[SnapshotEvidence],
    ) -> crate::Result<ImportRequest> {
        let scan = self
            .scan
            .as_ref()
            .filter(|s| s.complete())
            .ok_or_else(|| crate::invalid("income scan incomplete"))?;
        if snapshots.len() != 3 {
            return Err(crate::invalid("income snapshots incomplete"));
        }
        Ok(ImportRequest {
            wallet_id: self.config.wallet_id,
            through_time_ms: self.config.through_time_ms,
            private_connection_epoch: cut.0,
            gap_generation: cut.1,
            income_leaves: scan.leaf_sequences().to_vec(),
            snapshots: [
                snapshots[0].input().sequence,
                snapshots[1].input().sequence,
                snapshots[2].input().sequence,
            ],
        })
    }
    pub fn files(
        &mut self,
        proposal: IncomeImportProposal,
        controller: [u8; 16],
        writer: u64,
        now: MonoTime,
    ) -> crate::Result<Option<(IncomeImportProposal, SignedIncomeApproval)>> {
        let replace = self
            .proposal
            .as_ref()
            .is_none_or(|(old, _, until)| old.bytes() != proposal.bytes() || now.nanos() >= *until);
        if replace {
            let until = now
                .nanos()
                .checked_add(self.config.approval_valid_for_ns)
                .ok_or_else(|| crate::invalid("approval deadline overflow"))?;
            let payload = proposal.approval_payload(
                controller,
                self.config.request_id,
                writer,
                now,
                MonoTime::new(now.domain(), until),
            )?;
            let body = serde_json::to_vec_pretty(&serde_json::json!({
                "format":"tickwire.income-baseline-proposal.v1","manifest_hex":hex(proposal.bytes()),
                "manifest_sha256":hex(&proposal.digest()),"approval_payload_hex":hex(&payload),
                "request_id":hex(&self.config.request_id),"input_prefix":proposal.input_prefix(),
                "writer_epoch":writer,"expires_mono_ns":until,
                "approval_meaning":"risk-admin assertion that the pinned account baseline includes the listed income records; query results alone do not grant coverage"
            }))?;
            publish(&self.config.proposal_file, &body)?;
            self.proposal = Some((proposal, payload, until));
            self.poll_at = 0;
        }
        if now.nanos() < self.poll_at {
            return Ok(None);
        }
        self.poll_at = now
            .nanos()
            .saturating_add(self.config.file_poll_interval_ns);
        let Some(bytes) = read_approval(&self.config.approval_file)? else {
            return Ok(None);
        };
        let file: ApprovalFile = serde_json::from_slice(&bytes)?;
        if file.format != "tickwire.income-baseline-approval.v1" {
            return Err(crate::invalid("income approval format"));
        }
        let payload = unhex(&file.payload_hex, 512)?;
        let signature = unhex(&file.signature_hex, 64)?;
        let (proposal, expected, _) = self.proposal.as_ref().expect("published proposal");
        // An approval for an older prefix or prior request attempt is kept for
        // audit but cannot apply to this newly published proposal.
        if payload != *expected {
            return Ok(None);
        }
        Ok(Some((
            proposal.clone(),
            SignedIncomeApproval::new(&payload, &signature)?,
        )))
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ApprovalFile {
    format: String,
    payload_hex: String,
    signature_hex: String,
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|n| format!("{n:02x}")).collect()
}
fn unhex(s: &str, max: usize) -> crate::Result<Vec<u8>> {
    if !s.len().is_multiple_of(2)
        || s.len() > max * 2
        || !s
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(crate::invalid("income approval hex"));
    }
    s.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|v| u8::from_str_radix(std::str::from_utf8(v)?, 16).map_err(Into::into))
        .collect()
}
fn read_approval(path: &Path) -> crate::Result<Option<Vec<u8>>> {
    let file = match fs::File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let mut bytes = Vec::new();
    file.take(4097).read_to_end(&mut bytes)?;
    if bytes.len() > 4096 {
        return Err(crate::invalid("income approval file too large"));
    }
    Ok(Some(bytes))
}
fn publish(path: &Path, bytes: &[u8]) -> crate::Result<()> {
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    // A fresh suffix prevents a leftover from an interrupted publish blocking a
    // later process, while create_new rejects accidentally reusing any file.
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let suffix = NEXT.fetch_add(1, Ordering::Relaxed);
    let temporary = path.with_extension(format!(
        "{}.{suffix}.income-proposal.tmp",
        std::process::id()
    ));
    let mut options = fs::OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&temporary)?;
    let result = (|| {
        file.write_all(bytes)?;
        file.sync_all()?;
        fs::rename(&temporary, path)?;
        fs::File::open(parent)?.sync_all()
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result.map_err(Into::into)
}
fn fixed_path(path: &Path) -> crate::Result<PathBuf> {
    let name = path
        .file_name()
        .ok_or_else(|| crate::invalid("income file name"))?;
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    Ok(fs::canonicalize(parent)?.join(name))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn approval_file_is_typed_bounded_and_never_a_boolean() {
        assert!(serde_json::from_slice::<ApprovalFile>(br#"{"approved":true}"#).is_err());
        assert!(unhex("FF", 64).is_err());
        assert!(unhex(&"00".repeat(65), 64).is_err());
        assert_eq!(unhex("00ff", 64).unwrap(), vec![0, 255]);
        assert!(
            serde_json::from_slice::<ApprovalFile>(
                br#"{"format":"x","payload_hex":"00","signature_hex":"00","approved":true}"#
            )
            .is_err()
        );
    }
}
