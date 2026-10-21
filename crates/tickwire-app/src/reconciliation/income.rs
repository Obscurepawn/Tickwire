//! Operator-approved initial income coverage. Files carry unsigned proposals and
//! separately signed approvals; the trading process never owns the signing key.
use super::*;
use std::{
    fs,
    io::Write,
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
/// Periodic retained-range queries. This is not an assertion that Binance has
/// published every income row through the selected end time.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccountConfig {
    pub wallet_id: u64,
    pub lookback_ms: u64,
    pub interval_ns: u64,
    pub limit: u16,
    pub maximum_queries: usize,
}
impl AccountConfig {
    fn validate(&self) -> crate::Result<()> {
        if self.wallet_id == 0
            || self.lookback_ms == 0
            || self.lookback_ms > 7 * 24 * 60 * 60 * 1000
            || self.interval_ns == 0
        {
            return Err(crate::invalid("account income scan configuration"));
        }
        IncomeScan::new(self.query((1, 1), self.lookback_ms)?, self.maximum_queries)?;
        Ok(())
    }
    fn query(&self, cut: (u64, u64), end_ms: u64) -> tickwire_binance_account::Result<IncomeQuery> {
        if end_ms == 0 {
            return Err(tickwire_binance_account::Error::Identity);
        }
        let q = IncomeQuery {
            start_time_ms: end_ms.saturating_sub(self.lookback_ms),
            end_time_ms: end_ms,
            limit: self.limit,
            private_connection_epoch: cut.0,
            gap_generation: cut.1,
            purpose: IncomeUse::Account,
        };
        q.validate()?;
        Ok(q)
    }
}
pub(super) struct Work {
    pub reader: IncomeReader,
    pub config: Option<Config>,
    pub account_config: Option<AccountConfig>,
    next_scan_at: u64,
    closed_ledger: Option<u64>,
    pub scan: Option<IncomeScan>,
    proposal: Option<(IncomeImportProposal, Vec<u8>, u64)>,
    poll_at: u64,
}
impl Work {
    pub fn new(
        config: Option<Config>,
        account_config: Option<AccountConfig>,
        reader: ReconcileConfig,
    ) -> crate::Result<Self> {
        let config = config
            .map(|mut c| -> crate::Result<Config> {
                c.proposal_file = fixed_path(&c.proposal_file)?;
                c.approval_file = fixed_path(&c.approval_file)?;
                if c.wallet_id == 0
                    || c.request_id == [0; 16]
                    || c.approval_valid_for_ns == 0
                    || c.file_poll_interval_ns == 0
                    || c.proposal_file == c.approval_file
                {
                    return Err(crate::invalid("income import configuration"));
                }
                IncomeScan::new(
                    IncomeQuery {
                        start_time_ms: c.start_time_ms,
                        end_time_ms: c.through_time_ms,
                        limit: c.limit,
                        private_connection_epoch: 1,
                        gap_generation: 1,
                        purpose: IncomeUse::BaselineCoverage,
                    },
                    c.maximum_queries,
                )?;
                Ok(c)
            })
            .transpose()?;
        if let Some(c) = &account_config {
            c.validate()?;
            if config.as_ref().is_some_and(|i| i.wallet_id != c.wallet_id) {
                return Err(crate::invalid("income wallet mismatch"));
            }
        }
        if config.is_none() && account_config.is_none() {
            return Err(crate::invalid("income work configuration"));
        }
        Ok(Self {
            reader: IncomeReader::new(reader)?,
            config,
            account_config,
            scan: None,
            proposal: None,
            poll_at: 0,
            next_scan_at: 0,
            closed_ledger: None,
        })
    }
    pub fn wallet_id(&self) -> u64 {
        self.config
            .as_ref()
            .map(|c| c.wallet_id)
            .or_else(|| self.account_config.as_ref().map(|c| c.wallet_id))
            .expect("income config")
    }
    pub fn due(&self, has_baseline: bool, now: u64) -> bool {
        has_baseline && self.account_config.is_some() && now >= self.next_scan_at
    }
    pub fn after_closure(&mut self, ledger: u64) {
        self.scan = None;
        self.closed_ledger = Some(ledger);
    }
    pub fn completed(&mut self, now: u64) {
        self.reset();
        self.next_scan_at =
            now.saturating_add(self.account_config.as_ref().map_or(0, |c| c.interval_ns));
    }
    pub fn reset(&mut self) {
        self.reader.cancel();
        self.scan = None;
        self.proposal = None;
        self.poll_at = 0;
        self.closed_ledger = None;
    }
    pub fn query(
        &mut self,
        cut: (u64, u64),
        has_baseline: bool,
        ledger: u64,
        end_ms: u64,
    ) -> tickwire_binance_account::Result<Option<IncomeQuery>> {
        if self.closed_ledger == Some(ledger) {
            return Ok(None);
        }
        if self.closed_ledger.take().is_some() {
            self.scan = None;
        }
        if self.scan.is_none() {
            let (query, maximum) = if has_baseline {
                let Some(c) = &self.account_config else {
                    return Ok(None);
                };
                (c.query(cut, end_ms)?, c.maximum_queries)
            } else {
                let Some(c) = &self.config else {
                    return Ok(None);
                };
                (
                    IncomeQuery {
                        start_time_ms: c.start_time_ms,
                        end_time_ms: c.through_time_ms,
                        limit: c.limit,
                        private_connection_epoch: cut.0,
                        gap_generation: cut.1,
                        purpose: IncomeUse::BaselineCoverage,
                    },
                    c.maximum_queries,
                )
            };
            self.scan = Some(IncomeScan::new(query, maximum)?);
        }
        self.scan.as_ref().expect("initialized scan").next_query()
    }
    pub fn closure_request(
        &self,
        cut: tickwire_binance_account::reconciliation::activation::ActivationCut,
        snapshots: &[SnapshotEvidence],
    ) -> crate::Result<tickwire_binance_account::reconciliation::income_closure::ClosureRequest>
    {
        let scan = self
            .scan
            .as_ref()
            .filter(|s| s.complete())
            .ok_or_else(|| crate::invalid("income scan incomplete"))?;
        if snapshots.len() != 3 {
            return Err(crate::invalid("income snapshots incomplete"));
        }
        Ok(
            tickwire_binance_account::reconciliation::income_closure::ClosureRequest {
                income_leaves: scan.leaf_sequences().to_vec(),
                snapshots: [
                    snapshots[0].input().sequence,
                    snapshots[1].input().sequence,
                    snapshots[2].input().sequence,
                ],
                cut,
            },
        )
    }
    pub fn request(
        &self,
        cut: (u64, u64),
        snapshots: &[SnapshotEvidence],
    ) -> crate::Result<ImportRequest> {
        let config = self
            .config
            .as_ref()
            .ok_or_else(|| crate::invalid("income import disabled"))?;
        let scan = self
            .scan
            .as_ref()
            .filter(|s| s.complete())
            .ok_or_else(|| crate::invalid("income scan incomplete"))?;
        if snapshots.len() != 3 {
            return Err(crate::invalid("income snapshots incomplete"));
        }
        Ok(ImportRequest {
            wallet_id: config.wallet_id,
            through_time_ms: config.through_time_ms,
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
        let config = self
            .config
            .as_ref()
            .ok_or_else(|| crate::invalid("income import disabled"))?;
        let replace = self
            .proposal
            .as_ref()
            .is_none_or(|(old, _, until)| old.bytes() != proposal.bytes() || now.nanos() >= *until);
        if replace {
            let until = now
                .nanos()
                .checked_add(config.approval_valid_for_ns)
                .ok_or_else(|| crate::invalid("approval deadline overflow"))?;
            let payload = proposal.approval_payload(
                controller,
                config.request_id,
                writer,
                now,
                MonoTime::new(now.domain(), until),
            )?;
            let body = serde_json::to_vec_pretty(&serde_json::json!({
                "format":"tickwire.income-baseline-proposal.v1","manifest_hex":hex(proposal.bytes()),
                "manifest_sha256":hex(&proposal.digest()),"approval_payload_hex":hex(&payload),
                "request_id":hex(&config.request_id),"input_prefix":proposal.input_prefix(),
                "writer_epoch":writer,"expires_mono_ns":until,
                "approval_meaning":"risk-admin assertion that the pinned account baseline includes the listed income records; query results alone do not grant coverage"
            }))?;
            publish(&config.proposal_file, &body)?;
            self.proposal = Some((proposal, payload, until));
            self.poll_at = 0;
        }
        if now.nanos() < self.poll_at {
            return Ok(None);
        }
        self.poll_at = now.nanos().saturating_add(config.file_poll_interval_ns);
        let Some(bytes) = read_approval(&config.approval_file)? else {
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
pub(super) fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|n| format!("{n:02x}")).collect()
}
pub(super) fn unhex(s: &str, max: usize) -> crate::Result<Vec<u8>> {
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
    super::files::read(path, 4096)
}
pub(super) fn publish(path: &Path, bytes: &[u8]) -> crate::Result<()> {
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
pub(super) fn fixed_path(path: &Path) -> crate::Result<PathBuf> {
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
    fn ongoing_queries_use_account_purpose_and_an_explicit_bounded_retained_range() {
        let config = AccountConfig {
            wallet_id: 1,
            lookback_ms: 1000,
            interval_ns: 1_000_000_000,
            limit: 16,
            maximum_queries: 128,
        };
        config.validate().unwrap();
        let q = config.query((7, 3), 5000).unwrap();
        assert_eq!((q.start_time_ms, q.end_time_ms), (4000, 5000));
        assert_eq!(q.purpose, IncomeUse::Account);
        assert_eq!((q.private_connection_epoch, q.gap_generation), (7, 3));
        assert_eq!(config.query((7, 3), 500).unwrap().start_time_ms, 0);
        assert!(config.query((7, 3), 0).is_err());
        let mut invalid = config;
        invalid.maximum_queries = 129;
        assert!(invalid.validate().is_err());
        invalid.maximum_queries = 128;
        invalid.lookback_ms = 8 * 24 * 60 * 60 * 1000;
        assert!(invalid.validate().is_err());
    }
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
