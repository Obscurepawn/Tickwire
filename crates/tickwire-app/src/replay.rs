//! Offline recovery on bounded private copies; original journals remain unchanged.
use crate::{Result, config::Deployment, invalid};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};
use tickwire_account::{Account, Fixed, RecoveryRecord};
use tickwire_account_codec::{CanonicalBaseline, CanonicalRecord};
use tickwire_account_gateway::ReactivationVerifier;
use tickwire_account_journal::{
    AccountJournal, FileBackend, HistoryFrontier, RecoveryVerifier, VerificationError,
};
use tickwire_binance_account::{AccountIngress, StoredDecision};
use tickwire_journal::store::FileStore;
use tickwire_recovery::{ClosureBinding, FileBlobStore, Persistence, SharedBlobStore};
use tickwire_venue::PositionSide;

const MAX_COPY_BYTES: u64 = 512 * 1024 * 1024;
const MAX_COPY_FILES: usize = 65536;
const MAX_REPORT_BYTES: usize = 16 * 1024 * 1024;
static TEMPORARY: AtomicU64 = AtomicU64::new(0);

/// Recover the complete file prefixes, verify every historical dependency and
/// return a frozen account report. No credentials, clock or network are opened.
pub fn run(deployment: &Deployment) -> Result<Value> {
    let recovered = load(deployment)?;
    report(&recovered)
}

/// Export exact canonical bytes in WAL order and the material needed to verify
/// them. The destination must be a new absolute directory; it is never replaced.
pub fn export(deployment: &Deployment, destination: &Path) -> Result<Value> {
    if !destination.is_absolute() {
        return Err(invalid("export directory must be absolute"));
    }
    let recovered = load(deployment)?;
    let mut summary = report(&recovered)?;
    private_directory(destination)?;
    let records_dir = destination.join("records");
    private_directory(&records_dir)?;
    write_new(
        &destination.join("baseline.cbor"),
        recovered.journal.baseline().bytes(),
    )?;
    write_new(
        &destination.join("history.bin"),
        recovered.journal.history_frontier().bytes(),
    )?;
    let mut index = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(destination.join("records.jsonl"))?;
    for record in recovered.journal.records() {
        let name = format!("{:020}.cbor", record.sequence());
        write_new(&records_dir.join(&name), record.bytes())?;
        let entry = json!({"sequence":record.sequence(),"kind":kind(record.record()),"file":format!("records/{name}"),"length":record.bytes().len(),"sha256":hex(&record.digest())});
        serde_json::to_writer(&mut index, &entry)?;
        index.write_all(b"\n")?;
    }
    index.sync_all()?;
    File::open(&records_dir)?.sync_all()?;
    let mut budget = CopyBudget::default();
    for name in ["account", "input", "recovery"] {
        copy_directory(
            &recovered.scratch.0.join(name),
            &destination.join(name),
            name,
            &mut budget,
        )?;
    }
    summary["export_directory"] = json!(destination);
    write_new(
        &destination.join("report.json"),
        &serde_json::to_vec_pretty(&summary)?,
    )?;
    // Published last: a partial export has no completion manifest.
    let manifest = json!({"format":"tickwire.offline-replay.v1","baseline":"baseline.cbor","baseline_sha256":hex(&recovered.journal.baseline().digest()),"records":"records.jsonl","record_count":recovered.journal.position(),"history":"history.bin","history_sha256":hex(&recovered.journal.history_frontier().digest()),"account_wal":"account","input_wal":"input","recovery_objects":"recovery","frozen":true});
    write_new(
        &destination.join("MANIFEST.json"),
        &serde_json::to_vec_pretty(&manifest)?,
    )?;
    File::open(destination)?.sync_all()?;
    if let Some(parent) = destination.parent() {
        File::open(parent)?.sync_all()?;
    }
    Ok(summary)
}

struct Recovered {
    journal: AccountJournal<FileBackend>,
    account: Account,
    input: AccountIngress<FileStore>,
    pending_inputs: usize,
    account_tail: u64,
    input_tail: u64,
    scratch: Scratch,
    _locks: ReplayLocks,
}
/// One offline owner of the original directories, including partial setup.
#[derive(Default)]
struct ReplayLocks(Vec<File>);
impl Drop for ReplayLocks {
    fn drop(&mut self) {
        for file in &self.0 {
            let _ = file.unlock();
        }
    }
}
impl ReplayLocks {
    fn acquire(&mut self, root: &Path) -> Result<()> {
        let path = root.join("LOCK");
        if !fs::symlink_metadata(&path)?.file_type().is_file() {
            return Err(invalid("replay lock must be a regular existing file"));
        }
        let file = File::open(path)?;
        file.try_lock()
            .map_err(|e| io::Error::other(e.to_string()))?;
        self.0.push(file);
        Ok(())
    }
}
struct Historical<'a> {
    persistence: Persistence<SharedBlobStore<FileBlobStore>>,
    input: &'a AccountIngress<FileStore>,
}
impl RecoveryVerifier for Historical<'_> {
    fn verify_income_association(
        &mut self,
        baseline: &CanonicalBaseline,
        record: &CanonicalRecord,
        preceding: &HistoryFrontier,
        prior: &[CanonicalRecord],
        writer_epoch: u64,
    ) -> std::result::Result<(), VerificationError> {
        let RecoveryRecord::IncomeObservationsAssociated(binding) = record.record() else {
            return Err(VerificationError::Invalid);
        };
        let account = Account::recover(
            baseline.snapshot().clone(),
            baseline.catalog().clone(),
            &prior.iter().map(|r| r.record().clone()).collect::<Vec<_>>(),
        )
        .map_err(|_| VerificationError::Invalid)?;
        self.input
            .activation_verifier()
            .verify_income_association(baseline, &account, preceding, prior, binding, writer_epoch)
            .map_err(|_| VerificationError::Invalid)
    }

    fn verify_uncreated_algo(
        &mut self,
        baseline: &CanonicalBaseline,
        record: &CanonicalRecord,
        preceding: &HistoryFrontier,
        prior: &[CanonicalRecord],
        writer_epoch: u64,
    ) -> std::result::Result<(), VerificationError> {
        let RecoveryRecord::UncreatedAlgoClosed(binding) = record.record() else {
            return Err(VerificationError::Invalid);
        };
        let account = Account::recover(
            baseline.snapshot().clone(),
            baseline.catalog().clone(),
            &prior.iter().map(|r| r.record().clone()).collect::<Vec<_>>(),
        )
        .map_err(|_| VerificationError::Invalid)?;
        self.input
            .activation_verifier()
            .verify_uncreated_algo(baseline, &account, preceding, binding, writer_epoch)
            .map_err(|_| VerificationError::Invalid)
    }

    fn verify_account_inputs_closure(
        &mut self,
        baseline: &CanonicalBaseline,
        record: &CanonicalRecord,
        preceding: &HistoryFrontier,
        prior: &[CanonicalRecord],
        writer_epoch: u64,
    ) -> std::result::Result<(), VerificationError> {
        let RecoveryRecord::AccountInputsReconciled(binding) = record.record() else {
            return Err(VerificationError::Invalid);
        };
        let account = Account::recover(
            baseline.snapshot().clone(),
            baseline.catalog().clone(),
            &prior.iter().map(|r| r.record().clone()).collect::<Vec<_>>(),
        )
        .map_err(|_| VerificationError::Invalid)?;
        self.input
            .activation_verifier()
            .verify_account_inputs_closure(baseline, &account, preceding, binding, writer_epoch)
            .map_err(|_| VerificationError::Invalid)
    }

    fn verify_income_baseline(
        &mut self,
        baseline: &CanonicalBaseline,
        record: &CanonicalRecord,
        preceding: &HistoryFrontier,
        prior: &[CanonicalRecord],
    ) -> std::result::Result<(), VerificationError> {
        let RecoveryRecord::IncomeBaseline(binding) = record.record() else {
            return Err(VerificationError::Invalid);
        };
        let account = Account::recover(
            baseline.snapshot().clone(),
            baseline.catalog().clone(),
            &prior.iter().map(|r| r.record().clone()).collect::<Vec<_>>(),
        )
        .map_err(|_| VerificationError::Invalid)?;
        self.input
            .activation_verifier()
            .verify_income_baseline(baseline, &account, preceding, binding)
            .map_err(|_| VerificationError::Invalid)
    }

    fn verify_handover(
        &mut self,
        baseline: &CanonicalBaseline,
        record: &CanonicalRecord,
        prior: &[CanonicalRecord],
    ) -> std::result::Result<(), VerificationError> {
        tickwire_runtime::authority::verify_handover_objects(
            &self.persistence,
            baseline,
            record,
            prior,
        )
        .map_err(|_| VerificationError::Invalid)
    }
    fn verify_admission(
        &mut self,
        baseline: &CanonicalBaseline,
        record: &CanonicalRecord,
        preceding: &HistoryFrontier,
    ) -> std::result::Result<(), VerificationError> {
        let RecoveryRecord::Admit { command, .. } = record.record() else {
            return Err(VerificationError::Invalid);
        };
        if record.baseline() != baseline.digest()
            || preceding.baseline_digest() != baseline.digest()
            || preceding.scope() != &baseline.snapshot().scope
            || preceding.record_count().checked_add(1) != Some(record.sequence())
        {
            return Err(VerificationError::Invalid);
        }
        let r = &command.command().context.request;
        let binding = ClosureBinding {
            risk_domain_id: r.risk_domain_id,
            intent_id: r.intent_id,
            intent_exec_epoch: r.intent_exec_epoch,
            business_digest: record.digest(),
            strategy_generation: r.strategy_generation,
            config_revision: r.config_revision,
            recovery_object: r.recovery_object,
        };
        let verified = self
            .persistence
            .verify(&binding)
            .map_err(|_| VerificationError::Invalid)?;
        if verified.binding() != &binding
            || verified.command_digest() != Some(command.digest())
            || verified.account_baseline_digest() != Some(baseline.digest())
            || verified.account_history_digest() != Some(preceding.digest())
        {
            return Err(VerificationError::Invalid);
        }
        Ok(())
    }
    fn verify_reactivation(
        &mut self,
        baseline: &CanonicalBaseline,
        record: &CanonicalRecord,
        preceding: &HistoryFrontier,
        prior: &[CanonicalRecord],
    ) -> std::result::Result<(), VerificationError> {
        let RecoveryRecord::Reactivated(binding) = record.record() else {
            return Err(VerificationError::Invalid);
        };
        let inputs: Vec<_> = prior.iter().map(|r| r.record().clone()).collect();
        let account = Account::recover(
            baseline.snapshot().clone(),
            baseline.catalog().clone(),
            &inputs,
        )
        .map_err(|_| VerificationError::Invalid)?;
        self.input
            .activation_verifier()
            .verify(baseline, &account, preceding, binding)
            .map_err(|_| VerificationError::Invalid)
    }
}
fn load(deployment: &Deployment) -> Result<Recovered> {
    let configuration = deployment.assemble(deployment.baseline()?)?;
    let roots = [
        &deployment.directories.account,
        &deployment.directories.input,
        &deployment.directories.recovery,
    ];
    let canonical: Vec<_> = roots
        .iter()
        .map(|p| checked_directory(p))
        .collect::<Result<_>>()?;
    if canonical[0] == canonical[1] || canonical[0] == canonical[2] || canonical[1] == canonical[2]
    {
        return Err(invalid("replay directories overlap"));
    }
    let mut locks = ReplayLocks::default();
    for root in &canonical {
        locks.acquire(root)?;
    }
    let scratch = Scratch::new()?;
    let mut budget = CopyBudget::default();
    for (root, name) in canonical.iter().zip(["account", "input", "recovery"]) {
        copy_directory(root, &scratch.0.join(name), name, &mut budget)?;
    }
    let before_input = wal_bytes(&scratch.0.join("input"))?;
    let shared = SharedBlobStore::new(FileBlobStore::open(scratch.0.join("recovery"))?);
    let input = AccountIngress::recover(
        FileStore::open(scratch.0.join("input"))?,
        configuration.input,
        None,
    )?
    .with_closure_store(shared.clone());
    let input_tail = before_input
        .checked_sub(wal_bytes(&scratch.0.join("input"))?)
        .ok_or_else(|| invalid("input recovery grew a journal"))?;
    let mut verifier = Historical {
        persistence: Persistence::new(shared),
        input: &input,
    };
    let recovered = AccountJournal::recover(
        FileBackend::open(scratch.0.join("account"))?,
        configuration.account.journal,
        configuration.account.writer_epoch,
        None,
        &mut verifier,
    )?;
    drop(verifier);
    if !recovered.account.frozen() {
        return Err(invalid("offline recovery must remain frozen"));
    }
    let pending_inputs = pair_input(&input, &recovered.journal)?;
    Ok(Recovered {
        journal: recovered.journal,
        account: recovered.account,
        input,
        pending_inputs,
        account_tail: recovered.discarded_tail_bytes,
        input_tail,
        scratch,
        _locks: locks,
    })
}
fn pair_input(
    input: &AccountIngress<FileStore>,
    journal: &AccountJournal<FileBackend>,
) -> Result<usize> {
    input.validate_ws_origins(journal)?;
    let mut events = BTreeMap::new();
    for response in input.stored_ws_responses() {
        for (index, event) in response.events.iter().enumerate() {
            events.insert(response.input.sequence + index as u64 + 1, event);
        }
    }
    let account_inputs: BTreeMap<_, _> = input
        .account_inputs()
        .map(|(key, e)| (key.sequence, e))
        .collect();
    for record in input.stored_events() {
        if let StoredDecision::Bound(event) = record.decision {
            events.insert(record.input.sequence, event);
        }
    }
    for record in input.stored_rest() {
        if !journal.records().iter().any(|r|matches!(r.record(),RecoveryRecord::Admit{command,..} if command.as_ref()==record.command)){return Err(invalid("input response has no historical account admission"));}
        for (index, event) in record.events.iter().enumerate() {
            events.insert(record.input.sequence + index as u64 + 1, event);
        }
    }
    for (stream, sequence, event) in journal.ingress_events() {
        if stream == input.input_stream() && events.get(&sequence).copied() != Some(event) {
            return Err(invalid(
                "account WAL references missing or conflicting input",
            ));
        }
    }
    for (stream, sequence, event) in journal.ingress_account_inputs() {
        if stream == input.input_stream() && account_inputs.get(&sequence).copied() != Some(event) {
            return Err(invalid(
                "account WAL references missing or conflicting account input",
            ));
        }
    }
    let pending = account_inputs.into_iter().filter(|(sequence,input_event)| {
        !matches!(journal.ingress_record(input.input_stream(),*sequence),Some(RecoveryRecord::IngressAccountInput{input,..}) if input.as_ref()==*input_event)
    }).count();
    let mut pending_imports = 0;
    for binding in input.income_import_bindings() {
        let binding = binding?;
        if !journal.records().iter().any(|r| {
            matches!(r.record(), RecoveryRecord::IncomeBaseline(applied) if applied.as_ref() == &binding)
        }) {
            pending_imports += 1;
        }
    }
    for binding in input.income_association_bindings() {
        let binding = binding?;
        if !journal.records().iter().any(|r| matches!(r.record(),RecoveryRecord::IncomeObservationsAssociated(applied) if applied.as_ref()==&binding)) {pending_imports+=1;}
    }
    Ok(events
        .into_iter()
        .filter(|(sequence, event)| {
            journal.ingress_event(input.input_stream(), *sequence) != Some(*event)
        })
        .count()
        + pending
        + pending_imports)
}
fn report(recovered: &Recovered) -> Result<Value> {
    let baseline = recovered.journal.baseline();
    let account = &recovered.account;
    let mut assets = Vec::new();
    for asset in &baseline.snapshot().assets {
        let value = account
            .asset(asset.key)
            .map_err(|e| invalid(&format!("asset state: {e:?}")))?;
        assets.push(json!({"wallet_id":asset.key.wallet_id,"asset_id":asset.key.asset_id,"symbol":asset.asset.symbol.as_str(),"total":fixed(value.total),"reserved":fixed(value.reserved),"available":fixed(value.available),"deficit":fixed(value.deficit),"ownership":value.ownership.iter().map(|(strategy,amount)|json!({"strategy_id":strategy,"quantity":fixed(*amount)})).collect::<Vec<_>>()}));
    }
    let mut positions = Vec::new();
    for instrument in &baseline.catalog().entries {
        for (side, name) in [
            (PositionSide::Both, "both"),
            (PositionSide::Long, "long"),
            (PositionSide::Short, "short"),
        ] {
            let (quantity, owners) = account
                .position(instrument.instrument.instrument_id, side)
                .map_err(|e| invalid(&format!("position state: {e:?}")))?;
            if quantity.atoms() != 0 || !owners.is_empty() {
                positions.push(json!({"instrument_id":instrument.instrument.instrument_id,"symbol":instrument.instrument.symbol.as_str(),"side":name,"quantity":fixed(quantity),"entry_bound":account.position_entry_bound(instrument.instrument.instrument_id,side).map_err(|e|invalid(&format!("position entry: {e:?}")))?.map(fixed),"ownership":owners.iter().map(|(strategy,amount)|json!({"strategy_id":strategy,"quantity":fixed(*amount)})).collect::<Vec<_>>()}));
            }
        }
    }
    let mut orders = Vec::new();
    let mut pending = Vec::new();
    for key in account.order_keys() {
        let o = account
            .order(key)
            .ok_or_else(|| invalid("missing account order"))?;
        let item = json!({"intent_epoch":key.intent.epoch,"intent_id":key.intent.intent_id,"client_id":key.client_id.as_str(),"command_sha256":hex(&o.command_digest),"strategy_id":o.strategy,"origin_instance":hex(&o.origin.instance),"origin_generation":o.origin.generation,"manager_instance":hex(&o.manager.instance),"manager_generation":o.manager.generation,"owner_revision":o.owner_revision,"original_quantity":fixed(o.original_quantity),"filled_quantity":fixed(o.filled_quantity),"remaining_quantity":fixed(o.remaining_quantity),"reduce_allocation":fixed(o.reduce_allocation),"unknown":o.unknown,"terminal":o.terminal,"fee_reconciled":o.fee_reconciled,"aborted":o.aborted,"uncreated":o.uncreated,"confirmed":o.confirmed});
        if !o.aborted && !o.uncreated && (!o.terminal || !o.fee_reconciled || o.unknown) {
            pending.push(json!({"intent_epoch":key.intent.epoch,"intent_id":key.intent.intent_id,"client_id":key.client_id.as_str(),"unknown":o.unknown,"open":!o.terminal,"fees_pending":!o.fee_reconciled}));
        }
        orders.push(item);
    }
    let scope = &baseline.snapshot().scope;
    let report = json!({"format":"tickwire.offline-replay.v1","frozen":account.frozen(),"scope":{"risk_domain_id":hex(&scope.risk_domain_id),"venue":scope.venue,"account":scope.account,"environment":format!("{:?}",scope.environment)},"account_epoch":account.account_epoch(),"ledger_sequence":account.ledger_sequence(),"baseline_sha256":hex(&baseline.digest()),"account_stream":hex(&recovered.journal.config().physical.stream_epoch),"account_records":recovered.journal.position(),"history_sha256":hex(&recovered.journal.history_frontier().digest()),"input_stream":hex(&recovered.input.input_stream()),"input_records":recovered.input.position(),"input_facts_not_in_account":recovered.pending_inputs,"input_quarantines_and_gaps":recovered.input.quarantined(),"discarded_tail_bytes":{"account":recovered.account_tail,"input":recovered.input_tail},"required_frontier":Value::Null,"assets":assets,"positions":positions,"orders":orders,"pending_orders":pending,"rejected_event_sha256":account.rejected_event().map(|e|hex(&e.digest()))});
    if serde_json::to_vec(&report)?.len() > MAX_REPORT_BYTES {
        return Err(invalid("account report exceeds byte limit"));
    }
    Ok(report)
}
fn fixed(value: Fixed) -> Value {
    json!({"atoms":value.atoms().to_string(),"scale":value.scale()})
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
fn kind(record: &RecoveryRecord) -> &'static str {
    match record {
        RecoveryRecord::Admit { .. } => "admit",
        RecoveryRecord::VenueEvent(_) => "venue_event",
        RecoveryRecord::Aborted { .. } => "aborted",
        RecoveryRecord::FeesReconciled { .. } => "fees_reconciled",
        RecoveryRecord::UnattributedCashFlow { .. } => "unattributed_cashflow",
        RecoveryRecord::ExternalPosition { .. } => "external_position",
        RecoveryRecord::TransferAsset { .. } => "transfer_asset",
        RecoveryRecord::TransferPosition { .. } => "transfer_position",
        RecoveryRecord::IngressEvent { .. } => "ingress_event",
        RecoveryRecord::Reactivated(_) => "reactivated",
        RecoveryRecord::StrategyHandover(_) => "strategy_handover",
        RecoveryRecord::IngressAccountInput { .. } => "ingress_account_input",
        RecoveryRecord::IncomeObservationsAssociated(_) => "income_observations_associated",
        RecoveryRecord::UncreatedAlgoClosed(_) => "uncreated_algo_closed",
        RecoveryRecord::AccountInputsReconciled(_) => "account_inputs_reconciled",
        RecoveryRecord::IncomeBaseline(_) => "income_baseline",
    }
}
#[derive(Default)]
struct CopyBudget {
    bytes: u64,
    files: usize,
}
impl CopyBudget {
    fn reserve(&mut self, size: u64) -> Result<()> {
        let bytes = self
            .bytes
            .checked_add(size)
            .ok_or_else(|| invalid("replay byte overflow"))?;
        let files = self
            .files
            .checked_add(1)
            .ok_or_else(|| invalid("replay file overflow"))?;
        if bytes > MAX_COPY_BYTES || files > MAX_COPY_FILES {
            return Err(invalid("offline replay capacity"));
        }
        self.bytes = bytes;
        self.files = files;
        Ok(())
    }
}
fn checked_directory(path: &Path) -> Result<PathBuf> {
    if !path.is_absolute() || !fs::symlink_metadata(path)?.file_type().is_dir() {
        return Err(invalid("existing private replay directory required"));
    }
    Ok(fs::canonicalize(path)?)
}
fn accepted_file(name: &str, kind: &str) -> Result<bool> {
    if name == "LOCK" {
        return Ok(false);
    }
    if kind == "recovery" {
        let hash = ["object-", "manifest-", "binding-"]
            .into_iter()
            .find_map(|p| name.strip_prefix(p));
        return match hash {
            Some(hash)
                if hash.len() == 64
                    && hash
                        .bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)) =>
            {
                Ok(true)
            }
            Some(_) => Err(invalid("invalid recovery object filename")),
            None => Ok(false),
        };
    }
    if name == "MANIFEST.json"
        || kind == "account" && matches!(name, "ACCOUNT-BASELINE.cbor" | "ACCOUNT-BOOTSTRAP.json")
    {
        return Ok(true);
    }
    if name.starts_with("segment-") || name.ends_with(".wal") {
        let valid = name
            .strip_prefix("segment-")
            .and_then(|s| s.strip_suffix(".wal"))
            .is_some_and(|n| {
                n.len() == 20
                    && n.bytes().all(|b| b.is_ascii_digit())
                    && n.parse::<u64>().is_ok_and(|n| n > 0)
            });
        return if valid {
            Ok(true)
        } else {
            Err(invalid("invalid WAL filename"))
        };
    }
    Ok(false)
}
fn copy_directory(source: &Path, target: &Path, kind: &str, budget: &mut CopyBudget) -> Result<()> {
    private_directory(target)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name
            .to_str()
            .ok_or_else(|| invalid("non-UTF8 replay filename"))?;
        if !accepted_file(name, kind)? {
            continue;
        }
        let meta = fs::symlink_metadata(entry.path())?;
        if !meta.file_type().is_file() {
            return Err(invalid("replay input is not a regular file"));
        }
        budget.reserve(meta.len())?;
        let copied = fs::copy(entry.path(), target.join(name))?;
        if copied != meta.len() {
            return Err(invalid("replay input changed while locked"));
        }
        File::open(target.join(name))?.sync_all()?;
    }
    File::open(target)?.sync_all()?;
    Ok(())
}
fn private_directory(path: &Path) -> Result<()> {
    let mut b = fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        b.mode(0o700);
    }
    b.create(path)?;
    Ok(())
}
fn write_new(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut o = OpenOptions::new();
    o.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        o.mode(0o600);
    }
    let mut f = o.open(path)?;
    f.write_all(bytes)?;
    f.sync_all()?;
    Ok(())
}
fn wal_bytes(root: &Path) -> Result<u64> {
    let mut total = 0u64;
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        if entry.file_name().to_string_lossy().ends_with(".wal") {
            total = total
                .checked_add(entry.metadata()?.len())
                .ok_or_else(|| invalid("WAL length overflow"))?;
        }
    }
    Ok(total)
}
struct Scratch(PathBuf);
impl Scratch {
    fn new() -> Result<Self> {
        for _ in 0..64 {
            let n = TEMPORARY.fetch_add(1, Ordering::Relaxed);
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_nanos();
            let path = std::env::temp_dir()
                .join(format!("tickwire-offline-{}-{now}-{n}", std::process::id()));
            match private_directory(&path) {
                Ok(()) => return Ok(Self(path)),
                Err(e)
                    if e.downcast_ref::<io::Error>()
                        .is_some_and(|e| e.kind() == io::ErrorKind::AlreadyExists) => {}
                Err(e) => return Err(e),
            }
        }
        Err(invalid("temporary replay directory exhausted"))
    }
}
impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[cfg(test)]
#[path = "replay_income_tests.rs"]
pub(crate) mod income_tests;

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn report_preserves_i128_exactly() {
        let amount = Fixed::from_atoms(i128::MAX, 8).unwrap();
        assert_eq!(fixed(amount)["atoms"], i128::MAX.to_string());
        assert_eq!(fixed(amount)["scale"], 8);
    }
    #[test]
    fn export_source_names_and_capacity_are_bounded() {
        assert!(accepted_file("segment-00000000000000000001.wal", "account").unwrap());
        assert!(accepted_file("segment-1.wal", "input").is_err());
        assert!(accepted_file("../segment-00000000000000000001.wal", "input").is_err());
        assert!(!accepted_file("LOCK", "recovery").unwrap());
        assert!(accepted_file(&format!("object-{}", "ab".repeat(32)), "recovery").unwrap());
        let mut budget = CopyBudget::default();
        budget.reserve(MAX_COPY_BYTES).unwrap();
        assert!(budget.reserve(1).is_err());
        assert_eq!(budget.bytes, MAX_COPY_BYTES);
    }
}

#[cfg(all(test, target_os = "linux"))]
mod lock_tests {
    use super::*;
    #[test]
    fn replay_owner_releases_copied_locks_on_success_and_partial_setup_error() {
        for fail in [false, true] {
            let root = Scratch::new().unwrap();
            fs::write(root.0.join("LOCK"), b"").unwrap();
            let mut inherited = None;
            let contender = File::open(root.0.join("LOCK")).unwrap();
            let acquire = (|| -> Result<ReplayLocks> {
                let mut locks = ReplayLocks::default();
                locks.acquire(&root.0)?;
                inherited = Some(locks.0[0].try_clone()?);
                assert!(
                    contender.try_lock().is_err(),
                    "live replay remains exclusive"
                );
                if fail {
                    locks.acquire(&root.0.join("missing-root"))?;
                }
                Ok(locks)
            })();
            assert_eq!(acquire.is_err(), fail);
            drop(acquire);
            contender
                .try_lock()
                .expect("finished replay cannot be extended by an inherited copy");
            drop(inherited);
            let third = File::open(root.0.join("LOCK")).unwrap();
            assert!(
                third.try_lock().is_err(),
                "old-copy close must not release successor"
            );
            contender.unlock().unwrap();
            third.try_lock().unwrap();
            third.unlock().unwrap();
        }
    }
}
