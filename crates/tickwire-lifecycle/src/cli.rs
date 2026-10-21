//! Durable ordered-directory transport for the independent coordinator process.
use crate::*;
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};
use tickwire_clock::ClockDomain;
use tickwire_clock_sys::SystemClock;
use tickwire_journal::store::FileStore;

pub type CliResult<T> = std::result::Result<T, Box<dyn std::error::Error>>;
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Deployment {
    pub coordinator: CoordinatorConfig,
    pub writer_epoch: u64,
    pub recover: bool,
    /// Explicitly resume only after all current QueryApplied replies arrive.
    pub resume_after_queries: bool,
    pub signer_file: PathBuf,
    pub audit_directory: PathBuf,
    pub receipts_directory: PathBuf,
    /// Each member consumes requests_directory/<risk-domain-hex>/ in name order.
    pub requests_directory: PathBuf,
    /// Trusted operator updates; each file binds one exact coordinator version.
    pub readiness_directory: Option<PathBuf>,
    pub maximum_wait_ns: u64,
    pub poll_interval_ns: u64,
}
pub fn run(path: &Path, halt: bool) -> CliResult<()> {
    let d: Deployment = serde_json::from_slice(&read(path, 1024 * 1024, false)?)?;
    if d.maximum_wait_ns == 0 || !(1_000_000..=1_000_000_000).contains(&d.poll_interval_ns) {
        return Err(Error::Invalid.into());
    }
    for p in [
        &d.audit_directory,
        &d.receipts_directory,
        &d.requests_directory,
    ] {
        private_directory(p)?;
    }
    if let Some(path) = &d.readiness_directory {
        private_directory(path)?;
    }
    if d.audit_directory == d.receipts_directory
        || d.audit_directory == d.requests_directory
        || d.receipts_directory == d.requests_directory
    {
        return Err(Error::Identity.into());
    }
    let signer = Signer::from_pkcs8(&read(&d.signer_file, 16384, true)?)?;
    let domain = ClockDomain::new(
        d.coordinator.operation.clock_boot_id,
        d.coordinator.operation.clock_generation,
    )
    .map_err(|_| Error::Identity)?;
    let mut clock = SystemClock::new(domain)?;
    let now = clock.sample()?.monotonic;
    let store = FileStore::open(&d.audit_directory)?;
    let mut owner = if d.recover {
        Coordinator::recover(store, d.coordinator, d.writer_epoch, signer, None, now)?
    } else {
        Coordinator::create(store, d.coordinator, d.writer_epoch, signer, now)?
    };
    if halt {
        owner.halt(owner.version(), now)?;
    }
    let stop = Instant::now()
        .checked_add(Duration::from_nanos(d.maximum_wait_ns))
        .ok_or(Error::Capacity)?;
    let mut seen = BTreeMap::<PathBuf, Hash>::new();
    let mut readiness_seen = BTreeMap::<PathBuf, Hash>::new();
    loop {
        let now = clock.sample()?.monotonic;
        if now.nanos() >= owner.config().operation.valid_until_ns
            && !matches!(owner.stage(), Stage::Halting | Stage::Halted)
        {
            owner.halt(owner.version(), now)?;
        }
        publish(&owner, &d.requests_directory)?;
        ingest_directory(&mut owner, &d.receipts_directory, &mut seen, &mut clock)?;
        if let Some(path) = &d.readiness_directory {
            ingest_readiness(&mut owner, path, &mut readiness_seen, &mut clock)?;
        }
        let now = clock.sample()?.monotonic;
        if !halt && now.nanos() < owner.config().operation.valid_until_ns {
            match owner.stage() {
                Stage::AwaitingResume if d.resume_after_queries => {
                    owner.resume_after_queries(owner.version(), now)?;
                }
                Stage::PreparedAll => {
                    owner.begin_commit(owner.version(), now)?;
                }
                Stage::CommittedAll => {
                    owner.release(owner.version(), now)?;
                }
                _ => {}
            }
        }
        publish(&owner, &d.requests_directory)?;
        if matches!(owner.stage(), Stage::ActiveAll | Stage::Halted) {
            print_status(&owner);
            return Ok(());
        }
        if Instant::now() >= stop {
            if !matches!(owner.stage(), Stage::Halting | Stage::Halted) {
                owner.halt(owner.version(), clock.sample()?.monotonic)?;
                publish(&owner, &d.requests_directory)?;
            }
            print_status(&owner);
            return Err("coordinator wait ended before all applied receipts arrived".into());
        }
        std::thread::sleep(Duration::from_nanos(d.poll_interval_ns));
    }
}
fn publish(owner: &Coordinator<FileStore>, root: &Path) -> CliResult<()> {
    for request in owner.pending_requests() {
        let body: ControlRequest = serde_json::from_slice(&request.payload)?;
        let directory = root.join(hex(&body.member.risk_domain));
        private_directory(&directory)?;
        let path = directory.join(format!(
            "{:020}-{}.json",
            owner.pending_version(),
            hex(&body.request_id)
        ));
        let bytes = serde_json::to_vec(request)?;
        if bytes.len() > MAX_SIGNED_BYTES * 5 {
            return Err(Error::Capacity.into());
        }
        publish_new(&path, &bytes)?;
    }
    Ok(())
}
fn ingest_directory(
    owner: &mut Coordinator<FileStore>,
    root: &Path,
    seen: &mut BTreeMap<PathBuf, Hash>,
    clock: &mut SystemClock,
) -> CliResult<()> {
    let mut paths = Vec::new();
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        if entry.path().extension().is_some_and(|s| s == "json") {
            if paths.len() >= 4096 {
                return Err(Error::Capacity.into());
            }
            paths.push(entry.path());
        }
    }
    paths.sort();
    for path in paths {
        let bytes = read(&path, MAX_SIGNED_BYTES * 5, false)?;
        let hash: Hash = Sha256::digest(&bytes).into();
        if let Some(old) = seen.get(&path) {
            if old != &hash {
                return Err(Error::Conflict.into());
            }
            continue;
        }
        let signed: SignedReceipt = match serde_json::from_slice(&bytes) {
            Ok(signed) => signed,
            Err(_) => {
                eprintln!("non-applied or malformed receipt ignored");
                seen.insert(path, hash);
                continue;
            }
        };
        let time = clock.sample()?.monotonic;
        let result = if serde_json::from_slice::<ReplayPrepared>(&signed.payload).is_ok() {
            owner.ingest_replay(
                SignedReplayPrepared {
                    payload: signed.payload,
                    signature: signed.signature,
                },
                owner.version(),
                time,
            )
        } else if serde_json::from_slice::<ReplayObserved>(&signed.payload).is_ok() {
            owner.ingest_replay_observed(
                SignedReplayObserved {
                    payload: signed.payload,
                    signature: signed.signature,
                },
                owner.version(),
                time,
            )
        } else if serde_json::from_slice::<RebuildPrepared>(&signed.payload).is_ok() {
            owner.ingest_rebuild(
                SignedRebuildPrepared {
                    payload: signed.payload,
                    signature: signed.signature,
                },
                owner.version(),
                time,
            )
        } else if serde_json::from_slice::<RebuildObserved>(&signed.payload).is_ok() {
            owner.ingest_rebuild_observed(
                SignedRebuildObserved {
                    payload: signed.payload,
                    signature: signed.signature,
                },
                owner.version(),
                time,
            )
        } else {
            owner.ingest(signed, owner.version(), time)
        };
        match result {
            Ok(_) => {}
            Err(Error::Store | Error::Capacity) => return Err(result.unwrap_err().into()),
            Err(e) => eprintln!("receipt rejected: {e}"),
        }
        seen.insert(path, hash);
    }
    Ok(())
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadinessUpdate {
    pub operation_id: [u8; 16],
    pub operation_revision: u64,
    pub expected_version: u64,
    pub member: Member,
    #[serde(default)]
    pub proof: Option<Readiness>,
    #[serde(default)]
    pub refresh_replay: bool,
    #[serde(default)]
    pub refresh_rebuild: bool,
}
fn ingest_readiness(
    owner: &mut Coordinator<FileStore>,
    root: &Path,
    seen: &mut BTreeMap<PathBuf, Hash>,
    clock: &mut SystemClock,
) -> CliResult<()> {
    let mut paths = vec![];
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        if entry.path().extension().is_some_and(|e| e == "json") {
            if paths.len() >= 4096 {
                return Err(Error::Capacity.into());
            }
            paths.push(entry.path());
        }
    }
    paths.sort();
    for path in paths {
        let bytes = read(&path, 65536, false)?;
        let hash: Hash = Sha256::digest(&bytes).into();
        if let Some(prior) = seen.get(&path) {
            if *prior != hash {
                return Err(Error::Conflict.into());
            }
            continue;
        }
        let update: ReadinessUpdate = serde_json::from_slice(&bytes)?;
        if update.operation_id != owner.config().operation.operation_id
            || update.operation_revision != owner.config().operation.operation_revision
        {
            return Err(Error::Identity.into());
        }
        if update.expected_version > owner.version() {
            continue;
        }
        if update.expected_version == owner.version() {
            let time = clock.sample()?.monotonic;
            let result = match (update.refresh_replay, update.refresh_rebuild, update.proof) {
                (false, true, None) => {
                    owner.refresh_rebuild(&update.member, update.expected_version, time)
                }
                (true, false, None) => {
                    owner.refresh_replay(&update.member, update.expected_version, time)
                }
                (false, false, Some(proof)) => {
                    owner.update_readiness(&update.member, proof, update.expected_version, time)
                }
                _ => Err(Error::Invalid),
            };
            match result {
                Ok(_) => {}
                Err(Error::Store | Error::Capacity) => return Err(Error::Store.into()),
                Err(e) => eprintln!("readiness update rejected: {e}"),
            }
        }
        seen.insert(path, hash);
    }
    Ok(())
}
fn print_status(owner: &Coordinator<FileStore>) {
    println!(
        "{}",
        serde_json::json!({"operation_id":hex(&owner.config().operation.operation_id),
        "version":owner.version(),"stage":owner.stage(),"audit_position":owner.audit_position(),
        "pending":owner.pending_requests().len()})
    );
}
fn private_directory(path: &Path) -> CliResult<()> {
    if !path.is_absolute() {
        return Err(Error::Invalid.into());
    }
    let mut builder = fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    match builder.create(path) {
        Ok(()) => {
            File::open(path)?.sync_all()?;
            File::open(path.parent().ok_or(Error::Invalid)?)?.sync_all()?;
        }
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(e.into()),
    }
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_dir() {
        return Err(Error::Identity.into());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(Error::Identity.into());
        }
    }
    Ok(())
}
fn publish_new(path: &Path, bytes: &[u8]) -> CliResult<()> {
    if path.try_exists()? {
        if read(path, MAX_SIGNED_BYTES * 5, false)? != bytes {
            return Err(Error::Conflict.into());
        }
        return Ok(());
    }
    let root = path.parent().ok_or(Error::Invalid)?;
    let tmp = root.join(format!(
        ".{}.{}.tmp",
        std::process::id(),
        hex(&Sha256::digest(bytes))
    ));
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&tmp)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    match fs::hard_link(&tmp, path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            if read(path, MAX_SIGNED_BYTES * 5, false)? != bytes {
                return Err(Error::Conflict.into());
            }
        }
        Err(e) => return Err(e.into()),
    }
    fs::remove_file(tmp)?;
    File::open(root)?.sync_all()?;
    Ok(())
}
fn read(path: &Path, limit: usize, secret: bool) -> CliResult<Vec<u8>> {
    if !path.is_absolute() {
        return Err(Error::Invalid.into());
    }
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_file() || metadata.len() > limit as u64 {
        return Err(Error::Capacity.into());
    }
    #[cfg(unix)]
    if secret {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(Error::Identity.into());
        }
    }
    let mut bytes = Vec::new();
    File::open(path)?
        .take(limit as u64 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.is_empty() || bytes.len() > limit {
        return Err(Error::Capacity.into());
    }
    Ok(bytes)
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|v| format!("{v:02x}")).collect()
}
