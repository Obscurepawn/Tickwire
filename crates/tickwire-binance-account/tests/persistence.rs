#[path = "../../tickwire-account/tests/common/mod.rs"]
mod common;
use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
use tickwire_account::IntentKey;
use tickwire_account_codec::CanonicalBaseline;
use tickwire_binance_account::{AccountIngress, Config};
use tickwire_journal::{self as journal, store::FileStore};

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "tickwire-ingress-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn fixture() -> (Config, tickwire_venue::CanonicalCommand) {
    let (snapshot, catalog, command) = common::fixture();
    (
        Config {
            physical: journal::Config::new([81; 16], [1; 16]),
            writer_epoch: 4,
            baseline: CanonicalBaseline::new(snapshot, catalog).unwrap(),
            account_stream: [2; 16],
            max_records: 32,
            max_registrations: 8,
            max_raw_bytes: 32_768,
            max_retained_bytes: 1 << 20,
        },
        common::canonical(command),
    )
}
fn key() -> IntentKey {
    IntentKey {
        epoch: 4,
        intent_id: 3,
    }
}

#[test]
fn file_reopen_preserves_registered_command_and_sequence() {
    let directory = Directory::new();
    let (config, command) = fixture();
    let mut input =
        AccountIngress::create(FileStore::open(&directory.0).unwrap(), config.clone()).unwrap();
    assert_eq!(input.position(), 1);
    input.register_command(command.clone(), &[[7; 16]]).unwrap();
    assert_eq!(input.position(), 2);
    drop(input);
    let input =
        AccountIngress::recover(FileStore::open(&directory.0).unwrap(), config, None).unwrap();
    assert_eq!(input.position(), 2);
    assert_eq!(input.registered(key()).unwrap().bytes(), command.bytes());
}
#[test]
fn exact_registration_retry_does_not_append_but_changed_id_conflicts() {
    let directory = Directory::new();
    let (config, command) = fixture();
    let mut input = AccountIngress::create(FileStore::open(&directory.0).unwrap(), config).unwrap();
    input.register_command(command.clone(), &[[7; 16]]).unwrap();
    input.register_command(command.clone(), &[[7; 16]]).unwrap();
    assert_eq!(input.position(), 2);
    assert!(input.register_command(command, &[[8; 16]]).is_err());
    assert_eq!(input.position(), 2);
}
#[test]
fn different_baseline_cannot_reopen_the_input_log() {
    let directory = Directory::new();
    let (mut config, _) = fixture();
    drop(AccountIngress::create(FileStore::open(&directory.0).unwrap(), config.clone()).unwrap());
    let mut snapshot = config.baseline.snapshot().clone();
    snapshot.account_epoch += 1;
    config.baseline = CanonicalBaseline::new(snapshot, config.baseline.catalog().clone()).unwrap();
    assert!(AccountIngress::recover(FileStore::open(&directory.0).unwrap(), config, None).is_err());
}

#[test]
fn registration_epoch_must_match_its_physical_writer() {
    let directory = Directory::new();
    let (mut config, command) = fixture();
    drop(AccountIngress::create(FileStore::open(&directory.0).unwrap(), config.clone()).unwrap());
    let mut changed = command.command().clone();
    changed.context.request.intent_exec_epoch = 5;
    let changed = common::canonical(changed);
    let mut payload = b"TWAI0001".to_vec();
    payload.push(2);
    payload.extend((changed.bytes().len() as u32).to_le_bytes());
    payload.extend(changed.bytes());
    payload.push(1);
    payload.extend([7; 16]);
    let (mut physical, _) =
        journal::Journal::recover(FileStore::open(&directory.0).unwrap(), None).unwrap();
    physical.append_batch(4, &[&payload]).unwrap();
    drop(physical);
    config.writer_epoch = 5;
    assert!(matches!(
        AccountIngress::recover(FileStore::open(&directory.0).unwrap(), config, None),
        Err(tickwire_binance_account::Error::Identity)
    ));
}
