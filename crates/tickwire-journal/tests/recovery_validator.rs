use std::{fs, io::Write};
use tickwire_journal::{Config, Error, Journal, store::FileStore};

#[test]
fn rejected_application_prefix_preserves_torn_tail_and_acceptance_recovers_it() {
    let root = std::env::temp_dir().join(format!(
        "tickwire-recovery-validator-{}",
        std::process::id()
    ));
    let store = FileStore::open(&root).unwrap();
    let path = store.segment_path(1);
    let cfg = Config::new([3; 16], [4; 16]);
    let mut journal = Journal::create(store, cfg.clone()).unwrap();
    let end = journal.append_batch(2, &[b"application header"]).unwrap();
    drop(journal);
    let mut file = fs::OpenOptions::new().append(true).open(&path).unwrap();
    file.write_all(&tickwire_journal::frame::MAGIC[..3])
        .unwrap();
    file.sync_all().unwrap();
    drop(file);
    let original = fs::read(&path).unwrap();
    let mut called = false;
    let rejected = Journal::recover_checked(
        FileStore::open(&root).unwrap(),
        Some(end.frontier()),
        |config, records| {
            called = true;
            assert_eq!(config, &cfg);
            assert_eq!(records.records.len(), 1);
            assert_eq!(records.discarded_tail_bytes, 3);
            assert_eq!(records.observed_exec_epoch(), 2);
            Err(Error::Quarantined("application rejected"))
        },
    );
    assert!(called, "validator was not invoked");
    assert!(rejected.is_err());
    assert_eq!(fs::read(&path).unwrap(), original);
    let (mut recovered, records) = Journal::recover_checked(
        FileStore::open(&root).unwrap(),
        Some(end.frontier()),
        |_, records| {
            assert_eq!(records.records[0].payload, b"application header");
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(records.discarded_tail_bytes, 3);
    assert_eq!(
        fs::metadata(&path).unwrap().len(),
        original.len() as u64 - 3
    );
    assert_eq!(
        recovered
            .append_batch(2, &[b"next"])
            .unwrap()
            .frontier()
            .last_seq,
        2
    );
    drop(recovered);
    fs::remove_dir_all(root).unwrap();
}
