use super::*;

#[test]
fn running_candidate_retains_the_actual_warmed_application() {
    let fixture = Fixture::new();
    let factory = TestFactory::new();
    let candidate = execute_running(&factory, fixture.input().unwrap()).unwrap();
    assert_eq!(factory.calls.get(), 1);
    assert_eq!(candidate.checkpoint().cut.sequence, 2);
    let checkpoint = candidate.engine.checkpoint().unwrap();
    assert_eq!(&checkpoint, candidate.checkpoint());
    assert_eq!(candidate.engine.recorded().len(), 2);
    let state: State =
        serde_json::from_slice(&application::business_bytes(&checkpoint.business).unwrap())
            .unwrap();
    assert_eq!(state.observations, 1);
}

fn directory(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "tw-rebuilt-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

#[test]
fn persisted_running_history_keeps_genesis_warm_rows_and_the_same_instance() {
    let f = Fixture::new();
    let factory = TestFactory::new();
    let candidate = execute_running(&factory, f.input().unwrap()).unwrap();
    let expected = candidate.checkpoint().clone();
    let path = directory("history");
    let files = tickwire_recovery::FileBlobStore::open(&path).unwrap();
    let persisted = candidate.persist(files).unwrap();
    assert_eq!(factory.calls.get(), 1);
    assert_eq!(persisted.checkpoint(), &expected);
    assert_eq!(persisted.anchor().cut.sequence, 0);
    assert_ne!(persisted.genesis_commit(), persisted.commit());
    let objects = persisted.history_objects().unwrap();
    let history = tickwire_release::replay::history::verify(persisted.commit(), 2, |object| {
        objects
            .iter()
            .find(|b| Object::of(b) == object)
            .cloned()
            .ok_or(tickwire_release::Error::Missing)
    })
    .unwrap();
    assert_eq!(history.checkpoint(), &expected);
    assert_eq!(history.tail().rows(), persisted.owner.recorded());
    assert_eq!(history.tail().rows().len(), f.inputs.warm.len());
    assert_eq!(persisted.head().unwrap().cut, expected.cut);
    drop(persisted);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn installed_live_continues_after_warm_without_reexecuting_the_factory_or_callbacks() {
    use std::os::unix::fs::MetadataExt;
    use tickwire_ipc::Wire;
    let f = Fixture::new();
    let factory = TestFactory::new();
    let path = directory("live");
    let running = execute_running(&factory, f.input().unwrap())
        .unwrap()
        .persist(tickwire_recovery::FileBlobStore::open(&path).unwrap())
        .unwrap();
    let warm_commit = running.commit();
    let end = running.checkpoint().clone();
    let mut live = live::Live::from_rebuilt(running, path.clone()).unwrap();
    assert_eq!(live.committed().unwrap().0, &end);
    assert_eq!(factory.calls.get(), 1);
    let mut bytes = match &f.inputs.warm[0].input {
        Input::MarketApplied { record, .. } => record.clone(),
        _ => unreachable!(),
    };
    let mut header = RecordHeader::decode(&bytes[..RecordHeader::SIZE]).unwrap();
    header.sequence += 1;
    header.ingress_mono_ns = 1_700_000_000;
    header.publish_mono_ns = 1_700_000_000;
    header.encode(&mut bytes[..RecordHeader::SIZE]).unwrap();
    assert!(live.market(&bytes, &[], 1_700_000_000).unwrap().is_empty());
    let (current, _) = live.committed().unwrap();
    assert_eq!(current.cut.sequence, end.cut.sequence + 2);
    assert_eq!(current.runtime.accounts, end.runtime.accounts);
    let state: State = serde_json::from_slice(&live.checkpoint().unwrap()).unwrap();
    assert_eq!(state.observations, 2);
    let head = Head::decode(&std::fs::read(path.join("HEAD.json")).unwrap()).unwrap();
    let reader =
        tickwire_recovery::FileBlobReader::open(&path, std::fs::metadata(&path).unwrap().uid())
            .unwrap();
    let history = tickwire_release::replay::history::verify(head.commit, 3, |o| {
        reader
            .get_object(o.digest, MAX_BYTES)
            .map_err(|_| tickwire_release::Error::Missing)
    })
    .unwrap();
    assert_eq!(history.tail().rows().len(), 4);
    let last =
        InputCommit::decode(&reader.get_object(head.commit.digest, MAX_BYTES).unwrap()).unwrap();
    assert_eq!(last.previous_commit, Some(warm_commit));
    let baseline = CanonicalBaseline::decode(&f.inputs.accounts[0].baseline).unwrap();
    let (_, _, command) = crate::account_fixture::fixture();
    let fill = CanonicalRecord::new(
        baseline.digest(),
        2,
        RecoveryRecord::VenueEvent(Box::new(crate::account_fixture::event(
            crate::account_fixture::fill(&command, "after-warm-fill", "0.0005", "60000", "30"),
        ))),
    )
    .unwrap();
    let report = crate::reports::AccountReport::Record {
        position: 2,
        record: fill,
    };
    assert!(live.report(&report, 1_800_000_000).unwrap().is_empty());
    let checkpoint = live.committed().unwrap().0;
    assert_eq!(checkpoint.cut.sequence, 5);
    assert_eq!(checkpoint.runtime.accounts[0].sequence, 2);
    let head = Head::decode(&std::fs::read(path.join("HEAD.json")).unwrap()).unwrap();
    let history = tickwire_release::replay::history::verify(head.commit, 4, |o| {
        reader
            .get_object(o.digest, MAX_BYTES)
            .map_err(|_| tickwire_release::Error::Missing)
    })
    .unwrap();
    assert_eq!(history.tail().rows().len(), 5);
    assert_eq!(history.checkpoint(), checkpoint);
    assert!(
        live.report(&report, 1_800_000_000).is_err(),
        "the consumed account cut cannot replay twice"
    );
    drop(live);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn every_computation_genesis_and_warm_sync_failure_closes_the_running_candidate() {
    for fail in 1..=16 {
        let f = Fixture::new();
        let path = directory("failed-sync");
        let store = FailingFiles {
            store: tickwire_recovery::FileBlobStore::open(&path).unwrap(),
            puts: 0,
            fail,
        };
        let result = execute_running(&TestFactory::new(), f.input().unwrap())
            .unwrap()
            .persist(store);
        if fail <= 15 {
            assert!(matches!(result, Err(Error::Storage)), "write {fail}");
        } else {
            let owner = result.unwrap();
            assert_eq!(owner.owner.store.puts, 15);
            assert_eq!(owner.checkpoint().cut.sequence, 2);
        }
        assert!(!path.join("HEAD.json").exists());
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn rebuilt_live_refuses_an_existing_head_and_failed_head_sync_returns_no_owner() {
    for file in ["HEAD.json", "HEAD.pending"] {
        let f = Fixture::new();
        let path = directory("head");
        let running = execute_running(&TestFactory::new(), f.input().unwrap())
            .unwrap()
            .persist(tickwire_recovery::FileBlobStore::open(&path).unwrap())
            .unwrap();
        std::fs::write(path.join(file), b"existing-owner").unwrap();
        assert!(live::Live::from_rebuilt(running, path.clone()).is_err());
        assert_eq!(std::fs::read(path.join(file)).unwrap(), b"existing-owner");
        if file == "HEAD.pending" {
            assert!(!path.join("HEAD.json").exists());
        }
        std::fs::remove_dir_all(path).unwrap();
    }
}
