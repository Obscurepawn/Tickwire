//! Explicit Linux acceptance with a separately built registered worker. MD's
//! actual JOIN server and Publisher run in a parent thread, not another process.
#![cfg(target_os = "linux")]
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    sync::mpsc,
    time::Duration,
};
use tickwire_release::Object;
#[path = "capture_process/common.rs"]
mod common;
use common::*;

#[test]
#[ignore = "build tickwire-counting-worker, set TICKWIRE_CAPTURE_WORKER, then run --ignored --nocapture"]
fn registered_capture_process_seals_without_factory_and_closes_failures() {
    let worker = fs::canonicalize(
        std::env::var_os("TICKWIRE_CAPTURE_WORKER").expect("explicit registered worker executable"),
    )
    .unwrap();
    let (mut owner, market, _dir) = launch(&worker, Duration::from_secs(30));
    let pid = owner.process().pid;
    seed(&mut owner);
    let baseline_path = owner.bootstrap().output_directory.join("ATTEMPT.json");
    assert!(baseline_path.exists());
    let (mut snapshot, catalog, _) = account_fixture::fixture();
    snapshot.valid_until = u64::MAX;
    let baseline = tickwire_account_codec::CanonicalBaseline::new(snapshot, catalog).unwrap();
    let record = tickwire_account_codec::CanonicalRecord::new(
        baseline.digest(),
        1,
        tickwire_account::RecoveryRecord::UnattributedCashFlow {
            id: tickwire_venue::BoundedText::new("capture-fact").unwrap(),
            delta: tickwire_account::AssetAmount {
                key: account_fixture::key(1),
                amount: account_fixture::f(1),
            },
        },
    )
    .unwrap();
    owner.publish_record(1, &record).unwrap();
    owner.seal().unwrap();
    market.action.send(Action::Update).unwrap();
    wait(|| {
        owner.poll().unwrap();
        owner.captured().is_some()
    });
    let captured = owner.captured().unwrap();
    let exported = captured.objects().map(Object::of).collect::<Vec<_>>();
    assert!(exported.contains(&captured.receipt().bootstrap));
    assert!(exported.contains(&captured.receipt().request));
    assert!(exported.contains(&Object::of(&captured.receipt().completion.bytes().unwrap())));
    assert_eq!(captured.receipt().pid, pid);
    assert_eq!(captured.receipt().report_ring_sequence, 2);
    assert_eq!(captured.window().seal().end.accounts[0].sequence, 1);
    assert!(captured.window().seal().end.rows > 0);
    assert!(Path::new(&format!("/proc/{pid}")).exists());
    println!(
        "{}",
        serde_json::json!({"case":"captured","pid":pid,"worker_sha256":owner.bootstrap().identity.binary,"report_sequence":captured.receipt().report_ring_sequence,"market_applied":captured.receipt().market_applied,"seal":Object::of(&captured.window().seal().bytes().unwrap()),"factory_configuration":"invalid JSON; configure/create unreachable","md_scope":"actual JOIN server and memfd publisher in parent thread"})
    );
    if let Some(destination) = std::env::var_os("TICKWIRE_CAPTURE_EVIDENCE") {
        let destination = PathBuf::from(destination);
        fs::create_dir(&destination).unwrap();
        fs::set_permissions(&destination, fs::Permissions::from_mode(0o700)).unwrap();
        for entry in fs::read_dir(&owner.bootstrap().output_directory).unwrap() {
            let entry = entry.unwrap();
            if entry.file_name() == "LOCK" {
                continue;
            }
            assert!(entry.file_type().unwrap().is_file());
            fs::copy(entry.path(), destination.join(entry.file_name())).unwrap();
        }
        fs::write(
            destination.join("bootstrap.json"),
            owner.bootstrap().bytes().unwrap(),
        )
        .unwrap();
        fs::write(
            destination.join("receipt.json"),
            captured.receipt().bytes().unwrap(),
        )
        .unwrap();
        for bytes in captured.objects() {
            let object = Object::of(bytes);
            let name = object
                .digest
                .iter()
                .map(|v| format!("{v:02x}"))
                .collect::<String>();
            fs::write(destination.join(format!("export-{name}")), bytes).unwrap();
        }
        println!(
            "{}",
            serde_json::json!({"evidence_directory":destination,"export_objects":captured.objects().count()})
        );
    }
    for _ in 0..3 {
        owner.poll().unwrap();
        std::thread::sleep(Duration::from_millis(2));
    }
    let exit = owner.stop().unwrap();
    assert_eq!(exit.pid(), pid);
    assert!(!Path::new(&format!("/proc/{pid}")).exists());
    let (tx, rx) = mpsc::channel();
    market.action.send(Action::Detached(tx)).unwrap();
    assert_eq!(
        rx.recv().unwrap(),
        (true, true),
        "normal retirement must detach MD while keeping its bank live"
    );
    println!(
        "{}",
        serde_json::json!({"case":"graceful_stop","pid":pid,"successful_exit":exit.status().success(),"md_all_detached":true,"md_bank_live":true})
    );
    drop(market);
    let (mut owner, market, _dir) = launch(&worker, Duration::from_secs(15));
    let pid = owner.process().pid;
    seed(&mut owner);
    owner.seal().unwrap();
    wait(|| {
        let result = owner.poll();
        assert!(
            owner.captured().is_none(),
            "failure must never expose Captured"
        );
        result.is_err()
    });
    assert!(owner.failed());
    assert!(owner.captured().is_none());
    assert!(!Path::new(&format!("/proc/{pid}")).exists());
    assert!(owner.poll().is_err());
    drop(market);
    let (mut owner, market, _dir) = launch(&worker, Duration::from_secs(30));
    let pid = owner.process().pid;
    seed(&mut owner);
    owner.seal().unwrap();
    owner.poll().unwrap();
    market.action.send(Action::Update).unwrap();
    let marker = owner
        .bootstrap()
        .output_directory
        .join("CAPTURE_WINDOW.json");
    wait(|| {
        fs::read_dir(&owner.bootstrap().output_directory)
            .unwrap()
            .filter_map(Result::ok)
            .any(|entry| {
                entry.file_name().to_string_lossy().starts_with("object-")
                    && fs::read(entry.path()).is_ok_and(|raw| {
                        tickwire_strategy_sdk::capture::Receipt::decode(&raw).is_ok()
                    })
            })
    });
    fs::write(&marker, b"{}").unwrap();
    wait(|| {
        let result = owner.poll();
        assert!(
            owner.captured().is_none(),
            "failure must never expose Captured"
        );
        result.is_err()
    });
    assert!(owner.failed());
    assert!(owner.captured().is_none());
    assert!(!Path::new(&format!("/proc/{pid}")).exists());
    drop(market);
    let (owner, market, _dir) = launch(&worker, Duration::from_secs(30));
    let pid = owner.process().pid;
    drop(owner);
    assert!(!Path::new(&format!("/proc/{pid}")).exists());
    drop(market);
    println!(
        "{}",
        serde_json::json!({"case":"failure_and_drop","timeout_closed":true,"tampered_completion_closed":true,"children_reaped":true})
    );
}

#[test]
#[ignore = "requires TICKWIRE_CAPTURE_WORKER; independently verifies durable input roots"]
fn capture_rejects_missing_durable_bootstrap_or_request() {
    let worker = fs::canonicalize(
        std::env::var_os("TICKWIRE_CAPTURE_WORKER").expect("explicit registered worker executable"),
    )
    .unwrap();
    for bootstrap in [true, false] {
        let (mut owner, market, _directory) = launch(&worker, Duration::from_secs(30));
        seed(&mut owner);
        owner.seal().unwrap();
        owner.poll().unwrap();
        market.action.send(Action::Update).unwrap();
        let mut receipt = None;
        wait(|| {
            receipt = fs::read_dir(&owner.bootstrap().output_directory)
                .unwrap()
                .filter_map(Result::ok)
                .filter(|entry| entry.file_name().to_string_lossy().starts_with("object-"))
                .find_map(|entry| {
                    fs::read(entry.path())
                        .ok()
                        .and_then(|raw| tickwire_strategy_sdk::capture::Receipt::decode(&raw).ok())
                });
            receipt.is_some()
        });
        let receipt = receipt.unwrap();
        let root = if bootstrap {
            receipt.bootstrap
        } else {
            receipt.request
        };
        let name = root
            .digest
            .iter()
            .map(|v| format!("{v:02x}"))
            .collect::<String>();
        fs::remove_file(
            owner
                .bootstrap()
                .output_directory
                .join(format!("object-{name}")),
        )
        .unwrap();
        wait(|| {
            let result = owner.poll();
            assert!(
                owner.captured().is_none(),
                "missing durable root must not yield Captured"
            );
            result.is_err()
        });
        assert!(owner.failed());
        assert!(!Path::new(&format!("/proc/{}", owner.process().pid)).exists());
        drop(market);
    }
}

#[path = "rebuild_process/mod.rs"]
mod rebuild;
