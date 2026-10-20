use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
use tickwire_control::{Completion, RequestKey, RequestStore};
use tickwire_control_store::*;
use tickwire_journal::store::FileStore;
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "tickwire-control-store-{}-{}",
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
fn config() -> Config {
    Config {
        physical: tickwire_journal::Config::new([1; 16], [2; 16]),
        writer_epoch: 1,
        owner: [3; 16],
        max_requests: 4,
        max_pending: 2,
        max_response_bytes: 4096,
        max_retained_bytes: 1 << 20,
    }
}
fn key(n: u8) -> RequestKey {
    RequestKey {
        subject: [4; 16],
        epoch: 7,
        request_id: [n; 16],
    }
}
fn completion(n: u8) -> Completion {
    use tickwire_ipc::Wire;
    let body = tickwire_ipc::StatusBody {
        state_version: 9,
        ..Default::default()
    };
    let mut bytes = vec![0; tickwire_ipc::StatusBody::SIZE];
    body.encode(&mut bytes).unwrap();
    Completion {
        state_version: 9,
        response: tickwire_control::encode_frame(
            tickwire_ipc::ControlEnvelope {
                opcode: 49,
                flags: 10,
                request_id: [n; 16],
                sender_instance: [3; 16],
                target_instance: [4; 16],
                control_epoch: 7,
                ..Default::default()
            },
            &bytes,
        )
        .unwrap(),
    }
}
#[test]
fn received_and_completion_survive_real_file_reopen() {
    let dir = Directory::new();
    let cfg = config();
    let mut store = DurableRequests::create(FileStore::open(&dir.0).unwrap(), cfg.clone()).unwrap();
    assert!(store.insert_received(key(5), [6; 32]).unwrap());
    assert!(!store.insert_received(key(5), [6; 32]).unwrap());
    assert!(store.lookup(key(5)).unwrap().unwrap().completion.is_none());
    store.complete(key(5), [6; 32], completion(5)).unwrap();
    assert_eq!(store.position(), 3);
    drop(store);
    let mut cfg = cfg;
    cfg.writer_epoch = 2;
    let mut store = DurableRequests::recover(FileStore::open(&dir.0).unwrap(), cfg, None).unwrap();
    assert_eq!(
        store.lookup(key(5)).unwrap().unwrap().completion,
        Some(completion(5))
    );
    assert!(!store.insert_received(key(5), [6; 32]).unwrap());
    store.complete(key(5), [6; 32], completion(5)).unwrap();
    assert_eq!(store.position(), 3);
}
#[test]
fn pending_restart_does_not_offer_second_dispatch() {
    let dir = Directory::new();
    let mut store = DurableRequests::create(FileStore::open(&dir.0).unwrap(), config()).unwrap();
    assert!(store.insert_received(key(5), [6; 32]).unwrap());
    drop(store);
    let mut store =
        DurableRequests::recover(FileStore::open(&dir.0).unwrap(), config(), None).unwrap();
    assert!(!store.insert_received(key(5), [6; 32]).unwrap());
    assert!(store.lookup(key(5)).unwrap().unwrap().completion.is_none());
    assert_eq!(store.position(), 2);
}
#[test]
fn conflicting_completion_and_wrong_owner_do_not_append() {
    let dir = Directory::new();
    let mut store = DurableRequests::create(FileStore::open(&dir.0).unwrap(), config()).unwrap();
    store.insert_received(key(5), [6; 32]).unwrap();
    assert!(store.complete(key(5), [7; 32], completion(5)).is_err());
    assert!(store.complete(key(5), [6; 32], completion(8)).is_err());
    assert_eq!(store.position(), 2);
    store.complete(key(5), [6; 32], completion(5)).unwrap();
    let mut different = completion(5);
    different.state_version += 1;
    assert!(store.complete(key(5), [6; 32], different).is_err());
    assert_eq!(store.position(), 3);
    drop(store);
    let mut cfg = config();
    cfg.owner = [8; 16];
    assert!(DurableRequests::recover(FileStore::open(&dir.0).unwrap(), cfg, None).is_err());
}
#[test]
fn pending_capacity_releases_only_after_durable_completion() {
    let dir = Directory::new();
    let mut store = DurableRequests::create(FileStore::open(&dir.0).unwrap(), config()).unwrap();
    assert!(store.insert_received(key(5), [6; 32]).unwrap());
    assert!(store.insert_received(key(6), [7; 32]).unwrap());
    assert!(store.insert_received(key(7), [8; 32]).is_err());
    store.complete(key(5), [6; 32], completion(5)).unwrap();
    assert!(store.insert_received(key(7), [8; 32]).unwrap());
}
