use super::*;
use std::os::unix::fs::MetadataExt;
use tickwire_recovery::{BlobStore, StoreKey};
#[path = "../../../tickwire-strategy-sdk/tests/common/market_transition.rs"]
mod fixture;
struct Test {
    _directory: PrivateDirectory,
    store: tickwire_recovery::FileBlobStore,
    state: MarketTransition,
    host: Session,
    worker: Session,
}
impl Test {
    fn new() -> Self {
        let directory =
            PrivateDirectory::create_in(Path::new("/tmp"), "tw-host-transition").unwrap();
        let path = directory.path().join("objects");
        let store = tickwire_recovery::FileBlobStore::open(&path).unwrap();
        let process = ProcessIdentity {
            pid: 100,
            uid: std::fs::metadata("/proc/self").unwrap().uid(),
            gid: 0,
        };
        let a = fixture::approval();
        let (host, worker) = fixture::sessions();
        let request = protocol::request(
            &a,
            protocol::ARM,
            &serde_json::to_vec(&a).unwrap(),
            host.deadline(),
        )
        .unwrap();
        let mut state = MarketTransition::new(a.previous.clone(), path, process);
        state.approval = Some(a);
        state.arm_request = Some(request);
        Self {
            _directory: directory,
            store,
            state,
            host,
            worker,
        }
    }
    fn receipt(&mut self, receipt: &Receipt, kind: u16) -> Result<bool> {
        let bytes = serde_json::to_vec(receipt).unwrap();
        let object = Object::of(&bytes);
        self.store
            .put_sync(StoreKey::Object(object.digest), &bytes)
            .unwrap();
        let request = if kind == protocol::ARM {
            self.state.arm_request.as_ref().unwrap()
        } else {
            self.state.commit_request.as_ref().unwrap()
        };
        let raw = protocol::encode_reply(&self.worker, request, object).unwrap();
        let frame = self.host.validate(&raw, fixture::NOW).unwrap();
        self.state
            .observe(&frame.envelope, frame.body, fixture::NOW)
    }
    fn armed(&self) -> Receipt {
        let a = self.state.approval.as_ref().unwrap();
        Receipt::armed(
            a,
            100,
            tickwire_clock::MonoTime::new(a.previous.domain().unwrap(), fixture::NOW),
        )
        .unwrap()
    }
    fn quiesced(&self) -> Receipt {
        let a = self.state.approval.as_ref().unwrap();
        Receipt::quiesced(
            a,
            100,
            6,
            tickwire_clock::MonoTime::new(a.previous.domain().unwrap(), fixture::NOW),
        )
        .unwrap()
    }
}
#[test]
fn receipt_requires_actual_owned_pid_and_monotonic_phase() {
    let mut t = Test::new();
    let mut armed = t.armed();
    armed.pid += 1;
    assert!(t.receipt(&armed, protocol::ARM).is_err());
    assert!(t.state.receipt().is_none());
    let armed = t.armed();
    assert!(t.receipt(&armed, protocol::ARM).unwrap());
    assert!(t.receipt(&armed, protocol::ARM).unwrap());
    let q = t.quiesced();
    assert!(t.receipt(&q, protocol::ARM).unwrap());
    assert!(t.receipt(&armed, protocol::ARM).is_err());
    assert_eq!(t.state.receipt().unwrap(), &q);
}
#[test]
fn rejected_operation_cannot_later_become_armed() {
    let mut t = Test::new();
    let request = t.state.arm_request.clone().unwrap();
    let raw = protocol::encode_rejection(&t.worker, &request, protocol::UNSUPPORTED).unwrap();
    let frame = t.host.validate(&raw, fixture::NOW).unwrap();
    assert!(
        t.state
            .observe(&frame.envelope, frame.body, fixture::NOW)
            .unwrap()
    );
    assert_eq!(t.state.rejection(), Some(1));
    assert!(t.state.receipt().is_none());
    let armed = t.armed();
    assert!(t.receipt(&armed, protocol::ARM).is_err());
    assert!(t.state.receipt().is_none());
}
#[test]
fn activated_requires_the_approved_final_descriptor_and_prior_cut() {
    let mut t = Test::new();
    let armed = t.armed();
    t.receipt(&armed, protocol::ARM).unwrap();
    let q = t.quiesced();
    t.receipt(&q, protocol::ARM).unwrap();
    let a = t.state.approval.as_ref().unwrap();
    let commit = fixture::commit(a);
    let body = protocol::request(
        a,
        protocol::COMMIT,
        &serde_json::to_vec(&commit).unwrap(),
        t.host.deadline(),
    )
    .unwrap();
    let bank = ResourceHeader::decode(&commit.config.bank_header).unwrap();
    t.state.commit = Some(commit);
    t.state.commit_request = Some(body);
    let mut active = q.clone();
    active.phase = Phase::Activated;
    active.next_descriptor_hash = Some([99; 32]);
    active.activated_sequence = Some(7);
    assert!(t.receipt(&active, protocol::COMMIT).is_err());
    assert_eq!(t.state.receipt().unwrap(), &q);
    active.next_descriptor_hash = Some(bank.descriptor_hash);
    active.previous_applied = Some(5);
    assert!(t.receipt(&active, protocol::COMMIT).is_err());
    active.previous_applied = Some(6);
    t.receipt(&active, protocol::COMMIT).unwrap();
    assert_eq!(t.state.receipt().unwrap(), &active);
    assert_eq!(
        ResourceHeader::decode(&t.state.current.bank_header).unwrap(),
        bank
    );
}
