use std::{
    fs, io,
    path::PathBuf,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicU64, Ordering},
    },
};
use tickwire_business_journal::*;
use tickwire_execution::{
    self as ex,
    ports::{Exchange, ExecutionFact, SubmitOutcome},
};
use tickwire_journal::store::Store;
use tickwire_journal_codec as codec;
use tickwire_recovery as recovery;

// A spawned child can retain other tests' file-lock descriptions until exec.
// Keep process creation exclusive against complete WAL-owning test lifetimes;
// ordinary file tests may still run concurrently under shared guards.
static FORK_BARRIER: RwLock<()> = RwLock::new(());

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let p = std::env::temp_dir().join(format!(
            "tickwire-business-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&p).unwrap();
        Self(p)
    }
    fn wal(&self) -> PathBuf {
        self.0.join("wal")
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn config() -> BusinessConfig {
    BusinessConfig {
        physical: tickwire_journal::Config::new([1; 16], [2; 16]),
        domain: ex::Domain {
            venue: 1,
            account: 2,
            product: ex::Product::Spot,
        },
        initial_capital: 100,
    }
}
fn gate() -> ex::Gate {
    ex::Gate {
        epoch: 7,
        strategy_generation: 8,
        config_revision: 9,
        now: 1,
        lease_until: 100,
        enabled: true,
        market_valid: true,
        account_valid: true,
        recovered: true,
        owner_isolated: true,
    }
}
fn request(id: u64) -> ex::OrderRequest {
    ex::OrderRequest {
        command_digest: recovery::digest(&mock_command(id)),
        key: ex::OrderKey {
            venue: 1,
            account: 2,
            product: ex::Product::Spot,
            intent: id,
        },
        quantity: 2,
        unit_budget: 10,
        fee_budget: 2,
        epoch: 7,
        strategy_generation: 8,
        config_revision: 9,
        deadline: 90,
        recovery_object: id,
        commit_profile: ex::CommitProfile::LocalDurable,
    }
}
fn mock_command(id: u64) -> Vec<u8> {
    format!("mock-command:{id}").into_bytes()
}
fn identity(id: u64, seq: u64) -> codec::OrderIdentity {
    codec::OrderIdentity {
        venue: 1,
        account: 2,
        product: codec::Product::Spot,
        intent_id: id,
        intent_seq: seq,
        intent_exec_epoch: 7,
    }
}
struct MockAuthorizer<S: recovery::BlobStore = recovery::MockBlobStore>(
    Arc<Mutex<recovery::Persistence<S>>>,
);
impl<S: recovery::BlobStore> Clone for MockAuthorizer<S> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
impl Default for MockAuthorizer {
    fn default() -> Self {
        Self(Arc::new(Mutex::new(recovery::Persistence::new(
            recovery::MockBlobStore::default(),
        ))))
    }
}
// Independent test-only serialization matching the recovery input contract.
fn input_chunk() -> (Vec<u8>, recovery::InputBoundary) {
    let stream = [9; 16];
    let previous = [0; 32];
    let mut bytes = b"TWINP001".to_vec();
    bytes.extend(stream);
    bytes.extend(1u64.to_be_bytes());
    bytes.extend(1u32.to_be_bytes());
    bytes.extend(previous);
    let mut row = 1u64.to_be_bytes().to_vec();
    row.push(1);
    row.extend(5u32.to_be_bytes());
    row.extend(b"quote");
    let mut pre = b"tickwire.input-row.v1".to_vec();
    pre.extend(previous);
    pre.extend(&row);
    let last_hash = recovery::digest(&pre);
    bytes.extend(row);
    (
        bytes,
        recovery::InputBoundary {
            stream,
            first_sequence: 1,
            previous_hash: previous,
            last_sequence: 1,
            last_hash,
        },
    )
}
impl<S: recovery::BlobStore> MockAuthorizer<S> {
    fn authorize_mode(
        &mut self,
        r: AuthorizationRequest<'_>,
        include_command: bool,
    ) -> std::result::Result<recovery::VerifiedClosure, AuthorizationError> {
        let mut p = self.0.lock().unwrap();
        let mut objects = Vec::new();
        for kind in 1..=11 {
            let bytes = if kind == 1 {
                r.intent_payload.to_vec()
            } else {
                vec![kind; kind as usize + 1]
            };
            let hash = p.put_object(&bytes).map_err(AuthorizationError::Recovery)?;
            objects.push(recovery::DependencyObject {
                kind,
                content_hash: hash,
                byte_length: bytes.len() as u64,
                schema_id: 1,
                owner_identity: [1; 16],
            });
        }
        let (bytes, boundary) = input_chunk();
        let hash = p.put_object(&bytes).map_err(AuthorizationError::Recovery)?;
        objects.push(recovery::DependencyObject {
            kind: 12,
            content_hash: hash,
            byte_length: bytes.len() as u64,
            schema_id: 1,
            owner_identity: [1; 16],
        });
        if include_command {
            let bytes = mock_command(r.record.request.key.intent);
            let hash = p.put_object(&bytes).map_err(AuthorizationError::Recovery)?;
            objects.push(recovery::DependencyObject {
                kind: recovery::COMMAND_DEPENDENCY_KIND,
                content_hash: hash,
                byte_length: bytes.len() as u64,
                schema_id: 1,
                owner_identity: [1; 16],
            });
        }
        let edges = (1..objects.len() as u16).map(|i| (0, i)).collect();
        let manifest = recovery::RecoveryManifest {
            binding: *r.binding,
            logical_strategy_id: 1,
            instance_id: [3; 16],
            action_grant_ref: objects[3].content_hash,
            operation_ref: objects[6].content_hash,
            mode: recovery::RecoveryMode::DeterministicReplay,
            fault_model_id: [4; 32],
            pin_id: [5; 16],
            input_boundary: boundary,
            objects,
            edges,
        };
        p.publish(&manifest).map_err(AuthorizationError::Recovery)
    }
}
impl<S: recovery::BlobStore> CommitAuthorizer for MockAuthorizer<S> {
    fn current_gate(&mut self) -> std::result::Result<ex::Gate, AuthorizationError> {
        Ok(gate())
    }
    fn authorize(
        &mut self,
        r: AuthorizationRequest<'_>,
    ) -> std::result::Result<recovery::VerifiedClosure, AuthorizationError> {
        self.authorize_mode(r, true)
    }

    fn verify_recovery(
        &mut self,
        r: AuthorizationRequest<'_>,
    ) -> std::result::Result<recovery::VerifiedClosure, AuthorizationError> {
        self.0
            .lock()
            .unwrap()
            .verify(r.binding)
            .map_err(AuthorizationError::Recovery)
    }
}

struct ExpiringAuthorizer {
    inner: MockAuthorizer,
    calls: usize,
    after: ex::Gate,
}
impl CommitAuthorizer for ExpiringAuthorizer {
    fn current_gate(&mut self) -> std::result::Result<ex::Gate, AuthorizationError> {
        self.calls += 1;
        let mut g = gate();
        if self.calls > 1 {
            g = self.after;
        }
        Ok(g)
    }
    fn authorize(
        &mut self,
        r: AuthorizationRequest<'_>,
    ) -> std::result::Result<recovery::VerifiedClosure, AuthorizationError> {
        self.inner.authorize(r)
    }
    fn verify_recovery(
        &mut self,
        r: AuthorizationRequest<'_>,
    ) -> std::result::Result<recovery::VerifiedClosure, AuthorizationError> {
        self.inner.verify_recovery(r)
    }
}
#[test]
fn final_gate_expiry_during_io_keeps_durable_reservation_without_sending() {
    let _fork_barrier = FORK_BARRIER.read().unwrap();
    let d = Directory::new();
    let a = MockAuthorizer::default();
    let mut r: Runtime<_, _, _, 8, 16> = Runtime::create(
        FileBackend::open(d.wal()).unwrap(),
        config(),
        gate(),
        ExpiringAuthorizer {
            inner: a.clone(),
            calls: 0,
            after: ex::Gate { now: 91, ..gate() },
        },
        SpyExchange::default(),
    )
    .unwrap();
    assert!(r.submit(request(1)).is_err());
    assert_eq!(r.exchange().calls, 0);
    assert_eq!(r.view().held, 22);
    assert_eq!(r.view().journal_seq, 1);
    assert!(r.view().frozen);
    drop(r);
    assert_eq!(reopen(&d, a).unwrap().view().held, 22);
}
struct SpyExchange {
    calls: usize,
    outcome: SubmitOutcome,
}
impl Default for SpyExchange {
    fn default() -> Self {
        Self {
            calls: 0,
            outcome: SubmitOutcome::PendingResponse,
        }
    }
}
impl Exchange for SpyExchange {
    fn submit(&mut self, _: ex::OrderRequest) -> SubmitOutcome {
        self.calls += 1;
        self.outcome
    }
}
type Live = Runtime<FileBackend, MockAuthorizer, SpyExchange, 8, 16>;
fn create(d: &Directory, a: MockAuthorizer, e: SpyExchange) -> Live {
    Live::create(FileBackend::open(d.wal()).unwrap(), config(), gate(), a, e).unwrap()
}
fn reopen(d: &Directory, a: MockAuthorizer) -> Result<Live> {
    Live::recover(
        FileBackend::open(d.wal()).unwrap(),
        config(),
        gate(),
        a,
        SpyExchange::default(),
        None,
    )
}

#[test]
fn real_wal_reopens_committed_order_frozen_without_exchange_call() {
    let _fork_barrier = FORK_BARRIER.read().unwrap();
    let d = Directory::new();
    let a = MockAuthorizer::default();
    let mut r = create(&d, a.clone(), SpyExchange::default());
    r.submit(request(1)).unwrap();
    assert_eq!(r.exchange().calls, 1);
    assert_eq!(r.view().held, 22);
    drop(r);
    let r = reopen(&d, a).unwrap();
    assert!(r.view().frozen);
    assert_eq!(r.view().held, 22);
    assert_eq!(r.exchange().calls, 0);
}
#[test]
fn no_bytes_is_durable_before_release_and_replays_in_order() {
    let _fork_barrier = FORK_BARRIER.read().unwrap();
    let d = Directory::new();
    let a = MockAuthorizer::default();
    let mut r = create(
        &d,
        a.clone(),
        SpyExchange {
            calls: 0,
            outcome: SubmitOutcome::NoBytesAccepted,
        },
    );
    r.submit(request(1)).unwrap();
    assert_eq!(r.view().held, 0);
    assert_eq!(r.view().journal_seq, 2);
    r.submit(request(2)).unwrap();
    assert_eq!(r.view().journal_seq, 4);
    drop(r);
    let r = reopen(&d, a).unwrap();
    assert_eq!(r.view().held, 0);
    assert!(r.view().frozen);
    assert_eq!(r.order(1).unwrap().terminal, Some(ex::Terminal::Rejected));
}
#[test]
fn empty_recovery_is_frozen_and_bootstrap_capital_cannot_change() {
    let _fork_barrier = FORK_BARRIER.read().unwrap();
    let d = Directory::new();
    let a = MockAuthorizer::default();
    drop(create(&d, a.clone(), SpyExchange::default()));
    let r = reopen(&d, a.clone()).unwrap();
    assert!(r.view().frozen);
    drop(r);
    let mut changed = config();
    changed.initial_capital += 1;
    assert!(
        Live::recover(
            FileBackend::open(d.wal()).unwrap(),
            changed,
            gate(),
            a,
            SpyExchange::default(),
            None
        )
        .is_err()
    );
}
#[test]
fn missing_closure_stops_recovery_instead_of_republishing() {
    let _fork_barrier = FORK_BARRIER.read().unwrap();
    let d = Directory::new();
    let mut r = create(&d, MockAuthorizer::default(), SpyExchange::default());
    r.submit(request(1)).unwrap();
    drop(r);
    assert!(reopen(&d, MockAuthorizer::default()).is_err());
}
#[test]
fn fact_duplicates_are_durable_but_counted_once() {
    let _fork_barrier = FORK_BARRIER.read().unwrap();
    let d = Directory::new();
    let a = MockAuthorizer::default();
    let mut r = create(&d, a.clone(), SpyExchange::default());
    r.submit(request(1)).unwrap();
    let f = ex::Fill {
        trade_id: 4,
        quantity: 1,
        cost: 9,
    };
    r.record_fill(identity(1, 1), f).unwrap();
    r.record_fill(identity(1, 1), f).unwrap();
    assert_eq!(r.view().spent, 9);
    assert_eq!(r.view().journal_seq, 3);
    drop(r);
    let r = reopen(&d, a).unwrap();
    assert_eq!(r.view().spent, 9);
    assert_eq!(r.order(1).unwrap().filled, 1);
}
#[test]
fn terminal_missing_fill_details_and_late_fees_replay_exactly() {
    let _fork_barrier = FORK_BARRIER.read().unwrap();
    let d = Directory::new();
    let a = MockAuthorizer::default();
    let mut r = create(&d, a.clone(), SpyExchange::default());
    r.submit(request(1)).unwrap();
    r.record_fact(
        identity(1, 1),
        ExecutionFact::Terminal {
            intent: 1,
            status: ex::Terminal::Canceled,
            cumulative: 1,
        },
    )
    .unwrap();
    assert_eq!(r.view().held, 12);
    r.record_fill(
        identity(1, 1),
        ex::Fill {
            trade_id: 2,
            quantity: 1,
            cost: 9,
        },
    )
    .unwrap();
    r.record_fact(
        identity(1, 1),
        ExecutionFact::Fee {
            intent: 1,
            id: 2,
            amount: 1,
        },
    )
    .unwrap();
    let before = r.view();
    drop(r);
    let r = reopen(&d, a).unwrap();
    assert_eq!(r.view().held, before.held);
    assert_eq!(r.view().spent, before.spent);
}
#[test]
fn invalid_fact_is_preserved_then_live_and_recovery_freeze() {
    let _fork_barrier = FORK_BARRIER.read().unwrap();
    let d = Directory::new();
    let a = MockAuthorizer::default();
    let mut r = create(&d, a.clone(), SpyExchange::default());
    r.submit(request(1)).unwrap();
    assert!(
        r.record_fill(
            identity(1, 1),
            ex::Fill {
                trade_id: 1,
                quantity: 0,
                cost: 3
            }
        )
        .is_err()
    );
    assert!(r.view().frozen);
    assert_eq!(r.view().journal_seq, 2);
    drop(r);
    assert!(reopen(&d, a).is_err());
}
#[test]
fn sync_replica_has_no_implicit_mock_or_local_downgrade() {
    let _fork_barrier = FORK_BARRIER.read().unwrap();
    let d = Directory::new();
    let mut r = create(&d, MockAuthorizer::default(), SpyExchange::default());
    let mut req = request(1);
    req.commit_profile = ex::CommitProfile::SyncReplica;
    assert!(matches!(r.submit(req), Err(Error::UnsupportedProfile)));
    assert_eq!(r.exchange().calls, 0);
    assert_eq!(r.view().journal_seq, 0);
}
#[test]
fn commit_is_idempotent_but_changed_intent_is_rejected() {
    let _fork_barrier = FORK_BARRIER.read().unwrap();
    let d = Directory::new();
    let mut a = MockAuthorizer::default();
    let mut j = BusinessJournal::create(FileBackend::open(d.wal()).unwrap(), config(), 7).unwrap();
    let record = ex::DurableRecord {
        request: request(1),
        sequence: 1,
    };
    let event = intent_record(&config(), 7, 1, record);
    let binding = closure_binding(&event).unwrap();
    let bytes = codec::business_payload(&event);
    let cap = a
        .authorize(AuthorizationRequest {
            binding: &binding,
            record: &record,
            intent_payload: &bytes,
        })
        .unwrap();
    let first = j.commit_intent(record, &cap).unwrap();
    assert_eq!(j.commit_intent(record, &cap).unwrap(), first);
    assert_eq!(j.position(), 1);
    let mut changed = record;
    changed.request.deadline += 1;
    assert!(j.commit_intent(changed, &cap).is_err());
    assert_eq!(j.position(), 1);
}

#[test]
fn command_digest_is_recovered_from_actual_wal_and_rebinding_conflicts() {
    let _fork_barrier = FORK_BARRIER.read().unwrap();
    let d = Directory::new();
    let authorizer = MockAuthorizer::default();
    let mut live = create(&d, authorizer.clone(), SpyExchange::default());
    let q = request(1);
    live.submit(q).unwrap();
    let mut changed = q;
    changed.command_digest[31] ^= 1;
    assert!(matches!(live.submit(changed), Err(Error::Conflict)));
    assert!(live.view().frozen);
    drop(live);
    let recovered = reopen(&d, authorizer).unwrap();
    assert_eq!(recovered.order(1).unwrap().request, q);
    assert!(recovered.view().frozen);
}

#[test]
fn closure_without_full_command_cannot_authorize_wal() {
    let _fork_barrier = FORK_BARRIER.read().unwrap();
    let d = Directory::new();
    let mut a = MockAuthorizer::default();
    let record = ex::DurableRecord {
        request: request(1),
        sequence: 1,
    };
    let event = intent_record(&config(), 7, 1, record);
    let binding = closure_binding(&event).unwrap();
    let bytes = codec::business_payload(&event);
    let cap = a
        .authorize_mode(
            AuthorizationRequest {
                binding: &binding,
                record: &record,
                intent_payload: &bytes,
            },
            false,
        )
        .unwrap();
    let mut j = BusinessJournal::create(FileBackend::open(d.wal()).unwrap(), config(), 7).unwrap();
    assert!(j.commit_intent(record, &cap).is_err());
    assert_eq!(j.position(), 0);
}

#[test]
fn closure_with_matching_intent_but_another_command_is_rejected() {
    let _fork_barrier = FORK_BARRIER.read().unwrap();
    let d = Directory::new();
    let mut a = MockAuthorizer::default();
    let mut q = request(1);
    q.command_digest[0] ^= 1;
    let record = ex::DurableRecord {
        request: q,
        sequence: 1,
    };
    let event = intent_record(&config(), 7, 1, record);
    let binding = closure_binding(&event).unwrap();
    let bytes = codec::business_payload(&event);
    let cap = a
        .authorize(AuthorizationRequest {
            binding: &binding,
            record: &record,
            intent_payload: &bytes,
        })
        .unwrap();
    assert_eq!(cap.binding(), &binding);
    assert_ne!(cap.command_digest(), Some(q.command_digest));
    let mut j = BusinessJournal::create(FileBackend::open(d.wal()).unwrap(), config(), 7).unwrap();
    assert!(j.commit_intent(record, &cap).is_err());
    assert_eq!(j.position(), 0);
}

#[test]
fn recovered_commit_retry_keeps_original_position_across_writer_change() {
    let _fork_barrier = FORK_BARRIER.read().unwrap();
    let d = Directory::new();
    let mut a = MockAuthorizer::default();
    let record = ex::DurableRecord {
        request: request(1),
        sequence: 1,
    };
    let event = intent_record(&config(), 7, 1, record);
    let binding = closure_binding(&event).unwrap();
    let bytes = codec::business_payload(&event);
    let cap = a
        .authorize(AuthorizationRequest {
            binding: &binding,
            record: &record,
            intent_payload: &bytes,
        })
        .unwrap();
    let mut j = BusinessJournal::create(FileBackend::open(d.wal()).unwrap(), config(), 7).unwrap();
    let original = j.commit_intent(record, &cap).unwrap();
    drop(j);
    let mut j =
        BusinessJournal::recover(FileBackend::open(d.wal()).unwrap(), config(), 8, None).unwrap();
    let retry = j.commit_intent(record, &cap).unwrap();
    assert_eq!(retry.journal_seq(), original.journal_seq());
    assert_eq!(retry.logical_hash(), original.logical_hash());
    assert_eq!(j.position(), 1);
}

#[derive(Default)]
struct Faults {
    sync_count: usize,
    fail_sync: Option<usize>,
    append_count: usize,
    fail_append: Option<usize>,
}
struct FaultBackend {
    file: FileBackend,
    faults: Arc<Mutex<Faults>>,
}
impl Store for FaultBackend {
    fn read_manifest(&mut self) -> io::Result<Option<Vec<u8>>> {
        self.file.read_manifest()
    }
    fn publish_manifest(&mut self, b: &[u8]) -> io::Result<()> {
        self.file.publish_manifest(b)
    }
    fn segment_ids(&mut self) -> io::Result<Vec<u64>> {
        self.file.segment_ids()
    }
    fn read_segment(&mut self, id: u64, max: u64) -> io::Result<Vec<u8>> {
        self.file.read_segment(id, max)
    }
    fn create_segment(&mut self, id: u64) -> io::Result<()> {
        self.file.create_segment(id)
    }
    fn append_segment(&mut self, id: u64, offset: u64, b: &[u8]) -> io::Result<()> {
        let mut f = self.faults.lock().unwrap();
        f.append_count += 1;
        if f.fail_append == Some(f.append_count) {
            self.file.append_segment(id, offset, &b[..8])?;
            return Err(io::Error::other("injected torn write"));
        }
        self.file.append_segment(id, offset, b)
    }
    fn sync_segment(&mut self, id: u64) -> io::Result<()> {
        let mut f = self.faults.lock().unwrap();
        f.sync_count += 1;
        if f.fail_sync == Some(f.sync_count) {
            return Err(io::Error::other("injected sync error"));
        }
        self.file.sync_segment(id)
    }
    fn truncate_segment(&mut self, id: u64, len: u64) -> io::Result<()> {
        self.file.truncate_segment(id, len)
    }
}
impl Backend for FaultBackend {
    fn read_bootstrap(&mut self) -> io::Result<Option<Vec<u8>>> {
        self.file.read_bootstrap()
    }
    fn publish_bootstrap(&mut self, b: &[u8]) -> io::Result<()> {
        self.file.publish_bootstrap(b)
    }
}
#[test]
fn abort_sync_failure_retains_reservation_and_freezes() {
    let _fork_barrier = FORK_BARRIER.read().unwrap();
    let d = Directory::new();
    let faults = Arc::new(Mutex::new(Faults {
        sync_count: 0,
        fail_sync: Some(2),
        ..Default::default()
    }));
    let b = FaultBackend {
        file: FileBackend::open(d.wal()).unwrap(),
        faults,
    };
    let mut r: Runtime<_, _, _, 8, 16> = Runtime::create(
        b,
        config(),
        gate(),
        MockAuthorizer::default(),
        SpyExchange {
            calls: 0,
            outcome: SubmitOutcome::NoBytesAccepted,
        },
    )
    .unwrap();
    assert!(r.submit(request(1)).is_err());
    assert!(r.view().frozen);
    assert_eq!(r.view().held, 22);
    assert_eq!(r.view().journal_seq, 1);
    assert_eq!(r.exchange().calls, 1);
}

#[test]
fn recovered_writer_can_append_old_order_facts_but_never_send() {
    let _fork_barrier = FORK_BARRIER.read().unwrap();
    let d = Directory::new();
    let a = MockAuthorizer::default();
    let mut r = create(&d, a.clone(), SpyExchange::default());
    r.submit(request(1)).unwrap();
    drop(r);
    let mut g = gate();
    g.epoch = 8;
    let mut r: Live = Runtime::recover(
        FileBackend::open(d.wal()).unwrap(),
        config(),
        g,
        a.clone(),
        SpyExchange::default(),
        None,
    )
    .unwrap();
    r.record_fill(
        identity(1, 1),
        ex::Fill {
            trade_id: 1,
            quantity: 1,
            cost: 9,
        },
    )
    .unwrap();
    assert!(r.submit(request(2)).is_err());
    assert_eq!(r.exchange().calls, 0);
    drop(r);
    let r: Live = Runtime::recover(
        FileBackend::open(d.wal()).unwrap(),
        config(),
        g,
        a,
        SpyExchange::default(),
        None,
    )
    .unwrap();
    assert_eq!(r.view().spent, 9);
    assert_eq!(r.view().journal_seq, 2);
    assert!(r.view().frozen);
}
#[test]
fn conflicting_original_order_identity_is_preserved_and_quarantined() {
    let _fork_barrier = FORK_BARRIER.read().unwrap();
    let d = Directory::new();
    let a = MockAuthorizer::default();
    let mut r = create(&d, a.clone(), SpyExchange::default());
    r.submit(request(1)).unwrap();
    let mut wrong = identity(1, 1);
    wrong.intent_exec_epoch = 6;
    assert!(
        r.record_fill(
            wrong,
            ex::Fill {
                trade_id: 1,
                quantity: 1,
                cost: 9
            }
        )
        .is_err()
    );
    assert!(r.view().frozen);
    assert_eq!(r.view().journal_seq, 2);
    drop(r);
    assert!(reopen(&d, a).is_err());
}
#[test]
fn external_abort_cannot_release_an_order() {
    let _fork_barrier = FORK_BARRIER.read().unwrap();
    let d = Directory::new();
    let mut r = create(&d, MockAuthorizer::default(), SpyExchange::default());
    r.submit(request(1)).unwrap();
    let record = ex::DurableRecord {
        request: request(1),
        sequence: 1,
    };
    assert!(
        r.record_fact(identity(1, 1), ExecutionFact::AbortedBeforeSend { record })
            .is_err()
    );
    assert_eq!(r.view().held, 22);
    assert_eq!(r.view().journal_seq, 1);
    assert!(r.view().frozen);
}
struct FirstRejected {
    calls: usize,
}
impl Exchange for FirstRejected {
    fn submit(&mut self, _: ex::OrderRequest) -> SubmitOutcome {
        self.calls += 1;
        if self.calls == 1 {
            SubmitOutcome::NoBytesAccepted
        } else {
            SubmitOutcome::PendingResponse
        }
    }
}
#[test]
fn ordered_abort_before_next_intent_avoids_recovery_arithmetic_overflow() {
    let _fork_barrier = FORK_BARRIER.read().unwrap();
    let d = Directory::new();
    let a = MockAuthorizer::default();
    let mut c = config();
    c.initial_capital = i128::MAX;
    let mut r: Runtime<_, _, _, 8, 16> = Runtime::create(
        FileBackend::open(d.wal()).unwrap(),
        c.clone(),
        gate(),
        a.clone(),
        FirstRejected { calls: 0 },
    )
    .unwrap();
    let mut first = request(1);
    first.unit_budget = 1;
    first.fee_budget = 1;
    let mut second = first;
    second.key.intent = 2;
    second.command_digest = recovery::digest(&mock_command(2));
    second.recovery_object = 2;
    r.submit(first).unwrap();
    r.submit(second).unwrap();
    r.record_fill(
        identity(2, 2),
        ex::Fill {
            trade_id: 1,
            quantity: 2,
            cost: i128::MAX - 3,
        },
    )
    .unwrap();
    assert_eq!(r.view().held, 1);
    assert_eq!(r.view().available, 2);
    drop(r);
    let r: Live = Runtime::recover(
        FileBackend::open(d.wal()).unwrap(),
        c,
        gate(),
        a,
        SpyExchange::default(),
        None,
    )
    .unwrap();
    assert_eq!(r.view().held, 1);
    assert_eq!(r.view().spent, i128::MAX - 3);
    assert_eq!(r.view().available, 2);
    assert!(r.view().frozen);
}
#[test]
fn prepared_sync_failure_never_sends_and_keeps_reservation() {
    let _fork_barrier = FORK_BARRIER.read().unwrap();
    let d = Directory::new();
    let b = FaultBackend {
        file: FileBackend::open(d.wal()).unwrap(),
        faults: Arc::new(Mutex::new(Faults {
            sync_count: 0,
            fail_sync: Some(1),
            ..Default::default()
        })),
    };
    let mut r: Runtime<_, _, _, 8, 16> = Runtime::create(
        b,
        config(),
        gate(),
        MockAuthorizer::default(),
        SpyExchange::default(),
    )
    .unwrap();
    assert!(r.submit(request(1)).is_err());
    assert_eq!(r.exchange().calls, 0);
    assert_eq!(r.view().held, 22);
    assert_eq!(r.view().journal_seq, 0);
    assert!(r.view().frozen);
}
fn file_authorizer(path: PathBuf) -> MockAuthorizer<recovery::FileBlobStore> {
    MockAuthorizer(Arc::new(Mutex::new(recovery::Persistence::new(
        recovery::FileBlobStore::open(path).unwrap(),
    ))))
}
#[test]
#[ignore = "subprocess helper"]
fn file_crash_child() {
    let _fork_barrier = FORK_BARRIER.read().unwrap();
    let Some(root) = std::env::var_os("TICKWIRE_BUSINESS_CHILD_ROOT") else {
        return;
    };
    let root = PathBuf::from(root);
    let no_bytes = std::env::var("TICKWIRE_BUSINESS_CHILD_MODE").unwrap() == "abort";
    let mut r: Runtime<_, _, _, 8, 16> = Runtime::create(
        FileBackend::open(root.join("wal")).unwrap(),
        config(),
        gate(),
        file_authorizer(root.join("closure")),
        SpyExchange {
            calls: 0,
            outcome: if no_bytes {
                SubmitOutcome::NoBytesAccepted
            } else {
                SubmitOutcome::PendingResponse
            },
        },
    )
    .unwrap();
    r.submit(request(1)).unwrap();
    fs::write(root.join("ready"), format!("{}", r.view().held)).unwrap();
    loop {
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
}
#[test]
fn actual_process_kill_replays_synced_wal_and_file_closure_frozen() {
    let _fork_barrier = FORK_BARRIER.write().unwrap();
    use std::{
        process::{Command, Stdio},
        time::{Duration, Instant},
    };
    for mode in ["pending", "abort"] {
        let d = Directory::new();
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--ignored", "--exact", "file_crash_child", "--nocapture"])
            .env("TICKWIRE_BUSINESS_CHILD_ROOT", &d.0)
            .env("TICKWIRE_BUSINESS_CHILD_MODE", mode)
            .stdout(Stdio::null())
            .spawn()
            .unwrap();
        let start = Instant::now();
        while !d.0.join("ready").exists() {
            if let Some(status) = child.try_wait().unwrap() {
                panic!("child ended: {status}");
            }
            if start.elapsed() > Duration::from_secs(10) {
                let _ = child.kill();
                panic!("child timeout");
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        child.kill().unwrap();
        assert!(!child.wait().unwrap().success());
        let r: Runtime<_, _, _, 8, 16> = Runtime::recover(
            FileBackend::open(d.wal()).unwrap(),
            config(),
            gate(),
            file_authorizer(d.0.join("closure")),
            SpyExchange::default(),
            None,
        )
        .unwrap();
        assert!(r.view().frozen);
        assert_eq!(r.exchange().calls, 0);
        assert_eq!(r.view().held, if mode == "abort" { 0 } else { 22 });
        assert_eq!(r.view().journal_seq, if mode == "abort" { 2 } else { 1 });
    }
}

#[test]
fn torn_abort_keeps_reserve_then_recovery_discards_only_unsealed_tail() {
    let _fork_barrier = FORK_BARRIER.read().unwrap();
    let d = Directory::new();
    let a = MockAuthorizer::default();
    let b = FaultBackend {
        file: FileBackend::open(d.wal()).unwrap(),
        faults: Arc::new(Mutex::new(Faults {
            fail_append: Some(2),
            ..Default::default()
        })),
    };
    let mut r: Runtime<_, _, _, 8, 16> = Runtime::create(
        b,
        config(),
        gate(),
        a.clone(),
        SpyExchange {
            calls: 0,
            outcome: SubmitOutcome::NoBytesAccepted,
        },
    )
    .unwrap();
    assert!(r.submit(request(1)).is_err());
    assert_eq!(r.view().held, 22);
    assert!(r.view().frozen);
    drop(r);
    let r = reopen(&d, a).unwrap();
    assert!(r.view().frozen);
    assert_eq!(r.view().held, 22);
    assert_eq!(r.view().journal_seq, 1);
}
#[test]
#[ignore = "subprocess helper"]
fn cwd_child() {
    let _fork_barrier = FORK_BARRIER.read().unwrap();
    let Some(root) = std::env::var_os("TICKWIRE_BUSINESS_CWD_ROOT") else {
        return;
    };
    let root = PathBuf::from(root);
    std::env::set_current_dir(&root).unwrap();
    fs::create_dir("other").unwrap();
    let backend = FileBackend::open("wal").unwrap();
    std::env::set_current_dir("other").unwrap();
    let mut r: Live = Runtime::create(
        backend,
        config(),
        gate(),
        MockAuthorizer::default(),
        SpyExchange::default(),
    )
    .unwrap();
    r.submit(request(1)).unwrap();
    assert!(root.join("wal/BUSINESS.json").exists());
    assert!(root.join("wal/MANIFEST.json").exists());
    assert!(!root.join("other/wal").exists());
}
#[test]
fn relative_backend_remains_bound_to_locked_directory_after_cwd_change() {
    let _fork_barrier = FORK_BARRIER.write().unwrap();
    let d = Directory::new();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--ignored", "--exact", "cwd_child", "--nocapture"])
        .env("TICKWIRE_BUSINESS_CWD_ROOT", &d.0)
        .status()
        .unwrap();
    assert!(status.success());
}

#[test]
fn final_gate_revalidates_all_authority_and_validity_fields() {
    let _fork_barrier = FORK_BARRIER.read().unwrap();
    let mut variants = Vec::new();
    let mut g = gate();
    g.epoch = 8;
    variants.push(g);
    let mut g = gate();
    g.strategy_generation += 1;
    variants.push(g);
    let mut g = gate();
    g.config_revision += 1;
    variants.push(g);
    let mut g = gate();
    g.lease_until = 1;
    variants.push(g);
    let mut g = gate();
    g.enabled = false;
    variants.push(g);
    let mut g = gate();
    g.market_valid = false;
    variants.push(g);
    let mut g = gate();
    g.account_valid = false;
    variants.push(g);
    let mut g = gate();
    g.recovered = false;
    variants.push(g);
    let mut g = gate();
    g.owner_isolated = false;
    variants.push(g);
    for after in variants {
        let d = Directory::new();
        let mut r: Runtime<_, _, _, 8, 16> = Runtime::create(
            FileBackend::open(d.wal()).unwrap(),
            config(),
            gate(),
            ExpiringAuthorizer {
                inner: MockAuthorizer::default(),
                calls: 0,
                after,
            },
            SpyExchange::default(),
        )
        .unwrap();
        assert!(r.submit(request(1)).is_err());
        assert_eq!(r.exchange().calls, 0);
        assert_eq!(r.view().held, 22);
        assert_eq!(r.view().journal_seq, 1);
        assert!(r.view().frozen);
    }
}
#[test]
fn valid_physical_frame_with_invalid_business_payload_cannot_recover() {
    let _fork_barrier = FORK_BARRIER.read().unwrap();
    let d = Directory::new();
    drop(create(
        &d,
        MockAuthorizer::default(),
        SpyExchange::default(),
    ));
    let (mut j, _) = tickwire_journal::Journal::recover(
        tickwire_journal::store::FileStore::open(d.wal()).unwrap(),
        None,
    )
    .unwrap();
    j.append_batch(7, &[b"malformed business event"]).unwrap();
    drop(j);
    assert!(reopen(&d, MockAuthorizer::default()).is_err());
}
struct WrongCap(recovery::VerifiedClosure);
impl CommitAuthorizer for WrongCap {
    fn current_gate(&mut self) -> std::result::Result<ex::Gate, AuthorizationError> {
        Ok(gate())
    }
    fn authorize(
        &mut self,
        _: AuthorizationRequest<'_>,
    ) -> std::result::Result<recovery::VerifiedClosure, AuthorizationError> {
        Ok(self.0.clone())
    }
    fn verify_recovery(
        &mut self,
        _: AuthorizationRequest<'_>,
    ) -> std::result::Result<recovery::VerifiedClosure, AuthorizationError> {
        Ok(self.0.clone())
    }
}
#[test]
fn verified_closure_for_another_intent_cannot_authorize_wal_or_send() {
    let _fork_barrier = FORK_BARRIER.read().unwrap();
    let d = Directory::new();
    let mut a = MockAuthorizer::default();
    let record = ex::DurableRecord {
        request: request(2),
        sequence: 1,
    };
    let event = intent_record(&config(), 7, 1, record);
    let binding = closure_binding(&event).unwrap();
    let bytes = codec::business_payload(&event);
    let cap = a
        .authorize(AuthorizationRequest {
            binding: &binding,
            record: &record,
            intent_payload: &bytes,
        })
        .unwrap();
    let mut r: Runtime<_, _, _, 8, 16> = Runtime::create(
        FileBackend::open(d.wal()).unwrap(),
        config(),
        gate(),
        WrongCap(cap),
        SpyExchange::default(),
    )
    .unwrap();
    assert!(r.submit(request(1)).is_err());
    assert_eq!(r.exchange().calls, 0);
    assert_eq!(r.view().journal_seq, 0);
    assert_eq!(r.view().held, 22);
    assert!(r.view().frozen);
}
