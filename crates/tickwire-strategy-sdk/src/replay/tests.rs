use super::*;
use serde::{Deserialize, Serialize};

fn stream(n: u8) -> StreamKey {
    StreamKey {
        identity: [n; 32],
        generation: 1,
    }
}
fn source() -> Source {
    Source {
        logical_strategy: 8,
        instance: [2; 16],
        generation: 5,
        binary_digest: [3; 32],
        state_schema: 7,
        input_stream: [4; 16],
        clock: ClockDomain {
            boot_id: [5; 16],
            generation: 1,
        },
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct Stateful {
    observed: Vec<u8>,
    value: u64,
    action_happened: bool,
    fail: bool,
}
impl Stateful {
    pub(super) fn initial() -> Self {
        Self {
            observed: vec![],
            value: 0,
            action_happened: false,
            fail: false,
        }
    }
}
impl ReplayableStrategy for Stateful {
    fn checkpoint(&self) -> Result<Vec<u8>> {
        Ok(canonical(self)?)
    }
    fn apply(
        &mut self,
        input: &Input,
        c: &mut ReplayContext<'_>,
        out: &mut ReplayEffects,
    ) -> Result<()> {
        let tag = match input {
            Input::MarketApplied { record, .. } => {
                self.value += record[0] as u64;
                1
            }
            Input::MarketCallback { invoked: true, .. } => {
                c.arm_timer(1, c.now_ns() + 1)?;
                self.value += c.random_u64()?;
                2
            }
            Input::AccountCallback { canonical, .. } => {
                self.value += canonical[0] as u64;
                3
            }
            Input::TimerFired { .. } => {
                if !self.action_happened {
                    let mut command = crate::test_command::command();
                    command.context.strategy.created_mono_ns = c.now_ns();
                    let mut triggers = command.context.triggers.as_slice().to_vec();
                    for t in &mut triggers {
                        t.observed_mono_ns = c.now_ns();
                    }
                    command.context.triggers = tickwire_venue::BoundedList::new(triggers).unwrap();
                    out.emit(CanonicalCommand::new(command).unwrap())?;
                    self.action_happened = true;
                }
                4
            }
            Input::ConfigApply { .. } => {
                self.value += c.config().bytes[0] as u64;
                5
            }
            Input::External { bytes, .. } => {
                self.value += bytes[0] as u64;
                6
            }
            Input::Entropy { .. } => {
                self.value += c.random_u64()?;
                7
            }
            Input::ModelApply { id, .. } => {
                self.value += c.model(*id).unwrap().bytes[0] as u64;
                8
            }
            Input::MarketCallback { invoked: false, .. } => 9,
        };
        self.observed.push(tag);
        if self.fail {
            return Err(Error::Callback);
        }
        Ok(())
    }
}
struct Factory {
    binary: Hash,
}
impl SourceReplayFactory for Factory {
    type Running = Stateful;
    fn binary_digest(&self) -> Hash {
        self.binary
    }
    fn state_schema(&self) -> u64 {
        7
    }
    fn restore_source(&self, a: &VerifiedAnchor) -> Result<Stateful> {
        serde_json::from_slice(&a.checkpoint().business).map_err(|_| Error::Callback)
    }
}
pub(super) fn anchor_with(state: Stateful) -> VerifiedAnchor {
    let checkpoint = Checkpoint::new(
        source(),
        0,
        [0; 32],
        state.checkpoint().unwrap(),
        RuntimeState {
            now_ns: 10,
            config: VersionedBytes {
                revision: 1,
                bytes: vec![1],
            },
            models: vec![Model {
                id: 1,
                value: VersionedBytes {
                    revision: 1,
                    bytes: vec![2],
                },
            }],
            markets: vec![Cursor {
                stream: stream(1),
                sequence: 7,
                content_digest: [7; 32],
            }],
            accounts: vec![],
            external: vec![],
            timers: vec![],
            rng: Rng::Xoshiro256StarStarV1 {
                state: [1, 2, 3, 4],
                draws: 0,
            },
            effects: EffectCut {
                count: 0,
                digest: [0; 32],
            },
        },
    )
    .unwrap();
    verify_anchor(
        &checkpoint.bytes().unwrap(),
        &source(),
        Object::of(&[1]),
        &[(1, Object::of(&[2]))],
    )
    .unwrap()
}
fn anchor() -> VerifiedAnchor {
    anchor_with(Stateful::initial())
}
fn sequence() -> Vec<(Input, u64)> {
    vec![
        (
            Input::MarketApplied {
                stream: stream(1),
                sequence: 8,
                received_ns: 10,
                record: vec![2; 512],
                payload: vec![3],
            },
            10,
        ),
        (
            Input::MarketCallback {
                stream: stream(1),
                instrument: 12,
                invoked: true,
                maximum_age_ns: 100,
            },
            10,
        ),
        (
            Input::AccountCallback {
                stream: stream(2),
                position: 0,
                canonical: vec![3],
            },
            10,
        ),
        (
            Input::TimerFired {
                id: 1,
                generation: 1,
                deadline_ns: 11,
            },
            11,
        ),
        (
            Input::ConfigApply {
                previous: Object::of(&[1]),
                next: VersionedBytes {
                    revision: 2,
                    bytes: vec![4],
                },
            },
            11,
        ),
        (
            Input::External {
                stream: stream(3),
                sequence: 1,
                schema: 1,
                bytes: vec![5],
            },
            11,
        ),
        (
            Input::Entropy {
                stream: stream(3),
                sequence: 2,
                seed: [5, 6, 7, 8],
            },
            12,
        ),
        (
            Input::ModelApply {
                id: 1,
                previous: Object::of(&[2]),
                next: VersionedBytes {
                    revision: 2,
                    bytes: vec![6],
                },
            },
            12,
        ),
    ]
}
fn live(a: &VerifiedAnchor) -> Engine<Stateful> {
    let mut engine =
        Engine::new(a, Factory { binary: [3; 32] }.restore_source(a).unwrap()).unwrap();
    for (input, now) in sequence() {
        engine.apply_live(input, now).unwrap();
    }
    engine
}
fn tail(a: &VerifiedAnchor, e: &Engine<Stateful>) -> VerifiedTail {
    verify_tail(
        a,
        &encode_tail(a, e.recorded()).unwrap(),
        e.last_completed_cut(),
    )
    .unwrap()
}

#[test]
fn real_callbacks_reconstruct_market_account_timer_config_model_rng_and_state() {
    let a = anchor();
    let e = live(&a);
    let expected = e.checkpoint().unwrap();
    let actual = replay_source(&Factory { binary: [3; 32] }, &a, &tail(&a, &e)).unwrap();
    assert_eq!(actual.checkpoint(), &expected);
    let s: Stateful = serde_json::from_slice(&actual.checkpoint().business).unwrap();
    assert_eq!(s.observed, vec![1, 2, 3, 4, 5, 6, 7, 8]);
    assert!(s.action_happened);
    assert_eq!(s.value, 46_100);
    assert_eq!(expected.runtime.effects.count, 1);
}
#[test]
fn wrong_old_binary_is_rejected_before_a_callback() {
    let a = anchor();
    let e = live(&a);
    assert!(matches!(
        replay_source(&Factory { binary: [9; 32] }, &a, &tail(&a, &e)),
        Err(Error::Identity)
    ));
}
#[test]
fn callback_state_must_equal_the_actual_final_cut() {
    let a = anchor();
    let e = live(&a);
    let mut wrong = e.last_completed_cut().clone();
    wrong.business = Object::of(b"different actual C");
    let t = verify_tail(&a, &encode_tail(&a, e.recorded()).unwrap(), &wrong).unwrap();
    assert!(matches!(
        replay_source(&Factory { binary: [3; 32] }, &a, &t),
        Err(Error::StateMismatch)
    ));
}
#[test]
fn timer_rng_and_config_results_are_part_of_final_cut() {
    let a = anchor();
    let e = live(&a);
    let mut wrong = e.last_completed_cut().clone();
    wrong.runtime = Object::of(b"wrong timers or RNG");
    let t = verify_tail(&a, &encode_tail(&a, e.recorded()).unwrap(), &wrong).unwrap();
    assert!(matches!(
        replay_source(&Factory { binary: [3; 32] }, &a, &t),
        Err(Error::StateMismatch)
    ));
}
#[test]
fn historical_outputs_must_match_even_when_the_input_chain_is_self_consistent() {
    let a = anchor();
    let e = live(&a);
    let mut rows = e.recorded().to_vec();
    rows[3].effects.clear();
    let mut end = e.last_completed_cut().clone();
    end.chain = [0; 32];
    for row in &rows {
        end.chain = row_chain(end.chain, row).unwrap();
    }
    let t = verify_tail(&a, &encode_tail(&a, &rows).unwrap(), &end).unwrap();
    assert!(matches!(
        replay_source(&Factory { binary: [3; 32] }, &a, &t),
        Err(Error::EffectsMismatch)
    ));
}
#[test]
fn unsupported_factory_never_mints_a_completed_state() {
    struct NoFactory;
    impl SourceReplayFactory for NoFactory {
        type Running = Stateful;
        fn binary_digest(&self) -> Hash {
            [3; 32]
        }
        fn state_schema(&self) -> u64 {
            7
        }
    }
    let a = anchor();
    let e = live(&a);
    assert!(matches!(
        replay_source(&NoFactory, &a, &tail(&a, &e)),
        Err(Error::Unsupported)
    ));
}
#[test]
fn failed_callback_does_not_advance_the_cut_or_expose_a_usable_candidate() {
    let mut state = Stateful::initial();
    state.fail = true;
    let a = anchor_with(state.clone());
    let mut e = Engine::new(&a, state).unwrap();
    let cut = e.last_completed_cut().clone();
    assert!(matches!(
        e.apply_live(sequence()[0].0.clone(), 10),
        Err(Error::Callback)
    ));
    assert_eq!(e.last_completed_cut(), &cut);
    assert!(e.recorded().is_empty());
    assert!(matches!(e.checkpoint(), Err(Error::Poisoned)));
    assert!(matches!(
        e.apply_live(sequence()[1].0.clone(), 10),
        Err(Error::Poisoned)
    ));
}
#[test]
fn a_fired_or_cancelled_timer_cannot_fire_again() {
    let a = anchor();
    let mut e = live(&a);
    let cut = e.last_completed_cut().clone();
    assert!(matches!(
        e.apply_live(
            Input::TimerFired {
                id: 1,
                generation: 1,
                deadline_ns: 11
            },
            12
        ),
        Err(Error::Sequence)
    ));
    assert_eq!(e.last_completed_cut(), &cut);
}

#[test]
fn production_timer_poll_records_only_actual_due_callbacks_and_respects_budget() {
    let a = anchor();
    let mut e = Engine::new(&a, Stateful::initial()).unwrap();
    e.apply_live(sequence()[1].0.clone(), 10).unwrap();
    assert_eq!(e.next_deadline(), Some(11));
    assert!(e.poll_timers(10, 1).unwrap().is_empty());
    let actual = e.poll_timers(12, 1).unwrap();
    assert_eq!(actual.len(), 1);
    assert_eq!(actual[0].record.now_ns, 12);
    assert!(matches!(
        actual[0].record.input,
        Input::TimerFired {
            id: 1,
            generation: 1,
            deadline_ns: 11
        }
    ));
    assert_eq!(actual[0].commands.len(), 1);
    assert_eq!(e.next_deadline(), None);
    assert!(e.poll_timers(13, 1).unwrap().is_empty());
    let t = tail(&a, &e);
    assert_eq!(
        replay_source(&Factory { binary: [3; 32] }, &a, &t)
            .unwrap()
            .checkpoint(),
        &e.checkpoint().unwrap()
    );
}
#[test]
fn cancelled_timer_generation_cannot_be_reused_and_disabled_rng_is_explicit() {
    let a = anchor();
    let mut runtime = a.checkpoint().runtime.clone();
    {
        let mut c = ReplayContext {
            views: None,
            runtime: &mut runtime,
        };
        assert_eq!(c.arm_timer(1, 12).unwrap(), 1);
        c.cancel_timer(1, 1).unwrap();
        assert_eq!(c.arm_timer(1, 13).unwrap(), 2);
        assert_eq!(c.cancel_timer(1, 1), Err(Error::Sequence));
    }
    assert_eq!(runtime.timers[0].deadline_ns, Some(13));
    runtime.rng = Rng::Disabled;
    assert_eq!(
        ReplayContext {
            views: None,
            runtime: &mut runtime
        }
        .random_u64(),
        Err(Error::Unsupported)
    );
}

struct FileStoreFault {
    actual: std::rc::Rc<std::cell::RefCell<tickwire_recovery::FileBlobStore>>,
    puts: std::rc::Rc<std::cell::Cell<usize>>,
    fail: std::rc::Rc<std::cell::Cell<usize>>,
}
impl tickwire_recovery::BlobStore for FileStoreFault {
    fn get(
        &self,
        key: tickwire_recovery::StoreKey,
        n: usize,
    ) -> tickwire_recovery::Result<Vec<u8>> {
        self.actual.borrow().get(key, n)
    }
    fn put_sync(
        &mut self,
        key: tickwire_recovery::StoreKey,
        bytes: &[u8],
    ) -> tickwire_recovery::Result<()> {
        self.actual.borrow_mut().put_sync(key, bytes)?;
        let n = self.puts.get() + 1;
        self.puts.set(n);
        if n == self.fail.get() {
            return Err(std::io::Error::other("injected error after actual file sync").into());
        }
        Ok(())
    }
}
fn file_store() -> (std::path::PathBuf, FileStoreFault) {
    let path = std::env::temp_dir().join(format!(
        "tickwire-replay-sync-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let actual = tickwire_recovery::FileBlobStore::open(&path).unwrap();
    (
        path,
        FileStoreFault {
            actual: std::rc::Rc::new(std::cell::RefCell::new(actual)),
            puts: std::rc::Rc::new(std::cell::Cell::new(0)),
            fail: std::rc::Rc::new(std::cell::Cell::new(usize::MAX)),
        },
    )
}
struct Remove(std::path::PathBuf);
impl Drop for Remove {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
#[test]
fn every_input_sync_failure_holds_outputs_and_does_not_promote_the_memory_cut() {
    for failure in 1..=3 {
        let (path, store) = file_store();
        let _cleanup = Remove(path);
        let puts = store.puts.clone();
        let fail = store.fail.clone();
        let mut durable =
            durable::DurableEngine::new(anchor(), Stateful::initial(), store).unwrap();
        for (input, at) in sequence() {
            durable.apply(input, at).unwrap();
        }
        assert_eq!(durable.memory_cut().sequence, 8);
        assert_eq!(durable.last_durable_cut().sequence, 0);
        puts.set(0);
        fail.set(failure);
        assert!(
            matches!(durable.flush(), Err(Error::Storage)),
            "file completion {failure}"
        );
        assert!(durable.stopped());
        assert_eq!(durable.last_durable_cut().sequence, 0);
        assert!(matches!(durable.flush(), Err(Error::Poisoned)));
    }
}
#[test]
fn actual_files_and_exact_input_commit_exist_before_any_output_is_visible() {
    use tickwire_recovery::{BlobStore, StoreKey};
    let (path, store) = file_store();
    let _cleanup = Remove(path);
    let actual = store.actual.clone();
    let mut durable = durable::DurableEngine::new(anchor(), Stateful::initial(), store).unwrap();
    for (input, at) in sequence() {
        durable.apply(input, at).unwrap();
    }
    let completed = durable.flush().unwrap();
    assert_eq!(completed.cut().sequence, 8);
    let bytes = actual
        .borrow()
        .get(StoreKey::Object(completed.commit().digest), MAX_BYTES)
        .unwrap();
    let commit = InputCommit::decode(&bytes).unwrap();
    let c = Checkpoint::decode(
        &actual
            .borrow()
            .get(StoreKey::Object(commit.checkpoint.digest), MAX_BYTES)
            .unwrap(),
    )
    .unwrap();
    assert_eq!(&c.cut, completed.cut());
    let t = actual
        .borrow()
        .get(StoreKey::Object(commit.tail.digest), MAX_BYTES)
        .unwrap();
    verify_tail(&anchor(), &t, &c.cut).unwrap();
    let mut output = 0;
    completed
        .publish(|_| {
            output += 1;
            Ok(())
        })
        .unwrap();
    assert_eq!(output, 1);
    durable
        .flush()
        .unwrap()
        .publish(|_| {
            output += 1;
            Ok(())
        })
        .unwrap();
    assert_eq!(output, 1);
}

#[test]
fn successive_flushes_persist_only_new_input_rows_and_keep_the_full_capture() {
    let (path, store) = file_store();
    let _cleanup = Remove(path);
    let mut owner = durable::DurableEngine::new(anchor(), Stateful::initial(), store).unwrap();
    let initial = owner.anchor().clone();
    let mut previous = owner.head().unwrap();
    let mut persisted_rows = 0;
    let mut linked = true;
    for n in 1..=64 {
        owner
            .apply(
                Input::External {
                    stream: stream(3),
                    sequence: n,
                    schema: 1,
                    bytes: vec![1; 512],
                },
                10 + n,
            )
            .unwrap();
        let batch = owner.flush().unwrap();
        let commit = InputCommit::decode(&owner.read_object(batch.commit()).unwrap()).unwrap();
        let start = Checkpoint::decode(&owner.read_object(commit.anchor).unwrap()).unwrap();
        let models = start
            .runtime
            .models
            .iter()
            .map(|m| (m.id, Object::of(&m.value.bytes)))
            .collect::<Vec<_>>();
        let start = verify_anchor(
            &start.bytes().unwrap(),
            &source(),
            Object::of(&start.runtime.config.bytes),
            &models,
        )
        .unwrap();
        let rows = verify_tail(
            &start,
            &owner.read_object(commit.tail).unwrap(),
            batch.cut(),
        )
        .unwrap();
        persisted_rows += rows.rows().len();
        linked &=
            commit.anchor == previous.checkpoint && commit.previous_commit == Some(previous.commit);
        previous = owner.head().unwrap();
        batch
            .publish(|_| panic!("external inputs have no order"))
            .unwrap();
    }
    assert_eq!(
        persisted_rows, 64,
        "every input must occur in one durable tail segment"
    );
    assert!(
        linked,
        "every segment starts at its actual previous durable checkpoint"
    );
    let request = ControlInput::Capture {
        source: source(),
        anchor: Object::of(&initial.bytes().unwrap()),
        expected: initial.cut.clone(),
        pause: true,
    };
    let capture = owner.capture(&request).unwrap();
    let capture = CapturedTail::decode(&owner.read_object(capture).unwrap()).unwrap();
    let initial = verify_anchor(
        &initial.bytes().unwrap(),
        &source(),
        Object::of(&[1]),
        &[(1, Object::of(&[2]))],
    )
    .unwrap();
    let tail = verify_tail(
        &initial,
        &owner.read_object(capture.tail).unwrap(),
        &capture.cut,
    )
    .unwrap();
    assert_eq!(tail.rows().len(), 64);
    let replayed = replay_source(
        &Factory {
            binary: source().binary_digest,
        },
        &initial,
        &tail,
    )
    .unwrap();
    assert_eq!(replayed.checkpoint(), owner.durable_checkpoint());
}

#[test]
fn reopened_segmented_head_verifies_the_whole_chain_and_rejects_missing_or_rebound_segments() {
    let (path, store) = file_store();
    let _cleanup = Remove(path.clone());
    let mut owner = durable::DurableEngine::new(anchor(), Stateful::initial(), store).unwrap();
    let mut commits = Vec::new();
    for (input, at) in sequence() {
        owner.apply(input, at).unwrap();
        commits.push(owner.flush().unwrap().commit());
    }
    let expected = owner.durable_checkpoint().clone();
    let head = owner.head().unwrap();
    let head_path = path.join("HEAD.json");
    std::fs::write(&head_path, head.bytes().unwrap()).unwrap();
    std::fs::File::open(&head_path).unwrap().sync_all().unwrap();
    std::fs::File::open(&path).unwrap().sync_all().unwrap();
    drop(owner);
    let mut store = tickwire_recovery::FileBlobStore::open(&path).unwrap();
    let head = Head::decode(&std::fs::read(head_path).unwrap()).unwrap();
    let recovered = durable::verify_committed_history(&store, &head, 16).unwrap();
    assert_eq!(recovered.checkpoint(), &expected);
    assert_eq!(recovered.tail().rows().len(), 8);
    assert_eq!(
        replay_source(
            &Factory {
                binary: source().binary_digest
            },
            recovered.anchor(),
            recovered.tail()
        )
        .unwrap()
        .checkpoint(),
        &expected
    );
    assert!(durable::verify_committed_history(&store, &head, 2).is_err());
    use tickwire_recovery::{BlobStore, StoreKey};
    let get = |o: Object| store.get(StoreKey::Object(o.digest), MAX_BYTES).unwrap();
    let mut rebound = InputCommit::decode(&get(head.commit)).unwrap();
    rebound.previous_commit = Some(commits[0]);
    let bytes = rebound.bytes().unwrap();
    let rebound_object = Object::of(&bytes);
    store
        .put_sync(StoreKey::Object(rebound_object.digest), &bytes)
        .unwrap();
    let mut wrong = head.clone();
    wrong.commit = rebound_object;
    assert!(durable::verify_committed_history(&store, &wrong, 16).is_err());
    let missing = path.join(format!(
        "object-{}",
        commits[3]
            .digest
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>()
    ));
    std::fs::remove_file(missing).unwrap();
    assert!(durable::verify_committed_history(&store, &head, 16).is_err());
}

#[test]
fn legacy_full_prefix_is_a_verified_boundary_for_reopened_delta_history() {
    let (path, store) = file_store();
    let _cleanup = Remove(path.clone());
    let initial = anchor();
    let mut owner =
        durable::DurableEngine::new(initial.clone(), Stateful::initial(), store).unwrap();
    let first = owner.head().unwrap();
    let mut legacy_initial =
        InputCommit::decode(&owner.read_object(first.commit).unwrap()).unwrap();
    legacy_initial.schema = 1;
    let legacy_initial = owner
        .store_object(&legacy_initial.bytes().unwrap())
        .unwrap();
    for (input, at) in sequence().into_iter().take(4) {
        owner.apply(input, at).unwrap();
        owner.flush().unwrap();
    }
    let captured = owner
        .capture(&ControlInput::Capture {
            source: source(),
            anchor: first.checkpoint,
            expected: first.cut,
            pause: true,
        })
        .unwrap();
    let captured = CapturedTail::decode(&owner.read_object(captured).unwrap()).unwrap();
    // Schema 1 stored the complete S-to-current tail on every commit. This
    // persisted object is the upgrade boundary, not a new runtime admission.
    let legacy = InputCommit {
        schema: 1,
        anchor: captured.anchor,
        tail: captured.tail,
        checkpoint: captured.checkpoint,
        previous_commit: Some(legacy_initial),
    };
    owner.commit = owner.store_object(&legacy.bytes().unwrap()).unwrap();
    for (input, at) in sequence().into_iter().skip(4) {
        owner.apply(input, at).unwrap();
        owner.flush().unwrap();
    }
    let expected = owner.durable_checkpoint().clone();
    let head_path = path.join("HEAD.json");
    std::fs::write(&head_path, owner.head().unwrap().bytes().unwrap()).unwrap();
    std::fs::File::open(&head_path).unwrap().sync_all().unwrap();
    std::fs::File::open(&path).unwrap().sync_all().unwrap();
    drop(owner);
    let store = tickwire_recovery::FileBlobStore::open(&path).unwrap();
    let head = Head::decode(&std::fs::read(head_path).unwrap()).unwrap();
    let history = durable::verify_committed_history(&store, &head, 5).unwrap();
    assert_eq!(history.anchor().checkpoint(), initial.checkpoint());
    assert_eq!(history.tail().rows().len(), 8);
    assert_eq!(history.checkpoint(), &expected);
    assert_eq!(
        replay_source(
            &Factory {
                binary: source().binary_digest
            },
            history.anchor(),
            history.tail()
        )
        .unwrap()
        .checkpoint(),
        &expected
    );
    assert!(durable::verify_committed_history(&store, &head, 4).is_err());
    let object_path = |object: Object| {
        path.join(format!(
            "object-{}",
            object
                .digest
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>()
        ))
    };
    // Earlier cumulative copies are unnecessary once their full content is
    // verified at this exact checkpoint. The boundary tail itself is required.
    std::fs::remove_file(object_path(legacy_initial)).unwrap();
    assert!(durable::verify_committed_history(&store, &head, 5).is_ok());
    std::fs::remove_file(object_path(legacy.tail)).unwrap();
    assert!(durable::verify_committed_history(&store, &head, 5).is_err());
}

#[test]
fn shared_delta_history_rejects_broken_predecessors_rows_streams_schemas_and_budgets() {
    use tickwire_release::replay::history;
    let (path, store) = file_store();
    let _cleanup = Remove(path.clone());
    let mut owner = durable::DurableEngine::new(anchor(), Stateful::initial(), store).unwrap();
    for (input, at) in sequence() {
        owner.apply(input, at).unwrap();
        owner.flush().unwrap();
    }
    let head = owner.head().unwrap();
    let mut objects = std::collections::BTreeMap::new();
    for entry in std::fs::read_dir(&path).unwrap() {
        let entry = entry.unwrap();
        if entry.file_name().to_string_lossy().starts_with("object-") {
            let bytes = std::fs::read(entry.path()).unwrap();
            objects.insert(Object::of(&bytes).digest, bytes);
        }
    }
    let read = |o: Object| objects[&o.digest].clone();
    let original = InputCommit::decode(&read(head.commit)).unwrap();
    let base = Checkpoint::decode(&read(original.anchor)).unwrap();
    let models = base
        .runtime
        .models
        .iter()
        .map(|m| (m.id, Object::of(&m.value.bytes)))
        .collect::<Vec<_>>();
    let a = verify_anchor(
        &base.bytes().unwrap(),
        &base.source,
        Object::of(&base.runtime.config.bytes),
        &models,
    )
    .unwrap();
    let end = Checkpoint::decode(&read(original.checkpoint)).unwrap();
    let tail = verify_tail(&a, &read(original.tail), &end.cut).unwrap();
    for mutation in 0..10 {
        let mut changed = objects.clone();
        let mut commit = original.clone();
        let mut insert = |bytes: Vec<u8>| {
            let object = Object::of(&bytes);
            changed.insert(object.digest, bytes);
            object
        };
        match mutation {
            0 => {
                commit.previous_commit = Some(Object {
                    digest: [19; 32],
                    bytes: 100,
                })
            }
            // A predecessor returning to the current head contradicts the exact
            // predecessor checkpoint. A true SHA cycle also cannot bypass hashes.
            1 => commit.previous_commit = Some(head.commit),
            2 | 3 => {
                let mut rows = tail.rows().to_vec();
                if mutation == 2 {
                    rows.push(rows[0].clone());
                } else {
                    rows.clear();
                }
                commit.tail = insert(encode_tail(&a, &rows).unwrap());
            }
            4 => {
                let mut bytes = read(original.tail);
                bytes[8] ^= 1;
                commit.tail = insert(bytes);
            }
            5 => commit.anchor = head.checkpoint,
            6 => {
                let mut c = end.clone();
                c.source.instance[0] ^= 1;
                commit.checkpoint = insert(c.bytes().unwrap());
            }
            7 => commit.schema = 3,
            8 => {
                let mut rows = tail.rows().to_vec();
                rows[0].sequence += 1;
                commit.tail = insert(encode_tail(&a, &rows).unwrap());
            }
            _ => commit.tail.bytes = MAX_BYTES as u64 + 1,
        }
        let bytes = canonical(&commit).unwrap();
        let root = Object::of(&bytes);
        changed.insert(root.digest, bytes);
        let result = history::verify(root, 16, |o| {
            changed
                .get(&o.digest)
                .cloned()
                .ok_or(tickwire_release::Error::Missing)
        });
        assert!(result.is_err(), "mutation {mutation}");
    }
    for limit in [0, 2, MAX_ROWS + 2] {
        assert!(history::verify(head.commit, limit, |o| Ok(read(o))).is_err());
    }
    // A forged reader cannot satisfy a back edge by returning different bytes.
    assert!(
        history::verify(head.commit, 16, |o| {
            if o == original.previous_commit.unwrap() {
                Ok(read(head.commit))
            } else {
                Ok(read(o))
            }
        })
        .is_err()
    );
}
