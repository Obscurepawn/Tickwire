use super::*;
use std::{
    fs,
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::Path,
};
#[test]
fn config_rejects_missing_query_build_qualification_and_duplicate_groups() {
    let mut c = config(Path::new("/tmp/observation-test"));
    c.validate().unwrap();
    c.allowed_build_hashes.clear();
    assert!(c.validate().is_err());
    c.allowed_build_hashes = vec![[1; 32], [1; 32]];
    assert!(c.validate().is_err());
    c.allowed_build_hashes = vec![[0; 32]];
    assert!(c.validate().is_err());
    c.allowed_build_hashes = vec![[1; 32]];
    c.outbox_directory = c.inbox_directory.clone();
    assert!(c.validate().is_err());
}
fn config(root: &Path) -> Config {
    Config {
        inbox_directory: root.join("inbox"),
        outbox_directory: root.join("outbox"),
        journal_directory: root.join("journal"),
        stream_epoch: [33; 16],
        owner_uid: fs::metadata("/proc/self").unwrap().uid(),
        allowed_build_hashes: vec![[1; 32]],
        maximum_requests: 8,
        maximum_pending: 2,
        maximum_per_poll: 2,
        recover: false,
    }
}
struct Fixture {
    root: tickwire_control_sys::PrivateDirectory,
    config: Config,
}
impl Fixture {
    fn new() -> Self {
        let root =
            tickwire_control_sys::PrivateDirectory::create_in(Path::new("/tmp"), "tw-observe")
                .unwrap();
        let config = config(root.path());
        for p in [
            &config.inbox_directory,
            &config.outbox_directory,
            &config.journal_directory,
        ] {
            fs::create_dir(p).unwrap();
            fs::set_permissions(p, fs::Permissions::from_mode(0o700)).unwrap();
        }
        Self { root, config }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(self.root.path());
    }
}
#[test]
fn cold_files_bind_owner_inode_mode_and_reject_links() {
    let f = Fixture::new();
    let dir = files::Directory::open(&f.config.inbox_directory, f.config.owner_uid).unwrap();
    dir.put("a.json", b"hello").unwrap();
    assert_eq!(dir.read("a.json", 5).unwrap().unwrap(), b"hello");
    fs::hard_link(
        f.config.inbox_directory.join("a.json"),
        f.config.inbox_directory.join("hard.json"),
    )
    .unwrap();
    assert!(dir.read("a.json", 5).is_err());
    fs::remove_file(f.config.inbox_directory.join("hard.json")).unwrap();
    std::os::unix::fs::symlink("a.json", f.config.inbox_directory.join("link.json")).unwrap();
    assert!(dir.read("link.json", 5).is_err());
    fs::set_permissions(
        f.config.inbox_directory.join("a.json"),
        fs::Permissions::from_mode(0o644),
    )
    .unwrap();
    assert!(dir.read("a.json", 5).is_err());
    assert!(
        files::Directory::open(
            &f.config.inbox_directory,
            f.config.owner_uid.wrapping_add(1)
        )
        .is_err()
    );
    fs::rename(&f.config.inbox_directory, f.root.path().join("old")).unwrap();
    fs::create_dir(&f.config.inbox_directory).unwrap();
    fs::set_permissions(&f.config.inbox_directory, fs::Permissions::from_mode(0o700)).unwrap();
    assert!(dir.check().is_err());
}
#[test]
fn received_request_is_synced_then_unknown_owner_gets_explicit_failure() {
    let f = Fixture::new();
    let (policy, _) = crate::control::tests::inputs();
    let domain = policy.domain().unwrap();
    let plane = policy.build().unwrap();
    let mut inbox = ObservationInbox::open_with(f.config.clone(), &policy, plane, || {
        Ok(MonoTime::new(domain, 100))
    })
    .unwrap();
    let r = request(&inbox, 1, 900);
    write_request(&f, &r);
    let mut strategy =
        crate::strategy::FileStrategy::new(&crate::strategy::Config::Observe).unwrap();
    assert_eq!(
        inbox
            .poll_with(&mut strategy, || Ok(MonoTime::new(domain, 110)))
            .unwrap(),
        1
    );
    let out: Outcome = serde_json::from_slice(
        &fs::read(
            f.config
                .outbox_directory
                .join(format!("{}.json", store::hex(&r.request_id))),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(out.status, Status::Unsupported);
    assert!(out.observation.is_none());
}
fn request(inbox: &ObservationInbox, id: u8, until: u64) -> Request {
    Request {
        schema: 1,
        request_id: [id; 16],
        session_id: inbox.session.session_id,
        binding: inbox.binding.clone(),
        target: Target {
            logical_strategy_id: 7,
            instance: [7; 16],
            generation: 1,
        },
        valid_until_mono_ns: until,
        require_resources: false,
    }
}
fn write_request(f: &Fixture, r: &Request) {
    let dir = files::Directory::open(&f.config.inbox_directory, f.config.owner_uid).unwrap();
    dir.put(
        &format!("{}.json", store::hex(&r.request_id)),
        &serde_json::to_vec(r).unwrap(),
    )
    .unwrap();
}

struct PendingStrategy {
    registry: tickwire_supervisor::Registry,
    began: usize,
    polled: usize,
    polled_targets: Vec<[u8; 16]>,
    cancelled: usize,
}
impl PendingStrategy {
    fn new(f: &mut Fixture) -> Self {
        let executable = Path::new("/bin/sleep");
        let image = tickwire_recovery::digest(&fs::read(executable).unwrap());
        f.config.allowed_build_hashes = vec![image];
        let mut registry = tickwire_supervisor::Registry::new(2).unwrap();
        registry
            .spawn(tickwire_supervisor::LaunchSpec {
                identity: tickwire_control::Identity {
                    instance: [7; 16],
                    generation: 1,
                    role: 3,
                    logical_strategy: 7,
                    host_boot: [8; 16],
                    namespace: [9; 16],
                    build_hash: image,
                    qualification_hash: [10; 32],
                    manifest_hash: [11; 32],
                    arch: if cfg!(target_arch = "aarch64") { 2 } else { 1 },
                    trust_profile: 1,
                },
                executable: executable.to_owned(),
                arguments: vec!["30".into()],
                working_directory: f.root.path().to_owned(),
                environment: BTreeMap::new(),
                uid: f.config.owner_uid,
                gid: fs::metadata("/proc/self").unwrap().gid(),
                inherit_output: false,
            })
            .unwrap();
        Self {
            registry,
            began: 0,
            polled: 0,
            polled_targets: Vec::new(),
            cancelled: 0,
        }
    }
}
impl tickwire_runtime::node::Strategy for PendingStrategy {
    fn on_market(&mut self, _: &tickwire_binance_public::Notice<'_>) -> bool {
        true
    }
    fn next(
        &mut self,
        _: &tickwire_account::Account,
        _: tickwire_runtime::node::Health,
        _: tickwire_binance_clock::LocalTime,
    ) -> Option<tickwire_runtime::node::Submission> {
        None
    }
    fn on_submission(
        &mut self,
        _: &tickwire_venue::CanonicalCommand,
        _: &std::result::Result<tickwire_account_gateway::Status, tickwire_binance_account::Error>,
    ) {
    }
}
impl AppStrategy for PendingStrategy {
    fn instance(&self) -> Option<([u8; 16], u64)> {
        Some(([7; 16], 1))
    }
    fn failed(&self) -> bool {
        false
    }
    fn observation_process(
        &mut self,
        target: Target,
    ) -> tickwire_strategy_host::Result<tickwire_supervisor::LiveChildObservation> {
        Ok(self.registry.observe(target.instance)?)
    }
    fn begin_observation(
        &mut self,
        _: Target,
        _: tickwire_strategy_host::ObservationRequest,
    ) -> tickwire_strategy_host::Result<()> {
        self.began += 1;
        Ok(())
    }
    fn poll_observation(
        &mut self,
        target: Target,
    ) -> tickwire_strategy_host::Result<Option<tickwire_strategy_host::FreshObservation>> {
        self.polled += 1;
        self.polled_targets.push(target.instance);
        Ok(None)
    }
    fn cancel_observation(&mut self, _: Target, _: [u8; 16]) -> tickwire_strategy_host::Result<()> {
        self.cancelled += 1;
        Ok(())
    }
}
fn outcome(f: &Fixture, id: u8) -> Outcome {
    serde_json::from_slice(
        &fs::read(
            f.config
                .outbox_directory
                .join(format!("{}.json", store::hex(&[id; 16]))),
        )
        .unwrap(),
    )
    .unwrap()
}
#[test]
fn conflicting_request_cancels_original_pending_and_permanently_closes_id() {
    let mut f = Fixture::new();
    let mut strategy = PendingStrategy::new(&mut f);
    let (policy, _) = crate::control::tests::inputs();
    let domain = policy.domain().unwrap();
    let mut inbox =
        ObservationInbox::open_with(f.config.clone(), &policy, policy.build().unwrap(), || {
            Ok(MonoTime::new(domain, 100))
        })
        .unwrap();
    let mut r = request(&inbox, 1, 2_000_000_000);
    write_request(&f, &r);
    inbox
        .poll_with(&mut strategy, || Ok(MonoTime::new(domain, 110)))
        .unwrap();
    assert_eq!(strategy.began, 1);
    r.require_resources = true;
    files::Directory::open(&f.config.inbox_directory, f.config.owner_uid)
        .unwrap()
        .replace(
            &format!("{}.json", store::hex(&r.request_id)),
            &serde_json::to_vec(&r).unwrap(),
        )
        .unwrap();
    inbox
        .poll_with(&mut strategy, || Ok(MonoTime::new(domain, 20_000_000)))
        .unwrap();
    assert_eq!(
        strategy.cancelled, 1,
        "conflict must cancel the original pending owner"
    );
    assert!(!inbox.entries[&r.request_id].pending);
    let out = outcome(&f, 1);
    assert_eq!(out.status, Status::Rejected);
    assert_eq!(
        out.request_digest,
        tickwire_recovery::digest(&serde_json::to_vec(&r).unwrap())
    );
    let polls = strategy.polled;
    inbox
        .poll_with(&mut strategy, || Ok(MonoTime::new(domain, 40_000_000)))
        .unwrap();
    assert_eq!(strategy.polled, polls);
    assert_eq!(strategy.began, 1);
}
#[test]
fn replacing_lock_path_closes_original_writer_before_any_new_wal_record() {
    let f = Fixture::new();
    let (policy, _) = crate::control::tests::inputs();
    let binding = Binding::from_config(&policy);
    let (mut storage, _) = store::Storage::open(&f.config, &binding).unwrap();
    fs::rename(
        f.config.journal_directory.join("LOCK"),
        f.config.journal_directory.join("old-lock"),
    )
    .unwrap();
    files::Directory::open(&f.config.journal_directory, f.config.owner_uid)
        .unwrap()
        .open_file("LOCK", true, true)
        .unwrap();
    assert!(
        storage.check().is_err(),
        "held lock must still own the current pathname"
    );
    assert!(
        storage
            .record(&Record::Session {
                config_digest: [1; 32],
                session: Session {
                    schema: 1,
                    session_id: [2; 16],
                    session_number: 1,
                    binding,
                    started_mono_ns: 100
                }
            })
            .is_err()
    );
}

#[test]
fn exact_retries_do_not_dispatch_twice_and_same_owner_is_busy() {
    let mut f = Fixture::new();
    let mut strategy = PendingStrategy::new(&mut f);
    let (policy, _) = crate::control::tests::inputs();
    let domain = policy.domain().unwrap();
    let mut inbox =
        ObservationInbox::open_with(f.config.clone(), &policy, policy.build().unwrap(), || {
            Ok(MonoTime::new(domain, 100))
        })
        .unwrap();
    write_request(&f, &request(&inbox, 1, 2_000_000_000));
    inbox
        .poll_with(&mut strategy, || Ok(MonoTime::new(domain, 110)))
        .unwrap();
    write_request(&f, &request(&inbox, 2, 2_000_000_000));
    inbox
        .poll_with(&mut strategy, || Ok(MonoTime::new(domain, 20_000_000)))
        .unwrap();
    assert_eq!(strategy.began, 1);
    assert_eq!(outcome(&f, 2).status, Status::Busy);
    assert!(inbox.entries[&[1; 16]].pending);
}
#[test]
fn wrong_build_binding_instance_and_expired_requests_never_dispatch() {
    for reason in 0..5 {
        let mut f = Fixture::new();
        let mut strategy = PendingStrategy::new(&mut f);
        if reason == 0 {
            f.config.allowed_build_hashes = vec![[1; 32]];
        }
        let (policy, _) = crate::control::tests::inputs();
        let domain = policy.domain().unwrap();
        let mut inbox =
            ObservationInbox::open_with(f.config.clone(), &policy, policy.build().unwrap(), || {
                Ok(MonoTime::new(domain, 100))
            })
            .unwrap();
        let mut r = request(&inbox, 1, 2_000_000_000);
        match reason {
            1 => r.binding.writer_epoch += 1,
            2 => r.target.generation += 1,
            3 => r.valid_until_mono_ns = 109,
            4 => r.session_id = [99; 16],
            _ => {}
        }
        write_request(&f, &r);
        inbox
            .poll_with(&mut strategy, || Ok(MonoTime::new(domain, 110)))
            .unwrap();
        assert_eq!(strategy.began, 0);
        assert_eq!(
            outcome(&f, 1).status,
            if reason == 3 {
                Status::Expired
            } else {
                Status::Rejected
            }
        );
    }
}
#[test]
fn syncing_received_past_deadline_preserves_history_without_dispatch() {
    let mut f = Fixture::new();
    let mut strategy = PendingStrategy::new(&mut f);
    let (policy, _) = crate::control::tests::inputs();
    let domain = policy.domain().unwrap();
    let mut inbox =
        ObservationInbox::open_with(f.config.clone(), &policy, policy.build().unwrap(), || {
            Ok(MonoTime::new(domain, 100))
        })
        .unwrap();
    write_request(&f, &request(&inbox, 1, 200));
    let mut samples = 0;
    inbox
        .poll_with(&mut strategy, || {
            samples += 1;
            Ok(MonoTime::new(domain, if samples <= 2 { 110 } else { 210 }))
        })
        .unwrap();
    assert_eq!(strategy.began, 0);
    assert_eq!(outcome(&f, 1).status, Status::Expired);
    drop(inbox);
    f.config.recover = true;
    let recovered =
        ObservationInbox::open_with(f.config.clone(), &policy, policy.build().unwrap(), || {
            Ok(MonoTime::new(domain, 300))
        })
        .unwrap();
    assert_eq!(recovered.session().session_number, 2);
    let out = outcome(&f, 1);
    assert!(out.historical);
    assert_eq!(out.status, Status::Expired);
    assert_eq!(out.request.valid_until_mono_ns, 200);
}
#[test]
fn recovery_interrupts_pending_without_reissuing_and_keeps_closed_conflicts() {
    for conflict in [false, true] {
        let mut f = Fixture::new();
        let mut strategy = PendingStrategy::new(&mut f);
        let (policy, _) = crate::control::tests::inputs();
        let domain = policy.domain().unwrap();
        let mut inbox =
            ObservationInbox::open_with(f.config.clone(), &policy, policy.build().unwrap(), || {
                Ok(MonoTime::new(domain, 100))
            })
            .unwrap();
        let mut r = request(&inbox, 1, 2_000_000_000);
        write_request(&f, &r);
        inbox
            .poll_with(&mut strategy, || Ok(MonoTime::new(domain, 110)))
            .unwrap();
        if conflict {
            r.require_resources = true;
            files::Directory::open(&f.config.inbox_directory, f.config.owner_uid)
                .unwrap()
                .replace(
                    &format!("{}.json", store::hex(&r.request_id)),
                    &serde_json::to_vec(&r).unwrap(),
                )
                .unwrap();
            inbox
                .poll_with(&mut strategy, || Ok(MonoTime::new(domain, 20_000_000)))
                .unwrap();
        }
        drop(inbox);
        f.config.recover = true;
        let mut recovered =
            ObservationInbox::open_with(f.config.clone(), &policy, policy.build().unwrap(), || {
                Ok(MonoTime::new(domain, 40_000_000))
            })
            .unwrap();
        recovered
            .poll_with(&mut strategy, || Ok(MonoTime::new(domain, 40_000_010)))
            .unwrap();
        assert_eq!(strategy.began, 1);
        let out = outcome(&f, 1);
        assert!(out.historical);
        assert_eq!(
            out.status,
            if conflict {
                Status::Rejected
            } else {
                Status::Interrupted
            }
        );
        assert!(!recovered.entries[&r.request_id].pending);
    }
}
#[test]
fn provider_failure_cancels_query_without_marking_strategy_failed() {
    let mut f = Fixture::new();
    let mut strategy = PendingStrategy::new(&mut f);
    let (policy, _) = crate::control::tests::inputs();
    let domain = policy.domain().unwrap();
    let mut inbox =
        ObservationInbox::open_with(f.config.clone(), &policy, policy.build().unwrap(), || {
            Ok(MonoTime::new(domain, 100))
        })
        .unwrap();
    write_request(&f, &request(&inbox, 1, 2_000_000_000));
    inbox
        .poll_with(&mut strategy, || Ok(MonoTime::new(domain, 110)))
        .unwrap();
    fs::set_permissions(
        &f.config.outbox_directory,
        fs::Permissions::from_mode(0o755),
    )
    .unwrap();
    assert!(
        inbox
            .poll_with(&mut strategy, || Ok(MonoTime::new(domain, 20_000_000)))
            .is_err()
    );
    assert_eq!(strategy.cancelled, 1);
    assert!(inbox.error().is_some());
    assert!(!strategy.failed());
    assert_eq!(
        inbox
            .poll_with(&mut strategy, || panic!(
                "closed provider must not touch clock or filesystem"
            ))
            .unwrap(),
        0
    );
}
#[test]
fn before_next_scan_is_zero_allocation_and_does_not_poll_pending_owner() {
    let mut f = Fixture::new();
    let mut strategy = PendingStrategy::new(&mut f);
    let (policy, _) = crate::control::tests::inputs();
    let domain = policy.domain().unwrap();
    let mut inbox =
        ObservationInbox::open_with(f.config.clone(), &policy, policy.build().unwrap(), || {
            Ok(MonoTime::new(domain, 100))
        })
        .unwrap();
    write_request(&f, &request(&inbox, 1, 2_000_000_000));
    inbox
        .poll_with(&mut strategy, || Ok(MonoTime::new(domain, 110)))
        .unwrap();
    let counts = allocation_counter::measure(|| {
        assert_eq!(
            inbox
                .poll_with(&mut strategy, || Ok(MonoTime::new(domain, 111)))
                .unwrap(),
            0
        );
    });
    assert_eq!(counts.count_total, 0);
    assert_eq!(strategy.polled, 0);
}

#[test]
fn result_objects_above_batch_size_are_durable_and_recovery_checks_every_root() {
    let f = Fixture::new();
    let (policy, _) = crate::control::tests::inputs();
    let binding = Binding::from_config(&policy);
    let (storage, _) = store::Storage::open(&f.config, &binding).unwrap();
    let bytes = vec![0x45; 16 * 1024 * 1024 + 1];
    let root = storage.put_dependency(&bytes).unwrap();
    assert_eq!(storage.get_dependency(root).unwrap(), bytes);
    assert!(
        storage.put(&bytes).is_err(),
        "opaque bound must not raise typed Outcome bound"
    );
    let path = f
        .config
        .journal_directory
        .join(format!("object-{}", store::hex(&root.digest)));
    fs::write(&path, b"changed").unwrap();
    assert!(storage.get_dependency(root).is_err());
    fs::remove_file(path).unwrap();
    assert!(storage.get_dependency(root).is_err());
}

mod durable;

#[test]
fn pending_budget_rotates_between_actual_owners() {
    let mut f = Fixture::new();
    let mut strategy = PendingStrategy::new(&mut f);
    f.config.maximum_per_poll = 1;
    let mut identity = strategy
        .registry
        .observe([7; 16])
        .unwrap()
        .identity()
        .clone();
    identity.instance = [8; 16];
    identity.logical_strategy = 8;
    strategy
        .registry
        .spawn(tickwire_supervisor::LaunchSpec {
            identity,
            executable: Path::new("/bin/sleep").to_owned(),
            arguments: vec!["30".into()],
            working_directory: f.root.path().to_owned(),
            environment: BTreeMap::new(),
            uid: f.config.owner_uid,
            gid: fs::metadata("/proc/self").unwrap().gid(),
            inherit_output: false,
        })
        .unwrap();
    let (policy, _) = crate::control::tests::inputs();
    let domain = policy.domain().unwrap();
    let mut inbox =
        ObservationInbox::open_with(f.config.clone(), &policy, policy.build().unwrap(), || {
            Ok(MonoTime::new(domain, 100))
        })
        .unwrap();
    write_request(&f, &request(&inbox, 1, 2_000_000_000));
    let mut second = request(&inbox, 2, 2_000_000_000);
    second.target.instance = [8; 16];
    second.target.logical_strategy_id = 8;
    write_request(&f, &second);
    for now in [110, 20_000_000, 40_000_000, 60_000_000] {
        inbox
            .poll_with(&mut strategy, || Ok(MonoTime::new(domain, now)))
            .unwrap();
    }
    assert_eq!(strategy.began, 2);
    assert!(strategy.polled_targets.contains(&[7; 16]));
    assert!(
        strategy.polled_targets.contains(&[8; 16]),
        "later owner must receive its bounded polling turn"
    );
}

#[test]
fn multi_routes_exact_actual_instance_without_generation_fallback() {
    let mut f = Fixture::new();
    let strategy = PendingStrategy::new(&mut f);
    let (policy, _) = crate::control::tests::inputs();
    let mut multi =
        crate::strategy::MultiStrategy::new(vec![(7, Box::new(strategy))], policy.build().unwrap())
            .unwrap();
    let exact = Target {
        logical_strategy_id: 7,
        instance: [7; 16],
        generation: 1,
    };
    assert!(exact.matches(&multi.observation_process(exact).unwrap()));
    for target in [
        Target {
            generation: 2,
            ..exact
        },
        Target {
            logical_strategy_id: 8,
            ..exact
        },
        Target {
            instance: [8; 16],
            ..exact
        },
    ] {
        assert!(multi.observation_process(target).is_err());
    }
}
#[test]
fn pending_deadline_cancels_only_query_and_does_not_resume_on_retry() {
    let mut f = Fixture::new();
    let mut strategy = PendingStrategy::new(&mut f);
    let (policy, _) = crate::control::tests::inputs();
    let domain = policy.domain().unwrap();
    let mut inbox =
        ObservationInbox::open_with(f.config.clone(), &policy, policy.build().unwrap(), || {
            Ok(MonoTime::new(domain, 100))
        })
        .unwrap();
    write_request(&f, &request(&inbox, 1, 15_000_000));
    inbox
        .poll_with(&mut strategy, || Ok(MonoTime::new(domain, 110)))
        .unwrap();
    inbox
        .poll_with(&mut strategy, || Ok(MonoTime::new(domain, 20_000_000)))
        .unwrap();
    assert_eq!(strategy.began, 1);
    assert_eq!(strategy.cancelled, 1);
    assert!(!strategy.failed());
    assert_eq!(outcome(&f, 1).status, Status::Expired);
    inbox
        .poll_with(&mut strategy, || Ok(MonoTime::new(domain, 40_000_000)))
        .unwrap();
    assert_eq!(strategy.began, 1);
}
