#[allow(dead_code)]
mod deployment_support;
use deployment_support::Fixture;
use std::{
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tickwire_config::{
    Candidate, Format, Limits as ParseLimits,
    changes::{Authority, Class, Role, RoleGrant},
    delivery::ProjectionDelivery,
    patch::Head,
    plan::{Action, ApplyCeiling, PlanOptions},
    prepared::{Input, PreparedCandidate},
};
use tickwire_config_service::{
    Error,
    bootstrap::{CommittedHead, HeadRead},
    deployment::Limits,
    deployment_owner::{Assessment, Job, Owner, PlanAssessment, PlanJob},
    source_bundle::{Bundle, Limits as BundleLimits},
};
fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .try_into()
        .unwrap()
}
fn candidate(f: &Fixture) -> Candidate {
    Candidate::parse(
        Format::Json,
        f.candidate.canonical_bytes(),
        ParseLimits::default(),
    )
    .unwrap()
}
fn bundle(f: &Fixture) -> Bundle {
    Bundle::decode(&f.bundle_bytes(), BundleLimits::default()).unwrap()
}
fn fixture() -> Fixture {
    Fixture::new_at(now(), |_| {}, |_, _| {})
}
fn owner(f: &Fixture, max: usize) -> Owner {
    let mut limits = Limits::default();
    limits.accounts.max_age_ms = 60_000;
    limits.credentials.max_age_ms = 60_000;
    limits.resources.max_age_ms = 60_000;
    Owner::new(bundle(f), f.trust(1), limits, max).unwrap()
}
fn finish(o: &mut Owner, job: &mut Job) -> Result<Assessment, Error> {
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        match o.poll(job)? {
            Some(result) => return Ok(result),
            None => assert!(Instant::now() < deadline, "bounded computation must finish"),
        }
        thread::sleep(Duration::from_millis(1));
    }
}
#[test]
fn actual_background_work_uses_service_time_and_retains_capacity_until_consumed() {
    let f = fixture();
    let mut o = owner(&f, 1);
    let before = now();
    let mut job = o.start(candidate(&f)).unwrap();
    assert_eq!(o.in_flight(), 1);
    assert!(o.start(candidate(&f)).is_err());
    let deadline = Instant::now() + Duration::from_secs(8);
    while !job.computation_finished() {
        assert!(Instant::now() < deadline);
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(
        o.in_flight(),
        1,
        "completed unconsumed result still consumes capacity"
    );
    assert!(o.start(candidate(&f)).is_err());
    let assessment = finish(&mut o, &mut job).unwrap();
    o.revalidate(&assessment).unwrap();
    let checked = assessment.content();
    assert_eq!(checked.candidate_hash(), f.candidate.hash());
    assert!(checked.checked_unix_ms() >= before);
    assert!(checked.checked_unix_ms() <= now());
    assert_eq!(o.in_flight(), 0);
    assert!(o.poll(&mut job).is_err());
    let mut next = o.start(candidate(&f)).unwrap();
    finish(&mut o, &mut next).unwrap();
}
#[test]
fn replaced_context_and_cross_owner_jobs_cannot_return_old_validation() {
    let f = fixture();
    let mut a = owner(&f, 2);
    let mut b = owner(&f, 1);
    let mut job = a.start(candidate(&f)).unwrap();
    assert!(b.poll(&mut job).is_err());
    finish(&mut a, &mut job).unwrap();
    let mut stale = a.start(candidate(&f)).unwrap();
    a.replace(bundle(&f), f.trust(2), Limits::default())
        .unwrap();
    assert!(a.poll(&mut stale).is_err());
    drop(stale);
    let mut current = a.start(candidate(&f)).unwrap();
    let result = finish(&mut a, &mut current).unwrap();
    a.revalidate(&result).unwrap();
    assert!(b.revalidate(&result).is_err());
    assert_eq!(
        result
            .content()
            .accounts()
            .accounts()
            .values()
            .next()
            .unwrap()
            .evidence()
            .trust_hash(),
        f.trust(2).digest()
    );
    a.replace(bundle(&f), f.trust(3), Limits::default())
        .unwrap();
    assert!(a.revalidate(&result).is_err());
}
#[test]
fn failed_replacement_preserves_previous_context_and_dropped_jobs_release_capacity() {
    let f = fixture();
    let mut o = owner(&f, 1);
    let mut invalid = Limits::default();
    invalid.resources.max_age_ms = 0;
    assert!(o.replace(bundle(&f), f.trust(2), invalid).is_err());
    let job = o.start(candidate(&f)).unwrap();
    drop(job);
    let deadline = Instant::now() + Duration::from_secs(8);
    while o.in_flight() != 0 {
        assert!(Instant::now() < deadline);
        thread::sleep(Duration::from_millis(1));
    }
    let mut job = o.start(candidate(&f)).unwrap();
    let result = finish(&mut o, &mut job).unwrap();
    assert_eq!(
        result
            .content()
            .reports()
            .reports()
            .values()
            .next()
            .unwrap()
            .trust_hash(),
        f.trust(1).digest()
    );
    for count in [0, 9, usize::MAX] {
        assert!(Owner::new(bundle(&f), f.trust(1), Limits::default(), count).is_err());
    }
}
#[test]
fn stale_signed_observations_fail_with_service_time_without_http_supplied_time() {
    let f = Fixture::new_at(now() - 60_000, |_| {}, |_, _| {});
    let mut o = owner(&f, 1);
    let mut job = o.start(candidate(&f)).unwrap();
    assert!(finish(&mut o, &mut job).is_err());
    assert_eq!(o.in_flight(), 0);
}

struct PlanCluster {
    dir: std::path::PathBuf,
    nodes: Vec<tickwire_config_service::Replica<tickwire_journal::store::FileStore>>,
}
impl PlanCluster {
    fn new(candidate: &Candidate) -> Self {
        use ring::signature::{Ed25519KeyPair, KeyPair as _};
        use tickwire_config_service::{bootstrap::*, *};
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "tickwire-plan-owner-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        std::fs::create_dir(&dir).unwrap();
        let key = Ed25519KeyPair::from_seed_unchecked(&[31; 32]).unwrap();
        let trust = BootstrapTrust::new(
            [19; 16],
            [0, 0, 0, 0, 0, 0, 64, 0, 128, 0, 0, 0, 0, 0, 0, 1],
            "tickwire-qualification".into(),
            key.public_key().as_ref().try_into().unwrap(),
        )
        .unwrap();
        let nodes = (1..=3)
            .map(|id| {
                Replica::new_with_bootstrap(
                    tickwire_control_raft::Node::create(
                        tickwire_journal::store::FileStore::open(dir.join(id.to_string())).unwrap(),
                        tickwire_control_raft::Config {
                            physical: tickwire_journal::Config::new([id as u8; 16], [18; 16]),
                            node_id: id,
                            voters: [1, 2, 3],
                            writer_epoch: 1,
                            max_command_bytes: MAX_COMMAND_BYTES,
                            max_retained_bytes: 32 * 1024 * 1024,
                        },
                    )
                    .unwrap(),
                    Config {
                        cluster: [19; 16],
                        objects: ObjectLimits {
                            max_objects: 8,
                            max_inflight: 4,
                            max_payload_bytes: 4 * 1024 * 1024,
                        },
                    },
                    trust.clone(),
                )
                .unwrap()
            })
            .collect();
        let mut c = Self { dir, nodes };
        c.nodes[0].campaign().unwrap();
        c.pump(false);
        c.nodes[0].initialize().unwrap();
        c.pump(false);
        let object = [6; 16];
        let owner = [7; 16];
        c.nodes[0]
            .propose(ObjectCommand::Begin(ObjectSpec {
                key: object,
                owner,
                purpose: Purpose::Candidate,
                length: candidate.canonical_bytes().len() as u32,
                hash: candidate.hash(),
            }))
            .unwrap();
        c.pump(false);
        for (i, bytes) in candidate.canonical_bytes().chunks(CHUNK_BYTES).enumerate() {
            c.nodes[0]
                .propose(ObjectCommand::Chunk {
                    key: object,
                    owner,
                    offset: (i * CHUNK_BYTES) as u32,
                    bytes: bytes.to_vec(),
                })
                .unwrap();
            c.pump(false);
        }
        c.nodes[0]
            .propose(ObjectCommand::Seal { key: object, owner })
            .unwrap();
        c.pump(false);
        let nonce = [10; 16];
        let signature = key
            .sign(
                &trust
                    .signing_bytes(object, candidate.hash(), nonce)
                    .unwrap(),
            )
            .as_ref()
            .try_into()
            .unwrap();
        c.nodes[0]
            .bootstrap(Bootstrap {
                candidate: object,
                candidate_hash: candidate.hash(),
                nonce,
                signature,
            })
            .unwrap();
        c.pump(false);
        c
    }
    fn pump(&mut self, isolated: bool) {
        for _ in 0..128 {
            let mut messages = Vec::new();
            for node in &mut self.nodes {
                let out = node.drive().unwrap();
                assert!(out.applied.iter().all(|a| a.result().is_ok()));
                messages.extend(out.messages);
            }
            if messages.is_empty() {
                return;
            }
            for message in messages {
                if isolated && (message.from == 1 || message.to == 1) {
                    continue;
                }
                self.nodes[(message.to - 1) as usize]
                    .step(message.from, message)
                    .unwrap();
            }
        }
        panic!("raft did not settle")
    }
}
impl Drop for PlanCluster {
    fn drop(&mut self) {
        self.nodes.clear();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn committed(candidate: &Candidate) -> CommittedHead {
    let mut cluster = PlanCluster::new(candidate);
    let ticket = cluster.nodes[0].request_read().unwrap();
    cluster.pump(false);
    match cluster.nodes[0].read_head(&ticket).unwrap() {
        HeadRead::Ready(Some(head)) => *head,
        _ => panic!("expected committed configuration after actual safe read"),
    }
}
fn prepared(base: &CommittedHead, next: &Candidate) -> PreparedCandidate {
    PreparedCandidate::prepare(
        &Head::new(base.revision(), base.candidate()).unwrap(),
        &base.etag(),
        base.candidate(),
        Input::Full(next),
        &Authority::from_grants(vec![RoleGrant {
            role: Role::Editor,
            accounts: Default::default(),
            cluster_scope: true,
        }]),
        ParseLimits::default(),
    )
    .unwrap()
}
fn finish_plan(o: &mut Owner, job: &mut PlanJob) -> Result<PlanAssessment, Error> {
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        match o.poll_plan(job)? {
            Some(result) => return Ok(result),
            None => assert!(
                Instant::now() < deadline,
                "bounded plan computation must finish"
            ),
        }
        thread::sleep(Duration::from_millis(1));
    }
}
fn changed_base(f: &Fixture) -> Candidate {
    let mut value = f.candidate.value().clone();
    value["strategies"][0]["params"]["signal_window_events"] = 512.into();
    deployment_support::parse(&value)
}
#[test]
fn plan_retains_actual_base_and_complete_old_new_projection_closure() {
    let f = fixture();
    let base = committed(&changed_base(&f));
    let mut o = owner(&f, 1);
    let mut job = o
        .start_plan(
            base.clone(),
            prepared(&base, &f.candidate),
            PlanOptions::default(),
        )
        .unwrap();
    assert_eq!(o.in_flight(), 1);
    assert!(
        o.start(candidate(&f)).is_err(),
        "plan and validation share one capacity"
    );
    let deadline = Instant::now() + Duration::from_secs(8);
    while !job.computation_finished() {
        assert!(Instant::now() < deadline);
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(o.in_flight(), 1, "unconsumed plan retains its lease");
    let assessment = finish_plan(&mut o, &mut job).unwrap();
    o.revalidate_plan(&assessment).unwrap();
    assert_eq!(assessment.base(), &base);
    assert_eq!(assessment.prepared().candidate(), &f.candidate);
    assert_eq!(assessment.content().candidate_hash(), f.candidate.hash());
    assert_eq!(assessment.delivery().base_hash(), base.candidate().hash());
    assert_eq!(assessment.delivery().candidate_hash(), f.candidate.hash());
    assert_eq!(assessment.plan().base_revision(), base.revision());
    assert_eq!(assessment.plan().base_etag(), base.etag());
    assert_eq!(assessment.plan().candidate_hash(), f.candidate.hash());
    assert_eq!(
        assessment.delivery().targets().len(),
        f.candidate.value()["processes"].as_array().unwrap().len()
    );
    for (id, target) in assessment.delivery().targets() {
        assert!(assessment.plan().targets().contains_key(id));
        assert_eq!(
            target.old_projection().unwrap().global_hash(),
            base.candidate().hash()
        );
        let new = target.new_projection().unwrap();
        assert_eq!(new.global_hash(), f.candidate.hash());
        assert_eq!(
            new.canonical_bytes(),
            assessment.content().projections().targets()[id].canonical_bytes()
        );
    }
    assert_eq!(o.in_flight(), 0);
    assert!(o.poll_plan(&mut job).is_err());
    let mut validation = o.start(candidate(&f)).unwrap();
    assert!(
        o.start_plan(
            base.clone(),
            prepared(&base, &f.candidate),
            PlanOptions::default()
        )
        .is_err()
    );
    finish(&mut o, &mut validation).unwrap();
}
#[test]
fn plan_rejects_foreign_base_context_and_replaced_completed_assessment() {
    let f = fixture();
    let base = committed(&f.candidate);
    let foreign = committed(&changed_base(&f));
    let mut o = owner(&f, 1);
    assert!(
        o.start_plan(
            foreign,
            prepared(&base, &f.candidate),
            PlanOptions::default()
        )
        .is_err()
    );
    assert_eq!(o.in_flight(), 0);
    let mut job = o
        .start_plan(
            base.clone(),
            prepared(&base, &f.candidate),
            PlanOptions::default(),
        )
        .unwrap();
    let mut other = owner(&f, 1);
    assert!(other.poll_plan(&mut job).is_err());
    let assessment = finish_plan(&mut o, &mut job).unwrap();
    assert!(other.revalidate_plan(&assessment).is_err());
    o.replace(bundle(&f), f.trust(2), Limits::default())
        .unwrap();
    assert!(o.revalidate_plan(&assessment).is_err());
    let mut stale = o
        .start_plan(
            base.clone(),
            prepared(&base, &f.candidate),
            PlanOptions::default(),
        )
        .unwrap();
    o.replace(bundle(&f), f.trust(3), Limits::default())
        .unwrap();
    assert!(o.poll_plan(&mut stale).is_err());
    drop(stale);
    let deadline = Instant::now() + Duration::from_secs(8);
    while o.in_flight() != 0 {
        assert!(Instant::now() < deadline);
        thread::sleep(Duration::from_millis(1));
    }
}
#[test]
fn plan_accounts_for_all_retained_projection_bytes_and_rejects_stale_facts() {
    let f = fixture();
    let base = committed(&f.candidate);
    let content = f.check(now()).unwrap();
    let delivery = ProjectionDelivery::prepare(
        base.candidate(),
        &prepared(&base, &f.candidate),
        &f.artifacts,
        Default::default(),
    )
    .unwrap();
    let required = content.projections().stored_bytes() + delivery.stored_bytes();
    for (limit, succeeds) in [(required - 1, false), (required, true)] {
        let mut limits = Limits::default();
        limits.accounts.max_age_ms = 60_000;
        limits.credentials.max_age_ms = 60_000;
        limits.resources.max_age_ms = 60_000;
        limits.projections.max_total_bytes = limit;
        let mut o = Owner::new(bundle(&f), f.trust(1), limits, 1).unwrap();
        let mut job = o
            .start_plan(
                base.clone(),
                prepared(&base, &f.candidate),
                PlanOptions::default(),
            )
            .unwrap();
        assert_eq!(finish_plan(&mut o, &mut job).is_ok(), succeeds);
        assert_eq!(o.in_flight(), 0);
    }
    let stale = Fixture::new_at(now() - 60_000, |_| {}, |_, _| {});
    let base = committed(&stale.candidate);
    let mut o = owner(&stale, 1);
    let mut job = o
        .start_plan(
            base.clone(),
            prepared(&base, &stale.candidate),
            PlanOptions::default(),
        )
        .unwrap();
    assert!(finish_plan(&mut o, &mut job).is_err());
    assert_eq!(o.in_flight(), 0);
}

#[test]
fn new_content_cannot_hide_missing_old_projection_policy() {
    let f = fixture();
    let mut old = f.candidate.value().clone();
    old["local_config"]["projection_policy_ref"] = format!(
        "artifact://fixture/missing-old-policy@sha256:{}",
        "ab".repeat(32)
    )
    .into();
    let base = committed(&deployment_support::parse(&old));
    let mut o = owner(&f, 1);
    let mut validation = o.start(candidate(&f)).unwrap();
    finish(&mut o, &mut validation).unwrap();
    let mut job = o
        .start_plan(
            base.clone(),
            prepared(&base, &f.candidate),
            PlanOptions::default(),
        )
        .unwrap();
    assert_eq!(
        finish_plan(&mut o, &mut job).err(),
        Some(Error::Configuration(tickwire_config::Error::Semantic(
            "missing projection policy"
        )))
    );
    assert_eq!(o.in_flight(), 0);
}

#[test]
fn fully_qualified_risk_tightening_retains_the_global_qualification_refresh_gate() {
    let observed = now();
    let old = Fixture::new_at(observed, |_| {}, |_, _| {});
    let mut next = Fixture::new_at(
        observed,
        |v| v["risk_policies"][0]["max_gross_quote"] = "10000.00".into(),
        |_, _| {},
    );
    // Both sides have independently valid signed report graphs. Reusing next's
    // report references in a changed old candidate would hide this boundary.
    old.check(observed).unwrap();
    next.check(observed).unwrap();
    for (reference, bytes) in old.artifacts.objects() {
        next.artifacts.insert(reference, bytes).unwrap();
    }
    for (_, _, envelope) in old.signatures.envelopes() {
        next.signatures.insert(envelope).unwrap();
    }
    let base = committed(&old.candidate);
    let candidate = prepared(&base, &next.candidate);
    assert_eq!(candidate.changes().class(), Class::Quiesce);
    assert!(candidate.changes().requires_cluster_admin());
    assert_ne!(
        old.candidate.value()["qualification"]["deployment_manifest_ref"],
        next.candidate.value()["qualification"]["deployment_manifest_ref"]
    );
    let mut o = owner(&next, 1);
    let mut hot = o
        .start_plan(
            base.clone(),
            candidate,
            PlanOptions {
                apply_ceiling: ApplyCeiling::Hot,
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(
        finish_plan(&mut o, &mut hot).err(),
        Some(Error::Configuration(tickwire_config::Error::Semantic(
            "plan apply ceiling"
        )))
    );
    let mut full = o
        .start_plan(
            base.clone(),
            prepared(&base, &next.candidate),
            PlanOptions::default(),
        )
        .unwrap();
    let assessment = finish_plan(&mut o, &mut full).unwrap();
    assert_eq!(assessment.plan().required_class(), Class::Quiesce);
    assert_eq!(
        assessment.plan().targets().len(),
        old.candidate.value()["processes"].as_array().unwrap().len()
    );
    for target in assessment.plan().targets().values() {
        assert_eq!(target.action(), Action::Quiesce);
        assert!(
            target
                .changes()
                .iter()
                .any(|path| path.starts_with("/qualification/"))
        );
    }
}
