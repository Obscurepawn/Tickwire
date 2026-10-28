use super::*;
use std::io::Read as _;
use tickwire_config::{patch::Head, plan::PlanOptions};
use tickwire_config_service::{
    Change, Read,
    plan_archive::{PendingRecord, RecordProgress},
    planning::{PlanDraft, PlanInput},
    publication::{PreparedQuery, PreparedRead, StoredPrepared},
};
use tickwire_control_raft::driver::PeerDriver;
use tickwire_control_raft::snapshot::{MemoryObjects, ObjectRef, SnapshotLimits, SnapshotReader};

const KEY: &str = "archive-once";

fn archive_fixture(edit: impl FnOnce(&mut serde_json::Value)) -> deployment_support::Fixture {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    deployment_support::Fixture::new_at_with_lifetime(
        now,
        60_000,
        |v| {
            v["control"]["role_bindings"] = serde_json::json!([{
                "subject": SUBJECT, "role":"config-editor", "account_ids":[], "cluster_scope":true
            }]);
            edit(v);
        },
        |_, _| {},
    )
}

// This flow performs hundreds of actual three-voter syncs. Ordinary TLS tests
// retain their original 3s peer timeout; only this integration exchange waits 30s.
fn archive_exchange(
    server: ServerTls,
    client: Arc<rustls::ClientConfig>,
    inspect: impl FnOnce(&AuthenticatedPrincipal) + Send + 'static,
) -> (Result<(String, Hash)>, bool) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let worker = thread::spawn(move || {
        let (socket, _) = listener.accept().unwrap();
        let mut connection = server.accept(socket, Duration::from_secs(30), 65536)?;
        let mut bytes = [0; 4];
        connection
            .read_exact(&mut bytes)
            .map_err(|_| Error::Transport)?;
        assert_eq!(&bytes, b"PING");
        server.revalidate(connection.principal())?;
        inspect(connection.principal());
        connection
            .write_all(b"PONG")
            .map_err(|_| Error::Transport)?;
        connection.flush().map_err(|_| Error::Transport)?;
        Ok((
            connection.principal().subject().to_owned(),
            connection.principal().leaf_sha256(),
        ))
    });
    let socket = TcpStream::connect(address).unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(30)))
        .unwrap();
    socket
        .set_write_timeout(Some(Duration::from_secs(30)))
        .unwrap();
    let connection =
        rustls::ClientConnection::new(client, "localhost".try_into().unwrap()).unwrap();
    let mut stream = rustls::StreamOwned::new(connection, socket);
    let sent = stream.write_all(b"PING").and_then(|_| stream.flush());
    let mut answer = [0; 4];
    let received = sent.is_ok() && stream.read_exact(&mut answer).is_ok() && &answer == b"PONG";
    (worker.join().unwrap(), received)
}

fn cluster(sources: &deployment_support::Fixture) -> ApiCluster {
    let mut cluster = ApiCluster::new_with_object_limits(&sources.candidate, 512, 64 * 1024 * 1024);
    cluster.nodes[0]
        .control()
        .install_deployment_owner(source_owner(sources))
        .unwrap();
    cluster
}

fn draft(
    cluster: &mut ApiCluster,
    trust: &ServerTls,
    principal: &AuthenticatedPrincipal,
    base: &Candidate,
    candidate: Candidate,
    options: PlanOptions,
) -> PlanDraft {
    let mut pending = cluster.nodes[0]
        .control()
        .begin_plan(
            trust,
            principal,
            PlanInput::Full(candidate),
            Head::new(1, base).unwrap().etag(),
            options,
        )
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        cluster.pump(false);
        if let Some(draft) = cluster.nodes[0]
            .control()
            .poll_plan(trust, &mut pending)
            .unwrap()
        {
            return draft;
        }
        assert!(Instant::now() < deadline, "plan did not complete");
        thread::sleep(Duration::from_millis(1));
    }
}

#[derive(Default)]
struct Observed {
    objects: usize,
    proposals: usize,
    prepared: usize,
    first_record: Option<(u64, u64)>,
}

// Same actual drive/transport loop as ApiCluster::pump, with node 0's receipts
// delivered to the real pending proposal owner exactly once.
fn pump_record(
    cluster: &mut ApiCluster,
    pending: &mut PendingRecord,
    isolated: bool,
    observed: &mut Observed,
) {
    pump_records(cluster, &mut [pending], isolated, observed, false);
}

fn pump_records(
    cluster: &mut ApiCluster,
    pending: &mut [&mut PendingRecord],
    isolated: bool,
    observed: &mut Observed,
    allow_record_conflict: bool,
) {
    for _ in 0..128 {
        let mut messages = Vec::new();
        for (index, node) in cluster.nodes.iter_mut().enumerate() {
            let output = node.drive().unwrap();
            for applied in output.applied {
                let conflict = allow_record_conflict && applied.result() == Err(Error::Conflict);
                assert!(
                    applied.result().is_ok() || conflict,
                    "applied result: {:?}",
                    applied.result()
                );
                if index == 0 {
                    match applied.result() {
                        Ok(Change::Begun) => observed.objects += 1,
                        Ok(Change::PreparedStored { index, term, .. }) => {
                            observed.prepared += 1;
                            observed.first_record.get_or_insert((index, term));
                        }
                        _ => {}
                    }
                    let mut matched_record = false;
                    for pending in pending.iter_mut() {
                        if pending.observe(&applied).unwrap() {
                            matched_record |= pending.progress() == RecordProgress::Recording;
                            observed.proposals += 1;
                            assert!(
                                !pending.observe(&applied).unwrap(),
                                "same applied result consumed twice"
                            );
                        }
                    }
                    assert!(
                        !conflict || matched_record,
                        "only an actual competing RecordPrepared may conflict"
                    );
                }
            }
            messages.extend(output.messages);
        }
        if messages.is_empty() {
            return;
        }
        for message in messages {
            if isolated && (message.from == 1 || message.to == 1) {
                continue;
            }
            cluster.nodes[(message.to - 1) as usize]
                .step(message.from, message)
                .unwrap();
        }
    }
    panic!("record transport did not settle");
}

fn reach(
    cluster: &mut ApiCluster,
    trust: &ServerTls,
    pending: &mut PendingRecord,
    stage: RecordProgress,
    observed: &mut Observed,
) {
    let started = Instant::now();
    let deadline = started + Duration::from_secs(30);
    while pending.progress() != stage {
        assert!(
            Instant::now() < deadline,
            "record did not reach {stage:?}: {:?}",
            pending.progress()
        );
        let before = pending.progress();
        assert!(
            cluster.nodes[0]
                .control()
                .poll_record_plan(trust, pending)
                .unwrap_or_else(|error| panic!("record failed reaching {stage:?} from {before:?} after {:?}, {} objects / {} proposals: {error:?}", started.elapsed(), observed.objects, observed.proposals))
                .is_none()
        );
        if pending.progress() != stage {
            pump_record(cluster, pending, false, observed);
            thread::sleep(Duration::from_millis(1));
        }
    }
}

fn finish(
    cluster: &mut ApiCluster,
    trust: &ServerTls,
    pending: &mut PendingRecord,
    observed: &mut Observed,
) -> Result<StoredPrepared> {
    let started = Instant::now();
    let deadline = started + Duration::from_secs(30);
    loop {
        pump_record(cluster, pending, false, observed);
        let result = cluster.nodes[0].control().poll_record_plan(trust, pending);
        if let Some(stored) = result? {
            return Ok(stored);
        }
        assert!(
            Instant::now() < deadline,
            "record did not complete: {:?}",
            pending.progress()
        );
        thread::sleep(Duration::from_millis(1));
    }
}

fn saved(cluster: &mut ApiCluster, node: usize, key: &str) -> Option<StoredPrepared> {
    let read = cluster.nodes[node].request_read().unwrap();
    cluster.pump(false);
    let PreparedRead::Ready(view) = cluster.nodes[node]
        .read_prepared(
            &read,
            PreparedQuery::Idempotency {
                subject: SUBJECT,
                key,
            },
        )
        .unwrap()
    else {
        panic!("real prepared ReadIndex not ready");
    };
    assert_eq!(view.head().unwrap().revision(), 1);
    view.record().cloned()
}

fn desired_is_bootstrap(
    cluster: &mut ApiCluster,
    trust: &ServerTls,
    principal: &AuthenticatedPrincipal,
    base: &Candidate,
) {
    let request = tickwire_config_service::api::Request::parse(
        b"GET /v1/config/desired HTTP/1.1\r\nHost: localhost\r\n\r\n",
        1024,
    )
    .unwrap();
    let mut pending = cluster.nodes[0]
        .control()
        .begin_request(trust, principal, request)
        .unwrap();
    cluster.pump(false);
    let response = cluster.nodes[0]
        .control()
        .poll_request(trust, &mut pending)
        .unwrap()
        .unwrap();
    assert_eq!(response.status(), 200);
    let body: serde_json::Value = serde_json::from_slice(response.body()).unwrap();
    assert_eq!(body["revision"], "1");
    assert_eq!(body["origin"], "bootstrap");
    assert_eq!(body["config"], *base.value());
    assert!(body.get("operation_id").is_none());
}

struct MissingDependency<'a> {
    objects: &'a MemoryObjects,
    missing: ObjectRef,
}
impl SnapshotReader for MissingDependency<'_> {
    fn read(&self, reference: &ObjectRef) -> tickwire_control_raft::Result<Vec<u8>> {
        if *reference == self.missing {
            return Err(tickwire_control_raft::Error::Corrupt);
        }
        self.objects.read(reference)
    }
}

fn snapshot_and_restart(cluster: &mut ApiCluster, stored: &StoredPrepared) {
    let read = cluster.nodes[0].request_read().unwrap();
    cluster.pump(false);
    let Read::Ready(Some(original)) = cluster.nodes[0]
        .read_object(&read, stored.root().key)
        .unwrap()
    else {
        panic!("missing original archived root");
    };
    let mut objects = MemoryObjects::new(SnapshotLimits::default()).unwrap();
    let (cut, root) = cluster.nodes[0].capture_snapshot(&mut objects).unwrap();
    assert_eq!(root.schema(), 2);
    assert!(cut.index() >= stored.index());
    for node in &cluster.nodes {
        // Inspect each voter's actual replicated application snapshot. These
        // local checks are not advertised as follower linearizable reads.
        let (local_cut, local_root) = node.capture_snapshot(&mut objects).unwrap();
        assert_eq!(local_cut, cut);
        assert_eq!(local_root, root);
        let restored = node
            .prepare_snapshot(&local_cut, &local_root, &objects)
            .unwrap();
        assert_eq!(restored.cut(), &cut);
        assert_eq!(restored.root(), &root);
        assert_eq!(restored.head().unwrap().revision(), 1);
        assert_eq!(
            restored
                .prepared(PreparedQuery::Id(stored.id()))
                .unwrap()
                .unwrap(),
            *stored
        );
        assert_eq!(
            restored
                .prepared(PreparedQuery::Idempotency {
                    subject: SUBJECT,
                    key: KEY
                })
                .unwrap()
                .unwrap(),
            *stored
        );
        assert_eq!(restored.object(stored.root().key).unwrap(), original);
    }
    let dependencies: Vec<_> = objects
        .objects()
        .into_iter()
        .filter(|object| *object != root.root())
        .collect();
    assert!(
        dependencies.len() >= 2,
        "snapshot must include object payload and prepared records"
    );
    for missing in dependencies {
        assert!(
            cluster.nodes[1]
                .prepare_snapshot(
                    &cut,
                    &root,
                    &MissingDependency {
                        objects: &objects,
                        missing
                    }
                )
                .is_err()
        );
    }
    // Reopen each actual FileStore, then elect voter 2. Historical content and
    // original raft receipts must survive without replaying service admission.
    use ring::signature::{Ed25519KeyPair, KeyPair as _};
    let key = Ed25519KeyPair::from_seed_unchecked(&[31; 32]).unwrap();
    let trust = tickwire_config_service::bootstrap::BootstrapTrust::new(
        [19; 16],
        [0, 0, 0, 0, 0, 0, 64, 0, 128, 0, 0, 0, 0, 0, 0, 1],
        "tickwire-qualification".into(),
        key.public_key().as_ref().try_into().unwrap(),
    )
    .unwrap();
    cluster.nodes.clear();
    cluster.nodes = (1..=3)
        .map(|id| {
            let node = tickwire_control_raft::Node::recover(
                tickwire_journal::store::FileStore::open(cluster.dir.join(id.to_string())).unwrap(),
                tickwire_control_raft::Config {
                    physical: tickwire_journal::Config::new([id as u8; 16], [18; 16]),
                    node_id: id,
                    voters: [1, 2, 3],
                    writer_epoch: 1,
                    max_command_bytes: tickwire_config_service::MAX_COMMAND_BYTES,
                    max_retained_bytes: 32 * 1024 * 1024,
                },
            )
            .unwrap();
            tickwire_config_service::Replica::new_with_bootstrap(
                node,
                tickwire_config_service::Config {
                    cluster: [19; 16],
                    objects: tickwire_config_service::ObjectLimits {
                        max_objects: 512,
                        max_inflight: 4,
                        max_payload_bytes: 64 * 1024 * 1024,
                    },
                },
                trust.clone(),
            )
            .unwrap()
        })
        .collect();
    cluster.nodes[1].campaign().unwrap();
    cluster.pump(false);
    assert!(cluster.nodes[1].is_leader());
    assert_eq!(saved(cluster, 1, KEY).unwrap(), *stored);
    for node in &cluster.nodes {
        let mut objects = MemoryObjects::new(SnapshotLimits::default()).unwrap();
        let (cut, root) = node.capture_snapshot(&mut objects).unwrap();
        let recovered = node.prepare_snapshot(&cut, &root, &objects).unwrap();
        assert_eq!(
            recovered
                .prepared(PreparedQuery::Id(stored.id()))
                .unwrap()
                .unwrap(),
            *stored
        );
        assert_eq!(recovered.object(stored.root().key).unwrap(), original);
    }
}

#[test]
fn actual_archive_waits_for_quorum_and_all_voters_read_the_same_prepared_root() {
    let tls = Fixture::new(&[SUBJECT], false);
    let sources = archive_fixture(|_| {});
    let base = sources.candidate.clone();
    let mut cluster = cluster(&sources);
    let trust = tls.server();
    let completed = Arc::new(std::sync::Mutex::new(None));
    let output = Arc::clone(&completed);
    let (result, replied) = archive_exchange(trust.clone(), tls.client(true), move |principal| {
        let draft = draft(
            &mut cluster,
            &trust,
            principal,
            &base,
            base.clone(),
            PlanOptions::default(),
        );
        let targets = draft.plan().targets().len();
        let mut pending = cluster.nodes[0]
            .control()
            .begin_record_plan(&trust, principal, draft, KEY.into())
            .unwrap();
        let mut observed = Observed::default();
        for phase in [RecordProgress::Uploading, RecordProgress::Recording] {
            reach(&mut cluster, &trust, &mut pending, phase, &mut observed);
            let before = cluster.nodes[0].scanned_through();
            for _ in 0..4 {
                pump_record(&mut cluster, &mut pending, true, &mut observed);
                assert!(
                    cluster.nodes[0]
                        .control()
                        .poll_record_plan(&trust, &mut pending)
                        .unwrap()
                        .is_none()
                );
            }
            assert_eq!(cluster.nodes[0].scanned_through(), before);
            assert_eq!(observed.prepared, 0);
            for _ in 0..6 {
                cluster.nodes[0].tick().unwrap();
            }
        }
        reach(
            &mut cluster,
            &trust,
            &mut pending,
            RecordProgress::ReadingResult,
            &mut observed,
        );
        assert_eq!(observed.prepared, 1);
        pump_record(&mut cluster, &mut pending, true, &mut observed);
        assert!(
            cluster.nodes[0]
                .control()
                .poll_record_plan(&trust, &mut pending)
                .unwrap()
                .is_none()
        );
        for _ in 0..6 {
            cluster.nodes[0].tick().unwrap();
        }
        let stored = finish(&mut cluster, &trust, &mut pending, &mut observed).unwrap();
        assert!(
            observed.objects > targets,
            "archive must retain target objects and its input roots"
        );
        assert!(observed.proposals > observed.objects);
        assert_eq!(observed.prepared, 1);
        assert_eq!(stored.subject(), SUBJECT);
        assert_eq!(stored.leaf_sha256(), principal.leaf_sha256());
        assert_eq!(stored.idempotency_key(), KEY);
        assert_eq!(stored.if_match(), Head::new(1, &base).unwrap().etag());
        assert_eq!(pending.progress(), RecordProgress::Completed);
        assert!(
            cluster.nodes[0]
                .control()
                .poll_record_plan(&trust, &mut pending)
                .is_err()
        );
        assert_eq!(saved(&mut cluster, 0, KEY).unwrap(), stored);
        let read = cluster.nodes[0].request_read().unwrap();
        cluster.pump(false);
        let Read::Ready(Some(root)) = cluster.nodes[0]
            .read_object(&read, stored.root().key)
            .unwrap()
        else {
            panic!("missing actual archive root");
        };
        assert_eq!(root.spec(), stored.root());
        assert_eq!(Sha256::digest(root.bytes()).as_slice(), stored.root().hash);
        desired_is_bootstrap(&mut cluster, &trust, principal, &base);
        *output.lock().unwrap() = Some((cluster, stored));
    });
    assert!(result.is_ok() && replied);
    let (mut cluster, stored) = completed.lock().unwrap().take().unwrap();
    snapshot_and_restart(&mut cluster, &stored);
}

#[test]
fn canonical_request_retry_with_new_check_time_returns_original_record_without_upload() {
    let tls = Fixture::new(&[SUBJECT], false);
    let sources = archive_fixture(|_| {});
    let base = sources.candidate.clone();
    let mut cluster = cluster(&sources);
    let trust = tls.server();
    let (result, replied) = archive_exchange(trust.clone(), tls.client(true), move |principal| {
        let first = draft(
            &mut cluster,
            &trust,
            principal,
            &base,
            base.clone(),
            PlanOptions::default(),
        );
        let checked = first.content().checked_unix_ms();
        let mut pending = cluster.nodes[0]
            .control()
            .begin_record_plan(&trust, principal, first, KEY.into())
            .unwrap();
        let original =
            finish(&mut cluster, &trust, &mut pending, &mut Observed::default()).unwrap();
        let next = draft(
            &mut cluster,
            &trust,
            principal,
            &base,
            base.clone(),
            PlanOptions::default(),
        );
        assert!(next.content().checked_unix_ms() > checked);
        let mut retry = cluster.nodes[0]
            .control()
            .begin_record_plan(&trust, principal, next, KEY.into())
            .unwrap();
        let mut observed = Observed::default();
        let repeated = finish(&mut cluster, &trust, &mut retry, &mut observed).unwrap();
        assert_eq!(repeated, original);
        assert_eq!(observed.objects, 0);
        assert_eq!(observed.prepared, 0);
        assert_eq!(observed.proposals, 0);
        assert_eq!(saved(&mut cluster, 0, KEY).unwrap(), original);
        desired_is_bootstrap(&mut cluster, &trust, principal, &base);
    });
    assert!(result.is_ok() && replied);
}

#[test]
fn idempotency_key_rejects_different_candidate_or_options_without_new_record() {
    for change_candidate in [false, true] {
        let tls = Fixture::new(&[SUBJECT], false);
        let sources = archive_fixture(|_| {});
        let base = sources.candidate.clone();
        let mut cluster = cluster(&sources);
        let trust = tls.server();
        let (result, replied) =
            archive_exchange(trust.clone(), tls.client(true), move |principal| {
                let first = draft(
                    &mut cluster,
                    &trust,
                    principal,
                    &base,
                    base.clone(),
                    PlanOptions::default(),
                );
                let mut pending = cluster.nodes[0]
                    .control()
                    .begin_record_plan(&trust, principal, first, KEY.into())
                    .unwrap();
                let original =
                    finish(&mut cluster, &trust, &mut pending, &mut Observed::default()).unwrap();
                let mut options = PlanOptions::default();
                let candidate = if change_candidate {
                    let replacement = archive_fixture(|v| {
                        v["risk_policies"][0]["max_gross_quote"] = "20000000".into();
                    });
                    assert_ne!(replacement.candidate.hash(), base.hash());
                    cluster.nodes[0]
                        .control()
                        .replace_deployment_sources(
                            tickwire_config_service::source_bundle::Bundle::decode(
                                &replacement.bundle_bytes(),
                                Default::default(),
                            )
                            .unwrap(),
                            replacement.trust(1),
                            Default::default(),
                        )
                        .unwrap();
                    replacement.candidate
                } else {
                    options.apply_ceiling = tickwire_config::plan::ApplyCeiling::Hot;
                    base.clone()
                };
                let different = draft(&mut cluster, &trust, principal, &base, candidate, options);
                let mut retry = cluster.nodes[0]
                    .control()
                    .begin_record_plan(&trust, principal, different, KEY.into())
                    .unwrap();
                let mut observed = Observed::default();
                assert_eq!(
                    finish(&mut cluster, &trust, &mut retry, &mut observed).err(),
                    Some(Error::Conflict)
                );
                assert_eq!(observed.objects, 0);
                assert_eq!(observed.prepared, 0);
                assert_eq!(saved(&mut cluster, 0, KEY).unwrap(), original);
                desired_is_bootstrap(&mut cluster, &trust, principal, &base);
            });
        assert!(result.is_ok() && replied);
    }
}

#[test]
fn source_replacement_or_certificate_revocation_during_upload_cannot_record_success() {
    for revoke in [false, true] {
        let tls = Fixture::new(&[SUBJECT], false);
        let revoked = ServerTls::new(tls.material(true, false), vec![tls.peer()]).unwrap();
        let sources = archive_fixture(|_| {});
        let base = sources.candidate.clone();
        let mut cluster = cluster(&sources);
        let trust = tls.server();
        let (result, replied) =
            archive_exchange(trust.clone(), tls.client(true), move |principal| {
                let prepared = draft(
                    &mut cluster,
                    &trust,
                    principal,
                    &base,
                    base.clone(),
                    PlanOptions::default(),
                );
                let mut pending = cluster.nodes[0]
                    .control()
                    .begin_record_plan(&trust, principal, prepared, KEY.into())
                    .unwrap();
                let mut observed = Observed::default();
                reach(
                    &mut cluster,
                    &trust,
                    &mut pending,
                    RecordProgress::Uploading,
                    &mut observed,
                );
                let deadline = Instant::now() + Duration::from_secs(30);
                while observed.objects == 0 {
                    assert!(Instant::now() < deadline, "no archive object committed");
                    pump_record(&mut cluster, &mut pending, false, &mut observed);
                    assert!(
                        cluster.nodes[0]
                            .control()
                            .poll_record_plan(&trust, &mut pending)
                            .unwrap()
                            .is_none()
                    );
                }
                assert_eq!(pending.progress(), RecordProgress::Uploading);
                if !revoke {
                    cluster.nodes[0]
                        .control()
                        .replace_deployment_sources(
                            tickwire_config_service::source_bundle::Bundle::decode(
                                &sources.bundle_bytes(),
                                Default::default(),
                            )
                            .unwrap(),
                            sources.trust(1),
                            Default::default(),
                        )
                        .unwrap();
                }
                assert_eq!(
                    finish(
                        &mut cluster,
                        if revoke { &revoked } else { &trust },
                        &mut pending,
                        &mut observed
                    )
                    .err(),
                    Some(if revoke {
                        Error::Authentication
                    } else {
                        Error::Conflict
                    })
                );
                assert_eq!(pending.progress(), RecordProgress::Completed);
                assert_eq!(observed.prepared, 0);
                assert!(saved(&mut cluster, 0, KEY).is_none());
                desired_is_bootstrap(&mut cluster, &trust, principal, &base);
            });
        assert!(result.is_ok() && replied);
    }
}

#[test]
fn foreign_replica_cannot_consume_ticket_and_cancelled_upload_stays_unrecorded() {
    let tls = Fixture::new(&[SUBJECT], false);
    let sources = archive_fixture(|_| {});
    let base = sources.candidate.clone();
    let mut cluster = cluster(&sources);
    let trust = tls.server();
    let (result, replied) = archive_exchange(trust.clone(), tls.client(true), move |principal| {
        let prepared = draft(
            &mut cluster,
            &trust,
            principal,
            &base,
            base.clone(),
            PlanOptions::default(),
        );
        let mut pending = cluster.nodes[0]
            .control()
            .begin_record_plan(&trust, principal, prepared, KEY.into())
            .unwrap();
        assert_eq!(
            cluster.nodes[1]
                .control()
                .poll_record_plan(&trust, &mut pending)
                .err(),
            Some(Error::Identity)
        );
        assert_eq!(
            cluster.nodes[1].control().cancel_record_plan(&mut pending),
            Err(Error::Identity)
        );
        assert_eq!(pending.progress(), RecordProgress::ReadingExisting);
        let mut observed = Observed::default();
        reach(
            &mut cluster,
            &trust,
            &mut pending,
            RecordProgress::Uploading,
            &mut observed,
        );
        assert!(
            cluster.nodes[0]
                .control()
                .poll_record_plan(&trust, &mut pending)
                .unwrap()
                .is_none()
        );
        pump_record(&mut cluster, &mut pending, false, &mut observed);
        assert_eq!(observed.objects, 1);
        assert!(
            cluster.nodes[0]
                .control()
                .cancel_record_plan(&mut pending)
                .unwrap()
        );
        assert!(
            !cluster.nodes[0]
                .control()
                .cancel_record_plan(&mut pending)
                .unwrap()
        );
        assert_eq!(pending.progress(), RecordProgress::Completed);
        assert_eq!(
            cluster.nodes[0]
                .control()
                .poll_record_plan(&trust, &mut pending)
                .err(),
            Some(Error::NotReady)
        );
        cluster.pump(false);
        assert!(saved(&mut cluster, 0, KEY).is_none());
        desired_is_bootstrap(&mut cluster, &trust, principal, &base);
    });
    assert!(result.is_ok() && replied);
}

#[test]
fn concurrent_same_key_uploads_return_the_first_committed_record_after_real_safe_reads() {
    let tls = Fixture::new(&[SUBJECT], false);
    let sources = archive_fixture(|_| {});
    let base = sources.candidate.clone();
    let mut cluster = cluster(&sources);
    let trust = tls.server();
    let (result, replied) = archive_exchange(trust.clone(), tls.client(true), move |principal| {
        let a = draft(
            &mut cluster,
            &trust,
            principal,
            &base,
            base.clone(),
            PlanOptions::default(),
        );
        let a_checked = a.content().checked_unix_ms();
        let b = draft(
            &mut cluster,
            &trust,
            principal,
            &base,
            base.clone(),
            PlanOptions::default(),
        );
        assert!(b.content().checked_unix_ms() > a_checked);
        let mut a = cluster.nodes[0]
            .control()
            .begin_record_plan(&trust, principal, a, KEY.into())
            .unwrap();
        let mut b = cluster.nodes[0]
            .control()
            .begin_record_plan(&trust, principal, b, KEY.into())
            .unwrap();
        let mut observed = Observed::default();
        let deadline = Instant::now() + Duration::from_secs(30);
        // Neither request may record until both have uploaded their closure and
        // received the last pre-admission safe read, while no record exists.
        while a.progress() != RecordProgress::Rechecking
            || b.progress() != RecordProgress::Rechecking
        {
            assert!(
                Instant::now() < deadline,
                "concurrent upload stalled: {:?}/{:?}",
                a.progress(),
                b.progress()
            );
            pump_records(
                &mut cluster,
                &mut [&mut a, &mut b],
                false,
                &mut observed,
                false,
            );
            for pending in [&mut a, &mut b] {
                if pending.progress() != RecordProgress::Rechecking {
                    assert!(
                        cluster.nodes[0]
                            .control()
                            .poll_record_plan(&trust, pending)
                            .unwrap()
                            .is_none()
                    );
                }
            }
            thread::sleep(Duration::from_millis(1));
        }
        pump_records(
            &mut cluster,
            &mut [&mut a, &mut b],
            false,
            &mut observed,
            false,
        );
        assert_eq!(observed.prepared, 0);
        for pending in [&mut a, &mut b] {
            assert!(
                cluster.nodes[0]
                    .control()
                    .poll_record_plan(&trust, pending)
                    .unwrap()
                    .is_none()
            );
            assert_eq!(pending.progress(), RecordProgress::Recording);
        }
        pump_records(
            &mut cluster,
            &mut [&mut a, &mut b],
            false,
            &mut observed,
            true,
        );
        assert!(observed.first_record.is_some());
        for pending in [&mut a, &mut b] {
            assert!(
                cluster.nodes[0]
                    .control()
                    .poll_record_plan(&trust, pending)
                    .unwrap()
                    .is_none()
            );
            assert_eq!(pending.progress(), RecordProgress::ReadingResult);
        }
        pump_records(
            &mut cluster,
            &mut [&mut a, &mut b],
            true,
            &mut observed,
            true,
        );
        for pending in [&mut a, &mut b] {
            assert!(
                cluster.nodes[0]
                    .control()
                    .poll_record_plan(&trust, pending)
                    .unwrap()
                    .is_none()
            );
        }
        for _ in 0..6 {
            cluster.nodes[0].tick().unwrap();
        }
        let mut results = [None, None];
        let deadline = Instant::now() + Duration::from_secs(30);
        while results.iter().any(Option::is_none) {
            assert!(
                Instant::now() < deadline,
                "concurrent final reads did not complete"
            );
            pump_records(
                &mut cluster,
                &mut [&mut a, &mut b],
                false,
                &mut observed,
                true,
            );
            for (result, pending) in results.iter_mut().zip([&mut a, &mut b]) {
                if result.is_none() {
                    *result = cluster.nodes[0]
                        .control()
                        .poll_record_plan(&trust, pending)
                        .unwrap();
                }
            }
            thread::sleep(Duration::from_millis(1));
        }
        let first = results[0].take().unwrap();
        assert_eq!(results[1].as_ref().unwrap(), &first);
        assert_eq!(Some((first.index(), first.term())), observed.first_record);
        assert_eq!(saved(&mut cluster, 0, KEY).unwrap(), first);
        desired_is_bootstrap(&mut cluster, &trust, principal, &base);
    });
    assert!(result.is_ok() && replied);
}
