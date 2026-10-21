//! File-level source-sealed recovery. The test controller's isolation assertions
//! are trusted inputs; these tests authenticate them, not operating-system fencing.
#![recursion_limit = "256"]
use ring::signature::{Ed25519KeyPair, KeyPair};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};
use tickwire_account::*;
use tickwire_account_codec::{CanonicalBaseline, CanonicalRecord};
use tickwire_account_journal::{
    AccountJournal, FileBackend, HistoryFrontier, RecoveryVerifier, VerificationError,
};
use tickwire_app::{
    config::Deployment,
    failover::{self, FenceAuthority, RestoreConfig, RestoreMode},
};
use tickwire_binance_account::AccountIngress;
use tickwire_clock::{ClockDomain, MonoTime};
use tickwire_fencing::{
    ExecutionIdentity, FenceClaims, Scope as FenceScope, SignedDocument, Validity, Verifier,
};
use tickwire_journal::{Journal, PhysicalFrontier, store::FileStore};
use tickwire_recovery::{FileBlobStore, Persistence, SharedBlobStore, digest};
use tickwire_replica::{
    self as replica, CommitMode, FileMirror, Format, MirrorConfig, ObjectRef, Signer, SourceConfig,
    SourceWorker, Stream,
};

#[path = "../../tickwire-account/tests/common/venue.rs"]
mod venue;

const ACCOUNT_STREAM: [u8; 16] = [21; 16];
const INPUT_STREAM: [u8; 16] = [22; 16];
const SOURCE_EPOCH: u64 = 4;
const TARGET_EPOCH: u64 = 5;
const WAL_BYTES: u64 = 1024 * 1024;
const STARTED: &str = "FAILOVER-IN-PROGRESS";
const COMPLETE: &str = "FAILOVER-COMPLETE.json";

struct Temporary(PathBuf);
impl Temporary {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "tickwire-failover-files-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(fs::canonicalize(path).unwrap())
    }
}
impl Drop for Temporary {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn units(n: i128) -> Fixed {
    Fixed::from_atoms(n * 100_000_000, 8).unwrap()
}
fn quote() -> AssetKey {
    AssetKey {
        wallet_id: 1,
        asset_id: 2,
    }
}
fn baseline() -> CanonicalBaseline {
    let instrument = venue::command().context.instrument;
    let snapshot = Snapshot {
        scope: Scope {
            risk_domain_id: [1; 16],
            venue: 1,
            environment: tickwire_venue::Environment::Test,
            account: 2,
        },
        account_epoch: 1,
        ledger_sequence: 0,
        revisions: Revisions {
            risk: 10,
            fee: 1,
            bracket: 1,
            collateral: 1,
            valuation: 1,
        },
        valid_until: 1000,
        assets: vec![
            AssetSnapshot {
                key: AssetKey {
                    wallet_id: 1,
                    asset_id: 1,
                },
                asset: instrument.base.clone(),
                free: units(1),
                locked: units(0),
                nontrade_locked: units(0),
                exclusive_fee_pool: false,
                ownership: vec![],
            },
            AssetSnapshot {
                key: quote(),
                asset: instrument.quote.clone(),
                free: units(1000),
                locked: units(0),
                nontrade_locked: units(0),
                exclusive_fee_pool: false,
                ownership: vec![],
            },
        ],
        positions: vec![],
        venue_available: vec![],
    };
    let catalog = Catalog {
        entries: vec![InstrumentPolicy {
            instrument,
            product: tickwire_venue::Product::Spot,
            wallet_id: 1,
            fees: FeePolicy {
                rates: [units(0); 3],
                maximum_fills: 10,
                exclusive_pool: None,
            },
            futures: None,
            max_absolute_position: units(100),
            max_gross_quantity: units(100),
            strategy_limits: vec![(8, units(100))],
        }],
    };
    CanonicalBaseline::new(snapshot, catalog).unwrap()
}
fn physical(stream: [u8; 16]) -> tickwire_journal::Config {
    tickwire_journal::Config {
        stream_epoch: stream,
        domain: [1; 16],
        segment_max_bytes: WAL_BYTES,
    }
}
fn account_config(b: &CanonicalBaseline) -> tickwire_account_journal::Config {
    tickwire_account_journal::Config {
        physical: physical(ACCOUNT_STREAM),
        baseline_digest: b.digest(),
        initial_writer_epoch: SOURCE_EPOCH,
        scope: b.snapshot().scope.clone(),
    }
}
fn input_config(b: &CanonicalBaseline) -> tickwire_binance_account::Config {
    tickwire_binance_account::Config {
        physical: physical(INPUT_STREAM),
        writer_epoch: SOURCE_EPOCH,
        baseline: b.clone(),
        account_stream: ACCOUNT_STREAM,
        max_records: 64,
        income_association_authority: None,
        income_baseline_authority: None,
        max_registrations: 8,
        max_raw_bytes: 32768,
        max_retained_bytes: 1024 * 1024,
    }
}
fn save(path: &Path, value: &Value) {
    fs::write(path, serde_json::to_vec_pretty(value).unwrap()).unwrap();
}

struct Fixture {
    root: Temporary,
    baseline: CanonicalBaseline,
    receiver: PathBuf,
    fence: FenceAuthority,
    source_public: [u8; 32],
    seal_hash: [u8; 32],
    input_root: ObjectRef,
    bootstrap: ObjectRef,
    source_account_frontier: PhysicalFrontier,
    source_input_frontier: PhysicalFrontier,
    account_payloads: Vec<Vec<u8>>,
    input_payloads: Vec<Vec<u8>>,
    fence_document: SignedDocument,
    fence_claims: FenceClaims,
}
impl Fixture {
    fn new(copy_final_input: bool) -> Self {
        let root = Temporary::new();
        let b = baseline();
        fs::write(root.0.join("baseline.cbor"), b.bytes()).unwrap();
        let shared =
            SharedBlobStore::new(FileBlobStore::open(root.0.join("source-objects")).unwrap());
        let mut persistence = Persistence::new(shared.clone());
        let bootstrap_bytes =
            tickwire_account_journal::bootstrap_bytes(&account_config(&b)).unwrap();
        let bootstrap = ObjectRef::new(&bootstrap_bytes).unwrap();
        persistence.put_object(b.bytes()).unwrap();
        persistence.put_object(&bootstrap_bytes).unwrap();

        let account_path = root.0.join("source-account");
        let mut account = AccountJournal::create(
            FileBackend::open(&account_path).unwrap(),
            account_config(&b),
            b.clone(),
        )
        .unwrap();
        // A real replayable cash flow avoids inventing an admission authorization.
        account
            .append(RecoveryRecord::UnattributedCashFlow {
                id: venue::text("funding-1"),
                delta: AssetAmount {
                    key: quote(),
                    amount: units(7),
                },
            })
            .unwrap();
        let export = account.export_durable(1, 32, 65536).unwrap();
        let source_account_frontier = export.frontier().unwrap().clone();
        let account_payloads = export.records().iter().map(|r| r.payload.clone()).collect();
        drop(account);
        let (account, _) = Journal::recover(
            FileBackend::open(&account_path).unwrap(),
            Some(&source_account_frontier),
        )
        .unwrap();
        let mut closed_account = account.close_for_recovery().unwrap();

        // This input prefix is newer than the last account record. Only the final
        // source seal references it; ordinary account batches do not cover it.
        let mut input = AccountIngress::create(
            FileStore::open(root.0.join("source-input")).unwrap(),
            input_config(&b),
        )
        .unwrap();
        let input_export = input.export_durable(1, 32, 65536).unwrap();
        let source_input_frontier = input_export.frontier().unwrap().clone();
        let input_payloads: Vec<Vec<u8>> = input_export
            .records()
            .iter()
            .map(|r| r.payload.clone())
            .collect();
        let mut input_bytes = b"TWIPFX01".to_vec();
        input_bytes.extend(INPUT_STREAM);
        input_bytes.extend(ACCOUNT_STREAM);
        input_bytes.extend(b.digest());
        input_bytes.extend((input_export.records().len() as u64).to_le_bytes());
        input_bytes.extend((input_export.records().len() as u32).to_le_bytes());
        for r in input_export.records() {
            let object = ObjectRef::new(&r.payload).unwrap();
            persistence.put_object(&r.payload).unwrap();
            input_bytes.extend(r.seq.to_le_bytes());
            input_bytes.extend(r.exec_epoch.to_le_bytes());
            input_bytes.extend(object.bytes.to_le_bytes());
            input_bytes.extend(object.hash);
        }
        replica::InputPrefix::decode(&input_bytes).unwrap();
        persistence.put_object(&input_bytes).unwrap();
        let input_root = ObjectRef::new(&input_bytes).unwrap();
        let mut closed_input = input.close_for_recovery().unwrap();

        let source_identity = ExecutionIdentity {
            host_id: [31; 16],
            boot_id: [32; 16],
            exec_instance_id: [33; 16],
            exec_epoch: SOURCE_EPOCH,
            network_identity: [34; 32],
            credential_access_identity: [35; 32],
        };
        let mode = CommitMode::LocalDurableAsync {
            max_unreplicated_age_ns: 30_000_000_000,
        };
        let mut initial_objects = vec![ObjectRef::new(b.bytes()).unwrap(), bootstrap.clone()];
        initial_objects.sort_by_key(|o| o.hash);
        let stream = Stream {
            domain: [1; 16],
            journal_epoch: ACCOUNT_STREAM,
            source: source_identity.clone(),
            source_storage_id: [36; 16],
            profile_version: 1,
            commit_mode: mode.clone(),
            format: Format::Account {
                baseline: b.digest(),
            },
            initial_objects,
        };
        let mut source = SourceWorker::new(
            SourceConfig {
                stream: stream.clone(),
                profile_version: 1,
                mode,
                max_records: 64,
                max_pending_batches: 4,
                max_pending_bytes: 65536,
            },
            Persistence::new(shared),
        )
        .unwrap();
        source.restore_prefix(&export, &BTreeMap::new(), 0).unwrap();
        let domain = ClockDomain::new([40; 16], 1).unwrap();
        let mut clock = tickwire_binance_clock::SystemClock::new(domain).unwrap();
        let now = clock.sample().unwrap().monotonic;
        let fence_claims = FenceClaims {
            schema: 1,
            scope: FenceScope {
                risk_domain: [1; 16],
                venue: 1,
                account: 2,
                product: 1,
                environment: 2,
            },
            old_identity: source_identity,
            controller_id: [41; 16],
            operation_id: [42; 16],
            control_commit: 1,
            all_egress_hash: [43; 32],
            device_action_id: [44; 16],
            device_revision: 1,
            deny_tombstone_hash: [45; 32],
            readback_hash: [46; 32],
            probe_hash: [47; 32],
            all_egress_blocked: true,
            existing_sessions_revoked: true,
            credential_access_revoked: true,
            validity: Validity {
                clock_boot_id: domain.boot_id(),
                clock_generation: domain.generation(),
                issued_ns: now.nanos(),
                expires_ns: now.nanos() + 300_000_000_000,
            },
        };
        let controller = Ed25519KeyPair::from_seed_unchecked(&[51; 32]).unwrap();
        let emergency = Ed25519KeyPair::from_seed_unchecked(&[52; 32]).unwrap();
        let fence_public_key = controller.public_key().as_ref().try_into().unwrap();
        let emergency_public_key = emergency.public_key().as_ref().try_into().unwrap();
        let payload = serde_json::to_vec(&fence_claims).unwrap();
        let fence_document = SignedDocument {
            signature: controller.sign(&payload).as_ref().to_vec(),
            payload,
        };
        let verifier = Verifier::new([41; 16], fence_public_key, emergency_public_key).unwrap();
        let fence_cap = verifier
            .verify_fence(&fence_document, &fence_claims.scope, &stream.source, now)
            .unwrap();
        let signer = Signer::from_seed([53; 32]).unwrap();
        let signed_seal = source
            .seal_account(
                &mut closed_account,
                &mut closed_input,
                input_root.clone(),
                &fence_cap,
                [54; 16],
                b.digest(),
                now,
                &signer,
            )
            .unwrap();
        assert!(source.frozen());
        assert!(
            FileBackend::open(&account_path).is_err(),
            "closed source retains exclusive ownership"
        );
        assert!(FileStore::open(root.0.join("source-input")).is_err());

        let mirror_config = MirrorConfig {
            stream: stream.clone(),
            receiver_id: [61; 16],
            receiver_boot_id: [62; 16],
            mirror_assignment_epoch: 1,
            profile_version: 1,
            max_records: 64,
            physical: physical([63; 16]),
        };
        let mut mirror = FileMirror::create_files(
            mirror_config,
            root.0.join("mirror-journal"),
            root.0.join("mirror-objects"),
        )
        .unwrap();
        let batch = source.batch_range(1, 32).unwrap();
        for object in stream
            .initial_objects
            .iter()
            .chain(batch.entries.iter().map(|e| &e.payload))
        {
            copy_object(&mut mirror, object, &source.read_object(object).unwrap());
        }
        mirror
            .append(&batch, &Signer::from_seed([64; 32]).unwrap())
            .unwrap();
        if copy_final_input {
            for bytes in &input_payloads {
                copy_object(&mut mirror, &ObjectRef::new(bytes).unwrap(), bytes);
            }
            copy_object(&mut mirror, &input_root, &input_bytes);
        }
        let seal_bytes = signed_seal.bytes();
        let seal_hash = digest(&seal_bytes);
        copy_object(
            &mut mirror,
            &ObjectRef::new(&seal_bytes).unwrap(),
            &seal_bytes,
        );
        drop(mirror);
        fs::write(root.0.join("stream.cbor"), stream.bytes()).unwrap();
        let receiver = root.0.join("receiver.json");
        // open_store never constructs a socket or reads TLS/signing files.
        save(
            &receiver,
            &json!({
                "listen":"127.0.0.1:0", "stream_file":root.0.join("stream.cbor"),
                "journal_dir":root.0.join("mirror-journal"), "object_dir":root.0.join("mirror-objects"),
                "receiver_id":([61u8;16]), "mirror_assignment_epoch":1, "profile_version":1, "max_records":64,
                "physical":{"domain":([1u8;16]),"stream_epoch":([63u8;16]),"segment_max_bytes":WAL_BYTES},
                "tls":{"certificate_chain_der":[],"private_key_der":root.0.join("unused-key"),"trust_anchors_der":[],"peer_certificate_sha256":([65u8;32])},
                "signing_key_pkcs8":root.0.join("unused-key"), "source_signing_public_key":signer.public_key(),
                "network":{"timeout_ms":1000,"requests_per_poll":4,"bytes_per_poll":65536},
                "create":false,"required_prefix":null
            }),
        );
        let document = root.0.join("controller-fence.json");
        fs::write(&document, serde_json::to_vec(&fence_document).unwrap()).unwrap();
        Self {
            root,
            baseline: b,
            receiver,
            fence: FenceAuthority {
                controller_id: [41; 16],
                fence_public_key,
                emergency_public_key,
                document,
                clock_boot_id: domain.boot_id(),
                clock_generation: 1,
            },
            source_public: signer.public_key(),
            seal_hash,
            input_root,
            bootstrap,
            source_account_frontier,
            source_input_frontier,
            account_payloads,
            input_payloads,
            fence_document,
            fence_claims,
        }
    }
    fn target(&self, name: &str, input_limit: usize) -> (RestoreConfig, Deployment) {
        let parent = self.root.0.join(name);
        let path = self.root.0.join(format!("{name}.json"));
        let d = deployment_json(&self.root.0, &parent, input_limit);
        save(&path, &d);
        (
            RestoreConfig {
                receiver_config: self.receiver.clone(),
                deployment: path.clone(),
                fence: self.fence.clone(),
                proof: RestoreMode::SourceSeal {
                    seal_hash: self.seal_hash,
                    source_signing_public_key: self.source_public,
                },
                account_bootstrap_hash: self.bootstrap.hash,
            },
            Deployment::read(&path).unwrap(),
        )
    }
}
fn copy_object(mirror: &mut FileMirror, object: &ObjectRef, bytes: &[u8]) {
    assert!(
        mirror
            .object_chunk(
                object.clone(),
                0,
                bytes,
                tickwire_journal::frame::crc32c(bytes)
            )
            .unwrap()
    );
}

fn deployment_json(root: &Path, target: &Path, input_limit: usize) -> Value {
    json!({
        "baseline":root.join("baseline.cbor"), "directories":{"account":target.join("account"),"input":target.join("input"),"recovery":target.join("recovery")},
        "recover":true,"product":"spot","boot_id":([71u8;16]),"clock_generation":1,"owner_id":([72u8;16]),"writer_epoch":TARGET_EPOCH,"initial_writer_epoch":SOURCE_EPOCH,
        "account_stream":ACCOUNT_STREAM,"input_stream":INPUT_STREAM,"first_connection_epoch":2,"instrument_id":12,"book_source":1,
        "credential":{"api_key_file":root.join("unused-api"),"signing_key_file":root.join("unused-key"),"algorithm":"hmac","generation":1,"spot_logon":false},
        "network":{"rest":"127.0.0.1:1","time":"127.0.0.1:1","public_stream":"127.0.0.1:1","snapshot":"127.0.0.1:1","private_stream":"127.0.0.1:1","private_management":null,"trust_anchors":[]},
        "clock":{"max_round_trip_ns":1000000000,"max_width_ns":1000000000,"max_age_ns":10000000000u64,"quantization_error_ns":1000000,"drift_ppb":1,"max_adjustment_ns":1000000000,"calibration_interval_ns":1000000000,"calibration_retry_ns":100000000},
        "tuning":{"connect_timeout_ns":1000000000,"request_timeout_ns":1000000000,"delivery_budget_ns":1000000,"recv_window_micros":5000000,"maximum_book_age_ns":1000000000,"maximum_idle_wait_ns":1000000,"snapshot_limit":100,"maximum_intents":8,"maximum_input_records":input_limit,"maximum_input_bytes":1048576,"wal_segment_bytes":WAL_BYTES},
        "policy":{"scope":{"risk_domain_id":([1u8;16]),"venue":1,"environment":"test","account":2},"product":"spot","executor_id":([72u8;16]),"owner_generation":1,"writer_epoch":TARGET_EPOCH,"domain":{"process_instance":([71u8;16]),"generation":1},"versions":{"config_revision":6,"risk_policy_version":10,"order_config_version":11,"catalog_version":14,"catalog_hash":([5u8;32])},"max_strategies":4,"max_feeds":4,"marks_per_feed":2,"control_revision":1,"mode":"stopped","owner_lease":null,"strategy_grants":[],"management_grant":null},
        "quota":{"supervisor_generation":1,"consumer_instance":([72u8;16]),"consumer_generation":1,"consumer_lease_until_ns":1000,"producer_id":([73u8;16]),"policy_generation":1,"policy_starts_ns":0,"policy_ends_ns":1000,"windows":[],"grants":[],"rules":[]},
        "strategy":{"mode":"observe"}, "reconciliation":{"enabled":false,"reader_id":([74u8;16]),"first_request_epoch":1,"first_execution_connection_epoch":2,"retry_ns":1000000000,"valid_for_ns":1000000000},
        "stop_file":root.join("STOP"),"maximum_runtime_ns":1000000000
    })
}

struct RejectTradingClosures;
impl RecoveryVerifier for RejectTradingClosures {
    fn verify_admission(
        &mut self,
        _: &CanonicalBaseline,
        _: &CanonicalRecord,
        _: &HistoryFrontier,
    ) -> std::result::Result<(), VerificationError> {
        Err(VerificationError::Invalid)
    }
}

#[test]
fn authenticated_source_seal_restores_files_and_exact_required_frontiers() {
    let f = Fixture::new(true);
    let (config, deployment) = f.target("success", 64);
    failover::restore(&config).unwrap();
    let (account_required, input_required) =
        failover::validate_start(&deployment).unwrap().unwrap();
    let account_required = account_required.unwrap();
    assert_eq!(account_required, f.source_account_frontier);
    assert_eq!(input_required, f.source_input_frontier);
    let (journal, recovered) = Journal::recover(
        FileBackend::open(&deployment.directories.account).unwrap(),
        Some(&account_required),
    )
    .unwrap();
    assert_eq!(journal.durable_frontier(), Some(&account_required));
    assert_eq!(
        recovered
            .records
            .iter()
            .map(|r| r.payload.clone())
            .collect::<Vec<_>>(),
        f.account_payloads
    );
    assert!(
        recovered
            .records
            .iter()
            .all(|r| r.exec_epoch == SOURCE_EPOCH)
    );
    drop(journal);
    let (journal, recovered) = Journal::recover(
        FileStore::open(&deployment.directories.input).unwrap(),
        Some(&input_required),
    )
    .unwrap();
    assert_eq!(journal.durable_frontier(), Some(&input_required));
    assert_eq!(
        recovered
            .records
            .iter()
            .map(|r| r.payload.clone())
            .collect::<Vec<_>>(),
        f.input_payloads
    );
    drop(journal);
    let recovered = AccountJournal::recover(
        FileBackend::open(&deployment.directories.account).unwrap(),
        account_config(&f.baseline),
        TARGET_EPOCH,
        Some(&account_required),
        &mut RejectTradingClosures,
    )
    .unwrap();
    assert!(recovered.account.frozen());
    assert_eq!(recovered.account.ledger_sequence(), 1);
    assert_eq!(recovered.account.asset(quote()).unwrap().total, units(1007));
    assert_eq!(recovered.journal.records().len(), 1);
    drop(recovered);
    let objects = Persistence::new(FileBlobStore::open(&deployment.directories.recovery).unwrap());
    assert_eq!(
        objects.read_object(f.baseline.digest(), 65536).unwrap(),
        f.baseline.bytes()
    );
    assert_eq!(
        digest(&objects.read_object(f.input_root.hash, 65536).unwrap()),
        f.input_root.hash
    );
}

#[test]
fn incomplete_materialization_keeps_markers_and_cannot_start() {
    let f = Fixture::new(true);
    // Force a deterministic semantic-open error after both real WALs have been
    // materialized, without mocking write/fsync or claiming a power-loss test.
    let (config, deployment) = f.target("partial", 1);
    assert!(failover::restore(&config).is_err());
    for dir in [
        &deployment.directories.account,
        &deployment.directories.input,
        &deployment.directories.recovery,
    ] {
        assert!(dir.join(STARTED).is_file());
    }
    assert!(!deployment.directories.account.join(COMPLETE).exists());
    assert!(failover::validate_start(&deployment).is_err());
    let (journal, records) = Journal::recover(
        FileStore::open(&deployment.directories.input).unwrap(),
        None,
    )
    .unwrap();
    assert_eq!(
        records.records.len(),
        1,
        "failure occurred after materialization"
    );
    drop(journal);
}

#[test]
fn missing_final_input_rejects_restore_even_with_complete_account_prefix() {
    let f = Fixture::new(false);
    let (config, deployment) = f.target("missing-final", 64);
    assert!(failover::restore(&config).is_err());
    assert!(
        !deployment.directories.account.exists(),
        "missing closure is rejected before target publication"
    );
}

#[test]
fn completed_start_requires_final_object_and_exact_target_identity() {
    let f = Fixture::new(true);
    let (config, mut deployment) = f.target("changed-target", 64);
    failover::restore(&config).unwrap();
    deployment.writer_epoch += 1;
    deployment.policy.writer_epoch += 1;
    assert!(failover::validate_start(&deployment).is_err());
    let (config, deployment) = f.target("missing-complete", 64);
    failover::restore(&config).unwrap();
    fs::remove_file(deployment.directories.account.join(COMPLETE)).unwrap();
    assert!(failover::validate_start(&deployment).is_err());
    let (config, deployment) = f.target("missing-object", 64);
    failover::restore(&config).unwrap();
    let name = format!(
        "object-{}",
        f.input_root
            .hash
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>()
    );
    fs::remove_file(deployment.directories.recovery.join(name)).unwrap();
    assert!(failover::validate_start(&deployment).is_err());
}

#[test]
fn test_controller_fence_requires_authentic_exact_and_fresh_claims() {
    let f = Fixture::new(true);
    let verifier = Verifier::new(
        f.fence.controller_id,
        f.fence.fence_public_key,
        f.fence.emergency_public_key,
    )
    .unwrap();
    let domain = ClockDomain::new(f.fence.clock_boot_id, 1).unwrap();
    let now = MonoTime::new(domain, f.fence_claims.validity.issued_ns);
    let mut bad = f.fence_document.clone();
    bad.signature[0] ^= 1;
    assert!(
        verifier
            .verify_fence(
                &bad,
                &f.fence_claims.scope,
                &f.fence_claims.old_identity,
                now
            )
            .is_err()
    );
    let mut wrong = f.fence_claims.old_identity.clone();
    wrong.exec_epoch += 1;
    assert!(
        verifier
            .verify_fence(&f.fence_document, &f.fence_claims.scope, &wrong, now)
            .is_err()
    );
    let expired = MonoTime::new(domain, f.fence_claims.validity.expires_ns);
    assert!(
        verifier
            .verify_fence(
                &f.fence_document,
                &f.fence_claims.scope,
                &f.fence_claims.old_identity,
                expired
            )
            .is_err()
    );
}
