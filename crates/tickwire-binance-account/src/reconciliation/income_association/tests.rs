use super::encoding::AssociationRecord;
use super::*;
use crate::reconciliation::income_import::tests::{Memory, approved_fixture_authority, http};
use ring::signature::{Ed25519KeyPair, KeyPair};
use tickwire_account::{Account, IncomeIdentity, ObservationAssociation, RecoveryRecord};
use tickwire_account_journal::AccountJournal;
use tickwire_clock::{ClockDomain, MonoTime};
use tickwire_recovery::{MockBlobStore, Persistence, SharedBlobStore};

fn fixture() -> (
    AccountIngress<Memory>,
    Account,
    AccountJournal<Memory>,
    AssociationRequest,
    Ed25519KeyPair,
    ClockDomain,
    SharedBlobStore<MockBlobStore>,
) {
    let root = Ed25519KeyPair::from_seed_unchecked(&[50; 32]).unwrap();
    let leaf = Ed25519KeyPair::from_seed_unchecked(&[51; 32]).unwrap();
    let domain = ClockDomain::new([8; 16], 1).unwrap();
    let authorization = b"dedicated test root authorized scoped risk-admin decision";
    let claims = AuthorityClaims {
        controller_id: [10; 16],
        key_id: [11; 16],
        revision: 1,
        scope: tickwire_account::Scope {
            risk_domain_id: [1; 16],
            venue: 1,
            environment: v::Environment::Test,
            account: 2,
        },
        wallet_id: 1,
        execution_instance: [12; 16],
        writer_epoch: 4,
        clock_domain: domain,
        issued: 1,
        expires: 100,
        public_key: leaf.public_key().as_ref().try_into().unwrap(),
        authorization_digest: authority::hash(authorization),
    };
    let bytes = claims.bytes().unwrap();
    let certificate = SignedAssociationAuthority::new(&bytes, root.sign(&bytes).as_ref()).unwrap();
    let authority = AssociationAuthority {
        controller_id: [10; 16],
        public_key: root.public_key().as_ref().try_into().unwrap(),
        execution_instance: [12; 16],
        active_policy: Some(ActiveAssociationPolicy {
            certificate_digest: certificate.digest(),
            authorization_revision: 1,
        }),
    };
    let (input, mut account, mut journal, _) = approved_fixture_authority(Some(authority));
    let objects = SharedBlobStore::new(MockBlobStore::default());
    let mut persistence = Persistence::new(objects.clone());
    persistence.put_object(authorization).unwrap();
    persistence
        .put_object(b"test operator explicitly approved exact proposal")
        .unwrap();
    let mut input = input.with_closure_store(objects.clone());
    input.sync_association_policy().unwrap();
    let query = income::IncomeQuery {
        start_time_ms: 101,
        end_time_ms: 110,
        limit: 2,
        private_connection_epoch: 7,
        gap_generation: 3,
        purpose: income::IncomeUse::Account,
    };
    let body=br#"[{"symbol":"BTCUSDT","incomeType":"FUNDING_FEE","income":"-2","asset":"USDT","time":105,"tranId":43,"tradeId":""}]"#;
    let rows = income::bind(&input.config.baseline, query, body).unwrap();
    let row = rows[0].clone();
    let leaf_seq = input
        .append(Record::Income(income::IncomeRecord {
            query,
            epoch: 15,
            received: 20,
            clock_boot_id: [8; 16],
            clock_generation: 1,
            status: 200,
            accepted: true,
            raw: http(body),
            rows,
        }))
        .unwrap()
        .sequence;
    let effect = input
        .append(Record::IncomeEffect {
            response: leaf_seq,
            index: 0,
        })
        .unwrap();
    account.apply_account_input(&row).unwrap();
    journal
        .append(RecoveryRecord::IngressAccountInput {
            input_stream: effect.stream,
            input_sequence: effect.sequence,
            input: Box::new(row),
        })
        .unwrap();
    let meta = SourceMeta {
        epoch: 7,
        subscription: None,
        micros: false,
        received: 21,
    };
    let raw=br#"{"e":"ACCOUNT_UPDATE","E":106,"T":105,"a":{"m":"FUNDING_FEE","B":[{"a":"USDT","wb":"998","cw":"998","bc":"-2"}]}}"#;
    let observation =
        crate::account_facts::decode_observation(&input.config.baseline, &meta, raw, 4096).unwrap();
    let key = input
        .append(Record::Event(
            meta,
            raw.to_vec(),
            Decision::AccountBound(Box::new(observation.clone())),
        ))
        .unwrap();
    account.apply_account_input(&observation).unwrap();
    journal
        .append(RecoveryRecord::IngressAccountInput {
            input_stream: key.stream,
            input_sequence: key.sequence,
            input: Box::new(observation.clone()),
        })
        .unwrap();
    let Record::Snapshot(original) = &input.records[3] else {
        panic!("snapshot")
    };
    let original = original.clone();
    let mut snapshots = [0; 3];
    for (i, kind) in [
        bootstrap::SnapshotKind::Account,
        bootstrap::SnapshotKind::OpenOrders,
        bootstrap::SnapshotKind::ConditionalOrders,
    ]
    .into_iter()
    .enumerate()
    {
        let mut s = original.clone();
        s.query.kind = kind;
        s.ledger = account.ledger_sequence();
        s.received = 22 + i as u64;
        s.epoch = 16 + i as u64;
        if i == 0 {
            let raw = String::from_utf8(s.raw.clone()).unwrap();
            let body = raw.split("\r\n\r\n").nth(1).unwrap().replace("1000", "998");
            s.raw = http(body.as_bytes())
        } else {
            s.raw = http(b"[]")
        }
        snapshots[i] = input.append(Record::Snapshot(s)).unwrap().sequence;
    }
    input.cursor = input.records.len();
    let request = AssociationRequest {
        request_id: [20; 16],
        wallet_id: 1,
        associations: vec![ObservationAssociation {
            observation_digest: observation.digest(),
            incomes: vec![IncomeIdentity {
                income_type: v::BoundedText::new("FUNDING_FEE").unwrap(),
                transaction_id: v::BoundedText::new("43").unwrap(),
            }],
        }],
        queries: income_closure::ClosureRequest {
            income_leaves: vec![leaf_seq],
            snapshots,
            cut: activation::ActivationCut {
                private_connection_epoch: 7,
                gap_generation: 3,
                through_sequence: input.position(),
                now: MonoTime::new(domain, 25),
                valid_until: MonoTime::new(domain, 90),
                writer_epoch: 4,
                execution_connection_epoch: 1,
            },
        },
        certificate,
    };
    (input, account, journal, request, leaf, domain, objects)
}
fn approve(
    input: &mut AccountIngress<Memory>,
    account: &Account,
    journal: &AccountJournal<Memory>,
    request: AssociationRequest,
    leaf: &Ed25519KeyPair,
) -> AssociationRecord {
    let certificate = request.certificate.clone();
    let proposal = input
        .proposal(
            account,
            journal.history_frontier(),
            journal.records(),
            request,
        )
        .unwrap();
    let audit = authority::hash(b"test operator explicitly approved exact proposal");
    let payload = proposal
        .approval_payload(&certificate, audit, 25, 80)
        .unwrap();
    let approval =
        SignedAssociationApproval::new(certificate, &payload, leaf.sign(&payload).as_ref())
            .unwrap();
    let record = AssociationRecord {
        proposal,
        approval,
        accepted: 26,
        writer: 4,
    };
    input
        .validate_association_record(&record, input.position() + 1, 4)
        .unwrap();
    let store = input.closure_store.as_ref().unwrap();
    store.publish(&record.approval.bytes()).unwrap();
    record
}
#[test]
fn actual_policy_queries_and_snapshots_bind_association_before_financially_neutral_commit() {
    let (mut input, mut account, mut journal, request, leaf, _, _) = fixture();
    let before = account.asset(crate::unit_fixture::key(2)).unwrap();
    let record = approve(&mut input, &account, &journal, request, &leaf);
    let binding = record
        .proposal
        .manifest
        .binding(record.proposal.digest(), record.approval.digest())
        .unwrap();
    input
        .append(Record::IncomeAssociation(Box::new(record)))
        .unwrap();
    input
        .verify_income_association(
            &input.config.baseline,
            &account,
            journal.history_frontier(),
            journal.records(),
            &binding,
            4,
        )
        .unwrap();
    account.associate_income_observations(&binding).unwrap();
    journal
        .append(RecoveryRecord::IncomeObservationsAssociated(Box::new(
            binding.clone(),
        )))
        .unwrap();
    assert_eq!(account.asset(crate::unit_fixture::key(2)).unwrap(), before);
    assert!(account.frozen());
    assert_eq!(account.unexplained_account_observations().count(), 0);
    assert_eq!(account.unresolved_account_inputs(), 1);
    assert_eq!(
        account.associate_income_observations(&binding).unwrap(),
        Applied::Duplicate
    );
    assert_eq!(input.income_association_objects(&binding).unwrap().len(), 6);
}
#[test]
fn original_policy_replays_after_leaf_rotation_but_tampered_policy_is_rejected() {
    let (mut input, account, journal, request, leaf, _, objects) = fixture();
    let record = approve(&mut input, &account, &journal, request, &leaf);
    let binding = record
        .proposal
        .manifest
        .binding(record.proposal.digest(), record.approval.digest())
        .unwrap();
    input
        .append(Record::IncomeAssociation(Box::new(record)))
        .unwrap();
    let mut config = input.config.clone();
    config.writer_epoch = 5;
    config
        .income_association_authority
        .as_mut()
        .unwrap()
        .active_policy = Some(ActiveAssociationPolicy {
        certificate_digest: [77; 32],
        authorization_revision: 2,
    });
    let mut restored = AccountIngress::recover(input.journal.into_store(), config, None)
        .unwrap()
        .with_closure_store(objects);
    restored
        .verify_income_association(
            &restored.config.baseline,
            &account,
            journal.history_frontier(),
            journal.records(),
            &binding,
            5,
        )
        .unwrap();
    let policy = restored
        .records
        .iter_mut()
        .find_map(|r| match r {
            Record::AssociationPolicy(p) => Some(p),
            _ => None,
        })
        .unwrap();
    policy.active.as_mut().unwrap().certificate_digest = [88; 32];
    assert!(
        restored
            .verify_income_association(
                &restored.config.baseline,
                &account,
                journal.history_frontier(),
                journal.records(),
                &binding,
                5
            )
            .is_err()
    );
}
#[test]
fn real_dual_wal_pending_approval_completes_under_new_writer_and_replays_once() {
    use std::sync::atomic::{AtomicU64, Ordering};
    use tickwire_account_codec::CanonicalRecord;
    use tickwire_account_gateway::ReactivationVerifier;
    use tickwire_account_journal::{
        FileBackend, HistoryFrontier, RecoveryVerifier, VerificationError,
    };
    use tickwire_journal::store::FileStore;
    use tickwire_recovery::FileBlobStore;
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let path = std::env::temp_dir().join(format!(
        "tickwire-association-dual-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir(&path).unwrap();
    let (mut memory, account, journal, request, leaf, _, _) = fixture();
    let record = approve(&mut memory, &account, &journal, request, &leaf);
    let retry = record.clone();
    let binding = record
        .proposal
        .manifest
        .binding(record.proposal.digest(), record.approval.digest())
        .unwrap();
    memory
        .append(Record::IncomeAssociation(Box::new(record)))
        .unwrap();
    let objects = memory.income_association_objects(&binding).unwrap();
    let config = memory.config.clone();
    let cfg = journal.config().clone();
    let store = SharedBlobStore::new(FileBlobStore::open(path.join("objects")).unwrap());
    for b in objects {
        Persistence::new(store.clone()).put_object(&b).unwrap();
    }
    let mut input =
        AccountIngress::create(FileStore::open(path.join("input")).unwrap(), config.clone())
            .unwrap()
            .with_closure_store(store.clone());
    for r in memory.records.iter().skip(1) {
        input.append(r.clone()).unwrap();
    }
    let mut account_wal = AccountJournal::create(
        FileBackend::open(path.join("account")).unwrap(),
        cfg.clone(),
        config.baseline.clone(),
    )
    .unwrap();
    for r in journal.records() {
        account_wal.append(r.record().clone()).unwrap();
    }
    // Only the approval input has synchronized. The account still has no tag15.
    assert_eq!(account_wal.position(), 3);
    drop(account_wal);
    drop(input);
    drop(store);
    struct Verify<'a>(&'a AccountIngress<FileStore>);
    impl RecoveryVerifier for Verify<'_> {
        fn verify_admission(
            &mut self,
            _: &CanonicalBaseline,
            _: &CanonicalRecord,
            _: &HistoryFrontier,
        ) -> std::result::Result<(), VerificationError> {
            Err(VerificationError::Invalid)
        }
        fn verify_income_baseline(
            &mut self,
            b: &CanonicalBaseline,
            r: &CanonicalRecord,
            h: &HistoryFrontier,
            prior: &[CanonicalRecord],
        ) -> std::result::Result<(), VerificationError> {
            let RecoveryRecord::IncomeBaseline(binding) = r.record() else {
                return Err(VerificationError::Invalid);
            };
            let account = Account::recover(
                b.snapshot().clone(),
                b.catalog().clone(),
                &prior.iter().map(|r| r.record().clone()).collect::<Vec<_>>(),
            )
            .map_err(|_| VerificationError::Invalid)?;
            self.0
                .activation_verifier()
                .verify_income_baseline(b, &account, h, binding)
                .map_err(|_| VerificationError::Invalid)
        }
        fn verify_income_association(
            &mut self,
            b: &CanonicalBaseline,
            r: &CanonicalRecord,
            h: &HistoryFrontier,
            prior: &[CanonicalRecord],
            writer: u64,
        ) -> std::result::Result<(), VerificationError> {
            let RecoveryRecord::IncomeObservationsAssociated(binding) = r.record() else {
                return Err(VerificationError::Invalid);
            };
            let account = Account::recover(
                b.snapshot().clone(),
                b.catalog().clone(),
                &prior.iter().map(|r| r.record().clone()).collect::<Vec<_>>(),
            )
            .map_err(|_| VerificationError::Invalid)?;
            self.0
                .activation_verifier()
                .verify_income_association(b, &account, h, prior, binding, writer)
                .map_err(|_| VerificationError::Invalid)
        }
    }
    let mut new = config.clone();
    new.writer_epoch = 5;
    new.income_association_authority
        .as_mut()
        .unwrap()
        .active_policy = None;
    let mut input = AccountIngress::recover(
        FileStore::open(path.join("input")).unwrap(),
        new.clone(),
        None,
    )
    .unwrap()
    .with_closure_store(FileBlobStore::open(path.join("objects")).unwrap());
    // A later durable account record changes the approved prefix even though
    // the same signed approval remains present in the real input WAL.
    let mut changed_wal = AccountJournal::create(
        FileBackend::open(path.join("changed-account")).unwrap(),
        cfg.clone(),
        new.baseline.clone(),
    )
    .unwrap();
    for r in journal.records() {
        changed_wal.append(r.record().clone()).unwrap();
    }
    changed_wal
        .append(RecoveryRecord::UnattributedCashFlow {
            id: v::BoundedText::new("external-1").unwrap(),
            delta: tickwire_account::AssetAmount {
                key: crate::unit_fixture::key(2),
                amount: tickwire_risk::Fixed::from_atoms(-1, 8).unwrap(),
            },
        })
        .unwrap();
    drop(changed_wal);
    let changed = AccountJournal::recover(
        FileBackend::open(path.join("changed-account")).unwrap(),
        cfg.clone(),
        5,
        None,
        &mut Verify(&input),
    )
    .unwrap();
    assert!(
        input
            .verify_income_association(
                &new.baseline,
                &changed.account,
                changed.journal.history_frontier(),
                changed.journal.records(),
                &binding,
                5
            )
            .is_err()
    );
    drop(changed);
    let gateway_config = tickwire_account_gateway::Config {
        journal: cfg.clone(),
        writer_epoch: 5,
        route: tickwire_account_gateway::Route {
            venue: 1,
            account: 2,
            environment: v::Environment::Test,
            product: v::Product::LinearPerpetual,
            authority: "demo-fapi.binance.com".into(),
        },
        max_intents: 32,
    };
    let mut gateway = tickwire_account_gateway::AccountGateway::recover_with_reactivation(
        FileBackend::open(path.join("account")).unwrap(),
        gateway_config,
        RejectOrders,
        RejectOrders,
        None,
        &mut input.activation_verifier(),
    )
    .unwrap();
    let before = gateway
        .account()
        .asset(crate::unit_fixture::key(2))
        .unwrap();
    let prior_inputs = input.records.len() - 2;
    input.replay_pending(&mut gateway, prior_inputs).unwrap();
    let mut owner = RestOwner::from_parts(
        input,
        gateway,
        crate::rest::RestConfig {
            connection_epoch: 5,
            max_response_bytes: 4096,
            max_headers: 32,
        },
    )
    .unwrap();
    // Current leaf is revoked and the original approval is expired. Only the
    // exact already-synchronized request may finish against its original cut.
    let expired = MonoTime::new(retry.approval.authority().claims().clock_domain, 1000);
    assert_eq!(
        owner
            .accept_income_association(retry.proposal.clone(), retry.approval.clone(), expired)
            .unwrap(),
        Applied::New
    );
    assert_eq!(
        owner
            .accept_income_association(retry.proposal.clone(), retry.approval.clone(), expired)
            .unwrap(),
        Applied::Duplicate
    );
    assert_eq!(owner.gateway().journal_position(), 4);
    assert_eq!(
        owner.account().asset(crate::unit_fixture::key(2)).unwrap(),
        before
    );
    assert!(owner.account().frozen());
    drop(owner);
    new.writer_epoch = 6;
    new.income_association_authority
        .as_mut()
        .unwrap()
        .active_policy = Some(ActiveAssociationPolicy {
        certificate_digest: [77; 32],
        authorization_revision: 2,
    });
    let input = AccountIngress::recover(FileStore::open(path.join("input")).unwrap(), new, None)
        .unwrap()
        .with_closure_store(FileBlobStore::open(path.join("objects")).unwrap());
    let mut restored = AccountJournal::recover(
        FileBackend::open(path.join("account")).unwrap(),
        cfg.clone(),
        6,
        None,
        &mut Verify(&input),
    )
    .unwrap();
    assert_eq!(restored.account.ledger_sequence(), 4);
    assert_eq!(restored.journal.position(), 4);
    assert!(restored.account.frozen());
    assert_eq!(
        restored.account.asset(crate::unit_fixture::key(2)).unwrap(),
        before
    );
    assert_eq!(
        restored
            .account
            .associate_income_observations(&binding)
            .unwrap(),
        Applied::Duplicate
    );
    drop(restored);
    let hex: String = binding
        .approval_digest
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    std::fs::remove_file(path.join("objects").join(format!("object-{hex}"))).unwrap();
    assert!(
        AccountJournal::recover(
            FileBackend::open(path.join("account")).unwrap(),
            cfg,
            6,
            None,
            &mut Verify(&input)
        )
        .is_err()
    );
    drop(input);
    std::fs::remove_dir_all(path).unwrap();
}

struct RejectOrders;
impl tickwire_account_gateway::Authority for RejectOrders {
    fn prepare(
        &mut self,
        _: tickwire_account_gateway::AdmissionRequest<'_>,
    ) -> std::result::Result<
        tickwire_recovery::VerifiedClosure,
        tickwire_account_gateway::AuthorityError,
    > {
        Err(tickwire_account_gateway::AuthorityError::Denied)
    }
    fn verify(
        &mut self,
        _: tickwire_account_gateway::AdmissionRequest<'_>,
    ) -> std::result::Result<
        tickwire_recovery::VerifiedClosure,
        tickwire_account_gateway::AuthorityError,
    > {
        Err(tickwire_account_gateway::AuthorityError::Denied)
    }
    fn current_gate(
        &mut self,
        _: &v::CanonicalCommand,
        _: &tickwire_account::Admission,
    ) -> std::result::Result<tickwire_account_gateway::Gate, tickwire_account_gateway::AuthorityError>
    {
        Err(tickwire_account_gateway::AuthorityError::Denied)
    }
}
impl tickwire_account_gateway::Encoder for RejectOrders {
    fn encode(
        &mut self,
        _: &v::CanonicalCommand,
    ) -> tickwire_gateway::Result<tickwire_gateway::EncodedRequest> {
        Err(tickwire_gateway::Error::Invalid)
    }
}
#[test]
fn replica_dependencies_bind_all_authority_policy_and_audit_objects() {
    let (mut input, account, journal, request, leaf, _, _) = fixture();
    let r = approve(&mut input, &account, &journal, request, &leaf);
    let binding = r
        .proposal
        .manifest
        .binding(r.proposal.digest(), r.approval.digest())
        .unwrap();
    let roots = dependency_hashes(&binding, r.proposal.bytes(), &r.approval.bytes()).unwrap();
    input
        .append(Record::IncomeAssociation(Box::new(r.clone())))
        .unwrap();
    let objects = input.income_association_objects(&binding).unwrap();
    assert_eq!(
        roots,
        objects
            .iter()
            .map(|b| authority::hash(b))
            .collect::<Vec<_>>()
            .as_slice()
    );
    let mut other = binding.clone();
    other.request_id = [99; 16];
    assert!(dependency_hashes(&other, r.proposal.bytes(), &r.approval.bytes()).is_err());
    let mut bytes = r.proposal.bytes().to_vec();
    bytes.push(0);
    assert!(dependency_hashes(&binding, &bytes, &r.approval.bytes()).is_err());
}
