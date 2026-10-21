use super::*;
use std::{cell::RefCell, collections::BTreeMap, io, rc::Rc};
use tickwire_clock::ClockDomain;
use tickwire_journal::store::Store;

fn signer(seed: u8) -> Signer {
    Signer(Ed25519KeyPair::from_seed_unchecked(&[seed; 32]).unwrap())
}
fn now(n: u64) -> MonoTime {
    MonoTime::new(ClockDomain::new([90; 16], 1).unwrap(), n)
}
fn member_now(m: &Member, n: u64) -> MonoTime {
    MonoTime::new(
        ClockDomain::new(m.clock_boot_id, m.clock_generation).unwrap(),
        n,
    )
}
fn config() -> CoordinatorConfig {
    let members: Vec<_> = (1..=2)
        .map(|i| Member {
            risk_domain: [i; 16],
            exec_instance: [i + 2; 16],
            exec_epoch: 3,
            old_instance: [i + 4; 16],
            old_generation: 4,
            new_instance: [i + 6; 16],
            new_generation: 5,
            clock_boot_id: [i + 8; 16],
            clock_generation: 2,
            valid_until_ns: 10000,
        })
        .collect();
    CoordinatorConfig {
        coordinator_id: [50; 16],
        coordinator_key: signer(50).public_key(),
        operation: Operation {
            schema: 1,
            operation_id: [60; 16],
            operation_revision: 1,
            logical_strategy_id: 12,
            members: members.clone(),
            artifacts_digest: [1; 32],
            checkpoint_digest: [2; 32],
            rollback_digest: [3; 32],
            clock_boot_id: [90; 16],
            clock_generation: 1,
            valid_until_ns: 1000,
        },
        members: members
            .into_iter()
            .map(|m| MemberAuthority {
                public_key: signer(m.risk_domain[0]).public_key(),
                member: m,
                readiness: Some(Readiness {
                    checkpoint_digest: [2; 32],
                    exec_report_sequence: 10,
                    orders_digest: [4; 32],
                    ledger_sequence: 3,
                    input_seal_digest: [5; 32],
                }),
            })
            .collect(),
        audit: tickwire_journal::Config::new([8; 16], [9; 16]),
    }
}
fn request(signed: &SignedControlRequest) -> ControlRequest {
    decode_canonical(&signed.payload).unwrap()
}
fn answer(request: &ControlRequest, phase: AppliedPhase, version: u64) -> AppliedReceipt {
    AppliedReceipt {
        schema: 1,
        operation_id: request.operation.operation_id,
        operation_revision: request.operation.operation_revision,
        members_digest: request.operation.members_digest().unwrap(),
        logical_strategy_id: request.operation.logical_strategy_id,
        member: request.member.clone(),
        phase,
        state_version: version,
        handover_sequence: if matches!(phase, AppliedPhase::Prepared) {
            0
        } else {
            20
        },
        handover_digest: if matches!(phase, AppliedPhase::Prepared) {
            [0; 32]
        } else {
            [20; 32]
        },
        risk_gate_revision: 7,
        control_request_id: request.request_id,
        issued_ns: 2000,
        valid_until_ns: 9000,
    }
}
fn sign(r: &AppliedReceipt) -> SignedReceipt {
    signer(r.member.risk_domain[0]).sign_receipt(r).unwrap()
}
fn owner(store: Mem) -> Coordinator<Mem> {
    Coordinator::create(store, config(), 1, signer(50), now(1)).unwrap()
}
fn all(owner: &mut Coordinator<Mem>, phase: AppliedPhase, version: u64, time: u64) {
    for signed in owner.pending_requests().to_vec() {
        let receipt = answer(&request(&signed), phase, version);
        owner
            .ingest(sign(&receipt), owner.version(), now(time))
            .unwrap();
    }
}

#[test]
fn group_release_requires_every_actual_prepared_and_committed_receipt() {
    let mut owner = owner(Mem::default());
    assert_eq!(owner.stage(), Stage::Preparing);
    assert_eq!(owner.pending_requests().len(), 2);
    assert_eq!(
        owner.begin_commit(owner.version(), now(2)),
        Err(Error::Phase)
    );
    assert_eq!(owner.release(owner.version(), now(2)), Err(Error::Phase));
    all(&mut owner, AppliedPhase::Prepared, 1, 2);
    assert_eq!(owner.stage(), Stage::PreparedAll);
    owner.begin_commit(owner.version(), now(3)).unwrap();
    let r = answer(
        &request(&owner.pending_requests()[0]),
        AppliedPhase::Committed,
        2,
    );
    let signed = sign(&r);
    let applied = owner
        .ingest(signed.clone(), owner.version(), now(4))
        .unwrap();
    assert_eq!(owner.stage(), Stage::PartialCommitted);
    assert!(owner.group_release().is_none());
    assert_eq!(owner.release(owner.version(), now(4)), Err(Error::Phase));
    assert_eq!(
        owner.ingest(signed, 0, now(0)).unwrap(),
        ApplyResult {
            duplicate: true,
            ..applied
        }
    );
    all(&mut owner, AppliedPhase::Committed, 2, 4);
    owner.release(owner.version(), now(5)).unwrap();
    assert_eq!(owner.stage(), Stage::Releasing);
    assert!(owner.group_release().is_some());
    all(&mut owner, AppliedPhase::Released, 3, 6);
    assert_eq!(owner.stage(), Stage::ActiveAll);
}

#[test]
fn recipient_uses_its_own_monotonic_domain_and_checks_original_signed_request() {
    let c = config();
    let mut owner = owner(Mem::default());
    let first = &owner.pending_requests()[0];
    let m = &c.members[0].member;
    assert!(
        verify_control_request(
            first,
            c.coordinator_key,
            c.coordinator_id,
            m,
            member_now(m, 3000)
        )
        .is_ok()
    );
    assert!(matches!(
        verify_control_request(first, c.coordinator_key, c.coordinator_id, m, now(3)),
        Err(Error::Identity)
    ));
    let mut changed = first.clone();
    changed.payload[10] ^= 1;
    assert!(matches!(
        verify_control_request(
            &changed,
            c.coordinator_key,
            c.coordinator_id,
            m,
            member_now(m, 3000)
        ),
        Err(Error::Signature)
    ));
    all(&mut owner, AppliedPhase::Prepared, 1, 2);
    owner.begin_commit(owner.version(), now(3)).unwrap();
    all(&mut owner, AppliedPhase::Committed, 2, 4);
    owner.release(owner.version(), now(5)).unwrap();
    let group = owner.group_release().unwrap();
    // The member is at 3000 while the collector expires at 1000 in another domain.
    assert!(
        verify_group_release_for_member(
            group,
            c.coordinator_key,
            c.coordinator_id,
            &c.operation,
            m,
            member_now(m, 3000)
        )
        .is_ok()
    );
    assert!(matches!(
        verify_group_release_for_member(
            group,
            c.coordinator_key,
            c.coordinator_id,
            &c.operation,
            m,
            member_now(m, 10000)
        ),
        Err(Error::Expired)
    ));
    for signed in owner.pending_requests() {
        let req = request(signed);
        let verified = verify_control_request(
            signed,
            c.coordinator_key,
            c.coordinator_id,
            &req.member,
            member_now(&req.member, 3000),
        )
        .unwrap();
        assert_eq!(verified.request().action, ControlAction::Release);
    }
}

#[test]
fn recovery_issues_new_challenges_before_explicit_resume() {
    let store = Mem::default();
    let mut owner = owner(store.clone());
    all(&mut owner, AppliedPhase::Prepared, 1, 2);
    owner.begin_commit(owner.version(), now(3)).unwrap();
    let old = owner.pending_requests().to_vec();
    all(&mut owner, AppliedPhase::Committed, 2, 4);
    drop(owner);
    let mut recovered =
        Coordinator::recover(store, config(), 2, signer(50), None, now(10)).unwrap();
    assert_eq!(recovered.stage(), Stage::Reconciling);
    assert!(recovered.group_release().is_none());
    // A newly signed stale request ID still cannot satisfy a fresh query.
    let stale = answer(&request(&old[0]), AppliedPhase::Committed, 3);
    assert_eq!(
        recovered.ingest(sign(&stale), recovered.version(), now(11)),
        Err(Error::Conflict)
    );
    assert_eq!(recovered.stage(), Stage::Reconciling);
    assert_eq!(
        recovered.resume_after_queries(recovered.version(), now(11)),
        Err(Error::Phase)
    );
    all(&mut recovered, AppliedPhase::Committed, 2, 12);
    assert_eq!(recovered.stage(), Stage::AwaitingResume);
    recovered
        .resume_after_queries(recovered.version(), now(13))
        .unwrap();
    assert_eq!(recovered.stage(), Stage::CommittedAll);
    recovered.release(recovered.version(), now(14)).unwrap();
}

#[test]
fn changed_gate_revision_in_fresh_query_closes_operation_instead_of_reauthorizing() {
    let mut owner = owner(Mem::default());
    all(&mut owner, AppliedPhase::Prepared, 1, 2);
    owner.begin_commit(owner.version(), now(3)).unwrap();
    all(&mut owner, AppliedPhase::Committed, 2, 4);
    owner.query_applied(owner.version(), now(5)).unwrap();
    let mut receipt = answer(
        &request(&owner.pending_requests()[0]),
        AppliedPhase::Committed,
        3,
    );
    receipt.risk_gate_revision = 8;
    assert_eq!(
        owner.ingest(sign(&receipt), owner.version(), now(6)),
        Err(Error::Frozen)
    );
    assert_eq!(owner.stage(), Stage::Halting);
    assert!(
        owner
            .pending_requests()
            .iter()
            .all(|s| request(s).action == ControlAction::Halt)
    );
    assert!(owner.group_release().is_none());
}

fn partially_released_store(acknowledged: bool) -> (Mem, [SignedReceipt; 2]) {
    let store = Mem::default();
    let mut owner = owner(store.clone());
    all(&mut owner, AppliedPhase::Prepared, 1, 2);
    owner.begin_commit(owner.version(), now(3)).unwrap();
    let committed: Vec<_> = owner
        .pending_requests()
        .iter()
        .map(|r| sign(&answer(&request(r), AppliedPhase::Committed, 2)))
        .collect();
    for receipt in &committed {
        owner
            .ingest(receipt.clone(), owner.version(), now(4))
            .unwrap();
    }
    owner.release(owner.version(), now(5)).unwrap();
    // A applied Release. Its response may have reached the coordinator or been
    // lost before the crash. B still has only its durable Commit.
    if acknowledged {
        let applied = sign(&answer(
            &request(&owner.pending_requests()[0]),
            AppliedPhase::Released,
            3,
        ));
        owner.ingest(applied, owner.version(), now(6)).unwrap();
    }
    drop(owner);
    (store, committed.try_into().unwrap())
}

#[test]
fn partial_release_recovery_only_dispatches_the_unreleased_member() {
    for acknowledged in [false, true] {
        let (store, original_committed) = partially_released_store(acknowledged);
        let mut recovered =
            Coordinator::recover(store, config(), 2, signer(50), None, now(10)).unwrap();
        let queries = recovered.pending_requests().to_vec();
        assert_eq!(queries.len(), 2);
        assert!(
            queries
                .iter()
                .all(|r| request(r).action == ControlAction::QueryApplied)
        );
        let a = sign(&answer(&request(&queries[0]), AppliedPhase::Released, 3));
        recovered
            .ingest(a.clone(), recovered.version(), now(11))
            .unwrap();
        assert_eq!(
            recovered.resume_after_queries(recovered.version(), now(12)),
            Err(Error::Phase)
        );
        assert_eq!(
            recovered.release(recovered.version(), now(12)),
            Err(Error::Phase)
        );
        let b = sign(&answer(&request(&queries[1]), AppliedPhase::Committed, 2));
        recovered
            .ingest(b.clone(), recovered.version(), now(13))
            .unwrap();
        assert_eq!(recovered.stage(), Stage::AwaitingResume);
        assert_eq!(
            recovered.release(recovered.version(), now(14)),
            Err(Error::Phase)
        );
        recovered
            .resume_after_queries(recovered.version(), now(15))
            .unwrap();
        recovered.release(recovered.version(), now(16)).unwrap();
        assert_eq!(recovered.pending_requests().len(), 1);
        let pending = request(&recovered.pending_requests()[0]);
        assert_eq!(pending.action, ControlAction::Release);
        assert_eq!(pending.member.risk_domain, [2; 16]);
        let signed_group = recovered.group_release().unwrap();
        let group: GroupRelease = decode_canonical(&signed_group.payload).unwrap();
        // A's Released Query is not a substitute for its signed Committed receipt.
        assert_eq!(
            group.members[0].committed_receipt_digest,
            original_committed[0].digest()
        );
        assert_ne!(group.members[0].committed_receipt_digest, a.digest());
        assert_eq!(group.members[1].committed_receipt_digest, b.digest());
        let c = config();
        verify_group_release_for_member(
            signed_group,
            c.coordinator_key,
            c.coordinator_id,
            &c.operation,
            &pending.member,
            member_now(&pending.member, 3000),
        )
        .unwrap();
        let released = sign(&answer(&pending, AppliedPhase::Released, 3));
        recovered
            .ingest(released, recovered.version(), now(17))
            .unwrap();
        assert_eq!(recovered.stage(), Stage::ActiveAll);
        assert!(recovered.pending_requests().is_empty());
    }
}

#[test]
fn partial_release_recovery_does_not_resume_changed_or_halted_gates() {
    for changed in [false, true] {
        let (store, _) = partially_released_store(true);
        let mut recovered =
            Coordinator::recover(store, config(), 2, signer(50), None, now(10)).unwrap();
        let queries = recovered.pending_requests().to_vec();
        let a = sign(&answer(&request(&queries[0]), AppliedPhase::Released, 3));
        recovered.ingest(a, recovered.version(), now(11)).unwrap();
        let mut b = answer(
            &request(&queries[1]),
            if changed {
                AppliedPhase::Committed
            } else {
                AppliedPhase::Halted
            },
            3,
        );
        if changed {
            b.risk_gate_revision += 1;
        }
        let result = recovered.ingest(sign(&b), recovered.version(), now(12));
        if changed {
            assert_eq!(result, Err(Error::Frozen));
            assert_eq!(recovered.stage(), Stage::Halting);
        } else {
            result.unwrap();
            assert_eq!(
                recovered.resume_after_queries(recovered.version(), now(13)),
                Err(Error::Frozen)
            );
        }
        assert!(recovered.release(recovered.version(), now(14)).is_err());
        assert!(
            recovered
                .pending_requests()
                .iter()
                .all(|r| request(r).action != ControlAction::Release)
        );
    }
}

#[test]
fn failed_sync_never_exposes_candidate_dispatch_and_recovery_only_queries() {
    let store = Mem::default();
    let mut owner = owner(store.clone());
    all(&mut owner, AppliedPhase::Prepared, 1, 2);
    store.0.borrow_mut().fail_sync = true;
    assert_eq!(
        owner.begin_commit(owner.version(), now(3)),
        Err(Error::Store)
    );
    assert_eq!(owner.stage(), Stage::Frozen);
    assert!(owner.pending_requests().is_empty());
    assert_eq!(owner.release(owner.version(), now(4)), Err(Error::Store));
    drop(owner);
    let recovered = Coordinator::recover(store, config(), 2, signer(50), None, now(5)).unwrap();
    assert_eq!(recovered.stage(), Stage::Reconciling);
    assert!(
        recovered
            .pending_requests()
            .iter()
            .all(|r| request(r).action == ControlAction::QueryApplied)
    );
}

#[test]
fn halt_remains_authenticated_and_effective_after_activation_deadline() {
    let c = config();
    let mut owner = owner(Mem::default());
    owner.halt(owner.version(), now(1200)).unwrap();
    for signed in owner.pending_requests().to_vec() {
        let req = request(&signed);
        assert!(
            verify_control_request(
                &signed,
                c.coordinator_key,
                c.coordinator_id,
                &req.member,
                member_now(&req.member, 11000)
            )
            .is_ok()
        );
        let mut r = answer(&req, AppliedPhase::Halted, 1);
        r.issued_ns = 11000;
        r.valid_until_ns = 10000;
        owner.ingest(sign(&r), owner.version(), now(1201)).unwrap();
    }
    assert_eq!(owner.stage(), Stage::Halted);
    assert_eq!(
        owner.begin_commit(owner.version(), now(1202)),
        Err(Error::Expired)
    );
}

#[test]
fn changed_pinned_readiness_or_member_identity_rejects_existing_wal() {
    let store = Mem::default();
    drop(owner(store.clone()));
    let mut c = config();
    c.members[0]
        .readiness
        .as_mut()
        .unwrap()
        .exec_report_sequence += 1;
    assert!(matches!(
        Coordinator::recover(store, c, 2, signer(50), None, now(10)),
        Err(Error::Identity)
    ));
}

#[test]
fn stale_readiness_can_be_reprepared_without_undoing_another_members_commit() {
    let store = Mem::default();
    let mut owner = owner(store.clone());
    all(&mut owner, AppliedPhase::Prepared, 1, 2);
    owner.begin_commit(owner.version(), now(3)).unwrap();
    let first = answer(
        &request(&owner.pending_requests()[0]),
        AppliedPhase::Committed,
        2,
    );
    owner.ingest(sign(&first), owner.version(), now(4)).unwrap();
    let member = config().members[1].member.clone();
    let mut proof = config().members[1].readiness.clone().unwrap();
    proof.exec_report_sequence = 50;
    proof.checkpoint_digest = [80; 32];
    owner
        .update_readiness(&member, proof.clone(), owner.version(), now(5))
        .unwrap();
    assert_eq!(owner.pending_requests().len(), 1);
    let fresh = request(&owner.pending_requests()[0]);
    assert_eq!(fresh.action, ControlAction::Prepare);
    assert_eq!(fresh.proof, Some(proof.clone()));
    assert_eq!(owner.release(owner.version(), now(6)), Err(Error::Phase));
    let prepared = answer(&fresh, AppliedPhase::Prepared, 2);
    owner
        .ingest(sign(&prepared), owner.version(), now(6))
        .unwrap();
    assert_eq!(owner.stage(), Stage::PreparedAll);
    owner.begin_commit(owner.version(), now(7)).unwrap();
    assert_eq!(owner.pending_requests().len(), 1);
    assert_eq!(request(&owner.pending_requests()[0]).proof, Some(proof));
    all(&mut owner, AppliedPhase::Committed, 3, 8);
    assert_eq!(owner.stage(), Stage::CommittedAll);
    assert_eq!(
        owner.update_readiness(
            &member,
            config().members[1].readiness.clone().unwrap(),
            owner.version(),
            now(9)
        ),
        Err(Error::Phase)
    );
    drop(owner);
    let recovered = Coordinator::recover(store, config(), 2, signer(50), None, now(10)).unwrap();
    assert_eq!(recovered.stage(), Stage::Reconciling);
}

#[derive(Default)]
struct Memory {
    manifest: Option<Vec<u8>>,
    segments: BTreeMap<u64, Vec<u8>>,
    fail_sync: bool,
    syncs_before_failure: Option<usize>,
}
#[derive(Clone, Default)]
struct Mem(Rc<RefCell<Memory>>);
impl Store for Mem {
    fn read_manifest(&mut self) -> io::Result<Option<Vec<u8>>> {
        Ok(self.0.borrow().manifest.clone())
    }
    fn publish_manifest(&mut self, b: &[u8]) -> io::Result<()> {
        self.0.borrow_mut().manifest = Some(b.to_vec());
        Ok(())
    }
    fn segment_ids(&mut self) -> io::Result<Vec<u64>> {
        Ok(self.0.borrow().segments.keys().copied().collect())
    }
    fn read_segment(&mut self, id: u64, max: u64) -> io::Result<Vec<u8>> {
        let b = self
            .0
            .borrow()
            .segments
            .get(&id)
            .cloned()
            .ok_or_else(|| io::Error::other("missing"))?;
        if b.len() as u64 > max {
            return Err(io::Error::other("capacity"));
        }
        Ok(b)
    }
    fn create_segment(&mut self, id: u64) -> io::Result<()> {
        if self.0.borrow_mut().segments.insert(id, vec![]).is_some() {
            return Err(io::Error::other("exists"));
        }
        Ok(())
    }
    fn append_segment(&mut self, id: u64, offset: u64, b: &[u8]) -> io::Result<()> {
        let mut state = self.0.borrow_mut();
        let bytes = state
            .segments
            .get_mut(&id)
            .ok_or_else(|| io::Error::other("missing"))?;
        if bytes.len() as u64 != offset {
            return Err(io::Error::other("offset"));
        }
        bytes.extend(b);
        Ok(())
    }
    fn sync_segment(&mut self, _: u64) -> io::Result<()> {
        let mut state = self.0.borrow_mut();
        if let Some(remaining) = state.syncs_before_failure.as_mut() {
            if *remaining == 0 {
                state.syncs_before_failure = None;
                return Err(io::Error::other("lost sync reply"));
            }
            *remaining -= 1;
        }
        if std::mem::take(&mut state.fail_sync) {
            Err(io::Error::other("sync"))
        } else {
            Ok(())
        }
    }
    fn truncate_segment(&mut self, id: u64, len: u64) -> io::Result<()> {
        self.0
            .borrow_mut()
            .segments
            .get_mut(&id)
            .ok_or_else(|| io::Error::other("missing"))?
            .truncate(len as usize);
        Ok(())
    }
}

#[test]
fn shared_event_journal_recovers_large_complete_records_in_order() {
    let store = Mem::default();
    let cfg = config().audit;
    let mut journal = EventJournal::create(store.clone(), cfg.clone(), 1).unwrap();
    let large = vec![255u8; 100_000];
    journal.append(&large).unwrap();
    journal.append(&vec![1u8, 2, 3]).unwrap();
    let required = journal.frontier().cloned().unwrap();
    drop(journal);
    let (_, records) = EventJournal::recover::<Vec<u8>>(store, &cfg, 2, Some(&required)).unwrap();
    assert_eq!(records, vec![large, vec![1, 2, 3]]);
}

#[test]
fn lost_logical_commit_sync_reply_is_recovered_but_never_published_by_failed_owner() {
    let store = Mem::default();
    let cfg = config().audit;
    let mut journal = EventJournal::create(store.clone(), cfg.clone(), 1).unwrap();
    store.0.borrow_mut().syncs_before_failure = Some(1);
    assert_eq!(journal.append(&vec![1u8, 2, 3]), Err(Error::Store));
    assert!(journal.frontier().is_none());
    assert_eq!(journal.append(&vec![4u8]), Err(Error::Store));
    drop(journal);
    let (_, records) = EventJournal::recover::<Vec<u8>>(store, &cfg, 2, None).unwrap();
    assert_eq!(records, vec![vec![1, 2, 3]]);
}

#[test]
fn event_journal_enforces_recoverable_event_capacity_before_writing() {
    let store = Mem::default();
    let cfg = config().audit;
    let mut journal = EventJournal::create(store.clone(), cfg.clone(), 1).unwrap();
    for number in 0..4096u64 {
        journal.append(&number).unwrap();
    }
    let position = journal.position();
    assert_eq!(journal.append(&4096u64), Err(Error::Capacity));
    assert_eq!(journal.position(), position);
    assert!(journal.frontier().is_some());
    drop(journal);
    let (mut restored, records) = EventJournal::recover::<u64>(store, &cfg, 2, None).unwrap();
    assert_eq!(records.len(), 4096);
    assert_eq!(restored.append(&4096u64), Err(Error::Capacity));
    assert_eq!(restored.position(), position);
}

#[test]
fn replay_prepared_adopts_new_ready_before_commit_and_retains_original_challenge() {
    let mut config = config();
    config.operation.schema = 3;
    let store = Mem::default();
    let mut owner = Coordinator::create(store, config.clone(), 1, signer(50), now(1)).unwrap();
    let mut accepted = vec![];
    for original in owner.pending_requests().to_vec() {
        let prepare = request(&original);
        let object = |x| ReplayObject {
            digest: [x; 32],
            bytes: 128,
        };
        let receipt = ReplayPrepared {
            schema: 1,
            operation: config.operation.clone(),
            member: prepare.member.clone(),
            challenge: original.clone(),
            prepare: original,
            fixed: ReplayCut {
                checkpoint: object(30),
                sequence: 1,
                chain: [31; 32],
            },
            current: ReplayCut {
                checkpoint: object(32),
                sequence: 2,
                chain: [33; 32],
            },
            source_completion: object(34),
            target_install: object(35),
            readiness: Readiness {
                checkpoint_digest: [36; 32],
                input_seal_digest: [37; 32],
                exec_report_sequence: 11,
                ledger_sequence: 4,
                orders_digest: [38; 32],
            },
            state_version: 1,
            risk_gate_revision: 7,
            issued_ns: 2000,
            valid_until_ns: 9000,
        };
        let signed = signer(receipt.member.risk_domain[0])
            .sign_replay_prepared(&receipt)
            .unwrap();
        owner
            .ingest_replay(signed.clone(), owner.version(), now(2))
            .unwrap();
        assert!(
            owner
                .ingest_replay(signed.clone(), 0, now(0))
                .unwrap()
                .duplicate
        );
        accepted.push((signed, receipt));
    }
    assert_eq!(owner.stage(), Stage::PreparedAll);
    owner.begin_commit(owner.version(), now(3)).unwrap();
    for commit in owner.pending_requests() {
        let r = request(commit);
        let (signed, receipt) = accepted.iter().find(|(_, p)| p.member == r.member).unwrap();
        assert_eq!(r.schema, 2);
        assert_eq!(r.proof.as_ref(), Some(&receipt.readiness));
        assert_eq!(
            r.replay.as_ref(),
            Some(&ReplayAdoption::from_receipt(signed).unwrap())
        );
    }
}

fn replay_answer(original: SignedControlRequest) -> ReplayPrepared {
    let request = request(&original);
    let object = |x| ReplayObject {
        digest: [x; 32],
        bytes: 128,
    };
    ReplayPrepared {
        schema: 1,
        operation: request.operation.clone(),
        member: request.member.clone(),
        challenge: original.clone(),
        prepare: original,
        fixed: ReplayCut {
            checkpoint: object(30),
            sequence: 1,
            chain: [31; 32],
        },
        current: ReplayCut {
            checkpoint: object(32),
            sequence: 2,
            chain: [33; 32],
        },
        source_completion: object(34),
        target_install: object(35),
        readiness: Readiness {
            checkpoint_digest: [36; 32],
            input_seal_digest: [37; 32],
            exec_report_sequence: 11,
            ledger_sequence: 4,
            orders_digest: [38; 32],
        },
        state_version: 1,
        risk_gate_revision: 7,
        issued_ns: 2000,
        valid_until_ns: 9000,
    }
}
#[test]
fn replay_prepared_rejects_legacy_prepare_and_swapped_original_request() {
    let mut c = config();
    c.operation.schema = 3;
    let mut owner = Coordinator::create(Mem::default(), c.clone(), 1, signer(50), now(1)).unwrap();
    let initial = owner.pending_requests()[0].clone();
    let mut receipt = replay_answer(initial.clone());
    let mut changed = request(&initial);
    changed.request_id = [200; 16];
    receipt.prepare = signer(50).sign_request(&changed).unwrap();
    receipt.challenge = receipt.prepare.clone();
    let signed = signer(receipt.member.risk_domain[0])
        .sign_replay_prepared(&receipt)
        .unwrap();
    assert_eq!(
        owner.ingest_replay(signed, owner.version(), now(2)),
        Err(Error::Conflict)
    );
    assert_eq!(owner.stage(), Stage::Preparing);
    let legacy = sign(&answer(&request(&initial), AppliedPhase::Prepared, 1));
    assert_eq!(
        owner.ingest(legacy, owner.version(), now(2)),
        Err(Error::Phase)
    );
    assert_ne!(owner.stage(), Stage::PreparedAll);
}
#[test]
fn replay_prepared_recovery_requires_fresh_queries_and_reuses_saved_installation() {
    let mut c = config();
    c.operation.schema = 3;
    let store = Mem::default();
    let mut owner = Coordinator::create(store.clone(), c.clone(), 1, signer(50), now(1)).unwrap();
    let mut originals = vec![];
    for initial in owner.pending_requests().to_vec() {
        let r = replay_answer(initial);
        let s = signer(r.member.risk_domain[0])
            .sign_replay_prepared(&r)
            .unwrap();
        owner
            .ingest_replay(s.clone(), owner.version(), now(2))
            .unwrap();
        originals.push(s);
    }
    drop(owner);
    let mut owner = Coordinator::recover(store, c.clone(), 2, signer(50), None, now(3)).unwrap();
    assert_eq!(owner.stage(), Stage::Reconciling);
    assert_eq!(
        owner.begin_commit(owner.version(), now(4)),
        Err(Error::Phase)
    );
    all(&mut owner, AppliedPhase::Prepared, 1, 4);
    assert_eq!(owner.stage(), Stage::AwaitingResume);
    owner.resume_after_queries(owner.version(), now(5)).unwrap();
    assert_eq!(owner.stage(), Stage::Preparing);
    for challenge in owner.pending_requests().to_vec() {
        let r = request(&challenge);
        let old = originals
            .iter()
            .find(|s| ReplayAdoption::from_receipt(s).unwrap() == *r.replay.as_ref().unwrap())
            .unwrap();
        let mut refreshed: ReplayPrepared = decode_canonical(&old.payload).unwrap();
        refreshed.challenge = challenge;
        refreshed.state_version = 2;
        let signed = signer(refreshed.member.risk_domain[0])
            .sign_replay_prepared(&refreshed)
            .unwrap();
        owner
            .ingest_replay(signed, owner.version(), now(6))
            .unwrap();
    }
    owner.begin_commit(owner.version(), now(7)).unwrap();
    for signed in owner.pending_requests() {
        let r = request(signed);
        assert!(
            originals
                .iter()
                .any(|s| ReplayAdoption::from_receipt(s).unwrap().target_install
                    == r.replay.as_ref().unwrap().target_install)
        );
        assert!(
            verify_control_request(
                signed,
                c.coordinator_key,
                c.coordinator_id,
                &r.member,
                member_now(&r.member, 3000)
            )
            .is_ok()
        );
    }
}
#[test]
fn replay_prepared_sync_failure_never_exposes_a_commit_or_adopted_readiness() {
    let mut c = config();
    c.operation.schema = 3;
    let store = Mem::default();
    let mut owner = Coordinator::create(store.clone(), c, 1, signer(50), now(1)).unwrap();
    let r = replay_answer(owner.pending_requests()[0].clone());
    let s = signer(r.member.risk_domain[0])
        .sign_replay_prepared(&r)
        .unwrap();
    store.0.borrow_mut().fail_sync = true;
    assert_eq!(
        owner.ingest_replay(s, owner.version(), now(2)),
        Err(Error::Store)
    );
    assert_eq!(owner.stage(), Stage::Frozen);
    assert!(owner.pending_requests().is_empty());
    assert!(owner.latest_receipt(r.member.risk_domain).is_none());
}

#[test]
fn replay_refresh_changes_only_authenticated_consumption_and_not_the_fixed_install() {
    let mut c = config();
    c.operation.schema = 3;
    let mut owner = Coordinator::create(Mem::default(), c.clone(), 1, signer(50), now(1)).unwrap();
    let original = owner.pending_requests()[0].clone();
    let r = replay_answer(original.clone());
    let first = signer(r.member.risk_domain[0])
        .sign_replay_prepared(&r)
        .unwrap();
    owner
        .ingest_replay(first.clone(), owner.version(), now(2))
        .unwrap();
    owner
        .refresh_replay(&r.member, owner.version(), now(3))
        .unwrap();
    let request = owner
        .pending_requests()
        .iter()
        .find(|q| request(q).member == r.member)
        .unwrap()
        .clone();
    let mut next = r.clone();
    next.challenge = request.clone();
    next.state_version += 1;
    next.readiness.exec_report_sequence += 1;
    next.readiness.ledger_sequence += 1;
    next.readiness.checkpoint_digest = [81; 32];
    next.readiness.input_seal_digest = [82; 32];
    let signed = signer(r.member.risk_domain[0])
        .sign_replay_prepared(&next)
        .unwrap();
    owner
        .ingest_replay(signed.clone(), owner.version(), now(4))
        .unwrap();
    assert_eq!(next.prepare, original);
    assert_eq!(next.fixed, r.fixed);
    assert_eq!(next.current, r.current);
    assert_eq!(next.target_install, r.target_install);
    let mut changed = next;
    changed.current.sequence += 1;
    assert!(
        signer(r.member.risk_domain[0])
            .sign_replay_prepared(&changed)
            .is_err()
    );
    assert!(
        verify_control_request(
            &request,
            c.coordinator_key,
            c.coordinator_id,
            &r.member,
            member_now(&r.member, 3000)
        )
        .is_ok()
    );
}

#[test]
fn lost_replay_adoption_is_recovered_as_history_then_fresh_prepare_before_commit() {
    let mut cfg = config();
    cfg.operation.schema = 3;
    let store = Mem::default();
    let owner = Coordinator::create(store.clone(), cfg.clone(), 1, signer(50), now(1)).unwrap();
    let originals = owner.pending_requests().to_vec();
    drop(owner);
    let mut owner = Coordinator::recover(store, cfg, 2, signer(50), None, now(2)).unwrap();
    assert_eq!(
        owner.resume_after_queries(owner.version(), now(3)),
        Err(Error::Phase)
    );
    for query in owner.pending_requests().to_vec() {
        let q = request(&query);
        let original = originals
            .iter()
            .find(|r| request(r).member == q.member)
            .unwrap()
            .clone();
        let mut prepared = replay_answer(original);
        prepared.valid_until_ns = 6000;
        let prepared = signer(q.member.risk_domain[0])
            .sign_replay_prepared(&prepared)
            .unwrap();
        let mut applied = answer(&q, AppliedPhase::Prepared, 5);
        applied.issued_ns = 7000;
        applied.valid_until_ns = 9000;
        let observed = signer(q.member.risk_domain[0])
            .sign_replay_observed(&ReplayObserved {
                schema: 1,
                query,
                applied,
                prepared,
            })
            .unwrap();
        owner
            .ingest_replay_observed(observed, owner.version(), now(4))
            .unwrap();
    }
    assert_eq!(owner.stage(), Stage::AwaitingResume);
    owner.resume_after_queries(owner.version(), now(5)).unwrap();
    assert_eq!(owner.stage(), Stage::Preparing);
    assert_eq!(
        owner.begin_commit(owner.version(), now(6)),
        Err(Error::Phase)
    );
    for challenge in owner.pending_requests().to_vec() {
        let r = request(&challenge);
        assert_eq!(r.action, ControlAction::Prepare);
        let old = r.replay.as_ref().unwrap();
        assert!(originals.contains(&old.prepare));
        let mut refreshed = replay_answer(challenge);
        refreshed.prepare = old.prepare.clone();
        refreshed.state_version = 6;
        refreshed.issued_ns = 8000;
        refreshed.valid_until_ns = 9900;
        refreshed.readiness.exec_report_sequence = 12;
        let signed = signer(refreshed.member.risk_domain[0])
            .sign_replay_prepared(&refreshed)
            .unwrap();
        owner
            .ingest_replay(signed, owner.version(), now(7))
            .unwrap();
    }
    assert_eq!(owner.stage(), Stage::PreparedAll);
    owner.begin_commit(owner.version(), now(8)).unwrap();
    assert!(
        owner
            .pending_requests()
            .iter()
            .all(|r| request(r).proof.unwrap().exec_report_sequence == 12)
    );
}

#[test]
fn replay_query_rejects_halted_changed_gate_session_and_expired_member() {
    let mut cfg = config();
    cfg.operation.schema = 3;
    let store = Mem::default();
    let owner = Coordinator::create(store.clone(), cfg.clone(), 1, signer(50), now(1)).unwrap();
    let original = owner.pending_requests()[0].clone();
    drop(owner);
    let mut owner = Coordinator::recover(store, cfg.clone(), 2, signer(50), None, now(2)).unwrap();
    let query = owner.pending_requests()[0].clone();
    let q = request(&query);
    let prepared = signer(q.member.risk_domain[0])
        .sign_replay_prepared(&replay_answer(original))
        .unwrap();
    let observed = ReplayObserved {
        schema: 1,
        query: query.clone(),
        applied: answer(&q, AppliedPhase::Prepared, 5),
        prepared,
    };
    let key = signer(q.member.risk_domain[0]);
    for case in 0..4 {
        let mut bad = observed.clone();
        match case {
            0 => {
                bad.applied.phase = AppliedPhase::Halted;
            }
            1 => bad.applied.risk_gate_revision += 1,
            2 => bad.applied.member.exec_epoch += 1,
            _ => bad.applied.issued_ns = 10000,
        }
        assert!(key.sign_replay_observed(&bad).is_err(), "case {case}");
    }
    assert!(matches!(
        verify_control_request(
            &query,
            cfg.coordinator_key,
            cfg.coordinator_id,
            &q.member,
            member_now(&q.member, 10000)
        ),
        Err(Error::Expired)
    ));
    let mut bad = observed;
    let mut history: ReplayPrepared = decode_canonical(&bad.prepared.payload).unwrap();
    let mut other = request(&history.prepare);
    other.request_id = [201; 16];
    history.prepare = signer(50).sign_request(&other).unwrap();
    history.challenge = history.prepare.clone();
    bad.prepared = key.sign_replay_prepared(&history).unwrap();
    let signed = key.sign_replay_observed(&bad).unwrap();
    assert_eq!(
        owner.ingest_replay_observed(signed, owner.version(), now(3)),
        Err(Error::Conflict)
    );
    assert_eq!(owner.stage(), Stage::Reconciling);
}

#[test]
fn recovery_during_readiness_refresh_preserves_the_last_completed_result() {
    let mut cfg = config();
    cfg.operation.schema = 3;
    let store = Mem::default();
    let mut owner = Coordinator::create(store.clone(), cfg.clone(), 1, signer(50), now(1)).unwrap();
    let mut results = Vec::new();
    for initial in owner.pending_requests().to_vec() {
        let r = replay_answer(initial);
        let signed = signer(r.member.risk_domain[0])
            .sign_replay_prepared(&r)
            .unwrap();
        owner
            .ingest_replay(signed.clone(), owner.version(), now(2))
            .unwrap();
        results.push(signed);
    }
    owner
        .refresh_replay(&cfg.members[0].member, owner.version(), now(3))
        .unwrap();
    drop(owner);
    let mut owner = Coordinator::recover(store, cfg, 2, signer(50), None, now(4)).unwrap();
    for query in owner.pending_requests().to_vec() {
        let q = request(&query);
        let prepared = results
            .iter()
            .find(|s| {
                decode_canonical::<ReplayPrepared>(&s.payload)
                    .unwrap()
                    .member
                    == q.member
            })
            .unwrap()
            .clone();
        let observed = signer(q.member.risk_domain[0])
            .sign_replay_observed(&ReplayObserved {
                schema: 1,
                query,
                applied: answer(&q, AppliedPhase::Prepared, 2),
                prepared,
            })
            .unwrap();
        owner
            .ingest_replay_observed(observed, owner.version(), now(5))
            .unwrap();
    }
    owner.resume_after_queries(owner.version(), now(6)).unwrap();
    assert_eq!(owner.stage(), Stage::Preparing);
    assert_eq!(owner.pending_requests().len(), 2);
}

#[path = "rebuild_tests.rs"]
mod rebuild_cases;
