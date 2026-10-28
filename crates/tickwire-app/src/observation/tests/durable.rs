//! Storage fixtures exercise the journal protocol, never manufacture a live Host fact.
use super::*;
fn stored_observation(inbox: &mut ObservationInbox, r: &Request) -> Outcome {
    let cp = inbox
        .storage
        .put_dependency(b"storage fixture checkpoint")
        .unwrap();
    let seal = inbox
        .storage
        .put_dependency(b"storage fixture input seal")
        .unwrap();
    let mut roots = vec![cp, seal];
    roots.sort_by_key(|o| o.digest);
    let observation = Observation {
        objects: roots,
        process: Process {
            instance: r.target.instance,
            generation: r.target.generation,
            logical_strategy_id: r.target.logical_strategy_id,
            role: 3,
            build_hash: [1; 32],
            qualification_hash: [2; 32],
            manifest_hash: [3; 32],
            pid: 123,
            uid: 1,
            gid: 1,
            host_boot: [4; 16],
            approved_host_boot: [4; 16],
            namespace: [8; 16],
            arch: 1,
            trust_profile: 1,
            connected_epoch: Some(1),
            start_ticks: 1,
            image_bytes: 2,
        },
        resources: None,
        challenge: [5; 16],
        control_epoch: 1,
        report_sequence_floor: 1,
        ready_sequence: 2,
        account_position: 1,
        report_ring_sequence: 1,
        ledger_sequence: 1,
        account_input_digest: [6; 32],
        orders_digest: [7; 32],
        checkpoint: cp,
        input_seal: seal,
        cursor_resource: [8; 16],
        subscription_generation: 1,
        bank_resource: [9; 16],
        ring_epoch: 1,
        started_mono_ns: 110,
        observed_mono_ns: 120,
    };
    Outcome {
        schema: 1,
        request: r.clone(),
        request_digest: tickwire_recovery::digest(&serde_json::to_vec(r).unwrap()),
        status: Status::Observed,
        completed_mono_ns: 130,
        historical: false,
        observation: Some(observation),
    }
}
fn receive(inbox: &mut ObservationInbox, r: &Request) {
    let digest = tickwire_recovery::digest(&serde_json::to_vec(r).unwrap());
    inbox
        .storage
        .record(&Record::Received {
            request: r.clone(),
            admitted_mono_ns: 110,
        })
        .unwrap();
    inbox.entries.insert(
        r.request_id,
        Entry {
            request: r.clone(),
            digest,
            terminal: None,
            conflict: None,
            pending: false,
        },
    );
}
#[test]
fn recovery_requires_sorted_complete_roots_and_actual_cas_bytes() {
    for fault in 0..5 {
        let f = Fixture::new();
        let (policy, _) = crate::control::tests::inputs();
        let domain = policy.domain().unwrap();
        let mut inbox =
            ObservationInbox::open_with(f.config.clone(), &policy, policy.build().unwrap(), || {
                Ok(MonoTime::new(domain, 100))
            })
            .unwrap();
        let r = request(&inbox, 1, 200);
        receive(&mut inbox, &r);
        let mut out = stored_observation(&mut inbox, &r);
        let observation = out.observation.as_mut().unwrap();
        match fault {
            1 => {
                observation.objects.remove(0);
            }
            2 => observation.objects.reverse(),
            3 => {
                observation.objects.push(observation.objects[0]);
            }
            4 => fs::write(
                f.config.journal_directory.join(format!(
                    "object-{}",
                    store::hex(&observation.checkpoint.digest)
                )),
                b"tampered",
            )
            .unwrap(),
            _ => {}
        }
        let root = inbox
            .storage
            .put(&serde_json::to_vec(&out).unwrap())
            .unwrap();
        inbox
            .storage
            .record(&Record::Terminal {
                request_id: r.request_id,
                request_digest: out.request_digest,
                result: root,
            })
            .unwrap();
        drop(inbox);
        let mut config = f.config.clone();
        config.recover = true;
        let recovered =
            ObservationInbox::open_with(config, &policy, policy.build().unwrap(), || {
                Ok(MonoTime::new(domain, 300))
            });
        if fault == 0 {
            assert!(recovered.is_ok());
            let history = outcome(&f, 1);
            assert!(history.historical);
            assert_eq!(history.status, Status::Observed);
            assert_eq!(history.request.valid_until_mono_ns, 200);
            assert_eq!(history.completed_mono_ns, 130);
        } else {
            assert!(recovered.is_err(), "fault {fault}");
        }
    }
}
#[test]
fn late_sync_or_publish_converts_success_to_expired_and_recovery_keeps_it_closed() {
    for expire_at_sample in [2, 3] {
        let f = Fixture::new();
        let (policy, _) = crate::control::tests::inputs();
        let domain = policy.domain().unwrap();
        let mut inbox =
            ObservationInbox::open_with(f.config.clone(), &policy, policy.build().unwrap(), || {
                Ok(MonoTime::new(domain, 100))
            })
            .unwrap();
        let r = request(&inbox, 1, 200);
        receive(&mut inbox, &r);
        let out = stored_observation(&mut inbox, &r);
        let mut samples = 0;
        inbox
            .finish(out, &mut || {
                samples += 1;
                Ok(MonoTime::new(
                    domain,
                    if samples >= expire_at_sample {
                        210
                    } else {
                        140
                    },
                ))
            })
            .unwrap();
        assert_eq!(outcome(&f, 1).status, Status::Expired);
        drop(inbox);
        let mut config = f.config.clone();
        config.recover = true;
        let _recovered =
            ObservationInbox::open_with(config, &policy, policy.build().unwrap(), || {
                Ok(MonoTime::new(domain, 300))
            })
            .unwrap();
        let out = outcome(&f, 1);
        assert!(out.historical);
        assert_eq!(out.status, Status::Expired);
        assert!(out.observation.is_none());
    }
}
#[test]
fn conflict_history_cannot_regress_the_durable_clock() {
    let f = Fixture::new();
    let (policy, _) = crate::control::tests::inputs();
    let domain = policy.domain().unwrap();
    let mut inbox =
        ObservationInbox::open_with(f.config.clone(), &policy, policy.build().unwrap(), || {
            Ok(MonoTime::new(domain, 100))
        })
        .unwrap();
    let mut r = request(&inbox, 1, 200);
    receive(&mut inbox, &r);
    r.require_resources = true;
    let out = inbox.failure(&r, Status::Rejected, 109);
    let root = inbox
        .storage
        .put(&serde_json::to_vec(&out).unwrap())
        .unwrap();
    inbox
        .storage
        .record(&Record::Conflict {
            request_id: r.request_id,
            result: root,
        })
        .unwrap();
    drop(inbox);
    let mut config = f.config.clone();
    config.recover = true;
    assert!(
        ObservationInbox::open_with(config, &policy, policy.build().unwrap(), || Ok(
            MonoTime::new(domain, 300)
        ))
        .is_err()
    );
}
#[test]
fn recovery_bounds_aggregate_storage_before_decoding_journal() {
    let f = Fixture::new();
    let (policy, _) = crate::control::tests::inputs();
    let domain = policy.domain().unwrap();
    drop(
        ObservationInbox::open_with(f.config.clone(), &policy, policy.build().unwrap(), || {
            Ok(MonoTime::new(domain, 100))
        })
        .unwrap(),
    );
    let dir = files::Directory::open(&f.config.journal_directory, f.config.owner_uid).unwrap();
    for name in ["extra-a", "extra-b"] {
        dir.open_file(name, true, true)
            .unwrap()
            .set_len(MAX_TOTAL / 2)
            .unwrap();
    }
    let mut config = f.config.clone();
    config.recover = true;
    assert!(
        ObservationInbox::open_with(config, &policy, policy.build().unwrap(), || Ok(
            MonoTime::new(domain, 300)
        ))
        .is_err()
    );
}

#[test]
fn shared_verified_dependency_paths_become_one_durable_cas_reference() {
    let f = Fixture::new();
    let (policy, _) = crate::control::tests::inputs();
    let domain = policy.domain().unwrap();
    let mut inbox =
        ObservationInbox::open_with(f.config.clone(), &policy, policy.build().unwrap(), || {
            Ok(MonoTime::new(domain, 100))
        })
        .unwrap();
    let r = request(&inbox, 1, 200);
    receive(&mut inbox, &r);
    let mut out = stored_observation(&mut inbox, &r);
    let observation = out.observation.as_mut().unwrap();
    let cp = inbox
        .storage
        .get_dependency(observation.checkpoint)
        .unwrap();
    let seal = inbox
        .storage
        .get_dependency(observation.input_seal)
        .unwrap();
    // The same checkpoint is both a direct READY root and an installed-graph dependency.
    let entries = vec![
        (observation.checkpoint, cp.as_slice()),
        (observation.input_seal, seal.as_slice()),
        (observation.checkpoint, cp.as_slice()),
    ];
    let canonical = inbox::canonical_dependencies(entries).unwrap();
    observation.objects = canonical.iter().map(|(root, _)| *root).collect();
    assert_eq!(
        observation.objects.len(),
        2,
        "shared graph edges must not create duplicate CAS roots"
    );
    for (root, bytes) in canonical {
        assert_eq!(inbox.storage.put_dependency(bytes).unwrap(), root);
    }
    inbox
        .finish(out, &mut || Ok(MonoTime::new(domain, 140)))
        .unwrap();
    drop(inbox);
    let mut config = f.config.clone();
    config.recover = true;
    let _recovered = ObservationInbox::open_with(config, &policy, policy.build().unwrap(), || {
        Ok(MonoTime::new(domain, 300))
    })
    .unwrap();
    let out = outcome(&f, 1);
    assert!(out.historical);
    assert_eq!(out.status, Status::Observed);
    assert_eq!(out.observation.unwrap().objects.len(), 2);
}
#[test]
fn one_dependency_digest_cannot_refer_to_different_bytes() {
    let bytes = b"verified object";
    let object = Object::of(bytes);
    assert!(
        inbox::canonical_dependencies(vec![
            (object, bytes.as_slice()),
            (object, b"different bytes".as_slice())
        ])
        .is_err()
    );
}
