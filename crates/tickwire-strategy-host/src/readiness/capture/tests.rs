use super::*;
fn identity() -> Identity {
    Identity {
        instance: [6; 16],
        generation: 6,
        logical_strategy: 8,
        role: 3,
        namespace: [1; 16],
        qualification_hash: [2; 32],
        manifest_hash: [3; 32],
        arch: 1,
        trust_profile: 1,
        build_hash: [4; 32],
        host_boot: [5; 16],
    }
}
fn body() -> HandoverReadyBody {
    HandoverReadyBody {
        instance_id: [6; 16],
        strategy_generation: 6,
        logical_strategy_id: 8,
        ready_sequence: 4,
        exec_report_sequence: 6,
        report_ring_sequence: 7,
        ledger_sequence: 6,
        checkpoint_digest: [1; 32],
        checkpoint_bytes: 1,
        input_seal_digest: [2; 32],
        input_seal_bytes: 1,
        orders_digest: [3; 32],
    }
}
#[test]
fn announcements_never_publish_authority_and_final_failure_stays_unpublished() {
    {
        let mut progress = Progress::new(identity(), None);
        let mut published = false;
        let first = body();
        progress
            .accept(&first, false, || {
                published = true;
                Ok(())
            })
            .unwrap();
        assert!(
            !published,
            "an intermediate announcement must not publish READY"
        );
        let mut final_body = first.clone();
        final_body.ready_sequence += 1;
        assert!(
            progress
                .accept(&final_body, true, || Err(Error::Invalid))
                .is_err()
        );
        assert!(!published);
        assert_eq!(progress.last, Some(first));
        // Timeout/error drops this local watermark; no shared readiness is changed.
    }
    assert!(Progress::new(identity(), None).last.is_none());
}
#[test]
fn every_announcement_and_final_reply_preserve_all_monotone_watermarks() {
    let first = body();
    for change in 0..7 {
        let mut progress = Progress::new(identity(), Some(first.clone()));
        let mut bad = first.clone();
        bad.ready_sequence += 1;
        match change {
            0 => bad.instance_id[0] ^= 1,
            1 => bad.strategy_generation += 1,
            2 => bad.logical_strategy_id += 1,
            3 => bad.ready_sequence -= 2,
            4 => bad.exec_report_sequence -= 1,
            5 => bad.report_ring_sequence -= 1,
            6 => bad.ledger_sequence -= 1,
            _ => unreachable!(),
        }
        for sealed in [false, true] {
            let mut verified = false;
            assert!(
                progress
                    .accept(&bad, sealed, || {
                        verified = true;
                        Ok(())
                    })
                    .is_err()
            );
            assert!(!verified);
            assert_eq!(progress.last, Some(first.clone()));
        }
    }
    let mut progress = Progress::new(identity(), Some(first.clone()));
    progress.accept(&first, false, || Ok(())).unwrap();
    let mut bad = first.clone();
    bad.checkpoint_digest[0] ^= 1;
    assert!(progress.accept(&bad, true, || Ok(())).is_err());
}
#[test]
#[ignore = "uses retained fifth-round capture via TICKWIRE_CAPTURE_FIXTURE and TICKWIRE_CAPTURE_LOG"]
fn actual_two_ready_bodies_load_only_the_challenge_bound_final_closure() {
    let root = PathBuf::from(std::env::var_os("TICKWIRE_CAPTURE_FIXTURE").unwrap());
    let log = std::fs::read_to_string(std::env::var_os("TICKWIRE_CAPTURE_LOG").unwrap()).unwrap();
    let events = log
        .lines()
        .filter_map(|s| serde_json::from_str::<serde_json::Value>(s).ok())
        .collect::<Vec<_>>();
    let directory = root.join("next/recovery");
    let make = |sequence| {
        let event = events
            .iter()
            .find(|e| {
                e["event"] == "strategy_ready"
                    && e["generation"] == 6
                    && e["ready_sequence"] == sequence
            })
            .unwrap();
        let consumed = events
            .iter()
            .find(|e| {
                e["event"] == "strategy_reports_consumed"
                    && e["generation"] == 6
                    && e["ready_sequence"] == sequence
            })
            .unwrap();
        let digest: [u8; 32] = serde_json::from_value(event["input_seal_digest"].clone()).unwrap();
        let hex = digest
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        let raw = std::fs::read(directory.join(format!("{hex}.bin"))).unwrap();
        let seal = tickwire_recovery::HandoverSeal::decode(&raw).unwrap();
        HandoverReadyBody {
            instance_id: seal.instance,
            logical_strategy_id: seal.logical_strategy,
            strategy_generation: seal.generation,
            ready_sequence: sequence,
            exec_report_sequence: seal.report_position,
            ledger_sequence: seal.ledger_sequence,
            report_ring_sequence: consumed["report_sequence"].as_u64().unwrap(),
            checkpoint_digest: seal.checkpoint_digest,
            checkpoint_bytes: seal.checkpoint_bytes,
            input_seal_digest: digest,
            input_seal_bytes: raw.len() as u64,
            orders_digest: seal.orders_digest,
        }
    };
    let mut cache = cache::Cache::default();
    let context = cache::Context {
        identity: identity(),
        pid: 1300,
        uid: 0,
        gid: 0,
        epoch: 1,
        deadline: u64::MAX,
    };
    let (interim, final_body) = (make(4), make(5));
    assert_ne!(interim.input_seal_digest, final_body.input_seal_digest);
    let mut progress = Progress::new(identity(), None);
    let mut loads = 0;
    let mut result = None;
    let start = Instant::now();
    for (b, sealed) in [(&interim, false), (&final_body, true)] {
        progress
            .accept(b, sealed, || {
                loads += 1;
                result = Some(cache.load(&directory, &context, b, 1)?);
                Ok(())
            })
            .unwrap();
    }
    let (account, objects) = result.unwrap();
    eprintln!(
        "actual capture READY4→READY5: {loads} complete loads, {} objects, {} bytes, {:?}",
        objects.len(),
        objects.iter().map(Vec::len).sum::<usize>(),
        start.elapsed()
    );
    assert_eq!(
        loads, 1,
        "only final challenge-bound reply may load the closure"
    );
    let independently_verified =
        tickwire_strategy_sdk::handover::objects(&directory, &final_body).unwrap();
    assert_eq!((account, objects), independently_verified);
    let mut invalid = final_body.clone();
    invalid.ready_sequence += 1;
    invalid.input_seal_bytes += 1;
    assert!(
        progress
            .accept(&invalid, true, || cache
                .load(&directory, &context, &invalid, 1)
                .map(|_| ()))
            .is_err()
    );
    assert_eq!(progress.last, Some(final_body));
}
