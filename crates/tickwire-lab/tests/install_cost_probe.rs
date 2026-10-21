//! Diagnostic using the retained fourth Linux scenario, never a timing assertion.
#[test]
#[ignore = "requires TICKWIRE_INSTALL_COST_FIXTURE from a retained Linux run"]
fn retained_target_ready_and_cut_cost() {
    use std::{fs, path::PathBuf, time::Instant};
    let root = PathBuf::from(std::env::var_os("TICKWIRE_INSTALL_COST_FIXTURE").unwrap());
    let ready: serde_json::Value = serde_json::from_slice(
        &fs::read(root.join("next/reports/CONSUMED-06060606060606060606060606060606-6.json"))
            .unwrap(),
    )
    .unwrap();
    let h: [u8; 32] = serde_json::from_value(ready["input_seal_digest"].clone()).unwrap();
    let directory = root.join("next/recovery");
    let name = h.iter().map(|b| format!("{b:02x}")).collect::<String>();
    let raw = fs::read(directory.join(format!("{name}.bin"))).unwrap();
    let seal = tickwire_recovery::HandoverSeal::decode(&raw).unwrap();
    let body = tickwire_ipc::HandoverReadyBody {
        instance_id: seal.instance,
        logical_strategy_id: seal.logical_strategy,
        strategy_generation: seal.generation,
        exec_report_sequence: seal.report_position,
        ledger_sequence: seal.ledger_sequence,
        orders_digest: seal.orders_digest,
        checkpoint_digest: seal.checkpoint_digest,
        checkpoint_bytes: seal.checkpoint_bytes,
        input_seal_digest: h,
        input_seal_bytes: raw.len() as u64,
        ready_sequence: ready["ready_sequence"].as_u64().unwrap(),
        report_ring_sequence: ready["report_sequence"].as_u64().unwrap(),
    };
    for phase in [
        "observe_pending_ready",
        "observe_capture_reply",
        "load_objects_for_cut",
    ] {
        let start = Instant::now();
        let (_, objects) = tickwire_strategy_sdk::handover::objects(&directory, &body).unwrap();
        eprintln!(
            "{phase}: objects={} bytes={} elapsed={:?}",
            objects.len(),
            objects.iter().map(Vec::len).sum::<usize>(),
            start.elapsed()
        );
        if phase != "load_objects_for_cut" {
            continue;
        }
        let start = Instant::now();
        let mut hashed = 0usize;
        let mut find = |hash| -> &[u8] {
            objects
                .iter()
                .find(|b| {
                    hashed += b.len();
                    tickwire_recovery::digest(b) == hash
                })
                .unwrap()
        };
        let seal = tickwire_recovery::HandoverSeal::decode(find(h)).unwrap();
        let retained = tickwire_release::replay::retained::decode(find(seal.input_digest)).unwrap();
        let current = retained.current();
        let business =
            tickwire_strategy_sdk::replay::application::business_bytes(&current.business).unwrap();
        assert_eq!(
            find(seal.checkpoint_digest).get(96..),
            Some(business.as_slice())
        );
        assert_eq!(current.source.instance, seal.instance);
        eprintln!(
            "source_replay_cut: hashed={hashed} elapsed={:?} sequence={}",
            start.elapsed(),
            current.cut.sequence
        );
    }
}

#[cfg(unix)]
#[test]
#[ignore = "requires TICKWIRE_INSTALL_COST_FIXTURE from a retained Linux run"]
fn retained_install_source_proof_cost() {
    use std::{fs, os::unix::fs::MetadataExt, path::PathBuf, time::Instant};
    use tickwire_release::replay::ControlInput;
    let root = PathBuf::from(std::env::var_os("TICKWIRE_INSTALL_COST_FIXTURE").unwrap());
    let directory = root.join("next/reports");
    let raw =
        fs::read(directory.join("REPLAY-INSTALL-06060606060606060606060606060606-6.json")).unwrap();
    let ControlInput::Install { request, .. } = ControlInput::decode(&raw).unwrap() else {
        panic!("retained control is not Install")
    };
    let objects = tickwire_recovery::FileBlobReader::open(
        &directory,
        fs::metadata(&directory).unwrap().uid(),
    )
    .unwrap();
    let mut reads = 0usize;
    let mut bytes = 0usize;
    let start = Instant::now();
    let proof = request
        .source_proof(|object| {
            let value = objects
                .get_object(object.digest, tickwire_release::replay::MAX_BYTES)
                .map_err(|_| tickwire_release::Error::Missing)?;
            reads += 1;
            bytes += value.len();
            Ok(value)
        })
        .unwrap();
    eprintln!(
        "install_source_proof: reads={reads} bytes={bytes} elapsed={:?} fixed={} current={}",
        start.elapsed(),
        proof.fixed().cut.sequence,
        proof.current().cut.sequence,
    );
    assert_eq!(reads, request.source_objects.len());
    assert!(proof.current().cut.sequence > proof.fixed().cut.sequence);
}
