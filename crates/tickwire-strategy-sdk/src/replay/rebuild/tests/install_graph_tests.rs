use super::*;
use tickwire_release::rebuild::install;

#[test]
fn installation_graph_checks_the_actual_warm_chain_and_every_persisted_dependency() {
    let mut f = capture_tests::fixture();
    f.op.schema = 4;
    f.rebind();
    let mut capture = capture_tests::start(&f);
    for row in &f.inputs.warm {
        capture
            .push(row.input.clone(), capture_tests::at(row.now_ns))
            .unwrap();
    }
    let captured = capture.finish().unwrap();
    let seal = captured.seal().clone();
    let path = std::env::temp_dir().join(format!(
        "tw-install-graph-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let factory = TestFactory::new();
    let running = execute_running(&factory, f.input().unwrap())
        .unwrap()
        .persist(tickwire_recovery::FileBlobStore::open(&path).unwrap())
        .unwrap();
    let completion = running.computation().completion();
    let mut objects = std::collections::BTreeMap::<Hash, Vec<u8>>::new();
    for object in [
        completion.operation,
        completion.plans,
        completion.manifest,
        completion.contract,
        completion.inputs,
        completion.anchor,
        completion.checkpoint,
        completion.transcript,
        running.computation().object(),
    ] {
        objects.insert(object.digest, running.owner.read_object(object).unwrap());
    }
    for bytes in running.history_objects().unwrap() {
        objects.insert(Object::of(&bytes).digest, bytes);
    }
    let mut seed = f.inputs.clone();
    seed.warm.clear();
    let capture_dir = path.join("capture");
    let capture_completion =
        capture::store::Attempt::begin(&capture_dir, &seal.identity, &f.contract, &seed)
            .unwrap()
            .finish(captured)
            .unwrap();
    let window = capture::store::read_back(&capture_dir, &capture_completion).unwrap();
    let mut capture_bootstrap = crate::capture::tests::fixture();
    capture_bootstrap.identity = seal.identity.clone();
    capture_bootstrap.contract = f.contract.clone();
    capture_bootstrap.config = seed.config.clone();
    capture_bootstrap.models = seed.models.clone();
    capture_bootstrap.initial_account_position = seal.start.accounts[0].sequence;
    capture_bootstrap.deadline_mono_ns = f.op.members[0].valid_until_ns;
    let capture_request = crate::capture::SealRequest {
        schema: 1,
        capture: seal.identity.capture,
        bootstrap: Object::of(&capture_bootstrap.bytes().unwrap()),
        contract: seal.contract,
        account_position: seal.end.accounts[0].sequence,
        deadline_mono_ns: capture_bootstrap.deadline_mono_ns,
    };
    let capture_receipt = crate::capture::Receipt {
        schema: 1,
        bootstrap: capture_request.bootstrap,
        request: Object::of(&capture_request.bytes().unwrap()),
        completion: capture_completion.clone(),
        pid: std::process::id(),
        report_ring_sequence: ResourceHeader::decode(&capture_bootstrap.report_header)
            .unwrap()
            .first_seq
            + capture_request.account_position,
        market_applied: seal.end.markets[0].sequence,
        observed_mono_ns: seal.end.now_ns,
    };
    for raw in window.objects().map(<[u8]>::to_vec).chain([
        capture_bootstrap.bytes().unwrap(),
        capture_request.bytes().unwrap(),
        capture_receipt.bytes().unwrap(),
    ]) {
        objects.insert(Object::of(&raw).digest, raw);
    }
    let request = install::Request {
        schema: 1,
        operation: f.op.clone(),
        member: f.op.members[0].clone(),
        plans: Object::of(&f.plans.bytes().unwrap()),
        manifest: Object::of(&canonical(&f.manifest).unwrap()),
        capture_completion: Object::of(&capture_completion.bytes().unwrap()),
        capture_receipt: Object::of(&capture_receipt.bytes().unwrap()),
        capture_seal: Object::of(&seal.bytes().unwrap()),
        target_bootstrap: Object::of(b"qualified-target-bootstrap"),
        deadline_mono_ns: f.op.members[0].valid_until_ns,
    };
    let installation = install::Installation {
        schema: 1,
        request: Object::of(&request.bytes().unwrap()),
        capture_completion: request.capture_completion,
        capture_seal: request.capture_seal,
        capture_receipt: request.capture_receipt,
        warm_completion: running.computation().object(),
        target: f.plans.members[0].target.clone(),
        anchor: Object::of(&running.anchor().bytes().unwrap()),
        checkpoint: Object::of(&running.checkpoint().bytes().unwrap()),
        genesis_commit: running.genesis_commit(),
        commit: running.commit(),
        head: Object::of(&running.head().unwrap().bytes().unwrap()),
        pid: std::process::id(),
        completed_ns: seal.end.now_ns + 1,
    };
    for bytes in [
        request.bytes().unwrap(),
        seal.bytes().unwrap(),
        running.head().unwrap().bytes().unwrap(),
        b"qualified-target-bootstrap".to_vec(),
    ] {
        objects.insert(Object::of(&bytes).digest, bytes);
    }
    let bytes = installation.bytes().unwrap();
    let root = Object::of(&bytes);
    objects.insert(root.digest, bytes);
    let verify = |root, objects: &std::collections::BTreeMap<Hash, Vec<u8>>| {
        install::Verified::verify(
            root,
            &request,
            &f.plans,
            &f.contract,
            &f.inputs,
            &f.manifest,
            &seal,
            |o| {
                objects
                    .get(&o.digest)
                    .cloned()
                    .ok_or(tickwire_release::Error::Missing)
            },
        )
    };
    let verified = verify(root, &objects).expect("complete actual warm graph");
    assert_eq!(verified.checkpoint(), running.checkpoint());
    assert_eq!(factory.calls.get(), 1);
    for missing in objects.keys() {
        let mut bad = objects.clone();
        bad.remove(missing);
        assert!(verify(root, &bad).is_err(), "missing {missing:?}");
    }
    for case in 0..8 {
        let mut bad = installation.clone();
        match case {
            0 => bad.genesis_commit = bad.commit,
            1 => bad.anchor = bad.checkpoint,
            2 => bad.capture_receipt = bad.capture_seal,
            3 => bad.target.input_stream = [99; 16],
            4 => bad.completed_ns = request.deadline_mono_ns,
            5 => bad.completed_ns = seal.end.now_ns - 1,
            6 => bad.head = bad.checkpoint,
            _ => bad.warm_completion = bad.capture_completion,
        }
        let bytes = bad.bytes().unwrap();
        let bad_root = Object::of(&bytes);
        let mut graph = objects.clone();
        graph.insert(bad_root.digest, bytes);
        assert!(
            verify(bad_root, &graph).is_err(),
            "rebound installation {case}"
        );
    }
    let mut dependencies = Vec::new();
    for kind in 2u8..=11 {
        let bytes = vec![kind; 32];
        let file = path.join(format!("dependency-{kind}.bin"));
        std::fs::write(&file, &bytes).unwrap();
        dependencies.push(crate::recovery::DependencyFile {
            kind,
            schema_id: 1,
            owner: installation.target.instance,
            path: file,
            digest: tickwire_recovery::digest(&bytes),
        });
    }
    let template = crate::recovery::Template {
        dependencies,
        declared_rebuild: true,
        fault_model_id: [1; 32],
        pin_id: [2; 16],
        owner: installation.target.instance,
        input_stream: installation.target.input_stream,
        output_directory: path.join("recovery"),
        maximum_input_bytes: MAX_BYTES,
        order_ids: vec![],
    };
    let mut live = live::Live::from_rebuilt(running, path.clone()).unwrap();
    let (mut writer, mirror) = crate::recovery::RecoveryWriter::from_rebuild(
        template,
        &window,
        &live,
        &verified,
        &objects.values().cloned().collect::<Vec<_>>(),
    )
    .unwrap();
    let mut identity = crate::capture::tests::fixture().market.endpoint.local;
    identity.instance = installation.target.instance;
    identity.generation = installation.target.generation;
    identity.logical_strategy = installation.target.logical_strategy;
    let initial = mirror
        .ready(&identity, 2, 1, &writer, &live.checkpoint().unwrap())
        .unwrap();
    let recovery_path = path.join("recovery");
    let (_, exported) = crate::handover::objects(&recovery_path, &initial)
        .expect("v4 initial READY verifies its complete warm chain");
    for required in [
        capture_completion.attempt,
        capture_completion.seed,
        capture_receipt.bootstrap,
        capture_receipt.request,
    ] {
        assert!(
            exported.iter().any(|b| Object::of(b) == required),
            "capture dependency missing from exported v4 closure"
        );
        let file = recovery_path.join(format!(
            "{}.bin",
            required
                .digest
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>()
        ));
        let saved = std::fs::read(&file).unwrap();
        std::fs::remove_file(&file).unwrap();
        assert!(crate::handover::objects(&recovery_path, &initial).is_err());
        std::fs::write(&file, saved).unwrap();
    }
    let seal_path = recovery_path.join(format!(
        "{}.bin",
        initial
            .input_seal_digest
            .iter()
            .map(|v| format!("{v:02x}"))
            .collect::<String>()
    ));
    let initial_seal =
        tickwire_recovery::HandoverSeal::decode(&std::fs::read(seal_path).unwrap()).unwrap();
    assert_eq!(initial_seal.version, 4);
    assert_eq!(initial_seal.rebuild_installation_digest, Some(root.digest));
    assert_eq!(initial_seal.rebuild_rollback_digest, None);
    let mut raw = match &f.inputs.warm[0].input {
        Input::MarketApplied { record, .. } => record.clone(),
        _ => unreachable!(),
    };
    let mut header = RecordHeader::decode(&raw[..RecordHeader::SIZE]).unwrap();
    header.sequence += 1;
    header.ingress_mono_ns = 1_700_000_000;
    header.publish_mono_ns = 1_700_000_000;
    header.encode(&mut raw[..RecordHeader::SIZE]).unwrap();
    live.market(&raw, &[], 1_700_000_000).unwrap();
    for row in live.recorded_after(verified.checkpoint().cut.sequence) {
        writer.merged_row(row).unwrap();
    }
    writer
        .merged_checkpoint(live.committed().unwrap().0)
        .unwrap();
    let continued = mirror
        .ready(&identity, 2, 2, &writer, &live.checkpoint().unwrap())
        .unwrap();
    crate::handover::objects(&recovery_path, &continued)
        .expect("v4 READY preserves C-after actual market callbacks");
    assert_eq!(factory.calls.get(), 1);
    let missing = recovery_path.join(format!(
        "{}.bin",
        installation
            .genesis_commit
            .digest
            .iter()
            .map(|v| format!("{v:02x}"))
            .collect::<String>()
    ));
    std::fs::remove_file(missing).unwrap();
    assert!(crate::handover::objects(&recovery_path, &continued).is_err());
    drop(live);
    std::fs::remove_dir_all(path).unwrap();
}
