use super::*;

struct Directory(PathBuf);
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn fixture() -> (
    Directory,
    Driver,
    Ed25519KeyPair,
    tickwire_lifecycle::Signer,
    tickwire_lifecycle::ControlRequest,
) {
    let boot = [90; 16];
    let now = tickwire_clock_sys::SystemClock::new(ClockDomain::new(boot, 1).unwrap())
        .unwrap()
        .sample()
        .unwrap()
        .monotonic
        .nanos();
    let root = std::env::temp_dir().join(format!("tickwire-lab-halt-{}-{now}", std::process::id()));
    fs::create_dir(&root).unwrap();
    for path in [
        "handover/staging/01010101010101010101010101010101",
        "handover/requests",
        "handover/receipts",
    ] {
        fs::create_dir_all(root.join(path)).unwrap();
    }
    let bytes = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).unwrap();
    let signer = tickwire_lifecycle::Signer::from_pkcs8(bytes.as_ref()).unwrap();
    let pair = Ed25519KeyPair::from_pkcs8(bytes.as_ref()).unwrap();
    let member = Member {
        risk_domain: [1; 16],
        exec_instance: [4; 16],
        exec_epoch: 1,
        old_instance: [2; 16],
        old_generation: 5,
        new_instance: NEW_INSTANCE,
        new_generation: GENERATION,
        clock_boot_id: boot,
        clock_generation: 1,
        valid_until_ns: u64::MAX,
    };
    let operation = Operation {
        schema: 3,
        operation_id: [140; 16],
        operation_revision: 1,
        logical_strategy_id: 8,
        members: vec![member.clone()],
        artifacts_digest: [1; 32],
        checkpoint_digest: [2; 32],
        rollback_digest: [3; 32],
        clock_boot_id: boot,
        clock_generation: 1,
        valid_until_ns: u64::MAX,
    };
    let scenario = Scenario {
        schema: 1,
        kind: ScenarioKind::SpotHandover,
        fixture_only: true,
        root: root.clone(),
        bin_directory: root.clone(),
        listen: "127.0.0.1:1".parse().unwrap(),
        mirror: "127.0.0.1:2".parse().unwrap(),
        valid_until_ns: u64::MAX,
        boot,
        certificate: root.clone(),
        private_key: root.clone(),
        api_key: root.clone(),
        api_secret: root.clone(),
        md_config: root.clone(),
        app_config: root.clone(),
        mirror_config: root.clone(),
        worker_bootstrap: root.clone(),
    };
    let request = tickwire_lifecycle::ControlRequest {
        schema: 2,
        coordinator_id: [138; 16],
        operation: operation.clone(),
        member,
        request_id: [44; 16],
        action: ControlAction::Halt,
        proof: None,
        release: None,
        replay: None,
        rebuild: None,
        valid_until_ns: u64::MAX,
    };
    let driver = Driver {
        scenario,
        coordinator: None,
        operation: Some(operation),
        requests: BTreeMap::new(),
        replay: None,
        armed: true,
        receipts: BTreeMap::new(),
        keys: json!({"coordinator": signer.public_key(), "exec": signer.public_key()}),
        launched: true,
        grant_published: false,
        released: false,
        halt_pending: false,
        old_pid: Some(1),
        exec_pid: 2,
    };
    (Directory(root), driver, pair, signer, request)
}
fn signed(
    pair: &Ed25519KeyPair,
    request: &tickwire_lifecycle::ControlRequest,
) -> SignedControlRequest {
    let payload = serde_json::to_vec(request).unwrap();
    let mut message = b"tickwire.lifecycle.control.v1".to_vec();
    message.extend(&payload);
    SignedControlRequest {
        payload,
        signature: pair.sign(&message).as_ref().to_vec(),
    }
}

#[test]
fn signed_halt_is_relayed_unchanged_and_actual_terminal_receipt_is_retained() {
    let (directory, mut driver, pair, signer, request) = fixture();
    let signed = signed(&pair, &request);
    let bytes = serde_json::to_vec(&signed).unwrap();
    let name = "00000000000000000001-halt.json";
    fs::write(
        directory
            .0
            .join("handover/staging/01010101010101010101010101010101")
            .join(name),
        &bytes,
    )
    .unwrap();
    driver
        .relay()
        .expect("an authenticated coordinator halt must reach the execution owner");
    assert_eq!(
        fs::read(directory.0.join("handover/requests").join(name)).unwrap(),
        bytes
    );
    assert!(!driver.grant_published && !driver.released);
    assert!(driver.halt_pending);
    assert_eq!(driver.requests.get(&request.request_id), Some(&signed));
    driver.relay().unwrap();
    let receipt = tickwire_lifecycle::AppliedReceipt {
        schema: 1,
        operation_id: request.operation.operation_id,
        operation_revision: request.operation.operation_revision,
        members_digest: request.operation.members_digest().unwrap(),
        logical_strategy_id: 8,
        member: request.member,
        phase: AppliedPhase::Halted,
        state_version: 1,
        handover_sequence: 0,
        handover_digest: [0; 32],
        risk_gate_revision: 2,
        control_request_id: request.request_id,
        issued_ns: driver.now().unwrap().nanos(),
        valid_until_ns: u64::MAX,
    };
    let response = signer.sign_receipt(&receipt).unwrap();
    fs::write(
        directory.0.join("handover/receipts/halted.json"),
        serde_json::to_vec(&response).unwrap(),
    )
    .unwrap();
    assert_eq!(
        driver.collect_receipts().unwrap_err().to_string(),
        "handover halted after applied receipt"
    );
    assert_eq!(driver.receipts.get(&4), Some(&receipt));
    assert!(!driver.halt_pending);
    assert_eq!(
        fs::read(directory.0.join("handover/halted.json")).unwrap(),
        serde_json::to_vec(&response).unwrap()
    );
    assert!(!driver.released);
}

#[test]
fn forged_halt_is_not_relayed() {
    let (directory, mut driver, pair, _, request) = fixture();
    let mut signed = signed(&pair, &request);
    signed.signature[0] ^= 1;
    fs::write(
        directory
            .0
            .join("handover/staging/01010101010101010101010101010101/forged.json"),
        serde_json::to_vec(&signed).unwrap(),
    )
    .unwrap();
    assert!(driver.relay().is_err());
    assert!(driver.requests.is_empty());
    assert_eq!(
        fs::read_dir(directory.0.join("handover/requests"))
            .unwrap()
            .count(),
        0
    );
}
