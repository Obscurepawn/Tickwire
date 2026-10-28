use super::capture_tests::{at, fixture, identity, start};
use super::*;
use crate::capture::{Bootstrap, Receipt, SealRequest};

fn bootstrap(f: &Fixture) -> Bootstrap {
    let mut b = crate::capture::tests::fixture();
    let bank = ResourceHeader::decode(&f.inputs.markets[0].resource).unwrap();
    let id = identity();
    b.identity = id.clone();
    b.contract = f.contract.clone();
    b.config = f.inputs.config.clone();
    b.models = f.inputs.models.clone();
    b.market.bank_header = f.inputs.markets[0].resource.clone();
    b.market.maximum_levels = f.inputs.markets[0].maximum_levels;
    b.market.instruments = vec![crate::bootstrap::Instrument {
        id: 1001,
        version: 5,
        required_states: 7,
    }];
    b.market.catalog_hash = [0x22; 32];
    b.market.instrument_set_version = 5;
    let local = &mut b.market.endpoint.local;
    local.instance = id.instance;
    local.generation = id.generation;
    local.logical_strategy = id.logical_strategy;
    local.host_boot = id.clock.boot_id;
    local.namespace = bank.namespace_id;
    local.build_hash = id.binary;
    b.market.endpoint.peer.instance = bank.producer_instance;
    b.market.endpoint.peer.generation = bank.producer_generation;
    b.report.local = b.market.endpoint.local.clone();
    let mut report = ResourceHeader::decode(&b.report_header).unwrap();
    report.producer_instance = b.report.peer.instance;
    report.producer_generation = b.report.peer.generation;
    report.consumer_instance = id.instance;
    report.host_boot_id = id.clock.boot_id;
    report.namespace_id = bank.namespace_id;
    b.report_header = crate::bootstrap::wire_bytes(&report).unwrap();
    b.initial_account_position = f.inputs.accounts[0].records.len() as u64;
    b.deadline_mono_ns = 10_000_000_000;
    b
}
fn bound_request(b: &Bootstrap, position: u64) -> SealRequest {
    SealRequest {
        schema: 1,
        capture: b.identity.capture,
        bootstrap: Object::of(&b.bytes().unwrap()),
        contract: Object::of(&b.contract.bytes().unwrap()),
        account_position: position,
        deadline_mono_ns: b.deadline_mono_ns,
    }
}

#[test]
fn authenticated_capture_receipt_binds_initial_cut_versions_and_actual_market_resource() {
    let mut f = fixture();
    let model = VersionedBytes {
        revision: 1,
        bytes: b"capture-model".to_vec(),
    };
    f.contract.models = vec![ModelBinding {
        id: 1,
        value: Object::of(&model.bytes),
    }];
    f.inputs.models = vec![Model {
        id: 1,
        value: model,
    }];
    let b = bootstrap(&f);
    b.validate(at(f.inputs.start_ns)).unwrap();
    let mut owner = start(&f);
    for row in &f.inputs.warm {
        owner.push(row.input.clone(), at(row.now_ns)).unwrap();
    }
    let done = owner.finish().unwrap();
    let mut seed = f.inputs.clone();
    seed.warm.clear();
    let window = capture::VerifiedWindow::verify(
        &identity(),
        &f.contract,
        &seed,
        done.inputs(),
        done.seal(),
    )
    .unwrap();
    let request = bound_request(&b, done.seal().end.accounts[0].sequence);
    let receipt = Receipt {
        schema: 1,
        bootstrap: request.bootstrap,
        request: Object::of(&request.bytes().unwrap()),
        completion: capture::store::Completion {
            schema: 1,
            attempt: Object::of(b"attempt"),
            identity: identity(),
            contract: done.seal().contract,
            seed: done.seal().seed,
            inputs: done.seal().inputs,
            seal: Object::of(&done.seal().bytes().unwrap()),
        },
        pid: std::process::id(),
        report_ring_sequence: ResourceHeader::decode(&b.report_header).unwrap().first_seq
            + request.account_position,
        market_applied: done.seal().end.markets[0].sequence,
        observed_mono_ns: done.seal().end.now_ns + 1,
    };
    let now = at(receipt.observed_mono_ns + 1);
    receipt
        .verify(&b, &request, receipt.pid, now, &window)
        .unwrap();
    for case in 0..5 {
        let mut changed = b.clone();
        match case {
            0 => changed.initial_account_position -= 1,
            1 => changed.config.revision += 1,
            2 => changed.models[0].value.revision += 1,
            3 => changed.market.maximum_levels += 1,
            _ => changed.market.instruments[0].version += 1,
        }
        changed.validate(now).unwrap();
        let changed_request = bound_request(&changed, request.account_position);
        let mut changed_receipt = receipt.clone();
        changed_receipt.bootstrap = changed_request.bootstrap;
        changed_receipt.request = Object::of(&changed_request.bytes().unwrap());
        assert!(
            changed_receipt
                .verify(&changed, &changed_request, receipt.pid, now, &window)
                .is_err(),
            "case {case}"
        );
    }
    let mut template = b.clone();
    let mut header = ResourceHeader::decode(&template.market.bank_header).unwrap();
    header.descriptor_hash = [0; 32];
    template.market.bank_header = crate::bootstrap::wire_bytes(&header).unwrap();
    let template_request = bound_request(&template, request.account_position);
    let mut template_receipt = receipt.clone();
    template_receipt.bootstrap = template_request.bootstrap;
    template_receipt.request = Object::of(&template_request.bytes().unwrap());
    template_receipt
        .verify(&template, &template_request, receipt.pid, now, &window)
        .unwrap();
    header.descriptor_hash = [99; 32];
    template.market.bank_header = crate::bootstrap::wire_bytes(&header).unwrap();
    assert!(template.validate(now).is_err());
}
