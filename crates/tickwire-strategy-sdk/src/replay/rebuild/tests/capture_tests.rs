use super::*;
use tickwire_clock::{ClockDomain as MonoDomain, MonoTime};
pub(super) fn at(n: u64) -> MonoTime {
    MonoTime::new(MonoDomain::new([8; 16], 1).unwrap(), n)
}
pub(super) fn identity() -> tickwire_release::rebuild::window::Identity {
    tickwire_release::rebuild::window::Identity {
        capture: [91; 16],
        logical_strategy: 8,
        risk_domain: [1; 16],
        instance: [6; 16],
        generation: 7,
        binary: [14; 32],
        clock: ClockDomain {
            boot_id: [8; 16],
            generation: 1,
        },
    }
}

fn bind_resource_clock(f: &mut Fixture, boot: [u8; 16], domain: u32) {
    let seed = &mut f.inputs.markets[0];
    let mut resource = ResourceHeader::decode(&seed.resource).unwrap();
    resource.host_boot_id = boot;
    resource.clock_domain = domain;
    resource.descriptor_hash = [0; 32];
    resource.descriptor_hash =
        tickwire_recovery::digest(&crate::bootstrap::wire_bytes(&resource).unwrap());
    resource.encode(&mut seed.resource).unwrap();
    let stream = application::market_stream(&resource).unwrap();
    f.contract.markets[0].stream = stream.clone();
    for row in &mut f.inputs.warm {
        match &mut row.input {
            Input::MarketApplied { stream: actual, .. }
            | Input::MarketCallback { stream: actual, .. } => *actual = stream.clone(),
            _ => {}
        }
    }
    f.plans.members[0].end.markets[0].stream = stream;
    f.plans.members[0].end.markets[0].content_digest =
        tickwire_recovery::digest(&canonical(&f.inputs.warm[0].input).unwrap());
}
pub(super) fn fixture() -> Fixture {
    let mut f = Fixture::new();
    bind_resource_clock(&mut f, [8; 16], 1);
    f
}

#[test]
fn capture_rejects_a_market_from_another_monotonic_clock_domain() {
    for (boot, domain) in [([99; 16], 1), ([8; 16], 2)] {
        let mut f = fixture();
        bind_resource_clock(&mut f, boot, domain);
        let mut seed = f.inputs.clone();
        seed.warm.clear();
        assert!(
            capture::CaptureOnly::new(identity(), f.contract, seed, at(f.inputs.start_ns)).is_err()
        );
    }
}

#[test]
fn capture_only_builds_a_window_from_actual_market_and_account_inputs() {
    let f = fixture();
    let mut seed = f.inputs.clone();
    seed.warm.clear();
    let mut capture =
        capture::CaptureOnly::new(identity(), f.contract.clone(), seed, at(f.inputs.start_ns))
            .expect("create an input-only owner without a user factory");
    for row in &f.inputs.warm {
        capture.push(row.input.clone(), at(row.now_ns)).unwrap();
    }
    let done = capture.finish().unwrap();
    assert_eq!(done.inputs(), &f.inputs);
    assert_eq!(done.seal().end, f.plans.members[0].end);
    assert_eq!(done.seal().start.rows, 0);
    assert_eq!(done.seal().inputs, Object::of(&f.inputs.bytes().unwrap()));
    assert_eq!(done.seal().identity, identity());
    assert!(
        !String::from_utf8(done.seal().bytes().unwrap())
            .unwrap()
            .contains("checkpoint")
    );
}

pub(super) fn start(f: &Fixture) -> capture::CaptureOnly {
    let mut seed = f.inputs.clone();
    seed.warm.clear();
    capture::CaptureOnly::new(identity(), f.contract.clone(), seed, at(f.inputs.start_ns)).unwrap()
}

#[test]
fn captured_window_is_recomputed_from_the_complete_bound_inputs() {
    let f = fixture();
    let mut seed = f.inputs.clone();
    seed.warm.clear();
    let mut owner = start(&f);
    for row in &f.inputs.warm {
        owner.push(row.input.clone(), at(row.now_ns)).unwrap();
    }
    let done = owner.finish().unwrap();
    let verified = capture::VerifiedWindow::verify(
        &identity(),
        &f.contract,
        &seed,
        done.inputs(),
        done.seal(),
    )
    .expect("recompute the actual captured input window");
    assert_eq!(verified.inputs(), &f.inputs);
    assert_eq!(verified.seal(), done.seal());
}

#[test]
fn window_verification_rejects_structurally_valid_false_seals_and_changed_rows() {
    let f = fixture();
    let mut seed = f.inputs.clone();
    seed.warm.clear();
    let mut owner = start(&f);
    assert!(owner.qualified().is_err());
    for row in &f.inputs.warm {
        owner.push(row.input.clone(), at(row.now_ns)).unwrap();
    }
    owner.qualified().unwrap();
    let done = owner.finish().unwrap();
    for case in 0..14 {
        let mut id = identity();
        let mut contract = f.contract.clone();
        let mut seed = seed.clone();
        let mut inputs = f.inputs.clone();
        let mut seal = done.seal().clone();
        match case {
            0 => seal.input_chain = [88; 32],
            1 => seal.account_input_digest = [88; 32],
            2 => seal.end.accounts[0].content_digest = [88; 32],
            3 => seal.end.markets[0].sequence += 1,
            4 => seal.start.now_ns -= 1,
            5 => seal.end.rows += 1,
            6 => id.capture = [88; 16],
            7 => {
                inputs.warm.remove(0);
            }
            8 => inputs.warm[0].now_ns = seed.start_ns - 1,
            9 => seed.accounts[0].records.clear(),
            10 => contract.actions.clear(),
            11 => contract.markets[0].minimum_observations = 2,
            12 => inputs.config.revision += 1,
            _ => {
                inputs.warm.push(inputs.warm[1].clone());
                seal.inputs = Object::of(&inputs.bytes().unwrap());
            }
        }
        assert!(
            capture::VerifiedWindow::verify(&id, &contract, &seed, &inputs, &seal).is_err(),
            "case {case}"
        );
    }
}

#[test]
fn verified_window_checks_all_available_plan_bindings() {
    let f = fixture();
    let mut seed = f.inputs.clone();
    seed.warm.clear();
    let mut owner = start(&f);
    for row in &f.inputs.warm {
        owner.push(row.input.clone(), at(row.now_ns)).unwrap();
    }
    let done = owner.finish().unwrap();
    let verified = capture::VerifiedWindow::verify(
        &identity(),
        &f.contract,
        &seed,
        done.inputs(),
        done.seal(),
    )
    .unwrap();
    let mut plan = f.plans.members[0].clone();
    plan.contract = done.seal().contract;
    plan.inputs = done.seal().inputs;
    verified.validate_plan(&plan).unwrap();
    for case in 0..16 {
        let mut p = plan.clone();
        match case {
            0 => p.binding.logical_strategy += 1,
            1 => p.target.logical_strategy += 1,
            2 => p.target.instance = [88; 16],
            3 => p.target.generation += 1,
            4 => p.target.binary_digest = [88; 32],
            5 => p.target.state_schema += 1,
            6 => p.target.clock.generation += 1,
            7 => p.member.risk_domain = [88; 16],
            8 => p.member.new_instance = [88; 16],
            9 => p.member.new_generation += 1,
            10 => p.member.clock_boot_id = [88; 16],
            11 => p.member.clock_generation += 1,
            12 => p.member.valid_until_ns = p.end.now_ns,
            13 => p.contract.digest = [88; 32],
            14 => p.inputs.digest = [88; 32],
            _ => p.end.markets[0].content_digest = [88; 32],
        }
        assert!(verified.validate_plan(&p).is_err(), "plan case {case}");
    }
}

#[test]
fn self_consistent_hashes_do_not_turn_duplicate_callbacks_into_observations() {
    let f = fixture();
    let mut seed = f.inputs.clone();
    seed.warm.clear();
    let mut owner = start(&f);
    for row in &f.inputs.warm {
        owner.push(row.input.clone(), at(row.now_ns)).unwrap();
    }
    let done = owner.finish().unwrap();
    let mut contract = f.contract.clone();
    contract.markets[0].minimum_observations = 2;
    let mut inputs = done.inputs().clone();
    inputs.warm.push(inputs.warm[1].clone());
    let mut seal = done.seal().clone();
    seal.contract = Object::of(&contract.bytes().unwrap());
    seal.inputs = Object::of(&inputs.bytes().unwrap());
    seal.end.rows = inputs.warm.len() as u64;
    seal.input_chain = [0; 32];
    for (i, row) in inputs.warm.iter().enumerate() {
        seal.input_chain =
            tickwire_release::rebuild::window::next_chain(seal.input_chain, i as u64 + 1, row)
                .unwrap();
    }
    seal.bytes().unwrap();
    assert!(matches!(
        capture::VerifiedWindow::verify(&identity(), &contract, &seed, &inputs, &seal,),
        Err(Error::StateMismatch)
    ));
}

#[test]
fn capture_rejects_wrong_seed_identity_clock_and_contract() {
    for case in 0..7 {
        let f = fixture();
        let mut seed = f.inputs.clone();
        seed.warm.clear();
        let mut id = identity();
        let mut contract = f.contract.clone();
        let mut now = at(seed.start_ns);
        match case {
            0 => seed.warm = f.inputs.warm.clone(),
            1 => id.risk_domain = [99; 16],
            2 => id.binary = [99; 32],
            3 => seed.config.bytes.push(1),
            4 => now = MonoTime::new(MonoDomain::new([99; 16], 1).unwrap(), seed.start_ns),
            5 => contract.ordering = Ordering::RequiresOldProcessOrder,
            _ => now = at(seed.start_ns + 1),
        }
        assert!(
            capture::CaptureOnly::new(id, contract, seed, now).is_err(),
            "seed case {case}"
        );
    }
}

#[test]
fn failed_capture_push_keeps_the_exact_previous_window() {
    let f = fixture();
    for case in 0..6 {
        let mut owner = start(&f);
        let row = &f.inputs.warm[0];
        let mut input = row.input.clone();
        let mut now = at(row.now_ns);
        match case {
            0 => now = at(f.inputs.start_ns - 1),
            1 => now = MonoTime::new(MonoDomain::new([99; 16], 1).unwrap(), row.now_ns),
            2 => {
                if let Input::MarketApplied { sequence, .. } = &mut input {
                    *sequence += 1
                }
            }
            3 => {
                if let Input::MarketApplied { record, .. } = &mut input {
                    record[0] ^= 1
                }
            }
            4 => now = at(f.inputs.start_ns + f.contract.maximum_span_ns + 1),
            _ => {
                input = Input::ConfigApply {
                    previous: f.contract.config,
                    next: f.inputs.config.clone(),
                }
            }
        }
        assert!(owner.push(input, now).is_err(), "push case {case}");
        for row in &f.inputs.warm {
            owner.push(row.input.clone(), at(row.now_ns)).unwrap();
        }
        let result = owner.finish().unwrap();
        assert_eq!(result.inputs(), &f.inputs, "push case {case}");
        assert_eq!(result.seal().end, f.plans.members[0].end);
    }
}

#[test]
fn capture_finish_requires_actual_rows_span_and_distinct_observations() {
    let f = fixture();
    assert!(start(&f).finish().is_err());
    let mut owner = start(&f);
    owner
        .push(f.inputs.warm[0].input.clone(), at(f.inputs.warm[0].now_ns))
        .unwrap();
    assert!(owner.finish().is_err());
    let mut f = fixture();
    f.contract.markets[0].minimum_observations = 2;
    let mut owner = start(&f);
    for row in &f.inputs.warm {
        owner.push(row.input.clone(), at(row.now_ns)).unwrap();
    }
    owner
        .push(f.inputs.warm[1].input.clone(), at(f.inputs.warm[1].now_ns))
        .unwrap();
    assert!(owner.finish().is_err());
    let mut f = fixture();
    f.contract.minimum_span_ns += 1;
    let mut owner = start(&f);
    for row in &f.inputs.warm {
        owner.push(row.input.clone(), at(row.now_ns)).unwrap();
    }
    assert!(owner.finish().is_err());
}

#[test]
fn capture_enforces_window_budget_before_accepting_a_row() {
    for bytes_limit in [false, true] {
        let mut f = fixture();
        if bytes_limit {
            f.contract.maximum_bytes = f.inputs.bytes().unwrap().len() as u64;
        } else {
            f.contract.maximum_rows = 2;
        }
        let mut owner = start(&f);
        for row in &f.inputs.warm {
            owner.push(row.input.clone(), at(row.now_ns)).unwrap();
        }
        let row = f.inputs.warm.last().unwrap();
        assert!(owner.push(row.input.clone(), at(row.now_ns)).is_err());
        assert_eq!(owner.finish().unwrap().inputs(), &f.inputs);
    }
}

#[test]
fn capture_seals_real_account_records_and_ordered_input_chain() {
    let f = fixture();
    let mut owner = start(&f);
    let mut rows = f.inputs.warm.clone();
    let prefix = &f.inputs.accounts[0];
    let baseline = CanonicalBaseline::decode(&prefix.baseline).unwrap();
    let (_, _, command) = crate::account_fixture::fixture();
    let event = crate::account_fixture::event(crate::account_fixture::fill(
        &command,
        "captured-fill",
        "0.0005",
        "60000",
        "30",
    ));
    let fill = CanonicalRecord::new(
        baseline.digest(),
        2,
        RecoveryRecord::VenueEvent(Box::new(event)),
    )
    .unwrap();
    rows.push(TimedInput {
        now_ns: f.inputs.warm.last().unwrap().now_ns,
        input: Input::AccountCallback {
            stream: account::AccountReplica::stream(&baseline),
            position: 2,
            canonical: fill.bytes().to_vec(),
        },
    });
    let mut chain = [0; 32];
    for (i, row) in rows.iter().enumerate() {
        owner.push(row.input.clone(), at(row.now_ns)).unwrap();
        chain = tickwire_release::rebuild::window::next_chain(chain, i as u64 + 1, row).unwrap();
    }
    let row = rows.last().unwrap();
    assert!(owner.push(row.input.clone(), at(row.now_ns)).is_err());
    let completed = owner.finish().unwrap();
    let mut account_seal = tickwire_account_codec::InputSeal::new(baseline.digest()).unwrap();
    for raw in &prefix.records {
        account_seal
            .apply(&CanonicalRecord::decode(raw).unwrap())
            .unwrap();
    }
    account_seal.apply(&fill).unwrap();
    assert_eq!(completed.inputs().warm, rows);
    assert_eq!(completed.seal().input_chain, chain);
    assert_eq!(completed.seal().account_input_digest, account_seal.digest());
    assert_eq!(completed.seal().end.accounts[0].sequence, 2);
    assert_eq!(
        tickwire_release::rebuild::window::Seal::decode(&completed.seal().bytes().unwrap())
            .unwrap(),
        *completed.seal()
    );
}
