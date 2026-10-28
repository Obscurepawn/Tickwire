use super::*;
use crate::transition_fixture as fixture;
#[test]
fn authenticated_commands_bind_all_plan_and_object_fields() {
    let a = fixture::approval();
    a.validate(
        &a.previous,
        tickwire_clock::MonoTime::new(a.previous.domain().unwrap(), fixture::NOW),
    )
    .unwrap();
    let bytes = serde_json::to_vec(&a).unwrap();
    let (host, mut worker) = fixture::sessions();
    let b = request(&a, ARM, &bytes, host.deadline()).unwrap();
    let raw = encode_request(&host, &b).unwrap();
    let frame = worker.validate(&raw, fixture::NOW).unwrap();
    verify_request(
        &frame.envelope,
        &b,
        &a,
        &bytes,
        fixture::NOW,
        host.deadline(),
    )
    .unwrap();
    for case in 0..14 {
        let mut changed = b.clone();
        match case {
            0 => changed.instance_id = [1; 16],
            1 => changed.strategy_generation += 1,
            2 => changed.logical_strategy_id += 1,
            3 => changed.challenge = [1; 16],
            4 => changed.object_digest = [1; 32],
            5 => changed.object_bytes += 1,
            6 => changed.expected_sequence = 1,
            7 => changed.expected_cut_digest = [1; 32],
            8 => changed.operation_id = [1; 16],
            9 => changed.operation_revision += 1,
            10 => changed.schema = 2,
            11 => changed.replay_kind = COMMIT,
            12 => changed.valid_until_mono_ns += 1,
            13 => changed.replay_kind = 5,
            _ => unreachable!(),
        }
        assert!(
            verify_request(
                &frame.envelope,
                &changed,
                &a,
                &bytes,
                fixture::NOW,
                host.deadline()
            )
            .is_err(),
            "case {case}"
        );
    }
    assert!(
        verify_request(
            &frame.envelope,
            &b,
            &a,
            &bytes,
            b.valid_until_mono_ns,
            host.deadline()
        )
        .is_err()
    );
}
#[test]
fn explicit_rejection_echoes_request_and_does_not_forge_armed() {
    let a = fixture::approval();
    let bytes = serde_json::to_vec(&a).unwrap();
    let (mut host, worker) = fixture::sessions();
    let request = request(&a, ARM, &bytes, host.deadline()).unwrap();
    let raw = encode_rejection(&worker, &request, UNSUPPORTED).unwrap();
    let f = host.validate(&raw, fixture::NOW).unwrap();
    assert_eq!(
        verify_rejection(&f.envelope, &request, &request, fixture::NOW).unwrap(),
        UNSUPPORTED
    );
    let mut bad = request.clone();
    bad.object_digest = [7; 32];
    assert!(verify_rejection(&f.envelope, &bad, &request, fixture::NOW).is_err());
}

#[test]
fn interrupted_send_keeps_the_packet_pending_without_retrying_inside_the_turn() {
    let interrupted = tickwire_control_sys::Error::System {
        operation: "sendmsg",
        source: std::io::Error::from(std::io::ErrorKind::Interrupted),
    };
    assert!(!send_progress(Err(interrupted)).unwrap());
    assert!(!send_progress(Ok(false)).unwrap());
    assert!(send_progress(Ok(true)).unwrap());
    assert!(send_progress(Err(tickwire_control_sys::Error::Closed)).is_err());
}
