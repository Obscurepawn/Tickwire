use tickwire_control::*;
use tickwire_ipc::{ControlEnvelope, ControlFrame, HelloBody, QueryBody, Wire};

fn identity(id: u8, role: u16) -> Identity {
    Identity {
        instance: [id; 16],
        generation: 1,
        role,
        logical_strategy: if role == 3 { 7 } else { 0 },
        host_boot: [8; 16],
        namespace: [9; 16],
        build_hash: [10; 32],
        qualification_hash: [11; 32],
        manifest_hash: [12; 32],
        arch: 2,
        trust_profile: 1,
    }
}
fn policy(server: bool) -> Policy {
    Policy {
        local: identity(if server { 1 } else { 2 }, if server { 1 } else { 3 }),
        peer: identity(if server { 2 } else { 1 }, if server { 3 } else { 1 }),
        peer_process: ProcessIdentity {
            pid: if server { 200 } else { 100 },
            uid: 501,
            gid: 20,
        },
        required_features: 0x1c,
        supported_features: 0x1f,
        control_epoch: 13,
        lease_duration_ns: 3000,
        max_frame_bytes: 65536,
        nonce: [14; 32],
    }
}
fn connected() -> (Session, Session) {
    let p = policy(true);
    let client = ClientHandshake::new(policy(false), [15; 16]).unwrap();
    let (server, response) = Session::accept(&p, p.peer_process, client.hello(), 100).unwrap();
    let client = client
        .finish(policy(false).peer_process, &response, 101)
        .unwrap();
    (server, client)
}
fn request(client: &Session, id: u8, version: u64) -> Vec<u8> {
    let b = QueryBody::default();
    let mut body = vec![0; QueryBody::SIZE];
    b.encode(&mut body).unwrap();
    client
        .encode(
            ControlEnvelope {
                opcode: 48,
                flags: 1,
                request_id: [id; 16],
                expected_state_version: version,
                ..Default::default()
            },
            &body,
        )
        .unwrap()
}

#[test]
fn handshake_and_frame_bind_both_processes_and_instances() {
    let (mut server, client) = connected();
    let bytes = request(&client, 16, 0);
    let frame = server.validate(&bytes, 102).unwrap();
    assert_eq!(frame.envelope.sender_instance, [2; 16]);
    assert_eq!(server.features(), 0x1f);
    assert_eq!(server.deadline(), 3100);
    assert_eq!(client.deadline(), 3100);
}
#[test]
fn kernel_identity_cannot_be_replaced_by_hello_claims() {
    let client = ClientHandshake::new(policy(false), [15; 16]).unwrap();
    let p = policy(true);
    for actual in [
        ProcessIdentity {
            pid: 201,
            ..p.peer_process
        },
        ProcessIdentity {
            uid: 502,
            ..p.peer_process
        },
        ProcessIdentity {
            gid: 21,
            ..p.peer_process
        },
    ] {
        assert!(matches!(
            Session::accept(&p, actual, client.hello(), 100),
            Err(Error::Identity)
        ));
    }
}
#[test]
fn hello_must_match_registered_generation_and_artifact_hashes() {
    let client = ClientHandshake::new(policy(false), [15; 16]).unwrap();
    let p = policy(true);
    for which in 0..7 {
        let frame = ControlFrame::decode(client.hello()).unwrap();
        let mut hello = HelloBody::decode(frame.body).unwrap();
        match which {
            0 => hello.instance_generation += 1,
            1 => hello.namespace_id[0] ^= 1,
            2 => hello.host_boot_id[0] ^= 1,
            3 => hello.build_hash[0] ^= 1,
            4 => hello.sdk_qualification_hash[0] ^= 1,
            5 => hello.manifest_hash[0] ^= 1,
            _ => hello.role = 2,
        }
        let mut body = vec![0; HelloBody::SIZE];
        hello.encode(&mut body).unwrap();
        let bytes = encode_frame(frame.envelope, &body).unwrap();
        assert!(Session::accept(&p, p.peer_process, &bytes, 100).is_err());
    }
}
#[test]
fn mandatory_features_and_architecture_must_be_compatible() {
    for which in 0..3 {
        let mut p = policy(false);
        match which {
            0 => {
                p.required_features |= 32;
                p.supported_features |= 32;
            }
            1 => p.local.arch = 1,
            _ => p.local.trust_profile = 2,
        }
        let client = ClientHandshake::new(p, [15; 16]).unwrap();
        let server = policy(true);
        assert!(Session::accept(&server, server.peer_process, client.hello(), 100).is_err());
    }
}
#[test]
fn welcome_cannot_redirect_the_session_or_claim_extra_capability() {
    for which in 0..3 {
        let p = policy(true);
        let client = ClientHandshake::new(policy(false), [15; 16]).unwrap();
        let (_, response) = Session::accept(&p, p.peer_process, client.hello(), 100).unwrap();
        let frame = ControlFrame::decode(&response).unwrap();
        let mut e = frame.envelope;
        let mut w = tickwire_ipc::WelcomeBody::decode(frame.body).unwrap();
        match which {
            0 => e.request_id = [99; 16],
            1 => w.server_instance = [99; 16],
            _ => w.features |= 32,
        }
        let mut body = vec![0; tickwire_ipc::WelcomeBody::SIZE];
        w.encode(&mut body).unwrap();
        let modified = encode_frame(e, &body).unwrap();
        assert!(
            client
                .finish(policy(false).peer_process, &modified, 101)
                .is_err()
        );
    }
}
#[test]
fn expired_or_regressed_clock_never_reopens_a_session() {
    let (mut server, client) = connected();
    let bytes = request(&client, 16, 0);
    assert!(server.validate(&bytes, 3099).is_ok());
    assert!(matches!(server.validate(&bytes, 3100), Err(Error::Expired)));
    assert!(server.validate(&bytes, 100).is_err());
    assert!(server.renew([14; 32], 2, 100).is_err());
    let (mut server, client) = connected();
    assert!(server.validate(&request(&client, 17, 0), 99).is_err());
    assert!(server.validate(&request(&client, 17, 0), 101).is_err());
}
#[test]
fn renew_is_nonce_bound_and_strictly_increasing() {
    let (mut server, _) = connected();
    assert!(server.renew([0; 32], 2, 200).is_err());
    assert_eq!(server.deadline(), 3100);
    assert_eq!(server.renew([14; 32], 2, 201).unwrap(), 3201);
    assert!(server.renew([14; 32], 2, 202).is_err());
    assert_eq!(server.deadline(), 3201);
}
#[test]
fn frame_identity_epoch_and_control_kind_are_checked() {
    let (mut server, client) = connected();
    let bytes = request(&client, 16, 0);
    for which in 0..4 {
        let f = ControlFrame::decode(&bytes).unwrap();
        let mut e = f.envelope;
        match which {
            0 => e.sender_instance = [7; 16],
            1 => e.target_instance = [7; 16],
            2 => e.control_epoch -= 1,
            _ => e.request_id = [0; 16],
        }
        assert!(
            server
                .validate(&encode_frame(e, f.body).unwrap(), 102)
                .is_err()
        );
    }
}

fn applied(server: &Session, request: &[u8], version: u64) -> Vec<u8> {
    let f = ControlFrame::decode(request).unwrap();
    let body = tickwire_ipc::StatusBody {
        state_version: version,
        ..Default::default()
    };
    let mut bytes = vec![0; tickwire_ipc::StatusBody::SIZE];
    body.encode(&mut bytes).unwrap();
    server
        .encode(
            ControlEnvelope {
                opcode: 49,
                flags: 10,
                ..f.envelope
            },
            &bytes,
        )
        .unwrap()
}
#[test]
fn requests_are_registered_before_one_ticket_and_retried_before_version_check() {
    let (mut server, client) = connected();
    let mut store = MockRequestStore::new(2).unwrap();
    let bytes = request(&client, 16, 4);
    let Admission::Execute(ticket) = server.admit(&mut store, &bytes, 102, 4).unwrap() else {
        panic!()
    };
    assert!(matches!(
        server.admit(&mut store, &bytes, 103, 9).unwrap(),
        Admission::Pending
    ));
    let response = applied(&server, &bytes, 5);
    ticket.complete(&mut store, &response, 5).unwrap();
    let Admission::Completed(stored) = server.admit(&mut store, &bytes, 104, 9).unwrap() else {
        panic!()
    };
    assert_eq!(stored.response, response);
    assert_eq!(stored.state_version, 5);
    assert!(matches!(
        server.admit(&mut store, &request(&client, 17, 4), 105, 9),
        Err(Error::Version)
    ));
    assert!(matches!(
        server.admit(&mut store, &request(&client, 16, 5), 106, 5),
        Err(Error::Conflict)
    ));
}
#[test]
fn capacity_preserves_old_results_and_storage_failure_never_returns_a_ticket() {
    let (mut server, client) = connected();
    let mut store = MockRequestStore::new(1).unwrap();
    let bytes = request(&client, 16, 0);
    store.fail_next();
    assert!(matches!(
        server.admit(&mut store, &bytes, 102, 0),
        Err(Error::Store)
    ));
    let Admission::Execute(ticket) = server.admit(&mut store, &bytes, 103, 0).unwrap() else {
        panic!()
    };
    store.fail_next();
    assert!(
        ticket
            .complete(&mut store, &applied(&server, &bytes, 1), 1)
            .is_err()
    );
    assert!(matches!(
        server.admit(&mut store, &bytes, 104, 0).unwrap(),
        Admission::Pending
    ));
    ticket
        .complete(&mut store, &applied(&server, &bytes, 1), 1)
        .unwrap();
    assert!(matches!(
        server.admit(&mut store, &request(&client, 17, 1), 105, 1),
        Err(Error::Capacity)
    ));
    assert!(matches!(
        server.admit(&mut store, &bytes, 106, 2).unwrap(),
        Admission::Completed(_)
    ));
}
#[test]
fn completion_is_correlated_immutable_and_survives_session_recreation_in_shared_store() {
    let (mut server, client) = connected();
    let mut store = MockRequestStore::new(1).unwrap();
    let bytes = request(&client, 16, 0);
    let Admission::Execute(ticket) = server.admit(&mut store, &bytes, 102, 0).unwrap() else {
        panic!()
    };
    let good = applied(&server, &bytes, 1);
    let wrong = applied(&server, &request(&client, 17, 0), 1);
    assert!(ticket.complete(&mut store, &wrong, 1).is_err());
    ticket.complete(&mut store, &good, 1).unwrap();
    ticket.complete(&mut store, &good, 1).unwrap();
    assert!(matches!(
        ticket.complete(&mut store, &applied(&server, &bytes, 2), 2),
        Err(Error::Conflict)
    ));
    let (mut recreated, _) = connected(); // Models restoration of the same session, not new epoch allocation.
    assert!(matches!(
        recreated.admit(&mut store, &bytes, 103, 99).unwrap(),
        Admission::Completed(_)
    ));
}

#[test]
fn terminal_response_must_match_request_kind_and_reported_state_version() {
    let (mut server, client) = connected();
    let mut store = MockRequestStore::new(1).unwrap();
    let bytes = request(&client, 16, 0);
    let Admission::Execute(ticket) = server.admit(&mut store, &bytes, 102, 0).unwrap() else {
        panic!()
    };
    let good = applied(&server, &bytes, 1);
    assert!(ticket.complete(&mut store, &good, 2).is_err());
    let f = ControlFrame::decode(&good).unwrap();
    let welcome = tickwire_ipc::WelcomeBody::default();
    let mut body = vec![0; tickwire_ipc::WelcomeBody::SIZE];
    welcome.encode(&mut body).unwrap();
    let wrong = encode_frame(
        ControlEnvelope {
            opcode: 2,
            ..f.envelope
        },
        &body,
    )
    .unwrap();
    assert!(ticket.complete(&mut store, &wrong, 1).is_err());
    ticket.complete(&mut store, &good, 1).unwrap();
}

#[test]
fn lease_arithmetic_exhaustion_is_distinct_from_actual_session_expiry() {
    let limit = 1u64 << 63;
    let mut p = policy(true);
    p.lease_duration_ns = 10;
    let mut cp = policy(false);
    cp.lease_duration_ns = 10;
    let hello = ClientHandshake::new(cp, [15; 16]).unwrap();
    let (mut server, welcome) =
        Session::accept(&p, p.peer_process, hello.hello(), limit - 15).unwrap();
    let client = hello
        .finish(policy(false).peer_process, &welcome, limit - 14)
        .unwrap();
    assert!(matches!(
        server.renew([14; 32], 2, limit - 9),
        Err(Error::Bounds)
    ));
    assert_eq!(server.deadline(), limit - 5);
    assert!(server.validate(&request(&client, 16, 0), limit - 8).is_ok());
    assert!(matches!(
        server.validate(&request(&client, 16, 0), limit - 5),
        Err(Error::Expired)
    ));
}
