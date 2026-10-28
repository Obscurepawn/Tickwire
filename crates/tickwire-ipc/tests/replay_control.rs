use tickwire_ipc::*;
fn frame(body: &[u8], op: u16) -> Vec<u8> {
    let mut raw = vec![0; 128 + body.len()];
    raw[128..].copy_from_slice(body);
    let mut h = ControlEnvelope {
        magic: *b"TWCTL001",
        abi_major: 1,
        header_bytes: 128,
        frame_bytes: raw.len() as u32,
        opcode: op,
        flags: if op == 41 { 1 } else { 10 },
        body_crc32c: crc32c(body),
        ..Default::default()
    };
    h.encode(&mut raw[..128]).unwrap();
    h.header_crc32c = crc32c(&raw[..128]);
    h.encode(&mut raw[..128]).unwrap();
    raw
}
#[test]
fn replay_control_literal_shape_and_required_binding() {
    let mut b = [0u8; 256];
    b[..16].fill(1);
    b[16..24].copy_from_slice(&2u64.to_le_bytes());
    b[24..32].copy_from_slice(&3u64.to_le_bytes());
    b[32..48].fill(4);
    b[48..56].copy_from_slice(&5u64.to_le_bytes());
    b[56..88].fill(6);
    b[88..96].copy_from_slice(&7u64.to_le_bytes());
    b[104..136].fill(8);
    b[136..152].fill(9);
    b[152..160].copy_from_slice(&10u64.to_le_bytes());
    b[160..162].copy_from_slice(&1u16.to_le_bytes());
    b[162..164].copy_from_slice(&2u16.to_le_bytes());
    for op in [41, 42] {
        ControlFrame::decode(&frame(&b, op)).unwrap();
        for (start, end) in [
            (0, 16),
            (16, 24),
            (24, 32),
            (32, 48),
            (48, 56),
            (56, 88),
            (88, 96),
            (104, 136),
            (136, 152),
            (152, 160),
            (160, 162),
        ] {
            let mut bad = b;
            bad[start..end].fill(0);
            assert!(
                ControlFrame::decode(&frame(&bad, op)).is_err(),
                "{op} {start}"
            );
        }
        let mut bad = b;
        bad[164] = 1;
        assert!(ControlFrame::decode(&frame(&bad, op)).is_err());
    }
    let decoded = ReplayControlBody::decode(&b).unwrap();
    assert_eq!(decoded.replay_kind, 2);
    assert_eq!(decoded.expected_sequence, 0);
}

#[test]
fn market_transition_kinds_use_existing_authenticated_control_layout() {
    let body = ReplayControlBody {
        instance_id: [1; 16],
        strategy_generation: 2,
        logical_strategy_id: 3,
        challenge: [4; 16],
        valid_until_mono_ns: 5,
        object_digest: [6; 32],
        object_bytes: 7,
        expected_cut_digest: [8; 32],
        operation_id: [9; 16],
        operation_revision: 10,
        schema: 1,
        ..Default::default()
    };
    for kind in [6, 7] {
        let mut body = body.clone();
        body.replay_kind = kind;
        let mut bytes = [0; 256];
        body.encode(&mut bytes).unwrap();
        for opcode in [41, 42] {
            ControlFrame::decode(&frame(&bytes, opcode)).unwrap();
        }
    }
    let mut bad = body;
    bad.replay_kind = 8;
    let mut bytes = [0; 256];
    bad.encode(&mut bytes).unwrap();
    assert!(ControlFrame::decode(&frame(&bytes, 41)).is_err());
}

#[test]
fn market_arm_rejection_is_explicit_and_bounded() {
    let body = ReplayControlBody {
        instance_id: [1; 16],
        strategy_generation: 2,
        logical_strategy_id: 3,
        challenge: [4; 16],
        valid_until_mono_ns: 5,
        object_digest: [6; 32],
        object_bytes: 7,
        expected_cut_digest: [8; 32],
        operation_id: [9; 16],
        operation_revision: 10,
        schema: 1,
        replay_kind: 6,
        ..Default::default()
    };
    let mut bytes = [0; 256];
    body.encode(&mut bytes).unwrap();
    for status in 1..=4 {
        let mut raw = frame(&bytes, 42);
        let mut h = ControlEnvelope::decode(&raw[..128]).unwrap();
        h.flags = 18;
        h.status = status;
        h.header_crc32c = 0;
        h.encode(&mut raw[..128]).unwrap();
        h.header_crc32c = crc32c(&raw[..128]);
        h.encode(&mut raw[..128]).unwrap();
        assert_eq!(ControlFrame::decode(&raw).is_ok(), status <= 3);
    }
}
