use tickwire_control_peer::{Error, frame::*};
fn literal() -> Vec<u8> {
    include_str!("fixtures/heartbeat.hex")
        .trim()
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|p| u8::from_str_radix(std::str::from_utf8(p).unwrap(), 16).unwrap())
        .collect()
}
#[test]
fn fixed_header_literal_and_protobuf_identity_are_independently_encoded() {
    let bytes = literal();
    let frame = Frame::decode(&bytes).unwrap();
    assert_eq!(frame.header.kind, Kind::Raft);
    assert_eq!(frame.header.cluster, [1; 16]);
    assert_eq!(frame.header.group, [2; 16]);
    assert_eq!(
        (frame.header.from, frame.header.to, frame.header.sequence),
        (1, 2, 1)
    );
    assert_eq!(frame.encode().unwrap(), bytes);
    let message = frame.raft().unwrap();
    assert_eq!(message.from, 1);
    assert_eq!(message.to, 2);
    assert_eq!(message.term, 1);
    assert_eq!(
        message.get_msg_type(),
        tickwire_control_raft::MessageType::MsgHeartbeat
    );
}
#[test]
fn every_partial_fragment_waits_and_invalid_reserved_identity_or_length_fails() {
    let bytes = literal();
    let mut decoder = Decoder::new(1024).unwrap();
    for (i, b) in bytes.iter().enumerate() {
        decoder.push(&[*b]).unwrap();
        if i + 1 < bytes.len() {
            assert!(decoder.next_frame().unwrap().is_none());
        }
    }
    assert_eq!(
        decoder.next_frame().unwrap().unwrap().encode().unwrap(),
        bytes
    );
    assert_eq!(decoder.buffered_len(), 0);
    for offset in [0, 8, 10, 18, 23, 112, 127] {
        let mut bad = bytes.clone();
        bad[offset] ^= 1;
        assert!(Frame::decode(&bad).is_err(), "offset{offset}");
    }
    let mut bad = bytes.clone();
    bad[12..16].copy_from_slice(&((MAX_PAYLOAD_BYTES + 1) as u32).to_le_bytes());
    let mut decoder = Decoder::new(1024).unwrap();
    decoder.push(&bad[..HEADER_BYTES]).unwrap();
    assert!(matches!(decoder.next_frame(), Err(Error::Bounds)));
    let mut bad = Frame::decode(&bytes).unwrap();
    bad.header.from = 3;
    assert!(matches!(bad.raft(), Err(Error::Identity)));
}
#[test]
fn protobuf_repeated_fields_unknown_tags_and_local_commands_fail_before_dispatch() {
    let original = Frame::decode(&literal()).unwrap();
    for payload in [
        vec![8, 0],
        vec![8, 8, 8, 8],
        vec![0xff, 0xff, 0xff, 0xff, 0x7f],
        vec![0x3a, 0],
        vec![0x4a, 0],
    ] {
        let mut frame = original.clone();
        frame.payload = payload;
        assert!(frame.raft().is_err());
    }
    for length in 0..literal().len() {
        assert!(Frame::decode(&literal()[..length]).is_err());
    }
}
