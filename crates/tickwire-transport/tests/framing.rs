use tickwire_transport::{fix, http, ws};
#[test]
fn http_request_injection_and_explicit_lengths() {
    let r = http::encode_request("POST", "/order", &[("Host", "localhost")], b"{}", 1000).unwrap();
    assert_eq!(
        r,
        b"POST /order HTTP/1.1\r\nHost: localhost\r\nContent-Length: 2\r\n\r\n{}"
    );
    assert!(http::encode_request("POST", "/", &[("X", "ok\r\nEvil: yes")], b"", 1000).is_err());
    assert!(http::encode_request("GET", "/", &[("Content-Length", "99")], b"", 1000).is_err());
    assert!(http::encode_request("GET", "/", &[], b"large", 4).is_err());
}
#[test]
fn http_fragmented_chunked_trailers_and_pipeline() {
    let wire=b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n2\r\nde\r\n0\r\nX-Trailer: ok\r\n\r\nHTTP/1.1 204 No Content\r\n\r\n";
    let mut d = http::ResponseDecoder::new(1024, 16).unwrap();
    for &b in &wire[..wire.len() - 1] {
        d.push(&[b]).unwrap();
    }
    let r = d.next_response().unwrap().unwrap();
    assert_eq!(r.body, b"abcde");
    assert!(d.next_response().unwrap().is_none());
    d.push(&wire[wire.len() - 1..]).unwrap();
    assert_eq!(d.next_response().unwrap().unwrap().status, 204);
}
#[test]
fn http_ambiguous_length_and_truncated_eof_rejected() {
    for w in [
        b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\nContent-Length: 1\r\n\r\na".as_slice(),
        b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n",
        b"HTTP/1.1 200 OK\r\nContent-Length: 9999999\r\n\r\n",
    ] {
        let mut d = http::ResponseDecoder::new(1024, 16).unwrap();
        d.push(w).unwrap();
        assert!(d.next_response().is_err());
    }
    let mut d = http::ResponseDecoder::new(1024, 16).unwrap();
    d.push(b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nx")
        .unwrap();
    d.finish();
    assert!(d.next_response().is_err());
}
#[test]
fn ws_mask_roundtrip_and_random_masks() {
    let a = ws::encode_client(ws::Opcode::Text, b"hello", 100).unwrap();
    assert_eq!(a[0], 0x81);
    assert_eq!(a[1], 0x85);
    let text: Vec<u8> = a[6..]
        .iter()
        .enumerate()
        .map(|(i, v)| v ^ a[2 + i % 4])
        .collect();
    assert_eq!(text, b"hello");
}
#[test]
fn ws_fragmentation_with_interleaved_ping() {
    let mut d = ws::Decoder::new(100, 100).unwrap();
    d.push(&[
        0x01, 2, b'h', b'e', 0x89, 1, b'?', 0x80, 3, b'l', b'l', b'o',
    ])
    .unwrap();
    assert_eq!(
        d.next_message().unwrap().unwrap(),
        ws::Message {
            opcode: ws::Opcode::Ping,
            payload: b"?".to_vec()
        }
    );
    assert_eq!(
        d.next_message().unwrap().unwrap(),
        ws::Message {
            opcode: ws::Opcode::Text,
            payload: b"hello".to_vec()
        }
    );
}
#[test]
fn ws_protocol_errors_and_limits() {
    for bytes in [
        &[0x81, 0x80][..],
        &[0x89, 126, 0, 126],
        &[0x80, 0],
        &[0x81, 126, 0, 1, b'x'],
        &[0x81, 1, 0xff],
        &[0x88, 1, 0],
        &[0x88, 2, 3, 0xed],
        &[0xc1, 0],
    ] {
        let mut d = ws::Decoder::new(200, 200).unwrap();
        d.push(bytes).unwrap();
        assert!(d.next_message().is_err(), "{bytes:?}");
    }
    let mut d = ws::Decoder::new(3, 3).unwrap();
    assert!(d.push(&[0; 32]).is_err());
}
#[test]
fn ws_upgrade_validates_server_accept() {
    use base64::Engine;
    use sha1::Digest;
    let h = ws::ClientHandshake::new("localhost", "/ws", &[], 4096).unwrap();
    let request = std::str::from_utf8(h.request()).unwrap();
    let key = request
        .lines()
        .find_map(|s| s.strip_prefix("Sec-WebSocket-Key: "))
        .unwrap();
    let mut hash = sha1::Sha1::new();
    hash.update(key);
    hash.update("258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
    let accept = base64::engine::general_purpose::STANDARD.encode(hash.finalize());
    let r = http::Response {
        status: 101,
        headers: vec![
            ("upgrade".into(), b"websocket".to_vec()),
            ("connection".into(), b"keep-alive, Upgrade".to_vec()),
            ("sec-websocket-accept".into(), accept.into_bytes()),
        ],
        body: vec![],
    };
    h.validate(&r).unwrap();
    let mut bad = r;
    bad.headers[2].1 = b"wrong".to_vec();
    assert!(h.validate(&bad).is_err());
}
#[test]
fn fix_fragmented_body_length_checksum() {
    let bytes = fix::encode("FIX.4.4", b"35=0\x0149=sender\x01", 1024).unwrap();
    let mut d = fix::Decoder::new(1024).unwrap();
    for &b in &bytes[..bytes.len() - 1] {
        d.push(&[b]).unwrap();
        assert!(d.next_frame().unwrap().is_none());
    }
    d.push(&bytes[bytes.len() - 1..]).unwrap();
    assert_eq!(d.next_frame().unwrap().unwrap(), bytes);
    let mut bad = bytes;
    bad[16] ^= 1;
    d.push(&bad).unwrap();
    assert!(d.next_frame().is_err());
}
#[test]
fn chunk_size_over_limit_rejected_before_body_arrives() {
    let mut d = http::ResponseDecoder::new(1024, 16).unwrap();
    d.push(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n10000000\r\n")
        .unwrap();
    assert!(d.next_response().is_err());
}
#[test]
fn head_length_is_metadata_not_body_allocation() {
    let mut d = http::ResponseDecoder::new(1024, 16).unwrap();
    d.push(b"HTTP/1.1 200 OK\r\nContent-Length: 999999999\r\n\r\n")
        .unwrap();
    assert!(d.next_head_response().unwrap().unwrap().body.is_empty());
}
#[test]
fn incomplete_frame_eof_and_abnormal_websocket_close_are_errors() {
    let mut d = ws::Decoder::new(100, 100).unwrap();
    d.push(&[0x01, 1, b'x']).unwrap();
    assert!(d.next_message().unwrap().is_none());
    assert!(d.finish().is_err());
    let mut d = ws::Decoder::new(100, 100).unwrap();
    d.push(&[0x88, 0]).unwrap();
    assert_eq!(d.next_message().unwrap().unwrap().opcode, ws::Opcode::Close);
    d.finish().unwrap();
    let mut d = fix::Decoder::new(100).unwrap();
    d.push(b"8=FIX.4.4\x019=9\x0135=").unwrap();
    assert!(d.finish().is_err());
}
#[test]
fn websocket_budget_yield_is_distinct_from_incomplete_network_input() {
    let mut bytes = vec![0x01, 1, b'a'];
    for _ in 0..31 {
        bytes.extend_from_slice(&[0x00, 0]);
    }
    bytes.extend_from_slice(&[0x80, 1, b'b']);
    let mut d = ws::Decoder::new(128, 128).unwrap();
    d.push(&bytes).unwrap();
    assert_eq!(d.poll_message().unwrap(), ws::PollMessage::Yielded);
    assert_eq!(
        d.poll_message().unwrap(),
        ws::PollMessage::Message(ws::Message {
            opcode: ws::Opcode::Text,
            payload: b"ab".to_vec()
        })
    );
    assert_eq!(d.poll_message().unwrap(), ws::PollMessage::NeedInput);
    d.push(&[0x81, 4, b'x']).unwrap();
    assert!(d.buffered_len() > 0);
    assert_eq!(d.poll_message().unwrap(), ws::PollMessage::NeedInput);
}
