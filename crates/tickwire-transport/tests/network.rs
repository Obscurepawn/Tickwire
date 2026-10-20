use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::{Arc, mpsc},
    thread,
    time::{Duration, Instant},
};
use tickwire_transport::*;
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(5)
}
fn plain(limits: Limits) -> (Connection, TcpStream) {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let c =
        Connection::connect(l.local_addr().unwrap(), Security::Plain, limits, deadline()).unwrap();
    l.set_nonblocking(true).unwrap();
    let end = deadline();
    loop {
        if let Ok((s, _)) = l.accept() {
            s.set_nonblocking(false).unwrap();
            s.set_read_timeout(Some(Duration::from_millis(50))).unwrap();
            return (c, s);
        }
        assert!(Instant::now() < end);
        thread::yield_now();
    }
}
fn drive_ready(c: &mut Connection) {
    let mut p = mio::Poll::new().unwrap();
    c.register(p.registry(), mio::Token(0)).unwrap();
    let mut events = mio::Events::with_capacity(8);
    let end = deadline();
    while !c.is_ready() {
        assert!(Instant::now() < end, "handshake timed out");
        p.poll(&mut events, Some(Duration::from_millis(10)))
            .unwrap();
        c.drive(Instant::now(), IoBudget::default()).unwrap();
    }
}
#[test]
fn queued_drive_never_outputs_and_denial_is_terminal() {
    let (mut c, mut s) = plain(Limits::default());
    drive_ready(&mut c);
    c.queue(PreparedSend::new(b"order", deadline(), 100).unwrap())
        .unwrap();
    for _ in 0..3 {
        c.drive(Instant::now(), IoBudget::default()).unwrap();
    }
    let mut b = [0; 8];
    assert!(s.read(&mut b).is_err());
    assert_eq!(c.send_report().unwrap().state, SendState::Queued);
    let r = c.try_send(Instant::now(), || false).unwrap();
    assert_eq!(r.state, SendState::NoBytes(Failure::GateDenied));
    for _ in 0..3 {
        c.drive(Instant::now(), IoBudget::default()).unwrap();
    }
    assert!(s.read(&mut b).is_err());
    assert_eq!(c.take_finished().unwrap(), r);
}
#[test]
fn queued_deadline_never_resurrects() {
    let (mut c, mut s) = plain(Limits::default());
    drive_ready(&mut c);
    let d = Instant::now();
    c.queue(PreparedSend::new(b"order", d, 100).unwrap())
        .unwrap();
    c.drive(d, IoBudget::default()).unwrap();
    assert_eq!(
        c.send_report().unwrap().state,
        SendState::NoBytes(Failure::Deadline)
    );
    let mut b = [0; 8];
    assert!(s.read(&mut b).is_err());
    assert_eq!(
        c.try_send(d, || panic!("expired gate must not run"))
            .unwrap()
            .accepted_bytes,
        0
    );
}
#[test]
fn partial_tcp_write_is_accepted_and_cancel_cannot_be_no_bytes() {
    let (mut c, mut s) = plain(Limits {
        max_accept_bytes: 2,
        ..Limits::default()
    });
    drive_ready(&mut c);
    c.queue(PreparedSend::new(b"abcdef", deadline(), 100).unwrap())
        .unwrap();
    let r = c.try_send(Instant::now(), || true).unwrap();
    assert_eq!((r.accepted_bytes, r.state), (2, SendState::MayHaveSent));
    let mut b = [0; 2];
    s.read_exact(&mut b).unwrap();
    assert_eq!(&b, b"ab");
    assert_eq!(
        c.cancel().unwrap().state,
        SendState::Failed(Failure::Cancelled)
    );
    assert_eq!(s.read(&mut b).unwrap(), 0);
}
#[test]
fn bounded_receive_and_budget() {
    let (mut c, mut s) = plain(Limits {
        max_receive_bytes: 3,
        ..Limits::default()
    });
    drive_ready(&mut c);
    s.write_all(b"abcdef").unwrap();
    let r = c
        .drive(
            Instant::now(),
            IoBudget {
                max_syscalls: 1,
                max_bytes: 2,
            },
        )
        .unwrap();
    assert!(r.syscalls <= 1);
    assert!(r.network_read + r.network_written <= 2);
    let mut got = Vec::new();
    let until = deadline();
    while Instant::now() < until {
        c.drive(Instant::now(), IoBudget::default()).unwrap();
        let mut b = [0; 16];
        let n = c.read(&mut b);
        assert!(n <= 3);
        got.extend_from_slice(&b[..n]);
        if got.len() == 6 {
            break;
        }
    }
    assert_eq!(got, b"abcdef");
}
fn tls_pair() -> (Arc<rustls::ClientConfig>, Arc<rustls::ServerConfig>) {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let key = rustls::pki_types::PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
    let server = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert.cert.der().clone()], key.into())
        .unwrap();
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let client = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    (Arc::new(client), Arc::new(server))
}
#[test]
fn tls_acceptance_precedes_ciphertext_flush_and_failure_keeps_evidence() {
    let (client, server) = tls_pair();
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap();
    let (tx, rx) = mpsc::channel();
    let h = thread::spawn(move || {
        let (s, _) = l.accept().unwrap();
        s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        s.set_write_timeout(Some(Duration::from_secs(5))).unwrap();
        let mut tls = rustls::StreamOwned::new(rustls::ServerConnection::new(server).unwrap(), s);
        while tls.conn.is_handshaking() {
            tls.conn.complete_io(&mut tls.sock).unwrap();
        }
        tx.send(()).unwrap();
        let mut buf = [0; 1];
        assert!(tls.read(&mut buf).unwrap_or(0) == 0);
    });
    let mut c = Connection::connect(
        addr,
        Security::Tls {
            server_name: "localhost".try_into().unwrap(),
            config: client,
        },
        Limits {
            tls_buffer_bytes: 2,
            max_accept_bytes: 2,
            ..Limits::default()
        },
        deadline(),
    )
    .unwrap();
    drive_ready(&mut c);
    rx.recv_timeout(Duration::from_secs(5)).unwrap();
    c.queue(PreparedSend::new(b"order", deadline(), 100).unwrap())
        .unwrap();
    let r = c.try_send(Instant::now(), || true).unwrap();
    assert_eq!(r.state, SendState::MayHaveSent);
    assert_eq!(r.accepted_bytes, 2);
    assert_eq!(
        c.cancel().unwrap().state,
        SendState::Failed(Failure::Cancelled)
    );
    h.join().unwrap();
}
#[test]
fn tls_bad_hostname_fails_before_application_acceptance() {
    let (client, server) = tls_pair();
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap();
    let h = thread::spawn(move || {
        let (mut s, _) = l.accept().unwrap();
        s.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
        let mut tls = rustls::ServerConnection::new(server).unwrap();
        let _ = tls.complete_io(&mut s);
    });
    let mut c = Connection::connect(
        addr,
        Security::Tls {
            server_name: "wrong.invalid".try_into().unwrap(),
            config: client,
        },
        Limits::default(),
        deadline(),
    )
    .unwrap();
    c.queue(PreparedSend::new(b"order", deadline(), 100).unwrap())
        .unwrap();
    let end = deadline();
    while c.drive(Instant::now(), IoBudget::default()).is_ok() {
        assert!(Instant::now() < end);
        thread::yield_now();
    }
    assert_eq!(
        c.send_report().unwrap().state,
        SendState::NoBytes(Failure::Tls)
    );
    h.join().unwrap();
}
fn send_all(c: &mut Connection, bytes: &[u8]) {
    c.queue(PreparedSend::new(bytes, deadline(), 1 << 20).unwrap())
        .unwrap();
    let end = deadline();
    loop {
        assert!(Instant::now() < end);
        c.try_send(Instant::now(), || true).unwrap();
        c.drive(Instant::now(), IoBudget::default()).unwrap();
        if c.send_report().unwrap().state == SendState::Flushed {
            c.take_finished().unwrap();
            break;
        }
        thread::yield_now();
    }
}
fn receive_http(c: &mut Connection) -> http::Response {
    let mut d = http::ResponseDecoder::new(4096, 32).unwrap();
    let end = deadline();
    loop {
        assert!(Instant::now() < end);
        let r = c.drive(Instant::now(), IoBudget::default());
        let mut b = [0; 4096];
        let n = c.read(&mut b);
        d.push(&b[..n]).unwrap();
        if let Some(r) = d.next_response().unwrap() {
            return r;
        }
        r.unwrap();
        thread::yield_now();
    }
}
fn tls_client(addr: std::net::SocketAddr, config: Arc<rustls::ClientConfig>) -> Connection {
    let mut c = Connection::connect(
        addr,
        Security::Tls {
            server_name: "localhost".try_into().unwrap(),
            config,
        },
        Limits {
            max_accept_bytes: 3,
            ..Limits::default()
        },
        deadline(),
    )
    .unwrap();
    drive_ready(&mut c);
    c
}
#[test]
fn https_request_and_chunked_response_use_real_tls_socket() {
    let (client, server) = tls_pair();
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap();
    let h = thread::spawn(move || {
        let (s, _) = l.accept().unwrap();
        s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let mut tls = rustls::StreamOwned::new(rustls::ServerConnection::new(server).unwrap(), s);
        let mut request = Vec::new();
        loop {
            let mut b = [0; 1];
            tls.read_exact(&mut b).unwrap();
            request.push(b[0]);
            if request.ends_with(b"\r\n\r\n{}") {
                break;
            }
        }
        assert!(request.starts_with(b"POST /order HTTP/1.1\r\n"));
        for part in [
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n".as_slice(),
            b"2\r\n{}\r\n",
            b"0\r\n\r\n",
        ] {
            tls.write_all(part).unwrap();
            tls.flush().unwrap();
        }
        tls.conn.send_close_notify();
        tls.flush().unwrap();
    });
    let mut c = tls_client(addr, client);
    let request =
        http::encode_request("POST", "/order", &[("Host", "localhost")], b"{}", 4096).unwrap();
    send_all(&mut c, &request);
    assert_eq!(receive_http(&mut c).body, b"{}");
    h.join().unwrap();
}
#[test]
fn websocket_upgrade_masked_output_fragmented_input_over_tls() {
    use base64::Engine;
    use sha1::Digest;
    let (client, server) = tls_pair();
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap();
    let h = thread::spawn(move || {
        let (s, _) = l.accept().unwrap();
        s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let mut tls = rustls::StreamOwned::new(rustls::ServerConnection::new(server).unwrap(), s);
        let mut request = Vec::new();
        loop {
            let mut b = [0; 1];
            tls.read_exact(&mut b).unwrap();
            request.push(b[0]);
            if request.ends_with(b"\r\n\r\n") {
                break;
            }
        }
        let request = String::from_utf8(request).unwrap();
        let key = request
            .lines()
            .find_map(|s| s.strip_prefix("Sec-WebSocket-Key: "))
            .unwrap();
        let mut hash = sha1::Sha1::new();
        hash.update(key);
        hash.update("258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
        let accept = base64::engine::general_purpose::STANDARD.encode(hash.finalize());
        tls.write_all(format!("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n").as_bytes()).unwrap();
        tls.flush().unwrap();
        let mut b = [0; 8];
        tls.read_exact(&mut b).unwrap();
        assert_eq!(&b[..2], &[0x81, 0x82]);
        assert_eq!([b[6] ^ b[2], b[7] ^ b[3]], *b"{}");
        tls.write_all(&[0x01, 1, b'{', 0x89, 1, b'?', 0x80, 1, b'}'])
            .unwrap();
        tls.flush().unwrap();
        tls.conn.send_close_notify();
        tls.flush().unwrap();
    });
    let mut c = tls_client(addr, client);
    let handshake = ws::ClientHandshake::new("localhost", "/ws", &[], 4096).unwrap();
    send_all(&mut c, handshake.request());
    handshake.validate(&receive_http(&mut c)).unwrap();
    send_all(
        &mut c,
        &ws::encode_client(ws::Opcode::Text, b"{}", 128).unwrap(),
    );
    let mut d = ws::Decoder::new(128, 128).unwrap();
    let mut messages = Vec::new();
    let end = deadline();
    while messages.len() < 2 {
        assert!(Instant::now() < end);
        let r = c.drive(Instant::now(), IoBudget::default());
        let mut b = [0; 256];
        let n = c.read(&mut b);
        d.push(&b[..n]).unwrap();
        while let Some(m) = d.next_message().unwrap() {
            messages.push(m)
        }
        if messages.len() < 2 {
            r.unwrap();
            thread::yield_now();
        }
    }
    assert_eq!(messages[0].opcode, ws::Opcode::Ping);
    assert_eq!(messages[1].payload, b"{}");
    h.join().unwrap();
}
#[test]
fn fix_frames_share_the_same_tls_transport() {
    let (client, server) = tls_pair();
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap();
    let request = fix::encode("FIX.4.4", b"35=0\x0149=client\x01", 1024).unwrap();
    let expected = request.clone();
    let h = thread::spawn(move || {
        let (s, _) = l.accept().unwrap();
        s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let mut tls = rustls::StreamOwned::new(rustls::ServerConnection::new(server).unwrap(), s);
        let mut got = vec![0; expected.len()];
        tls.read_exact(&mut got).unwrap();
        assert_eq!(got, expected);
        let response = fix::encode("FIX.4.4", b"35=0\x0149=server\x01", 1024).unwrap();
        for piece in response.chunks(3) {
            tls.write_all(piece).unwrap();
            tls.flush().unwrap();
        }
        tls.conn.send_close_notify();
        tls.flush().unwrap();
    });
    let mut c = tls_client(addr, client);
    send_all(&mut c, &request);
    let mut d = fix::Decoder::new(1024).unwrap();
    let end = deadline();
    loop {
        assert!(Instant::now() < end);
        let r = c.drive(Instant::now(), IoBudget::default());
        let mut b = [0; 1024];
        let n = c.read(&mut b);
        d.push(&b[..n]).unwrap();
        if let Some(frame) = d.next_frame().unwrap() {
            assert!(frame.windows(9).any(|w| w == b"49=server"));
            break;
        }
        r.unwrap();
        thread::yield_now();
    }
    h.join().unwrap();
}
#[test]
fn deadline_after_tls_acceptance_closes_and_preserves_count() {
    let (client, server) = tls_pair();
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap();
    let h = thread::spawn(move || {
        let (mut s, _) = l.accept().unwrap();
        s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let mut tls = rustls::ServerConnection::new(server).unwrap();
        while tls.is_handshaking() {
            tls.complete_io(&mut s).unwrap();
        }
        let mut b = [0; 1024];
        let _ = s.read(&mut b);
    });
    let mut c = tls_client(addr, client);
    let end = deadline();
    c.queue(PreparedSend::new(b"order", end, 100).unwrap())
        .unwrap();
    assert_eq!(
        c.try_send(Instant::now(), || true).unwrap().accepted_bytes,
        3
    );
    assert!(c.drive(end, IoBudget::default()).is_err());
    let r = c.send_report().unwrap();
    assert_eq!(
        (r.state, r.accepted_bytes),
        (SendState::Failed(Failure::Deadline), 3)
    );
    assert!(
        c.queue(PreparedSend::new(b"x", deadline(), 100).unwrap())
            .is_err()
    );
    h.join().unwrap();
}
#[test]
fn actual_backpressure_keeps_partial_write_without_reauthorizing() {
    let (mut c, _server) = plain(Limits {
        max_send_bytes: 16 << 20,
        max_accept_bytes: 1 << 20,
        ..Limits::default()
    });
    drive_ready(&mut c);
    c.queue(PreparedSend::new(&vec![7; 16 << 20], deadline(), 16 << 20).unwrap())
        .unwrap();
    let mut calls = 0;
    let first = c
        .try_send(Instant::now(), || {
            calls += 1;
            true
        })
        .unwrap();
    assert!(first.accepted_bytes > 0);
    let mut previous = first.accepted_bytes;
    let mut blocked = false;
    for _ in 0..100 {
        let r = c
            .try_send(Instant::now(), || panic!("already accepted"))
            .unwrap();
        if r.accepted_bytes == previous {
            blocked = true;
            break;
        }
        previous = r.accepted_bytes;
    }
    assert!(blocked, "socket send buffer should fill");
    assert_eq!(calls, 1);
    assert_eq!(
        c.cancel().unwrap().state,
        SendState::Failed(Failure::Cancelled)
    );
}
#[test]
fn decrypted_plaintext_respects_each_drive_budget() {
    let (client, server) = tls_pair();
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap();
    let h = thread::spawn(move || {
        let (s, _) = l.accept().unwrap();
        s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let mut tls = rustls::StreamOwned::new(rustls::ServerConnection::new(server).unwrap(), s);
        tls.write_all(&[7; 128]).unwrap();
        tls.flush().unwrap();
        tls.conn.send_close_notify();
        tls.flush().unwrap();
    });
    let mut c = Connection::connect(
        addr,
        Security::Tls {
            server_name: "localhost".try_into().unwrap(),
            config: client,
        },
        Limits::default(),
        deadline(),
    )
    .unwrap();
    let mut total = 0;
    let end = deadline();
    while total < 128 {
        assert!(Instant::now() < end);
        let r = c
            .drive(
                Instant::now(),
                IoBudget {
                    max_syscalls: 2,
                    max_bytes: 1,
                },
            )
            .unwrap();
        assert!(r.received <= 1, "plaintext budget exceeded: {}", r.received);
        let mut b = [0; 256];
        total += c.read(&mut b);
    }
    h.join().unwrap();
}
#[test]
fn unready_queue_never_calls_gate_and_terminal_slot_reuses_new_id() {
    let (mut c, mut s) = plain(Limits::default());
    let first = c
        .queue(PreparedSend::new(b"old", deadline(), 100).unwrap())
        .unwrap();
    assert_eq!(
        c.try_send(Instant::now(), || panic!("connection not ready"))
            .unwrap()
            .state,
        SendState::Queued
    );
    drive_ready(&mut c);
    assert_eq!(
        c.try_send(Instant::now(), || false).unwrap().state,
        SendState::NoBytes(Failure::GateDenied)
    );
    assert_eq!(c.take_finished().unwrap().id, first);
    let next = c
        .queue(PreparedSend::new(b"new", deadline(), 100).unwrap())
        .unwrap();
    assert!(next > first);
    assert_eq!(
        c.try_send(Instant::now(), || true).unwrap().state,
        SendState::Flushed
    );
    let mut b = [0; 3];
    s.read_exact(&mut b).unwrap();
    assert_eq!(&b, b"new");
}
