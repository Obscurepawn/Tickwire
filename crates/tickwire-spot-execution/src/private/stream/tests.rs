use super::*;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use ring::digest::{SHA1_FOR_LEGACY_USE_ONLY, digest};
use serde_json::json;
use std::{
    fs,
    io::{BufRead, BufReader, Read, Write},
    net::{TcpListener, TcpStream},
    sync::{
        atomic::{AtomicU64, Ordering},
        mpsc,
    },
    time::Instant,
};
use tickwire_venue::Environment;
type Peer = BufReader<rustls::StreamOwned<rustls::ServerConnection, TcpStream>>;

fn config(venue: SpotVenue) -> StreamConfig {
    static ID: AtomicU64 = AtomicU64::new(0);
    StreamConfig {
        directory: std::env::temp_dir().join(format!(
            "tw-private-stream-{}-{}",
            std::process::id(),
            ID.fetch_add(1, Ordering::Relaxed)
        )),
        scope: Scope {
            risk_domain_id: [1; 16],
            account: 7,
            venue: venue.id(),
            environment: Environment::Test,
        },
        uid: "12466".into(),
        stream_id: [4; 16],
        boot_id: [8; 16],
    }
}
fn frame(peer: &mut impl Write, body: &[u8]) {
    let mut h = vec![0x81];
    if body.len() < 126 {
        h.push(body.len() as u8)
    } else {
        h.push(126);
        h.extend((body.len() as u16).to_be_bytes());
    }
    peer.write_all(&h).unwrap();
    peer.write_all(body).unwrap();
    peer.flush().unwrap();
}
fn message(peer: &mut impl Read) -> Vec<u8> {
    let mut header = [0; 2];
    peer.read_exact(&mut header).unwrap();
    assert_eq!(header[0], 0x81);
    let mut len = usize::from(header[1] & 127);
    if len == 126 {
        peer.read_exact(&mut header).unwrap();
        len = u16::from_be_bytes(header).into();
    }
    let mut mask = [0; 4];
    peer.read_exact(&mut mask).unwrap();
    let mut payload = vec![0; len];
    peer.read_exact(&mut payload).unwrap();
    for (i, c) in payload.iter_mut().enumerate() {
        *c ^= mask[i % 4];
    }
    payload
}
fn server_auth(
    listener: &TcpListener,
    server: &Arc<rustls::ServerConfig>,
    venue: SpotVenue,
) -> Peer {
    let (sock, _) = listener.accept().unwrap();
    sock.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    sock.set_nodelay(true).unwrap();
    let mut p = BufReader::new(rustls::StreamOwned::new(
        rustls::ServerConnection::new(server.clone()).unwrap(),
        sock,
    ));
    let mut header = String::new();
    while !header.ends_with("\r\n\r\n") {
        assert!(p.read_line(&mut header).unwrap() > 0);
    }
    let key = header
        .lines()
        .find_map(|l| l.strip_prefix("Sec-WebSocket-Key: "))
        .unwrap();
    let accept = STANDARD.encode(digest(
        &SHA1_FOR_LEGACY_USE_ONLY,
        format!("{key}258EAFA5-E914-47DA-95CA-C5AB0DC85B11").as_bytes(),
    ));
    write!(p.get_mut(),"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n").unwrap();
    p.get_mut().flush().unwrap();
    let auth: Value = serde_json::from_slice(&message(&mut p)).unwrap();
    let bybit = venue == SpotVenue::Bybit;
    assert_eq!(auth["op"], if bybit { "auth" } else { "login" });
    let ack = if bybit {
        json!({"op":"auth","success":true,"conn_id":"a"})
    } else {
        json!({"event":"login","code":"0","connId":"a"})
    };
    frame(p.get_mut(), ack.to_string().as_bytes());
    let sub: Value = serde_json::from_slice(&message(&mut p)).unwrap();
    assert_eq!(sub["op"], "subscribe");
    let ack = if bybit {
        json!({"op":"subscribe","success":true,"req_id":"twaccount","conn_id":"a"})
    } else {
        json!({"event":"subscribe","id":"twaccount","arg":{"channel":"orders","instType":"SPOT"},"connId":"a"})
    };
    frame(p.get_mut(), ack.to_string().as_bytes());
    p
}
fn wait(mut accept: impl FnMut() -> bool) {
    let until = Instant::now() + Duration::from_secs(7);
    while !accept() {
        assert!(Instant::now() < until, "private stream deadline");
        thread::sleep(Duration::from_millis(2));
    }
}
#[test]
fn reconnect_keeps_native_rows_and_marks_each_lost_epoch() {
    for venue in [SpotVenue::Bybit, SpotVenue::Okx] {
        let cfg = config(venue);
        let root = cfg.directory.clone();
        let mut capture = Capture::open(cfg.clone()).unwrap();
        let r = capture.reader();
        let shared = r.0.clone();
        let (host, path) = endpoint(venue, Environment::Test);
        let cert = rcgen::generate_simple_self_signed(vec![host.to_owned()]).unwrap();
        let mut roots = rustls::RootCertStore::empty();
        roots.add(cert.cert.der().clone()).unwrap();
        let key = rustls::pki_types::PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
        let srv = Arc::new(
            rustls::ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(vec![cert.cert.der().clone()], key.into())
                .unwrap(),
        );
        let tls = Arc::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth(),
        );
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let (send, recv) = mpsc::channel();
        let (pong_sent, observed_pong) = mpsc::channel();
        let peer = thread::spawn(move || {
            let bybit = venue == SpotVenue::Bybit;
            let okx_ready=br#"{"id":"twaccount","event":"subscribe","arg":{"channel":"account"},"connId":"a"}"#;
            for attempt in 0..2 {
                let mut p = server_auth(&listener, &srv, venue);
                if attempt == 0 {
                    let rows = if bybit {
                        json!({"topic":"execution.spot","data":[{"category":"spot","symbol":"BTCUSDT","execId":"a"},{"category":"spot","symbol":"BTCUSDT","execId":"b"}]})
                    } else {
                        json!({"arg":{"channel":"orders","instType":"SPOT"},"data":[{"instType":"SPOT","instId":"BTC-USDT","tradeId":"a"},{"instType":"SPOT","instId":"BTC-USDT","tradeId":"b"}]})
                    };
                    frame(p.get_mut(), rows.to_string().as_bytes());
                    if !bybit {
                        recv.recv_timeout(Duration::from_secs(4)).unwrap();
                        frame(p.get_mut(), okx_ready);
                    }
                    let ping = message(&mut p);
                    if bybit {
                        assert_eq!(
                            serde_json::from_slice::<Value>(&ping).unwrap()["op"],
                            "ping"
                        );
                        frame(
                            p.get_mut(),
                            br#"{"op":"pong","conn_id":"a","args":["123"]}"#,
                        );
                    } else {
                        assert_eq!(ping, b"ping");
                        frame(p.get_mut(), b"pong");
                    }
                    pong_sent.send(()).unwrap();
                    // Hold TCP open and ignore future heartbeats; the client must record its own loss.
                    recv.recv_timeout(Duration::from_secs(5)).unwrap();
                } else {
                    if !bybit {
                        frame(p.get_mut(), okx_ready);
                    }
                    recv.recv_timeout(Duration::from_secs(5)).unwrap();
                }
            }
        });
        let reader = r.clone();
        let worker = thread::spawn(move || {
            let start = Instant::now();
            let creds = if venue == SpotVenue::Bybit {
                Credentials::bybit("key", b"secret")
            } else {
                Credentials::okx("key", b"secret", "pass")
            };
            let d = || {
                let s = network::tls_at(host, [addr], tls.clone())?;
                s.sock.set_read_timeout(Some(Duration::from_millis(15)))?;
                wire::Socket::open(
                    s,
                    host,
                    path,
                    &creds,
                    || Ok(1700000000000),
                    wire::Timing {
                        setup: Duration::from_secs(3),
                        ping: Duration::from_millis(240),
                        idle: Duration::from_millis(900),
                    },
                )
            };
            run(&mut capture, || start.elapsed().as_nanos() as u64 + 1, d).unwrap();
        });
        if venue == SpotVenue::Okx {
            wait(|| reader.drain(0, 8).unwrap().len() == 3);
            assert!(!reader.status().unwrap().ready);
            send.send(()).unwrap();
        }
        wait(|| {
            reader
                .status()
                .is_some_and(|s| s.ready && s.durable_sequence == 4)
        });
        let live = reader.status().unwrap();
        assert!(live.current(live.epoch, 4, live.last_received_ns, 10));
        assert!(!live.current(live.epoch, 3, live.last_received_ns, 10));
        assert!(!live.current(live.epoch, 4, live.last_received_ns + 11, 10));
        let events = reader.drain(0, 4).unwrap();
        let ids = events
            .iter()
            .filter_map(|e| {
                if let EventKind::Data { row, .. } = &e.kind {
                    row[if venue == SpotVenue::Bybit {
                        "execId"
                    } else {
                        "tradeId"
                    }]
                    .as_str()
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        assert_eq!(ids, ["a", "b"]);
        assert_eq!(reader.drain(2, 1).unwrap()[0].sequence, 3);
        observed_pong.recv_timeout(Duration::from_secs(3)).unwrap();
        wait(|| {
            reader.status().is_some_and(|s| {
                s.last_received_ns > live.last_received_ns && s.ready && s.durable_sequence == 4
            })
        });
        wait(|| {
            reader
                .drain(4, 1)
                .unwrap()
                .first()
                .is_some_and(|e| matches!(e.kind, EventKind::Gap { .. }))
        });
        assert!(!reader.status().unwrap().ready);
        send.send(()).unwrap();
        wait(|| {
            reader
                .status()
                .is_some_and(|s| s.epoch == live.epoch + 1 && s.durable_sequence == 7 && s.ready)
        });
        shared.stop.store(true, Ordering::Release);
        send.send(()).unwrap();
        worker.join().unwrap();
        peer.join().unwrap();
        assert!(!reader.failed());
        let reopened = Capture::open(cfg).unwrap();
        let restored = reopened.reader();
        assert!(restored.has_initial_cursor(live.epoch, 5));
        assert!(!restored.has_initial_cursor(live.epoch, 6));
        assert!(matches!(
            restored.drain(7, 1).unwrap()[0].kind,
            EventKind::Gap { .. }
        ));
        drop(reopened);
        fs::remove_dir_all(root).unwrap();
    }
}
#[test]
fn crash_invalidates_previous_ready_epoch_and_rejects_scope_changes_and_corrupt_body() {
    let cfg = config(SpotVenue::Bybit);
    let root = cfg.directory.clone();
    let mut capture = Capture::open(cfg.clone()).unwrap();
    capture.begin(20).unwrap();
    capture.append(Kind::Ready, 21).unwrap();
    let old = capture.reader().status().unwrap().epoch;
    let body=br#"{"topic":"execution.spot","data":[{"category":"spot","symbol":"BTCUSDT","execId":"f"}]}"#;
    let mut feed =
        super::super::PrivateFeed::new(&Credentials::bybit("k", b"s"), 1700000000000).unwrap();
    feed.on_text(br#"{"op":"auth","success":true,"conn_id":"a"}"#)
        .unwrap();
    feed.on_text(br#"{"op":"subscribe","req_id":"twaccount","success":true,"conn_id":"a"}"#)
        .unwrap();
    let super::super::Reaction::Data(batch) = feed.on_text(body).unwrap() else {
        panic!("missing fill")
    };
    capture.data(batch, body, 22).unwrap();
    drop(capture);
    let mut wrong = cfg.clone();
    wrong.uid = "99".into();
    assert!(Capture::open(wrong).is_err());
    let mut next = Capture::open(cfg.clone()).unwrap();
    let r = next.reader();
    assert!(next.open_epoch());
    assert!(r.has_initial_cursor(old, 3));
    assert!(!r.status().unwrap().ready);
    next.gap("process_restart", 1).unwrap();
    next.begin(2).unwrap();
    let reset = r.drain(3, 2).unwrap();
    assert_eq!(reset[0].received_ns, 22);
    assert_eq!(reset[1].epoch, old + 1);
    assert!(!r.has_initial_cursor(old + 1, 5));
    drop(next);
    let file = fs::read_dir(root.join("frames"))
        .unwrap()
        .flatten()
        .find(|e| e.file_name().to_string_lossy().starts_with("object-"))
        .unwrap();
    fs::write(file.path(), b"wrong").unwrap();
    assert!(Capture::open(cfg).is_err());
    assert!(r.drain(2, 1).is_err());
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn failed_wal_never_advances_a_cursor_or_authorizes_a_first_write() {
    let cfg = config(SpotVenue::Okx);
    let mut capture = Capture::open(cfg.clone()).unwrap();
    let r = capture.reader();
    capture.begin(1).unwrap();
    capture.append(Kind::Ready, 2).unwrap();
    capture.settled(true).unwrap();
    let before = r.status().unwrap();
    assert!(before.current(before.epoch, 2, 2, 10));
    {
        let _worker_updating = r.0.status.lock().unwrap();
        assert!(r.status().is_none());
    }
    capture.pending(); // The worker saw bytes or a socket error, before any disk I/O.
    assert!(!r.status().unwrap().current(before.epoch, 2, 3, 10));
    fs::remove_file(cfg.directory.join("segment-00000000000000000001.wal")).unwrap();
    assert!(capture.gap("connection_lost", 3).is_err());
    assert!(!r.status().unwrap().ready);
    assert_eq!(r.drain(0, 10).unwrap().len(), 2);
    drop(capture);
    assert!(r.failed());
    assert!(r.status().is_none());
    fs::remove_dir_all(cfg.directory).unwrap();
}

#[test]
fn an_empty_decoder_buffer_between_fragments_is_not_a_drained_connection() {
    use tickwire_transport::ws::{Decoder, PollMessage};
    let mut ws = Decoder::new(1024, 1024).unwrap();
    ws.push(&[0x01, 0]).unwrap(); // non-final empty text fragment
    assert_eq!(ws.poll_message().unwrap(), PollMessage::NeedInput);
    assert_eq!(ws.buffered_len(), 0);
    assert!(!ws.at_message_boundary());
    ws.push(&[0x89, 0]).unwrap(); // a heartbeat may interleave with a fragmented message
    assert!(matches!(
        ws.poll_message().unwrap(),
        PollMessage::Message(_)
    ));
    assert!(!ws.at_message_boundary());
    ws.push(&[0x80, 0]).unwrap();
    assert!(matches!(
        ws.poll_message().unwrap(),
        PollMessage::Message(_)
    ));
    assert!(ws.at_message_boundary());
}
