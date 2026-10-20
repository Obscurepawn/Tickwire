use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant},
};
use tickwire_binance_clock::*;
use tickwire_clock::{MILLISECOND, SECOND};
type Server = rustls::StreamOwned<rustls::ServerConnection, TcpStream>;
struct TestClock {
    ns: Arc<AtomicU64>,
    domain: ClockDomain,
}
impl MonotonicClock for TestClock {
    fn sample(&mut self) -> Result<LocalTime> {
        Ok(LocalTime {
            instant: Instant::now(),
            monotonic: MonoTime::new(self.domain, self.ns.load(Ordering::SeqCst)),
        })
    }
}
fn config(source: Source) -> Config {
    Config {
        source,
        domain: ClockDomain::new([4; 16], 1).unwrap(),
        calibration: CalibrationConfig {
            max_round_trip_ns: SECOND,
            max_width_ns: 2 * SECOND,
            max_age_ns: 10 * SECOND,
            quantization_error_ns: MILLISECOND,
            drift_ppb: 0,
            max_adjustment_ns: 10 * MILLISECOND,
        },
        unit: TimeUnit::Milliseconds,
        first_sample_serial: 1,
        first_connection_epoch: 1,
        connect_timeout_ns: 5 * SECOND,
        request_timeout_ns: 2 * SECOND,
        max_response_bytes: 4096,
        max_headers: 16,
        io: Limits::default(),
    }
}
fn certs(names: Vec<String>) -> (Arc<rustls::ClientConfig>, Arc<rustls::ServerConfig>) {
    let cert = rcgen::generate_simple_self_signed(names).unwrap();
    let server = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(
            vec![cert.cert.der().clone()],
            rustls::pki_types::PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der()).into(),
        )
        .unwrap();
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    (
        Arc::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth(),
        ),
        Arc::new(server),
    )
}
fn fixture(
    cfg: Config,
) -> (
    BinanceTimeClient<TestClock>,
    TcpListener,
    Arc<rustls::ServerConfig>,
    Arc<AtomicU64>,
) {
    let (tls, server) = certs(vec![
        "api.binance.com".into(),
        "testnet.binance.vision".into(),
        "fapi.binance.com".into(),
        "demo-fapi.binance.com".into(),
    ]);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let ns = Arc::new(AtomicU64::new(0));
    let client = BinanceTimeClient::new(
        cfg,
        NetworkConfig {
            address: listener.local_addr().unwrap(),
            tls,
        },
        TestClock {
            ns: ns.clone(),
            domain: cfg.domain,
        },
    )
    .unwrap();
    (client, listener, server, ns)
}
fn accept(listener: &TcpListener, config: Arc<rustls::ServerConfig>) -> Server {
    let end = Instant::now() + Duration::from_secs(6);
    let socket = loop {
        match listener.accept() {
            Ok((s, _)) => break s,
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                assert!(Instant::now() < end);
                thread::sleep(Duration::from_millis(1));
            }
            Err(e) => panic!("{e}"),
        }
    };
    socket.set_nonblocking(false).unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(6)))
        .unwrap();
    socket
        .set_write_timeout(Some(Duration::from_secs(6)))
        .unwrap();
    rustls::StreamOwned::new(rustls::ServerConnection::new(config).unwrap(), socket)
}
fn headers(server: &mut Server) -> String {
    let mut bytes = Vec::new();
    while !bytes.ends_with(b"\r\n\r\n") {
        let mut b = [0];
        server.read_exact(&mut b).unwrap();
        bytes.push(b[0]);
        assert!(bytes.len() < 4096);
    }
    String::from_utf8(bytes).unwrap()
}
fn response(server: &mut Server, status: u16, body: &[u8], extra: &str) {
    let header = format!(
        "HTTP/1.1 {status} Result\r\nContent-Length: {}\r\n{extra}\r\n",
        body.len()
    );
    server.write_all(header.as_bytes()).unwrap();
    server.write_all(body).unwrap();
    server.flush().unwrap();
}
fn allow(_: &TimeCharge, _: &LocalTime) -> bool {
    true
}
#[test]
fn a_provider_clock_fault_revokes_status_and_cannot_be_cleared_by_a_later_read() {
    struct FailingClock {
        failed: Arc<AtomicU64>,
        domain: ClockDomain,
        error: tickwire_clock::Error,
    }
    impl MonotonicClock for FailingClock {
        fn sample(&mut self) -> Result<LocalTime> {
            if self.failed.load(Ordering::SeqCst) != 0 {
                Err(Error::Clock(self.error))
            } else {
                Ok(LocalTime {
                    instant: Instant::now(),
                    monotonic: MonoTime::new(self.domain, 0),
                })
            }
        }
    }
    for error in [
        tickwire_clock::Error::Domain,
        tickwire_clock::Error::TimeBackwards,
        tickwire_clock::Error::Overflow,
        tickwire_clock::Error::ClockJump,
        tickwire_clock::Error::Faulted,
    ] {
        let cfg = config(Source::SpotProduction);
        let (tls, _) = certs(vec!["api.binance.com".into()]);
        let failed = Arc::new(AtomicU64::new(0));
        let mut client = BinanceTimeClient::new(
            cfg,
            NetworkConfig {
                address: "127.0.0.1:1".parse().unwrap(),
                tls,
            },
            FailingClock {
                failed: failed.clone(),
                domain: cfg.domain,
                error,
            },
        )
        .unwrap();
        assert!(client.sample().unwrap().signed.is_none());
        failed.store(1, Ordering::SeqCst);
        assert!(matches!(client.sample(), Err(Error::Clock(found)) if found == error));
        assert_eq!(client.status(), CalibrationStatus::Faulted(error));
        failed.store(0, Ordering::SeqCst);
        assert!(matches!(client.sample(), Err(Error::Clock(found)) if found == error));
        assert_eq!(client.start(&mut allow), Err(Error::Clock(error)));
    }
}
fn finish(
    client: &mut BinanceTimeClient<TestClock>,
    permit: &mut impl TimePermit,
    budget: IoBudget,
) -> Outcome {
    let end = Instant::now() + Duration::from_secs(6);
    loop {
        let report = client.poll(permit, budget).unwrap();
        assert!(report.syscalls <= budget.max_syscalls);
        assert!(report.network_bytes <= budget.max_bytes);
        if let Some(out) = report.outcome {
            return out;
        }
        assert!(Instant::now() < end, "client did not complete");
        thread::sleep(Duration::from_millis(1));
    }
}
#[test]
fn each_product_and_environment_uses_its_authenticated_host_and_empty_time_get() {
    for (source, host, path) in [
        (Source::SpotProduction, "api.binance.com", "/api/v3/time"),
        (Source::SpotTest, "testnet.binance.vision", "/api/v3/time"),
        (Source::UsdmProduction, "fapi.binance.com", "/fapi/v1/time"),
        (Source::UsdmTest, "demo-fapi.binance.com", "/fapi/v1/time"),
    ] {
        let (mut client, listener, server, _) = fixture(config(source));
        assert!(client.sample().unwrap().signed.is_none());
        assert_eq!(client.start(&mut allow), Ok(1));
        let peer = thread::spawn(move || {
            let mut stream = accept(&listener, server);
            let h = headers(&mut stream);
            assert!(h.starts_with(&format!("GET {path} HTTP/1.1\r\n")));
            assert!(h.contains(&format!("Host: {host}\r\n")));
            assert!(h.contains("Content-Length: 0\r\n"));
            assert!(!h.contains("X-MBX-APIKEY"));
            response(
                &mut stream,
                200,
                br#"{"serverTime":1000}"#,
                "X-MBX-USED-WEIGHT-1M: 1\r\n",
            );
        });
        let out = finish(&mut client, &mut allow, IoBudget::default());
        assert_eq!(out.connection_epoch, 1);
        assert!(out.calibration.is_ok());
        assert_eq!(out.http.unwrap().status, 200);
        assert_eq!(client.sample().unwrap().signed.unwrap().timestamp(), 1000);
        peer.join().unwrap();
    }
}
#[test]
fn sample_begins_after_request_permit_and_ends_after_complete_response() {
    let (mut client, listener, server, ns) = fixture(config(Source::SpotTest));
    let stamp = ns.clone();
    let mut charges = Vec::new();
    let mut permit = |c: &TimeCharge, _: &LocalTime| {
        charges.push(*c);
        if c.kind == ChargeKind::Query {
            stamp.store(100 * MILLISECOND, Ordering::SeqCst);
        }
        true
    };
    client.start(&mut permit).unwrap();
    let peer = thread::spawn(move || {
        let mut s = accept(&listener, server);
        headers(&mut s);
        ns.store(120 * MILLISECOND, Ordering::SeqCst);
        response(&mut s, 200, br#"{"serverTime":1000}"#, "");
    });
    let out = finish(&mut client, &mut permit, IoBudget::default());
    let reading = out.calibration.unwrap();
    assert_eq!(
        (
            reading.lower_ns(),
            reading.upper_ns(),
            reading.sampled_at_ns()
        ),
        (999 * MILLISECOND, 1021 * MILLISECOND, 120 * MILLISECOND)
    );
    assert_eq!(
        charges
            .iter()
            .map(|c| (c.kind, c.api_weight))
            .collect::<Vec<_>>(),
        vec![(ChargeKind::Connect, 0), (ChargeKind::Query, 1)]
    );
    peer.join().unwrap();
}
#[test]
fn fragmented_complete_response_is_kept_when_peer_closes_with_tiny_io_budget() {
    let (mut client, listener, server, _) = fixture(config(Source::UsdmTest));
    client.start(&mut allow).unwrap();
    let peer = thread::spawn(move || {
        let mut s = accept(&listener, server);
        headers(&mut s);
        for piece in [
            b"HTTP/1.1 200 OK\r\nContent-Length: 19\r\n\r\n".as_slice(),
            b"{\"serverTime\":",
            b"1000}",
        ] {
            s.write_all(piece).unwrap();
            s.flush().unwrap();
        }
        s.conn.send_close_notify();
        s.flush().unwrap();
    });
    let out = finish(
        &mut client,
        &mut allow,
        IoBudget {
            max_syscalls: 2,
            max_bytes: 97,
        },
    );
    assert!(out.calibration.is_ok(), "{:?}", out.calibration);
    peer.join().unwrap();
}
#[test]
fn duplicate_wrong_typed_zero_and_truncated_time_responses_are_rejected() {
    for body in [
        br#"{"serverTime":1000,"serverTime":1001}"#.as_slice(),
        br#"{"serverTime":"1000"}"#,
        br#"{"serverTime":0}"#,
        br#"{"serverTime":-1}"#,
        br#"{"serverTime":1.5}"#,
        br#"{"serverTime":1000"#,
    ] {
        let (mut client, listener, server, _) = fixture(config(Source::SpotTest));
        client.start(&mut allow).unwrap();
        let body = body.to_vec();
        let peer = thread::spawn(move || {
            let mut s = accept(&listener, server);
            headers(&mut s);
            response(&mut s, 200, &body, "");
        });
        let out = finish(&mut client, &mut allow, IoBudget::default());
        assert!(out.calibration.is_err());
        assert!(client.sample().unwrap().signed.is_none());
        peer.join().unwrap();
    }
}
#[test]
fn non_success_status_preserves_rate_limit_metadata_without_calibration() {
    let (mut client, listener, server, _) = fixture(config(Source::SpotTest));
    client.start(&mut allow).unwrap();
    let peer = thread::spawn(move || {
        let mut s = accept(&listener, server);
        headers(&mut s);
        response(&mut s, 429, br#"{"serverTime":1000}"#, "Retry-After: 2\r\n");
    });
    let out = finish(&mut client, &mut allow, IoBudget::default());
    assert_eq!(out.calibration, Err(Error::HttpStatus(429)));
    let meta = out.http.unwrap();
    assert_eq!(meta.status, 429);
    assert!(
        meta.headers
            .iter()
            .any(|(k, v)| k.eq_ignore_ascii_case("Retry-After") && v == b"2")
    );
    peer.join().unwrap();
}
#[test]
fn a_reply_before_complete_application_acceptance_cannot_calibrate() {
    let mut cfg = config(Source::SpotTest);
    cfg.io.max_accept_bytes = 1;
    let (mut client, listener, server, _) = fixture(cfg);
    client.start(&mut allow).unwrap();
    let peer = thread::spawn(move || {
        let mut s = accept(&listener, server);
        let mut first = [0];
        s.read_exact(&mut first).unwrap();
        response(&mut s, 200, br#"{"serverTime":1000}"#, "");
    });
    let out = finish(&mut client, &mut allow, IoBudget::default());
    assert_eq!(out.calibration, Err(Error::Protocol));
    assert!(client.sample().unwrap().signed.is_none());
    peer.join().unwrap();
}
#[test]
fn cancellation_consumes_connection_epoch_and_next_sample_uses_a_fresh_socket() {
    let (mut client, listener, server, _) = fixture(config(Source::SpotTest));
    assert_eq!(client.start(&mut allow), Ok(1));
    // Observe the first TCP connection before canceling; a canceled pending SYN
    // need not appear in the server's accept queue on every operating system.
    let end = Instant::now() + Duration::from_secs(2);
    let mut old = loop {
        match listener.accept() {
            Ok((socket, _)) => break socket,
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                assert!(Instant::now() < end);
                thread::yield_now();
            }
            Err(e) => panic!("{e}"),
        }
    };
    old.set_nonblocking(false).unwrap();
    old.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    client.cancel().unwrap();
    assert_eq!(old.read(&mut [0]).unwrap(), 0);
    assert_eq!(client.start(&mut allow), Ok(2));
    let peer = thread::spawn(move || {
        let mut s = accept(&listener, server);
        headers(&mut s);
        response(&mut s, 200, br#"{"serverTime":1000}"#, "");
    });
    let out = finish(&mut client, &mut allow, IoBudget::default());
    assert_eq!(out.connection_epoch, 2);
    assert!(out.calibration.is_ok());
    peer.join().unwrap();
}
#[test]
fn clock_owner_rechecks_fresh_time_before_sending_an_old_signed_stamp() {
    let (mut client, listener, server, ns) = fixture(config(Source::SpotTest));
    client.start(&mut allow).unwrap();
    let peer = thread::spawn(move || {
        let mut s = accept(&listener, server);
        headers(&mut s);
        response(&mut s, 200, br#"{"serverTime":1000}"#, "");
    });
    finish(&mut client, &mut allow, IoBudget::default())
        .calibration
        .unwrap();
    let signed = client.sample().unwrap().signed.unwrap();
    ns.store(20 * MILLISECOND, Ordering::SeqCst);
    assert_eq!(
        client.check_send(&signed, 10 * MILLISECOND, 0),
        Err(Error::Clock(tickwire_clock::Error::TooOld))
    );
    peer.join().unwrap();
}
#[test]
fn connection_permission_is_required_and_exhausted_epochs_are_never_reused() {
    let mut cfg = config(Source::SpotTest);
    cfg.first_connection_epoch = u64::MAX;
    let (mut client, _, _, _) = fixture(cfg);
    let mut deny = |_: &TimeCharge, _: &LocalTime| false;
    assert_eq!(client.start(&mut deny), Err(Error::Denied));
    assert_eq!(client.start(&mut allow), Err(Error::Exhausted));
}

#[test]
fn a_zero_accept_attempt_rechecks_the_current_query_permission() {
    let mut cfg = config(Source::SpotTest);
    cfg.io.tls_buffer_bytes = 1;
    let (mut client, listener, server, _) = fixture(cfg);
    let calls = Arc::new(AtomicU64::new(0));
    let count = calls.clone();
    let mut permit = move |charge: &TimeCharge, _: &LocalTime| {
        charge.kind == ChargeKind::Connect || count.fetch_add(1, Ordering::SeqCst) == 0
    };
    client.start(&mut permit).unwrap();
    let peer = thread::spawn(move || {
        let mut s = accept(&listener, server);
        let mut bytes = [0; 1];
        s.read(&mut bytes).unwrap_or_default()
    });
    let end = Instant::now() + Duration::from_secs(3);
    while calls.load(Ordering::SeqCst) == 0 {
        assert!(Instant::now() < end);
        let r = client
            .poll(
                &mut permit,
                IoBudget {
                    max_syscalls: 2,
                    max_bytes: 97,
                },
            )
            .unwrap();
        assert!(r.outcome.is_none());
        thread::sleep(Duration::from_millis(1));
    }
    let progress = client.in_flight().unwrap();
    assert!(progress.measurement_started);
    assert_eq!(
        progress.accepted_application_bytes, 0,
        "fixture must leave the first TLS writer attempt blocked"
    );
    let outcome = finish(&mut client, &mut permit, IoBudget::default());
    assert_eq!(outcome.calibration, Err(Error::Denied));
    assert!(calls.load(Ordering::SeqCst) >= 2);
    assert_eq!(peer.join().unwrap(), 0);
}

#[test]
fn network_owner_marks_instant_rollback_as_a_permanent_clock_fault() {
    struct Backward {
        instant: Instant,
        domain: ClockDomain,
        calls: u64,
    }
    impl MonotonicClock for Backward {
        fn sample(&mut self) -> Result<LocalTime> {
            self.calls += 1;
            Ok(LocalTime {
                instant: self.instant - Duration::from_nanos(self.calls),
                monotonic: MonoTime::new(self.domain, 0),
            })
        }
    }
    let cfg = config(Source::SpotTest);
    let (tls, _) = certs(vec!["testnet.binance.vision".into()]);
    let mut client = BinanceTimeClient::new(
        cfg,
        NetworkConfig {
            address: "127.0.0.1:1".parse().unwrap(),
            tls,
        },
        Backward {
            instant: Instant::now(),
            domain: cfg.domain,
            calls: 0,
        },
    )
    .unwrap();
    assert!(client.sample().is_ok());
    assert!(matches!(
        client.sample(),
        Err(Error::Clock(tickwire_clock::Error::TimeBackwards))
    ));
    assert_eq!(
        client.status(),
        CalibrationStatus::Faulted(tickwire_clock::Error::TimeBackwards)
    );
}

#[test]
fn a_monotonic_network_deadline_overflow_revokes_clock_use() {
    let (mut client, _, _, ns) = fixture(config(Source::SpotTest));
    ns.store(u64::MAX - 1, Ordering::SeqCst);
    assert_eq!(
        client.start(&mut allow),
        Err(Error::Clock(tickwire_clock::Error::Overflow))
    );
    assert_eq!(
        client.status(),
        CalibrationStatus::Faulted(tickwire_clock::Error::Overflow)
    );
    assert!(client.sample().is_err());
}

#[test]
fn a_failed_second_sample_keeps_the_first_calibrations_original_expiry() {
    let (mut client, listener, server, ns) = fixture(config(Source::SpotTest));
    client.start(&mut allow).unwrap();
    let peer = thread::spawn(move || {
        {
            let mut s = accept(&listener, server.clone());
            headers(&mut s);
            response(&mut s, 200, br#"{"serverTime":1000}"#, "");
        }
        let mut s = accept(&listener, server);
        headers(&mut s);
        response(&mut s, 503, b"{}", "");
    });
    let first = finish(&mut client, &mut allow, IoBudget::default())
        .calibration
        .unwrap();
    ns.store(MILLISECOND, Ordering::SeqCst);
    assert_eq!(client.start(&mut allow), Ok(2));
    assert_eq!(
        finish(&mut client, &mut allow, IoBudget::default()).calibration,
        Err(Error::HttpStatus(503))
    );
    assert_eq!(
        client.status(),
        CalibrationStatus::Calibrated {
            sample_serial: 1,
            valid_until_ns: first.valid_until_ns()
        }
    );
    assert_eq!(client.sample().unwrap().signed.unwrap().timestamp(), 1001);
    peer.join().unwrap();
}

#[test]
fn a_trusted_ca_cannot_substitute_a_certificate_for_a_different_host() {
    let cfg = config(Source::SpotTest);
    let (tls, server) = certs(vec!["different.example".into()]);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let mut client = BinanceTimeClient::new(
        cfg,
        NetworkConfig {
            address: listener.local_addr().unwrap(),
            tls,
        },
        TestClock {
            ns: Arc::new(AtomicU64::new(0)),
            domain: cfg.domain,
        },
    )
    .unwrap();
    client.start(&mut allow).unwrap();
    let peer = thread::spawn(move || {
        let mut s = accept(&listener, server);
        let _ = s.read(&mut [0; 1]);
    });
    let out = finish(&mut client, &mut allow, IoBudget::default());
    assert_eq!(out.calibration, Err(Error::Transport));
    assert!(client.sample().unwrap().signed.is_none());
    peer.join().unwrap();
}

#[test]
fn an_oversized_declared_http_body_is_rejected_before_reading_its_payload() {
    let mut cfg = config(Source::SpotTest);
    cfg.max_response_bytes = 128;
    let (mut client, listener, server, _) = fixture(cfg);
    client.start(&mut allow).unwrap();
    let peer = thread::spawn(move || {
        let mut s = accept(&listener, server);
        headers(&mut s);
        s.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 1000000\r\n\r\n")
            .unwrap();
        s.flush().unwrap();
    });
    let out = finish(&mut client, &mut allow, IoBudget::default());
    assert_eq!(out.calibration, Err(Error::Protocol));
    assert!(client.sample().unwrap().signed.is_none());
    peer.join().unwrap();
}
