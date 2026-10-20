mod common;
use common::*;
use std::{
    io::{Read, Write},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant},
};
use tickwire_binance_private::*;
fn response(s: &mut Server, status: u16, body: &str) {
    write!(
        s,
        "HTTP/1.1 {status} Response\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
    .unwrap();
    s.flush().unwrap();
}
struct ClockWithJump {
    start: Instant,
    jump: Arc<AtomicU64>,
}
impl Clock for ClockWithJump {
    fn check_signed(
        &mut self,
        _: &AccountScope,
        _: &SignedTime,
        _: &ClockSample,
        _: u64,
    ) -> Result<()> {
        Err(Error::Unsupported)
    }
    fn sample(&mut self) -> Result<ClockSample> {
        let n = self.start.elapsed().as_nanos() as u64 + self.jump.load(Ordering::SeqCst);
        Ok(ClockSample {
            instant: Instant::now(),
            monotonic: MonoTime::new(test_domain(), n),
            signing_time: None,
        })
    }
}
#[test]
fn unknown_delete_is_terminal_and_keeps_remote_ownership_uncertain() {
    let (tls, server) = certs();
    let m = listener();
    let w = listener();
    let ma = m.local_addr().unwrap();
    let wa = w.local_addr().unwrap();
    let mh = thread::spawn({
        let server = server.clone();
        move || {
            let mut c = accept(&m, server.clone());
            assert!(headers(&mut c).starts_with("POST "));
            response(&mut c, 200, "{\"listenKey\":\"PRIVATE-KEY-ONE\"}");
            drop(c);
            let mut c = accept(&m, server);
            assert!(headers(&mut c).starts_with("DELETE "));
            response(&mut c, 500, "{\"code\":-1007,\"msg\":\"timeout\"}");
        }
    });
    let wh = thread::spawn(move || {
        let mut c = accept(&w, server);
        upgrade(&mut c, "/private/ws/PRIVATE-KEY-ONE");
        let mut b = [0];
        let _ = c.read(&mut b);
    });
    let mut c = PrivateClient::new(
        SessionConfig::new(scope(Market::Usdm)),
        Authentication::UsdmApiKey { api_key: KEY },
        NetworkConfig {
            stream_address: wa,
            management_address: Some(ma),
            tls,
        },
        20,
    )
    .unwrap();
    let mut clock = TestClock {
        start: Instant::now(),
    };
    let mut permit = |_: &Charge, _: &ClockSample| true;
    let mut stopped = false;
    let mut close = None;
    let end = Instant::now() + Duration::from_secs(3);
    while c.status().phase != Phase::Stopped && Instant::now() < end {
        c.poll(&mut clock, PollBudget::default(), &mut permit, |d| {
            if let Delivery::Control(o) = d
                && o.token.operation() == Operation::UsdmClose
            {
                close = Some(o.state);
            }
            Ok(())
        })
        .unwrap();
        if !stopped && c.status().phase == Phase::Receiving {
            c.stop(StopMode::CloseRemoteStream).unwrap();
            stopped = true;
        }
        thread::yield_now();
    }
    assert_eq!(c.status().phase, Phase::Stopped);
    assert!(c.status().remote_close_unknown);
    assert_eq!(close, Some(ControlState::MayHaveApplied));
    assert!(!format!("{c:?}").contains("PRIVATE-KEY-ONE"));
    for _ in 0..10 {
        c.poll(&mut clock, PollBudget::default(), &mut permit, |_| Ok(()))
            .unwrap();
    }
    drop(c);
    mh.join().unwrap();
    wh.join().unwrap();
}
#[test]
fn lease_expiry_cannot_be_reversed_by_late_successful_renewal() {
    let (tls, server) = certs();
    let m = listener();
    let w = listener();
    let ma = m.local_addr().unwrap();
    let wa = w.local_addr().unwrap();
    let jumped = Arc::new(AtomicBool::new(false));
    let jump = Arc::new(AtomicU64::new(0));
    let ready = Arc::new(AtomicBool::new(false));
    let mh = thread::spawn({
        let server = server.clone();
        let jump = jump.clone();
        let ready = ready.clone();
        let jumped = jumped.clone();
        move || {
            let mut c = accept(&m, server.clone());
            assert!(headers(&mut c).starts_with("POST "));
            response(&mut c, 200, "{\"listenKey\":\"PRIVATE-KEY-ONE\"}");
            drop(c);
            let mut c = accept(&m, server);
            assert!(headers(&mut c).starts_with("PUT "));
            ready.store(true, Ordering::SeqCst);
            while !jumped.load(Ordering::SeqCst) {
                thread::yield_now();
            }
            assert!(jump.load(Ordering::SeqCst) >= 3_600_000_000_000);
            // The peer may already have closed after invalidating the old lease.
            let _ = write!(
                c,
                "HTTP/1.1 200 OK\r\nContent-Length: 31\r\n\r\n{{\"listenKey\":\"PRIVATE-KEY-ONE\"}}"
            );
            let _ = c.flush();
        }
    });
    let wh = thread::spawn(move || {
        let mut c = accept(&w, server);
        upgrade(&mut c, "/private/ws/PRIVATE-KEY-ONE");
        let mut b = [0];
        let _ = c.read(&mut b);
    });
    let mut cfg = SessionConfig::new(scope(Market::Usdm));
    cfg.timing.renew_ns = 10_000_000;
    let mut c = PrivateClient::new(
        cfg,
        Authentication::UsdmApiKey { api_key: KEY },
        NetworkConfig {
            stream_address: wa,
            management_address: Some(ma),
            tls,
        },
        20,
    )
    .unwrap();
    let mut clock = ClockWithJump {
        start: Instant::now(),
        jump: jump.clone(),
    };
    let mut permit = |_: &Charge, _: &ClockSample| true;
    let mut confirmed = false;
    let end = Instant::now() + Duration::from_secs(3);
    while !ready.load(Ordering::SeqCst) && Instant::now() < end {
        c.poll(&mut clock, PollBudget::default(), &mut permit, |_| Ok(()))
            .unwrap();
        thread::yield_now();
    }
    assert!(ready.load(Ordering::SeqCst));
    jump.store(3_600_000_000_000, Ordering::SeqCst);
    jumped.store(true, Ordering::SeqCst);
    c.poll(&mut clock, PollBudget::default(), &mut permit, |d| {
        if let Delivery::Control(o) = d
            && o.token.operation() == Operation::UsdmKeepalive
            && o.state == ControlState::Confirmed
        {
            confirmed = true;
        }
        Ok(())
    })
    .unwrap();
    assert!(!confirmed);
    assert_ne!(c.status().phase, Phase::Receiving);
    assert!(c.status().lease_until_ns.is_none());
    c.stop(StopMode::Disconnect).unwrap();
    drop(c);
    mh.join().unwrap();
    wh.join().unwrap();
}
#[test]
fn premature_delete_response_preserves_partial_acceptance_and_remote_ownership() {
    let (tls, server) = certs();
    let m = listener();
    let w = listener();
    let ma = m.local_addr().unwrap();
    let wa = w.local_addr().unwrap();
    let replied = Arc::new(AtomicBool::new(false));
    let accepted = Arc::new(AtomicBool::new(false));
    let mh = thread::spawn({
        let server = server.clone();
        let replied = replied.clone();
        move || {
            let mut s = accept(&m, server.clone());
            assert!(headers(&mut s).starts_with("POST "));
            response(&mut s, 200, "{\"listenKey\":\"PRIVATE-KEY-ONE\"}");
            drop(s);
            let mut s = accept(&m, server);
            let mut first = [0];
            s.read_exact(&mut first).unwrap();
            assert_eq!(&first, b"D");
            response(&mut s, 200, "{}");
            replied.store(true, Ordering::SeqCst);
            let mut b = [0];
            let _ = s.read(&mut b);
        }
    });
    let wh = thread::spawn(move || {
        let mut s = accept(&w, server);
        upgrade(&mut s, "/private/ws/PRIVATE-KEY-ONE");
        let mut b = [0];
        let _ = s.read(&mut b);
    });
    let mut cfg = SessionConfig::new(scope(Market::Usdm));
    cfg.limits.io.max_accept_bytes = 1;
    let mut c = PrivateClient::new(
        cfg,
        Authentication::UsdmApiKey { api_key: KEY },
        NetworkConfig {
            stream_address: wa,
            management_address: Some(ma),
            tls,
        },
        20,
    )
    .unwrap();
    let mut clock = TestClock {
        start: Instant::now(),
    };
    let mut permit = |q: &Charge, _: &ClockSample| {
        if q.kind == ChargeKind::Api(Operation::UsdmClose) {
            accepted.store(true, Ordering::SeqCst);
        }
        true
    };
    let end = Instant::now() + Duration::from_secs(5);
    let mut stopping = false;
    let mut waited = false;
    let mut close = None;
    while c.status().phase != Phase::Stopped && Instant::now() < end {
        c.poll(&mut clock, PollBudget::default(), &mut permit, |d| {
            if let Delivery::Control(o) = d
                && o.token.operation() == Operation::UsdmClose
            {
                close = Some(o.state);
            }
            Ok(())
        })
        .unwrap();
        if c.status().phase == Phase::Receiving && !stopping {
            c.stop(StopMode::CloseRemoteStream).unwrap();
            stopping = true;
        }
        if accepted.load(Ordering::SeqCst) && !waited {
            while !replied.load(Ordering::SeqCst) && Instant::now() < end {
                thread::yield_now();
            }
            assert!(replied.load(Ordering::SeqCst));
            waited = true;
        }
        thread::yield_now();
    }
    assert_eq!(c.status().phase, Phase::Stopped);
    let uncertain = c.status().remote_close_unknown;
    drop(c);
    mh.join().unwrap();
    wh.join().unwrap();
    assert_eq!(close, Some(ControlState::MayHaveApplied));
    assert!(uncertain);
}
#[test]
fn expired_key_recreates_same_string_with_new_connection_identity() {
    let (tls, server) = certs();
    let m = listener();
    let w = listener();
    let ma = m.local_addr().unwrap();
    let wa = w.local_addr().unwrap();
    let mh = thread::spawn({
        let server = server.clone();
        move || {
            for method in ["POST", "PUT", "POST"] {
                let mut s = accept(&m, server.clone());
                assert!(headers(&mut s).starts_with(method));
                if method == "PUT" {
                    response(&mut s, 400, "{\"code\":-1125,\"msg\":\"expired\"}");
                } else {
                    response(&mut s, 200, "{\"listenKey\":\"SAME-PRIVATE-KEY\"}");
                }
            }
        }
    });
    let wh = thread::spawn(move || {
        for _ in 0..2 {
            let mut s = accept(&w, server.clone());
            upgrade(&mut s, "/private/ws/SAME-PRIVATE-KEY");
            frame(&mut s, 1, br#"{"e":"futureAccountEvent","E":10}"#);
            let mut b = [0];
            let _ = s.read(&mut b);
        }
    });
    let mut cfg = SessionConfig::new(scope(Market::Usdm));
    cfg.timing.renew_ns = 50_000_000;
    let mut c = PrivateClient::new(
        cfg,
        Authentication::UsdmApiKey { api_key: KEY },
        NetworkConfig {
            stream_address: wa,
            management_address: Some(ma),
            tls,
        },
        20,
    )
    .unwrap();
    let mut clock = TestClock {
        start: Instant::now(),
    };
    let mut permit = |_: &Charge, _: &ClockSample| true;
    let mut epochs = Vec::new();
    let mut rejected = false;
    let end = Instant::now() + Duration::from_secs(4);
    while epochs.len() < 2 && Instant::now() < end {
        c.poll(&mut clock, PollBudget::default(), &mut permit, |d| {
            match d {
                Delivery::Event(e) => epochs.push(e.session().identity().connection_epoch),
                Delivery::Control(o) if o.code == Some(-1125) => {
                    assert_eq!(o.state, ControlState::Rejected);
                    rejected = true;
                }
                _ => {}
            }
            Ok(())
        })
        .unwrap();
        thread::yield_now();
    }
    c.stop(StopMode::Disconnect).unwrap();
    drop(c);
    mh.join().unwrap();
    wh.join().unwrap();
    assert!(rejected);
    assert_eq!(epochs, [20, 21]);
}
