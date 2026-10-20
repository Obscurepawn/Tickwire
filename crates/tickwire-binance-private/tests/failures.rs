mod common;
use common::*;
use std::{
    io::Read,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant},
};
use tickwire_binance_private::*;

fn client<'a>(
    tls: Arc<rustls::ClientConfig>,
    address: std::net::SocketAddr,
    key: &'a SigningKey,
    config: SessionConfig,
) -> PrivateClient<'a> {
    PrivateClient::new(
        config,
        Authentication::SpotSignature {
            api_key: KEY,
            signing_key: key,
        },
        NetworkConfig {
            stream_address: address,
            management_address: None,
            tls,
        },
        3,
    )
    .unwrap()
}
fn event(subscription: u64) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({"subscriptionId":subscription,"event":{"e":"balanceUpdate","E":10,"a":"USDT","d":"1","T":9}})).unwrap()
}
#[test]
fn one_message_budget_eventually_delivers_buffered_events_without_more_network_input() {
    let (tls, server) = certs();
    let l = listener();
    let addr = l.local_addr().unwrap();
    let h = thread::spawn(move || {
        let mut s = accept(&l, server);
        upgrade(&mut s, "/ws-api/v3");
        let q = json(&mut s);
        reply(&mut s, &q["id"], serde_json::json!({"subscriptionId":0}));
        for _ in 0..8 {
            frame(&mut s, 1, &event(0));
        }
        let mut b = [0];
        let _ = s.read(&mut b);
    });
    let key = SigningKey::hmac(SECRET).unwrap();
    let mut cfg = SessionConfig::new(scope(Market::Spot));
    cfg.limits.early_messages = 2;
    let mut c = client(tls, addr, &key, cfg);
    let mut clock = TestClock {
        start: Instant::now(),
    };
    let mut permit = |_: &Charge, _: &ClockSample| true;
    let end = Instant::now() + Duration::from_secs(3);
    let mut count = 0;
    while count < 8 && Instant::now() < end {
        c.poll(
            &mut clock,
            PollBudget {
                max_messages: 1,
                ..PollBudget::default()
            },
            &mut permit,
            |d| {
                if let Delivery::Event(_) = d {
                    count += 1;
                }
                Ok(())
            },
        )
        .unwrap();
        thread::yield_now();
    }
    c.stop(StopMode::Disconnect).unwrap();
    drop(c);
    h.join().unwrap();
    assert_eq!(count, 8);
}
#[test]
fn duplicate_subscription_identity_fields_fail_closed() {
    let (tls, server) = certs();
    let l = listener();
    let addr = l.local_addr().unwrap();
    let h = thread::spawn(move || {
        let mut s = accept(&l, server);
        upgrade(&mut s, "/ws-api/v3");
        let q = json(&mut s);
        frame(&mut s,1,format!("{{\"id\":{},\"status\":200,\"result\":{{\"subscriptionId\":999,\"subscriptionId\":0}}}}",q["id"]).as_bytes());
        let mut b = [0];
        let _ = s.read(&mut b);
    });
    let key = SigningKey::hmac(SECRET).unwrap();
    let mut c = client(tls, addr, &key, SessionConfig::new(scope(Market::Spot)));
    let mut clock = TestClock {
        start: Instant::now(),
    };
    let mut permit = |_: &Charge, _: &ClockSample| true;
    let end = Instant::now() + Duration::from_secs(2);
    let mut receiving = false;
    let mut gap = false;
    while !gap && !receiving && Instant::now() < end {
        c.poll(&mut clock, PollBudget::default(), &mut permit, |d| {
            if let Delivery::Gap(_) = d {
                gap = true;
            }
            Ok(())
        })
        .unwrap();
        receiving = c.status().phase == Phase::Receiving;
        thread::yield_now();
    }
    c.stop(StopMode::Disconnect).unwrap();
    drop(c);
    h.join().unwrap();
    assert!(!receiving);
    assert!(gap);
}
#[test]
fn listen_key_is_redacted_from_delivered_event_debug() {
    // Unknown events may contain credentials too: Debug must not print raw JSON.
    let (tls, server) = certs();
    let l = listener();
    let addr = l.local_addr().unwrap();
    let h = thread::spawn(move || {
        let mut s = accept(&l, server);
        upgrade(&mut s, "/ws-api/v3");
        let q = json(&mut s);
        reply(&mut s, &q["id"], serde_json::json!({"subscriptionId":0}));
        frame(&mut s,1,br#"{"subscriptionId":0,"event":{"e":"futureEvent","E":10,"listenKey":"SECRET-LISTEN-KEY"}}"#);
        let mut b = [0];
        let _ = s.read(&mut b);
    });
    let key = SigningKey::hmac(SECRET).unwrap();
    let mut c = client(tls, addr, &key, SessionConfig::new(scope(Market::Spot)));
    let mut clock = TestClock {
        start: Instant::now(),
    };
    let mut permit = |_: &Charge, _: &ClockSample| true;
    let end = Instant::now() + Duration::from_secs(3);
    let mut debug = None;
    while debug.is_none() && Instant::now() < end {
        c.poll(&mut clock, PollBudget::default(), &mut permit, |d| {
            if let Delivery::Event(e) = d {
                debug = Some(format!("{e:?}"));
            }
            Ok(())
        })
        .unwrap();
        thread::yield_now();
    }
    c.stop(StopMode::Disconnect).unwrap();
    drop(c);
    h.join().unwrap();
    assert!(!debug.unwrap().contains("SECRET-LISTEN-KEY"));
}
struct JumpClock {
    start: Instant,
    jump: Arc<AtomicU64>,
}
impl Clock for JumpClock {
    fn check_signed(
        &mut self,
        s: &AccountScope,
        t: &SignedTime,
        n: &ClockSample,
        w: u64,
    ) -> Result<()> {
        check_test_stamp(s, t, n, w)
    }
    fn sample(&mut self) -> Result<ClockSample> {
        let n = self.start.elapsed().as_nanos() as u64 + self.jump.load(Ordering::SeqCst);
        Ok(test_sample(n, 1_770_000_000_000 + n / 1_000_000))
    }
}
#[test]
fn signature_expiring_inside_permission_callback_has_no_application_bytes() {
    let (tls, server) = certs();
    let l = listener();
    let addr = l.local_addr().unwrap();
    let h = thread::spawn(move || {
        let mut s = accept(&l, server);
        upgrade(&mut s, "/ws-api/v3");
        let mut b = [0];
        let n = s.read(&mut b).unwrap_or(0);
        assert_eq!(n, 0);
    });
    let key = SigningKey::hmac(SECRET).unwrap();
    let mut c = client(tls, addr, &key, SessionConfig::new(scope(Market::Spot)));
    let jump = Arc::new(AtomicU64::new(0));
    let mut clock = JumpClock {
        start: Instant::now(),
        jump: jump.clone(),
    };
    let mut permit = |q: &Charge, _: &ClockSample| {
        if matches!(q.kind, ChargeKind::Api(_)) {
            jump.store(10_000_000_000, Ordering::SeqCst);
        }
        true
    };
    let end = Instant::now() + Duration::from_secs(3);
    let mut outcome = None;
    while outcome.is_none() && Instant::now() < end {
        c.poll(&mut clock, PollBudget::default(), &mut permit, |d| {
            if let Delivery::Control(o) = d {
                outcome = Some(o.state);
            }
            Ok(())
        })
        .unwrap();
        thread::yield_now();
    }
    c.stop(StopMode::Disconnect).unwrap();
    drop(c);
    h.join().unwrap();
    assert_eq!(outcome, Some(ControlState::NoBytes));
}
#[test]
fn accepted_request_timeout_is_unknown_and_old_reply_cannot_activate_new_epoch() {
    let (tls, server) = certs();
    let l = listener();
    let addr = l.local_addr().unwrap();
    let h = thread::spawn(move || {
        let old = {
            let mut s = accept(&l, server.clone());
            upgrade(&mut s, "/ws-api/v3");
            json(&mut s)["id"].clone()
        };
        let mut s = accept(&l, server);
        upgrade(&mut s, "/ws-api/v3");
        let q = json(&mut s);
        assert_ne!(q["id"], old);
        reply(&mut s, &old, serde_json::json!({"subscriptionId":99}));
        frame(&mut s, 1, &event(0));
        reply(&mut s, &q["id"], serde_json::json!({"subscriptionId":0}));
        let mut b = [0];
        let _ = s.read(&mut b);
    });
    let key = SigningKey::hmac(SECRET).unwrap();
    let mut c = client(tls, addr, &key, SessionConfig::new(scope(Market::Spot)));
    let mut clock = TestClock {
        start: Instant::now(),
    };
    let mut permit = |_: &Charge, _: &ClockSample| true;
    let end = Instant::now() + Duration::from_secs(3);
    let mut unknown = false;
    let mut epoch = None;
    while epoch.is_none() && Instant::now() < end {
        c.poll(&mut clock, PollBudget::default(), &mut permit, |d| {
            match d {
                Delivery::Control(o) => {
                    assert_ne!(o.state, ControlState::NoBytes);
                    unknown |= o.state == ControlState::MayHaveApplied;
                }
                Delivery::Event(e) => epoch = Some(e.session().identity().connection_epoch),
                _ => {}
            }
            Ok(())
        })
        .unwrap();
        thread::yield_now();
    }
    c.stop(StopMode::Disconnect).unwrap();
    drop(c);
    h.join().unwrap();
    assert!(unknown);
    assert_eq!(epoch, Some(4));
}
#[test]
fn early_event_overflow_and_sink_failure_both_remove_receiving_qualification() {
    for overflow in [true, false] {
        let (tls, server) = certs();
        let l = listener();
        let addr = l.local_addr().unwrap();
        let h = thread::spawn(move || {
            let mut s = accept(&l, server);
            upgrade(&mut s, "/ws-api/v3");
            let q = json(&mut s);
            if overflow {
                frame(&mut s, 1, &event(0));
                frame(&mut s, 1, &event(0));
            } else {
                reply(&mut s, &q["id"], serde_json::json!({"subscriptionId":0}));
                frame(&mut s, 1, &event(0));
            }
            let mut b = [0];
            let _ = s.read(&mut b);
        });
        let key = SigningKey::hmac(SECRET).unwrap();
        let mut cfg = SessionConfig::new(scope(Market::Spot));
        cfg.limits.early_messages = 1;
        let mut c = client(tls, addr, &key, cfg);
        let mut clock = TestClock {
            start: Instant::now(),
        };
        let mut permit = |_: &Charge, _: &ClockSample| true;
        let end = Instant::now() + Duration::from_secs(3);
        let mut gap = false;
        let mut sink_error = false;
        while !gap && !sink_error && Instant::now() < end {
            let r = c.poll(&mut clock, PollBudget::default(), &mut permit, |d| {
                match d {
                    Delivery::Gap(_) => gap = true,
                    Delivery::Event(_) => return Err(Error::Sink),
                    _ => {}
                }
                Ok(())
            });
            sink_error = matches!(r, Err(Error::Sink));
            if !sink_error {
                r.unwrap();
            }
            thread::yield_now();
        }
        assert_ne!(c.status().phase, Phase::Receiving);
        assert!(if overflow { gap } else { sink_error });
        c.stop(StopMode::Disconnect).unwrap();
        drop(c);
        h.join().unwrap();
    }
}
#[test]
fn upgrade_permission_denial_emits_no_http_request() {
    let (tls, server) = certs();
    let l = listener();
    let addr = l.local_addr().unwrap();
    let h = thread::spawn(move || {
        let mut s = accept(&l, server);
        let mut b = [0];
        assert_eq!(s.read(&mut b).unwrap_or(0), 0);
    });
    let key = SigningKey::hmac(SECRET).unwrap();
    let mut c = client(tls, addr, &key, SessionConfig::new(scope(Market::Spot)));
    let mut clock = TestClock {
        start: Instant::now(),
    };
    let mut permit = |q: &Charge, _: &ClockSample| q.kind != ChargeKind::Upgrade;
    let end = Instant::now() + Duration::from_secs(3);
    let mut gap = false;
    while !gap && Instant::now() < end {
        c.poll(&mut clock, PollBudget::default(), &mut permit, |d| {
            if let Delivery::Gap(_) = d {
                gap = true;
            }
            Ok(())
        })
        .unwrap();
        thread::yield_now();
    }
    c.stop(StopMode::Disconnect).unwrap();
    drop(c);
    h.join().unwrap();
    assert!(gap);
}
#[test]
fn server_timeout_error_after_acceptance_is_not_a_definite_rejection() {
    let (tls, server) = certs();
    let l = listener();
    let addr = l.local_addr().unwrap();
    let h = thread::spawn(move || {
        let mut s = accept(&l, server);
        upgrade(&mut s, "/ws-api/v3");
        let q = json(&mut s);
        frame(&mut s,1,&serde_json::to_vec(&serde_json::json!({"id":q["id"],"status":500,"error":{"code":-1007,"msg":"Timeout"}})).unwrap());
        let mut b = [0];
        let _ = s.read(&mut b);
    });
    let key = SigningKey::hmac(SECRET).unwrap();
    let mut c = client(tls, addr, &key, SessionConfig::new(scope(Market::Spot)));
    let mut clock = TestClock {
        start: Instant::now(),
    };
    let mut permit = |_: &Charge, _: &ClockSample| true;
    let end = Instant::now() + Duration::from_secs(3);
    let mut outcome = None;
    while outcome.is_none() && Instant::now() < end {
        c.poll(&mut clock, PollBudget::default(), &mut permit, |d| {
            if let Delivery::Control(o) = d {
                outcome = Some(o);
            }
            Ok(())
        })
        .unwrap();
        thread::yield_now();
    }
    c.stop(StopMode::Disconnect).unwrap();
    drop(c);
    h.join().unwrap();
    let o = outcome.unwrap();
    assert_eq!(o.status, Some(500));
    assert_eq!(o.state, ControlState::MayHaveApplied);
}
#[test]
fn rate_limit_cooldown_blocks_reconnect_attempts() {
    let (tls, server) = certs();
    let l = listener();
    let addr = l.local_addr().unwrap();
    let h = thread::spawn(move || {
        let mut s = accept(&l, server);
        upgrade(&mut s, "/ws-api/v3");
        let q = json(&mut s);
        frame(&mut s,1,&serde_json::to_vec(&serde_json::json!({"id":q["id"],"status":429,"error":{"code":-1003,"msg":"rate limited","data":{"retryAfter":1_770_000_060_000u64}}})).unwrap());
        let mut b = [0];
        let _ = s.read(&mut b);
    });
    let key = SigningKey::hmac(SECRET).unwrap();
    let mut c = client(tls, addr, &key, SessionConfig::new(scope(Market::Spot)));
    let jump = Arc::new(AtomicU64::new(0));
    let mut clock = JumpClock {
        start: Instant::now(),
        jump: jump.clone(),
    };
    let mut connections = 0;
    let mut permit = |q: &Charge, _: &ClockSample| {
        if q.kind == ChargeKind::StreamConnect {
            connections += 1;
        }
        true
    };
    let end = Instant::now() + Duration::from_secs(3);
    let mut gap = false;
    while !gap && Instant::now() < end {
        c.poll(&mut clock, PollBudget::default(), &mut permit, |d| {
            if let Delivery::Gap(_) = d {
                gap = true;
            }
            Ok(())
        })
        .unwrap();
        thread::yield_now();
    }
    jump.store(5_000_000_000, Ordering::SeqCst);
    c.poll(&mut clock, PollBudget::default(), &mut permit, |_| Ok(()))
        .unwrap();
    c.stop(StopMode::Disconnect).unwrap();
    drop(c);
    h.join().unwrap();
    assert!(gap);
    assert_eq!(connections, 1);
}
#[test]
fn complete_events_buffered_before_eof_are_drained_with_bounded_work() {
    let (tls, server) = certs();
    let l = listener();
    let addr = l.local_addr().unwrap();
    let h = thread::spawn(move || {
        let mut s = accept(&l, server);
        upgrade(&mut s, "/ws-api/v3");
        let q = json(&mut s);
        reply(&mut s, &q["id"], serde_json::json!({"subscriptionId":0}));
        // Client status request proves it observed the subscription before EOF.
        let query = json(&mut s);
        assert_eq!(query["method"], "session.status");
        reply(
            &mut s,
            &query["id"],
            serde_json::json!({"apiKey":null,"userDataStream":true}),
        );
        let query = json(&mut s);
        reply(
            &mut s,
            &query["id"],
            serde_json::json!([{"subscriptionId":0}]),
        );
        for _ in 0..8 {
            frame(&mut s, 1, &event(0));
        }
    });
    let key = SigningKey::hmac(SECRET).unwrap();
    let mut c = client(tls, addr, &key, SessionConfig::new(scope(Market::Spot)));
    let mut clock = TestClock {
        start: Instant::now(),
    };
    let mut permit = |_: &Charge, _: &ClockSample| true;
    let end = Instant::now() + Duration::from_secs(3);
    let mut count = 0;
    let mut queried = false;
    let mut gap = false;
    while count < 8 && Instant::now() < end {
        c.poll(
            &mut clock,
            PollBudget {
                max_messages: 1,
                ..PollBudget::default()
            },
            &mut permit,
            |d| {
                match d {
                    Delivery::Event(_) => count += 1,
                    Delivery::Gap(_) => gap = true,
                    _ => {}
                }
                Ok(())
            },
        )
        .unwrap();
        if !queried && c.status().phase == Phase::Receiving {
            c.request_status().unwrap();
            queried = true;
        }
        thread::yield_now();
    }
    c.stop(StopMode::Disconnect).unwrap();
    drop(c);
    h.join().unwrap();
    assert_eq!(count, 8);
    assert!(gap);
}
#[test]
fn server_close_is_echoed_through_control_permission() {
    let (tls, server) = certs();
    let l = listener();
    let addr = l.local_addr().unwrap();
    let h = thread::spawn(move || {
        let mut s = accept(&l, server);
        upgrade(&mut s, "/ws-api/v3");
        let q = json(&mut s);
        reply(&mut s, &q["id"], serde_json::json!({"subscriptionId":0}));
        frame(&mut s, 8, &1000u16.to_be_bytes());
        let (op, body) = read_frame(&mut s);
        assert_eq!(op, 8);
        assert_eq!(body, 1000u16.to_be_bytes());
    });
    let key = SigningKey::hmac(SECRET).unwrap();
    let mut c = client(tls, addr, &key, SessionConfig::new(scope(Market::Spot)));
    let mut clock = TestClock {
        start: Instant::now(),
    };
    let mut close = false;
    let mut permit = |q: &Charge, _: &ClockSample| {
        close |= q.kind == ChargeKind::Close;
        true
    };
    let end = Instant::now() + Duration::from_secs(2);
    while !h.is_finished() && Instant::now() < end {
        c.poll(&mut clock, PollBudget::default(), &mut permit, |_| Ok(()))
            .unwrap();
        thread::yield_now();
    }
    c.stop(StopMode::Disconnect).unwrap();
    drop(c);
    h.join().unwrap();
    assert!(close);
}
#[test]
fn invalid_transport_limits_and_early_data_are_rejected_before_connect() {
    let (tls, _) = certs();
    let addr = "127.0.0.1:1".parse().unwrap();
    let key = SigningKey::hmac(SECRET).unwrap();
    for zero in [true, false] {
        let mut cfg = SessionConfig::new(scope(Market::Spot));
        cfg.limits.io.max_accept_bytes = if zero { 0 } else { (16 << 20) + 1 };
        assert!(
            PrivateClient::new(
                cfg,
                Authentication::SpotSignature {
                    api_key: KEY,
                    signing_key: &key
                },
                NetworkConfig {
                    stream_address: addr,
                    management_address: None,
                    tls: tls.clone()
                },
                1
            )
            .is_err()
        );
    }
    let mut invalid = (*tls).clone();
    invalid.enable_early_data = true;
    assert!(
        PrivateClient::new(
            SessionConfig::new(scope(Market::Spot)),
            Authentication::SpotSignature {
                api_key: KEY,
                signing_key: &key
            },
            NetworkConfig {
                stream_address: addr,
                management_address: None,
                tls: Arc::new(invalid)
            },
            1
        )
        .is_err()
    );
}
#[test]
fn subscription_ack_and_eof_in_one_read_never_publish_healthy_state() {
    let (tls, server) = certs();
    let l = listener();
    let addr = l.local_addr().unwrap();
    let finished = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let sent = finished.clone();
    let h = thread::spawn(move || {
        let mut s = accept(&l, server);
        upgrade(&mut s, "/ws-api/v3");
        let q = json(&mut s);
        reply(&mut s, &q["id"], serde_json::json!({"subscriptionId":0}));
        frame(&mut s, 1, &event(0));
        drop(s);
        sent.store(true, Ordering::SeqCst);
    });
    let key = SigningKey::hmac(SECRET).unwrap();
    let mut c = client(tls, addr, &key, SessionConfig::new(scope(Market::Spot)));
    let mut clock = TestClock {
        start: Instant::now(),
    };
    let accepted = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let seen = accepted.clone();
    let mut permit = move |q: &Charge, _: &ClockSample| {
        if matches!(q.kind, ChargeKind::Api(_)) {
            seen.store(true, Ordering::SeqCst);
        }
        true
    };
    let end = Instant::now() + Duration::from_secs(3);
    let mut waited = false;
    let mut healthy = false;
    let mut gap = false;
    let mut delivered = false;
    while !delivered && Instant::now() < end {
        c.poll(
            &mut clock,
            PollBudget {
                max_messages: 1,
                ..PollBudget::default()
            },
            &mut permit,
            |d| {
                match d {
                    Delivery::State(s) if s.phase == Phase::Receiving => healthy = true,
                    Delivery::Gap(_) => gap = true,
                    Delivery::Event(_) => {
                        assert!(gap);
                        delivered = true;
                    }
                    _ => {}
                }
                Ok(())
            },
        )
        .unwrap();
        if accepted.load(Ordering::SeqCst) && !waited {
            while !finished.load(Ordering::SeqCst) && Instant::now() < end {
                thread::yield_now();
            }
            assert!(finished.load(Ordering::SeqCst));
            waited = true;
        }
        thread::yield_now();
    }
    c.stop(StopMode::Disconnect).unwrap();
    drop(c);
    h.join().unwrap();
    assert!(delivered);
    assert!(!healthy);
}
