mod common;
use common::*;
use std::{
    io::{Read, Write},
    thread,
    time::{Duration, Instant},
};
use tickwire_binance_private::*;
fn make<'a>(
    tls: std::sync::Arc<rustls::ClientConfig>,
    addr: std::net::SocketAddr,
    key: &'a SigningKey,
) -> PrivateClient<'a> {
    PrivateClient::new(
        SessionConfig::new(scope(Market::Spot)),
        Authentication::SpotSignature {
            api_key: KEY,
            signing_key: key,
        },
        NetworkConfig {
            stream_address: addr,
            management_address: None,
            tls,
        },
        11,
    )
    .unwrap()
}
#[test]
fn termination_shutdown_revocation_and_wrong_subscription_all_revoke_health() {
    for scenario in 0..4 {
        let (tls, server) = certs();
        let l = listener();
        let addr = l.local_addr().unwrap();
        let h = thread::spawn(move || {
            let mut s = accept(&l, server);
            upgrade(&mut s, "/ws-api/v3");
            let q = json(&mut s);
            reply(&mut s, &q["id"], serde_json::json!({"subscriptionId":0}));
            let message=match scenario{0=>br#"{"subscriptionId":0,"event":{"e":"eventStreamTerminated","E":10}}"#.as_slice(),1=>br#"{"event":{"e":"serverShutdown","E":10}}"#,2=>br#"{"id":null,"status":401,"error":{"code":-2015,"msg":"revoked"}}"#,_=>br#"{"subscriptionId":99,"event":{"e":"balanceUpdate","E":10,"a":"USDT","d":"1","T":9}}"#};
            frame(&mut s, 1, message);
            let mut b = [0];
            let _ = s.read(&mut b);
        });
        let key = SigningKey::hmac(SECRET).unwrap();
        let mut c = make(tls, addr, &key);
        let mut clock = TestClock {
            start: Instant::now(),
        };
        let mut permit = |_: &Charge, _: &ClockSample| true;
        let end = Instant::now() + Duration::from_secs(3);
        let mut gap = false;
        let mut events = 0;
        while !gap && Instant::now() < end {
            c.poll(&mut clock, PollBudget::default(), &mut permit, |d| {
                match d {
                    Delivery::Gap(_) => gap = true,
                    Delivery::Event(_) => {
                        assert!(gap, "termination must revoke before delivery");
                        events += 1;
                    }
                    _ => {}
                }
                Ok(())
            })
            .unwrap();
            thread::yield_now();
        }
        assert!(gap);
        assert_ne!(c.status().phase, Phase::Receiving);
        assert_eq!(events, usize::from(scenario == 0));
        c.stop(StopMode::Disconnect).unwrap();
        drop(c);
        h.join().unwrap();
    }
}
#[test]
fn authenticated_logon_response_must_echo_the_expected_key() {
    let (tls, server) = certs();
    let l = listener();
    let addr = l.local_addr().unwrap();
    let h = thread::spawn(move || {
        let mut s = accept(&l, server);
        upgrade(&mut s, "/ws-api/v3");
        let q = json(&mut s);
        reply(
            &mut s,
            &q["id"],
            serde_json::json!({"apiKey":"ANOTHER-ACCOUNT-KEY","userDataStream":false}),
        );
        let mut b = [0];
        assert_eq!(s.read(&mut b).unwrap_or(0), 0);
    });
    let key = SigningKey::ed25519_pkcs8(ED).unwrap();
    let mut c = PrivateClient::new(
        SessionConfig::new(scope(Market::Spot)),
        Authentication::SpotLogon {
            api_key: KEY,
            signing_key: &key,
        },
        NetworkConfig {
            stream_address: addr,
            management_address: None,
            tls,
        },
        11,
    )
    .unwrap();
    let mut clock = TestClock {
        start: Instant::now(),
    };
    let mut permit = |_: &Charge, _: &ClockSample| true;
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
    assert!(gap);
    c.stop(StopMode::Disconnect).unwrap();
    drop(c);
    h.join().unwrap();
}
#[test]
fn more_than_32_fragments_resume_without_waiting_for_new_input() {
    let (tls, server) = certs();
    let l = listener();
    let addr = l.local_addr().unwrap();
    let h = thread::spawn(move || {
        let mut s = accept(&l, server);
        upgrade(&mut s, "/ws-api/v3");
        let q = json(&mut s);
        reply(&mut s, &q["id"], serde_json::json!({"subscriptionId":0}));
        let b=br#"{"subscriptionId":0,"event":{"e":"balanceUpdate","E":10,"a":"USDT","d":"1","T":9}}"#;
        let mut batch = vec![1, 1, b[0]];
        for _ in 0..32 {
            batch.extend_from_slice(&[0, 0]);
        }
        batch.extend_from_slice(&[0x80, (b.len() - 1) as u8]);
        batch.extend_from_slice(&b[1..]);
        s.write_all(&batch).unwrap();
        s.flush().unwrap();
        let mut b = [0];
        let _ = s.read(&mut b);
    });
    let key = SigningKey::hmac(SECRET).unwrap();
    let mut c = make(tls, addr, &key);
    let mut clock = TestClock {
        start: Instant::now(),
    };
    let mut permit = |_: &Charge, _: &ClockSample| true;
    let end = Instant::now() + Duration::from_secs(3);
    let mut events = 0;
    let mut yielded = false;
    while events == 0 && Instant::now() < end {
        let r = c
            .poll(&mut clock, PollBudget::default(), &mut permit, |d| {
                if let Delivery::Event(_) = d {
                    events += 1;
                }
                Ok(())
            })
            .unwrap();
        yielded |= r.yielded;
        thread::yield_now();
    }
    assert!(yielded);
    assert_eq!(events, 1);
    c.stop(StopMode::Disconnect).unwrap();
    drop(c);
    h.join().unwrap();
}
#[test]
fn mio_registration_drives_the_real_connection() {
    let (tls, server) = certs();
    let l = listener();
    let addr = l.local_addr().unwrap();
    let h = thread::spawn(move || {
        let mut s = accept(&l, server);
        upgrade(&mut s, "/ws-api/v3");
        let q = json(&mut s);
        reply(&mut s, &q["id"], serde_json::json!({"subscriptionId":0}));
        let mut b = [0];
        let _ = s.read(&mut b);
    });
    let key = SigningKey::hmac(SECRET).unwrap();
    let mut c = make(tls, addr, &key);
    let mut clock = TestClock {
        start: Instant::now(),
    };
    let mut permit = |_: &Charge, _: &ClockSample| true;
    let mut reactor = mio::Poll::new().unwrap();
    let mut events = mio::Events::with_capacity(4);
    let tokens = RegistrationTokens {
        stream: mio::Token(1),
        management: mio::Token(2),
    };
    c.register(reactor.registry(), tokens).unwrap();
    let end = Instant::now() + Duration::from_secs(3);
    while c.status().phase != Phase::Receiving && Instant::now() < end {
        c.poll(&mut clock, PollBudget::default(), &mut permit, |_| Ok(()))
            .unwrap();
        c.reregister(reactor.registry(), tokens).unwrap();
        reactor
            .poll(&mut events, Some(Duration::from_millis(10)))
            .unwrap();
    }
    assert_eq!(c.status().phase, Phase::Receiving);
    c.stop(StopMode::Disconnect).unwrap();
    drop(c);
    h.join().unwrap();
}
