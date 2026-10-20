mod common;
use common::*;
use std::{
    io::{Read, Write},
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant},
};
use tickwire_binance_private::*;
fn spot(logon: bool, algorithm: u8, window: u64) {
    let (tls, server) = certs();
    let l = listener();
    let address = l.local_addr().unwrap();
    let h = thread::spawn(move || {
        let mut s = accept(&l, server);
        upgrade(&mut s, "/ws-api/v3");
        let first = json(&mut s);
        verify_signature(&first, algorithm);
        let sub = if logon {
            assert_eq!(first["method"], "session.logon");
            reply(
                &mut s,
                &first["id"],
                serde_json::json!({"apiKey":KEY,"authorizedSince":1,"connectedSince":1,"serverTime":1,"userDataStream":false}),
            );
            let sub = json(&mut s);
            assert_eq!(sub["method"], "userDataStream.subscribe");
            assert_eq!(sub["params"]["apiKey"], KEY);
            sub
        } else {
            assert_eq!(first["method"], "userDataStream.subscribe.signature");
            first
        };
        // An early event tests buffered delivery without inventing an authenticated context.
        frame(&mut s,1,br#"{"subscriptionId":0,"event":{"e":"balanceUpdate","E":10,"a":"USDT","d":"1","T":9}}"#);
        reply(&mut s, &sub["id"], serde_json::json!({"subscriptionId":0}));
        frame(&mut s, 9, b"ping-payload");
        let (op, pong) = read_frame(&mut s);
        assert_eq!(op, 10);
        assert_eq!(pong, b"ping-payload");
        let unsubscribe = json(&mut s);
        assert_eq!(unsubscribe["method"], "userDataStream.unsubscribe");
        assert_eq!(unsubscribe["params"]["subscriptionId"], 0);
        reply(&mut s, &unsubscribe["id"], serde_json::json!({}));
        if logon {
            let logout = json(&mut s);
            assert_eq!(logout["method"], "session.logout");
            reply(
                &mut s,
                &logout["id"],
                serde_json::json!({"apiKey":null,"userDataStream":false}),
            );
        }
        let mut b = [0];
        let _ = s.read(&mut b);
    });
    let signing = match algorithm {
        0 => SigningKey::hmac(SECRET).unwrap(),
        1 => SigningKey::ed25519_pkcs8(ED).unwrap(),
        2 => SigningKey::rsa_pkcs8(RSA).unwrap(),
        _ => unreachable!(),
    };
    let auth = if logon {
        Authentication::SpotLogon {
            api_key: KEY,
            signing_key: &signing,
        }
    } else {
        Authentication::SpotSignature {
            api_key: KEY,
            signing_key: &signing,
        }
    };
    let mut config = SessionConfig::new(scope(Market::Spot));
    config.timing.recv_window_micros = window;
    let mut client = PrivateClient::new(
        config,
        auth,
        NetworkConfig {
            stream_address: address,
            management_address: None,
            tls,
        },
        7,
    )
    .unwrap();
    let charges = Arc::new(Mutex::new(Vec::new()));
    let recorded = charges.clone();
    let mut permit = move |c: &Charge, _: &ClockSample| {
        recorded.lock().unwrap().push(c.kind);
        true
    };
    let mut clock = TestClock {
        start: Instant::now(),
    };
    let end = Instant::now() + Duration::from_secs(6);
    let mut events = 0;
    let mut stopping = false;
    loop {
        assert!(Instant::now() < end, "client={:?}", client.status());
        client
            .poll(&mut clock, PollBudget::default(), &mut permit, |d| {
                if let Delivery::Event(e) = d {
                    assert_eq!(e.session().identity().connection_epoch, 7);
                    events += 1;
                }
                Ok(())
            })
            .unwrap();
        if events == 1 && !stopping && charges.lock().unwrap().contains(&ChargeKind::Pong) {
            client.stop(StopMode::CloseRemoteStream).unwrap();
            stopping = true;
        }
        if client.status().phase == Phase::Stopped {
            break;
        }
        thread::yield_now();
    }
    assert_eq!(events, 1);
    assert!(client.status().subscription_id.is_none());
    let kinds = charges.lock().unwrap();
    assert!(kinds.contains(&ChargeKind::StreamConnect));
    assert!(kinds.contains(&ChargeKind::Upgrade));
    drop(kinds);
    drop(client);
    h.join().unwrap();
}
#[test]
fn spot_hmac_signature_subscription_over_actual_wss() {
    spot(false, 0, 5_000_000);
}
#[test]
fn spot_ed25519_logon_and_subscription_over_actual_wss() {
    spot(true, 1, 5_000_000);
}
#[test]
fn spot_rsa_signature_subscription_over_actual_wss() {
    spot(false, 2, 5_000_000);
}

#[test]
fn usdm_create_renew_order_event_and_terminal_close_use_real_https_wss() {
    let (tls, server) = certs();
    let m = listener();
    let s = listener();
    let ma = m.local_addr().unwrap();
    let sa = s.local_addr().unwrap();
    let management = thread::spawn({
        let server = server.clone();
        move || {
            for method in ["POST", "PUT", "DELETE"] {
                let mut c = accept(&m, server.clone());
                let h = headers(&mut c);
                assert!(h.starts_with(&format!("{method} /fapi/v1/listenKey HTTP/1.1\r\n")));
                assert!(h.contains(&format!("X-MBX-APIKEY: {KEY}")));
                let b = if method == "DELETE" {
                    "{}"
                } else {
                    "{\"listenKey\":\"TEST-LISTEN-KEY\"}"
                };
                write!(
                    c,
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{b}",
                    b.len()
                )
                .unwrap();
                c.flush().unwrap();
            }
        }
    });
    let stream = thread::spawn(move || {
        let mut c = accept(&s, server);
        upgrade(&mut c, "/private/ws/TEST-LISTEN-KEY");
        frame(&mut c,1,br#"{"e":"ORDER_TRADE_UPDATE","E":10,"T":9,"o":{"s":"BTCUSDT","c":"test-1","S":"BUY","o":"LIMIT","f":"GTC","q":"0.001","p":"60000","x":"TRADE","X":"FILLED","i":22,"l":"0.001","z":"0.001","L":"60000","n":"0.01","N":"USDT","T":9,"t":33,"m":true,"ps":"BOTH","rp":"0"}}"#);
        let mut b = [0];
        let _ = c.read(&mut b);
    });
    let mut config = SessionConfig::new(scope(Market::Usdm));
    config.timing.renew_ns = 100_000_000;
    let mut client = PrivateClient::new(
        config,
        Authentication::UsdmApiKey { api_key: KEY },
        NetworkConfig {
            stream_address: sa,
            management_address: Some(ma),
            tls,
        },
        9,
    )
    .unwrap();
    let mut clock = TestClock {
        start: Instant::now(),
    };
    let mut permit = |_: &Charge, _: &ClockSample| true;
    let mut events = 0;
    let mut renewed = false;
    let mut stopping = false;
    let end = Instant::now() + Duration::from_secs(6);
    loop {
        assert!(Instant::now() < end, "{:?}", client.status());
        client
            .poll(&mut clock, PollBudget::default(), &mut permit, |d| {
                match d {
                    Delivery::Event(e) => {
                        assert_eq!(e.session().identity().connection_epoch, 9);
                        events += 1;
                    }
                    Delivery::Control(o)
                        if o.token.operation() == Operation::UsdmKeepalive
                            && o.state == ControlState::Confirmed =>
                    {
                        renewed = true
                    }
                    _ => {}
                }
                Ok(())
            })
            .unwrap();
        if renewed && events == 1 && !stopping {
            client.stop(StopMode::CloseRemoteStream).unwrap();
            stopping = true;
        }
        if client.status().phase == Phase::Stopped {
            break;
        }
        thread::yield_now();
    }
    assert!(renewed);
    assert_eq!(events, 1);
    drop(client);
    management.join().unwrap();
    stream.join().unwrap();
}

#[test]
fn fractional_windows_with_trailing_zero_sign_actual_wss_numbers() {
    for window in [5_000_120, 5_000_100] {
        spot(false, 0, window);
    }
}
#[test]
fn spot_ed25519_signature_subscription_over_actual_wss() {
    spot(false, 1, 5_000_000);
}
