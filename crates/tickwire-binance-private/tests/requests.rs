mod common;
use common::{test_domain, test_sample};
use ring::hmac;
use tickwire_binance_private::{request::*, *};
fn scope(market: Market) -> AccountScope {
    AccountScope {
        risk_domain_id: [1; 16],
        venue: 1,
        environment: Environment::Test,
        account: 2,
        market,
        credential_generation: 3,
    }
}
fn clock() -> ClockSample {
    test_sample(100, 1_770_000_000_000)
}
#[test]
fn signature_subscription_hmac_signs_the_actual_parameters() {
    let key = SigningKey::hmac(b"TEST-SECRET").unwrap();
    let auth = Authentication::SpotSignature {
        api_key: "TEST-KEY",
        signing_key: &key,
    };
    let out = prepare(
        &scope(Market::Spot),
        &auth,
        RequestToken::new(4, 5, Operation::SpotSubscribeSignature).unwrap(),
        &clock(),
        5_000_123,
        MAX_REQUEST_BYTES,
    )
    .unwrap();
    let json: serde_json::Value = serde_json::from_slice(out.payload()).unwrap();
    assert_eq!(json["id"], "4-5");
    assert_eq!(json["method"], "userDataStream.subscribe.signature");
    assert_eq!(json["params"]["recvWindow"].to_string(), "5000.123");
    let raw = b"apiKey=TEST-KEY&recvWindow=5000.123&timestamp=1770000000000";
    let signature = hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, b"TEST-SECRET"), raw);
    let expected: String = signature
        .as_ref()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    assert_eq!(json["params"]["signature"], expected);
    assert_eq!(out.authority(), "ws-api.testnet.binance.vision");
    assert_eq!(out.path(), "/ws-api/v3");
    assert_ne!(out.digest(), [0; 32]);
}
#[test]
fn logon_uses_ed25519_and_explicit_key_subscription() {
    use ring::signature::KeyPair;
    let der = include_bytes!("../../tickwire-binance/tests/data/PUBLIC_BINANCE_ED25519.pk8.der");
    let key = SigningKey::ed25519_pkcs8(der).unwrap();
    let auth = Authentication::SpotLogon {
        api_key: "TEST-KEY",
        signing_key: &key,
    };
    let out = prepare(
        &scope(Market::Spot),
        &auth,
        RequestToken::new(1, 1, Operation::SpotLogon).unwrap(),
        &clock(),
        5_000_000,
        MAX_REQUEST_BYTES,
    )
    .unwrap();
    let json: serde_json::Value = serde_json::from_slice(out.payload()).unwrap();
    assert_eq!(json["method"], "session.logon");
    use base64::Engine;
    let sig = base64::engine::general_purpose::STANDARD
        .decode(json["params"]["signature"].as_str().unwrap())
        .unwrap();
    let pair = ring::signature::Ed25519KeyPair::from_pkcs8_maybe_unchecked(der).unwrap();
    ring::signature::UnparsedPublicKey::new(&ring::signature::ED25519, pair.public_key().as_ref())
        .verify(
            b"apiKey=TEST-KEY&recvWindow=5000&timestamp=1770000000000",
            &sig,
        )
        .unwrap();
    let sub = prepare(
        &scope(Market::Spot),
        &auth,
        RequestToken::new(1, 2, Operation::SpotSubscribe).unwrap(),
        &clock(),
        5_000_000,
        MAX_REQUEST_BYTES,
    )
    .unwrap();
    let j: serde_json::Value = serde_json::from_slice(sub.payload()).unwrap();
    assert_eq!(j["params"]["apiKey"], "TEST-KEY");
    assert!(j["params"].get("signature").is_none());
}
#[test]
fn usdm_has_api_key_only_and_no_invented_listen_key_parameter() {
    let auth = Authentication::UsdmApiKey {
        api_key: "TEST-KEY",
    };
    for (op, method) in [
        (Operation::UsdmCreate, Method::Post),
        (Operation::UsdmKeepalive, Method::Put),
        (Operation::UsdmClose, Method::Delete),
    ] {
        let p = prepare(
            &scope(Market::Usdm),
            &auth,
            RequestToken::new(1, 1, op).unwrap(),
            &clock(),
            5_000_000,
            MAX_REQUEST_BYTES,
        )
        .unwrap();
        assert_eq!(p.authority(), "demo-fapi.binance.com");
        assert_eq!(p.path(), "/fapi/v1/listenKey");
        assert_eq!(p.method(), method);
        assert_eq!(p.api_key_header(), Some("TEST-KEY"));
        assert!(p.payload().is_empty());
        assert!(!format!("{p:?}").contains("TEST-KEY"));
    }
}
#[test]
fn unsubscribe_binds_subscription_zero_and_queries_have_no_signature() {
    let key = SigningKey::hmac(b"secret").unwrap();
    let auth = Authentication::SpotSignature {
        api_key: "TEST-KEY",
        signing_key: &key,
    };
    for op in [
        Operation::SpotStatus,
        Operation::SpotSubscriptions,
        Operation::SpotUnsubscribe { subscription_id: 0 },
    ] {
        let p = prepare(
            &scope(Market::Spot),
            &auth,
            RequestToken::new(1, 2, op).unwrap(),
            &clock(),
            5_000_000,
            MAX_REQUEST_BYTES,
        )
        .unwrap();
        let j: serde_json::Value = serde_json::from_slice(p.payload()).unwrap();
        assert!(j["params"].get("signature").is_none());
        if matches!(op, Operation::SpotUnsubscribe { .. }) {
            assert_eq!(j["params"]["subscriptionId"], 0);
        }
    }
}
#[test]
fn queued_signature_expires_and_calibration_loss_is_not_a_fresh_clock() {
    let key = SigningKey::hmac(b"secret").unwrap();
    let auth = Authentication::SpotSignature {
        api_key: "TEST-KEY",
        signing_key: &key,
    };
    let c = clock();
    let p = prepare(
        &scope(Market::Spot),
        &auth,
        RequestToken::new(1, 1, Operation::SpotSubscribeSignature).unwrap(),
        &c,
        1000,
        MAX_REQUEST_BYTES,
    )
    .unwrap();
    assert!(p.fresh(&c));
    let mut later = c;
    later.monotonic = MonoTime::new(test_domain(), later.monotonic_ns() + 1_000_000);
    assert!(!p.fresh(&later));
    later = c;
    later.signing_time = None;
    assert!(!p.fresh(&later));
    later = c;
    later.signing_time = test_sample(100, 1_770_000_000_002).signing_time;
    assert!(!p.fresh(&later));
}
#[test]
fn wrong_product_key_algorithm_bounds_and_missing_clock_are_rejected() {
    let key = SigningKey::hmac(b"secret").unwrap();
    let auth = Authentication::SpotLogon {
        api_key: "TEST-KEY",
        signing_key: &key,
    };
    assert_eq!(
        prepare(
            &scope(Market::Spot),
            &auth,
            RequestToken::new(1, 1, Operation::SpotLogon).unwrap(),
            &clock(),
            5000,
            MAX_REQUEST_BYTES
        )
        .unwrap_err(),
        Error::Unsupported
    );
    let auth = Authentication::SpotSignature {
        api_key: "TEST-KEY",
        signing_key: &key,
    };
    assert_eq!(
        prepare(
            &scope(Market::Usdm),
            &auth,
            RequestToken::new(1, 1, Operation::SpotSubscribeSignature).unwrap(),
            &clock(),
            5000,
            MAX_REQUEST_BYTES
        )
        .unwrap_err(),
        Error::Identity
    );
    assert!(
        prepare(
            &scope(Market::Spot),
            &auth,
            RequestToken::new(1, 1, Operation::SpotSubscribeSignature).unwrap(),
            &clock(),
            60_000_001,
            MAX_REQUEST_BYTES
        )
        .is_err()
    );
    assert!(
        prepare(
            &scope(Market::Spot),
            &auth,
            RequestToken::new(1, 1, Operation::SpotSubscribeSignature).unwrap(),
            &clock(),
            5000,
            2
        )
        .is_err()
    );
    let mut c = clock();
    c.signing_time = None;
    assert_eq!(
        prepare(
            &scope(Market::Spot),
            &auth,
            RequestToken::new(1, 1, Operation::SpotSubscribeSignature).unwrap(),
            &c,
            5000,
            MAX_REQUEST_BYTES
        )
        .unwrap_err(),
        Error::Clock
    );
}
