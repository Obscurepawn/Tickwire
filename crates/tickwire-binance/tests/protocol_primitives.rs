use tickwire_binance::{
    Error,
    auth::{Endpoint, SigningKey, fix_logon_preimage},
    decimal::Decimal,
    params::{Params, Value, rest_preimage},
};

const SECRET: &[u8] = b"NhqPtmdSJYdKjVHjA7PZj4Mge3R5YNiP1e3UZjInClVN65XAbvqqM6A7H5fATj0j";
const API_KEY: &str = "vmPUZE6mv9SD5VNHk4HlWFsOr6aKE2zvsw0MuIgwCIPy6utIco14y7Ju91duEh8A";

fn order_params(symbol: &str) -> Params<'_, 12> {
    let mut p = Params::new();
    for (k, v) in [
        ("symbol", symbol),
        ("side", "BUY"),
        ("type", "LIMIT"),
        ("timeInForce", "GTC"),
        ("quantity", "1"),
        ("price", "0.1"),
    ] {
        p.push(k, Value::String(v)).unwrap();
    }
    p.push("recvWindow", Value::Integer(5000)).unwrap();
    p.push("timestamp", Value::Integer(1499827319559)).unwrap();
    p
}

#[test]
fn official_spot_rest_hmac_ascii_vector() {
    let mut payload = [0; 1024];
    let n = order_params("LTCBTC").rest(&mut payload).unwrap();
    assert_eq!(&payload[..n], b"symbol=LTCBTC&side=BUY&type=LIMIT&timeInForce=GTC&quantity=1&price=0.1&recvWindow=5000&timestamp=1499827319559");
    let mut sig = [0; 64];
    let len = SigningKey::hmac(SECRET)
        .unwrap()
        .sign(Endpoint::SpotRest, &payload[..n], &mut sig)
        .unwrap();
    assert_eq!(
        &sig[..len],
        b"c8db56825ae71d6d79447849e617115f4a920fa2acdcab2b053c4b2838bd6b71"
    );
}

#[test]
fn official_spot_rest_unicode_is_percent_encoded_before_signing() {
    let mut payload = [0; 1024];
    let n = order_params("１２３４５６").rest(&mut payload).unwrap();
    assert!(payload[..n].starts_with(b"symbol=%EF%BC%91%EF%BC%92%EF%BC%93"));
    let mut sig = [0; 64];
    let len = SigningKey::hmac(SECRET)
        .unwrap()
        .sign(Endpoint::SpotRest, &payload[..n], &mut sig)
        .unwrap();
    assert_eq!(
        &sig[..len],
        b"e1353ec6b14d888f1164ae9af8228a3dbd508bc82eb867db8ab6046442f33ef3"
    );
}

#[test]
fn official_spot_ws_vector_sorts_keys_without_changing_decimal_strings() {
    let mut p = Params::<12>::new();
    for (k, v) in [
        ("symbol", "BTCUSDT"),
        ("side", "SELL"),
        ("type", "LIMIT"),
        ("timeInForce", "GTC"),
        ("quantity", "0.01000000"),
        ("price", "52000.00"),
    ] {
        p.push(k, Value::String(v)).unwrap();
    }
    p.push("recvWindow", Value::Integer(100)).unwrap();
    p.push("timestamp", Value::Integer(1645423376532)).unwrap();
    p.push("apiKey", Value::String(API_KEY)).unwrap();
    let mut payload = [0; 1024];
    let n = p.websocket_preimage(&mut payload).unwrap();
    assert!(payload[..n].starts_with(b"apiKey="));
    let mut sig = [0; 64];
    let len = SigningKey::hmac(SECRET)
        .unwrap()
        .sign(Endpoint::SpotWebSocket, &payload[..n], &mut sig)
        .unwrap();
    assert_eq!(
        &sig[..len],
        b"aa1b5712c094bc4e57c05a1a5c1fd8d88dcd628338ea863fec7b88e59fe2db24"
    );
    let n = p.json(&mut payload).unwrap();
    let v: serde_json::Value = serde_json::from_slice(&payload[..n]).unwrap();
    assert_eq!(v["price"].as_str(), Some("52000.00"));
    assert_eq!(v["timestamp"].as_u64(), Some(1645423376532));
}

#[test]
fn websocket_uses_raw_utf8_while_rest_escapes_values_once() {
    let mut p = Params::<2>::new();
    p.push("symbol", Value::String("１２&+%=x")).unwrap();
    let mut b = [0; 128];
    let n = p.websocket_preimage(&mut b).unwrap();
    assert_eq!(&b[..n], "symbol=１２&+%=x".as_bytes());
    let n = p.rest(&mut b).unwrap();
    assert_eq!(&b[..n], b"symbol=%EF%BC%91%EF%BC%92%26%2B%25%3Dx");
}

#[test]
fn rest_preimage_does_not_insert_a_separator_between_query_and_body() {
    let mut out = [0; 64];
    let n = rest_preimage(b"symbol=BTCUSDT", b"side=BUY", &mut out).unwrap();
    assert_eq!(&out[..n], b"symbol=BTCUSDTside=BUY");
}

#[test]
fn official_fix_ed25519_vector_and_distinct_preimage() {
    let key =
        SigningKey::ed25519_pkcs8(include_bytes!("data/PUBLIC_BINANCE_ED25519.pk8.der")).unwrap();
    let mut preimage = [0; 128];
    let n =
        fix_logon_preimage("EXAMPLE", "SPOT", 1, "20240627-11:17:25.223", &mut preimage).unwrap();
    assert_eq!(
        &preimage[..n],
        b"A\x01EXAMPLE\x01SPOT\x011\x0120240627-11:17:25.223"
    );
    let mut signature = [0; 88];
    let n = key
        .sign(Endpoint::SpotFixLogon, &preimage[..n], &mut signature)
        .unwrap();
    assert_eq!(
        &signature[..n],
        b"4MHXelVVcpkdwuLbl6n73HQUXUf1dse2PCgT1DYqW9w8AVZ1RACFGM+5UdlGPrQHrgtS3CvsRURC1oj73j8gCA=="
    );
}

#[test]
fn rsa_pkcs1_sha256_matches_independent_openssl_vector() {
    let v: serde_json::Value = serde_json::from_str(include_str!("data/rsa-vector.json")).unwrap();
    let key = SigningKey::rsa_pkcs8(include_bytes!("data/TEST_ONLY_RSA.pk8.der")).unwrap();
    let mut signature = [0; 512];
    let n = key
        .sign(
            Endpoint::SpotRest,
            v["preimage"].as_str().unwrap().as_bytes(),
            &mut signature,
        )
        .unwrap();
    assert_eq!(
        &signature[..n],
        v["signature_base64"].as_str().unwrap().as_bytes()
    );
}

#[test]
fn endpoint_algorithm_matrix_rejects_unsupported_combination() {
    let key = SigningKey::hmac(SECRET).unwrap();
    let mut out = [0xA5; 512];
    for e in [
        Endpoint::SpotFixLogon,
        Endpoint::SpotSessionLogon,
        Endpoint::UsdmSessionLogon,
    ] {
        assert_eq!(
            key.sign(e, b"payload", &mut out),
            Err(Error::UnsupportedAlgorithm)
        );
    }
    let ed =
        SigningKey::ed25519_pkcs8(include_bytes!("data/PUBLIC_BINANCE_ED25519.pk8.der")).unwrap();
    assert_eq!(
        ed.sign(Endpoint::UsdmRest, b"payload", &mut out),
        Err(Error::UnsupportedAlgorithm)
    );
    assert_eq!(out, [0xA5; 512]);
}

#[test]
fn bad_keys_and_fix_field_injection_are_rejected() {
    assert!(matches!(SigningKey::hmac(b""), Err(Error::InvalidKey)));
    assert!(matches!(
        SigningKey::rsa_pkcs8(b"bad"),
        Err(Error::InvalidKey)
    ));
    assert!(matches!(
        SigningKey::ed25519_pkcs8(b"bad"),
        Err(Error::InvalidKey)
    ));
    let mut out = [0xA5; 128];
    assert_eq!(
        fix_logon_preimage("A\x01B", "SPOT", 1, "20240627-11:17:25.223", &mut out),
        Err(Error::InvalidParameter)
    );
    assert_eq!(out, [0xA5; 128]);
}

#[test]
fn parameter_limits_reject_duplicates_reserved_keys_and_capacity() {
    let mut p = Params::<1>::new();
    assert_eq!(
        p.push("signature", Value::String("injected")),
        Err(Error::InvalidParameter)
    );
    assert_eq!(
        p.push("x&other", Value::String("injected")),
        Err(Error::InvalidParameter)
    );
    p.push("symbol", Value::String("BTCUSDT")).unwrap();
    assert_eq!(
        p.push("symbol", Value::String("ETHUSDT")),
        Err(Error::DuplicateParameter)
    );
    assert_eq!(p.push("side", Value::String("BUY")), Err(Error::Capacity));
}

#[test]
fn output_capacity_failure_is_atomic_for_all_encoders() {
    let p = order_params("LTCBTC");
    let mut out = [0xA5; 4];
    assert_eq!(p.rest(&mut out), Err(Error::Capacity));
    assert_eq!(p.websocket_preimage(&mut out), Err(Error::Capacity));
    assert_eq!(p.json(&mut out), Err(Error::Capacity));
    assert_eq!(
        rest_preimage(b"long", b"body", &mut out),
        Err(Error::Capacity)
    );
    assert_eq!(
        SigningKey::hmac(SECRET)
            .unwrap()
            .sign(Endpoint::SpotRest, b"x", &mut out),
        Err(Error::Capacity)
    );
    assert_eq!(out, [0xA5; 4]);
}

#[test]
fn decimal_exact_conversion_and_i128_minimum() {
    for (input, atoms, scale) in [
        ("0.01000000", 1000000, 8),
        ("-12.340", -12340, 3),
        ("-170141183460469231731687303715884105728", i128::MIN, 0),
    ] {
        let d = Decimal::parse(input).unwrap();
        assert_eq!((d.mantissa(), d.scale()), (atoms, scale));
        let mut out = [0; 80];
        let n = d.write(&mut out).unwrap();
        assert_eq!(&out[..n], input.as_bytes());
    }
}

#[test]
fn decimal_invalid_precision_and_rescale_fail_without_rounding() {
    for text in [
        "",
        "1e-8",
        "NaN",
        "+1",
        " 1",
        "1.",
        ".1",
        "1.2.3",
        "170141183460469231731687303715884105728",
    ] {
        assert!(Decimal::parse(text).is_err(), "{text}");
    }
    assert_eq!(
        Decimal::parse("1.001").unwrap().rescale_exact(2),
        Err(Error::InvalidDecimal)
    );
    assert_eq!(
        Decimal::parse("1.00").unwrap().rescale_exact(0),
        Decimal::new(1, 0)
    );
    assert_eq!(
        Decimal::new(i128::MAX, 0).unwrap().rescale_exact(1),
        Err(Error::Overflow)
    );
    assert_eq!(Decimal::new(1, 39), Err(Error::InvalidDecimal));
}

#[test]
fn json_numbers_reject_non_json_leading_zero_spellings_without_mutating_params() {
    use tickwire_binance::params::{Params, Value};
    for value in ["01", "-01", "00.1", "-00.1"] {
        let mut p = Params::<2>::new();
        assert!(
            p.push("recvWindow", Value::Number(value)).is_err(),
            "{value}"
        );
        let mut out = [0; 32];
        let n = p.json(&mut out).unwrap();
        assert_eq!(&out[..n], b"{}");
    }
}

#[test]
fn fix_sender_comp_id_rejects_printable_but_unlisted_punctuation_atomically() {
    use tickwire_binance::auth::fix_logon_preimage;
    for sender in ["BAD!", "A=B", "A:B"] {
        let mut out = [0x7a; 128];
        assert!(
            fix_logon_preimage(sender, "SPOT", 1, "20261020-12:34:56.123", &mut out).is_err(),
            "{sender}"
        );
        assert_eq!(out, [0x7a; 128]);
    }
    let mut out = [0; 128];
    assert!(fix_logon_preimage("aZ09-_", "SPOT", 1, "20261020-12:34:56.123", &mut out).is_ok());
}
