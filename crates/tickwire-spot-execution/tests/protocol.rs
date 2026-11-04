#[path = "../../tickwire-venue/tests/common/mod.rs"]
mod fixture;
use tickwire_gateway::Encoder;
use tickwire_spot_execution::{Credentials, SpotEncoder, SpotVenue};
use tickwire_venue::{CanonicalCommand, CommandBody, Environment, Symbol};

fn command(venue: SpotVenue) -> CanonicalCommand {
    let mut cmd = fixture::command();
    cmd.context.request.venue = venue.id();
    cmd.context.instrument.symbol = Symbol::new(if venue == SpotVenue::Okx {
        "BTC-USDT"
    } else {
        "BTCUSDT"
    })
    .unwrap();
    let CommandBody::New(order) = &mut cmd.body else {
        unreachable!()
    };
    order.client_id = fixture::text("client1");
    CanonicalCommand::new(cmd).unwrap()
}
#[test]
fn new_requests_are_scoped_signed_and_keep_the_canonical_id() {
    for venue in [SpotVenue::Bybit, SpotVenue::Okx] {
        let key = if venue == SpotVenue::Bybit {
            Credentials::bybit("public", b"secret")
        } else {
            Credentials::okx("public", b"secret", "pass")
        };
        let mut encoder =
            SpotEncoder::new(key, Environment::Test, 2, || Ok(1_700_000_000_123)).unwrap();
        let cmd = command(venue);
        let request = encoder.encode(&cmd).unwrap();
        assert_eq!(request.command_digest(), cmd.digest());
        let http = std::str::from_utf8(request.bytes()).unwrap();
        assert!(http.contains("client1"));
        assert!(http.starts_with(if venue == SpotVenue::Bybit {
            "POST /v5/order/create "
        } else {
            "POST /api/v5/trade/order "
        }));
        assert!(!http.contains("secret"));
    }
}

#[test]
fn fixed_hmac_vectors_cover_both_post_bodies_and_get_queries() {
    for venue in [SpotVenue::Bybit, SpotVenue::Okx] {
        let key = if venue == SpotVenue::Bybit {
            Credentials::bybit("public", b"secret")
        } else {
            Credentials::okx("public", b"secret", "pass")
        };
        let mut enc =
            SpotEncoder::new(key, Environment::Test, 2, || Ok(1_700_000_000_123)).unwrap();
        let original = command(venue);
        let mut raw = original.command().clone();
        raw.body = CommandBody::Query(fixture::target());
        let query = CanonicalCommand::new(raw).unwrap();
        let golden = if venue == SpotVenue::Bybit {
            [
                "918483e53076a1b9d02e4d4b70388c956907850aba8a247cdd659dc23589d95e",
                "b4797d65995496d2e083dacb925bc4138861e8885bd6598b32bc0a35da582f73",
            ]
        } else {
            [
                "JKWZEJ+eDqP04yz21c3YmomultQIchW8OHOhYw5YlTs=",
                "eKQRMKhT5Z6q0mJ8VEbZpXgmjXsoDueIC5hHggaoFpE=",
            ]
        };
        for (c, signature) in [original, query].iter().zip(golden) {
            let output = enc.encode(c).unwrap();
            let http = std::str::from_utf8(output.bytes()).unwrap();
            assert!(http.contains(signature));
            if venue == SpotVenue::Okx {
                assert!(http.contains("x-simulated-trading: 1\r\n"));
                assert!(http.contains("2023-11-14T22:13:20.123Z"))
            }
        }
    }
}
#[test]
fn errors_and_missing_orders_never_become_order_states() {
    use tickwire_spot_execution::{Reply, ReportError, decode_reply};
    let original = command(SpotVenue::Okx);
    let mut raw = original.command().clone();
    raw.body = CommandBody::Query(fixture::target());
    let query = CanonicalCommand::new(raw).unwrap();
    assert_eq!(
        decode_reply(&query, 503, b"unavailable"),
        Ok(Reply::Unknown)
    );
    assert_eq!(
        decode_reply(&query, 200, br#"{"code":"0","data":[]}"#),
        Ok(Reply::Unknown)
    );
    assert_eq!(
        decode_reply(&query, 200, br#"{"code":"51603","data":[]}"#),
        Ok(Reply::RequestIssue {
            code: "51603".into()
        })
    );
    assert_eq!(
        decode_reply(
            &original,
            200,
            br#"{"code":"0","data":[{"sCode":"0","ordId":"1","clOrdId":"wrong"}]}"#
        ),
        Err(ReportError::Identity)
    );
    let mut encoder = SpotEncoder::new(
        Credentials::bybit("key", b"secret"),
        Environment::Test,
        2,
        || Ok(1000),
    )
    .unwrap();
    assert!(encoder.encode(&original).is_err());
}
#[test]
fn market_order_wire_preserves_base_units_and_forbids_okx_balance_trimming() {
    for venue in [SpotVenue::Bybit, SpotVenue::Okx] {
        let original = command(venue);
        let mut c = original.command().clone();
        let CommandBody::New(ref mut new) = c.body else {
            unreachable!()
        };
        new.order_type = tickwire_venue::OrderType::Market;
        new.time_in_force = None;
        new.price = None;
        let credentials = if venue == SpotVenue::Bybit {
            Credentials::bybit("key", b"secret")
        } else {
            Credentials::okx("key", b"secret", "pass")
        };
        let mut encoder =
            SpotEncoder::new(credentials, Environment::Test, 2, || Ok(1700000000123)).unwrap();
        let wire = encoder.encode(&CanonicalCommand::new(c).unwrap()).unwrap();
        let (_, body) = std::str::from_utf8(wire.bytes())
            .unwrap()
            .split_once("\r\n\r\n")
            .unwrap();
        let body: serde_json::Value = serde_json::from_str(body).unwrap();
        if venue == SpotVenue::Bybit {
            assert_eq!(body["marketUnit"], "baseCoin");
        } else {
            assert_eq!(body["tgtCcy"], "base_ccy");
            assert_eq!(body["banAmend"], true);
        }
    }
}
