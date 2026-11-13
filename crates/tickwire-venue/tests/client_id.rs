use tickwire_venue::{
    ClientId, ClientIdDomain, DecodedClientId, Environment, PARENT_STRUCTURE_LEG, Product,
    REPLACEMENT_ORDER_LEG, SELF_DESCRIBING_CLIENT_ID_LEN, SelfDescribingClientId, venue_id,
};

fn sample_domain() -> ClientIdDomain {
    ClientIdDomain {
        risk_domain_id: [0x42; 16],
        venue: venue_id::OKX,
        environment: Environment::Production,
        account: 9_001,
        product: Product::Spot,
    }
}

#[test]
fn round_trips_boundary_fields_and_satisfies_okx_bybit_binance_charset() {
    let domain = sample_domain();
    let cases = [
        SelfDescribingClientId {
            logical_strategy_id: 1,
            leg: 0,
            epoch: 1,
            intent_id: 1,
        },
        SelfDescribingClientId {
            logical_strategy_id: 1024,
            leg: REPLACEMENT_ORDER_LEG,
            epoch: 42,
            intent_id: 987_654_321,
        },
        SelfDescribingClientId {
            logical_strategy_id: u16::MAX,
            leg: PARENT_STRUCTURE_LEG,
            epoch: u32::MAX,
            intent_id: (1u64 << 63) - 1,
        },
    ];
    for id in cases {
        let encoded = id.encode(&domain).expect("valid self-describing ClientId");
        let raw = encoded.as_str();
        assert_eq!(raw.len(), SELF_DESCRIBING_CLIENT_ID_LEN);
        assert!(raw.starts_with("t1"));
        // Strictly ASCII lowercase alphanumeric: valid for OKX (<=32 alnum), Bybit (<=36), Binance (<=36).
        assert!(
            raw.bytes()
                .all(|b| b.is_ascii_digit() || b.is_ascii_lowercase())
        );
        assert_eq!(
            SelfDescribingClientId::decode(&encoded, &domain),
            DecodedClientId::Managed(id)
        );
        let uuid = id.deterministic_order_id(&domain);
        assert_ne!(uuid, [0; 16]);
        assert_eq!(uuid, id.deterministic_order_id(&domain));
    }
}

#[test]
fn rejects_zero_or_out_of_range_fields_on_encode() {
    let domain = sample_domain();
    let invalid = [
        SelfDescribingClientId {
            logical_strategy_id: 0,
            leg: 0,
            epoch: 1,
            intent_id: 1,
        },
        SelfDescribingClientId {
            logical_strategy_id: 1,
            leg: 16,
            epoch: 1,
            intent_id: 1,
        },
        SelfDescribingClientId {
            logical_strategy_id: 1,
            leg: 0,
            epoch: 0,
            intent_id: 1,
        },
        SelfDescribingClientId {
            logical_strategy_id: 1,
            leg: 0,
            epoch: 1,
            intent_id: 0,
        },
        SelfDescribingClientId {
            logical_strategy_id: 1,
            leg: 0,
            epoch: 1,
            intent_id: 1u64 << 63,
        },
    ];
    for bad in invalid {
        assert!(bad.encode(&domain).is_err());
    }
}

#[test]
fn classifies_manual_tampered_and_cross_domain_ids_as_external() {
    let domain = sample_domain();
    let id = SelfDescribingClientId {
        logical_strategy_id: 7,
        leg: 2,
        epoch: 19,
        intent_id: 4096,
    };
    let encoded = id.encode(&domain).unwrap();

    // Manual / legacy client IDs classify cleanly as External.
    let manual = ClientId::new("manual-order-2026-btc-01").unwrap();
    assert_eq!(
        SelfDescribingClientId::decode(&manual, &domain),
        DecodedClientId::External
    );

    // Wrong account, venue, environment, product, or risk_domain_id fails the 34-bit domain tag.
    let mut wrong_account = domain;
    wrong_account.account += 1;
    assert_eq!(
        SelfDescribingClientId::decode(&encoded, &wrong_account),
        DecodedClientId::External
    );

    let mut wrong_venue = domain;
    wrong_venue.venue = venue_id::BINANCE;
    assert_eq!(
        SelfDescribingClientId::decode(&encoded, &wrong_venue),
        DecodedClientId::External
    );

    let mut wrong_env = domain;
    wrong_env.environment = Environment::Test;
    assert_eq!(
        SelfDescribingClientId::decode(&encoded, &wrong_env),
        DecodedClientId::External
    );

    let mut wrong_product = domain;
    wrong_product.product = Product::LinearPerpetual;
    assert_eq!(
        SelfDescribingClientId::decode(&encoded, &wrong_product),
        DecodedClientId::External
    );

    // Flipping any character in the 30-char payload fails tag verification.
    let mut tampered_bytes = encoded.as_str().as_bytes().to_vec();
    tampered_bytes[10] = if tampered_bytes[10] == b'0' {
        b'1'
    } else {
        b'0'
    };
    let tampered = ClientId::new(String::from_utf8(tampered_bytes).unwrap()).unwrap();
    assert_eq!(
        SelfDescribingClientId::decode(&tampered, &domain),
        DecodedClientId::External
    );
}
