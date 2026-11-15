mod common;
use common::*;
use tickwire_venue::*;

fn sample_modified(product: Product) -> VenueEvent {
    let mut e = event();
    e.identity.product = product;
    e.body = VenueEventBody::Modified(Box::new(ModifyObservation {
        target: OrderTarget {
            order_id: [9; 16],
            expected_owner_revision: 1,
            original_intent_exec_epoch: 4,
            client_id: Some(text("test-1")),
            venue_order_id: Some(text("000123")),
        },
        amendment_id: Some(text("amd-77")),
        intent: Some(IntentKey {
            epoch: 4,
            intent_id: 11,
        }),
        prior_client_id: Some(text("test-1")),
        new_client_id: Some(text("test-1-mod")),
        venue_order_id: Some(text("000124")),
        new_quantity: d(15, -4),
        new_price: d(6010025, -2),
        cumulative_filled: Some(d(5, -4)),
    }));
    e
}

fn sample_modify_rejected(product: Product, reason: TerminalReason) -> VenueEvent {
    let mut e = event();
    e.identity.product = product;
    e.body = VenueEventBody::ModifyRejected(Box::new(ModifyRejection {
        target: OrderTarget {
            order_id: [9; 16],
            expected_owner_revision: 1,
            original_intent_exec_epoch: 4,
            client_id: Some(text("test-1")),
            venue_order_id: Some(text("000123")),
        },
        amendment_id: Some(text("amd-rej-88")),
        intent: Some(IntentKey {
            epoch: 4,
            intent_id: 12,
        }),
        new_client_id: Some(text("test-1-mod")),
        venue_order_id: Some(text("000123")),
        code: Some(-5022),
        reason,
    }));
    e
}

#[test]
fn modified_and_modify_rejected_roundtrip_schema_five_on_spot_and_perpetual() {
    for product in [Product::Spot, Product::LinearPerpetual] {
        let mod_ev = CanonicalEvent::new(sample_modified(product)).unwrap();
        assert_eq!(CanonicalEvent::decode(mod_ev.bytes()).unwrap(), mod_ev);

        let version_idx = 2 + "tickwire.venue-event".len();
        assert_eq!(mod_ev.bytes()[version_idx], 5);

        for wrong_schema in [1u8, 2, 3, 4, 6] {
            let mut mutated = mod_ev.bytes().to_vec();
            mutated[version_idx] = wrong_schema;
            assert_eq!(
                CanonicalEvent::decode(&mutated).unwrap_err(),
                Error::Schema,
                "schema {wrong_schema} must be rejected for tag 8"
            );
        }

        for prefix in 0..mod_ev.bytes().len() {
            assert!(CanonicalEvent::decode(&mod_ev.bytes()[..prefix]).is_err());
        }
        let mut trailing = mod_ev.bytes().to_vec();
        trailing.push(0);
        assert!(CanonicalEvent::decode(&trailing).is_err());

        let rej_ev = CanonicalEvent::new(sample_modify_rejected(
            product,
            TerminalReason::PostOnlyWouldTake,
        ))
        .unwrap();
        assert_eq!(CanonicalEvent::decode(rej_ev.bytes()).unwrap(), rej_ev);
        assert_eq!(rej_ev.bytes()[version_idx], 5);
    }
}

#[test]
fn all_terminal_reasons_roundtrip_and_unknown_discriminant_rejects() {
    assert_eq!(TerminalReason::ALL.len(), 31);

    let mut digests = std::collections::BTreeSet::new();
    for (expected_code, reason) in TerminalReason::ALL.iter().copied().enumerate() {
        assert_eq!(reason.code() as usize, expected_code);
        assert_eq!(TerminalReason::from_code(reason.code()), Some(reason));
        assert!(!reason.as_str().is_empty());
        assert!(!reason.ors_name().is_empty());
        assert_eq!(reason.to_string(), reason.as_str());
        let encoded = CanonicalEvent::new(sample_modify_rejected(Product::Spot, reason)).unwrap();
        let decoded = CanonicalEvent::decode(encoded.bytes()).unwrap();
        assert_eq!(decoded, encoded);
        assert!(digests.insert(encoded.digest()));
    }
    assert_eq!(TerminalReason::from_code(31), None);

    // Verify all ORS constant aliases map to their expected canonical variants.
    assert_eq!(TerminalReason::NO_REASON, TerminalReason::NoReason);
    assert_eq!(TerminalReason::MKT_CANCELED, TerminalReason::MarketCanceled);
    assert_eq!(
        TerminalReason::MKT_IOC_CANCELED,
        TerminalReason::ExpiredIocOrFok
    );
    assert_eq!(
        TerminalReason::MKT_REJECT_POST_ONLY,
        TerminalReason::PostOnlyWouldTake
    );
    assert_eq!(
        TerminalReason::MKT_REDUCE_ONLY_CANCELED,
        TerminalReason::ReduceOnlyClippedOrCanceled
    );
    assert_eq!(
        TerminalReason::MKT_CANCELED_SELFTRADE,
        TerminalReason::SelfTradePreventedByVenue
    );
    assert_eq!(TerminalReason::MKT_DELAYED, TerminalReason::MarketDelayed);
    assert_eq!(
        TerminalReason::REJECT_STALE_ORDER,
        TerminalReason::RejectStaleOrder
    );
    assert_eq!(
        TerminalReason::ALLOCATION_ERROR,
        TerminalReason::AllocationError
    );
    assert_eq!(
        TerminalReason::UNEXPECTED_MSG_SEQNUM,
        TerminalReason::UnexpectedMsgSeqnum
    );
    assert_eq!(
        TerminalReason::INVALID_STRAT_ID,
        TerminalReason::InvalidStratId
    );
    assert_eq!(
        TerminalReason::INVALID_ACCOUNT_ID,
        TerminalReason::InvalidAccountId
    );
    assert_eq!(
        TerminalReason::REJECT_ROUTING_ID_MISMATCH,
        TerminalReason::RejectRoutingIdMismatch
    );
    assert_eq!(TerminalReason::ADMIN_ACTION, TerminalReason::AdminAction);
    assert_eq!(
        TerminalReason::RESTRICTED_CANCEL_ONLY,
        TerminalReason::RestrictedCancelOnly
    );
    assert_eq!(
        TerminalReason::MKT_REJECT_MARKET_DOWN,
        TerminalReason::MarketRejectMarketDown
    );
    assert_eq!(
        TerminalReason::RFQ_QUOTE_NOT_SUPPORTED,
        TerminalReason::RfqQuoteNotSupported
    );

    // Mutate the trailing CBOR u8 discriminant of InternalCancel (30 -> [0x18, 30]) to 31.
    let last_ev = CanonicalEvent::new(sample_modify_rejected(
        Product::Spot,
        TerminalReason::InternalCancel,
    ))
    .unwrap();
    let mut bad = last_ev.bytes().to_vec();
    let last = bad.len() - 1;
    assert_eq!(bad[last], 30);
    bad[last] = 31;
    assert_eq!(CanonicalEvent::decode(&bad).unwrap_err(), Error::Enum);
}

#[test]
fn modify_event_validation_rejects_invalid_quantities_prices_and_identities() {
    for bad_case in 0..8 {
        let mut ev = sample_modified(Product::LinearPerpetual);
        let VenueEventBody::Modified(ref mut m) = ev.body else {
            unreachable!()
        };
        match bad_case {
            0 => m.new_quantity = d(0, 0),
            1 => m.new_quantity = d(-1, -3),
            2 => m.new_price = d(0, 0),
            3 => m.new_price = d(-100, 0),
            4 => m.cumulative_filled = Some(d(-1, -4)),
            5 => m.cumulative_filled = Some(d(16, -4)),
            6 => {
                m.target.client_id = None;
                m.target.venue_order_id = None;
            }
            _ => {
                m.intent = Some(IntentKey {
                    epoch: 3,
                    intent_id: 11,
                });
            }
        }
        assert!(
            CanonicalEvent::new(ev).is_err(),
            "case {bad_case} must fail validation"
        );
    }

    // Optional fields absent (venue-initiated price-band clamp without local intent) are valid.
    let mut minimal = sample_modified(Product::Spot);
    let VenueEventBody::Modified(ref mut m) = minimal.body else {
        unreachable!()
    };
    m.amendment_id = None;
    m.intent = None;
    m.prior_client_id = None;
    m.new_client_id = None;
    m.venue_order_id = None;
    m.cumulative_filled = None;
    let encoded = CanonicalEvent::new(minimal).unwrap();
    assert_eq!(CanonicalEvent::decode(encoded.bytes()).unwrap(), encoded);
}

#[test]
fn legacy_event_tags_keep_original_schemas_unchanged() {
    let version_idx = 2 + "tickwire.venue-event".len();
    let fill_ev = CanonicalEvent::new(event()).unwrap();
    assert_eq!(fill_ev.bytes()[version_idx], 1);

    let mut rej = event();
    rej.body = VenueEventBody::Rejected {
        client_id: Some(text("test-1")),
        venue_code: text("-2010"),
        message: BoundedMessage::new("insufficient balance").unwrap(),
    };
    let rej_ev = CanonicalEvent::new(rej).unwrap();
    assert_eq!(rej_ev.bytes()[version_idx], 1);
}

#[test]
fn benign_refusal_predicate_and_binance_spot_venue_classifiers_are_exact() {
    assert!(TerminalReason::CrossesBook.is_benign_business_refusal());
    assert!(TerminalReason::SelfTradePreventedByVenue.is_benign_business_refusal());
    assert!(TerminalReason::PostOnlyWouldTake.is_benign_business_refusal());
    assert!(TerminalReason::ExpiredIocOrFok.is_benign_business_refusal());
    assert!(TerminalReason::ReduceOnlyClippedOrCanceled.is_benign_business_refusal());
    assert!(TerminalReason::OrderNotOpen.is_benign_business_refusal());
    assert!(TerminalReason::TooManyPendingActions.is_benign_business_refusal());
    assert!(TerminalReason::NoChange.is_benign_business_refusal());
    assert!(TerminalReason::AbortedBeforeSend.is_benign_business_refusal());

    assert!(!TerminalReason::Filled.is_benign_business_refusal());
    assert!(!TerminalReason::UserCanceled.is_benign_business_refusal());
    assert!(!TerminalReason::InsufficientFunds.is_benign_business_refusal());
    assert!(!TerminalReason::RateLimited.is_benign_business_refusal());
    assert!(!TerminalReason::InvalidPrice.is_benign_business_refusal());
    assert!(!TerminalReason::InvalidQuantity.is_benign_business_refusal());
    assert!(!TerminalReason::VenueRejected.is_benign_business_refusal());
    assert!(!TerminalReason::Unknown.is_benign_business_refusal());

    // Binance classifier coverage
    assert_eq!(
        classify_binance_reason(
            Some(-5022),
            "Due to the order could not be executed as maker, the Post Only order will be rejected.",
            None,
            Some(TimeInForce::PostOnly),
            true
        ),
        TerminalReason::PostOnlyWouldTake
    );
    assert_eq!(
        classify_binance_reason(None, "EXPIRED", None, Some(TimeInForce::PostOnly), true),
        TerminalReason::PostOnlyWouldTake
    );
    assert_eq!(
        classify_binance_reason(
            None,
            "EXPIRED_IN_MATCH",
            Some("EXPIRE_TAKER"),
            Some(TimeInForce::Gtc),
            false
        ),
        TerminalReason::SelfTradePreventedByVenue
    );
    assert_eq!(
        classify_binance_reason(
            None,
            "EXPIRED",
            Some("EXPIRE_MAKER"),
            Some(TimeInForce::Gtc),
            false
        ),
        TerminalReason::SelfTradePreventedByVenue
    );
    assert_eq!(
        classify_binance_reason(None, "EXPIRED", None, Some(TimeInForce::Ioc), false),
        TerminalReason::ExpiredIocOrFok
    );
    assert_eq!(
        classify_binance_reason(
            Some(-2022),
            "ReduceOnly Order is rejected.",
            None,
            None,
            false
        ),
        TerminalReason::ReduceOnlyClippedOrCanceled
    );
    assert_eq!(
        classify_binance_reason(Some(-2019), "Margin is insufficient.", None, None, false),
        TerminalReason::InsufficientFunds
    );
    assert_eq!(
        classify_binance_reason(
            Some(-2010),
            "Account has insufficient balance for requested action.",
            None,
            None,
            false
        ),
        TerminalReason::InsufficientFunds
    );
    assert_eq!(
        classify_binance_reason(
            Some(-2010),
            "Order would immediately match and take.",
            None,
            Some(TimeInForce::PostOnly),
            true
        ),
        TerminalReason::PostOnlyWouldTake
    );
    assert_eq!(
        classify_binance_reason(Some(-1015), "Too many new orders.", None, None, false),
        TerminalReason::RateLimited
    );
    assert_eq!(
        classify_binance_reason(Some(429), "Too Many Requests", None, None, false),
        TerminalReason::RateLimited
    );
    assert_eq!(
        classify_binance_reason(Some(-2011), "Unknown order sent.", None, None, false),
        TerminalReason::OrderNotOpen
    );
    assert_eq!(
        classify_binance_reason(Some(-2013), "Order does not exist.", None, None, false),
        TerminalReason::OrderNotOpen
    );
    assert_eq!(
        classify_binance_reason(
            Some(-5027),
            "No need to modify the order.",
            None,
            None,
            false
        ),
        TerminalReason::NoChange
    );
    assert_eq!(
        classify_binance_reason(
            Some(-1013),
            "Filter failure: PRICE_FILTER",
            None,
            None,
            false
        ),
        TerminalReason::InvalidPrice
    );
    assert_eq!(
        classify_binance_reason(Some(-1013), "Filter failure: LOT_SIZE", None, None, false),
        TerminalReason::InvalidQuantity
    );
    assert_eq!(
        classify_binance_reason(
            Some(-1013),
            "Filter failure: MIN_NOTIONAL",
            None,
            None,
            false
        ),
        TerminalReason::InvalidQuantity
    );
    assert_eq!(
        classify_binance_reason(None, "FILLED", None, Some(TimeInForce::Gtc), false),
        TerminalReason::Filled
    );
    assert_eq!(
        classify_binance_reason(None, "CANCELED", None, Some(TimeInForce::Gtc), false),
        TerminalReason::UserCanceled
    );

    // Bybit / OKX spot classifier coverage
    assert_eq!(
        classify_spot_venue_reason(
            None,
            "EC_PostOnlyWillTakeLiquidity",
            "cancelled by post-only",
            true,
            Some(TimeInForce::PostOnly)
        ),
        TerminalReason::PostOnlyWouldTake
    );
    assert_eq!(
        classify_spot_venue_reason(
            Some(51019),
            "51019",
            "Order cancellation failed due to post_only",
            true,
            Some(TimeInForce::PostOnly)
        ),
        TerminalReason::PostOnlyWouldTake
    );
    assert_eq!(
        classify_spot_venue_reason(Some(10006), "10006", "Too many visits!", false, None),
        TerminalReason::RateLimited
    );
    assert_eq!(
        classify_spot_venue_reason(
            Some(51008),
            "51008",
            "Order placement failed due to insufficient balance",
            false,
            None
        ),
        TerminalReason::InsufficientFunds
    );
    assert_eq!(
        classify_spot_venue_reason(
            Some(51400),
            "51400",
            "Cancellation failed as the order has been completed or canceled",
            false,
            None
        ),
        TerminalReason::OrderNotOpen
    );
    assert_eq!(
        classify_spot_venue_reason(
            Some(110003),
            "110003",
            "Order price is out of permissible range",
            false,
            None
        ),
        TerminalReason::InvalidPrice
    );
    assert_eq!(
        classify_spot_venue_reason(
            Some(110004),
            "110004",
            "Order quantity invalid",
            false,
            None
        ),
        TerminalReason::InvalidQuantity
    );
    assert_eq!(
        classify_spot_venue_reason(
            Some(51509),
            "51509",
            "Order has not been modified",
            false,
            None
        ),
        TerminalReason::NoChange
    );
    assert_eq!(
        classify_spot_venue_reason(
            None,
            "SMP",
            "Canceled due to self-trade prevention",
            false,
            Some(TimeInForce::Gtc)
        ),
        TerminalReason::SelfTradePreventedByVenue
    );
    assert_eq!(
        classify_spot_venue_reason(
            None,
            "CANCELED",
            "IOC unfilled remainder",
            false,
            Some(TimeInForce::Ioc)
        ),
        TerminalReason::ExpiredIocOrFok
    );

    // Stateless ORS cancel/reject classifier distinguishes NO_REASON vs MKT_CANCELED, RPI, MKT_DELAYED, and MKT_REJECT_MARKET_DOWN.
    assert_eq!(
        classify_ors_cancel_reason(None, "CANCELED", None, Some(TimeInForce::Gtc), false, true),
        TerminalReason::NO_REASON
    );
    assert_eq!(
        classify_ors_cancel_reason(None, "CANCELED", None, Some(TimeInForce::Gtc), false, false),
        TerminalReason::MKT_CANCELED
    );
    assert_eq!(
        classify_ors_cancel_reason(None, "EXPIRED", None, Some(TimeInForce::Rpi), false, false),
        TerminalReason::MKT_REJECT_POST_ONLY
    );
    assert_eq!(
        classify_ors_cancel_reason(
            Some(-1007),
            "Timeout waiting for backend",
            None,
            None,
            false,
            false
        ),
        TerminalReason::MKT_DELAYED
    );
    assert_eq!(
        classify_ors_cancel_reason(Some(-1008), "Market is closed", None, None, false, false),
        TerminalReason::MKT_REJECT_MARKET_DOWN
    );
}
