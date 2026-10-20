use tickwire_binance::{Error, request::Market, response::*};
#[test]
fn unknown_errors_never_turn_into_authoritative_rejection() {
    for (market, status, code, msg) in [
        (Market::Spot, 500, None, ""),
        (
            Market::Spot,
            400,
            Some(-1007),
            "Timeout waiting for response from backend server.",
        ),
        (
            Market::Usdm,
            503,
            None,
            "Unknown error, please check your request or try again later.",
        ),
        (Market::Usdm, 502, None, "bad gateway"),
    ] {
        assert_eq!(classify(market, status, code, msg), FailureKind::Unknown);
    }
}
#[test]
fn futures_documented_503_failures_are_distinguished_from_unknown() {
    assert_eq!(
        classify(Market::Usdm, 503, None, "Service Unavailable."),
        FailureKind::Rejected
    );
    assert_eq!(
        classify(
            Market::Usdm,
            503,
            Some(-1008),
            "Request throttled by system-level protection. Reduce-only/close-position orders are exempt. Please try again."
        ),
        FailureKind::Rejected
    );
    assert_eq!(
        classify(Market::Usdm, 503, None, "unrecognized new error"),
        FailureKind::Unknown
    );
}
#[test]
fn not_found_rate_limits_auth_and_partial_results_are_preserved() {
    for (status, code, kind) in [
        (400, -2013, FailureKind::NotFound),
        (429, -1003, FailureKind::RateLimited),
        (418, -1003, FailureKind::Banned),
        (401, -2015, FailureKind::Authentication),
        (400, -1021, FailureKind::ClockSkew),
        (409, -2021, FailureKind::Partial),
    ] {
        assert_eq!(classify(Market::Spot, status, Some(code), ""), kind);
    }
}
#[test]
fn websocket_envelope_preserves_correlation_raw_result_and_rate_limits() {
    let input=br#"{"id":"req-7","status":200,"result":{"orderId":9007199254740993,"price":"0.10000000"},"rateLimits":[{"count":3}]}"#;
    let r = decode_websocket(Market::Spot, input).unwrap();
    assert_eq!(r.request_id.unwrap().get(), r#""req-7""#);
    assert!(r.result.unwrap().get().contains("9007199254740993"));
    assert!(r.failure.is_none());
    assert_eq!(r.rate_limits.unwrap().get(), r#"[{"count":3}]"#);
}
#[test]
fn partial_cancel_replace_retains_both_result_branches() {
    let r=decode_websocket(Market::Spot,br#"{"id":1,"status":409,"error":{"code":-2021,"msg":"partial","data":{"cancelResult":"SUCCESS","newOrderResult":"FAILURE"}}}"#).unwrap();
    assert_eq!(r.failure, Some(FailureKind::Partial));
    assert!(
        r.error
            .unwrap()
            .data
            .unwrap()
            .get()
            .contains("cancelResult")
    );
}
#[test]
fn invalid_or_ambiguous_envelopes_are_rejected() {
    for input in [
        br#"{"id":1,"status":200,"result":{},"error":{"code":-1,"msg":"x"}}"#.as_slice(),
        br#"{"id":1,"status":200,"status":400,"result":{}}"#.as_slice(),
        br#"{"id":1,"status":200}"#.as_slice(),
        br#"{"id":1,"status":200,"result":{}} trailing"#.as_slice(),
    ] {
        assert!(matches!(
            decode_websocket(Market::Spot, input),
            Err(Error::InvalidResponse)
        ));
    }
}
#[test]
fn rest_error_and_success_use_http_status_without_losing_error_code() {
    let r = decode_rest(
        Market::Usdm,
        400,
        br#"{"code":-2013,"msg":"Order does not exist."}"#,
    )
    .unwrap();
    assert_eq!(r.failure, Some(FailureKind::NotFound));
    assert_eq!(r.error.unwrap().code, -2013);
    let r = decode_rest(Market::Spot, 200, br#"[{"orderId":7}]"#).unwrap();
    assert!(r.failure.is_none());
    assert_eq!(r.result.unwrap().get(), r#"[{"orderId":7}]"#);
}

#[test]
fn malformed_http_200_error_and_non_success_status_never_become_success() {
    for input in [
        br#"{"code":-1007,"msg":1}"#.as_slice(),
        br#"{"code":-1007,"code":200,"msg":"timeout"}"#.as_slice(),
        br#"{"code":-1007}"#.as_slice(),
    ] {
        assert!(matches!(
            decode_rest(Market::Spot, 200, input),
            Err(Error::InvalidResponse)
        ));
    }
    assert!(decode_rest(Market::Spot, 302, b"{}").is_err());
    assert!(decode_websocket(Market::Spot, br#"{"id":1,"status":302,"result":{}}"#).is_err());
}

#[test]
fn typed_order_ack_does_not_invent_status_or_quantity() {
    let r=decode_order(br#"{"symbol":"BTCUSDT","orderId":9007199254740993,"clientOrderId":"c-1","transactTime":1507725176595}"#).unwrap();
    assert_eq!(r.order_id, 9007199254740993);
    assert!(r.status.is_none());
    assert!(r.executed_quantity.is_none());
}
#[test]
fn typed_order_exact_decimals_and_fill_payload_are_preserved() {
    let r=decode_order(br#"{"symbol":"BTCUSDT","orderId":7,"clientOrderId":"c-1","status":"PARTIALLY_FILLED","price":"60000.00","origQty":"0.001","executedQty":"0.0005","cummulativeQuoteQty":"30.00000000","fills":[{"tradeId":9,"commission":"0.01","commissionAsset":"BNB"}]}"#).unwrap();
    assert_eq!(
        r.executed_quantity
            .unwrap()
            .rescale_exact(8)
            .unwrap()
            .mantissa(),
        50000
    );
    assert_eq!(
        r.cumulative_quote
            .unwrap()
            .rescale_exact(0)
            .unwrap()
            .mantissa(),
        30
    );
    assert!(r.fills.unwrap().get().contains("commissionAsset"));
}
#[test]
fn typed_order_rejects_wrong_types_duplicates_negative_quantities_and_missing_identity() {
    for s in [
        r#"{"symbol":"BTCUSDT","orderId":7,"clientOrderId":"c","price":0.1}"#,
        r#"{"symbol":"BTCUSDT","orderId":7,"orderId":8,"clientOrderId":"c"}"#,
        r#"{"symbol":"BTCUSDT","orderId":7,"clientOrderId":"c","executedQty":"-1"}"#,
        r#"{"symbol":"BTCUSDT","orderId":7,"clientOrderId":"c","executedQty":"1e-3"}"#,
        r#"{"symbol":"BTCUSDT","orderId":7}"#,
    ] {
        assert!(decode_order(s.as_bytes()).is_err(), "{s}");
    }
}
#[test]
fn new_order_status_is_preserved_without_assuming_terminal() {
    let r = decode_order(
        br#"{"symbol":"BTCUSDT","orderId":7,"clientOrderId":"c","status":"FUTURE_NEW_STATE"}"#,
    )
    .unwrap();
    assert_eq!(r.status.as_deref(), Some("FUTURE_NEW_STATE"));
}
#[test]
fn algo_identity_is_not_interchanged_with_child_order_identity() {
    let r=decode_algo(br#"{"algoId":3000000000003505,"clientAlgoId":"algo-1","symbol":"BTCUSDT","algoStatus":"NEW","triggerPrice":"120000.00"}"#).unwrap();
    assert_eq!(r.algo_id, 3000000000003505);
    assert_eq!(r.client_algo_id, "algo-1");
    assert!(decode_order(br#"{"algoId":3,"clientAlgoId":"a","symbol":"BTCUSDT"}"#).is_err());
    assert!(decode_algo(br#"{"orderId":3,"clientOrderId":"a","symbol":"BTCUSDT"}"#).is_err());
}

#[test]
fn backend_timeout_and_unknown_new_error_codes_do_not_authorize_release() {
    assert_eq!(classify(Market::Usdm, 408, None, ""), FailureKind::Unknown);
    for code in [-1000, -1001, -19999] {
        assert_eq!(
            classify(Market::Spot, 400, Some(code), "new or internal error"),
            FailureKind::Unknown
        );
    }
    for code in [-1013, -1100, -2010] {
        assert_eq!(
            classify(Market::Spot, 400, Some(code), "rejected"),
            FailureKind::Rejected
        );
    }
}
