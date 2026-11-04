use serde_json::{Value, json};
use tickwire_spot_execution::{
    SpotVenue::{Bybit, Okx},
    observe::Query,
};
use tickwire_transport::http::Response;
fn http(status: u16, body: Value) -> Response {
    Response {
        status,
        headers: vec![("Secret".into(), b"never-print".to_vec())],
        body: serde_json::to_vec(&body).unwrap(),
    }
}
#[test]
fn pages_keep_actual_status_and_only_requested_business_facts() {
    let q = Query::Balance;
    let denied = q
        .evidence(Okx, http(401, json!({"code":"50113","msg":"never-print"})))
        .unwrap();
    assert_eq!(denied["http_status"], 401);
    assert_eq!(denied["success"], false);
    assert!(!denied.to_string().contains("never-print"));
    for (venue, body) in [
        (
            Bybit,
            json!({"retCode":0,"result":{"list":[{"accountType":"UNIFIED","secret":"never-print","coin":[{"coin":"BTC","walletBalance":"1","secret":"never-print"}]}]}}),
        ),
        (
            Okx,
            json!({"code":"0","data":[{"secret":"never-print","details":[{"ccy":"BTC","cashBal":"1","secret":"never-print"}]}]}),
        ),
    ] {
        let evidence = q.evidence(venue, http(200, body)).unwrap();
        let out = evidence.to_string();
        assert_eq!(evidence["success"], true);
        assert!(out.contains("BTC"));
        assert!(!out.contains("never-print"));
    }
    let q = Query::Order {
        symbol: "BTC-USDT",
        client_id: "abc",
    };
    assert!(q.evidence(Okx,http(200,json!({"code":"0","data":[{"instType":"SPOT","instId":"BTC-USDT","clOrdId":"someoneElse"}]}))).is_err());
    let empty = q
        .evidence(Okx, http(200, json!({"code":"0","data":[]})))
        .unwrap();
    assert_eq!(empty["rows"], json!([]));
    assert_eq!(empty["coverage"], "one_response");
}
#[test]
fn encoded_bybit_cursors_roundtrip_without_reencoding_delimiters_or_injecting_queries() {
    let path = Query::Fills {
        window: None,
        symbol: Some("BTCUSDT"),
        cursor: Some("42%3A2%2C42%3A1&a=b"),
    }
    .path(Bybit)
    .unwrap();
    assert!(path.ends_with("&cursor=42%3A2%2C42%3A1%26a%3Db"));
    assert!(
        Query::OpenOrders {
            symbol: Some("BTC-USDT"),
            cursor: Some("12&bad")
        }
        .path(Okx)
        .is_err()
    );
    assert!(
        Query::OpenOrders {
            symbol: Some("BTC-USDT"),
            cursor: Some("123")
        }
        .path(Okx)
        .unwrap()
        .ends_with("&after=123")
    );
}
#[test]
fn scan_checks_expected_uid_and_fetches_every_page_in_a_fixed_window() {
    use tickwire_spot_execution::observe::scan;
    let mut paths = Vec::new();
    let mut read = |q: Query<'_>| -> Result<Value, Box<dyn std::error::Error>> {
        let path = q.path(Bybit)?;
        let body = if path == "/v5/user/query-api" {
            json!({"retCode":0,"result":{"userID":42,"apiKey":"never-print"}})
        } else if path.contains("wallet-balance") {
            json!({"retCode":0,"result":{"list":[{"accountType":"UNIFIED","coin":[]}]}})
        } else if path.contains("realtime") {
            json!({"retCode":0,"result":{"category":"spot","nextPageCursor":"","list":[]}})
        } else {
            assert!(!path.contains("symbol="));
            assert!(path.contains("startTime=1000000&endTime=1001000"));
            let next = if path.contains("cursor=") {
                ""
            } else {
                "p%3A2"
            };
            let id = if next.is_empty() { "2" } else { "1" };
            json!({"retCode":0,"result":{"category":"spot","nextPageCursor":next,"list":[{"symbol":"BTCUSDT","execId":id,"execTime":"1000001"}]}})
        };
        paths.push(path);
        Ok(q.evidence(Bybit, http(200, body))?)
    };
    let result = scan(Bybit, "42", 1000000, 1001000, &mut read).unwrap();
    assert_eq!(result["fills"]["rows"].as_array().unwrap().len(), 2);
    assert_eq!(result["fills"]["pages"], 2);
    assert_eq!(result["fresh_risk_qualified"], false);
    assert!(!result.to_string().contains("never-print"));
    assert!(scan(Bybit, "43", 1000000, 1001000, &mut read).is_err());
}
#[test]
fn okx_scan_follows_bill_id_filter_and_rechecks_uid_at_the_end() {
    use tickwire_spot_execution::observe::scan;
    let scenario = |last_uid: &str| {
        let mut identities = 0;
        let mut after_bill = false;
        let result = scan(Okx, "987", 1000, 2000, |q| {
            let path = q.path(Okx)?;
            let rows = if path == "/api/v5/account/config" {
                identities += 1;
                json!([{"uid":if identities==1{"987"}else{last_uid},"mainUid":"other","secret":"never-print"}])
            } else if path.contains("/account/balance") {
                json!([{"details":[]}])
            } else if path.contains("orders-pending") {
                json!([])
            } else {
                assert!(path.contains("instType=SPOT&begin=1000&end=2000"));
                if path.contains("after=") {
                    after_bill = path.ends_with("after=100");
                    json!([])
                } else {
                    Value::Array((1..=100).map(|i|json!({"instType":"SPOT","instId":"BTC-USDT","billId":i.to_string(),"ts":"1400"})).collect())
                }
            };
            Ok(q.evidence(Okx, http(200, json!({"code":"0","data":rows})))?)
        });
        assert!(after_bill);
        result
    };
    let report = scenario("987").unwrap();
    assert_eq!(report["fills"]["pages"], 2);
    assert_eq!(report["fills"]["rows"].as_array().unwrap().len(), 100);
    assert!(scenario("another").is_err());
}
