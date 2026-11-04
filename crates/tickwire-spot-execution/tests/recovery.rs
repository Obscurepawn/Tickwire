use serde_json::{Value, json};
use tickwire_spot_execution::{
    SpotVenue,
    recovery::{self, OrderLookup},
};
use tickwire_transport::http::Response;
use tickwire_venue::Environment;
fn order(v: SpotVenue) -> Value {
    if v == SpotVenue::Bybit {
        json!({"symbol":"BTCUSDT","orderId":"o-1","orderLinkId":"tw1","price":"60000","qty":"0.01","cumExecQty":"0.01","orderStatus":"Filled","orderType":"Limit","timeInForce":"GTC","side":"Buy","isLeverage":"0","stopOrderType":"","createdTime":"1001","updatedTime":"1003"})
    } else {
        json!({"instType":"SPOT","instId":"BTC-USDT","ordId":"1","clOrdId":"tw1","px":"60000","sz":"0.01","accFillSz":"0.01","state":"filled","ordType":"limit","side":"buy","tdMode":"cash","cTime":"1001","uTime":"1003","algoId":"","tgtCcy":""})
    }
}
#[test]
fn native_limit_persistence_and_immediate_market_execution_are_verified() {
    use tickwire_venue::TimeInForce::{Fok, Gtc, Ioc, PostOnly};
    for (venue, field, values) in [
        (
            SpotVenue::Bybit,
            "timeInForce",
            ["GTC", "IOC", "FOK", "PostOnly"],
        ),
        (
            SpotVenue::Okx,
            "ordType",
            ["limit", "ioc", "fok", "post_only"],
        ),
    ] {
        let mut row = order(venue);
        for (wire, tif) in values.into_iter().zip([Gtc, Ioc, Fok, PostOnly]) {
            row[field] = json!(wire);
            assert_eq!(
                recovery::decode_order_row(venue, &row)
                    .unwrap()
                    .time_in_force,
                Some(tif)
            );
        }
        row[field] = json!("unrecognized");
        assert!(recovery::decode_order_row(venue, &row).is_err());
        if venue == SpotVenue::Bybit {
            row["orderType"] = json!("Market");
            row["marketUnit"] = json!("baseCoin");
            row[field] = json!("GTC");
            assert!(recovery::decode_order_row(venue, &row).is_err());
            row[field] = json!("IOC");
        } else {
            row[field] = json!("market");
            row["tgtCcy"] = json!("base_ccy");
        }
        assert_eq!(
            recovery::decode_order_row(venue, &row)
                .unwrap()
                .time_in_force,
            None
        );
    }
}

fn trade(v: SpotVenue) -> Value {
    if v == SpotVenue::Bybit {
        json!({"symbol":"BTCUSDT","orderId":"o-1","orderLinkId":"tw1","execType":"Trade","execId":"0ab1bdf7-4219-438b-b30a-32ec863018f7","execPrice":"60000","execQty":"0.01","execValue":"600","execTime":"1002","execFee":"0.001","feeCurrency":"USDT","side":"Buy","isMaker":true,"extraFees":""})
    } else {
        json!({"instType":"SPOT","instId":"BTC-USDT","ordId":"1","clOrdId":"tw1","billId":"90","tradeId":"57","subType":"1","fillPx":"60000","fillSz":"0.01","fillTime":"1002","fee":"-0.001","feeCcy":"USDT","side":"buy","execType":"M","ts":"1003","tradeQuoteCcy":"USDT"})
    }
}
fn fixture(v: SpotVenue, p: &str) -> Value {
    let by = v == SpotVenue::Bybit;
    if p == "/v5/user/query-api" {
        return json!({"retCode":0,"result":{"userID":42,"apiKey":"SECRET-NEVER-ON-DISK"}});
    }
    if p == "/api/v5/account/config" {
        return json!({"code":"0","data":[{"uid":"42","acctLv":"1","label":"PRIVATE-LABEL"}]});
    }
    if p == "/v5/account/info" {
        return json!({"retCode":0,"result":{"marginMode":"REGULAR_MARGIN","unifiedMarginStatus":5}});
    }
    if p == "/v5/spot-margin-trade/state" {
        return json!({"retCode":0,"result":{"spotMarginMode":"0"}});
    }
    let cat = p
        .split("category=")
        .nth(1)
        .and_then(|s| s.split('&').next())
        .unwrap_or("spot");
    let a = if !by && p.contains("&after=") {
        json!([])
    } else if p.contains("instruments-info") {
        json!([{"settleCoin":"USDT"}])
    } else if p.contains("wallet-balance") {
        json!([{"accountType":"UNIFIED","totalInitialMargin":"0","totalMaintenanceMargin":"0","totalPerpUPL":"0","coin":[{"coin":"USDT","walletBalance":"80","equity":"80","locked":"60","spotBorrow":"0","borrowAmount":"0","bonus":"0","unrealisedPnl":"0","spotHedgingQty":"0"}]}])
    } else if p.contains("/account/balance") {
        json!([{"uTime":"1003","details":[{"ccy":"USDT","eq":"80","cashBal":"80","availBal":"20","frozenBal":"60","liab":"","isoEq":"","spotInUseAmt":"","fixedBal":"0","rewardBal":"0"}]}])
    } else if p.starts_with("/v5/account/transaction-log") {
        json!([{"id":"bill1","type":"TRADE","category":"spot","symbol":"BTCUSDT","orderId":"o-1","tradeId":"0ab1bdf7-4219-438b-b30a-32ec863018f7","transactionTime":"1002","currency":"BTC"}])
    } else if p.starts_with("/api/v5/account/bills") {
        json!([{"billId":"90","type":"2","subType":"1","instType":"SPOT","instId":"BTC-USDT","ordId":"1","tradeId":"57","ts":"1003"}])
    } else if p.contains("history") || p.contains("clOrdId=") {
        json!([order(v)])
    } else if p.contains("execution/list") || p.contains("/trade/fills") {
        json!([trade(v)])
    } else {
        json!([])
    };
    if by {
        json!({"retCode":0,"result":{"category":cat,"nextPageCursor":"","list":a}})
    } else {
        json!({"code":"0","data":a})
    }
}
fn response(data: Value) -> Response {
    Response {
        status: 200,
        headers: vec![],
        body: serde_json::to_vec(&data).unwrap(),
    }
}
#[test]
fn both_cash_profiles_are_replayable_and_rest_fees_are_individual_trade_deltas() {
    for v in [SpotVenue::Bybit, SpotVenue::Okx] {
        let symbol = if v == SpotVenue::Bybit {
            "BTCUSDT"
        } else {
            "BTC-USDT"
        };
        let targets = [OrderLookup {
            symbol: symbol.into(),
            client_id: "tw1".into(),
        }];
        let evidence = recovery::acquire_for(
            v,
            Environment::Production,
            "42",
            (1000, 1100),
            1200,
            &targets,
            |p| Ok(response(fixture(v, p))),
        )
        .unwrap();
        let s = recovery::replay_evidence(&evidence.evidence_bytes).unwrap();
        assert_eq!(s.fingerprint, evidence.fingerprint);
        assert_eq!(s.history, s.target_orders);
        assert_eq!(s.balances[0].available.as_plain_string(), "20");
        let f = &s.fills[0];
        assert_eq!(f.fee.as_plain_string(), "-0.001");
        assert_eq!(f.fee_currency, "USDT");
        let row: Value = serde_json::from_slice(&f.wire_row).unwrap();
        if v == SpotVenue::Okx {
            assert_eq!(row["fillFee"], "-0.001");
            assert_eq!(row["fillFeeCcy"], "USDT");
        } else {
            assert_eq!(row["category"], "spot");
        }
        let mut capture: Value = serde_json::from_slice(&s.evidence_bytes).unwrap();
        use base64::{Engine as _, engine::general_purpose::STANDARD};
        for page in capture["pages"].as_array().unwrap() {
            let body =
                String::from_utf8(STANDARD.decode(page[2].as_str().unwrap()).unwrap()).unwrap();
            assert!(!body.contains("SECRET-NEVER-ON-DISK") && !body.contains("PRIVATE-LABEL"));
        }
        capture["pages"].as_array_mut().unwrap().pop();
        assert!(recovery::replay_evidence(&serde_json::to_vec(&capture).unwrap()).is_err());
    }
}
#[test]
fn missing_extra_fee_identity_unsupported_exposure_and_uncovered_history_fail() {
    for (v, case) in [
        (SpotVenue::Okx, "missing-fee"),
        (SpotVenue::Okx, "borrowed"),
        (SpotVenue::Okx, "algo"),
        (SpotVenue::Okx, "other-account"),
        (SpotVenue::Bybit, "extra-fee"),
        (SpotVenue::Bybit, "derivative"),
        (SpotVenue::Bybit, "cursor"),
    ] {
        let mut identities = 0;
        let r = recovery::acquire(v, Environment::Test, "42", 1000, 1100, 1200, |p| {
            let mut data = fixture(v, p);
            match case {
                "missing-fee" if p.contains("/trade/fills") => {
                    data["data"][0].as_object_mut().unwrap().remove("feeCcy");
                }
                "borrowed" if p.contains("account/balance") => {
                    data["data"][0]["details"][0]["liab"] = json!("1")
                }
                "algo" if p.contains("ordType=oco") => data["data"] = json!([{"algoId":"123"}]),
                "other-account" if p.contains("account/config") => {
                    identities += 1;
                    if identities == 2 {
                        data["data"][0]["uid"] = json!("43");
                    }
                }
                "extra-fee" if p.contains("execution/list") => {
                    data["result"]["list"][0]["extraFees"] = json!("[regional tax]")
                }
                "derivative" if p.contains("category=inverse") && p.contains("position/list") => {
                    data["result"]["list"] = json!([{"size":"2"}])
                }
                "cursor" if p.contains("execution/list") => {
                    data["result"]["nextPageCursor"] = json!("repeat")
                }
                _ => {}
            }
            Ok(response(data))
        });
        assert!(r.is_err(), "{case}");
    }
    for (v, retention) in [
        (SpotVenue::Bybit, 24 * 3_600_000),
        (SpotVenue::Okx, 2 * 3_600_000),
    ] {
        assert!(
            recovery::acquire(
                v,
                Environment::Test,
                "42",
                1,
                100,
                1 + retention,
                |_| panic!("expired")
            )
            .is_err()
        );
    }
}

#[test]
fn bybit_bill_ids_can_repeat_across_currency_legs_of_one_trade() {
    for currency in ["USDT", "BTC"] {
        let result = recovery::acquire(
            SpotVenue::Bybit,
            Environment::Test,
            "42",
            1000,
            1100,
            1200,
            |path| {
                let mut body = fixture(SpotVenue::Bybit, path);
                if path.starts_with("/v5/account/transaction-log?") {
                    let rows = body["result"]["list"].as_array_mut().unwrap();
                    let mut second = rows[0].clone();
                    second["currency"] = json!(currency);
                    rows.push(second);
                }
                Ok(response(body))
            },
        );
        assert_eq!(result.is_ok(), currency == "USDT");
    }
}

#[test]
fn offsetting_external_wallet_transfers_do_not_qualify_the_same_ending_balance() {
    let result = recovery::acquire(
        SpotVenue::Okx,
        Environment::Production,
        "42",
        1000,
        1100,
        1200,
        |path| {
            let body = if path.starts_with("/api/v5/account/bills?") {
                json!({"code":"0","data":[
                    {"billId":"2","ts":"1005","type":"1","balChg":"200"},
                    {"billId":"1","ts":"1004","type":"1","balChg":"-200"}
                ]})
            } else {
                fixture(SpotVenue::Okx, path)
            };
            Ok(response(body))
        },
    );
    assert!(
        result.is_err(),
        "cash movements require external attribution"
    );
}

#[test]
fn open_order_pages_use_venue_cursor_and_replay_checks_the_next_request() {
    for venue in [SpotVenue::Bybit, SpotVenue::Okx] {
        let mut count = 0;
        let e = recovery::acquire(venue, Environment::Test, "42", 1000, 1100, 1200, |p| {
            let mut data = fixture(venue, p);
            if p.contains("/trade/orders-pending")
                || p.starts_with("/v5/order/realtime?category=spot")
            {
                let bybit = venue == SpotVenue::Bybit;
                let continuation = p.contains("&after=") || p.contains("&cursor=");
                let ids = if p.ends_with("&after=99") {
                    vec![]
                } else if continuation {
                    assert!(p.ends_with(if bybit { "&cursor=p%3A2" } else { "&after=101" }));
                    vec![99]
                } else if bybit {
                    vec![200]
                } else {
                    (101..=200).rev().collect()
                };
                let rows: Vec<_> = ids
                    .into_iter()
                    .map(|id| {
                        let mut r = order(venue);
                        r[if bybit { "orderId" } else { "ordId" }] = json!(id.to_string());
                        r[if bybit { "orderLinkId" } else { "clOrdId" }] =
                            json!(format!("open{id}"));
                        r[if bybit { "orderStatus" } else { "state" }] =
                            json!(if bybit { "New" } else { "live" });
                        r[if bybit { "cumExecQty" } else { "accFillSz" }] = json!("0");
                        r
                    })
                    .collect();
                count += rows.len();
                if bybit {
                    data["result"]["list"] = json!(rows);
                    data["result"]["nextPageCursor"] =
                        json!(if continuation { "" } else { "p%3A2" });
                } else {
                    data["data"] = json!(rows);
                }
            }
            Ok(response(data))
        })
        .unwrap();
        assert_eq!(count, if venue == SpotVenue::Bybit { 2 } else { 101 });
        assert_eq!(e.open_orders.len(), count);
        let mut capture: Value = serde_json::from_slice(&e.evidence_bytes).unwrap();
        let next = capture["pages"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|p| {
                p[0].as_str()
                    .unwrap()
                    .contains(if venue == SpotVenue::Bybit {
                        "&cursor="
                    } else {
                        "&after="
                    })
            })
            .unwrap();
        next[0] = json!("/wrong-page");
        assert!(recovery::replay_evidence(&serde_json::to_vec(&capture).unwrap()).is_err());
    }
}
