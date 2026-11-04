use serde_json::json;
use tickwire_spot_execution::{
    Credentials,
    private::{PrivateFeed, Reaction},
};
#[test]
fn login_and_channel_acks_are_required_before_data() {
    let key = Credentials::okx("key", b"secret", "pass");
    let mut f = PrivateFeed::new(&key, 1700000000123).unwrap();
    assert_eq!(f.login()["op"], "login");
    let orders = json!({"arg":{"channel":"orders","instType":"SPOT"},"data":[{"instType":"SPOT","instId":"BTC-USDT","clOrdId":"a","tradeId":"3","fillSz":"1","fillFee":"-0.1","fillFeeCcy":"USDT","apiKey":"never-print"}]});
    assert!(f.on_text(&serde_json::to_vec(&orders).unwrap()).is_err());
    let mut f = PrivateFeed::new(&key, 1700000000123).unwrap();
    let Reaction::Subscribe(request) = f
        .on_text(br#"{"event":"login","code":"0","connId":"a"}"#)
        .unwrap()
    else {
        panic!("OKX login must subscribe to orders and account events");
    };
    assert_eq!(request["args"][1]["channel"], "account");
    assert_eq!(
        request["args"][1]["extraParams"],
        r#"{"updateInterval":"0"}"#
    );
    assert!(matches!(f.on_text(br#"{"id":"twaccount","event":"subscribe","connId":"a","arg":{"channel":"orders","instType":"SPOT"}}"#).unwrap(),Reaction::Control));
    let Reaction::Data(data) = f.on_text(&serde_json::to_vec(&orders).unwrap()).unwrap() else {
        panic!()
    };
    assert_eq!(data.rows[0]["fillFee"], "-0.1");
    assert!(!data.rows[0].to_string().contains("never-print"));
    assert!(!f.subscribed());
    assert!(matches!(
        f.on_text(
            br#"{"id":"twaccount","event":"subscribe","connId":"a","arg":{"channel":"account"}}"#
        )
        .unwrap(),
        Reaction::Control
    ));
    assert!(f.subscribed());
}
#[test]
fn bybit_private_topics_share_an_ack_but_keep_wallet_scope_separate() {
    let key = Credentials::bybit("key", b"secret");
    let mut f = PrivateFeed::new(&key, 1700000000123).unwrap();
    assert_eq!(f.login()["args"][1], 1700000005123u64);
    assert!(matches!(
        f.on_text(br#"{"op":"auth","success":true,"conn_id":"a"}"#)
            .unwrap(),
        Reaction::Subscribe(_)
    ));
    f.on_text(br#"{"req_id":"twaccount","op":"subscribe","success":true,"conn_id":"a"}"#)
        .unwrap();
    assert!(f.subscribed());
    let Reaction::Data(data)=f.on_text(br#"{"topic":"execution.spot","data":[{"category":"spot","symbol":"BTCUSDT","execId":"ab","execTime":"123","private":"never-print"}]}"#).unwrap()else{panic!()};
    assert_eq!(data.channel, "fills");
    assert!(!data.rows[0].to_string().contains("never-print"));
    assert!(
        f.on_text(
            br#"{"topic":"execution.spot","data":[{"category":"linear","symbol":"BTCUSDT"}]}"#
        )
        .is_err()
    );
}

#[test]
fn a_bybit_heartbeat_error_invalidates_the_authenticated_connection() {
    let mut f = PrivateFeed::new(&Credentials::bybit("k", b"s"), 1700000000010).unwrap();
    f.on_text(br#"{"success":true,"op":"auth","conn_id":"c"}"#)
        .unwrap();
    f.on_text(br#"{"success":true,"op":"subscribe","req_id":"twaccount","conn_id":"c"}"#)
        .unwrap();
    assert!(f.subscribed());
    assert!(
        f.on_text(br#"{"op":"ping","success":false,"ret_msg":"session closed","conn_id":"c"}"#)
            .is_err()
    );
    assert!(!f.subscribed());
}
