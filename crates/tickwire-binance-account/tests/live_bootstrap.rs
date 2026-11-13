#[path = "../../tickwire-account-gateway/tests/common/mod.rs"]
mod gateway;
#[path = "../../tickwire-binance-private/tests/common/mod.rs"]
mod network;

use std::{collections::BTreeMap, io::Read, thread, time::Instant};
use tickwire_account::{Applied, IntentKey, ManagerIdentity, OrderKey};
use tickwire_account_gateway::AccountGateway;
use tickwire_account_journal::InMemoryBackend;
use tickwire_binance::request::Market;
use tickwire_binance_account::{
    AccountIngress, Config, Outcome, reconciliation::bootstrap::bootstrap_from_remote_snapshots,
};
use tickwire_binance_private as private;
use tickwire_journal::store::InMemoryStore;
use tickwire_venue::{self as v, ClientIdDomain, PositionSide, SelfDescribingClientId};

#[test]
fn spot_cold_bootstrap_decodes_client_order_id_and_processes_live_ws_fill_in_memory() {
    let (template, catalog, cmd) = gateway::fixture::fixture();
    let domain = ClientIdDomain {
        risk_domain_id: template.scope.risk_domain_id,
        venue: template.scope.venue,
        environment: template.scope.environment,
        account: template.scope.account,
        product: v::Product::Spot,
    };
    let sd = SelfDescribingClientId {
        logical_strategy_id: 8,
        leg: 0,
        epoch: 2,
        intent_id: 7,
    };
    let cid = sd.encode(&domain).unwrap();

    let manager = ManagerIdentity {
        instance: cmd.context.strategy.instance_id,
        generation: cmd.context.request.strategy_generation,
    };
    let managers = BTreeMap::from([(8u64, manager)]);

    // Remote Spot /api/v3/account + /api/v3/openOrders:
    // - 1 BTC free, 800 USDT free, 200 USDT locked = 60 managed (0.001 @ 60000) + 140 external
    let account_json = br#"{"canTrade":true,"accountType":"SPOT","balances":[{"asset":"BTC","free":"1.00000000","locked":"0"},{"asset":"USDT","free":"800.00000000","locked":"200.00000000"}]}"#;
    let open_orders_json = serde_json::to_vec(&serde_json::json!([
        {
            "symbol": "BTCUSDT",
            "orderId": 9901,
            "clientOrderId": cid.as_str(),
            "status": "NEW",
            "side": "BUY",
            "type": "LIMIT",
            "origQty": "0.001",
            "executedQty": "0",
            "price": "60000",
            "timeInForce": "GTC",
            "orderListId": -1
        },
        {
            "symbol": "BTCUSDT",
            "orderId": 9902,
            "clientOrderId": "manual-ui-order",
            "status": "NEW",
            "side": "BUY",
            "type": "LIMIT",
            "origQty": "0.002",
            "executedQty": "0",
            "price": "70000",
            "timeInForce": "GTC",
            "orderListId": -1
        }
    ]))
    .unwrap();

    let remote_boot = bootstrap_from_remote_snapshots(
        Market::Spot,
        template,
        catalog,
        &managers,
        account_json,
        &open_orders_json,
        false,
    )
    .unwrap();

    assert_eq!(remote_boot.live.external_open_orders(), 1);
    assert_eq!(remote_boot.live.imported_orders().len(), 1);
    assert_eq!(remote_boot.registrations.len(), 1);

    let baseline = remote_boot.live.baseline().clone();
    let gw_cfg = gateway::config(&baseline);
    let (conn, peer) = gateway::peer(&gw_cfg.route.authority, 16384);
    let mut gw = AccountGateway::create_from_live_exchange(
        InMemoryBackend::new(),
        gw_cfg,
        remote_boot.live,
        gateway::MockAuthority::default(),
        gateway::TestEncoder,
        conn,
    )
    .unwrap();

    let ingress_cfg = Config {
        physical: tickwire_journal::Config::new([81; 16], [1; 16]),
        writer_epoch: 4,
        baseline,
        account_stream: [2; 16],
        max_records: 64,
        income_association_authority: None,
        income_baseline_authority: None,
        max_registrations: 16,
        max_raw_bytes: 32_768,
        max_retained_bytes: 1 << 20,
    };
    let mut ingress = AccountIngress::create_from_live_exchange(
        InMemoryStore::new(),
        ingress_cfg,
        &remote_boot.registrations,
    )
    .unwrap();

    // Deliver a live Binance Spot WebSocket executionReport FILL for the cold-imported order!
    let ws_fill = serde_json::to_vec(&serde_json::json!({
        "subscriptionId": 0,
        "event": {
            "e": "executionReport",
            "E": 10,
            "s": "BTCUSDT",
            "c": cid.as_str(),
            "S": "BUY",
            "o": "LIMIT",
            "f": "GTC",
            "q": "0.001",
            "p": "60000",
            "g": -1,
            "C": "",
            "x": "TRADE",
            "X": "FILLED",
            "i": 9901,
            "l": "0.001",
            "z": "0.001",
            "L": "60000",
            "n": "0.01",
            "N": "USDT",
            "T": 9,
            "t": 33,
            "I": 44,
            "m": true,
            "Z": "60",
            "Y": "60",
            "r": "NONE"
        }
    }))
    .unwrap();

    let (tls, server) = network::certs();
    let listener = network::listener();
    let address = listener.local_addr().unwrap();
    let server_handle = thread::spawn(move || {
        let mut s = network::accept(&listener, server);
        network::upgrade(&mut s, "/ws-api/v3");
        let request = network::json(&mut s);
        network::verify_signature(&request, 0);
        network::reply(
            &mut s,
            &request["id"],
            serde_json::json!({"subscriptionId": 0}),
        );
        network::frame(&mut s, 1, &ws_fill);
        let _ = s.read(&mut [0]);
    });
    let key = private::SigningKey::hmac(network::SECRET).unwrap();
    let mut client = private::PrivateClient::new(
        private::SessionConfig::new(network::scope(private::Market::Spot)),
        private::Authentication::SpotSignature {
            api_key: network::KEY,
            signing_key: &key,
        },
        private::NetworkConfig {
            stream_address: address,
            management_address: None,
            tls,
        },
        5,
    )
    .unwrap();
    let mut clock = network::TestClock {
        start: Instant::now(),
    };
    let mut permit = |_: &private::Charge, _: &private::ClockSample| true;
    let mut outcomes = Vec::new();
    let deadline = gateway::until();
    while outcomes.is_empty() {
        assert!(Instant::now() < deadline);
        client
            .poll(
                &mut clock,
                private::PollBudget::default(),
                &mut permit,
                |d| {
                    if let private::Delivery::Event(ev) = d {
                        outcomes.push(
                            ingress
                                .ingest(&mut gw, &ev)
                                .map_err(|_| private::Error::Sink)?,
                        );
                    }
                    Ok(())
                },
            )
            .unwrap();
        thread::yield_now();
    }
    client.stop(private::StopMode::Disconnect).unwrap();
    drop(client);
    server_handle.join().unwrap();

    assert!(matches!(
        outcomes[0],
        Outcome::Applied {
            applied: Applied::New,
            ..
        }
    ));
    let imported_key = OrderKey {
        intent: IntentKey {
            epoch: 2,
            intent_id: 7,
        },
        client_id: cid,
    };
    let order_view = gw.account().order(&imported_key).unwrap();
    assert!(order_view.terminal);
    assert_eq!(order_view.filled_quantity, gateway::fixture::f(100_000));

    drop(gw);
    let _ = peer.join().unwrap();
}

#[test]
fn usdm_cold_bootstrap_imports_shared_pool_position_and_reduce_only_order() {
    let (template, catalog, cmd) = gateway::fixture::futures_fixture();
    let domain = ClientIdDomain {
        risk_domain_id: template.scope.risk_domain_id,
        venue: template.scope.venue,
        environment: template.scope.environment,
        account: template.scope.account,
        product: v::Product::LinearPerpetual,
    };
    let sd = SelfDescribingClientId {
        logical_strategy_id: 8,
        leg: 0,
        epoch: 1,
        intent_id: 4,
    };
    let cid = sd.encode(&domain).unwrap();
    let manager = ManagerIdentity {
        instance: cmd.context.strategy.instance_id,
        generation: cmd.context.request.strategy_generation,
    };
    let managers = BTreeMap::from([(8u64, manager)]);

    let assets: Vec<_> = [("BTC", "0", "0"), ("USDT", "1000", "980")]
        .into_iter()
        .map(|(asset, wallet, available)| {
            serde_json::json!({
                "asset": asset,
                "walletBalance": wallet,
                "crossWalletBalance": wallet,
                "marginBalance": wallet,
                "availableBalance": available,
                "unrealizedProfit": "0",
                "maintMargin": "0",
                "initialMargin": "20",
                "positionInitialMargin": "20",
                "openOrderInitialMargin": "0",
                "crossUnPnl": "0"
            })
        })
        .collect();
    let account_json = serde_json::to_vec(&serde_json::json!({
        "canTrade": true,
        "multiAssetsMargin": false,
        "totalInitialMargin": "20",
        "totalMaintMargin": "0",
        "totalUnrealizedProfit": "0",
        "totalPositionInitialMargin": "20",
        "totalOpenOrderInitialMargin": "0",
        "totalCrossUnPnl": "0",
        "assets": assets,
        "positions": [{
            "symbol": "BTCUSDT",
            "positionSide": "BOTH",
            "positionAmt": "2",
            "entryPrice": "100",
            "unrealizedProfit": "0",
            "isolatedMargin": "0",
            "isolatedWallet": "0",
            "notional": "200",
            "initialMargin": "20",
            "maintMargin": "0"
        }]
    }))
    .unwrap();
    let open_orders_json = serde_json::to_vec(&serde_json::json!([{
        "symbol": "BTCUSDT",
        "orderId": 8801,
        "clientOrderId": cid.as_str(),
        "status": "NEW",
        "side": "SELL",
        "type": "LIMIT",
        "origQty": "1",
        "executedQty": "0",
        "price": "105",
        "timeInForce": "GTC",
        "positionSide": "BOTH",
        "reduceOnly": true
    }]))
    .unwrap();

    let boot = bootstrap_from_remote_snapshots(
        Market::Usdm,
        template,
        catalog,
        &managers,
        &account_json,
        &open_orders_json,
        true,
    )
    .unwrap();
    assert!(!boot.live.account().frozen());
    let (pos_qty, pos_owners) = boot
        .live
        .account()
        .position(12, PositionSide::Both)
        .unwrap();
    assert_eq!(pos_qty, gateway::fixture::units(2));
    assert_eq!(pos_owners.get(&0), Some(&gateway::fixture::units(2)));

    let order_key = OrderKey {
        intent: IntentKey {
            epoch: 1,
            intent_id: 4,
        },
        client_id: cid,
    };
    assert_eq!(
        boot.live
            .account()
            .order(&order_key)
            .unwrap()
            .reduce_allocation,
        gateway::fixture::units(1)
    );
    assert_eq!(
        boot.live.account().venue_shadow(1).unwrap(),
        gateway::fixture::units(980)
    );
}
