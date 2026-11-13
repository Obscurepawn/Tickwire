#[path = "../../tickwire-account-gateway/tests/common/mod.rs"]
mod common;

use common::fixture;
use std::collections::BTreeMap;
use tickwire_account::{Applied, IntentKey, ManagerIdentity, OrderKey};
use tickwire_account_gateway::{AccountGateway, Gate};
use tickwire_account_journal::InMemoryBackend;
use tickwire_recovery::{RuntimeBlobStore, SharedBlobStore};
use tickwire_runtime::authority::{LocalAuthority, RecoveryQueue};
use tickwire_spot_execution::{
    Credentials, RestSession, SpotEncoder, SpotVenue,
    recovery::{RemoteBalance, RemoteOrder, RestEvidence},
};
use tickwire_spot_runner::{AccountRunner, live_bootstrap::bootstrap_spot_from_rest_evidence};
use tickwire_venue::{
    self as v, ClientIdDomain, Environment, OrderStatus, OrderType, SelfDescribingClientId, Side,
    TimeInForce,
};

#[test]
fn okx_in_memory_cold_bootstrap_decodes_32_char_client_id_and_processes_live_fill() {
    let venue = SpotVenue::Okx;
    let (mut snapshot, mut catalog, source) = fixture::fixture();
    snapshot.scope.venue = venue.id();
    catalog.entries[0].instrument.symbol = v::Symbol::new("BTC-USDT").unwrap();

    let domain = ClientIdDomain {
        risk_domain_id: snapshot.scope.risk_domain_id,
        venue: snapshot.scope.venue,
        environment: snapshot.scope.environment,
        account: snapshot.scope.account,
        product: v::Product::Spot,
    };
    let sd = SelfDescribingClientId {
        logical_strategy_id: 8,
        leg: 0,
        epoch: 2,
        intent_id: 9,
    };
    let cid = sd.encode(&domain).unwrap();
    assert_eq!(cid.as_str().len(), 32);

    let manager = ManagerIdentity {
        instance: source.context.strategy.instance_id,
        generation: source.context.request.strategy_generation,
    };
    let managers = BTreeMap::from([(8u64, manager)]);

    // OKX REST snapshot:
    // - 1 BTC available, 800 USDT available, 200 USDT locked = 60 managed (0.001 @ 60000) + 140 manual
    let evidence = RestEvidence {
        venue,
        environment: Environment::Test,
        uid: "123456".to_owned(),
        start_ms: 1000,
        end_ms: 2000,
        balances: vec![
            RemoteBalance {
                symbol: "BTC".to_owned(),
                total: "1".parse().unwrap(),
                available: "1".parse().unwrap(),
                locked: "0".parse().unwrap(),
            },
            RemoteBalance {
                symbol: "USDT".to_owned(),
                total: "1000".parse().unwrap(),
                available: "800".parse().unwrap(),
                locked: "200".parse().unwrap(),
            },
        ],
        open_orders: vec![
            RemoteOrder {
                symbol: "BTC-USDT".to_owned(),
                native_id: "77001".to_owned(),
                client_id: cid.as_str().to_owned(),
                side: Side::Buy,
                status: OrderStatus::New,
                order_type: OrderType::Limit,
                time_in_force: Some(TimeInForce::Gtc),
                qty: "0.001".parse().unwrap(),
                cumulative: "0".parse().unwrap(),
                price: Some("60000".parse().unwrap()),
                created_ms: 1500,
                updated_ms: 1500,
            },
            RemoteOrder {
                symbol: "BTC-USDT".to_owned(),
                native_id: "77002".to_owned(),
                client_id: "manualokxuiorder".to_owned(),
                side: Side::Buy,
                status: OrderStatus::New,
                order_type: OrderType::Limit,
                time_in_force: Some(TimeInForce::Gtc),
                qty: "0.002".parse().unwrap(),
                cumulative: "0".parse().unwrap(),
                price: Some("70000".parse().unwrap()),
                created_ms: 1500,
                updated_ms: 1500,
            },
        ],
        history: vec![],
        target_orders: vec![],
        fills: vec![],
        fingerprint: [9; 32],
        evidence_bytes: vec![1],
    };

    let boot =
        bootstrap_spot_from_rest_evidence(snapshot, catalog, &managers, &evidence, "123456", false)
            .unwrap();
    assert_eq!(boot.live.external_open_orders(), 1);
    assert_eq!(boot.origins.len(), 1);

    let encoder = SpotEncoder::new(
        Credentials::okx("key", b"secret", "pass"),
        Environment::Test,
        2,
        || Ok(1_700_000_000_000),
    )
    .unwrap();
    let mut gw_cfg = common::config(boot.live.baseline());
    gw_cfg.route = encoder.route();
    let (connection, peer) = common::peer(&gw_cfg.route.authority, 65536);

    let queue = RecoveryQueue::new(4, 1024 * 1024).unwrap();
    let objects = SharedBlobStore::new(RuntimeBlobStore::in_memory());
    let authority = LocalAuthority::new(
        objects.clone(),
        queue.clone(),
        |_: &v::CanonicalCommand, _: &tickwire_account::Admission| {
            Ok(Gate {
                now: 61,
                lease_until: 1000,
                writer_epoch: 4,
                strategy_generation: 5,
                config_revision: 6,
                owner_isolated: true,
                action_permitted: true,
                market_valid: true,
            })
        },
        [2; 16],
    );

    let gateway = AccountGateway::create_from_live_exchange(
        InMemoryBackend::new(),
        gw_cfg,
        boot.live,
        authority,
        encoder,
        connection,
    )
    .unwrap();

    let mut runner = AccountRunner::new_with_imported_origins(
        RestSession::new(gateway, 1).unwrap(),
        queue,
        objects,
        &boot.origins,
    )
    .unwrap();

    // Verify 60 USDT managed order reserved + 140 USDT nontrade_locked leaves 800 USDT available out of 1000 total
    let usdt = runner.account().asset(fixture::key(2)).unwrap();
    assert_eq!(usdt.total, fixture::units(1000));
    assert_eq!(usdt.available, fixture::units(800));
    assert_eq!(usdt.reserved, fixture::units(60));

    // Deliver a live OKX orders-channel execution fill for the cold-imported 32-char clientOrderId!
    let okx_fill = serde_json::to_vec(&serde_json::json!({
        "instType": "SPOT",
        "instId": "BTC-USDT",
        "ordId": "77001",
        "clOrdId": cid.as_str(),
        "tradeId": "90001",
        "side": "buy",
        "fillPx": "60000",
        "fillSz": "0.00100000",
        "fillFee": "0",
        "fillFeeCcy": "BTC",
        "fillTime": "1700000001000",
        "execType": "M"
    }))
    .unwrap();

    let applied = runner
        .record_fill(cid.as_str(), [41; 16], 1, 1, 100, &okx_fill)
        .unwrap();
    assert_eq!(applied, Applied::New);

    let key = OrderKey {
        intent: IntentKey {
            epoch: 2,
            intent_id: 9,
        },
        client_id: cid,
    };
    let order = runner.account().order(&key).unwrap();
    assert_eq!(order.filled_quantity, fixture::f(100_000));

    drop(runner);
    let _ = peer.join().unwrap();
}
