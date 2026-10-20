use super::*;
use quota::{
    BucketId, ConsumerId, Coordinator, MockQuotaStore, ScopeKind, WindowConfig, WindowKey,
};
fn now(n: u64) -> MonoTime {
    MonoTime::new(ClockDomain::new([1; 16], 1).unwrap(), n)
}
fn fixture() -> (Endpoint, ReadBinding, WindowKey) {
    let key = WindowKey {
        bucket: BucketId {
            namespace: [2; 16],
            scope: ScopeKind::Account,
            subject: [3; 16],
            pool: 1,
        },
        window: 1,
    };
    let mut c = Coordinator::open(MockQuotaStore::default(), 1, now(0)).unwrap();
    c.add_window(
        WindowConfig {
            key,
            revision: 1,
            starts_ns: 0,
            ends_ns: 1000,
            limit: 20,
            recovery_reserve: 10,
        },
        now(0),
    )
    .unwrap();
    let mut limiter = c
        .register_consumer(
            ConsumerId {
                instance: [4; 16],
                generation: 1,
            },
            900,
            now(0),
        )
        .unwrap();
    let grant = c
        .issue(
            1,
            &limiter,
            Class::Recovery,
            &[Cost {
                window: key,
                units: 10,
            }],
            800,
            now(0),
        )
        .unwrap();
    limiter.install(grant, now(0)).unwrap();
    let policy = Policy {
        generation: 1,
        starts_ns: 0,
        ends_ns: 700,
        rules: [
            ReadAction::Connect,
            ReadAction::TradePage,
            ReadAction::AccountSnapshot,
            ReadAction::OpenOrders,
            ReadAction::ConditionalOrders,
            ReadAction::OrderQuery,
        ]
        .into_iter()
        .map(|a| Rule {
            source: Source::SpotTest,
            action: Action::Read(a),
            class: Class::Recovery,
            api_weight: 0,
            websocket_messages: 0,
            costs: vec![Cost {
                window: key,
                units: 1,
            }],
        })
        .collect(),
    };
    let binding = ReadBinding {
        owner_id: [5; 16],
        domain: now(0).domain(),
        scope: private::AccountScope {
            risk_domain_id: [1; 16],
            venue: 1,
            environment: private::Environment::Test,
            account: 2,
            market: private::Market::Spot,
            credential_generation: 1,
        },
    };
    (
        Endpoint::new(limiter, [6; 16], now(0).domain(), policy).unwrap(),
        binding,
        key,
    )
}
#[test]
fn read_attempts_dedupe_per_connection_and_bind_the_complete_query() {
    let (mut endpoint, b, key) = fixture();
    let query = read::TradeQuery {
        intent: tickwire_account::IntentKey {
            epoch: 1,
            intent_id: 1,
        },
        order_id: [7; 16],
        from_id: 0,
        limit: 16,
        private_connection_epoch: 2,
        gap_generation: 3,
    };
    let q = ReadQuery::Trade(query);
    assert_eq!(
        endpoint.consume_read(&b, &b.scope, q, 1, read::ReadOperation::Connect, now(1)),
        Ok(Consumed::New)
    );
    assert_eq!(
        endpoint.consume_read(&b, &b.scope, q, 1, read::ReadOperation::TradePage, now(2)),
        Ok(Consumed::New)
    );
    assert_eq!(
        endpoint.consume_read(&b, &b.scope, q, 1, read::ReadOperation::TradePage, now(3)),
        Ok(Consumed::Duplicate)
    );
    assert_eq!(
        endpoint.consume_read(&b, &b.scope, q, 2, read::ReadOperation::TradePage, now(4)),
        Ok(Consumed::New)
    );
    assert_eq!(endpoint.remaining(key, Class::Recovery), 7);
    let mut changed = query;
    changed.from_id = 1;
    assert!(
        endpoint
            .consume_read(
                &b,
                &b.scope,
                ReadQuery::Trade(changed),
                1,
                read::ReadOperation::TradePage,
                now(5)
            )
            .is_err()
    );
    assert!(endpoint.is_frozen());
}
#[test]
fn snapshot_permit_uses_explicit_endpoint_rule_and_scope() {
    use read::bootstrap::{SnapshotKind, SnapshotPermit as _, SnapshotQuery};
    let (mut endpoint, b, key) = fixture();
    let q = SnapshotQuery {
        kind: SnapshotKind::ConditionalOrders,
        private_connection_epoch: 2,
        gap_generation: 3,
    };
    let sample = private::ClockSample {
        instant: std::time::Instant::now(),
        monotonic: now(1),
        signing_time: None,
    };
    assert!(
        endpoint
            .read(b.clone())
            .allow(&b.scope, q, 1, read::ReadOperation::Connect, &sample)
    );
    assert!(endpoint.read(b.clone()).allow(
        &b.scope,
        q,
        1,
        read::ReadOperation::TradePage,
        &sample
    ));
    assert!(endpoint.read(b.clone()).allow(
        &b.scope,
        q,
        1,
        read::ReadOperation::TradePage,
        &sample
    ));
    assert_eq!(endpoint.remaining(key, Class::Recovery), 8);
    let mut wrong = b.scope.clone();
    wrong.account += 1;
    assert!(
        !endpoint
            .read(b)
            .allow(&wrong, q, 2, read::ReadOperation::Connect, &sample)
    );
}
#[test]
fn order_reader_uses_its_own_attempt_namespace() {
    use read::orders::{OrderPermit as _, OrderQuery};
    let (mut endpoint, b, key) = fixture();
    let query = OrderQuery {
        intent: tickwire_account::IntentKey {
            epoch: 1,
            intent_id: 1,
        },
        order_id: [7; 16],
        private_connection_epoch: 2,
        gap_generation: 3,
    };
    let sample = private::ClockSample {
        instant: std::time::Instant::now(),
        monotonic: now(1),
        signing_time: None,
    };
    assert!(endpoint.read(b.clone()).allow(
        &b.scope,
        &query,
        1,
        read::ReadOperation::Connect,
        &sample
    ));
    assert!(endpoint.read(b.clone()).allow(
        &b.scope,
        &query,
        1,
        read::ReadOperation::TradePage,
        &sample
    ));
    assert!(endpoint.read(b.clone()).allow(
        &b.scope,
        &query,
        1,
        read::ReadOperation::TradePage,
        &sample
    ));
    assert_eq!(endpoint.remaining(key, Class::Recovery), 8);
}
