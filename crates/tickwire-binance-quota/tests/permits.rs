use tickwire_binance_clock as time;
use tickwire_binance_private as private;
use tickwire_binance_public as public;
use tickwire_binance_quota::*;
use tickwire_quota::{
    BucketId, ConsumerId, Coordinator, MockQuotaStore, ScopeKind, WindowConfig, WindowKey,
};
#[path = "../../tickwire-venue/tests/common/mod.rs"]
mod venue_fixture;
fn now(n: u64) -> MonoTime {
    MonoTime::new(ClockDomain::new([1; 16], 1).unwrap(), n)
}
fn key() -> WindowKey {
    WindowKey {
        bucket: BucketId {
            namespace: [2; 16],
            scope: ScopeKind::Ip,
            subject: [3; 16],
            pool: 1,
        },
        window: 1,
    }
}
fn rule(action: Action, weight: u32) -> Rule {
    Rule {
        source: Source::SpotTest,
        action,
        class: Class::Normal,
        api_weight: weight,
        websocket_messages: 0,
        costs: vec![Cost {
            window: key(),
            units: 1,
        }],
    }
}
fn endpoint(rules: Vec<Rule>) -> Endpoint {
    let mut c = Coordinator::open(MockQuotaStore::default(), 1, now(0)).unwrap();
    c.add_window(
        WindowConfig {
            key: key(),
            revision: 1,
            starts_ns: 0,
            ends_ns: 200,
            limit: 10,
            recovery_reserve: 0,
        },
        now(0),
    )
    .unwrap();
    let mut e = c
        .register_consumer(
            ConsumerId {
                instance: [4; 16],
                generation: 1,
            },
            190,
            now(0),
        )
        .unwrap();
    let grant = c
        .issue(
            1,
            &e,
            Class::Normal,
            &[Cost {
                window: key(),
                units: 8,
            }],
            180,
            now(0),
        )
        .unwrap();
    e.install(grant, now(0)).unwrap();
    Endpoint::new(
        e,
        [5; 16],
        now(0).domain(),
        Policy {
            generation: 1,
            starts_ns: 0,
            ends_ns: 100,
            rules,
        },
    )
    .unwrap()
}
#[test]
fn time_query_is_idempotent_but_policy_expiry_is_rechecked() {
    let mut e = endpoint(vec![rule(Action::TimeQuery, 1)]);
    let b = TimeBinding {
        owner_id: [6; 16],
        source: Source::SpotTest,
        domain: now(0).domain(),
    };
    let c = time::TimeCharge {
        source: b.source,
        domain: b.domain,
        connection_epoch: 1,
        kind: time::ChargeKind::Query,
        api_weight: 1,
    };
    assert_eq!(e.consume_time(&b, &c, now(1)), Ok(Consumed::New));
    assert_eq!(e.consume_time(&b, &c, now(2)), Ok(Consumed::Duplicate));
    assert_eq!(e.remaining(key(), Class::Normal), 7);
    assert!(e.consume_time(&b, &c, now(100)).is_err());
}
#[test]
fn public_and_private_owner_identities_share_one_bucket() {
    let action = Action::Public {
        lane: public::Lane::Snapshot,
        kind: public::ChargeKind::Snapshot,
    };
    let mut e = endpoint(vec![rule(action, 5), rule(Action::SpotStatus, 2)]);
    let b = PublicBinding {
        owner_id: [7; 16],
        source: Source::SpotTest,
        domain: now(0).domain(),
    };
    let c = public::PublicCharge {
        owner_id: b.owner_id,
        source: b.source,
        domain: b.domain,
        lane: public::Lane::Snapshot,
        connection_epoch: 1,
        operation: 1,
        kind: public::ChargeKind::Snapshot,
        api_weight: 5,
    };
    assert_eq!(e.consume_public(&b, &c, now(1)), Ok(Consumed::New));
    let b = PrivateBinding {
        owner_id: [8; 16],
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
    let c = private::Charge {
        scope: b.scope.clone(),
        connection_epoch: 1,
        attempt: 1,
        kind: private::ChargeKind::Api(private::Operation::SpotStatus),
        bytes: 12,
        digest: [9; 32],
        api_weight: 2,
        websocket_messages: 0,
    };
    assert_eq!(e.consume_private(&b, &c, now(1)), Ok(Consumed::New));
    assert_eq!(e.remaining(key(), Class::Normal), 6);
}
#[test]
fn command_classification_uses_the_original_immutable_command() {
    let mut e = endpoint(vec![rule(
        Action::Command {
            transport: Transport::Rest,
            action: CommandAction::New,
        },
        0,
    )]);
    let c = tickwire_venue::CanonicalCommand::new(venue_fixture::command()).unwrap();
    let b = CommandBinding {
        owner_id: [10; 16],
        domain: now(0).domain(),
        risk_domain_id: [1; 16],
        account: 2,
        product: tickwire_venue::Product::Spot,
        environment: tickwire_venue::Environment::Test,
    };
    assert_eq!(
        e.consume_command(&b, &c, Transport::Rest, now(60)),
        Ok(Consumed::New)
    );
    assert_eq!(
        e.consume_command(&b, &c, Transport::Rest, now(61)),
        Ok(Consumed::Duplicate)
    );
    let mut changed = c.command().clone();
    changed.context.request.config_revision += 1;
    let changed = tickwire_venue::CanonicalCommand::new(changed).unwrap();
    assert!(
        e.consume_command(&b, &changed, Transport::Rest, now(62))
            .is_err()
    );
}
#[test]
fn actual_permit_traits_share_the_owner_and_http_ban_freezes_every_lane() {
    use private::ControlPermit as _;
    use public::PublicPermit as _;
    use time::TimePermit as _;
    let action = Action::Public {
        lane: public::Lane::Snapshot,
        kind: public::ChargeKind::Snapshot,
    };
    let mut private_rule = rule(Action::SpotStatus, 2);
    private_rule.websocket_messages = 1;
    let mut e = endpoint(vec![
        rule(Action::TimeQuery, 1),
        rule(action, 5),
        private_rule,
    ]);
    let b = TimeBinding {
        owner_id: [6; 16],
        source: Source::SpotTest,
        domain: now(0).domain(),
    };
    let tc = time::TimeCharge {
        source: b.source,
        domain: b.domain,
        connection_epoch: 1,
        kind: time::ChargeKind::Query,
        api_weight: 1,
    };
    let local = time::LocalTime {
        instant: std::time::Instant::now(),
        monotonic: now(1),
    };
    assert!(e.time(b).allow(&tc, &local));
    let pb = PrivateBinding {
        owner_id: [8; 16],
        domain: b.domain,
        scope: private::AccountScope {
            risk_domain_id: [1; 16],
            venue: 1,
            environment: private::Environment::Test,
            account: 2,
            market: private::Market::Spot,
            credential_generation: 1,
        },
    };
    let pc = private::Charge {
        scope: pb.scope.clone(),
        connection_epoch: 1,
        attempt: 1,
        kind: private::ChargeKind::Api(private::Operation::SpotStatus),
        bytes: 12,
        digest: [9; 32],
        api_weight: 2,
        websocket_messages: 1,
    };
    assert!(e.private(pb).allow(
        &pc,
        &private::ClockSample {
            instant: local.instant,
            monotonic: now(1),
            signing_time: None
        }
    ));
    let pub_binding = PublicBinding {
        owner_id: [7; 16],
        source: b.source,
        domain: b.domain,
    };
    let charge = public::PublicCharge {
        owner_id: pub_binding.owner_id,
        source: b.source,
        domain: b.domain,
        lane: public::Lane::Snapshot,
        connection_epoch: 1,
        operation: 1,
        kind: public::ChargeKind::Snapshot,
        api_weight: 5,
    };
    assert!(e.public(pub_binding).allow(&charge, now(1)));
    assert_eq!(e.remaining(key(), Class::Normal), 5);
    e.public(pub_binding).observe_http(&public::HttpFeedback {
        charge,
        status: 429,
        headers: vec![],
    });
    assert!(e.is_frozen());
    assert!(!e.time(b).allow(&tc, &local));
}
#[test]
fn wrong_binding_and_reported_cost_never_consume_quota() {
    let mut e = endpoint(vec![rule(Action::TimeQuery, 1)]);
    let b = TimeBinding {
        owner_id: [6; 16],
        source: Source::SpotTest,
        domain: now(0).domain(),
    };
    let mut c = time::TimeCharge {
        source: b.source,
        domain: b.domain,
        connection_epoch: 1,
        kind: time::ChargeKind::Query,
        api_weight: 2,
    };
    assert_eq!(e.consume_time(&b, &c, now(1)), Err(Error::Identity));
    c.api_weight = 1;
    c.source = Source::SpotProduction;
    assert_eq!(e.consume_time(&b, &c, now(1)), Err(Error::Identity));
    c.source = b.source;
    c.kind = time::ChargeKind::Connect;
    assert_eq!(e.consume_time(&b, &c, now(1)), Err(Error::Policy));
    assert_eq!(e.remaining(key(), Class::Normal), 8);
}
#[test]
fn identity_capacity_never_evicts_a_successful_attempt() {
    let mut e = endpoint(vec![rule(Action::TimeQuery, 1)]);
    let b = TimeBinding {
        owner_id: [6; 16],
        source: Source::SpotTest,
        domain: now(0).domain(),
    };
    let charge = |epoch| time::TimeCharge {
        source: b.source,
        domain: b.domain,
        connection_epoch: epoch,
        kind: time::ChargeKind::Query,
        api_weight: 1,
    };
    assert_eq!(e.consume_time(&b, &charge(1), now(1)), Ok(Consumed::New));
    for i in 2..=MAX_IDENTITIES as u64 {
        let _ = e.consume_time(&b, &charge(i), now(1));
    }
    assert_eq!(
        e.consume_time(&b, &charge(MAX_IDENTITIES as u64 + 1), now(1)),
        Err(Error::Capacity)
    );
    assert_eq!(
        e.consume_time(&b, &charge(1), now(1)),
        Ok(Consumed::Duplicate)
    );
}
