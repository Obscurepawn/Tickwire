use std::net::{IpAddr, Ipv4Addr};
use tickwire_binance_quota::bootstrap::{MultiIpBootstrapConfig, Source as BootSource};
use tickwire_binance_quota::{
    ActionBudgetClass, CommandAction, CommandBinding, Consumed, IpKey, MultiIpCommandQuota,
    MultiIpQuotaError, RateReject, ReadAction, Source, Transport, classify_command_action,
};
use tickwire_clock::{ClockDomain, MonoTime};
use tickwire_venue::{
    BoundedList, CanonicalCommand, CommandBody, Environment, ListKind, ListOrder, Product,
};

#[path = "../../tickwire-venue/tests/common/mod.rs"]
mod venue_fixture;

const MS: u64 = 1_000_000;
const SEC: u64 = 1_000_000_000;

fn domain() -> ClockDomain {
    ClockDomain::new([11; 16], 1).unwrap()
}

fn mono(ns: u64) -> MonoTime {
    MonoTime::new(domain(), ns)
}

fn ip(last: u8) -> IpKey {
    IpKey::from_v4(Ipv4Addr::new(10, 40, 0, last))
}

fn spot_binding() -> CommandBinding {
    CommandBinding {
        owner_id: [1; 16],
        risk_domain_id: [1; 16],
        account: 2,
        product: Product::Spot,
        environment: Environment::Test,
        domain: domain(),
    }
}

fn usdm_binding() -> CommandBinding {
    CommandBinding {
        owner_id: [2; 16],
        risk_domain_id: [1; 16],
        account: 2,
        product: Product::LinearPerpetual,
        environment: Environment::Test,
        domain: domain(),
    }
}

#[test]
fn spot_and_usdm_command_actions_isolate_cancels_and_queries_from_order_caps() {
    let ip_a = ip(1);
    let ip_b = ip(2);

    // Verify Spot vs USDM Amend / Modify classification rules.
    assert_eq!(
        classify_command_action(
            Source::SpotProduction,
            Transport::Rest,
            CommandAction::Amend
        ),
        Ok(ActionBudgetClass::Modify {
            counts_as_order: false,
            ip_weight: 4,
        })
    );
    assert_eq!(
        classify_command_action(
            Source::SpotProduction,
            Transport::Rest,
            CommandAction::Modify
        ),
        Err(MultiIpQuotaError::UnsupportedAction)
    );
    assert_eq!(
        classify_command_action(
            Source::UsdmProduction,
            Transport::Rest,
            CommandAction::Modify
        ),
        Ok(ActionBudgetClass::Modify {
            counts_as_order: true,
            ip_weight: 1,
        })
    );
    assert_eq!(
        classify_command_action(
            Source::UsdmProduction,
            Transport::WebSocket,
            CommandAction::Modify
        ),
        Ok(ActionBudgetClass::Modify {
            counts_as_order: true,
            ip_weight: 0,
        })
    );

    // 1. Spot: exhaust 100/10s ORDER cap across New, CancelReplace, PeggedNew, Sor.
    let mut spot = MultiIpCommandQuota::new(spot_binding(), &[ip_a, ip_b]).unwrap();
    for i in 0..25u64 {
        let t = i * MS;
        spot.consume_for_ip(ip_a, Transport::Rest, CommandAction::New, 12, t)
            .unwrap();
        spot.consume_for_ip(
            ip_b,
            Transport::WebSocket,
            CommandAction::CancelReplace,
            12,
            t,
        )
        .unwrap();
        spot.consume_for_ip(ip_a, Transport::Rest, CommandAction::PeggedNew, 12, t)
            .unwrap();
        spot.consume_for_ip(ip_b, Transport::WebSocket, CommandAction::Sor, 12, t)
            .unwrap();
    }
    assert!(matches!(
        spot.consume_for_ip(ip_a, Transport::Rest, CommandAction::New, 12, 100 * MS),
        Err(MultiIpQuotaError::Rate(RateReject::Exhausted {
            limit: 100,
            ..
        }))
    ));

    // Pure cancels, Spot Amend (keepPriority), CommandAction::Query, and ReadAction all succeed!
    spot.consume_for_ip(ip_a, Transport::Rest, CommandAction::Cancel, 12, 101 * MS)
        .unwrap();
    spot.consume_for_ip(
        ip_b,
        Transport::WebSocket,
        CommandAction::CancelList,
        12,
        102 * MS,
    )
    .unwrap();
    spot.consume_for_ip(ip_a, Transport::Rest, CommandAction::Amend, 12, 103 * MS)
        .unwrap();
    spot.consume_for_ip(ip_b, Transport::Rest, CommandAction::Query, 12, 104 * MS)
        .unwrap();
    spot.consume_read_for_ip(ip_a, ReadAction::OpenOrders, 105 * MS)
        .unwrap();

    // 2. USD-M: exhaust 300/10s ORDER cap across New, Modify, and Algo.
    let mut usdm = MultiIpCommandQuota::new(usdm_binding(), &[ip_a, ip_b]).unwrap();
    for i in 0..100u64 {
        let t = i * MS;
        usdm.consume_for_ip(ip_a, Transport::Rest, CommandAction::New, 12, t)
            .unwrap();
        usdm.consume_for_ip(ip_b, Transport::WebSocket, CommandAction::Modify, 12, t)
            .unwrap();
        usdm.consume_for_ip(ip_a, Transport::WebSocket, CommandAction::Algo, 12, t)
            .unwrap();
    }
    assert!(matches!(
        usdm.consume_for_ip(ip_a, Transport::WebSocket, CommandAction::New, 12, 200 * MS),
        Err(MultiIpQuotaError::Rate(RateReject::Exhausted {
            limit: 300,
            ..
        }))
    ));
    assert!(matches!(
        usdm.consume_for_ip(ip_b, Transport::Rest, CommandAction::Modify, 12, 200 * MS),
        Err(MultiIpQuotaError::Rate(RateReject::Exhausted {
            limit: 300,
            ..
        }))
    ));

    // USD-M Cancel, CancelAlgo, Query, and ReadAction::Income continue without interruption!
    usdm.consume_for_ip(
        ip_a,
        Transport::WebSocket,
        CommandAction::Cancel,
        12,
        201 * MS,
    )
    .unwrap();
    usdm.consume_for_ip(
        ip_b,
        Transport::Rest,
        CommandAction::CancelAlgo,
        12,
        202 * MS,
    )
    .unwrap();
    usdm.consume_for_ip(ip_a, Transport::Rest, CommandAction::Query, 12, 203 * MS)
        .unwrap();
    usdm.consume_read_for_ip(ip_b, ReadAction::Income, 204 * MS)
        .unwrap();
}

#[test]
fn spot_list_debits_exact_leg_count_and_rolls_back_on_ip_weight_exhaustion() {
    let ip_a = ip(10);
    let ip_b = ip(11);
    let mut spot = MultiIpCommandQuota::new(spot_binding(), &[ip_a, ip_b]).unwrap();

    // Exhaust ip_a's 6,000/60s weight via 150 reconciliation snapshots (150 * 40 = 6000).
    for i in 0..150u64 {
        spot.consume_read_for_ip(ip_a, ReadAction::ConditionalOrders, i * MS)
            .unwrap();
    }
    let ip_a_id = spot.venue_limiter().ip_window_id(ip_a).unwrap();
    let ord_10s = spot.venue_limiter().account_order_window_ids()[0];
    let ord_day = spot.venue_limiter().account_order_window_ids()[1];
    assert_eq!(
        spot.venue_limiter().limiter().window_used(ip_a_id, SEC),
        Some(6_000)
    );
    assert_eq!(
        spot.venue_limiter().limiter().window_used(ord_10s, SEC),
        Some(0)
    );

    // Build an authenticated 3-leg OTOCO CanonicalCommand.
    let mut raw = venue_fixture::command();
    raw.context.request.deadline = 30 * SEC;
    let mut leg1 = venue_fixture::order();
    leg1.client_id = venue_fixture::text("otoco-leg-1");
    let mut leg2 = venue_fixture::order();
    leg2.client_id = venue_fixture::text("otoco-leg-2");
    let mut leg3 = venue_fixture::order();
    leg3.client_id = venue_fixture::text("otoco-leg-3");
    raw.body = CommandBody::SpotList(ListOrder {
        list_id: [8; 16],
        client_id: venue_fixture::text("otoco-list-1"),
        kind: ListKind::Otoco,
        legs: BoundedList::new(vec![leg1, leg2, leg3]).unwrap(),
    });
    let otoco = CanonicalCommand::new(raw).unwrap();

    // Attempting OTOCO on exhausted ip_a must fail AND roll back the 3 order units on 10s & 86400s!
    assert!(
        spot.can_afford_ip(
            ip_a,
            Transport::Rest,
            CommandAction::SpotList {
                kind: ListKind::Otoco,
                legs: 3,
                advanced: false,
            },
            12,
            SEC,
        )
        .is_err()
    );
    assert!(matches!(
        spot.consume_command_for_ip(ip_a, &otoco, Transport::Rest, mono(SEC)),
        Err(MultiIpQuotaError::Rate(RateReject::Exhausted {
            limit: 6_000,
            ..
        }))
    ));
    assert_eq!(
        spot.venue_limiter().limiter().window_used(ord_10s, SEC),
        Some(0)
    );
    assert_eq!(
        spot.venue_limiter().limiter().window_used(ord_day, SEC),
        Some(0)
    );

    // Failing over to sibling ip_b admits the OTOCO, charging exactly 3 order units and 1 IP weight!
    assert_eq!(
        spot.consume_command_for_ip(ip_b, &otoco, Transport::Rest, mono(SEC)),
        Ok(Consumed::New)
    );
    let ip_b_id = spot.venue_limiter().ip_window_id(ip_b).unwrap();
    assert_eq!(
        spot.venue_limiter().limiter().window_used(ord_10s, SEC),
        Some(3)
    );
    assert_eq!(
        spot.venue_limiter().limiter().window_used(ord_day, SEC),
        Some(3)
    );
    assert_eq!(
        spot.venue_limiter().limiter().window_used(ip_b_id, SEC),
        Some(1)
    );
}

#[test]
fn zero_byte_retry_is_idempotent_per_intent_and_ip_while_429_vs_418_isolate_properly() {
    let addr_a = IpAddr::V4(Ipv4Addr::new(10, 50, 0, 1));
    let addr_b = IpAddr::V4(Ipv4Addr::new(10, 50, 0, 2));
    let ip_a = IpKey::from_ip(addr_a);
    let ip_b = IpKey::from_ip(addr_b);

    let cfg = MultiIpBootstrapConfig {
        owner_id: [1; 16],
        risk_domain_id: [1; 16],
        account: 2,
        source: BootSource::SpotTest,
        local_ips: vec![addr_a, addr_b],
        exch_warn_pct: 80,
    };
    let mut quota = cfg.build(domain()).unwrap();

    let mut raw = venue_fixture::command();
    raw.context.request.deadline = 60 * SEC;
    let cmd = CanonicalCommand::new(raw.clone()).unwrap();

    // First send on ip_a charges 1 order + 1 IP weight.
    assert_eq!(
        quota.consume_command_for_ip(ip_a, &cmd, Transport::Rest, mono(SEC)),
        Ok(Consumed::New)
    );
    // Zero-byte WouldBlock retry on the same (intent_exec_epoch, intent_id, Rest, ip_a) is Duplicate!
    assert_eq!(
        quota.consume_command_for_ip(ip_a, &cmd, Transport::Rest, mono(SEC + 5 * MS)),
        Ok(Consumed::Duplicate)
    );
    let ord_10s = quota.venue_limiter().account_order_window_ids()[0];
    let ip_a_id = quota.venue_limiter().ip_window_id(ip_a).unwrap();
    assert_eq!(
        quota
            .venue_limiter()
            .limiter()
            .window_used(ord_10s, SEC + 5 * MS),
        Some(1)
    );
    assert_eq!(
        quota
            .venue_limiter()
            .limiter()
            .window_used(ip_a_id, SEC + 5 * MS),
        Some(1)
    );

    // HTTP 429 on ip_a cools down ONLY ip_a until t = 15s; ip_b stays live!
    quota.observe_http_status(ip_a, 429, 15 * SEC).unwrap();
    assert!(matches!(
        quota.consume_command_for_ip(ip_a, &cmd, Transport::Rest, mono(2 * SEC)),
        Err(MultiIpQuotaError::Rate(RateReject::IpCooldown {
            local_ip,
            ..
        })) if local_ip == ip_a
    ));

    // Re-routing the same intent to healthy ip_b succeeds as Consumed::New on ip_b, then deduplicates on ip_b!
    assert_eq!(
        quota.consume_command_for_ip(ip_b, &cmd, Transport::Rest, mono(2 * SEC)),
        Ok(Consumed::New)
    );
    assert_eq!(
        quota.consume_command_for_ip(ip_b, &cmd, Transport::Rest, mono(2 * SEC + MS)),
        Ok(Consumed::Duplicate)
    );

    // Header reconciliation on ip_b triggers warn at >= 80% (4800 / 6000).
    let report = quota
        .reconcile_ip_weight_header(ip_b, 4_800, 3 * SEC)
        .unwrap();
    assert!(report.warn);
    assert!(report.newly_warned);
    assert_eq!(report.effective_used, 4_800);

    let ord_report = quota.reconcile_order_count_header(10, 80, 3 * SEC).unwrap();
    assert!(ord_report.warn);
    assert_eq!(ord_report.effective_used, 80);

    // At t = 15s, ip_a recovers from its 429 cooldown!
    quota
        .consume_for_ip(ip_a, Transport::Rest, CommandAction::Cancel, 12, 15 * SEC)
        .unwrap();

    // Conflicting command digest on an existing (intent_exec_epoch, intent_id) freezes the account!
    let mut mutated = raw;
    mutated.context.request.config_revision += 1;
    let conflicting = CanonicalCommand::new(mutated).unwrap();
    assert_eq!(
        quota.consume_command_for_ip(ip_b, &conflicting, Transport::Rest, mono(16 * SEC)),
        Err(MultiIpQuotaError::Conflict)
    );
    assert!(quota.is_account_banned());

    // Separate check: explicit HTTP 418 also bans the entire account across all IPs.
    let mut quota2 = cfg.build(domain()).unwrap();
    quota2.observe_http_status(ip_a, 418, 120 * SEC).unwrap();
    assert!(quota2.is_account_banned());
    assert_eq!(
        quota2.consume_for_ip(ip_b, Transport::Rest, CommandAction::Cancel, 12, 20 * SEC),
        Err(MultiIpQuotaError::AccountBanned)
    );
}
