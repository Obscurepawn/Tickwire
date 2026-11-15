use std::net::{IpAddr, Ipv4Addr};
use std::sync::atomic::{AtomicUsize, Ordering};
use tickwire_binance_quota::bootstrap::{MultiIpBootstrapConfig, Source as BootSource};
use tickwire_binance_quota::{
    AccountRateOverrides, ActionBudgetClass, BinanceMarket, CommandAction, CommandBinding,
    Consumed, ExchRateLimitLevel, IpBudgetSpec, IpKey, MultiIpCommandQuota, MultiIpQuotaError,
    RateReject, ReadAction, RouteAdmissionReject, SharedBinanceOrsQuota, Source, Transport,
    classify_command_action, classify_market_command_action,
};
use tickwire_clock::{ClockDomain, MonoTime};
use tickwire_venue::{
    BoundedList, CanonicalCommand, CommandBody, Environment, ListKind, ListOrder, OrderRoute,
    Product,
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

#[test]
fn shared_binance_ors_quota_isolates_all_four_markets_shares_ip_across_accounts_and_nacks_on_single_pick()
 {
    let ors_quota = SharedBinanceOrsQuota::new();
    let addr_shared = IpAddr::V4(Ipv4Addr::new(10, 60, 0, 1));
    let addr_spare = IpAddr::V4(Ipv4Addr::new(10, 60, 0, 2));
    let ip_shared = IpKey::from_ip(addr_shared);
    let ip_spare = IpKey::from_ip(addr_spare);

    // Install tight Spot IP weight cap (4/60s) on `addr_shared` and default (6000/60s) on `addr_spare`.
    ors_quota
        .install_ip_budgets(
            BinanceMarket::Spot,
            &[
                IpBudgetSpec::new(addr_shared, 4),
                IpBudgetSpec::new(addr_spare, 0),
            ],
        )
        .unwrap();

    // Register Account 1 and Account 2 across all 4 Binance market lines in the SAME SharedRateLimiter!
    for acct_ref in [1u8, 2u8] {
        for market in BinanceMarket::ALL {
            ors_quota
                .register_account(market, acct_ref, AccountRateOverrides::default())
                .unwrap();
        }
    }

    // Verify Spot (100), Usdm (300), Coinm (1200), and Options (100) primary ORDER caps coexist without colliding!
    let lim = ors_quota.limiter();
    assert_eq!(
        lim.window_limit(BinanceMarket::Spot.primary_order_window_id(1).unwrap()),
        Some(100)
    );
    assert_eq!(
        lim.window_limit(BinanceMarket::Usdm.primary_order_window_id(1).unwrap()),
        Some(300)
    );
    assert_eq!(
        lim.window_limit(BinanceMarket::Coinm.primary_order_window_id(1).unwrap()),
        Some(1_200)
    );
    assert_eq!(
        lim.window_limit(BinanceMarket::Options.primary_order_window_id(1).unwrap()),
        Some(100)
    );
    assert_eq!(
        BinanceMarket::from_product(Product::CoinM),
        BinanceMarket::Coinm
    );
    assert_eq!(
        BinanceMarket::from_product(Product::Option),
        BinanceMarket::Options
    );
    assert_eq!(
        classify_market_command_action(
            BinanceMarket::Coinm,
            Transport::WebSocket,
            CommandAction::New
        ),
        Ok(ActionBudgetClass::Order {
            order_units: 1,
            ip_weight: 0
        })
    );

    // Account 1 spends 2 Spot IP weight on `ip_shared`, Account 2 spends 2 Spot IP weight -> 4/4 exhausted!
    for t in [SEC, SEC + MS] {
        ors_quota
            .spend_command_for_ip(
                BinanceMarket::Spot,
                1,
                ip_shared,
                Transport::Rest,
                CommandAction::New,
                t,
            )
            .unwrap();
    }
    for t in [SEC + 2 * MS, SEC + 3 * MS] {
        ors_quota
            .spend_command_for_ip(
                BinanceMarket::Spot,
                2,
                ip_shared,
                Transport::WebSocket,
                CommandAction::New,
                t,
            )
            .unwrap();
    }

    // Single-pick route admission on exhausted `ip_shared` MUST call `pick_once` once, return RateLimitedNack,
    // and roll back Account 1's Spot 10s & 86400s ORDER counters back to 2!
    let pick_count = AtomicUsize::new(0);
    let route_shared = OrderRoute {
        connection_id: 3,
        account_ref: 1,
        ready: true,
        colo: false,
        product_mask: BinanceMarket::Spot.product_mask(),
        local_ip: Ipv4Addr::new(10, 60, 0, 1).to_bits(),
        ema_score_us: 180,
    };
    let nack = ors_quota.pick_route_and_spend_command_or_nack(
        BinanceMarket::Spot,
        1,
        Transport::Rest,
        CommandAction::New,
        || {
            pick_count.fetch_add(1, Ordering::SeqCst);
            Some(route_shared)
        },
        SEC + 4 * MS,
    );
    assert_eq!(pick_count.load(Ordering::SeqCst), 1);
    assert!(matches!(
        nack,
        Err(RouteAdmissionReject::RateLimitedNack(
            RateReject::Exhausted { limit: 4, .. }
        ))
    ));
    assert_eq!(
        lim.window_used(
            BinanceMarket::Spot.primary_order_window_id(1).unwrap(),
            SEC + 4 * MS
        ),
        Some(2)
    );

    // Meanwhile, USD-M, COIN-M, and Options on the SAME `ip_shared` have independent market-line IP windows!
    ors_quota
        .spend_command_for_ip(
            BinanceMarket::Usdm,
            1,
            ip_shared,
            Transport::Rest,
            CommandAction::New,
            SEC + 5 * MS,
        )
        .unwrap();
    ors_quota
        .spend_command_for_ip(
            BinanceMarket::Coinm,
            1,
            ip_shared,
            Transport::WebSocket,
            CommandAction::New,
            SEC + 6 * MS,
        )
        .unwrap();
    ors_quota
        .spend_command_for_ip(
            BinanceMarket::Options,
            2,
            ip_shared,
            Transport::Rest,
            CommandAction::New,
            SEC + 7 * MS,
        )
        .unwrap();

    // Background `AsyncRestQuerier` spend on `ip_spare` charges pure IP weight without touching ORDER caps!
    ors_quota
        .spend_query_for_ip(
            BinanceMarket::Spot,
            1,
            ip_spare,
            ReadAction::OpenOrders,
            SEC + 8 * MS,
        )
        .unwrap();
    assert_eq!(
        lim.window_used(BinanceMarket::Spot.ip_window_id(ip_spare), SEC + 8 * MS),
        Some(6)
    );
    assert_eq!(
        lim.window_used(
            BinanceMarket::Spot.primary_order_window_id(1).unwrap(),
            SEC + 8 * MS
        ),
        Some(2)
    );

    // HTTP 429 on `ip_shared` cools down BOTH Account 1 and Account 2 on `ip_shared` while `ip_spare` stays live;
    // HTTP 418 on Account 1 freezes ONLY Account 1 while Account 2 stays live on `ip_spare`!
    ors_quota
        .observe_http_status(1, ip_shared, 429, 10 * SEC)
        .unwrap();
    assert!(matches!(
        ors_quota.spend_command_for_ip(
            BinanceMarket::Usdm,
            2,
            ip_shared,
            Transport::Rest,
            CommandAction::Cancel,
            2 * SEC,
        ),
        Err(MultiIpQuotaError::Rate(RateReject::IpCooldown { .. }))
    ));
    ors_quota
        .spend_command_for_ip(
            BinanceMarket::Spot,
            2,
            ip_spare,
            Transport::Rest,
            CommandAction::Cancel,
            2 * SEC,
        )
        .unwrap();

    ors_quota
        .observe_http_status(1, ip_spare, 418, 60 * SEC)
        .unwrap();
    assert_eq!(
        ors_quota.spend_command_for_ip(
            BinanceMarket::Spot,
            1,
            ip_spare,
            Transport::Rest,
            CommandAction::Cancel,
            3 * SEC,
        ),
        Err(MultiIpQuotaError::AccountBanned)
    );
    ors_quota
        .spend_command_for_ip(
            BinanceMarket::Spot,
            2,
            ip_spare,
            Transport::Rest,
            CommandAction::Cancel,
            3 * SEC,
        )
        .unwrap();

    // Rising-edge `rateLimits[]` reconciliation on Account 2 USD-M 10s ORDER cap (300):
    // 239 (<80%) -> None; 240 (==80%) -> WarnThreshold once; 250 -> None; 300 (==100%) -> CeilingReached once;
    // 239 (<80%) -> None (re-arms); 240 -> WarnThreshold again!
    assert_eq!(
        ors_quota
            .reconcile_order_count(BinanceMarket::Usdm, 2, 10, 239, 300, 4 * SEC)
            .unwrap(),
        None
    );
    let warn = ors_quota
        .reconcile_order_count(BinanceMarket::Usdm, 2, 10, 240, 300, 4 * SEC + MS)
        .unwrap()
        .expect("must emit rising-edge 80% warn");
    assert_eq!(warn.level, ExchRateLimitLevel::WarnThreshold);
    assert!(warn.crossed_warn && !warn.crossed_ceiling);
    assert_eq!(
        ors_quota
            .reconcile_order_count(BinanceMarket::Usdm, 2, 10, 250, 300, 4 * SEC + 2 * MS)
            .unwrap(),
        None
    );
    let ceil = ors_quota
        .reconcile_order_count(BinanceMarket::Usdm, 2, 10, 300, 300, 4 * SEC + 3 * MS)
        .unwrap()
        .expect("must emit rising-edge 100% ceiling");
    assert_eq!(ceil.level, ExchRateLimitLevel::CeilingReached);
    assert!(ceil.crossed_ceiling);
    assert_eq!(
        ors_quota
            .reconcile_order_count(BinanceMarket::Usdm, 2, 10, 300, 300, 4 * SEC + 4 * MS)
            .unwrap(),
        None
    );
    assert_eq!(
        ors_quota
            .reconcile_order_count(BinanceMarket::Usdm, 2, 10, 239, 300, 11 * SEC)
            .unwrap(),
        None
    );
    assert!(
        ors_quota
            .reconcile_order_count(BinanceMarket::Usdm, 2, 10, 240, 300, 11 * SEC + MS)
            .unwrap()
            .is_some()
    );
}
