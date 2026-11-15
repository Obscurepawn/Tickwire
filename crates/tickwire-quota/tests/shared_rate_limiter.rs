//! Integration and multithreaded contention tests for `SharedRateLimiter`, `OrsAccountQuota`,
//! `OrsVenueProfile`, single-pick route NACK, and rising-edge `ExchRateLimitEvent` reconciliation.
#![forbid(unsafe_code)]

use std::{
    net::{IpAddr, Ipv4Addr},
    sync::{
        Arc, Barrier,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
};
use tickwire_quota::{
    ExchRateLimitLevel, InstrumentScope, IpBudgetSpec, OrsAccountQuota, OrsVenueProfile,
    RouteAdmissionReject, SharedRateLimiter, SharedSpend,
    rate_limiter::{IpKey, RateFlags, RateReject, RateRule},
};
use tickwire_venue::OrderRoute;

const MS: u64 = 1_000_000;
const SEC: u64 = 1_000_000_000;

fn ipv4(last: u8) -> IpAddr {
    IpAddr::V4(Ipv4Addr::new(10, 90, 0, last))
}

#[test]
fn shared_ip_budget_is_shared_across_multiple_account_sessions_and_isolates_429() {
    let limiter = SharedRateLimiter::new();
    let ip_shared = ipv4(1);
    let ip_dedicated = ipv4(2);
    let key_shared = IpKey::from_ip(ip_shared);
    let key_dedicated = IpKey::from_ip(ip_dedicated);

    limiter
        .install_venue_ip_budgets(
            OrsVenueProfile::BinanceSpot,
            &[
                IpBudgetSpec::new(ip_shared, 10),
                IpBudgetSpec::new(ip_dedicated, 6_000),
            ],
        )
        .unwrap();

    let acct_1 = OrsAccountQuota::register(&limiter, OrsVenueProfile::BinanceSpot, 1).unwrap();
    let acct_2 = OrsAccountQuota::register(&limiter, OrsVenueProfile::BinanceSpot, 2).unwrap();

    // Account 1 spends 6 IP weight on `ip_shared`, Account 2 spends 4 IP weight -> total 10/10!
    for i in 0..6u64 {
        acct_1
            .allow_place(key_shared, InstrumentScope::None, 1, 1, i * MS)
            .unwrap();
    }
    for i in 0..4u64 {
        acct_2
            .allow_place(key_shared, InstrumentScope::None, 1, 1, (10 + i) * MS)
            .unwrap();
    }

    // Account 2's 5th order on `ip_shared` hits the shared 10/60s IP cap AND rolls back its order debits!
    let err = acct_2
        .allow_place(key_shared, InstrumentScope::None, 1, 1, 20 * MS)
        .unwrap_err();
    assert!(matches!(err, RateReject::Exhausted { limit: 10, .. }));

    let a2_10s = acct_2.order_primary_handle().unwrap().id();
    let a2_day = acct_2.order_secondary_handle().unwrap().id();
    assert_eq!(limiter.window_used(a2_10s, 20 * MS), Some(4));
    assert_eq!(limiter.window_used(a2_day, 20 * MS), Some(4));

    // Meanwhile, `ip_dedicated` still has full budget for Account 2!
    acct_2
        .allow_place(key_dedicated, InstrumentScope::None, 1, 1, 21 * MS)
        .unwrap();
    assert_eq!(limiter.window_used(a2_10s, 21 * MS), Some(5));

    // HTTP 429 cooldown on `ip_shared` isolates to `ip_shared` without affecting `ip_dedicated`.
    limiter.note_ip_cooldown(key_shared, 120 * SEC).unwrap();
    assert!(matches!(
        acct_1.allow_query(key_shared, 1, 65 * SEC),
        Err(RateReject::IpCooldown { local_ip, .. }) if local_ip == key_shared
    ));
    acct_1.allow_query(key_dedicated, 1, 65 * SEC).unwrap();
}

#[test]
fn pick_route_and_place_or_nack_invokes_picker_once_and_never_retries_another_connection() {
    let limiter = SharedRateLimiter::new();
    let ip_exhausted = ipv4(10);
    let ip_healthy = ipv4(11);
    limiter
        .install_venue_ip_budgets(
            OrsVenueProfile::BinanceUsdm,
            &[
                IpBudgetSpec::new(ip_exhausted, 1),
                IpBudgetSpec::new(ip_healthy, 2_400),
            ],
        )
        .unwrap();

    let acct = OrsAccountQuota::register(&limiter, OrsVenueProfile::BinanceUsdm, 7).unwrap();
    // Consume the 1 weight unit on `ip_exhausted`.
    acct.allow_query(IpKey::from_ip(ip_exhausted), 1, SEC)
        .unwrap();

    let pick_calls = AtomicUsize::new(0);
    let route_on_exhausted_ip = OrderRoute {
        connection_id: 0,
        account_ref: 7,
        ready: true,
        colo: false,
        product_mask: 0x0002,
        local_ip: Ipv4Addr::new(10, 90, 0, 10).to_bits(),
        ema_score_us: 250,
    };

    // Even though `ip_healthy` exists in the process, `pick_route_and_place_or_nack` MUST call
    // `pick_once` once and immediately return `RateLimitedNack` without retrying or leaking order units!
    let res = acct.pick_route_and_place_or_nack(
        || {
            pick_calls.fetch_add(1, Ordering::SeqCst);
            Some(route_on_exhausted_ip)
        },
        InstrumentScope::None,
        1,
        1,
        SEC + MS,
    );
    assert_eq!(pick_calls.load(Ordering::SeqCst), 1);
    assert!(matches!(
        res,
        Err(RouteAdmissionReject::RateLimitedNack(
            RateReject::Exhausted { limit: 1, .. }
        ))
    ));
    assert_eq!(
        limiter.window_used(acct.order_primary_handle().unwrap().id(), SEC + MS),
        Some(0),
        "failed IP spend must atomically roll back 10s and 60s USD-M order buckets"
    );
    assert_eq!(
        limiter.window_used(acct.order_secondary_handle().unwrap().id(), SEC + MS),
        Some(0)
    );
}

#[test]
fn compiled_venue_defaults_cover_binance_all_markets_okx_uta_and_bybit_uta() {
    let limiter = SharedRateLimiter::new();
    let ip = IpKey::from_ip(ipv4(20));

    // 1. Binance COIN-M: 1200/60s account ORDER, 0 WS IP weight on new order, weight on cancel/query.
    let coinm = OrsAccountQuota::register(&limiter, OrsVenueProfile::BinanceCoinm, 11).unwrap();
    assert!(coinm.order_primary_handle().is_some());
    assert!(coinm.order_secondary_handle().is_none());
    coinm
        .allow_place(ip, InstrumentScope::None, 1, 0, SEC)
        .unwrap();
    let ip_coinm_id = limiter
        .resolve_venue_ip_window(OrsVenueProfile::BinanceCoinm, ip)
        .unwrap()
        .id();
    assert_eq!(limiter.window_used(ip_coinm_id, SEC), Some(0));
    coinm
        .allow_cancel(ip, InstrumentScope::None, 1, SEC)
        .unwrap();
    assert_eq!(limiter.window_used(ip_coinm_id, SEC), Some(1));

    // 2. Binance Options: 100/10s + 1200/60s account ORDER + 2400/60s IP weight.
    let eopt = OrsAccountQuota::register(&limiter, OrsVenueProfile::BinanceOptions, 12).unwrap();
    assert_eq!(
        limiter.window_limit(eopt.order_primary_handle().unwrap().id()),
        Some(100)
    );
    assert_eq!(
        limiter.window_limit(eopt.order_secondary_handle().unwrap().id()),
        Some(1_200)
    );
    eopt.allow_place(ip, InstrumentScope::None, 1, 1, SEC)
        .unwrap();

    // 3. OKX UTA: 1000/2s sub-account ORDER + 60/2s per-symbol ORDER/CANCEL + 60/2s per-family Options!
    let okx = OrsAccountQuota::register(&limiter, OrsVenueProfile::OkxUta, 13).unwrap();
    for i in 0..60u64 {
        okx.allow_place(ip, InstrumentScope::Symbol(501), 1, 0, SEC + i * MS)
            .unwrap();
    }
    // 61st order on symbol 501 rejects and rolls back the 1000/2s account counter to 60!
    assert!(matches!(
        okx.allow_place(ip, InstrumentScope::Symbol(501), 1, 0, SEC + 70 * MS),
        Err(RateReject::Exhausted { limit: 60, .. })
    ));
    assert_eq!(
        limiter.window_used(okx.order_primary_handle().unwrap().id(), SEC + 70 * MS),
        Some(60)
    );
    // Cancelling on symbol 501 succeeds because CANCEL has its own 60/2s symbol bucket!
    okx.allow_cancel(ip, InstrumentScope::Symbol(501), 0, SEC + 71 * MS)
        .unwrap();
    // OKX Options family bucket (Family(9001)) has an independent 60/2s window!
    okx.allow_place(ip, InstrumentScope::Family(9001), 1, 0, SEC + 72 * MS)
        .unwrap();

    // 4. Bybit UTA: 20/1s rolling per-product ORDER & CANCEL (Spot=1 vs Linear=2 isolated!).
    let bybit = OrsAccountQuota::register(&limiter, OrsVenueProfile::BybitUta, 14).unwrap();
    for i in 0..20u64 {
        bybit
            .allow_place(
                ip,
                InstrumentScope::Product(1),
                1,
                1,
                10 * SEC + i * 10 * MS,
            )
            .unwrap();
    }
    assert!(matches!(
        bybit.allow_place(ip, InstrumentScope::Product(1), 1, 1, 10 * SEC + 250 * MS),
        Err(RateReject::Exhausted { limit: 20, .. })
    ));
    // Cancel on Spot (product=1) AND new order on Linear (product=2) both succeed!
    bybit
        .allow_cancel(ip, InstrumentScope::Product(1), 1, 10 * SEC + 255 * MS)
        .unwrap();
    bybit
        .allow_place(ip, InstrumentScope::Product(2), 1, 1, 10 * SEC + 260 * MS)
        .unwrap();
    // After 1 full second slides past the first sub-bucket, Spot (product=1) admits again!
    bybit
        .allow_place(ip, InstrumentScope::Product(1), 1, 1, 11 * SEC + 100 * MS)
        .unwrap();
}

#[test]
fn multithreaded_rolling_and_fixed_contention_never_over_admits_or_corrupts_rollback() {
    let limiter = Arc::new(SharedRateLimiter::new());
    let rolling_order = RateRule::account(1, 2, RateFlags::ORDER.union(RateFlags::ROLLING), 200);
    let tight_ip = RateRule::ip(
        IpKey::from_ip(ipv4(55)),
        60,
        RateFlags::REQUEST.union(RateFlags::WEIGHT),
        120,
    );
    let h_ord = limiter.add(rolling_order).unwrap();
    let h_ip = limiter.add(tight_ip).unwrap();

    let threads = 8usize;
    let barrier = Arc::new(Barrier::new(threads));
    let admitted = Arc::new(AtomicUsize::new(0));
    let mut joins = Vec::with_capacity(threads);

    for t in 0..threads {
        let lim = Arc::clone(&limiter);
        let bar = Arc::clone(&barrier);
        let adm = Arc::clone(&admitted);
        joins.push(thread::spawn(move || {
            bar.wait();
            for i in 0..50u64 {
                let now_ns = 5 * SEC + ((t as u64) * 3 + i) * MS;
                let spends = [SharedSpend::handle(h_ord, 1), SharedSpend::handle(h_ip, 1)];
                if lim.allow(&spends, now_ns).is_ok() {
                    adm.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }
    for j in joins {
        j.join().unwrap();
    }

    // 400 concurrent attempts competed for `h_ord` (cap 200) + `h_ip` (cap 120):
    // Exactly 120 must succeed, AND the 280 rejected attempts that first reserved `h_ord`
    // before failing on `h_ip` MUST have rolled back `h_ord` to exactly 120!
    assert_eq!(admitted.load(Ordering::SeqCst), 120);
    assert_eq!(
        limiter.window_used(h_ip.id(), 5 * SEC + 200 * MS),
        Some(120)
    );
    assert_eq!(
        limiter.window_used(h_ord.id(), 5 * SEC + 200 * MS),
        Some(120),
        "every rejected multi-bucket spend must conserve rolling window units under contention"
    );
}

#[test]
fn reconcile_venue_usage_emits_rising_edge_at_80_pct_and_100_pct_and_rearms_only_below_threshold() {
    let limiter = SharedRateLimiter::new();
    let rule = RateRule::account(3, 10, RateFlags::ORDER, 100);
    let h = limiter.add(rule).unwrap();

    // Spend 5 local orders first to verify `reconcile_venue_usage` does NOT overwrite local counters.
    limiter
        .allow(&[SharedSpend::handle(h, 5)], 10 * SEC)
        .unwrap();
    assert_eq!(limiter.window_used(h.id(), 10 * SEC), Some(5));

    // 79/100 (< 80%): silent.
    assert_eq!(
        limiter
            .reconcile_venue_usage(h.id(), 79, 100, 10 * SEC + MS)
            .unwrap(),
        None
    );

    // 80/100 (rising edge >= 80%): emits WarnThreshold once!
    let ev1 = limiter
        .reconcile_venue_usage(h.id(), 80, 100, 10 * SEC + 2 * MS)
        .unwrap()
        .expect("rising edge at 80%");
    assert_eq!(ev1.level, ExchRateLimitLevel::WarnThreshold);
    assert!(ev1.crossed_warn);
    assert!(!ev1.crossed_ceiling);
    assert_eq!(limiter.window_used(h.id(), 10 * SEC + 2 * MS), Some(5));

    // 85/100 and 99/100 while already warned: silent!
    assert!(
        limiter
            .reconcile_venue_usage(h.id(), 85, 100, 10 * SEC + 3 * MS)
            .unwrap()
            .is_none()
    );
    assert!(
        limiter
            .reconcile_venue_usage(h.id(), 99, 100, 10 * SEC + 4 * MS)
            .unwrap()
            .is_none()
    );

    // 100/100 (rising edge to ceiling): emits CeilingReached once!
    let ev2 = limiter
        .reconcile_venue_usage(h.id(), 100, 100, 10 * SEC + 5 * MS)
        .unwrap()
        .expect("rising edge at 100%");
    assert_eq!(ev2.level, ExchRateLimitLevel::CeilingReached);
    assert!(!ev2.crossed_warn);
    assert!(ev2.crossed_ceiling);

    // Remaining at 100/100: silent!
    assert!(
        limiter
            .reconcile_venue_usage(h.id(), 100, 100, 10 * SEC + 6 * MS)
            .unwrap()
            .is_none()
    );

    // Drop to 85/100: re-arms ceiling (< 100%), keeps warn latched (>= 80%).
    assert!(
        limiter
            .reconcile_venue_usage(h.id(), 85, 100, 10 * SEC + 7 * MS)
            .unwrap()
            .is_none()
    );
    let ev3 = limiter
        .reconcile_venue_usage(h.id(), 100, 100, 10 * SEC + 8 * MS)
        .unwrap()
        .expect("re-armed ceiling fires again");
    assert_eq!(ev3.level, ExchRateLimitLevel::CeilingReached);
    assert!(!ev3.crossed_warn);
    assert!(ev3.crossed_ceiling);

    // Drop strictly below 80% (79/100): re-arms BOTH warn and ceiling!
    assert!(
        limiter
            .reconcile_venue_usage(h.id(), 79, 100, 20 * SEC)
            .unwrap()
            .is_none()
    );
    let ev4 = limiter
        .reconcile_venue_usage(h.id(), 82, 100, 20 * SEC + MS)
        .unwrap()
        .expect("re-armed 80% warn fires again");
    assert_eq!(ev4.level, ExchRateLimitLevel::WarnThreshold);
    assert!(ev4.crossed_warn);
}
