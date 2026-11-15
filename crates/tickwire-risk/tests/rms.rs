#![forbid(unsafe_code)]

use std::thread;
use tickwire_risk::{
    FatFingerRatio, Fixed, RmsConfigErrorKind, RmsEngine, RmsOrderCheck, RmsRejectReason, RmsSide,
    SymbolLimitConfig, WindowTier,
};

const NS_PER_SEC: u64 = 1_000_000_000;

fn default_symbol_cfg() -> SymbolLimitConfig {
    SymbolLimitConfig {
        enabled: true,
        allow_bypass: true,
        max_order_qty_lots: 100,
        max_order_notional: 2_000_000,
        notional_multiplier: 1,
        new_count_limits: [3, 5, 7],
        order_qty_limits: [150, 300, 450],
        fat_finger_ratio: FatFingerRatio::from_bps(500), // 5%
    }
}

#[test]
fn fat_finger_ratio_uses_exact_integer_bounds_for_buy_and_sell() {
    let ratio = FatFingerRatio::parse_str("0.05").unwrap();
    assert_eq!(ratio, FatFingerRatio::from_bps(500).unwrap());
    assert_eq!(ratio, FatFingerRatio::from_ppm(50_000).unwrap());
    assert_eq!(
        ratio,
        FatFingerRatio::from_fixed(Fixed::parse("0.05", 4).unwrap()).unwrap()
    );

    // Reference 10_000 ticks +/- 5% => [9_500, 10_500].
    assert_eq!(ratio.bounds_ticks(10_000), Some((9_500, 10_500)));
    assert!(
        ratio
            .check_price_ticks(RmsSide::Buy, 10_500, 10_000)
            .is_ok()
    );
    assert!(matches!(
        ratio.check_price_ticks(RmsSide::Buy, 10_501, 10_000),
        Err(RmsRejectReason::FatFingerPrice {
            price_ticks: 10_501,
            ref_price_ticks: 10_000,
            min_sell_ticks: 9_500,
            max_buy_ticks: 10_500,
        })
    ));
    assert!(
        ratio
            .check_price_ticks(RmsSide::Sell, 9_500, 10_000)
            .is_ok()
    );
    assert!(matches!(
        ratio.check_price_ticks(RmsSide::Sell, 9_499, 10_000),
        Err(RmsRejectReason::FatFingerPrice { .. })
    ));

    // Fractional tick rounding must be conservative:
    // ref = 101 ticks, 5% => buy floor(101 * 1.05) = floor(106.05) = 106;
    // sell ceil(101 * 0.95) = ceil(95.95) = 96.
    assert_eq!(ratio.bounds_ticks(101), Some((96, 106)));
    assert!(ratio.check_price_ticks(RmsSide::Buy, 107, 101).is_err());
    assert!(ratio.check_price_ticks(RmsSide::Sell, 95, 101).is_err());
}

#[test]
fn three_tier_rolling_windows_enforce_and_expire_across_1s_5s_10s() {
    let mut engine = RmsEngine::from_configs(&[default_symbol_cfg()]).unwrap();
    engine.set_reference_price_ticks(0, 10_000).unwrap();

    let mk = |now_mono_ns: u64, qty_lots: i64| RmsOrderCheck {
        symbol_id: 0,
        side: RmsSide::Buy,
        price_ticks: Some(10_000),
        qty_lots,
        reference_price_ticks: None,
        now_mono_ns,
        bypass_rms: false,
    };

    // 3 orders within t = 0..200ms hit the 1s new_count cap of 3.
    engine.check_order(&mk(0, 40)).unwrap();
    engine.check_order(&mk(100_000_000, 40)).unwrap();
    engine.check_order(&mk(200_000_000, 40)).unwrap();
    assert_eq!(
        engine.check_order(&mk(900_000_000, 10)),
        Err(RmsRejectReason::NewCountWindow {
            tier: WindowTier::OneSecond,
            projected: 4,
            limit: 3,
        })
    );
    // Rejected order must not pollute rolling counters.
    assert_eq!(
        engine.symbol(0).unwrap().new_count_window().current_sums(),
        [3, 3, 3]
    );
    assert_eq!(
        engine.symbol(0).unwrap().order_qty_window().current_sums(),
        [120, 120, 120]
    );

    // At t = 1.0s, the t = 0 bucket expires from the 1s window (leaving 2 in 1s, 3 in 5s/10s).
    engine.check_order(&mk(NS_PER_SEC, 50)).unwrap();
    // At t = 1.3s, all initial 0..200ms buckets expired from 1s, so 1s has 1 order, 5s has 4 orders.
    engine
        .check_order(&mk(NS_PER_SEC + 300_000_000, 50))
        .unwrap();
    // Now 5s window has 5 orders (limit 5), so a 6th before 5.0s fails on FiveSeconds tier!
    assert_eq!(
        engine.check_order(&mk(2 * NS_PER_SEC, 10)),
        Err(RmsRejectReason::NewCountWindow {
            tier: WindowTier::FiveSeconds,
            projected: 6,
            limit: 5,
        })
    );

    // At t = 5.3s, initial 3 orders expired from 5s (5s has 2, 10s has 5). Two more reach 10s cap of 7.
    engine
        .check_order(&mk(5 * NS_PER_SEC + 300_000_000, 50))
        .unwrap();
    engine
        .check_order(&mk(5 * NS_PER_SEC + 400_000_000, 50))
        .unwrap();
    assert_eq!(
        engine.check_order(&mk(6 * NS_PER_SEC, 10)),
        Err(RmsRejectReason::NewCountWindow {
            tier: WindowTier::TenSeconds,
            projected: 8,
            limit: 7,
        })
    );

    // At t = 16.0s (> 10s idle), all buckets clear completely.
    engine.check_order(&mk(16 * NS_PER_SEC, 100)).unwrap();
    assert_eq!(
        engine.symbol(0).unwrap().new_count_window().current_sums(),
        [1, 1, 1]
    );
    assert_eq!(
        engine.symbol(0).unwrap().order_qty_window().current_sums(),
        [100, 100, 100]
    );
}

#[test]
fn single_order_qty_notional_and_controlled_bypass_rms_enforce_cleanly() {
    let mut cfg = default_symbol_cfg();
    cfg.max_order_qty_lots = 50;
    cfg.max_order_notional = 400_000;
    let mut engine = RmsEngine::from_configs(&[cfg]).unwrap();
    engine.set_reference_price_ticks(0, 10_000).unwrap();

    // Exceeds max_order_qty_lots (60 > 50).
    assert_eq!(
        engine.check_order(&RmsOrderCheck {
            symbol_id: 0,
            side: RmsSide::Buy,
            price_ticks: Some(10_000),
            qty_lots: 60,
            reference_price_ticks: None,
            now_mono_ns: 1_000,
            bypass_rms: false,
        }),
        Err(RmsRejectReason::MaxOrderQty {
            qty_lots: 60,
            limit_lots: 50,
        })
    );

    // Within qty (45 <= 50) but exceeds max_order_notional (45 * 10_000 = 450_000 > 400_000).
    assert_eq!(
        engine.check_order(&RmsOrderCheck {
            symbol_id: 0,
            side: RmsSide::Buy,
            price_ticks: Some(10_000),
            qty_lots: 45,
            reference_price_ticks: None,
            now_mono_ns: 1_000,
            bypass_rms: false,
        }),
        Err(RmsRejectReason::MaxOrderNotional {
            notional: 450_000,
            limit: 400_000,
        })
    );

    // With bypass_rms = true and allow_bypass = true, soft limits pass AND flow is recorded.
    engine
        .check_order(&RmsOrderCheck {
            symbol_id: 0,
            side: RmsSide::Buy,
            price_ticks: Some(12_000),
            qty_lots: 60,
            reference_price_ticks: None,
            now_mono_ns: 1_000,
            bypass_rms: true,
        })
        .unwrap();
    assert_eq!(
        engine.symbol(0).unwrap().order_qty_window().current_sums(),
        [60, 60, 60]
    );

    // Disabling allow_bypass blocks bypass_rms overrides.
    cfg.allow_bypass = false;
    engine.reload_handle().set_symbol_config(0, cfg).unwrap();
    assert!(
        engine
            .check_order(&RmsOrderCheck {
                symbol_id: 0,
                side: RmsSide::Buy,
                price_ticks: Some(12_000),
                qty_lots: 60,
                reference_price_ticks: None,
                now_mono_ns: 2_000,
                bypass_rms: true,
            })
            .is_err()
    );
}

#[test]
fn static_config_parse_reload_and_resp_atomic_fee_accumulation_work_concurrently() {
    let initial = "\
        # Initial static RMS table\n\
        symbol_id=0 enabled=true allow_bypass=false max_qty=100 max_notional=5000000 new_count=10/30/60 order_qty=500/1500/3000 fat_finger=0.05\n\
        symbol_id=1 enabled=true allow_bypass=true max_qty=200 max_notional=9000000 new_count=20/60/120 order_qty=1000/3000/6000 fat_finger=250bps\n\
    ";
    let mut engine = RmsEngine::from_static_config(4, initial).unwrap();
    assert_eq!(engine.generation(), 1);
    assert_eq!(engine.config(0).unwrap().max_order_qty_lots, 100);
    assert_eq!(engine.config(1).unwrap().max_order_qty_lots, 200);
    assert!(!engine.config(2).unwrap().enabled);

    engine.set_reference_price_ticks(0, 10_000).unwrap();
    engine
        .check_order(&RmsOrderCheck {
            symbol_id: 0,
            side: RmsSide::Buy,
            price_ticks: Some(10_100),
            qty_lots: 50,
            reference_price_ticks: None,
            now_mono_ns: 100_000,
            bypass_rms: false,
        })
        .unwrap();

    // Invalid reload (non-monotonic 1s/5s/10s window) must fail and preserve prior config & generation.
    let bad_reload = "symbol_id=0 enabled=true max_qty=999 max_notional=9999999 new_count=50/20/60 order_qty=500/1500/3000 fat_finger=0.05";
    let err = engine.reload_limits(bad_reload).unwrap_err();
    assert_eq!(err.kind, RmsConfigErrorKind::InvalidNewCountWindows);
    assert_eq!(engine.generation(), 1);
    assert_eq!(engine.config(0).unwrap().max_order_qty_lots, 100);

    // Out-of-range symbol_id must fail and preserve prior config & generation.
    let oob_reload = "symbol_id=9 enabled=true max_qty=250 max_notional=9999999 new_count=10/20/30 order_qty=500/1500/3000 fat_finger=none";
    let oob_err = engine.reload_limits(oob_reload).unwrap_err();
    assert!(matches!(
        oob_err.kind,
        RmsConfigErrorKind::SymbolOutOfRange {
            symbol_id: 9,
            capacity: 4
        }
    ));
    assert_eq!(engine.generation(), 1);

    // Valid off-thread SIGUSR1 reload updates limits while preserving live rolling windows, and
    // concurrent RESP thread accumulates fees + rebates atomically.
    let handle_reload = engine.reload_handle();
    let handle_resp = engine.reload_handle();

    let t_reload = thread::spawn(move || {
        let good_reload = "symbol_id=0 enabled=true allow_bypass=true max_qty=300 max_notional=15000000 new_count=15/45/90 order_qty=800/2400/4800 fat_finger=0.02";
        handle_reload.reload_limits(good_reload).unwrap()
    });
    let t_resp = thread::spawn(move || {
        for _ in 0..100 {
            handle_resp.record_execution_fee(0, 250).unwrap();
        }
        // Post-trade rebate amendment (-50 atoms).
        handle_resp.record_execution_fee(0, -50).unwrap()
    });

    assert_eq!(t_reload.join().unwrap(), 2);
    assert_eq!(t_resp.join().unwrap(), 24_950);
    assert_eq!(engine.generation(), 2);
    assert_eq!(engine.config(0).unwrap().max_order_qty_lots, 300);
    assert_eq!(engine.cumulative_fee(0), Some((24_950, 101)));
    assert_eq!(
        engine.symbol(0).unwrap().order_qty_window().current_sums(),
        [50, 50, 50]
    );
}
