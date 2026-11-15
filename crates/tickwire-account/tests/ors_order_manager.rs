#[path = "common/mod.rs"]
mod common;

use tickwire_account::{
    DueQueryAction, FillKey, FillObservationInput, FillRecordOutcome, FillRecordPool, InlineAscii,
    MAX_CANCEL_BACKOFF_ATTEMPTS, OrderManager, OrderManagerConfig, OrsAccountDomainConfig,
    OrsCancelObservation, OrsCancelRequest, OrsExchangeBootstrapSnapshot, OrsFeeSpec,
    OrsFillObservation, OrsModifyRequest, OrsNewOrderRequest, OrsOrderFilter,
    OrsOrderQuerySnapshot, OrsQueriedOrderStatus, OrsQueryReconcileAction, OrsReport,
    OrsRestBalanceRow, OrsRestOpenOrderRow, OrsRestPositionRow, OrsSymbolSpec,
    encode_tw1_client_id_inline, make_ors_order_id,
};
use tickwire_shm_sys::MlockPolicy;
use tickwire_venue as v;

const ASSET_BTC: u64 = 1;
const ASSET_USDT: u64 = 2;
const ASSET_BNB: u64 = 3;

const WALLET_SPOT: u16 = 1;
const WALLET_USDM: u16 = 2;
const WALLET_COINM: u16 = 3;
const WALLET_OPTION: u16 = 4;

fn sample_domain(account_ref: u8, exchange_account_id: u64) -> OrsAccountDomainConfig {
    OrsAccountDomainConfig {
        account_ref,
        risk_domain_id: [0x42; 16],
        venue: 7,
        environment: v::Environment::Production,
        exchange_account_id,
        epoch: 9,
    }
}

fn multi_product_symbols() -> Vec<OrsSymbolSpec> {
    vec![
        // symbol 0: BTCUSDT Spot (tick = 0.01 USDT [scale 2], step = 0.001 BTC [scale 3], quote scale 6, base scale 6)
        OrsSymbolSpec {
            symbol_id: 0,
            instrument_id: 100,
            product: v::Product::Spot,
            wallet_id: WALLET_SPOT,
            base_asset_id: ASSET_BTC,
            base_scale: 6,
            quote_asset_id: ASSET_USDT,
            quote_scale: 6,
            settlement_asset_id: ASSET_USDT,
            settlement_scale: 6,
            price_tick_atoms: 1,
            price_scale: 2,
            qty_step_atoms: 1,
            qty_scale: 3,
            contract_size_atoms: 1,
            contract_scale: 0,
            initial_margin_bps: 10_000,
        },
        // symbol 1: BTCUSDT USD-M Perp (10x leverage -> 1000 bps IM)
        OrsSymbolSpec {
            symbol_id: 1,
            instrument_id: 101,
            product: v::Product::UsdM,
            wallet_id: WALLET_USDM,
            base_asset_id: ASSET_BTC,
            base_scale: 6,
            quote_asset_id: ASSET_USDT,
            quote_scale: 6,
            settlement_asset_id: ASSET_USDT,
            settlement_scale: 6,
            price_tick_atoms: 1,
            price_scale: 1,
            qty_step_atoms: 1,
            qty_scale: 3,
            contract_size_atoms: 1,
            contract_scale: 0,
            initial_margin_bps: 1_000,
        },
        // symbol 2: BTCUSD Coin-M Inverse Perp (1 contract = 100 USD, settled in BTC, 2000 bps IM = 5x)
        OrsSymbolSpec {
            symbol_id: 2,
            instrument_id: 102,
            product: v::Product::CoinM,
            wallet_id: WALLET_COINM,
            base_asset_id: ASSET_BTC,
            base_scale: 8,
            quote_asset_id: ASSET_USDT,
            quote_scale: 2,
            settlement_asset_id: ASSET_BTC,
            settlement_scale: 8,
            price_tick_atoms: 1,
            price_scale: 0,
            qty_step_atoms: 1,
            qty_scale: 0,
            contract_size_atoms: 100,
            contract_scale: 0,
            initial_margin_bps: 2_000,
        },
        // symbol 3: BTC Call Option (settled in USDT, 1500 bps IM)
        OrsSymbolSpec {
            symbol_id: 3,
            instrument_id: 103,
            product: v::Product::Option,
            wallet_id: WALLET_OPTION,
            base_asset_id: ASSET_BTC,
            base_scale: 6,
            quote_asset_id: ASSET_USDT,
            quote_scale: 6,
            settlement_asset_id: ASSET_USDT,
            settlement_scale: 6,
            price_tick_atoms: 1,
            price_scale: 0,
            qty_step_atoms: 1,
            qty_scale: 2,
            contract_size_atoms: 1,
            contract_scale: 0,
            initial_margin_bps: 1_500,
        },
    ]
}

fn build_manager(slots_per_strat: usize, allow_cancel_before_ack: bool) -> OrderManager<true> {
    let mut cfg =
        OrderManagerConfig::single_account_default(sample_domain(1, 9001), multi_product_symbols());
    cfg.accounts.push(sample_domain(2, 9002));
    cfg.slots_per_strat = slots_per_strat;
    cfg.allow_cancel_before_ack = allow_cancel_before_ack;
    cfg.max_query_count = Some(3);
    cfg.query_desk_capacity = 4;
    cfg.asset_refs = vec![
        v::AssetRef {
            id: ASSET_BTC,
            version: 1,
            symbol: v::AssetSymbol::new("BTC").unwrap(),
            exponent: -6,
        },
        v::AssetRef {
            id: ASSET_USDT,
            version: 1,
            symbol: v::AssetSymbol::new("USDT").unwrap(),
            exponent: -6,
        },
        v::AssetRef {
            id: ASSET_BNB,
            version: 1,
            symbol: v::AssetSymbol::new("BNB").unwrap(),
            exponent: -6,
        },
    ];
    let mgr = OrderManager::<true>::new(cfg).expect("OrderManager::new");
    mgr.wallet_ledger()
        .set_balance(
            1,
            v::AssetKey::new(WALLET_SPOT, ASSET_USDT),
            6,
            1_000_000_000_000, // 1,000,000 USDT
            0,
        )
        .unwrap();
    mgr.wallet_ledger()
        .set_balance(
            1,
            v::AssetKey::new(WALLET_SPOT, ASSET_BTC),
            6,
            100_000_000, // 100 BTC
            0,
        )
        .unwrap();
    mgr.wallet_ledger()
        .set_balance(
            1,
            v::AssetKey::new(WALLET_USDM, ASSET_USDT),
            6,
            500_000_000_000, // 500,000 USDT
            0,
        )
        .unwrap();
    mgr.wallet_ledger()
        .set_balance(
            1,
            v::AssetKey::new(WALLET_COINM, ASSET_BTC),
            8,
            1_000_000_000, // 10 BTC
            0,
        )
        .unwrap();
    mgr.wallet_ledger()
        .set_balance(
            1,
            v::AssetKey::new(WALLET_OPTION, ASSET_USDT),
            6,
            200_000_000_000, // 200,000 USDT
            0,
        )
        .unwrap();
    mgr
}

#[test]
fn inline_tw1_client_id_matches_venue_codec_byte_for_byte() {
    let domain_cfg = sample_domain(1, 9001);
    for product in [
        v::Product::Spot,
        v::Product::UsdM,
        v::Product::CoinM,
        v::Product::Option,
        v::Product::LinearPerpetual,
        v::Product::InverseFuture,
    ] {
        let domain = domain_cfg.client_id_domain(product);
        let model =
            v::SelfDescribingClientId::from_ors_order(7, 0, domain_cfg.epoch, 123_456).unwrap();
        let expected = model.encode(&domain).unwrap();
        let inline = encode_tw1_client_id_inline(&model, &domain).unwrap();
        assert_eq!(inline.as_str(), expected.as_str());
        assert_eq!(
            v::SelfDescribingClientId::decode_str(inline.as_str(), &domain),
            v::DecodedClientId::Managed(model)
        );
    }
}

#[test]
fn slot_ring_collision_immediate_retirement_dry_run_and_late_fee_amendment() {
    let mgr = build_manager(4, true);
    mgr.dop_state().configure_strategy(3, &[1]).unwrap();
    let login = mgr.dop_state().login(3, 1).unwrap();
    assert_eq!(login.next_order_seqnum, 1);
    assert_eq!(login.default_account_ref, 1);

    // 1. Multiple consecutive dry_run probes use the dedicated scratch slot without consuming
    //    regular slots, holds, or STP state.
    for i in 1..=2u64 {
        let dry = mgr
            .place_order(&OrsNewOrderRequest {
                strat_id: 3,
                account_ref: 0,
                msg_seqnum: i,
                order_seqnum: i,
                symbol_id: 0,
                side: v::Side::Buy,
                price_ticks: 5_000_000, // 50,000.00 USDT
                qty_lots: 100,          // 0.100 BTC -> 5,000 USDT hold during probe
                tif: v::TimeInForce::Gtc,
                post_only: false,
                reduce_only: false,
                check_stp: true,
                dry_run: true,
                now_mono_ns: 1_000 * i,
            })
            .unwrap();
        assert!(!dry.should_send_to_venue);
        assert_eq!(mgr.stp_book().best_bid_ticks(0), None);
    }
    let (_, locked_after_dry) = mgr
        .wallet_ledger()
        .balance(1, v::AssetKey::new(WALLET_SPOT, ASSET_USDT))
        .unwrap();
    assert_eq!(locked_after_dry, 0);

    // 2. Place live order at order_seqnum = 5 (slot offset = 5 & 3 == 1).
    let placed = mgr
        .place_order(&OrsNewOrderRequest {
            strat_id: 3,
            account_ref: 0,
            msg_seqnum: 3,
            order_seqnum: 5,
            symbol_id: 0,
            side: v::Side::Buy,
            price_ticks: 5_000_000, // 50,000.00
            qty_lots: 100,          // 0.100 BTC -> 5,000 USDT = 5_000_000_000 atoms
            tif: v::TimeInForce::Gtc,
            post_only: false,
            reduce_only: false,
            check_stp: true,
            dry_run: false,
            now_mono_ns: 10_000,
        })
        .unwrap();
    assert!(placed.should_send_to_venue);
    assert_eq!(mgr.stp_book().best_bid_ticks(0), Some(5_000_000));

    // 3. Colliding order_seqnum = 9 (9 & 3 == 1) while seqnum 5 is still LIVE refuses with
    //    ALLOCATION_ERROR without evicting seqnum 5!
    let collision = mgr
        .place_order(&OrsNewOrderRequest {
            strat_id: 3,
            account_ref: 0,
            msg_seqnum: 4,
            order_seqnum: 9,
            symbol_id: 0,
            side: v::Side::Buy,
            price_ticks: 4_900_000,
            qty_lots: 50,
            tif: v::TimeInForce::Gtc,
            post_only: false,
            reduce_only: false,
            check_stp: true,
            dry_run: false,
            now_mono_ns: 11_000,
        })
        .unwrap_err();
    assert_eq!(collision.reason, v::RefusalReason::ALLOCATION_ERROR);
    assert!(mgr.lock_order(placed.ors_order_id).is_some());

    // 4. Full fill via TRADE_LITE (`fee: None` — fee NOT yet reconciled!) MUST immediately retire
    //    the order slot AND unlink from OrsStpBook in the SAME call!
    let lite_rep = mgr
        .on_fill(&OrsFillObservation {
            ors_order_id: placed.ors_order_id,
            account_ref: 1,
            symbol_id: 0,
            side: v::Side::Buy,
            liquidity: v::Liquidity::Taker,
            exec_ref: 777,
            trade_id: InlineAscii::try_from_str("777").unwrap(),
            qty_lots: 100,
            price_ticks: 5_000_000,
            fee: None,
            received_mono_ns: 12_000,
        })
        .unwrap()
        .expect("TRADE_LITE fill report");
    let OrsReport::Fill(fill_r) = lite_rep else {
        panic!("expected Fill report");
    };
    assert!(!fill_r.amendment);
    assert!(!fill_r.projected);
    assert!(fill_r.order_retired);
    assert!(mgr.lock_order(placed.ors_order_id).is_none());
    assert_eq!(mgr.stp_book().best_bid_ticks(0), None);

    // 5. Slot (offset 1) is immediately reusable by order_seqnum = 13 (13 & 3 == 1)!
    let reused = mgr
        .place_order(&OrsNewOrderRequest {
            strat_id: 3,
            account_ref: 1,
            msg_seqnum: 5,
            order_seqnum: 13,
            symbol_id: 0,
            side: v::Side::Buy,
            price_ticks: 4_800_000,
            qty_lots: 50,
            tif: v::TimeInForce::Gtc,
            post_only: false,
            reduce_only: false,
            check_stp: true,
            dry_run: false,
            now_mono_ns: 13_000,
        })
        .unwrap();
    assert_eq!(reused.slot_idx, placed.slot_idx);

    // 6. Now the delayed full TRADE + fee patch for retired order 5 (`exec_ref = 777`) arrives!
    //    `FillRecordPool` matches it, updates ONLY the fee delta in `UtaWalletLedger`, leaves
    //    slot occupant 13 untouched, and emits `Fill { amendment: true }`!
    let (btc_before, _) = mgr
        .wallet_ledger()
        .balance(1, v::AssetKey::new(WALLET_SPOT, ASSET_BTC))
        .unwrap();
    let (usdt_before, usdt_locked_before) = mgr
        .wallet_ledger()
        .balance(1, v::AssetKey::new(WALLET_SPOT, ASSET_USDT))
        .unwrap();

    let fee_patch = mgr
        .on_fill(&OrsFillObservation {
            ors_order_id: placed.ors_order_id,
            account_ref: 1,
            symbol_id: 0,
            side: v::Side::Buy,
            liquidity: v::Liquidity::Taker,
            exec_ref: 777,
            trade_id: InlineAscii::try_from_str("777").unwrap(),
            qty_lots: 100,
            price_ticks: 5_000_000,
            fee: Some(OrsFeeSpec::new(
                v::AssetKey::new(WALLET_SPOT, ASSET_USDT),
                -2_500_000, // -2.50 USDT commission
                6,
            )),
            received_mono_ns: 14_000,
        })
        .unwrap()
        .expect("amendment fill report");
    let OrsReport::Fill(amend_r) = fee_patch else {
        panic!("expected amendment Fill report");
    };
    assert!(amend_r.amendment);
    assert!(!amend_r.projected);
    let venue_fill = amend_r.to_venue_fill(&mgr).unwrap();
    assert!(venue_fill.amendment);

    // BTC quantity was NOT double-credited; USDT locked for order 13 was NOT touched; only -2.50
    // USDT fee was deducted!
    let (btc_after, _) = mgr
        .wallet_ledger()
        .balance(1, v::AssetKey::new(WALLET_SPOT, ASSET_BTC))
        .unwrap();
    let (usdt_after, usdt_locked_after) = mgr
        .wallet_ledger()
        .balance(1, v::AssetKey::new(WALLET_SPOT, ASSET_USDT))
        .unwrap();
    assert_eq!(btc_after, btc_before);
    assert_eq!(usdt_locked_after, usdt_locked_before);
    assert_eq!(usdt_after, usdt_before - 2_500_000);

    // Duplicate arrival of the exact same full trade is suppressed (`None`).
    let dup = mgr
        .on_fill(&OrsFillObservation {
            ors_order_id: placed.ors_order_id,
            account_ref: 1,
            symbol_id: 0,
            side: v::Side::Buy,
            liquidity: v::Liquidity::Taker,
            exec_ref: 777,
            trade_id: InlineAscii::try_from_str("777").unwrap(),
            qty_lots: 100,
            price_ticks: 5_000_000,
            fee: Some(OrsFeeSpec::new(
                v::AssetKey::new(WALLET_SPOT, ASSET_USDT),
                -2_500_000,
                6,
            )),
            received_mono_ns: 15_000,
        })
        .unwrap();
    assert!(dup.is_none());

    // Post-trade maker rebate revision (`+1.00 USDT` instead of `-2.50 USDT`) emits a second
    // `amendment = true` with net delta `+3.50 USDT`!
    let rebate_rev = mgr
        .on_fill(&OrsFillObservation {
            ors_order_id: placed.ors_order_id,
            account_ref: 1,
            symbol_id: 0,
            side: v::Side::Buy,
            liquidity: v::Liquidity::Maker,
            exec_ref: 777,
            trade_id: InlineAscii::try_from_str("777").unwrap(),
            qty_lots: 100,
            price_ticks: 5_000_000,
            fee: Some(OrsFeeSpec::new(
                v::AssetKey::new(WALLET_SPOT, ASSET_USDT),
                1_000_000,
                6,
            )),
            received_mono_ns: 16_000,
        })
        .unwrap()
        .expect("rebate revision report");
    let OrsReport::Fill(rebate_r) = rebate_rev else {
        panic!("expected Fill");
    };
    assert!(rebate_r.amendment);
    assert_eq!(rebate_r.fee.unwrap().signed_atoms, 3_500_000);
    let (usdt_final, _) = mgr
        .wallet_ledger()
        .balance(1, v::AssetKey::new(WALLET_SPOT, ASSET_USDT))
        .unwrap();
    assert_eq!(usdt_final, usdt_before + 1_000_000);
}

#[test]
fn fill_record_pool_lazy_retention_reuse_respects_fill_memory_s() {
    // 2^2 = 4 buckets, 10s retention window.
    let pool = FillRecordPool::new(2, 10, MlockPolicy::BestEffort).unwrap();
    assert_eq!(pool.bucket_count(), 4);

    // Insert 32 distinct executions at t = 1s (fills all 4 * 8 = 32 ways).
    let mut inserted = 0usize;
    let mut probe_id = 1u64;
    while inserted < 32 {
        let res = pool.record_or_amend(FillObservationInput {
            key: FillKey {
                ors_order_id: make_ors_order_id(probe_id, 0),
                exec_ref: probe_id,
                side: v::Side::Buy,
                symbol_id: 0,
            },
            qty_lots: 1,
            price_ticks: 100,
            fee: None,
            now_mono_ns: 1_000_000_000,
        });
        if matches!(res, Ok(FillRecordOutcome::Applied { .. })) {
            inserted += 1;
        }
        probe_id += 1;
    }

    // Before retention expiry (t = 5s < 11s), inserting into a full bucket refuses with
    // ALLOCATION_ERROR rather than evicting an unexpired dedup record.
    let early = pool.record_or_amend(FillObservationInput {
        key: FillKey {
            ors_order_id: make_ors_order_id(999_999, 0),
            exec_ref: 999_999,
            side: v::Side::Buy,
            symbol_id: 0,
        },
        qty_lots: 1,
        price_ticks: 100,
        fee: None,
        now_mono_ns: 5_000_000_000,
    });
    assert_eq!(early, Err(v::RefusalReason::ALLOCATION_ERROR));

    // At t = 12s (age = 11s >= 10s retention), lazy overwrite reuse succeeds in O(1)!
    let reused = pool
        .record_or_amend(FillObservationInput {
            key: FillKey {
                ors_order_id: make_ors_order_id(999_999, 0),
                exec_ref: 999_999,
                side: v::Side::Buy,
                symbol_id: 0,
            },
            qty_lots: 1,
            price_ticks: 100,
            fee: None,
            now_mono_ns: 12_000_000_000,
        })
        .unwrap();
    assert!(matches!(reused, FillRecordOutcome::Applied { .. }));
}

#[test]
fn one_ack_front_ack_venue_modify_projected_fill_cancel_before_ack_and_reasons() {
    let mgr = build_manager(16, true);
    mgr.dop_state().configure_strategy(1, &[1]).unwrap();
    mgr.dop_state().login(1, 1).unwrap();

    // Order 1: FrontAck -> cancel-before-ack -> One Ack -> ProjectedFill -> Strategy Cancel (`NO_REASON`).
    let o1 = mgr
        .place_order(&OrsNewOrderRequest {
            strat_id: 1,
            account_ref: 1,
            msg_seqnum: 10,
            order_seqnum: 1,
            symbol_id: 1, // USD-M
            side: v::Side::Buy,
            price_ticks: 500_000, // 50,000.0
            qty_lots: 10,
            tif: v::TimeInForce::Gtc,
            post_only: false,
            reduce_only: false,
            check_stp: true,
            dry_run: false,
            now_mono_ns: 1_000,
        })
        .unwrap();

    // FrontAck once; duplicate FrontAck is None.
    let fa1 = mgr
        .on_front_ack(o1.ors_order_id, Some("EX-1001"), 1_100)
        .expect("first FrontAck");
    if let OrsReport::FrontAck(fa) = fa1 {
        assert_eq!(
            fa.to_venue_front_ack()
                .unwrap()
                .venue_order_id
                .unwrap()
                .as_str(),
            "EX-1001"
        );
    } else {
        panic!("expected FrontAck");
    }
    assert!(
        mgr.on_front_ack(o1.ors_order_id, Some("EX-1001"), 1_200)
            .is_none()
    );

    // Cancel before Ack (`allow_cancel_before_ack == true`): arms `cancel_on_ack`, `should_send_now == false`.
    let cxl_pre = mgr
        .request_cancel(&OrsCancelRequest {
            strat_id: 1,
            account_ref: 1,
            msg_seqnum: 11,
            ors_order_id: o1.ors_order_id,
            now_mono_ns: 1_300,
        })
        .unwrap();
    assert!(!cxl_pre.should_send_now);

    // First Ack transitions PendingNew -> Open and signals `dispatch_pending_cancel = true`.
    let ack1 = mgr
        .on_ack(o1.ors_order_id, Some("EX-1001"), None, None, 1_400)
        .unwrap();
    assert!(matches!(ack1.report, Some(OrsReport::Ack(_))));
    assert!(ack1.dispatch_pending_cancel);

    // Second identical Ack (from the other stream) is a no-op!
    let ack2 = mgr
        .on_ack(o1.ors_order_id, Some("EX-1001"), None, None, 1_500)
        .unwrap();
    assert!(ack2.report.is_none());

    // Unsolicited venue modify (`3.7`) via second Ack / MODIFIED moving qty 10 -> 8 emits `Modified`.
    let vmod = mgr
        .on_ack(
            o1.ors_order_id,
            Some("EX-1001"),
            Some(500_000),
            Some(8),
            1_600,
        )
        .unwrap();
    assert!(matches!(vmod.report, Some(OrsReport::Modified(_))));

    // ProjectedFill (`on_cum_qty`) advances watermark to 3 lots; duplicate/stale 2 lots is ignored.
    let proj = mgr
        .on_cum_qty(o1.ors_order_id, 3, 500_000, 1_700)
        .unwrap()
        .expect("ProjectedFill");
    if let OrsReport::Fill(pf) = proj {
        assert!(pf.projected);
        assert!(!pf.amendment);
        assert_eq!(pf.last_qty_lots, 3);
    } else {
        panic!("expected projected Fill");
    }
    assert!(
        mgr.on_cum_qty(o1.ors_order_id, 2, 500_000, 1_750)
            .unwrap()
            .is_none()
    );

    // Terminal cancel releases `8 - 3 = 5` lots (never double-deducting the 3 projected lots!)
    // and reports `NO_REASON` because `pending_cxl > 0`.
    let cxl_rep = mgr
        .on_canceled(&OrsCancelObservation {
            ors_order_id: o1.ors_order_id,
            cum_qty_lots: None,
            error_code: None,
            status_or_msg: "CANCELED",
            stp_mode_or_reason: None,
            explicit_reason: None,
            received_mono_ns: 1_800,
        })
        .unwrap()
        .expect("Canceled report");
    if let OrsReport::Canceled(cr) = cxl_rep {
        assert_eq!(cr.canceled_qty_lots, 5);
        assert_eq!(cr.reason, v::TerminalReason::NO_REASON);
    } else {
        panic!("expected Canceled");
    }

    // Order 2: Unsolicited exchange cancel (`pending_cxl == 0`) -> `MKT_CANCELED`;
    // Order 3: Post-only cancel -> `MKT_REJECT_POST_ONLY`;
    // Order 4: IOC expiry -> `MKT_IOC_CANCELED`;
    // Order 5: Venue STP cancel -> `MKT_CANCELED_SELFTRADE`;
    // Order 6: Reduce-only cancel -> `MKT_REDUCE_ONLY_CANCELED`.
    let cases = [
        (
            2u64,
            v::TimeInForce::Gtc,
            false,
            false,
            "CANCELED",
            None,
            v::TerminalReason::MKT_CANCELED,
        ),
        (
            3u64,
            v::TimeInForce::PostOnly,
            true,
            false,
            "GTX_ORDER_REJECT",
            None,
            v::TerminalReason::MKT_REJECT_POST_ONLY,
        ),
        (
            4u64,
            v::TimeInForce::Ioc,
            false,
            false,
            "EXPIRED",
            None,
            v::TerminalReason::MKT_IOC_CANCELED,
        ),
        (
            5u64,
            v::TimeInForce::Gtc,
            false,
            false,
            "EXPIRED_IN_MATCH",
            Some("EXPIRE_TAKER"),
            v::TerminalReason::MKT_CANCELED_SELFTRADE,
        ),
        (
            6u64,
            v::TimeInForce::Gtc,
            false,
            true,
            "REDUCE_ONLY_REJECT",
            None,
            v::TerminalReason::MKT_REDUCE_ONLY_CANCELED,
        ),
    ];
    for (msg_seq, (oseq, tif, post_only, reduce_only, msg, stp_tok, expected_reason)) in
        (12u64..).zip(cases)
    {
        let p = mgr
            .place_order(&OrsNewOrderRequest {
                strat_id: 1,
                account_ref: 1,
                msg_seqnum: msg_seq,
                order_seqnum: oseq,
                symbol_id: 1,
                side: v::Side::Buy,
                price_ticks: 490_000,
                qty_lots: 5,
                tif,
                post_only,
                reduce_only,
                check_stp: true,
                dry_run: false,
                now_mono_ns: 2_000 + oseq,
            })
            .unwrap();
        let rep = mgr
            .on_canceled(&OrsCancelObservation {
                ors_order_id: p.ors_order_id,
                cum_qty_lots: None,
                error_code: None,
                status_or_msg: msg,
                stp_mode_or_reason: stp_tok,
                explicit_reason: None,
                received_mono_ns: 3_000 + oseq,
            })
            .unwrap()
            .unwrap();
        let OrsReport::Canceled(c) = rep else {
            panic!("expected Canceled");
        };
        assert_eq!(c.reason, expected_reason);
    }

    // Also verify `allow_cancel_before_ack = false` refuses pre-ack cancels immediately.
    let mgr_no_pre = build_manager(8, false);
    mgr_no_pre.dop_state().configure_strategy(1, &[1]).unwrap();
    mgr_no_pre.dop_state().login(1, 1).unwrap();
    let p_unacked = mgr_no_pre
        .place_order(&OrsNewOrderRequest {
            strat_id: 1,
            account_ref: 1,
            msg_seqnum: 1,
            order_seqnum: 1,
            symbol_id: 1,
            side: v::Side::Buy,
            price_ticks: 490_000,
            qty_lots: 2,
            tif: v::TimeInForce::Gtc,
            post_only: false,
            reduce_only: false,
            check_stp: true,
            dry_run: false,
            now_mono_ns: 100,
        })
        .unwrap();
    let pre_err = mgr_no_pre
        .request_cancel(&OrsCancelRequest {
            strat_id: 1,
            account_ref: 1,
            msg_seqnum: 2,
            ors_order_id: p_unacked.ors_order_id,
            now_mono_ns: 110,
        })
        .unwrap_err();
    assert_eq!(pre_err.reason, v::RefusalReason::TooManyPendingActions);
}

#[test]
fn stp_lock_free_cross_dual_price_strategy_modify_and_check_stp_disabled() {
    let mgr = build_manager(16, true);
    mgr.dop_state().configure_strategy(0, &[1]).unwrap();
    mgr.dop_state().configure_strategy(1, &[1]).unwrap();
    mgr.dop_state().login(0, 1).unwrap();
    mgr.dop_state().login(1, 1).unwrap();

    // Strategy 0 rests a Buy on USD-M at 500_000 (`50,000.0`).
    let buy = mgr
        .place_order(&OrsNewOrderRequest {
            strat_id: 0,
            account_ref: 1,
            msg_seqnum: 1,
            order_seqnum: 1,
            symbol_id: 1,
            side: v::Side::Buy,
            price_ticks: 500_000,
            qty_lots: 10,
            tif: v::TimeInForce::Gtc,
            post_only: false,
            reduce_only: false,
            check_stp: true,
            dry_run: false,
            now_mono_ns: 100,
        })
        .unwrap();
    mgr.on_ack(buy.ors_order_id, Some("B-1"), None, None, 110)
        .unwrap();
    assert_eq!(mgr.stp_book().best_bid_ticks(1), Some(500_000));

    // Strategy 1 attempts a crossing Sell at 500_000 -> refused lock-free with MKT_CANCELED_SELFTRADE!
    let cross_err = mgr
        .place_order(&OrsNewOrderRequest {
            strat_id: 1,
            account_ref: 1,
            msg_seqnum: 1,
            order_seqnum: 1,
            symbol_id: 1,
            side: v::Side::Sell,
            price_ticks: 500_000,
            qty_lots: 5,
            tif: v::TimeInForce::Gtc,
            post_only: false,
            reduce_only: false,
            check_stp: true,
            dry_run: false,
            now_mono_ns: 120,
        })
        .unwrap_err();
    assert_eq!(cross_err.reason, v::RefusalReason::MKT_CANCELED_SELFTRADE);

    // Strategy 0 submits an upward modify `500_000 -> 505_000`: dual old+new price publishes 505_000!
    mgr.request_modify(&OrsModifyRequest {
        strat_id: 0,
        account_ref: 1,
        msg_seqnum: 2,
        ors_order_id: buy.ors_order_id,
        new_price_ticks: 505_000,
        new_qty_lots: 12,
        now_mono_ns: 130,
    })
    .unwrap();
    assert_eq!(mgr.stp_book().best_bid_ticks(1), Some(505_000));

    // Sell at 504_000 crosses the pending 505_000 modify price -> refused!
    let cross_pend = mgr
        .place_order(&OrsNewOrderRequest {
            strat_id: 1,
            account_ref: 1,
            msg_seqnum: 2,
            order_seqnum: 2,
            symbol_id: 1,
            side: v::Side::Sell,
            price_ticks: 504_000,
            qty_lots: 5,
            tif: v::TimeInForce::Gtc,
            post_only: false,
            reduce_only: false,
            check_stp: true,
            dry_run: false,
            now_mono_ns: 140,
        })
        .unwrap_err();
    assert_eq!(cross_pend.reason, v::RefusalReason::MKT_CANCELED_SELFTRADE);

    // Modify reject rolls back both extra hold and STP best bid back to 500_000!
    let mod_rej = mgr
        .on_modify_rejected(buy.ors_order_id, v::TerminalReason::VenueRejected, 150)
        .unwrap();
    assert!(matches!(mod_rej, OrsReport::ModifyRejected(_)));
    assert_eq!(mgr.stp_book().best_bid_ticks(1), Some(500_000));

    // Downward modify `500_000 -> 495_000` keeps best_bid at 500_000 while in flight, then drops
    // to 495_000 once confirmed by `on_modify_ack`!
    mgr.request_modify(&OrsModifyRequest {
        strat_id: 0,
        account_ref: 1,
        msg_seqnum: 3,
        ors_order_id: buy.ors_order_id,
        new_price_ticks: 495_000,
        new_qty_lots: 10,
        now_mono_ns: 160,
    })
    .unwrap();
    assert_eq!(mgr.stp_book().best_bid_ticks(1), Some(500_000));
    mgr.on_modify_ack(buy.ors_order_id, 495_000, 10, Some("B-1"), 170)
        .unwrap();
    assert_eq!(mgr.stp_book().best_bid_ticks(1), Some(495_000));

    // Per-order `check_stp: false` bypasses STP when explicitly requested.
    let unguarded_sell = mgr
        .place_order(&OrsNewOrderRequest {
            strat_id: 1,
            account_ref: 1,
            msg_seqnum: 3,
            order_seqnum: 3,
            symbol_id: 1,
            side: v::Side::Sell,
            price_ticks: 495_000,
            qty_lots: 2,
            tif: v::TimeInForce::Gtc,
            post_only: false,
            reduce_only: false,
            check_stp: false,
            dry_run: false,
            now_mono_ns: 180,
        })
        .unwrap();
    assert!(unguarded_sell.should_send_to_venue);

    // `OrderManager<false>` disables ORS STP across the entire venue instance.
    let cfg_no_stp =
        OrderManagerConfig::single_account_default(sample_domain(1, 9001), multi_product_symbols());
    let mgr_no_stp = OrderManager::<false>::new(cfg_no_stp).unwrap();
    assert_eq!(mgr_no_stp.stp_book().num_symbols(), 0);
}

#[test]
fn dop_state_dual_sequence_login_routing_and_set_strat_enabled_auto_cancel() {
    let mgr = build_manager(16, true);
    mgr.dop_state().configure_strategy(5, &[1]).unwrap();

    // Login with floor 20; first request sets msg_seqnum baseline (e.g. 100).
    let ack = mgr.dop_state().login(5, 20).unwrap();
    assert_eq!(ack.next_order_seqnum, 20);
    assert_eq!(ack.default_account_ref, 1);

    // Duplicate login while active is rejected with INVALID_STRAT_ID.
    assert_eq!(
        mgr.dop_state().login(5, 25),
        Err(v::RefusalReason::INVALID_STRAT_ID)
    );

    // Place order with account_ref = 0 -> resolves to account 1, msg_seqnum baseline = 100.
    let o1 = mgr
        .place_order(&OrsNewOrderRequest {
            strat_id: 5,
            account_ref: 0,
            msg_seqnum: 100,
            order_seqnum: 20,
            symbol_id: 1,
            side: v::Side::Buy,
            price_ticks: 480_000,
            qty_lots: 5,
            tif: v::TimeInForce::Gtc,
            post_only: false,
            reduce_only: false,
            check_stp: true,
            dry_run: false,
            now_mono_ns: 1_000,
        })
        .unwrap();
    assert_eq!(o1.account_ref, 1);

    // Out-of-order msg_seqnum (102 instead of 101) refuses UNEXPECTED_MSG_SEQNUM WITHOUT advancing
    // last_recv_seqnum!
    let gap_err = mgr
        .place_order(&OrsNewOrderRequest {
            strat_id: 5,
            account_ref: 0,
            msg_seqnum: 102,
            order_seqnum: 21,
            symbol_id: 1,
            side: v::Side::Buy,
            price_ticks: 480_000,
            qty_lots: 5,
            tif: v::TimeInForce::Gtc,
            post_only: false,
            reduce_only: false,
            check_stp: true,
            dry_run: false,
            now_mono_ns: 1_010,
        })
        .unwrap_err();
    assert_eq!(gap_err.reason, v::RefusalReason::UNEXPECTED_MSG_SEQNUM);
    assert_eq!(mgr.dop_state().last_recv_seqnum(5), Some(100));

    // Unauthorized account_ref = 2 (registered on ORS, not authorized for strat 5) -> REJECT_ROUTING_ID_MISMATCH!
    let unauth_err = mgr
        .place_order(&OrsNewOrderRequest {
            strat_id: 5,
            account_ref: 2,
            msg_seqnum: 101,
            order_seqnum: 21,
            symbol_id: 1,
            side: v::Side::Buy,
            price_ticks: 480_000,
            qty_lots: 5,
            tif: v::TimeInForce::Gtc,
            post_only: false,
            reduce_only: false,
            check_stp: true,
            dry_run: false,
            now_mono_ns: 1_020,
        })
        .unwrap_err();
    assert_eq!(
        unauth_err.reason,
        v::RefusalReason::REJECT_ROUTING_ID_MISMATCH
    );

    // Unknown account_ref = 99 -> INVALID_ACCOUNT_ID!
    let unk_err = mgr
        .place_order(&OrsNewOrderRequest {
            strat_id: 5,
            account_ref: 99,
            msg_seqnum: 102,
            order_seqnum: 21,
            symbol_id: 1,
            side: v::Side::Buy,
            price_ticks: 480_000,
            qty_lots: 5,
            tif: v::TimeInForce::Gtc,
            post_only: false,
            reduce_only: false,
            check_stp: true,
            dry_run: false,
            now_mono_ns: 1_030,
        })
        .unwrap_err();
    assert_eq!(unk_err.reason, v::RefusalReason::INVALID_ACCOUNT_ID);

    // Logout & re-login preserves `next_order_seqnum = max(login_seq, 21) = 21` and resets `last_recv_seqnum = 0`.
    mgr.dop_state().logout(5).unwrap();
    let relogin = mgr.dop_state().login(5, 10).unwrap();
    assert_eq!(relogin.next_order_seqnum, 21);
    assert_eq!(mgr.dop_state().last_recv_seqnum(5), Some(0));

    // Disable `(account_ref = 1, strat_id = 5)` -> returns open order `o1` to auto-cancel with ADMIN_ACTION
    // and blocks new orders with RESTRICTED_CANCEL_ONLY!
    let to_cancel = mgr.set_strat_enabled_collect(1, 5, false).unwrap();
    assert_eq!(to_cancel.len(), 1);
    assert_eq!(to_cancel[0].ors_order_id, o1.ors_order_id);
    assert_eq!(to_cancel[0].reason, v::TerminalReason::ADMIN_ACTION);

    let disabled_err = mgr
        .place_order(&OrsNewOrderRequest {
            strat_id: 5,
            account_ref: 1,
            msg_seqnum: 1,
            order_seqnum: 21,
            symbol_id: 1,
            side: v::Side::Buy,
            price_ticks: 480_000,
            qty_lots: 5,
            tif: v::TimeInForce::Gtc,
            post_only: false,
            reduce_only: false,
            check_stp: true,
            dry_run: false,
            now_mono_ns: 1_040,
        })
        .unwrap_err();
    assert_eq!(
        disabled_err.reason,
        v::RefusalReason::RESTRICTED_CANCEL_ONLY
    );

    // When venue confirms cancel of `o1`, its terminal reason is ADMIN_ACTION!
    let admin_cxl_rep = mgr
        .on_canceled(&OrsCancelObservation {
            ors_order_id: o1.ors_order_id,
            cum_qty_lots: None,
            error_code: None,
            status_or_msg: "CANCELED",
            stp_mode_or_reason: None,
            explicit_reason: None,
            received_mono_ns: 1_050,
        })
        .unwrap()
        .unwrap();
    let OrsReport::Canceled(ac) = admin_cxl_rep else {
        panic!("expected Canceled");
    };
    assert_eq!(ac.reason, v::TerminalReason::ADMIN_ACTION);
}

#[test]
fn query_desk_coalescing_backoff_fills_before_cancel_eviction_and_cold_bootstrap() {
    let mgr = build_manager(16, true);
    mgr.dop_state().configure_strategy(2, &[1]).unwrap();
    mgr.dop_state().login(2, 1).unwrap();

    // Multi-wallet UTA check across Coin-M (symbol 2) and Option (symbol 3).
    let coinm_ord = mgr
        .place_order(&OrsNewOrderRequest {
            strat_id: 2,
            account_ref: 1,
            msg_seqnum: 1,
            order_seqnum: 1,
            symbol_id: 2, // Coin-M
            side: v::Side::Sell,
            price_ticks: 50_000,
            qty_lots: 10, // 10 * 100 USD / 50,000 = 0.02 BTC * 20% = 0.004 BTC = 400_000 sats
            tif: v::TimeInForce::Gtc,
            post_only: false,
            reduce_only: false,
            check_stp: true,
            dry_run: false,
            now_mono_ns: 100,
        })
        .unwrap();
    let (_, coinm_locked) = mgr
        .wallet_ledger()
        .balance(1, v::AssetKey::new(WALLET_COINM, ASSET_BTC))
        .unwrap();
    assert_eq!(coinm_locked, 400_000);

    let opt_ord = mgr
        .place_order(&OrsNewOrderRequest {
            strat_id: 2,
            account_ref: 1,
            msg_seqnum: 2,
            order_seqnum: 2,
            symbol_id: 3, // Option
            side: v::Side::Buy,
            price_ticks: 2_000,
            qty_lots: 100, // 1.00 BTC * 2000 * 15% = 300 USDT = 300_000_000 atoms
            tif: v::TimeInForce::Gtc,
            post_only: false,
            reduce_only: false,
            check_stp: true,
            dry_run: false,
            now_mono_ns: 110,
        })
        .unwrap();
    let (_, opt_locked) = mgr
        .wallet_ledger()
        .balance(1, v::AssetKey::new(WALLET_OPTION, ASSET_USDT))
        .unwrap();
    assert_eq!(opt_locked, 300_000_000);

    // Ambiguous `-1007` on `coinm_ord` schedules `StatusAsk` instead of retiring!
    assert!(
        mgr.on_rejected(coinm_ord.ors_order_id, Some(-1007), "TIMEOUT", None, 200)
            .is_none()
    );
    assert!(mgr.lock_order(coinm_ord.ors_order_id).is_some());

    // Strategy sends Cancel while Place `StatusAsk` is pending -> coalesces `cancel_after = true`!
    let coalesced = mgr
        .request_cancel(&OrsCancelRequest {
            strat_id: 2,
            account_ref: 1,
            msg_seqnum: 3,
            ors_order_id: coinm_ord.ors_order_id,
            now_mono_ns: 210,
        })
        .unwrap();
    assert!(coalesced.coalesced_in_query_desk);
    assert!(!coalesced.should_send_now);

    let mut due = [DueQueryAction::StatusAsk {
        ors_order_id: 0,
        account_ref: 0,
        symbol_id: 0,
        attempt: 0,
        cancel_after: false,
    }; 4];
    let n_due = mgr
        .query_desk()
        .poll_due(1, 250, &mut due, |s_idx, oid| mgr.is_slot_live(s_idx, oid));
    assert_eq!(n_due, 1);
    assert_eq!(
        due[0],
        DueQueryAction::StatusAsk {
            ors_order_id: coinm_ord.ors_order_id,
            account_ref: 1,
            symbol_id: 2,
            attempt: 1,
            cancel_after: true,
        }
    );

    // Snapshot reconciliation: order is Canceled with a 4-lot fill -> applies the 4-lot fill FIRST,
    // then settles the remaining 6 canceled lots!
    let trade = OrsFillObservation {
        ors_order_id: coinm_ord.ors_order_id,
        account_ref: 1,
        symbol_id: 2,
        side: v::Side::Sell,
        liquidity: v::Liquidity::Maker,
        exec_ref: 555,
        trade_id: InlineAscii::try_from_str("555").unwrap(),
        qty_lots: 4,
        price_ticks: 50_000,
        fee: None,
        received_mono_ns: 300,
    };
    let mut reconciled_reports = Vec::new();
    let action = mgr
        .reconcile_query_snapshot(
            &OrsOrderQuerySnapshot {
                ors_order_id: coinm_ord.ors_order_id,
                account_ref: 1,
                symbol_id: 2,
                status: OrsQueriedOrderStatus::Canceled,
                venue_order_id: Some("CM-1"),
                price_ticks: Some(50_000),
                orig_qty_lots: Some(10),
                cum_filled_lots: 4,
                avg_fill_price_ticks: Some(50_000),
                trades: &[trade],
                received_mono_ns: 300,
            },
            &mut reconciled_reports,
        )
        .unwrap();
    assert_eq!(action, OrsQueryReconcileAction::Resolved);
    assert_eq!(reconciled_reports.len(), 2);
    assert!(matches!(reconciled_reports[0], OrsReport::Fill(_)));
    if let OrsReport::Canceled(c) = reconciled_reports[1] {
        assert_eq!(c.canceled_qty_lots, 6);
        assert_eq!(c.cumulative_filled_lots, 4);
        assert_eq!(c.reason, v::TerminalReason::NO_REASON);
    } else {
        panic!("expected Canceled second");
    }
    assert_eq!(mgr.wallet_ledger().position_lots(1, 2), Some(-4));

    // Cancel exponential backoff up to 8 attempts on `opt_ord`, plus `max_query_count = Some(3)`
    // dead-slot release (`INTERNAL_CANCEL`)!
    for attempt in 1..=MAX_CANCEL_BACKOFF_ATTEMPTS {
        let sched = mgr
            .query_desk()
            .schedule_cancel_backoff(
                1,
                opt_ord.ors_order_id,
                opt_ord.slot_idx,
                2,
                3,
                2,
                1_000,
                |s_idx, oid| mgr.is_slot_live(s_idx, oid),
            )
            .unwrap();
        assert_eq!(sched.unwrap().cancel_attempts, attempt);
    }
    let exhausted = mgr
        .query_desk()
        .schedule_cancel_backoff(
            1,
            opt_ord.ors_order_id,
            opt_ord.slot_idx,
            2,
            3,
            2,
            1_000,
            |s_idx, oid| mgr.is_slot_live(s_idx, oid),
        )
        .unwrap();
    assert!(exhausted.is_none());

    // Reaching `max_query_count = 3` releases `opt_ord` with `INTERNAL_CANCEL`!
    assert!(
        mgr.on_query_inconclusive(opt_ord.ors_order_id, 2_000)
            .is_none()
    );
    assert!(
        mgr.on_query_inconclusive(opt_ord.ors_order_id, 3_000)
            .is_none()
    );
    let dead_rel = mgr
        .on_query_inconclusive(opt_ord.ors_order_id, 4_000)
        .expect("INTERNAL_CANCEL release at max_query_count=3");
    if let OrsReport::Canceled(c) = dead_rel {
        assert_eq!(c.reason, v::TerminalReason::INTERNAL_CANCEL);
    } else {
        panic!("expected INTERNAL_CANCEL");
    }
    assert!(mgr.lock_order(opt_ord.ors_order_id).is_none());

    // Stateless cold Exchange REST bootstrap (`Account + OpenOrders` + `tw1...`) on a fresh ORS!
    let cold_mgr = build_manager(16, true);
    cold_mgr.dop_state().configure_strategy(4, &[1]).unwrap();
    let domain_cfg = sample_domain(1, 9001);
    let tw1 = v::SelfDescribingClientId::from_ors_order(4, 0, domain_cfg.epoch, 42)
        .unwrap()
        .encode(&domain_cfg.client_id_domain(v::Product::UsdM))
        .unwrap();

    let boot = cold_mgr
        .bootstrap_from_exchange(
            1,
            &OrsExchangeBootstrapSnapshot {
                balances: vec![OrsRestBalanceRow {
                    key: v::AssetKey::new(WALLET_USDM, ASSET_USDT),
                    scale: 6,
                    free_atoms: 100_000_000_000,
                    locked_atoms: 0,
                }],
                positions: vec![OrsRestPositionRow {
                    symbol_id: 1,
                    signed_lots: 15,
                }],
                open_orders: vec![
                    OrsRestOpenOrderRow {
                        symbol_id: 1,
                        client_id: tw1.as_str().to_owned(),
                        venue_order_id: String::from("REST-42"),
                        side: v::Side::Sell,
                        tif: v::TimeInForce::Gtc,
                        post_only: true,
                        reduce_only: false,
                        price_ticks: 520_000,
                        orig_qty_lots: 10,
                        cum_filled_lots: 2,
                        created_mono_ns: 500,
                    },
                    OrsRestOpenOrderRow {
                        symbol_id: 1,
                        client_id: String::from("manual-gui-order"),
                        venue_order_id: String::from("REST-EXT"),
                        side: v::Side::Buy,
                        tif: v::TimeInForce::Gtc,
                        post_only: false,
                        reduce_only: false,
                        price_ticks: 450_000,
                        orig_qty_lots: 5,
                        cum_filled_lots: 0,
                        created_mono_ns: 500,
                    },
                ],
                balances_include_open_order_locks: false,
            },
        )
        .unwrap();

    assert_eq!(boot.managed_open_orders, 1);
    assert_eq!(boot.external_open_orders, 1);
    assert_eq!(cold_mgr.stp_book().best_ask_ticks(1), Some(520_000));
    assert_eq!(cold_mgr.dop_state().next_order_seqnum(4), Some(43));

    let mut found = [0u64; 4];
    let n_found = cold_mgr.find_orders(
        &OrsOrderFilter {
            strat_id: Some(4),
            account_ref: None,
            symbol_id: None,
        },
        &mut found,
    );
    assert_eq!(n_found, 1);
    assert_eq!(found[0], make_ors_order_id(42, 4));
}

#[test]
fn single_domain_account_trade_lite_then_fee_patch_amends_without_freezing() {
    let (s, mut c, cmd) = common::fixture();
    c.entries[0].fees.rates = [common::f(1_000_000), common::f(0), common::f(0)]; // 1% fee cap
    let mut a = tickwire_account::Account::from_snapshot(s, c).unwrap();
    let admission = a.admit(&common::canonical(cmd.clone()), 60).unwrap();

    // 1. TRADE_LITE arrives first (`fees` empty) filling all 0.001 BTC!
    let mut lite = common::fill(&cmd, "t-900", "0.001", "50000", "50");
    if let v::VenueEventBody::Fill(ref mut f_body) = lite.body {
        f_body.observation.status = v::OrderStatus::Filled;
        f_body.observation.cumulative_quantity = Some("0.001".parse().unwrap());
    }
    a.apply_event(&common::event(lite.clone())).unwrap();
    assert!(!a.frozen());
    let btc_after_lite = a.asset(common::key(1)).unwrap().total;
    let usdt_after_lite = a.asset(common::key(2)).unwrap().total;

    // 2. Full TRADE arrives later on the second stream with the SAME `trade_id = "t-900"` and a
    //    -0.000005 BTC (500 sat) commission: applies ONLY the 500 sat fee amendment, does NOT
    //    double-credit BTC or double-debit USDT, and does NOT freeze `Account`!
    let mut full = lite;
    full.identity.source_connection_epoch += 1;
    full.identity.event_id = common::venue::text("ev-full-t900");
    if let v::VenueEventBody::Fill(ref mut f_body) = full.body {
        f_body.fees = v::BoundedList::new(vec![v::AssetDelta {
            asset: cmd.context.instrument.base.clone(),
            amount: "-0.000005".parse().unwrap(),
        }])
        .unwrap();
    }
    a.apply_event(&common::event(full)).unwrap();
    assert!(!a.frozen());
    assert_eq!(a.asset(common::key(2)).unwrap().total, usdt_after_lite);
    assert_eq!(
        a.asset(common::key(1)).unwrap().total,
        btc_after_lite.checked_sub(common::f(500)).unwrap()
    );
    assert_eq!(
        a.order(&admission.orders()[0]).unwrap().filled_quantity,
        common::f(100_000)
    );
}
