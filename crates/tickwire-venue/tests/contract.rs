mod common;
use common::*;
use tickwire_venue::*;

#[test]
fn complete_command_is_immutable_and_canonical_roundtrip() {
    let source = command();
    let prepared = CanonicalCommand::new(source.clone()).unwrap();
    assert_eq!(prepared.command(), &source);
    assert_eq!(
        CanonicalCommand::decode(prepared.bytes()).unwrap(),
        prepared
    );
}

#[test]
fn economic_fields_change_the_digest() {
    let original = CanonicalCommand::new(command()).unwrap();
    let mut changed = command();
    if let CommandBody::New(ref mut order) = changed.body {
        order.side = Side::Sell;
    }
    assert_ne!(
        original.digest(),
        CanonicalCommand::new(changed).unwrap().digest()
    );
}

#[test]
fn market_with_price_is_rejected() {
    let mut changed = command();
    if let CommandBody::New(ref mut order) = changed.body {
        order.order_type = OrderType::Market;
    }
    assert_eq!(
        CanonicalCommand::new(changed).unwrap_err(),
        Error::OrderCombination
    );
}

#[test]
fn decimal_normalization_and_minimum_integer() {
    assert_eq!(d(1200, -3), d(12, -1));
    assert_eq!(d(1200, -3).as_plain_string(), "1.2");
    assert_eq!(d(i128::MIN, 0).as_plain_string(), i128::MIN.to_string());
}

#[test]
fn unicode_catalog_symbols_are_preserved_without_folding() {
    let mut c = command();
    c.context.instrument.symbol = Symbol::new("Ω_Δ_123").unwrap();
    c.context.instrument.base.symbol = AssetSymbol::new("€").unwrap();
    let canonical = CanonicalCommand::new(c.clone()).unwrap();
    assert_eq!(
        CanonicalCommand::decode(canonical.bytes())
            .unwrap()
            .command(),
        &c
    );
}

#[test]
fn rpi_time_in_force_and_multi_product_routing_semantics() {
    let mut rpi_cmd = command();
    if let CommandBody::New(ref mut order) = rpi_cmd.body {
        order.time_in_force = Some(TimeInForce::Rpi);
    }
    let encoded_rpi = CanonicalCommand::new(rpi_cmd.clone()).unwrap();
    assert_eq!(
        CanonicalCommand::decode(encoded_rpi.bytes())
            .unwrap()
            .command(),
        &rpi_cmd
    );

    // RPI cannot combine with iceberg quantity or non-Limit order types.
    let mut bad_rpi = rpi_cmd.clone();
    if let CommandBody::New(ref mut order) = bad_rpi.body {
        order.iceberg_quantity = Some(d(5, -3));
    }
    assert_eq!(
        CanonicalCommand::new(bad_rpi).unwrap_err(),
        Error::OrderCombination
    );

    // Multi-product aliases and USD-M / COIN-M commands round-trip cleanly.
    assert_eq!(Product::Options, Product::Option);
    assert_eq!(Product::LinearPerp, Product::LinearPerpetual);
    assert_eq!(Product::InversePerp, Product::InversePerpetual);
    assert_eq!(Product::LinearFutures, Product::LinearFuture);
    assert_eq!(Product::InverseFutures, Product::InverseFuture);
    for (idx, p) in Product::ALL.iter().copied().enumerate() {
        assert_eq!(p.code() as usize, idx + 1);
        assert_eq!(Product::from_code(p.code()), Some(p));
        assert_eq!(p.mask_bit(), 1u16 << idx);
    }

    for prod in [Product::UsdM, Product::CoinM] {
        let mut deriv = command();
        deriv.context.request.product = prod;
        deriv.context.instrument.contract = ContractKind::Perpetual;
        if let CommandBody::New(ref mut order) = deriv.body {
            order.mode = ProductMode::Derivative {
                position_mode: PositionMode::OneWay,
                margin_mode: MarginMode::Cross,
            };
        }
        let prepared = CanonicalCommand::new(deriv.clone()).unwrap();
        assert_eq!(
            CanonicalCommand::decode(prepared.bytes())
                .unwrap()
                .command(),
            &deriv
        );
    }

    // AssetKey partitions wallet collateral cleanly.
    let spot_usdt = AssetKey::new(0, 100);
    let usdm_usdt = AssetKey::new(1, 100);
    assert_ne!(spot_usdt, usdm_usdt);

    // OrderRoute 9/8 integer EWMA hysteresis picker.
    let routes = [
        OrderRoute {
            connection_id: 10,
            account_ref: 0,
            ready: true,
            colo: false,
            product_mask: Product::Spot.mask_bit(),
            local_ip: 0x0a00_0001,
            ema_score_us: 100,
        },
        OrderRoute {
            connection_id: 20,
            account_ref: 0,
            ready: true,
            colo: true,
            product_mask: Product::Spot.mask_bit() | Product::UsdM.mask_bit(),
            local_ip: 0x0a00_0002,
            ema_score_us: 90, // 90 * 9 = 810 >= 100 * 8 = 800 -> keeps sticky route 10!
        },
        OrderRoute {
            connection_id: 30,
            account_ref: 0,
            ready: true,
            colo: true,
            product_mask: 0, // universal product mask
            local_ip: 0x0a00_0003,
            ema_score_us: 80, // 80 * 9 = 720 < 100 * 8 = 800 -> switches to route 30!
        },
    ];
    assert_eq!(
        pick_order_route(&routes[..2], Some(Product::Spot), 10, false),
        Some(10)
    );
    assert_eq!(
        pick_order_route(&routes, Some(Product::Spot), 10, false),
        Some(30)
    );
    assert_eq!(
        pick_order_route(&routes, Some(Product::Spot), 10, true),
        Some(10)
    );
    assert_eq!(
        pick_order_route(
            &routes[..2],
            Some(Product::UsdM),
            OrderRoute::NO_CONNECTION,
            false
        ),
        Some(20)
    );
}

#[test]
fn ors_front_ack_and_amendment_fill_validation() {
    let front_ack = FrontAck {
        ors_order_id: 42,
        strat_id: 3,
        account_ref: 0,
        order_seqnum: 1001,
        orig_seqnum: 1001,
        client_id: Some(text("tw100000000000000000000000000000")),
        venue_order_id: Some(text("998877")),
        received_mono_ns: 500_000,
    };
    front_ack.validate().unwrap();

    let mut invalid_ack = front_ack.clone();
    invalid_ack.strat_id = ALL_ORS_STRATS;
    assert_eq!(invalid_ack.validate().unwrap_err(), Error::Identity);

    let quote_asset = command().context.instrument.quote;
    let normal_fill = Fill {
        ors_order_id: 42,
        strat_id: 3,
        account_ref: 0,
        order_seqnum: 1001,
        exec_ref: 7001,
        trade_id: Some(VenueTradeId::new("trd-1").unwrap()),
        side: Side::Buy,
        liquidity: Liquidity::Maker,
        last_quantity: d(25, -3),
        last_price: d(650_000, -1),
        cumulative_filled: d(25, -3),
        cumulative_projected: d(25, -3),
        fee: Some(AssetDelta {
            asset: quote_asset.clone(),
            amount: d(-15, -2),
        }),
        amendment: false,
        projected: false,
        received_mono_ns: 600_000,
    };
    normal_fill.validate().unwrap();

    // Post-trade fee/rebate amendment fill allows zero last_quantity when fee delta is present.
    let mut amendment_fill = normal_fill.clone();
    amendment_fill.amendment = true;
    amendment_fill.last_quantity = d(0, 0);
    amendment_fill.fee = Some(AssetDelta {
        asset: quote_asset,
        amount: d(5, -2),
    });
    amendment_fill.validate().unwrap();

    // Amendment fills cannot be projected and must include a fee delta.
    let mut bad_projected = amendment_fill.clone();
    bad_projected.projected = true;
    assert_eq!(
        bad_projected.validate().unwrap_err(),
        Error::OrderCombination
    );

    let mut missing_fee = amendment_fill;
    missing_fee.fee = None;
    assert_eq!(missing_fee.validate().unwrap_err(), Error::Field);
}
