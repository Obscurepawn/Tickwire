#![allow(dead_code)]
use tickwire_venue::*;
pub fn d(n: i128, e: i8) -> Decimal {
    Decimal::new(n, e).unwrap()
}
pub fn text<const N: usize>(s: &str) -> BoundedText<N> {
    BoundedText::new(s).unwrap()
}
pub fn asset(id: u64, symbol: &str) -> AssetRef {
    AssetRef {
        id,
        version: 1,
        symbol: AssetSymbol::new(symbol).unwrap(),
        exponent: -8,
    }
}
pub fn command() -> Command {
    Command {
        context: CommandContext {
            request: RequestIdentity {
                risk_domain_id: [1; 16],
                venue: 1,
                environment: Environment::Test,
                account: 2,
                product: Product::Spot,
                intent_id: 3,
                intent_exec_epoch: 4,
                strategy_generation: 5,
                config_revision: 6,
                deadline: 100,
                recovery_object: 7,
            },
            strategy: StrategyContext {
                logical_strategy_id: 8,
                instance_id: [2; 16],
                strategy_intent_seq: 1,
                authorization_epoch: 9,
                risk_policy_version: 10,
                order_config_version: 11,
                created_mono_ns: 50,
                economic_intent_id: [3; 16],
                hedge_group_id: [0; 16],
                leg_id: 0,
                recovery_manifest_hash: [4; 32],
            },
            instrument: InstrumentBinding {
                instrument_id: 12,
                instrument_version: 13,
                catalog_version: 14,
                catalog_hash: [5; 32],
                symbol: Symbol::new("BTCUSDT").unwrap(),
                base: asset(1, "BTC"),
                quote: asset(2, "USDT"),
                settlement: asset(2, "USDT"),
                price_tick: d(1, -2),
                quantity_step: d(1, -5),
                contract: ContractKind::Cash,
            },
            triggers: BoundedList::new(vec![TriggerRef {
                topic_id: 1,
                feed_generation: 1,
                ring_epoch: 1,
                sequence: 1,
                instrument_id: 12,
                instrument_version: 13,
                observed_mono_ns: 49,
            }])
            .unwrap(),
        },
        body: CommandBody::New(OrderSpec {
            client_id: text("test-1"),
            side: Side::Buy,
            order_type: OrderType::Limit,
            quantity: Quantity::Base(d(1, -3)),
            price: Some(d(60000, 0)),
            time_in_force: Some(TimeInForce::Gtc),
            position_side: PositionSide::Both,
            mode: ProductMode::Spot,
            reduce_only: false,
            self_trade_prevention: SelfTradePrevention::Default,
            trigger: None,
            trailing: None,
            iceberg_quantity: None,
        }),
    }
}
pub fn order() -> OrderSpec {
    if let CommandBody::New(o) = command().body {
        o
    } else {
        unreachable!()
    }
}
pub fn target() -> OrderTarget {
    OrderTarget {
        order_id: [9; 16],
        expected_owner_revision: 1,
        original_intent_exec_epoch: 2,
        client_id: Some(text("original")),
        venue_order_id: Some(text("native-9")),
    }
}
pub fn event() -> VenueEvent {
    VenueEvent {
        identity: EventIdentity {
            risk_domain_id: [1; 16],
            venue: 1,
            environment: Environment::Test,
            account: 2,
            product: Product::Spot,
            instrument_id: 12,
            instrument_version: 13,
            source_connection_epoch: 9,
            source_sequence: Some(10),
            event_id: text("trade-event-1"),
            venue_time_ns: Some(1234),
            received_mono_ns: 200,
        },
        body: VenueEventBody::Fill(Box::new(FillEvent {
            observation: OrderObservation {
                order: OrderIdentity {
                    order_id: [9; 16],
                    original_intent_id: 3,
                    original_intent_exec_epoch: 4,
                    client_id: Some(text("test-1")),
                    venue_order_id: Some(text("000123")),
                    parent_order_id: None,
                    list_id: None,
                    venue_algo_id: None,
                    venue_list_id: None,
                    list_client_id: None,
                    algo_client_id: None,
                },
                status: OrderStatus::PartiallyFilled,
                side: Side::Buy,
                position_side: PositionSide::Both,
                original_quantity: Some(d(1, -3)),
                cumulative_quantity: Some(d(5, -4)),
                cumulative_quote: None,
                leaves_quantity: None,
                order_price: Some(d(60000, 0)),
            },
            trade_id: text("0000456"),
            last_quantity: d(5, -4),
            last_price: d(60000, 0),
            last_quote: None,
            realized_pnl: None,
            liquidity: Liquidity::Maker,
            fees: BoundedList::new(vec![
                AssetDelta {
                    asset: asset(2, "USDT"),
                    amount: d(-2, -2),
                },
                AssetDelta {
                    asset: asset(3, "BNB"),
                    amount: d(1, -5),
                },
            ])
            .unwrap(),
        })),
    }
}
