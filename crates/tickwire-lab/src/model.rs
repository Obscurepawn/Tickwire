use crate::Result;
use tickwire_account::*;
use tickwire_venue as v;
pub fn model(
    now: u64,
    expires: u64,
) -> Result<(
    tickwire_account_codec::CanonicalBaseline,
    v::CanonicalCommand,
)> {
    let decimal = |n, e| v::Decimal::new(n, e).map_err(|e| format!("{e:?}"));
    let asset = |id, s: &str| v::AssetRef {
        id,
        version: 1,
        symbol: v::AssetSymbol::new(s).unwrap(),
        exponent: -8,
    };
    let instrument = v::InstrumentBinding {
        instrument_id: 12,
        instrument_version: 13,
        catalog_version: 14,
        catalog_hash: [5; 32],
        symbol: v::Symbol::new("BTCUSDT").unwrap(),
        base: asset(1, "BTC"),
        quote: asset(2, "USDT"),
        settlement: asset(2, "USDT"),
        price_tick: decimal(1, -2)?,
        quantity_step: decimal(1, -5)?,
        contract: v::ContractKind::Cash,
    };
    let units = |n: i128| Fixed::from_atoms(n * 100_000_000, 8).unwrap();
    let snapshot = Snapshot {
        scope: Scope {
            risk_domain_id: [1; 16],
            venue: 1,
            environment: v::Environment::Test,
            account: 2,
        },
        account_epoch: 1,
        ledger_sequence: 0,
        revisions: Revisions {
            risk: 10,
            fee: 1,
            bracket: 1,
            collateral: 1,
            valuation: 1,
        },
        valid_until: expires,
        assets: vec![
            (1, instrument.base.clone(), 1),
            (2, instrument.quote.clone(), 1000),
        ]
        .into_iter()
        .map(|(id, asset, n)| AssetSnapshot {
            key: AssetKey {
                wallet_id: 1,
                asset_id: id,
            },
            asset,
            free: units(n),
            locked: units(0),
            nontrade_locked: units(0),
            exclusive_fee_pool: false,
            ownership: vec![],
        })
        .collect(),
        positions: vec![],
        venue_available: vec![],
    };
    let catalog = Catalog {
        entries: vec![InstrumentPolicy {
            instrument: instrument.clone(),
            product: v::Product::Spot,
            wallet_id: 1,
            fees: FeePolicy {
                rates: [units(0); 3],
                maximum_fills: 10,
                exclusive_pool: None,
            },
            futures: None,
            max_absolute_position: units(100),
            max_gross_quantity: units(100),
            strategy_limits: vec![(8, units(100))],
        }],
    };
    let command = v::Command {
        context: v::CommandContext {
            request: v::RequestIdentity {
                risk_domain_id: [1; 16],
                venue: 1,
                environment: v::Environment::Test,
                account: 2,
                product: v::Product::Spot,
                intent_id: 3,
                intent_exec_epoch: 4,
                strategy_generation: 5,
                config_revision: 6,
                deadline: expires,
                recovery_object: 7,
            },
            strategy: v::StrategyContext {
                logical_strategy_id: 8,
                instance_id: [2; 16],
                strategy_intent_seq: 1,
                authorization_epoch: 9,
                risk_policy_version: 10,
                order_config_version: 11,
                created_mono_ns: now,
                economic_intent_id: [3; 16],
                hedge_group_id: [0; 16],
                leg_id: 0,
                recovery_manifest_hash: [4; 32],
            },
            instrument,
            triggers: v::BoundedList::new(vec![v::TriggerRef {
                topic_id: 101,
                feed_generation: 1,
                ring_epoch: 1,
                sequence: 1,
                instrument_id: 12,
                instrument_version: 13,
                observed_mono_ns: now,
            }])
            .unwrap(),
        },
        body: v::CommandBody::New(v::OrderSpec {
            client_id: v::BoundedText::new("lab-spot-1").unwrap(),
            side: v::Side::Buy,
            order_type: v::OrderType::Limit,
            quantity: v::Quantity::Base(decimal(1, -3)?),
            price: Some(decimal(60000, 0)?),
            time_in_force: Some(v::TimeInForce::Gtc),
            position_side: v::PositionSide::Both,
            mode: v::ProductMode::Spot,
            reduce_only: false,
            self_trade_prevention: v::SelfTradePrevention::Default,
            trigger: None,
            trailing: None,
            iceberg_quantity: None,
        }),
    };
    Ok((
        tickwire_account_codec::CanonicalBaseline::new(snapshot, catalog)
            .map_err(|e| format!("{e:?}"))?,
        v::CanonicalCommand::new(command).map_err(|e| format!("{e:?}"))?,
    ))
}

/// Both instruments are economically known before the market subscription grows.
pub fn expansion_model(
    now: u64,
    expires: u64,
) -> Result<(
    tickwire_account_codec::CanonicalBaseline,
    v::CanonicalCommand,
)> {
    let (base, command) = model(now, expires)?;
    let mut snapshot = base.snapshot().clone();
    let mut catalog = base.catalog().clone();
    let eth = v::AssetRef {
        id: 3,
        version: 1,
        symbol: v::AssetSymbol::new("ETH").unwrap(),
        exponent: -8,
    };
    snapshot.assets.push(AssetSnapshot {
        key: AssetKey {
            wallet_id: 1,
            asset_id: 3,
        },
        asset: eth.clone(),
        free: Fixed::from_atoms(0, 8).unwrap(),
        locked: Fixed::from_atoms(0, 8).unwrap(),
        nontrade_locked: Fixed::from_atoms(0, 8).unwrap(),
        exclusive_fee_pool: false,
        ownership: vec![],
    });
    let mut additional = catalog.entries[0].clone();
    additional.instrument.instrument_id = u64::MAX;
    additional.instrument.base = eth;
    additional.instrument.symbol = v::Symbol::new("ETHUSDT").unwrap();
    catalog.entries.push(additional);
    Ok((
        tickwire_account_codec::CanonicalBaseline::new(snapshot, catalog)
            .map_err(|e| format!("{e:?}"))?,
        command,
    ))
}
