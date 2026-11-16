//! Exact fixed-point economic parameters and `CanonicalCommand` builder for the `spot-fill`
//! 3-process laboratory scenario.

#![forbid(unsafe_code)]

use crate::Result;
use tickwire_account::Fixed;
use tickwire_venue::{
    self as v, ClientIdDomain, Environment, Product, SelfDescribingClientId, venue_id,
};

pub const LAB_RISK_DOMAIN: [u8; 16] = [0x4C; 16];
pub const LAB_EXCHANGE_UID: u64 = 900_002;
pub const LAB_STRAT_ID: u8 = 1;
pub const LAB_LOGICAL_STRATEGY_ID: u64 = 8;
pub const LAB_ACCOUNT_REF: u8 = 1;
pub const LAB_COLD_OPEN_SEQNUM: u64 = 3;

/// Active `BTCUSDT` instrument identifier (`1`, matching `symbol_id = 1` in `tickwire-app`).
pub const LAB_BTC_INSTRUMENT_ID: u64 = 1;
pub const LAB_INSTRUMENT_VERSION: u64 = 1;
pub const LAB_CATALOG_VERSION: u64 = 14;
pub const LAB_CATALOG_HASH: [u8; 32] = [5; 32];

/// Initial venue wallet balances (`1.00000000 BTC`, `1000.00000000 USDT` total with `60.00000000`
/// initially locked by the cold-recovered resting order `order_seqnum = 3`).
pub const INITIAL_BTC_FREE_ATOMS: i128 = 100_000_000;
pub const INITIAL_USDT_TOTAL_ATOMS: i128 = 100_000_000_000;
pub const ORDER_NOTIONAL_USDT_ATOMS: i128 = 6_000_000_000; // 60.00000000 USDT
pub const ORDER_QTY_BTC_ATOMS: i128 = 100_000; // 0.00100000 BTC (`100` lots at `0.00001` step)
pub const FILL_COMMISSION_USDT_ATOMS: i128 = 6_000_000; // 0.06000000 USDT (10 bps)

/// Returns the canonical `ClientIdDomain` for `BTCUSDT` Spot in the laboratory scenario.
pub fn spot_client_id_domain() -> ClientIdDomain {
    ClientIdDomain {
        risk_domain_id: LAB_RISK_DOMAIN,
        venue: venue_id::BINANCE,
        environment: Environment::Test,
        account: LAB_EXCHANGE_UID,
        product: Product::Spot,
    }
}

/// Mints the cold-recovered open order `tw1...` `SelfDescribingClientId` (`order_seqnum = 3`).
pub fn cold_open_order_client_id() -> Result<String> {
    let cid = SelfDescribingClientId::from_ors_order(LAB_STRAT_ID, 0, 1, LAB_COLD_OPEN_SEQNUM)
        .map_err(|e| format!("SelfDescribingClientId::from_ors_order failed: {e:?}"))?
        .encode(&spot_client_id_domain())
        .map_err(|e| format!("SelfDescribingClientId::encode failed: {e:?}"))?;
    Ok(cid.as_str().to_owned())
}

/// Expected post-fill / post-cancel wallet balances (`scale = 8`) once:
/// 1. Cold open order (`order_seqnum = 3`, `60 USDT` hold) is canceled (`60 USDT` unlocked), and
/// 2. Live strategy order (`0.001 BTC @ 60,000 USDT`, `0.06 USDT` commission) fills completely.
pub fn expected_post_fill_balances() -> (Fixed, Fixed) {
    let btc = Fixed::from_atoms(INITIAL_BTC_FREE_ATOMS + ORDER_QTY_BTC_ATOMS, 8)
        .expect("exact BTC post-fill balance");
    let usdt = Fixed::from_atoms(
        INITIAL_USDT_TOTAL_ATOMS - ORDER_NOTIONAL_USDT_ATOMS - FILL_COMMISSION_USDT_ATOMS,
        8,
    )
    .expect("exact USDT post-fill balance");
    (btc, usdt)
}

/// Builds the immutable `CanonicalCommand` template consumed by `tickwire-strategy-worker` (`FixedDemo`).
pub fn build_demo_command(now_mono_ns: u64, expires_mono_ns: u64) -> Result<v::CanonicalCommand> {
    let decimal = |n, e| v::Decimal::new(n, e).map_err(|err| format!("{err:?}"));
    let asset = |id, s: &str| -> Result<v::AssetRef> {
        Ok(v::AssetRef {
            id,
            version: 1,
            symbol: v::AssetSymbol::new(s).map_err(|err| format!("{err:?}"))?,
            exponent: -8,
        })
    };
    let instrument = v::InstrumentBinding {
        instrument_id: LAB_BTC_INSTRUMENT_ID,
        instrument_version: LAB_INSTRUMENT_VERSION,
        catalog_version: LAB_CATALOG_VERSION,
        catalog_hash: LAB_CATALOG_HASH,
        symbol: v::Symbol::new("BTCUSDT").map_err(|err| format!("{err:?}"))?,
        base: asset(1, "BTC")?,
        quote: asset(2, "USDT")?,
        settlement: asset(2, "USDT")?,
        price_tick: decimal(1, -2)?,
        quantity_step: decimal(1, -5)?,
        contract: v::ContractKind::Cash,
    };

    let command = v::Command {
        context: v::CommandContext {
            request: v::RequestIdentity {
                risk_domain_id: LAB_RISK_DOMAIN,
                venue: venue_id::BINANCE,
                environment: Environment::Test,
                account: LAB_EXCHANGE_UID,
                product: Product::Spot,
                intent_id: LAB_COLD_OPEN_SEQNUM + 1,
                intent_exec_epoch: 1,
                strategy_generation: 5,
                config_revision: 1,
                deadline: expires_mono_ns,
                recovery_object: 1,
            },
            strategy: v::StrategyContext {
                logical_strategy_id: LAB_LOGICAL_STRATEGY_ID,
                instance_id: [2; 16],
                strategy_intent_seq: LAB_COLD_OPEN_SEQNUM + 1,
                authorization_epoch: 1,
                risk_policy_version: 1,
                order_config_version: 1,
                created_mono_ns: now_mono_ns,
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
                instrument_id: LAB_BTC_INSTRUMENT_ID,
                instrument_version: LAB_INSTRUMENT_VERSION,
                observed_mono_ns: now_mono_ns,
            }])
            .map_err(|err| format!("{err:?}"))?,
        },
        body: v::CommandBody::New(v::OrderSpec {
            client_id: v::BoundedText::new("lab-spot-hint").map_err(|err| format!("{err:?}"))?,
            side: v::Side::Buy,
            order_type: v::OrderType::Limit,
            quantity: v::Quantity::Base(decimal(1, -3)?), // 0.001 BTC = 100 lots of 0.00001
            price: Some(decimal(60_000, 0)?),             // 60000.00 USDT = 6,000,000 ticks
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
    Ok(v::CanonicalCommand::new(command).map_err(|err| format!("{err:?}"))?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cold_client_id_roundtrips_and_post_fill_balances_are_exact() {
        let cid = cold_open_order_client_id().unwrap();
        assert_eq!(cid.len(), 32);
        assert!(cid.starts_with("tw1"));
        let v::DecodedClientId::Managed(decoded) =
            SelfDescribingClientId::decode_str(&cid, &spot_client_id_domain())
        else {
            panic!("expected DecodedClientId::Managed");
        };
        assert_eq!(decoded.ors_strat_id(), Some(LAB_STRAT_ID));
        assert_eq!(decoded.intent_id, LAB_COLD_OPEN_SEQNUM);

        let (btc, usdt) = expected_post_fill_balances();
        assert_eq!(btc.atoms(), 100_100_000);
        assert_eq!(usdt.atoms(), 93_994_000_000);
    }
}
