//! Cold stateless exchange bootstrap from authenticated Binance Spot / USDⓈ-M REST snapshots.

use crate::registry::Registration;
use crate::{Error, Result};
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};
use tickwire_account::{
    Catalog, Fixed, LiveOpenOrder, ManagerIdentity, PositionSnapshot, Snapshot,
};
use tickwire_account_gateway::VerifiedLiveBootstrap;
use tickwire_binance::{decimal::Decimal, request::Market};
use tickwire_venue as v;
use tickwire_venue::{ClientIdDomain, DecodedClientId, PositionSide, SelfDescribingClientId};

const MAX_BODY: usize = 64 * 1024;
const MAX_ROWS: usize = 1024;

#[derive(Clone, Debug)]
pub struct ImportedOrderRegistration {
    pub command: v::CanonicalCommand,
    pub order_id: [u8; 16],
    pub venue_order_id: v::VenueOrderId,
}

#[derive(Clone, Debug)]
pub struct RemoteLiveBootstrap {
    pub live: VerifiedLiveBootstrap,
    pub registrations: Vec<ImportedOrderRegistration>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct OpenOrderRow<'a> {
    symbol: &'a str,
    order_id: u64,
    client_order_id: &'a str,
    status: &'a str,
    side: &'a str,
    #[serde(rename = "type")]
    kind: &'a str,
    orig_qty: Decimal,
    executed_qty: Decimal,
    price: Decimal,
    #[serde(default)]
    stop_price: Option<Decimal>,
    time_in_force: &'a str,
    position_side: Option<&'a str>,
    reduce_only: Option<bool>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SpotAccount<'a> {
    can_trade: bool,
    account_type: &'a str,
    #[serde(borrow)]
    balances: Vec<SpotBalanceRow<'a>>,
}

#[derive(Deserialize)]
struct SpotBalanceRow<'a> {
    asset: &'a str,
    free: Decimal,
    locked: Decimal,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct UsdmAccount<'a> {
    can_trade: Option<bool>,
    multi_assets_margin: Option<bool>,
    #[serde(borrow)]
    assets: Vec<UsdmAssetRow<'a>>,
    #[serde(borrow)]
    positions: Vec<UsdmPositionRow<'a>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct UsdmAssetRow<'a> {
    asset: &'a str,
    wallet_balance: Decimal,
    available_balance: Decimal,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct UsdmPositionRow<'a> {
    symbol: &'a str,
    position_side: &'a str,
    position_amt: Decimal,
    entry_price: Option<Decimal>,
    #[serde(default)]
    isolated_margin: Option<Decimal>,
}

/// Reconstructs a `VerifiedLiveBootstrap` and `AccountIngress` registrations directly from
/// authenticated `/api/v3/account` + `/api/v3/openOrders` (Spot) or `/fapi/v3/account` +
/// `/fapi/v1/openOrders` (USDⓈ-M) JSON bodies without requiring prior local WAL files.
pub fn bootstrap_from_remote_snapshots(
    market: Market,
    mut template: Snapshot,
    catalog: Catalog,
    strategy_managers: &BTreeMap<u64, ManagerIdentity>,
    account_body: &[u8],
    open_orders_body: &[u8],
    freeze_on_external: bool,
) -> Result<RemoteLiveBootstrap> {
    if account_body.is_empty()
        || open_orders_body.is_empty()
        || account_body.len() > MAX_BODY
        || open_orders_body.len() > MAX_BODY
    {
        return Err(Error::Capacity);
    }

    for p in &catalog.entries {
        let matches = match market {
            Market::Spot => p.product == v::Product::Spot,
            Market::Usdm => matches!(
                p.product,
                v::Product::LinearPerpetual | v::Product::LinearFuture
            ),
        };
        if !matches {
            return Err(Error::Identity);
        }
    }

    match market {
        Market::Spot => populate_spot_balances(&mut template, account_body)?,
        Market::Usdm => populate_usdm_state(&mut template, &catalog, account_body)?,
    }

    let rows: Vec<OpenOrderRow<'_>> =
        serde_json::from_slice(open_orders_body).map_err(|_| Error::Encoding)?;
    if rows.len() > MAX_ROWS {
        return Err(Error::Capacity);
    }

    let mut live_orders = Vec::with_capacity(rows.len());
    let mut registrations = Vec::new();
    let mut seen_native = BTreeSet::new();

    for row in rows {
        if row.order_id == 0
            || row.order_id > i64::MAX as u64
            || !seen_native.insert((row.symbol, row.order_id))
            || !matches!(row.status, "NEW" | "PARTIALLY_FILLED" | "PENDING_NEW")
        {
            return Err(Error::Conflict);
        }
        let policy = catalog
            .entries
            .iter()
            .find(|p| p.instrument.symbol.as_str() == row.symbol)
            .ok_or(Error::Conflict)?;

        let venue_order_id =
            v::VenueOrderId::new(row.order_id.to_string()).map_err(|_| Error::Encoding)?;
        let base_scale = asset_scale(&policy.instrument.base)?;
        let filled_quantity = fixed(row.executed_qty, base_scale)?;

        let domain = ClientIdDomain {
            risk_domain_id: template.scope.risk_domain_id,
            venue: template.scope.venue,
            environment: template.scope.environment,
            account: template.scope.account,
            product: policy.product,
        };

        let decoded = SelfDescribingClientId::decode_str(row.client_order_id, &domain);
        match decoded {
            DecodedClientId::External => {
                // Use a valid bounded placeholder ClientId if the external venue string exceeds 36 bytes
                // or contains characters outside ClientId's charset; SelfDescribingClientId::decode will
                // still classify it as External.
                let external_cid = v::ClientId::new(row.client_order_id)
                    .or_else(|_| v::ClientId::new("external-unmanaged-order"))
                    .map_err(|_| Error::Encoding)?;
                let spec = build_order_spec(market, policy, &row, external_cid)?;
                let dummy_manager =
                    strategy_managers
                        .values()
                        .next()
                        .copied()
                        .unwrap_or(ManagerIdentity {
                            instance: [1; 16],
                            generation: 1,
                        });
                live_orders.push(LiveOpenOrder {
                    instrument_id: policy.instrument.instrument_id,
                    spec,
                    venue_order_id: Some(venue_order_id),
                    filled_quantity,
                    manager: dummy_manager,
                    command_digest: [0; 32],
                });
            }
            DecodedClientId::Managed(sd) => {
                let cid = v::ClientId::new(row.client_order_id).map_err(|_| Error::Encoding)?;
                let spec = build_order_spec(market, policy, &row, cid)?;
                let strategy_id = u64::from(sd.logical_strategy_id);
                let manager = *strategy_managers.get(&strategy_id).ok_or(Error::Identity)?;
                let order_id = sd.deterministic_order_id(&domain);
                let command = build_synthetic_command(
                    &template,
                    policy,
                    sd,
                    manager,
                    order_id,
                    spec.clone(),
                )?;
                let digest = command.digest();
                live_orders.push(LiveOpenOrder {
                    instrument_id: policy.instrument.instrument_id,
                    spec,
                    venue_order_id: Some(venue_order_id.clone()),
                    filled_quantity,
                    manager,
                    command_digest: digest,
                });
                registrations.push(ImportedOrderRegistration {
                    command,
                    order_id,
                    venue_order_id,
                });
            }
        }
    }

    let imported_commands = registrations.iter().map(|r| r.command.clone()).collect();
    let live = VerifiedLiveBootstrap::new(template, catalog, &live_orders, freeze_on_external)
        .map_err(Error::Gateway)?
        .with_imported_commands(imported_commands);
    Ok(RemoteLiveBootstrap {
        live,
        registrations,
    })
}

pub(crate) fn build_imported_registry(
    config: &crate::Config,
    imported: &[ImportedOrderRegistration],
) -> Result<BTreeMap<tickwire_account::IntentKey, Registration>> {
    let mut registry = BTreeMap::new();
    for item in imported {
        let key = crate::intent(&item.command);
        if registry.contains_key(&key) {
            return Err(Error::Conflict);
        }
        let mut reg =
            crate::registry::prepare(config, &registry, &item.command, &[item.order_id], false)?;
        reg.orders[0].venue_order_id = Some(item.venue_order_id.clone());
        registry.insert(key, reg);
    }
    Ok(registry)
}

fn populate_spot_balances(template: &mut Snapshot, body: &[u8]) -> Result<()> {
    let remote: SpotAccount<'_> = serde_json::from_slice(body).map_err(|_| Error::Encoding)?;
    if !remote.can_trade || remote.account_type != "SPOT" {
        return Err(Error::Unsupported);
    }
    if remote.balances.len() > MAX_ROWS {
        return Err(Error::Capacity);
    }
    let mut seen = BTreeSet::new();
    let mut by_symbol = BTreeMap::new();
    for row in remote.balances {
        if row.asset.is_empty()
            || !seen.insert(row.asset)
            || row.free.mantissa() < 0
            || row.locked.mantissa() < 0
        {
            return Err(Error::Encoding);
        }
        by_symbol.insert(row.asset, (row.free, row.locked));
    }
    for asset in &mut template.assets {
        let scale = asset_scale(&asset.asset)?;
        if let Some(&(free, locked)) = by_symbol.get(asset.asset.symbol.as_str()) {
            asset.free = fixed(free, scale)?;
            asset.locked = fixed(locked, scale)?;
        } else {
            let zero = Fixed::zero(scale).map_err(|_| Error::Encoding)?;
            asset.free = zero;
            asset.locked = zero;
        }
        // Attribute all settled wallet balances to shared account pool (strategy 0).
        asset.ownership.clear();
    }
    template.positions.clear();
    template.venue_available.clear();
    Ok(())
}

fn populate_usdm_state(template: &mut Snapshot, catalog: &Catalog, body: &[u8]) -> Result<()> {
    let remote: UsdmAccount<'_> = serde_json::from_slice(body).map_err(|_| Error::Encoding)?;
    if remote.can_trade == Some(false) || remote.multi_assets_margin == Some(true) {
        return Err(Error::Unsupported);
    }
    if remote.assets.len() > MAX_ROWS || remote.positions.len() > MAX_ROWS {
        return Err(Error::Capacity);
    }
    let mut seen_assets = BTreeSet::new();
    let mut by_symbol = BTreeMap::new();
    for row in remote.assets {
        if row.asset.is_empty()
            || !seen_assets.insert(row.asset)
            || row.wallet_balance.mantissa() < 0
            || row.available_balance.mantissa() < 0
        {
            return Err(Error::Encoding);
        }
        by_symbol.insert(row.asset, (row.wallet_balance, row.available_balance));
    }
    for asset in &mut template.assets {
        let scale = asset_scale(&asset.asset)?;
        let zero = Fixed::zero(scale).map_err(|_| Error::Encoding)?;
        if let Some(&(wallet, _)) = by_symbol.get(asset.asset.symbol.as_str()) {
            asset.free = fixed(wallet, scale)?;
        } else {
            asset.free = zero;
        }
        asset.locked = zero;
        asset.nontrade_locked = zero;
        asset.ownership.clear();
    }

    let mut venue_available = BTreeMap::new();
    for policy in &catalog.entries {
        let scale = asset_scale(&policy.instrument.settlement)?;
        let (_, avail_dec) = by_symbol
            .get(policy.instrument.settlement.symbol.as_str())
            .copied()
            .ok_or(Error::Conflict)?;
        let avail = fixed(avail_dec, scale)?;
        if venue_available
            .insert(policy.wallet_id, avail)
            .is_some_and(|prev| prev != avail)
        {
            return Err(Error::Conflict);
        }
    }
    template.venue_available = venue_available.into_iter().collect();

    let mut positions = Vec::new();
    let mut seen_pos = BTreeSet::new();
    for row in remote.positions {
        if row.symbol.is_empty() || !seen_pos.insert((row.symbol, row.position_side)) {
            return Err(Error::Encoding);
        }
        if row.isolated_margin.is_some_and(|d| d.mantissa() != 0) {
            return Err(Error::Unsupported);
        }
        if row.position_amt.mantissa() == 0 {
            continue;
        }
        let policy = catalog
            .entries
            .iter()
            .find(|p| p.instrument.symbol.as_str() == row.symbol)
            .ok_or(Error::Conflict)?;
        let fp = policy.futures.as_ref().ok_or(Error::Identity)?;
        let side = match row.position_side {
            "BOTH" => PositionSide::Both,
            "LONG" => PositionSide::Long,
            "SHORT" => PositionSide::Short,
            _ => return Err(Error::Encoding),
        };
        if (fp.position_mode == v::PositionMode::OneWay) != (side == PositionSide::Both) {
            return Err(Error::Conflict);
        }
        let base_scale = asset_scale(&policy.instrument.base)?;
        let raw_qty = fixed(row.position_amt, base_scale)?;
        let quantity = if side == PositionSide::Short {
            if raw_qty.atoms() > 0 {
                return Err(Error::Conflict);
            }
            raw_qty.checked_neg().map_err(|_| Error::Capacity)?
        } else {
            if side == PositionSide::Long && raw_qty.atoms() < 0 {
                return Err(Error::Conflict);
            }
            raw_qty
        };
        let entry_scale = fp.stress_high.scale();
        let entry_bound = match row.entry_price {
            Some(ep) if ep.mantissa() > 0 => fixed(ep, entry_scale)?,
            _ => fp.stress_high,
        };
        positions.push(PositionSnapshot {
            instrument_id: policy.instrument.instrument_id,
            side,
            quantity,
            entry_bound,
            ownership: Vec::new(),
        });
    }
    template.positions = positions;
    Ok(())
}

fn build_order_spec(
    market: Market,
    policy: &tickwire_account::InstrumentPolicy,
    row: &OpenOrderRow<'_>,
    client_id: v::ClientId,
) -> Result<v::OrderSpec> {
    let side = match row.side {
        "BUY" => v::Side::Buy,
        "SELL" => v::Side::Sell,
        _ => return Err(Error::Encoding),
    };
    let parse_tif = |tif: &str| -> Result<v::TimeInForce> {
        match tif {
            "GTC" => Ok(v::TimeInForce::Gtc),
            "IOC" => Ok(v::TimeInForce::Ioc),
            "FOK" => Ok(v::TimeInForce::Fok),
            "GTX" if market == Market::Usdm => Ok(v::TimeInForce::PostOnly),
            _ => Err(Error::Unsupported),
        }
    };
    let (order_type, time_in_force) = match (market, row.kind) {
        (Market::Spot, "LIMIT_MAKER") => (v::OrderType::Limit, Some(v::TimeInForce::PostOnly)),
        (_, "LIMIT") => (v::OrderType::Limit, Some(parse_tif(row.time_in_force)?)),
        (_, "STOP_LOSS_LIMIT" | "STOP") => {
            (v::OrderType::StopLimit, Some(parse_tif(row.time_in_force)?))
        }
        (_, "TAKE_PROFIT_LIMIT" | "TAKE_PROFIT") => (
            v::OrderType::TakeProfitLimit,
            Some(parse_tif(row.time_in_force)?),
        ),
        _ => return Err(Error::Unsupported),
    };
    let trigger = if matches!(
        order_type,
        v::OrderType::StopLimit | v::OrderType::TakeProfitLimit
    ) {
        let sp = row.stop_price.ok_or(Error::Encoding)?;
        Some(v::Trigger {
            price: to_venue_decimal(sp)?,
            working_price: v::WorkingPrice::Contract,
            price_protect: false,
        })
    } else {
        None
    };
    let position_side = match market {
        Market::Spot => PositionSide::Both,
        Market::Usdm => match row.position_side.unwrap_or("BOTH") {
            "BOTH" => PositionSide::Both,
            "LONG" => PositionSide::Long,
            "SHORT" => PositionSide::Short,
            _ => return Err(Error::Encoding),
        },
    };
    let mode = match &policy.futures {
        None => v::ProductMode::Spot,
        Some(fp) => v::ProductMode::Derivative {
            position_mode: fp.position_mode,
            margin_mode: fp.margin_mode,
        },
    };
    Ok(v::OrderSpec {
        client_id,
        side,
        order_type,
        quantity: v::Quantity::Base(to_venue_decimal(row.orig_qty)?),
        price: Some(to_venue_decimal(row.price)?),
        time_in_force,
        position_side,
        mode,
        reduce_only: row.reduce_only.unwrap_or(false),
        self_trade_prevention: v::SelfTradePrevention::Default,
        trigger,
        trailing: None,
        iceberg_quantity: None,
    })
}

fn build_synthetic_command(
    template: &Snapshot,
    policy: &tickwire_account::InstrumentPolicy,
    sd: SelfDescribingClientId,
    manager: ManagerIdentity,
    order_id: [u8; 16],
    spec: v::OrderSpec,
) -> Result<v::CanonicalCommand> {
    let cmd = v::Command {
        context: v::CommandContext {
            request: v::RequestIdentity {
                risk_domain_id: template.scope.risk_domain_id,
                venue: template.scope.venue,
                environment: template.scope.environment,
                account: template.scope.account,
                product: policy.product,
                intent_id: sd.intent_id,
                intent_exec_epoch: u64::from(sd.epoch),
                strategy_generation: manager.generation.max(1),
                config_revision: 1,
                deadline: u64::MAX,
                recovery_object: 1,
            },
            strategy: v::StrategyContext {
                logical_strategy_id: u64::from(sd.logical_strategy_id),
                instance_id: manager.instance,
                strategy_intent_seq: sd.intent_id,
                authorization_epoch: 1,
                risk_policy_version: template.revisions.risk.max(1),
                order_config_version: 1,
                created_mono_ns: 1,
                economic_intent_id: order_id,
                hedge_group_id: [0; 16],
                leg_id: u32::from(sd.leg),
                recovery_manifest_hash: [0xFF; 32],
            },
            instrument: policy.instrument.clone(),
            triggers: v::BoundedList::new(vec![v::TriggerRef {
                topic_id: 1,
                feed_generation: 1,
                ring_epoch: 1,
                sequence: 1,
                instrument_id: policy.instrument.instrument_id,
                instrument_version: policy.instrument.instrument_version,
                observed_mono_ns: 1,
            }])
            .map_err(|_| Error::Identity)?,
        },
        body: v::CommandBody::New(spec),
    };
    v::CanonicalCommand::new(cmd).map_err(|_| Error::Identity)
}

fn asset_scale(a: &v::AssetRef) -> Result<u8> {
    u8::try_from(-i16::from(a.exponent)).map_err(|_| Error::Encoding)
}

fn fixed(value: Decimal, scale: u8) -> Result<Fixed> {
    let rescaled = value.rescale_exact(scale).map_err(|_| Error::Encoding)?;
    Fixed::from_atoms(rescaled.mantissa(), scale).map_err(|_| Error::Encoding)
}

fn to_venue_decimal(value: Decimal) -> Result<v::Decimal> {
    let scale = i8::try_from(value.scale()).map_err(|_| Error::Encoding)?;
    v::Decimal::new(value.mantissa(), -scale).map_err(|_| Error::Encoding)
}
