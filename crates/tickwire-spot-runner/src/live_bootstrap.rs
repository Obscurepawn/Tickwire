//! Cold stateless exchange bootstrap for Bybit and OKX cash-spot accounts from `RestEvidence`.

use crate::{Error, Result};
use std::collections::{BTreeMap, BTreeSet};
use tickwire_account::{
    Catalog, Fixed, InstrumentPolicy, IntentKey, LiveOpenOrder, ManagerIdentity, Snapshot,
};
use tickwire_account_gateway::VerifiedLiveBootstrap;
use tickwire_spot_execution::{SpotVenue, recovery::RestEvidence};
use tickwire_venue as v;
use tickwire_venue::{ClientIdDomain, DecodedClientId, SelfDescribingClientId};

#[derive(Clone, Debug)]
pub struct ImportedSpotOrigin {
    pub intent: IntentKey,
    pub client_id: String,
    pub order_id: [u8; 16],
    pub command: v::CanonicalCommand,
}

#[derive(Clone, Debug)]
pub struct SpotLiveBootstrap {
    pub live: VerifiedLiveBootstrap,
    pub origins: Vec<ImportedSpotOrigin>,
}

/// Reconstructs a `VerifiedLiveBootstrap` and `AccountRunner` origin bindings directly from
/// an authenticated Bybit V5 or OKX V5 `RestEvidence` snapshot without prior local WAL files.
pub fn bootstrap_spot_from_rest_evidence(
    mut template: Snapshot,
    catalog: Catalog,
    strategy_managers: &BTreeMap<u64, ManagerIdentity>,
    evidence: &RestEvidence,
    expected_uid: &str,
    freeze_on_external: bool,
) -> Result<SpotLiveBootstrap> {
    if expected_uid.is_empty()
        || evidence.uid != expected_uid
        || SpotVenue::from_id(template.scope.venue) != Some(evidence.venue)
        || template.scope.environment != evidence.environment
    {
        return Err(Error::Origin("spot live bootstrap scope or UID mismatch"));
    }
    if catalog.entries.is_empty()
        || catalog
            .entries
            .iter()
            .any(|p| p.product != v::Product::Spot)
    {
        return Err(Error::Origin(
            "spot live bootstrap requires a cash-spot catalog",
        ));
    }

    let mut by_symbol = BTreeMap::new();
    for row in &evidence.balances {
        if by_symbol.insert(row.symbol.as_str(), row).is_some() {
            return Err(Error::Origin(
                "duplicate balance symbol in spot REST evidence",
            ));
        }
    }
    for asset in &mut template.assets {
        let scale = asset_scale(&asset.asset)?;
        let zero = Fixed::zero(scale).map_err(|_| Error::Origin("invalid zero fixed scale"))?;
        if let Some(row) = by_symbol.get(asset.asset.symbol.as_str()) {
            let free = fixed(row.available, scale)?;
            let locked = fixed(row.locked, scale)?;
            let total = fixed(row.total, scale)?;
            let sum = free
                .checked_add(locked)
                .map_err(|_| Error::Origin("spot balance overflow"))?;
            if sum != total {
                return Err(Error::Origin(
                    "spot balance total != available + locked in REST evidence",
                ));
            }
            asset.free = free;
            asset.locked = locked;
            asset.nontrade_locked = zero;
            asset.ownership.clear();
        } else {
            asset.free = zero;
            asset.locked = zero;
            asset.nontrade_locked = zero;
            asset.ownership.clear();
        }
    }
    template.positions.clear();
    template.venue_available.clear();

    let mut live_orders = Vec::with_capacity(evidence.open_orders.len());
    let mut origins = Vec::new();
    let mut seen_native = BTreeSet::new();

    for row in &evidence.open_orders {
        if row.native_id.is_empty()
            || !seen_native.insert((row.symbol.as_str(), row.native_id.as_str()))
            || !matches!(
                row.status,
                v::OrderStatus::New | v::OrderStatus::PartiallyFilled
            )
        {
            return Err(Error::Origin(
                "invalid open order row in spot REST evidence",
            ));
        }
        let policy = catalog
            .entries
            .iter()
            .find(|p| p.instrument.symbol.as_str() == row.symbol)
            .ok_or(Error::Origin("open order symbol missing from spot catalog"))?;

        let venue_order_id = v::VenueOrderId::new(row.native_id.clone())
            .map_err(|_| Error::Origin("invalid native order ID"))?;
        let base_scale = asset_scale(&policy.instrument.base)?;
        let filled_quantity = fixed(row.cumulative, base_scale)?;

        let domain = ClientIdDomain {
            risk_domain_id: template.scope.risk_domain_id,
            venue: template.scope.venue,
            environment: template.scope.environment,
            account: template.scope.account,
            product: v::Product::Spot,
        };

        match SelfDescribingClientId::decode_str(&row.client_id, &domain) {
            DecodedClientId::External => {
                let external_cid = v::ClientId::new(row.client_id.clone())
                    .or_else(|_| v::ClientId::new("external-unmanaged-order"))
                    .map_err(|_| Error::Origin("invalid external client ID"))?;
                let spec = build_spot_order_spec(row, external_cid)?;
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
                let cid = v::ClientId::new(row.client_id.clone())
                    .map_err(|_| Error::Origin("invalid managed client ID"))?;
                let spec = build_spot_order_spec(row, cid)?;
                let strategy_id = u64::from(sd.logical_strategy_id);
                let manager = *strategy_managers.get(&strategy_id).ok_or(Error::Origin(
                    "managed open order references unknown strategy",
                ))?;
                let order_id = sd.deterministic_order_id(&domain);
                let command = build_synthetic_spot_command(
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
                    venue_order_id: Some(venue_order_id),
                    filled_quantity,
                    manager,
                    command_digest: digest,
                });
                origins.push(ImportedSpotOrigin {
                    intent: IntentKey {
                        epoch: u64::from(sd.epoch),
                        intent_id: sd.intent_id,
                    },
                    client_id: row.client_id.clone(),
                    order_id,
                    command,
                });
            }
        }
    }

    let imported_commands = origins.iter().map(|o| o.command.clone()).collect();
    let live = VerifiedLiveBootstrap::new(template, catalog, &live_orders, freeze_on_external)
        .map_err(|_| Error::Origin("spot live bootstrap account reconstruction failed"))?
        .with_imported_commands(imported_commands);
    Ok(SpotLiveBootstrap { live, origins })
}

fn build_spot_order_spec(
    row: &tickwire_spot_execution::recovery::RemoteOrder,
    client_id: v::ClientId,
) -> Result<v::OrderSpec> {
    if row.order_type != v::OrderType::Limit {
        return Err(Error::Origin("unsupported open cash order type"));
    }
    let price = row
        .price
        .filter(|p| p.coefficient() > 0)
        .ok_or(Error::Origin("open limit order requires positive price"))?;
    if row.qty.coefficient() <= 0 {
        return Err(Error::Origin("open order requires positive quantity"));
    }
    Ok(v::OrderSpec {
        client_id,
        side: row.side,
        order_type: v::OrderType::Limit,
        quantity: v::Quantity::Base(row.qty),
        price: Some(price),
        time_in_force: Some(row.time_in_force.unwrap_or(v::TimeInForce::Gtc)),
        position_side: v::PositionSide::Both,
        mode: v::ProductMode::Spot,
        reduce_only: false,
        self_trade_prevention: v::SelfTradePrevention::Default,
        trigger: None,
        trailing: None,
        iceberg_quantity: None,
    })
}

fn build_synthetic_spot_command(
    template: &Snapshot,
    policy: &InstrumentPolicy,
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
                product: v::Product::Spot,
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
            .map_err(|_| Error::Origin("invalid trigger list"))?,
        },
        body: v::CommandBody::New(spec),
    };
    v::CanonicalCommand::new(cmd).map_err(|_| Error::Origin("invalid synthetic spot command"))
}

fn asset_scale(asset: &v::AssetRef) -> Result<u8> {
    if asset.exponent > 0 {
        return Err(Error::Origin("positive asset exponent unsupported"));
    }
    u8::try_from(-i16::from(asset.exponent))
        .ok()
        .filter(|&s| s <= 18)
        .ok_or(Error::Origin("asset scale out of bounds"))
}

fn fixed(value: v::Decimal, target_scale: u8) -> Result<Fixed> {
    if value.exponent() > 0 {
        let factor = 10i128
            .checked_pow(u32::from(value.exponent().unsigned_abs()) + u32::from(target_scale))
            .ok_or(Error::Origin("REST decimal positive exponent overflow"))?;
        let atoms = value
            .coefficient()
            .checked_mul(factor)
            .ok_or(Error::Origin("REST decimal atoms overflow"))?;
        return Fixed::from_atoms(atoms, target_scale)
            .map_err(|_| Error::Origin("REST decimal conversion failed"));
    }
    let val_scale = value.exponent().unsigned_abs();
    let f = Fixed::from_atoms(value.coefficient(), val_scale)
        .map_err(|_| Error::Origin("invalid REST decimal"))?;
    if val_scale <= target_scale {
        let factor = 10i128
            .checked_pow(u32::from(target_scale - val_scale))
            .ok_or(Error::Origin("REST decimal rescale factor overflow"))?;
        let atoms = f
            .atoms()
            .checked_mul(factor)
            .ok_or(Error::Origin("REST decimal rescale overflow"))?;
        Fixed::from_atoms(atoms, target_scale)
            .map_err(|_| Error::Origin("REST decimal rescale failed"))
    } else {
        let diff = u32::from(val_scale - target_scale);
        let factor = 10i128
            .checked_pow(diff)
            .ok_or(Error::Origin("REST decimal scale factor overflow"))?;
        if f.atoms() % factor != 0 {
            return Err(Error::Origin(
                "REST decimal exceeds configured asset precision",
            ));
        }
        Fixed::from_atoms(f.atoms() / factor, target_scale)
            .map_err(|_| Error::Origin("REST decimal precision conversion failed"))
    }
}
