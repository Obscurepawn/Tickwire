use super::*;
use tickwire_risk::reservation;

/// Cold stateless bootstrap result pairing the `CanonicalBaseline`-ready normalized snapshot
/// with the reconstructed in-memory `Account`.
#[derive(Debug, Clone)]
pub struct LiveBootstrap {
    pub normalized_snapshot: Snapshot,
    pub account: Account,
    pub imported_orders: Vec<OrderKey>,
    pub external_open_orders: usize,
}

struct PreparedImport {
    instrument_id: u64,
    strategy: u64,
    intent: IntentKey,
    spec: v::OrderSpec,
    venue_order_id: Option<v::VenueOrderId>,
    internal_id: [u8; 16],
    original: Fixed,
    filled: Fixed,
    remaining: Fixed,
    price: Fixed,
    manager: ManagerIdentity,
    command_digest: v::Hash,
}

impl Account {
    /// Highest imported `(epoch, intent_id)` floor for `strategy`, if any live order was imported.
    pub fn strategy_sequence_floor(&self, strategy: u64) -> Option<IntentKey> {
        self.strategy_sequence_floors.get(&strategy).copied()
    }

    /// Highest `intent_exec_epoch` observed across all cold-imported open orders.
    pub fn max_imported_epoch(&self) -> Option<u64> {
        self.strategy_sequence_floors
            .values()
            .map(|k| k.epoch)
            .max()
    }

    /// Reconstructs an `Account` from an authenticated exchange balance/position snapshot and
    /// live `openOrders` without requiring any prior local WAL.
    ///
    /// Managed orders carrying a valid `SelfDescribingClientId` for `(scope, product)` are
    /// attributed back to `(logical_strategy_id, epoch, intent_id)` with exact reservations and
    /// deterministic internal UUIDs; unrecognized client IDs are counted in `external_open_orders`
    /// and have their locked Spot balances folded into `nontrade_locked`.
    pub fn from_live_exchange(
        snapshot: Snapshot,
        catalog: Catalog,
        open_orders: &[LiveOpenOrder],
        freeze_on_external: bool,
    ) -> Result<LiveBootstrap> {
        if open_orders.len() > MAX_ORDERS {
            return Err(Error::Capacity);
        }
        if catalog.entries.is_empty() || catalog.entries.len() > MAX_INSTRUMENTS {
            return Err(Error::InvalidSnapshot);
        }

        let mut catalog_by_id = BTreeMap::new();
        for p in &catalog.entries {
            if catalog_by_id
                .insert(p.instrument.instrument_id, p)
                .is_some()
            {
                return Err(Error::Catalog);
            }
        }

        let mut explained_locked: BTreeMap<AssetKey, Fixed> = BTreeMap::new();
        let mut prepared = Vec::with_capacity(open_orders.len());
        let mut external_open_orders = 0usize;

        for live in open_orders {
            let p = *catalog_by_id
                .get(&live.instrument_id)
                .ok_or(Error::Catalog)?;
            let domain = v::ClientIdDomain {
                risk_domain_id: snapshot.scope.risk_domain_id,
                venue: snapshot.scope.venue,
                environment: snapshot.scope.environment,
                account: snapshot.scope.account,
                product: p.product,
            };
            let decoded = match v::SelfDescribingClientId::decode(&live.spec.client_id, &domain) {
                v::DecodedClientId::Managed(d) => d,
                v::DecodedClientId::External => {
                    external_open_orders += 1;
                    continue;
                }
            };
            let strategy = u64::from(decoded.logical_strategy_id);
            if !p.strategy_limits.iter().any(|x| x.0 == strategy) {
                return Err(Error::Identity);
            }

            let original = match live.spec.quantity {
                v::Quantity::Base(q) => exact_decimal(q, asset_scale(&p.instrument.base)?)?,
                _ => return Err(Error::UnboundedExecution),
            };
            original.positive()?;
            live.filled_quantity.nonnegative()?;
            if live.filled_quantity.scale() != original.scale()
                || live.filled_quantity.atoms() > original.atoms()
            {
                return Err(Error::InvalidSnapshot);
            }
            if !matches!(
                live.spec.order_type,
                v::OrderType::Limit | v::OrderType::StopLimit | v::OrderType::TakeProfitLimit
            ) {
                return Err(Error::UnboundedExecution);
            }
            let price_scale = p
                .futures
                .as_ref()
                .map(|x| x.stress_high.scale())
                .unwrap_or(asset_scale(&p.instrument.quote)?);
            let price = exact_decimal(
                live.spec.price.ok_or(Error::UnboundedExecution)?,
                price_scale,
            )?;
            price.positive()?;
            let remaining = original.checked_sub(live.filled_quantity)?;

            if p.futures.is_none() {
                if live.spec.mode != v::ProductMode::Spot {
                    return Err(Error::Unsupported);
                }
                let principal = if live.spec.side == v::Side::Buy {
                    AssetAmount {
                        key: AssetKey {
                            wallet_id: p.wallet_id,
                            asset_id: p.instrument.quote.id,
                        },
                        amount: remaining.mul_to(
                            price,
                            asset_scale(&p.instrument.quote)?,
                            Rounding::Ceil,
                        )?,
                    }
                } else {
                    AssetAmount {
                        key: AssetKey {
                            wallet_id: p.wallet_id,
                            asset_id: p.instrument.base.id,
                        },
                        amount: remaining,
                    }
                };
                add(&mut explained_locked, principal.key, principal.amount)?;
            }

            prepared.push(PreparedImport {
                instrument_id: live.instrument_id,
                strategy,
                intent: IntentKey {
                    epoch: u64::from(decoded.epoch),
                    intent_id: decoded.intent_id,
                },
                spec: live.spec.clone(),
                venue_order_id: live.venue_order_id.clone(),
                internal_id: decoded.deterministic_order_id(&domain),
                original,
                filled: live.filled_quantity,
                remaining,
                price,
                manager: live.manager,
                command_digest: live.command_digest,
            });
        }

        let mut normalized_snapshot = snapshot;
        for key in explained_locked.keys() {
            if !normalized_snapshot.assets.iter().any(|a| a.key == *key) {
                return Err(Error::Catalog);
            }
        }
        for a in &mut normalized_snapshot.assets {
            let scale = asset_scale(&a.asset)?;
            for x in [a.free, a.locked, a.nontrade_locked] {
                x.nonnegative()?;
                if x.scale() != scale {
                    return Err(Error::InvalidSnapshot);
                }
            }
            if a.key.asset_id != a.asset.id {
                return Err(Error::InvalidSnapshot);
            }
            let explained = explained_locked
                .get(&a.key)
                .copied()
                .unwrap_or(Fixed::zero(scale)?);
            let min_locked = a.nontrade_locked.checked_add(explained)?;
            if a.locked.atoms() < min_locked.atoms() {
                return Err(Error::Unreconciled);
            }
            let unexplained_locked = a.locked.checked_sub(min_locked)?;
            let effective_nontrade = a.nontrade_locked.checked_add(unexplained_locked)?;
            a.free = a.free.checked_add(explained)?;
            a.locked = effective_nontrade;
            a.nontrade_locked = effective_nontrade;
        }

        let mut account = Self::from_snapshot(normalized_snapshot.clone(), catalog)?;
        let mut imported_orders = Vec::with_capacity(prepared.len());
        let mut must_freeze = false;

        for item in prepared {
            let p = account
                .catalog
                .get(&item.instrument_id)
                .ok_or(Error::Catalog)?
                .clone();
            let order_key = OrderKey {
                intent: item.intent,
                client_id: item.spec.client_id.clone(),
            };
            if account.orders.contains_key(&order_key) {
                return Err(Error::Conflict);
            }
            if account.orders.values().any(|o| {
                o.instrument == item.instrument_id && o.key.client_id == item.spec.client_id
            }) {
                return Err(Error::Conflict);
            }
            if let Some(ref native) = item.venue_order_id
                && account.orders.values().any(|o| {
                    o.instrument == item.instrument_id && o.native.as_ref() == Some(native)
                })
            {
                return Err(Error::Conflict);
            }

            let mut reduce = item.original.zero_like();
            if let Some(fp) = &p.futures {
                if item.spec.mode
                    != (v::ProductMode::Derivative {
                        position_mode: fp.position_mode,
                        margin_mode: fp.margin_mode,
                    })
                {
                    return Err(Error::Catalog);
                }
                let reducing = item.spec.reduce_only
                    || matches!(
                        (item.spec.position_side, item.spec.side),
                        (v::PositionSide::Long, v::Side::Sell)
                            | (v::PositionSide::Short, v::Side::Buy)
                    );
                if reducing && item.remaining.atoms() > 0 {
                    let actual = account.position_quantity(&p, item.spec.position_side)?;
                    let existing: Vec<_> = account
                        .orders
                        .values()
                        .filter(|o| {
                            o.instrument == item.instrument_id
                                && o.spec.position_side == item.spec.position_side
                        })
                        .map(|o| o.reduce)
                        .collect();
                    let side = match item.spec.position_side {
                        v::PositionSide::Both => reservation::PositionSide::Both,
                        v::PositionSide::Long => reservation::PositionSide::Long,
                        v::PositionSide::Short => reservation::PositionSide::Short,
                    };
                    let step = exact_decimal(p.instrument.quantity_step, item.remaining.scale())?;
                    match reservation::allocate_reduce(
                        actual,
                        &existing,
                        item.remaining,
                        step,
                        false,
                        side,
                    ) {
                        Ok(allocation) => {
                            let buy = allocation.side == tickwire_risk::futures::Side::Buy;
                            if buy == (item.spec.side == v::Side::Buy)
                                && allocation.reduce_only == item.spec.reduce_only
                            {
                                reduce = allocation.allocated;
                            } else {
                                must_freeze = true;
                            }
                        }
                        Err(_) => {
                            must_freeze = true;
                        }
                    }
                }
            } else {
                let spending_asset = if item.spec.side == v::Side::Buy {
                    p.instrument.quote.id
                } else {
                    p.instrument.base.id
                };
                if account.assets[&AssetKey {
                    wallet_id: p.wallet_id,
                    asset_id: spending_asset,
                }]
                    .baseline
                    .exclusive_fee_pool
                {
                    must_freeze = true;
                }
            }

            let fee_asset = if p.futures.is_some() {
                &p.instrument.settlement
            } else if item.spec.side == v::Side::Buy {
                &p.instrument.base
            } else {
                &p.instrument.quote
            };
            let fee_zero = Fixed::zero(asset_scale(fee_asset)?)?;
            let settlement_zero = Fixed::zero(asset_scale(&p.instrument.settlement)?)?;

            account.orders.insert(
                order_key.clone(),
                Order {
                    key: order_key.clone(),
                    digest: item.command_digest,
                    instrument: item.instrument_id,
                    strategy: item.strategy,
                    origin: item.manager,
                    manager: item.manager,
                    spec: item.spec,
                    amendable: true,
                    original: item.original,
                    effective: item.original,
                    observed_cumulative: item.filled,
                    filled: item.filled,
                    price: item.price,
                    received: fee_zero,
                    notional: settlement_zero,
                    paid: BTreeMap::new(),
                    terminal_cumulative: None,
                    observed_unknown: false,
                    recovery_unknown: false,
                    observation_unreconciled: false,
                    terminal: false,
                    fees_closed: false,
                    aborted: false,
                    uncreated: false,
                    economic: item.filled.atoms() > 0,
                    confirmed: true,
                    reduce,
                    native: item.venue_order_id,
                    internal_id: Some(item.internal_id),
                    owner_revision: 1,
                    shadow_fee_credit: fee_zero,
                },
            );

            account
                .strategy_sequence_floors
                .entry(item.strategy)
                .and_modify(|floor| {
                    if item.intent > *floor {
                        *floor = item.intent;
                    }
                })
                .or_insert(item.intent);
            imported_orders.push(order_key);
        }

        if must_freeze
            || (freeze_on_external && external_open_orders > 0)
            || account.evaluate().is_err()
        {
            account.frozen = true;
        }

        Ok(LiveBootstrap {
            normalized_snapshot,
            account,
            imported_orders,
            external_open_orders,
        })
    }
}
