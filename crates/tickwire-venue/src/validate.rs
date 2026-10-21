use crate::*;

fn nonzero<const N: usize>(v: &[u8; N]) -> bool {
    v.iter().any(|x| *x != 0)
}
fn positive(d: Decimal) -> Result<(), Error> {
    if d.is_positive() {
        Ok(())
    } else {
        Err(Error::Quantity)
    }
}
pub(crate) fn asset(a: &AssetRef) -> Result<(), Error> {
    if a.id == 0 || a.version == 0 || !(-38..=38).contains(&a.exponent) {
        return Err(Error::Instrument);
    }
    Ok(())
}
/// Normalized positive decimals are divisible iff the coefficient ratio and decimal shift divide.
fn multiple(value: Decimal, step: Decimal) -> bool {
    value.is_positive() && exact_units(value, step)
}
fn exact_units(value: Decimal, step: Decimal) -> bool {
    if !step.is_positive() || value.exponent() < step.exponent() {
        return false;
    }
    let m = step.coefficient() as u128;
    let mut remainder = value.coefficient().unsigned_abs() % m;
    for _ in 0..i16::from(value.exponent()) - i16::from(step.exponent()) {
        // 10*r mod m, without overflowing even at i128::MAX.
        let two = (remainder + remainder) % m;
        let four = (two + two) % m;
        let eight = (four + four) % m;
        remainder = (eight + two) % m;
    }
    remainder == 0
}
fn quantity(value: Decimal, instrument: &InstrumentBinding) -> Result<(), Error> {
    if multiple(value, instrument.quantity_step) {
        Ok(())
    } else {
        Err(Error::Quantity)
    }
}
fn price(value: Decimal, instrument: &InstrumentBinding) -> Result<(), Error> {
    if multiple(value, instrument.price_tick) {
        Ok(())
    } else {
        Err(Error::Price)
    }
}
pub(crate) fn instrument(i: &InstrumentBinding, p: Product) -> Result<(), Error> {
    if i.instrument_id == 0
        || i.instrument_version == 0
        || i.catalog_version == 0
        || !nonzero(&i.catalog_hash)
        || !i.price_tick.is_positive()
        || !i.quantity_step.is_positive()
    {
        return Err(Error::Instrument);
    }
    asset(&i.base)?;
    asset(&i.quote)?;
    asset(&i.settlement)?;
    if i.base.id == i.quote.id {
        return Err(Error::Instrument);
    }
    match (p, i.contract) {
        (Product::Spot, ContractKind::Cash)
        | (Product::LinearPerpetual | Product::InversePerpetual, ContractKind::Perpetual) => {}
        (
            Product::LinearFuture | Product::InverseFuture,
            ContractKind::Delivery { expiry_utc_ns },
        ) if expiry_utc_ns > 0 => {}
        (
            Product::Option,
            ContractKind::Option {
                expiry_utc_ns,
                strike,
                ..
            },
        ) if expiry_utc_ns > 0 && strike.is_positive() => {}
        _ => return Err(Error::ProductCombination),
    }
    Ok(())
}
fn order(o: &OrderSpec, c: &CommandContext) -> Result<(), Error> {
    order_with_peg(o, c, None)
}
fn order_with_peg(o: &OrderSpec, c: &CommandContext, peg: Option<Peg>) -> Result<(), Error> {
    let p = c.request.product;
    let i = &c.instrument;
    match (p, o.mode) {
        (Product::Spot, ProductMode::Spot | ProductMode::Margin { .. }) => {
            if o.position_side != PositionSide::Both || o.reduce_only {
                return Err(Error::ProductCombination);
            }
        }
        (
            Product::LinearPerpetual
            | Product::InversePerpetual
            | Product::LinearFuture
            | Product::InverseFuture,
            ProductMode::Derivative { position_mode, .. },
        ) => {
            if (position_mode == PositionMode::OneWay) != (o.position_side == PositionSide::Both) {
                return Err(Error::ProductCombination);
            }
        }
        (Product::Option, ProductMode::Option) => {}
        _ => return Err(Error::ProductCombination),
    }
    let limit = matches!(
        o.order_type,
        OrderType::Limit | OrderType::StopLimit | OrderType::TakeProfitLimit
    );
    let conditional = matches!(
        o.order_type,
        OrderType::StopLimit
            | OrderType::StopMarket
            | OrderType::TakeProfitLimit
            | OrderType::TakeProfitMarket
    );
    if (o.price.is_some() && !limit)
        || (limit && o.price.is_none() && peg.is_none())
        || o.time_in_force.is_some() != limit
    {
        return Err(Error::OrderCombination);
    }
    if let Some(peg) = peg {
        if p != Product::Spot || o.mode != ProductMode::Spot {
            return Err(Error::ProductCombination);
        }
        if !limit || peg.offset_levels.is_some_and(|n| n > 100) {
            return Err(Error::OrderCombination);
        }
        match (o.order_type, o.time_in_force, peg.price_type) {
            (OrderType::Limit, Some(TimeInForce::PostOnly), PegPriceType::Market) => {
                return Err(Error::OrderCombination);
            }
            (OrderType::Limit, Some(TimeInForce::Gtc | TimeInForce::PostOnly), _) => {}
            (OrderType::Limit, _, PegPriceType::Primary) => return Err(Error::OrderCombination),
            _ => {}
        }
    }
    if conditional {
        if o.trigger.is_none() && !(p == Product::Spot && o.trailing.is_some()) {
            return Err(Error::OrderCombination);
        }
        if p != Product::Spot && o.trailing.is_some() {
            return Err(Error::OrderCombination);
        }
    } else if o.order_type == OrderType::TrailingStop {
        if p == Product::Spot || o.trailing.is_none() || o.trigger.is_some() {
            return Err(Error::OrderCombination);
        }
    } else if o.trigger.is_some() || o.trailing.is_some() {
        return Err(Error::OrderCombination);
    }
    if let Some(v) = o.price {
        price(v, i)?
    }
    if let Some(TimeInForce::Gtd { expire_utc_ns: 0 }) = o.time_in_force {
        return Err(Error::Deadline);
    }
    match o.quantity {
        Quantity::Base(v) => quantity(v, i)?,
        Quantity::Quote(v) => {
            positive(v)?;
            if p != Product::Spot
                || o.order_type != OrderType::Market
                || o.iceberg_quantity.is_some()
            {
                return Err(Error::OrderCombination);
            }
            let unit = Decimal::new(1, i.quote.exponent)?;
            if !multiple(v, unit) {
                return Err(Error::Quantity);
            }
        }
        Quantity::ClosePosition => {
            if !matches!(
                p,
                Product::LinearPerpetual
                    | Product::InversePerpetual
                    | Product::LinearFuture
                    | Product::InverseFuture
            ) || !matches!(
                o.order_type,
                OrderType::StopMarket | OrderType::TakeProfitMarket
            ) || o.reduce_only
            {
                return Err(Error::OrderCombination);
            }
        }
    }
    if let Some(t) = o.trigger {
        price(t.price, i)?;
        if p == Product::Spot && (t.working_price != WorkingPrice::Contract || t.price_protect) {
            return Err(Error::ProductCombination);
        }
    }
    if let Some(t) = o.trailing {
        match (p, t.distance) {
            (Product::Spot, TrailingDistance::BasisPoints(v)) if v > 0 => {}
            (Product::Spot, _) | (_, TrailingDistance::BasisPoints(_)) => {
                return Err(Error::ProductCombination);
            }
            (_, TrailingDistance::Percent(v)) => positive(v)?,
        }
        if p == Product::Spot
            && (t.working_price != WorkingPrice::Contract || t.activation_price.is_some())
        {
            return Err(Error::ProductCombination);
        }
        if let Some(v) = t.activation_price {
            price(v, i)?
        }
    }
    if let Some(v) = o.iceberg_quantity {
        quantity(v, i)?;
        if !limit
            || !matches!(o.quantity, Quantity::Base(_))
            || !(o.time_in_force == Some(TimeInForce::Gtc)
                || (p == Product::Spot
                    && o.order_type == OrderType::Limit
                    && o.time_in_force == Some(TimeInForce::PostOnly)))
        {
            return Err(Error::OrderCombination);
        }
        if let Quantity::Base(total) = o.quantity
            && compare_positive(v, total) == std::cmp::Ordering::Greater
        {
            return Err(Error::Quantity);
        }
    }
    Ok(())
}
fn compare_positive(a: Decimal, b: Decimal) -> std::cmp::Ordering {
    let mut da = a.coefficient().to_string();
    let mut db = b.coefficient().to_string();
    let ea = da.len() as i16 + i16::from(a.exponent());
    let eb = db.len() as i16 + i16::from(b.exponent());
    if ea != eb {
        return ea.cmp(&eb);
    }
    let n = da.len().max(db.len());
    da.extend(std::iter::repeat_n('0', n - da.len()));
    db.extend(std::iter::repeat_n('0', n - db.len()));
    da.cmp(&db)
}
fn target(t: &OrderTarget, c: &CommandContext) -> Result<(), Error> {
    if !nonzero(&t.order_id)
        || t.expected_owner_revision == 0
        || t.original_intent_exec_epoch == 0
        || t.original_intent_exec_epoch > c.request.intent_exec_epoch
        || (t.client_id.is_none() && t.venue_order_id.is_none())
    {
        return Err(Error::Identity);
    }
    Ok(())
}
pub(crate) fn command(c: &Command) -> Result<(), Error> {
    let ctx = &c.context;
    let r = &ctx.request;
    let s = &ctx.strategy;
    if !nonzero(&r.risk_domain_id)
        || r.venue == 0
        || r.account == 0
        || r.intent_id == 0
        || r.intent_exec_epoch == 0
        || r.strategy_generation == 0
        || r.config_revision == 0
        || r.recovery_object == 0
        || s.logical_strategy_id == 0
        || !nonzero(&s.instance_id)
        || s.strategy_intent_seq == 0
        || s.authorization_epoch == 0
        || s.risk_policy_version == 0
        || s.order_config_version == 0
        || !nonzero(&s.economic_intent_id)
    {
        return Err(Error::Identity);
    }
    if s.created_mono_ns >= r.deadline {
        return Err(Error::Deadline);
    }
    instrument(&ctx.instrument, r.product)?;
    for (n, t) in ctx.triggers.as_slice().iter().enumerate() {
        if t.topic_id == 0
            || t.feed_generation == 0
            || t.ring_epoch == 0
            || t.sequence == 0
            || t.instrument_id == 0
            || t.instrument_version == 0
            || t.observed_mono_ns > s.created_mono_ns
        {
            return Err(Error::Identity);
        }
        if ctx.triggers.as_slice()[..n]
            .iter()
            .any(|prior| prior.topic_id == t.topic_id)
        {
            return Err(Error::Duplicate);
        }
    }
    if matches!(
        c.body,
        CommandBody::New(_)
            | CommandBody::CancelReplace { .. }
            | CommandBody::Modify(_)
            | CommandBody::SpotList(_)
            | CommandBody::SpotAdvanced(_)
            | CommandBody::Algo(_)
    ) && (ctx.triggers.as_slice().is_empty() || !nonzero(&s.recovery_manifest_hash))
    {
        return Err(Error::Identity);
    }
    match &c.body {
        CommandBody::SpotAdvanced(a) => {
            if r.product != Product::Spot {
                return Err(Error::ProductCombination);
            }
            match a {
                SpotAdvancedCommand::New(o) => {
                    if o.peg.is_none() {
                        return Err(Error::OrderCombination);
                    }
                    order_with_peg(&o.order, ctx, o.peg)?;
                }
                SpotAdvancedCommand::CancelReplace {
                    target: t,
                    replacement: o,
                    ..
                } => {
                    target(t, ctx)?;
                    if o.peg.is_none() {
                        return Err(Error::OrderCombination);
                    }
                    order_with_peg(&o.order, ctx, o.peg)?;
                }
                SpotAdvancedCommand::List(l) => {
                    let legs = l.legs.as_slice();
                    if !nonzero(&l.list_id)
                        || legs.len() != if l.kind == ListKind::Otoco { 3 } else { 2 }
                        || !legs.iter().any(|o| o.peg.is_some())
                    {
                        return Err(Error::OrderCombination);
                    }
                    for (n, o) in legs.iter().enumerate() {
                        if o.order.mode != ProductMode::Spot {
                            return Err(Error::ProductCombination);
                        }
                        order_with_peg(&o.order, ctx, o.peg)?;
                        if l.kind == ListKind::Oto
                            && n > 0
                            && matches!(o.order.quantity, Quantity::Quote(_))
                        {
                            return Err(Error::OrderCombination);
                        }
                        if o.order.client_id == l.client_id
                            || legs[..n]
                                .iter()
                                .any(|prior| prior.order.client_id == o.order.client_id)
                        {
                            return Err(Error::Duplicate);
                        }
                    }
                }
                SpotAdvancedCommand::SorNew(o) => {
                    order(o, ctx)?;
                    if o.mode != ProductMode::Spot {
                        return Err(Error::ProductCombination);
                    }
                    if !matches!(o.order_type, OrderType::Limit | OrderType::Market)
                        || !matches!(o.quantity, Quantity::Base(_))
                        || o.time_in_force == Some(TimeInForce::PostOnly)
                    {
                        return Err(Error::OrderCombination);
                    }
                }
            }
        }
        CommandBody::New(o) => order(o, ctx)?,
        CommandBody::Cancel(t) | CommandBody::Query(t) => target(t, ctx)?,
        CommandBody::CancelReplace {
            target: t,
            replacement,
            ..
        } => {
            target(t, ctx)?;
            order(replacement, ctx)?
        }
        CommandBody::AmendKeepPriority {
            target: t,
            new_quantity,
            ..
        } => {
            target(t, ctx)?;
            quantity(*new_quantity, &ctx.instrument)?
        }
        CommandBody::Modify(m) => {
            target(&m.target, ctx)?;
            quantity(m.quantity, &ctx.instrument)?;
            price(m.price, &ctx.instrument)?;
            if r.product == Product::Spot
                && (m.reduce_only || m.position_side != PositionSide::Both)
            {
                return Err(Error::ProductCombination);
            }
        }
        CommandBody::SpotList(l) => {
            if r.product != Product::Spot {
                return Err(Error::ProductCombination);
            }
            if !nonzero(&l.list_id)
                || l.legs.as_slice().len() != if l.kind == ListKind::Otoco { 3 } else { 2 }
            {
                return Err(Error::OrderCombination);
            }
            for (n, o) in l.legs.as_slice().iter().enumerate() {
                order(o, ctx)?;
                if l.kind == ListKind::Oto && n > 0 && matches!(o.quantity, Quantity::Quote(_)) {
                    return Err(Error::OrderCombination);
                }
                if o.client_id == l.client_id
                    || l.legs.as_slice()[..n]
                        .iter()
                        .any(|prior| prior.client_id == o.client_id)
                {
                    return Err(Error::Duplicate);
                }
            }
        }
        CommandBody::CancelList(t) => {
            if r.product != Product::Spot {
                return Err(Error::ProductCombination);
            }
            if !nonzero(&t.list_id)
                || t.expected_owner_revision == 0
                || t.original_intent_exec_epoch == 0
                || t.original_intent_exec_epoch > r.intent_exec_epoch
                || (t.client_id.is_none() && t.venue_list_id.is_none())
            {
                return Err(Error::Identity);
            }
        }
        CommandBody::Algo(a) => {
            if r.product == Product::Spot || !nonzero(&a.parent_order_id) {
                return Err(Error::ProductCombination);
            }
            order(&a.order, ctx)?;
            if !matches!(
                a.order.order_type,
                OrderType::StopLimit
                    | OrderType::StopMarket
                    | OrderType::TakeProfitLimit
                    | OrderType::TakeProfitMarket
                    | OrderType::TrailingStop
            ) {
                return Err(Error::OrderCombination);
            }
        }
        CommandBody::CancelAlgo(t) => {
            if r.product == Product::Spot {
                return Err(Error::ProductCombination);
            }
            if !nonzero(&t.parent_order_id)
                || t.expected_owner_revision == 0
                || t.original_intent_exec_epoch == 0
                || t.original_intent_exec_epoch > r.intent_exec_epoch
                || (t.client_id.is_none() && t.venue_algo_id.is_none())
            {
                return Err(Error::Identity);
            }
        }
    }
    Ok(())
}
fn event_order(o: &OrderIdentity) -> Result<(), Error> {
    if o.client_id.is_none() && o.venue_order_id.is_none() && o.venue_algo_id.is_none() {
        return Err(Error::Identity);
    }
    let known = nonzero(&o.order_id);
    if known != (o.original_intent_id > 0) || known != (o.original_intent_exec_epoch > 0) {
        return Err(Error::Identity);
    }
    if o.parent_order_id.is_some_and(|id| !nonzero(&id))
        || o.list_id.is_some_and(|id| !nonzero(&id))
    {
        return Err(Error::Identity);
    }
    Ok(())
}
fn observation(o: &OrderObservation) -> Result<(), Error> {
    event_order(&o.order)?;
    for v in [
        o.original_quantity,
        o.cumulative_quantity,
        o.cumulative_quote,
        o.leaves_quantity,
        o.order_price,
    ]
    .into_iter()
    .flatten()
    {
        if v.coefficient() < 0 {
            return Err(Error::Quantity);
        }
    }
    Ok(())
}
pub(crate) fn cashflow(delta: &AssetDelta) -> Result<(), Error> {
    asset(&delta.asset)?;
    if delta.amount.coefficient() != 0
        && !exact_units(delta.amount, Decimal::new(1, delta.asset.exponent)?)
    {
        return Err(Error::Quantity);
    }
    Ok(())
}
pub(crate) fn fees(v: &BoundedList<AssetDelta, 8>) -> Result<(), Error> {
    for (n, delta) in v.as_slice().iter().enumerate() {
        cashflow(delta)?;
        if v.as_slice()[..n]
            .iter()
            .any(|a| a.asset.id == delta.asset.id)
        {
            return Err(Error::Duplicate);
        }
    }
    Ok(())
}
pub(crate) fn event(e: &VenueEvent) -> Result<(), Error> {
    let i = &e.identity;
    if !nonzero(&i.risk_domain_id)
        || i.venue == 0
        || i.account == 0
        || i.instrument_id == 0
        || i.instrument_version == 0
        || i.source_connection_epoch == 0
        || i.source_sequence == Some(0)
    {
        return Err(Error::Identity);
    }
    match &e.body {
        VenueEventBody::AmendmentRejected(a) => {
            event_order(&a.order)?;
            if i.product != Product::Spot
                || a.request_intent_id == 0
                || a.request_intent_exec_epoch == 0
                || a.request_digest == [0; 32]
                || a.order.venue_order_id.is_none()
                || a.order.client_id.is_none()
                || a.order.parent_order_id.is_some()
                || a.order.list_id.is_some()
                || a.order.venue_algo_id.is_some()
                || a.order.venue_list_id.is_some()
                || a.order.algo_client_id.is_some()
                || a.order.list_client_id.is_some()
            {
                return Err(Error::Identity);
            }
        }
        VenueEventBody::Amendment(a) => {
            event_order(&a.order)?;
            positive(a.previous_quantity)?;
            positive(a.new_quantity)?;
            if compare_positive(a.new_quantity, a.previous_quantity) != std::cmp::Ordering::Less {
                return Err(Error::Quantity);
            }
            if i.product != Product::Spot
                || a.request_intent_id == 0
                || a.request_intent_exec_epoch == 0
                || a.request_digest == [0; 32]
                || a.order.venue_order_id.is_none()
                || a.order.client_id.is_none()
                || a.order.parent_order_id.is_some()
                || a.order.list_id.is_some()
                || a.order.venue_algo_id.is_some()
                || a.order.venue_list_id.is_some()
                || a.order.algo_client_id.is_some()
                || a.order.list_client_id.is_some()
            {
                return Err(Error::Identity);
            }
            if let Some(o) = &a.observation {
                observation(o)?;
                if o.order != a.order || o.original_quantity != Some(a.new_quantity) {
                    return Err(Error::Identity);
                }
            }
        }
        VenueEventBody::Structure(s) => structure(s, i.product)?,
        VenueEventBody::Order(o) => observation(o)?,
        VenueEventBody::Fill(f) => {
            observation(&f.observation)?;
            positive(f.last_quantity)?;
            positive(f.last_price)?;
            if f.last_quote.is_some_and(|v| v.coefficient() < 0) {
                return Err(Error::Quantity);
            }
            if let Some(pnl) = &f.realized_pnl {
                cashflow(pnl)?
            }
            fees(&f.fees)?
        }
        VenueEventBody::Fee { order, deltas, .. } => {
            event_order(order)?;
            if deltas.as_slice().is_empty() {
                return Err(Error::Bounds);
            }
            fees(deltas)?
        }
        VenueEventBody::Rejected { .. } => {}
    }
    Ok(())
}

fn structure(s: &StructureObservation, product: Product) -> Result<(), Error> {
    let p = &s.parent;
    if !nonzero(&p.structure_id) || p.original_intent_id == 0 || p.original_intent_exec_epoch == 0 {
        return Err(Error::Identity);
    }
    let algo = p.kind == StructureKind::Algo;
    if (algo && !matches!(product, Product::LinearPerpetual | Product::LinearFuture))
        || (!algo && product != Product::Spot)
    {
        return Err(Error::ProductCombination);
    }
    let maximum = match p.kind {
        StructureKind::Algo => 1,
        StructureKind::Otoco => 3,
        _ => 2,
    };
    let children = s.children.as_slice();
    if children.len() > maximum {
        return Err(Error::Bounds);
    }
    for (index, child) in children.iter().enumerate() {
        let o = &child.order;
        event_order(o)?;
        if !nonzero(&o.order_id)
            || o.original_intent_id != p.original_intent_id
            || o.original_intent_exec_epoch != p.original_intent_exec_epoch
            || o.client_id.is_none()
            || o.venue_order_id.is_none()
            || p.venue_parent_id.is_none()
        {
            return Err(Error::Identity);
        }
        if algo {
            if o.parent_order_id != Some(p.structure_id)
                || o.venue_algo_id != p.venue_parent_id
                || o.algo_client_id.as_ref() != Some(&p.client_id)
                || o.list_id.is_some()
                || o.list_client_id.is_some()
                || o.venue_list_id.is_some()
            {
                return Err(Error::Identity);
            }
        } else if o.list_id != Some(p.structure_id)
            || o.venue_list_id != p.venue_parent_id
            || o.list_client_id.as_ref() != Some(&p.client_id)
            || o.parent_order_id.is_some()
            || o.venue_algo_id.is_some()
            || o.algo_client_id.is_some()
        {
            return Err(Error::Identity);
        }
        if children[..index].iter().any(|prior| {
            prior.order.order_id == o.order_id
                || prior.order.client_id == o.client_id
                || prior.order.venue_order_id == o.venue_order_id
        }) {
            return Err(Error::Duplicate);
        }
    }
    Ok(())
}
