use super::*;
use tickwire_binance_sbe as sbe;

/// Decode a Spot SBE diff against the authenticated catalog and connection generation.
pub fn decode_sbe_diff<const N: usize>(
    bytes: &[u8],
    context: &DepthContext,
    received: u64,
    max_bytes: usize,
) -> Result<DepthUpdate<N>> {
    check_input::<N>(bytes, max_bytes)?;
    let mut update = DepthUpdate::empty(context, received);
    let fields = decode_with(bytes, context, max_bytes, N, |side, level| match side {
        Side::Bid => update.0.bids.push(level),
        Side::Ask => update.0.asks.push(level),
    })?;
    update.0.set_fields(fields);
    Ok(update)
}
pub(super) fn decode_with(
    bytes: &[u8],
    context: &DepthContext,
    max_bytes: usize,
    maximum: usize,
    mut emit: impl FnMut(Side, Level) -> Result<()>,
) -> Result<DepthFields> {
    if context.market != Market::Spot || context.time_unit != TimeUnit::Microseconds {
        return Err(Error::IdentityMismatch);
    }
    let unit = |d: tickwire_venue::Decimal| {
        sbe::DecimalUnit::new(d.coefficient(), d.exponent()).map_err(error)
    };
    let frame = sbe::decode_frame(
        bytes,
        sbe::Limits {
            max_frame_bytes: max_bytes,
            max_depth_levels: maximum,
            max_trades: 1,
        },
    )
    .map_err(error)?;
    if context.instrument.instrument_id == 0
        || context.generation == 0
        || frame.symbol() != context.instrument.symbol.as_str()
    {
        return Err(Error::IdentityMismatch);
    }
    if frame.kind() != sbe::Kind::DepthDiff {
        return Err(Error::InvalidResponse);
    }
    let bids = frame.bids().ok_or(Error::InvalidResponse)?;
    let asks = frame.asks().ok_or(Error::InvalidResponse)?;
    let price_tick = unit(context.instrument.price_tick)?;
    let quantity_step = unit(context.instrument.quantity_step)?;
    for (side, raw) in [(Side::Bid, bids), (Side::Ask, asks)] {
        for from in raw.iter() {
            let price =
                sbe::decimal_to_units(from.price_mantissa, frame.price_exponent(), price_tick)
                    .map_err(error)?;
            let quantity = sbe::decimal_to_units(
                from.quantity_mantissa,
                frame.quantity_exponent(),
                quantity_step,
            )
            .map_err(error)?;
            if price <= 0 || quantity < 0 {
                return Err(Error::InvalidResponse);
            }
            emit(
                side,
                Level {
                    price: PriceTicks(price),
                    quantity: QtyLots(quantity),
                },
            )?;
        }
    }
    Ok(DepthFields {
        first: frame.first_update_id().ok_or(Error::InvalidResponse)?,
        last: frame.last_update_id().ok_or(Error::InvalidResponse)?,
        previous: None,
        event_time_ns: Some(frame.event_time_ns()),
        transaction_time_ns: None,
    })
}
fn error(e: sbe::Error) -> Error {
    match e {
        sbe::Error::Overflow => Error::Overflow,
        sbe::Error::Capacity => Error::Capacity,
        sbe::Error::Identity => Error::IdentityMismatch,
        sbe::Error::NotIntegral => Error::InvalidDecimal,
        _ => Error::InvalidResponse,
    }
}
