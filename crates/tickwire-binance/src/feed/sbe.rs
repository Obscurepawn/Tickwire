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
            max_depth_levels: N,
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
    // Decode directly into the final typed arrays. A second normalized depth
    // object plus the caller's fixed-capacity result exceeds worker stacks.
    let mut update = DepthUpdate(DecodedDepth {
        key: context.key,
        generation: context.generation,
        bids: Levels {
            values: [EMPTY_LEVEL; N],
            len: bids.len(),
        },
        asks: Levels {
            values: [EMPTY_LEVEL; N],
            len: asks.len(),
        },
        first: frame.first_update_id().ok_or(Error::InvalidResponse)?,
        last: frame.last_update_id().ok_or(Error::InvalidResponse)?,
        previous: None,
        received,
        event_time_ns: Some(frame.event_time_ns()),
        transaction_time_ns: None,
    });
    for (raw, levels) in [(bids, &mut update.0.bids), (asks, &mut update.0.asks)] {
        for (from, to) in raw.iter().zip(&mut levels.values) {
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
            *to = Level {
                price: PriceTicks(price),
                quantity: QtyLots(quantity),
            };
        }
    }
    Ok(update)
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
