//! Independent, allocating serde oracle reproducing the previous diff decoder.
use serde::Deserialize;
use serde_json::value::RawValue;
use tickwire_binance::{decimal::Decimal, feed::DepthUpdateRef, request::Market};

#[derive(Debug, PartialEq, Eq)]
pub struct Facts {
    first: u64,
    last: u64,
    previous: Option<u64>,
    event_ns: Option<u64>,
    transaction_ns: Option<u64>,
    bids: Vec<(i64, i64)>,
    asks: Vec<(i64, i64)>,
}
impl Facts {
    pub fn borrowed(update: &DepthUpdateRef<'_>) -> Self {
        let e = update.event();
        Self {
            first: e.first_update_id,
            last: e.last_update_id,
            previous: e.previous_update_id,
            event_ns: update.event_time_ns(),
            transaction_ns: update.transaction_time_ns(),
            bids: e.bids.iter().map(|l| (l.price.0, l.quantity.0)).collect(),
            asks: e.asks.iter().map(|l| (l.price.0, l.quantity.0)).collect(),
        }
    }
}
pub fn decode(bytes: &[u8], market: Market) -> Result<Facts, ()> {
    #[derive(Deserialize)]
    struct Envelope<'a> {
        stream: Option<String>,
        #[serde(borrow)]
        data: Option<&'a RawValue>,
    }
    let envelope: Envelope<'_> = serde_json::from_slice(bytes).map_err(|_| ())?;
    let payload = match (envelope.stream, envelope.data) {
        (None, None) => bytes,
        (Some(stream), Some(data)) => {
            let suffix = stream.strip_prefix("btcusdt").ok_or(())?;
            if !matches!(
                (market, suffix),
                (_, "@depth" | "@depth@100ms")
                    | (Market::Spot, "@depth@1000ms")
                    | (Market::Usdm, "@depth@500ms")
            ) {
                return Err(());
            }
            data.get().as_bytes()
        }
        _ => return Err(()),
    };
    #[derive(Deserialize)]
    struct Wire<'a> {
        e: String,
        s: String,
        #[serde(rename = "E")]
        event: u64,
        #[serde(rename = "T")]
        transaction: Option<u64>,
        #[serde(rename = "U")]
        first: u64,
        #[serde(rename = "u")]
        last: u64,
        pu: Option<u64>,
        st: Option<u64>,
        #[serde(borrow)]
        b: &'a RawValue,
        #[serde(borrow)]
        a: &'a RawValue,
    }
    let wire: Wire<'_> = serde_json::from_slice(payload).map_err(|_| ())?;
    if wire.e != "depthUpdate"
        || wire.s != "BTCUSDT"
        || (market == Market::Spot && (wire.pu.is_some() || wire.st.is_some()))
        || (market == Market::Usdm && wire.st.is_some_and(|p| p != 1))
        || wire.first > wire.last
        || (market == Market::Usdm && (wire.pu.is_none() || wire.transaction.is_none()))
    {
        return Err(());
    }
    let levels = |raw: &RawValue| -> Result<Vec<(i64, i64)>, ()> {
        let pairs: Vec<[Decimal; 2]> = serde_json::from_str(raw.get()).map_err(|_| ())?;
        pairs
            .into_iter()
            .map(|[price, quantity]| {
                // This oracle's fixture grid is 0.01 price and 0.00001 quantity.
                if price.mantissa() <= 0 || quantity.mantissa() < 0 {
                    return Err(());
                }
                let p = price.rescale_exact(2).map_err(|_| ())?.mantissa();
                let q = quantity.rescale_exact(5).map_err(|_| ())?.mantissa();
                Ok((
                    i64::try_from(p).map_err(|_| ())?,
                    i64::try_from(q).map_err(|_| ())?,
                ))
            })
            .collect()
    };
    let bids = levels(wire.b)?;
    let asks = levels(wire.a)?;
    if bids.len() + asks.len() > 8 {
        return Err(());
    }
    let multiplier = if market == Market::Spot {
        1000
    } else {
        1_000_000
    };
    Ok(Facts {
        first: wire.first,
        last: wire.last,
        previous: wire.pu,
        event_ns: Some(wire.event.checked_mul(multiplier).ok_or(())?),
        transaction_ns: wire
            .transaction
            .map(|n| n.checked_mul(multiplier).ok_or(()))
            .transpose()?,
        bids,
        asks,
    })
}
