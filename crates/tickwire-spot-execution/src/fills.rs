//! Decode one authenticated Bybit execution or OKX orders-channel trade row.
use crate::{ReportError, SpotVenue, client_ok, hex, symbol_ok};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde_json::Value;
use tickwire_account::{AssetSnapshot, IntentKey, Scope};
use tickwire_venue as v;

/// Captured provenance, preserved byte-for-byte when replaying the same ingress record.
pub struct FillSource<'a> {
    pub scope: &'a Scope,
    pub intent: IntentKey,
    pub order_id: [u8; 16],
    pub input_stream: [u8; 16],
    pub input_sequence: u64,
    pub connection_epoch: u64,
    pub received_mono_ns: u64,
}
fn field<'a>(v: &'a Value, k: &str) -> Result<&'a str, ReportError> {
    v.get(k).and_then(Value::as_str).ok_or(ReportError::Invalid)
}
fn decimal(v: &Value, k: &str) -> Result<v::Decimal, ReportError> {
    field(v, k)?.parse().map_err(|_| ReportError::Invalid)
}
// VenueTradeId allows 32 ASCII bytes. Encode the full Bybit 128-bit UUID reversibly.
// Prefixes keep UUIDs disjoint from compact, non-UUID execution identifiers.
fn trade_id(venue: SpotVenue, raw: &str) -> Result<v::VenueTradeId, ReportError> {
    if venue == SpotVenue::Okx {
        if !raw.bytes().all(|v| v.is_ascii_digit()) {
            return Err(ReportError::Invalid);
        }
        return v::VenueTradeId::new(raw).map_err(|_| ReportError::Invalid);
    }
    let mut encoded = String::new();
    if raw.len() == 36 {
        let mut bytes = [0u8; 16];
        let mut n = 0usize;
        for (i, c) in raw.bytes().enumerate() {
            if matches!(i, 8 | 13 | 18 | 23) {
                if c != b'-' {
                    return Err(ReportError::Invalid);
                }
                continue;
            }
            let nibble = (c as char).to_digit(16).ok_or(ReportError::Invalid)? as u8;
            bytes[n / 2] = bytes[n / 2] << 4 | nibble;
            n += 1;
        }
        encoded.push('U');
        encoded.push_str(&URL_SAFE_NO_PAD.encode(bytes));
    } else {
        if !symbol_ok(raw) {
            return Err(ReportError::Invalid);
        }
        encoded.push('R');
        encoded.push_str(raw);
    }
    v::VenueTradeId::new(encoded).map_err(|_| ReportError::Invalid)
}
/// `row` is one data element from the authenticated execution stream, not a public trade.
pub(crate) fn normalize(
    cmd: &v::CanonicalCommand,
    source: &FillSource<'_>,
    assets: &[AssetSnapshot],
    row: &[u8],
) -> Result<v::CanonicalEvent, ReportError> {
    if row.len() > 32 * 1024 {
        return Err(ReportError::Bounds);
    }
    let r = &cmd.command().context.request;
    if r.risk_domain_id != source.scope.risk_domain_id
        || r.venue != source.scope.venue
        || r.account != source.scope.account
        || r.environment != source.scope.environment
        || r.product != v::Product::Spot
        || source.intent.epoch != r.intent_exec_epoch
        || source.intent.intent_id != r.intent_id
        || source.input_stream == [0; 16]
        || source.input_sequence == 0
        || source.connection_epoch == 0
        || source.received_mono_ns == 0
    {
        return Err(ReportError::Identity);
    }
    let venue = SpotVenue::from_id(r.venue).ok_or(ReportError::Identity)?;
    let c = cmd.command();
    let v::CommandBody::New(order) = &c.body else {
        return Err(ReportError::Identity);
    };
    let item: Value = serde_json::from_slice(row).map_err(|_| ReportError::Invalid)?;
    let bybit = venue == SpotVenue::Bybit;
    let k = |a, b| if bybit { a } else { b };
    if field(&item, k("category", "instType"))? != k("spot", "SPOT")
        || field(&item, k("symbol", "instId"))? != c.context.instrument.symbol.as_str()
        || field(&item, k("orderLinkId", "clOrdId"))? != order.client_id.as_str()
        || !client_ok(venue, order.client_id.as_str())
    {
        return Err(ReportError::Identity);
    }
    if bybit
        && (field(&item, "execType")? != "Trade"
            || item.get("extraFees").is_some_and(|v| {
                !(v.as_str() == Some("") || v.as_array().is_some_and(Vec::is_empty))
            }))
    {
        return Err(ReportError::Invalid);
    }
    let received_side = field(&item, "side")?;
    let expected_side = if order.side == v::Side::Buy {
        k("Buy", "buy")
    } else {
        k("Sell", "sell")
    };
    if received_side != expected_side {
        return Err(ReportError::Identity);
    }
    let raw_order_id = field(&item, k("orderId", "ordId"))?;
    if !symbol_ok(raw_order_id) {
        return Err(ReportError::Invalid);
    }
    let native = v::VenueOrderId::new(raw_order_id).map_err(|_| ReportError::Invalid)?;
    let trade = trade_id(venue, field(&item, k("execId", "tradeId"))?)?;
    let price = decimal(&item, k("execPrice", "fillPx"))?;
    let qty = decimal(&item, k("execQty", "fillSz"))?;
    let fee = decimal(&item, k("execFee", "fillFee"))?;
    let fee = if bybit {
        v::Decimal::new(
            fee.coefficient()
                .checked_neg()
                .ok_or(ReportError::Invalid)?,
            fee.exponent(),
        )
        .map_err(|_| ReportError::Invalid)?
    } else {
        fee
    };
    let mut fees = vec![];
    if fee.coefficient() != 0 {
        let ccy = field(&item, k("feeCurrency", "fillFeeCcy"))?;
        let a = assets
            .iter()
            .find(|a| a.asset.symbol.as_str() == ccy)
            .ok_or(ReportError::Identity)?;
        if assets
            .iter()
            .any(|other| other.asset.symbol.as_str() == ccy && other.asset != a.asset)
        {
            return Err(ReportError::Identity);
        }
        fees.push(v::AssetDelta {
            asset: a.asset.clone(),
            amount: fee,
        });
    }
    let at = field(&item, k("execTime", "fillTime"))?
        .parse::<u64>()
        .ok()
        .and_then(|n| n.checked_mul(1_000_000))
        .filter(|n| *n != 0)
        .ok_or(ReportError::Invalid)?;
    let liquidity = if bybit {
        if item["isMaker"].as_bool().ok_or(ReportError::Invalid)? {
            v::Liquidity::Maker
        } else {
            v::Liquidity::Taker
        }
    } else {
        match field(&item, "execType")? {
            "M" => v::Liquidity::Maker,
            "T" => v::Liquidity::Taker,
            _ => v::Liquidity::Unknown,
        }
    };
    let quote = if bybit {
        decimal(&item, "execValue")?
    } else {
        // The OKX orders channel reports the last base size and its execution
        // price. Only an exact cash notional at the quote asset scale is usable.
        let quote = v::Decimal::new(
            qty.coefficient()
                .checked_mul(price.coefficient())
                .ok_or(ReportError::Bounds)?,
            qty.exponent()
                .checked_add(price.exponent())
                .ok_or(ReportError::Bounds)?,
        )
        .map_err(|_| ReportError::Bounds)?;
        if quote.exponent() < c.context.instrument.quote.exponent {
            return Err(ReportError::Invalid);
        }
        quote
    };
    let observation = v::OrderObservation {
        order: v::OrderIdentity {
            order_id: source.order_id,
            original_intent_id: r.intent_id,
            original_intent_exec_epoch: r.intent_exec_epoch,
            client_id: Some(order.client_id.clone()),
            venue_order_id: Some(native),
            parent_order_id: None,
            list_id: None,
            venue_algo_id: None,
            venue_list_id: None,
            list_client_id: None,
            algo_client_id: None,
        },
        status: v::OrderStatus::Unknown,
        side: order.side,
        position_side: v::PositionSide::Both,
        original_quantity: None,
        cumulative_quantity: None,
        cumulative_quote: None,
        leaves_quantity: None,
        order_price: None,
    };
    // Source position identifies the journal item; economic deduplication is by order and venue trade ID.
    let event_id = v::BoundedText::new(format!(
        "{}-{}",
        hex(&source.input_stream),
        source.input_sequence
    ))
    .map_err(|_| ReportError::Identity)?;
    v::CanonicalEvent::new(v::VenueEvent {
        identity: v::EventIdentity {
            risk_domain_id: r.risk_domain_id,
            venue: r.venue,
            account: r.account,
            environment: r.environment,
            product: r.product,
            instrument_id: c.context.instrument.instrument_id,
            instrument_version: c.context.instrument.instrument_version,
            source_connection_epoch: source.connection_epoch,
            source_sequence: None,
            event_id,
            venue_time_ns: Some(at),
            received_mono_ns: source.received_mono_ns,
        },
        body: v::VenueEventBody::Fill(Box::new(v::FillEvent {
            observation,
            trade_id: trade,
            last_price: price,
            last_quantity: qty,
            last_quote: Some(quote),
            liquidity,
            fees: v::BoundedList::new(fees).map_err(|_| ReportError::Invalid)?,
            realized_pnl: None,
        })),
    })
    .map_err(|_| ReportError::Invalid)
}
