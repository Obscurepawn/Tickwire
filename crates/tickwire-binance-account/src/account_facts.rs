//! Bounded normalization of account-wide observations; this module grants no authority.
use crate::*;
use tickwire_binance::events::{PrivateBody, SessionIdentity};

pub(crate) fn decimal(value: tickwire_binance::decimal::Decimal) -> Result<v::Decimal> {
    v::Decimal::new(value.mantissa(), -(value.scale() as i8)).map_err(|_| Error::Encoding)
}
pub(crate) fn identity(
    baseline: &CanonicalBaseline,
    session: &SessionIdentity,
) -> Result<v::AccountIdentity> {
    let scope = &baseline.snapshot().scope;
    if session.risk_domain_id != scope.risk_domain_id
        || session.venue != scope.venue
        || session.account != scope.account
        || session.environment != scope.environment
        || session.connection_epoch == 0
    {
        return Err(Error::Identity);
    }
    let first = baseline.catalog().entries.first().ok_or(Error::Identity)?;
    if baseline.catalog().entries.iter().any(|p| {
        p.wallet_id != first.wallet_id
            || !matches!(
                p.product,
                v::Product::LinearPerpetual | v::Product::LinearFuture
            )
            || p.futures
                .as_ref()
                .is_none_or(|f| f.margin_mode != v::MarginMode::Cross)
    }) {
        return Err(Error::Unsupported);
    }
    Ok(v::AccountIdentity {
        risk_domain_id: scope.risk_domain_id,
        venue: scope.venue,
        environment: scope.environment,
        account: scope.account,
        market: v::AccountMarket::Usdm,
        wallet_id: first.wallet_id,
    })
}
pub(crate) fn asset(
    baseline: &CanonicalBaseline,
    wallet: u64,
    symbol: &str,
) -> Result<v::AssetRef> {
    let mut found = baseline
        .snapshot()
        .assets
        .iter()
        .filter(|a| a.key.wallet_id == wallet && a.asset.symbol.as_str() == symbol);
    let asset = found.next().ok_or(Error::Identity)?.asset.clone();
    if found.next().is_some() {
        return Err(Error::Identity);
    }
    Ok(asset)
}
/// Bind a sparse authenticated observation to the pinned catalog. No bc field is
/// converted to a cashflow and missing assets/positions remain unreported.
pub fn bind_observation(
    baseline: &CanonicalBaseline,
    session: &SessionIdentity,
    body: &PrivateBody<'_>,
    event_ns: u64,
    transaction_ns: Option<u64>,
) -> Result<v::CanonicalAccountInput> {
    let identity = identity(baseline, session)?;
    let PrivateBody::UsdmAccount {
        reason,
        funding_symbol,
        balances,
        positions,
    } = body
    else {
        return Err(Error::Unsupported);
    };
    if balances.len() > v::MAX_ACCOUNT_OBSERVATION_ASSETS
        || positions.len() > v::MAX_ACCOUNT_OBSERVATION_POSITIONS
        || !event_ns.is_multiple_of(1_000_000)
        || transaction_ns.is_some_and(|n| !n.is_multiple_of(1_000_000))
    {
        return Err(Error::Capacity);
    }
    let balances = balances
        .iter()
        .map(|b| {
            Ok(v::WalletObservation {
                asset: asset(baseline, identity.wallet_id, &b.asset)?,
                wallet_balance: decimal(b.wallet_balance)?,
                cross_wallet_balance: decimal(b.cross_wallet_balance)?,
                balance_change: decimal(b.balance_change_excluding_pnl_and_commission)?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let positions = positions
        .iter()
        .map(|p| {
            let mut policies = baseline
                .catalog()
                .entries
                .iter()
                .filter(|i| i.instrument.symbol.as_str() == p.symbol);
            let policy = policies.next().ok_or(Error::Identity)?;
            if policies.next().is_some() {
                return Err(Error::Identity);
            }
            Ok(v::PositionObservation {
                instrument_id: policy.instrument.instrument_id,
                instrument_version: policy.instrument.instrument_version,
                symbol: policy.instrument.symbol.clone(),
                side: match p.position_side.as_ref() {
                    "BOTH" => v::PositionSide::Both,
                    "LONG" => v::PositionSide::Long,
                    "SHORT" => v::PositionSide::Short,
                    _ => return Err(Error::Unsupported),
                },
                quantity: decimal(p.position_amount)?,
                entry_price: decimal(p.entry_price)?,
                break_even_price: p.break_even_price.map(decimal).transpose()?,
                accumulated_realized: decimal(p.accumulated_realized)?,
                unrealized_pnl: decimal(p.unrealized_pnl)?,
                margin_mode: match p.margin_type.as_ref() {
                    "cross" => v::MarginMode::Cross,
                    "isolated" => v::MarginMode::Isolated,
                    _ => return Err(Error::Unsupported),
                },
                isolated_wallet: decimal(p.isolated_wallet)?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    v::CanonicalAccountInput::new(v::AccountInput {
        identity,
        body: v::AccountInputBody::Observation(v::AccountObservation {
            reason: v::BoundedText::new(reason.as_ref()).map_err(|_| Error::Encoding)?,
            funding_symbol: funding_symbol
                .as_deref()
                .map(v::Symbol::new)
                .transpose()
                .map_err(|_| Error::Encoding)?,
            event_time_ms: event_ns / 1_000_000,
            transaction_time_ms: transaction_ns.map(|n| n / 1_000_000),
            balances: v::BoundedList::new(balances).map_err(|_| Error::Capacity)?,
            positions: v::BoundedList::new(positions).map_err(|_| Error::Capacity)?,
        }),
    })
    .map_err(|_| Error::Encoding)
}

pub(crate) fn decode_observation(
    baseline: &CanonicalBaseline,
    meta: &SourceMeta,
    raw: &[u8],
    maximum: usize,
) -> Result<v::CanonicalAccountInput> {
    use tickwire_binance::{events::*, feed::TimeUnit, request::Market};
    if meta.subscription.is_some() || meta.micros {
        return Err(Error::Identity);
    }
    let scope = &baseline.snapshot().scope;
    let context = SessionContext::new(
        SessionIdentity {
            risk_domain_id: scope.risk_domain_id,
            venue: scope.venue,
            environment: scope.environment,
            account: scope.account,
            connection_epoch: meta.epoch,
        },
        Market::Usdm,
        PrivateStream::UsdmListenKey,
        TimeUnit::Milliseconds,
    )
    .map_err(|_| Error::Identity)?;
    let event = decode_private::<128>(&context, raw, maximum, meta.received)
        .map_err(|_| Error::Encoding)?;
    bind_observation(
        baseline,
        context.identity(),
        event.body(),
        event.event_time_ns(),
        event.transaction_time_ns(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn durable_observation_preserves_raw_and_rebinds_exact_content() {
        let (snapshot, catalog, _) = crate::unit_fixture::futures_fixture();
        let baseline = CanonicalBaseline::new(snapshot, catalog).unwrap();
        let meta = SourceMeta {
            epoch: 7,
            subscription: None,
            micros: false,
            received: 50,
        };
        let raw=br#"{"e":"ACCOUNT_UPDATE","E":100,"T":99,"a":{"m":"FUNDING_FEE","B":[{"a":"USDT","wb":"998","cw":"998","bc":"-2"}]}}"#;
        let input = decode_observation(&baseline, &meta, raw, 4096).unwrap();
        let bytes = codec::encode(&Record::Event(
            meta,
            raw.to_vec(),
            Decision::AccountBound(Box::new(input.clone())),
        ))
        .unwrap();
        let Record::Event(meta, stored_raw, Decision::AccountBound(stored)) =
            codec::decode(&bytes).unwrap()
        else {
            panic!("account event")
        };
        assert_eq!(stored_raw, raw);
        assert_eq!(stored.as_ref(), &input);
        assert_eq!(
            decode_observation(&baseline, &meta, &stored_raw, 4096).unwrap(),
            input
        );
        let changed = String::from_utf8(stored_raw).unwrap().replace("998", "997");
        assert_ne!(
            decode_observation(&baseline, &meta, changed.as_bytes(), 4096).unwrap(),
            input
        );
    }
}
