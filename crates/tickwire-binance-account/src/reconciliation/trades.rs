use super::*;
use serde::Deserialize;
use tickwire_binance::decimal::Decimal;
#[derive(Deserialize)]
struct Trade<'a> {
    #[serde(borrow)]
    symbol: &'a str,
    id: u64,
    #[serde(rename = "orderId")]
    order: u64,
    price: Decimal,
    qty: Decimal,
    #[serde(rename = "quoteQty")]
    quote: Decimal,
    commission: Decimal,
    #[serde(borrow, rename = "commissionAsset")]
    asset: &'a str,
    #[serde(alias = "isBuyer")]
    buyer: bool,
    #[serde(alias = "isMaker")]
    maker: bool,
    #[serde(borrow)]
    side: Option<&'a str>,
    #[serde(borrow, rename = "positionSide")]
    position: Option<&'a str>,
    #[serde(rename = "realizedPnl")]
    pnl: Option<Decimal>,
    time: u64,
}
pub(super) fn bind(
    command: &CanonicalCommand,
    identity: &v::OrderIdentity,
    assets: &[tickwire_account::AssetSnapshot],
    query: TradeQuery,
    epoch: u64,
    received: u64,
    raw: &[u8],
) -> Result<Vec<CanonicalEvent>> {
    query.validate()?;
    let rows: Vec<&serde_json::value::RawValue> =
        serde_json::from_slice(raw).map_err(|_| Error::Encoding)?;
    if rows.len() > query.limit as usize {
        return Err(Error::Capacity);
    }
    let c = &command.command().context;
    let (side, position) = match &command.command().body {
        v::CommandBody::New(s) | v::CommandBody::CancelReplace { replacement: s, .. } => {
            (s.side, s.position_side)
        }
        v::CommandBody::SpotList(list) => {
            let s = list
                .legs
                .as_slice()
                .iter()
                .find(|s| Some(&s.client_id) == identity.client_id.as_ref())
                .ok_or(Error::Identity)?;
            (s.side, s.position_side)
        }
        v::CommandBody::Algo(algo)
            if identity.parent_order_id == Some(algo.parent_order_id)
                && identity.algo_client_id.as_ref() == Some(&algo.order.client_id)
                && identity.client_id.as_ref() == Some(&algo.order.client_id)
                && identity.venue_algo_id.is_some()
                && identity.venue_order_id.is_some() =>
        {
            (algo.order.side, algo.order.position_side)
        }
        _ => return Err(Error::Unsupported),
    };
    let mut previous = None;
    let mut events = Vec::new();
    for raw in rows {
        let trade: Trade<'_> = serde_json::from_str(raw.get()).map_err(|_| Error::Encoding)?;
        if trade.id > i64::MAX as u64
            || trade.order > i64::MAX as u64
            || trade.id < query.from_id
            || previous.is_some_and(|old| trade.id <= old)
            || trade.symbol != c.instrument.symbol.as_str()
            || identity.order_id != query.order_id
            || identity
                .venue_order_id
                .as_ref()
                .is_none_or(|id| id.as_str() != trade.order.to_string())
            || trade.qty.mantissa() <= 0
            || trade.price.mantissa() <= 0
            || trade.quote.mantissa() <= 0
            || trade.buyer != (side == v::Side::Buy)
        {
            return Err(Error::Identity);
        }
        let usdm = c.request.product != v::Product::Spot;
        if usdm
            && (trade.pnl.is_none()
                || trade.side != Some(if side == v::Side::Buy { "BUY" } else { "SELL" })
                || trade.position
                    != Some(match position {
                        v::PositionSide::Both => "BOTH",
                        v::PositionSide::Long => "LONG",
                        v::PositionSide::Short => "SHORT",
                    }))
        {
            return Err(Error::Identity);
        }
        if !usdm && trade.pnl.is_some() {
            return Err(Error::Identity);
        }
        previous = Some(trade.id);
        let asset = assets
            .iter()
            .map(|a| &a.asset)
            .find(|a| a.symbol.as_str() == trade.asset)
            .ok_or(Error::Identity)?
            .clone();
        let fee = v::AssetDelta {
            asset,
            amount: v::Decimal::new(
                trade
                    .commission
                    .mantissa()
                    .checked_neg()
                    .ok_or(Error::Encoding)?,
                -(trade.commission.scale() as i8),
            )
            .map_err(|_| Error::Encoding)?,
        };
        let hash = ring::digest::digest(&ring::digest::SHA256, raw.get().as_bytes());
        let hex: String = hash.as_ref().iter().map(|b| format!("{b:02x}")).collect();
        events.push(
            CanonicalEvent::new(v::VenueEvent {
                identity: v::EventIdentity {
                    risk_domain_id: c.request.risk_domain_id,
                    venue: c.request.venue,
                    environment: c.request.environment,
                    account: c.request.account,
                    product: c.request.product,
                    instrument_id: c.instrument.instrument_id,
                    instrument_version: c.instrument.instrument_version,
                    source_connection_epoch: epoch,
                    source_sequence: None,
                    event_id: v::BoundedText::new(hex).map_err(|_| Error::Encoding)?,
                    venue_time_ns: Some(trade.time.checked_mul(1_000_000).ok_or(Error::Encoding)?),
                    received_mono_ns: received,
                },
                body: v::VenueEventBody::Fill(Box::new(v::FillEvent {
                    observation: v::OrderObservation {
                        order: identity.clone(),
                        status: v::OrderStatus::Unknown,
                        side,
                        position_side: position,
                        original_quantity: None,
                        cumulative_quantity: None,
                        cumulative_quote: None,
                        leaves_quantity: None,
                        order_price: None,
                    },
                    trade_id: v::VenueTradeId::new(trade.id.to_string())
                        .map_err(|_| Error::Encoding)?,
                    last_quantity: decimal(trade.qty)?,
                    last_price: decimal(trade.price)?,
                    last_quote: Some(decimal(trade.quote)?),
                    realized_pnl: trade
                        .pnl
                        .map(|p| {
                            Ok::<_, Error>(v::AssetDelta {
                                asset: c.instrument.settlement.clone(),
                                amount: decimal(p)?,
                            })
                        })
                        .transpose()?,
                    liquidity: if trade.maker {
                        v::Liquidity::Maker
                    } else {
                        v::Liquidity::Taker
                    },
                    fees: v::BoundedList::new(vec![fee]).map_err(|_| Error::Encoding)?,
                })),
            })
            .map_err(|_| Error::Encoding)?,
        );
    }
    Ok(events)
}
fn decimal(n: Decimal) -> Result<v::Decimal> {
    v::Decimal::new(n.mantissa(), -(n.scale() as i8)).map_err(|_| Error::Encoding)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn setup(
        futures: bool,
    ) -> (
        CanonicalCommand,
        v::OrderIdentity,
        Vec<tickwire_account::AssetSnapshot>,
        TradeQuery,
    ) {
        let (snapshot, _, command) = if futures {
            crate::unit_fixture::futures_fixture()
        } else {
            crate::unit_fixture::fixture()
        };
        let event = crate::unit_fixture::fill(&command, "1", "0.0005", "60000", "30");
        let v::VenueEventBody::Fill(fill) = event.body else {
            unreachable!()
        };
        let mut identity = fill.observation.order;
        identity.venue_order_id = Some(v::VenueOrderId::new("77").unwrap());
        let command = crate::unit_fixture::canonical(command);
        let query = TradeQuery {
            intent: intent(&command),
            order_id: identity.order_id,
            from_id: 1,
            limit: 2,
            private_connection_epoch: 1,
            gap_generation: 1,
        };
        (command, identity, snapshot.assets, query)
    }
    const SPOT:&[u8]=br#"[{"symbol":"BTCUSDT","id":1,"orderId":77,"price":"60000","qty":"0.0005","quoteQty":"30","commission":"0.03","commissionAsset":"USDT","isBuyer":true,"isMaker":false,"time":100}]"#;
    #[test]
    fn spot_exact_trade_has_fee_and_no_invented_cumulative() {
        let (c, id, a, q) = setup(false);
        let events = bind(&c, &id, &a, q, 9, 100, SPOT).unwrap();
        let v::VenueEventBody::Fill(fill) = &events[0].event().body else {
            panic!()
        };
        assert_eq!(fill.last_quote, Some("30".parse().unwrap()));
        assert_eq!(fill.fees.as_slice()[0].amount, "-0.03".parse().unwrap());
        assert_eq!(fill.observation.cumulative_quantity, None);
        assert_eq!(fill.observation.status, v::OrderStatus::Unknown);
    }
    #[test]
    fn trade_identity_and_ordering_are_strict() {
        let (c, id, a, q) = setup(false);
        for raw in [
            String::from_utf8(SPOT.to_vec())
                .unwrap()
                .replace("77", "78"),
            String::from_utf8(SPOT.to_vec())
                .unwrap()
                .replace("BTCUSDT", "ETHUSDT"),
        ] {
            assert!(bind(&c, &id, &a, q, 9, 100, raw.as_bytes()).is_err());
        }
        let one = std::str::from_utf8(SPOT).unwrap();
        let duplicated = format!("[{},{}]", &one[1..one.len() - 1], &one[1..one.len() - 1]);
        assert!(bind(&c, &id, &a, q, 9, 100, duplicated.as_bytes()).is_err());
    }
    #[test]
    fn futures_requires_reported_pnl_and_position() {
        let (c, id, a, q) = setup(true);
        assert!(bind(&c, &id, &a, q, 9, 100, SPOT).is_err());
        let raw = std::str::from_utf8(SPOT).unwrap().replace(
            "\"time\":100",
            "\"time\":100,\"side\":\"BUY\",\"positionSide\":\"BOTH\",\"realizedPnl\":\"0\"",
        );
        let events = bind(&c, &id, &a, q, 9, 100, raw.as_bytes()).unwrap();
        let v::VenueEventBody::Fill(fill) = &events[0].event().body else {
            panic!()
        };
        assert!(fill.realized_pnl.is_some());
    }
    #[test]
    fn algo_trade_query_uses_original_potential_leg_and_exact_native_child() {
        let (command, mut identity, assets, query) = setup(true);
        let mut command = command.command().clone();
        let v::CommandBody::New(mut order) = command.body.clone() else {
            unreachable!()
        };
        order.order_type = v::OrderType::StopLimit;
        order.trigger = Some(v::Trigger {
            price: v::Decimal::new(110, 0).unwrap(),
            working_price: v::WorkingPrice::Contract,
            price_protect: false,
        });
        command.body = v::CommandBody::Algo(v::AlgoOrder {
            parent_order_id: [73; 16],
            order,
        });
        let command = CanonicalCommand::new(command).unwrap();
        identity.parent_order_id = Some([73; 16]);
        identity.venue_algo_id = Some(v::VenueOrderId::new("55").unwrap());
        identity.algo_client_id = identity.client_id.clone();
        let raw = std::str::from_utf8(SPOT).unwrap().replace(
            "\"time\":100",
            "\"time\":100,\"side\":\"BUY\",\"positionSide\":\"BOTH\",\"realizedPnl\":\"0\"",
        );
        let events = bind(&command, &identity, &assets, query, 9, 100, raw.as_bytes()).unwrap();
        let v::VenueEventBody::Fill(fill) = &events[0].event().body else {
            panic!()
        };
        assert_eq!(fill.observation.order, identity);
        assert_eq!(fill.fees.as_slice()[0].amount, "-0.03".parse().unwrap());
        assert_eq!(fill.observation.cumulative_quantity, None);
        let wrong = raw.replace("\"orderId\":77", "\"orderId\":78");
        assert!(
            bind(
                &command,
                &identity,
                &assets,
                query,
                9,
                100,
                wrong.as_bytes()
            )
            .is_err()
        );
    }
    #[test]
    fn raw_response_and_trade_query_round_trip_together() {
        let (c, id, a, q) = setup(false);
        let events = bind(&c, &id, &a, q, 9, 100, SPOT).unwrap();
        let record = crate::rest::RestRecord {
            command: c,
            query: Some(q),
            order_query: None,
            amendment_query: None,
            structure_query: None,
            structure_domain: None,
            epoch: 9,
            received: 100,
            status: 200,
            raw: SPOT.to_vec(),
            disposition: RestDisposition::NeedsReconciliation,
            events,
        };
        let bytes = codec::encode(&Record::Rest(record)).unwrap();
        assert_eq!(bytes[8], 7);
        let decoded = codec::decode(&bytes).unwrap();
        assert_eq!(codec::encode(&decoded).unwrap(), bytes);
        let Record::Rest(decoded) = decoded else {
            panic!()
        };
        assert_eq!(decoded.query, Some(q));
        assert_eq!(decoded.raw, SPOT);
    }
}
