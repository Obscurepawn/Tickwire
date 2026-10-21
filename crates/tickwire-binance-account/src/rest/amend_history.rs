use super::*;
use crate::reconciliation::amendments::AmendmentQuery;
use serde::Deserialize;
use tickwire_binance::decimal::Decimal;
#[derive(Deserialize)]
struct Row<'a> {
    #[serde(borrow)]
    symbol: &'a str,
    #[serde(rename = "orderId")]
    order_id: u64,
    #[serde(rename = "executionId")]
    execution_id: u64,
    #[serde(borrow, rename = "origClientOrderId")]
    original_client: &'a str,
    #[serde(borrow, rename = "newClientOrderId")]
    new_client: &'a str,
    #[serde(rename = "origQty")]
    before: Decimal,
    #[serde(rename = "newQty")]
    after: Decimal,
    time: u64,
}
struct Rows<'a>(Vec<&'a serde_json::value::RawValue>);
impl<'de> Deserialize<'de> for Rows<'de> {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = Rows<'de>;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("at most 16 amendment rows")
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                let mut rows = Vec::new();
                while let Some(row) = seq.next_element()? {
                    if rows.len() == 16 {
                        return Err(serde::de::Error::custom("amendment page capacity"));
                    }
                    rows.push(row);
                }
                Ok(Rows(rows))
            }
        }
        d.deserialize_seq(Visitor)
    }
}
fn d(n: Decimal) -> Result<v::Decimal> {
    v::Decimal::new(n.mantissa(), -(n.scale() as i8)).map_err(|_| Error::Encoding)
}
fn fixed(n: tickwire_account::Fixed) -> Result<v::Decimal> {
    v::Decimal::new(n.atoms(), -(n.scale() as i8)).map_err(|_| Error::Encoding)
}
fn rows<'a>(query: AmendmentQuery, body: &'a [u8]) -> Result<Vec<Row<'a>>> {
    query.validate()?;
    let parsed: Rows<'a> = serde_json::from_slice(body).map_err(|_| Error::Encoding)?;
    if parsed.0.len() > query.limit as usize {
        return Err(Error::Capacity);
    }
    let mut result = Vec::new();
    let mut previous = None;
    for raw in parsed.0 {
        let row: Row<'a> = serde_json::from_str(raw.get()).map_err(|_| Error::Encoding)?;
        if row.execution_id == 0
            || row.execution_id > i64::MAX as u64
            || row.execution_id < query.from_execution_id
            || previous.is_some_and(|p| row.execution_id <= p)
        {
            return Err(Error::Identity);
        }
        previous = Some(row.execution_id);
        result.push(row);
    }
    Ok(result)
}
#[allow(clippy::too_many_arguments)]
pub(crate) fn bind_rows(
    command: &CanonicalCommand,
    identity: &v::OrderIdentity,
    query: AmendmentQuery,
    amendments: &[tickwire_account::AmendmentView],
    epoch: u64,
    received: u64,
    body: &[u8],
) -> Result<Vec<CanonicalEvent>> {
    let mut events = Vec::new();
    for row in rows(query, body)? {
        let exact: Vec<_> = amendments
            .iter()
            .filter(|a| {
                a.order.intent == query.intent
                    && Some(&a.order.client_id) == identity.client_id.as_ref()
                    && a.executions
                        .contains(&(v::AmendmentSource::RestHistory, row.execution_id))
            })
            .collect();
        let before = d(row.before)?;
        let after = d(row.after)?;
        let a = match exact.as_slice() {
            [a] => *a,
            [] => {
                let matches: Vec<_> = amendments
                    .iter()
                    .filter(|a| {
                        a.order.intent == query.intent
                            && Some(&a.order.client_id) == identity.client_id.as_ref()
                            && fixed(a.previous_quantity).ok() == Some(before)
                            && fixed(a.new_quantity).ok() == Some(after)
                    })
                    .collect();
                match matches.as_slice() {
                    [a] => *a,
                    _ => return Err(Error::Conflict),
                }
            }
            _ => return Err(Error::Conflict),
        };
        let binding = v::AmendmentObservation {
            order: identity.clone(),
            request_intent_id: a.intent.intent_id,
            request_intent_exec_epoch: a.intent.epoch,
            request_digest: a.command_digest,
            execution_source: v::AmendmentSource::RestHistory,
            execution_id: row.execution_id,
            previous_quantity: fixed(a.previous_quantity)?,
            new_quantity: fixed(a.new_quantity)?,
            observation: None,
        };
        events.push(event(
            command, identity, query, epoch, received, body, &row, &binding,
        )?);
    }
    Ok(events)
}
#[allow(clippy::too_many_arguments)]
fn event(
    command: &CanonicalCommand,
    id: &v::OrderIdentity,
    q: AmendmentQuery,
    epoch: u64,
    received: u64,
    body: &[u8],
    row: &Row<'_>,
    a: &v::AmendmentObservation,
) -> Result<CanonicalEvent> {
    let c = &command.command().context;
    let v::CommandBody::New(spec) = &command.command().body else {
        return Err(Error::Identity);
    };
    if c.request.product != v::Product::Spot
        || spec.order_type != v::OrderType::Limit
        || spec.time_in_force != Some(v::TimeInForce::Gtc)
        || spec.iceberg_quantity.is_some()
        || intent(command) != q.intent
        || id.order_id != q.order_id
        || id.list_id.is_some()
        || id.parent_order_id.is_some()
        || row.symbol != c.instrument.symbol.as_str()
        || row.original_client != spec.client_id.as_str()
        || row.new_client != spec.client_id.as_str()
        || id
            .venue_order_id
            .as_ref()
            .is_none_or(|n| n.as_str() != row.order_id.to_string())
        || a.order != *id
        || a.previous_quantity != d(row.before)?
        || a.new_quantity != d(row.after)?
        || a.execution_id != row.execution_id
        || a.execution_source != v::AmendmentSource::RestHistory
        || a.observation.is_some()
    {
        return Err(Error::Identity);
    }
    let mut h = ring::digest::Context::new(&ring::digest::SHA256);
    h.update(b"tickwire.rest.amendment-history.v1");
    h.update(&command.digest());
    h.update(&a.request_digest);
    h.update(&epoch.to_le_bytes());
    h.update(&row.execution_id.to_le_bytes());
    h.update(body);
    let event_id = h
        .finish()
        .as_ref()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
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
            event_id: v::BoundedText::new(event_id).map_err(|_| Error::Encoding)?,
            venue_time_ns: Some(row.time.checked_mul(1_000_000).ok_or(Error::Capacity)?),
            received_mono_ns: received,
        },
        body: v::VenueEventBody::Amendment(Box::new(a.clone())),
    })
    .map_err(|_| Error::Encoding)
}
pub(super) fn verify_raw<S: Store>(input: &AccountIngress<S>, record: &RestRecord) -> Result<()> {
    let q = record.amendment_query.ok_or(Error::Identity)?;
    q.validate()?;
    if record.disposition == RestDisposition::Unknown {
        return if record.events.is_empty() {
            Ok(())
        } else {
            Err(Error::Identity)
        };
    }
    if record.disposition != RestDisposition::Observed || record.status != 200 {
        return Err(Error::Identity);
    }
    let mut decoder =
        ResponseDecoder::new(input.config.max_raw_bytes, 256).map_err(Error::Transport)?;
    decoder.push(&record.raw).map_err(Error::Transport)?;
    decoder.finish();
    let response = decoder
        .next_response()
        .map_err(Error::Transport)?
        .ok_or(Error::Encoding)?;
    if response.status != record.status || !decoder.take_buffered().is_empty() {
        return Err(Error::Identity);
    }
    let rows = rows(q, &response.body)?;
    if rows.len() != record.events.len() {
        return Err(Error::Identity);
    }
    let reg = input.registry.get(&q.intent).ok_or(Error::Identity)?;
    if reg.command != record.command {
        return Err(Error::Identity);
    }
    let identity = reg
        .orders
        .iter()
        .find(|id| id.order_id == q.order_id)
        .ok_or(Error::Identity)?;
    for (row, expected) in rows.iter().zip(&record.events) {
        let v::VenueEventBody::Amendment(a) = &expected.event().body else {
            return Err(Error::Identity);
        };
        let actual = event(
            &record.command,
            identity,
            q,
            record.epoch,
            record.received,
            &response.body,
            row,
            a,
        )?;
        if actual != *expected {
            return Err(Error::Identity);
        }
    }
    Ok(())
}
impl<S: Store> AccountIngress<S> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn persist_amendment_query<B: Backend, A: Authority, E: Encoder>(
        &mut self,
        gateway: &mut AccountGateway<B, A, E>,
        query: AmendmentQuery,
        epoch: u64,
        received: u64,
        status: u16,
        raw: Vec<u8>,
        body: Option<&[u8]>,
    ) -> Result<RestOutcome> {
        self.ready(gateway)?;
        query.validate()?;
        gateway.freeze_account();
        let reg = self.registry.get(&query.intent).ok_or(Error::Identity)?;
        if gateway.command(query.intent) != Some(&reg.command) {
            return Err(Error::Identity);
        }
        let identity = reg
            .orders
            .iter()
            .find(|id| id.order_id == query.order_id)
            .ok_or(Error::Identity)?;
        let mut record = RestRecord {
            command: reg.command.clone(),
            query: None,
            order_query: None,
            structure_query: None,
            amendment_query: Some(query),
            structure_domain: None,
            epoch,
            received,
            status,
            raw,
            disposition: RestDisposition::Unknown,
            events: Vec::new(),
        };
        if status == 200
            && let Some(body) = body
            && let Ok(events) = bind_rows(
                &reg.command,
                identity,
                query,
                &gateway.account().amendments().collect::<Vec<_>>(),
                epoch,
                received,
                body,
            )
        {
            let mut candidate = gateway.account().clone();
            if events.iter().all(|e| candidate.apply_event(e).is_ok()) {
                record.events = events;
                record.disposition = RestDisposition::Observed;
            }
        }
        self.commit_rest(gateway, record)
    }
}
