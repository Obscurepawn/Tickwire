use crate::*;
use tickwire_account::Account;
use tickwire_binance::events::OrderUpdate;

pub(crate) fn bind_private(
    account: &Account,
    original: &CanonicalCommand,
    canonical: CanonicalEvent,
    wire: &OrderUpdate<'_>,
) -> Result<CanonicalEvent> {
    let v::VenueEventBody::Order(observation) = &canonical.event().body else {
        return Err(Error::Identity);
    };
    let key = tickwire_account::OrderKey {
        intent: IntentKey {
            epoch: observation.order.original_intent_exec_epoch,
            intent_id: observation.order.original_intent_id,
        },
        client_id: observation.order.client_id.clone().ok_or(Error::Identity)?,
    };
    let execution = wire.execution_id.ok_or(Error::Identity)?;
    let all: Vec<_> = account.amendments().filter(|a| a.order == key).collect();
    // An old execution wins over a later pending mutation. REST and private IDs
    // intentionally occupy different namespaces: the protocol does not equate them.
    let exact: Vec<_> = all
        .iter()
        .filter(|a| {
            a.executions
                .contains(&(v::AmendmentSource::PrivateStream, execution))
        })
        .collect();
    let quantity = observation.original_quantity.ok_or(Error::Identity)?;
    let selected = match exact.as_slice() {
        [one] => *one,
        [] => {
            let candidates: Vec<_> = all
                .iter()
                .filter(|a| decimal(a.new_quantity).ok() == Some(quantity))
                .collect();
            match candidates.as_slice() {
                [one] => *one,
                _ => return Err(Error::Conflict),
            }
        }
        _ => return Err(Error::Conflict),
    };
    let binding = v::AmendmentObservation {
        order: observation.order.clone(),
        request_intent_id: selected.intent.intent_id,
        request_intent_exec_epoch: selected.intent.epoch,
        request_digest: selected.command_digest,
        execution_source: v::AmendmentSource::PrivateStream,
        execution_id: execution,
        previous_quantity: decimal(selected.previous_quantity)?,
        new_quantity: decimal(selected.new_quantity)?,
        observation: Some(*observation.clone()),
    };
    from_bound(original, canonical, wire, &binding)
}
fn decimal(value: tickwire_account::Fixed) -> Result<v::Decimal> {
    v::Decimal::new(value.atoms(), -(value.scale() as i8)).map_err(|_| Error::Encoding)
}
fn from_bound(
    original: &CanonicalCommand,
    canonical: CanonicalEvent,
    wire: &OrderUpdate<'_>,
    binding: &v::AmendmentObservation,
) -> Result<CanonicalEvent> {
    let v::CommandBody::New(spec) = &original.command().body else {
        return Err(Error::Identity);
    };
    let v::VenueEventBody::Order(observation) = &canonical.event().body else {
        return Err(Error::Identity);
    };
    if original.command().context.request.product != v::Product::Spot
        || spec.order_type != v::OrderType::Limit
        || spec.time_in_force != Some(v::TimeInForce::Gtc)
        || spec.iceberg_quantity.is_some()
        || wire.execution_type != "REPLACED"
        || wire.order_type != "LIMIT"
        || wire.time_in_force != "GTC"
        || wire.client_id != spec.client_id.as_str()
        || wire
            .original_client_id
            .as_deref()
            .is_some_and(|c| !c.is_empty() && c != spec.client_id.as_str())
        || wire.list_id.is_some()
        || wire.last_quantity.mantissa() != 0
        || wire.last_price.mantissa() != 0
        || wire.last_quote.is_some_and(|q| q.mantissa() != 0)
        || wire.commission.is_some_and(|q| q.mantissa() != 0)
        || observation.order_price != spec.price
        || observation.original_quantity != Some(binding.new_quantity)
        || observation.order != binding.order
        || wire.execution_id != Some(binding.execution_id)
        || binding.execution_source != v::AmendmentSource::PrivateStream
    {
        return Err(Error::Identity);
    }
    let mut e = canonical.event().clone();
    let mut binding = binding.clone();
    binding.observation = Some(*observation.clone());
    e.body = v::VenueEventBody::Amendment(Box::new(binding));
    CanonicalEvent::new(e).map_err(|_| Error::Encoding)
}
impl<S: Store> AccountIngress<S> {
    pub(crate) fn check_amendment_raw(
        &self,
        meta: &SourceMeta,
        raw: &[u8],
        event: &CanonicalEvent,
    ) -> Result<()> {
        let v::VenueEventBody::Amendment(a) = &event.event().body else {
            return Err(Error::Identity);
        };
        let reg = self
            .registry
            .get(&IntentKey {
                epoch: a.order.original_intent_exec_epoch,
                intent_id: a.order.original_intent_id,
            })
            .ok_or(Error::Identity)?;
        let identity = reg
            .orders
            .iter()
            .find(|id| id.order_id == a.order.order_id)
            .ok_or(Error::Identity)?;
        let scope = &self.config.baseline.snapshot().scope;
        use tickwire_binance::events::{
            PrivateBody, PrivateStream, SessionContext, SessionIdentity,
        };
        let session = SessionContext::new(
            SessionIdentity {
                risk_domain_id: scope.risk_domain_id,
                venue: scope.venue,
                account: scope.account,
                environment: scope.environment,
                connection_epoch: meta.epoch,
            },
            self.market()?,
            PrivateStream::SpotSubscription {
                subscription_id: meta.subscription.ok_or(Error::Identity)?,
            },
            if meta.micros {
                tickwire_binance::feed::TimeUnit::Microseconds
            } else {
                tickwire_binance::feed::TimeUnit::Milliseconds
            },
        )
        .map_err(|_| Error::Identity)?;
        let mut envelope = format!(
            "{{\"subscriptionId\":{},\"event\":",
            meta.subscription.unwrap()
        )
        .into_bytes();
        envelope.extend_from_slice(raw);
        envelope.push(b'}');
        let decoded = tickwire_binance::events::decode_private::<3>(
            &session,
            &envelope,
            self.config
                .max_raw_bytes
                .checked_add(128)
                .ok_or(Error::Capacity)?,
            meta.received,
        )
        .map_err(|_| Error::Encoding)?;
        let PrivateBody::Order(wire) = decoded.body() else {
            return Err(Error::Identity);
        };
        let ordinary = tickwire_binance::events::bind_order(&decoded, &reg.command, identity, &[])
            .map_err(|_| Error::Identity)?;
        let actual = from_bound(&reg.command, ordinary, wire, a)?;
        if &actual != event {
            return Err(Error::Identity);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gateway_fixture::fixture;
    use tickwire_binance::events::{PrivateBody, PrivateStream, SessionContext, SessionIdentity};
    use tickwire_binance::{feed::TimeUnit, request::Market};

    fn context() -> SessionContext {
        SessionContext::new(
            SessionIdentity {
                risk_domain_id: [1; 16],
                venue: 1,
                account: 2,
                environment: v::Environment::Test,
                connection_epoch: 7,
            },
            Market::Spot,
            PrivateStream::SpotSubscription { subscription_id: 9 },
            TimeUnit::Milliseconds,
        )
        .unwrap()
    }
    fn wire(q: &str, execution: u64, cumulative: &str) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({"subscriptionId":9,"event":{"e":"executionReport","E":10,"s":"BTCUSDT","c":"test-1","S":"BUY","o":"LIMIT","f":"GTC","q":q,"p":"60000","g":-1,"C":"","x":"REPLACED","X":"NEW","i":21,"l":"0","z":cumulative,"L":"0","n":"0","N":null,"T":9,"t":-1,"I":execution,"m":true,"Z":"0","Y":"0","r":"NONE"}})).unwrap()
    }
    fn bind(
        a: &Account,
        c: &CanonicalCommand,
        id: &v::OrderIdentity,
        raw: &[u8],
    ) -> Result<CanonicalEvent> {
        let ctx = context();
        let event = tickwire_binance::events::decode_private::<8>(&ctx, raw, 4096, 100).unwrap();
        let canonical = tickwire_binance::events::bind_order(&event, c, id, &[]).unwrap();
        let PrivateBody::Order(o) = event.body() else {
            panic!()
        };
        bind_private(a, c, canonical, o)
    }
    fn setup() -> (Account, CanonicalCommand, v::OrderIdentity) {
        let (mut a, c) = fixture::setup();
        let c = fixture::canonical(c);
        a.admit(&c, 60).unwrap();
        let mut event = fixture::venue::event();
        let v::VenueEventBody::Fill(f) = event.body else {
            panic!()
        };
        let mut o = f.observation;
        o.order.venue_order_id = Some(v::VenueOrderId::new("21").unwrap());
        o.status = v::OrderStatus::New;
        o.cumulative_quantity = Some("0".parse().unwrap());
        let id = o.order.clone();
        event.body = v::VenueEventBody::Order(Box::new(o));
        a.apply_event(&CanonicalEvent::new(event).unwrap()).unwrap();
        (a, c, id)
    }
    fn request(
        a: &mut Account,
        c: &CanonicalCommand,
        id: &v::OrderIdentity,
        n: u64,
        q: &str,
    ) -> CanonicalCommand {
        let mut c = c.command().clone();
        c.context.request.intent_id = n;
        c.context.request.recovery_object += n;
        c.body = v::CommandBody::AmendKeepPriority {
            target: v::OrderTarget {
                order_id: id.order_id,
                expected_owner_revision: 1,
                original_intent_exec_epoch: 4,
                client_id: id.client_id.clone(),
                venue_order_id: id.venue_order_id.clone(),
            },
            new_quantity: q.parse().unwrap(),
            new_client_id: id.client_id.clone(),
        };
        let c = fixture::canonical(c);
        a.admit(&c, 62).unwrap();
        c
    }
    #[test]
    fn private_replaced_binds_completed_a_before_pending_b_and_keeps_observation() {
        let (mut a, c, id) = setup();
        let first = request(&mut a, &c, &id, 4, "0.0008");
        let e = bind(&a, &c, &id, &wire("0.0008", 99, "0"))
            .expect("actual REPLACED maps to durable mutation");
        a.apply_event(&e).unwrap();
        let second = request(&mut a, &c, &id, 5, "0.0006");
        let late = bind(&a, &c, &id, &wire("0.0008", 99, "0.0004")).unwrap();
        let v::VenueEventBody::Amendment(ref x) = late.event().body else {
            panic!()
        };
        assert_eq!(x.request_digest, first.digest());
        assert_ne!(x.request_digest, second.digest());
        a.apply_event(&late).unwrap();
        assert!(a.frozen());
        assert_eq!(
            a.amendments()
                .find(|x| x.intent.intent_id == 5)
                .unwrap()
                .confirmed,
            false
        );
        assert_eq!(
            a.order_keys()
                .map(|k| a.order(k).unwrap().effective_quantity)
                .collect::<Vec<_>>(),
            vec![fixture::f(80_000)]
        );
    }
    #[test]
    fn reused_execution_or_ambiguous_same_quantity_never_selects_sole_pending_request() {
        let (mut a, c, id) = setup();
        let first = request(&mut a, &c, &id, 4, "0.0008");
        let e = bind(&a, &c, &id, &wire("0.0008", 99, "0")).unwrap();
        a.apply_event(&e).unwrap();
        request(&mut a, &c, &id, 5, "0.0006");
        assert!(bind(&a, &c, &id, &wire("0.0006", 99, "0")).is_err());
        let (mut a, c, id) = setup();
        request(&mut a, &c, &id, 4, "0.0008");
        let old = a
            .admission(IntentKey {
                epoch: 4,
                intent_id: 4,
            })
            .unwrap()
            .clone();
        a.apply_abort(&old).unwrap();
        request(&mut a, &c, &id, 5, "0.0008");
        assert!(bind(&a, &c, &id, &wire("0.0008", 100, "0")).is_err());
        assert_ne!(first.digest(), [0; 32]);
    }
    #[test]
    fn amendment_history_restores_missing_response_and_reuses_exact_execution() {
        let (mut a, c, id) = setup();
        let req = request(&mut a, &c, &id, 4, "0.0008");
        let query = crate::reconciliation::amendments::AmendmentQuery {
            intent: IntentKey {
                epoch: 4,
                intent_id: 3,
            },
            order_id: id.order_id,
            from_execution_id: 0,
            limit: 16,
            private_connection_epoch: 7,
            gap_generation: 1,
        };
        let raw=br#"[{"symbol":"BTCUSDT","orderId":21,"executionId":75,"origClientOrderId":"test-1","newClientOrderId":"test-1","origQty":"0.001","newQty":"0.0008","time":1741926410255}]"#;
        let rows = crate::rest::amend_history::bind_rows(
            &c,
            &id,
            query,
            &a.amendments().collect::<Vec<_>>(),
            8,
            100,
            raw,
        )
        .expect("history row must bind original durable mutation");
        assert_eq!(rows.len(), 1);
        let v::VenueEventBody::Amendment(ref result) = rows[0].event().body else {
            panic!()
        };
        assert_eq!(result.request_digest, req.digest());
        assert_eq!(result.execution_source, v::AmendmentSource::RestHistory);
        assert!(result.observation.is_none());
        a.apply_event(&rows[0]).unwrap();
        assert_eq!(
            a.asset(fixture::key(2)).unwrap().reserved,
            fixture::units(48)
        );
        assert_eq!(
            a.apply_event(&rows[0]).unwrap(),
            tickwire_account::Applied::Duplicate
        );
    }
}
