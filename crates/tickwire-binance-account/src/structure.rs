use crate::*;
use tickwire_binance::events::{PrivateStream, SessionContext, SessionIdentity};
use tickwire_binance::feed::TimeUnit;

impl<S: Store> AccountIngress<S> {
    pub(crate) fn check_structure_raw(
        &self,
        meta: &SourceMeta,
        raw: &[u8],
        event: &CanonicalEvent,
    ) -> Result<()> {
        let v::VenueEventBody::Structure(s) = &event.event().body else {
            return Err(Error::Identity);
        };
        let reg = self
            .registry
            .get(&IntentKey {
                epoch: s.parent.original_intent_exec_epoch,
                intent_id: s.parent.original_intent_id,
            })
            .ok_or(Error::Identity)?;
        let scope = &self.config.baseline.snapshot().scope;
        let session = SessionContext::new(
            SessionIdentity {
                risk_domain_id: scope.risk_domain_id,
                venue: scope.venue,
                environment: scope.environment,
                account: scope.account,
                connection_epoch: meta.epoch,
            },
            self.market()?,
            match meta.subscription {
                Some(subscription_id) => PrivateStream::SpotSubscription { subscription_id },
                None => PrivateStream::UsdmListenKey,
            },
            if meta.micros {
                TimeUnit::Microseconds
            } else {
                TimeUnit::Milliseconds
            },
        )
        .map_err(|_| Error::Identity)?;
        let envelope = if let Some(subscription) = meta.subscription {
            let mut bytes = format!("{{\"subscriptionId\":{subscription},\"event\":").into_bytes();
            bytes.extend_from_slice(raw);
            bytes.push(b'}');
            bytes
        } else {
            raw.to_vec()
        };
        let parsed = tickwire_binance::events::decode_private::<3>(
            &session,
            &envelope,
            self.config
                .max_raw_bytes
                .checked_add(128)
                .ok_or(Error::Capacity)?,
            meta.received,
        )
        .map_err(|_| Error::Encoding)?;
        let actual = tickwire_binance::events::bind_structure(&parsed, &reg.command, &reg.orders)
            .map_err(|_| Error::Identity)?;
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
    use crate::reconciliation::income_import::tests::Memory;
    fn input() -> AccountIngress<Memory> {
        let (s, c, _) = fixture::fixture();
        AccountIngress::create(
            Memory::default(),
            Config {
                physical: tickwire_journal::Config::new([81; 16], [1; 16]),
                writer_epoch: 4,
                baseline: CanonicalBaseline::new(s, c).unwrap(),
                account_stream: [2; 16],
                max_records: 64,
                max_registrations: 8,
                max_raw_bytes: 32768,
                max_retained_bytes: 1 << 20,
                income_association_authority: None,
                income_baseline_authority: None,
            },
        )
        .unwrap()
    }
    fn command(n: u8) -> CanonicalCommand {
        let (_, _, mut c) = fixture::fixture();
        let v::CommandBody::New(mut a) = c.body.clone() else {
            unreachable!()
        };
        a.client_id = v::ClientId::new(format!("leg-{n}-a")).unwrap();
        let mut b = a.clone();
        b.client_id = v::ClientId::new(format!("leg-{n}-b")).unwrap();
        c.context.request.intent_id = n as u64;
        c.body = v::CommandBody::SpotList(v::ListOrder {
            list_id: [n; 16],
            client_id: v::ClientId::new(format!("parent-{n}")).unwrap(),
            kind: v::ListKind::Oto,
            legs: v::BoundedList::new(vec![a, b]).unwrap(),
        });
        CanonicalCommand::new(c).unwrap()
    }
    fn observed(input: &AccountIngress<Memory>, n: u8, parent: u64, child: u64) -> CanonicalEvent {
        let raw=serde_json::to_vec(&serde_json::json!({"orderListId":parent,"contingencyType":"OTO","listStatusType":"ALL_DONE","listOrderStatus":"ALL_DONE","listClientOrderId":format!("parent-{n}"),"symbol":"BTCUSDT","orders":[{"symbol":"BTCUSDT","orderId":child,"clientOrderId":format!("leg-{n}-a")},{"symbol":"BTCUSDT","orderId":child+1,"clientOrderId":format!("leg-{n}-b")}]})).unwrap();
        let reg = &input.registry[&IntentKey {
            epoch: 4,
            intent_id: n as u64,
        }];
        tickwire_binance::events::bind_structure_response(
            &SessionIdentity {
                risk_domain_id: [1; 16],
                venue: 1,
                account: 2,
                environment: v::Environment::Test,
                connection_epoch: 7,
            },
            tickwire_binance::request::Market::Spot,
            &reg.command,
            &reg.orders,
            &raw,
            100,
        )
        .unwrap()
    }
    #[test]
    fn native_parent_and_child_ids_cannot_be_reassigned_to_another_retained_structure() {
        let mut input = input();
        input
            .register_command(command(3), &[[30; 16], [31; 16]])
            .unwrap();
        input
            .register_command(command(4), &[[40; 16], [41; 16]])
            .unwrap();
        let first = observed(&input, 3, 55, 21);
        registry::learn(&mut input.registry, &first).unwrap();
        for (parent, child) in [(55, 31), (56, 21)] {
            let conflict = observed(&input, 4, parent, child);
            assert!(matches!(
                registry::learn(&mut input.registry, &conflict),
                Err(Error::Conflict)
            ));
            let untouched = &input.registry[&IntentKey {
                epoch: 4,
                intent_id: 4,
            }];
            assert!(
                untouched
                    .orders
                    .iter()
                    .all(|o| o.venue_order_id.is_none() && o.venue_list_id.is_none())
            );
        }
        let valid = observed(&input, 4, 56, 31);
        registry::learn(&mut input.registry, &valid).unwrap();
        registry::learn(&mut input.registry, &first).unwrap();
        assert_eq!(input.registry.len(), 2);
    }
}
