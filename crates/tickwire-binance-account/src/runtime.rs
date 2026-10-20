use crate::*;
use std::time::Instant;
use tickwire_account_gateway::{AccountGateway, Authority, Encoder, Status};
use tickwire_account_journal::Backend;
use tickwire_binance::{events::PrivateStream, feed::TimeUnit, request::Market};
use tickwire_binance_private::{GapNotice, PrivateEvent};

impl<S: Store> AccountIngress<S> {
    pub(crate) fn paired<B: Backend, A: Authority, E: Encoder>(
        &self,
        gateway: &AccountGateway<B, A, E>,
    ) -> Result<()> {
        if gateway.baseline().digest() != self.config.baseline.digest()
            || gateway.journal_config().physical.stream_epoch != self.config.account_stream
        {
            return Err(Error::Identity);
        }
        for record in &self.records {
            if let Record::Rest(rest)=record {
                if gateway.command(intent(&rest.command))!=Some(&rest.command) {return Err(Error::Identity);}
            }
        }
        // Every already-applied key in this input stream must have its exact input.
        // This also catches pairing a truncated inbox with a newer account WAL.
        for (stream, sequence, event) in gateway.ingress_events() {
            if stream == self.config.physical.stream_epoch {
                let index = usize::try_from(sequence)
                    .ok()
                    .and_then(|n| n.checked_sub(1))
                    .ok_or(Error::Identity)?;
                if self.effect(index) != Some(event)
                {
                    return Err(Error::Identity);
                }
            }
        }
        Ok(())
    }
    pub(crate) fn ready<B: Backend, A: Authority, E: Encoder>(
        &self,
        gateway: &AccountGateway<B, A, E>,
    ) -> Result<()> {
        self.paired(gateway)?;
        if self.stopped {
            return Err(Error::Stopped);
        }
        if self.cursor != self.records.len() {
            return Err(Error::ReplayPending);
        }
        Ok(())
    }
    pub fn submit<B: Backend, A: Authority, E: Encoder>(
        &mut self,
        gateway: &mut AccountGateway<B, A, E>,
        command: CanonicalCommand,
        ids: &[[u8; 16]],
        evaluated_at: u64,
        deadline: Instant,
    ) -> Result<Status> {
        self.ready(gateway)?;
        match command.command().body {
            v::CommandBody::New(_)
            | v::CommandBody::CancelReplace { .. }
            | v::CommandBody::SpotList(_) => {
                if let Err(error) = self.register_command(command.clone(), ids) {
                    if self.stopped {
                        gateway.freeze_account();
                    }
                    return Err(error);
                }
            }
            v::CommandBody::Cancel(_) | v::CommandBody::Query(_) | v::CommandBody::CancelList(_) if ids.is_empty() => {}
            _ => return Err(Error::Unsupported),
        }
        gateway
            .submit(command, evaluated_at, deadline)
            .map_err(Error::Gateway)
    }
    pub fn ingest<B: Backend, A: Authority, E: Encoder>(
        &mut self,
        gateway: &mut AccountGateway<B, A, E>,
        event: &PrivateEvent<'_>,
    ) -> Result<Outcome> {
        let result = self.ingest_inner(gateway, event);
        if result.is_err() {
            gateway.freeze_account();
        }
        result
    }
    fn ingest_inner<B: Backend, A: Authority, E: Encoder>(
        &mut self,
        gateway: &mut AccountGateway<B, A, E>,
        event: &PrivateEvent<'_>,
    ) -> Result<Outcome> {
        self.ready(gateway)?;
        let s = event.session().identity();
        let scope = &self.config.baseline.snapshot().scope;
        if s.risk_domain_id != scope.risk_domain_id
            || s.venue != scope.venue
            || s.account != scope.account
            || s.environment != scope.environment
            || event.session().market() != self.market()?
            || s.connection_epoch == 0
        {
            self.stopped = true;
            return Err(Error::Identity);
        }
        let raw = event.raw().get().as_bytes();
        if raw.len() > self.config.max_raw_bytes {
            self.stopped = true;
            return Err(Error::Capacity);
        }
        let meta = SourceMeta {
            epoch: s.connection_epoch,
            subscription: match event.session().stream() {
                PrivateStream::SpotSubscription { subscription_id } => Some(subscription_id),
                PrivateStream::UsdmListenKey => None,
            },
            micros: event.session().time_unit() == TimeUnit::Microseconds,
            received: event.received_mono_ns(),
        };
        let decision = self.decide(gateway, event);
        let mut learned = self.registry.clone();
        let decision = match decision {
            Decision::Bound(e) => {
                if registry::learn(&mut learned, &e).is_ok() {
                    Decision::Bound(e)
                } else {
                    Decision::Quarantine(QuarantineReason::BindingConflict)
                }
            }
            other => other,
        };
        if matches!(decision, Decision::Quarantine(_)) {
            gateway.freeze_account();
        }
        let key = self.append(Record::Event(meta, raw.to_vec(), decision.clone()))?;
        match decision {
            Decision::Bound(event) => {
                self.registry = learned;
                let result = gateway.record_ingress_event(key.stream, key.sequence, *event);
                match result {
                    Ok(applied) => {
                        self.cursor = self.records.len();
                        Ok(Outcome::Applied {
                            input: key,
                            applied,
                        })
                    }
                    Err(error) => {
                        self.stopped = true;
                        Err(Error::Gateway(error))
                    }
                }
            }
            Decision::Quarantine(reason) => {
                self.cursor = self.records.len();
                Ok(Outcome::Quarantined { input: key, reason })
            }
        }
    }
    fn decide<B: Backend, A: Authority, E: Encoder>(
        &self,
        gateway: &AccountGateway<B, A, E>,
        event: &PrivateEvent<'_>,
    ) -> Decision {
        let tickwire_binance::events::PrivateBody::Order(order) = event.body() else {
            return Decision::Quarantine(QuarantineReason::AccountReconciliation);
        };
        let native = order.order_id.to_string();
        let mut native_matches = Vec::new();
        let mut client_matches = Vec::new();
        for reg in self.registry.values() {
            if reg.command.command().context.instrument.symbol.as_str() != order.symbol {
                continue;
            }
            for identity in &reg.orders {
                if identity
                    .venue_order_id
                    .as_ref()
                    .is_some_and(|id| id.as_str() == native)
                {
                    native_matches.push((reg, identity));
                }
                if identity.client_id.as_ref().is_some_and(|id| {
                    id.as_str() == order.client_id
                        || order.original_client_id.as_deref() == Some(id.as_str())
                }) {
                    client_matches.push((reg, identity));
                }
            }
        }
        let candidates = if native_matches.is_empty() {
            client_matches
        } else {
            native_matches
        };
        let (reg, identity) = match candidates.as_slice() {
            [] => return Decision::Quarantine(QuarantineReason::UnknownOrder),
            [one] => *one,
            _ => return Decision::Quarantine(QuarantineReason::AmbiguousOrder),
        };
        if gateway.command(intent(&reg.command)) != Some(&reg.command) {
            return Decision::Quarantine(QuarantineReason::UnadmittedOrder);
        }
        let mut assets = Vec::new();
        for asset in &self.config.baseline.snapshot().assets {
            if !assets.contains(&asset.asset) {
                assets.push(asset.asset.clone());
            }
        }
        let Ok(canonical) = event.bind_order(&reg.command, identity, &assets) else {
            return Decision::Quarantine(QuarantineReason::BindingConflict);
        };
        if gateway.account().clone().apply_event(&canonical).is_err() {
            return Decision::Quarantine(QuarantineReason::AccountRejected);
        }
        Decision::Bound(Box::new(canonical))
    }
    pub fn source_gap<B: Backend, A: Authority, E: Encoder>(
        &mut self,
        gateway: &mut AccountGateway<B, A, E>,
        gap: &GapNotice,
    ) -> Result<InputKey> {
        gateway.freeze_account();
        self.ready(gateway)?;
        let scope = &self.config.baseline.snapshot().scope;
        let input = gap.token.scope();
        if input.risk_domain_id != scope.risk_domain_id
            || input.venue != scope.venue
            || input.environment != scope.environment
            || input.account != scope.account
            || input.market != self.market()?
        {
            self.stopped = true;
            return Err(Error::Identity);
        }
        let key = self.append(Record::Gap {
            epoch: gap.token.connection_epoch(),
            generation: gap.token.generation(),
            reason: gap_reason(gap.reason),
        })?;
        self.cursor = self.records.len();
        Ok(key)
    }
    pub fn replay_pending<B: Backend, A: Authority, E: Encoder>(
        &mut self,
        gateway: &mut AccountGateway<B, A, E>,
        maximum: usize,
    ) -> Result<usize> {
        // Recovery and partial replay remain closed to new risk throughout.
        gateway.freeze_account();
        self.paired(gateway)?;
        if self.stopped {
            return Err(Error::Stopped);
        }
        let mut processed = 0;
        while self.cursor < self.records.len() && processed < maximum {
            if let Some(event) = self.effect(self.cursor)
                && let Err(error) = gateway.record_ingress_event(
                    self.config.physical.stream_epoch,
                    self.cursor as u64 + 1,
                    event.clone(),
                )
            {
                self.stopped = true;
                return Err(Error::Gateway(error));
            }
            self.cursor += 1;
            processed += 1;
        }
        Ok(processed)
    }
    pub(crate) fn market(&self) -> Result<Market> {
        Ok(match product(&self.config)? {
            v::Product::Spot => Market::Spot,
            v::Product::LinearFuture | v::Product::LinearPerpetual => Market::Usdm,
            _ => return Err(Error::Unsupported),
        })
    }
}
fn gap_reason(reason: tickwire_binance_private::Error) -> u8 {
    use tickwire_binance_private::Error as E;
    match reason {
        E::Bounds => 1,
        E::Identity => 2,
        E::Unsupported => 3,
        E::Clock => 4,
        E::ClockDomain => 5,
        E::Stale => 6,
        E::Busy => 7,
        E::Protocol => 8,
        E::Transport => 9,
        E::Authentication => 10,
        E::Capacity => 11,
        E::Closed => 12,
        E::Exhausted => 13,
        E::Sink => 14,
    }
}
