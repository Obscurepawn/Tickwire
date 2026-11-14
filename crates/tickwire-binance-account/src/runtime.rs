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
        let frontier = gateway.history_frontier();
        let record_count = frontier.record_count();
        let ledger_seq = gateway.account().ledger_sequence();
        if record_count < self.min_gateway_record_count.get()
            || ledger_seq < self.min_gateway_ledger_seq.get()
        {
            return Err(Error::Identity);
        }
        let paired_key = (frontier.digest(), record_count, self.position());
        if record_count > 0 && self.last_paired_state.get() == Some(paired_key) {
            return Ok(());
        }
        let allow_evicted = self.retention_window_ns.is_some();
        for record in &self.records {
            if let Record::WsResponse(response) = record {
                let skip = allow_evicted
                    && response.binding.is_some_and(|b_seq| {
                        b_seq <= self.evicted_prefix as u64
                            || self
                                .ws_bindings()
                                .find(|b| b.sequence == b_seq)
                                .is_some_and(|b| {
                                    let k = intent(&b.command);
                                    (self.evicted_intents.contains(&k)
                                        || self.verified_gateway_intents.borrow().contains(&k))
                                        && !gateway.account().is_intent_active(k)
                                        && (gateway.admission_origin(k).is_none()
                                            || !self.registry.contains_key(&k))
                                })
                    });
                if !skip {
                    self.verify_ws_response(gateway, response)?;
                }
            }
            if let Record::WsBinding(binding) = record {
                let k = intent(&binding.command);
                if allow_evicted
                    && (self.evicted_intents.contains(&k)
                        || self.verified_gateway_intents.borrow().contains(&k))
                    && !gateway.account().is_intent_active(k)
                    && gateway.admission_origin(k).is_none()
                {
                    // Previously verified and retired past retention window.
                } else {
                    self.verify_ws_binding(gateway, binding)?;
                    self.verified_gateway_intents.borrow_mut().insert(k);
                }
            }
            if let Record::Rest(rest) = record {
                let k = intent(&rest.command);
                if allow_evicted
                    && (self.evicted_intents.contains(&k)
                        || self.verified_gateway_intents.borrow().contains(&k))
                    && !gateway.account().is_intent_active(k)
                    && gateway.command(k).is_none()
                {
                    // Previously verified and retired past retention window.
                } else if gateway.command(k) != Some(&rest.command) {
                    return Err(Error::Identity);
                } else {
                    self.verified_gateway_intents.borrow_mut().insert(k);
                }
            }
        }
        // Every already-applied key in this input stream must have its exact input.
        // This also catches pairing a truncated inbox with a newer account WAL.
        for (stream, sequence, event) in gateway.ingress_events() {
            if stream == self.config.physical.stream_epoch {
                if sequence == 0 || sequence > self.position() {
                    return Err(Error::Identity);
                }
                if sequence <= self.evicted_prefix as u64 {
                    continue;
                }
                let index = self.local_index(sequence).ok_or(Error::Identity)?;
                if self.effect(index) != Some(event) {
                    return Err(Error::Identity);
                }
            }
        }
        for (stream, sequence, event) in gateway.ingress_account_inputs() {
            if stream == self.config.physical.stream_epoch {
                if sequence == 0 || sequence > self.position() {
                    return Err(Error::Identity);
                }
                if sequence <= self.evicted_prefix as u64 {
                    continue;
                }
                let index = self.local_index(sequence).ok_or(Error::Identity)?;
                if self.account_effect(index) != Some(event) {
                    return Err(Error::Identity);
                }
            }
        }
        for binding in gateway.account().income_baselines() {
            self.income_coverage_object(binding)?;
        }
        for binding in gateway.account().income_associations() {
            self.income_association_objects(binding)?;
        }
        if record_count > 0 {
            self.min_gateway_record_count
                .set(self.min_gateway_record_count.get().max(record_count));
            self.min_gateway_ledger_seq
                .set(self.min_gateway_ledger_seq.get().max(ledger_seq));
            self.last_paired_state.set(Some(paired_key));
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
    fn is_intent_in_flight<B: Backend, A: Authority, E: Encoder>(
        gateway: &AccountGateway<B, A, E>,
        key: IntentKey,
    ) -> bool {
        gateway.account().is_intent_active(key)
            || matches!(
                gateway.status(key),
                Some(
                    Status::AwaitingCommit
                        | Status::Queued
                        | Status::PartiallyAccepted
                        | Status::Unknown
                )
            )
    }
    /// Evicts retired `OrderRegistry` entries, WS binding slots, and (when unfrozen and outside
    /// reconciliation/income proofs) expired prefix records past `retention_window_ns`.
    pub fn evict_expired<B: Backend, A: Authority, E: Encoder>(
        &mut self,
        gateway: &mut AccountGateway<B, A, E>,
        now_mono_ns: u64,
    ) -> usize {
        if now_mono_ns == 0 || self.stopped || self.cursor != self.records.len() {
            return 0;
        }
        if self.paired(gateway).is_err() {
            return 0;
        }
        self.observe_mono_ns(now_mono_ns);
        if self.retention_window_overridden
            && gateway.retention_window_ns() != self.retention_window_ns
        {
            gateway.set_retention_window_ns(self.retention_window_ns);
        } else if !self.retention_window_overridden
            && gateway.retention_window_ns() != self.retention_window_ns
        {
            self.retention_window_ns = gateway.retention_window_ns();
        }
        let Some(window_ns) = self.retention_window_ns else {
            return 0;
        };
        let mut evicted = 0usize;
        let mut to_remove_reg = Vec::new();
        for (key, _) in &self.registry {
            if Self::is_intent_in_flight(gateway, *key) {
                continue;
            }
            let expired = if let Some(retired_at) = gateway.account().intent_retired_mono_ns(*key) {
                now_mono_ns >= retired_at.saturating_add(window_ns)
            } else if gateway.account().admission(*key).is_none() {
                let registered_at = self
                    .registered_mono_ns
                    .get(key)
                    .copied()
                    .unwrap_or(now_mono_ns);
                registered_at > 0 && now_mono_ns >= registered_at.saturating_add(window_ns)
            } else {
                false
            };
            if expired {
                to_remove_reg.push(*key);
            }
        }
        for key in to_remove_reg {
            self.registry.remove(&key);
            self.registered_mono_ns.remove(&key);
            self.evicted_intents.insert(key);
            evicted += 1;
        }
        gateway.evict_retired_entries(now_mono_ns);
        for record in &self.records {
            match record {
                Record::Rest(r) => {
                    let k = intent(&r.command);
                    if !Self::is_intent_in_flight(gateway, k) && gateway.command(k).is_none() {
                        self.evicted_intents.insert(k);
                    }
                }
                Record::WsBinding(b) => {
                    let k = intent(&b.command);
                    if !Self::is_intent_in_flight(gateway, k)
                        && gateway.admission_origin(k).is_none()
                    {
                        self.evicted_intents.insert(k);
                    }
                }
                _ => {}
            }
        }
        let can_trim_prefix = !gateway.account().frozen()
            && self.closure_store.is_none()
            && self.config.income_baseline_authority.is_none()
            && self.config.income_association_authority.is_none()
            && gateway.account().income_baselines().next().is_none()
            && gateway.account().income_associations().next().is_none();
        if can_trim_prefix && self.records.len() > 1 {
            let mut cut = 0usize;
            while cut + 1 < self.records.len() {
                let ts = self.record_mono_ns[cut];
                if ts == 0 || now_mono_ns < ts.saturating_add(window_ns) {
                    break;
                }
                if let Record::WsBinding(b) = &self.records[cut] {
                    let k = intent(&b.command);
                    let awaiting_ws = matches!(
                        gateway.status(k),
                        Some(
                            Status::AwaitingCommit
                                | Status::Queued
                                | Status::PartiallyAccepted
                                | Status::Unknown
                        )
                    );
                    if awaiting_ws {
                        break;
                    }
                }
                cut += 1;
            }
            while cut > 0
                && cut < self.records.len()
                && matches!(
                    self.records[cut],
                    Record::RestEffect { .. }
                        | Record::WsEffect { .. }
                        | Record::IncomeEffect { .. }
                )
            {
                cut -= 1;
            }
            if cut > 0 {
                let freed_bytes: usize = self.record_sizes[..cut].iter().sum();
                self.records.drain(..cut);
                self.record_sizes.drain(..cut);
                self.record_mono_ns.drain(..cut);
                self.evicted_prefix += cut;
                self.cursor -= cut;
                self.retained = self.retained.saturating_sub(freed_bytes);
                self.last_paired_state.set(None);
                evicted += cut;
            }
        }
        let frontier = gateway.history_frontier();
        let record_count = frontier.record_count();
        if record_count > 0 {
            self.min_gateway_record_count
                .set(self.min_gateway_record_count.get().max(record_count));
            self.min_gateway_ledger_seq.set(
                self.min_gateway_ledger_seq
                    .get()
                    .max(gateway.account().ledger_sequence()),
            );
            self.last_paired_state
                .set(Some((frontier.digest(), record_count, self.position())));
        }
        evicted
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
        self.evict_expired(gateway, evaluated_at);
        match command.command().body {
            v::CommandBody::New(_)
            | v::CommandBody::CancelReplace { .. }
            | v::CommandBody::SpotList(_)
            | v::CommandBody::Algo(_) => {
                if let Err(error) = self.register_command(command.clone(), ids) {
                    if self.stopped {
                        gateway.freeze_account();
                    }
                    return Err(error);
                }
            }
            v::CommandBody::Cancel(_)
            | v::CommandBody::Query(_)
            | v::CommandBody::CancelList(_)
            | v::CommandBody::CancelAlgo(_)
            | v::CommandBody::AmendKeepPriority { .. }
                if ids.is_empty() => {}
            _ => return Err(Error::Unsupported),
        }
        gateway
            .submit(command, evaluated_at, deadline)
            .map_err(Error::Gateway)
    }
    pub fn submit_on_slot<B: Backend, A: Authority, E: Encoder>(
        &mut self,
        gateway: &mut AccountGateway<B, A, E>,
        conn_id: u16,
        command: CanonicalCommand,
        ids: &[[u8; 16]],
        evaluated_at: u64,
        deadline: Instant,
    ) -> Result<(Status, u64)> {
        self.ready(gateway)?;
        self.evict_expired(gateway, evaluated_at);
        match command.command().body {
            v::CommandBody::New(_)
            | v::CommandBody::CancelReplace { .. }
            | v::CommandBody::SpotList(_)
            | v::CommandBody::Algo(_) => {
                if let Err(error) = self.register_command(command.clone(), ids) {
                    if self.stopped {
                        gateway.freeze_account();
                    }
                    return Err(error);
                }
            }
            v::CommandBody::Cancel(_)
            | v::CommandBody::Query(_)
            | v::CommandBody::CancelList(_)
            | v::CommandBody::CancelAlgo(_)
            | v::CommandBody::AmendKeepPriority { .. }
                if ids.is_empty() => {}
            _ => return Err(Error::Unsupported),
        }
        gateway
            .submit_on_slot(conn_id, command, evaluated_at, deadline)
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
        self.evict_expired(gateway, event.received_mono_ns());
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
        let decision = match self.decide(gateway, event) {
            Decision::Bound(e) => {
                if self.registry.check_learn(&e).is_ok() {
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
                if let Err(error) = self.registry.learn(&event) {
                    self.stopped = true;
                    return Err(error);
                }
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
            Decision::AccountBound(event) => {
                match gateway.record_account_input(key.stream, key.sequence, *event) {
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
        match event.body() {
            tickwire_binance::events::PrivateBody::List(list) => {
                let Some(reg) = self.registry.by_list_client_id(&list.list_client_id) else {
                    return Decision::Quarantine(QuarantineReason::UnknownOrder);
                };
                let Ok(bound) = event.bind_structure(&reg.command, &reg.orders) else {
                    return Decision::Quarantine(QuarantineReason::UnknownOrder);
                };
                if gateway.command(intent(&reg.command)) != Some(&reg.command) {
                    return Decision::Quarantine(QuarantineReason::UnadmittedOrder);
                }
                if gateway.account().clone().apply_event(&bound).is_err() {
                    return Decision::Quarantine(QuarantineReason::AccountRejected);
                }
                return Decision::Bound(Box::new(bound));
            }
            tickwire_binance::events::PrivateBody::Algo(algo) => {
                let Some(reg) = self.registry.by_algo_client_id(&algo.client_algo_id) else {
                    return Decision::Quarantine(QuarantineReason::UnknownOrder);
                };
                let Ok(bound) = event.bind_structure(&reg.command, &reg.orders) else {
                    return Decision::Quarantine(QuarantineReason::UnknownOrder);
                };
                if gateway.command(intent(&reg.command)) != Some(&reg.command) {
                    return Decision::Quarantine(QuarantineReason::UnadmittedOrder);
                }
                if gateway.account().clone().apply_event(&bound).is_err() {
                    return Decision::Quarantine(QuarantineReason::AccountRejected);
                }
                return Decision::Bound(Box::new(bound));
            }
            tickwire_binance::events::PrivateBody::UsdmAccount { .. } => {
                let Ok(input) = account_facts::bind_observation(
                    &self.config.baseline,
                    event.session().identity(),
                    event.body(),
                    event.event_time_ns(),
                    event.transaction_time_ns(),
                ) else {
                    return Decision::Quarantine(QuarantineReason::BindingConflict);
                };
                if gateway
                    .account()
                    .clone()
                    .apply_account_input(&input)
                    .is_err()
                {
                    return Decision::Quarantine(QuarantineReason::AccountRejected);
                }
                return Decision::AccountBound(Box::new(input));
            }
            _ => {}
        }
        let tickwire_binance::events::PrivateBody::Order(order) = event.body() else {
            return Decision::Quarantine(QuarantineReason::AccountReconciliation);
        };
        let candidates = self.registry.lookup_private_order(
            &order.symbol,
            order.order_id,
            &order.client_id,
            order.original_client_id.as_deref(),
        );
        let (reg, identity) = match candidates.as_slice() {
            [] => return Decision::Quarantine(QuarantineReason::UnknownOrder),
            [one] => *one,
            _ => return Decision::Quarantine(QuarantineReason::AmbiguousOrder),
        };
        if gateway.command(intent(&reg.command)) != Some(&reg.command) {
            return Decision::Quarantine(QuarantineReason::UnadmittedOrder);
        }
        let Ok(canonical) = event.bind_order(&reg.command, identity, &self.baseline_assets) else {
            return Decision::Quarantine(QuarantineReason::BindingConflict);
        };
        let canonical = if order.execution_type == "REPLACED" {
            let Ok(bound) =
                amendment::bind_private(gateway.account(), &reg.command, canonical, order)
            else {
                return Decision::Quarantine(QuarantineReason::BindingConflict);
            };
            let v::VenueEventBody::Amendment(a) = &bound.event().body else {
                unreachable!()
            };
            if gateway
                .command(IntentKey {
                    epoch: a.request_intent_exec_epoch,
                    intent_id: a.request_intent_id,
                })
                .map(CanonicalCommand::digest)
                != Some(a.request_digest)
            {
                return Decision::Quarantine(QuarantineReason::UnadmittedOrder);
            }
            bound
        } else {
            canonical
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
            let seq = self.seq_at(self.cursor);
            if let Some(event) = self.effect(self.cursor)
                && let Err(error) = gateway.record_ingress_event(
                    self.config.physical.stream_epoch,
                    seq,
                    event.clone(),
                )
            {
                self.stopped = true;
                return Err(Error::Gateway(error));
            }
            if let Some(input) = self.account_effect(self.cursor)
                && let Err(error) = gateway.record_account_input(
                    self.config.physical.stream_epoch,
                    seq,
                    input.clone(),
                )
            {
                self.stopped = true;
                return Err(Error::Gateway(error));
            }
            if let Record::IncomeImport(record) = &self.records[self.cursor] {
                let binding = self.import_binding(record, seq)?;
                if gateway.account().income_baseline(binding.wallet_id) != Some(&binding) {
                    gateway
                        .install_income_baseline(binding, &mut self.activation_verifier())
                        .map_err(Error::Gateway)?;
                }
            }
            if let Record::IncomeAssociation(record) = &self.records[self.cursor] {
                let binding = record
                    .proposal
                    .manifest
                    .binding(record.proposal.digest(), record.approval.digest())?;
                gateway
                    .associate_income_observations(binding, &mut self.activation_verifier())
                    .map_err(Error::Gateway)?;
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
