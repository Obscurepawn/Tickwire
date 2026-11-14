use super::*;
impl<S: Store> AccountIngress<S> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn persist_rest<B: Backend, A: Authority, E: Encoder>(
        &mut self,
        gateway: &mut AccountGateway<B, A, E>,
        command: CanonicalCommand,
        epoch: u64,
        received: u64,
        status: u16,
        raw: Vec<u8>,
        body: Option<&[u8]>,
    ) -> Result<RestOutcome> {
        self.persist_rest_with_options(gateway, command, epoch, received, status, raw, body, false)
    }
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn persist_rest_with_options<B: Backend, A: Authority, E: Encoder>(
        &mut self,
        gateway: &mut AccountGateway<B, A, E>,
        command: CanonicalCommand,
        epoch: u64,
        received: u64,
        status: u16,
        raw: Vec<u8>,
        body: Option<&[u8]>,
        isolated_ip_rate_limit: bool,
    ) -> Result<RestOutcome> {
        self.ready(gateway)?;
        self.evict_expired(gateway, received);
        let request = intent(&command);
        if epoch == 0
            || raw.len() > self.config.max_raw_bytes
            || gateway.command(request) != Some(&command)
        {
            gateway.freeze_account();
            return Err(Error::Identity);
        }
        let mut record = RestRecord {
            command,
            query: None,
            order_query: None,
            amendment_query: None,
            structure_query: None,
            structure_domain: None,
            epoch,
            received,
            status,
            raw,
            disposition: RestDisposition::Unknown,
            events: Vec::new(),
        };
        if let Some(body) = body
            && let Ok((disposition, events)) = binding::decide(self, gateway, &record, body)
        {
            record.disposition = disposition;
            record.events = events;
        }
        self.commit_rest_with_options(gateway, record, isolated_ip_rate_limit)
    }
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn persist_structure_query<B: Backend, A: Authority, E: Encoder>(
        &mut self,
        gateway: &mut AccountGateway<B, A, E>,
        query: crate::reconciliation::structures::StructureQuery,
        epoch: u64,
        domain: tickwire_clock::ClockDomain,
        received: u64,
        status: u16,
        raw: Vec<u8>,
        body: Option<&[u8]>,
    ) -> Result<RestOutcome> {
        self.ready(gateway)?;
        query.validate()?;
        gateway.freeze_account();
        let command = self
            .registry
            .get(&query.intent)
            .ok_or(Error::Identity)?
            .command
            .clone();
        if gateway.command(query.intent) != Some(&command) {
            return Err(Error::Identity);
        }
        let parent = match &command.command().body {
            v::CommandBody::SpotList(list) => list.list_id,
            v::CommandBody::Algo(algo) => algo.parent_order_id,
            _ => return Err(Error::Identity),
        };
        if parent != query.structure_id {
            return Err(Error::Identity);
        }
        let mut record = RestRecord {
            command,
            query: None,
            order_query: None,
            amendment_query: None,
            structure_query: Some(query),
            structure_domain: Some(domain),
            epoch,
            received,
            status,
            raw,
            disposition: RestDisposition::Unknown,
            events: Vec::new(),
        };
        if let Some(body) = body
            && let Ok((disposition, events)) = binding::decide(self, gateway, &record, body)
            && events.len() == 1
        {
            record.disposition = disposition;
            record.events = events;
        }
        self.commit_rest(gateway, record)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn persist_order_query<B: Backend, A: Authority, E: Encoder>(
        &mut self,
        gateway: &mut AccountGateway<B, A, E>,
        query: crate::reconciliation::orders::OrderQuery,
        epoch: u64,
        received: u64,
        status: u16,
        raw: Vec<u8>,
        body: Option<&[u8]>,
    ) -> Result<RestOutcome> {
        self.ready(gateway)?;
        query.validate()?;
        if !gateway.per_order_doubt_isolation() {
            gateway.freeze_account();
        }
        let command = self
            .registry
            .get(&query.intent)
            .ok_or(Error::Identity)?
            .command
            .clone();
        if gateway.command(query.intent) != Some(&command) {
            return Err(Error::Identity);
        }
        let mut record = RestRecord {
            command,
            query: None,
            order_query: Some(query),
            amendment_query: None,
            structure_query: None,
            structure_domain: None,
            epoch,
            received,
            status,
            raw,
            disposition: RestDisposition::Unknown,
            events: Vec::new(),
        };
        if let Some(body) = body
            && let Ok((disposition, events)) = binding::decide(self, gateway, &record, body)
            && events.len() == 1
        {
            record.disposition = disposition;
            record.events = events;
        }
        self.commit_rest(gateway, record)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn persist_uncreated_order_query<B: Backend, A: Authority, E: Encoder>(
        &mut self,
        gateway: &mut AccountGateway<B, A, E>,
        query: crate::reconciliation::orders::OrderQuery,
        epoch: u64,
        received: u64,
        status: u16,
        raw: Vec<u8>,
        body: &[u8],
    ) -> Result<RestOutcome> {
        self.ready(gateway)?;
        query.validate()?;
        let command = self
            .registry
            .get(&query.intent)
            .ok_or(Error::Identity)?
            .command
            .clone();
        if gateway.command(query.intent) != Some(&command) {
            return Err(Error::Identity);
        }
        let mut record = RestRecord {
            command,
            query: None,
            order_query: Some(query),
            amendment_query: None,
            structure_query: None,
            structure_domain: None,
            epoch,
            received,
            status,
            raw,
            disposition: RestDisposition::Unknown,
            events: Vec::new(),
        };
        let decoded = tickwire_binance::response::decode_rest(self.market()?, status, body)
            .map_err(|_| Error::Encoding)?;
        let api_err = decoded.error.as_ref().ok_or(Error::Encoding)?;
        if decoded.failure != Some(tickwire_binance::response::FailureKind::NotFound)
            || api_err.code != -2013
        {
            return Err(Error::Identity);
        }
        let event = binding::order_rejection(
            self,
            &record,
            b"tickwire.rest.observation.v1",
            api_err.code,
            &api_err.msg,
        )?;
        gateway
            .account()
            .clone()
            .apply_event(&event)
            .map_err(|_| Error::Identity)?;
        record.disposition = RestDisposition::Rejected;
        record.events = vec![event];
        self.commit_rest(gateway, record)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn persist_per_order_trade_query<B: Backend, A: Authority, E: Encoder>(
        &mut self,
        gateway: &mut AccountGateway<B, A, E>,
        query: crate::reconciliation::TradeQuery,
        epoch: u64,
        received: u64,
        status: u16,
        raw: Vec<u8>,
        body: Option<&[u8]>,
    ) -> Result<(RestOutcome, usize, Option<u64>)> {
        self.ready(gateway)?;
        query.validate()?;
        let reg = self.registry.get(&query.intent).ok_or(Error::Identity)?;
        let identity = reg
            .orders
            .iter()
            .find(|id| id.order_id == query.order_id)
            .ok_or(Error::Identity)?
            .clone();
        if gateway.command(query.intent) != Some(&reg.command) || identity.venue_order_id.is_none()
        {
            return Err(Error::Identity);
        }
        let mut record = RestRecord {
            command: reg.command.clone(),
            query: Some(query),
            order_query: None,
            amendment_query: None,
            structure_query: None,
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
        {
            match crate::reconciliation::bind_trade_page(
                &reg.command,
                &identity,
                &self.config.baseline.snapshot().assets,
                query,
                epoch,
                received,
                body,
            ) {
                Ok(events) => {
                    let mut candidate = gateway.account().clone();
                    let mut valid = true;
                    for event in &events {
                        if candidate.apply_event(event).is_err() {
                            valid = false;
                            break;
                        }
                    }
                    if valid {
                        record.disposition = RestDisposition::NeedsReconciliation;
                        record.events = events;
                    } else {
                        gateway.freeze_account();
                    }
                }
                Err(_) => {
                    gateway.freeze_account();
                }
            }
        }
        let returned = record.events.len();
        let next_from_id = record.events.last().and_then(|e| match &e.event().body {
            v::VenueEventBody::Fill(fill) => {
                fill.trade_id.as_str().parse::<u64>().ok()?.checked_add(1)
            }
            _ => None,
        });
        let outcome = self.commit_rest(gateway, record)?;
        Ok((outcome, returned, next_from_id))
    }

    pub(crate) fn commit_rest<B: Backend, A: Authority, E: Encoder>(
        &mut self,
        gateway: &mut AccountGateway<B, A, E>,
        record: RestRecord,
    ) -> Result<RestOutcome> {
        self.commit_rest_with_options(gateway, record, false)
    }

    pub(crate) fn commit_rest_with_options<B: Backend, A: Authority, E: Encoder>(
        &mut self,
        gateway: &mut AccountGateway<B, A, E>,
        record: RestRecord,
        isolated_ip_rate_limit: bool,
    ) -> Result<RestOutcome> {
        let isolation = gateway.per_order_doubt_isolation();
        if !isolation {
            if record.disposition != RestDisposition::Observed {
                gateway.freeze_account();
            }
        } else if record.disposition != RestDisposition::Observed
            && is_category_b_rest_hazard_with_options(self, &record, isolated_ip_rate_limit)
        {
            gateway.freeze_account();
        }
        self.validate_rest(&record)?;
        let request = intent(&record.command);
        let sequence = self.position() + 1;
        let count = record.events.len();
        let disposition = record.disposition;
        let is_direct_command = record.query.is_none()
            && record.order_query.is_none()
            && record.amendment_query.is_none()
            && record.structure_query.is_none();
        let cmd_clone = record.command.clone();
        let events_snapshot = record.events.clone();
        let mut records = vec![Record::Rest(record.clone())];
        for index in 0..count {
            records.push(Record::RestEffect {
                response: sequence,
                index: index as u8,
            });
        }
        let encoded: Vec<Vec<u8>> = records.iter().map(codec::encode).collect::<Result<_>>()?;
        let retained = encoded.iter().try_fold(self.retained, |n, b| {
            n.checked_add(b.len()).ok_or(Error::Capacity)
        })?;
        if self.records.len() + records.len() > self.config.max_records
            || retained > self.config.max_retained_bytes
        {
            self.stopped = true;
            return Err(Error::Capacity);
        }
        let bytes: Vec<&[u8]> = encoded.iter().map(Vec::as_slice).collect();
        if let Err(error) = self.journal.append_batch(self.config.writer_epoch, &bytes) {
            self.stopped = true;
            gateway.freeze_account();
            return Err(error.into());
        }
        let received_mono = record.received;
        for (rec, enc) in records.into_iter().zip(encoded) {
            self.push_encoded_record(rec, enc.len(), received_mono);
        }
        self.retained = retained;
        for (index, event) in record.events.into_iter().enumerate() {
            registry::learn(&mut self.registry, &event)?;
            if let Err(error) = gateway.record_ingress_event(
                self.config.physical.stream_epoch,
                sequence + index as u64 + 1,
                event,
            ) {
                self.stopped = true;
                gateway.freeze_account();
                return Err(Error::Gateway(error));
            }
        }
        self.cursor = self.records.len();
        if isolation && is_direct_command && !gateway.account().frozen() {
            apply_per_order_isolation(gateway, request, &cmd_clone, disposition, &events_snapshot);
        }
        Ok(RestOutcome {
            request,
            input: Some(InputKey {
                stream: self.config.physical.stream_epoch,
                sequence,
            }),
            disposition,
            applied_events: count,
        })
    }
    pub(crate) fn effect(&self, index: usize) -> Option<&CanonicalEvent> {
        match self.records.get(index)? {
            Record::Event(_, _, Decision::Bound(event)) => Some(event),
            Record::WsEffect { response, index } => match self.record_at_seq(*response)? {
                Record::WsResponse(record) => record.events.get(*index as usize),
                _ => None,
            },
            Record::RestEffect { response, index } => match self.record_at_seq(*response)? {
                Record::Rest(record) => record.events.get(*index as usize),
                _ => None,
            },
            _ => None,
        }
    }
    pub fn stored_rest(&self) -> impl Iterator<Item = StoredRest<'_>> {
        self.records
            .iter()
            .enumerate()
            .filter_map(|(index, record)| {
                let Record::Rest(record) = record else {
                    return None;
                };
                Some(StoredRest {
                    input: InputKey {
                        stream: self.config.physical.stream_epoch,
                        sequence: self.seq_at(index),
                    },
                    command: &record.command,
                    query: record.query,
                    order_query: record.order_query,
                    structure_query: record.structure_query,
                    amendment_query: record.amendment_query,
                    structure_domain: record.structure_domain,
                    connection_epoch: record.epoch,
                    received_mono_ns: record.received,
                    status: record.status,
                    raw: &record.raw,
                    disposition: record.disposition,
                    events: &record.events,
                })
            })
    }
    pub(crate) fn validate_rest(&self, record: &RestRecord) -> Result<()> {
        if record.structure_query.is_some() != record.structure_domain.is_some() {
            return Err(Error::Identity);
        }
        if [
            record.query.is_some(),
            record.order_query.is_some(),
            record.structure_query.is_some(),
            record.amendment_query.is_some(),
        ]
        .iter()
        .filter(|v| **v)
        .count()
            > 1
        {
            return Err(Error::Identity);
        }
        if record
            .structure_query
            .is_some_and(|q| q.intent != intent(&record.command))
            || record
                .query
                .is_some_and(|q| q.intent != intent(&record.command))
            || record
                .order_query
                .is_some_and(|q| q.intent != intent(&record.command))
        {
            return Err(Error::Identity);
        }
        if record
            .amendment_query
            .is_some_and(|q| q.intent != intent(&record.command))
        {
            return Err(Error::Identity);
        }
        let c = &record.command.command().context;
        let scope = &self.config.baseline.snapshot().scope;
        if record.epoch == 0
            || record.raw.len() > self.config.max_raw_bytes
            || c.request.account != scope.account
            || c.request.risk_domain_id != scope.risk_domain_id
            || c.request.venue != scope.venue
            || c.request.environment != scope.environment
            || c.request.product != product(&self.config)?
            || c.request.intent_exec_epoch > self.config.writer_epoch
        {
            return Err(Error::Identity);
        }
        if record.events.len()
            > if record.query.is_some() || record.amendment_query.is_some() {
                16
            } else {
                4
            }
            || record.disposition == RestDisposition::NoBytes
            || (record.disposition == RestDisposition::Unknown && !record.events.is_empty())
            || (record.disposition == RestDisposition::Rejected
                && !record.events.is_empty()
                && !(record.events.len() == 1
                    && matches!(
                        record.events[0].event().body,
                        v::VenueEventBody::AmendmentRejected(_)
                            | v::VenueEventBody::Rejected { .. }
                    )))
        {
            return Err(Error::Identity);
        }
        let meta = SourceMeta {
            epoch: record.epoch,
            subscription: None,
            micros: false,
            received: record.received,
        };
        if record.amendment_query.is_some() {
            super::amend_history::verify_raw(self, record)?;
        }
        for event in &record.events {
            self.check_event(&meta, event)?;
            if matches!(event.event().body, v::VenueEventBody::Rejected { .. }) {
                if record.disposition != RestDisposition::Rejected
                    || record.events.len() != 1
                    || record.query.is_some()
                    || record.structure_query.is_some()
                    || record.amendment_query.is_some()
                {
                    return Err(Error::Identity);
                }
                binding::verify_rejection_raw(self, record, event)?;
                self.registry.check_learn(event)?;
                continue;
            }
            if record.amendment_query.is_none()
                && matches!(
                    event.event().body,
                    v::VenueEventBody::Amendment(_) | v::VenueEventBody::AmendmentRejected(_)
                )
            {
                super::amendment::verify_raw(self, record, event)?;
            }
            if let v::VenueEventBody::Structure(s) = &event.event().body {
                super::structure::verify_raw(self, record, event)?;
                let key = IntentKey {
                    epoch: s.parent.original_intent_exec_epoch,
                    intent_id: s.parent.original_intent_id,
                };
                let creating = &self.registry.get(&key).ok_or(Error::Identity)?.command;
                if record.query.is_some()
                    || record.order_query.is_some()
                    || record
                        .structure_query
                        .is_some_and(|q| q.structure_id != s.parent.structure_id || q.intent != key)
                    || !super::structure::allowed(&record.command, creating, &s.parent)
                {
                    return Err(Error::Identity);
                }
                continue;
            }
            if record.structure_query.is_some() {
                return Err(Error::Identity);
            }
            let id = registry::order(event)?;
            let key = IntentKey {
                epoch: id.original_intent_exec_epoch,
                intent_id: id.original_intent_id,
            };
            let creating = &self.registry.get(&key).ok_or(Error::Identity)?.command;
            if record
                .amendment_query
                .is_some_and(|q| q.order_id != id.order_id || q.intent != intent(creating))
                || record
                    .order_query
                    .is_some_and(|q| q.order_id != id.order_id || q.intent != intent(creating))
                || record
                    .query
                    .is_some_and(|q| q.order_id != id.order_id || q.intent != intent(creating))
                || (record.query.is_none() && !binding::allowed(&record.command, creating, id))
            {
                return Err(Error::Identity);
            }
        }
        Ok(())
    }
}

pub(crate) fn is_category_b_rest_hazard_with_options<S: Store>(
    input: &AccountIngress<S>,
    record: &RestRecord,
    isolated_ip_rate_limit: bool,
) -> bool {
    if record.structure_query.is_some() || record.amendment_query.is_some() {
        return true;
    }
    if !matches!(
        record.command.command().body,
        v::CommandBody::New(_)
            | v::CommandBody::Cancel(_)
            | v::CommandBody::AmendKeepPriority { .. }
            | v::CommandBody::Query(_)
    ) {
        return true;
    }
    if record.events.iter().any(|e| {
        matches!(
            &e.event().body,
            v::VenueEventBody::Order(o) if o.status == v::OrderStatus::Unknown
        )
    }) {
        return true;
    }
    if record.status == 0 {
        return false;
    }
    if matches!(record.status, 401 | 403 | 418) {
        return true;
    }
    if record.status == 429 && !isolated_ip_rate_limit {
        return true;
    }
    if (200..300).contains(&record.status) && record.disposition == RestDisposition::Unknown {
        return true;
    }
    let Ok(market) = input.market() else {
        return true;
    };
    let Ok(mut decoder) = ResponseDecoder::new(input.config.max_raw_bytes, 256) else {
        return true;
    };
    if decoder.push(&record.raw).is_ok() {
        decoder.finish();
        if let Ok(Some(http_resp)) = decoder.next_response()
            && decoder.take_buffered().is_empty()
            && let Ok(decoded) =
                tickwire_binance::response::decode_rest(market, http_resp.status, &http_resp.body)
        {
            if decoded
                .error
                .as_ref()
                .is_some_and(|e| matches!(e.code, -1021 | -1002 | -2014 | -2015 | -1022))
            {
                return true;
            }
            if matches!(
                decoded.failure,
                Some(
                    tickwire_binance::response::FailureKind::Banned
                        | tickwire_binance::response::FailureKind::Authentication
                        | tickwire_binance::response::FailureKind::ClockSkew
                        | tickwire_binance::response::FailureKind::Partial
                )
            ) {
                return true;
            }
            if decoded.failure == Some(tickwire_binance::response::FailureKind::RateLimited)
                && !isolated_ip_rate_limit
            {
                return true;
            }
            if decoded.failure == Some(tickwire_binance::response::FailureKind::Rejected)
                && record.order_query.is_none()
                && record.query.is_none()
                && matches!(record.command.command().body, v::CommandBody::New(_))
                && record.events.is_empty()
            {
                return true;
            }
        }
    }
    false
}

pub(crate) fn apply_per_order_isolation<B: Backend, A: Authority, E: Encoder>(
    gateway: &mut AccountGateway<B, A, E>,
    request: IntentKey,
    command: &CanonicalCommand,
    disposition: RestDisposition,
    events: &[CanonicalEvent],
) {
    match disposition {
        RestDisposition::Observed | RestDisposition::Rejected | RestDisposition::NoBytes => {}
        RestDisposition::Unknown => {
            let reason = match &command.command().body {
                v::CommandBody::Cancel(_) => tickwire_account::DoubtReason::CancelAmbiguous,
                v::CommandBody::AmendKeepPriority { .. } => {
                    tickwire_account::DoubtReason::AmendAmbiguous
                }
                v::CommandBody::New(_) | v::CommandBody::Query(_) => {
                    tickwire_account::DoubtReason::PlaceAmbiguous
                }
                _ => {
                    gateway.freeze_account();
                    return;
                }
            };
            if gateway.isolate_intent_in_doubt(request, reason).is_err() {
                gateway.freeze_account();
            }
        }
        RestDisposition::NeedsReconciliation => {
            let mut order_keys = Vec::new();
            for event in events {
                if let Ok(id) = registry::order(event)
                    && let Some(cid) = id.client_id.clone()
                {
                    order_keys.push(tickwire_account::OrderKey {
                        intent: IntentKey {
                            epoch: id.original_intent_exec_epoch,
                            intent_id: id.original_intent_id,
                        },
                        client_id: cid,
                    });
                }
            }
            if order_keys.is_empty() {
                gateway.freeze_account();
                return;
            }
            for order_key in order_keys {
                let Some(view) = gateway.account().order(&order_key) else {
                    gateway.freeze_account();
                    return;
                };
                if view.unknown {
                    gateway.freeze_account();
                    return;
                }
                if view.remaining_quantity.atoms() > 0 && !view.fee_reconciled {
                    let reported_fill_exceeds_local =
                        events.iter().any(|ev| match &ev.event().body {
                            v::VenueEventBody::Order(o) => o
                                .cumulative_quantity
                                .and_then(|q| {
                                    tickwire_account::Fixed::parse(
                                        &q.as_plain_string(),
                                        view.filled_quantity.scale(),
                                    )
                                    .ok()
                                })
                                .is_none_or(|cum| cum.atoms() > view.filled_quantity.atoms()),
                            v::VenueEventBody::Amendment(a) => a
                                .observation
                                .as_ref()
                                .and_then(|o| o.cumulative_quantity)
                                .and_then(|q| {
                                    tickwire_account::Fixed::parse(
                                        &q.as_plain_string(),
                                        view.filled_quantity.scale(),
                                    )
                                    .ok()
                                })
                                .is_none_or(|cum| cum.atoms() > view.filled_quantity.atoms()),
                            _ => false,
                        });
                    if reported_fill_exceeds_local
                        && gateway
                            .isolate_intent_in_doubt(
                                order_key.intent,
                                tickwire_account::DoubtReason::FillCatchup,
                            )
                            .is_err()
                    {
                        gateway.freeze_account();
                        return;
                    }
                }
            }
        }
    }
}
