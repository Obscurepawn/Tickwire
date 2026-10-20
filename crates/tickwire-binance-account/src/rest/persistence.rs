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
        self.ready(gateway)?;
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
        let mut record = RestRecord {
            command,
            query: None,
            order_query: Some(query),
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
    pub(crate) fn commit_rest<B: Backend, A: Authority, E: Encoder>(
        &mut self,
        gateway: &mut AccountGateway<B, A, E>,
        record: RestRecord,
    ) -> Result<RestOutcome> {
        if record.disposition != RestDisposition::Observed {
            gateway.freeze_account();
        }
        self.validate_rest(&record)?;
        let request = intent(&record.command);
        let sequence = self.position() + 1;
        let count = record.events.len();
        let disposition = record.disposition;
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
        self.records.extend(records);
        self.retained = retained;
        for (index, event) in record.events.into_iter().enumerate() {
            registry::learn(&mut self.registry, &event)?;
            if let Err(error) = gateway.record_ingress_event(
                self.config.physical.stream_epoch,
                sequence + index as u64 + 1,
                event,
            ) {
                self.stopped = true;
                return Err(Error::Gateway(error));
            }
        }
        self.cursor = self.records.len();
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
            Record::RestEffect { response, index } => match self
                .records
                .get(usize::try_from(*response).ok()?.checked_sub(1)?)?
            {
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
                        sequence: index as u64 + 1,
                    },
                    command: &record.command,
                    query: record.query,
                    order_query: record.order_query,
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
        if record.query.is_some() && record.order_query.is_some() {
            return Err(Error::Identity);
        }
        if record
            .query
            .is_some_and(|q| q.intent != intent(&record.command))
            || record
                .order_query
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
        if record.events.len() > if record.query.is_some() { 16 } else { 3 }
            || record.disposition == RestDisposition::NoBytes
            || (matches!(
                record.disposition,
                RestDisposition::Unknown | RestDisposition::Rejected
            ) && !record.events.is_empty())
        {
            return Err(Error::Identity);
        }
        let meta = SourceMeta {
            epoch: record.epoch,
            subscription: None,
            micros: false,
            received: record.received,
        };
        for event in &record.events {
            self.check_event(&meta, event)?;
            let id = registry::order(event)?;
            let key = IntentKey {
                epoch: id.original_intent_exec_epoch,
                intent_id: id.original_intent_id,
            };
            let creating = &self.registry.get(&key).ok_or(Error::Identity)?.command;
            if record
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
