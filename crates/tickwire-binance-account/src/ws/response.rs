use super::*;
use crate::rest::{RestDisposition, RestRecord};
use tickwire_account_gateway::{AccountGateway, Authority, Encoder};
use tickwire_account_journal::Backend;

pub(super) fn derive<S: Store, B: Backend, A: Authority, E: Encoder>(
    input: &AccountIngress<S>,
    gateway: &AccountGateway<B, A, E>,
    binding: &BindingRecord,
    received: u64,
    raw: &[u8],
) -> Result<(RestDisposition, Vec<CanonicalEvent>)> {
    derive_checked(input, gateway, binding, received, raw, true)
}
fn derive_checked<S: Store, B: Backend, A: Authority, E: Encoder>(
    input: &AccountIngress<S>,
    gateway: &AccountGateway<B, A, E>,
    binding: &BindingRecord,
    received: u64,
    raw: &[u8],
    apply_candidate: bool,
) -> Result<(RestDisposition, Vec<CanonicalEvent>)> {
    if raw.is_empty() || raw.len() > 32 * 1024 {
        return Err(Error::Capacity);
    }
    let decoded = tickwire_binance::response::decode_websocket(input.market()?, raw)
        .map_err(|_| Error::Encoding)?;
    let id: &str = serde_json::from_str(decoded.request_id.ok_or(Error::Identity)?.get())
        .map_err(|_| Error::Identity)?;
    if id != request_id(&binding.session.identity, binding.sequence).as_str() {
        return Err(Error::Identity);
    }
    // The common order mapper receives the decoded WS envelope directly. This
    // context is not an HTTP record and is never appended as one.
    let context = RestRecord {
        command: binding.command.clone(),
        query: None,
        order_query: None,
        amendment_query: None,
        structure_query: None,
        structure_domain: None,
        epoch: binding.session.identity.connection_epoch,
        received,
        status: decoded.status,
        raw: raw.to_vec(),
        disposition: RestDisposition::Unknown,
        events: Vec::new(),
    };
    crate::rest::binding::decide_decoded(
        input,
        gateway,
        &context,
        decoded,
        b"tickwire.ws.observation.v1",
        apply_candidate,
    )
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum ResponseDecision {
    Matched = 1,
    Unmatched = 2,
    Duplicate = 3,
    Malformed = 4,
    Incomplete = 5,
}
#[derive(Clone)]
pub(crate) struct ResponseRecord {
    pub session: SessionRecord,
    pub received: u64,
    pub binding: Option<u64>,
    pub raw: Vec<u8>,
    pub decision: ResponseDecision,
    pub disposition: RestDisposition,
    pub events: Vec<CanonicalEvent>,
}
pub struct StoredWsResponse<'a> {
    pub input: InputKey,
    pub session: &'a SessionIdentity,
    pub binding_sequence: Option<u64>,
    pub received_mono_ns: u64,
    pub raw: &'a [u8],
    pub decision: ResponseDecision,
    pub disposition: RestDisposition,
    pub events: &'a [CanonicalEvent],
}
pub struct WsResponseOutcome {
    pub input: InputKey,
    pub request: Option<IntentKey>,
    pub decision: ResponseDecision,
    pub disposition: RestDisposition,
    pub applied_events: usize,
}
impl<S: Store> AccountIngress<S> {
    /// The live session and the actual gateway output position are checked here.
    /// Raw bytes are retained even when their request ID cannot be correlated.
    pub fn persist_ws_response<B: Backend, A: Authority, E: Encoder>(
        &mut self,
        gateway: &mut AccountGateway<B, A, E>,
        session: &DurableWsSession,
        pending: Option<IntentKey>,
        received: u64,
        raw: Vec<u8>,
    ) -> Result<WsResponseOutcome> {
        self.ready(gateway)?;
        if !Rc::ptr_eq(&session.owner, &self.ws_owner.0)
            || !session.owner.alive.get()
            || session.owner.epoch.get() != session.identity.connection_epoch
            || self
                .latest_ws_session()
                .is_none_or(|r| r.identity != session.identity)
            || raw.len() > 32 * 1024
            || raw.len() > self.config.max_raw_bytes
        {
            gateway.stop_sending();
            return Err(Error::Identity);
        }
        let binding = pending
            .and_then(|key| {
                self.ws_bindings()
                    .find(|b| intent(&b.command) == key && b.session.identity == session.identity)
            })
            .or_else(|| {
                // After the current request finishes, only an already-observed
                // historical ID may classify a response. It cannot authorize send.
                let id = response_id(self.market().ok()?, &raw).ok()?;
                self.ws_bindings().find(|b| b.session.identity == session.identity
                    && request_id(&b.session.identity, b.sequence).as_str() == id
                    && self.records.iter().any(|r| matches!(r, Record::WsResponse(old)
                        if old.binding == Some(b.sequence) && old.decision == ResponseDecision::Matched)))
            })
            .cloned();
        let mut record = ResponseRecord {
            session: self.latest_ws_session().unwrap().clone(),
            binding: binding.as_ref().map(|b| b.sequence),
            received,
            raw,
            decision: ResponseDecision::Unmatched,
            disposition: RestDisposition::Unknown,
            events: Vec::new(),
        };
        if let Some(binding) = &binding {
            let decoded = tickwire_binance::response::decode_websocket(self.market()?, &record.raw);
            record.decision = match decoded {
                Err(_) => ResponseDecision::Malformed,
                Ok(ref d) if d.request_id.and_then(|r| serde_json::from_str::<&str>(r.get()).ok())
                    != Some(request_id(&binding.session.identity, binding.sequence).as_str()) => ResponseDecision::Unmatched,
                Ok(_) if self.records.iter().any(|r| matches!(r, Record::WsResponse(old)
                    if old.binding == Some(binding.sequence) && old.decision == ResponseDecision::Matched)) => ResponseDecision::Duplicate,
                Ok(_) if gateway.output_progress(intent(&binding.command)).is_none_or(|p|
                    p.total_bytes == 0 || p.accepted_bytes != p.total_bytes) => ResponseDecision::Incomplete,
                Ok(_) => match derive(self, gateway, binding, received, &record.raw) {
                    Ok((disposition, events)) => { record.disposition=disposition; record.events=events; ResponseDecision::Matched }
                    Err(_) => ResponseDecision::Malformed,
                }
            };
        }
        if record.decision != ResponseDecision::Matched {
            gateway.stop_sending();
        }
        self.commit_ws_response(gateway, record)
    }
    pub(super) fn commit_ws_response<B: Backend, A: Authority, E: Encoder>(
        &mut self,
        gateway: &mut AccountGateway<B, A, E>,
        record: ResponseRecord,
    ) -> Result<WsResponseOutcome> {
        self.validate_ws_response(&record, self.config.writer_epoch)?;
        self.verify_ws_response(gateway, &record)?;
        if record.disposition != RestDisposition::Observed {
            gateway.freeze_account();
        }
        let sequence = self.position().checked_add(1).ok_or(Error::Capacity)?;
        let count = record.events.len();
        let request = record
            .binding
            .and_then(|seq| self.ws_bindings().find(|b| b.sequence == seq))
            .map(|b| intent(&b.command));
        let mut records = vec![Record::WsResponse(record.clone())];
        for index in 0..count {
            records.push(Record::WsEffect {
                response: sequence,
                index: index as u8,
            });
        }
        let encoded = records
            .iter()
            .map(codec::encode)
            .collect::<Result<Vec<_>>>()?;
        let retained = encoded.iter().try_fold(self.retained, |n, b| {
            n.checked_add(b.len()).ok_or(Error::Capacity)
        })?;
        if self.records.len() + records.len() > self.config.max_records
            || retained > self.config.max_retained_bytes
        {
            self.stopped = true;
            gateway.freeze_account();
            return Err(Error::Capacity);
        }
        let bytes = encoded.iter().map(Vec::as_slice).collect::<Vec<_>>();
        if let Err(error) = self.journal.append_batch(self.config.writer_epoch, &bytes) {
            self.stopped = true;
            gateway.freeze_account();
            return Err(error.into());
        }
        self.records.extend(records);
        self.retained = retained;
        for (index, event) in record.events.iter().enumerate() {
            if let Err(error) = registry::learn(&mut self.registry, event) {
                self.stopped = true;
                gateway.freeze_account();
                return Err(error);
            }
            if let Err(error) = gateway.record_ingress_event(
                self.input_stream(),
                sequence + index as u64 + 1,
                event.clone(),
            ) {
                self.stopped = true;
                gateway.freeze_account();
                return Err(Error::Gateway(error));
            }
        }
        self.cursor = self.records.len();
        Ok(WsResponseOutcome {
            input: InputKey {
                stream: self.input_stream(),
                sequence,
            },
            request,
            decision: record.decision,
            disposition: record.disposition,
            applied_events: count,
        })
    }
    pub(crate) fn validate_ws_response(&self, r: &ResponseRecord, writer: u64) -> Result<()> {
        valid_session(&r.session)?;
        if self.latest_ws_session() != Some(&r.session)
            || r.session.identity.writer_epoch != writer
            || r.raw.len() > self.config.max_raw_bytes
            || r.raw.len() > 32 * 1024
            || r.events.len() > 4
            || r.disposition == RestDisposition::NoBytes
            || (r.decision != ResponseDecision::Matched
                && (r.disposition != RestDisposition::Unknown || !r.events.is_empty()))
            || (matches!(
                r.disposition,
                RestDisposition::Unknown | RestDisposition::Rejected
            ) && !r.events.is_empty())
        {
            return Err(Error::Identity);
        }
        if let Some(sequence) = r.binding {
            let b = self
                .ws_bindings()
                .find(|b| b.sequence == sequence)
                .ok_or(Error::Identity)?;
            if b.session != r.session {
                return Err(Error::Identity);
            }
            if r.decision == ResponseDecision::Duplicate
                && (response_id(self.market()?, &r.raw)?
                    != request_id(&b.session.identity, b.sequence).as_str()
                    || !self.records.iter().any(|old| {
                        matches!(old,Record::WsResponse(old)
                        if old.binding==r.binding && old.decision==ResponseDecision::Matched)
                    }))
            {
                return Err(Error::Identity);
            }
            if r.decision == ResponseDecision::Matched {
                let id = response_id(self.market()?, &r.raw)?;
                if id!=request_id(&b.session.identity,b.sequence).as_str() || self.records.iter().any(|old|
                    matches!(old,Record::WsResponse(old) if old.binding==r.binding && old.decision==ResponseDecision::Matched)) {
                    return Err(Error::Conflict);
                }
            }
        } else if matches!(
            r.decision,
            ResponseDecision::Matched | ResponseDecision::Duplicate
        ) {
            return Err(Error::Identity);
        }
        Ok(())
    }
    pub(crate) fn verify_ws_response<B: Backend, A: Authority, E: Encoder>(
        &self,
        gateway: &AccountGateway<B, A, E>,
        r: &ResponseRecord,
    ) -> Result<()> {
        if r.decision == ResponseDecision::Matched {
            let b = self
                .ws_bindings()
                .find(|b| Some(b.sequence) == r.binding)
                .ok_or(Error::Identity)?;
            self.verify_ws_binding(gateway, b)?;
            let (disposition, events) =
                derive_checked(self, gateway, b, r.received, &r.raw, false)?;
            if disposition != r.disposition || events != r.events {
                return Err(Error::Identity);
            }
        }
        Ok(())
    }
    pub fn stored_ws_responses(&self) -> impl Iterator<Item = StoredWsResponse<'_>> {
        self.records.iter().enumerate().filter_map(|(index, r)| {
            let Record::WsResponse(r) = r else {
                return None;
            };
            Some(StoredWsResponse {
                input: InputKey {
                    stream: self.input_stream(),
                    sequence: index as u64 + 1,
                },
                session: &r.session.identity,
                binding_sequence: r.binding,
                received_mono_ns: r.received,
                raw: &r.raw,
                decision: r.decision,
                disposition: r.disposition,
                events: &r.events,
            })
        })
    }
}
fn response_id(market: tickwire_binance::request::Market, raw: &[u8]) -> Result<&str> {
    let d =
        tickwire_binance::response::decode_websocket(market, raw).map_err(|_| Error::Encoding)?;
    serde_json::from_str(d.request_id.ok_or(Error::Identity)?.get()).map_err(|_| Error::Identity)
}
fn blob(out: &mut Vec<u8>, bytes: &[u8]) -> Result<()> {
    if out
        .len()
        .checked_add(bytes.len())
        .and_then(|n| n.checked_add(4))
        .is_none_or(|n| n > MAX_RECORD_BYTES)
    {
        return Err(Error::Capacity);
    }
    out.extend((bytes.len() as u32).to_le_bytes());
    out.extend(bytes);
    Ok(())
}
pub(crate) fn encode(r: &ResponseRecord) -> Result<Vec<u8>> {
    if r.events.len() > 4 || r.raw.len() > 32 * 1024 {
        return Err(Error::Capacity);
    }
    let mut out = b"TWWR0001".to_vec();
    blob(&mut out, &encode_session(&r.session)?)?;
    out.extend(r.received.to_le_bytes());
    out.extend(r.binding.unwrap_or(0).to_le_bytes());
    out.push(r.decision as u8);
    out.push(match r.disposition {
        RestDisposition::Observed => 1,
        RestDisposition::NeedsReconciliation => 2,
        RestDisposition::Rejected => 3,
        RestDisposition::Unknown => 4,
        RestDisposition::NoBytes => return Err(Error::Encoding),
    });
    blob(&mut out, &r.raw)?;
    out.push(r.events.len() as u8);
    for e in &r.events {
        blob(&mut out, e.bytes())?;
    }
    Ok(out)
}
pub(crate) fn decode(bytes: &[u8]) -> Result<ResponseRecord> {
    let mut reader = Reader::new(bytes);
    if reader.take(8)? != b"TWWR0001" {
        return Err(Error::Encoding);
    }
    let session = decode_session(reader.blob()?)?;
    let received = reader.u64()?;
    let binding = reader.u64()?;
    let decision = match reader.byte()? {
        1 => ResponseDecision::Matched,
        2 => ResponseDecision::Unmatched,
        3 => ResponseDecision::Duplicate,
        4 => ResponseDecision::Malformed,
        5 => ResponseDecision::Incomplete,
        _ => return Err(Error::Encoding),
    };
    let disposition = match reader.byte()? {
        1 => RestDisposition::Observed,
        2 => RestDisposition::NeedsReconciliation,
        3 => RestDisposition::Rejected,
        4 => RestDisposition::Unknown,
        _ => return Err(Error::Encoding),
    };
    let raw = reader.blob()?.to_vec();
    let count = reader.byte()?;
    if count > 4 {
        return Err(Error::Capacity);
    }
    let mut events = Vec::with_capacity(count as usize);
    for _ in 0..count {
        events.push(CanonicalEvent::decode(reader.blob()?).map_err(|_| Error::Encoding)?);
    }
    let out = ResponseRecord {
        session,
        received,
        binding: (binding != 0).then_some(binding),
        raw,
        decision,
        disposition,
        events,
    };
    if encode(&out)? != bytes {
        return Err(Error::Encoding);
    }
    Ok(out)
}
