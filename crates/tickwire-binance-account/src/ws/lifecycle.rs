use super::*;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum SessionEventKind {
    Upgraded = 1,
    Disconnected = 2,
    ProtocolFault = 3,
    PeerClose = 4,
}
#[derive(Clone)]
pub(crate) struct SessionEvent {
    pub session: SessionRecord,
    pub received: u64,
    pub kind: SessionEventKind,
    pub raw: Vec<u8>,
}
impl<S: Store> AccountIngress<S> {
    pub(super) fn ws_session_event(
        &mut self,
        session: &DurableWsSession,
        received: u64,
        kind: SessionEventKind,
        raw: Vec<u8>,
    ) -> Result<InputKey> {
        if self.stopped
            || self.cursor != self.records.len()
            || !Rc::ptr_eq(&session.owner, &self.ws_owner.0)
        {
            return Err(Error::Stopped);
        }
        let actual = self.latest_ws_session().ok_or(Error::Identity)?;
        if actual.identity != session.identity {
            return Err(Error::Identity);
        }
        let record = SessionEvent {
            session: actual.clone(),
            received,
            kind,
            raw,
        };
        self.validate_ws_event(&record, self.config.writer_epoch)?;
        let input = self.append(Record::WsSessionEvent(record))?;
        self.cursor = self.records.len();
        Ok(input)
    }
    pub(crate) fn validate_ws_event(&self, event: &SessionEvent, writer: u64) -> Result<()> {
        if self.latest_ws_session() != Some(&event.session)
            || event.session.identity.writer_epoch != writer
            || event.raw.len() > 32768
            || (event.kind == SessionEventKind::Upgraded && !event.raw.is_empty())
            || (event.kind == SessionEventKind::PeerClose && event.raw.len() > 125)
        {
            return Err(Error::Identity);
        }
        Ok(())
    }
}
pub(crate) fn encode(r: &SessionEvent) -> Result<Vec<u8>> {
    if r.raw.len() > 32768 {
        return Err(Error::Capacity);
    }
    let session = encode_session(&r.session)?;
    let mut out = b"TWWE0001".to_vec();
    out.extend((session.len() as u32).to_le_bytes());
    out.extend(session);
    out.extend(r.received.to_le_bytes());
    out.push(r.kind as u8);
    out.extend((r.raw.len() as u32).to_le_bytes());
    out.extend(&r.raw);
    Ok(out)
}
pub(crate) fn decode(bytes: &[u8]) -> Result<SessionEvent> {
    let mut r = Reader::new(bytes);
    if r.take(8)? != b"TWWE0001" {
        return Err(Error::Encoding);
    }
    let session = decode_session(r.blob()?)?;
    let received = r.u64()?;
    let kind = match r.byte()? {
        1 => SessionEventKind::Upgraded,
        2 => SessionEventKind::Disconnected,
        3 => SessionEventKind::ProtocolFault,
        4 => SessionEventKind::PeerClose,
        _ => return Err(Error::Encoding),
    };
    let raw = r.blob()?.to_vec();
    let out = SessionEvent {
        session,
        received,
        kind,
        raw,
    };
    if encode(&out)? != bytes {
        return Err(Error::Encoding);
    }
    Ok(out)
}
