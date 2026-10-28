use crate::protocol::*;
use crate::replica::MarketReplica;
use crate::{
    Error, FeedKey, FeedObservation, Invalidation, MarketSink, MarketSource, PollReport, Result,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::File,
    os::{fd::AsFd, unix::fs::FileExt},
    time::{Duration, Instant},
};
use tickwire_clock::{ClockDomain, MonoTime};
use tickwire_control::{ClientHandshake, ProcessIdentity, Session};
use tickwire_control_sys::{Connection, ReceivedFrame};
use tickwire_ipc::{ControlEnvelope, JoinBody, RecordHeader, ResourceHeader, Wire};
use tickwire_join::{JoinConsumer, TopicRegistry};
mod pending;
mod transition;
pub use pending::{JoinProgress, PendingJoin};
#[cfg(all(test, target_os = "linux"))]
mod pending_tests;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectConfig {
    pub endpoint: Endpoint,
    /// Exact approved MD bank header, including full topic/catalog/arena identity.
    pub bank_header: Vec<u8>,
    pub catalog_hash: [u8; 32],
    pub instrument_set_version: u64,
    pub instruments: Vec<Instrument>,
    pub cursor_id: [u8; 16],
    pub subscription_generation: u64,
    pub request_id: [u8; 16],
    pub maximum_levels: usize,
    pub startup_timeout_ms: u64,
    pub clock_generation: u64,
}
impl ConnectConfig {
    pub fn domain(&self) -> Result<ClockDomain> {
        ClockDomain::new(self.endpoint.local.host_boot, self.clock_generation)
            .map_err(|_| Error::Identity)
    }
    pub fn registry(&self) -> Result<TopicRegistry> {
        if self.instruments.is_empty() || self.instruments.len() > 4096 {
            return Err(Error::Bounds);
        }
        Ok(TopicRegistry {
            catalog_hash: self.catalog_hash,
            instrument_set_version: self.instrument_set_version,
            instruments: self
                .instruments
                .iter()
                .map(|i| tickwire_snapshot::InstrumentSpec {
                    instrument_id: i.id,
                    instrument_version: i.version,
                    required_states: i.required_states,
                })
                .collect(),
        })
    }
}
/// A completed actual JOIN. The baseline is retained verbatim for strategy recovery.
pub struct Client {
    pub consumer: JoinConsumer<4>,
    pub replica: MarketReplica,
    pub connection: Connection,
    pub session: Session,
    pub baseline: JoinBootstrap,
    pub baseline_bytes: Vec<u8>,
    pub baseline_now: u64,
    /// Exact post-snapshot records consumed before activation, with the actual
    /// local apply time. A strategy must journal them before its first callback.
    pub catchup: Vec<CatchupRecord>,
    pub bank: ResourceHeader,
    pub join: JoinBody,
    pub domain: ClockDomain,
}
#[derive(Clone, Debug)]
pub struct CatchupRecord {
    pub record: Vec<u8>,
    pub payload: Vec<u8>,
    pub instrument: u64,
    pub applied_mono_ns: u64,
}
const MAX_CATCHUP_BYTES: usize = 64 * 1024 * 1024;

fn retain_catchup(
    records: &mut Vec<CatchupRecord>,
    bytes: &mut usize,
    capacity: usize,
    record: CatchupRecord,
) -> Result<()> {
    let next = bytes
        .checked_add(record.record.len())
        .and_then(|n| n.checked_add(record.payload.len()))
        .and_then(|n| n.checked_add(32))
        .ok_or(Error::Bounds)?;
    if records.len() >= capacity || next > MAX_CATCHUP_BYTES {
        return Err(Error::Bounds);
    }
    records.push(record);
    *bytes = next;
    Ok(())
}

fn approved_header(packet: &ReceivedFrame) -> Result<ResourceHeader> {
    if packet.fds().len() != 1 {
        return Err(Error::Identity);
    }
    let file = File::from(packet.fds()[0].try_clone()?);
    let mut bytes = [0; 4096];
    file.read_exact_at(&mut bytes, 0)?;
    let header = ResourceHeader::decode(&bytes)?;
    if file.metadata()?.len() != header.resource_bytes {
        return Err(Error::Identity);
    }
    let mut zero = header.clone();
    zero.descriptor_hash = [0; 32];
    if header.descriptor_hash != <[u8; 32]>::from(Sha256::digest(wire_bytes(&zero)?)) {
        return Err(Error::Identity);
    }
    // The caller checks this exact hash against the authenticated offer descriptor
    // before using any field to admit the mapping.
    Ok(header)
}
impl Client {
    /// # Safety
    /// The endpoint policy names an authenticated qualified MD writer, whose
    /// immutable bank and cursor gating contracts hold for all registered readers.
    pub unsafe fn connect(
        config: &ConnectConfig,
        clock: &mut impl FnMut() -> Result<MonoTime>,
    ) -> Result<Self> {
        let deadline = Instant::now()
            .checked_add(Duration::from_millis(config.startup_timeout_ms))
            .ok_or(Error::Bounds)?;
        // SAFETY: the caller establishes the same qualified writer contract.
        let mut pending = unsafe { PendingJoin::new(config, clock()?)? };
        loop {
            if Instant::now() >= deadline {
                return Err(Error::Deadline);
            }
            let progress = pending.poll(clock()?, 64)?;
            if progress.ready {
                return pending.take_client().ok_or(Error::NotReady);
            }
            if !progress.runnable {
                std::thread::sleep(Duration::from_millis(1));
            }
        }
    }
}
pub struct ShmSource {
    client: Client,
    stopped: bool,
    announced: bool,
    last_now: u64,
    transition: Option<transition::SourceTransition>,
}
impl ShmSource {
    pub fn new(client: Client) -> Self {
        Self {
            last_now: client.baseline_now,
            client,
            stopped: false,
            announced: false,
            transition: None,
        }
    }
    pub fn client(&self) -> &Client {
        &self.client
    }
    pub fn into_client(self) -> Client {
        self.client
    }
    fn observation(&self, id: u64, _now: MonoTime, _age: u64) -> Result<FeedObservation> {
        let v = self.client.replica.view(id).ok_or(Error::Identity)?;
        Ok(self.observation_from_view(&v))
    }
    fn observation_from_view(&self, v: &crate::replica::MarketView<'_>) -> FeedObservation {
        let mark = v.trigger();
        let received = v.received_mono_ns();
        let published = v.published_mono_ns();
        FeedObservation {
            key: FeedKey {
                topic_id: mark.topic_id,
                instrument_id: v.instrument(),
            },
            instrument_version: v.version(),
            feed_generation: mark.feed_generation,
            ring_epoch: mark.ring_epoch,
            sequence: mark.sequence,
            received_at: MonoTime::new(self.client.domain, received),
            published_at: MonoTime::new(self.client.domain, published),
            live: !self.stopped && v.continuous(),
        }
    }
    fn invalidate(&mut self, sink: &mut dyn MarketSink) -> Result<()> {
        self.stopped = true;
        self.client.replica.invalidate();
        let mut accepted = true;
        for id in self.client.replica.instruments() {
            accepted &= sink.invalidated(Invalidation {
                key: FeedKey {
                    topic_id: self.client.bank.topic_id,
                    instrument_id: id,
                },
                feed_generation: self.client.bank.feed_generation,
                ring_epoch: self.client.bank.ring_epoch,
            });
        }
        if accepted { Ok(()) } else { Err(Error::Sink) }
    }
    fn drive(
        &mut self,
        now: MonoTime,
        age: u64,
        budget: usize,
        sink: &mut dyn MarketSink,
    ) -> Result<PollReport> {
        if now.domain() != self.client.domain || age == 0 || budget == 0 {
            return Err(Error::NotReady);
        }
        if now.nanos() < self.last_now {
            return Err(Error::Stale);
        }
        self.last_now = now.nanos();
        if let Some(progress) = self.drive_transition(now, budget)? {
            return Ok(progress);
        }
        if self.stopped {
            return Err(Error::NotReady);
        }
        if now.nanos() >= self.client.session.deadline() {
            return Err(Error::Deadline);
        }
        match self.client.connection.try_recv() {
            Ok(Some(packet)) => {
                if !packet.fds().is_empty() {
                    return Err(Error::Identity);
                }
                let h = ControlEnvelope::decode(packet.bytes().get(..128).ok_or(Error::Bounds)?)?;
                if h.opcode == 20 {
                    let expansion = self.expansion_quiesce(h.request_id)?;
                    let response = self.client.consumer.quiesce(
                        &mut self.client.session,
                        packet.bytes(),
                        now.nanos(),
                    )?;
                    if expansion {
                        self.queue_quiesced(response)?;
                        self.invalidate(sink)?;
                        return self.drive_transition(now, budget)?.ok_or(Error::NotReady);
                    }
                    if !self.client.connection.try_send(&response, &[])? {
                        return Err(Error::NotReady);
                    }
                    return Err(Error::NotReady);
                }
                self.client.session.validate(packet.bytes(), now.nanos())?;
                return Err(Error::NotReady);
            }
            Ok(None) => {}
            Err(e) => return Err(e.into()),
        }
        if !self.announced {
            for id in self.client.replica.instruments() {
                if !sink.observed(self.observation(id, now, age)?) {
                    return Err(Error::Sink);
                }
            }
            self.announced = true;
        }
        let mut out = PollReport {
            next_deadline_ns: Some(self.client.session.deadline()),
            ..Default::default()
        };
        while out.records < budget {
            let Some(guard) = self.client.consumer.borrow()? else {
                break;
            };
            let h = RecordHeader::decode(&guard.bytes()[..128])?;
            let payload = if h.message_type == 259 {
                guard.depth_payload()?
            } else {
                &[]
            };
            let dense = self.client.replica.apply_dense(guard.bytes(), payload)?;
            let seq = guard.sequence();
            drop(guard);
            self.client.consumer.apply(seq)?;
            let view = self
                .client
                .replica
                .view_dense(dense)
                .ok_or(Error::Identity)?;
            if !sink.observed(self.observation_from_view(&view)) {
                return Err(Error::Sink);
            }
            out.records += 1;
        }
        out.runnable = out.records == budget;
        for id in self.client.replica.instruments() {
            let o = self.observation(id, now, age)?;
            if !o.live
                && !sink.invalidated(Invalidation {
                    key: o.key,
                    feed_generation: o.feed_generation,
                    ring_epoch: o.ring_epoch,
                })
            {
                return Err(Error::Sink);
            }
        }
        Ok(out)
    }
}
impl MarketSource for ShmSource {
    fn domain(&self) -> ClockDomain {
        self.client.domain
    }
    fn poll(
        &mut self,
        now: MonoTime,
        maximum_age_ns: u64,
        budget: usize,
        sink: &mut dyn MarketSink,
    ) -> Result<PollReport> {
        match self.drive(now, maximum_age_ns, budget, sink) {
            Ok(v) => Ok(v),
            Err(e) => {
                self.fail_transition();
                let _ = self.invalidate(sink);
                Err(e)
            }
        }
    }
    fn is_live(&self, now: MonoTime, maximum_age_ns: u64) -> bool {
        !self.stopped
            && now.nanos() < self.client.session.deadline()
            && self.client.replica.instruments().all(|id| {
                self.observation(id, now, maximum_age_ns).is_ok_and(|o| {
                    o.live
                        && self.client.replica.view(id).is_some_and(|v| v.live())
                        && now.domain() == self.client.domain
                        && now.nanos() >= o.received_at.nanos()
                        && now.nanos() >= o.published_at.nanos()
                        && now.nanos() - o.received_at.nanos() <= maximum_age_ns
                })
            })
    }
    fn stop(&mut self, sink: &mut dyn MarketSink) -> Result<()> {
        self.fail_transition();
        self.invalidate(sink)
    }
}

#[cfg(test)]
mod catchup_tests {
    use super::*;
    #[test]
    fn retained_prefix_keeps_exact_record_payload_and_apply_time_with_atomic_bounds() {
        let record = CatchupRecord {
            record: vec![7; 512],
            payload: vec![8; 1024],
            instrument: 12,
            applied_mono_ns: 55,
        };
        let mut records = vec![];
        let mut bytes = 0;
        retain_catchup(&mut records, &mut bytes, 1, record.clone()).unwrap();
        assert_eq!(records[0].record, record.record);
        assert_eq!(records[0].payload, record.payload);
        assert_eq!(records[0].applied_mono_ns, 55);
        let before = bytes;
        assert!(retain_catchup(&mut records, &mut bytes, 1, record).is_err());
        assert_eq!(records.len(), 1);
        assert_eq!(bytes, before);
    }
}
