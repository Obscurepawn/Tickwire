//! Capture facts from an owned registered process, without an intent lane.
use super::*;
use tickwire_account_codec::{CanonicalBaseline, CanonicalRecord};
use tickwire_ipc::{ReplayAppliedBody, ReplayControlBody};
use tickwire_release::Object;
use tickwire_strategy_sdk::{
    capture as protocol_types,
    replay::rebuild::capture::{VerifiedWindow, store},
};
mod launch;
mod protocol;
mod retirement;

pub struct Launch {
    pub spec: LaunchSpec,
    pub bootstrap: protocol_types::Bootstrap,
    pub directory_parent: PathBuf,
    pub supervisor: Identity,
    pub parent_process: ProcessIdentity,
    pub md_admission_directory: PathBuf,
    pub snapshot_id: [u8; 16],
}
/// Historical capture facts, with no deployment or execution authority.
pub struct Captured {
    receipt: protocol_types::Receipt,
    window: VerifiedWindow,
    receipt_bytes: Vec<u8>,
    bootstrap_bytes: Vec<u8>,
    request_bytes: Vec<u8>,
}
impl Captured {
    pub fn receipt(&self) -> &protocol_types::Receipt {
        &self.receipt
    }
    pub fn window(&self) -> &VerifiedWindow {
        &self.window
    }
    pub fn objects(&self) -> impl Iterator<Item = &[u8]> {
        self.window.objects().chain([
            self.bootstrap_bytes.as_slice(),
            self.request_bytes.as_slice(),
            self.receipt_bytes.as_slice(),
        ])
    }
}
pub struct Owner {
    bootstrap: protocol_types::Bootstrap,
    registry: Registry,
    child: ProcessIdentity,
    report: AuthenticatedChild,
    reports: Reports,
    subscription: tickwire_shm::SubscriptionKey,
    clock: SystemClock,
    request: Option<(protocol_types::SealRequest, ReplayControlBody)>,
    outgoing: Option<Vec<u8>>,
    captured: Option<Captured>,
    failed: bool,
    stopped: bool,
    _directory: PrivateDirectory,
    _listener: ControlListener,
}
impl Owner {
    pub fn bootstrap(&self) -> &protocol_types::Bootstrap {
        &self.bootstrap
    }
    pub fn process(&self) -> ProcessIdentity {
        self.child
    }
    pub fn failed(&self) -> bool {
        self.failed
    }
    pub fn captured(&self) -> Option<&Captured> {
        if self.failed || self.stopped {
            None
        } else {
            self.captured.as_ref()
        }
    }
    fn current(&mut self) -> Result<tickwire_clock::MonoTime> {
        if self.failed || self.stopped {
            return Err(Error::NotReady);
        }
        let now = self.clock.sample()?.monotonic;
        if now.nanos() >= self.bootstrap.deadline_mono_ns
            || now.nanos() >= self.report.session.deadline()
            || self.registry.process(self.bootstrap.identity.instance)? != self.child
            || self.report.process != self.child
        {
            return Err(Error::NotReady);
        }
        Ok(now)
    }
    fn close<T>(&mut self, result: Result<T>) -> Result<T> {
        if result.is_err() {
            self.failed = true;
            self.outgoing = None;
            if !self.stopped
                && self
                    .registry
                    .terminate(self.bootstrap.identity.instance)
                    .is_ok()
            {
                self.stopped = true;
            }
        }
        result
    }
    pub fn publish_baseline(&mut self, baseline: &CanonicalBaseline) -> Result<u64> {
        let result = (|| {
            if self.request.is_some() {
                return Err(Error::NotReady);
            }
            let now = self.current()?;
            let sequence = self.reports.publish_baseline(baseline, now.nanos())?;
            self.current()?;
            Ok(sequence)
        })();
        self.close(result)
    }
    pub fn publish_record(&mut self, position: u64, record: &CanonicalRecord) -> Result<u64> {
        let result = (|| {
            if self.request.is_some() {
                return Err(Error::NotReady);
            }
            let now = self.current()?;
            let sequence = self.reports.publish_record(position, record, now.nanos())?;
            self.current()?;
            Ok(sequence)
        })();
        self.close(result)
    }
    /// Fix the actual report position; no further account publication is allowed.
    pub fn seal(&mut self) -> Result<()> {
        let result = (|| {
            let now = self.current()?;
            if self.request.is_some() {
                return Ok(());
            }
            let position = self.reports.last_position();
            if self.reports.account_input_digest(position).is_none() {
                return Err(Error::NotReady);
            }
            let request = protocol_types::SealRequest {
                schema: 1,
                capture: self.bootstrap.identity.capture,
                bootstrap: Object::of(&self.bootstrap.bytes()?),
                contract: Object::of(&self.bootstrap.contract.bytes()?),
                account_position: position,
                deadline_mono_ns: self
                    .bootstrap
                    .deadline_mono_ns
                    .min(self.report.session.deadline()),
            };
            request.validate(&self.bootstrap, now)?;
            self.reports.put_object(&request.bytes()?)?;
            let wire = protocol_types::wire_request(
                &self.bootstrap,
                &request,
                self.report.session.deadline(),
            )?;
            self.current()?;
            self.outgoing = Some(protocol_types::encode_request(&self.report.session, &wire)?);
            self.request = Some((request, wire));
            Ok(())
        })();
        self.close(result)
    }
    /// Bounded control polling; completion reads are cold, bounded durable work.
    pub fn poll(&mut self) -> Result<()> {
        let result = self.poll_inner();
        self.close(result)
    }
    fn poll_inner(&mut self) -> Result<()> {
        self.current()?;
        if let Some(bytes) = &self.outgoing
            && tickwire_strategy_sdk::market_transition::try_send(
                &mut self.report.connection,
                bytes,
            )?
        {
            self.outgoing = None;
        }
        for _ in 0..16 {
            let packet = match self.report.connection.try_recv() {
                Ok(Some(packet)) => packet,
                Ok(None) => break,
                Err(tickwire_control_sys::Error::System { source, .. })
                    if source.kind() == std::io::ErrorKind::Interrupted =>
                {
                    break;
                }
                Err(e) => return Err(e.into()),
            };
            if !packet.fds().is_empty() {
                return Err(Error::Invalid);
            }
            let now = self.current()?;
            let frame = self.report.session.validate(packet.bytes(), now.nanos())?;
            let (request, wire) = self.request.as_ref().ok_or(Error::Invalid)?;
            if frame.envelope.flags == 18 {
                protocol_types::verify_rejection(
                    &frame.envelope,
                    &ReplayControlBody::decode(frame.body)?,
                    wire,
                    now.nanos(),
                )?;
                return Err(Error::NotReady);
            }
            let object = protocol::reply(
                &frame.envelope,
                &ReplayAppliedBody::decode(frame.body)?,
                wire,
                now.nanos(),
            )?;
            let reader = tickwire_recovery::FileBlobReader::open(
                &self.bootstrap.output_directory,
                self.child.uid,
            )?;
            let bytes = reader.get_object(object.digest, protocol_types::MAX_BYTES)?;
            object.verify(&bytes, protocol_types::MAX_BYTES as u64)?;
            let receipt = protocol_types::Receipt::decode(&bytes)?;
            let bootstrap_bytes =
                reader.get_object(receipt.bootstrap.digest, protocol_types::MAX_BYTES)?;
            receipt
                .bootstrap
                .verify(&bootstrap_bytes, protocol_types::MAX_BYTES as u64)?;
            let request_bytes =
                reader.get_object(receipt.request.digest, protocol_types::MAX_BYTES)?;
            receipt
                .request
                .verify(&request_bytes, protocol_types::MAX_BYTES as u64)?;
            if bootstrap_bytes != self.bootstrap.bytes()? || request_bytes != request.bytes()? {
                return Err(Error::Invalid);
            }

            let window = store::read_back(&self.bootstrap.output_directory, &receipt.completion)?;
            let request = request.clone();
            let now = self.current()?;
            receipt.verify(&self.bootstrap, &request, self.child.pid, now, &window)?;
            if self.reports.account_input_digest(request.account_position)
                != Some(window.seal().account_input_digest)
                || !self.reports.confirms_cursor(
                    self.subscription,
                    request.account_position,
                    receipt.report_ring_sequence,
                )?
            {
                return Err(Error::Invalid);
            }
            if let Some(old) = &self.captured {
                if old.receipt != receipt {
                    return Err(Error::Invalid);
                }
            } else {
                self.captured = Some(Captured {
                    receipt,
                    window,
                    receipt_bytes: bytes,
                    bootstrap_bytes,
                    request_bytes,
                });
            }
        }
        self.current()?;
        Ok(())
    }
}
