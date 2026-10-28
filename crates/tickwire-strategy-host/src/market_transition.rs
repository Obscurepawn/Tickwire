//! Nonblocking commands for the actual owned remote MD consumer.
use super::*;
use tickwire_market_source::transition::{Approval, Commit, Phase, Receipt};
use tickwire_release::Object;
use tickwire_strategy_sdk::market_transition as protocol;

pub(super) struct MarketTransition {
    current: tickwire_market_source::ConnectConfig,
    directory: PathBuf,
    process: ProcessIdentity,
    approval: Option<Approval>,
    commit: Option<Commit>,
    arm_request: Option<tickwire_ipc::ReplayControlBody>,
    commit_request: Option<tickwire_ipc::ReplayControlBody>,
    outgoing: Option<Vec<u8>>,
    receipt: Option<Receipt>,
    rejection: Option<u32>,
}
impl MarketTransition {
    pub fn new(
        current: tickwire_market_source::ConnectConfig,
        directory: PathBuf,
        process: ProcessIdentity,
    ) -> Self {
        Self {
            current,
            directory,
            process,
            approval: None,
            commit: None,
            arm_request: None,
            commit_request: None,
            outgoing: None,
            receipt: None,
            rejection: None,
        }
    }
    pub(crate) fn busy(&self) -> bool {
        self.outgoing.is_some()
            || (self.approval.is_some()
                && self.rejection.is_none()
                && self
                    .receipt
                    .as_ref()
                    .is_none_or(|r| r.phase != Phase::Activated))
    }
    pub fn receipt(&self) -> Option<&Receipt> {
        self.receipt.as_ref()
    }
    pub fn rejection(&self) -> Option<u32> {
        self.rejection
    }
    pub fn arm(
        &mut self,
        approval: &Approval,
        lane: &AuthenticatedChild,
        reports: &mut Reports,
        now: u64,
    ) -> Result<()> {
        if lane.process != self.process
            || lane.session.peer() != &self.current.endpoint.local.identity()
        {
            return Err(Error::Invalid);
        }
        if self
            .approval
            .as_ref()
            .is_some_and(|a| a.operation_id == approval.operation_id)
        {
            let old = self.approval.as_ref().ok_or(Error::Invalid)?;
            if old.digest()? != approval.digest()? {
                return Err(Error::Invalid);
            }
            return Ok(());
        }
        if self.approval.is_some()
            && self.rejection.is_none()
            && self
                .receipt
                .as_ref()
                .is_none_or(|r| r.phase != Phase::Activated)
        {
            return Err(Error::Busy);
        }
        approval.validate(
            &self.current,
            tickwire_clock::MonoTime::new(self.current.domain()?, now),
        )?;
        let bytes = serde_json::to_vec(approval).map_err(|_| Error::Invalid)?;
        reports.put_object(&bytes)?;
        let request = protocol::request(approval, protocol::ARM, &bytes, lane.session.deadline())?;
        self.outgoing = Some(protocol::encode_request(&lane.session, &request)?);
        self.arm_request = Some(request);
        self.commit_request = None;
        self.commit = None;
        self.approval = Some(approval.clone());
        self.receipt = None;
        self.rejection = None;
        Ok(())
    }
    pub fn commit(
        &mut self,
        commit: &Commit,
        lane: &AuthenticatedChild,
        reports: &mut Reports,
        now: u64,
    ) -> Result<()> {
        if lane.process != self.process {
            return Err(Error::Invalid);
        }
        let approval = self.approval.as_ref().ok_or(Error::NotReady)?;
        if let Some(old) = &self.commit {
            return if serde_json::to_vec(old).map_err(|_| Error::Invalid)?
                == serde_json::to_vec(commit).map_err(|_| Error::Invalid)?
            {
                Ok(())
            } else {
                Err(Error::Invalid)
            };
        }
        let receipt = self.receipt.as_ref().ok_or(Error::NotReady)?;
        if self.rejection.is_some() || receipt.phase != Phase::Quiesced || self.outgoing.is_some() {
            return Err(Error::NotReady);
        }
        commit.validate(
            approval,
            receipt.previous_applied.ok_or(Error::Invalid)?,
            tickwire_clock::MonoTime::new(self.current.domain()?, now),
        )?;
        let bytes = serde_json::to_vec(commit).map_err(|_| Error::Invalid)?;
        reports.put_object(&bytes)?;
        let request =
            protocol::request(approval, protocol::COMMIT, &bytes, lane.session.deadline())?;
        self.outgoing = Some(protocol::encode_request(&lane.session, &request)?);
        self.commit_request = Some(request);
        self.commit = Some(commit.clone());
        Ok(())
    }
    pub fn flush(&mut self, lane: &mut AuthenticatedChild, now: u64) -> Result<()> {
        if self
            .approval
            .as_ref()
            .is_some_and(|a| now >= a.deadline_mono_ns)
            && self
                .receipt
                .as_ref()
                .is_none_or(|r| r.phase != Phase::Activated)
            && self.rejection.is_none()
        {
            if self
                .receipt
                .as_ref()
                .is_some_and(|r| r.phase == Phase::Quiesced)
            {
                return Err(Error::NotReady);
            }
            self.rejection = Some(u32::from(protocol::INVALID));
            self.outgoing = None;
        }
        if let Some(bytes) = &self.outgoing
            && protocol::try_send(&mut lane.connection, bytes)?
        {
            self.outgoing = None;
        }
        Ok(())
    }
    /// Receives only frames already authenticated by ReadyProof's single lane dispatcher.
    pub fn observe(&mut self, envelope: &ControlEnvelope, bytes: &[u8], now: u64) -> Result<bool> {
        if envelope.opcode != 42 {
            return Ok(false);
        }
        let reply = tickwire_ipc::ReplayAppliedBody::decode(bytes)?;
        let request = match reply.replay_kind {
            protocol::ARM => self.arm_request.as_ref(),
            protocol::COMMIT => self.commit_request.as_ref(),
            _ => return Ok(false),
        }
        .ok_or(Error::Invalid)?;
        if envelope.flags == 18 {
            let echoed = tickwire_ipc::ReplayControlBody::decode(bytes)?;
            let status = protocol::verify_rejection(envelope, &echoed, request, now)?;
            if self
                .receipt
                .as_ref()
                .is_some_and(|r| r.phase != Phase::Armed)
            {
                return Err(Error::Invalid);
            }
            self.rejection = Some(u32::from(status));
            self.outgoing = None;
            return Ok(true);
        }
        if self.rejection.is_some() {
            return Err(Error::Invalid);
        }
        let object =
            tickwire_strategy_sdk::replay::control::verify_reply(envelope, &reply, request, now)
                .map_err(tickwire_strategy_sdk::Error::Replay)?;
        object.validate(protocol::MAX_BYTES as u64)?;
        let reader = tickwire_recovery::FileBlobReader::open(&self.directory, self.process.uid)?;
        let raw = reader.get_object(object.digest, protocol::MAX_BYTES)?;
        if Object::of(&raw) != object {
            return Err(Error::Invalid);
        }
        let receipt = Receipt::decode(&raw)?;
        let approval = self.approval.as_ref().ok_or(Error::Invalid)?;
        let phase = if reply.replay_kind == protocol::COMMIT {
            Phase::Activated
        } else {
            if !matches!(receipt.phase, Phase::Armed | Phase::Quiesced) {
                return Err(Error::Invalid);
            }
            receipt.phase
        };
        approval.validate_receipt(
            &receipt,
            self.process.pid,
            phase,
            tickwire_clock::MonoTime::new(self.current.domain()?, now),
        )?;
        if let Some(old) = &self.receipt {
            if old == &receipt {
                return Ok(true);
            }
            let valid = matches!(
                (old.phase, receipt.phase),
                (Phase::Armed, Phase::Quiesced) | (Phase::Quiesced, Phase::Activated)
            );
            if !valid
                || old.observed_mono_ns > receipt.observed_mono_ns
                || (old.previous_applied.is_some()
                    && old.previous_applied != receipt.previous_applied)
            {
                return Err(Error::Invalid);
            }
        } else if phase != Phase::Armed {
            return Err(Error::Invalid);
        }
        if phase == Phase::Activated {
            let commit = self.commit.as_ref().ok_or(Error::Invalid)?;
            let bank = ResourceHeader::decode(&commit.config.bank_header)?;
            if receipt.next_descriptor_hash != Some(bank.descriptor_hash) {
                return Err(Error::Invalid);
            }
            self.current = commit.config.clone();
        }
        self.receipt = Some(receipt);
        Ok(true)
    }
}

#[cfg(test)]
mod tests;
