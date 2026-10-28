use super::*;
use crate::transition::{Approval, Commit, Phase, Receipt};

pub(super) struct SourceTransition {
    approval: Approval,
    receipt: Receipt,
    outbox: Option<Vec<u8>>,
    pending: Option<PendingJoin>,
    commit_hash: Option<[u8; 32]>,
    released: bool,
    failed: bool,
}

impl ShmSource {
    /// The caller admits this operation through its trusted control boundary.
    /// All peers remain pinned to the existing qualified MD writer.
    pub fn arm_transition(
        &mut self,
        approval: Approval,
        current: &ConnectConfig,
        now: MonoTime,
    ) -> Result<Receipt> {
        if now.domain() != self.client.domain || now.nanos() < self.last_now {
            return Err(Error::Stale);
        }
        let hash = approval.digest()?;
        if let Some(state) = &self.transition {
            if state.approval.operation_id == approval.operation_id {
                if state.failed || state.receipt.approval_hash != hash {
                    return Err(Error::Identity);
                }
                if !state.released && now.nanos() >= state.approval.deadline_mono_ns {
                    return Err(Error::Deadline);
                }
                return Ok(state.receipt.clone());
            }
            if !state.released || state.failed {
                return Err(Error::NotReady);
            }
        }
        if self.stopped || now.nanos() < self.last_now {
            return Err(Error::NotReady);
        }
        approval.validate(current, now)?;
        approval.validate_client(&self.client, now)?;
        let receipt = Receipt::armed(&approval, std::process::id(), now)?;
        self.transition = Some(SourceTransition {
            approval,
            receipt: receipt.clone(),
            outbox: None,
            pending: None,
            commit_hash: None,
            released: false,
            failed: false,
        });
        self.last_now = now.nanos();
        Ok(receipt)
    }
    pub fn commit_transition(&mut self, commit: Commit, now: MonoTime) -> Result<()> {
        let state = self.transition.as_mut().ok_or(Error::NotReady)?;
        let hash: [u8; 32] = Sha256::digest(serde_json::to_vec(&commit)?).into();
        if state.failed || now.domain() != self.client.domain || now.nanos() < self.last_now {
            return Err(Error::NotReady);
        }
        if let Some(previous) = state.commit_hash {
            return if previous == hash {
                Ok(())
            } else {
                Err(Error::Identity)
            };
        }
        if state.receipt.phase != Phase::Quiesced || state.outbox.is_some() {
            return Err(Error::NotReady);
        }
        commit.validate(
            &state.approval,
            state.receipt.previous_applied.ok_or(Error::Identity)?,
            now,
        )?;
        // SAFETY: Approval validated the same qualified writer, OS credentials and
        // immutable resource contract as the existing Client. The trusted caller
        // approved the exact new subscription; PendingJoin authenticates it again.
        state.pending = Some(unsafe { PendingJoin::new(&commit.config, now)? });
        state.commit_hash = Some(hash);
        self.last_now = now.nanos();
        Ok(())
    }
    pub fn transition_receipt(&self) -> Option<&Receipt> {
        self.transition
            .as_ref()
            .filter(|s| !s.failed)
            .map(|s| &s.receipt)
    }
    /// Release observations only after the caller synchronizes its effective receipt.
    pub fn release_transition(&mut self, now: MonoTime) -> Result<()> {
        let state = self.transition.as_mut().ok_or(Error::NotReady)?;
        if state.failed
            || state.receipt.phase != Phase::Activated
            || state.pending.is_some()
            || now.domain() != self.client.domain
            || now.nanos() < self.last_now
            || now.nanos() >= self.client.session.deadline()
        {
            return Err(Error::NotReady);
        }
        if !state.released {
            state.approval.validate_receipt(
                &state.receipt,
                std::process::id(),
                Phase::Activated,
                now,
            )?;
            state.released = true;
            self.stopped = false;
            self.announced = false;
        }
        self.last_now = now.nanos();
        Ok(())
    }
    pub(super) fn fail_transition(&mut self) {
        if let Some(state) = &mut self.transition {
            state.failed = true;
            state.outbox = None;
            if let Some(mut pending) = state.pending.take() {
                pending.cancel();
            }
        }
    }
    pub(super) fn expansion_quiesce(&mut self, request_id: [u8; 16]) -> Result<bool> {
        let Some(state) = &self.transition else {
            return Ok(false);
        };
        if state.released {
            return Ok(false);
        }
        if state.failed || state.receipt.phase != Phase::Armed || state.outbox.is_some() {
            return Err(Error::NotReady);
        }
        let bank = &self.client.bank;
        let join = &self.client.join;
        let sub = tickwire_shm::SubscriptionKey {
            bank: tickwire_shm::BankKey {
                resource: u128::from_le_bytes(bank.resource_id),
                channel: u128::from_le_bytes(bank.channel_id),
                channel_epoch: bank.channel_epoch,
                ring_epoch: bank.ring_epoch,
                feed_generation: bank.feed_generation,
            },
            instance: u128::from_le_bytes(join.subscriber_instance),
            generation: join.subscription_generation,
            cursor: u128::from_le_bytes(join.cursor_resource_id),
        };
        Ok(request_id == crate::transition::quiesce_request_id(state.approval.operation_id, sub))
    }
    pub(super) fn queue_quiesced(&mut self, response: Vec<u8>) -> Result<()> {
        let state = self.transition.as_mut().ok_or(Error::NotReady)?;
        if state.outbox.is_some() || state.receipt.phase != Phase::Armed {
            return Err(Error::NotReady);
        }
        state.outbox = Some(response);
        Ok(())
    }
    /// None means the existing active subscription should continue polling.
    pub(super) fn drive_transition(
        &mut self,
        now: MonoTime,
        budget: usize,
    ) -> Result<Option<PollReport>> {
        let Some(state) = &mut self.transition else {
            return Ok(None);
        };
        if state.failed {
            return Err(Error::NotReady);
        }
        if state.released {
            return Ok(None);
        }
        if now.nanos() >= state.approval.deadline_mono_ns {
            if state.receipt.phase == Phase::Armed && state.outbox.is_none() {
                self.transition = None;
                return Ok(None);
            }
            return Err(Error::Deadline);
        }
        if let Some(bytes) = &state.outbox {
            match self.client.connection.try_send(bytes, &[]) {
                Ok(true) => {
                    state.receipt = Receipt::quiesced(
                        &state.approval,
                        std::process::id(),
                        self.client.consumer.watermarks().applied,
                        now,
                    )?;
                    state.outbox = None;
                }
                Ok(false) => {}
                Err(tickwire_control_sys::Error::System { source, .. })
                    if source.kind() == std::io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error.into()),
            }
            return Ok(Some(PollReport {
                next_deadline_ns: Some(state.approval.deadline_mono_ns),
                ..Default::default()
            }));
        }
        if let Some(pending) = &mut state.pending {
            let progress = pending.poll(now, budget)?;
            if progress.ready {
                let client = pending.take_client().ok_or(Error::NotReady)?;
                state.receipt = Receipt::activated(
                    &state.approval,
                    std::process::id(),
                    state.receipt.previous_applied.ok_or(Error::Identity)?,
                    &client,
                    now,
                )?;
                self.client = client;
                state.pending = None;
                self.announced = false;
            }
            return Ok(Some(PollReport {
                records: progress.records,
                runnable: progress.runnable,
                next_deadline_ns: Some(
                    progress
                        .next_deadline_ns
                        .min(state.approval.deadline_mono_ns),
                ),
            }));
        }
        match state.receipt.phase {
            Phase::Armed => Ok(None),
            Phase::Quiesced | Phase::Activated => Ok(Some(PollReport {
                next_deadline_ns: Some(state.approval.deadline_mono_ns),
                ..Default::default()
            })),
        }
    }
}
