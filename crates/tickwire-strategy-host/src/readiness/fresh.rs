//! One explicit cold query and bounded authenticated late-response quarantine.
use super::*;
use crate::observation::{self, FreshObservation, Kernel, ObservationRequest, Prefixes};
use tickwire_clock::MonoTime;
use tickwire_ipc::{CheckpointSealBody, CheckpointSealReplyBody};
const IDS: usize = 4096;
const CLOSED: usize = 16;
#[derive(Clone, Copy)]
enum Failure {
    Expired,
    Cancelled,
    Capacity,
}
impl Failure {
    fn error(self) -> Error {
        match self {
            Self::Expired => Error::ObservationExpired,
            Self::Cancelled => Error::ObservationCancelled,
            Self::Capacity => Error::ObservationCapacity,
        }
    }
}
struct Sent {
    envelope: ControlEnvelope,
    body: CheckpointSealBody,
    previous: u64,
}
pub(super) struct Pending {
    request: ObservationRequest,
    context: cache::Context,
    before: Kernel,
    started: MonoTime,
    sent: bool,
    wire: Vec<u8>,
    proof: Sent,
    prefixes: Prefixes,
    reply: Option<CheckpointSealReplyBody>,
    announced: Option<HandoverReadyBody>,
    failure: Option<Failure>,
}
#[derive(Default)]
pub(super) struct State {
    pub(super) active: Option<Box<Pending>>,
    closed: Vec<Sent>,
    ids: Vec<[u8; 16]>,
    query_watermark: u64,
}
fn advances(body: &HandoverReadyBody, last: &HandoverReadyBody) -> bool {
    body == last
        || (body.ready_sequence > last.ready_sequence
            && body.exec_report_sequence >= last.exec_report_sequence
            && body.report_ring_sequence >= last.report_ring_sequence
            && body.ledger_sequence >= last.ledger_sequence)
}
// A queued query snapshot may precede a later periodic announcement on the wire.
// Only the fixed challenge baseline constrains it; periodic progress stays separate.
fn validate_response(
    proof: &Sent,
    envelope: &ControlEnvelope,
    bytes: &[u8],
) -> Result<CheckpointSealReplyBody> {
    let reply = tickwire_strategy_sdk::checkpoint::verify_reply(
        envelope,
        bytes,
        &proof.envelope,
        &proof.body,
        proof.previous,
    )?;
    Ok(reply)
}
#[derive(Clone)]
struct ChallengeSeed {
    instance: [u8; 16],
    sequence: u64,
    epoch: u64,
    request: [u8; 16],
    host_boot: [u8; 16],
    pid: u32,
    start_ticks: u64,
}
fn challenge(seed: &ChallengeSeed) -> Result<[u8; 16]> {
    let mut material = b"tickwire.host.observation.v1".to_vec();
    material.extend(seed.instance);
    material.extend(seed.sequence.to_le_bytes());
    material.extend(seed.epoch.to_le_bytes());
    material.extend(seed.request);
    material.extend(seed.host_boot);
    material.extend(seed.pid.to_le_bytes());
    material.extend(seed.start_ticks.to_le_bytes());
    let hash = tickwire_recovery::digest(&material);
    hash[..16].try_into().map_err(|_| Error::Invalid)
}
impl State {
    fn capacity(&self, id: [u8; 16]) -> Result<()> {
        if self.ids.contains(&id) {
            return Err(Error::Invalid);
        }
        if self.ids.len() == IDS || self.closed.len() == CLOSED {
            return Err(Error::ObservationCapacity);
        }
        Ok(())
    }
    fn idle(&self, now: u64) -> Result<()> {
        if self.active.is_some() || self.closed.iter().any(|s| now < s.body.valid_until_mono_ns) {
            Err(Error::Busy)
        } else {
            Ok(())
        }
    }
    pub(super) fn flush(&mut self, lane: &mut AuthenticatedChild, now: u64) -> Result<()> {
        if let Some(p) = &mut self.active {
            if now >= p.proof.body.valid_until_mono_ns {
                p.failure = Some(Failure::Expired);
            }
            if p.failure.is_none() && !p.sent && lane.connection.try_send(&p.wire, &[])? {
                p.sent = true;
            }
        }
        Ok(())
    }
    pub(super) fn announcement(&mut self, body: &HandoverReadyBody) -> Result<()> {
        let p = self.active.as_mut().ok_or(Error::Invalid)?;
        if body.instance_id != p.context.identity.instance
            || body.logical_strategy_id != p.context.identity.logical_strategy
            || body.strategy_generation != p.context.identity.generation
            || p.announced.as_ref().is_some_and(|old| !advances(body, old))
        {
            return Err(Error::Invalid);
        }
        p.announced = Some(body.clone());
        Ok(())
    }
    fn reply(&mut self, envelope: &ControlEnvelope, bytes: &[u8]) -> Result<bool> {
        if envelope.opcode != 40 {
            return Ok(false);
        }
        if let Some(p) = &mut self.active
            && envelope.request_id == p.proof.envelope.request_id
        {
            if !p.sent || p.reply.is_some() {
                return Err(Error::Invalid);
            }
            let reply = validate_response(&p.proof, envelope, bytes)?;
            self.query_watermark = self.query_watermark.max(reply.ready.ready_sequence);
            p.reply = Some(reply);
            return Ok(true);
        }
        if let Some(index) = self
            .closed
            .iter()
            .position(|p| envelope.request_id == p.envelope.request_id)
        {
            let p = &self.closed[index];
            let reply = tickwire_strategy_sdk::checkpoint::verify_reply(
                envelope,
                bytes,
                &p.envelope,
                &p.body,
                p.previous,
            )?;
            self.query_watermark = self.query_watermark.max(reply.ready.ready_sequence);
            self.closed.remove(index);
            return Ok(true);
        }
        Ok(false)
    }
    fn close(&mut self) -> Option<Box<Pending>> {
        let p = self.active.take()?;
        // Receiving the matching response proves the worker cleared its query.
        if p.sent && p.reply.is_none() {
            self.closed.push(Sent {
                envelope: p.proof.envelope.clone(),
                body: p.proof.body.clone(),
                previous: p.proof.previous,
            });
        }
        Some(p)
    }
}
impl ReadyProof {
    pub(crate) fn observing(&self) -> bool {
        self.fresh.active.is_some()
    }
    pub(crate) fn observation_idle(&self, now: u64) -> Result<()> {
        self.fresh.idle(now)
    }
    pub(crate) fn record_report_prefix(&mut self, reports: &Reports) {
        if let Some(p) = &mut self.fresh.active {
            let n = reports.last_position();
            if reports
                .account_input_digest(n)
                .is_none_or(|d| p.prefixes.record(n, d).is_err())
            {
                p.failure = Some(Failure::Capacity);
            }
        }
    }
    pub(crate) fn observe_fresh_reply(
        &mut self,
        envelope: &ControlEnvelope,
        bytes: &[u8],
    ) -> Result<bool> {
        self.fresh.reply(envelope, bytes)
    }
    pub(crate) fn begin_observation(
        &mut self,
        request: ObservationRequest,
        lane: &mut AuthenticatedChild,
        reports: &Reports,
        registry: &mut Registry,
        clock: &mut SystemClock,
    ) -> Result<()> {
        let started = clock.sample()?.monotonic;
        self.observation_idle(started.nanos())?;
        if self.directory.is_none() || self.pause_callbacks {
            return Err(Error::ObservationUnsupported);
        }
        if request.request_id == [0; 16] || request.deadline.domain() != started.domain() {
            return Err(Error::Invalid);
        }
        if request.deadline.nanos() <= started.nanos()
            || request.deadline.nanos() > lane.session.deadline()
        {
            return Err(Error::ObservationExpired);
        }
        self.fresh.capacity(request.request_id)?;
        if request.require_resources && self.resources.is_none() {
            return Err(Error::ObservationUnsupported);
        }
        let context = self.context(lane, started.nanos())?;
        let before =
            observation::sample(registry, self.identity.instance, self.resources.is_some())?;
        if before.process().identity() != &self.identity
            || before.process().process() != lane.process
            || before.process().connected_epoch() != Some(context.epoch)
        {
            return Err(Error::Invalid);
        }
        let now = clock.sample()?.monotonic;
        if now.domain() != started.domain() || now.nanos() >= request.deadline.nanos() {
            return Err(Error::ObservationExpired);
        }
        let floor = reports.last_position();
        let digest = reports.account_input_digest(floor).ok_or(Error::NotReady)?;
        self.seal_sequence = self.seal_sequence.checked_add(1).ok_or(Error::Invalid)?;
        let challenge = challenge(&ChallengeSeed {
            instance: self.identity.instance,
            sequence: self.seal_sequence,
            epoch: context.epoch,
            request: request.request_id,
            host_boot: before.process().host_boot(),
            pid: before.process().process().pid,
            start_ticks: before.process().start_ticks(),
        })?;
        let body = CheckpointSealBody {
            instance_id: self.identity.instance,
            logical_strategy_id: self.identity.logical_strategy,
            strategy_generation: self.identity.generation,
            challenge,
            report_sequence: floor,
            valid_until_mono_ns: request.deadline.nanos(),
            seal_mode: tickwire_strategy_sdk::checkpoint::OBSERVE_CURRENT,
            schema: 1,
        };
        let wire = lane.session.encode(
            ControlEnvelope {
                opcode: 39,
                flags: 1,
                request_id: challenge,
                expected_state_version: floor,
                ..Default::default()
            },
            &wire_bytes(&body)?,
        )?;
        let envelope = ControlEnvelope::decode(&wire[..128])?;
        // Accepted IDs are never recycled, even when dispatch subsequently fails.
        self.fresh.ids.push(request.request_id);
        self.fresh.active = Some(Box::new(Pending {
            request,
            context,
            before,
            started,
            sent: false,
            wire,
            proof: Sent {
                envelope,
                body,
                previous: self
                    .last
                    .as_ref()
                    .map_or(0, |r| r.ready_sequence)
                    .max(self.fresh.query_watermark),
            },
            prefixes: Prefixes::new(floor, digest),
            reply: None,
            announced: self.last.clone(),
            failure: None,
        }));
        self.fresh.flush(lane, now.nanos())
    }
    pub(crate) fn cancel_observation(&mut self, id: [u8; 16]) -> Result<()> {
        let p = self.fresh.active.as_mut().ok_or(Error::NotReady)?;
        if p.request.request_id != id {
            return Err(Error::Invalid);
        }
        p.failure = Some(Failure::Cancelled);
        self.fresh.close();
        Ok(())
    }
    pub(crate) fn finish_observation(
        &mut self,
        lane: &AuthenticatedChild,
        reports: &Reports,
        sub: tickwire_shm::SubscriptionKey,
        registry: &mut Registry,
        clock: &mut SystemClock,
    ) -> Result<Option<FreshObservation>> {
        let Some(p) = self.fresh.active.as_mut() else {
            return Ok(None);
        };
        let now = clock.sample()?.monotonic;
        if now.nanos() >= p.request.deadline.nanos() {
            p.failure = Some(Failure::Expired);
        }
        if let Some(failure) = p.failure {
            self.fresh.close();
            return Err(failure.error());
        }
        if p.reply.is_none() {
            return Ok(None);
        }
        let p = self.fresh.close().ok_or(Error::Invalid)?;
        let reply = p.reply.ok_or(Error::Invalid)?;
        if self.context(lane, now.nanos())? != p.context {
            return Err(Error::Invalid);
        }
        let (account, objects) = self.cache.load(
            self.directory.as_ref().ok_or(Error::Invalid)?,
            &p.context,
            &reply.ready,
            now.nanos(),
        )?;
        if p.prefixes.get(reply.ready.exec_report_sequence) != Some(account)
            || !reports.confirms_cursor(
                sub,
                reply.ready.exec_report_sequence,
                reply.ready.report_ring_sequence,
            )?
        {
            return Err(Error::Invalid);
        }
        let after =
            observation::sample(registry, self.identity.instance, self.resources.is_some())?;
        if !observation::same_process(&p.before, &after) {
            return Err(Error::Invalid);
        }
        let current = observation::configuration::current(
            &reply.ready,
            &objects,
            after.process().identity(),
            after.process().image_hash(),
            p.started.domain(),
        )?;
        let (configuration, inputs) = match current {
            Some(current) => (
                Some(observation::configuration::configuration(
                    &current.checkpoint,
                )?),
                observation::inputs::extract(
                    &current,
                    &objects,
                    &reply.ready,
                    observation::inputs::report(reports, sub, reply.ready.report_ring_sequence),
                )?,
            ),
            None => (None, None),
        };
        let completed = clock.sample()?.monotonic;
        if completed.domain() != p.started.domain()
            || completed.nanos() < p.started.nanos()
            || completed.nanos() >= p.request.deadline.nanos()
        {
            return Err(Error::ObservationExpired);
        }
        if self.context(lane, completed.nanos())? != p.context {
            return Err(Error::Invalid);
        }
        Ok(Some(FreshObservation {
            request: p.request,
            challenge: p.proof.body.challenge,
            epoch: p.context.epoch,
            floor: p.proof.body.report_sequence,
            ready: reply.ready,
            account,
            before: p.before,
            after,
            started: p.started,
            completed,
            subscription: sub,
            objects,
            configuration,
            inputs,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn sent(challenge: u8) -> Sent {
        Sent {
            envelope: ControlEnvelope {
                opcode: 39,
                flags: 1,
                request_id: [challenge; 16],
                control_epoch: 7,
                expected_state_version: 3,
                ..Default::default()
            },
            body: CheckpointSealBody {
                instance_id: [4; 16],
                logical_strategy_id: 5,
                strategy_generation: 6,
                challenge: [challenge; 16],
                report_sequence: 3,
                valid_until_mono_ns: 100,
                seal_mode: 2,
                schema: 1,
            },
            previous: 8,
        }
    }
    fn response(p: &Sent) -> (ControlEnvelope, CheckpointSealReplyBody) {
        (
            ControlEnvelope {
                opcode: 40,
                flags: 10,
                ..p.envelope.clone()
            },
            CheckpointSealReplyBody {
                challenge: p.body.challenge,
                report_sequence: 3,
                seal_mode: 2,
                schema: 1,
                ready: HandoverReadyBody {
                    instance_id: [4; 16],
                    logical_strategy_id: 5,
                    strategy_generation: 6,
                    ready_sequence: 9,
                    exec_report_sequence: 5,
                    ..Default::default()
                },
            },
        )
    }
    #[test]
    fn cancelled_sent_query_keeps_lane_until_reply_or_original_deadline() {
        let mut state = State::default();
        state.closed.push(sent(1));
        assert!(matches!(state.idle(99), Err(Error::Busy)));
        assert!(state.idle(100).is_ok());
        // Expiry releases exclusion, but cannot forget a potentially late reply.
        assert_eq!(state.closed.len(), 1);
        let (env, reply) = response(&state.closed[0]);
        assert!(state.reply(&env, &wire_bytes(&reply).unwrap()).unwrap());
        assert!(state.closed.is_empty());
        assert!(state.idle(99).is_ok());
    }
    #[test]
    fn late_response_never_answers_a_different_query_and_mutations_do_not_drain() {
        let mut state = State::default();
        state.closed.push(sent(1));
        let (env, reply) = response(&state.closed[0]);
        let mut other = env.clone();
        other.request_id = [2; 16];
        assert!(!state.reply(&other, &wire_bytes(&reply).unwrap()).unwrap());
        let mut wrong = reply.clone();
        wrong.challenge = [2; 16];
        assert!(state.reply(&env, &wire_bytes(&wrong).unwrap()).is_err());
        let mut wrong = reply.clone();
        wrong.ready.exec_report_sequence = 2;
        assert!(state.reply(&env, &wire_bytes(&wrong).unwrap()).is_err());
        let mut wrong_env = env.clone();
        wrong_env.control_epoch += 1;
        assert!(
            state
                .reply(&wrong_env, &wire_bytes(&reply).unwrap())
                .is_err()
        );
        assert_eq!(state.closed.len(), 1);
        assert!(state.reply(&env, &wire_bytes(&reply).unwrap()).unwrap());
    }
    #[test]
    fn announcement_cannot_regress_market_account_or_ledger_watermarks() {
        let old = HandoverReadyBody {
            ready_sequence: 9,
            exec_report_sequence: 4,
            report_ring_sequence: 5,
            ledger_sequence: 3,
            ..Default::default()
        };
        let next = HandoverReadyBody {
            ready_sequence: 10,
            ..old.clone()
        };
        assert!(advances(&next, &old));
        assert!(advances(&old, &old));
        for field in 0..4 {
            let mut bad = next.clone();
            match field {
                0 => bad.ready_sequence = 8,
                1 => bad.exec_report_sequence = 3,
                2 => bad.report_ring_sequence = 4,
                _ => bad.ledger_sequence = 2,
            }
            assert!(!advances(&bad, &old));
        }
    }
    #[test]
    fn queued_query_reply_remains_valid_after_a_newer_periodic_announcement() {
        let proof = sent(1);
        let (env, reply) = response(&proof);
        let later = HandoverReadyBody {
            ready_sequence: 10,
            exec_report_sequence: 6,
            ..reply.ready.clone()
        };
        let result = validate_response(&proof, &env, &wire_bytes(&reply).unwrap()).unwrap();
        assert!(!advances(&result.ready, &later));
        assert_eq!(result.ready.ready_sequence, 9);
        assert_eq!(result.ready.exec_report_sequence, 5);
    }

    #[test]
    fn capacity_and_single_use_ids_never_evict_unanswered_challenges() {
        let mut state = State::default();
        for n in 0..IDS as u128 {
            let id = (n + 1).to_le_bytes();
            state.capacity(id).unwrap();
            state.ids.push(id);
        }
        assert!(matches!(
            state.capacity((IDS as u128 + 1).to_le_bytes()),
            Err(Error::ObservationCapacity)
        ));
        assert!(matches!(
            state.capacity(1_u128.to_le_bytes()),
            Err(Error::Invalid)
        ));
        let mut state = State::default();
        for n in 0..CLOSED {
            state.closed.push(sent(n as u8 + 1));
        }
        assert!(state.idle(100).is_ok());
        assert!(matches!(
            state.capacity([99; 16]),
            Err(Error::ObservationCapacity)
        ));
        assert_eq!(state.closed.len(), CLOSED);
    }
    #[test]
    fn challenge_binds_the_real_kernel_incarnation_as_well_as_request_and_session() {
        let seed = ChallengeSeed {
            instance: [1; 16],
            sequence: 2,
            epoch: 3,
            request: [4; 16],
            host_boot: [5; 16],
            pid: 6,
            start_ticks: 7,
        };
        let expected = challenge(&seed).unwrap();
        assert_ne!(expected, [0; 16]);
        assert_eq!(challenge(&seed).unwrap(), expected);
        for field in 0..7 {
            let mut changed = seed.clone();
            match field {
                0 => changed.host_boot[0] ^= 1,
                1 => changed.pid += 1,
                2 => changed.start_ticks += 1,
                3 => changed.request[0] ^= 1,
                4 => changed.epoch += 1,
                5 => changed.sequence += 1,
                _ => changed.instance[0] ^= 1,
            }
            assert_ne!(challenge(&changed).unwrap(), expected, "field {field}");
        }
    }
}
