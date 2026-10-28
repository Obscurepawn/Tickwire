//! Graceful report-triggered retirement keeps unrelated MD subscribers live.
use crate::{Error, Result, bootstrap::wire_bytes};
use tickwire_control::Session;
use tickwire_ipc::{ControlEnvelope, QuiesceBody, ResourceHeader, Wire};
use tickwire_shm::SubscriptionKey;
pub fn body(bank: &ResourceHeader, sub: SubscriptionKey, membership: u64) -> QuiesceBody {
    QuiesceBody {
        subscriber_instance: sub.instance.to_le_bytes(),
        bank_resource_id: bank.resource_id,
        cursor_resource_id: sub.cursor.to_le_bytes(),
        subscription_generation: sub.generation,
        channel_epoch: bank.channel_epoch,
        ring_epoch: bank.ring_epoch,
        membership_version: membership,
        detach_reason: 2,
        ..Default::default()
    }
}
pub fn request(
    session: &Session,
    bank: &ResourceHeader,
    sub: SubscriptionKey,
    membership: u64,
    id: [u8; 16],
) -> Result<Vec<u8>> {
    if id == [0; 16] {
        return Err(Error::Identity);
    }
    Ok(session.encode(
        ControlEnvelope {
            opcode: 20,
            flags: 1,
            request_id: id,
            expected_state_version: membership,
            ..Default::default()
        },
        &wire_bytes(&body(bank, sub, membership))?,
    )?)
}
pub fn check_request(
    envelope: &ControlEnvelope,
    raw: &[u8],
    bank: &ResourceHeader,
    sub: SubscriptionKey,
    membership: u64,
) -> Result<QuiesceBody> {
    let expected = body(bank, sub, membership);
    let actual = QuiesceBody::decode(raw)?;
    if envelope.opcode != 20
        || envelope.flags != 1
        || envelope.request_id == [0; 16]
        || envelope.expected_state_version != membership
        || actual != expected
    {
        return Err(Error::Identity);
    }
    Ok(actual)
}
pub fn check_reply(
    envelope: &ControlEnvelope,
    raw: &[u8],
    request: &ControlEnvelope,
    expected: &QuiesceBody,
) -> Result<QuiesceBody> {
    let actual = QuiesceBody::decode(raw)?;
    let mut normalized = actual.clone();
    normalized.released_seq = 0;
    normalized.applied_seq = 0;
    normalized.evidence_kind = 0;
    if envelope.opcode != 20
        || envelope.flags != 10
        || envelope.request_id != request.request_id
        || envelope.control_epoch != request.control_epoch
        || envelope.expected_state_version != request.expected_state_version
        || actual.evidence_kind != 1
        || actual.applied_seq > actual.released_seq
        || normalized != *expected
    {
        return Err(Error::Identity);
    }
    Ok(actual)
}

/// Shared by normal and capture workers after callbacks and borrowed payloads
/// have stopped. Success requires the MD owner's actual mapped cursor detach.
pub(crate) fn depart_market(
    connection: &mut tickwire_control_sys::Connection,
    session: &mut Session,
    consumer: &tickwire_join::JoinConsumer<4>,
    join: &tickwire_ipc::JoinBody,
    request_id: [u8; 16],
    clock: &mut tickwire_clock_sys::SystemClock,
    until: std::time::Instant,
) -> Result<()> {
    let departure = tickwire_market_source::retirement::request(session, join, request_id)?;
    let departure_request = ControlEnvelope::decode(&departure[..128])?;
    connection
        .send(&departure, &[], until)
        .map_err(|e| Error::from(e).context("retirement: send MD detach"))?;
    let command = connection
        .recv(until)
        .map_err(|e| Error::from(e).context("retirement: await MD quiesce"))?;
    if !command.fds().is_empty() {
        return Err(Error::Identity);
    }
    let response = consumer
        .quiesce(session, command.bytes(), clock.sample()?.monotonic.nanos())
        .map_err(|e| Error::from(e).context("retirement: release market cursor"))?;
    let market_proof = QuiesceBody::decode(&response[128..])?;
    connection
        .send(&response, &[], until)
        .map_err(|e| Error::from(e).context("retirement: send MD quiesced"))?;
    let ack = connection
        .recv(until)
        .map_err(|e| Error::from(e).context("retirement: await MD detached"))?;
    if !ack.fds().is_empty() {
        return Err(Error::Identity);
    }
    let ack = session.validate(ack.bytes(), clock.sample()?.monotonic.nanos())?;
    tickwire_market_source::retirement::verify_applied(
        &ack.envelope,
        ack.body,
        &departure_request,
        &market_proof,
    )?;
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn retirement_reply_cannot_replace_the_requested_cursor_or_evidence() {
        let expected = QuiesceBody {
            subscriber_instance: [1; 16],
            bank_resource_id: [2; 16],
            cursor_resource_id: [3; 16],
            subscription_generation: 4,
            channel_epoch: 5,
            ring_epoch: 6,
            membership_version: 7,
            detach_reason: 2,
            ..Default::default()
        };
        let request = ControlEnvelope {
            opcode: 20,
            flags: 1,
            request_id: [8; 16],
            control_epoch: 9,
            expected_state_version: 7,
            ..Default::default()
        };
        let reply = ControlEnvelope {
            flags: 10,
            ..request.clone()
        };
        let mut proof = expected.clone();
        proof.evidence_kind = 1;
        proof.released_seq = 12;
        proof.applied_seq = 11;
        assert!(check_reply(&reply, &wire_bytes(&proof).unwrap(), &request, &expected).is_ok());
        for field in 0..6 {
            let mut bad = proof.clone();
            match field {
                0 => bad.cursor_resource_id[0] ^= 1,
                1 => bad.subscription_generation += 1,
                2 => bad.evidence_kind = 0,
                3 => bad.exit_evidence_id = [1; 16],
                4 => bad.applied_seq = 13,
                _ => bad.bank_resource_id[0] ^= 1,
            };
            assert!(check_reply(&reply, &wire_bytes(&bad).unwrap(), &request, &expected).is_err());
        }
    }
}
