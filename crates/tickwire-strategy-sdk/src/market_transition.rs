//! Binding for trusted Host market commands on the existing authenticated report lane.
use crate::{Error, Result};
use tickwire_control::Session;
use tickwire_ipc::{ControlEnvelope, ReplayControlBody};
use tickwire_market_source::transition::Approval;
use tickwire_release::Object;

pub const ARM: u16 = 6;
pub const COMMIT: u16 = 7;
pub const MAX_BYTES: usize = 2 * 1024 * 1024;

pub fn request(
    approval: &Approval,
    kind: u16,
    bytes: &[u8],
    until: u64,
) -> Result<ReplayControlBody> {
    if !matches!(kind, ARM | COMMIT) || bytes.is_empty() || bytes.len() > MAX_BYTES {
        return Err(Error::Bounds);
    }
    let object = Object::of(bytes);
    let mut challenge = Vec::from(object.digest);
    challenge.extend(approval.operation_id);
    challenge.extend(kind.to_le_bytes());
    let local = &approval.previous.endpoint.local;
    Ok(ReplayControlBody {
        instance_id: local.instance,
        strategy_generation: local.generation,
        logical_strategy_id: local.logical_strategy,
        challenge: tickwire_recovery::digest(&challenge)[..16]
            .try_into()
            .map_err(|_| Error::Bounds)?,
        valid_until_mono_ns: approval.deadline_mono_ns.min(until),
        object_digest: object.digest,
        object_bytes: object.bytes,
        expected_sequence: 0,
        expected_cut_digest: approval.approval_digest,
        operation_id: approval.operation_id,
        operation_revision: approval.revision,
        schema: 1,
        replay_kind: kind,
    })
}

/// Session validation of the actual sender must precede this object binding.
pub fn verify_request(
    envelope: &ControlEnvelope,
    body: &ReplayControlBody,
    approval: &Approval,
    bytes: &[u8],
    now: u64,
    until: u64,
) -> Result<()> {
    if envelope.opcode != 41
        || envelope.flags != 1
        || envelope.request_id != body.challenge
        || envelope.expected_state_version != 0
        || now >= body.valid_until_mono_ns
        || *body != request(approval, body.replay_kind, bytes, until)?
    {
        return Err(Error::Identity);
    }
    Ok(())
}

pub fn encode_request(session: &Session, body: &ReplayControlBody) -> Result<Vec<u8>> {
    Ok(session.encode(
        ControlEnvelope {
            opcode: 41,
            flags: 1,
            request_id: body.challenge,
            ..Default::default()
        },
        &crate::bootstrap::wire_bytes(body)?,
    )?)
}
pub fn encode_reply(
    session: &Session,
    body: &ReplayControlBody,
    object: Object,
) -> Result<Vec<u8>> {
    let reply = tickwire_ipc::ReplayAppliedBody {
        instance_id: body.instance_id,
        strategy_generation: body.strategy_generation,
        logical_strategy_id: body.logical_strategy_id,
        challenge: body.challenge,
        valid_until_mono_ns: body.valid_until_mono_ns,
        object_digest: object.digest,
        object_bytes: object.bytes,
        expected_sequence: body.expected_sequence,
        expected_cut_digest: body.expected_cut_digest,
        operation_id: body.operation_id,
        operation_revision: body.operation_revision,
        schema: body.schema,
        replay_kind: body.replay_kind,
    };
    Ok(session.encode(
        ControlEnvelope {
            opcode: 42,
            flags: 10,
            request_id: body.challenge,
            ..Default::default()
        },
        &crate::bootstrap::wire_bytes(&reply)?,
    )?)
}

pub const UNSUPPORTED: u16 = 1;
pub const BUSY: u16 = 2;
pub const INVALID: u16 = 3;
pub fn encode_rejection(
    session: &Session,
    body: &ReplayControlBody,
    status: u16,
) -> Result<Vec<u8>> {
    if !matches!(status, UNSUPPORTED | BUSY | INVALID) {
        return Err(Error::Bounds);
    }
    Ok(session.encode(
        ControlEnvelope {
            opcode: 42,
            flags: 18,
            status,
            request_id: body.challenge,
            ..Default::default()
        },
        &crate::bootstrap::wire_bytes(body)?,
    )?)
}
pub fn verify_rejection(
    envelope: &ControlEnvelope,
    body: &ReplayControlBody,
    request: &ReplayControlBody,
    now: u64,
) -> Result<u16> {
    if envelope.opcode != 42
        || envelope.flags != 18
        || envelope.request_id != request.challenge
        || envelope.expected_state_version != 0
        || body != request
        || now >= request.valid_until_mono_ns
        || !matches!(envelope.status, UNSUPPORTED | BUSY | INVALID)
    {
        return Err(Error::Identity);
    }
    Ok(envelope.status)
}

pub fn subscription(
    config: &tickwire_market_source::ConnectConfig,
) -> Result<tickwire_shm::SubscriptionKey> {
    use tickwire_ipc::Wire;
    let h = tickwire_ipc::ResourceHeader::decode(&config.bank_header)?;
    Ok(tickwire_shm::SubscriptionKey {
        bank: tickwire_shm::BankKey {
            resource: u128::from_le_bytes(h.resource_id),
            channel: u128::from_le_bytes(h.channel_id),
            channel_epoch: h.channel_epoch,
            ring_epoch: h.ring_epoch,
            feed_generation: h.feed_generation,
        },
        instance: u128::from_le_bytes(config.endpoint.local.instance),
        generation: config.subscription_generation,
        cursor: u128::from_le_bytes(config.cursor_id),
    })
}

#[cfg(test)]
mod tests;

/// One attempt only. Backpressure and an interrupted syscall keep the outbox intact.
pub fn try_send(connection: &mut tickwire_control_sys::Connection, bytes: &[u8]) -> Result<bool> {
    send_progress(connection.try_send(bytes, &[]))
}
fn send_progress(result: tickwire_control_sys::Result<bool>) -> Result<bool> {
    match result {
        Err(tickwire_control_sys::Error::System { source, .. })
            if source.kind() == std::io::ErrorKind::Interrupted =>
        {
            Ok(false)
        }
        other => Ok(other?),
    }
}
