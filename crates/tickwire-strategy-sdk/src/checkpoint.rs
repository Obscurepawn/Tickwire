//! Challenge-bound current callback state; cached READY is not a seal response.
use crate::{Error, Result, bootstrap::IdentityConfig};
use tickwire_ipc::{CheckpointSealBody, CheckpointSealReplyBody, ControlEnvelope, Wire};
pub fn verify_request(
    envelope: &ControlEnvelope,
    raw: &[u8],
    identity: &IdentityConfig,
    now: u64,
    session_until: u64,
) -> Result<CheckpointSealBody> {
    let b = CheckpointSealBody::decode(raw)?;
    if envelope.opcode != 39
        || envelope.flags != 1
        || envelope.request_id != b.challenge
        || b.challenge == [0; 16]
        || envelope.expected_state_version != b.report_sequence
        || b.schema != 1
        || b.seal_mode > 1
        || b.instance_id != identity.instance
        || b.strategy_generation != identity.generation
        || b.logical_strategy_id != identity.logical_strategy
        || now >= b.valid_until_mono_ns
        || b.valid_until_mono_ns > session_until
    {
        return Err(Error::Identity);
    }
    Ok(b)
}
pub fn verify_reply(
    envelope: &ControlEnvelope,
    raw: &[u8],
    request: &ControlEnvelope,
    want: &CheckpointSealBody,
    previous_ready: u64,
) -> Result<CheckpointSealReplyBody> {
    let b = CheckpointSealReplyBody::decode(raw)?;
    if envelope.opcode != 40
        || envelope.flags != 10
        || envelope.request_id != request.request_id
        || envelope.control_epoch != request.control_epoch
        || envelope.expected_state_version != request.expected_state_version
        || b.challenge != want.challenge
        || b.report_sequence != want.report_sequence
        || b.seal_mode != want.seal_mode
        || b.schema != 1
        || b.ready.instance_id != want.instance_id
        || b.ready.strategy_generation != want.strategy_generation
        || b.ready.logical_strategy_id != want.logical_strategy_id
        || b.ready.exec_report_sequence != want.report_sequence
        || b.ready.ready_sequence <= previous_ready
    {
        return Err(Error::Identity);
    }
    Ok(b)
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::bootstrap::wire_bytes;
    #[test]
    fn cached_or_other_challenge_ready_is_not_fresh_checkpoint_completion() {
        let request = ControlEnvelope {
            opcode: 39,
            flags: 1,
            request_id: [1; 16],
            control_epoch: 2,
            expected_state_version: 3,
            ..Default::default()
        };
        let want = CheckpointSealBody {
            instance_id: [4; 16],
            logical_strategy_id: 5,
            strategy_generation: 6,
            challenge: [1; 16],
            report_sequence: 3,
            valid_until_mono_ns: 100,
            seal_mode: 1,
            schema: 1,
        };
        let reply = ControlEnvelope {
            opcode: 40,
            flags: 10,
            ..request.clone()
        };
        let ready = tickwire_ipc::HandoverReadyBody {
            instance_id: [4; 16],
            logical_strategy_id: 5,
            strategy_generation: 6,
            exec_report_sequence: 3,
            ready_sequence: 8,
            ..Default::default()
        };
        let body = CheckpointSealReplyBody {
            challenge: [1; 16],
            report_sequence: 3,
            seal_mode: 1,
            schema: 1,
            ready,
        };
        assert!(verify_reply(&reply, &wire_bytes(&body).unwrap(), &request, &want, 7).is_ok());
        assert!(verify_reply(&reply, &wire_bytes(&body).unwrap(), &request, &want, 8).is_err());
        let mut bad = body.clone();
        bad.challenge[0] ^= 1;
        assert!(verify_reply(&reply, &wire_bytes(&bad).unwrap(), &request, &want, 7).is_err());
        let mut bad = body.clone();
        bad.ready.exec_report_sequence += 1;
        assert!(verify_reply(&reply, &wire_bytes(&bad).unwrap(), &request, &want, 7).is_err());
    }
}
