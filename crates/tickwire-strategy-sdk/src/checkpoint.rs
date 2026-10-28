//! Challenge-bound current callback state; cached READY is not a seal response.
use crate::{Error, Result, bootstrap::IdentityConfig};
use tickwire_ipc::{CheckpointSealBody, CheckpointSealReplyBody, ControlEnvelope, Wire};
/// Explicitly qualified current-state query; the report position is a lower bound.
/// Existing capture modes 0 and 1 retain their exact-cut semantics.
pub const OBSERVE_CURRENT: u16 = tickwire_ipc::CHECKPOINT_OBSERVE_CURRENT;
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
        || b.seal_mode > OBSERVE_CURRENT
        || b.instance_id != identity.instance
        || b.strategy_generation != identity.generation
        || b.logical_strategy_id != identity.logical_strategy
        || b.valid_until_mono_ns > session_until
    {
        return Err(Error::Identity);
    }
    if now >= b.valid_until_mono_ns {
        return Err(if b.seal_mode == OBSERVE_CURRENT {
            Error::Deadline
        } else {
            Error::Identity
        });
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
        || tickwire_ipc::validate_checkpoint_reply(want, &b, previous_ready).is_err()
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

    #[test]
    fn continuous_query_accepts_a_later_actual_prefix_but_not_an_earlier_one() {
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
            seal_mode: 2,
            schema: 1,
        };
        let reply = ControlEnvelope {
            opcode: 40,
            flags: 10,
            ..request.clone()
        };
        let body = CheckpointSealReplyBody {
            challenge: want.challenge,
            report_sequence: want.report_sequence,
            seal_mode: 2,
            schema: 1,
            ready: tickwire_ipc::HandoverReadyBody {
                instance_id: want.instance_id,
                logical_strategy_id: want.logical_strategy_id,
                strategy_generation: want.strategy_generation,
                exec_report_sequence: 5,
                ready_sequence: 8,
                ..Default::default()
            },
        };
        assert!(verify_reply(&reply, &wire_bytes(&body).unwrap(), &request, &want, 7).is_ok());
        for position in [0, 2] {
            let mut early = body.clone();
            early.ready.exec_report_sequence = position;
            assert!(
                verify_reply(&reply, &wire_bytes(&early).unwrap(), &request, &want, 7).is_err()
            );
        }
        let mut wrong_echo = body.clone();
        wrong_echo.report_sequence = 5;
        assert!(
            verify_reply(
                &reply,
                &wire_bytes(&wrong_echo).unwrap(),
                &request,
                &want,
                7
            )
            .is_err()
        );
        for mode in [0, 1] {
            let mut exact = want.clone();
            exact.seal_mode = mode;
            let mut response = body.clone();
            response.seal_mode = mode;
            assert!(
                verify_reply(&reply, &wire_bytes(&response).unwrap(), &request, &exact, 7).is_err()
            );
            response.ready.exec_report_sequence = exact.report_sequence;
            assert!(
                verify_reply(&reply, &wire_bytes(&response).unwrap(), &request, &exact, 7).is_ok()
            );
        }
    }

    #[test]
    fn only_valid_expired_queries_can_be_discarded_without_stopping_the_worker() {
        let identity = IdentityConfig {
            instance: [4; 16],
            generation: 6,
            logical_strategy: 5,
            role: 3,
            host_boot: [7; 16],
            namespace: [8; 16],
            build_hash: [9; 32],
            qualification_hash: [10; 32],
            manifest_hash: [11; 32],
            arch: 1,
            trust_profile: 1,
        };
        let request = ControlEnvelope {
            opcode: 39,
            flags: 1,
            request_id: [1; 16],
            expected_state_version: 3,
            ..Default::default()
        };
        let mut body = CheckpointSealBody {
            instance_id: identity.instance,
            logical_strategy_id: identity.logical_strategy,
            strategy_generation: identity.generation,
            challenge: request.request_id,
            report_sequence: 3,
            valid_until_mono_ns: 100,
            seal_mode: OBSERVE_CURRENT,
            schema: 1,
        };
        assert!(verify_request(&request, &wire_bytes(&body).unwrap(), &identity, 99, 200).is_ok());
        assert!(matches!(
            verify_request(&request, &wire_bytes(&body).unwrap(), &identity, 100, 200),
            Err(Error::Deadline)
        ));
        body.instance_id[0] ^= 1;
        assert!(matches!(
            verify_request(&request, &wire_bytes(&body).unwrap(), &identity, 100, 200),
            Err(Error::Identity)
        ));
        body.instance_id = identity.instance;
        assert!(matches!(
            verify_request(&request, &wire_bytes(&body).unwrap(), &identity, 100, 99),
            Err(Error::Identity)
        ));
        for mode in [0, 1, 3, u16::MAX] {
            body.seal_mode = mode;
            assert!(matches!(
                verify_request(&request, &wire_bytes(&body).unwrap(), &identity, 100, 200),
                Err(Error::Identity)
            ));
        }
    }
}
