use crate::{Error, Result};
use tickwire_ipc::{ControlEnvelope, ReplayAppliedBody, ReplayControlBody};
use tickwire_release::Object;

pub(super) fn reply(
    envelope: &ControlEnvelope,
    body: &ReplayAppliedBody,
    request: &ReplayControlBody,
    now: u64,
) -> Result<Object> {
    if envelope.opcode != 42
        || envelope.flags != 10
        || envelope.status != 0
        || envelope.request_id != request.challenge
        || envelope.expected_state_version != 0
        || body.instance_id != request.instance_id
        || body.strategy_generation != request.strategy_generation
        || body.logical_strategy_id != request.logical_strategy_id
        || body.challenge != request.challenge
        || body.valid_until_mono_ns != request.valid_until_mono_ns
        || body.expected_sequence != request.expected_sequence
        || body.expected_cut_digest != request.expected_cut_digest
        || body.operation_id != request.operation_id
        || body.operation_revision != request.operation_revision
        || body.schema != request.schema
        || body.replay_kind != 8
        || request.replay_kind != 8
        || now >= request.valid_until_mono_ns
    {
        return Err(Error::Invalid);
    }
    let object = Object {
        digest: body.object_digest,
        bytes: body.object_bytes,
    };
    object.validate(tickwire_strategy_sdk::capture::MAX_BYTES as u64)?;
    Ok(object)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn fixture() -> (ControlEnvelope, ReplayAppliedBody, ReplayControlBody) {
        let request = ReplayControlBody {
            instance_id: [1; 16],
            strategy_generation: 2,
            logical_strategy_id: 3,
            challenge: [4; 16],
            valid_until_mono_ns: 100,
            object_digest: [5; 32],
            object_bytes: 50,
            expected_sequence: 7,
            expected_cut_digest: [8; 32],
            operation_id: [9; 16],
            operation_revision: 1,
            schema: 1,
            replay_kind: 8,
        };
        let body = ReplayAppliedBody {
            instance_id: request.instance_id,
            strategy_generation: request.strategy_generation,
            logical_strategy_id: request.logical_strategy_id,
            challenge: request.challenge,
            valid_until_mono_ns: request.valid_until_mono_ns,
            object_digest: [10; 32],
            object_bytes: 60,
            expected_sequence: request.expected_sequence,
            expected_cut_digest: request.expected_cut_digest,
            operation_id: request.operation_id,
            operation_revision: 1,
            schema: 1,
            replay_kind: 8,
        };
        (
            ControlEnvelope {
                opcode: 42,
                flags: 10,
                request_id: request.challenge,
                ..Default::default()
            },
            body,
            request,
        )
    }
    #[test]
    fn capture_reply_binds_every_authenticated_fact_and_bounds() {
        let (envelope, body, request) = fixture();
        assert_eq!(
            reply(&envelope, &body, &request, 99).unwrap(),
            Object {
                digest: [10; 32],
                bytes: 60
            }
        );
        for case in 0..19 {
            let (mut e, mut b, r) = fixture();
            match case {
                0 => e.opcode = 37,
                1 => e.flags = 18,
                2 => e.status = 1,
                3 => e.request_id = [11; 16],
                4 => e.expected_state_version = 7,
                5 => b.instance_id = [11; 16],
                6 => b.strategy_generation += 1,
                7 => b.logical_strategy_id += 1,
                8 => b.challenge = [11; 16],
                9 => b.valid_until_mono_ns += 1,
                10 => b.expected_sequence += 1,
                11 => b.expected_cut_digest = [11; 32],
                12 => b.operation_id = [11; 16],
                13 => b.operation_revision += 1,
                14 => b.schema += 1,
                15 => b.replay_kind = 3,
                16 => b.object_digest = [0; 32],
                17 => b.object_bytes = 0,
                _ => b.object_bytes = 2 * 1024 * 1024 + 1,
            }
            assert!(reply(&e, &b, &r, 99).is_err(), "case {case}");
        }
        assert!(reply(&envelope, &body, &request, 100).is_err());
    }
}
