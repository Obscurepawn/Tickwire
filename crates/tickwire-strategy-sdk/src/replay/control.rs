//! Semantic binding for authenticated 41/42 requests; wire validation is in IPC.
use super::*;
use tickwire_ipc::{ControlEnvelope, ReplayControlBody};
/// Called only after Session authenticated the peer and envelope.
pub fn verify_reply(
    envelope: &ControlEnvelope,
    body: &tickwire_ipc::ReplayAppliedBody,
    request: &ReplayControlBody,
    now: u64,
) -> Result<Object> {
    if envelope.opcode != 42
        || envelope.flags != 10
        || envelope.request_id != request.challenge
        || envelope.expected_state_version != request.expected_sequence
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
        || body.replay_kind != request.replay_kind
        || now >= request.valid_until_mono_ns
    {
        return Err(Error::Identity);
    }
    let object = Object {
        digest: body.object_digest,
        bytes: body.object_bytes,
    };
    object.validate(MAX_BYTES as u64)?;
    Ok(object)
}
pub fn verify_request(
    envelope: &ControlEnvelope,
    body: &ReplayControlBody,
    input: &ControlInput,
    current: &Checkpoint,
    now: u64,
    until: u64,
) -> Result<()> {
    let source = input.source();
    let expected = input.expected();
    let object = Object::of(&input.bytes()?);
    let kind = match input {
        ControlInput::Capture { .. } => 1,
        ControlInput::Auxiliary { .. } => 5,
        ControlInput::Install { .. } => 3,
    };
    if envelope.opcode != 41
        || envelope.flags != 1
        || envelope.request_id != body.challenge
        || envelope.expected_state_version != body.expected_sequence
        || body.schema != 1
        || body.replay_kind != kind
        || body.challenge == [0; 16]
        || body.operation_id == [0; 16]
        || body.operation_revision == 0
        || now >= body.valid_until_mono_ns
        || body.valid_until_mono_ns > until
        || source != &current.source
        || body.instance_id != source.instance
        || body.strategy_generation != source.generation
        || body.logical_strategy_id != source.logical_strategy
        || body.object_digest != object.digest
        || body.object_bytes != object.bytes
        || body.expected_sequence != expected.sequence
        || body.expected_cut_digest != Object::of(&canonical(expected)?).digest
    {
        return Err(Error::Identity);
    }
    if matches!(
        input,
        ControlInput::Auxiliary { .. } | ControlInput::Install { .. }
    ) && expected != &current.cut
    {
        return Err(Error::Sequence);
    }
    if let ControlInput::Install { request, .. } = input {
        request.bytes()?;
        if request.operation.operation_id != body.operation_id
            || request.operation.operation_revision != body.operation_revision
            || request.target.instance != source.instance
            || request.target.generation != source.generation
            || request.target.logical_strategy != source.logical_strategy
            || request.target.binary_digest != source.binary_digest
            || request.target.state_schema != source.state_schema
            || request.target.clock != source.clock
            || request.target.input_stream == source.input_stream
        {
            return Err(Error::Identity);
        }
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    fn fixture() -> (ControlEnvelope, ReplayControlBody, ControlInput, Checkpoint) {
        let checkpoint = super::super::tests::anchor_with(super::super::tests::Stateful::initial())
            .checkpoint()
            .clone();
        let input = ControlInput::Capture {
            source: checkpoint.source.clone(),
            anchor: Object::of(&checkpoint.bytes().unwrap()),
            expected: checkpoint.cut.clone(),
            pause: true,
        };
        let object = Object::of(&input.bytes().unwrap());
        let body = ReplayControlBody {
            instance_id: checkpoint.source.instance,
            strategy_generation: checkpoint.source.generation,
            logical_strategy_id: checkpoint.source.logical_strategy,
            challenge: [9; 16],
            valid_until_mono_ns: 90,
            object_digest: object.digest,
            object_bytes: object.bytes,
            expected_sequence: checkpoint.cut.sequence,
            expected_cut_digest: Object::of(&canonical(&checkpoint.cut).unwrap()).digest,
            operation_id: [7; 16],
            operation_revision: 2,
            schema: 1,
            replay_kind: 1,
        };
        let envelope = ControlEnvelope {
            opcode: 41,
            flags: 1,
            request_id: body.challenge,
            expected_state_version: body.expected_sequence,
            ..Default::default()
        };
        (envelope, body, input, checkpoint)
    }
    #[test]
    fn capture_binds_actual_source_and_prior_cut_not_only_nonzero_fields() {
        let (e, b, i, c) = fixture();
        assert!(verify_request(&e, &b, &i, &c, 50, 100).is_ok());
        for n in 0..8 {
            let mut x = b.clone();
            match n {
                0 => x.instance_id = [8; 16],
                1 => x.expected_cut_digest = [1; 32],
                2 => x.expected_sequence += 1,
                3 => x.replay_kind = 5,
                4 => x.object_bytes += 1,
                5 => x.operation_revision = 0,
                6 => x.challenge = [0; 16],
                _ => x.logical_strategy_id += 1,
            };
            assert!(
                verify_request(&e, &x, &i, &c, 50, 100).is_err(),
                "mutation {n}"
            );
        }
    }
    #[test]
    fn expired_or_cross_session_replay_request_cannot_start_a_callback() {
        let (e, b, i, c) = fixture();
        assert!(verify_request(&e, &b, &i, &c, 90, 100).is_err());
        assert!(verify_request(&e, &b, &i, &c, 50, 80).is_err());
        let mut e = e;
        e.request_id = [3; 16];
        assert!(verify_request(&e, &b, &i, &c, 50, 100).is_err());
    }
    #[test]
    fn applied_reply_binds_every_request_field_and_exclusive_deadline() {
        let (_, request, _, _) = fixture();
        let expected = Object::of(b"durable installed target");
        let body = tickwire_ipc::ReplayAppliedBody {
            instance_id: request.instance_id,
            strategy_generation: request.strategy_generation,
            logical_strategy_id: request.logical_strategy_id,
            challenge: request.challenge,
            valid_until_mono_ns: request.valid_until_mono_ns,
            object_digest: expected.digest,
            object_bytes: expected.bytes,
            expected_sequence: request.expected_sequence,
            expected_cut_digest: request.expected_cut_digest,
            operation_id: request.operation_id,
            operation_revision: request.operation_revision,
            schema: 1,
            replay_kind: request.replay_kind,
        };
        let envelope = ControlEnvelope {
            opcode: 42,
            flags: 10,
            request_id: request.challenge,
            expected_state_version: request.expected_sequence,
            ..Default::default()
        };
        assert_eq!(verify_reply(&envelope, &body, &request, 50), Ok(expected));
        for mutation in 0..17 {
            let mut e = envelope.clone();
            let mut b = body.clone();
            match mutation {
                0 => e.opcode = 41,
                1 => e.flags = 6,
                2 => e.request_id = [1; 16],
                3 => e.expected_state_version += 1,
                4 => b.instance_id = [8; 16],
                5 => b.strategy_generation += 1,
                6 => b.logical_strategy_id += 1,
                7 => b.challenge = [6; 16],
                8 => b.valid_until_mono_ns += 1,
                9 => b.expected_sequence += 1,
                10 => b.expected_cut_digest = [5; 32],
                11 => b.operation_id = [4; 16],
                12 => b.operation_revision += 1,
                13 => b.schema = 2,
                14 => b.replay_kind = 2,
                15 => b.object_digest = [0; 32],
                _ => b.object_bytes = 0,
            }
            assert!(
                verify_reply(&e, &b, &request, 50).is_err(),
                "mutation {mutation}"
            );
        }
        assert!(verify_reply(&envelope, &body, &request, 90).is_err());
    }
}
