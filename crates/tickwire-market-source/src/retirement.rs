//! Subscriber-requested DETACH: the publisher first obtains its normal mapped
//! QUIESCED proof, then acknowledges exactly that proof. EOF is never evidence.
use crate::{Error, Result, protocol::wire_bytes};
use sha2::{Digest, Sha256};
use tickwire_control::Session;
use tickwire_ipc::{ControlEnvelope, JoinBody, QuiesceBody, StatusBody, Wire};
pub fn body(join: &JoinBody) -> QuiesceBody {
    QuiesceBody {
        subscriber_instance: join.subscriber_instance,
        bank_resource_id: join.bank_resource_id,
        cursor_resource_id: join.cursor_resource_id,
        subscription_generation: join.subscription_generation,
        channel_epoch: join.channel_epoch,
        ring_epoch: join.ring_epoch,
        membership_version: join.membership_version,
        detach_reason: 2,
        ..Default::default()
    }
}
pub fn request(session: &Session, join: &JoinBody, id: [u8; 16]) -> Result<Vec<u8>> {
    if id == [0; 16] {
        return Err(Error::Identity);
    }
    Ok(session.encode(
        ControlEnvelope {
            opcode: 21,
            flags: 1,
            request_id: id,
            expected_state_version: join.membership_version,
            ..Default::default()
        },
        &wire_bytes(&body(join))?,
    )?)
}
pub fn verify_request(envelope: &ControlEnvelope, raw: &[u8], join: &JoinBody) -> Result<()> {
    if envelope.opcode != 21
        || envelope.flags != 1
        || envelope.request_id == [0; 16]
        || envelope.expected_state_version != join.membership_version
        || QuiesceBody::decode(raw)? != body(join)
    {
        return Err(Error::Identity);
    }
    Ok(())
}
fn status(request: &ControlEnvelope, proof: &QuiesceBody) -> Result<StatusBody> {
    if proof.evidence_kind != 1
        || proof.exit_evidence_id != [0; 16]
        || proof.applied_seq > proof.released_seq
    {
        return Err(Error::Identity);
    }
    Ok(StatusBody {
        subject_id: proof.bank_resource_id,
        original_request_id: request.request_id,
        original_control_epoch: request.control_epoch,
        state_version: proof.membership_version,
        membership_version: proof.membership_version,
        ring_epoch: proof.ring_epoch,
        released_seq: proof.released_seq,
        state: 3,
        result_hash: Sha256::digest(wire_bytes(proof)?).into(),
        ..Default::default()
    })
}
/// Call only after JoinOwner::quiesced checked the real registered cursor.
pub fn applied(
    session: &Session,
    request: &ControlEnvelope,
    proof: &QuiesceBody,
) -> Result<Vec<u8>> {
    Ok(session.encode(
        ControlEnvelope {
            opcode: 49,
            flags: 10,
            request_id: request.request_id,
            expected_state_version: request.expected_state_version,
            ..Default::default()
        },
        &wire_bytes(&status(request, proof)?)?,
    )?)
}
pub fn verify_applied(
    envelope: &ControlEnvelope,
    raw: &[u8],
    request: &ControlEnvelope,
    proof: &QuiesceBody,
) -> Result<()> {
    if envelope.opcode != 49
        || envelope.flags != 10
        || envelope.request_id != request.request_id
        || envelope.control_epoch != request.control_epoch
        || envelope.expected_state_version != request.expected_state_version
        || StatusBody::decode(raw)? != status(request, proof)?
    {
        return Err(Error::Identity);
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn retirement_requests_and_receipts_bind_exact_subscription_and_evidence() {
        let join = JoinBody {
            subscriber_instance: [1; 16],
            bank_resource_id: [2; 16],
            cursor_resource_id: [3; 16],
            subscription_generation: 4,
            channel_epoch: 5,
            ring_epoch: 6,
            membership_version: 7,
            ..Default::default()
        };
        let request = ControlEnvelope {
            opcode: 21,
            flags: 1,
            request_id: [8; 16],
            control_epoch: 9,
            expected_state_version: 7,
            ..Default::default()
        };
        assert!(verify_request(&request, &wire_bytes(&body(&join)).unwrap(), &join).is_ok());
        for field in 0..8 {
            let mut wrong = body(&join);
            match field {
                0 => wrong.subscriber_instance[0] ^= 1,
                1 => wrong.bank_resource_id[0] ^= 1,
                2 => wrong.cursor_resource_id[0] ^= 1,
                3 => wrong.subscription_generation += 1,
                4 => wrong.channel_epoch += 1,
                5 => wrong.ring_epoch += 1,
                6 => wrong.membership_version += 1,
                _ => wrong.evidence_kind = 1,
            }
            assert!(verify_request(&request, &wire_bytes(&wrong).unwrap(), &join).is_err());
        }
        let mut proof = body(&join);
        proof.evidence_kind = 1;
        proof.applied_seq = 11;
        proof.released_seq = 11;
        proof.membership_version = 10;
        let envelope = ControlEnvelope {
            opcode: 49,
            flags: 10,
            ..request.clone()
        };
        let raw = wire_bytes(&status(&request, &proof).unwrap()).unwrap();
        assert!(verify_applied(&envelope, &raw, &request, &proof).is_ok());
        proof.released_seq += 1;
        assert!(verify_applied(&envelope, &raw, &request, &proof).is_err());
        proof.evidence_kind = 0;
        assert!(status(&request, &proof).is_err());
        let mut wrong = envelope.clone();
        wrong.control_epoch += 1;
        assert!(verify_applied(&wrong, &raw, &request, &proof).is_err());
    }
}
