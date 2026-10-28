//! Pure checkpoint response binding; authentication and live cursor checks are owners' work.
use crate::{CheckpointSealBody, CheckpointSealReplyBody, Error, Result};

/// A continuous-state query uses the requested report position as a lower bound.
pub const CHECKPOINT_OBSERVE_CURRENT: u16 = 2;

pub fn validate_checkpoint_reply(
    request: &CheckpointSealBody,
    reply: &CheckpointSealReplyBody,
    previous_ready_sequence: u64,
) -> Result<()> {
    let ready = &reply.ready;
    let position_matches = if request.seal_mode == CHECKPOINT_OBSERVE_CURRENT {
        ready.exec_report_sequence >= request.report_sequence
    } else {
        ready.exec_report_sequence == request.report_sequence
    };
    if request.seal_mode > CHECKPOINT_OBSERVE_CURRENT
        || request.schema != 1
        || request.challenge == [0; 16]
        || reply.schema != 1
        || reply.seal_mode != request.seal_mode
        || reply.challenge != request.challenge
        || reply.report_sequence != request.report_sequence
        || ready.instance_id != request.instance_id
        || ready.logical_strategy_id != request.logical_strategy_id
        || ready.strategy_generation != request.strategy_generation
        || ready.ready_sequence <= previous_ready_sequence
        || !position_matches
    {
        return Err(Error::Identity);
    }
    Ok(())
}
