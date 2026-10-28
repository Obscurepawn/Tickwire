use crate::{Result, Scenario};
use serde_json::{Value, json};
use std::fs;
use tickwire_recovery::{HandoverSeal, digest};
use tickwire_release::{
    Object,
    replay::{
        Checkpoint,
        retained::{self, RecordedCut},
        source::VerifiedSourceReplay,
    },
};

pub(super) fn verify_prefix(base: &Checkpoint, candidate: &RecordedCut) -> Result<()> {
    candidate.tail_from(base)?;
    Ok(())
}
fn recorded(
    proof: Object,
    mut read: impl FnMut([u8; 32]) -> Result<Vec<u8>>,
) -> Result<RecordedCut> {
    let bytes = read(proof.digest)?;
    proof.verify(&bytes, tickwire_release::replay::MAX_BYTES as u64)?;
    let seal = HandoverSeal::decode(&bytes)?;
    let inputs = read(seal.input_digest)?;
    Object {
        digest: seal.input_digest,
        bytes: seal.input_bytes,
    }
    .verify(&inputs, tickwire_release::replay::MAX_BYTES as u64)?;
    Ok(retained::decode(&inputs)?)
}
/// Bind the independently observed expansion to the actual source proof used
/// by the target install and rollback, including both complete checkpoints.
pub(crate) fn verify_completed(s: &Scenario, source: &VerifiedSourceReplay) -> Result<Value> {
    let evidence: Value = super::read(&s.root.join("market-expanded.json"))?;
    let before = &evidence["source_before_c"];
    let fixed_proof: Object = serde_json::from_value(before["source_s"]["seal"].clone())?;
    let observed_proof: Object = serde_json::from_value(before["seal"].clone())?;
    if source.completion().fixed_proof != fixed_proof {
        return Err("expansion fixed source seal changed".into());
    }
    let source_read = |hash| -> Result<Vec<u8>> {
        source
            .objects()
            .find(|b| digest(b) == hash)
            .map(Vec::from)
            .ok_or_else(|| "expansion source closure object missing".into())
    };
    let fixed = recorded(fixed_proof, source_read)?;
    let current = recorded(source.completion().current_proof, source_read)?;
    let observed = recorded(observed_proof, |hash| {
        Ok(fs::read(s.root.join(format!(
            "strategy/recovery/{}.bin",
            super::hex(&hash)
        )))?)
    })?;
    let object = |c: &Checkpoint| -> Result<Object> { Ok(Object::of(&c.bytes()?)) };
    if fixed.current() != source.fixed()
        || current.current() != source.current()
        || before["source_s"]["checkpoint"] != json!(object(fixed.current())?)
        || before["source_s"]["replay_cut"] != json!(fixed.current().cut)
        || before["checkpoint"] != json!(object(observed.current())?)
        || before["replay_cut"] != json!(observed.current().cut)
    {
        return Err("expansion complete source checkpoint changed".into());
    }
    verify_prefix(fixed.current(), &observed)?;
    verify_prefix(observed.current(), &current)?;
    Ok(json!({
        "fixed_proof":fixed_proof,"fixed_checkpoint":object(fixed.current())?,
        "observed_proof":observed_proof,"observed_checkpoint":object(observed.current())?,
        "current_proof":source.completion().current_proof,"current_checkpoint":object(current.current())?,
        "fixed_cut":fixed.current().cut,"observed_cut":observed.current().cut,"current_cut":current.current().cut,
        "source_completion":source.root(),"prefix_verified":true
    }))
}
