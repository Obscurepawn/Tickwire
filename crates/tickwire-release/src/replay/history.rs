//! Bounded verification of cumulative and delta input commits. This checks
//! immutable content and cuts, not callback execution or runtime authority.
use super::*;

pub struct CommittedHistory {
    anchor: VerifiedAnchor,
    tail: VerifiedTail,
    checkpoint: Checkpoint,
}
impl CommittedHistory {
    pub fn anchor(&self) -> &VerifiedAnchor {
        &self.anchor
    }
    pub fn tail(&self) -> &VerifiedTail {
        &self.tail
    }
    pub fn checkpoint(&self) -> &Checkpoint {
        &self.checkpoint
    }
    pub fn into_parts(self) -> (VerifiedAnchor, VerifiedTail, Checkpoint) {
        (self.anchor, self.tail, self.checkpoint)
    }
}
/// Follow every schema-2 predecessor to its exact checkpoint; a schema-1
/// complete prefix is a verified boundary. The reader must provide bounded
/// reads; this function checks every returned length/digest and total budget.
pub fn verify(
    root: Object,
    maximum_commits: usize,
    mut get: impl FnMut(Object) -> Result<Vec<u8>>,
) -> Result<CommittedHistory> {
    if maximum_commits == 0 || maximum_commits > MAX_ROWS + 1 {
        return Err(Error::Bounds);
    }
    let mut seen_objects = std::collections::BTreeSet::new();
    let mut total_objects = 0usize;
    let mut read = |object: Object| -> Result<Vec<u8>> {
        object.validate(MAX_BYTES as u64)?;
        let bytes = get(object)?;
        object.verify(&bytes, MAX_BYTES as u64)?;
        if seen_objects.insert(object.digest) {
            total_objects = total_objects
                .checked_add(bytes.len())
                .filter(|n| *n <= tickwire_recovery::MAX_CLOSURE_BYTES as usize)
                .ok_or(Error::Bounds)?;
        }
        Ok(bytes)
    };
    let mut commit_object = root;
    let first = InputCommit::decode(&read(root)?)?;
    let endpoint_checkpoint = Checkpoint::decode(&read(first.checkpoint)?)?;
    let source = endpoint_checkpoint.source;
    let mut checkpoint_object = first.checkpoint;
    let mut seen_commits = std::collections::BTreeSet::new();
    let mut endpoint = None;
    let mut segments = Vec::new();
    let mut total_rows = 0usize;
    let mut total_bytes = 68usize;
    for _ in 0..maximum_commits {
        if !seen_commits.insert(commit_object.digest) {
            return Err(Error::Identity);
        }
        let commit = InputCommit::decode(&read(commit_object)?)?;
        if commit.checkpoint != checkpoint_object {
            return Err(Error::Identity);
        }
        let checkpoint = Checkpoint::decode(&read(checkpoint_object)?)?;
        let anchor_bytes = read(commit.anchor)?;
        let anchor = Checkpoint::decode(&anchor_bytes)?;
        if checkpoint.source != source || anchor.source != source {
            return Err(Error::Identity);
        }
        if endpoint.is_none() {
            endpoint = Some(checkpoint.clone());
        }
        let models = anchor
            .runtime
            .models
            .iter()
            .map(|m| (m.id, Object::of(&m.value.bytes)))
            .collect::<Vec<_>>();
        let anchor = verify_anchor(
            &anchor_bytes,
            &source,
            Object::of(&anchor.runtime.config.bytes),
            &models,
        )?;
        let bytes = read(commit.tail)?;
        let tail = verify_tail(&anchor, &bytes, &checkpoint.cut)?;
        total_rows = total_rows
            .checked_add(tail.rows().len())
            .filter(|n| *n <= MAX_ROWS)
            .ok_or(Error::Bounds)?;
        total_bytes = total_bytes
            .checked_add(bytes.len() - 68)
            .filter(|n| *n <= MAX_BYTES)
            .ok_or(Error::Bounds)?;
        if commit.previous_commit.is_none()
            && (commit.anchor != commit.checkpoint || !tail.rows().is_empty())
        {
            return Err(Error::Identity);
        }
        if commit.schema == 1 || commit.previous_commit.is_none() {
            segments.push(tail.rows().to_vec());
            let rows = segments.into_iter().rev().flatten().collect::<Vec<_>>();
            let checkpoint = endpoint.ok_or(Error::Identity)?;
            let tail = verify_tail(&anchor, &encode_tail(&anchor, &rows)?, &checkpoint.cut)?;
            return Ok(CommittedHistory {
                anchor,
                tail,
                checkpoint,
            });
        }
        if tail.rows().is_empty() {
            return Err(Error::Identity);
        }
        segments.push(tail.rows().to_vec());
        checkpoint_object = commit.anchor;
        commit_object = commit.previous_commit.ok_or(Error::Identity)?;
    }
    Err(Error::Bounds)
}
