//! Complete candidate preparation; no deployment or commit authority.
use crate::{
    Candidate, Error, Hash, Limits, Result,
    changes::{Authority, ChangeSet, Role},
    patch::{Head, ScopedPatch},
};

pub enum Input<'a> {
    Full(&'a Candidate),
    Patch(&'a ScopedPatch),
}
pub struct PreparedCandidate {
    base_hash: Hash,
    candidate: Candidate,
    changes: ChangeSet,
}
impl PreparedCandidate {
    pub fn prepare(
        head: &Head,
        if_match: &str,
        base: &Candidate,
        input: Input<'_>,
        authority: &Authority,
        limits: Limits,
    ) -> Result<Self> {
        let candidate = match input {
            Input::Full(candidate) => {
                if !authority.covers(&[Role::Editor], &Default::default(), true) {
                    return Err(Error::Semantic("authorization"));
                }
                if head.hash() != base.hash() || head.etag() != if_match {
                    return Err(Error::Revision);
                }
                candidate.normalized_configuration(limits)?
            }
            Input::Patch(patch) => patch
                .apply(head, if_match, base, authority)?
                .normalized_configuration(limits)?,
        };
        let comparison = base.normalized_configuration(limits)?;
        let changes = ChangeSet::between(&comparison, &candidate)?;
        changes.authorize_plan(authority)?;
        Ok(Self {
            base_hash: base.hash(),
            candidate,
            changes,
        })
    }
    pub fn base_hash(&self) -> Hash {
        self.base_hash
    }
    pub fn candidate(&self) -> &Candidate {
        &self.candidate
    }
    pub fn changes(&self) -> &ChangeSet {
        &self.changes
    }
}
