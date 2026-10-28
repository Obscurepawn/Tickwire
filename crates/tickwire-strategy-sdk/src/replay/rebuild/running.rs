use super::*;

/// The same application that executed the verified warm window; no authority.
pub struct RunningCandidate<S: crate::Strategy> {
    pub(super) candidate: Candidate,
    pub(super) engine: Engine<application::Application<S>>,
    pub(super) anchor: VerifiedAnchor,
}
impl<S: crate::Strategy> RunningCandidate<S> {
    pub fn checkpoint(&self) -> &Checkpoint {
        self.candidate.checkpoint()
    }
    pub fn completion(&self) -> &Completion {
        self.candidate.completion()
    }
    pub fn persist<B: tickwire_recovery::BlobStore>(
        self,
        mut store: B,
    ) -> Result<PersistedRunning<S, B>> {
        if self.engine.checkpoint()? != self.candidate.checkpoint {
            return Err(Error::StateMismatch);
        }
        let computation = self.candidate.persist(&mut store)?;
        let (owner, genesis) =
            durable::DurableEngine::from_recorded(self.anchor, self.engine, store)?;
        Ok(PersistedRunning {
            computation,
            owner,
            genesis,
        })
    }
}
pub struct PersistedRunning<S: crate::Strategy, B> {
    pub(crate) computation: Persisted,
    pub(crate) owner: durable::DurableEngine<application::Application<S>, B>,
    pub(crate) genesis: Object,
}
impl<S: crate::Strategy, B: tickwire_recovery::BlobStore> PersistedRunning<S, B> {
    pub fn computation(&self) -> &Persisted {
        &self.computation
    }
    pub fn checkpoint(&self) -> &Checkpoint {
        self.owner.durable_checkpoint()
    }
    pub fn anchor(&self) -> &Checkpoint {
        self.owner.anchor()
    }
    pub fn genesis_commit(&self) -> Object {
        self.genesis
    }
    pub fn commit(&self) -> Object {
        self.owner.commit()
    }
    pub fn head(&self) -> Result<Head> {
        self.owner.head()
    }
    /// The original genesis and complete warm delta, without running callbacks.
    pub fn history_objects(&self) -> Result<Vec<Vec<u8>>> {
        let mut out = Vec::new();
        let mut seen = BTreeSet::new();
        for root in [self.genesis, self.commit()] {
            let bytes = self.owner.read_object(root)?;
            let commit = InputCommit::decode(&bytes)?;
            if seen.insert(root.digest) {
                out.push(bytes);
            }
            for object in [commit.anchor, commit.tail, commit.checkpoint] {
                if seen.insert(object.digest) {
                    out.push(self.owner.read_object(object)?);
                }
            }
        }
        Ok(out)
    }
}
