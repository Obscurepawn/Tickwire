use super::*;
use replay::{Checkpoint, Head, InputCommit};
/// Verified immutable content only. The caller separately authenticates the
/// producing child, its sessions, mapped cursors and current deadline.
pub struct Verified {
    root: Object,
    request: Request,
    installation: Installation,
    checkpoint: Checkpoint,
    completion: Completion,
    objects: std::collections::BTreeMap<Hash, Vec<u8>>,
}
impl Verified {
    pub fn request(&self) -> &Request {
        &self.request
    }
    pub fn root(&self) -> Object {
        self.root
    }
    pub fn installation(&self) -> &Installation {
        &self.installation
    }
    pub fn checkpoint(&self) -> &Checkpoint {
        &self.checkpoint
    }
    pub fn completion(&self) -> &Completion {
        &self.completion
    }
    pub fn objects(&self) -> impl Iterator<Item = &[u8]> {
        self.objects.values().map(Vec::as_slice)
    }
    #[allow(clippy::too_many_arguments)]
    pub fn verify(
        root: Object,
        request: &Request,
        plans: &Plans,
        contract: &Contract,
        inputs: &Inputs,
        manifest: &ReleaseManifest,
        seal: &window::Seal,
        mut get: impl FnMut(Object) -> Result<Vec<u8>>,
    ) -> Result<Self> {
        let plan = request.verify(plans, contract, inputs, manifest, seal)?;
        let mut objects: std::collections::BTreeMap<Hash, Vec<u8>> =
            std::collections::BTreeMap::new();
        let mut total = 0u64;
        let mut read = |object: Object| -> Result<Vec<u8>> {
            object.validate(replay::MAX_BYTES as u64)?;
            if let Some(bytes) = objects.get(&object.digest) {
                object.verify(bytes, replay::MAX_BYTES as u64)?;
                return Ok(bytes.clone());
            }
            total = total
                .checked_add(object.bytes)
                .filter(|n| *n <= tickwire_recovery::MAX_CLOSURE_BYTES)
                .ok_or(Error::Bounds)?;
            let bytes = get(object)?;
            object.verify(&bytes, replay::MAX_BYTES as u64)?;
            objects.insert(object.digest, bytes.clone());
            Ok(bytes)
        };
        let installation = Installation::decode(&read(root)?)?;
        if installation.request != Object::of(&request.bytes()?)
            || installation.capture_completion != request.capture_completion
            || installation.capture_seal != request.capture_seal
            || installation.capture_receipt != request.capture_receipt
            || installation.target != plan.target
            || installation.completed_ns < seal.end.now_ns
            || installation.completed_ns >= request.deadline_mono_ns
        {
            return Err(Error::Identity);
        }
        if read(installation.request)? != request.bytes()?
            || read(request.plans)? != plans.bytes()?
            || read(request.manifest)? != crate::encode(manifest)?
            || read(request.capture_seal)? != seal.bytes()?
        {
            return Err(Error::Identity);
        }
        super::capture::verify(&installation, request, contract, inputs, seal, &mut read)?;
        read(request.target_bootstrap)?;
        let completion = Completion::decode(&read(installation.warm_completion)?)?;
        if completion.schema != 1
            || completion.operation != Object::of(&replay::canonical(&request.operation)?)
            || completion.plans != request.plans
            || completion.manifest != request.manifest
            || completion.contract != plan.contract
            || completion.inputs != plan.inputs
            || completion.anchor != installation.anchor
            || completion.checkpoint != installation.checkpoint
            || completion.account_position != seal.end.accounts[0].sequence
            || completion.account_seal != seal.account_input_digest
            || completion.consumed_actions
                != contract
                    .actions
                    .iter()
                    .map(|a| a.economic_intent)
                    .collect::<Vec<_>>()
            || completion.observations.len() != contract.markets.len()
        {
            return Err(Error::Identity);
        }
        if completion
            .observations
            .iter()
            .zip(&contract.markets)
            .any(|(o, r)| {
                o.stream != r.stream
                    || o.instrument != r.instrument
                    || o.category != r.category
                    || o.count < r.minimum_observations
            })
        {
            return Err(Error::Identity);
        }
        if read(completion.operation)? != replay::canonical(&request.operation)?
            || read(completion.contract)? != contract.bytes()?
            || read(completion.inputs)? != inputs.bytes()?
        {
            return Err(Error::Identity);
        }
        let head = Head::decode(&read(installation.head)?)?;
        let commit = InputCommit::decode(&read(installation.commit)?)?;
        let genesis = InputCommit::decode(&read(installation.genesis_commit)?)?;
        if commit.schema != 2
            || commit.previous_commit != Some(installation.genesis_commit)
            || commit.anchor != installation.anchor
            || commit.checkpoint != installation.checkpoint
            || commit.tail != completion.transcript
            || genesis.schema != 2
            || genesis.previous_commit.is_some()
            || genesis.anchor != installation.anchor
            || genesis.checkpoint != installation.anchor
        {
            return Err(Error::Identity);
        }
        let history = replay::history::verify(installation.commit, 2, &mut read)?;
        let checkpoint = history.checkpoint().clone();
        let start = history.anchor().checkpoint();
        if Object::of(&checkpoint.bytes()?) != installation.checkpoint
            || checkpoint.source != plan.target
            || checkpoint.cut.sequence != plan.end.rows
            || checkpoint.runtime.now_ns != plan.end.now_ns
            || checkpoint.runtime.markets != plan.end.markets
            || checkpoint.runtime.accounts != plan.end.accounts
            || checkpoint.runtime.external != plan.end.external
            || checkpoint.runtime.config != inputs.config
            || checkpoint.runtime.models != inputs.models
            || checkpoint.runtime.effects.count != 0
            || checkpoint.runtime.effects.digest != [0; 32]
            || head.source != plan.target
            || head.commit != installation.commit
            || head.checkpoint != installation.checkpoint
            || head.cut != checkpoint.cut
            || history.anchor().checkpoint().cut.sequence != 0
            || start.cut.chain != [0; 32]
            || start.runtime.now_ns != inputs.start_ns
            || start.runtime.markets != seal.start.markets
            || start.runtime.accounts != seal.start.accounts
            || start.runtime.external != seal.start.external
            || start.runtime.config != inputs.config
            || start.runtime.models != inputs.models
            || start.runtime.rng != contract.rng
            || !start.runtime.timers.is_empty()
            || start.runtime.effects.count != 0
            || start.runtime.effects.digest != [0; 32]
            || history.tail().rows().len() != inputs.warm.len()
            || history
                .tail()
                .rows()
                .iter()
                .zip(&inputs.warm)
                .any(|(a, b)| a.input != b.input || a.now_ns != b.now_ns || !a.effects.is_empty())
        {
            return Err(Error::Identity);
        }
        Ok(Self {
            root,
            request: request.clone(),
            installation,
            checkpoint,
            completion,
            objects,
        })
    }
}
