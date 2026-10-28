use super::*;
impl Verified {
    /// Bind the original warm engine to a later complete retained input tail.
    pub fn bind_seal(
        &self,
        seal: &tickwire_recovery::HandoverSeal,
        checkpoint: &[u8],
        inputs: &[u8],
    ) -> Result<()> {
        seal.verify_checkpoint(checkpoint)
            .map_err(|_| Error::Identity)?;
        if seal.version != 4
            || seal.rebuild_installation_digest != Some(self.root().digest)
            || seal.instance != self.installation().target.instance
            || seal.generation != self.installation().target.generation
            || seal.logical_strategy != self.installation().target.logical_strategy
            || Object::of(inputs)
                != (Object {
                    digest: seal.input_digest,
                    bytes: seal.input_bytes,
                })
        {
            return Err(Error::Identity);
        }
        let retained = replay::retained::decode(inputs)?;
        retained.tail_from(self.checkpoint())?;
        let current = retained.current();
        let account = &self.checkpoint().runtime.accounts[0];
        if Object::of(&retained.anchor().checkpoint().bytes()?) != self.installation().anchor
            || current.runtime.accounts.len() != 1
            || current.runtime.accounts[0].stream != account.stream
            || current.runtime.accounts[0].sequence != seal.report_position
            || replay::install::business_state(current)? != checkpoint[96..]
        {
            return Err(Error::Identity);
        }
        Ok(())
    }
}
/// Read-only v4 dependency verification. Initial installation READY may lack a
/// rollback; callers authorizing handover or using an old source require it.
pub fn verify_seal(
    seal: &tickwire_recovery::HandoverSeal,
    checkpoint: &[u8],
    inputs: &[u8],
    mut read: impl FnMut(Hash) -> Result<Vec<u8>>,
) -> Result<Vec<Vec<u8>>> {
    verify_seal_at(seal, checkpoint, inputs, &mut read, 0)
}
pub(crate) fn verify_seal_at(
    seal: &tickwire_recovery::HandoverSeal,
    checkpoint: &[u8],
    inputs: &[u8],
    read: &mut dyn FnMut(Hash) -> Result<Vec<u8>>,
    depth: usize,
) -> Result<Vec<Vec<u8>>> {
    if depth >= 16 {
        return Err(Error::Bounds);
    }
    seal.validate().map_err(|_| Error::Identity)?;
    if seal.version != 4 {
        return Err(Error::Identity);
    }
    let mut objects = std::collections::BTreeMap::<Hash, Vec<u8>>::new();
    let mut total = 0usize;
    let mut get = |hash: Hash| -> Result<Vec<u8>> {
        if let Some(bytes) = objects.get(&hash) {
            return Ok(bytes.clone());
        }
        if hash == [0; 32] || objects.len() >= 512 {
            return Err(Error::Bounds);
        }
        let bytes = read(hash)?;
        if bytes.is_empty()
            || bytes.len() > tickwire_recovery::MAX_OBJECT_BYTES
            || Object::of(&bytes).digest != hash
        {
            return Err(Error::Identity);
        }
        total = total
            .checked_add(bytes.len())
            .filter(|n| *n <= tickwire_recovery::MAX_CLOSURE_BYTES as usize)
            .ok_or(Error::Bounds)?;
        objects.insert(hash, bytes.clone());
        Ok(bytes)
    };
    let root = seal.rebuild_installation_digest.ok_or(Error::Missing)?;
    let object = Object::of(&get(root)?);
    let installed = verify(object, &mut get)?;
    installed.bind_seal(seal, checkpoint, inputs)?;
    if let Some(root) = seal.rebuild_rollback_digest {
        let request = installed.request();
        let plans = Plans::decode(&get(request.plans.digest)?)?;
        let manifest: ReleaseManifest = super::super::decode(&get(request.manifest.digest)?)?;
        let window = window::Seal::decode(&get(request.capture_seal.digest)?)?;
        let contract = Contract::decode(&get(window.contract.digest)?)?;
        let inputs = Inputs::decode(&get(window.inputs.digest)?)?;
        super::super::probe::Verified::verify_at(
            Object::of(&get(root)?),
            &request.operation,
            &request.member,
            rollback::Forward {
                plans: &plans,
                contract: &contract,
                inputs: &inputs,
                manifest: &manifest,
            },
            &mut get,
            depth + 1,
        )?;
    }
    Ok(objects.into_values().collect())
}
