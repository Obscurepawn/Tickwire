use crate::*;
pub(crate) fn verify<S: BlobStore>(
    p: &mut Persistence<S>,
    stream: &Stream,
    e: &Entry,
) -> Result<()> {
    if !matches!(stream.format, Format::Account { .. }) || e.kind != 15 {
        return Ok(());
    }
    let bytes = p.read_object(e.payload.hash, e.payload.bytes as usize)?;
    let record =
        tickwire_account_codec::CanonicalRecord::decode(&bytes).map_err(|_| Error::Conflict)?;
    let tickwire_account::RecoveryRecord::IncomeObservationsAssociated(binding) = record.record()
    else {
        return Err(Error::Conflict);
    };
    let load = |p: &Persistence<S>, hash: Hash| -> Result<Vec<u8>> {
        let object = e
            .extra_objects
            .iter()
            .find(|o| o.hash == hash)
            .ok_or(Error::Missing)?;
        Ok(p.read_object(hash, object.bytes as usize)?)
    };
    let manifest = load(p, binding.manifest_digest)?;
    let approval = load(p, binding.approval_digest)?;
    let roots = tickwire_binance_account::reconciliation::income_association::dependency_hashes(
        binding, &manifest, &approval,
    )
    .map_err(|_| Error::Conflict)?;
    require_roots(p, &e.extra_objects, &roots)
}
fn require_roots<S: BlobStore>(
    p: &Persistence<S>,
    objects: &[ObjectRef],
    roots: &[Hash],
) -> Result<()> {
    for root in roots {
        let object = objects
            .iter()
            .find(|o| o.hash == *root)
            .ok_or(Error::Missing)?;
        if p.read_object(*root, object.bytes as usize)?.len() as u64 != object.bytes {
            return Err(Error::Conflict);
        }
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn every_declared_association_root_must_be_in_the_transported_objects() {
        let mut p = Persistence::new(tickwire_recovery::MockBlobStore::default());
        let objects = (1u8..=6)
            .map(|n| {
                let b = vec![n];
                p.put_object(&b).unwrap();
                ObjectRef::new(&b).unwrap()
            })
            .collect::<Vec<_>>();
        let roots = objects.iter().map(|o| o.hash).collect::<Vec<_>>();
        require_roots(&p, &objects, &roots).unwrap();
        for missing in 0..objects.len() {
            let mut subset = objects.clone();
            subset.remove(missing);
            assert!(matches!(
                require_roots(&p, &subset, &roots),
                Err(Error::Missing)
            ));
        }
    }
}
