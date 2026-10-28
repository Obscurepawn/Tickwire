use super::*;
use tickwire_release::{
    ReleaseManifest, Role,
    rebuild::{Plans, rollback},
};
impl<R: RecoveryResolver + 'static> Hosted<R> {
    pub(super) fn input(
        &mut self,
        operation: &tickwire_release::Operation,
        member: &tickwire_release::Member,
    ) -> Result<RebuildInput> {
        let owner = self.capture.as_mut().ok_or(Error::NotReady)?;
        owner.poll()?;
        let captured = owner.captured.as_ref().ok_or(Error::NotReady)?;
        let mut map = BTreeMap::new();
        let reader = tickwire_recovery::FileBlobReader::open(&self.publication, self.spec.uid)?;
        let list: Vec<Object> =
            worker::read_config(&self.publication.join("objects.json"), 2 * 1024 * 1024)?;
        if list.len() > 512 || list.windows(2).any(|w| w[0].digest >= w[1].digest) {
            return Err(Error::Invalid);
        }
        let total = list.iter().try_fold(0u64, |n, o| {
            o.validate(tickwire_recovery::MAX_OBJECT_BYTES as u64)?;
            n.checked_add(o.bytes)
                .filter(|n| *n <= tickwire_recovery::MAX_CLOSURE_BYTES)
                .ok_or(tickwire_release::Error::Bounds)
        })?;
        if total == 0 {
            return Err(Error::Invalid);
        }
        for object in list {
            let bytes = reader.get_object(object.digest, tickwire_recovery::MAX_OBJECT_BYTES)?;
            object.verify(&bytes, tickwire_recovery::MAX_OBJECT_BYTES as u64)?;
            insert(&mut map, bytes)?;
        }
        for bytes in captured.objects() {
            insert(&mut map, bytes.to_vec())?;
        }
        let bootstrap = insert(
            &mut map,
            tickwire_release::replay::canonical(&self.bootstrap)?,
        )?;
        let plans = Plans::decode(
            map.get(&operation.checkpoint_digest)
                .ok_or(Error::Invalid)?,
        )?;
        let manifest: ReleaseManifest =
            serde_json::from_slice(map.get(&operation.artifacts_digest).ok_or(Error::Invalid)?)
                .map_err(|_| Error::Invalid)?;
        let rollback =
            rollback::Plans::decode(map.get(&operation.rollback_digest).ok_or(Error::Invalid)?)?;
        let request = tickwire_release::rebuild::install::Request {
            schema: 1,
            operation: operation.clone(),
            member: member.clone(),
            plans: root(&map, operation.checkpoint_digest)?,
            manifest: root(&map, operation.artifacts_digest)?,
            capture_completion: Object::of(&captured.receipt().completion.bytes()?),
            capture_seal: Object::of(&captured.window().seal().bytes()?),
            capture_receipt: Object::of(&captured.receipt().bytes()?),
            target_bootstrap: bootstrap,
            deadline_mono_ns: operation
                .valid_until_ns
                .min(owner.bootstrap.deadline_mono_ns)
                .min(owner.report.session.deadline()),
        };
        insert(&mut map, request.bytes()?)?;
        let files: Vec<tickwire_release::files::Location> = worker::read_config(
            &self.publication.join("forward-files.json"),
            2 * 1024 * 1024,
        )?;
        let target = manifest
            .targets
            .iter()
            .find(|t| &t.member == member)
            .ok_or(Error::Invalid)?;
        let loaded = tickwire_release::files::load(&target.files, &files)?;
        if loaded.path(&Role::Binary) != Some(self.spec.executable.as_path())
            || loaded
                .object(&Role::Binary)
                .is_none_or(|o| o.digest != self.spec.identity.build_hash)
            || loaded.object(&Role::Bootstrap) != Some(bootstrap)
        {
            return Err(Error::Invalid);
        }
        loaded.recheck()?;
        let input = RebuildInput::from_authenticated_child(
            request,
            plans,
            captured.window().contract().clone(),
            captured.window().inputs().clone(),
            manifest,
            captured.window().seal().clone(),
            rollback,
            owner.child.pid,
            map.into_values().collect(),
        )?;
        let protocol = install::Request {
            request: input.request().clone(),
            bootstrap: self.bootstrap.clone(),
            files,
            objects: input.objects().iter().map(|v| Object::of(v)).collect(),
        };
        protocol.validate(
            &owner.bootstrap,
            captured.receipt(),
            captured.window(),
            owner.clock.sample()?.monotonic,
        )?;
        owner.current()?;
        self.installation = Some(protocol);
        Ok(input)
    }
}
