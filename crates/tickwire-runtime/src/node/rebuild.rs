//! Content checks for Host-owned rebuild stages. Constructors require the
//! caller to authenticate the child and its current report cursor first.
use super::HandoverReadiness;
use std::collections::BTreeMap;
use tickwire_release::{
    Error, Hash, Object, ReleaseManifest, Result,
    rebuild::{Contract, Inputs, Plan, Plans, install, rollback, window::Seal},
    replay::MAX_BYTES,
};

#[derive(Clone)]
pub struct RebuildInput {
    request: install::Request,
    plans: Plans,
    contract: Contract,
    inputs: Inputs,
    manifest: ReleaseManifest,
    seal: Seal,
    rollback_plans: rollback::Plans,
    worker_pid: u32,
    objects: Vec<Vec<u8>>,
}
impl RebuildInput {
    /// The Host first verifies the actual capture receipt, PID and sealed input
    /// closure. This constructor verifies publication content without granting
    /// permission to execute a factory or forward a command.
    #[allow(clippy::too_many_arguments)]
    pub fn from_authenticated_child(
        request: install::Request,
        plans: Plans,
        contract: Contract,
        inputs: Inputs,
        manifest: ReleaseManifest,
        seal: Seal,
        rollback_plans: rollback::Plans,
        worker_pid: u32,
        objects: Vec<Vec<u8>>,
    ) -> Result<Self> {
        if worker_pid == 0 {
            return Err(Error::Identity);
        }
        request.verify(&plans, &contract, &inputs, &manifest, &seal)?;
        let map = index(objects)?;
        for bytes in [
            request.bytes()?,
            plans.bytes()?,
            contract.bytes()?,
            inputs.bytes()?,
            tickwire_release::replay::canonical(&manifest)?,
            seal.bytes()?,
            rollback_plans.bytes()?,
        ] {
            if get(&map, Object::of(&bytes))? != bytes {
                return Err(Error::Identity);
            }
        }
        for root in [
            request.capture_completion,
            request.capture_receipt,
            request.target_bootstrap,
        ] {
            get(&map, root)?;
        }
        rollback_plans.verify(
            &request.operation,
            &request.member,
            rollback::Forward {
                plans: &plans,
                contract: &contract,
                inputs: &inputs,
                manifest: &manifest,
            },
            |hash| map.get(&hash).cloned().ok_or(Error::Missing),
        )?;
        Ok(Self {
            request,
            plans,
            contract,
            inputs,
            manifest,
            seal,
            rollback_plans,
            worker_pid,
            objects: map.into_values().collect(),
        })
    }
    pub fn request(&self) -> &install::Request {
        &self.request
    }
    pub fn plans(&self) -> &Plans {
        &self.plans
    }
    pub fn plan(&self) -> &Plan {
        self.plans
            .members
            .iter()
            .find(|p| p.member == self.request.member)
            .expect("verified rebuild member")
    }
    pub fn contract(&self) -> &Contract {
        &self.contract
    }
    pub fn inputs(&self) -> &Inputs {
        &self.inputs
    }
    pub fn manifest(&self) -> &ReleaseManifest {
        &self.manifest
    }
    pub fn seal(&self) -> &Seal {
        &self.seal
    }
    pub fn rollback_plans(&self) -> &rollback::Plans {
        &self.rollback_plans
    }
    pub fn worker_pid(&self) -> u32 {
        self.worker_pid
    }
    pub fn objects(&self) -> &[Vec<u8>] {
        &self.objects
    }
}

pub struct InstalledRebuild {
    input: RebuildInput,
    verified: install::Verified,
    readiness: HandoverReadiness,
    objects: Vec<Vec<u8>>,
}
impl InstalledRebuild {
    pub fn from_authenticated_child(
        input: RebuildInput,
        root: Object,
        readiness: HandoverReadiness,
        objects: Vec<Vec<u8>>,
    ) -> Result<Self> {
        let map = index(objects.into_iter().chain(input.objects.iter().cloned()))?;
        let verified = install::Verified::verify(
            root,
            &input.request,
            &input.plans,
            &input.contract,
            &input.inputs,
            &input.manifest,
            &input.seal,
            |object| get(&map, object),
        )?;
        if verified.installation().pid != input.worker_pid {
            return Err(Error::Identity);
        }
        verify_ready(&verified, &readiness, &map)?;
        Ok(Self {
            input,
            verified,
            readiness,
            objects: map.into_values().collect(),
        })
    }
    pub fn input(&self) -> &RebuildInput {
        &self.input
    }
    pub fn verified(&self) -> &install::Verified {
        &self.verified
    }
    pub fn root(&self) -> Object {
        self.verified.root()
    }
    pub fn readiness(&self) -> &HandoverReadiness {
        &self.readiness
    }
    pub fn objects(&self) -> &[Vec<u8>] {
        &self.objects
    }
}

pub struct PreparedRebuild {
    installed: InstalledRebuild,
    rollback_completion: Object,
    readiness: HandoverReadiness,
    objects: Vec<Vec<u8>>,
}
impl PreparedRebuild {
    /// Host must first observe the independently owned rollback probe's real
    /// successful exit and verify its complete persisted result.
    pub fn from_authenticated_ready(
        installed: InstalledRebuild,
        rollback_completion: Object,
        readiness: HandoverReadiness,
        objects: Vec<Vec<u8>>,
    ) -> Result<Self> {
        let map = index(objects.into_iter().chain(installed.objects.iter().cloned()))?;
        get(&map, rollback_completion)?;
        let seal = tickwire_recovery::HandoverSeal::decode(
            map.get(&readiness.input_seal_digest)
                .ok_or(Error::Missing)?,
        )
        .map_err(|_| Error::Identity)?;
        if seal.rebuild_rollback_digest != Some(rollback_completion.digest) {
            return Err(Error::Identity);
        }
        verify_ready(&installed.verified, &readiness, &map)?;
        Ok(Self {
            installed,
            rollback_completion,
            readiness,
            objects: map.into_values().collect(),
        })
    }
    pub fn installed(&self) -> &InstalledRebuild {
        &self.installed
    }
    pub fn rollback_completion(&self) -> Object {
        self.rollback_completion
    }
    pub fn root(&self) -> Object {
        self.rollback_completion
    }
    pub fn readiness(&self) -> &HandoverReadiness {
        &self.readiness
    }
    pub fn objects(&self) -> &[Vec<u8>] {
        &self.objects
    }
}

fn index(objects: impl IntoIterator<Item = Vec<u8>>) -> Result<BTreeMap<Hash, Vec<u8>>> {
    let mut map = BTreeMap::new();
    let mut bytes = 0usize;
    for (count, object) in objects.into_iter().enumerate() {
        if count >= 1024 || object.is_empty() || object.len() > tickwire_recovery::MAX_OBJECT_BYTES
        {
            return Err(Error::Bounds);
        }
        let root = Object::of(&object);
        if let Some(prior) = map.get(&root.digest) {
            if prior != &object {
                return Err(Error::Identity);
            }
            continue;
        }
        bytes = bytes
            .checked_add(object.len())
            .filter(|n| *n <= tickwire_recovery::MAX_CLOSURE_BYTES as usize)
            .ok_or(Error::Bounds)?;
        if map.len() >= 512 {
            return Err(Error::Bounds);
        }
        map.insert(root.digest, object);
    }
    Ok(map)
}
fn get(map: &BTreeMap<Hash, Vec<u8>>, object: Object) -> Result<Vec<u8>> {
    let bytes = map.get(&object.digest).ok_or(Error::Missing)?;
    object.verify(bytes, MAX_BYTES as u64)?;
    Ok(bytes.clone())
}
pub fn verify_rebuild_ready(
    verified: &install::Verified,
    ready: &HandoverReadiness,
    objects: &[Vec<u8>],
) -> Result<()> {
    verify_ready(verified, ready, &index(objects.iter().cloned())?)
}
fn verify_ready(
    verified: &install::Verified,
    ready: &HandoverReadiness,
    map: &BTreeMap<Hash, Vec<u8>>,
) -> Result<()> {
    let raw = map.get(&ready.input_seal_digest).ok_or(Error::Missing)?;
    let seal = tickwire_recovery::HandoverSeal::decode(raw).map_err(|_| Error::Identity)?;
    if seal.version != 4
        || seal.rebuild_installation_digest != Some(verified.root().digest)
        || seal.instance != ready.instance_id
        || seal.generation != ready.strategy_generation
        || seal.instance != verified.installation().target.instance
        || seal.generation != verified.installation().target.generation
        || seal.logical_strategy != verified.installation().target.logical_strategy
        || seal.checkpoint_digest != ready.checkpoint_digest
        || seal.report_position != ready.exec_report_sequence
        || seal.ledger_sequence != ready.ledger_sequence
        || seal.orders_digest != ready.orders_digest
    {
        return Err(Error::Identity);
    }
    let checkpoint = get(
        map,
        Object {
            digest: seal.checkpoint_digest,
            bytes: seal.checkpoint_bytes,
        },
    )?;
    let inputs = get(
        map,
        Object {
            digest: seal.input_digest,
            bytes: seal.input_bytes,
        },
    )?;
    let account = get(
        map,
        Object {
            digest: seal.account_input_digest,
            bytes: seal.account_input_bytes,
        },
    )?;
    tickwire_handover::verify(&seal, &checkpoint, &inputs, &account)
        .map_err(|_| Error::Identity)?;
    install::verify_seal(&seal, &checkpoint, &inputs, |hash| {
        map.get(&hash).cloned().ok_or(Error::Missing)
    })?;
    let retained = tickwire_release::replay::retained::decode(&inputs)?;
    if retained.current().source != verified.installation().target
        || Object::of(&retained.anchor().checkpoint().bytes()?) != verified.installation().anchor
    {
        return Err(Error::Identity);
    }
    retained.tail_from(verified.checkpoint())?;
    Ok(())
}
