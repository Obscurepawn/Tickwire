//! Authenticated strategy replacement and replay evidence.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HandoverReadiness {
    pub instance_id: [u8; 16],
    pub strategy_generation: u64,
    pub checkpoint_digest: [u8; 32],
    pub exec_report_sequence: u64,
    pub orders_digest: [u8; 32],
    pub ledger_sequence: u64,
    pub input_seal_digest: [u8; 32],
}
/// Installation evidence from an authenticated host. The rollback probe completes release preflight.
pub struct InstalledReplay {
    root: tickwire_release::Object,
    verified: tickwire_release::replay::install::Verified,
    readiness: HandoverReadiness,
    worker_pid: u32,
    objects: Vec<Vec<u8>>,
}
impl InstalledReplay {
    /// The adapter must first authenticate 42/37 against its owned child and
    /// registered report cursor. Content is independently checked here.
    pub fn from_authenticated_child(
        root: tickwire_release::Object,
        source: &tickwire_release::replay::source::VerifiedSourceReplay,
        readiness: HandoverReadiness,
        worker_pid: u32,
        objects: Vec<Vec<u8>>,
    ) -> tickwire_release::Result<Self> {
        use tickwire_release::{Error, Object};
        if worker_pid == 0 || objects.len() > 512 {
            return Err(Error::Bounds);
        }
        let mut total = 0usize;
        let mut map = std::collections::BTreeMap::new();
        for bytes in objects {
            total = total
                .checked_add(bytes.len())
                .filter(|n| *n <= tickwire_recovery::MAX_CLOSURE_BYTES as usize)
                .ok_or(Error::Bounds)?;
            map.insert(Object::of(&bytes).digest, bytes);
        }
        let get = |hash: [u8; 32]| map.get(&hash).cloned().ok_or(Error::Missing);
        root.verify(
            &get(root.digest)?,
            tickwire_release::replay::MAX_BYTES as u64,
        )?;
        let verified = tickwire_release::replay::install::verify(root.digest, get)?;
        if verified.request.source_completion != source.root()
            || verified.request.operation != source.completion().operation
            || verified.request.member != source.completion().member
            || readiness.instance_id != verified.request.target.instance
            || readiness.strategy_generation != verified.request.target.generation
        {
            return Err(Error::Identity);
        }
        let seal = tickwire_recovery::HandoverSeal::decode(&get(readiness.input_seal_digest)?)
            .map_err(|_| Error::Identity)?;
        if seal.checkpoint_digest != readiness.checkpoint_digest
            || seal.report_position != readiness.exec_report_sequence
            || seal.ledger_sequence != readiness.ledger_sequence
            || seal.orders_digest != readiness.orders_digest
        {
            return Err(Error::Identity);
        }
        verified.bind_seal(
            &seal,
            &get(seal.checkpoint_digest)?,
            &get(seal.input_digest)?,
        )?;
        tickwire_release::replay::startup::verify_seal(
            &seal,
            &get(seal.checkpoint_digest)?,
            &get(seal.input_digest)?,
            get,
        )?;
        Ok(Self {
            root,
            verified,
            readiness,
            worker_pid,
            objects: map.into_values().collect(),
        })
    }
    pub fn root(&self) -> tickwire_release::Object {
        self.root
    }
    pub fn verified(&self) -> &tickwire_release::replay::install::Verified {
        &self.verified
    }
    pub fn readiness(&self) -> &HandoverReadiness {
        &self.readiness
    }
    pub fn worker_pid(&self) -> u32 {
        self.worker_pid
    }
    pub fn objects(&self) -> &[Vec<u8>] {
        &self.objects
    }
}
/// Actual installation followed by an owned C-state rollback probe and a fresh
/// authenticated target READY containing that exact durable completion root.
pub struct PreparedReplay {
    installed: InstalledReplay,
    root: tickwire_release::Object,
    readiness: HandoverReadiness,
    objects: Vec<Vec<u8>>,
}
impl PreparedReplay {
    pub fn from_authenticated_ready(
        installed: InstalledReplay,
        root: tickwire_release::Object,
        readiness: HandoverReadiness,
        objects: Vec<Vec<u8>>,
    ) -> tickwire_release::Result<Self> {
        use tickwire_release::{Error, Object};
        if objects.len() > 512 {
            return Err(Error::Bounds);
        }
        let mut total = 0usize;
        let mut map = std::collections::BTreeMap::new();
        for bytes in objects {
            total = total
                .checked_add(bytes.len())
                .filter(|n| *n <= tickwire_recovery::MAX_CLOSURE_BYTES as usize)
                .ok_or(Error::Bounds)?;
            map.insert(Object::of(&bytes).digest, bytes);
        }
        let get = |h| map.get(&h).cloned().ok_or(Error::Missing);
        root.verify(
            &get(root.digest)?,
            tickwire_release::replay::MAX_BYTES as u64,
        )?;
        let done = tickwire_release::replay::rollback::verify(root.digest, get)?;
        if done.receipt.installation != installed.root
            || done.installation.request != installed.verified.request
        {
            return Err(Error::Identity);
        }
        let seal = tickwire_recovery::HandoverSeal::decode(&get(readiness.input_seal_digest)?)
            .map_err(|_| Error::Identity)?;
        if seal.replay_rollback_digest != Some(root.digest)
            || seal.instance != readiness.instance_id
            || seal.generation != readiness.strategy_generation
            || seal.checkpoint_digest != readiness.checkpoint_digest
            || seal.report_position != readiness.exec_report_sequence
            || seal.ledger_sequence != readiness.ledger_sequence
            || seal.orders_digest != readiness.orders_digest
        {
            return Err(Error::Identity);
        }
        tickwire_release::replay::startup::verify_seal(
            &seal,
            &get(seal.checkpoint_digest)?,
            &get(seal.input_digest)?,
            get,
        )?;
        Ok(Self {
            installed,
            root,
            readiness,
            objects: map.into_values().collect(),
        })
    }
    pub fn installed(&self) -> &InstalledReplay {
        &self.installed
    }
    pub fn root(&self) -> tickwire_release::Object {
        self.root
    }
    pub fn readiness(&self) -> &HandoverReadiness {
        &self.readiness
    }
    pub fn objects(&self) -> &[Vec<u8>] {
        &self.objects
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StrategySwitch {
    pub logical_strategy_id: u64,
    pub old_instance: [u8; 16],
    pub old_generation: u64,
    pub new_instance: [u8; 16],
    pub new_generation: u64,
}
