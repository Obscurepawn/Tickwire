//! Deterministic plan drafts. Caller-supplied revisions are not trusted runtime facts.
use crate::{
    Candidate, Error, Hash, Result,
    changes::Class,
    delivery::{DeliveryKind, ProjectionDelivery},
    patch::Head,
    prepared::PreparedCandidate,
    projection::Projection,
    strict,
};
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
mod dependencies;
mod encoded;
mod rules;
pub use encoded::EncodedPlan;
pub use rules::rules_digest;
pub const MAX_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum ApplyCeiling {
    Hot,
    Quiesce,
    RollingRestart,
}
impl ApplyCeiling {
    fn permits(self, class: Class) -> bool {
        class
            <= match self {
                Self::Hot => Class::Hot,
                Self::Quiesce => Class::Quiesce,
                Self::RollingRestart => Class::RollingRestart,
            }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum ExposurePolicy {
    RejectIfExceeded,
    FreezeUntilWithin,
    CancelThenFreeze,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PlanOptions {
    pub apply_ceiling: ApplyCeiling,
    pub exposure_policy: ExposurePolicy,
    pub requested_target_ids: BTreeSet<String>,
}
impl Default for PlanOptions {
    fn default() -> Self {
        Self {
            apply_ceiling: ApplyCeiling::RollingRestart,
            exposure_policy: ExposurePolicy::RejectIfExceeded,
            requested_target_ids: BTreeSet::new(),
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum Action {
    NoOpRevisionAdvance,
    Hot,
    Quiesce,
    RollingRestart,
    Retire,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub enum Precondition {
    CurrentAuthority,
    ContentQualification,
    RetainedRegistryHead,
    RuntimeIdentity,
    EffectiveBaseline,
    RuntimeCapability,
    Exposure,
    Quiescence,
    PlannedSuccessor,
    Retirement,
    DirectoryMigration,
    CredentialHandoff,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionBinding {
    global_hash: Hash,
    projection_hash: Hash,
    policy_revision: u64,
}
impl ProjectionBinding {
    fn of(p: &Projection) -> Self {
        Self {
            global_hash: p.global_hash(),
            projection_hash: p.hash(),
            policy_revision: p.policy_revision(),
        }
    }
    pub fn global_hash(&self) -> Hash {
        self.global_hash
    }
    pub fn projection_hash(&self) -> Hash {
        self.projection_hash
    }
    pub fn policy_revision(&self) -> u64 {
        self.policy_revision
    }
    fn value(&self) -> Value {
        json!({"global_hash":hex(self.global_hash),"projection_hash":hex(self.projection_hash),"policy_revision":self.policy_revision.to_string()})
    }
}

#[derive(Debug)]
pub struct TargetPlan {
    action: Action,
    required_class: Option<Class>,
    old_projection: Option<ProjectionBinding>,
    new_projection: Option<ProjectionBinding>,
    old_host: Option<String>,
    new_host: Option<String>,
    accounts: BTreeSet<String>,
    changes: BTreeSet<String>,
    preconditions: BTreeSet<Precondition>,
}
impl TargetPlan {
    pub fn action(&self) -> Action {
        self.action
    }
    pub fn required_class(&self) -> Option<Class> {
        self.required_class
    }
    pub fn old_projection(&self) -> Option<&ProjectionBinding> {
        self.old_projection.as_ref()
    }
    pub fn new_projection(&self) -> Option<&ProjectionBinding> {
        self.new_projection.as_ref()
    }
    pub fn old_host(&self) -> Option<&str> {
        self.old_host.as_deref()
    }
    pub fn new_host(&self) -> Option<&str> {
        self.new_host.as_deref()
    }
    pub fn accounts(&self) -> &BTreeSet<String> {
        &self.accounts
    }
    /// Escaped JSON-pointer paths; freely named parameter keys remain distinct.
    pub fn changes(&self) -> &BTreeSet<String> {
        &self.changes
    }
    /// Unverified requirements, never evidence that a target supports this action.
    pub fn preconditions(&self) -> &BTreeSet<Precondition> {
        &self.preconditions
    }
    fn value(&self) -> Value {
        json!({"action":self.action,"required_class":self.required_class.map(class_name),
            "old_projection":self.old_projection.as_ref().map(ProjectionBinding::value),
            "new_projection":self.new_projection.as_ref().map(ProjectionBinding::value),
            "old_host":self.old_host,"new_host":self.new_host,"accounts":self.accounts,
            "changes":self.changes,"preconditions":self.preconditions})
    }
}

#[derive(Debug)]
pub struct ComputedPlan {
    base_revision: u64,
    base_hash: Hash,
    base_etag: String,
    candidate_hash: Hash,
    required_class: Class,
    options: PlanOptions,
    targets: BTreeMap<String, TargetPlan>,
    preconditions: BTreeSet<Precondition>,
    canonical: Vec<u8>,
    digest: Hash,
}
impl ComputedPlan {
    pub fn verify_encoded(
        encoded: &EncodedPlan,
        base_revision: u64,
        base: &Candidate,
        prepared: &PreparedCandidate,
        delivery: &ProjectionDelivery,
    ) -> Result<Self> {
        let computed = Self::compute(
            base_revision,
            base,
            prepared,
            delivery,
            encoded.options().clone(),
        )?;
        if computed.canonical_bytes() != encoded.canonical_bytes()
            || computed.digest() != encoded.digest()
        {
            return Err(Error::Semantic("plan recomputation mismatch"));
        }
        Ok(computed)
    }
    pub fn compute(
        base_revision: u64,
        base: &Candidate,
        prepared: &PreparedCandidate,
        delivery: &ProjectionDelivery,
        options: PlanOptions,
    ) -> Result<Self> {
        let revision = base_revision.checked_add(1).ok_or(Error::Revision)?;
        if base_revision == 0 || revision >= 1 << 63 {
            return Err(Error::Revision);
        }
        if base.hash() != prepared.base_hash()
            || base.hash() != delivery.base_hash()
            || prepared.candidate().hash() != delivery.candidate_hash()
        {
            return Err(Error::Revision);
        }
        let ids: BTreeSet<_> = delivery.targets().keys().cloned().collect();
        if &ids != prepared.changes().delivery_targets()
            || !options.requested_target_ids.is_subset(&ids)
        {
            return Err(Error::Semantic("plan target closure"));
        }
        if !options.apply_ceiling.permits(prepared.changes().class()) {
            return Err(Error::Semantic("plan apply ceiling"));
        }
        // Bound path expansion before copying it into every dependent target.
        // The final exact canonical-size check remains authoritative.
        let mut source_path_bytes = 0;
        let changes = prepared
            .changes()
            .entries()
            .iter()
            .map(|change| {
                let path = pointer(change.parts())?;
                charge(&mut source_path_bytes, path.len())?;
                Ok((change, path))
            })
            .collect::<Result<Vec<_>>>()?;
        let mut copied_path_bytes = 0;
        let mut target_bytes = 0;
        let mut targets = BTreeMap::new();
        for (id, target) in delivery.targets() {
            let old = target.old_projection();
            let new = target.new_projection();
            for (projection, hash) in [(old, base.hash()), (new, prepared.candidate().hash())] {
                if let Some(p) = projection
                    && (p.global_hash() != hash || p.value()["process"]["id"] != *id)
                {
                    return Err(Error::Semantic("plan projection identity"));
                }
            }
            let mut class = None;
            let mut paths = BTreeSet::new();
            let mut accounts = BTreeSet::new();
            let mut preconditions = BTreeSet::from([
                Precondition::RuntimeIdentity,
                Precondition::RuntimeCapability,
            ]);
            if old.is_some() {
                preconditions.insert(Precondition::EffectiveBaseline);
            }
            for p in [old, new].into_iter().flatten() {
                for a in dependencies::rows(p.value(), "account_views") {
                    accounts.insert(a["id"].as_str().ok_or(Error::Schema)?.to_owned());
                }
            }
            for (change, path) in &changes {
                if dependencies::affects(change, old, new, base, prepared.candidate()) {
                    class = Some(class.map_or(change.class(), |c: Class| c.max(change.class())));
                    if !paths.contains(path) {
                        charge(&mut copied_path_bytes, path.len())?;
                        paths.insert(path.clone());
                    }
                    let parts = change.parts();
                    if parts.first().is_some_and(|g| g == "risk_policies")
                        || parts.first().is_some_and(|g| g == "strategies")
                            && parts.get(2).is_some_and(|p| p == "risk_budgets")
                    {
                        preconditions.insert(Precondition::Exposure);
                    }
                    if parts.first().is_some_and(|g| g == "local_config")
                        && parts.get(1).is_some_and(|p| p == "root_dir")
                    {
                        preconditions.insert(Precondition::DirectoryMigration);
                    }
                    if parts.last().is_some_and(|p| {
                        p.contains("credential_ref")
                            || p == "mtls_identity_ref"
                            || p == "metrics_identity_ref"
                    }) {
                        preconditions.insert(Precondition::CredentialHandoff);
                    }
                }
            }
            if matches!(target.kind(), DeliveryKind::Added | DeliveryKind::Retired) {
                class = Some(Class::RollingRestart);
            }
            if target.kind() == DeliveryKind::Changed && class.is_none() {
                return Err(Error::Semantic("unclassified projection change"));
            }
            if class.is_some_and(|class| !options.apply_ceiling.permits(class)) {
                return Err(Error::Semantic("plan apply ceiling"));
            }
            let action = if target.kind() == DeliveryKind::Retired {
                preconditions.extend([Precondition::Retirement, Precondition::Quiescence]);
                Action::Retire
            } else {
                match class {
                    None => Action::NoOpRevisionAdvance,
                    Some(Class::Hot) => Action::Hot,
                    Some(Class::Quiesce) => {
                        preconditions.insert(Precondition::Quiescence);
                        Action::Quiesce
                    }
                    Some(Class::RollingRestart) => {
                        preconditions.insert(Precondition::PlannedSuccessor);
                        if old.is_some() {
                            preconditions.insert(Precondition::Quiescence);
                        }
                        Action::RollingRestart
                    }
                    Some(Class::ForbiddenRuntime) => {
                        return Err(Error::Semantic("runtime change forbidden"));
                    }
                }
            };
            let target = TargetPlan {
                action,
                required_class: class,
                old_projection: old.map(ProjectionBinding::of),
                new_projection: new.map(ProjectionBinding::of),
                old_host: old.map(host).transpose()?,
                new_host: new.map(host).transpose()?,
                accounts,
                changes: paths,
                preconditions,
            };
            charge(&mut target_bytes, strict::canonical(&target.value())?.len())?;
            charge(&mut target_bytes, id.len())?;
            targets.insert(id.clone(), target);
        }
        let preconditions = BTreeSet::from([
            Precondition::CurrentAuthority,
            Precondition::ContentQualification,
            Precondition::RetainedRegistryHead,
        ]);
        let base_etag = Head::new(base_revision, base)?.etag();
        let values: BTreeMap<_, _> = targets
            .iter()
            .map(|(id, target)| (id, target.value()))
            .collect();
        let value = json!({"schema":1,"classification_version":1,"base_revision":base_revision.to_string(),"next_revision":revision.to_string(),
            "base_hash":hex(base.hash()),"base_etag":base_etag,"candidate_hash":hex(prepared.candidate().hash()),
            "required_class":class_name(prepared.changes().class()),"requires_risk_admin":prepared.changes().requires_risk_admin(),
            "requires_cluster_admin":prepared.changes().requires_cluster_admin(),"affected_accounts":prepared.changes().accounts(),
            "options":options,"targets":values,"preconditions":preconditions});
        let canonical = strict::canonical(&value)?;
        if canonical.len() > MAX_BYTES {
            return Err(Error::Capacity);
        }
        let mut digest = Sha256::new();
        digest.update(b"tickwire.config.computed-plan.v1\0");
        digest.update(&canonical);
        Ok(Self {
            base_revision,
            base_hash: base.hash(),
            base_etag,
            candidate_hash: prepared.candidate().hash(),
            required_class: prepared.changes().class(),
            options,
            targets,
            preconditions,
            canonical,
            digest: digest.finalize().into(),
        })
    }
    pub fn base_revision(&self) -> u64 {
        self.base_revision
    }
    pub fn next_revision(&self) -> u64 {
        self.base_revision + 1
    }
    pub fn base_hash(&self) -> Hash {
        self.base_hash
    }
    pub fn base_etag(&self) -> &str {
        &self.base_etag
    }
    pub fn candidate_hash(&self) -> Hash {
        self.candidate_hash
    }
    pub fn required_class(&self) -> Class {
        self.required_class
    }
    pub fn options(&self) -> &PlanOptions {
        &self.options
    }
    pub fn digest(&self) -> Hash {
        self.digest
    }
    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical
    }
    pub fn targets(&self) -> &BTreeMap<String, TargetPlan> {
        &self.targets
    }
    pub fn preconditions(&self) -> &BTreeSet<Precondition> {
        &self.preconditions
    }
}
fn host(p: &Projection) -> Result<String> {
    Ok(p.value()["process"]["host_id"]
        .as_str()
        .ok_or(Error::Schema)?
        .to_owned())
}
fn class_name(class: Class) -> &'static str {
    match class {
        Class::Hot => "Hot",
        Class::Quiesce => "Quiesce",
        Class::RollingRestart => "RollingRestart",
        Class::ForbiddenRuntime => "ForbiddenRuntime",
    }
}
fn hex(hash: Hash) -> String {
    format!(
        "sha256:{}",
        hash.iter().map(|b| format!("{b:02x}")).collect::<String>()
    )
}
fn pointer(parts: &[String]) -> Result<String> {
    let mut length = 0;
    for part in parts {
        charge(&mut length, 1)?;
        charge(&mut length, part.len())?;
        charge(
            &mut length,
            part.bytes().filter(|b| matches!(b, b'~' | b'/')).count(),
        )?;
    }
    Ok(parts
        .iter()
        .map(|s| format!("/{}", s.replace('~', "~0").replace('/', "~1")))
        .collect())
}
fn charge(total: &mut usize, bytes: usize) -> Result<()> {
    *total = total.checked_add(bytes).ok_or(Error::Capacity)?;
    if *total > MAX_BYTES {
        return Err(Error::Capacity);
    }
    Ok(())
}
